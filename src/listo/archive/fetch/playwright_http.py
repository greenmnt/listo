from __future__ import annotations

import logging
import os
import random
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request
from dataclasses import dataclass

# patchright is a drop-in replacement for playwright with anti-detection
# patches (CDP, navigator props, runtime fingerprints) — needed for sites with
# Kasada / DataDome / similar bot defenses.
from patchright.sync_api import Browser, BrowserContext, Page, sync_playwright


# ─────────────────────────────────────────────────────────────────────────
# Fetch modes — a "preset" is the combination of LISTO_BROWSER + LISTO_CDP_ATTACH.
# Once Kasada flags a fingerprint, that preset stops working for some hours.
# Keeping every preset as a code path means we can rotate back to a previously
# rested one and check whether it's recovered.
#
#   LISTO_BROWSER=chromium           # patchright bundled chromium-1208
#   LISTO_BROWSER=chrome             # patchright + /usr/bin/google-chrome
#   LISTO_BROWSER=firefox            # patchright + bundled firefox
#   LISTO_CDP_ATTACH=1               # vanilla google-chrome via CDP attach (no patchright runtime)
#   LISTO_CDP_EXTERNAL_PORT=9222     # attach to a chrome the user already started
#
# Empirical state (2026-04-29):
#   chromium / chrome / firefox via patchright ─── all currently 429'd by Kasada.
#   CDP-attach (we spawn) ─── works for ~60 min / ~480 pages then 429s.
#                  Mitigated by the time-based recycle in this file.
#   CDP-attach (external) ─── attach to the user's own chrome (started with
#                  --remote-debugging-port=N --user-data-dir=DIR). Same chrome
#                  binary + GPU + fonts as their daily browser, so the
#                  fingerprint is identical to one that already passes Kasada
#                  manually. No spawn, no recycle.
#
# Setup for external CDP attach (option 2):
#   google-chrome --remote-debugging-port=9222 \
#       --user-data-dir=/tmp/listo-real-chrome &
#   # Visit https://www.realestate.com.au/ once in that window to seed cookies.
#   LISTO_CDP_EXTERNAL_PORT=9222 ./scripts/fetch_pool.sh
#
# To force-retry an old preset later (when its fingerprint may have decayed):
#   LISTO_BROWSER=chromium ./scripts/fetch_pool.sh        # try patchright again
#   LISTO_CDP_ATTACH=1 ./scripts/fetch_pool.sh            # we spawn chrome
# ─────────────────────────────────────────────────────────────────────────

from listo.config import settings
from listo.fetch.http import BlockedError, FetchResult

logger = logging.getLogger(__name__)


# Wait for a page-specific signal that the actual content (not a bot-defense
# interstitial) has loaded.
_REALESTATE_READY_JS = (
    "() => typeof window.ArgonautExchange === 'object' "
    "&& window.ArgonautExchange !== null"
)
_DOMAIN_READY_JS = (
    "() => !!document.getElementById('__NEXT_DATA__')"
)


class PlaywrightFetcher:
    """Headless-Chromium fetcher for sites with active bot defense (Kasada).

    Mirrors the curl_cffi Fetcher interface so callers don't care which is used.
    Launches a single browser context that's reused for every get() within the
    same `with` block — this lets the Kasada cookie issued on the first request
    be reused for subsequent ones, which is faster and looks more human.
    """

    # Engine selector: "chromium" (patchright's bundled, default), "chrome"
    # (the real google-chrome binary on this machine), or "firefox" (patchright
    # firefox). Choose at construction time, override globally via LISTO_BROWSER.
    # Different engines present different fingerprints to anti-bot vendors —
    # Kasada is heavily calibrated for chromium, so firefox is the easiest
    # escape hatch when the chromium fingerprint is being flagged.
    _CHROME_BINARY_CANDIDATES = (
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/snap/bin/chromium",
    )

    def __init__(
        self,
        ready_check_js: str | None = None,
        warmup_url: str | None = None,
        recycle_after_pages: int = 20,
        cookie_domain: str | None = None,
        engine: str | None = None,
    ) -> None:
        self._pw = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._ready_check = ready_check_js
        self._warmup_url = warmup_url
        self._warmed_up = False
        # Recycle (close + relaunch) the chromium context every N successful
        # page fetches, where N is rolled from `_RECYCLE_RANGE_PAGES` each
        # time a context opens. Bounds memory growth — over hundreds of pages
        # the JS heap, image cache, and DOM state in a single context climb
        # past 1GB. Each recycle costs ~10s (re-warmup re-issues the Kasada
        # cookie), so at ~22 pages mean the overhead is ~0.4s/page.
        #
        # The range (vs. a constant N) jitters session length so all our
        # sessions don't terminate at exactly the same page count — that
        # would be a session-level fingerprint Kasada could flag (real
        # users don't reload at fixed intervals).
        #
        # `recycle_after_pages` is a kept as a master toggle: any value > 0
        # enables recycling on the class range; 0 disables.
        self._recycle_after_pages = recycle_after_pages
        self._recycle_target = recycle_after_pages  # rerolled in _open_context
        self._pages_since_recycle = 0
        # Optional: prime the chromium context with cookies from the user's
        # real Chromium profile (e.g. a valid KP_UIDz Kasada token). When set,
        # injection happens after every _open_context() — so freshly recycled
        # contexts also start with a known-good session.
        self._cookie_domain = cookie_domain
        # LISTO_BROWSER overrides any constructor default — useful for env-var
        # toggling from the watchdog without touching the factory functions.
        self._engine = (os.environ.get("LISTO_BROWSER") or engine or "chromium").lower()
        if self._engine not in {"chromium", "chrome", "firefox"}:
            raise ValueError(
                f"unknown engine {self._engine!r}; expected chromium, chrome, or firefox"
            )
        # CDP-attach mode: spawn a vanilla chromium ourselves (no patchright
        # launch flags, no automation-control patches) and connect_over_cdp().
        # This is the only mode that currently bypasses Kasada — patchright's
        # runtime injection is being detected. Toggle via LISTO_CDP_ATTACH=1.
        # Forces engine=chrome since we use the user's google-chrome binary.
        #
        # External-port variant: LISTO_CDP_EXTERNAL_PORT=9222 attaches to a
        # chrome the user already started (with --remote-debugging-port=9222
        # and a separate --user-data-dir). Same physical chrome environment
        # they use daily, so the fingerprint already passes Kasada manually.
        # Setting this implies CDP-attach and disables recycling, since we
        # can't relaunch a browser we don't own.
        ext_port_raw = (os.environ.get("LISTO_CDP_EXTERNAL_PORT") or "").strip()
        self._cdp_external_port: int | None = int(ext_port_raw) if ext_port_raw else None
        self._cdp_attach = (
            self._cdp_external_port is not None
            or (os.environ.get("LISTO_CDP_ATTACH") or "").lower()
            in {"1", "true", "yes", "on"}
        )
        # State for the chromium subprocess that backs CDP mode (only used when
        # we spawn chromium ourselves — None in external-port mode).
        self._chromium_proc: subprocess.Popen | None = None
        self._cdp_port: int | None = None
        self._cdp_user_data_dir: str | None = None
        # Wall-clock when the current context started — drives the time-based
        # recycle. Initialised to 0 so the first get() doesn't trigger a
        # spurious recycle before the first context has even been opened.
        self._context_started_at: float = 0.0

    # Smallest realistic AU laptop viewports. We deliberately keep these tight
    # (≤ 1.3M pixels) to bound chromium's compositor + image-cache memory:
    # under load, viewport area drives a meaningful fraction of per-worker RSS.
    # Going below 1280 wide risks tripping the site's mobile-layout breakpoint
    # (different JS payload — possibly no `ArgonautExchange` blob), so 1280 is
    # the floor. 1366×768 is by far the most common laptop in the world; the
    # others cover Macs/Chromebooks. Each fetcher (and each post-recycle
    # context) picks one at random for fingerprint variety.
    _VIEWPORT_POOL = (
        {"width": 1280, "height": 720},
        {"width": 1280, "height": 800},
        {"width": 1366, "height": 768},
        {"width": 1440, "height": 900},
    )
    _COLOR_SCHEME_POOL = ("light", "dark", "no-preference")

    # Locale+timezone pairs we rotate through per recycle. AU-only (the site is
    # AU); Auckland is a near-neighbour timezone the site happily serves to.
    # Pairing locale and timezone consistently avoids "AU locale + Tokyo TZ"
    # type mismatches, which themselves are bot signals.
    _LOCALE_TZ_POOL = (
        ("en-AU", "Australia/Brisbane"),
        ("en-AU", "Australia/Sydney"),
        ("en-AU", "Australia/Melbourne"),
        ("en-AU", "Australia/Perth"),
        ("en-NZ", "Pacific/Auckland"),
    )

    # Force a recycle after this many wall-clock seconds even if the page
    # count threshold hasn't been hit. Empirical: in CDP-attach mode Kasada
    # starts blocking around the 60-min / ~480-pages-per-fingerprint mark.
    # Recycling proactively at ~25 min keeps each chromium well under that.
    _RECYCLE_MAX_WALL_SECONDS = 25 * 60

    # Inclusive [min, max] page count for a single chromium context's lifetime.
    # Mean ~22 pages; range chosen empirically to balance memory recovery vs.
    # the per-recycle Kasada warmup cost.
    _RECYCLE_RANGE_PAGES = (18, 27)

    def __enter__(self) -> "PlaywrightFetcher":
        # Kasada blocks anything that doesn't have a real GPU + real compositor.
        # Solution: drive chromium against a headless mutter compositor with a
        # virtual monitor. Mutter has full GPU access (NVIDIA/Intel) so chromium
        # gets real WebGL — Kasada is satisfied and no windows appear on screen.
        #
        # Setup (once per machine boot): start mutter on wayland-99:
        #   mutter --headless --virtual-monitor=1920x1080 --wayland-display=wayland-99 &
        #
        # We ALWAYS force WAYLAND_DISPLAY=wayland-99 here (overriding whatever
        # the calling shell has). If we honored the inherited value, a normal
        # shell would have WAYLAND_DISPLAY=wayland-0 (the user's live session)
        # and chromium would helpfully open windows on their real display.
        runtime_dir = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        # Default to wayland-99 (the headless mutter compositor — invisible).
        # Set LISTO_WAYLAND_DISPLAY=wayland-0 (or whatever the user's real session
        # uses) for visible-window mode — useful when the headless fingerprint is
        # being flagged and we need to fall back to "windows on the user's screen".
        target_display = os.environ.get("LISTO_WAYLAND_DISPLAY", "wayland-99")
        socket_path = f"{runtime_dir}/{target_display}"
        if not os.path.exists(socket_path):
            if target_display == "wayland-99":
                hint = (
                    f"mutter compositor socket not found at {socket_path}. Start it with:\n"
                    "  mutter --headless --virtual-monitor=1920x1080 --wayland-display=wayland-99 &"
                )
            else:
                hint = (
                    f"wayland socket {socket_path} not found. Pick the one your GUI session uses:\n"
                    f"  ls {runtime_dir}/wayland-* "
                )
            raise RuntimeError(hint)
        os.environ["WAYLAND_DISPLAY"] = target_display
        # No DISPLAY: force pure-Wayland so chromium can't fall back to X11.
        os.environ.pop("DISPLAY", None)
        # Firefox needs MOZ_ENABLE_WAYLAND=1 to use the wayland socket; chromium
        # uses --ozone-platform instead. Set unconditionally — it's a no-op for chromium.
        os.environ["MOZ_ENABLE_WAYLAND"] = "1"

        self._pw = sync_playwright().start()
        # If anything below raises, Python's context-manager protocol skips
        # __exit__ — so we'd leak `self._pw` and its asyncio loop attached to
        # this thread, which would make the *next* sync_playwright().start()
        # call raise "Sync API inside the asyncio loop". Clean up explicitly.
        try:
            self._open_context()
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *_a) -> None:
        self.close()

    def _open_context(self) -> None:
        """Launch a fresh chromium context with a freshly-randomized fingerprint.

        Called once on enter and again on each recycle. Each call picks a new
        viewport + color scheme, so within a single worker the session
        identity rotates over time (less obvious than a fixed fingerprint
        repeating thousands of pages).
        """
        viewport = random.choice(self._VIEWPORT_POOL)
        color_scheme = random.choice(self._COLOR_SCHEME_POOL)
        locale, tz = random.choice(self._LOCALE_TZ_POOL)
        logger.info(
            "PlaywrightFetcher fingerprint: engine=%s cdp=%s viewport=%s color=%s locale=%s tz=%s",
            self._engine, self._cdp_attach, viewport, color_scheme, locale, tz,
        )

        # Options that apply to a BrowserContext (whether created via
        # launch_persistent_context or new_context).
        context_opts = dict(
            viewport=viewport,
            color_scheme=color_scheme,
            locale=locale,
            timezone_id=tz,
            # Real users have already granted geolocation on most major sites.
            # A fresh context with no permissions is itself a "freshly minted"
            # signal anti-bot vendors flag.
            permissions=["geolocation"],
        )
        # launch-level options (not valid for new_context).
        launch_opts = dict(
            user_data_dir="",
            headless=False,    # Kasada detects --headless even with patchright
            # 200ms gap before each Playwright command. Adds ~5-10s per real
            # listing fetch but makes request cadence look human, not scripted.
            slow_mo=200,
        )
        if self._cdp_attach:
            # Spawn vanilla chromium ourselves and attach over CDP — no
            # patchright launch flags or runtime patches at all. Each
            # _open_context() spawns a fresh chromium so recycle works as
            # before. In external-port mode we skip the spawn entirely and
            # connect to the chrome the user already started.
            if self._cdp_external_port is not None:
                self._cdp_port = self._cdp_external_port
            else:
                self._spawn_chromium_for_cdp()
            self._browser = self._pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{self._cdp_port}"
            )
            # Reuse the default context (vanilla chromium starts with one) —
            # creating a new BrowserContext over CDP applies playwright wrapping
            # to the new context, which is part of what Kasada flags. The
            # existing default context has none of that wrapping.
            self._context = (
                self._browser.contexts[0]
                if self._browser.contexts
                else self._browser.new_context(**context_opts)
            )
            # Even on the existing default context we can grant permissions
            # post-hoc via CDP — matches what real users have done by clicking
            # "Allow" on past sites.
            try:
                self._context.grant_permissions(["geolocation"])
            except Exception:  # noqa: BLE001
                pass
        elif self._engine == "firefox":
            # Firefox doesn't accept --ozone-platform; MOZ_ENABLE_WAYLAND in env
            # handles the wayland binding. No anti-detection flags either.
            self._context = self._pw.firefox.launch_persistent_context(
                **launch_opts, **context_opts,
            )
        else:
            chrome_args = [
                "--ozone-platform=wayland",
                "--enable-features=UseOzonePlatform",
                # First-run dialogs / onboarding pop a different DOM that some
                # sites' fingerprint scripts notice on first navigation.
                "--no-first-run",
                # Explicitly disable the AutomationControlled blink feature
                # (belt-and-braces with ignore_default_args below).
                "--disable-blink-features=AutomationControlled",
                # Stop chromium from leaking the local LAN IP via WebRTC even
                # when public IP looks fine — a known Kasada / DataDome signal.
                "--force-webrtc-ip-handling-policy",
            ]
            kwargs = dict(
                launch_opts,
                **context_opts,
                args=chrome_args,
                # Strips Playwright's default --enable-automation flag, which
                # wires up the "Chrome is being controlled" infobar and the
                # navigator.webdriver = true marker.
                ignore_default_args=["--enable-automation"],
            )
            if self._engine == "chrome":
                # Use the real google-chrome binary on this machine instead of
                # patchright's bundled chromium-1208. Pick the first that exists.
                exe = next(
                    (p for p in self._CHROME_BINARY_CANDIDATES if os.path.exists(p)),
                    None,
                )
                if exe is None:
                    raise RuntimeError(
                        "LISTO_BROWSER=chrome but no chrome binary found in "
                        f"{self._CHROME_BINARY_CANDIDATES}"
                    )
                kwargs["executable_path"] = exe
            self._context = self._pw.chromium.launch_persistent_context(**kwargs)
        # Inject user-browser cookies BEFORE creating a page — once a page has
        # navigated, add_cookies still works but the in-flight requests on
        # that page won't carry the new cookies.
        #
        # External-port mode skips this: that chrome already has whatever
        # cookies the user seeded by visiting realestate.com.au manually.
        # Overlaying cookies from a different chromium profile on top would
        # mix sessions and likely trip Kasada.
        if self._cookie_domain and self._cdp_external_port is None:
            self._inject_cookies()
        # In CDP mode (we-spawned) the default context already has an
        # about:blank page from chromium's startup. Reuse it to keep the
        # wrapping minimal. In external-port mode we open a fresh tab so we
        # don't commandeer whatever the user has open.
        if self._cdp_attach and self._cdp_external_port is None and self._context.pages:
            self._page = self._context.pages[0]
        else:
            self._page = self._context.new_page()
        self._log_egress_ip()
        self._warmed_up = False
        self._pages_since_recycle = 0
        self._context_started_at = time.time()
        # External-port mode: disable recycling — we can't relaunch a chrome
        # we don't own, and reconnecting playwright to the same chrome over
        # and over buys us nothing.
        if self._cdp_external_port is not None:
            self._recycle_after_pages = 0
        # Roll a fresh page-count threshold for this context so sessions
        # don't all terminate at the same number.
        if self._recycle_after_pages > 0:
            lo, hi = self._RECYCLE_RANGE_PAGES
            self._recycle_target = random.randint(lo, hi)

    def _inject_cookies(self) -> None:
        """Pull cookies from the user's real Chromium profile and seed the
        patchright context. Lets the scraper inherit a valid Kasada session
        instead of solving the JS challenge from scratch each time."""
        from listo.fetch.cookies import (
            have_kasada_token,
            load_playwright_cookies_for,
        )
        try:
            cookies = load_playwright_cookies_for(self._cookie_domain)
        except Exception as e:  # noqa: BLE001
            logger.warning("cookie load failed for %s: %s", self._cookie_domain, e)
            return
        if not cookies:
            logger.info("no cookies found for %s", self._cookie_domain)
            return
        try:
            self._context.add_cookies(cookies)
        except Exception as e:  # noqa: BLE001
            logger.warning("add_cookies failed (%s); continuing without", e)
            return
        names = {c["name"]: c["value"] for c in cookies}
        logger.info(
            "injected %d cookies for %s (kasada token: %s)",
            len(cookies), self._cookie_domain, have_kasada_token(names),
        )

    def _log_egress_ip(self) -> None:
        """Log the egress IP the browser is using.

        Useful for confirming proxy / mobile-tether routing is in effect, and
        for correlating Kasada 429s with IP changes across recycles.
        """
        if self._page is None:
            return
        try:
            response = self._page.goto(
                "https://api.ipify.org?format=json",
                wait_until="domcontentloaded",
                timeout=10_000,
            )
            status = response.status if response is not None else 0
            body = self._page.content()
        except Exception as e:  # noqa: BLE001
            logger.warning("egress IP probe failed: %s", e)
            return
        import re
        m = re.search(r'"ip"\s*:\s*"([^"]+)"', body)
        ip = m.group(1) if m else "unknown"
        logger.info("egress IP: %s (probe status=%s)", ip, status)

    def _spawn_chromium_for_cdp(self) -> None:
        """Launch a vanilla chromium with --remote-debugging-port for CDP attach.

        Crucially: NO --no-sandbox, --disable-blink-features=AutomationControlled,
        or --remote-debugging-pipe — those are the flags Kasada is fingerprinting.
        We pick a free port per worker so multiple workers don't collide.
        """
        exe = next(
            (p for p in self._CHROME_BINARY_CANDIDATES if os.path.exists(p)),
            None,
        )
        if exe is None:
            raise RuntimeError(
                "LISTO_CDP_ATTACH=1 but no chrome binary found in "
                f"{self._CHROME_BINARY_CANDIDATES}"
            )
        # Grab a free port. There's a tiny race between close-and-launch but
        # chromium will fail loudly if the port is taken, so we'll know.
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self._cdp_port = s.getsockname()[1]
        self._cdp_user_data_dir = tempfile.mkdtemp(prefix="listo_cdp_")
        cmd = [
            exe,
            f"--remote-debugging-port={self._cdp_port}",
            "--ozone-platform=wayland",
            f"--user-data-dir={self._cdp_user_data_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "about:blank",
        ]
        # start_new_session=True puts chromium in its own session+pgid (so its
        # PID is also its PGID). On teardown we signal the entire group, which
        # reaches the GPU / network / utility helper processes that chromium
        # spawns — those would otherwise survive a plain SIGTERM to the main
        # process and leak across recycles.
        self._chromium_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        # Wait up to 15s for the debug port to come up.
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{self._cdp_port}/json/version", timeout=1
                ).read()
                logger.info(
                    "CDP chromium ready on port %d (PID=%d, user-data-dir=%s)",
                    self._cdp_port, self._chromium_proc.pid, self._cdp_user_data_dir,
                )
                return
            except Exception:
                if self._chromium_proc.poll() is not None:
                    raise RuntimeError(
                        f"chromium subprocess exited (code={self._chromium_proc.returncode}) "
                        "before debug port came up"
                    )
                time.sleep(0.5)
        # Timeout — clean up before raising.
        self._terminate_chromium_for_cdp()
        raise RuntimeError(
            f"chromium debug port {self._cdp_port} never came up within 15s"
        )

    def _terminate_chromium_for_cdp(self) -> None:
        """Stop the chromium subprocess and clean up its profile dir."""
        # Cache user_data_dir — needed for the post-pgid sweep below.
        user_data_dir = self._cdp_user_data_dir
        if self._chromium_proc is not None:
            pid = self._chromium_proc.pid
            try:
                # Kill the entire process group (chromium spawns GPU / network
                # / utility helpers that aren't direct children).
                os.killpg(pid, signal.SIGTERM)
                try:
                    self._chromium_proc.wait(5)
                except subprocess.TimeoutExpired:
                    os.killpg(pid, signal.SIGKILL)
                    self._chromium_proc.wait(2)
            except ProcessLookupError:
                pass
            except Exception:  # noqa: BLE001
                pass
            self._chromium_proc = None
        # Chromium re-pgids its GPU/network/utility helpers internally for its
        # sandbox model, so they escape `killpg` above. Find them by their
        # --user-data-dir flag (unique per fetcher instance) and force-kill.
        if user_data_dir:
            try:
                subprocess.run(
                    ["pkill", "-KILL", "-f", user_data_dir],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                )
            except Exception:  # noqa: BLE001
                pass
            if os.path.exists(user_data_dir):
                shutil.rmtree(user_data_dir, ignore_errors=True)
            self._cdp_user_data_dir = None
        self._cdp_port = None

    def _close_context(self) -> None:
        """Close the chromium context but keep the playwright dispatcher alive."""
        if self._page is not None:
            try:
                self._page.close()
            except Exception:  # noqa: BLE001
                pass
            self._page = None
        # In external-port mode the "context" is the user's default context —
        # closing it would close every tab they have open. Skip; we only close
        # our own page above.
        if self._context is not None and self._cdp_external_port is None:
            try:
                self._context.close()
            except Exception:  # noqa: BLE001
                pass
        self._context = None
        # In CDP mode, also disconnect from the browser and tear down the
        # chromium subprocess we spawned. Each recycle gets a fresh chromium.
        # External-port mode: just disconnect the playwright Browser handle
        # (a thin CDP wrapper), leaving the user's chrome process running.
        if self._cdp_attach:
            if self._browser is not None:
                try:
                    self._browser.close()
                except Exception:  # noqa: BLE001
                    pass
                self._browser = None
            if self._cdp_external_port is None:
                self._terminate_chromium_for_cdp()

    def _recycle(self) -> None:
        """Drop accumulated chromium state by relaunching the context.

        After hundreds of pages, a single chromium context's JS heap + DOM +
        image cache balloons past 1GB. Closing and relaunching reclaims it.
        Note: the Kasada session cookie is lost too, but the next get() will
        re-warmup against the homepage and Kasada will re-issue.
        """
        logger.info("recycling chromium context after %d pages",
                    self._pages_since_recycle)
        self._close_context()
        self._open_context()

    def close(self) -> None:
        self._close_context()
        if self._browser is not None:
            try:
                self._browser.close()
            except Exception:  # noqa: BLE001
                pass
        if self._pw is not None:
            try:
                self._pw.stop()
            except Exception:  # noqa: BLE001
                pass
        # Belt-and-braces: even if _close_context already terminated chromium,
        # make sure nothing's leaked here on abnormal exit paths. Skipped in
        # external-port mode — that chrome belongs to the user.
        if self._cdp_external_port is None:
            self._terminate_chromium_for_cdp()

    def _sleep_jitter(self) -> None:
        # CDP-attach mode is more sensitive to request rate (Kasada's IP-level
        # score climbs faster), so default to a wider 5-12s spread when in CDP.
        # Caller-set LISTO_REQUEST_MIN/MAX_DELAY env vars (read into settings)
        # still override this — the wider bounds are a safer default, not a cap.
        if self._cdp_attach and settings.request_min_delay < 5.0:
            lo, hi = 5.0, 12.0
        else:
            lo, hi = settings.request_min_delay, settings.request_max_delay
        time.sleep(random.uniform(lo, hi))

    def _warmup(self) -> None:
        if self._warmed_up or not self._warmup_url or self._page is None:
            return
        logger.info("warmup: %s", self._warmup_url)
        t0 = time.time()
        try:
            # `domcontentloaded` is enough — Kasada keeps the request log busy
            # so `networkidle` never fires. Give the JS challenge time to finish
            # via the explicit sleep below.
            response = self._page.goto(
                self._warmup_url,
                wait_until="domcontentloaded",
                timeout=settings.fetch_timeout * 1000,
            )
            status = response.status if response is not None else 0
            title = ""
            try:
                title = self._page.title() or ""
            except Exception:  # noqa: BLE001
                pass
            logger.info(
                "warmup response: status=%s title=%r (%.1fs)",
                status, title[:80], time.time() - t0,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("warmup navigation failed: %s", e)
        time.sleep(random.uniform(4.0, 6.0))
        self._warmed_up = True

    # Back-off ladder for 429 (rate-limited): 30s, 2min, 8min, 30min, then give up.
    _RETRY_429_DELAYS = (30, 120, 480, 1800)
    # Back-off ladder for 403 (blocked / challenge failed): just one attempt
    # since 403 is usually a sustained ban not a transient burst.
    _RETRY_403_DELAYS = (60,)

    def get(self, url: str) -> FetchResult:
        if self._page is None:
            raise RuntimeError("Fetcher not entered (use `with PlaywrightFetcher() as f:`)")
        # Trigger recycle on EITHER condition: enough pages OR enough wall time.
        # The wall-time bound is the new lever — Kasada's risk score for a
        # fingerprint accumulates over real time, not just per-request, so
        # rotating before we cross its threshold (~60 min observed) is what
        # keeps us under the radar.
        page_recycle = (
            self._recycle_after_pages > 0
            and self._pages_since_recycle >= self._recycle_target
        )
        time_recycle = (
            self._recycle_after_pages > 0
            and (time.time() - self._context_started_at) >= self._RECYCLE_MAX_WALL_SECONDS
        )
        if page_recycle or time_recycle:
            logger.info(
                "recycling: reason=%s pages=%d/%d wall=%ds/%ds",
                "time" if time_recycle else "pages",
                self._pages_since_recycle, self._recycle_target,
                int(time.time() - self._context_started_at), self._RECYCLE_MAX_WALL_SECONDS,
            )
            self._recycle()
        self._warmup()

        attempt = 0
        while True:
            attempt += 1
            self._sleep_jitter()
            try:
                response = self._page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=settings.fetch_timeout * 1000,
                )
            except Exception as e:  # noqa: BLE001
                # Transient navigation failure (timeout / net::ERR_*). Retry with
                # the same back-off ladder as 429.
                if attempt <= len(self._RETRY_429_DELAYS):
                    wait = self._RETRY_429_DELAYS[attempt - 1]
                    logger.warning("nav failed (%s); attempt %d, sleeping %ds", e, attempt, wait)
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"playwright goto failed after {attempt} tries: {e}") from e

            status = response.status if response is not None else 0
            if status == 429:
                if attempt <= len(self._RETRY_429_DELAYS):
                    wait = self._RETRY_429_DELAYS[attempt - 1]
                    logger.warning("429 from %s; attempt %d, sleeping %ds", url, attempt, wait)
                    time.sleep(wait)
                    continue
                raise BlockedError(f"429 Too Many Requests from {url} after {attempt} tries")
            if status == 403:
                if attempt <= len(self._RETRY_403_DELAYS):
                    wait = self._RETRY_403_DELAYS[attempt - 1]
                    logger.warning("403 from %s; attempt %d, sleeping %ds", url, attempt, wait)
                    time.sleep(wait)
                    continue
                raise BlockedError(f"403 Forbidden from {url} after {attempt} tries")

            if self._ready_check:
                try:
                    self._page.wait_for_function(
                        self._ready_check, timeout=settings.fetch_timeout * 1000
                    )
                except Exception:
                    logger.warning("readiness check timed out for %s", url)

            body = self._page.content()
            headers = response.headers if response is not None else {}
            self._pages_since_recycle += 1
            return FetchResult(url=url, status=status, body=body, headers=headers)


def realestate_fetcher() -> PlaywrightFetcher:
    """Factory: PlaywrightFetcher tuned for realestate.com.au (waits for ArgonautExchange)."""
    return PlaywrightFetcher(
        ready_check_js=_REALESTATE_READY_JS,
        warmup_url="https://www.realestate.com.au/",
        # Inject Kasada session cookies from the user's real Chromium so each
        # context starts pre-authorised instead of having to solve the JS
        # challenge cold (which has been failing under elevated bot-risk).
        cookie_domain="realestate.com.au",
    )


def domain_fetcher() -> PlaywrightFetcher:
    """Factory: PlaywrightFetcher tuned for domain.com.au (waits for __NEXT_DATA__)."""
    return PlaywrightFetcher(
        ready_check_js=_DOMAIN_READY_JS,
        warmup_url="https://www.domain.com.au/",
    )
