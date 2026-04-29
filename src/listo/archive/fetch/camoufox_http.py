from __future__ import annotations

import logging
import os
import random
import time
from pathlib import Path
from urllib.parse import urlparse, unquote

# camoufox is a stealth-patched Firefox build with extensive anti-fingerprint
# work (Navigator props, WebGL, Canvas, font fingerprint, etc.) that the
# patchright-based path can't match. It's a drop-in for Playwright Firefox:
# Camoufox(...) is a context manager that yields a Playwright Browser.
from camoufox.sync_api import Camoufox

from listo.config import settings
from listo.fetch.http import BlockedError, FetchResult

logger = logging.getLogger(__name__)


# Same readiness signal as PlaywrightFetcher — we wait until realestate's
# ArgonautExchange JSON blob exists, which means the actual page (not a
# Kasada interstitial) has finished its boot.
_REALESTATE_READY_JS = (
    "() => typeof window.ArgonautExchange === 'object' "
    "&& window.ArgonautExchange !== null"
)


class CamoufoxFetcher:
    """Camoufox-based fetcher for sites with active bot defense (Kasada).

    Mirrors PlaywrightFetcher's public surface so callers don't care which is
    used: same FetchResult, same BlockedError, same context-manager pattern.

    Camoufox patches Firefox at the binary + JS-injection level rather than
    via Playwright's runtime hooks, which is why it survives fingerprint
    checks that patchright currently fails on realestate.com.au. We pin
    fingerprint params at launch and rotate them per recycle.
    """

    # Window pool — bounded compositor + image-cache memory. Floor of 1280
    # avoids tripping mobile-layout breakpoints (the realestate site ships a
    # different JS bundle below ~1024px which doesn't expose ArgonautExchange).
    _WINDOW_POOL = (
        (1280, 720),
        (1280, 800),
        (1366, 768),
        (1440, 900),
    )
    # AU-only locales (the site is AU). en-NZ is a near-neighbour the site
    # serves cleanly. Camoufox auto-resolves timezone from geoip, so we don't
    # need to pin a paired timezone here.
    _LOCALE_POOL = ("en-AU", "en-NZ")

    # Recycle the entire Camoufox instance after this many wall-clock seconds
    # OR this many pages. Rotating the *whole* browser (not just the context)
    # is what actually resets the fingerprint Camoufox computed at launch.
    # Numbers cribbed from PlaywrightFetcher's empirical Kasada thresholds.
    _RECYCLE_MAX_WALL_SECONDS = 25 * 60
    _RECYCLE_RANGE_PAGES = (18, 27)

    # Same back-off ladders as PlaywrightFetcher.
    _RETRY_429_DELAYS = (30, 120, 480, 1800)
    _RETRY_403_DELAYS = (60,)

    def __init__(
        self,
        ready_check_js: str | None = None,
        warmup_url: str | None = None,
        recycle_after_pages: int = 20,
        cookie_domain: str | None = None,
    ) -> None:
        self._ready_check = ready_check_js
        self._warmup_url = warmup_url
        self._cookie_domain = cookie_domain
        self._recycle_after_pages = recycle_after_pages
        self._recycle_target = recycle_after_pages
        self._pages_since_recycle = 0
        self._context_started_at = 0.0

        # Camoufox lifecycle handles. _cf_cm is the Camoufox(...) context
        # manager; we hold its return value (the Playwright Browser) directly.
        self._cf_cm = None
        self._browser = None
        self._context = None
        self._page = None
        self._warmed_up = False

        # Optional residential proxy. Parsed once at construction so misformed
        # URLs fail fast instead of on the first recycle.
        self._proxy = self._parse_proxy_env()

    @staticmethod
    def _parse_proxy_env() -> dict | None:
        """Parse LISTO_PROXY_URL into a Playwright-shaped proxy dict.

        Accepts http://user:pass@host:port or http://host:port. Returns None
        if the env var is unset or empty so Camoufox launches direct.
        """
        raw = os.environ.get("LISTO_PROXY_URL", "").strip()
        if not raw:
            return None
        u = urlparse(raw)
        if not u.hostname or not u.scheme:
            raise ValueError(
                f"LISTO_PROXY_URL={raw!r} is not a valid URL "
                "(expected scheme://[user:pass@]host:port)"
            )
        port_part = f":{u.port}" if u.port else ""
        proxy: dict[str, str] = {"server": f"{u.scheme}://{u.hostname}{port_part}"}
        if u.username:
            proxy["username"] = unquote(u.username)
        if u.password:
            proxy["password"] = unquote(u.password)
        return proxy

    def __enter__(self) -> "CamoufoxFetcher":
        # Camoufox is Firefox-based, so we need MOZ_ENABLE_WAYLAND=1 and a
        # working wayland socket. Reuse the same mutter-on-wayland-99 trick
        # as PlaywrightFetcher: real GPU, no visible windows, Kasada-friendly
        # WebGL fingerprint.
        runtime_dir = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
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
        os.environ.pop("DISPLAY", None)
        os.environ["MOZ_ENABLE_WAYLAND"] = "1"

        try:
            self._open_browser()
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *_a) -> None:
        self.close()

    def _open_browser(self) -> None:
        """Launch a fresh Camoufox instance with a freshly-randomized fingerprint.

        Called once on enter and again on each recycle. Camoufox stamps the
        fingerprint at launch, so a recycle has to relaunch the whole browser
        (unlike the patchright path where context-only recycle is enough).
        """
        window = random.choice(self._WINDOW_POOL)
        locale = random.choice(self._LOCALE_POOL)
        logger.info(
            "CamoufoxFetcher fingerprint: window=%s locale=%s proxy=%s",
            window, locale, "yes" if self._proxy else "no",
        )

        # Persistent profile dir under ~/.cache so the solved Kasada session
        # (cookies, IndexedDB, localStorage) survives between recycles and
        # across process restarts. The roundproxies + hackernoon Kasada
        # writeups both converge on this: solve the challenge once, then
        # subsequent loads recognise the previously-solved session.
        profile_dir = Path.home() / ".cache" / "listo" / "camoufox-profile"
        profile_dir.mkdir(parents=True, exist_ok=True)

        kwargs: dict = dict(
            # headless=False against the wayland socket — Kasada checks WebGL
            # and software-rasterized headless fails. mutter --headless gives
            # us real GPU without visible windows.
            headless=False,
            # Spoof Windows: HackerNoon's Kasada writeup explicitly used
            # os="windows" and it worked. Linux Chrome/Firefox is rare in the
            # wild (~2-3% of users) so a Linux UA is itself a bot signal to
            # detectors that expect the dominant Windows pattern.
            os="windows",
            locale=locale,
            # geoip=True auto-resolves timezone + lat/lon from the egress IP.
            # Without a proxy this is the user's real (AU) IP, which lines up
            # nicely with the en-AU locale. With a proxy it'll re-resolve from
            # the proxy's exit IP.
            geoip=True,
            # humanize adds small mouse movements and click delays. The
            # behavioural-detection layer Kasada runs flags zero-mouse,
            # straight-line interaction patterns; humanize defeats that.
            humanize=True,
            window=window,
            block_images=False,
            persistent_context=True,
            user_data_dir=str(profile_dir),
        )
        if self._proxy is not None:
            kwargs["proxy"] = self._proxy

        # When persistent_context=True, Camoufox yields a BrowserContext
        # directly (Firefox's launch_persistent_context returns a context, not
        # a Browser). Detect by interface so the rest of the code works
        # regardless of which mode we're in.
        self._cf_cm = Camoufox(**kwargs)
        yielded = self._cf_cm.__enter__()
        if hasattr(yielded, "new_context"):
            self._browser = yielded
            self._context = self._browser.new_context()
        else:
            self._browser = None
            self._context = yielded

        if self._cookie_domain:
            self._inject_cookies()
        # In persistent-context mode the context already has an about:blank
        # page from launch — reuse it instead of creating a second.
        if self._context.pages:
            self._page = self._context.pages[0]
        else:
            self._page = self._context.new_page()
        self._log_egress_ip()

        self._warmed_up = False
        self._pages_since_recycle = 0
        self._context_started_at = time.time()
        if self._recycle_after_pages > 0:
            lo, hi = self._RECYCLE_RANGE_PAGES
            self._recycle_target = random.randint(lo, hi)

    def _inject_cookies(self) -> None:
        """Seed the context with cookies from the user's real Chromium profile.

        Lets us start with a valid Kasada session instead of solving the JS
        challenge cold from a fresh Firefox profile every recycle.
        """
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

    def _close_browser(self) -> None:
        """Tear down the current Camoufox instance, leaving env intact."""
        if self._page is not None:
            try:
                self._page.close()
            except Exception:  # noqa: BLE001
                pass
            self._page = None
        if self._context is not None:
            try:
                self._context.close()
            except Exception:  # noqa: BLE001
                pass
            self._context = None
        if self._cf_cm is not None:
            try:
                self._cf_cm.__exit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass
            self._cf_cm = None
            self._browser = None

    def _recycle(self) -> None:
        """Close the current Camoufox + relaunch with fresh fingerprint.

        Cost is ~3-5s of Firefox startup per recycle; at the default ~22-page
        recycle interval that's well under 1% overhead.
        """
        logger.info("recycling camoufox after %d pages", self._pages_since_recycle)
        self._close_browser()
        self._open_browser()

    def close(self) -> None:
        self._close_browser()

    def _sleep_jitter(self) -> None:
        # The user's source recommends 1-5s; we go 3-7s by default since
        # CDP-mode in the patchright path empirically needed 5-12s and Kasada
        # tracks request rate over rolling windows. Caller-set
        # LISTO_REQUEST_MIN/MAX_DELAY env vars (read into settings) override.
        if settings.request_min_delay < 3.0:
            lo, hi = 3.0, 7.0
        else:
            lo, hi = settings.request_min_delay, settings.request_max_delay
        time.sleep(random.uniform(lo, hi))

    def _warmup(self) -> None:
        if self._warmed_up or not self._warmup_url or self._page is None:
            return
        logger.info("warmup: %s", self._warmup_url)
        try:
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
            logger.info("warmup response: status=%s title=%r", status, title[:80])
        except Exception as e:  # noqa: BLE001
            logger.warning("warmup navigation failed: %s", e)
        time.sleep(random.uniform(4.0, 6.0))
        self._warmed_up = True

    def get(self, url: str) -> FetchResult:
        if self._page is None:
            raise RuntimeError("Fetcher not entered (use `with CamoufoxFetcher() as f:`)")
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
                if attempt <= len(self._RETRY_429_DELAYS):
                    wait = self._RETRY_429_DELAYS[attempt - 1]
                    logger.warning("nav failed (%s); attempt %d, sleeping %ds", e, attempt, wait)
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"camoufox goto failed after {attempt} tries: {e}") from e

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


def realestate_camoufox_fetcher() -> CamoufoxFetcher:
    """Factory: CamoufoxFetcher tuned for realestate.com.au."""
    return CamoufoxFetcher(
        ready_check_js=_REALESTATE_READY_JS,
        warmup_url="https://www.realestate.com.au/",
        cookie_domain="realestate.com.au",
    )
