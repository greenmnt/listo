from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass

# patchright is a drop-in replacement for playwright with anti-detection
# patches (CDP, navigator props, runtime fingerprints) — needed for sites with
# Kasada / DataDome / similar bot defenses.
from patchright.sync_api import Browser, BrowserContext, Page, sync_playwright

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

    def __init__(
        self, ready_check_js: str | None = None, warmup_url: str | None = None
    ) -> None:
        self._pw = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._ready_check = ready_check_js
        self._warmup_url = warmup_url
        self._warmed_up = False

    def __enter__(self) -> "PlaywrightFetcher":
        # Kasada blocks anything that doesn't have a real GPU + real compositor.
        # Solution: drive chromium against a headless mutter compositor with a
        # virtual monitor. Mutter has full GPU access (NVIDIA/Intel) so chromium
        # gets real WebGL — Kasada is satisfied and no windows appear on screen.
        #
        # Setup (once): start mutter on wayland-99, then export WAYLAND_DISPLAY:
        #   mutter --headless --virtual-monitor=1920x1080 --wayland-display=wayland-99 &
        #   export WAYLAND_DISPLAY=wayland-99
        wayland_display = os.environ.get("WAYLAND_DISPLAY")
        if not wayland_display:
            raise RuntimeError(
                "WAYLAND_DISPLAY not set. Start a headless mutter compositor first:\n"
                "  mutter --headless --virtual-monitor=1920x1080 --wayland-display=wayland-99 &\n"
                "  export WAYLAND_DISPLAY=wayland-99"
            )
        runtime_dir = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        socket_path = f"{runtime_dir}/{wayland_display}"
        if not os.path.exists(socket_path):
            raise RuntimeError(
                f"WAYLAND_DISPLAY={wayland_display} but socket {socket_path} doesn't exist. "
                "Start mutter --headless first (see playwright_http.py docstring)."
            )
        # No DISPLAY: force pure-Wayland so chromium can't fall back to X11.
        os.environ.pop("DISPLAY", None)

        self._pw = sync_playwright().start()
        self._context = self._pw.chromium.launch_persistent_context(
            user_data_dir="",  # ephemeral profile per fetcher
            headless=False,    # Kasada detects --headless even with patchright
            no_viewport=True,
            locale="en-AU",
            timezone_id="Australia/Brisbane",
            args=["--ozone-platform=wayland", "--enable-features=UseOzonePlatform"],
        )
        self._page = self._context.new_page()
        return self

    def __exit__(self, *_a) -> None:
        self.close()

    def close(self) -> None:
        if self._context is not None:
            try:
                self._context.close()
            except Exception:  # noqa: BLE001
                pass
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

    def _sleep_jitter(self) -> None:
        delay = random.uniform(settings.request_min_delay, settings.request_max_delay)
        time.sleep(delay)

    def _warmup(self) -> None:
        if self._warmed_up or not self._warmup_url or self._page is None:
            return
        logger.info("warmup: %s", self._warmup_url)
        try:
            # `domcontentloaded` is enough — Kasada keeps the request log busy
            # so `networkidle` never fires. Give the JS challenge time to finish
            # via the explicit sleep below.
            self._page.goto(
                self._warmup_url,
                wait_until="domcontentloaded",
                timeout=settings.fetch_timeout * 1000,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("warmup navigation failed: %s", e)
        time.sleep(random.uniform(3.0, 5.0))
        self._warmed_up = True

    def get(self, url: str) -> FetchResult:
        if self._page is None:
            raise RuntimeError("Fetcher not entered (use `with PlaywrightFetcher() as f:`)")
        self._warmup()
        self._sleep_jitter()
        # `domcontentloaded` returns when the initial HTML is parsed; Kasada's
        # challenge JS will already have started. We then wait for the readiness
        # check to confirm the real page (not the challenge wall) has rendered.
        try:
            response = self._page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=settings.fetch_timeout * 1000,
            )
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"playwright goto failed: {e}") from e

        status = response.status if response is not None else 0
        if status == 403:
            raise BlockedError(f"403 Forbidden from {url}")
        if status == 429:
            raise RuntimeError(f"429 Too Many Requests from {url}")

        if self._ready_check:
            try:
                self._page.wait_for_function(
                    self._ready_check, timeout=settings.fetch_timeout * 1000
                )
            except Exception:
                logger.warning("readiness check timed out for %s", url)

        body = self._page.content()
        headers = response.headers if response is not None else {}
        return FetchResult(url=url, status=status, body=body, headers=headers)


def realestate_fetcher() -> PlaywrightFetcher:
    """Factory: PlaywrightFetcher tuned for realestate.com.au (waits for ArgonautExchange)."""
    return PlaywrightFetcher(
        ready_check_js=_REALESTATE_READY_JS,
        warmup_url="https://www.realestate.com.au/",
    )


def domain_fetcher() -> PlaywrightFetcher:
    """Factory: PlaywrightFetcher tuned for domain.com.au (waits for __NEXT_DATA__)."""
    return PlaywrightFetcher(
        ready_check_js=_DOMAIN_READY_JS,
        warmup_url="https://www.domain.com.au/",
    )
