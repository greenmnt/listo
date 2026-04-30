"""Shared helper for fetching pages through the user's running Chrome.

The bypass for realestate.com.au's Kasada wall is to drive a real Chrome
that the user already has running with `--remote-debugging-port=9222
--user-data-dir=/tmp/listo-chrome`. Once the user has visited
realestate.com.au at least once manually, Kasada has issued the cookies
and we can navigate via CDP without tripping the wall.

This module exposes:
- `connect()` — context manager yielding a Playwright browser bound to CDP.
- `fetch_html(url, *, wait_for=...)` — one-shot fetch returning (status, html).
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass

from playwright.sync_api import sync_playwright


CDP_URL = "http://localhost:9222"
DEFAULT_TIMEOUT_MS = 30_000

logger = logging.getLogger(__name__)


class CdpUnavailableError(RuntimeError):
    """Chrome isn't reachable on :9222. Surface a clear error to the user."""


@dataclass
class CdpFetchResult:
    url: str
    final_url: str
    http_status: int
    html: str
    elapsed_seconds: float


@contextmanager
def cdp_session():
    """Open a Playwright session attached to the running Chrome.

    Yields a (browser, context) pair. Uses the existing default context
    (so cookies/Kasada session is shared with the user's interactive
    browsing).
    """
    with sync_playwright() as pw:
        try:
            browser = pw.chromium.connect_over_cdp(CDP_URL, timeout=5_000)
        except Exception as exc:  # noqa: BLE001
            raise CdpUnavailableError(
                "Couldn't connect to Chrome on http://localhost:9222.\n"
                "Launch Chrome with:\n"
                "  google-chrome --remote-debugging-port=9222 --user-data-dir=/tmp/listo-chrome\n"
                "and visit realestate.com.au once manually so the Kasada cookie issues."
            ) from exc
        if not browser.contexts:
            raise CdpUnavailableError("Chrome has no browser contexts — open a tab first.")
        ctx = browser.contexts[0]
        try:
            yield browser, ctx
        finally:
            # We don't close the user's browser; Playwright will detach.
            pass


def fetch_html(
    url: str,
    *,
    wait_for_function: str | None = None,
    wait_until: str = "domcontentloaded",
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
    settle_ms: int = 0,
) -> CdpFetchResult:
    """Open a fresh tab, navigate to `url`, return parsed result.

    `wait_for_function` is JS that should return truthy once the page is
    parse-ready (e.g. `() => !!window.ArgonautExchange`). Fails open if
    the function never resolves — we still return whatever HTML we have.
    """
    t0 = time.time()
    with cdp_session() as (_browser, ctx):
        page = ctx.new_page()
        try:
            resp = page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            if wait_for_function:
                try:
                    page.wait_for_function(wait_for_function, timeout=10_000)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("wait_for_function timed out for %s: %s", url, exc)
            if settle_ms > 0:
                page.wait_for_timeout(settle_ms)
            html = page.content()
            return CdpFetchResult(
                url=url,
                final_url=page.url,
                http_status=resp.status if resp else 0,
                html=html,
                elapsed_seconds=time.time() - t0,
            )
        finally:
            page.close()
