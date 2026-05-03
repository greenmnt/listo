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
import os
import random
import time
from contextlib import contextmanager
from dataclasses import dataclass

# Silence Playwright's internal Node `url.parse()` deprecation banner
# (DEP0169). Has to be set BEFORE `sync_playwright` spawns its Node
# driver subprocess. Harmless — it's an upstream-Playwright issue.
os.environ.setdefault("NODE_NO_WARNINGS", "1")

from playwright.sync_api import sync_playwright  # noqa: E402


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


def fetch_html_via_google_click(
    url: str,
    *,
    wait_for_function: str | None = None,
    wait_until: str = "domcontentloaded",
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
    settle_seconds: float = 4.0,
) -> CdpFetchResult:
    """Fetch `url` by clicking through a Google search result.

    Why: REA's Kasada wall scores requests partly on the Referer
    header. A direct `page.goto(rea_url)` arrives with no Referer,
    which looks bot-shaped. Clicking from a Google results page sets
    `Referer: https://www.google.com/...` authentically (Chrome does
    this for free on real clicks).

    Flow:
      1. Find / create the persistent Google tab in the live Chrome
         (reuses any captcha-cleared session cookie).
      2. Type the target URL into Google's search box (looks like a
         human pasting a link, gets the URL into Google's index lookup).
      3. Find an `<a href*="<url>">` on the results page and click it.
      4. Wait for the destination page to load + a randomised
         `settle_seconds` window so the tab dwells like a human read.
      5. Capture HTML, then close the destination tab. Don't close
         the Google tab — keep it warm for subsequent fetches.

    If the click step fails (link not visible / Google didn't surface
    the URL), fall back to a direct `page.goto(url)` with a synthetic
    `Referer: https://www.google.com/` header.
    """
    # Local import to avoid circular: search → cdp → search.
    from listo.property_history.search import _find_google_tab, _human_search

    t0 = time.time()
    with cdp_session() as (_browser, ctx):
        google_tab = _find_google_tab(ctx)
        if google_tab is None:
            google_tab = ctx.new_page()
            google_tab.goto(
                "https://www.google.com/",
                wait_until="domcontentloaded",
                timeout=timeout_ms,
            )

        _human_search(google_tab, url)

        target_page = None
        http_status = 0
        # Strategy 1: click an anchor whose href contains the target.
        try:
            google_tab.wait_for_selector(f'a[href*="{url}"]', timeout=5_000)
            link = google_tab.locator(f'a[href*="{url}"]').first
            with ctx.expect_page(timeout=timeout_ms) as new_page_info:
                modifier = "Meta" if os.uname().sysname == "Darwin" else "Control"
                link.click(modifiers=[modifier])
            target_page = new_page_info.value
            target_page.wait_for_load_state(wait_until, timeout=timeout_ms)
            # Click-through doesn't surface a response object; leave
            # http_status at 0 (callers infer from html shape).
        except Exception as exc:  # noqa: BLE001
            logger.info(
                "google click-through failed for %s (%s) — falling back "
                "to direct goto with synthetic Referer",
                url, exc,
            )
            target_page = ctx.new_page()
            try:
                target_page.set_extra_http_headers(
                    {"Referer": "https://www.google.com/"}
                )
            except Exception:  # noqa: BLE001
                pass
            resp = target_page.goto(
                url, wait_until=wait_until, timeout=timeout_ms,
            )
            http_status = resp.status if resp else 0

        try:
            if wait_for_function:
                try:
                    target_page.wait_for_function(
                        wait_for_function, timeout=10_000,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "wait_for_function timed out for %s: %s", url, exc,
                    )
            dwell = random.uniform(settle_seconds * 0.7, settle_seconds * 1.3)
            time.sleep(dwell)
            html = target_page.content()
            final_url = target_page.url
            return CdpFetchResult(
                url=url,
                final_url=final_url,
                http_status=http_status,
                html=html,
                elapsed_seconds=time.time() - t0,
            )
        finally:
            try:
                target_page.close()
            except Exception:  # noqa: BLE001
                pass
