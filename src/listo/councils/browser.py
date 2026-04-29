"""Shared playwright session helper for vendor scrapers.

Council portals don't have Kasada-style bot defenses; they're mostly
ASP.NET WebForms (ePathway, eTrack) or SaaS SPAs (T1Cloud). A plain
headless chromium is fine — no Wayland/mutter gymnastics needed. This
helper just gives every vendor scraper a consistent way to launch.
"""
from __future__ import annotations

import time
from contextlib import contextmanager

from patchright.sync_api import Browser, BrowserContext, Page, sync_playwright


@contextmanager
def browser_context(*, headless: bool = True, timezone: str = "Australia/Brisbane", locale: str = "en-AU"):
    """Yield (BrowserContext, Page). Closes on exit."""
    pw = sync_playwright().start()
    try:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir="",
            headless=headless,
            no_viewport=True,
            locale=locale,
            timezone_id=timezone,
        )
        try:
            page = ctx.new_page()
            yield ctx, page
        finally:
            ctx.close()
    finally:
        pw.stop()


def jitter_sleep(min_s: float, max_s: float) -> None:
    import random
    time.sleep(random.uniform(min_s, max_s))
