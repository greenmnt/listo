from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Iterable

import browser_cookie3

logger = logging.getLogger(__name__)

# Snap chromium stores its profile under the snap sandbox, not ~/.config.
_SNAP_CHROMIUM = Path.home() / "snap" / "chromium" / "common" / "chromium"
_DEFAULT_CONFIG_CHROMIUM = Path.home() / ".config" / "chromium"


def _detect_chromium_profile() -> Path | None:
    """Locate the active Chromium 'Default' profile directory on Linux."""
    candidates = [_SNAP_CHROMIUM, _DEFAULT_CONFIG_CHROMIUM]
    for base in candidates:
        cookies = base / "Default" / "Cookies"
        if cookies.exists():
            return base
        cookies_v10 = base / "Default" / "Network" / "Cookies"
        if cookies_v10.exists():
            return base
    return None


def load_cookies_for(domain: str) -> dict[str, str]:
    """Load cookies for a given domain from Chromium's local cookie jar.

    Returns a {name: value} dict suitable for attaching to an HTTP request.
    Raises RuntimeError if Chromium isn't found or has no cookies for the domain.
    """
    profile = _detect_chromium_profile()
    if profile is None:
        raise RuntimeError(
            "Chromium profile not found. Open Chromium and visit the target site, "
            "then try again. Looked in: "
            f"{_SNAP_CHROMIUM}, {_DEFAULT_CONFIG_CHROMIUM}"
        )

    # browser_cookie3.chromium accepts a `cookie_file` path — we point it at the
    # snap-sandboxed location explicitly because it doesn't probe for the snap path.
    cookie_file = profile / "Default" / "Cookies"
    if not cookie_file.exists():
        cookie_file = profile / "Default" / "Network" / "Cookies"
    if not cookie_file.exists():
        raise RuntimeError(f"Cookies DB not found under {profile}")

    try:
        jar = browser_cookie3.chromium(cookie_file=str(cookie_file), domain_name=domain)
    except browser_cookie3.BrowserCookieError as e:
        raise RuntimeError(f"Failed to read Chromium cookies: {e}") from e

    cookies: dict[str, str] = {}
    for c in jar:
        # browser_cookie3 returns http.cookiejar.Cookie objects
        cookies[c.name] = c.value
    return cookies


def have_kasada_token(cookies: dict[str, str]) -> bool:
    """Return True if the cookie set looks like Kasada has issued a token."""
    return any(name.startswith("KP_UIDz") for name in cookies)
