from __future__ import annotations

import logging
import random
import re
import time
from pathlib import Path

import browser_cookie3

logger = logging.getLogger(__name__)

# Candidate browser profile locations, in priority order. Chrome first because
# it's typically the user's daily-driver; Chromium is a fallback. Each entry
# is a base directory; we look for `Default/Cookies` (or .../Network/Cookies)
# beneath it.
_PROFILE_CANDIDATES = (
    Path.home() / ".config" / "google-chrome",
    Path.home() / "snap" / "google-chrome" / "common" / ".config" / "google-chrome",
    Path.home() / ".config" / "BraveSoftware" / "Brave-Browser",
    Path.home() / "snap" / "chromium" / "common" / "chromium",
    Path.home() / ".config" / "chromium",
)


def _detect_chromium_profile() -> Path | None:
    """Locate an active chromium-family browser profile directory.

    Picks the freshest one (latest mtime on the Cookies DB) so if the user has
    multiple chromium-based browsers installed but only uses one daily, we
    pull from the active one instead of a stale install.
    """
    found: list[tuple[float, Path]] = []
    for base in _PROFILE_CANDIDATES:
        for sub in ("Default/Cookies", "Default/Network/Cookies"):
            cookies = base / sub
            if cookies.exists():
                try:
                    mtime = cookies.stat().st_mtime
                except OSError:
                    mtime = 0.0
                found.append((mtime, base))
                break
    if not found:
        return None
    found.sort(reverse=True)
    return found[0][1]


# Cookie names that suggest user-identifying data — login state, account
# tokens, saved-search/wishlist/profile preferences. We strip these before
# injection so the scraper can't accidentally act on the user's account or
# leak their identity through tracking pipelines that attach cookies to
# behavioural fingerprints.
_PERSONAL_COOKIE_PATTERNS = re.compile(
    r"login|auth|account|user(?!agent)|profile|email|signin|sign_in|"
    r"customer|member|me\b|mybox|wishlist|favourite|favorite|saved|"
    r"jwt|access_token|refresh_token|id_token|sso|logged|"
    r"identity|session_user|whoami",
    re.IGNORECASE,
)


def _is_personal_cookie(name: str) -> bool:
    return bool(_PERSONAL_COOKIE_PATTERNS.search(name))


def _decoy_third_party_cookies() -> list[dict]:
    """Generate plausible-looking _ga / _gid cookies for popular sites.

    These don't get sent to realestate.com.au directly (cookies are domain-
    scoped) — but the realestate page embeds third-party trackers (Google
    Analytics, Facebook Pixel, etc.). When chromium fires those embedded
    requests, it attaches the matching domain's cookies. Without these
    decoys, the chromium has zero history with any major property; with
    them, it looks like a normal browser that's been around the web.

    Format mimics GA's own client-id format: `GA1.2.<rand10>.<unix-ts>`.
    Timestamps are randomised within the last year so the cookie ages
    look organic rather than all stamped at the same moment.
    """
    now = int(time.time())
    far_future = now + 60 * 60 * 24 * 365 * 2  # 2 years

    decoy_domains = (
        # Google properties — realestate.com.au's GA + Ads tags hit these
        ".google.com",
        ".google.com.au",
        ".youtube.com",
        ".doubleclick.net",
        # Meta — most AU sites embed FB Pixel
        ".facebook.com",
        ".instagram.com",
        # Other AU/global trackers a real user would have hit
        ".linkedin.com",
        ".twitter.com",
        ".x.com",
        ".reddit.com",
        ".amazon.com.au",
        ".news.com.au",
        ".smh.com.au",
        ".abc.net.au",
        ".bing.com",
    )

    out: list[dict] = []
    for domain in decoy_domains:
        client_id = random.randint(1_000_000_000, 9_999_999_999)
        # Initial visit between 30 days and 1 year ago — old enough to look
        # established, recent enough to still be a current cookie.
        first_seen = now - random.randint(30 * 86400, 365 * 86400)
        out.append({
            "name": "_ga",
            "value": f"GA1.2.{client_id}.{first_seen}",
            "domain": domain,
            "path": "/",
            "secure": True,
            "httpOnly": False,
            "sameSite": "Lax",
            "expires": far_future,
        })
        # _gid is a 24h tracker GA also sets — having both is the realistic state
        gid_id = random.randint(1_000_000_000, 9_999_999_999)
        out.append({
            "name": "_gid",
            "value": f"GA1.2.{gid_id}.{now - random.randint(3600, 86400)}",
            "domain": domain,
            "path": "/",
            "secure": True,
            "httpOnly": False,
            "sameSite": "Lax",
            "expires": now + 86400,
        })
    return out


def load_cookies_for(domain: str) -> dict[str, str]:
    """Load cookies for a given domain from the user's chromium-family browser.

    Returns a {name: value} dict suitable for attaching to an HTTP request.
    Raises RuntimeError if no profile is found or the cookies DB is missing.
    """
    profile = _detect_chromium_profile()
    if profile is None:
        raise RuntimeError(
            "No chromium-family browser profile found. Looked in: "
            + ", ".join(str(p) for p in _PROFILE_CANDIDATES)
        )

    cookie_file = profile / "Default" / "Cookies"
    if not cookie_file.exists():
        cookie_file = profile / "Default" / "Network" / "Cookies"
    if not cookie_file.exists():
        raise RuntimeError(f"Cookies DB not found under {profile}")

    try:
        jar = browser_cookie3.chromium(cookie_file=str(cookie_file), domain_name=domain)
    except browser_cookie3.BrowserCookieError as e:
        raise RuntimeError(f"Failed to read browser cookies: {e}") from e

    cookies: dict[str, str] = {}
    for c in jar:
        cookies[c.name] = c.value
    return cookies


def have_kasada_token(cookies: dict[str, str]) -> bool:
    """Return True if the cookie set looks like Kasada has issued a token."""
    return any(name.startswith("KP_UIDz") for name in cookies)


def load_playwright_cookies_for(
    domain: str,
    *,
    strip_personal: bool = True,
    include_decoys: bool = True,
) -> list[dict]:
    """Same source as load_cookies_for, but returns playwright-shaped dicts
    ready for `context.add_cookies(...)`.

    Carries domain/path/expiry/secure/httpOnly/sameSite from the user's real
    browser jar so the injected cookies behave identically to the ones the
    user's browser sends.

    `strip_personal` (default True): drop cookies whose names look like
    login / account / profile data, so we don't carry the user's identity
    or session into the scraper context. The Kasada token (KP_UIDz) and
    analytics cookies are kept since those are what we actually need.

    `include_decoys` (default True): also append plausible _ga/_gid cookies
    for popular third-party domains, so embedded trackers see a "lived-in"
    browser instead of a freshly-spawned context.
    """
    profile = _detect_chromium_profile()
    if profile is None:
        raise RuntimeError(
            "No chromium-family browser profile found. Looked in: "
            + ", ".join(str(p) for p in _PROFILE_CANDIDATES)
        )
    cookie_file = profile / "Default" / "Cookies"
    if not cookie_file.exists():
        cookie_file = profile / "Default" / "Network" / "Cookies"
    if not cookie_file.exists():
        raise RuntimeError(f"Cookies DB not found under {profile}")
    jar = browser_cookie3.chromium(cookie_file=str(cookie_file), domain_name=domain)

    out: list[dict] = []
    stripped: list[str] = []
    for c in jar:
        if strip_personal and _is_personal_cookie(c.name):
            stripped.append(c.name)
            continue
        # Playwright accepts only "Strict" | "Lax" | "None" for sameSite. Chromium's
        # SQLite stores 0=None, 1=Lax, 2=Strict; browser_cookie3 doesn't surface it,
        # so default to "Lax" — the safe default that matches most cookies.
        cookie = {
            "name": c.name,
            "value": c.value,
            "domain": c.domain,
            "path": c.path or "/",
            "secure": bool(c.secure),
            "httpOnly": bool(c.has_nonstandard_attr("HttpOnly")),
            "sameSite": "Lax",
        }
        if c.expires:
            cookie["expires"] = int(c.expires)
        out.append(cookie)
    if stripped:
        logger.info(
            "stripped %d personal cookie(s) from %s: %s",
            len(stripped), domain, ", ".join(stripped[:8]),
        )
    if include_decoys:
        decoys = _decoy_third_party_cookies()
        out.extend(decoys)
        logger.info("appended %d decoy third-party cookies", len(decoys))
    return out
