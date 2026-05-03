"""URL discovery via Google search, driven through the user's running Chrome.

Why Google through CDP? Domain's Next.js redirect pages and REA's
Argonaut PDPs aren't always linked from each property's own profile —
*especially* the post-redev unit children (`1/124 Sunshine Parade` only
appears in Google's index, not in the parent's PDP). Google search
also indexes individual sold-listing pages on both sites, which gives us
URL discovery for free.

We use plain Google + `site:` filter rather than DuckDuckGo or Brave —
DDG silently empty-pages bot-shaped requests; Google through a real
authenticated Chrome works fine because we look like the user.

Throttling: keep `MIN_GAP_SECONDS` between consecutive searches.
Caching: every URL we see goes into `discovered_urls` so re-runs of the
same property are zero-cost.
"""
from __future__ import annotations

import hashlib
import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from listo.db import session_scope
from listo.models import DiscoveredUrl
from listo.property_history.cdp import cdp_session


logger = logging.getLogger(__name__)


GOOGLE_BASE = "https://www.google.com/search?q="
MIN_GAP_SECONDS = 8.0  # be polite — the user's IP is at stake


# ---------- URL classification ----------


_REA_PDP_RE = re.compile(r"^https?://www\.realestate\.com\.au/property/[a-z0-9-]+/?$")
_REA_SOLD_RE = re.compile(r"^https?://www\.realestate\.com\.au/sold/[^?#]+?-(\d{6,})\b")
_REA_BUY_RE = re.compile(r"^https?://www\.realestate\.com\.au/(?:property-|buy/)[^?#]+?-(\d{6,})\b")
_DOMAIN_PDP_RE = re.compile(r"^https?://www\.domain\.com\.au/property-profile/[a-z0-9-]+/?$")
_DOMAIN_LISTING_RE = re.compile(r"^https?://www\.domain\.com\.au/(?!property-profile/)[a-z0-9-]+-(\d{6,})\b")


def classify_url(url: str) -> str | None:
    """Return one of: rea_pdp / rea_sold / domain_pdp / domain_listing / None."""
    if _REA_PDP_RE.match(url):
        return "rea_pdp"
    if _REA_SOLD_RE.match(url):
        return "rea_sold"
    if _REA_BUY_RE.match(url):
        return "rea_buy"
    if _DOMAIN_PDP_RE.match(url):
        return "domain_pdp"
    if _DOMAIN_LISTING_RE.match(url):
        return "domain_listing"
    return None


# ---------- URL extraction from a Google results page ----------


# Google sometimes embeds URLs with `&`-style escapes inside JS. We grab
# anything that looks like a real-estate URL and clean trailing junk.
_REA_URL_RE = re.compile(
    r'https?://www\.realestate\.com\.au/(?:property|sold|buy|property-)[A-Za-z0-9._/+%~:@?=&#-]+'
)
_DOMAIN_URL_RE = re.compile(
    r'https?://www\.domain\.com\.au/[A-Za-z0-9._/+%~:@?=&#-]+'
)


def _clean_url(url: str) -> str:
    """Normalise a URL surfaced by Google: strip text-fragments, HTML entities,
    JS string-boundary backslashes, query strings, and trailing punctuation.

    Google embeds text-fragment hints (`#:~:text=...`), HTML-encodes some
    URLs (`&amp;`), and includes percent-encoded variants alongside the
    plain form. All of these resolve to the same canonical URL, so we
    canonicalise aggressively before classification.
    """
    # 1. JS string-boundary: drop everything from the first backslash.
    url = url.split("\\")[0]
    # 2. HTML entity: `&amp;` → `&` (then we strip query so it doesn't matter).
    url = url.replace("&amp;", "&")
    # 3. Drop fragment (text-fragment hints, anchors).
    url = url.split("#")[0]
    # 4. Drop query string.
    url = url.split("?")[0]
    # 5. Drop trailing punctuation that often gets glued by surrounding text.
    url = url.rstrip("/.,;)>'\"")
    # 6. Re-add a trailing slash for REA PDP URLs (their canonical form has it).
    if re.match(r"^https?://www\.realestate\.com\.au/property/[a-z0-9-]+$", url):
        url = url + "/"
    return url


def _extract_urls(html: str) -> set[str]:
    found: set[str] = set()
    seen_listing_ids: dict[str, set[str]] = {"rea": set(), "domain": set()}
    for pat in (_REA_URL_RE, _DOMAIN_URL_RE):
        for m in pat.findall(html):
            url = _clean_url(m)
            cls = classify_url(url)
            if cls is None:
                continue
            # Dedup by listing ID — Google often returns multiple URL
            # variants pointing to the same listing (with/without
            # text-fragments, %-encoded vs raw, etc.). Keep the first
            # canonical form we see per listing id.
            site = "rea" if "realestate.com.au" in url else "domain"
            id_match = (
                _REA_SOLD_RE.match(url) or _REA_BUY_RE.match(url)
                or _DOMAIN_LISTING_RE.match(url)
            )
            if id_match:
                lid = id_match.group(1)
                if lid in seen_listing_ids[site]:
                    continue
                seen_listing_ids[site].add(lid)
            found.add(url)
    return found


# Catch-all URL pattern for unscoped Google searches. We keep this lax
# (any https://) and filter Google/CDN noise in `_extract_general_urls`.
_ANY_URL_RE = re.compile(r'https?://[^\s"\'<>()]+')

# Google-internal + boilerplate hosts to drop from general extraction.
# These appear in every results page (toolbar links, CDN assets, support
# pages, etc.) and aren't real third-party hits.
_GENERAL_URL_HOST_DENYLIST = {
    "google.com", "www.google.com", "policies.google.com", "support.google.com",
    "maps.google.com", "translate.google.com", "accounts.google.com",
    "schema.org", "www.w3.org", "gstatic.com", "www.gstatic.com",
    "googleusercontent.com", "googleapis.com",
    # Realestate + Domain are already covered by the scoped queries.
    "www.realestate.com.au", "realestate.com.au",
    "www.domain.com.au", "domain.com.au",
}


def _extract_general_urls(html: str) -> set[str]:
    """Pull every plausible third-party URL out of a Google results page.

    Used for unscoped searches (no `site:` filter) where any non-Google
    domain that mentions the address is potential evidence — e.g. agency
    sites like rwbgrentals.com that index 'leased' listings outside the
    REA / Domain duopoly.
    """
    found: set[str] = set()
    for m in _ANY_URL_RE.findall(html):
        url = _clean_url(m)
        # Cheap host extraction — _clean_url already drops query/fragment.
        host_match = re.match(r"^https?://([^/]+)", url)
        if not host_match:
            continue
        host = host_match.group(1).lower()
        if host in _GENERAL_URL_HOST_DENYLIST:
            continue
        # Drop google subdomains catch-all (e.g. webcache.googleusercontent.com).
        if host.endswith(".google.com") or host.endswith(".googleusercontent.com"):
            continue
        found.add(url)
    return found


# ---------- Throttle ----------


_LAST_SEARCH_AT: float = 0.0


def _throttle() -> None:
    global _LAST_SEARCH_AT
    now = time.time()
    wait = MIN_GAP_SECONDS - (now - _LAST_SEARCH_AT)
    if wait > 0:
        logger.info("throttle: sleeping %.1fs before next Google search", wait)
        time.sleep(wait)
    _LAST_SEARCH_AT = time.time()


# ---------- One Google search ----------


@dataclass
class SearchResult:
    query: str
    urls: list[str]
    page_html_size: int


def _is_google_captcha(html: str) -> bool:
    """Detect Google's 'unusual traffic' CAPTCHA / sorry page. Multiple
    independent markers — fire on any one to be robust against minor
    template changes."""
    if not html:
        return False
    for marker in (
        "Our systems have detected unusual traffic",
        'id="captcha-form"',
        'class="g-recaptcha"',
        "recaptcha/enterprise.js",
    ):
        if marker in html:
            return True
    return False


def _wait_for_captcha_solve(query: str) -> None:
    """Block on STDIN until the user solves the CAPTCHA in the live
    Chrome on :9222. They visit `https://www.google.com/search?q=...`
    in any tab, click through the reCAPTCHA, and press Enter here.

    Falls back to a sleep when STDIN isn't a TTY (e.g. piped runs);
    the next search after the wait will retry."""
    import sys
    msg = (
        "\n"
        "================================================================\n"
        "  GOOGLE CAPTCHA detected for query:\n"
        f"    {query!r}\n"
        "  Open the listo Chrome (the one on :9222), solve the captcha,\n"
        "  then press Enter here to continue.\n"
        "================================================================\n"
    )
    logger.warning("google captcha hit; waiting for user to solve")
    print(msg, flush=True)
    if sys.stdin.isatty():
        try:
            input("press Enter after solving captcha ... ")
        except (EOFError, KeyboardInterrupt):
            raise
    else:
        # Headless run: sleep a long-enough beat so the rate-limit
        # token bucket can refill. Caller will still retry.
        logger.warning("STDIN not a TTY — sleeping 120s instead of waiting for user")
        time.sleep(120)


_CAPTCHA_RETRY_LIMIT = 3


_GOOGLE_HOME = "https://www.google.com/"
_GOOGLE_SEARCH_INPUT = 'textarea[name="q"], input[name="q"]'


def _find_google_tab(ctx):
    """Return an existing Google tab from the context, if any.
    Prefers a tab already on a results page (so a captcha solve there
    is reused). Returns None if no Google tab is open."""
    candidates = []
    for p in ctx.pages:
        try:
            u = p.url or ""
        except Exception:  # noqa: BLE001
            continue
        if "google.com" in u:
            candidates.append((u, p))
    if not candidates:
        return None
    # Prefer search-results tabs over plain google.com home.
    for u, p in candidates:
        if "/search" in u:
            return p
    return candidates[0][1]


def _human_search(page, query: str) -> None:
    """Type `query` into Google's search input with human-like jitter,
    then press Enter. Reuses whatever Google tab `page` already is.

    Why type instead of navigating to `?q=...`? Google's bot-detector
    weights URL navigations harder than in-page typed searches; typing
    into the existing tab uses up the same captcha-cleared session
    cookie and reads as a continuing user action."""
    # If we somehow ended up on a non-Google URL, go home first.
    try:
        cur = page.url or ""
    except Exception:  # noqa: BLE001
        cur = ""
    if "google.com" not in cur:
        page.goto(_GOOGLE_HOME, wait_until="domcontentloaded", timeout=20_000)

    try:
        page.wait_for_selector(_GOOGLE_SEARCH_INPUT, timeout=5_000)
    except Exception:  # noqa: BLE001
        # Search box not found — bounce through home and retry once.
        page.goto(_GOOGLE_HOME, wait_until="domcontentloaded", timeout=20_000)
        page.wait_for_selector(_GOOGLE_SEARCH_INPUT, timeout=10_000)

    page.click(_GOOGLE_SEARCH_INPUT)
    # Clear whatever's in the box.
    page.keyboard.press("ControlOrMeta+a")
    page.keyboard.press("Backspace")

    # Variable per-character delay so the pacing looks human.
    for ch in query:
        page.keyboard.insert_text(ch)
        time.sleep(random.uniform(0.04, 0.16))
        # Occasional longer thinking-pause.
        if random.random() < 0.06:
            time.sleep(random.uniform(0.20, 0.55))

    # Tiny settle before submitting.
    time.sleep(random.uniform(0.30, 0.80))
    page.keyboard.press("Enter")
    page.wait_for_load_state("domcontentloaded", timeout=20_000)
    # Allow deferred result blocks to render.
    time.sleep(random.uniform(0.50, 1.20))


def _do_google_search(
    query: str,
    *,
    extractor,
    log_label: str,
) -> SearchResult:
    """Shared CDP-search body. REUSES an existing Google tab whenever
    one is open in the live Chrome — so a captcha-cleared session
    cookie persists across many queries. Types the query into the
    search box with human-like jitter rather than navigating to
    `?q=...`."""
    _throttle()
    logger.info("%s: %s", log_label, query)
    with cdp_session() as (_browser, ctx):
        page = _find_google_tab(ctx)
        opened_new_tab = False
        if page is None:
            page = ctx.new_page()
            page.goto(_GOOGLE_HOME, wait_until="domcontentloaded", timeout=20_000)
            opened_new_tab = True
        try:
            _human_search(page, query)
            html = page.content()
            for attempt in range(1, _CAPTCHA_RETRY_LIMIT + 1):
                if not _is_google_captcha(html):
                    break
                logger.warning(
                    "captcha detected (html %d bytes, attempt %d/%d)",
                    len(html), attempt, _CAPTCHA_RETRY_LIMIT,
                )
                try:
                    page.bring_to_front()
                except Exception:  # noqa: BLE001
                    pass
                if attempt >= _CAPTCHA_RETRY_LIMIT:
                    raise GoogleCaptchaError(
                        f"google captcha persists after {_CAPTCHA_RETRY_LIMIT} retries"
                    )
                _wait_for_captcha_solve(query)
                # User just solved a captcha on this tab — Google's
                # cookie now lets us search again. Re-type rather
                # than reload (we may have lost the query state).
                _human_search(page, query)
                html = page.content()
        finally:
            # Keep the Google tab open across searches so future
            # searches reuse the same captcha-cleared session. We only
            # close if we opened it AND it landed in an obviously bad
            # state (e.g. blank page).
            if opened_new_tab and not html:
                try:
                    page.close()
                except Exception:  # noqa: BLE001
                    pass
    urls = sorted(extractor(html))
    return SearchResult(query=query, urls=urls, page_html_size=len(html))


class GoogleCaptchaError(RuntimeError):
    """Raised when Google's CAPTCHA persists past the retry budget."""


def google_search(query: str) -> SearchResult:
    """Drive Chrome to Google, return cleaned URLs found in the results.

    Uses the existing CDP session (the one the user already warmed for
    Kasada). Google sees a real signed-in browser, so results are
    high-quality.
    """
    return _do_google_search(query, extractor=_extract_urls, log_label="google search")


def google_search_general(query: str) -> SearchResult:
    """Like `google_search` but extracts URLs from any third-party domain
    (REA / Domain / agency sites / etc.). Used for unscoped queries
    where the goal is 'does anyone on the open web mention this exact
    address phrase?' — evidence the unit was built and listed at some
    point, even outside REA/Domain.
    """
    return _do_google_search(
        query, extractor=_extract_general_urls, log_label="google search (general)",
    )


def _quote(s: str) -> str:
    """URL-encode without pulling urllib — preserves expected operators."""
    out = []
    for ch in s:
        if ch.isalnum() or ch in "._-~":
            out.append(ch)
        elif ch == " ":
            out.append("+")
        else:
            out.append(f"%{ord(ch):02X}")
    return "".join(out)


# ---------- Persist into discovered_urls ----------


def _sha256(s: str) -> bytes:
    return hashlib.sha256(s.encode("utf-8")).digest()


def cache_urls(*, search_address: str, query: str, urls: Iterable[str]) -> int:
    """Upsert each URL into discovered_urls. Returns the count of new rows.

    Existing rows (matched by url_hash) are left alone; we only add new
    URLs. `fetched_at` is left NULL — set it when the fetcher actually
    pulls the URL.
    """
    now = datetime.utcnow()
    inserted = 0
    with session_scope() as s:
        for url in urls:
            kind = classify_url(url)
            if kind is None:
                continue
            stmt = mysql_insert(DiscoveredUrl).values(
                search_address=search_address[:255],
                search_query=query[:255],
                url=url[:1024],
                url_hash=_sha256(url),
                url_kind=kind,
                search_engine="google",
                discovered_at=now,
                fetched_at=None,
            )
            # ON DUPLICATE KEY UPDATE: keep existing fetched_at, refresh
            # search_address (we've now seen this URL surface for this address).
            stmt = stmt.on_duplicate_key_update(
                search_address=stmt.inserted.search_address,
                search_query=stmt.inserted.search_query,
            )
            res = s.execute(stmt)
            if res.rowcount == 1:  # rowcount is 1 for INSERT, 2 for UPDATE in MySQL
                inserted += 1
    return inserted


def cache_urls_general(*, search_address: str, query: str, urls: Iterable[str]) -> int:
    """Persist results from an unscoped Google search. URL kind is fixed
    to 'unit_general' — we don't try to tell rentals from sales from
    body-corp pages here, only that the phrase surfaced *somewhere*.
    """
    now = datetime.utcnow()
    inserted = 0
    with session_scope() as s:
        for url in urls:
            stmt = mysql_insert(DiscoveredUrl).values(
                search_address=search_address[:255],
                search_query=query[:255],
                url=url[:1024],
                url_hash=_sha256(url),
                url_kind="unit_general",
                search_engine="google",
                discovered_at=now,
                fetched_at=None,
            )
            stmt = stmt.on_duplicate_key_update(
                search_address=stmt.inserted.search_address,
                search_query=stmt.inserted.search_query,
            )
            res = s.execute(stmt)
            if res.rowcount == 1:
                inserted += 1
    return inserted


# ---------- High-level: discover everything for an address ----------


@dataclass
class DiscoveryResult:
    search_address: str
    queries: list[str]
    rea_pdp_urls: list[str]
    rea_sold_urls: list[str]
    domain_pdp_urls: list[str]
    domain_listing_urls: list[str]


def discover_for_address(
    search_address: str,
    *,
    suburb_hint: str | None = None,
) -> DiscoveryResult:
    """Run Google searches and return all classified URLs for an address.

    Sample call:
        discover_for_address('124 Sunshine Parade Miami')

    Issues two queries (REA + Domain) — Google's `site:` filter is more
    accurate when scoped, and the dedup happens in `discovered_urls`.
    """
    # The quoted phrase is the strict match. Suburb hint is added unquoted
    # only if it's not already present in `search_address` — otherwise
    # Google sees the suburb twice and quality drops.
    suffix = f" {suburb_hint}" if suburb_hint and suburb_hint.lower() not in search_address.lower() else ""
    queries = [
        f'site:realestate.com.au "{search_address}"{suffix}',
        f'site:domain.com.au "{search_address}"{suffix}',
    ]

    rea_pdp: set[str] = set()
    rea_sold: set[str] = set()
    domain_pdp: set[str] = set()
    domain_listing: set[str] = set()

    for q in queries:
        res = google_search(q)
        cache_urls(search_address=search_address, query=q, urls=res.urls)
        for url in res.urls:
            kind = classify_url(url)
            if kind == "rea_pdp": rea_pdp.add(url)
            elif kind == "rea_sold": rea_sold.add(url)
            elif kind == "domain_pdp": domain_pdp.add(url)
            elif kind == "domain_listing": domain_listing.add(url)

    return DiscoveryResult(
        search_address=search_address,
        queries=queries,
        rea_pdp_urls=sorted(rea_pdp),
        rea_sold_urls=sorted(rea_sold),
        domain_pdp_urls=sorted(domain_pdp),
        domain_listing_urls=sorted(domain_listing),
    )


def mark_fetched(url: str) -> None:
    """Stamp `fetched_at` on the discovered_urls row for this URL."""
    with session_scope() as s:
        row = s.execute(
            select(DiscoveredUrl).where(DiscoveredUrl.url_hash == _sha256(url))
        ).scalar_one_or_none()
        if row:
            row.fetched_at = datetime.utcnow()
