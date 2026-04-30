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


def google_search(query: str) -> SearchResult:
    """Drive Chrome to Google, return cleaned URLs found in the results.

    Uses the existing CDP session (the one the user already warmed for
    Kasada). Google sees a real signed-in browser, so results are
    high-quality.
    """
    _throttle()
    url = GOOGLE_BASE + _quote(query)
    logger.info("google search: %s", query)
    with cdp_session() as (_browser, ctx):
        page = ctx.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=20_000)
            html = page.content()
        finally:
            page.close()
    urls = sorted(_extract_urls(html))
    return SearchResult(query=query, urls=urls, page_html_size=len(html))


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
