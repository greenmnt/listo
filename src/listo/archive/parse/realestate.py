from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from selectolax.parser import HTMLParser

from listo.parse.json_unescape import (
    extract_argonaut_exchange,
    parse_stringified_json,
    recursively_parse_json,
)


REALESTATE_LISTING_TYPES = {
    "BuyResidentialListing",
    "SoldResidentialListing",
    "RentResidentialListing",
}

_LISTING_KIND = {
    "BuyResidentialListing": "buy",
    "SoldResidentialListing": "sold",
    "RentResidentialListing": "rent",
}


@dataclass
class RawListing:
    source_listing_id: str
    listing_kind: str
    status: str
    full_address: str
    suburb: str
    postcode: str
    state: str
    price_text: str | None
    price_min: int | None
    price_max: int | None
    beds: int | None
    baths: int | None
    parking: int | None
    property_type: str | None
    land_size_m2: int | None
    agent_name: str | None
    agency_name: str | None
    url: str
    sold_date: date | None = None
    sold_price: int | None = None
    sale_method: str | None = None


@dataclass
class ParsedPage:
    listings: list[RawListing] = field(default_factory=list)
    current_page: int | None = None
    total_pages: int | None = None
    total_results: int | None = None


_PRICE_RE = re.compile(r"\$([\d,]+(?:\.\d+)?)\s*([kKmM]?)")
_RENT_INDICATORS = re.compile(
    r"per\s+(?:week|month)"
    r"|/\s*w(?:k|eek)?\b"
    r"|\bp\.?\s*/?\s*w\.?\b"
    r"|\bpcm\b"
    r"|\bweekly\b",
    re.I,
)
_DATE_RE = re.compile(r"^(\d{1,2})\s+(\w{3,9})\s+(\d{4})$")
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


# Cap prices at $200M — anything higher is almost certainly a parse artefact
# (e.g. domain text "Offers Above $505,000530,000" matches as one $505b number).
# INT UNSIGNED max is ~4.3B which would otherwise let garbage through.
_PRICE_MAX = 200_000_000


def _parse_price(text: str | None) -> tuple[int | None, int | None]:
    """Pull min/max integer prices from a freeform price string. Returns (min,max)."""
    if not text:
        return None, None
    matches = _PRICE_RE.findall(text)
    if not matches:
        return None, None
    values: list[int] = []
    for amount, suffix in matches:
        try:
            n = float(amount.replace(",", ""))
        except ValueError:
            continue
        if suffix.lower() == "k":
            n *= 1_000
        elif suffix.lower() == "m":
            n *= 1_000_000
        v = int(n)
        if v <= _PRICE_MAX:
            values.append(v)
    if not values:
        return None, None
    return min(values), max(values)


def _parse_date_display(text: str | None) -> date | None:
    if not text:
        return None
    m = _DATE_RE.match(text.strip())
    if not m:
        return None
    day_s, mon_s, year_s = m.groups()
    mon = _MONTHS.get(mon_s.lower())
    if not mon:
        return None
    try:
        return date(int(year_s), mon, int(day_s))
    except ValueError:
        return None


def _intval(node: Any) -> int | None:
    if isinstance(node, dict):
        v = node.get("value")
        if isinstance(v, (int, float)):
            iv = int(v)
            # beds/baths/parking are tinyint unsigned (max 255). Anything
            # over 99 is source-data corruption (real listing seen in the
            # wild: parking=256). Drop it rather than overflow the column.
            return iv if 0 <= iv <= 99 else None
    return None


def _land_size_m2(property_sizes: dict | None) -> int | None:
    if not isinstance(property_sizes, dict):
        return None
    land = property_sizes.get("land")
    if not isinstance(land, dict):
        return None
    val = land.get("displayValue")
    unit = (land.get("sizeUnit") or {}).get("displayValue", "")
    try:
        n = float(val)
    except (TypeError, ValueError):
        return None
    if "m" in unit.lower():
        return int(n)
    if "ha" in unit.lower():
        return int(n * 10_000)
    return int(n)


def extract_argonaut_root(html: str) -> dict | None:
    """Locate the ArgonautExchange JSON in the HTML and return the parsed dict."""
    doc = HTMLParser(html)
    for script in doc.css("script"):
        text = script.text() or ""
        if "window.ArgonautExchange" not in text:
            continue
        json_text = extract_argonaut_exchange(text)
        if not json_text:
            continue
        import json as _json

        try:
            return _json.loads(json_text)
        except _json.JSONDecodeError:
            continue
    return None


def extract_urql_cache(html: str) -> dict | None:
    """Return the urqlClientCache as a fully-resolved dict, or None."""
    root = extract_argonaut_root(html)
    if not root:
        return None
    cache_str = (
        root.get("resi-property_listing-experience-web", {}).get("urqlClientCache")
    )
    if not isinstance(cache_str, str):
        return None
    parsed = parse_stringified_json(cache_str)
    return recursively_parse_json(parsed)


def _walk_for_listings(node: Any) -> list[dict]:
    """Collect listings from search-results subtrees only.

    Skips `exclusiveShowcase` (promotional placements that don't match the
    search query and are missing postcode/sale data).
    """
    found: list[dict] = []

    def visit_search_results(n: Any) -> None:
        if isinstance(n, dict):
            tn = n.get("__typename")
            if tn in REALESTATE_LISTING_TYPES:
                found.append(n)
                return
            for v in n.values():
                visit_search_results(v)
        elif isinstance(n, list):
            for v in n:
                visit_search_results(v)

    def visit(n: Any) -> None:
        if isinstance(n, dict):
            for k, v in n.items():
                if k == "exclusiveShowcase":
                    continue  # off-topic promotional content
                if k in ("exact", "surrounding"):
                    visit_search_results(v)
                    continue
                visit(v)
        elif isinstance(n, list):
            for v in n:
                visit(v)

    visit(node)
    return found


def _find_pagination(node: Any) -> tuple[int | None, int | None, int | None]:
    """Return (current_page, max_page, total_results) — first match wins."""

    def visit(n: Any) -> tuple[int | None, int | None, int | None]:
        if isinstance(n, dict):
            tn = n.get("__typename")
            if tn == "Pagination":
                return (n.get("page"), n.get("maxPageNumberAvailable"), None)
            for v in n.values():
                got = visit(v)
                if got != (None, None, None):
                    return got
        elif isinstance(n, list):
            for v in n:
                got = visit(v)
                if got != (None, None, None):
                    return got
        return (None, None, None)

    cur, mx, _ = visit(node)
    total = _find_total(node)
    return cur, mx, total


def _find_total(node: Any) -> int | None:
    if isinstance(node, dict):
        if "totalResultsCount" in node and isinstance(node["totalResultsCount"], int):
            return node["totalResultsCount"]
        for v in node.values():
            got = _find_total(v)
            if got is not None:
                return got
    elif isinstance(node, list):
        for v in node:
            got = _find_total(v)
            if got is not None:
                return got
    return None


def _to_raw_listing(listing: dict) -> RawListing | None:
    tn = listing.get("__typename")
    kind = _LISTING_KIND.get(tn)
    if kind is None:
        return None

    addr = listing.get("address") or {}
    display = addr.get("display") or {}
    full_address = display.get("fullAddress") or display.get("shortAddress") or ""
    suburb = addr.get("suburb") or ""
    postcode = addr.get("postcode") or ""
    state = addr.get("state") or ""

    price = listing.get("price") or {}
    price_text = price.get("display")
    if kind in ("buy", "sold") and price_text and _RENT_INDICATORS.search(price_text):
        return None  # rental that leaked into a buy/sold search — skip
    price_min, price_max = _parse_price(price_text)

    gf = listing.get("generalFeatures") or {}
    beds = _intval(gf.get("bedrooms"))
    baths = _intval(gf.get("bathrooms"))
    parking = _intval(gf.get("parkingSpaces"))

    pt = listing.get("propertyType") or {}
    property_type = pt.get("display")

    land_m2 = _land_size_m2(listing.get("propertySizes"))

    listers = listing.get("listers") or []
    agent_name = listers[0].get("name") if listers and isinstance(listers[0], dict) else None
    company = listing.get("listingCompany") or {}
    agency_name = company.get("name")

    links = listing.get("_links") or {}
    canonical = links.get("canonical") or links.get("trackedCanonical") or {}
    url = canonical.get("href") or ""
    # trackedCanonical hrefs include `?sourcePage={sourcePage}&sourceElement={sourceElement}`
    # template placeholders — strip them.
    if url and "{sourcePage}" in url:
        url = url.split("?", 1)[0]

    sold_date = None
    sold_price_val = None
    sale_method = None
    status = "unknown"
    if kind == "sold":
        ds = listing.get("dateSold") or {}
        sold_date = _parse_date_display(ds.get("display"))
        sold_price_val = price_max  # SoldPrice display sometimes includes a value
        status = "sold"
    elif kind == "buy":
        status = "active"
    elif kind == "rent":
        status = "active"

    short = display.get("shortAddress") or ""
    # Use the explicit short address (which preserves "1003/472" form) if present;
    # otherwise fall back to the suburb/state-stripped version of full address.
    raw_street = short or full_address

    return RawListing(
        source_listing_id=str(listing.get("id") or ""),
        listing_kind=kind,
        status=status,
        full_address=raw_street,
        suburb=suburb,
        postcode=postcode,
        state=state,
        price_text=price_text,
        price_min=price_min,
        price_max=price_max,
        beds=beds,
        baths=baths,
        parking=parking,
        property_type=property_type,
        land_size_m2=land_m2,
        agent_name=agent_name,
        agency_name=agency_name,
        url=url,
        sold_date=sold_date,
        sold_price=sold_price_val if kind == "sold" else None,
        sale_method=sale_method,
    )


def parse(html: str) -> ParsedPage:
    """Parse a realestate.com.au search-results HTML page."""
    cache = extract_urql_cache(html)
    if not cache:
        return ParsedPage()
    raw_listings = _walk_for_listings(cache)
    listings: list[RawListing] = []
    for r in raw_listings:
        rl = _to_raw_listing(r)
        if rl and rl.source_listing_id:
            listings.append(rl)
    cur, mx, total = _find_pagination(cache)
    return ParsedPage(
        listings=listings,
        current_page=cur,
        total_pages=mx,
        total_results=total,
    )


def peek_pagination(html: str) -> tuple[int | None, int | None]:
    """Cheap pagination probe: returns (current_page, total_pages) without full parse."""
    cache = extract_urql_cache(html)
    if not cache:
        return (None, None)
    cur, mx, _ = _find_pagination(cache)
    return cur, mx
