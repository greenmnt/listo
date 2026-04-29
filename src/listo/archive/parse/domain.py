from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

from selectolax.parser import HTMLParser

from listo.parse.realestate import RawListing, ParsedPage


_PRICE_RE = re.compile(r"\$([\d,]+(?:\.\d+)?)\s*([kKmM]?)")
# Rental indicators in a price_text string. If any match, the listing is a
# rental and should be skipped from buy/sold search results.
_RENT_INDICATORS = re.compile(
    r"per\s+(?:week|month)"          # "per week", "per month"
    r"|/\s*w(?:k|eek)?\b"            # "/wk", "/week", "/ week", " / week"
    r"|\bp\.?\s*/?\s*w\.?\b"         # "pw", "p/w", "p.w.", "p w"
    r"|\bpcm\b"                      # per calendar month
    r"|\bweekly\b",                  # "weekly", "weekly rent"
    re.I,
)


def _looks_like_rental(price_text: str | None) -> bool:
    return bool(price_text and _RENT_INDICATORS.search(price_text))
_SOLD_TAG_RE = re.compile(
    r"^Sold(?: by (\w+(?:\s\w+)?))?\s+(?:on\s+)?(\d{1,2}\s+\w+\s+\d{4})$"
)
_DATE_RE = re.compile(r"^(\d{1,2})\s+(\w{3,9})\s+(\d{4})$")
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

DOMAIN_BASE_URL = "https://www.domain.com.au"


def _parse_date(text: str) -> date | None:
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


_PRICE_MAX = 200_000_000


def _parse_price(text: str | None) -> tuple[int | None, int | None]:
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


def _parse_sold_tag(tag_text: str | None) -> tuple[date | None, str | None]:
    """Parse 'Sold by private treaty 03 Dec 2025' -> (date, 'private treaty')."""
    if not tag_text:
        return None, None
    m = _SOLD_TAG_RE.match(tag_text.strip())
    if not m:
        return None, None
    method, date_str = m.groups()
    return _parse_date(date_str), method


def _land_size_m2(features: dict | None) -> int | None:
    if not isinstance(features, dict):
        return None
    val = features.get("landSize")
    unit = (features.get("landUnit") or "").lower()
    try:
        n = float(val)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    if "ha" in unit:
        return int(n * 10_000)
    return int(n)


def extract_next_data(html: str) -> dict | None:
    """Extract the parsed Next.js __NEXT_DATA__ payload from a domain.com.au page."""
    doc = HTMLParser(html)
    node = doc.css_first('script#__NEXT_DATA__')
    if not node:
        return None
    text = node.text() or ""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _to_raw_listing(model: dict, listing_id: str, listing_kind: str) -> RawListing | None:
    addr = model.get("address") or {}
    street = addr.get("street") or ""
    suburb = addr.get("suburb") or ""
    postcode = addr.get("postcode") or ""
    state = addr.get("state") or ""

    if not (street and postcode):
        return None

    features = model.get("features") or {}
    branding = model.get("branding") or {}

    price_text = model.get("price")
    # Domain often spills rentals into /sale/ search results (with "premiumplus"
    # cross-suburb promotion). When the price text says "per week" / "p/w" etc.,
    # this is a rental — skip it so it doesn't pollute the duplex matcher.
    if listing_kind in ("buy", "sold") and _looks_like_rental(price_text):
        return None
    price_min, price_max = _parse_price(price_text)

    rel_url = model.get("url") or ""
    url = rel_url if rel_url.startswith("http") else f"{DOMAIN_BASE_URL}{rel_url}"

    sold_date = None
    sale_method = None
    sold_price_val = None
    status = "unknown"
    tags = model.get("tags") or {}
    tag_text = tags.get("tagText")
    tag_class = tags.get("tagClassName") or ""
    if listing_kind == "sold" or "sold" in tag_class.lower():
        listing_kind = "sold"
        status = "sold"
        sold_date, sale_method = _parse_sold_tag(tag_text)
        sold_price_val = price_max
    elif listing_kind == "buy":
        status = "active"
    elif listing_kind == "rent":
        status = "active"

    return RawListing(
        source_listing_id=str(listing_id),
        listing_kind=listing_kind,
        status=status,
        full_address=street,
        suburb=suburb,
        postcode=postcode,
        state=state,
        price_text=price_text,
        price_min=price_min,
        price_max=price_max,
        beds=features.get("beds") if isinstance(features.get("beds"), int) else None,
        baths=features.get("baths") if isinstance(features.get("baths"), int) else None,
        parking=features.get("parking") if isinstance(features.get("parking"), int) else None,
        property_type=features.get("propertyTypeFormatted") or features.get("propertyType"),
        land_size_m2=_land_size_m2(features),
        agent_name=branding.get("agentNames"),
        agency_name=branding.get("brandName"),
        url=url,
        sold_date=sold_date,
        sold_price=sold_price_val,
        sale_method=sale_method,
    )


def _detect_listing_kind(query: dict | None) -> str:
    if not isinstance(query, dict):
        return "buy"
    lt = (query.get("listingType") or "").lower()
    if "sold" in lt:
        return "sold"
    if "rent" in lt:
        return "rent"
    return "buy"


def parse_next_data(next_data: dict, default_kind: str | None = None) -> ParsedPage:
    page_props = (next_data.get("props") or {}).get("pageProps") or {}
    cp = page_props.get("componentProps") or {}
    listings_map = cp.get("listingsMap") or {}

    kind = default_kind or _detect_listing_kind(next_data.get("query"))

    listings: list[RawListing] = []
    for lid, entry in listings_map.items():
        if not isinstance(entry, dict):
            continue
        # Skip non-listing entries (project headers, ads, etc.)
        if entry.get("listingType") and entry.get("listingType") != "listing":
            continue
        model = entry.get("listingModel")
        if not isinstance(model, dict):
            continue
        rl = _to_raw_listing(model, str(lid), kind)
        if rl:
            listings.append(rl)

    return ParsedPage(
        listings=listings,
        current_page=cp.get("currentPage"),
        total_pages=cp.get("totalPages"),
        total_results=cp.get("totalListings"),
    )


def parse(html: str) -> ParsedPage:
    """Parse a domain.com.au search-results HTML page."""
    nd = extract_next_data(html)
    if not nd:
        return ParsedPage()
    return parse_next_data(nd)


def peek_pagination(html: str) -> tuple[int | None, int | None]:
    nd = extract_next_data(html)
    if not nd:
        return (None, None)
    cp = ((nd.get("props") or {}).get("pageProps") or {}).get("componentProps") or {}
    return cp.get("currentPage"), cp.get("totalPages")
