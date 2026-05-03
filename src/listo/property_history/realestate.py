"""Realestate.com.au property-profile (PDP) fetcher + parser.

REA's PDP carries:
- `propertyProfile.property` — id, slug, address, attributes, AVM
- `propertyProfile.property.propertyTimeline` — sale + listing history
- `propertyProfile.streetDetails.neighbouringProperties` — adjacent units
- `propertyProfile.pca.pcaPropertyLink` — direct sibling URL on
  property.com.au (REA Group owns both, same numeric id)

Reachable only via real Chrome (Kasada). See `cdp.py` for the bypass.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from listo.address import normalize_address
from listo.db import session_scope
from listo.models import RealestateProperty, RealestateSale
from listo.property_history.cdp import fetch_html, fetch_html_via_google_click
from listo.property_history.storage import store_raw_page


logger = logging.getLogger(__name__)


REA_BASE = "https://www.realestate.com.au"
PDP_PATH = "/property"
SOURCE = "realestate_property"
PAGE_TYPE = "pdp"


# ---------- Argonaut extraction ----------


_ARGONAUT_KEY = "resi-property_property-profile"


def _extract_argonaut(html: str) -> dict | None:
    """Find `window.ArgonautExchange = { ... }` and return the parsed object."""
    i = html.find("window.ArgonautExchange")
    if i < 0:
        return None
    b = html.find("{", i)
    if b < 0:
        return None
    depth = 0
    end = b
    for k, c in enumerate(html[b:]):
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = b + k + 1
                break
    try:
        return json.loads(html[b:end])
    except json.JSONDecodeError:
        return None


def _extract_property_detail(html: str) -> dict | None:
    """Pull the `property_detail_data` payload out of ArgonautExchange.

    The Argonaut payload looks like:
        {
          "resi-property_property-profile": {
            "property_detail_data": "<JSON-string>"
          }
        }
    """
    argo = _extract_argonaut(html)
    if not argo:
        return None
    container = argo.get(_ARGONAUT_KEY)
    if not isinstance(container, dict):
        return None
    pdd = container.get("property_detail_data")
    if not isinstance(pdd, str):
        return None
    try:
        return json.loads(pdd)
    except json.JSONDecodeError:
        return None


# ---------- URL building ----------


def _slugify(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def slug_for_address(
    *, street_number: str, street_norm: str, suburb: str, state: str, postcode: str,
    unit_number: str | None = None,
) -> str:
    """Build the REA PDP slug. REA uses *abbreviated* street types (Pde,
    not Parade) — exactly what `address.normalize_address.street_norm`
    gives us.

    Examples:
        124 Sunshine Pde, Miami QLD 4220 → 124-sunshine-pde-miami-qld-4220
        1/124 Sunshine Pde, Miami QLD 4220 → unit-1-124-sunshine-pde-miami-qld-4220
    """
    base_parts = [street_number, street_norm, suburb, state, postcode]
    base = "-".join(_slugify(str(p)) for p in base_parts if p)
    if unit_number:
        return f"unit-{_slugify(unit_number)}-{base}"
    return base


def url_for_slug(slug: str) -> str:
    return f"{REA_BASE}{PDP_PATH}/{slug}/"


# ---------- parse ----------


@dataclass
class ParsedReaPdp:
    rea_property_id: int
    url_slug: str
    pca_property_url: str | None
    display_address: str
    unit_number: str
    street_number: str
    street_name: str
    suburb: str
    postcode: str
    state: str
    lat: float | None
    lng: float | None
    property_type: str | None
    bedrooms: int | None
    bathrooms: int | None
    car_spaces: int | None
    land_area_m2: int | None
    floor_area_m2: int | None
    year_built: int | None
    status_label: str | None
    market_status: str | None
    valuation_low: int | None
    valuation_mid: int | None
    valuation_high: int | None
    valuation_confidence: str | None
    rent_estimate_weekly: int | None
    rent_yield_pct: float | None
    raw_property: dict
    timeline: list[dict]


def _attr_value(attrs: dict, key: str) -> Any:
    """REA wraps numeric attrs as `{value, displayValue, sizeUnit}`. Return
    just the numeric `value`, or None if missing/non-numeric."""
    node = attrs.get(key)
    if isinstance(node, dict):
        v = node.get("value")
        if isinstance(v, (int, float)):
            return v
    return None


def _land_m2(node: dict | None) -> int | None:
    """`landArea = {value, sizeUnit: {displayValue, key}}`.
    `key` is `square_meters` / `hectares` / `acres`.
    """
    if not isinstance(node, dict):
        return None
    val = node.get("value")
    if not isinstance(val, (int, float)):
        return None
    unit_key = ((node.get("sizeUnit") or {}).get("key") or "").lower()
    if "hectare" in unit_key:
        return int(val * 10_000)
    if "acre" in unit_key:
        return int(val * 4046.8564)
    # default: m²
    return int(val)


def _avm_sale(valuations: dict | None) -> tuple[int | None, int | None, int | None, str | None]:
    """Pull (low, mid, high, confidence) from valuations.sale."""
    if not isinstance(valuations, dict):
        return (None, None, None, None)
    sale = valuations.get("sale") or {}
    if not isinstance(sale, dict):
        return (None, None, None, None)
    est = sale.get("estimate") or {}
    if not isinstance(est, dict):
        # Some payloads put the values directly on `sale`.
        est = sale
    low = est.get("low") or est.get("lowerPrice")
    mid = est.get("mid") or est.get("midPrice") or est.get("estimate")
    high = est.get("high") or est.get("upperPrice")
    confidence = est.get("confidence") or est.get("priceConfidence")
    return (
        int(low) if isinstance(low, (int, float)) else None,
        int(mid) if isinstance(mid, (int, float)) else None,
        int(high) if isinstance(high, (int, float)) else None,
        str(confidence) if confidence else None,
    )


def _avm_rental(valuations: dict | None) -> tuple[int | None, float | None]:
    if not isinstance(valuations, dict):
        return (None, None)
    rental = valuations.get("rental") or {}
    if not isinstance(rental, dict):
        return (None, None)
    weekly = rental.get("weeklyEstimate") or rental.get("estimate")
    yld = rental.get("yieldEstimate") or rental.get("yield")
    return (
        int(weekly) if isinstance(weekly, (int, float)) else None,
        float(yld) if isinstance(yld, (int, float)) else None,
    )


def _parse_iso_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _parse_price(text: str | None) -> int | None:
    """'$1,200,000' → 1200000. Returns None if can't extract a single value."""
    if not text:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_pdp(html: str) -> ParsedReaPdp | None:
    pdd = _extract_property_detail(html)
    if not pdd:
        return None
    pp = pdd.get("propertyProfile")
    if not isinstance(pp, dict):
        return None
    prop = pp.get("property")
    if not isinstance(prop, dict):
        return None

    rea_id = prop.get("id")
    try:
        rea_id_int = int(rea_id)
    except (TypeError, ValueError):
        return None  # without an id, we can't dedupe properly

    attrs = prop.get("attributes") or {}
    valuations = prop.get("valuations") or {}
    geocode = prop.get("geocode") or {}
    status_badge = prop.get("statusBadge") or {}

    # Floor area might be an int directly or wrapped — handle both.
    floor_node = attrs.get("floorArea")
    floor_m2: int | None = None
    if isinstance(floor_node, dict):
        floor_m2 = _land_m2(floor_node)
    elif isinstance(floor_node, (int, float)):
        floor_m2 = int(floor_node)

    year_built = _attr_value(attrs, "yearBuilt")
    if isinstance(year_built, float):
        year_built = int(year_built)

    pca_url = ((pp.get("pca") or {}).get("pcaPropertyLink") or {}).get("href")

    # Address: normalize_address gives us a NormalizedAddress with the
    # decomposed parts. We feed it the short form (no unit prefix split)
    # because REA's `unitNumber` is exposed separately when present.
    short = prop.get("shortAddress") or ""
    suburb = prop.get("suburb") or ""
    postcode = str(prop.get("postcode") or "")
    norm = normalize_address(short, suburb, postcode)

    avm_low, avm_mid, avm_high, avm_conf = _avm_sale(valuations)
    rent_weekly, rent_yld = _avm_rental(valuations)

    return ParsedReaPdp(
        rea_property_id=rea_id_int,
        url_slug=str(prop.get("slug") or "")[:255],
        pca_property_url=str(pca_url)[:1024] if pca_url else None,
        display_address=str(prop.get("fullAddress") or "")[:255],
        unit_number=norm.unit_number,
        street_number=norm.street_number,
        street_name=norm.street_name,
        suburb=str(prop.get("suburb") or "")[:80],
        postcode=postcode[:4],
        state=str(prop.get("state") or "")[:3],
        lat=geocode.get("latitude") if isinstance(geocode.get("latitude"), (int, float)) else None,
        lng=geocode.get("longitude") if isinstance(geocode.get("longitude"), (int, float)) else None,
        property_type=((attrs.get("propertyType") or "") or None) if isinstance(attrs.get("propertyType"), str) else None,
        bedrooms=int(_attr_value(attrs, "bedrooms")) if _attr_value(attrs, "bedrooms") is not None else None,
        bathrooms=int(_attr_value(attrs, "bathrooms")) if _attr_value(attrs, "bathrooms") is not None else None,
        car_spaces=int(_attr_value(attrs, "carSpaces")) if _attr_value(attrs, "carSpaces") is not None else None,
        land_area_m2=_land_m2(attrs.get("landArea")),
        floor_area_m2=floor_m2,
        year_built=year_built if isinstance(year_built, int) else None,
        status_label=str(status_badge.get("label"))[:40] if status_badge.get("label") else None,
        market_status=str(pp.get("trackingMarketStatus") or "")[:40] or None,
        valuation_low=avm_low,
        valuation_mid=avm_mid,
        valuation_high=avm_high,
        valuation_confidence=avm_conf[:40] if avm_conf else None,
        rent_estimate_weekly=rent_weekly,
        rent_yield_pct=rent_yld,
        raw_property=prop,
        timeline=[ev for ev in (prop.get("propertyTimeline") or []) if isinstance(ev, dict)],
    )


# ---------- persist ----------


def persist(parsed: ParsedReaPdp, *, raw_page_id: int, fetch_url: str) -> int:
    now = datetime.utcnow()
    with session_scope() as s:
        rp = RealestateProperty(
            raw_page_id=raw_page_id,
            property_id=None,
            rea_property_id=parsed.rea_property_id,
            url_slug=parsed.url_slug,
            url=fetch_url[:1024],
            pca_property_url=parsed.pca_property_url,
            display_address=parsed.display_address,
            unit_number=parsed.unit_number,
            street_number=parsed.street_number,
            street_name=parsed.street_name,
            suburb=parsed.suburb,
            postcode=parsed.postcode,
            state=parsed.state,
            lat=parsed.lat,
            lng=parsed.lng,
            property_type=parsed.property_type,
            bedrooms=parsed.bedrooms,
            bathrooms=parsed.bathrooms,
            car_spaces=parsed.car_spaces,
            land_area_m2=parsed.land_area_m2,
            floor_area_m2=parsed.floor_area_m2,
            year_built=parsed.year_built,
            status_label=parsed.status_label,
            market_status=parsed.market_status,
            valuation_low=parsed.valuation_low,
            valuation_mid=parsed.valuation_mid,
            valuation_high=parsed.valuation_high,
            valuation_confidence=parsed.valuation_confidence,
            rent_estimate_weekly=parsed.rent_estimate_weekly,
            rent_yield_pct=parsed.rent_yield_pct,
            raw_property_json=parsed.raw_property,
            fetched_at=now,
            parsed_at=now,
        )
        s.add(rp)
        s.flush()
        rp_id = rp.id

        for ev in parsed.timeline:
            s.add(RealestateSale(
                realestate_property_id=rp_id,
                property_id=None,
                raw_page_id=raw_page_id,
                event_date=_parse_iso_date(ev.get("date")),
                event_price=_parse_price(ev.get("price")),
                price_text=str(ev.get("price"))[:120] if ev.get("price") else None,
                event_type=str(ev.get("eventType") or "unknown")[:40],
                agency_name=(str(ev.get("agency"))[:160].strip() if ev.get("agency") else None),
                listing_url=None,  # PDP timeline doesn't include direct URLs (only image media)
                raw_event_json=ev,
            ))
    return rp_id


# ---------- top-level ----------


@dataclass
class FetchReaPdpResult:
    url: str
    http_status: int
    raw_page_id: int
    realestate_property_id: int | None
    parsed: ParsedReaPdp | None
    error: str | None = None


def fetch_and_persist(url: str) -> FetchReaPdpResult:
    # REA's Kasada wall scores the Referer header — direct page.goto
    # arrives with an empty Referer (bot-shaped). Click-through from a
    # Google search sets the canonical google.com Referer, plus a
    # randomised dwell on the loaded tab makes the visit read human.
    fetched = fetch_html_via_google_click(
        url,
        wait_for_function="() => !!window.ArgonautExchange",
        wait_until="domcontentloaded",
        settle_seconds=4.0,
    )
    raw_page_id = store_raw_page(
        source=SOURCE,
        page_type=PAGE_TYPE,
        url=fetched.final_url,
        body=fetched.html,
        http_status=fetched.http_status,
        headers={},  # CDP doesn't surface response headers cleanly; fine to leave empty
    )
    if fetched.http_status != 200 and fetched.http_status != 0:
        return FetchReaPdpResult(
            url=fetched.final_url, http_status=fetched.http_status,
            raw_page_id=raw_page_id, realestate_property_id=None,
            parsed=None, error=f"HTTP {fetched.http_status}",
        )

    parsed = parse_pdp(fetched.html)
    if not parsed:
        # Likely a Kasada interstitial (tiny body) or a search-redirect.
        return FetchReaPdpResult(
            url=fetched.final_url, http_status=fetched.http_status,
            raw_page_id=raw_page_id, realestate_property_id=None,
            parsed=None,
            error="no propertyProfile.property record found "
                  "(Kasada interstitial? page redirected?)",
        )

    rp_id = persist(parsed, raw_page_id=raw_page_id, fetch_url=fetched.final_url)
    return FetchReaPdpResult(
        url=fetched.final_url, http_status=fetched.http_status,
        raw_page_id=raw_page_id, realestate_property_id=rp_id,
        parsed=parsed, error=None,
    )


def fetch_by_address(address: str) -> FetchReaPdpResult:
    """'124 Sunshine Pde, Miami QLD 4220' → fetch_and_persist(rea url)."""
    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) < 2:
        raise ValueError(f"need 'street, suburb STATE postcode': {address!r}")

    street = parts[0]
    tail = " ".join(parts[1:])
    m = re.search(r"(.+?)\s+([A-Z]{2,3})\s+(\d{4})$", tail)
    if not m:
        raise ValueError(f"can't parse suburb/state/postcode from {tail!r}")
    suburb = m.group(1).strip()
    state = m.group(2).strip()
    postcode = m.group(3).strip()

    norm = normalize_address(street, suburb, postcode)
    slug = slug_for_address(
        street_number=norm.street_number,
        street_norm=norm.street_norm,
        suburb=norm.suburb,
        state=state.lower(),
        postcode=postcode,
        unit_number=norm.unit_number or None,
    )
    return fetch_and_persist(url_for_slug(slug))
