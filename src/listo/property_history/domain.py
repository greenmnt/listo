"""Domain.com.au property-profile (PDP) fetcher + parser.

Domain serves PDPs as plain HTML — no Kasada, no auth — and embeds the
full Apollo cache in `<script id="__NEXT_DATA__">`. From there we can pull:

- A canonical `Property` record (address, type, attributes, lot/plan).
- A `timeline` list of every sale + rental event Domain knows about.
- Domain's own valuation midpoint + rental estimate.

The fetch step writes the raw HTML to `raw_pages` (source=`domain_property`,
page_type=`pdp`); the parse step writes one `domain_properties` row plus
N `domain_sales` rows per timeline event.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import httpx

from listo.address import normalize_address
from listo.db import session_scope
from listo.models import DomainProperty, DomainSale
from listo.property_history.storage import store_raw_page


logger = logging.getLogger(__name__)


DOMAIN_BASE = "https://www.domain.com.au"
PDP_PATH = "/property-profile"
SOURCE = "domain_property"
PAGE_TYPE = "pdp"

# Used by curl probes — Domain's PDP serves cleanly to a plain Chrome UA.
_DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
)


_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>',
    re.DOTALL,
)


# ---------- URL building ----------


def _slugify(s: str) -> str:
    """Loose slug: lowercase, hyphens, collapse runs."""
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def slug_for_address(
    *, street_number: str, street: str, suburb: str, state: str, postcode: str,
    unit_number: str | None = None,
) -> str:
    """Build the Domain PDP slug. Examples:
        124 Sunshine Pde, Miami QLD 4220 → 124-sunshine-parade-miami-qld-4220
        1/124 Sunshine Pde, Miami QLD 4220 → 1-124-sunshine-parade-miami-qld-4220

    Domain's slug uses the long form of the street type ('parade' not 'pde')
    — without expansion, the URL 404s rather than redirecting. We canonicalise
    the last token of `street` to the canonical long form (handles short
    codes 'pde' → 'parade' AND variant long forms 'Boulevarde' → 'boulevard').
    """
    from listo.address import canonical_long_form

    tokens = (street or "").split()
    if tokens:
        tokens[-1] = canonical_long_form(tokens[-1])
    expanded_street = " ".join(tokens)

    parts: list[str] = []
    if unit_number:
        parts.append(unit_number)
    parts += [street_number, expanded_street, suburb, state, postcode]
    return "-".join(_slugify(str(p)) for p in parts if p)


def url_for_slug(slug: str) -> str:
    return f"{DOMAIN_BASE}{PDP_PATH}/{slug}"


# ---------- HTTP fetch ----------


@dataclass
class FetchResult:
    url: str
    http_status: int
    body: str
    headers: dict[str, str]


def fetch_pdp(url: str, *, timeout: float = 30.0) -> FetchResult:
    """Plain-HTTP GET. Domain has no Kasada / Cloudflare wall on PDPs."""
    headers = {
        "User-Agent": _DEFAULT_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
    }
    with httpx.Client(http2=True, follow_redirects=True, timeout=timeout) as client:
        resp = client.get(url, headers=headers)
    return FetchResult(
        url=str(resp.url),
        http_status=resp.status_code,
        body=resp.text,
        headers={k: v for k, v in resp.headers.items()},
    )


# ---------- parse ----------


def extract_next_data(html: str) -> dict | None:
    m = _NEXT_DATA_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def _find_property_record(next_data: dict) -> tuple[str | None, dict | None]:
    """Walk the Apollo cache for the single Property record.

    Returns (apollo_key, property_dict) or (None, None) if missing.
    The Apollo key is relay-style ('Property:UHJvcGVydHk6...').
    """
    apollo = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("__APOLLO_STATE__", {})
    )
    for key, val in apollo.items():
        if isinstance(val, dict) and val.get("__typename") == "Property":
            return key, val
    return None, None


def _parse_iso_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        # Domain ships datetimes; we only care about the date.
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _safe_int(v: Any) -> int | None:
    try:
        if v is None or isinstance(v, bool):
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> float | None:
    try:
        if v is None or isinstance(v, bool):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


@dataclass
class ParsedPdp:
    domain_property_id: str
    domain_apollo_id: str
    url_slug: str
    display_address: str
    unit_number: str
    street_number: str
    street_name: str
    street_type: str
    suburb: str
    postcode: str
    state: str
    lat: float | None
    lng: float | None
    lot_number: str | None
    plan_number: str | None
    property_type: str | None
    bedrooms: int | None
    bathrooms: int | None
    parking_spaces: int | None
    land_area_m2: int | None
    internal_area_m2: int | None
    valuation_low: int | None
    valuation_mid: int | None
    valuation_high: int | None
    valuation_confidence: str | None
    valuation_date: date | None
    rent_estimate_weekly: int | None
    rent_yield_pct: float | None
    raw_property: dict
    timeline: list[dict]


def parse_pdp(html: str) -> ParsedPdp | None:
    """Parse a Domain PDP HTML into a ParsedPdp record. Returns None if the
    Apollo Property entry can't be found (e.g. the page redirected to a
    suburb-search instead of a property-profile)."""
    nd = extract_next_data(html)
    if not nd:
        return None
    apollo_key, prop = _find_property_record(nd)
    if not prop or not apollo_key:
        return None

    addr = prop.get("address") or {}
    valuation = prop.get("valuation") or {}
    rent = prop.get("rentalEstimate") or {}
    geo = (addr.get("geolocation") or {}) if isinstance(addr, dict) else {}

    return ParsedPdp(
        domain_property_id=str(prop.get("propertyId") or "")[:64],
        domain_apollo_id=apollo_key[:255],
        url_slug=str(prop.get("hpgSlug") or "")[:255],
        display_address=str(addr.get("displayAddress") or "")[:255],
        unit_number=str(addr.get("unitNumber") or "")[:16],
        street_number=str(addr.get("streetNumber") or "")[:16],
        street_name=str(addr.get("streetName") or "")[:120],
        street_type=str(addr.get("streetType") or "")[:20],
        suburb=str(addr.get("suburbName") or "")[:80],
        postcode=str(addr.get("postcode") or "")[:4],
        state=str(addr.get("state") or "")[:3],
        lat=_safe_float(geo.get("latitude")),
        lng=_safe_float(geo.get("longitude")),
        lot_number=(str(prop["lotNumber"])[:20] if prop.get("lotNumber") else None),
        plan_number=(str(prop["planNumber"])[:20] if prop.get("planNumber") else None),
        property_type=(str(prop["type"])[:40] if prop.get("type") else None),
        bedrooms=_safe_int(prop.get("bedrooms")),
        bathrooms=_safe_int(prop.get("bathrooms")),
        parking_spaces=_safe_int(prop.get("parkingSpaces")),
        # Apollo serialises the field-with-args as e.g. `landArea({"unit":"SQUARE_METERS"})`.
        land_area_m2=_safe_int(prop.get('landArea({"unit":"SQUARE_METERS"})')),
        internal_area_m2=_safe_int(prop.get('internalArea({"unit":"SQUARE_METERS"})')),
        valuation_low=_safe_int(valuation.get("lowerPrice")),
        valuation_mid=_safe_int(valuation.get("midPrice")),
        valuation_high=_safe_int(valuation.get("upperPrice")),
        valuation_confidence=(str(valuation.get("priceConfidence"))[:40] if valuation.get("priceConfidence") else None),
        valuation_date=_parse_iso_date(valuation.get("date")),
        rent_estimate_weekly=_safe_int(rent.get("weeklyRentEstimate")),
        rent_yield_pct=_safe_float(rent.get("percentYieldRentEstimate")),
        raw_property=prop,
        timeline=[ev for ev in (prop.get("timeline") or []) if isinstance(ev, dict)],
    )


# ---------- persist ----------


def persist(parsed: ParsedPdp, *, raw_page_id: int, fetch_url: str) -> int:
    """Upsert a domain_properties row + one domain_sales row per timeline event.

    Keyed on `domain_property_id` (Domain's own ID for the property, e.g.
    'AS-2461-VY'). Rerunning the scraper for the same listing refreshes
    all parsed fields and the raw_property_json blob without leaving
    duplicate rows behind.

    Returns the domain_properties.id of the upserted row.
    """
    from sqlalchemy.dialects.mysql import insert as mysql_insert
    from sqlalchemy import select as sa_select

    now = datetime.utcnow()
    values = dict(
        raw_page_id=raw_page_id,
        property_id=None,
        domain_property_id=parsed.domain_property_id,
        domain_apollo_id=parsed.domain_apollo_id,
        url_slug=parsed.url_slug,
        url=fetch_url[:1024],
        display_address=parsed.display_address,
        unit_number=parsed.unit_number,
        street_number=parsed.street_number,
        street_name=parsed.street_name,
        street_type=parsed.street_type,
        suburb=parsed.suburb,
        postcode=parsed.postcode,
        state=parsed.state,
        lat=parsed.lat,
        lng=parsed.lng,
        lot_number=parsed.lot_number,
        plan_number=parsed.plan_number,
        property_type=parsed.property_type,
        bedrooms=parsed.bedrooms,
        bathrooms=parsed.bathrooms,
        parking_spaces=parsed.parking_spaces,
        land_area_m2=parsed.land_area_m2,
        internal_area_m2=parsed.internal_area_m2,
        valuation_low=parsed.valuation_low,
        valuation_mid=parsed.valuation_mid,
        valuation_high=parsed.valuation_high,
        valuation_confidence=parsed.valuation_confidence,
        valuation_date=parsed.valuation_date,
        rent_estimate_weekly=parsed.rent_estimate_weekly,
        rent_yield_pct=parsed.rent_yield_pct,
        raw_property_json=parsed.raw_property,
        fetched_at=now,
        parsed_at=now,
    )

    with session_scope() as s:
        stmt = mysql_insert(DomainProperty).values(**values)
        # Refresh every column on conflict — re-runs may have updated
        # valuations, timelines, or the price-history payload. Skip
        # `id` and `property_id` (the latter is set by a separate
        # consolidation step that takes priority over scraper output).
        update_cols = {
            k: stmt.inserted[k]
            for k in values
            if k not in ("property_id",)
        }
        s.execute(stmt.on_duplicate_key_update(**update_cols))
        # Resolve the natural-key row to get its id back.
        dp_id = s.execute(
            sa_select(DomainProperty.id).where(
                DomainProperty.domain_property_id == parsed.domain_property_id
            )
        ).scalar_one()

        # Re-runs replace the timeline entirely — Domain's price-
        # history blob is the source of truth, and a fresh fetch may
        # have new events / revised prices / removed errors. Without
        # this, every scrape appended duplicate DomainSale rows.
        from sqlalchemy import delete as sa_delete
        s.execute(
            sa_delete(DomainSale).where(DomainSale.domain_property_id == dp_id)
        )
        for ev in parsed.timeline:
            agency = ev.get("agency") or {}
            sale_meta = ev.get("saleMetadata") or {}
            s.add(DomainSale(
                domain_property_id=dp_id,
                property_id=None,
                raw_page_id=raw_page_id,
                event_date=_parse_iso_date(ev.get("eventDate")),
                event_price=_safe_int(ev.get("eventPrice")),
                category=str(ev.get("category") or "Unknown")[:20],
                price_description=(str(ev.get("priceDescription"))[:120] if ev.get("priceDescription") else None),
                is_sold=bool(sale_meta.get("isSold") or False),
                is_major_event=bool(ev.get("isMajorEvent") or False),
                days_on_market=_safe_int(ev.get("daysOnMarket")),
                agency_name=(str(agency.get("name"))[:160] if isinstance(agency, dict) and agency.get("name") else None),
                agency_profile_url=(str(agency.get("profileUrl"))[:255] if isinstance(agency, dict) and agency.get("profileUrl") else None),
                raw_event_json=ev,
            ))

    return dp_id


# ---------- top-level: fetch + parse + persist ----------


@dataclass
class FetchPdpResult:
    url: str
    http_status: int
    raw_page_id: int
    domain_property_id: int | None
    parsed: ParsedPdp | None
    error: str | None = None


def fetch_and_persist(url: str) -> FetchPdpResult:
    """One-shot: fetch a Domain PDP, archive raw HTML, parse, persist."""
    fetched = fetch_pdp(url)

    raw_page_id = store_raw_page(
        source=SOURCE,
        page_type=PAGE_TYPE,
        url=fetched.url,
        body=fetched.body,
        http_status=fetched.http_status,
        headers=fetched.headers,
    )

    if fetched.http_status != 200:
        return FetchPdpResult(
            url=fetched.url, http_status=fetched.http_status,
            raw_page_id=raw_page_id, domain_property_id=None,
            parsed=None, error=f"HTTP {fetched.http_status}",
        )

    parsed = parse_pdp(fetched.body)
    if not parsed:
        return FetchPdpResult(
            url=fetched.url, http_status=fetched.http_status,
            raw_page_id=raw_page_id, domain_property_id=None,
            parsed=None, error="no Apollo Property record found",
        )

    dp_id = persist(parsed, raw_page_id=raw_page_id, fetch_url=fetched.url)
    return FetchPdpResult(
        url=fetched.url, http_status=fetched.http_status,
        raw_page_id=raw_page_id, domain_property_id=dp_id,
        parsed=parsed, error=None,
    )


def fetch_by_address(address: str, *, default_state: str = "QLD") -> FetchPdpResult:
    """Fetch a Domain PDP from a freeform address like
    '124 Sunshine Pde, Miami, QLD 4220'.

    Splits on commas to pull out suburb/state/postcode, then runs the
    standard normaliser to get the slug components.

    Records the outcome in `property_scrape_attempts` so future runs
    can skip addresses Domain genuinely has no profile for, instead
    of re-attempting them on every batch.
    """
    from listo.property_history.scrape_attempts import record_attempt

    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) < 2:
        raise ValueError(f"need at least 'street, suburb [state] postcode': {address!r}")

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
        street=norm.street_name,
        suburb=norm.suburb,
        state=state.lower(),
        postcode=postcode,
        unit_number=norm.unit_number or None,
    )
    url = url_for_slug(slug)
    result = fetch_and_persist(url)

    if result.error is None:
        attempt_result = "found"
    elif result.http_status == 404:
        attempt_result = "not_found"
    else:
        attempt_result = "error"
    try:
        record_attempt(
            source="domain",
            display_address=address,
            url=url,
            http_status=result.http_status,
            result=attempt_result,
            error_message=result.error,
        )
    except Exception:  # noqa: BLE001 — telemetry failure must not break the run
        logger.warning("failed to record property_scrape_attempt for %s", address)

    return result
