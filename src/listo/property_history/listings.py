"""Sold-listing detail-page scrapers for both realestate.com.au (via CDP /
Kasada bypass) and domain.com.au (plain HTTP).

Listing-detail pages add what the property-profile timeline can't:
- Agent's full description text (gold for Ollama summarisation later).
- Photo gallery (visual evidence of pre-redev vs post-redev state).
- Listing-time price + auction details + days on market.

Parse intent: extract the headline fields that we'd display in a UI
(price, date, beds/baths/etc., description) plus archive the *full*
parsed JSON payload so we can re-extract more later without re-fetching.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import httpx

from sqlalchemy import select

from listo.db import session_scope
from listo.models import DomainListing, RealestateListing
from listo.property_history.cdp import fetch_html as cdp_fetch_html
from listo.property_history.storage import store_raw_page


logger = logging.getLogger(__name__)


# ---------- common helpers ----------


def _parse_iso_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).date()
    except ValueError:
        return None


_PRICE_RE = re.compile(r"\$([\d,]+)")


def _parse_price_text(text: str | None) -> int | None:
    if not text:
        return None
    m = _PRICE_RE.search(text)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _safe_int(v: Any) -> int | None:
    try:
        if isinstance(v, bool):
            return None
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


# ============================================================================
# realestate.com.au listing pages — /sold/property-..., /property-..., /buy/...
# ============================================================================


REA_LISTING_ID_RE = re.compile(r"-(\d{6,})\b")
REA_SOURCE = "realestate_listing"
REA_PAGE_TYPE = "listing"


@dataclass
class ParsedReaListing:
    rea_listing_id: int
    listing_kind: str
    display_address: str
    property_type: str | None
    price_text: str | None
    sold_price: int | None
    sold_date: date | None
    sale_method: str | None
    bedrooms: int | None
    bathrooms: int | None
    car_spaces: int | None
    land_area_m2: int | None
    floor_area_m2: int | None
    agency_name: str | None
    agent_name: str | None
    description: str | None
    features: list[str] | None
    photos: list[dict] | None
    raw_listing: dict


def _rea_extract_listing_id(url: str) -> int | None:
    m = REA_LISTING_ID_RE.search(url.rstrip("/"))
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _rea_listing_kind_from_url(url: str) -> str:
    if "/sold/" in url:
        return "sold"
    if "/buy/" in url or "/property-" in url:
        return "buy"
    return "unknown"


def _rea_extract_argonaut(html: str) -> dict | None:
    """REA listing pages use the same Argonaut wrapper as PDPs."""
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


def _rea_walk_listing_node(argo: dict) -> dict | None:
    """Walk the Argonaut payload looking for the listing detail object.

    The key varies: search-result pages use `resi-property_listing-experience-web`,
    detail pages typically use `resi-property_listing-experience-web` too but
    with a different `urqlClientCache` shape. We search for any node with
    `__typename` ending in 'ResidentialListing' and return that whole subtree.
    """
    found: list[dict] = []

    def walk(n: Any) -> None:
        if isinstance(n, dict):
            tn = n.get("__typename") or ""
            if tn.endswith("ResidentialListing"):
                found.append(n)
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for v in n:
                walk(v)

    # Strings inside Argonaut are often double-encoded JSON — recursively decode.
    def maybe_parse(v: Any) -> Any:
        if isinstance(v, str) and (v.lstrip().startswith("{") or v.lstrip().startswith("[")):
            try:
                return maybe_parse(json.loads(v))
            except json.JSONDecodeError:
                return v
        if isinstance(v, dict):
            return {k: maybe_parse(x) for k, x in v.items()}
        if isinstance(v, list):
            return [maybe_parse(x) for x in v]
        return v

    walk(maybe_parse(argo))
    # Prefer the largest one (the full listing detail, not a stripped reference).
    if not found:
        return None
    return max(found, key=lambda d: len(json.dumps(d, default=str)))


def parse_rea_listing(url: str, html: str) -> ParsedReaListing | None:
    listing_id = _rea_extract_listing_id(url)
    if listing_id is None:
        return None
    kind = _rea_listing_kind_from_url(url)

    argo = _rea_extract_argonaut(html)
    listing_node: dict | None = None
    if argo:
        listing_node = _rea_walk_listing_node(argo)

    if not listing_node:
        # Fallback: minimal record from the URL alone so we at least
        # archive the raw_page_id.
        return ParsedReaListing(
            rea_listing_id=listing_id, listing_kind=kind,
            display_address="", property_type=None, price_text=None,
            sold_price=None, sold_date=None, sale_method=None,
            bedrooms=None, bathrooms=None, car_spaces=None,
            land_area_m2=None, floor_area_m2=None,
            agency_name=None, agent_name=None, description=None,
            features=None, photos=None, raw_listing={},
        )

    addr = listing_node.get("address") or {}
    display = addr.get("display") or {}
    price = listing_node.get("price") or {}
    gf = listing_node.get("generalFeatures") or {}
    pt = listing_node.get("propertyType") or {}
    listers = listing_node.get("listers") or []
    company = listing_node.get("listingCompany") or {}
    description = listing_node.get("description") or listing_node.get("descriptionMarkup")
    features = listing_node.get("features") or listing_node.get("propertyFeatures")
    media = listing_node.get("media") or {}
    images = (media.get("images") or []) if isinstance(media, dict) else []

    sold_date = None
    sale_method = None
    if kind == "sold":
        ds = listing_node.get("dateSold") or {}
        sold_date = _parse_iso_date(ds.get("date") or ds.get("display"))
        sm = listing_node.get("saleMethod") or {}
        sale_method = sm.get("display") if isinstance(sm, dict) else (str(sm) if sm else None)

    return ParsedReaListing(
        rea_listing_id=listing_id,
        listing_kind=kind,
        display_address=str(display.get("fullAddress") or display.get("shortAddress") or "")[:255],
        property_type=(pt.get("display") if isinstance(pt, dict) else (str(pt) if pt else None)),
        price_text=str(price.get("display"))[:160] if price.get("display") else None,
        sold_price=_parse_price_text(price.get("display")),
        sold_date=sold_date,
        sale_method=str(sale_method)[:40] if sale_method else None,
        bedrooms=_extract_int_attr(gf.get("bedrooms")),
        bathrooms=_extract_int_attr(gf.get("bathrooms")),
        car_spaces=_extract_int_attr(gf.get("parkingSpaces")),
        land_area_m2=_extract_size(listing_node.get("propertySizes"), "land"),
        floor_area_m2=_extract_size(listing_node.get("propertySizes"), "building"),
        agency_name=str(company.get("name"))[:160] if company.get("name") else None,
        agent_name=(str(listers[0].get("name"))[:160] if listers and isinstance(listers[0], dict) and listers[0].get("name") else None),
        description=str(description)[:65_000] if description else None,
        features=[str(f) for f in features] if isinstance(features, list) else None,
        photos=[{"url": img.get("server") or img.get("templatedUrl") or img.get("url"),
                 "id": img.get("id")} for img in images][:60] if images else None,
        raw_listing=listing_node,
    )


def _extract_int_attr(node: Any) -> int | None:
    """Pull an int from a `{value, displayValue}` REA attribute, with sanity cap."""
    if isinstance(node, dict):
        v = node.get("value")
        if isinstance(v, (int, float)):
            iv = int(v)
            return iv if 0 <= iv <= 99 else None
    return None


def _extract_size(node: dict | None, kind: str) -> int | None:
    if not isinstance(node, dict):
        return None
    sub = node.get(kind)
    if not isinstance(sub, dict):
        return None
    val = sub.get("displayValue") or sub.get("value")
    unit = ((sub.get("sizeUnit") or {}).get("displayValue") or "").lower()
    try:
        n = float(val)
    except (TypeError, ValueError):
        return None
    if "m" in unit:
        return int(n)
    if "ha" in unit:
        return int(n * 10_000)
    return int(n)


def fetch_rea_listing(url: str) -> tuple[int, int | None, ParsedReaListing | None, str | None]:
    """Returns (raw_page_id, realestate_listings.id or None, parsed or None, error).

    Idempotent: if a row already exists for this rea_listing_id, it's
    returned unchanged (no re-fetch). This lets the orchestrator be sloppy
    about duplicate URLs without tripping the unique constraint.
    """
    listing_id = _rea_extract_listing_id(url)
    if listing_id is not None:
        with session_scope() as s:
            existing = s.execute(
                select(RealestateListing).where(
                    RealestateListing.rea_listing_id == listing_id
                )
            ).scalar_one_or_none()
            if existing:
                return existing.raw_page_id, existing.id, None, None  # already fetched

    fetched = cdp_fetch_html(
        url, wait_for_function="() => !!window.ArgonautExchange",
        wait_until="domcontentloaded",
    )
    raw_page_id = store_raw_page(
        source=REA_SOURCE, page_type=REA_PAGE_TYPE,
        url=fetched.final_url, body=fetched.html,
        http_status=fetched.http_status, headers={},
    )
    parsed = parse_rea_listing(fetched.final_url, fetched.html)
    if not parsed:
        return raw_page_id, None, None, "couldn't extract listing id from URL"

    now = datetime.utcnow()
    with session_scope() as s:
        rl = RealestateListing(
            raw_page_id=raw_page_id,
            realestate_property_id=None,
            rea_listing_id=parsed.rea_listing_id,
            url=fetched.final_url[:1024],
            listing_kind=parsed.listing_kind,
            listing_status=None,
            display_address=parsed.display_address,
            property_type=parsed.property_type,
            price_text=parsed.price_text,
            sold_price=parsed.sold_price,
            sold_date=parsed.sold_date,
            sale_method=parsed.sale_method,
            bedrooms=parsed.bedrooms,
            bathrooms=parsed.bathrooms,
            car_spaces=parsed.car_spaces,
            land_area_m2=parsed.land_area_m2,
            floor_area_m2=parsed.floor_area_m2,
            agency_name=parsed.agency_name,
            agent_name=parsed.agent_name,
            description=parsed.description,
            features_json=parsed.features,
            photos_json=parsed.photos,
            raw_listing_json=parsed.raw_listing,
            fetched_at=now, parsed_at=now,
        )
        s.add(rl)
        s.flush()
        return raw_page_id, rl.id, parsed, None


# ============================================================================
# domain.com.au listing pages — /{slug}-{listingId}
# ============================================================================


DOMAIN_LISTING_ID_RE = re.compile(r"-(\d{6,})/?$")
DOMAIN_SOURCE = "domain_listing"
DOMAIN_PAGE_TYPE = "listing"

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>',
    re.DOTALL,
)
_DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
)


@dataclass
class ParsedDomainListing:
    domain_listing_id: int
    listing_kind: str
    display_address: str
    property_type: str | None
    price_text: str | None
    sold_price: int | None
    sold_date: date | None
    sale_method: str | None
    bedrooms: int | None
    bathrooms: int | None
    car_spaces: int | None
    land_area_m2: int | None
    agency_name: str | None
    agent_name: str | None
    description: str | None
    features: list[str] | None
    photos: list[dict] | None
    raw_listing: dict


def _domain_extract_listing_id(url: str) -> int | None:
    m = DOMAIN_LISTING_ID_RE.search(url.rstrip("/"))
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _domain_extract_next_data(html: str) -> dict | None:
    m = _NEXT_DATA_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def _domain_walk_listing(next_data: dict) -> dict | None:
    """Domain listings have `pageProps.componentProps.listingDetail` (or
    similar). Walk the Apollo state for the largest object with
    `__typename` like 'BuyResidentialListing' / 'SoldHistoricalListing'."""
    apollo = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("__APOLLO_STATE__")
    )
    candidates: list[dict] = []
    if isinstance(apollo, dict):
        for v in apollo.values():
            if isinstance(v, dict):
                tn = v.get("__typename") or ""
                if "Listing" in tn:
                    candidates.append(v)
    # Also try componentProps directly (non-Apollo Domain pages).
    cp = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("componentProps", {})
    )
    if isinstance(cp, dict):
        for k in ("listingDetail", "listing", "model"):
            if isinstance(cp.get(k), dict):
                candidates.append(cp[k])
    if not candidates:
        return None
    return max(candidates, key=lambda d: len(json.dumps(d, default=str)))


def parse_domain_listing(url: str, html: str) -> ParsedDomainListing | None:
    listing_id = _domain_extract_listing_id(url)
    if listing_id is None:
        return None

    nd = _domain_extract_next_data(html)
    listing = _domain_walk_listing(nd) if nd else None
    listing_kind = "sold" if "/sold-" in url or "sold" in url.lower() else "unknown"

    if not listing:
        return ParsedDomainListing(
            domain_listing_id=listing_id, listing_kind=listing_kind,
            display_address="", property_type=None, price_text=None,
            sold_price=None, sold_date=None, sale_method=None,
            bedrooms=None, bathrooms=None, car_spaces=None,
            land_area_m2=None, agency_name=None, agent_name=None,
            description=None, features=None, photos=None, raw_listing={},
        )

    addr = listing.get("address") or {}
    price_text = listing.get("price") or listing.get("priceText")
    description = listing.get("description") or listing.get("seoDescription")
    features = listing.get("features")
    if isinstance(features, dict):
        features = features.get("propertyFeatures") or features.get("listingFeatures")
    media = listing.get("media") or {}
    images = (media.get("images") or media.get("media") or []) if isinstance(media, dict) else []

    # Some Domain payloads put beds/baths under listingModel.features
    feat_block = listing.get("features") or listing.get("generalFeatures") or {}
    if not isinstance(feat_block, dict):
        feat_block = {}

    return ParsedDomainListing(
        domain_listing_id=listing_id,
        listing_kind=str(listing.get("listingType") or listing_kind)[:20],
        display_address=str(addr.get("street") or addr.get("displayAddress") or "")[:255],
        property_type=str(feat_block.get("propertyType") or feat_block.get("propertyTypeFormatted") or "")[:40] or None,
        price_text=str(price_text)[:160] if price_text else None,
        sold_price=_parse_price_text(price_text if isinstance(price_text, str) else None),
        sold_date=_parse_iso_date(listing.get("dateSold") or (listing.get("tags") or {}).get("date")),
        sale_method=None,
        bedrooms=_safe_int(feat_block.get("beds")),
        bathrooms=_safe_int(feat_block.get("baths")),
        car_spaces=_safe_int(feat_block.get("parking")),
        land_area_m2=_safe_int(feat_block.get("landSize")),
        agency_name=(str((listing.get("branding") or {}).get("brandName"))[:160] if (listing.get("branding") or {}).get("brandName") else None),
        agent_name=(str((listing.get("branding") or {}).get("agentNames"))[:160] if (listing.get("branding") or {}).get("agentNames") else None),
        description=str(description)[:65_000] if description else None,
        features=[str(f) for f in features] if isinstance(features, list) else None,
        photos=[{"url": img.get("url") or img.get("templatedUrl"),
                 "caption": img.get("caption")} for img in images][:60] if images else None,
        raw_listing=listing,
    )


def fetch_domain_listing(url: str) -> tuple[int, int | None, ParsedDomainListing | None, str | None]:
    """Plain HTTP — Domain has no Kasada wall.

    Idempotent: if we already have a row for this domain_listing_id, return it.
    """
    listing_id = _domain_extract_listing_id(url)
    if listing_id is not None:
        with session_scope() as s:
            existing = s.execute(
                select(DomainListing).where(
                    DomainListing.domain_listing_id == listing_id
                )
            ).scalar_one_or_none()
            if existing:
                return existing.raw_page_id, existing.id, None, None

    headers = {
        "User-Agent": _DEFAULT_UA,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
    }
    with httpx.Client(http2=True, follow_redirects=True, timeout=30.0) as client:
        resp = client.get(url, headers=headers)
    body = resp.text
    final_url = str(resp.url)

    raw_page_id = store_raw_page(
        source=DOMAIN_SOURCE, page_type=DOMAIN_PAGE_TYPE,
        url=final_url, body=body,
        http_status=resp.status_code,
        headers={k: v for k, v in resp.headers.items()},
    )

    if resp.status_code != 200:
        return raw_page_id, None, None, f"HTTP {resp.status_code}"

    parsed = parse_domain_listing(final_url, body)
    if not parsed:
        return raw_page_id, None, None, "couldn't extract listing id"

    now = datetime.utcnow()
    with session_scope() as s:
        dl = DomainListing(
            raw_page_id=raw_page_id,
            domain_property_id=None,
            domain_listing_id=parsed.domain_listing_id,
            url=final_url[:1024],
            listing_kind=parsed.listing_kind,
            listing_status=None,
            display_address=parsed.display_address,
            property_type=parsed.property_type,
            price_text=parsed.price_text,
            sold_price=parsed.sold_price,
            sold_date=parsed.sold_date,
            sale_method=parsed.sale_method,
            bedrooms=parsed.bedrooms,
            bathrooms=parsed.bathrooms,
            car_spaces=parsed.car_spaces,
            land_area_m2=parsed.land_area_m2,
            agency_name=parsed.agency_name,
            agent_name=parsed.agent_name,
            description=parsed.description,
            features_json=parsed.features,
            photos_json=parsed.photos,
            raw_listing_json=parsed.raw_listing,
            fetched_at=now, parsed_at=now,
        )
        s.add(dl)
        s.flush()
        return raw_page_id, dl.id, parsed, None
