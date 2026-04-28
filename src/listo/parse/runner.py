from __future__ import annotations

import gzip
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from sqlalchemy import select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import Session

from listo.address import normalize_address
from listo.db import session_scope
from listo.models import Listing, Property, RawPage, Sale
from listo.parse import domain as domain_parse
from listo.parse import realestate as realestate_parse
from listo.parse.realestate import ParsedPage, RawListing


@dataclass
class RunStats:
    raw_pages_processed: int = 0
    listings_upserted: int = 0
    sales_upserted: int = 0
    properties_upserted: int = 0
    errors: int = 0


def _decompress(body_gz: bytes) -> str:
    return gzip.decompress(body_gz).decode("utf-8", errors="replace")


def _parse_html(source: str, html: str) -> ParsedPage:
    if source == "realestate":
        return realestate_parse.parse(html)
    if source == "domain":
        return domain_parse.parse(html)
    raise ValueError(f"unknown source: {source}")


def _upsert_property(s: Session, raw: RawListing, now: datetime) -> int:
    """Insert or update a property; return its id."""
    norm = normalize_address(raw.full_address, raw.suburb, raw.postcode)

    dialect_name = s.bind.dialect.name
    insert_fn = mysql_insert if dialect_name == "mysql" else sqlite_insert

    payload = {
        "match_key": norm.match_key,
        "unit_number": norm.unit_number,
        "street_number": norm.street_number,
        "street_name": norm.street_name,
        "street_norm": norm.street_norm,
        "suburb": norm.suburb,
        "suburb_norm": norm.suburb_norm,
        "postcode": norm.postcode,
        "state": raw.state or "QLD",
        "property_type": raw.property_type,
        "first_seen_at": now,
    }

    stmt = insert_fn(Property).values(**payload)
    if dialect_name == "mysql":
        stmt = stmt.on_duplicate_key_update(
            street_number=stmt.inserted.street_number,
            street_name=stmt.inserted.street_name,
            street_norm=stmt.inserted.street_norm,
            suburb=stmt.inserted.suburb,
            suburb_norm=stmt.inserted.suburb_norm,
            postcode=stmt.inserted.postcode,
            state=stmt.inserted.state,
            property_type=stmt.inserted.property_type,
        )
    else:  # sqlite (tests)
        stmt = stmt.on_conflict_do_update(
            index_elements=["match_key", "unit_number"],
            set_=dict(
                street_number=stmt.excluded.street_number,
                street_name=stmt.excluded.street_name,
                street_norm=stmt.excluded.street_norm,
                suburb=stmt.excluded.suburb,
                suburb_norm=stmt.excluded.suburb_norm,
                postcode=stmt.excluded.postcode,
                state=stmt.excluded.state,
                property_type=stmt.excluded.property_type,
            ),
        )
    s.execute(stmt)

    row = s.execute(
        select(Property.id).where(
            Property.match_key == norm.match_key,
            Property.unit_number == norm.unit_number,
        )
    ).first()
    if not row:
        raise RuntimeError(f"property upsert succeeded but row not found: {norm.match_key!r}")
    return row[0]


def _upsert_listing(
    s: Session, raw: RawListing, property_id: int, raw_page_id: int, source: str, now: datetime
) -> None:
    dialect_name = s.bind.dialect.name
    insert_fn = mysql_insert if dialect_name == "mysql" else sqlite_insert

    payload = {
        "property_id": property_id,
        "source": source,
        "source_listing_id": raw.source_listing_id,
        "raw_page_id": raw_page_id,
        "listing_kind": raw.listing_kind,
        "status": raw.status,
        "price_text": raw.price_text,
        "price_min": raw.price_min,
        "price_max": raw.price_max,
        "beds": raw.beds,
        "baths": raw.baths,
        "parking": raw.parking,
        "property_type": raw.property_type,
        "land_size_m2": raw.land_size_m2,
        "agent_name": raw.agent_name,
        "agency_name": raw.agency_name,
        "url": raw.url,
        "first_seen_at": now,
        "last_seen_at": now,
    }

    stmt = insert_fn(Listing).values(**payload)
    update_cols = {
        "raw_page_id": payload["raw_page_id"],
        "listing_kind": payload["listing_kind"],
        "status": payload["status"],
        "price_text": payload["price_text"],
        "price_min": payload["price_min"],
        "price_max": payload["price_max"],
        "beds": payload["beds"],
        "baths": payload["baths"],
        "parking": payload["parking"],
        "property_type": payload["property_type"],
        "land_size_m2": payload["land_size_m2"],
        "agent_name": payload["agent_name"],
        "agency_name": payload["agency_name"],
        "url": payload["url"],
        "last_seen_at": now,
    }
    if dialect_name == "mysql":
        stmt = stmt.on_duplicate_key_update(**update_cols)
    else:
        stmt = stmt.on_conflict_do_update(
            index_elements=["source", "source_listing_id"], set_=update_cols
        )
    s.execute(stmt)


def _upsert_sale(
    s: Session, raw: RawListing, property_id: int, raw_page_id: int, source: str
) -> None:
    if raw.listing_kind != "sold":
        return
    if not raw.sold_date:
        return  # no usable sale record without a date

    dialect_name = s.bind.dialect.name
    insert_fn = mysql_insert if dialect_name == "mysql" else sqlite_insert

    payload = {
        "property_id": property_id,
        "source": source,
        "source_listing_id": raw.source_listing_id,
        "raw_page_id": raw_page_id,
        "sold_date": raw.sold_date,
        "sold_price": raw.sold_price,
        "sale_method": raw.sale_method,
    }
    stmt = insert_fn(Sale).values(**payload)
    update_cols = {
        "raw_page_id": payload["raw_page_id"],
        "sold_price": payload["sold_price"],
        "sale_method": payload["sale_method"],
    }
    if dialect_name == "mysql":
        stmt = stmt.on_duplicate_key_update(**update_cols)
    else:
        stmt = stmt.on_conflict_do_update(
            index_elements=["property_id", "source", "source_listing_id", "sold_date"],
            set_=update_cols,
        )
    s.execute(stmt)


def _process_one_raw(
    s: Session, raw_page: RawPage, stats: RunStats
) -> None:
    html = _decompress(raw_page.body_gz)
    parsed = _parse_html(raw_page.source, html)
    now = datetime.utcnow()
    for raw in parsed.listings:
        prop_id = _upsert_property(s, raw, now)
        _upsert_listing(s, raw, prop_id, raw_page.id, raw_page.source, now)
        _upsert_sale(s, raw, prop_id, raw_page.id, raw_page.source)
        stats.listings_upserted += 1
        if raw.listing_kind == "sold" and raw.sold_date:
            stats.sales_upserted += 1
        stats.properties_upserted += 1
    raw_page.parsed_at = now
    raw_page.parse_error = None


def parse_unparsed(source: str | None = None, limit: int | None = None) -> RunStats:
    """Iterate raw_pages where parsed_at IS NULL, parse, and upsert downstream rows."""
    stats = RunStats()
    with session_scope() as s:
        q = select(RawPage).where(RawPage.parsed_at.is_(None))
        if source:
            q = q.where(RawPage.source == source)
        q = q.order_by(RawPage.id.asc())
        if limit:
            q = q.limit(limit)
        for raw_page in s.scalars(q):
            try:
                _process_one_raw(s, raw_page, stats)
                stats.raw_pages_processed += 1
                s.commit()  # commit per page so partial progress survives errors
            except Exception as e:
                s.rollback()
                stats.errors += 1
                # Re-fetch to mark the error
                fresh = s.get(RawPage, raw_page.id)
                if fresh:
                    fresh.parse_error = str(e)[:500]
                    s.commit()
    return stats


def reparse_all(source: str | None = None) -> RunStats:
    """Force re-parse: clear parsed_at on matching pages, then call parse_unparsed."""
    with session_scope() as s:
        stmt = "UPDATE raw_pages SET parsed_at = NULL, parse_error = NULL"
        if source:
            stmt += f" WHERE source = '{source}'"
        s.execute(text(stmt))
    return parse_unparsed(source=source)
