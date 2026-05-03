from __future__ import annotations

from datetime import datetime, date

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    JSON,
    LargeBinary,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.mysql import MEDIUMBLOB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from listo.db import Base


class RawPage(Base):
    __tablename__ = "raw_pages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(40), nullable=False)
    page_type: Mapped[str] = mapped_column(String(40), nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    url_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    suburb: Mapped[str | None] = mapped_column(String(80))
    postcode: Mapped[str | None] = mapped_column(String(4))
    page_index: Mapped[int | None] = mapped_column(Integer)
    http_status: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    content_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    body_gz: Mapped[bytes] = mapped_column(MEDIUMBLOB().with_variant(LargeBinary, "sqlite"), nullable=False)
    headers_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    parsed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    parse_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ix_raw_source_type_fetched", "source", "page_type", "fetched_at"),
        Index("ix_raw_url_hash", "url_hash"),
        Index("ix_raw_content_hash", "content_hash"),
        Index("ix_raw_unparsed", "parsed_at", "source"),
    )


class Property(Base):
    __tablename__ = "properties"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    match_key: Mapped[str] = mapped_column(String(160), nullable=False)
    unit_number: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    street_number: Mapped[str] = mapped_column(String(16), nullable=False)
    street_name: Mapped[str] = mapped_column(String(120), nullable=False)
    street_norm: Mapped[str] = mapped_column(String(120), nullable=False)
    suburb: Mapped[str] = mapped_column(String(80), nullable=False)
    suburb_norm: Mapped[str] = mapped_column(String(80), nullable=False)
    postcode: Mapped[str] = mapped_column(String(4), nullable=False)
    state: Mapped[str] = mapped_column(String(3), nullable=False, default="QLD")
    property_type: Mapped[str | None] = mapped_column(String(40))
    lat: Mapped[float | None] = mapped_column()
    lng: Mapped[float | None] = mapped_column()
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)

    __table_args__ = (
        UniqueConstraint("match_key", "unit_number", name="uq_prop_full"),
        Index("ix_prop_match", "match_key"),
        Index("ix_prop_suburb_postcode", "suburb_norm", "postcode"),
    )


class Listing(Base):
    __tablename__ = "listings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    property_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("properties.id"), nullable=False
    )
    source: Mapped[str] = mapped_column(String(40), nullable=False)
    source_listing_id: Mapped[str] = mapped_column(String(32), nullable=False)
    raw_page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("raw_pages.id"), nullable=False
    )
    listing_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    price_text: Mapped[str | None] = mapped_column(String(120))
    price_min: Mapped[int | None] = mapped_column(Integer)
    price_max: Mapped[int | None] = mapped_column(Integer)
    beds: Mapped[int | None] = mapped_column(SmallInteger)
    baths: Mapped[int | None] = mapped_column(SmallInteger)
    parking: Mapped[int | None] = mapped_column(SmallInteger)
    property_type: Mapped[str | None] = mapped_column(String(40))
    land_size_m2: Mapped[int | None] = mapped_column(Integer)
    agent_name: Mapped[str | None] = mapped_column(String(160))
    agency_name: Mapped[str | None] = mapped_column(String(160))
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)

    property = relationship("Property")
    raw_page = relationship("RawPage")

    __table_args__ = (
        UniqueConstraint("source", "source_listing_id", name="uq_listing_source"),
        Index("ix_listing_property", "property_id"),
    )


class Sale(Base):
    __tablename__ = "sales"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    property_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("properties.id"), nullable=False
    )
    source: Mapped[str] = mapped_column(String(40), nullable=False)
    source_listing_id: Mapped[str | None] = mapped_column(String(32))
    raw_page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("raw_pages.id"), nullable=False
    )
    sold_date: Mapped[date | None] = mapped_column(Date)
    sold_price: Mapped[int | None] = mapped_column(Integer)
    sale_method: Mapped[str | None] = mapped_column(String(40))

    property = relationship("Property")
    raw_page = relationship("RawPage")

    __table_args__ = (
        UniqueConstraint(
            "property_id", "source", "source_listing_id", "sold_date", name="uq_sale"
        ),
        Index("ix_sale_property_date", "property_id", "sold_date"),
    )


class CouncilApplication(Base):
    __tablename__ = "council_applications"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    council_slug: Mapped[str] = mapped_column(String(40), nullable=False)
    vendor: Mapped[str] = mapped_column(String(40), nullable=False)
    application_id: Mapped[str] = mapped_column(String(60), nullable=False)
    application_url: Mapped[str | None] = mapped_column(String(1024))
    type_code: Mapped[str | None] = mapped_column(String(8))
    application_type: Mapped[str | None] = mapped_column(String(120))
    description: Mapped[str | None] = mapped_column(Text)
    approved_units: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str | None] = mapped_column(String(60))
    decision_outcome: Mapped[str | None] = mapped_column(String(60))
    decision_authority: Mapped[str | None] = mapped_column(String(120))
    lodged_date: Mapped[date | None] = mapped_column(Date)
    decision_date: Mapped[date | None] = mapped_column(Date)
    n_submissions: Mapped[int | None] = mapped_column(Integer)
    conditions_count: Mapped[int | None] = mapped_column(Integer)
    applicant_name: Mapped[str | None] = mapped_column(String(255))
    builder_name: Mapped[str | None] = mapped_column(String(255))
    architect_name: Mapped[str | None] = mapped_column(String(255))
    owner_name: Mapped[str | None] = mapped_column(String(255))
    internal_property_id: Mapped[str | None] = mapped_column(String(60))
    lot_on_plan: Mapped[str | None] = mapped_column(String(120))
    raw_address: Mapped[str | None] = mapped_column(String(500))
    street_address: Mapped[str | None] = mapped_column(String(255))
    suburb: Mapped[str | None] = mapped_column(String(120))
    postcode: Mapped[str | None] = mapped_column(String(4))
    state: Mapped[str | None] = mapped_column(String(3))
    match_key: Mapped[str | None] = mapped_column(String(255))
    raw_listing_row: Mapped[dict | None] = mapped_column(JSON)
    raw_detail_fields: Mapped[dict | None] = mapped_column(JSON)
    list_first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    detail_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    docs_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    summarised_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)

    __table_args__ = (
        UniqueConstraint("council_slug", "application_id", name="uq_council_app"),
        Index("ix_ca_match_key", "match_key"),
        Index("ix_ca_internal_property", "internal_property_id"),
        Index("ix_ca_lodged", "council_slug", "lodged_date"),
        Index("ix_ca_type", "type_code", "lodged_date"),
        Index("ix_ca_suburb", "suburb", "postcode"),
        Index("ix_ca_pending_detail", "detail_fetched_at", "council_slug"),
        Index("ix_ca_pending_docs", "docs_fetched_at", "council_slug"),
        Index("ix_ca_pending_summary", "summarised_at"),
    )


class CouncilApplicationDocument(Base):
    __tablename__ = "council_application_documents"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("council_applications.id", ondelete="CASCADE"), nullable=False
    )
    doc_oid: Mapped[str | None] = mapped_column(String(60))
    doc_type: Mapped[str | None] = mapped_column(String(120))
    title: Mapped[str | None] = mapped_column(String(500))
    source_url: Mapped[str | None] = mapped_column(String(1024))
    file_path: Mapped[str | None] = mapped_column(String(500))
    content_hash: Mapped[bytes | None] = mapped_column(LargeBinary(32))
    mime_type: Mapped[str | None] = mapped_column(String(80))
    file_size: Mapped[int | None] = mapped_column(BigInteger)
    page_count: Mapped[int | None] = mapped_column(Integer)
    extracted_text: Mapped[str | None] = mapped_column(Text)
    extraction_notes: Mapped[str | None] = mapped_column(Text)
    downloaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    # Date the council published this doc on the portal (from the
    # docs-portal table's "Date published" column). Sorting on this
    # gives submission order: application form first, decision notice
    # last. NULL for metadata-only rows where the scraper recorded the
    # doc inventory but didn't download bytes.
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))

    application = relationship("CouncilApplication")

    __table_args__ = (
        UniqueConstraint("application_id", "doc_oid", name="uq_cad_app_oid"),
        Index("ix_cad_app", "application_id"),
        Index("ix_cad_type", "doc_type"),
        Index("ix_cad_content_hash", "content_hash"),
    )


class CouncilRequest(Base):
    __tablename__ = "council_requests"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    council_slug: Mapped[str] = mapped_column(String(40), nullable=False)
    vendor: Mapped[str] = mapped_column(String(40), nullable=False)
    purpose: Mapped[str] = mapped_column(String(40), nullable=False)
    method: Mapped[str] = mapped_column(String(8), nullable=False)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    url_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    http_status: Mapped[int | None] = mapped_column(SmallInteger)
    elapsed_ms: Mapped[int | None] = mapped_column(Integer)
    bytes_received: Mapped[int | None] = mapped_column(BigInteger)
    content_hash: Mapped[bytes | None] = mapped_column(LargeBinary(32))
    attempt_index: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=1)
    raw_page_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("raw_pages.id", ondelete="SET NULL")
    )
    document_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("council_application_documents.id", ondelete="SET NULL")
    )
    application_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("council_applications.id", ondelete="SET NULL")
    )
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)

    __table_args__ = (
        Index("ix_creq_started", "council_slug", "started_at"),
        Index("ix_creq_url_hash", "url_hash"),
        Index("ix_creq_purpose", "purpose", "started_at"),
        Index("ix_creq_app", "application_id"),
    )


class CouncilScrapeWindow(Base):
    """One row per (council, backend, date-window) scrape attempt.

    Lets us see at a glance whether a given date range has been fully
    walked, what's currently running, and what failed and where.
    """
    __tablename__ = "council_scrape_windows"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    council_slug: Mapped[str] = mapped_column(String(40), nullable=False)
    vendor: Mapped[str] = mapped_column(String(40), nullable=False)
    backend_name: Mapped[str] = mapped_column(String(60), nullable=False)
    date_from: Mapped[date] = mapped_column(Date, nullable=False)
    date_to: Mapped[date] = mapped_column(Date, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    pages_walked: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    apps_yielded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    apps_with_docs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    files_downloaded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ix_csw_council_window", "council_slug", "date_from", "date_to"),
        Index("ix_csw_status", "council_slug", "status", "finished_at"),
    )


class MortgageRate(Base):
    __tablename__ = "mortgage_rates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    series_id: Mapped[str] = mapped_column(String(20), nullable=False)
    series_label: Mapped[str | None] = mapped_column(String(255))
    month: Mapped[date] = mapped_column(Date, nullable=False)
    rate_pct: Mapped[float] = mapped_column(nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="rba_f5")
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)

    __table_args__ = (
        UniqueConstraint("series_id", "month", name="uq_rate_series_month"),
        Index("ix_rate_month", "month"),
    )


class CrawlRun(Base):
    __tablename__ = "crawl_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(40), nullable=False)
    page_type: Mapped[str] = mapped_column(String(40), nullable=False)
    suburb: Mapped[str] = mapped_column(String(80), nullable=False)
    postcode: Mapped[str] = mapped_column(String(4), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    pages_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_page: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ix_runs_resume", "source", "page_type", "suburb", "postcode", "started_at"),
    )


# ---------------- property history (per-source PDP snapshots) ----------------


class DomainProperty(Base):
    __tablename__ = "domain_properties"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    raw_page_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("raw_pages.id"), nullable=False)
    property_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("properties.id"))

    domain_property_id: Mapped[str] = mapped_column(String(64), nullable=False)
    domain_apollo_id: Mapped[str] = mapped_column(String(255), nullable=False)
    url_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)

    display_address: Mapped[str] = mapped_column(String(255), nullable=False)
    unit_number: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    street_number: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    street_name: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    street_type: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    suburb: Mapped[str] = mapped_column(String(80), nullable=False)
    postcode: Mapped[str] = mapped_column(String(4), nullable=False)
    state: Mapped[str] = mapped_column(String(3), nullable=False)
    lat: Mapped[float | None] = mapped_column()
    lng: Mapped[float | None] = mapped_column()

    lot_number: Mapped[str | None] = mapped_column(String(20))
    plan_number: Mapped[str | None] = mapped_column(String(20))

    property_type: Mapped[str | None] = mapped_column(String(40))
    bedrooms: Mapped[int | None] = mapped_column(SmallInteger)
    bathrooms: Mapped[int | None] = mapped_column(SmallInteger)
    parking_spaces: Mapped[int | None] = mapped_column(SmallInteger)
    land_area_m2: Mapped[int | None] = mapped_column(Integer)
    internal_area_m2: Mapped[int | None] = mapped_column(Integer)

    valuation_low: Mapped[int | None] = mapped_column(Integer)
    valuation_mid: Mapped[int | None] = mapped_column(Integer)
    valuation_high: Mapped[int | None] = mapped_column(Integer)
    valuation_confidence: Mapped[str | None] = mapped_column(String(40))
    valuation_date: Mapped[date | None] = mapped_column(Date)
    rent_estimate_weekly: Mapped[int | None] = mapped_column(Integer)
    rent_yield_pct: Mapped[float | None] = mapped_column()

    raw_property_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    parsed_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)


class DomainSale(Base):
    __tablename__ = "domain_sales"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    domain_property_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("domain_properties.id"), nullable=False
    )
    property_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("properties.id"))
    raw_page_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("raw_pages.id"), nullable=False)

    event_date: Mapped[date | None] = mapped_column(Date)
    event_price: Mapped[int | None] = mapped_column(Integer)
    category: Mapped[str] = mapped_column(String(20), nullable=False)
    price_description: Mapped[str | None] = mapped_column(String(120))
    is_sold: Mapped[bool] = mapped_column(default=False)
    is_major_event: Mapped[bool] = mapped_column(default=False)
    days_on_market: Mapped[int | None] = mapped_column(Integer)
    agency_name: Mapped[str | None] = mapped_column(String(160))
    agency_profile_url: Mapped[str | None] = mapped_column(String(255))

    raw_event_json: Mapped[dict] = mapped_column(JSON, nullable=False)


class RealestateProperty(Base):
    __tablename__ = "realestate_properties"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    raw_page_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("raw_pages.id"), nullable=False)
    property_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("properties.id"))

    rea_property_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    url_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    pca_property_url: Mapped[str | None] = mapped_column(String(1024))

    display_address: Mapped[str] = mapped_column(String(255), nullable=False)
    unit_number: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    street_number: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    street_name: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    suburb: Mapped[str] = mapped_column(String(80), nullable=False)
    postcode: Mapped[str] = mapped_column(String(4), nullable=False)
    state: Mapped[str] = mapped_column(String(3), nullable=False)
    lat: Mapped[float | None] = mapped_column()
    lng: Mapped[float | None] = mapped_column()

    property_type: Mapped[str | None] = mapped_column(String(40))
    bedrooms: Mapped[int | None] = mapped_column(SmallInteger)
    bathrooms: Mapped[int | None] = mapped_column(SmallInteger)
    car_spaces: Mapped[int | None] = mapped_column(SmallInteger)
    land_area_m2: Mapped[int | None] = mapped_column(Integer)
    floor_area_m2: Mapped[int | None] = mapped_column(Integer)
    year_built: Mapped[int | None] = mapped_column(SmallInteger)

    status_label: Mapped[str | None] = mapped_column(String(40))
    market_status: Mapped[str | None] = mapped_column(String(40))

    valuation_low: Mapped[int | None] = mapped_column(Integer)
    valuation_mid: Mapped[int | None] = mapped_column(Integer)
    valuation_high: Mapped[int | None] = mapped_column(Integer)
    valuation_confidence: Mapped[str | None] = mapped_column(String(40))
    rent_estimate_weekly: Mapped[int | None] = mapped_column(Integer)
    rent_yield_pct: Mapped[float | None] = mapped_column()

    raw_property_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    parsed_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)


class RealestateSale(Base):
    __tablename__ = "realestate_sales"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    realestate_property_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("realestate_properties.id"), nullable=False
    )
    property_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("properties.id"))
    raw_page_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("raw_pages.id"), nullable=False)

    event_date: Mapped[date | None] = mapped_column(Date)
    event_price: Mapped[int | None] = mapped_column(Integer)
    price_text: Mapped[str | None] = mapped_column(String(120))
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    agency_name: Mapped[str | None] = mapped_column(String(160))
    listing_url: Mapped[str | None] = mapped_column(String(1024))

    raw_event_json: Mapped[dict] = mapped_column(JSON, nullable=False)


class RealestateListing(Base):
    __tablename__ = "realestate_listings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    raw_page_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("raw_pages.id"), nullable=False)
    realestate_property_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("realestate_properties.id")
    )

    rea_listing_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)

    listing_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    listing_status: Mapped[str | None] = mapped_column(String(40))
    display_address: Mapped[str] = mapped_column(String(255), nullable=False)
    property_type: Mapped[str | None] = mapped_column(String(40))

    price_text: Mapped[str | None] = mapped_column(String(160))
    sold_price: Mapped[int | None] = mapped_column(Integer)
    sold_date: Mapped[date | None] = mapped_column(Date)
    sale_method: Mapped[str | None] = mapped_column(String(40))

    bedrooms: Mapped[int | None] = mapped_column(SmallInteger)
    bathrooms: Mapped[int | None] = mapped_column(SmallInteger)
    car_spaces: Mapped[int | None] = mapped_column(SmallInteger)
    land_area_m2: Mapped[int | None] = mapped_column(Integer)
    floor_area_m2: Mapped[int | None] = mapped_column(Integer)

    agency_name: Mapped[str | None] = mapped_column(String(160))
    agent_name: Mapped[str | None] = mapped_column(String(160))

    description: Mapped[str | None] = mapped_column(Text)
    features_json: Mapped[dict | None] = mapped_column(JSON)
    photos_json: Mapped[dict | None] = mapped_column(JSON)

    raw_listing_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    parsed_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)


class DomainListing(Base):
    __tablename__ = "domain_listings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    raw_page_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("raw_pages.id"), nullable=False)
    domain_property_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("domain_properties.id")
    )

    domain_listing_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)

    listing_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    listing_status: Mapped[str | None] = mapped_column(String(40))
    display_address: Mapped[str] = mapped_column(String(255), nullable=False)
    property_type: Mapped[str | None] = mapped_column(String(40))

    price_text: Mapped[str | None] = mapped_column(String(160))
    sold_price: Mapped[int | None] = mapped_column(Integer)
    sold_date: Mapped[date | None] = mapped_column(Date)
    sale_method: Mapped[str | None] = mapped_column(String(40))

    bedrooms: Mapped[int | None] = mapped_column(SmallInteger)
    bathrooms: Mapped[int | None] = mapped_column(SmallInteger)
    car_spaces: Mapped[int | None] = mapped_column(SmallInteger)
    land_area_m2: Mapped[int | None] = mapped_column(Integer)

    agency_name: Mapped[str | None] = mapped_column(String(160))
    agent_name: Mapped[str | None] = mapped_column(String(160))

    description: Mapped[str | None] = mapped_column(Text)
    features_json: Mapped[dict | None] = mapped_column(JSON)
    photos_json: Mapped[dict | None] = mapped_column(JSON)

    raw_listing_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    parsed_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)


class DiscoveredUrl(Base):
    __tablename__ = "discovered_urls"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    search_address: Mapped[str] = mapped_column(String(255), nullable=False)
    search_query: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    url_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    url_kind: Mapped[str] = mapped_column(String(40), nullable=False)
    search_engine: Mapped[str] = mapped_column(String(20), nullable=False)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))


class PropertyScrapeAttempt(Base):
    """One row per (source, display_address). Records every direct-slug
    fetch we made against Domain or REA — including 404s — so the
    scrape-batch dedup logic can distinguish "we never tried" from
    "we tried and the source genuinely has no profile for this
    address". See alembic/versions/0024_property_scrape_attempts.py."""

    __tablename__ = "property_scrape_attempts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    display_address: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    http_status: Mapped[int | None] = mapped_column(SmallInteger)
    result: Mapped[str] = mapped_column(String(20), nullable=False)
    error_message: Mapped[str | None] = mapped_column(String(500))
    attempted_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)


# ---------------- DA summaries (Ollama-extracted) ----------------


class DaDocSummary(Base):
    __tablename__ = "da_doc_summaries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    document_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("council_application_documents.id"), nullable=False
    )
    application_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("council_applications.id"), nullable=False
    )
    doc_type: Mapped[str | None] = mapped_column(String(120))
    doc_position: Mapped[str] = mapped_column(String(10), nullable=False)
    tier: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=1)
    model: Mapped[str] = mapped_column(String(80), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(20), nullable=False)
    summarised_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)

    template_key: Mapped[str | None] = mapped_column(String(40))
    text_chars: Mapped[int | None] = mapped_column(Integer)
    text_sha256: Mapped[bytes | None] = mapped_column(LargeBinary(32))
    pages_used: Mapped[str | None] = mapped_column(String(60))
    extraction_method: Mapped[str] = mapped_column(String(20), nullable=False, default="pymupdf")
    extraction_notes: Mapped[str | None] = mapped_column(Text)

    applicant_name: Mapped[str | None] = mapped_column(String(255))
    applicant_acn: Mapped[str | None] = mapped_column(String(9))
    applicant_abn: Mapped[str | None] = mapped_column(String(11))
    applicant_entity_type: Mapped[str | None] = mapped_column(String(20))
    applicant_agent_name: Mapped[str | None] = mapped_column(String(255))
    builder_name: Mapped[str | None] = mapped_column(String(255))
    architect_name: Mapped[str | None] = mapped_column(String(255))
    owner_name: Mapped[str | None] = mapped_column(String(255))
    owner_acn: Mapped[str | None] = mapped_column(String(9))
    owner_abn: Mapped[str | None] = mapped_column(String(11))
    owner_entity_type: Mapped[str | None] = mapped_column(String(20))
    dwelling_count: Mapped[int | None] = mapped_column(SmallInteger)
    dwelling_kind: Mapped[str | None] = mapped_column(String(40))
    project_description: Mapped[str | None] = mapped_column(Text)
    lot_on_plan: Mapped[str | None] = mapped_column(String(120))
    street_address: Mapped[str | None] = mapped_column(String(255))
    confidence: Mapped[str | None] = mapped_column(String(10))

    raw_response_json: Mapped[dict] = mapped_column(JSON, nullable=False)


class DaSummary(Base):
    __tablename__ = "da_summaries"

    application_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("council_applications.id"), primary_key=True
    )
    applicant_name: Mapped[str | None] = mapped_column(String(255))
    applicant_acn: Mapped[str | None] = mapped_column(String(9))
    applicant_abn: Mapped[str | None] = mapped_column(String(11))
    applicant_entity_type: Mapped[str | None] = mapped_column(String(20))
    applicant_agent_name: Mapped[str | None] = mapped_column(String(255))
    builder_name: Mapped[str | None] = mapped_column(String(255))
    architect_name: Mapped[str | None] = mapped_column(String(255))
    owner_name: Mapped[str | None] = mapped_column(String(255))
    owner_acn: Mapped[str | None] = mapped_column(String(9))
    owner_abn: Mapped[str | None] = mapped_column(String(11))
    owner_entity_type: Mapped[str | None] = mapped_column(String(20))
    dwelling_count: Mapped[int | None] = mapped_column(SmallInteger)
    dwelling_kind: Mapped[str | None] = mapped_column(String(40))
    project_description: Mapped[str | None] = mapped_column(Text)
    lot_on_plan: Mapped[str | None] = mapped_column(String(120))
    street_address: Mapped[str | None] = mapped_column(String(255))
    source_doc_ids_json: Mapped[dict | None] = mapped_column(JSON)

    # FKs into companies — populated by aggregate.py
    applicant_company_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("companies.id"))
    applicant_agent_company_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("companies.id"))
    builder_company_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("companies.id"))
    architect_company_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("companies.id"))
    owner_company_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("companies.id"))

    n_docs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    n_docs_downloaded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_pages: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    n_information_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    n_amendments: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    n_specialist_reports: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    days_lodge_to_decide: Mapped[int | None] = mapped_column(Integer)
    first_doc_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    last_doc_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))

    n_docs_summarised: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="incomplete")
    aggregated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    acn: Mapped[str | None] = mapped_column(String(9))
    abn: Mapped[str | None] = mapped_column(String(11))
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    norm_name: Mapped[str] = mapped_column(String(255), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False, default="unknown")
    asic_status: Mapped[str | None] = mapped_column(String(20))
    asic_company_type: Mapped[str | None] = mapped_column(String(120))
    asic_locality: Mapped[str | None] = mapped_column(String(120))
    asic_regulator: Mapped[str | None] = mapped_column(String(80))
    asic_registration_date: Mapped[date | None] = mapped_column(Date)
    asic_next_review_date: Mapped[date | None] = mapped_column(Date)
    asic_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)


class ApplicationEntity(Base):
    """Many-to-many link between a DA and a `companies` row, with role + provenance.

    Lets a single project carry multiple owners (e.g. spouses on the
    same title), distinguish the applicant from their c/- agent, and
    record which document each entity claim came from. Stage-2 project
    synthesis reads all rows for an application and decides which to
    promote into the denormalised pointers on `da_summaries`.
    """
    __tablename__ = "application_entities"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("council_applications.id", ondelete="CASCADE"), nullable=False
    )
    company_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[str] = mapped_column(String(30), nullable=False)
    is_primary: Mapped[bool] = mapped_column(default=False)
    source_doc_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("council_application_documents.id", ondelete="SET NULL")
    )
    source_field: Mapped[str | None] = mapped_column(String(80))
    extractor: Mapped[str] = mapped_column(String(40), nullable=False)
    confidence: Mapped[str | None] = mapped_column(String(10))
    extracted_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "application_id", "company_id", "role", "source_doc_id", "extractor",
            name="uq_ae_dedup",
        ),
        Index("ix_ae_app", "application_id", "role"),
        Index("ix_ae_co", "company_id", "role"),
        Index("ix_ae_doc", "source_doc_id"),
    )


class PromptTemplate(Base):
    __tablename__ = "prompt_templates"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    prompt_version: Mapped[str] = mapped_column(String(20), nullable=False)
    template_key: Mapped[str] = mapped_column(String(40), nullable=False)
    system_prompt: Mapped[str] = mapped_column(Text, nullable=False)
    user_template: Mapped[str] = mapped_column(Text, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text)
    first_used_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)


class DocumentFeatures(Base):
    __tablename__ = "document_features"

    document_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("council_application_documents.id"), primary_key=True
    )
    analyzed_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    analyzer_version: Mapped[str] = mapped_column(String(20), nullable=False)
    mime_type: Mapped[str | None] = mapped_column(String(80))
    page_count: Mapped[int | None] = mapped_column(Integer)
    total_text_chars: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mean_chars_per_page: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    has_acroform: Mapped[bool] = mapped_column(default=False)
    n_text_widgets: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    n_text_widgets_filled: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    n_checkbox_widgets: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pdf_producer: Mapped[str | None] = mapped_column(String(255))
    pdf_creator: Mapped[str | None] = mapped_column(String(255))
    pdf_format: Mapped[str | None] = mapped_column(String(40))
    treatment: Mapped[str] = mapped_column(String(40), nullable=False)
    extraction_notes: Mapped[str | None] = mapped_column(Text)


class DaBuildFeatures(Base):
    """Per-chunk physical / build-cost extractions (build-features lane).

    One row per (document_id, prompt_version, template_key, chunk_index).
    Chunked so 80-page specialist reports get split into ~5-page windows.
    Aggregator merges chunks → per-document → per-application.
    """
    __tablename__ = "da_build_features"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("council_applications.id"), nullable=False
    )
    document_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("council_application_documents.id"), nullable=False
    )
    doc_type: Mapped[str | None] = mapped_column(String(120))
    prompt_version: Mapped[str] = mapped_column(String(20), nullable=False)
    template_key: Mapped[str] = mapped_column(String(40), nullable=False)
    model: Mapped[str] = mapped_column(String(80), nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    page_start: Mapped[int] = mapped_column(Integer, nullable=False)
    page_end: Mapped[int] = mapped_column(Integer, nullable=False)
    extracted_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    extraction_method: Mapped[str] = mapped_column(String(20), nullable=False)
    text_chars: Mapped[int | None] = mapped_column(Integer)

    gfa_m2: Mapped[int | None] = mapped_column(Integer)
    site_area_m2: Mapped[int | None] = mapped_column(Integer)
    internal_area_m2: Mapped[int | None] = mapped_column(Integer)
    external_area_m2: Mapped[int | None] = mapped_column(Integer)
    levels: Mapped[int | None] = mapped_column(SmallInteger)
    has_basement: Mapped[bool | None] = mapped_column()
    garage_spaces: Mapped[int | None] = mapped_column(SmallInteger)
    bedrooms: Mapped[int | None] = mapped_column(SmallInteger)
    bathrooms: Mapped[int | None] = mapped_column(SmallInteger)

    materials_walls: Mapped[str | None] = mapped_column(String(300))
    materials_roof: Mapped[str | None] = mapped_column(String(200))
    materials_floor: Mapped[str | None] = mapped_column(String(200))
    fittings_quality: Mapped[str | None] = mapped_column(String(20))
    fittings_notes: Mapped[str | None] = mapped_column(String(400))

    landscaping_summary: Mapped[str | None] = mapped_column(String(400))
    plant_species_json: Mapped[list | None] = mapped_column(JSON)
    has_pool: Mapped[bool | None] = mapped_column()

    confidence: Mapped[str | None] = mapped_column(String(10))
    notes: Mapped[str | None] = mapped_column(String(300))
    raw_response_json: Mapped[dict] = mapped_column(JSON, nullable=False)


class BusinessLink(Base):
    __tablename__ = "business_links"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    business_name: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    business_role: Mapped[str] = mapped_column(String(20), nullable=False)
    url: Mapped[str | None] = mapped_column(String(1024))
    url_kind: Mapped[str | None] = mapped_column(String(20))
    search_query: Mapped[str | None] = mapped_column(String(255))
    search_engine: Mapped[str] = mapped_column(String(20), nullable=False, default="google")
    confidence: Mapped[str | None] = mapped_column(String(10))
    candidates_json: Mapped[dict | None] = mapped_column(JSON)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
