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
