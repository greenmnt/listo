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


SOURCE_ENUM = Enum("realestate", "domain", name="source_enum")
PAGE_TYPE_ENUM = Enum(
    "search_sold", "search_buy", "search_rent", "listing", name="page_type_enum"
)
LISTING_KIND_ENUM = Enum("buy", "rent", "sold", name="listing_kind_enum")
LISTING_STATUS_ENUM = Enum(
    "active", "sold", "withdrawn", "unknown", name="listing_status_enum"
)
RUN_STATUS_ENUM = Enum(
    "running", "done", "failed", "partial", name="run_status_enum"
)


class RawPage(Base):
    __tablename__ = "raw_pages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(SOURCE_ENUM, nullable=False)
    page_type: Mapped[str] = mapped_column(PAGE_TYPE_ENUM, nullable=False)
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
    source: Mapped[str] = mapped_column(SOURCE_ENUM, nullable=False)
    source_listing_id: Mapped[str] = mapped_column(String(32), nullable=False)
    raw_page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("raw_pages.id"), nullable=False
    )
    listing_kind: Mapped[str] = mapped_column(LISTING_KIND_ENUM, nullable=False)
    status: Mapped[str] = mapped_column(LISTING_STATUS_ENUM, nullable=False)
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
    source: Mapped[str] = mapped_column(SOURCE_ENUM, nullable=False)
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


class DevApplication(Base):
    __tablename__ = "dev_applications"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    council_slug: Mapped[str] = mapped_column(String(40), nullable=False)
    application_id: Mapped[str] = mapped_column(String(40), nullable=False)
    application_type: Mapped[str | None] = mapped_column(String(80))
    type_code: Mapped[str | None] = mapped_column(String(8))
    description: Mapped[str | None] = mapped_column(Text)
    approved_units: Mapped[int | None] = mapped_column(Integer)
    internal_property_id: Mapped[str | None] = mapped_column(String(40))
    lot_on_plan: Mapped[str | None] = mapped_column(String(80))
    raw_address: Mapped[str | None] = mapped_column(String(255))
    match_key: Mapped[str | None] = mapped_column(String(160))
    suburb: Mapped[str | None] = mapped_column(String(80))
    postcode: Mapped[str | None] = mapped_column(String(4))
    state: Mapped[str | None] = mapped_column(String(3))
    status: Mapped[str | None] = mapped_column(String(40))
    decision_outcome: Mapped[str | None] = mapped_column(String(40))
    decision_authority: Mapped[str | None] = mapped_column(String(80))
    lodged_date: Mapped[date | None] = mapped_column(Date)
    decision_date: Mapped[date | None] = mapped_column(Date)
    n_submissions: Mapped[int | None] = mapped_column(Integer)
    conditions_count: Mapped[int | None] = mapped_column(Integer)
    applicant_name: Mapped[str | None] = mapped_column(String(160))
    builder_name: Mapped[str | None] = mapped_column(String(160))
    architect_name: Mapped[str | None] = mapped_column(String(160))
    source_url: Mapped[str | None] = mapped_column(String(500))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)

    __table_args__ = (
        UniqueConstraint("council_slug", "application_id", name="uq_da_council_app"),
        Index("ix_da_match_key", "match_key"),
        Index("ix_da_internal_property", "internal_property_id"),
        Index("ix_da_lodged", "council_slug", "lodged_date"),
        Index("ix_da_type", "type_code", "lodged_date"),
    )


class DaDocument(Base):
    __tablename__ = "da_documents"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("dev_applications.id", ondelete="CASCADE"), nullable=False
    )
    doc_type: Mapped[str | None] = mapped_column(String(80))
    doc_oid: Mapped[str | None] = mapped_column(String(40))
    title: Mapped[str | None] = mapped_column(String(255))
    source_url: Mapped[str | None] = mapped_column(String(500))
    file_path: Mapped[str | None] = mapped_column(String(500))
    content_hash: Mapped[bytes | None] = mapped_column(LargeBinary(32))
    mime_type: Mapped[str | None] = mapped_column(String(60))
    file_size: Mapped[int | None] = mapped_column(BigInteger)
    page_count: Mapped[int | None] = mapped_column(Integer)
    extracted_text: Mapped[str | None] = mapped_column(Text)
    extraction_notes: Mapped[str | None] = mapped_column(Text)
    downloaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))

    application = relationship("DevApplication")

    __table_args__ = (
        Index("ix_doc_app", "application_id"),
        Index("ix_doc_type", "doc_type"),
    )


class DaFlag(Base):
    __tablename__ = "da_flags"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("dev_applications.id", ondelete="CASCADE"), nullable=False
    )
    flag_kind: Mapped[str] = mapped_column(String(40), nullable=False)
    severity: Mapped[str] = mapped_column(
        Enum("info", "warn", "high", name="flag_severity_enum"), nullable=False, default="warn"
    )
    detail: Mapped[str | None] = mapped_column(String(255))

    application = relationship("DevApplication")

    __table_args__ = (
        UniqueConstraint("application_id", "flag_kind", name="uq_flag"),
        Index("ix_flag_kind", "flag_kind"),
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
    source: Mapped[str] = mapped_column(SOURCE_ENUM, nullable=False)
    page_type: Mapped[str] = mapped_column(String(20), nullable=False)
    suburb: Mapped[str] = mapped_column(String(80), nullable=False)
    postcode: Mapped[str] = mapped_column(String(4), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    pages_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_page: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(RUN_STATUS_ENUM, nullable=False)
    error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ix_runs_resume", "source", "page_type", "suburb", "postcode", "started_at"),
    )
