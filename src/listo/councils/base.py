"""Vendor-agnostic council DA scraper protocol + shared dataclasses.

A scraper is responsible for *extraction only*: it talks to one council
portal vendor (Infor ePathway, TechOne eTrack, TechOne T1Cloud, …) and
yields/returns plain dataclasses. Persistence lives in
`listo.councils.orchestrator`, which drives the scraper through phases:

    list  →  detail  →  documents  →  download

Each phase emits records the orchestrator upserts into council_applications
and council_application_documents, plus a request log row in
council_requests for every HTTP fetch.

Scrapers receive a `RequestSink` callback so they can record each fetch
without owning DB state. The sink writes raw HTML bodies into raw_pages
(returning a raw_page_id) and binary downloads onto disk + a row in
council_application_documents (returning a document_id).
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator, Protocol


def url_hash(url: str) -> bytes:
    return hashlib.sha256(url.encode("utf-8")).digest()


def content_hash(body: bytes) -> bytes:
    return hashlib.sha256(body).digest()


@dataclass
class FetchRecord:
    """Metadata for one HTTP request the scraper made.

    The orchestrator uses this to write a council_requests row. If `body`
    is set and is HTML, the orchestrator also writes a raw_pages row and
    populates `raw_page_id` on the resulting council_requests row.
    """
    purpose: str                         # 'list' | 'detail' | 'docs_index' | 'doc_download'
    method: str                          # 'GET' | 'POST'
    url: str
    started_at: datetime
    elapsed_ms: int | None = None
    http_status: int | None = None
    bytes_received: int | None = None
    body: bytes | None = None            # raw HTML when applicable; orchestrator decides what to store
    body_is_html: bool = False
    content_type: str | None = None
    error: str | None = None
    attempt_index: int = 1
    application_id: int | None = None    # FK to council_applications, when known


class RequestSink(Protocol):
    """Scraper → orchestrator hook. Called once per HTTP fetch."""
    def record(self, fetch: FetchRecord) -> int | None: ...


@dataclass
class DaListingRow:
    """One row from a search-results table — what the scraper sees before
    drilling into a detail page.

    For vendors where the listing-page link is a javascript postback
    (Infor ePathway, eTrack), the scraper resolves the detail page
    inline during list phase and attaches the result to `inline_detail`.
    Vendors that expose stable detail URLs in the listing page can leave
    `inline_detail=None` and let the orchestrator's detail phase fetch
    them lazily.
    """
    council_slug: str
    vendor: str
    application_id: str                  # 'MCU/2025/64'
    application_url: str | None = None
    type_code: str | None = None
    application_type: str | None = None
    lodged_date: date | None = None
    raw_address: str | None = None
    street_address: str | None = None
    suburb: str | None = None
    postcode: str | None = None
    state: str | None = None
    lot_on_plan: str | None = None
    status: str | None = None
    raw_row: dict[str, Any] = field(default_factory=dict)
    inline_detail: "DaDetailRecord | None" = None
    # Vendors that download all documents during the list walk (so each
    # yield = one fully-complete app) populate this. Empty list means
    # "we looked, there were no documents". None means "we didn't try —
    # the docs phase will pick this up later".
    inline_documents: "list[DownloadedDocument] | None" = None


@dataclass
class DaDetailRecord:
    """One DA after the detail page has been fetched + parsed."""
    council_slug: str
    vendor: str
    application_id: str
    application_url: str | None = None
    application_type: str | None = None
    type_code: str | None = None
    description: str | None = None
    status: str | None = None
    decision_outcome: str | None = None
    decision_authority: str | None = None
    lodged_date: date | None = None
    decision_date: date | None = None
    n_submissions: int | None = None
    conditions_count: int | None = None
    applicant_name: str | None = None
    builder_name: str | None = None
    architect_name: str | None = None
    owner_name: str | None = None
    internal_property_id: str | None = None
    lot_on_plan: str | None = None
    raw_address: str | None = None
    street_address: str | None = None
    suburb: str | None = None
    postcode: str | None = None
    state: str | None = None
    raw_fields: dict[str, Any] = field(default_factory=dict)


@dataclass
class DaDocumentRef:
    """A document link discovered on a detail/docs-index page, before
    download. The orchestrator pairs this with download() to fetch bytes."""
    doc_oid: str | None                  # portal-stable id when available
    title: str | None = None
    doc_type: str | None = None
    source_url: str | None = None
    size_text: str | None = None         # '1.89Mb' as displayed; we also store file_size after download
    published_at: datetime | None = None # when the council made it public


@dataclass
class DownloadedDocument:
    """One document persisted to council_application_documents.

    When file_path is None, this is a *metadata-only* record — we
    indexed the document on the portal but didn't download the bytes.
    file_size may still be populated from the size_text on the portal
    listing as a best-effort estimate. content_hash / page_count /
    mime_type are only set when bytes were actually downloaded.
    """
    doc_oid: str | None
    title: str | None
    doc_type: str | None
    source_url: str | None
    file_path: str | None
    file_size: int | None
    mime_type: str | None
    content_hash: bytes | None
    page_count: int | None
    published_at: datetime | None = None


class CouncilScraper(Protocol):
    """One implementation per (vendor, council) — typically vendor-shaped
    with a council-specific config baked in via the registry.
    """
    council_slug: str
    vendor: str

    def iter_listings(
        self,
        *,
        date_from: date,
        date_to: date,
        sink: RequestSink,
        skip_application_ids: set[str] | None = None,
        allowed_type_codes: set[str] | None = None,
    ) -> Iterator[DaListingRow]:
        """Walk the search results for the given lodgement-date range.

        Pagination, retries, and minimum delays are the scraper's
        responsibility. Yields one row per DA found. May yield duplicates
        across pages if the upstream site is buggy — the orchestrator
        dedupes on (council_slug, application_id).

        skip_application_ids: applications already fully processed on a
        previous run. Scrapers should yield a lightweight DaListingRow
        for matches without re-doing detail/docs work, so resume after
        a crash or session-expiry is fast.

        allowed_type_codes: when set, scrapers must still yield every
        listing row in the date window so list-phase coverage stays
        complete, but should skip the detail+docs inline fetch for any
        application whose type code falls outside the set. The
        orchestrator's detail/docs phases enforce the same filter, so a
        non-residential row gets persisted as a bare listing without
        ever costing the heavy fetch work.
        """
        ...

    def fetch_detail(
        self,
        listing: DaListingRow,
        sink: RequestSink,
    ) -> DaDetailRecord:
        """Fetch the detail page for one application."""
        ...

    def list_documents(
        self,
        detail: DaDetailRecord,
        sink: RequestSink,
    ) -> list[DaDocumentRef]:
        """Enumerate all documents attached to one application."""
        ...

    def download(
        self,
        doc: DaDocumentRef,
        target_dir: Path,
        sink: RequestSink,
    ) -> DownloadedDocument:
        """Download one document into `target_dir`, return file metadata."""
        ...
