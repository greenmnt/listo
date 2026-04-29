"""Drive a CouncilScraper through the list → detail → docs phases and
persist results.

Phase 1 (list):
  Walk every search-results page in the date window. For each row,
  upsert a council_applications stub (raw_listing_row, list_first_seen_at).

Phase 2 (detail):
  For each council_applications row in the window with detail_fetched_at
  IS NULL, fetch the detail page and update the structured columns +
  raw_detail_fields. Marks detail_fetched_at.

Phase 3 (docs):
  For each application with docs_fetched_at IS NULL, list documents and
  download each one. Marks docs_fetched_at.

All HTTP fetches go through DbRequestSink which writes raw_pages +
council_requests as side effects.
"""
from __future__ import annotations

import gzip
import logging
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert

from listo.councils.base import (
    CouncilScraper,
    DaListingRow,
    FetchRecord,
    RequestSink,
    content_hash,
    url_hash,
)
from listo.councils.registry import CouncilBackend, CouncilDef
from listo.db import session_scope
from listo.models import (
    CouncilApplication,
    CouncilApplicationDocument,
    CouncilRequest,
    RawPage,
)


logger = logging.getLogger(__name__)

DEFAULT_DOC_DIR = Path("data/da_docs")


# ---------------- request sink ----------------


class DbRequestSink(RequestSink):
    """Concrete RequestSink that writes to raw_pages + council_requests.

    Per the storage rule: HTML responses go into raw_pages (gzip'd
    body); every fetch — HTML or not, success or not — gets a
    council_requests row.
    """
    def __init__(self, *, council_slug: str, vendor: str):
        self.council_slug = council_slug
        self.vendor = vendor

    def record(self, fetch: FetchRecord) -> int | None:
        raw_page_id: int | None = None
        ch: bytes | None = None
        if fetch.body_is_html and fetch.body:
            ch = content_hash(fetch.body)
            raw_page_id = self._upsert_raw_page(fetch, content_hash_=ch)
        with session_scope() as s:
            req = CouncilRequest(
                council_slug=self.council_slug,
                vendor=self.vendor,
                purpose=fetch.purpose,
                method=fetch.method,
                url=fetch.url[:2048],
                url_hash=url_hash(fetch.url),
                http_status=fetch.http_status,
                elapsed_ms=fetch.elapsed_ms,
                bytes_received=fetch.bytes_received,
                content_hash=ch,
                attempt_index=fetch.attempt_index,
                raw_page_id=raw_page_id,
                application_id=fetch.application_id,
                error=fetch.error,
                started_at=fetch.started_at,
            )
            s.add(req)
        return raw_page_id

    def _upsert_raw_page(self, fetch: FetchRecord, *, content_hash_: bytes) -> int:
        """Insert a raw_pages row if a row with the same url_hash +
        content_hash doesn't already exist; return the resulting id.
        """
        uh = url_hash(fetch.url)
        body_gz = gzip.compress(fetch.body or b"")
        with session_scope() as s:
            existing = s.execute(
                select(RawPage.id)
                .where(RawPage.url_hash == uh)
                .where(RawPage.content_hash == content_hash_)
                .limit(1)
            ).scalar_one_or_none()
            if existing:
                return existing
            rp = RawPage(
                source=f"council_{self.council_slug}",
                page_type=fetch.purpose,                # 'list' | 'detail' | 'docs_index'
                url=fetch.url[:1024],
                url_hash=uh,
                http_status=fetch.http_status or 0,
                fetched_at=fetch.started_at,
                content_hash=content_hash_,
                body_gz=body_gz,
                headers_json={"content_type": fetch.content_type} if fetch.content_type else {},
            )
            s.add(rp)
            s.flush()
            return rp.id


# ---------------- upsert helpers ----------------


def _serialise_row(row: dict) -> dict:
    """Pop unhashable values (none expected in our row dicts) and ensure
    JSON-friendly. Right now this is a passthrough."""
    return {k: v for k, v in row.items() if v is not None}


def upsert_listing(row: DaListingRow) -> int:
    """Insert or update a council_applications row from a listing-table
    row. Returns the application's id."""
    now = datetime.utcnow()
    payload = dict(
        council_slug=row.council_slug,
        vendor=row.vendor,
        application_id=row.application_id,
        application_url=row.application_url,
        type_code=row.type_code,
        application_type=row.application_type,
        lodged_date=row.lodged_date,
        raw_address=row.raw_address,
        street_address=row.street_address,
        suburb=row.suburb,
        postcode=row.postcode,
        state=row.state,
        status=row.status,
        lot_on_plan=row.lot_on_plan,
        raw_listing_row=_serialise_row(row.raw_row),
        list_first_seen_at=now,
        last_seen_at=now,
    )
    with session_scope() as s:
        stmt = mysql_insert(CouncilApplication).values(**payload)
        # On duplicate (council_slug, application_id): update last_seen_at
        # and any structured columns that may have changed; preserve the
        # original list_first_seen_at and the per-stage timestamps.
        stmt = stmt.on_duplicate_key_update(
            application_url=stmt.inserted.application_url,
            type_code=stmt.inserted.type_code,
            application_type=stmt.inserted.application_type,
            lodged_date=stmt.inserted.lodged_date,
            raw_address=stmt.inserted.raw_address,
            street_address=stmt.inserted.street_address,
            suburb=stmt.inserted.suburb,
            postcode=stmt.inserted.postcode,
            state=stmt.inserted.state,
            status=stmt.inserted.status,
            lot_on_plan=stmt.inserted.lot_on_plan,
            raw_listing_row=stmt.inserted.raw_listing_row,
            last_seen_at=stmt.inserted.last_seen_at,
        )
        s.execute(stmt)
        # Fetch the id (executemany insert+update doesn't reliably return it)
        app_id = s.execute(
            select(CouncilApplication.id)
            .where(CouncilApplication.council_slug == row.council_slug)
            .where(CouncilApplication.application_id == row.application_id)
        ).scalar_one()
        return app_id


def upsert_detail(application_pk: int, detail) -> None:
    """Update the council_applications row with detail-page fields."""
    now = datetime.utcnow()
    with session_scope() as s:
        s.execute(
            update(CouncilApplication)
            .where(CouncilApplication.id == application_pk)
            .values(
                application_url=detail.application_url,
                application_type=detail.application_type or CouncilApplication.application_type,
                type_code=detail.type_code or CouncilApplication.type_code,
                description=detail.description,
                approved_units=_extract_units(detail.description),
                status=detail.status or CouncilApplication.status,
                decision_outcome=detail.decision_outcome,
                decision_authority=detail.decision_authority,
                lodged_date=detail.lodged_date or CouncilApplication.lodged_date,
                decision_date=detail.decision_date,
                n_submissions=detail.n_submissions,
                conditions_count=detail.conditions_count,
                applicant_name=detail.applicant_name,
                builder_name=detail.builder_name,
                architect_name=detail.architect_name,
                owner_name=detail.owner_name,
                internal_property_id=detail.internal_property_id,
                lot_on_plan=detail.lot_on_plan,
                raw_address=detail.raw_address or CouncilApplication.raw_address,
                street_address=detail.street_address or CouncilApplication.street_address,
                suburb=detail.suburb or CouncilApplication.suburb,
                postcode=detail.postcode or CouncilApplication.postcode,
                state=detail.state or CouncilApplication.state,
                raw_detail_fields=detail.raw_fields,
                detail_fetched_at=now,
                last_seen_at=now,
            )
        )


def upsert_document(application_pk: int, dl) -> int:
    """Insert/update a council_application_documents row. Returns id.

    Handles both downloaded and metadata-only rows. A row is considered
    metadata-only when dl.file_path is None — in that case downloaded_at
    stays NULL so callers can distinguish 'we have the bytes' from 'we
    indexed the doc but didn't fetch it'. Re-running the same doc with
    a populated DownloadedDocument later will UPDATE the row to fill in
    file_path / content_hash / downloaded_at without losing the
    original published_at.
    """
    now = datetime.utcnow() if dl.file_path else None
    payload = dict(
        application_id=application_pk,
        doc_oid=dl.doc_oid,
        doc_type=dl.doc_type,
        title=dl.title,
        source_url=dl.source_url,
        file_path=dl.file_path,
        content_hash=dl.content_hash,
        mime_type=dl.mime_type,
        file_size=dl.file_size,
        page_count=dl.page_count,
        downloaded_at=now,
        published_at=dl.published_at,
    )
    with session_scope() as s:
        stmt = mysql_insert(CouncilApplicationDocument).values(**payload)
        stmt = stmt.on_duplicate_key_update(
            doc_type=stmt.inserted.doc_type,
            title=stmt.inserted.title,
            source_url=stmt.inserted.source_url,
            file_path=stmt.inserted.file_path,
            content_hash=stmt.inserted.content_hash,
            mime_type=stmt.inserted.mime_type,
            file_size=stmt.inserted.file_size,
            page_count=stmt.inserted.page_count,
            downloaded_at=stmt.inserted.downloaded_at,
            published_at=stmt.inserted.published_at,
        )
        s.execute(stmt)
        doc_id = s.execute(
            select(CouncilApplicationDocument.id)
            .where(CouncilApplicationDocument.application_id == application_pk)
            .where(CouncilApplicationDocument.doc_oid == dl.doc_oid)
        ).scalar_one()
        return doc_id


def mark_docs_fetched(application_pk: int) -> None:
    now = datetime.utcnow()
    with session_scope() as s:
        s.execute(
            update(CouncilApplication)
            .where(CouncilApplication.id == application_pk)
            .values(docs_fetched_at=now, last_seen_at=now)
        )


def _extract_units(description: str | None) -> int | None:
    from listo.councils.parsing import extract_approved_units
    return extract_approved_units(description)


# ---------------- phase drivers ----------------


def run_council(
    council: CouncilDef,
    *,
    date_from: date,
    date_to: date,
    do_list: bool = True,
    do_detail: bool = True,
    do_docs: bool = True,
    detail_limit: int | None = None,
    docs_limit: int | None = None,
    doc_dir: Path = DEFAULT_DOC_DIR,
) -> dict:
    """Run all configured backends for a council across the date window."""
    stats = {"list": 0, "detail": 0, "docs": 0, "doc_files": 0}
    for backend in council.backends:
        if not backend.covers(date_from=date_from, date_to=date_to):
            continue
        lo, hi = backend.clamp(date_from=date_from, date_to=date_to)
        logger.info("backend=%s window=%s..%s", backend.name, lo, hi)

        scraper_ctx = backend.factory()
        with scraper_ctx as scraper:
            sink = DbRequestSink(council_slug=scraper.council_slug, vendor=scraper.vendor)
            # Record this scrape attempt so 'is window X..Y done?' has a
            # direct answer in the DB rather than being inferred from
            # per-app timestamps. We update the row on success/failure.
            window_id = _start_scrape_window(
                council_slug=scraper.council_slug,
                vendor=scraper.vendor,
                backend_name=backend.name,
                date_from=lo, date_to=hi,
            )
            list_count = detail_count = docs_apps = docs_files = 0
            try:
                if do_list:
                    list_count = _phase_list(scraper, sink, date_from=lo, date_to=hi)
                    stats["list"] += list_count
                if do_detail:
                    detail_count = _phase_detail(
                        scraper, sink,
                        council_slug=scraper.council_slug, vendor=scraper.vendor,
                        date_from=lo, date_to=hi, limit=detail_limit,
                    )
                    stats["detail"] += detail_count
                if do_docs:
                    docs_apps, docs_files = _phase_docs(
                        scraper, sink,
                        council_slug=scraper.council_slug, vendor=scraper.vendor,
                        date_from=lo, date_to=hi, limit=docs_limit, doc_dir=doc_dir,
                    )
                    stats["docs"] += docs_apps
                    stats["doc_files"] += docs_files
            except Exception as e:
                _finish_scrape_window(
                    window_id, status="failed", error=str(e)[:1000],
                    apps_yielded=list_count, files_downloaded=docs_files,
                )
                raise
            else:
                _finish_scrape_window(
                    window_id, status="completed",
                    apps_yielded=list_count, files_downloaded=docs_files,
                )

    return stats


def _start_scrape_window(
    *,
    council_slug: str,
    vendor: str,
    backend_name: str,
    date_from: date,
    date_to: date,
) -> int:
    from listo.models import CouncilScrapeWindow
    now = datetime.utcnow()
    with session_scope() as s:
        row = CouncilScrapeWindow(
            council_slug=council_slug,
            vendor=vendor,
            backend_name=backend_name,
            date_from=date_from,
            date_to=date_to,
            started_at=now,
            status="running",
        )
        s.add(row)
        s.flush()
        return row.id


def _finish_scrape_window(
    window_id: int,
    *,
    status: str,
    error: str | None = None,
    apps_yielded: int = 0,
    files_downloaded: int = 0,
) -> None:
    from listo.models import CouncilScrapeWindow
    now = datetime.utcnow()
    with session_scope() as s:
        s.execute(
            update(CouncilScrapeWindow)
            .where(CouncilScrapeWindow.id == window_id)
            .values(
                finished_at=now,
                status=status,
                error=error,
                apps_yielded=apps_yielded,
                files_downloaded=files_downloaded,
            )
        )


def _phase_list(scraper, sink, *, date_from: date, date_to: date) -> int:
    logger.info("=== phase: list (%s..%s) ===", date_from, date_to)
    # Skip applications that already completed on a previous run. The
    # scraper still walks the same pages (ePathway pagination is a
    # postback chain — random access isn't available), but it yields
    # without clicking into already-complete rows.
    with session_scope() as s:
        already_done = set(s.execute(
            select(CouncilApplication.application_id)
            .where(CouncilApplication.council_slug == scraper.council_slug)
            .where(CouncilApplication.docs_fetched_at.is_not(None))
            .where(CouncilApplication.lodged_date.between(date_from, date_to))
        ).scalars().all())
    if already_done:
        logger.info(
            "phase list: %d applications already complete in window — will skip in walk",
            len(already_done),
        )
    n = 0
    n_with_docs = 0
    n_docs_indexed = 0
    n_files_downloaded = 0
    for row in scraper.iter_listings(
        date_from=date_from, date_to=date_to, sink=sink,
        skip_application_ids=already_done,
    ):
        app_pk = upsert_listing(row)
        # Vendors like Infor ePathway resolve the detail page during the
        # list walk (results page anchors are javascript postbacks, not
        # URLs we can goto later). When that happens, persist the detail
        # immediately so the detail phase has nothing to redo.
        if row.inline_detail is not None:
            upsert_detail(app_pk, row.inline_detail)
        # When the scraper also walks the documents portal inline, the
        # whole app — listing + detail + every document — becomes
        # consistent on a per-row basis. Persist each doc (metadata-only
        # rows have file_path=None) and stamp docs_fetched_at so a
        # Ctrl+C lands a clean state and re-runs skip the app entirely.
        if row.inline_documents is not None:
            for dl in row.inline_documents:
                upsert_document(app_pk, dl)
                n_docs_indexed += 1
                if dl.file_path:
                    n_files_downloaded += 1
            mark_docs_fetched(app_pk)
            n_with_docs += 1
        n += 1
    logger.info(
        "=== phase list complete: %d applications "
        "(%d apps with docs, %d docs indexed, %d files downloaded) ===",
        n, n_with_docs, n_docs_indexed, n_files_downloaded,
    )
    return n


def _phase_detail(
    scraper, sink, *,
    council_slug: str, vendor: str,
    date_from: date, date_to: date, limit: int | None,
) -> int:
    """Fetch detail for applications in window with detail_fetched_at IS NULL."""
    with session_scope() as s:
        q = (
            select(CouncilApplication.id, CouncilApplication.application_id, CouncilApplication.application_url)
            .where(CouncilApplication.council_slug == council_slug)
            .where(CouncilApplication.vendor == vendor)
            .where(CouncilApplication.detail_fetched_at.is_(None))
            .where(CouncilApplication.lodged_date.between(date_from, date_to))
            .order_by(CouncilApplication.lodged_date.asc(), CouncilApplication.id.asc())
        )
        if limit is not None:
            q = q.limit(limit)
        pending = s.execute(q).all()

    logger.info(
        "=== phase: detail (%d pending in %s..%s) ===",
        len(pending), date_from, date_to,
    )
    n = 0
    n_skipped_no_url = 0
    total = len(pending)
    for i, (app_pk, app_id, app_url) in enumerate(pending, start=1):
        # Apps without a usable detail URL can only be reached through
        # the list-walk path (which clicks the application_id link from
        # results). The standalone detail phase has nothing to fetch
        # for them, so skip without erroring.
        if not app_url or "Error.aspx" in app_url:
            n_skipped_no_url += 1
            continue
        logger.info("[detail %d/%d] %s", i, total, app_id)
        listing = DaListingRow(
            council_slug=council_slug,
            vendor=vendor,
            application_id=app_id,
            application_url=app_url,
        )
        try:
            detail = scraper.fetch_detail(listing, sink)
            upsert_detail(app_pk, detail)
            n += 1
        except Exception as e:
            logger.warning("[detail %d/%d] FAILED %s: %s", i, total, app_id, e)
    logger.info(
        "=== phase detail complete: %d/%d applications updated (skipped %d with no usable URL) ===",
        n, total, n_skipped_no_url,
    )
    return n


def _phase_docs(
    scraper, sink, *,
    council_slug: str, vendor: str,
    date_from: date, date_to: date, limit: int | None,
    doc_dir: Path,
) -> tuple[int, int]:
    """Download documents for applications with detail but no docs."""
    with session_scope() as s:
        q = (
            select(CouncilApplication.id, CouncilApplication.application_id, CouncilApplication.application_url)
            .where(CouncilApplication.council_slug == council_slug)
            .where(CouncilApplication.vendor == vendor)
            .where(CouncilApplication.detail_fetched_at.is_not(None))
            .where(CouncilApplication.docs_fetched_at.is_(None))
            .where(CouncilApplication.lodged_date.between(date_from, date_to))
            .order_by(CouncilApplication.lodged_date.asc(), CouncilApplication.id.asc())
        )
        if limit is not None:
            q = q.limit(limit)
        pending = s.execute(q).all()

    logger.info(
        "=== phase: docs (%d apps pending in %s..%s) ===",
        len(pending), date_from, date_to,
    )
    apps_done = 0
    files_done = 0
    total = len(pending)
    for i, (app_pk, app_id, app_url) in enumerate(pending, start=1):
        from listo.councils.base import DaDetailRecord
        # Skip apps whose stored URL clearly can't reach the docs portal
        # (e.g. legacy javascript: postback URLs, error-page captures, or
        # missing URLs). Don't stamp docs_fetched_at — leave them NULL so
        # the next list-walk run re-clicks them via the inline path.
        if not app_url or "Error.aspx" in app_url or "?Id=" not in app_url and "&Id=" not in app_url:
            logger.info(
                "[docs %d/%d] %s — skipping, no usable detail URL (%s)",
                i, total, app_id, (app_url or "")[:80],
            )
            continue
        detail = DaDetailRecord(
            council_slug=council_slug,
            vendor=vendor,
            application_id=app_id,
            application_url=app_url,
        )
        try:
            docs = scraper.list_documents(detail, sink)
        except Exception as e:
            logger.warning("[docs %d/%d] index failed for %s: %s", i, total, app_id, e)
            continue
        logger.info("[docs %d/%d] %s — %d documents", i, total, app_id, len(docs))

        # Apply the same first+last download policy that the inline
        # list walk uses, so legacy apps being processed here don't
        # silently pull every plan set + technical report. Sort by
        # published_at when available; otherwise use the order returned
        # by the docs portal (already chronological per its default
        # sort on the Date published column).
        from listo.councils.parsing import parse_size_to_bytes
        from listo.councils.infor_epathway import _select_download_indices

        ordered_docs = sorted(
            docs,
            key=lambda r: (r.published_at is None, r.published_at or ""),
        )
        download_idx = _select_download_indices(len(ordered_docs))

        target = doc_dir / app_id.replace("/", "_")
        for j, ref in enumerate(ordered_docs, start=1):
            do_download = (j - 1) in download_idx
            tag = "DOWNLOAD" if do_download else "metadata"
            logger.info(
                "[docs %d/%d] %s   ↳ %d/%d [%s] %s (%s)",
                i, total, app_id, j, len(ordered_docs), tag,
                (ref.title or ref.doc_oid or "?")[:60],
                ref.size_text or "?",
            )
            if do_download:
                try:
                    dl = scraper.download(ref, target, sink)
                    upsert_document(app_pk, dl)
                    files_done += 1
                    continue
                except Exception as e:
                    logger.warning(
                        "[docs %d/%d] %s   ↳ download failed for %s: %s — keeping metadata-only",
                        i, total, app_id, ref.doc_oid, e,
                    )
            # Metadata-only persist: insert/update the row without
            # downloading bytes. file_size is best-effort from the
            # portal's text estimate.
            from listo.councils.base import DownloadedDocument
            upsert_document(app_pk, DownloadedDocument(
                doc_oid=ref.doc_oid,
                title=ref.title,
                doc_type=ref.doc_type,
                source_url=ref.source_url,
                file_path=None,
                file_size=parse_size_to_bytes(ref.size_text),
                mime_type=None,
                content_hash=None,
                page_count=None,
                published_at=ref.published_at,
            ))
        mark_docs_fetched(app_pk)
        apps_done += 1
    logger.info(
        "=== phase docs complete: %d apps, %d files ===", apps_done, files_done,
    )
    return apps_done, files_done
