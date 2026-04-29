"""Camoufox-engine variant of fetch/realestate.py.

The orchestration (CrawlRun bookkeeping, page-level dedup, bucketed price-bracket
recursion, resume-from-cache) is identical to the playwright/patchright path —
the only thing that differs is which fetcher class is instantiated. To avoid
duplicating ~200 lines we reuse the helpers from realestate.py directly. The
existing module is kept untouched so the playwright path can keep running
side-by-side; flip engines by passing engine='camoufox' through the CLI.
"""
from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import update

from listo.db import session_scope
from listo.fetch.camoufox_http import realestate_camoufox_fetcher
from listo.fetch.http import BlockedError
from listo.fetch.realestate import (
    DEFAULT_BUCKET_PROPERTY_TYPES,
    BucketedFetchResult,
    FetchSuburbResult,
    PageKind,
    _BUCKET_MIN_WIDTH,
    _INITIAL_PRICE_BRACKETS,
    _PAGE_TYPE_FOR,
    _process_bucket,
    _recent_done_run_exists,
    bucketed_search_url,  # noqa: F401  (re-exported for CLI symmetry)
    search_url,
)
from listo.fetch.writer import store_raw_page, was_fetched_recently
from listo.models import CrawlRun
from listo.parse.realestate import peek_pagination

logger = logging.getLogger(__name__)


def fetch_suburb_camoufox(
    suburb: str,
    postcode: str,
    kind: PageKind,
    *,
    state: str = "QLD",
    max_pages: int = 80,
    start_page: int = 1,
    max_age_days: int | None = None,
    force: bool = False,
) -> FetchSuburbResult:
    """Camoufox variant of fetch_suburb. Same semantics, different engine."""
    page_type = _PAGE_TYPE_FOR[kind]

    if max_age_days is not None and not force:
        existing = _recent_done_run_exists("realestate", page_type, suburb, postcode, max_age_days)
        if existing:
            logger.info(
                "skip realestate-camoufox %s %s %s: done %s ago (%d pages)",
                kind, suburb, postcode,
                (datetime.utcnow() - existing.finished_at),
                existing.pages_fetched,
            )
            return FetchSuburbResult(
                suburb=suburb, postcode=postcode, state=state,
                pages_fetched=0, last_page=existing.last_page,
                status="skipped", error=None,
            )

    started = datetime.utcnow()
    with session_scope() as s:
        run = CrawlRun(
            source="realestate", page_type=page_type, suburb=suburb,
            postcode=postcode, started_at=started, status="running",
        )
        s.add(run)
        s.flush()
        run_id = run.id

    pages_fetched = 0
    last_page = start_page - 1
    final_status = "done"
    error: str | None = None

    with realestate_camoufox_fetcher() as fetcher:
        for page in range(start_page, start_page + max_pages):
            url = search_url(suburb, postcode, state, kind, page)

            if max_age_days is not None and not force and was_fetched_recently(
                "realestate", url, max_age_hours=max_age_days * 24
            ):
                last_page = page
                continue

            try:
                result = fetcher.get(url)
            except BlockedError as e:
                final_status = "failed"
                error = str(e)
                break
            except Exception as e:  # noqa: BLE001
                final_status = "partial"
                error = f"{type(e).__name__}: {e}"
                break

            store_raw_page(
                source="realestate", page_type=page_type, url=url,
                body=result.body, http_status=result.status,
                headers=result.headers, suburb=suburb,
                postcode=postcode, page_index=page,
            )
            pages_fetched += 1
            last_page = page

            try:
                cur, mx = peek_pagination(result.body)
            except Exception:
                cur, mx = (None, None)
            if mx and page >= mx:
                break

    finished = datetime.utcnow()
    with session_scope() as s:
        s.execute(
            update(CrawlRun)
            .where(CrawlRun.id == run_id)
            .values(
                finished_at=finished, pages_fetched=pages_fetched,
                last_page=last_page, status=final_status, error=error,
            )
        )

    return FetchSuburbResult(
        suburb=suburb, postcode=postcode, state=state,
        pages_fetched=pages_fetched, last_page=last_page,
        status=final_status, error=error,
    )


def fetch_suburb_bucketed_camoufox(
    suburb: str,
    postcode: str,
    kind: PageKind,
    *,
    state: str = "QLD",
    property_types: tuple[str, ...] = DEFAULT_BUCKET_PROPERTY_TYPES,
    max_pages_per_bucket: int = 80,
    max_age_days: int | None = None,
    force: bool = False,
) -> BucketedFetchResult:
    """Camoufox variant of fetch_suburb_bucketed. Reuses the same _process_bucket
    helper since it's engine-agnostic — it only calls fetcher.get(url)."""
    page_type = _PAGE_TYPE_FOR[kind]

    if max_age_days is not None and not force:
        existing = _recent_done_run_exists("realestate", page_type, suburb, postcode, max_age_days)
        if existing:
            logger.info(
                "skip realestate-camoufox-bucketed %s %s %s: done %s ago",
                kind, suburb, postcode, datetime.utcnow() - existing.finished_at,
            )
            return BucketedFetchResult(
                suburb=suburb, postcode=postcode, state=state,
                pages_fetched=0, buckets_processed=0, buckets_overflowed=0,
                status="skipped", error=None,
            )

    started = datetime.utcnow()
    with session_scope() as s:
        run = CrawlRun(
            source="realestate", page_type=page_type, suburb=suburb,
            postcode=postcode, started_at=started, status="running",
        )
        s.add(run); s.flush()
        run_id = run.id

    queue: list[tuple[int, int]] = list(_INITIAL_PRICE_BRACKETS)
    pages_fetched = 0
    buckets_processed = 0
    buckets_overflowed = 0
    final_status = "done"
    error: str | None = None

    with realestate_camoufox_fetcher() as fetcher:
        while queue:
            lo, hi = queue.pop(0)
            try:
                pages, overflow = _process_bucket(
                    fetcher, suburb=suburb, postcode=postcode, state=state,
                    kind=kind, page_type=page_type, property_types=property_types,
                    lo=lo, hi=hi, max_pages=max_pages_per_bucket,
                    max_age_days=max_age_days, force=force,
                )
            except BlockedError as e:
                final_status = "failed"; error = str(e)
                break
            except Exception as e:  # noqa: BLE001
                final_status = "partial"; error = f"{type(e).__name__}: {e}"
                break

            pages_fetched += pages
            buckets_processed += 1
            if overflow == "split":
                if hi - lo >= _BUCKET_MIN_WIDTH * 2:
                    mid = (lo + hi) // 2
                    queue.insert(0, (mid, hi))
                    queue.insert(0, (lo, mid))
                else:
                    buckets_overflowed += 1
            elif overflow == "incomplete":
                buckets_overflowed += 1
            logger.info(
                "bucket [$%d-$%d]: pages=%d overflow=%s — total fetched so far: %d pages, %d buckets",
                lo, hi, pages, overflow, pages_fetched, buckets_processed,
            )

    finished = datetime.utcnow()
    with session_scope() as s:
        s.execute(update(CrawlRun).where(CrawlRun.id == run_id).values(
            finished_at=finished, pages_fetched=pages_fetched, last_page=0,
            status=final_status, error=error,
        ))

    return BucketedFetchResult(
        suburb=suburb, postcode=postcode, state=state,
        pages_fetched=pages_fetched, buckets_processed=buckets_processed,
        buckets_overflowed=buckets_overflowed, status=final_status, error=error,
    )
