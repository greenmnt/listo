from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from sqlalchemy import select, update

from listo.db import session_scope
from listo.fetch.http import BlockedError
from listo.fetch.playwright_http import realestate_fetcher
from listo.fetch.writer import get_cached_body, store_raw_page, was_fetched_recently
from listo.models import CrawlRun
from listo.parse.realestate import peek_pagination
from listo.suburbs import slugify_realestate

logger = logging.getLogger(__name__)

PageKind = Literal["sold", "buy"]

_PAGE_TYPE_FOR = {
    "sold": "search_sold",
    "buy": "search_buy",
}


def search_url(suburb: str, postcode: str, state: str, kind: PageKind, page: int) -> str:
    slug = slugify_realestate(suburb)
    section = "sold" if kind == "sold" else "buy"
    state_l = state.lower()
    # %2c is the URL-encoded comma between locality and state
    return f"https://www.realestate.com.au/{section}/in-{slug}%2c+{state_l}+{postcode}/list-{page}"


# Property types we fetch for the duplex 'before' signal. We deliberately
# EXCLUDE unit+apartment in the default pass: the dominant volume in many
# Gold Coast suburbs is high-rise units (Surfers Paradise, Main Beach) and
# they aren't the redev source — they're the redev outcome. Houses, land,
# townhouses, and villas are what becomes a duplex. Units can be re-fetched
# later via a separate pass with property_types=('unit+apartment',).
DEFAULT_BUCKET_PROPERTY_TYPES = (
    "house", "land", "townhouse", "villa",
)

# realestate caps results at 80 pages × 25 = 2000 listings per query. Split
# brackets if their total exceeds this floor (with margin) AND the bracket is
# still wider than the minimum width.
_BUCKET_OVERFLOW_THRESHOLD = 1900
_BUCKET_MIN_WIDTH = 25_000   # smallest dollar-width below which we accept the cap

# Initial price brackets covering the full residential range. The recursive
# splitter narrows any bracket whose total > overflow threshold.
_INITIAL_PRICE_BRACKETS: tuple[tuple[int, int], ...] = (
    # (0,            500_000),
    (500_000,      700_000),
    (700_000,      900_000),
    (900_000,    1_100_000),
    (1_100_000,  1_300_000),
    (1_300_000,  1_500_000),
    (1_500_000,  1_750_000),
    (1_750_000,  2_000_000),
    (2_000_000,  2_500_000),
    (2_500_000,  3_000_000),
    (3_000_000,  4_000_000),
    (4_000_000,  5_500_000),
    (5_500_000,  8_000_000),
    (8_000_000, 15_000_000),
    (15_000_000, 50_000_000),
)


def bucketed_search_url(
    suburb: str,
    postcode: str,
    state: str,
    kind: PageKind,
    page: int,
    *,
    property_types: tuple[str, ...] = DEFAULT_BUCKET_PROPERTY_TYPES,
    min_price: int | None = None,
    max_price: int | None = None,
) -> str:
    """Build a property-type + price-range filtered URL.

    Pattern: /sold/property-{types-hyphen-joined}[-between-{min}-{max}]-in-{slug},+{state}+{postcode}/list-{N}?source=refinement
    """
    slug = slugify_realestate(suburb)
    section = "sold" if kind == "sold" else "buy"
    state_l = state.lower()
    types_seg = "-".join(property_types)
    if min_price is not None and max_price is not None:
        bracket = f"-between-{min_price}-{max_price}"
    else:
        bracket = ""
    return (
        f"https://www.realestate.com.au/{section}/property-{types_seg}{bracket}-in-"
        f"{slug},+{state_l}+{postcode}/list-{page}?source=refinement"
    )


@dataclass
class FetchSuburbResult:
    suburb: str
    postcode: str
    state: str
    pages_fetched: int
    last_page: int
    status: str
    error: str | None = None


def _recent_done_run_exists(
    source: str, page_type: str, suburb: str, postcode: str, max_age_days: int
) -> CrawlRun | None:
    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    with session_scope() as s:
        row = s.execute(
            select(CrawlRun)
            .where(
                CrawlRun.source == source,
                CrawlRun.page_type == page_type,
                CrawlRun.suburb == suburb,
                CrawlRun.postcode == postcode,
                CrawlRun.status == "done",
                CrawlRun.finished_at >= cutoff,
            )
            .order_by(CrawlRun.finished_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if row:
            # Detach from session for safe access after session close.
            s.expunge(row)
        return row


def fetch_suburb(
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
    """Fetch search-result pages for one suburb. Returns a summary.

    If `max_age_days` is set and a successful run for this (suburb, page_type)
    finished within that window, skip the whole suburb (unless force=True).
    """
    page_type = _PAGE_TYPE_FOR[kind]

    if max_age_days is not None and not force:
        existing = _recent_done_run_exists("realestate", page_type, suburb, postcode, max_age_days)
        if existing:
            logger.info(
                "skip realestate %s %s %s: done %s ago (%d pages)",
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

    with realestate_fetcher() as fetcher:
        for page in range(start_page, start_page + max_pages):
            url = search_url(suburb, postcode, state, kind, page)

            # Page-level dedup: if this URL was already fetched recently with
            # the same content, skip the HTTP call entirely.
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


# ----------------------------------------------------------------------------
# Bucketed fetch: split by price ranges so each (suburb, type, price-range)
# query stays under the 80-page cap. Lets us reach the full sale history
# instead of just the most-recent ~2000.
# ----------------------------------------------------------------------------


@dataclass
class BucketedFetchResult:
    suburb: str
    postcode: str
    state: str
    pages_fetched: int
    buckets_processed: int
    buckets_overflowed: int  # leaf buckets that still hit the cap (incomplete)
    status: str
    error: str | None = None


def fetch_suburb_bucketed(
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
    """Walk price brackets recursively. For each bracket: fetch page 1, peek
    total_results. If over the cap, split in half and recurse. Otherwise fetch
    all pages of that bracket.

    Storage uses the same raw_pages table — bucketed page-1 fetches that overlap
    with sibling brackets get deduped by content_hash inside writer.store_raw_page.
    """
    page_type = _PAGE_TYPE_FOR[kind]

    if max_age_days is not None and not force:
        existing = _recent_done_run_exists("realestate", page_type, suburb, postcode, max_age_days)
        if existing:
            logger.info(
                "skip realestate-bucketed %s %s %s: done %s ago",
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

    # Queue starts with the broadest brackets; we'll recurse-split as needed.
    queue: list[tuple[int, int]] = list(_INITIAL_PRICE_BRACKETS)
    pages_fetched = 0
    buckets_processed = 0
    buckets_overflowed = 0
    final_status = "done"
    error: str | None = None

    with realestate_fetcher() as fetcher:
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
                # Split bracket [lo, hi] into halves, prepend to queue
                if hi - lo >= _BUCKET_MIN_WIDTH * 2:
                    mid = (lo + hi) // 2
                    queue.insert(0, (mid, hi))
                    queue.insert(0, (lo, mid))
                else:
                    buckets_overflowed += 1   # too narrow to split further
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


def _process_bucket(
    fetcher,
    *,
    suburb: str, postcode: str, state: str,
    kind: PageKind, page_type: str,
    property_types: tuple[str, ...],
    lo: int, hi: int,
    max_pages: int,
    max_age_days: int | None,
    force: bool,
) -> tuple[int, str]:
    """Fetch page 1 of a bracket, decide split-or-continue based on total.
    Returns (pages_fetched, status) where status is 'split' | 'complete' | 'incomplete'.
    'incomplete' means we hit the cap on a bracket we couldn't split further.
    """
    page1_url = bucketed_search_url(
        suburb, postcode, state, kind, page=1,
        property_types=property_types, min_price=lo, max_price=hi,
    )
    pages_fetched = 0
    pages_skipped_cached = 0

    # Default cache window for resume: be generous (90 days). The whole point
    # of resume is "if we already have it, don't re-fetch" — the data doesn't
    # go stale that fast for sold listings.
    cache_age_hours = (max_age_days * 24) if max_age_days is not None else (90 * 24)

    # Try to use a cached page 1 for the split decision before issuing any network.
    body_for_decision: str | None = None
    if not force:
        body_for_decision = get_cached_body("realestate", page1_url, max_age_hours=cache_age_hours)

    if body_for_decision is None:
        # Fetch page 1 fresh.
        result = fetcher.get(page1_url)
        store_raw_page(
            source="realestate", page_type=page_type, url=page1_url,
            body=result.body, http_status=result.status, headers=result.headers,
            suburb=suburb, postcode=postcode, page_index=1,
        )
        pages_fetched += 1
        body_for_decision = result.body
    else:
        pages_skipped_cached += 1

    try:
        cur, total_pages_remote = peek_pagination(body_for_decision)
    except Exception:
        cur, total_pages_remote = (None, None)
    from listo.parse.realestate import parse as _parse_re
    parsed = _parse_re(body_for_decision)
    total_results = parsed.total_results or 0

    # Decide if we need to split
    if total_results > _BUCKET_OVERFLOW_THRESHOLD and (hi - lo) >= _BUCKET_MIN_WIDTH * 2:
        logger.info("  bucket %s [$%d-$%d]: %d listings → SPLIT (cached=%s)",
                    suburb, lo, hi, total_results, pages_skipped_cached > 0)
        return pages_fetched, "split"

    last_page_to_fetch = min(max_pages, total_pages_remote or max_pages)
    incomplete = (total_pages_remote or 0) > max_pages and total_results > _BUCKET_OVERFLOW_THRESHOLD
    logger.info("  bucket %s [$%d-$%d]: %d listings, fetching %d pages (cached page1=%s)",
                suburb, lo, hi, total_results, last_page_to_fetch, pages_skipped_cached > 0)

    for page in range(2, last_page_to_fetch + 1):
        url = bucketed_search_url(
            suburb, postcode, state, kind, page=page,
            property_types=property_types, min_price=lo, max_price=hi,
        )
        # Skip pages already cached so a restart resumes mid-bucket.
        if not force and was_fetched_recently("realestate", url, max_age_hours=cache_age_hours):
            pages_skipped_cached += 1
            continue
        r = fetcher.get(url)
        store_raw_page(
            source="realestate", page_type=page_type, url=url,
            body=r.body, http_status=r.status, headers=r.headers,
            suburb=suburb, postcode=postcode, page_index=page,
        )
        pages_fetched += 1
        if page % 20 == 0:
            logger.info("    %s [$%d-$%d] page %d/%d (fetched=%d, skipped=%d)",
                        suburb, lo, hi, page, last_page_to_fetch,
                        pages_fetched, pages_skipped_cached)
        try:
            cur2, mx2 = peek_pagination(r.body)
        except Exception:
            cur2, mx2 = (None, None)
        if mx2 and page >= mx2:
            break

    if pages_skipped_cached and not pages_fetched:
        logger.info("  bucket %s [$%d-$%d]: fully cached, skipped %d pages",
                    suburb, lo, hi, pages_skipped_cached)

    return pages_fetched, ("incomplete" if incomplete else "complete")
