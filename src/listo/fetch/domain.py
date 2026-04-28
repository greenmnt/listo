from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from sqlalchemy import select, update

from listo.db import session_scope
from listo.fetch.http import BlockedError
from listo.fetch.playwright_http import domain_fetcher
from listo.fetch.writer import store_raw_page, was_fetched_recently
from listo.models import CrawlRun
from listo.parse.domain import peek_pagination
from listo.suburbs import slugify_domain

logger = logging.getLogger(__name__)

PageKind = Literal["sold", "buy"]

_PAGE_TYPE_FOR = {
    "sold": "search_sold",
    "buy": "search_buy",
}


def search_url(suburb: str, postcode: str, state: str, kind: PageKind, page: int) -> str:
    slug = slugify_domain(suburb)
    state_l = state.lower()
    if kind == "sold":
        return (
            f"https://www.domain.com.au/sold-listings/{slug}-{state_l}-{postcode}/"
            f"?excludepricewithheld=1&page={page}"
        )
    return f"https://www.domain.com.au/sale/{slug}-{state_l}-{postcode}/?page={page}"


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
    page_type = _PAGE_TYPE_FOR[kind]

    if max_age_days is not None and not force:
        existing = _recent_done_run_exists("domain", page_type, suburb, postcode, max_age_days)
        if existing:
            logger.info(
                "skip domain %s %s %s: done %s ago (%d pages)",
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
            source="domain", page_type=page_type, suburb=suburb,
            postcode=postcode, started_at=started, status="running",
        )
        s.add(run)
        s.flush()
        run_id = run.id

    pages_fetched = 0
    last_page = start_page - 1
    final_status = "done"
    error: str | None = None

    with domain_fetcher() as fetcher:
        for page in range(start_page, start_page + max_pages):
            url = search_url(suburb, postcode, state, kind, page)

            if max_age_days is not None and not force and was_fetched_recently(
                "domain", url, max_age_hours=max_age_days * 24
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
                source="domain", page_type=page_type, url=url,
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
