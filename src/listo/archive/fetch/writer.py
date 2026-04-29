from __future__ import annotations

import gzip
import hashlib
import json
from datetime import datetime, timedelta

from sqlalchemy import select

from listo.config import settings
from listo.db import session_scope
from listo.models import RawPage


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def was_fetched_recently(source: str, url: str, max_age_hours: int) -> bool:
    """Return True if a raw_pages row for (source, url) exists within the window."""
    url_hash = _sha256(url.encode("utf-8"))
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
    with session_scope() as s:
        row = s.execute(
            select(RawPage.id).where(
                RawPage.source == source,
                RawPage.url_hash == url_hash,
                RawPage.fetched_at >= cutoff,
            ).limit(1)
        ).first()
        return row is not None


def get_cached_body(source: str, url: str, max_age_hours: int) -> str | None:
    """Return the most recent cached body for this URL within the window, or None.

    Used by the bucketed fetcher to decide split-or-fetch for a price bracket
    without re-issuing the network request when we already have page 1.
    """
    import gzip
    url_hash = _sha256(url.encode("utf-8"))
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
    with session_scope() as s:
        row = s.execute(
            select(RawPage.body_gz).where(
                RawPage.source == source,
                RawPage.url_hash == url_hash,
                RawPage.fetched_at >= cutoff,
            )
            .order_by(RawPage.fetched_at.desc())
            .limit(1)
        ).first()
        if not row:
            return None
        try:
            return gzip.decompress(row[0]).decode("utf-8", errors="replace")
        except Exception:
            return None


def store_raw_page(
    *,
    source: str,
    page_type: str,
    url: str,
    body: str,
    http_status: int,
    headers: dict[str, str],
    suburb: str | None = None,
    postcode: str | None = None,
    page_index: int | None = None,
) -> int | None:
    """Insert a raw_pages row.

    Returns the inserted row id, or None if a recent identical fetch was
    deduplicated (same url_hash + content_hash within the dedup window).
    """
    body_bytes = body.encode("utf-8")
    content_hash = _sha256(body_bytes)
    url_hash = _sha256(url.encode("utf-8"))
    body_gz = gzip.compress(body_bytes)
    now = datetime.utcnow()
    cutoff = now - timedelta(hours=settings.dedup_window_hours)

    with session_scope() as s:
        existing = s.execute(
            select(RawPage.id).where(
                RawPage.source == source,
                RawPage.url_hash == url_hash,
                RawPage.content_hash == content_hash,
                RawPage.fetched_at >= cutoff,
            )
        ).first()
        if existing:
            return None

        row = RawPage(
            source=source,
            page_type=page_type,
            url=url,
            url_hash=url_hash,
            suburb=suburb,
            postcode=postcode,
            page_index=page_index,
            http_status=http_status,
            fetched_at=now,
            content_hash=content_hash,
            body_gz=body_gz,
            headers_json=json.loads(json.dumps(dict(headers), default=str)),
        )
        s.add(row)
        s.flush()
        return row.id
