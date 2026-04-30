"""Tiny helper for inserting raw_pages rows from the property-history
fetchers. Mirrors the helper from `archive/fetch/writer.py` but kept in
the new module so the property-history code is self-contained — no
imports from the archive."""
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
) -> int:
    """Insert a raw_pages row, returning its id.

    Dedups against (source, url_hash, content_hash) within
    `settings.dedup_window_hours`. When a duplicate is found the existing
    row's id is returned rather than inserting again — matters because the
    property-history flow always wants a raw_page_id to anchor the parsed
    snapshot to, even when nothing changed.
    """
    body_bytes = body.encode("utf-8")
    content_h = _sha256(body_bytes)
    url_h = _sha256(url.encode("utf-8"))
    body_gz = gzip.compress(body_bytes)
    now = datetime.utcnow()
    cutoff = now - timedelta(hours=settings.dedup_window_hours)

    with session_scope() as s:
        existing = s.execute(
            select(RawPage.id).where(
                RawPage.source == source,
                RawPage.url_hash == url_h,
                RawPage.content_hash == content_h,
                RawPage.fetched_at >= cutoff,
            )
        ).scalar_one_or_none()
        if existing is not None:
            return existing

        row = RawPage(
            source=source,
            page_type=page_type,
            url=url,
            url_hash=url_h,
            suburb=suburb,
            postcode=postcode,
            page_index=None,
            http_status=http_status,
            fetched_at=now,
            content_hash=content_h,
            body_gz=body_gz,
            headers_json=json.loads(json.dumps(dict(headers), default=str)),
        )
        s.add(row)
        s.flush()
        return row.id
