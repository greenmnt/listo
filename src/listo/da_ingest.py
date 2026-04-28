"""DA enrichment pipeline: search council DA register for each duplex candidate,
fetch detail, download every document, upsert into dev_applications + da_documents.

Idempotent — re-runs skip applications already present (by council_slug +
application_id) and documents already downloaded (by content_hash).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from sqlalchemy import select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from listo.address import normalize_address
from listo.cogc_pdonline import (
    DEFAULT_DOC_DIR,
    ENQUIRY_LIST_AFTER_JULY_2017,
    ENQUIRY_LIST_BEFORE_JULY_2017,
    GoldCoastPdOnline,
    DaSearchResult,
)
from listo.councils import (
    council_for_postcode,
    extract_approved_units,
    extract_internal_property_id,
    extract_type_code,
)
from listo.db import session_scope
from listo.models import DaDocument, DevApplication, Property

logger = logging.getLogger(__name__)


@dataclass
class CandidateAddress:
    match_key: str
    street_number: str
    street_name_short: str   # 'Breaker' (without 'Street')
    suburb: str
    postcode: str
    state: str


@dataclass
class IngestStats:
    candidates_processed: int = 0
    candidates_skipped_no_council: int = 0
    applications_inserted: int = 0
    applications_skipped_existing: int = 0
    documents_downloaded: int = 0
    documents_skipped_existing: int = 0
    errors: int = 0


# Map common street-suffix tokens (full + abbreviation) so we can strip them
# off our normalized street_name and pass the bare street name to the council
# search (which has its own dropdown for street type).
_STREET_TYPE_TOKENS = {
    "ave","avenue","st","street","rd","road","dr","drive","ct","court",
    "pde","parade","tce","terrace","cres","crescent","bvd","boulevard",
    "boulevarde","hwy","highway","pl","place","ln","lane","cl","close",
    "way","cct","circuit","esp","esplanade","prom","promenade","gr","grove",
    "rise","row","view","sq","square","loop","trail","park",
}


def _strip_street_suffix(street_name: str) -> str:
    """'Breaker Street' -> 'Breaker'. Keeps the name as-is if no suffix found."""
    parts = street_name.strip().split()
    if not parts:
        return street_name
    if parts[-1].lower() in _STREET_TYPE_TOKENS:
        parts = parts[:-1]
    return " ".join(parts)


def candidates_needing_enrichment(limit: int | None = None) -> list[CandidateAddress]:
    """Return duplex candidates without any DA data yet, ready to enrich."""
    sql = text(
        """
        SELECT p.match_key,
               MIN(p.street_number)                        AS street_number,
               MIN(p.street_name)                          AS street_name,
               MIN(p.suburb)                               AS suburb,
               MIN(p.postcode)                             AS postcode,
               MIN(p.state)                                AS state,
               (SELECT COUNT(*) FROM dev_applications da WHERE da.match_key = p.match_key) AS da_rows
        FROM properties p
        GROUP BY p.match_key
        HAVING SUM(CASE WHEN p.unit_number = '' THEN 1 ELSE 0 END) >= 1
           AND SUM(CASE WHEN p.unit_number <> '' THEN 1 ELSE 0 END) >= 1
        ORDER BY p.match_key
        """
    )
    out: list[CandidateAddress] = []
    with session_scope() as s:
        for r in s.execute(sql).fetchall():
            if r.da_rows > 0:
                continue
            short = _strip_street_suffix(r.street_name)
            out.append(CandidateAddress(
                match_key=r.match_key,
                street_number=r.street_number,
                street_name_short=short,
                suburb=r.suburb,
                postcode=r.postcode,
                state=r.state,
            ))
            if limit and len(out) >= limit:
                break
    return out


def _application_already_ingested(council_slug: str, application_id: str) -> bool:
    with session_scope() as s:
        return s.execute(
            select(DevApplication.id).where(
                DevApplication.council_slug == council_slug,
                DevApplication.application_id == application_id,
            )
        ).first() is not None


def _upsert_application(
    *,
    council_slug: str,
    application_id: str,
    detail,                   # DaDetailRecord
    match_key: str,
    suburb: str,
    postcode: str,
    state: str,
) -> int:
    """Insert (or update) a dev_applications row, return its id."""
    now = datetime.utcnow()
    type_code = extract_type_code(application_id) or detail.type_code
    approved_units = extract_approved_units(detail.description)
    internal_property_id = (
        detail.internal_property_id or extract_internal_property_id(detail.description)
    )
    payload = {
        "council_slug": council_slug,
        "application_id": application_id,
        "application_type": detail.application_type,
        "type_code": type_code,
        "description": detail.description,
        "approved_units": approved_units,
        "internal_property_id": internal_property_id,
        "lot_on_plan": detail.lot_on_plan,
        "raw_address": detail.raw_address,
        "match_key": match_key,
        "suburb": suburb,
        "postcode": postcode,
        "state": state,
        "status": detail.status,
        "decision_outcome": detail.decision_outcome,
        "decision_authority": detail.decision_authority,
        "lodged_date": detail.lodged_date,
        "decision_date": detail.decision_date,
        "applicant_name": detail.applicant_name,
        "source_url": detail.source_url,
        "first_seen_at": now,
        "last_seen_at": now,
    }
    with session_scope() as s:
        stmt = mysql_insert(DevApplication).values(**payload)
        stmt = stmt.on_duplicate_key_update(
            application_type=stmt.inserted.application_type,
            type_code=stmt.inserted.type_code,
            description=stmt.inserted.description,
            approved_units=stmt.inserted.approved_units,
            internal_property_id=stmt.inserted.internal_property_id,
            lot_on_plan=stmt.inserted.lot_on_plan,
            raw_address=stmt.inserted.raw_address,
            match_key=stmt.inserted.match_key,
            suburb=stmt.inserted.suburb,
            postcode=stmt.inserted.postcode,
            state=stmt.inserted.state,
            status=stmt.inserted.status,
            decision_outcome=stmt.inserted.decision_outcome,
            decision_authority=stmt.inserted.decision_authority,
            lodged_date=stmt.inserted.lodged_date,
            decision_date=stmt.inserted.decision_date,
            applicant_name=stmt.inserted.applicant_name,
            source_url=stmt.inserted.source_url,
            last_seen_at=stmt.inserted.last_seen_at,
        )
        s.execute(stmt)

        row = s.execute(
            select(DevApplication.id).where(
                DevApplication.council_slug == council_slug,
                DevApplication.application_id == application_id,
            )
        ).first()
        if not row:
            raise RuntimeError(f"upsert succeeded but row not found: {council_slug}/{application_id}")
        return row[0]


def _document_already_stored(application_id: int, oid: str) -> bool:
    with session_scope() as s:
        return s.execute(
            select(DaDocument.id).where(
                DaDocument.application_id == application_id,
                DaDocument.doc_oid == oid,
            )
        ).first() is not None


def _store_document_row(
    *,
    application_id: int,
    oid: str,
    name: str | None,
    type_label: str | None,
    source_url: str | None,
    file_path: str,
    file_size: int,
    page_count: int | None,
    mime_type: str | None,
    content_hash: bytes,
) -> None:
    payload = {
        "application_id": application_id,
        "doc_type": type_label,
        "doc_oid": oid,
        "title": name,
        "source_url": source_url,
        "file_path": file_path,
        "content_hash": content_hash,
        "mime_type": mime_type,
        "file_size": file_size,
        "page_count": page_count,
        "downloaded_at": datetime.utcnow(),
    }
    with session_scope() as s:
        s.execute(mysql_insert(DaDocument).values(**payload).prefix_with("IGNORE"))


# ---------------- main loop ----------------

def enrich_candidates(
    *,
    limit: int | None = None,
    download_documents: bool = True,
    doc_dir: Path = DEFAULT_DOC_DIR,
) -> IngestStats:
    """Walk duplex candidates and ingest their council DA data + documents."""
    stats = IngestStats()
    candidates = candidates_needing_enrichment(limit=limit)
    logger.info("enriching %d candidate addresses", len(candidates))

    with GoldCoastPdOnline(headless=False, jitter_min=0.5, jitter_max=2.0) as gc:
        for cand in candidates:
            council = council_for_postcode(cand.postcode)
            if council is None:
                stats.candidates_skipped_no_council += 1
                continue

            for register in (ENQUIRY_LIST_AFTER_JULY_2017, ENQUIRY_LIST_BEFORE_JULY_2017):
                try:
                    results = gc.search_by_address(
                        cand.street_number, cand.street_name_short, cand.suburb,
                        enquiry_list=register,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning("search %s in %s failed: %s",
                                   (cand.street_number, cand.street_name_short, cand.suburb),
                                   register, e)
                    stats.errors += 1
                    continue

                for r in results:
                    try:
                        _process_one_application(
                            gc=gc, council_slug=council.slug, search_result=r,
                            cand=cand, doc_dir=doc_dir,
                            download_documents=download_documents, stats=stats,
                        )
                    except Exception as e:  # noqa: BLE001
                        logger.warning("processing %s failed: %s", r.application_id, e)
                        stats.errors += 1
                        continue

            stats.candidates_processed += 1
            logger.info(
                "candidate %d/%d done: %s — apps=%d docs=%d errors=%d",
                stats.candidates_processed, len(candidates),
                cand.match_key, stats.applications_inserted,
                stats.documents_downloaded, stats.errors,
            )
    return stats


def _process_one_application(
    *,
    gc: GoldCoastPdOnline,
    council_slug: str,
    search_result: DaSearchResult,
    cand: CandidateAddress,
    doc_dir: Path,
    download_documents: bool,
    stats: IngestStats,
) -> None:
    """Process one search result: skip if already ingested, else fetch detail +
    documents, upsert."""
    if _application_already_ingested(council_slug, search_result.application_id):
        stats.applications_skipped_existing += 1
        return

    detail = gc.get_detail(search_result)
    app_id_db = _upsert_application(
        council_slug=council_slug,
        application_id=search_result.application_id,
        detail=detail,
        match_key=cand.match_key,
        suburb=cand.suburb,
        postcode=cand.postcode,
        state=cand.state,
    )
    stats.applications_inserted += 1

    if not download_documents:
        return

    internal_id = gc._docs_portal_id(detail.source_url or "")  # noqa: SLF001
    if not internal_id:
        return
    try:
        docs = gc.list_documents(internal_id)
    except Exception as e:  # noqa: BLE001
        logger.warning("list_documents for %s failed: %s", search_result.application_id, e)
        return

    # Per-application sub-directory keyed by application_id (slugified)
    safe_app = re.sub(r"[^A-Za-z0-9._-]+", "_", search_result.application_id)
    target = doc_dir / safe_app

    docs_to_fetch = [d for d in docs if not _document_already_stored(app_id_db, d.oid)]
    skipped_existing = len(docs) - len(docs_to_fetch)
    stats.documents_skipped_existing += skipped_existing
    if not docs_to_fetch:
        return

    downloaded = gc.download_documents(docs_to_fetch, target)
    by_oid = {d.oid: d for d in docs}
    for dl in downloaded:
        meta = by_oid.get(dl.oid)
        _store_document_row(
            application_id=app_id_db,
            oid=dl.oid,
            name=dl.name or (meta.name if meta else None),
            type_label=(meta.type_label if meta else None),
            source_url=(meta.source_url if meta else None),
            file_path=dl.file_path,
            file_size=dl.file_size,
            page_count=dl.page_count,
            mime_type=dl.mime_type,
            content_hash=dl.content_hash,
        )
        stats.documents_downloaded += 1
