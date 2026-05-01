"""Harvest `application_entities` rows from already-extracted document text.

Tier-0 of the entity-extraction pipeline: pure regex over council
correspondence (Confirmation Notice, Decision Notice, Cover Letter,
Information Request). Cheap, deterministic, no LLM. The output is
the high-priority anchor for project-level synthesis.

For each document with extracted text:

  1. Run `parse_cogc_letter`. Skip if not a COGC letter.
  2. From the **recipient block** (top of letter): emit applicant +
     optional c/- agent rows. Multi-name strings are split (one row
     per person).
  3. From the **structured Applicant fields** ("Applicant name:" /
     "Applicant contact details:"): emit applicant + agent rows.
  4. From the **narrative refer-by-name** ("I refer to the
     development application lodged by: <NAME>"): emit applicant rows.

Each (application, company, role, source_doc) tuple is upserted
idempotently — re-running the harvester is a no-op.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from sqlalchemy import select, text as sql_text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from listo.db import session_scope
from listo.models import (
    ApplicationEntity,
    Company,
    CouncilApplicationDocument,
)
from listo.da_summaries.aggregate import _norm_name, _upsert_company
from listo.da_summaries.cogc_correspondence import (
    ParsedCorrespondence,
    extract_inline_co_agent,
    guess_entity_type,
    parse_cogc_letter,
    split_party_names,
)
from listo.da_summaries.text_extract import extract_text_for_prompt


logger = logging.getLogger(__name__)


EXTRACTOR = "cogc_correspondence_regex"


# ---------------------------------------------------------------- planning


def _extract_emit_plan(
    parsed: ParsedCorrespondence,
) -> list[tuple[str, str, str, str, str]]:
    """Turn a ParsedCorrespondence into a flat list of (name, role, source_field,
    confidence, entity_type) emissions. One emission per person.

    Dedup happens later when the (company_id, role) tuple is uniqued
    against the application.
    """
    emissions: list[tuple[str, str, str, str, str]] = []

    rb = parsed.recipient
    if rb and rb.primary_name:
        primary, inline_agent = extract_inline_co_agent(rb.primary_name)
        for name in split_party_names(primary):
            emissions.append((name, "applicant", "recipient_block", "high", guess_entity_type(name)))
        # An inline "C/- <agent>" on the primary line wins over the
        # structured care_of_agent — they describe the same agent.
        agent = inline_agent or rb.care_of_agent
        if agent:
            # Agent is always treated as a single entity — agents are
            # planning consultancies, never split.
            emissions.append(
                (agent, "agent", "recipient_block", "high", "company")
            )

    af = parsed.applicant_field
    if af and af.applicant_name:
        primary, inline_agent = extract_inline_co_agent(af.applicant_name)
        for name in split_party_names(primary):
            emissions.append((name, "applicant", "applicant_name_field", "high", guess_entity_type(name)))
        agent = inline_agent or af.care_of_agent
        if agent:
            emissions.append(
                (agent, "agent", "applicant_name_field", "high", "company")
            )

    if parsed.refer_by_name:
        primary, inline_agent = extract_inline_co_agent(parsed.refer_by_name)
        for name in split_party_names(primary):
            emissions.append((name, "applicant", "refer_by_name", "medium", guess_entity_type(name)))
        if inline_agent:
            emissions.append(
                (inline_agent, "agent", "refer_by_name", "medium", "company")
            )

    return emissions


# ---------------------------------------------------------------- DB write


def _upsert_application_entity(
    s,
    *,
    application_id: int,
    company_id: int,
    role: str,
    source_doc_id: int | None,
    source_field: str,
    confidence: str,
    is_primary: bool,
) -> None:
    """Idempotent insert into application_entities — keyed on the unique
    (application_id, company_id, role, source_doc_id, extractor) tuple."""
    stmt = mysql_insert(ApplicationEntity).values(
        application_id=application_id,
        company_id=company_id,
        role=role,
        is_primary=is_primary,
        source_doc_id=source_doc_id,
        source_field=source_field,
        extractor=EXTRACTOR,
        confidence=confidence,
        extracted_at=datetime.utcnow(),
    )
    # ON DUPLICATE: refresh confidence + source_field (cheap, lets a
    # later re-parse strengthen the row) but keep `extracted_at` from
    # the first sighting.
    stmt = stmt.on_duplicate_key_update(
        confidence=stmt.inserted.confidence,
        source_field=stmt.inserted.source_field,
        is_primary=stmt.inserted.is_primary,
    )
    s.execute(stmt)


# ---------------------------------------------------------------- driver


def _iter_candidate_docs(s, app_pk: int | None, limit: int | None):
    """Document rows that might be COGC correspondence, ordered for
    deterministic-ish output. Filters at the SQL level on doc_type to
    avoid scanning every form/plan PDF — every COGC correspondence
    doc_type contains one of these substrings.

    Returns docs whether or not `extracted_text` is populated; missing
    text is extracted on-the-fly inside `_harvest` and persisted back
    so subsequent runs are O(SELECT)."""
    q = (
        select(
            CouncilApplicationDocument.id,
            CouncilApplicationDocument.application_id,
            CouncilApplicationDocument.doc_type,
            CouncilApplicationDocument.mime_type,
            CouncilApplicationDocument.file_path,
            CouncilApplicationDocument.extracted_text,
        )
        .where(CouncilApplicationDocument.file_path.is_not(None))
        .where(
            CouncilApplicationDocument.doc_type.ilike("%Decision Notice%")
            | CouncilApplicationDocument.doc_type.ilike("%Confirmation Notice%")
            | CouncilApplicationDocument.doc_type.ilike("%Information Request%")
            | CouncilApplicationDocument.doc_type.ilike("%Cover Letter%")
            | CouncilApplicationDocument.doc_type.ilike("%Response to IR%")
        )
        .order_by(
            CouncilApplicationDocument.application_id,
            CouncilApplicationDocument.id,
        )
    )
    if app_pk is not None:
        q = q.where(CouncilApplicationDocument.application_id == app_pk)
    if limit is not None:
        q = q.limit(limit)
    return s.execute(q).all()


def harvest_application(s, app_pk: int) -> dict:
    """Harvest entities for a single application; returns a stats dict."""
    rows = _iter_candidate_docs(s, app_pk, None)
    return _harvest(s, rows)


def harvest_all(app_pk: int | None = None, limit: int | None = None) -> dict:
    """Walk every (or one) application's candidate docs, harvesting entities."""
    with session_scope() as s:
        rows = _iter_candidate_docs(s, app_pk, limit)
        return _harvest(s, rows)


def _harvest(s, rows: Iterable) -> dict:
    stats = {
        "docs_seen": 0,
        "docs_text_extracted": 0,
        "docs_text_skipped": 0,
        "docs_cogc": 0,
        "emissions": 0,
        "entity_rows_written": 0,
        "companies_seen": set(),
    }

    for r in rows:
        stats["docs_seen"] += 1

        text = r.extracted_text
        if not text:
            ext = extract_text_for_prompt(
                file_path=r.file_path, mime_type=r.mime_type, doc_type=r.doc_type,
            )
            text = ext.text
            if not text:
                stats["docs_text_skipped"] += 1
                continue
            # Persist so the next run skips re-extraction. Same session,
            # commits with everything else at scope-exit.
            s.execute(
                sql_text(
                    "UPDATE council_application_documents "
                    "SET extracted_text = :t WHERE id = :i"
                ),
                {"t": text, "i": r.id},
            )
            stats["docs_text_extracted"] += 1

        parsed = parse_cogc_letter(text)
        if parsed is None or not parsed.is_cogc:
            continue
        stats["docs_cogc"] += 1

        emissions = _extract_emit_plan(parsed)
        if not emissions:
            continue
        stats["emissions"] += len(emissions)

        # Within a single document, the first applicant emission is "primary".
        primary_marked = False
        for name, role, source_field, confidence, etype in emissions:
            company_id = _upsert_company(
                s, display_name=name, entity_type=etype,
            )
            if company_id is None:
                continue
            stats["companies_seen"].add(company_id)
            is_primary = (role == "applicant" and not primary_marked)
            if is_primary:
                primary_marked = True
            _upsert_application_entity(
                s,
                application_id=r.application_id,
                company_id=company_id,
                role=role,
                source_doc_id=r.id,
                source_field=source_field,
                confidence=confidence,
                is_primary=is_primary,
            )
            stats["entity_rows_written"] += 1

    stats["companies_seen"] = len(stats["companies_seen"])
    return stats
