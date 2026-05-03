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
from listo.da_summaries.applicant_letter import (
    ParsedApplicantLetter,
    parse_applicant_letter,
)
from listo.da_summaries.cogc_correspondence import (
    ParsedCorrespondence,
    extract_inline_co_agent,
    guess_entity_type,
    parse_cogc_letter,
    split_party_names,
)
from listo.da_summaries.entity_evidence import (
    DocLayout,
    find_offset_in_text,
    layout_for_span,
    record_evidence,
)
from listo.da_summaries.plan_fingerprints import extract_fingerprints
from listo.da_summaries.plans_title import extract_from_plan_pdf


def _extract_full_pdf_text(file_path: str) -> str:
    """Raw, untruncated PyMuPDF text for the whole document.

    Why: the prompt-shaping helper (`extract_text_for_prompt`) caps
    output at ~12k chars to fit an LLM context. COGC letterhead lives
    in the FOOTER (Council of the City of Gold Coast / phone / email /
    City Development Branch), so any letter >12k chars loses every
    fingerprint and `is_cogc_correspondence` returns False — the
    harvester silently bails. The regex doesn't care about prompt
    budgets; give it the whole text.
    """
    import fitz
    try:
        doc = fitz.open(file_path)
    except Exception:  # noqa: BLE001
        return ""
    try:
        return "\n".join(p.get_text() for p in doc)
    finally:
        doc.close()


PLAN_DOC_TYPES = (
    "%Plans%",
    "%Drawings%",
)


# Bumped when the regex behaviour changes — old predictions stay
# queryable in entity_evidence under the previous version string.
EXTRACTOR_CORRESPONDENCE = "cogc_correspondence_regex_v1"
EXTRACTOR_PLANS = "plans_title_regex_v1"
EXTRACTOR_APPLICANT_LETTER = "applicant_letter_regex_v1"

# Plans larger than this hang pymupdf for minutes (very complex
# vector content). Most house/duplex plans are <5MB; only very
# large multi-stage drawing sets exceed this.
MAX_PLAN_BYTES = 10 * 1024 * 1024  # 10 MB

# Same per-doc cap for correspondence — bundled IR responses can run
# 50+ MB with attached technical reports, but the regex parser only
# needs the cover letter pages. Skip oversize docs rather than burn
# pymupdf time loading multi-hundred-page bundles.
MAX_CORRESPONDENCE_BYTES = 10 * 1024 * 1024  # 10 MB


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


def _extract_emit_applicant(
    parsed: ParsedApplicantLetter,
) -> list[tuple[str, str, str, str, str]]:
    """Turn ParsedApplicantLetter into (name, role, source_field,
    confidence, entity_type) emissions. The applicant firm and its
    individual signatory both get role 'agent' — the firm is acting
    on behalf of the property owner."""
    emissions: list[tuple[str, str, str, str, str]] = []
    if parsed.letterhead_company:
        emissions.append((
            parsed.letterhead_company, "agent", "letterhead", "high", "company",
        ))
    if (
        parsed.signoff_company
        and parsed.signoff_company != parsed.letterhead_company
    ):
        emissions.append((
            parsed.signoff_company, "agent", "signoff_company", "high", "company",
        ))
    if parsed.signoff_name:
        emissions.append((
            parsed.signoff_name, "agent", "signoff_name", "high", "individual",
        ))
    return emissions


# ---------------------------------------------------------------- DB write


def _record_fingerprint(s, *, application_id: int, source_doc_id: int, fp) -> None:
    """Idempotent insert into doc_fingerprints — keyed on
    (source_doc_id, fingerprint_kind, normalized_value). Re-runs
    refresh raw_value/span/layout but never touch resolution fields."""
    import json as _json
    s.execute(
        sql_text("""
            INSERT INTO doc_fingerprints (
                application_id, source_doc_id,
                fingerprint_kind, raw_value, normalized_value,
                span_start, span_end, page_index, layout
            ) VALUES (
                :app_id, :doc_id,
                :kind, :raw, :norm,
                :sstart, :send, :pidx, :layout
            )
            ON DUPLICATE KEY UPDATE
                raw_value  = VALUES(raw_value),
                span_start = VALUES(span_start),
                span_end   = VALUES(span_end),
                page_index = VALUES(page_index),
                layout     = COALESCE(VALUES(layout), layout)
        """),
        {
            "app_id": application_id,
            "doc_id": source_doc_id,
            "kind": fp.kind,
            "raw": fp.raw_value[:500],
            "norm": fp.normalized_value[:255],
            "sstart": fp.span_start,
            "send": fp.span_end,
            "pidx": fp.page_index,
            "layout": _json.dumps(fp.layout) if fp.layout else None,
        },
    )


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


def _iter_candidate_docs(
    s, app_pk: int | None, limit: int | None,
    type_codes: set[str] | None = None,
    max_docs_per_app: int | None = None,
):
    """Document rows that might be COGC correspondence or plan title
    blocks, ordered deterministic-ish.

    `type_codes` (optional) restricts to applications whose
    application_id starts with one of these prefixes (e.g.
    {'MCU','COM','EDA'} for residential redev). Mirrors the
    --types option on `listo council scrape-monthly`.

    `max_docs_per_app` skips applications with more than N total docs.
    Default None (no cap) — big projects (apartment blocks, mixed-use)
    are exactly where most architects/builders/structural engineers
    live, and the per-doc size cap (`MAX_PLAN_BYTES`,
    `MAX_CORRESPONDENCE_BYTES`) already protects against single huge
    files. The 14-doc cap was a proxy for "skip big files" that
    doubled as "skip big projects" — wrong filter for that goal."""
    q = (
        select(
            CouncilApplicationDocument.id,
            CouncilApplicationDocument.application_id,
            CouncilApplicationDocument.doc_type,
            CouncilApplicationDocument.mime_type,
            CouncilApplicationDocument.file_path,
            CouncilApplicationDocument.file_size,
            CouncilApplicationDocument.extracted_text,
        )
        .where(CouncilApplicationDocument.file_path.is_not(None))
        .where(
            CouncilApplicationDocument.doc_type.ilike("%Decision Notice%")
            | CouncilApplicationDocument.doc_type.ilike("%Confirmation Notice%")
            | CouncilApplicationDocument.doc_type.ilike("%Information Request%")
            | CouncilApplicationDocument.doc_type.ilike("%Cover Letter%")
            | CouncilApplicationDocument.doc_type.ilike("%Response to IR%")
            | CouncilApplicationDocument.doc_type.ilike("%Plans%")
            | CouncilApplicationDocument.doc_type.ilike("%Drawings%")
        )
        .order_by(
            CouncilApplicationDocument.application_id,
            CouncilApplicationDocument.id,
        )
    )
    if app_pk is not None:
        q = q.where(CouncilApplicationDocument.application_id == app_pk)
    if type_codes:
        # Filter by the application_id prefix on the parent app row.
        from listo.models import CouncilApplication
        # Use OR of LIKE for each prefix (stays index-friendly).
        like_clauses = [
            CouncilApplication.application_id.like(f"{tc}/%")
            for tc in sorted(type_codes)
        ]
        from sqlalchemy import or_
        q = q.join(
            CouncilApplication,
            CouncilApplication.id == CouncilApplicationDocument.application_id,
        ).where(or_(*like_clauses))
    if max_docs_per_app:
        # Subquery: app_ids with ≤ N total docs.
        from sqlalchemy import func, select as _select
        small_app_ids = (
            _select(CouncilApplicationDocument.application_id)
            .group_by(CouncilApplicationDocument.application_id)
            .having(func.count(CouncilApplicationDocument.id) <= max_docs_per_app)
            .scalar_subquery()
        )
        q = q.where(CouncilApplicationDocument.application_id.in_(small_app_ids))
    if limit is not None:
        q = q.limit(limit)
    return s.execute(q).all()


def harvest_application(s, app_pk: int) -> dict:
    """Harvest entities for a single application; returns a stats dict."""
    rows = _iter_candidate_docs(s, app_pk, None)
    return _harvest(s, rows)


def harvest_all(
    app_pk: int | None = None,
    limit: int | None = None,
    type_codes: set[str] | None = None,
    max_docs_per_app: int | None = None,
) -> dict:
    """Walk every (or one) application's candidate docs, harvesting entities."""
    with session_scope() as s:
        rows = _iter_candidate_docs(
            s, app_pk, limit,
            type_codes=type_codes,
            max_docs_per_app=max_docs_per_app,
        )
        return _harvest(s, rows)


def _is_plan_doc(doc_type: str | None) -> bool:
    dt = (doc_type or "").lower()
    return "plan" in dt or "drawing" in dt


def _harvest(s, rows: Iterable) -> dict:
    stats = {
        "docs_seen": 0,
        "docs_text_extracted": 0,
        "docs_text_skipped": 0,
        "docs_cogc": 0,
        "docs_applicant_letter": 0,
        "docs_plans_with_block": 0,
        # Plans we scanned but couldn't extract any entity from. By law
        # every plan PDF must identify a builder/architect/owner-builder,
        # so this counter is a recall-failure metric — review candidates
        # for ML training priorities.
        "docs_plans_no_hit": 0,
        "docs_plans_skipped_large": 0,
        "docs_errored": 0,
        "emissions": 0,
        "entity_rows_written": 0,
        "evidence_rows": 0,
        "fingerprints_recorded": 0,
        "companies_seen": set(),
    }

    # Materialise rows so we can commit-per-doc without holding the
    # cursor open across commits.
    rows = list(rows)
    total = len(rows)

    for i, r in enumerate(rows, 1):
        stats["docs_seen"] += 1

        # Periodic progress log so a long run is observable from the
        # tail of the log file.
        if i == 1 or i % 50 == 0 or i == total:
            logger.info(
                "progress: %d/%d  evidence=%d  fingerprints=%d  companies=%d",
                i, total, stats["evidence_rows"],
                stats["fingerprints_recorded"], len(stats["companies_seen"]),
            )

        try:
            _harvest_one(s, r, stats)
            # Commit per-doc. A bad PDF doesn't take the whole run down,
            # and DB watchers can see progress in real time.
            s.commit()
        except Exception as exc:  # noqa: BLE001
            stats["docs_errored"] += 1
            logger.warning(
                "doc %s (app=%s) errored: %s",
                r.file_path, r.application_id, exc,
            )
            s.rollback()
            continue

    stats["companies_seen"] = len(stats["companies_seen"])
    return stats


def _harvest_one(s, r, stats: dict) -> None:
    """Process a single doc — record fingerprints + entity_evidence +
    application_entities. Caller wraps in try/except + commit/rollback
    so per-doc failures don't break the run.
    """
    if _is_plan_doc(r.doc_type):
        # Skip oversized plans — pymupdf hangs on huge / complex
        # ones and our 12-page scan doesn't help when the file
        # itself is slow to open.
        if r.file_size and r.file_size > MAX_PLAN_BYTES:
            stats["docs_plans_skipped_large"] += 1
            logger.info(
                "plan doc %s skipped (file_size=%.1fMB > %dMB cap)",
                r.file_path, r.file_size / 1024 / 1024, MAX_PLAN_BYTES // 1024 // 1024,
            )
            return
        hits, page_layouts = extract_from_plan_pdf(r.file_path)

        # Always capture identity fingerprints (URL / email / licence /
        # ACN / ABN / phone) from every plan page, regardless of
        # whether a named entity was extracted. These are joined to
        # other docs later to resolve logo-only architect names.
        fp_count = 0
        for pl in page_layouts:
            for fp in extract_fingerprints(pl):
                _record_fingerprint(
                    s,
                    application_id=r.application_id,
                    source_doc_id=r.id,
                    fp=fp,
                )
                fp_count += 1
        stats["fingerprints_recorded"] += fp_count

        if not hits:
            stats["docs_plans_no_hit"] += 1
            logger.warning(
                "plan doc %s (app=%s) yielded 0 named entities (%d fingerprints)",
                r.file_path, r.application_id, fp_count,
            )
            return
        stats["docs_plans_with_block"] += 1
        for hit in hits:
            # Record evidence first — independent of upsert success so
            # the training set captures every regex emission.
            page_idx = hit.page - 1
            layout = None
            if 0 <= page_idx < len(page_layouts):
                layout = layout_for_span(
                    page_layouts[page_idx], hit.span_start, hit.span_end,
                )
            record_evidence(
                s,
                application_id=r.application_id,
                source_doc_id=r.id,
                extractor=EXTRACTOR_PLANS,
                source_text=hit.page_text,
                span_start=hit.span_start,
                span_end=hit.span_end,
                candidate_name=hit.name,
                candidate_role=hit.role,
                confidence=hit.confidence,
                layout=layout,
            )
            stats["evidence_rows"] += 1

            company_id = _upsert_company(
                s,
                display_name=hit.name,
                acn=hit.acn,
                abn=hit.abn,
                entity_type="company",
            )
            if company_id is None:
                continue
            stats["companies_seen"].add(company_id)
            stats["emissions"] += 1
            _upsert_application_entity(
                s,
                application_id=r.application_id,
                company_id=company_id,
                role=hit.role,
                source_doc_id=r.id,
                source_field="plans_title_block",
                confidence=hit.confidence,
                is_primary=False,
            )
            stats["entity_rows_written"] += 1
        return

    # Correspondence path — same per-doc size guard as plans. Bundled
    # IR responses can be 30-50 MB; the regex parser only needs the
    # cover-letter pages, not the attached technical reports.
    if r.file_size and r.file_size > MAX_CORRESPONDENCE_BYTES:
        stats["docs_text_skipped"] += 1
        logger.info(
            "correspondence doc %s skipped (file_size=%.1fMB > %dMB cap)",
            r.file_path, r.file_size / 1024 / 1024,
            MAX_CORRESPONDENCE_BYTES // 1024 // 1024,
        )
        return

    # Pull text once, cache, parse with the COGC letter parser.
    # Always read raw (un-truncated) PDF text: `cached_extracted_text`
    # may be the prompt-shaped 12k-cap version from earlier runs,
    # which loses the COGC footer fingerprints.
    text = _extract_full_pdf_text(r.file_path)
    if not text:
        stats["docs_text_skipped"] += 1
        return
    # Persist the un-truncated text so summariser/aggregator stages
    # share the same cache. Earlier truncated values get overwritten.
    if text != r.extracted_text:
        s.execute(
            sql_text(
                "UPDATE council_application_documents "
                "SET extracted_text = :t WHERE id = :i"
            ),
            {"t": text, "i": r.id},
        )
        stats["docs_text_extracted"] += 1

    # Try COGC first. is_cogc can over-fire on applicant letters that
    # mention council in their recipient block, so we only commit to
    # COGC if the parser actually produces emissions. Otherwise fall
    # through to the applicant-letter parser (which has its own
    # stricter `looks_like_council_authored` gate).
    cogc_emissions: list[tuple[str, str, str, str, str]] = []
    parsed = parse_cogc_letter(text)
    if parsed is not None and parsed.is_cogc:
        cogc_emissions = _extract_emit_plan(parsed)

    if cogc_emissions:
        stats["docs_cogc"] += 1
        _emit_correspondence_rows(
            s, r=r, text=text, emissions=cogc_emissions,
            extractor=EXTRACTOR_CORRESPONDENCE, stats=stats,
        )
        return

    parsed_applicant = parse_applicant_letter(text)
    if parsed_applicant is None:
        return
    stats["docs_applicant_letter"] += 1
    emissions = _extract_emit_applicant(parsed_applicant)
    _emit_correspondence_rows(
        s, r=r, text=text, emissions=emissions,
        extractor=EXTRACTOR_APPLICANT_LETTER, stats=stats,
    )


def _emit_correspondence_rows(
    s, *,
    r,
    text: str,
    emissions: list[tuple[str, str, str, str, str]],
    extractor: str,
    stats: dict,
) -> None:
    """Persist evidence + application_entity rows for a flat list of
    correspondence emissions. Shared by the COGC and applicant-letter
    paths.

    `emissions` is a list of (name, role, source_field, confidence,
    entity_type) tuples — same shape produced by `_extract_emit_plan`
    and `_extract_emit_applicant`.
    """
    if not emissions:
        return
    stats["emissions"] += len(emissions)

    # Lazily load layout-aware view of the doc — only worth it if
    # the parser actually emitted anything. One PDF reopen per doc,
    # not per emission.
    doc_layout: DocLayout | None = None
    doc_layout_attempted = False

    # Within a single document, the first applicant emission is "primary".
    primary_marked = False
    for name, role, source_field, confidence, etype in emissions:
        evidence_text = text
        evidence_span: tuple[int, int] | None = None
        evidence_layout: dict | None = None

        if not doc_layout_attempted:
            doc_layout = DocLayout.from_pdf(r.file_path)
            doc_layout_attempted = True

        if doc_layout is not None:
            hit = doc_layout.find_layout_for_name(name)
            if hit is not None:
                evidence_span = (hit[0], hit[1])
                evidence_layout = hit[2]
                evidence_text = doc_layout.concat_text

        if evidence_span is None:
            evidence_span = find_offset_in_text(text, name)

        if evidence_span is not None:
            record_evidence(
                s,
                application_id=r.application_id,
                source_doc_id=r.id,
                extractor=extractor,
                source_text=evidence_text,
                span_start=evidence_span[0],
                span_end=evidence_span[1],
                candidate_name=name,
                candidate_role=role,
                confidence=confidence,
                layout=evidence_layout,
            )
            stats["evidence_rows"] += 1

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
