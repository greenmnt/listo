"""Phase 2.5 — build-features extraction.

Walks an application's build-relevant documents (Drawings, Stamped
Approved Plans, Plans, Supporting Documents, Specialist Reports), splits
each into overlapping page chunks, runs each chunk through the
appropriate v4 build_features template, and writes the JSON output to
`da_build_features`.

Mirrors the shape of summarise.py / escalate.py:
- Idempotent per (document_id, prompt_version, template_key, chunk_index).
  Re-runs skip already-extracted chunks unless --force.
- Modulo partitioning across N machines via (computer_index, computer_count).
- Returns counts of apps/docs/chunks visited / skipped / failed.

This is the build lane only. Risk flags + council correspondence get
their own runners (Stage 2).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, text as sql_text

from listo.db import session_scope
from listo.models import (
    CouncilApplication,
    CouncilApplicationDocument,
    DaBuildFeatures,
)
from listo.da_summaries.client import OllamaError, OllamaExtractor
from listo.da_summaries.chunking import chunk_pages
from listo.da_summaries.prompts import (
    BUILD_DOC_TYPES,
    register_templates,
    render,
    select_build_template_key,
)
from listo.da_summaries.schemas import BuildFeatures
from listo.da_summaries.text_extract import extract_per_page


logger = logging.getLogger(__name__)


BUILD_PROMPT_VERSION = "v4"


@dataclass
class FeaturesStats:
    apps_visited: int = 0
    docs_processed: int = 0
    docs_skipped_no_text: int = 0
    chunks_processed: int = 0
    chunks_failed: int = 0


def _select_apps(
    *,
    council_slug: str | None,
    app_id_str: str | None,
    limit: int | None,
    computer_index: int,
    computer_count: int,
) -> list[tuple[int, str]]:
    """Apps that have a da_summaries row (i.e. were summarised) and at
    least one BUILD_DOC_TYPES document on file. Order by id for
    deterministic resume."""
    conditions = [
        "ds.application_id = ca.id",
        "ca.id % :count = :index",
        "EXISTS ("
        "  SELECT 1 FROM council_application_documents cad "
        "   WHERE cad.application_id = ca.id "
        "     AND cad.file_path IS NOT NULL "
        "     AND ("
        + " OR ".join(f"cad.doc_type LIKE '%{dt}%'" for dt in BUILD_DOC_TYPES)
        + ")"
        ")"
    ]
    params: dict[str, object] = {"count": computer_count, "index": computer_index}
    if council_slug:
        conditions.append("ca.council_slug = :slug")
        params["slug"] = council_slug
    if app_id_str:
        conditions.append("ca.application_id = :appid")
        params["appid"] = app_id_str

    where = " AND ".join(conditions)
    sql = sql_text(f"""
        SELECT ca.id, ca.application_id
          FROM da_summaries ds
          JOIN council_applications ca ON ca.id = ds.application_id
         WHERE {where}
         ORDER BY ca.id ASC
         {f'LIMIT {int(limit)}' if limit else ''}
    """)
    with session_scope() as s:
        rows = s.execute(sql, params).fetchall()
        return [(r[0], r[1]) for r in rows]


def _pick_build_docs(app_pk: int) -> list[CouncilApplicationDocument]:
    """All build-relevant docs for an app that have a file on disk."""
    or_clause = " OR ".join(f"doc_type LIKE '%{dt}%'" for dt in BUILD_DOC_TYPES)
    sql = sql_text(f"""
        SELECT id FROM council_application_documents
         WHERE application_id = :app_pk
           AND file_path IS NOT NULL
           AND ({or_clause})
         ORDER BY published_at IS NULL, published_at ASC, id ASC
    """)
    with session_scope() as s:
        ids = [r[0] for r in s.execute(sql, {"app_pk": app_pk}).fetchall()]
        if not ids:
            return []
        docs = s.execute(
            select(CouncilApplicationDocument).where(CouncilApplicationDocument.id.in_(ids))
        ).scalars().all()
        for d in docs:
            s.expunge(d)
        ordered = {d.id: d for d in docs}
        return [ordered[i] for i in ids if i in ordered]


def _existing_chunk_indices(
    *, document_id: int, prompt_version: str, template_key: str
) -> set[int]:
    """Already-extracted chunk indices for this (doc, version, template)."""
    sql = sql_text("""
        SELECT chunk_index FROM da_build_features
         WHERE document_id = :doc_id
           AND prompt_version = :v
           AND template_key = :tk
    """)
    with session_scope() as s:
        return {
            r[0] for r in s.execute(
                sql, {"doc_id": document_id, "v": prompt_version, "tk": template_key}
            ).fetchall()
        }


def _process_one_chunk(
    extractor: OllamaExtractor,
    *,
    app_pk: int,
    app_id_str: str,
    doc: CouncilApplicationDocument,
    template_key: str,
    chunk_index: int,
    page_start: int,
    page_end: int,
    text: str,
    extraction_method: str,
    prompt_version: str,
    now: datetime,
) -> str:
    """Run the LLM on one chunk and write a row. Returns 'p' / 'f'."""
    tpl, user = render(
        prompt_version=prompt_version,
        template_key=template_key,
        text=text,
        app_id=app_id_str,
        extra={"page_start": page_start, "page_end": page_end},
    )
    try:
        result = extractor.extract_as(
            BuildFeatures, system=tpl.system_prompt, user=user
        )
    except OllamaError as exc:
        logger.warning(
            "  [%s/doc=%d/chunk=%d] Ollama error: %s",
            app_id_str, doc.id, chunk_index, exc,
        )
        return "f"

    f = result.parsed
    with session_scope() as s:
        s.add(DaBuildFeatures(
            application_id=app_pk,
            document_id=doc.id,
            doc_type=doc.doc_type,
            prompt_version=prompt_version,
            template_key=template_key,
            model=result.model,
            chunk_index=chunk_index,
            page_start=page_start,
            page_end=page_end,
            extracted_at=now,
            extraction_method=extraction_method,
            text_chars=len(text),
            gfa_m2=f.gfa_m2,
            site_area_m2=f.site_area_m2,
            internal_area_m2=f.internal_area_m2,
            external_area_m2=f.external_area_m2,
            levels=f.levels,
            has_basement=f.has_basement,
            garage_spaces=f.garage_spaces,
            bedrooms=f.bedrooms,
            bathrooms=f.bathrooms,
            materials_walls=f.materials_walls,
            materials_roof=f.materials_roof,
            materials_floor=f.materials_floor,
            fittings_quality=f.fittings_quality,
            fittings_notes=f.fittings_notes,
            landscaping_summary=f.landscaping_summary,
            plant_species_json=list(f.plant_species) if f.plant_species else None,
            has_pool=f.has_pool,
            confidence=f.confidence,
            notes=f.notes,
            raw_response_json=result.raw_response,
        ))
    logger.info(
        "  [%s/doc=%d/chunk=%d pp%d-%d] %s — gfa=%s levels=%s garage=%s pool=%s conf=%s",
        app_id_str, doc.id, chunk_index, page_start, page_end,
        template_key, f.gfa_m2, f.levels, f.garage_spaces, f.has_pool, f.confidence,
    )
    return "p"


def _process_one_doc(
    extractor: OllamaExtractor,
    *,
    app_pk: int,
    app_id_str: str,
    doc: CouncilApplicationDocument,
    prompt_version: str,
    force: bool,
    chunk_size: int,
    chunk_overlap: int,
    stats: FeaturesStats,
) -> None:
    extracted = extract_per_page(file_path=doc.file_path, mime_type=doc.mime_type)
    if not extracted.pages or extracted.method == "skipped":
        logger.info(
            "  [%s/doc=%d] %s — skipped: %s",
            app_id_str, doc.id, doc.doc_type, extracted.notes,
        )
        stats.docs_skipped_no_text += 1
        return

    template_key = select_build_template_key(doc.doc_type)
    chunks = chunk_pages(extracted.pages, size=chunk_size, overlap=chunk_overlap)
    if not chunks:
        logger.info(
            "  [%s/doc=%d] %s — no usable chunks (all pages below %d chars)",
            app_id_str, doc.id, doc.doc_type, 200,
        )
        stats.docs_skipped_no_text += 1
        return

    already = (
        set() if force
        else _existing_chunk_indices(
            document_id=doc.id, prompt_version=prompt_version, template_key=template_key
        )
    )
    to_run = [c for c in chunks if c.chunk_index not in already]
    if not to_run:
        logger.info(
            "  [%s/doc=%d] %s — all %d chunks already extracted (use --force to redo)",
            app_id_str, doc.id, doc.doc_type, len(chunks),
        )
        return

    stats.docs_processed += 1
    now = datetime.utcnow()
    for chunk in to_run:
        r = _process_one_chunk(
            extractor,
            app_pk=app_pk, app_id_str=app_id_str, doc=doc,
            template_key=template_key,
            chunk_index=chunk.chunk_index,
            page_start=chunk.page_start, page_end=chunk.page_end,
            text=chunk.text,
            extraction_method=extracted.method,
            prompt_version=prompt_version, now=now,
        )
        if r == "p":
            stats.chunks_processed += 1
        else:
            stats.chunks_failed += 1


def run(
    *,
    council_slug: str | None = None,
    app_id_str: str | None = None,
    limit: int | None = None,
    force: bool = False,
    model: str | None = None,
    prompt_version: str = BUILD_PROMPT_VERSION,
    chunk_size: int = 5,
    chunk_overlap: int = 1,
    computer_index: int = 0,
    computer_count: int = 1,
) -> FeaturesStats:
    """Phase 2.5 runner — build-features extraction across one or many DAs."""
    register_templates(prompt_version)
    extractor = OllamaExtractor(model=model)
    stats = FeaturesStats()

    apps = _select_apps(
        council_slug=council_slug,
        app_id_str=app_id_str,
        limit=limit,
        computer_index=computer_index,
        computer_count=computer_count,
    )
    logger.info(
        "phase 2.5 (features): %d apps to process (machine %d/%d, model=%s, prompt=%s, chunk=%d/%d)",
        len(apps), computer_index, computer_count,
        model or "(default)", prompt_version, chunk_size, chunk_overlap,
    )

    for app_pk, app_id in apps:
        stats.apps_visited += 1
        docs = _pick_build_docs(app_pk)
        if not docs:
            continue
        for doc in docs:
            _process_one_doc(
                extractor,
                app_pk=app_pk, app_id_str=app_id, doc=doc,
                prompt_version=prompt_version, force=force,
                chunk_size=chunk_size, chunk_overlap=chunk_overlap,
                stats=stats,
            )

    return stats
