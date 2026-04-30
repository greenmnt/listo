"""Phase 1 — first-and-last-doc summarisation.

For every council_application that has at least one downloaded PDF, we
pick the earliest published doc (typically `DA Form 1`) and the latest
(typically `Signed Decision Notice`), extract their text, run them
through the LLM, and persist a row per (document, prompt_version) into
`da_doc_summaries`.

Resume: a doc is skipped if a row for it already exists under the
current `prompt_version`. `--force` deletes existing rows first.

Two-machine partitioning: `--computer-index N --computer-count M` filters
documents by `id % count = index`.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

import hashlib

from sqlalchemy import select, text as sql_text, delete

from listo.db import session_scope
from listo.models import (
    CouncilApplication,
    CouncilApplicationDocument,
    DaDocSummary,
)
from listo.da_summaries.classify import (
    SKIP_TREATMENTS,
    get_or_compute_features,
    pick_template_key,
)
from listo.da_summaries.client import OllamaExtractor, OllamaError
from listo.da_summaries.prompts import TEMPLATES, register_templates, render
from listo.da_summaries.schemas import PROMPT_VERSION
from listo.da_summaries.text_extract import extract_text_for_prompt


logger = logging.getLogger(__name__)


@dataclass
class RunStats:
    apps_visited: int = 0
    docs_processed: int = 0
    docs_skipped_no_text: int = 0
    docs_skipped_already_done: int = 0
    docs_failed: int = 0


def _select_pending_apps(
    *,
    council_slug: str | None,
    app_id_str: str | None,
    limit: int | None,
    computer_index: int,
    computer_count: int,
    prompt_version: str,
    force: bool,
) -> list[tuple[int, str]]:
    """Return [(application_pk, application_id_str), ...] for apps that
    don't yet have both first+last summaries under this prompt_version.

    Modulo partitioning is applied at the application level so each
    machine takes a disjoint set of *applications* — keeps the
    'first+last pair' coupled on one machine.
    """
    conditions = [
        "ca.docs_fetched_at IS NOT NULL",
        f"ca.id % :count = :index",
    ]
    params: dict[str, object] = {"count": computer_count, "index": computer_index, "v": prompt_version}
    if council_slug:
        conditions.append("ca.council_slug = :slug")
        params["slug"] = council_slug
    if app_id_str:
        conditions.append("ca.application_id = :appid")
        params["appid"] = app_id_str

    # Apps with at least one downloaded PDF...
    # ...minus apps with 2 (first+last) doc-summaries already present
    # under this prompt_version (or 1 if the app has only one PDF).
    where = " AND ".join(conditions)
    sql = sql_text(f"""
        SELECT ca.id, ca.application_id
          FROM council_applications ca
         WHERE {where}
           AND EXISTS (
             SELECT 1 FROM council_application_documents cad
              WHERE cad.application_id = ca.id
                AND cad.file_path IS NOT NULL
                AND cad.mime_type LIKE 'application/pdf%%'
           )
           AND (
             :force = 1
             OR (SELECT COUNT(*) FROM da_doc_summaries dds
                  WHERE dds.application_id = ca.id
                    AND dds.prompt_version = :v
                    AND dds.tier = 1) < 2
           )
         ORDER BY ca.id ASC
         {f'LIMIT {int(limit)}' if limit else ''}
    """)
    params["force"] = 1 if force else 0
    with session_scope() as s:
        rows = s.execute(sql, params).fetchall()
        return [(r[0], r[1]) for r in rows]


def _select_first_last_docs(app_pk: int) -> list[tuple[CouncilApplicationDocument, str]]:
    """Return up to two (doc, position) pairs — 'first' and 'last' — per app.

    Falls back to id ordering if `published_at` is NULL on any candidate.
    """
    with session_scope() as s:
        # Earliest by published_at, fallback to id.
        first = s.execute(
            select(CouncilApplicationDocument).where(
                CouncilApplicationDocument.application_id == app_pk,
                CouncilApplicationDocument.file_path.is_not(None),
                CouncilApplicationDocument.mime_type.like("application/pdf%"),
            ).order_by(
                CouncilApplicationDocument.published_at.is_(None),
                CouncilApplicationDocument.published_at.asc(),
                CouncilApplicationDocument.id.asc(),
            ).limit(1)
        ).scalar_one_or_none()

        last = s.execute(
            select(CouncilApplicationDocument).where(
                CouncilApplicationDocument.application_id == app_pk,
                CouncilApplicationDocument.file_path.is_not(None),
                CouncilApplicationDocument.mime_type.like("application/pdf%"),
            ).order_by(
                CouncilApplicationDocument.published_at.desc(),
                CouncilApplicationDocument.id.desc(),
            ).limit(1)
        ).scalar_one_or_none()

        if first is None and last is None:
            return []
        if first is not None and last is not None and first.id == last.id:
            # Single-PDF app — only summarise once, mark as 'first'.
            s.expunge(first)
            return [(first, "first")]
        out: list[tuple[CouncilApplicationDocument, str]] = []
        if first is not None:
            s.expunge(first)
            out.append((first, "first"))
        if last is not None:
            s.expunge(last)
            out.append((last, "last"))
        return out


def _existing_summary_doc_ids(app_pk: int, prompt_version: str, tier: int = 1) -> set[int]:
    with session_scope() as s:
        rows = s.execute(
            select(DaDocSummary.document_id).where(
                DaDocSummary.application_id == app_pk,
                DaDocSummary.prompt_version == prompt_version,
                DaDocSummary.tier == tier,
            )
        ).all()
        return {r[0] for r in rows}


def _process_one_doc(
    extractor: OllamaExtractor,
    *,
    app_pk: int,
    app_id_str: str,
    doc: CouncilApplicationDocument,
    position: str,
    tier: int,
    prompt_version: str,
) -> str:
    """Returns a single-letter status: 'p' processed, 's' skipped no-text, 'f' failed."""
    # 1. Classify the doc by features (cached). Features pick the
    # template; doc_type only resolves narrative-vs-decision.
    features = get_or_compute_features(
        document_id=doc.id, file_path=doc.file_path, mime_type=doc.mime_type,
    )
    template_key = pick_template_key(features=features, doc_type=doc.doc_type)

    now = datetime.utcnow()

    # 2. Skip image-only / unsupported docs without invoking the LLM.
    if template_key is None or features.treatment in SKIP_TREATMENTS:
        with session_scope() as s:
            s.add(DaDocSummary(
                document_id=doc.id,
                application_id=app_pk,
                doc_type=doc.doc_type,
                doc_position=position,
                tier=tier,
                model="(skipped)",
                prompt_version=prompt_version,
                template_key=None,
                summarised_at=now,
                text_chars=features.total_text_chars,
                text_sha256=None,
                pages_used=None,
                extraction_method="skipped",
                extraction_notes=f"treatment={features.treatment}: {features.extraction_notes or 'no LLM-readable content'}",
                raw_response_json={"skipped": True, "treatment": features.treatment},
            ))
        logger.info(
            "  [%s/%s] %s %s — skip (treatment=%s)",
            app_id_str, doc.id, position, doc.doc_type, features.treatment,
        )
        return "s"

    # 3. Extract text for the LLM.
    extracted = extract_text_for_prompt(
        file_path=doc.file_path,
        mime_type=doc.mime_type,
        doc_type=doc.doc_type,
    )

    if extracted.text is None:
        # text_extract disagreed with classify (rare). Persist as skipped.
        with session_scope() as s:
            s.add(DaDocSummary(
                document_id=doc.id,
                application_id=app_pk,
                doc_type=doc.doc_type,
                doc_position=position,
                tier=tier,
                model="(skipped)",
                prompt_version=prompt_version,
                template_key=template_key,
                summarised_at=now,
                text_chars=0,
                text_sha256=None,
                pages_used=extracted.pages_used,
                extraction_method=extracted.method,
                extraction_notes=extracted.notes,
                raw_response_json={"skipped": True, "reason": extracted.notes},
            ))
        logger.info("  [%s/%s] %s %s — skipped: %s", app_id_str, doc.id, position, doc.doc_type, extracted.notes)
        return "s"

    # Use the feature-classified template_key (chosen above), not the
    # doc_type-only fallback.
    tpl, user = render(
        prompt_version=prompt_version,
        template_key=template_key,
        text=extracted.text,
        app_id=app_id_str,
    )
    text_sha256 = hashlib.sha256(extracted.text.encode("utf-8")).digest()
    try:
        result = extractor.extract(system=tpl.system_prompt, user=user)
    except OllamaError as exc:
        logger.warning("  [%s/%s] %s — Ollama error: %s", app_id_str, doc.id, position, exc)
        return "f"

    f = result.facts
    with session_scope() as s:
        s.add(DaDocSummary(
            document_id=doc.id,
            application_id=app_pk,
            doc_type=doc.doc_type,
            doc_position=position,
            tier=tier,
            model=result.model,
            prompt_version=prompt_version,
            template_key=tpl.template_key,
            summarised_at=now,
            text_chars=len(extracted.text),
            text_sha256=text_sha256,
            pages_used=extracted.pages_used,
            extraction_method=extracted.method,
            extraction_notes=None,
            applicant_name=f.applicant_name,
            applicant_acn=getattr(f, "applicant_acn", None),
            applicant_abn=getattr(f, "applicant_abn", None),
            applicant_entity_type=getattr(f, "applicant_entity_type", None),
            applicant_agent_name=getattr(f, "applicant_agent_name", None),
            builder_name=f.builder_name,
            architect_name=f.architect_name,
            owner_name=f.owner_name,
            owner_acn=getattr(f, "owner_acn", None),
            owner_abn=getattr(f, "owner_abn", None),
            owner_entity_type=getattr(f, "owner_entity_type", None),
            dwelling_count=f.dwelling_count,
            dwelling_kind=f.dwelling_kind.value if f.dwelling_kind else None,
            project_description=f.project_description,
            lot_on_plan=f.lot_on_plan,
            street_address=f.street_address,
            confidence=f.confidence,
            raw_response_json=result.raw_response,
        ))
    logger.info(
        "  [%s/%s] %s %s — kind=%s count=%s confidence=%s",
        app_id_str, doc.id, position, doc.doc_type,
        f.dwelling_kind.value if f.dwelling_kind else "?",
        f.dwelling_count, f.confidence,
    )
    return "p"


def run(
    *,
    council_slug: str | None = None,
    app_id_str: str | None = None,
    limit: int | None = None,
    force: bool = False,
    model: str | None = None,
    prompt_version: str = PROMPT_VERSION,
    computer_index: int = 0,
    computer_count: int = 1,
) -> RunStats:
    """Phase 1 runner."""
    if computer_count < 1 or not (0 <= computer_index < computer_count):
        raise ValueError(f"bad partition: index={computer_index} count={computer_count}")

    register_templates(prompt_version)
    extractor = OllamaExtractor(model=model)
    stats = RunStats()

    if force and (app_id_str or council_slug):
        # Wipe existing tier-1 rows for the targeted scope so we re-run cleanly.
        with session_scope() as s:
            scope = sql_text("""
                DELETE dds FROM da_doc_summaries dds
                  JOIN council_applications ca ON ca.id = dds.application_id
                 WHERE dds.tier = 1 AND dds.prompt_version = :v
                   AND (:slug IS NULL OR ca.council_slug = :slug)
                   AND (:appid IS NULL OR ca.application_id = :appid)
            """)
            s.execute(scope, {"v": prompt_version, "slug": council_slug, "appid": app_id_str})

    apps = _select_pending_apps(
        council_slug=council_slug,
        app_id_str=app_id_str,
        limit=limit,
        computer_index=computer_index,
        computer_count=computer_count,
        prompt_version=prompt_version,
        force=force,
    )

    logger.info(
        "phase 1: %d apps to summarise (machine %d/%d, model=%s, prompt=%s)",
        len(apps), computer_index, computer_count, extractor.model, prompt_version,
    )

    for app_pk, app_id in apps:
        stats.apps_visited += 1
        already = _existing_summary_doc_ids(app_pk, prompt_version, tier=1)
        for doc, position in _select_first_last_docs(app_pk):
            if doc.id in already:
                stats.docs_skipped_already_done += 1
                continue
            r = _process_one_doc(
                extractor,
                app_pk=app_pk, app_id_str=app_id, doc=doc, position=position,
                tier=1, prompt_version=prompt_version,
            )
            if r == "p":
                stats.docs_processed += 1
            elif r == "s":
                stats.docs_skipped_no_text += 1
            elif r == "f":
                stats.docs_failed += 1

    return stats
