"""Phase 2 — escalate incomplete DAs to tier-2 priority docs.

When phase-1 (first+last) doesn't yield enough info, fetch up to N more
docs in priority order and run them through the LLM. Limit prevents the
worst case of summarising 30 docs for a single app.

Order picks docs richest in the missing fields first:
  1. Specialist Reports — verbose project description + parties
  2. Stamped Approved Plans — title-block (architect, dwellings)
  3. Plans — same as above but pre-approval
  4. Amended DA Form 1 — updated applicant/builder
  5. Supporting Documents — varied
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, text as sql_text

from listo.db import session_scope
from listo.models import (
    CouncilApplicationDocument,
    DaDocSummary,
    DaSummary,
)
from listo.da_summaries.client import OllamaExtractor
from listo.da_summaries.prompts import register_templates
from listo.da_summaries.schemas import PROMPT_VERSION
from listo.da_summaries.summarise import _process_one_doc


logger = logging.getLogger(__name__)


# MySQL FIELD() priority list — higher position = higher priority.
TIER2_PRIORITY = [
    "Specialist Reports",
    "Stamped Approved Plans",
    "Plans",
    "Amended DA Form 1",
    "Supporting Documents",
    "Cover Letter",
]


@dataclass
class EscalateStats:
    apps_visited: int = 0
    docs_processed: int = 0
    docs_skipped_no_text: int = 0
    docs_failed: int = 0


def _select_incomplete_apps(
    *,
    council_slug: str | None,
    app_id_str: str | None,
    limit: int | None,
    computer_index: int,
    computer_count: int,
    force: bool,
) -> list[tuple[int, str]]:
    """Apps with status='incomplete' (or 'escalated' if --force)."""
    conditions = [
        "ds.application_id = ca.id",
        "ca.id % :count = :index",
    ]
    params: dict[str, object] = {"count": computer_count, "index": computer_index}
    if force:
        conditions.append("ds.status IN ('incomplete', 'escalated')")
    else:
        conditions.append("ds.status = 'incomplete'")
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


def _pick_tier2_docs(
    app_pk: int, *, max_docs: int, prompt_version: str
) -> list[CouncilApplicationDocument]:
    """Up to `max_docs` PDF docs that haven't been summarised yet, in priority order."""
    # Build a CASE for FIELD-equivalent priority ranking.
    order_clause = " ".join(
        f"WHEN cad.doc_type LIKE '%{dt}%' THEN {len(TIER2_PRIORITY) - i}"
        for i, dt in enumerate(TIER2_PRIORITY)
    )
    sql = sql_text(f"""
        SELECT cad.id
          FROM council_application_documents cad
         WHERE cad.application_id = :app_pk
           AND cad.file_path IS NOT NULL
           AND cad.mime_type LIKE 'application/pdf%%'
           AND NOT EXISTS (
             SELECT 1 FROM da_doc_summaries dds
              WHERE dds.document_id = cad.id
                AND dds.prompt_version = :v
           )
           AND (
             {' OR '.join(f"cad.doc_type LIKE '%{dt}%'" for dt in TIER2_PRIORITY)}
           )
         ORDER BY (CASE {order_clause} ELSE 0 END) DESC,
                  cad.published_at ASC, cad.id ASC
         LIMIT :limit
    """)
    with session_scope() as s:
        ids = [r[0] for r in s.execute(sql, {"app_pk": app_pk, "v": prompt_version, "limit": max_docs}).fetchall()]
        if not ids:
            return []
        docs = s.execute(
            select(CouncilApplicationDocument).where(CouncilApplicationDocument.id.in_(ids))
        ).scalars().all()
        for d in docs:
            s.expunge(d)
        # Preserve the priority order from the SQL above.
        ordered = {d.id: d for d in docs}
        return [ordered[i] for i in ids if i in ordered]


def run(
    *,
    council_slug: str | None = None,
    app_id_str: str | None = None,
    limit: int | None = None,
    max_tier2_docs: int = 3,
    force: bool = False,
    model: str | None = None,
    prompt_version: str = PROMPT_VERSION,
    computer_index: int = 0,
    computer_count: int = 1,
) -> EscalateStats:
    """Phase 2 runner. Run aggregate after this to update da_summaries.status."""
    register_templates(prompt_version)
    extractor = OllamaExtractor(model=model)
    stats = EscalateStats()

    apps = _select_incomplete_apps(
        council_slug=council_slug,
        app_id_str=app_id_str,
        limit=limit,
        computer_index=computer_index,
        computer_count=computer_count,
        force=force,
    )
    logger.info(
        "phase 2: %d incomplete apps to escalate (machine %d/%d, max_tier2=%d)",
        len(apps), computer_index, computer_count, max_tier2_docs,
    )

    for app_pk, app_id in apps:
        stats.apps_visited += 1
        docs = _pick_tier2_docs(app_pk, max_docs=max_tier2_docs, prompt_version=prompt_version)
        if not docs:
            logger.info("  [%s] no tier-2 candidates left — marking as escalated", app_id)
            with session_scope() as s:
                s.execute(
                    sql_text("UPDATE da_summaries SET status='escalated' WHERE application_id = :app_pk"),
                    {"app_pk": app_pk},
                )
            continue
        for doc in docs:
            r = _process_one_doc(
                extractor,
                app_pk=app_pk, app_id_str=app_id, doc=doc, position="tier2",
                tier=2, prompt_version=prompt_version,
            )
            if r == "p":
                stats.docs_processed += 1
            elif r == "s":
                stats.docs_skipped_no_text += 1
            elif r == "f":
                stats.docs_failed += 1

    return stats
