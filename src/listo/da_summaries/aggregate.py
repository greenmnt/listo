"""Phase 3 — aggregate per-doc summaries + compute process stats → da_summaries.

For each application with new doc summaries (or all if --force / --app-id),
run the precedence rules from the plan to merge per-doc fields into one
row. Compute pure-SQL process stats (doc count, total bytes, etc.).
Set the `status` per the completeness rule.

Idempotent: every call upserts the row.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, text as sql_text
from sqlalchemy.dialects.mysql import insert as mysql_insert

import re

from listo.db import session_scope
from listo.models import (
    Company,
    CouncilApplication,
    DaDocSummary,
    DaSummary,
)
from listo.da_summaries.schemas import DwellingKind, KIND_VALUES, PROMPT_VERSION, is_complete


logger = logging.getLogger(__name__)


@dataclass
class AggregateStats:
    apps_aggregated: int = 0
    apps_complete: int = 0
    apps_incomplete: int = 0
    apps_escalated: int = 0


# Precedence orders — by `doc_position` (which corresponds to tier-1 first/last
# OR tier-2 escalated). Earlier position in the list wins.
APPLICANT_ORDER = ["first", "tier2", "last"]
BUILDER_ORDER = ["first", "tier2", "last"]
ARCHITECT_ORDER = ["tier2", "first", "last"]    # plans/specialist most likely to name architect
OWNER_ORDER = ["first", "tier2", "last"]
LOT_ORDER = ["first", "last", "tier2"]
ADDRESS_ORDER = ["first", "last", "tier2"]
DESCRIPTION_ORDER = ["tier2", "first", "last"]  # specialist reports have the best descriptions
KIND_ORDER = ["last", "tier2", "first"]         # decision notice authoritative
COUNT_ORDER = ["last", "tier2", "first"]


def _first_non_null(rows: list[DaDocSummary], order: list[str], field: str) -> tuple[object | None, int | None]:
    """Walk doc_position priority order, return (value, source_doc_id) for the first non-null."""
    by_pos: dict[str, list[DaDocSummary]] = {}
    for r in rows:
        by_pos.setdefault(r.doc_position, []).append(r)
    for pos in order:
        for r in by_pos.get(pos, []):
            v = getattr(r, field)
            if v is not None and v != "" and v != DwellingKind.UNKNOWN.value:
                return v, r.document_id
    return None, None


def _splice_street_number(raw_address: str | None, street_address: str | None) -> str | None:
    """If `street_address` is missing a leading street number, look in
    `raw_address` for `<number> <street_name>` and prepend it.

    Why: the LLM occasionally drops the street number when raw_address
    has a "Lot N SPnnnnn," prefix (e.g. raw="Lot 2 SP304034, 15 Matasha
    Crescent, PIMPAMA QLD 4209" → street="Matasha Crescent, Pimpama, QLD
    4209"). Without the number, downstream LIKE matches against
    domain_properties / realestate_properties.display_address fail.
    """
    if not street_address or not raw_address:
        return street_address
    if re.match(r"^\s*\d", street_address):
        return street_address
    street_name = street_address.split(",", 1)[0].strip()
    if not street_name:
        return street_address
    # Match a number-prefixed token (allowing 1A, 1-3, 1/15) followed by
    # the street name. Skip "Lot N" / "SPnnnn" prefixes by anchoring on
    # the street name itself.
    pattern = rf"(\d+[A-Za-z]?(?:[-/]\d+[A-Za-z]?)?)\s+{re.escape(street_name)}"
    m = re.search(pattern, raw_address, re.IGNORECASE)
    if not m:
        return street_address
    return f"{m.group(1)} {street_address}"


def _canonicalise_street_address(street_address: str | None) -> str | None:
    """Normalise punctuation so LIKE matches against domain_properties
    /realestate_properties.display_address succeed.

    The LLM commonly emits ", Suburb, STATE PCODE" but Domain stores
    ", Suburb STATE PCODE" (no comma before the state). We strip that
    extra comma so the prefix match works.
    """
    if not street_address:
        return street_address
    # ", <STATE> <pc>" at end → " <STATE> <pc>"
    normalised = re.sub(
        r",\s*(QLD|NSW|VIC|TAS|WA|SA|NT|ACT)\s+(\d{4})\s*$",
        r" \1 \2",
        street_address,
        flags=re.IGNORECASE,
    )
    return normalised


# ---------- companies upsert ----------


_COMPANY_SUFFIX_RE = re.compile(
    r"\b(pty\.?\s*ltd\.?|pty\.?\s*limited|limited|inc\.?|corp\.?|llc|ltd\.?)\b\.?\s*$",
    re.IGNORECASE,
)
_DIGITS_ONLY = re.compile(r"\D+")


def _norm_name(display: str) -> str:
    """Lowercase, strip the company suffix + extra whitespace + punctuation
    inside parens. Used as the join key when ACN isn't available."""
    s = display.lower().strip()
    # Drop bracketed annotations like '(A.C.N 652 330 928)' or '(qld)'
    s = re.sub(r"\([^)]*\)", "", s)
    # Drop trailing 'pty ltd' / 'limited' etc.
    s = _COMPANY_SUFFIX_RE.sub("", s).strip()
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _normalise_acn(acn: str | None) -> str | None:
    if not acn:
        return None
    digits = _DIGITS_ONLY.sub("", acn)
    return digits if len(digits) == 9 else None


def _normalise_abn(abn: str | None) -> str | None:
    if not abn:
        return None
    digits = _DIGITS_ONLY.sub("", abn)
    return digits if len(digits) == 11 else None


def _upsert_company(
    s,
    *,
    display_name: str | None,
    acn: str | None = None,
    abn: str | None = None,
    entity_type: str | None = None,
) -> int | None:
    """Get-or-create a `companies` row. Match priority: ACN > ABN > norm_name.

    Returns the company id, or None if we got no usable name.
    """
    if not display_name or not display_name.strip():
        return None

    display = display_name.strip()[:255]
    norm = _norm_name(display)
    if not norm:
        return None
    acn_clean = _normalise_acn(acn)
    abn_clean = _normalise_abn(abn)
    et = (entity_type or "unknown").strip().lower() or "unknown"

    # Match by ACN > ABN > norm_name
    existing: Company | None = None
    if acn_clean:
        existing = s.execute(
            select(Company).where(Company.acn == acn_clean)
        ).scalar_one_or_none()
    if existing is None and abn_clean:
        existing = s.execute(
            select(Company).where(Company.abn == abn_clean)
        ).scalar_one_or_none()
    if existing is None:
        existing = s.execute(
            select(Company).where(Company.norm_name == norm).limit(1)
        ).scalar_one_or_none()

    if existing is not None:
        # Enrich with any new info we just learned (ACN > ABN > entity_type).
        if acn_clean and not existing.acn:
            existing.acn = acn_clean
        if abn_clean and not existing.abn:
            existing.abn = abn_clean
        if et != "unknown" and existing.entity_type == "unknown":
            existing.entity_type = et
        return existing.id

    new = Company(
        acn=acn_clean,
        abn=abn_clean,
        display_name=display,
        norm_name=norm[:255],
        entity_type=et,
        first_seen_at=datetime.utcnow(),
    )
    s.add(new)
    s.flush()
    return new.id


def _aggregate_one(s, app_pk: int, prompt_version: str) -> str:
    """Aggregate one application; returns the chosen status."""
    rows = s.execute(
        select(DaDocSummary).where(
            DaDocSummary.application_id == app_pk,
            DaDocSummary.prompt_version == prompt_version,
            DaDocSummary.extraction_method != "skipped",
        )
    ).scalars().all()

    source: dict[str, int] = {}
    fields: dict[str, object | None] = {}

    for field, order in [
        ("applicant_name", APPLICANT_ORDER),
        ("applicant_acn", APPLICANT_ORDER),
        ("applicant_abn", APPLICANT_ORDER),
        ("applicant_entity_type", APPLICANT_ORDER),
        ("applicant_agent_name", APPLICANT_ORDER),
        ("builder_name", BUILDER_ORDER),
        ("architect_name", ARCHITECT_ORDER),
        ("owner_name", OWNER_ORDER),
        ("owner_acn", OWNER_ORDER),
        ("owner_abn", OWNER_ORDER),
        ("owner_entity_type", OWNER_ORDER),
        ("lot_on_plan", LOT_ORDER),
        ("street_address", ADDRESS_ORDER),
        ("dwelling_kind", KIND_ORDER),
        ("dwelling_count", COUNT_ORDER),
    ]:
        v, src = _first_non_null(rows, order, field)
        fields[field] = v
        if src is not None:
            source[field] = src

    # project_description: prefer the longest one from priority order
    descs = [r for r in rows if r.project_description]
    if descs:
        # Find the row in priority order, then take the longest within that position.
        desc_pos: dict[str, list[DaDocSummary]] = {}
        for r in descs:
            desc_pos.setdefault(r.doc_position, []).append(r)
        for pos in DESCRIPTION_ORDER:
            cand = desc_pos.get(pos, [])
            if cand:
                best = max(cand, key=lambda r: len(r.project_description or ""))
                fields["project_description"] = (best.project_description or "")[:1000]
                source["project_description"] = best.document_id
                break

    # Process stats
    pstats = s.execute(sql_text("""
        SELECT
          COUNT(*)                                                            AS n_docs,
          SUM(file_path IS NOT NULL)                                          AS n_docs_downloaded,
          COALESCE(SUM(file_size), 0)                                         AS total_bytes,
          COALESCE(SUM(page_count), 0)                                        AS total_pages,
          SUM(doc_type LIKE '%Information Request%')                          AS n_information_requests,
          SUM(doc_type LIKE 'Amended%' OR doc_type LIKE '%mended%')           AS n_amendments,
          SUM(doc_type LIKE '%Specialist%')                                   AS n_specialist_reports,
          MIN(CASE WHEN file_path IS NOT NULL THEN published_at END)          AS first_doc_at,
          MAX(CASE WHEN file_path IS NOT NULL THEN published_at END)          AS last_doc_at
          FROM council_application_documents WHERE application_id = :app_pk
    """), {"app_pk": app_pk}).fetchone()

    ca_row = s.execute(sql_text("""
        SELECT DATEDIFF(decision_date, lodged_date) AS d, raw_address
          FROM council_applications WHERE id = :app_pk
    """), {"app_pk": app_pk}).fetchone()
    days_lodge_to_decide_row = ca_row  # back-compat alias for downstream code

    # Splice a street number in from raw_address when the LLM dropped it
    # (typically when raw_address has a "Lot N SPnnn," prefix), then
    # canonicalise punctuation. Without these, the API's LIKE-based
    # property matches against domain_properties.display_address fail.
    if fields.get("street_address") and ca_row is not None:
        spliced = _splice_street_number(
            ca_row.raw_address, fields.get("street_address")
        )
        fields["street_address"] = _canonicalise_street_address(spliced)

    n_docs_summarised = sum(1 for r in rows if r.extraction_method != "skipped")

    # Decide the status
    complete = is_complete(
        dwelling_count=fields.get("dwelling_count"),
        dwelling_kind=fields.get("dwelling_kind"),
        applicant_name=fields.get("applicant_name"),
        builder_name=fields.get("builder_name"),
        architect_name=fields.get("architect_name"),
    )
    has_tier2 = any(r.tier == 2 for r in rows)
    if complete:
        status = "complete"
    elif has_tier2:
        status = "escalated"
    else:
        status = "incomplete"

    # Resolve company FKs (creates rows in `companies` keyed by ACN /
    # ABN / norm_name). Each role gets its own company id; the agent
    # is its own company because it's usually a planning consultancy.
    applicant_company_id = _upsert_company(
        s,
        display_name=fields.get("applicant_name"),
        acn=fields.get("applicant_acn"),
        abn=fields.get("applicant_abn"),
        entity_type=fields.get("applicant_entity_type"),
    )
    applicant_agent_company_id = _upsert_company(
        s, display_name=fields.get("applicant_agent_name"),
    )
    builder_company_id = _upsert_company(s, display_name=fields.get("builder_name"))
    architect_company_id = _upsert_company(s, display_name=fields.get("architect_name"))
    owner_company_id = _upsert_company(
        s,
        display_name=fields.get("owner_name"),
        acn=fields.get("owner_acn"),
        abn=fields.get("owner_abn"),
        entity_type=fields.get("owner_entity_type"),
    )

    now = datetime.utcnow()
    payload = {
        "application_id": app_pk,
        "applicant_name": fields.get("applicant_name"),
        "applicant_acn": fields.get("applicant_acn"),
        "applicant_abn": fields.get("applicant_abn"),
        "applicant_entity_type": fields.get("applicant_entity_type"),
        "applicant_agent_name": fields.get("applicant_agent_name"),
        "builder_name": fields.get("builder_name"),
        "architect_name": fields.get("architect_name"),
        "owner_name": fields.get("owner_name"),
        "owner_acn": fields.get("owner_acn"),
        "owner_abn": fields.get("owner_abn"),
        "owner_entity_type": fields.get("owner_entity_type"),
        "dwelling_count": fields.get("dwelling_count"),
        "dwelling_kind": fields.get("dwelling_kind"),
        "project_description": fields.get("project_description"),
        "lot_on_plan": fields.get("lot_on_plan"),
        "street_address": fields.get("street_address"),
        "source_doc_ids_json": source if source else None,
        "applicant_company_id": applicant_company_id,
        "applicant_agent_company_id": applicant_agent_company_id,
        "builder_company_id": builder_company_id,
        "architect_company_id": architect_company_id,
        "owner_company_id": owner_company_id,
        "n_docs": int(pstats.n_docs or 0),
        "n_docs_downloaded": int(pstats.n_docs_downloaded or 0),
        "total_bytes": int(pstats.total_bytes or 0),
        "total_pages": int(pstats.total_pages or 0),
        "n_information_requests": int(pstats.n_information_requests or 0),
        "n_amendments": int(pstats.n_amendments or 0),
        "n_specialist_reports": int(pstats.n_specialist_reports or 0),
        "days_lodge_to_decide": (
            int(days_lodge_to_decide_row.d) if days_lodge_to_decide_row and days_lodge_to_decide_row.d is not None else None
        ),
        "first_doc_at": pstats.first_doc_at,
        "last_doc_at": pstats.last_doc_at,
        "n_docs_summarised": n_docs_summarised,
        "status": status,
        "aggregated_at": now,
    }

    stmt = mysql_insert(DaSummary).values(**payload)
    update_cols = {k: stmt.inserted[k] for k in payload if k != "application_id"}
    stmt = stmt.on_duplicate_key_update(**update_cols)
    s.execute(stmt)

    # Stamp council_applications.summarised_at so the resume query has a hook.
    s.execute(
        sql_text("UPDATE council_applications SET summarised_at = :now WHERE id = :app_pk"),
        {"now": now, "app_pk": app_pk},
    )

    return status


def run(
    *,
    council_slug: str | None = None,
    app_id_str: str | None = None,
    prompt_version: str = PROMPT_VERSION,
) -> AggregateStats:
    """Phase 3 runner. Aggregates every app that has at least one
    non-skipped da_doc_summaries row under this prompt_version."""
    stats = AggregateStats()

    where_extra = ""
    params: dict[str, object] = {"v": prompt_version}
    if council_slug:
        where_extra += " AND ca.council_slug = :slug"
        params["slug"] = council_slug
    if app_id_str:
        where_extra += " AND ca.application_id = :appid"
        params["appid"] = app_id_str

    apps_sql = sql_text(f"""
        SELECT DISTINCT ca.id, ca.application_id
          FROM da_doc_summaries dds
          JOIN council_applications ca ON ca.id = dds.application_id
         WHERE dds.prompt_version = :v
           AND dds.extraction_method != 'skipped'
           {where_extra}
         ORDER BY ca.id ASC
    """)

    with session_scope() as s:
        apps = s.execute(apps_sql, params).fetchall()
        logger.info("phase 3: aggregating %d apps", len(apps))

        for app_pk, app_id in apps:
            try:
                status = _aggregate_one(s, app_pk, prompt_version)
            except Exception as exc:  # noqa: BLE001
                logger.warning("aggregate failed for %s: %s", app_id, exc)
                continue
            stats.apps_aggregated += 1
            if status == "complete":
                stats.apps_complete += 1
            elif status == "escalated":
                stats.apps_escalated += 1
            else:
                stats.apps_incomplete += 1

    return stats
