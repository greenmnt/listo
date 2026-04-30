"""CLI for the DA summarisation pipeline. Wired into the top-level
`listo` CLI as `listo da ...`."""
from __future__ import annotations

import logging

import typer
from sqlalchemy import text as sql_text

from listo.db import session_scope
from listo.da_summaries import aggregate as agg_mod
from listo.da_summaries import builtcheck as built_mod
from listo.da_summaries import businesses as biz_mod
from listo.da_summaries import escalate as esc_mod
from listo.da_summaries import features as feat_mod
from listo.da_summaries import summarise as sum_mod
from listo.da_summaries.schemas import PROMPT_VERSION


da_app = typer.Typer(no_args_is_help=True, help="Ollama-based DA-document summarisation")

logger = logging.getLogger(__name__)


# ---------- summarise ----------


@da_app.command("summarise")
def summarise(
    slug: str = typer.Option(None, "--slug", help="council slug (e.g. cogc)"),
    app_id: str = typer.Option(None, "--app-id", help="single application id, e.g. EDA/2021/97"),
    limit: int = typer.Option(None, "--limit", help="cap apps processed this run"),
    force: bool = typer.Option(False, "--force", help="re-summarise even if rows exist for this prompt_version"),
    model: str = typer.Option(None, "--model", help="override LISTO_OLLAMA_MODEL for this run"),
    prompt_version: str = typer.Option(PROMPT_VERSION, "--prompt-version"),
    computer_index: int = typer.Option(0, "--computer-index", help="0-based partition index"),
    computer_count: int = typer.Option(1, "--computer-count", help="total machines partitioning the work"),
) -> None:
    """Phase 1: process the first and last document per DA."""
    stats = sum_mod.run(
        council_slug=slug, app_id_str=app_id, limit=limit, force=force,
        model=model, prompt_version=prompt_version,
        computer_index=computer_index, computer_count=computer_count,
    )
    typer.echo("")
    typer.echo(f"  apps visited:           {stats.apps_visited}")
    typer.echo(f"  docs processed:         {stats.docs_processed}")
    typer.echo(f"  docs skipped (no text): {stats.docs_skipped_no_text}")
    typer.echo(f"  docs skipped (cached):  {stats.docs_skipped_already_done}")
    typer.echo(f"  docs failed:            {stats.docs_failed}")


# ---------- escalate ----------


@da_app.command("escalate")
def escalate(
    slug: str = typer.Option(None, "--slug"),
    app_id: str = typer.Option(None, "--app-id"),
    limit: int = typer.Option(None, "--limit"),
    max_tier2_docs: int = typer.Option(3, "--max-tier2-docs"),
    force: bool = typer.Option(False, "--force"),
    model: str = typer.Option(None, "--model"),
    prompt_version: str = typer.Option(PROMPT_VERSION, "--prompt-version"),
    computer_index: int = typer.Option(0, "--computer-index"),
    computer_count: int = typer.Option(1, "--computer-count"),
) -> None:
    """Phase 2: tier-2 docs for incomplete DAs."""
    stats = esc_mod.run(
        council_slug=slug, app_id_str=app_id, limit=limit,
        max_tier2_docs=max_tier2_docs, force=force,
        model=model, prompt_version=prompt_version,
        computer_index=computer_index, computer_count=computer_count,
    )
    typer.echo("")
    typer.echo(f"  apps visited:           {stats.apps_visited}")
    typer.echo(f"  docs processed:         {stats.docs_processed}")
    typer.echo(f"  docs skipped (no text): {stats.docs_skipped_no_text}")
    typer.echo(f"  docs failed:            {stats.docs_failed}")


# ---------- features ----------


@da_app.command("features")
def features(
    slug: str = typer.Option(None, "--slug"),
    app_id: str = typer.Option(None, "--app-id"),
    limit: int = typer.Option(None, "--limit"),
    force: bool = typer.Option(False, "--force", help="re-extract even if rows exist for this (doc, version, template_key)"),
    model: str = typer.Option(None, "--model"),
    prompt_version: str = typer.Option("v4", "--prompt-version"),
    chunk_size: int = typer.Option(5, "--chunk-size", help="pages per chunk"),
    chunk_overlap: int = typer.Option(1, "--chunk-overlap", help="page overlap between chunks"),
    computer_index: int = typer.Option(0, "--computer-index"),
    computer_count: int = typer.Option(1, "--computer-count"),
) -> None:
    """Phase 2.5: extract physical/build-cost features from drawings + design reports."""
    stats = feat_mod.run(
        council_slug=slug, app_id_str=app_id, limit=limit, force=force,
        model=model, prompt_version=prompt_version,
        chunk_size=chunk_size, chunk_overlap=chunk_overlap,
        computer_index=computer_index, computer_count=computer_count,
    )
    typer.echo("")
    typer.echo(f"  apps visited:           {stats.apps_visited}")
    typer.echo(f"  docs processed:         {stats.docs_processed}")
    typer.echo(f"  docs skipped (no text): {stats.docs_skipped_no_text}")
    typer.echo(f"  chunks processed:       {stats.chunks_processed}")
    typer.echo(f"  chunks failed:          {stats.chunks_failed}")


# ---------- check-built ----------


@da_app.command("check-built")
def check_built(
    slug: str = typer.Option(None, "--slug"),
    app_id: str = typer.Option(None, "--app-id"),
    min_age_months: int = typer.Option(6, "--min-age-months", help="skip DAs decided more recently than N months ago"),
    redo: bool = typer.Option(False, "--redo", help="re-search even if discovered_urls already has entries for this address"),
) -> None:
    """Phase X: Google-search unit-prefixed addresses to detect duplexes
    that were built but never went on the market for sale (held / rented).

    For each approved DA without post-decision sales, search for
    `<unit>/<street>` on realestate.com.au and domain.com.au. Hits land
    in `discovered_urls` so the API can compute built_status.
    """
    stats = built_mod.run(
        council_slug=slug, app_id_str=app_id,
        min_age_months=min_age_months, redo=redo,
    )
    typer.echo("")
    typer.echo(f"  apps visited:           {stats.apps_visited}")
    typer.echo(f"  apps skipped (no addr): {stats.apps_skipped_no_address}")
    typer.echo(f"  queries run:            {stats.queries_run}")
    typer.echo(f"  units with evidence:    {stats.units_with_evidence}")


# ---------- aggregate ----------


@da_app.command("aggregate")
def aggregate(
    slug: str = typer.Option(None, "--slug"),
    app_id: str = typer.Option(None, "--app-id"),
    prompt_version: str = typer.Option(PROMPT_VERSION, "--prompt-version"),
) -> None:
    """Phase 3: merge per-doc rows + compute process stats → da_summaries."""
    stats = agg_mod.run(
        council_slug=slug, app_id_str=app_id, prompt_version=prompt_version,
    )
    typer.echo("")
    typer.echo(f"  apps aggregated: {stats.apps_aggregated}")
    typer.echo(f"  complete:        {stats.apps_complete}")
    typer.echo(f"  escalated:       {stats.apps_escalated}")
    typer.echo(f"  incomplete:      {stats.apps_incomplete}")


# ---------- businesses ----------


@da_app.command("businesses")
def businesses(
    slug: str = typer.Option(None, "--slug"),
    limit: int = typer.Option(None, "--limit"),
    force: bool = typer.Option(False, "--force"),
    computer_index: int = typer.Option(0, "--computer-index"),
    computer_count: int = typer.Option(1, "--computer-count"),
) -> None:
    """Phase 4: Google-search builder/architect names; persist canonical URLs."""
    stats = biz_mod.run(
        council_slug=slug, limit=limit, force=force,
        computer_index=computer_index, computer_count=computer_count,
    )
    typer.echo("")
    typer.echo(f"  queries run:        {stats.queried}")
    typer.echo(f"  high confidence:    {stats.persisted_high}")
    typer.echo(f"  medium confidence:  {stats.persisted_medium}")
    typer.echo(f"  low confidence:     {stats.persisted_low}")
    typer.echo(f"  no match:           {stats.persisted_no_match}")


# ---------- prompts ----------


@da_app.command("prompts")
def prompts(
    show_text: bool = typer.Option(False, "--show-text", help="print full system + user templates"),
    version: str = typer.Option(None, "--version", help="filter to a single prompt_version"),
) -> None:
    """List the prompt templates locked into the DB.

    These are write-once per (prompt_version, template_key) — once a
    row is in `prompt_templates`, the in-code template can't overwrite
    it. To change a template, bump `PROMPT_VERSION` in schemas.py.

    Use this when looking at a wrong row in `da_doc_summaries`: cross-
    reference its `prompt_version` + `template_key` to see exactly what
    instructions the LLM had at the time.
    """
    where = "WHERE prompt_version = :v" if version else ""
    params = {"v": version} if version else {}
    with session_scope() as s:
        rows = s.execute(sql_text(f"""
            SELECT id, prompt_version, template_key, system_prompt, user_template,
                   notes, first_used_at
              FROM prompt_templates
              {where}
              ORDER BY prompt_version, id
        """), params).fetchall()

    if not rows:
        typer.echo("(no templates registered yet — run a summarise to register)")
        return

    for r in rows:
        typer.secho(
            f"\n[{r.prompt_version}/{r.template_key}]",
            fg=typer.colors.CYAN, bold=True,
        )
        typer.echo(f"  notes:        {r.notes or '—'}")
        typer.echo(f"  first used:   {r.first_used_at}")
        typer.echo(f"  system chars: {len(r.system_prompt)}  user chars: {len(r.user_template)}")
        if show_text:
            typer.echo("  --- SYSTEM ---")
            for line in r.system_prompt.splitlines():
                typer.echo(f"    {line}")
            typer.echo("  --- USER TEMPLATE ---")
            for line in r.user_template.splitlines():
                typer.echo(f"    {line}")


# ---------- status ----------


@da_app.command("status")
def status() -> None:
    """Snapshot of summarisation progress."""
    with session_scope() as s:
        cad = s.execute(sql_text("""
            SELECT
              SUM(file_path IS NOT NULL AND mime_type LIKE 'application/pdf%') AS downloaded_pdfs,
              COUNT(*)                                                        AS total_docs
              FROM council_application_documents
        """)).fetchone()
        dds = s.execute(sql_text("""
            SELECT
              COUNT(*)                                AS total_dds,
              SUM(extraction_method = 'skipped')      AS dds_skipped,
              SUM(tier = 1)                           AS dds_tier1,
              SUM(tier = 2)                           AS dds_tier2,
              COUNT(DISTINCT application_id)          AS apps_with_dds
              FROM da_doc_summaries
        """)).fetchone()
        ds = s.execute(sql_text("""
            SELECT
              COUNT(*)                                AS total,
              SUM(status = 'complete')                AS n_complete,
              SUM(status = 'incomplete')              AS n_incomplete,
              SUM(status = 'escalated')               AS n_escalated
              FROM da_summaries
        """)).fetchone()
        bl = s.execute(sql_text("""
            SELECT
              COUNT(*)                                AS total,
              SUM(confidence = 'high')                AS high,
              SUM(confidence = 'medium')              AS med,
              SUM(confidence = 'low')                 AS low,
              SUM(confidence = 'no_match')            AS none
              FROM business_links
        """)).fetchone()
        unique_biz = s.execute(sql_text("""
            SELECT COUNT(DISTINCT LOWER(TRIM(name))) AS n FROM (
              SELECT builder_name AS name FROM da_summaries
               WHERE builder_name IS NOT NULL AND builder_name <> ''
              UNION ALL
              SELECT architect_name AS name FROM da_summaries
               WHERE architect_name IS NOT NULL AND architect_name <> ''
            ) u
        """)).fetchone()

    typer.echo(f"council_application_documents: {cad.downloaded_pdfs or 0:>6} PDFs downloaded / {cad.total_docs:>6} total")
    typer.echo(f"da_doc_summaries:              {dds.total_dds or 0:>6} rows ({dds.dds_tier1 or 0} tier-1, {dds.dds_tier2 or 0} tier-2, {dds.dds_skipped or 0} skipped)")
    typer.echo(f"  → covers:                    {dds.apps_with_dds or 0:>6} applications")
    typer.echo(f"da_summaries:                  {ds.total or 0:>6} rows")
    typer.echo(f"  → status complete:           {ds.n_complete or 0:>6}")
    typer.echo(f"  → status escalated:          {ds.n_escalated or 0:>6}")
    typer.echo(f"  → status incomplete:         {ds.n_incomplete or 0:>6}")
    typer.echo(f"unique builders+architects:    {unique_biz.n or 0:>6}")
    typer.echo(f"business_links:                {bl.total or 0:>6} rows ({bl.high or 0} high, {bl.med or 0} med, {bl.low or 0} low, {bl.none or 0} no_match)")
