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
from listo.da_summaries import harvest_entities as he_mod
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


# ---------- harvest-entities ----------


@da_app.command("harvest-entities")
def harvest_entities(
    app_id: str = typer.Option(None, "--app-id", help="single application code, e.g. COM/2021/115"),
    limit: int = typer.Option(None, "--limit", help="cap docs processed this run"),
    types: str = typer.Option(
        "MCU,COM,EDA,EXA,PDA,FDA,ROL", "--types",
        help="comma-separated app type-code prefixes; pass 'all' to scan every category",
    ),
    max_docs_per_app: int = typer.Option(
        0, "--max-docs-per-app",
        help="skip apps with more than N total docs; 0 (default) disables — "
             "the per-doc size cap (10MB) already protects against single huge files, "
             "and big projects are exactly where most architects/builders live",
    ),
) -> None:
    """Tier-0 entity harvest: regex-parse COGC correspondence into application_entities.

    Walks council_application_documents whose doc_type looks like council
    correspondence (Decision/Confirmation Notice, Cover Letter, Information
    Request) and whose extracted_text is populated. Splits multi-name
    recipients ("Peter Dawson and Noela Roberts") into separate companies +
    application_entities rows. Idempotent.
    """
    app_pk: int | None = None
    if app_id:
        with session_scope() as s:
            row = s.execute(sql_text(
                "SELECT id FROM council_applications WHERE application_id = :a LIMIT 1"
            ), {"a": app_id}).fetchone()
            if row is None:
                typer.echo(f"no application with code {app_id!r}")
                raise typer.Exit(1)
            app_pk = row.id

    type_codes: set[str] | None = None
    if types and types.lower() != "all":
        type_codes = {t.strip().upper() for t in types.split(",") if t.strip()}

    max_docs = max_docs_per_app if max_docs_per_app and max_docs_per_app > 0 else None

    stats = he_mod.harvest_all(
        app_pk=app_pk, limit=limit,
        type_codes=type_codes, max_docs_per_app=max_docs,
    )
    typer.echo("")
    typer.echo(f"  docs scanned:       {stats['docs_seen']}")
    typer.echo(f"  text extracted now: {stats['docs_text_extracted']}")
    typer.echo(f"  text skipped:       {stats['docs_text_skipped']}")
    typer.echo(f"  cogc letters:       {stats['docs_cogc']}")
    typer.echo(f"  applicant letters:  {stats.get('docs_applicant_letter', 0)}")
    typer.echo(f"  plans w/ block:     {stats['docs_plans_with_block']}")
    typer.echo(f"  plans w/ NO hit:    {stats['docs_plans_no_hit']} (recall failures)")
    typer.echo(f"  plans skipped (>{stats.get('docs_plans_skipped_large', 0) and '10MB' or '10MB'}):  {stats['docs_plans_skipped_large']}")
    typer.echo(f"  emissions:          {stats['emissions']}")
    typer.echo(f"  entity rows:        {stats['entity_rows_written']}")
    typer.echo(f"  evidence rows:      {stats['evidence_rows']}")
    typer.echo(f"  fingerprints:       {stats['fingerprints_recorded']}")
    typer.echo(f"  distinct companies: {stats['companies_seen']}")


# ---------- resolve-fingerprints ----------


@da_app.command("resolve-fingerprints")
def resolve_fingerprints(
    app_id: str = typer.Option(None, "--app-id", help="single application code, e.g. MCU/2021/648"),
    bind_as_entity: bool = typer.Option(
        True, "--bind-as-entity/--no-bind",
        help="also write a row to application_entities for resolved firms (default: yes)",
    ),
) -> None:
    """Resolve URL / email fingerprints to named firms by cross-referencing
    other docs in the same application.

    For each unresolved URL fingerprint (e.g. `borisdesign.com.au`),
    extracts text from sibling docs and searches for spellings of the
    firm name (`Boris Design`, `BORIS DESIGN`, `borisdesign`). On a hit,
    upserts a `companies` row, marks the fingerprint resolved, and
    (with --bind-as-entity) emits an `application_entities` row with
    role=architect so the timeline view shows it.
    """
    from listo.da_summaries import resolve_fingerprints as rf_mod

    if app_id:
        with session_scope() as s:
            row = s.execute(sql_text(
                "SELECT id FROM council_applications WHERE application_id = :a LIMIT 1"
            ), {"a": app_id}).fetchone()
            if row is None:
                typer.echo(f"no application with code {app_id!r}")
                raise typer.Exit(1)
            app_pk = row.id
        stats = rf_mod.resolve_app(app_pk)
    else:
        stats = rf_mod.resolve_all()

    typer.echo("")
    typer.echo(f"  fingerprints scanned:    {stats.fingerprints_seen}")
    typer.echo(f"  distinct domains:        {stats.domains_seen}")
    typer.echo(f"  sibling docs extracted:  {stats.docs_text_extracted}")
    typer.echo(f"  domains resolved:        {stats.domains_resolved}")
    typer.echo(f"  fingerprints updated:    {stats.fingerprints_updated}")

    if not bind_as_entity or stats.domains_resolved == 0:
        return

    # For every newly-resolved fingerprint, write an application_entities
    # row pointing at the resolved company — but ONLY if that company
    # has no other entity row for this app yet. If owns_copyright /
    # property_of / correspondence already gave us an entity row for
    # the same company, we'd just be creating a duplicate-role row.
    with session_scope() as s:
        rows = s.execute(sql_text("""
            SELECT df.application_id, df.source_doc_id, df.resolved_company_id,
                   c.display_name
            FROM doc_fingerprints df
            JOIN companies c ON c.id = df.resolved_company_id
            WHERE df.resolved_via = 'cross_doc_match'
              AND df.fingerprint_kind IN ('url', 'email')
              AND NOT EXISTS (
                SELECT 1 FROM application_entities ae
                WHERE ae.application_id = df.application_id
                  AND ae.company_id = df.resolved_company_id
              )
        """)).fetchall()

        from sqlalchemy.dialects.mysql import insert as mysql_insert
        from listo.models import ApplicationEntity
        from datetime import datetime

        n_bound = 0
        for r in rows:
            stmt = mysql_insert(ApplicationEntity).values(
                application_id=r.application_id,
                company_id=r.resolved_company_id,
                role="architect",
                is_primary=False,
                source_doc_id=r.source_doc_id,
                source_field="plans_fingerprint_resolved",
                extractor="fingerprint_resolver_v1",
                confidence="medium",
                extracted_at=datetime.utcnow(),
            ).on_duplicate_key_update(
                source_field=sql_text("VALUES(source_field)"),
                confidence=sql_text("VALUES(confidence)"),
            )
            s.execute(stmt)
            n_bound += 1

    typer.echo(f"  bound as application_entities: {n_bound}")


# ---------- filter-entities ----------


@da_app.command("filter-entities")
def filter_entities(
    dry_run: bool = typer.Option(
        False, "--dry-run/--apply",
        help="just report counts; don't update DB",
    ),
    version: str = typer.Option(
        None, "--version",
        help="override the heuristic ruleset version (default: VERSION in entity_filter.py)",
    ),
) -> None:
    """Sweep `entity_evidence` for obvious nonsense and mark those rows
    `status='rejected'` with `verifier='heuristic_v<n>'`.

    The audit trail (verifier + notes columns) lets you re-run with
    refined rules and see exactly which version dropped each row. Only
    operates on rows whose status is still 'predicted', so reruns don't
    re-touch already-reviewed rows.

    Workflow: --dry-run first → spot-check the kept set
    (`SELECT ... WHERE status='predicted'`) → add new rules to
    entity_filter.py → bump VERSION → --apply.
    """
    from listo.da_summaries import entity_filter

    stats = entity_filter.run(dry_run=dry_run, version=version)
    typer.echo("")
    typer.echo(f"  inspected: {stats.inspected}")
    typer.echo(f"  rejected:  {stats.rejected}")
    typer.echo(f"  kept:      {stats.kept}")
    typer.echo("")
    typer.echo("  per-rule counts:")
    for name, n in sorted(stats.by_rule.items(), key=lambda kv: -kv[1]):
        typer.echo(f"    {name:<22} {n:>6}")


# ---------- review-labels ----------


@da_app.command("review-labels")
def review_labels(
    extractor: str = typer.Option(None, "--extractor", help="filter to one extractor (e.g. plans_title_regex_v1)"),
    role: str = typer.Option(None, "--role", help="filter to one candidate_role"),
    limit: int = typer.Option(50, "--limit", help="max rows to walk this session"),
    only_predicted: bool = typer.Option(
        True, "--only-predicted/--include-reviewed",
        help="default: skip rows already verified/rejected/corrected",
    ),
) -> None:
    """Manually verify entity_evidence rows. For each row shows:
      - the candidate name + role + confidence
      - a context window of source_text around the span
      - layout (bbox / page) when present

    Then prompts:
      [y]es      — mark verified, candidate is correct
      [n]o       — mark rejected, candidate is wrong (false positive)
      [c]orrect  — supply a fixed name and/or role
      [s]kip     — leave as predicted, move on
      [q]uit     — stop the session
    """
    where = ["status != 'rejected'"] if False else []
    if only_predicted:
        where.append("status = 'predicted'")
    if extractor:
        where.append("extractor = :extractor")
    if role:
        where.append("candidate_role = :role")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    params = {"limit": limit}
    if extractor:
        params["extractor"] = extractor
    if role:
        params["role"] = role

    with session_scope() as s:
        rows = s.execute(sql_text(f"""
            SELECT id, application_id, source_doc_id, extractor,
                   candidate_name, candidate_role, confidence,
                   span_start, span_end, source_text, layout, status
            FROM entity_evidence
            {where_sql}
            ORDER BY id
            LIMIT :limit
        """), params).fetchall()

        if not rows:
            typer.echo("no rows match")
            return

        typer.echo(f"reviewing {len(rows)} rows")
        typer.echo("")

        n_verified = n_rejected = n_corrected = n_skipped = 0
        for i, r in enumerate(rows, 1):
            ctx_lo = max(0, r.span_start - 100)
            ctx_hi = min(len(r.source_text or ""), r.span_end + 100)
            before = (r.source_text or "")[ctx_lo:r.span_start].replace("\n", " ⏎ ")
            inside = (r.source_text or "")[r.span_start:r.span_end].replace("\n", " ⏎ ")
            after = (r.source_text or "")[r.span_end:ctx_hi].replace("\n", " ⏎ ")

            typer.echo(f"[{i}/{len(rows)}] evidence #{r.id}  doc={r.source_doc_id}  app={r.application_id}")
            typer.echo(f"  extractor: {r.extractor}")
            typer.secho(
                f"  candidate: {r.candidate_name!r}   role={r.candidate_role}   conf={r.confidence}",
                fg=typer.colors.CYAN,
            )
            typer.echo(f"  context:   …{before}\033[1;32m[{inside}]\033[0m{after}…")
            if r.layout:
                import json as _json
                lj = _json.loads(r.layout) if isinstance(r.layout, str) else r.layout
                bbox = lj.get("bbox")
                size = lj.get("size")
                if bbox and size:
                    typer.echo(f"  layout:    page={lj.get('page_index')}  bbox={bbox}  size={size}pt")
            typer.echo("")
            ans = typer.prompt("    [y]es / [n]o / [c]orrect / [s]kip / [q]uit", default="s").strip().lower()

            if ans in ("q", "quit"):
                typer.echo("aborting session")
                break
            if ans in ("s", "skip", ""):
                n_skipped += 1
                continue
            if ans in ("y", "yes"):
                _update_status(s, r.id, "verified")
                n_verified += 1
            elif ans in ("n", "no", "reject"):
                _update_status(s, r.id, "rejected")
                n_rejected += 1
            elif ans in ("c", "correct"):
                new_name = typer.prompt("    correct name (Enter to keep)", default=r.candidate_name)
                new_role = typer.prompt("    correct role (Enter to keep)", default=r.candidate_role)
                _update_correction(s, r.id, new_name, new_role)
                n_corrected += 1
            else:
                typer.echo(f"    (didn't recognise {ans!r}, skipping)")
                n_skipped += 1
            typer.echo("")

        typer.echo("")
        typer.echo(f"  verified:  {n_verified}")
        typer.echo(f"  rejected:  {n_rejected}")
        typer.echo(f"  corrected: {n_corrected}")
        typer.echo(f"  skipped:   {n_skipped}")


def _update_status(s, evidence_id: int, status: str) -> None:
    s.execute(sql_text("""
        UPDATE entity_evidence
        SET status = :st, verifier = 'human:cli', verified_at = NOW(3)
        WHERE id = :id
    """), {"st": status, "id": evidence_id})


def _update_correction(s, evidence_id: int, name: str, role: str) -> None:
    s.execute(sql_text("""
        UPDATE entity_evidence
        SET status = 'corrected',
            truth_name = :name,
            truth_role = :role,
            verifier = 'human:cli',
            verified_at = NOW(3)
        WHERE id = :id
    """), {"name": name, "role": role, "id": evidence_id})


# ---------- export-labels ----------


@da_app.command("export-labels")
def export_labels(
    out_path: str = typer.Option(
        "data/labeling/entity_train.jsonl", "--out",
        help="output JSONL path (relative to repo root)",
    ),
) -> None:
    """Convert entity_evidence rows into LayoutLMv3 token-classification
    training format. One JSON line per (doc, page) tuple — see
    `labeling/dataset.py` for the schema.

    Re-run anytime; idempotently overwrites the output file."""
    from pathlib import Path
    from listo.da_summaries.labeling import dataset as ds_mod

    p = Path(out_path)
    stats = ds_mod.export_all(p)

    typer.echo("")
    typer.echo(f"  output:                  {p}")
    typer.echo(f"  docs scanned:            {stats.docs_seen}")
    typer.echo(f"  pages emitted:           {stats.pages_emitted}")
    typer.echo(f"  total words:             {stats.spans_total}")
    typer.echo(f"  words tagged (non-O):    {stats.spans_tagged}")
    typer.echo(f"  entity rows used:        {stats.entity_rows_used}")
    if stats.spans_total:
        pct = 100 * stats.spans_tagged / stats.spans_total
        typer.echo(f"  positive label rate:     {pct:.1f}%")


# ---------- train-entity-model ----------


@da_app.command("train-entity-model")
def train_entity_model(
    jsonl: str = typer.Option(
        "data/labeling/entity_train.jsonl", "--jsonl",
        help="training JSONL produced by `da export-labels`",
    ),
    out: str = typer.Option(
        "data/labeling/model", "--out",
        help="directory to save the fine-tuned model + processor",
    ),
    epochs: int = typer.Option(4, "--epochs"),
    batch_size: int = typer.Option(4, "--batch-size"),
    lr: float = typer.Option(5e-5, "--lr"),
    val_frac: float = typer.Option(0.2, "--val-frac"),
    seed: int = typer.Option(42, "--seed"),
    limit: int = typer.Option(0, "--limit",
        help="train on first N records only (smoke test)"),
    max_length: int = typer.Option(512, "--max-length",
        help="processor sequence length — drop to 256 for tight CPU RAM"),
    images_dir: str = typer.Option("data/labeling/images", "--images-dir",
        help="pre-rendered page images dir (run `da render-training-images` to populate)"),
) -> None:
    """Fine-tune LayoutLMv3 on the exported entity_train.jsonl.

    Stratified split by doc_id (no same-document leakage between train/val).
    Outputs a HuggingFace checkpoint dir at --out usable by
    `LayoutLMv3ForTokenClassification.from_pretrained()`."""
    import sys
    from listo.da_summaries.labeling import train as train_mod

    sys.argv = [
        "train",
        "--jsonl", jsonl,
        "--out", out,
        "--epochs", str(epochs),
        "--batch-size", str(batch_size),
        "--lr", str(lr),
        "--val-frac", str(val_frac),
        "--seed", str(seed),
        "--limit", str(limit),
        "--max-length", str(max_length),
        "--images-dir", images_dir,
    ]
    train_mod.main()


# ---------- render-training-images ----------


@da_app.command("render-training-images")
def render_training_images(
    jsonl: str = typer.Option(
        "data/labeling/entity_train.jsonl", "--jsonl",
        help="training JSONL produced by `da export-labels`",
    ),
    images_dir: str = typer.Option(
        "data/labeling/images", "--out",
        help="directory to write 224x224 JPEGs into",
    ),
) -> None:
    """Pre-render every PDF page referenced in the training JSONL into
    `data/labeling/images/{doc_id}_p{page}.jpg`.

    Run this on whichever machine has the PDFs (typically the server).
    Then rsync just the JSONL + images dir (~60 MB total) to the GPU
    host — no PDFs needed there. Idempotent: skips files that already
    exist."""
    from pathlib import Path
    from listo.da_summaries.labeling import images as img_mod

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    stats = img_mod.render_all(Path(jsonl), Path(images_dir))

    typer.echo("")
    typer.echo(f"  pages seen:        {stats.pages_seen}")
    typer.echo(f"  newly rendered:    {stats.pages_rendered}")
    typer.echo(f"  skipped existing:  {stats.pages_skipped_existing}")
    typer.echo(f"  failed:            {stats.pages_failed}")
    typer.echo(f"  output:            {images_dir}")


# ---------- reclassify-docs ----------


@da_app.command("reclassify-docs")
def reclassify_docs(
    only_null: bool = typer.Option(
        False, "--only-null",
        help="only update rows where doc_kind IS NULL",
    ),
) -> None:
    """Apply `classify_doc_kind` to every council_application_documents row.

    Idempotent — safe to re-run after the classifier rules evolve."""
    from listo.da_summaries.doc_kind import classify_doc_kind

    with session_scope() as s:
        rows = s.execute(
            sql_text(
                "SELECT id, doc_type, doc_kind FROM council_application_documents"
                + (" WHERE doc_kind IS NULL" if only_null else "")
            )
        ).fetchall()

        n_changed = 0
        n_total = len(rows)
        for r in rows:
            new_kind = classify_doc_kind(r.doc_type)
            if r.doc_kind == new_kind:
                continue
            s.execute(
                sql_text(
                    "UPDATE council_application_documents SET doc_kind = :k WHERE id = :i"
                ),
                {"k": new_kind, "i": r.id},
            )
            n_changed += 1

    typer.echo(f"  scanned: {n_total}")
    typer.echo(f"  updated: {n_changed}")


# ---------- timeline ----------


@da_app.command("timeline")
def timeline(
    app_id: str = typer.Argument(..., help="application code, e.g. MCU/2020/98"),
) -> None:
    """Show the entity timeline for an application — entities grouped by
    DA workflow stage (submission → ir_response → decision), with
    new-in-this-stage rows marked '+'.

    Requires `doc_kind` to be populated; run `listo da reclassify-docs` first."""
    from listo.da_summaries.doc_kind import DOC_KIND_LABEL, DOC_KIND_ORDER

    with session_scope() as s:
        app_row = s.execute(
            sql_text(
                "SELECT id, suburb, description, lodged_date "
                "FROM council_applications WHERE application_id = :a LIMIT 1"
            ),
            {"a": app_id},
        ).fetchone()
        if app_row is None:
            typer.echo(f"no application with id {app_id!r}")
            raise typer.Exit(1)

        # All docs we hold for this app, classified.
        doc_rows = s.execute(
            sql_text("""
                SELECT id, doc_type, doc_kind
                FROM council_application_documents
                WHERE application_id = :app_pk
                ORDER BY id
            """),
            {"app_pk": app_row.id},
        ).fetchall()
        all_docs_per_kind: dict[str, list] = {}
        for d in doc_rows:
            all_docs_per_kind.setdefault(d.doc_kind or "other", []).append(d)

        rows = s.execute(
            sql_text("""
                SELECT
                  d.id          AS doc_id,
                  d.doc_type    AS doc_type,
                  d.doc_kind    AS doc_kind,
                  ae.role       AS role,
                  ae.confidence AS confidence,
                  c.id          AS company_id,
                  c.display_name AS name,
                  c.entity_type  AS etype
                FROM application_entities ae
                JOIN council_application_documents d ON d.id = ae.source_doc_id
                JOIN companies c ON c.id = ae.company_id
                WHERE ae.application_id = :app_pk
                ORDER BY d.id, ae.id
            """),
            {"app_pk": app_row.id},
        ).fetchall()

        docs_with_entities: dict[str, set[int]] = {}
        per_kind_entities: dict[str, list[tuple[str, str, str, int]]] = {}
        for r in rows:
            kind = r.doc_kind or "other"
            docs_with_entities.setdefault(kind, set()).add(r.doc_id)
            per_kind_entities.setdefault(kind, []).append(
                (r.role, r.name, r.confidence or "-", r.company_id)
            )

    typer.echo("")
    typer.echo(f"  Timeline for {app_id} — {app_row.suburb or ''}")
    typer.echo(f"  {(app_row.description or '')[:80]}")
    if app_row.lodged_date:
        typer.echo(f"  lodged: {app_row.lodged_date}")
    typer.echo("  " + "═" * 70)

    seen_so_far: dict[str, set[int]] = {}  # role → {company_id}
    everything_seen: set[int] = set()
    for kind in DOC_KIND_ORDER:
        all_kind_docs = all_docs_per_kind.get(kind, [])
        entries = per_kind_entities.get(kind, [])
        if not all_kind_docs and not entries:
            continue

        n_total = len(all_kind_docs)
        n_with_ents = len(docs_with_entities.get(kind, set()))
        n_no_match = n_total - n_with_ents
        label = DOC_KIND_LABEL[kind]
        recall_note = ""
        if n_no_match > 0:
            recall_note = f"  ({n_no_match} not yet matched)"
        typer.secho(
            f"  [{label}]  {n_with_ents}/{n_total} docs with entities{recall_note}",
            fg=typer.colors.CYAN,
        )

        if not entries:
            # List the unmatched docs so the user sees what we're missing.
            for d in all_kind_docs:
                typer.secho(
                    f"      ? {d.doc_type or '(no type)'}  (doc {d.id})",
                    fg=typer.colors.YELLOW,
                )
            typer.echo("")
            continue

        # Dedup within this kind by (role, company_id).
        seen_in_kind: set[tuple[str, int]] = set()
        for role, name, conf, co_id in entries:
            key = (role, co_id)
            if key in seen_in_kind:
                continue
            seen_in_kind.add(key)
            is_new = co_id not in everything_seen
            marker = "+" if is_new else " "
            colour = typer.colors.GREEN if is_new else None
            typer.secho(
                f"    {marker} {role:<11} {name}  ({conf})",
                fg=colour,
            )
            everything_seen.add(co_id)
            seen_so_far.setdefault(role, set()).add(co_id)

        # Surface unmatched docs in this stage too — recall holes.
        unmatched_ids = {d.id for d in all_kind_docs} - docs_with_entities.get(kind, set())
        if unmatched_ids:
            for d in all_kind_docs:
                if d.id in unmatched_ids:
                    typer.secho(
                        f"      ? {d.doc_type or '(no type)'}  (doc {d.id} — no entities harvested)",
                        fg=typer.colors.YELLOW,
                    )

        typer.echo("")

    if not seen_so_far:
        typer.echo("  (no entities harvested yet — run `listo da harvest-entities`)")
        return

    typer.echo("  " + "─" * 70)
    typer.echo(f"  {len(everything_seen)} distinct entities across the timeline.")


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
