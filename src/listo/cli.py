from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import typer
from sqlalchemy import func, select, text

from listo.db import session_scope
from listo.models import (
    CouncilApplication,
    CouncilApplicationDocument,
    CouncilRequest,
    CrawlRun,
    RawPage,
)

app = typer.Typer(no_args_is_help=True, add_completion=False)
council_app = typer.Typer(no_args_is_help=True, help="Council DA scraping")
enrich_app = typer.Typer(no_args_is_help=True)
app.add_typer(council_app, name="council")
app.add_typer(enrich_app, name="enrich")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


# ---------------- top-level ----------------


@app.command("status")
def status() -> None:
    """Snapshot of raw_pages, council_applications, documents, and requests."""
    with session_scope() as s:
        rp = s.execute(select(func.count()).select_from(RawPage)).scalar() or 0
        ca = s.execute(select(func.count()).select_from(CouncilApplication)).scalar() or 0
        ca_with_detail = s.execute(
            select(func.count()).select_from(CouncilApplication).where(CouncilApplication.detail_fetched_at.is_not(None))
        ).scalar() or 0
        ca_with_docs = s.execute(
            select(func.count()).select_from(CouncilApplication).where(CouncilApplication.docs_fetched_at.is_not(None))
        ).scalar() or 0
        cad = s.execute(select(func.count()).select_from(CouncilApplicationDocument)).scalar() or 0
        creq = s.execute(select(func.count()).select_from(CouncilRequest)).scalar() or 0
        per_council = s.execute(text("""
            SELECT council_slug, vendor,
                   COUNT(*) AS apps,
                   SUM(detail_fetched_at IS NOT NULL) AS with_detail,
                   SUM(docs_fetched_at IS NOT NULL)   AS with_docs,
                   MIN(lodged_date) AS earliest,
                   MAX(lodged_date) AS latest
              FROM council_applications
             GROUP BY council_slug, vendor
             ORDER BY council_slug, vendor
        """)).fetchall()

    typer.echo(f"raw_pages:                     {rp}")
    typer.echo(f"council_applications:          {ca}  (detail: {ca_with_detail}, docs: {ca_with_docs})")
    typer.echo(f"council_application_documents: {cad}")
    typer.echo(f"council_requests:              {creq}")
    typer.echo("")
    typer.echo("per (council, vendor):")
    for row in per_council:
        typer.echo(
            f"  {row.council_slug:<10} {row.vendor:<20} apps={row.apps:<6} "
            f"detail={row.with_detail or 0:<5} docs={row.with_docs or 0:<5} "
            f"{row.earliest} → {row.latest}"
        )


# ---------------- council ----------------


@council_app.command("list")
def council_list() -> None:
    """List registered councils and the date windows each backend covers."""
    from listo.councils.registry import COUNCILS

    for slug, c in COUNCILS.items():
        typer.echo(f"{slug:<12} {c.name} ({c.state})")
        for b in c.backends:
            cov_from = b.covers_from.isoformat() if b.covers_from else "—"
            cov_to = b.covers_to.isoformat() if b.covers_to else "—"
            typer.echo(f"    {b.name:<24}  covers {cov_from} → {cov_to}")


@council_app.command("scrape")
def council_scrape(
    slug: str = typer.Argument(..., help="council slug (e.g. cogc, newcastle)"),
    date_from: str = typer.Option(..., "--from", help="lodgement date >= YYYY-MM-DD"),
    date_to: str = typer.Option(..., "--to", help="lodgement date <= YYYY-MM-DD"),
    list_only: bool = typer.Option(False, "--list-only", help="phase 1 only: collect listing rows"),
    detail_only: bool = typer.Option(False, "--detail-only", help="phase 2 only: fetch details for already-listed apps"),
    docs_only: bool = typer.Option(False, "--docs-only", help="phase 3 only: download documents for apps with details"),
    detail_limit: int = typer.Option(0, "--detail-limit", help="cap detail fetches this run (0 = no cap)"),
    docs_limit: int = typer.Option(0, "--docs-limit", help="cap doc-download apps this run (0 = no cap)"),
) -> None:
    """Scrape a council across the given lodgement-date window.

    Default behaviour runs all three phases (list → detail → docs).
    Use --list-only / --detail-only / --docs-only to run a single
    phase. Re-running is safe — per-stage timestamps drive resume.
    """
    from listo.councils.orchestrator import run_council
    from listo.councils.registry import get_council

    council = get_council(slug)
    df = date.fromisoformat(date_from)
    dt_to = date.fromisoformat(date_to)

    do_list = True
    do_detail = True
    do_docs = True
    if list_only:
        do_detail = do_docs = False
    if detail_only:
        do_list = do_docs = False
    if docs_only:
        do_list = do_detail = False

    typer.echo(
        f"scraping {council.name} ({slug}) {df} → {dt_to}  "
        f"phases: list={do_list} detail={do_detail} docs={do_docs}"
    )
    stats = run_council(
        council,
        date_from=df,
        date_to=dt_to,
        do_list=do_list,
        do_detail=do_detail,
        do_docs=do_docs,
        detail_limit=detail_limit or None,
        docs_limit=docs_limit or None,
    )
    typer.echo(
        f"done — listed: {stats['list']}, detailed: {stats['detail']}, "
        f"docs apps: {stats['docs']}, doc files: {stats['doc_files']}"
    )


@council_app.command("coverage")
def council_coverage(
    slug: str = typer.Argument(..., help="council slug (e.g. cogc)"),
    status_filter: str = typer.Option("", "--status", help="filter to one status: running / completed / failed / aborted"),
    limit: int = typer.Option(50, "--limit", help="cap rows returned"),
) -> None:
    """Show every scrape attempt against this council, with date window
    and status. Use this to see what's been finished and what's still
    pending. Note: applications are persisted with the *data* slug
    ('cogc') even when invoked via 'cogc_http', so coverage rows for
    the two backends interleave under the same slug.
    """
    where = "council_slug = :slug"
    params = {"slug": slug}
    if status_filter:
        where += " AND status = :status"
        params["status"] = status_filter
    sql = text(f"""
        SELECT date_from, date_to, status, started_at, finished_at,
               TIMESTAMPDIFF(SECOND, started_at, COALESCE(finished_at, NOW())) AS elapsed_s,
               apps_yielded, files_downloaded, backend_name, vendor,
               COALESCE(LEFT(error, 80), '') AS error_snippet
          FROM council_scrape_windows
         WHERE {where}
         ORDER BY started_at DESC
         LIMIT :limit
    """)
    params["limit"] = limit
    with session_scope() as s:
        rows = s.execute(sql, params).fetchall()
    if not rows:
        typer.echo(f"no scrape windows recorded for {slug}")
        return
    typer.echo(
        f"{'date_from':<11} {'date_to':<11} {'status':<10} "
        f"{'apps':>5} {'files':>5} {'elapsed':>8}  {'backend':<26} when"
    )
    for r in rows:
        elapsed = f"{r.elapsed_s}s" if r.elapsed_s is not None else "—"
        when = (r.finished_at or r.started_at).strftime("%Y-%m-%d %H:%M")
        typer.echo(
            f"{r.date_from} {r.date_to} {r.status:<10} "
            f"{r.apps_yielded:>5} {r.files_downloaded:>5} {elapsed:>8}  "
            f"{r.backend_name:<26} {when}"
            + (f"  ERROR: {r.error_snippet}" if r.error_snippet else "")
        )


@council_app.command("resume")
def council_resume(
    slug: str = typer.Argument(..., help="council slug"),
) -> None:
    """Resume scraping for any pending detail/docs work in council_applications.

    Operates over the full lodgement-date span already in the database
    for this council. List phase is skipped (no new dates to fetch).
    """
    from listo.councils.orchestrator import run_council
    from listo.councils.registry import get_council

    with session_scope() as s:
        rng = s.execute(text("""
            SELECT MIN(lodged_date) AS lo, MAX(lodged_date) AS hi
              FROM council_applications WHERE council_slug = :slug
        """), {"slug": slug}).first()
    if not rng or not rng.lo:
        typer.echo(f"no applications in db for {slug} — run `listo council scrape {slug} --from … --to …` first")
        raise typer.Exit(1)

    council = get_council(slug)
    typer.echo(f"resuming {slug} over {rng.lo} → {rng.hi} (detail + docs phases)")
    stats = run_council(
        council,
        date_from=rng.lo,
        date_to=rng.hi,
        do_list=False,
        do_detail=True,
        do_docs=True,
    )
    typer.echo(
        f"done — detailed: {stats['detail']}, docs apps: {stats['docs']}, doc files: {stats['doc_files']}"
    )


# ---------------- enrich ----------------


@enrich_app.command("rates")
def enrich_rates() -> None:
    """Download RBA F5 (Indicator Lending Rates) and upsert into mortgage_rates."""
    from listo import rba

    stats = rba.ingest()
    typer.echo(
        f"RBA F5: {stats.series} series × {stats.months} months — "
        f"{stats.rows_upserted} rows upserted, {stats.skipped_blank} blank cells skipped"
    )

    with session_scope() as s:
        rng = s.execute(text(
            "SELECT MIN(month) AS earliest, MAX(month) AS latest, COUNT(*) AS total "
            "FROM mortgage_rates"
        )).first()
        typer.echo(f"  date range: {rng.earliest} → {rng.latest}, total rows: {rng.total}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
