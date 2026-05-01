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
asic_app = typer.Typer(no_args_is_help=True, help="ASIC Connect Online lookups")
app.add_typer(council_app, name="council")
app.add_typer(enrich_app, name="enrich")
app.add_typer(asic_app, name="asic")

from listo.property_history.cli import property_app  # noqa: E402

app.add_typer(property_app, name="property")

from listo.da_summaries.cli import da_app  # noqa: E402

app.add_typer(da_app, name="da")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


# Default residential-redev allowlist for council DA scraping. Derived
# empirically from the on-disk filter the team applied during cleanup —
# OPW (Operational Works), MIN (Minor Change), OPV, OPT, OTH, TRO are
# civil/admin work and were ~91% deleted; the codes below were ~95% kept.
RESIDENTIAL_TYPE_CODES = "MCU,COM,ROL,EDA,EXA,PDA,FDA"


def _parse_types(types: str | None) -> set[str] | None:
    """Translate the --types CLI option into the set the orchestrator wants.

    'all' / '*' / '' → None (no filter, fetch everything).
    Anything else → uppercased {'MCU', 'COM', ...}.
    """
    if not types:
        return None
    t = types.strip()
    if t.lower() in ("all", "*"):
        return None
    return {x.strip().upper() for x in t.split(",") if x.strip()}


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


@council_app.command("fetch-app-docs")
def council_fetch_app_docs(
    app_id: str = typer.Argument(..., help="application_id, e.g. MCU/2020/492"),
) -> None:
    """Re-run the docs phase for one application, downloading every
    listed document (forces LISTO_DOWNLOAD_ALL). Use when phase 1 / 2
    of the LLM pipeline can't find applicant/builder/architect because
    the relevant tier-2 docs were never downloaded.
    """
    from listo.councils.orchestrator import fetch_app_docs

    apps_done, files_done = fetch_app_docs(app_id)
    typer.echo(f"  apps:  {apps_done}")
    typer.echo(f"  files: {files_done}")


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
    types: str = typer.Option(
        RESIDENTIAL_TYPE_CODES, "--types",
        help="comma-separated type-code allowlist for detail+docs fetch. "
             "Pass 'all' to disable the filter and fetch every category.",
    ),
) -> None:
    """Scrape a council across the given lodgement-date window.

    Default behaviour runs all three phases (list → detail → docs).
    Use --list-only / --detail-only / --docs-only to run a single
    phase. Re-running is safe — per-stage timestamps drive resume.

    The default --types allowlist captures residential-redev DA codes
    (MCU/COM/ROL/EDA/EXA/PDA/FDA) and skips civil/admin work like
    OPW/MIN/OPV/OPT/OTH so we don't waste bandwidth on categories we
    won't use downstream. Listing rows are still recorded for
    excluded types — only detail+docs get skipped.
    """
    from listo.councils.orchestrator import run_council
    from listo.councils.registry import get_council

    council = get_council(slug)
    df = date.fromisoformat(date_from)
    dt_to = date.fromisoformat(date_to)
    allowed = _parse_types(types)

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
        f"phases: list={do_list} detail={do_detail} docs={do_docs}  "
        f"types: {sorted(allowed) if allowed else 'ALL'}"
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
        allowed_type_codes=allowed,
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


def _iter_months(start: date, end: date) -> list[tuple[date, date]]:
    """Return [(first-of-month, last-of-month), ...] inclusive."""
    out: list[tuple[date, date]] = []
    y, m = start.year, start.month
    while True:
        first = date(y, m, 1)
        if first > end:
            break
        if m == 12:
            last = date(y, 12, 31)
        else:
            last = date(y, m + 1, 1) - timedelta(days=1)
        out.append((first, last))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def _month_completed(slug: str, first: date, last: date) -> dict | None:
    """If a council_scrape_windows row already has status='completed' for
    exactly this month boundary, return a dict with its stats. Otherwise
    None.
    """
    with session_scope() as s:
        row = s.execute(text("""
            SELECT id, status, apps_yielded, files_downloaded, finished_at
              FROM council_scrape_windows
             WHERE council_slug = :slug
               AND date_from = :df
               AND date_to = :dt
               AND status = 'completed'
             ORDER BY finished_at DESC
             LIMIT 1
        """), {"slug": slug, "df": first.isoformat(), "dt": last.isoformat()}).fetchone()
    return dict(row._mapping) if row else None


@council_app.command("scrape-monthly")
def council_scrape_monthly(
    slug: str = typer.Argument(..., help="council slug (e.g. cogc, newcastle)"),
    date_from: str = typer.Option(..., "--from", help="start month YYYY-MM-01"),
    date_to: str = typer.Option(..., "--to", help="end month YYYY-MM-DD (inclusive)"),
    list_only: bool = typer.Option(False, "--list-only"),
    detail_only: bool = typer.Option(False, "--detail-only"),
    docs_only: bool = typer.Option(False, "--docs-only"),
    detail_limit: int = typer.Option(0, "--detail-limit"),
    docs_limit: int = typer.Option(0, "--docs-limit"),
    worker_index: int = typer.Option(0, "--worker-index", help="0-based partition index for this worker"),
    worker_count: int = typer.Option(1, "--worker-count", help="total parallel workers splitting the months"),
    force: bool = typer.Option(False, "--force", help="re-scrape months already marked completed"),
    types: str = typer.Option(
        RESIDENTIAL_TYPE_CODES, "--types",
        help="comma-separated type-code allowlist; pass 'all' to fetch every category.",
    ),
) -> None:
    """Iterate the date range one month at a time, skipping months already
    marked `completed` in council_scrape_windows. Each month is its own
    scrape attempt — clean failure boundaries, clean resume, easy
    'which months are done?' query.

    Run multiple parallel workers (same machine OR different machines) by
    setting --worker-index / --worker-count. Each worker takes months
    where (month_index % count == index). Disjoint sets, no claim
    contention.
    """
    from listo.councils.orchestrator import run_council
    from listo.councils.registry import get_council

    council = get_council(slug)
    df = date.fromisoformat(date_from)
    dt_to = date.fromisoformat(date_to)
    months = _iter_months(df, dt_to)
    allowed = _parse_types(types)

    do_list = not (detail_only or docs_only)
    do_detail = not (list_only or docs_only)
    do_docs = not (list_only or detail_only)

    typer.echo(
        f"scraping {council.name} ({slug}) {len(months)} month(s) "
        f"worker {worker_index}/{worker_count}  "
        f"phases: list={do_list} detail={do_detail} docs={do_docs}  "
        f"types: {sorted(allowed) if allowed else 'ALL'}"
    )

    n_done = 0
    n_skipped = 0
    n_failed = 0
    for idx, (first, last) in enumerate(months):
        if idx % worker_count != worker_index:
            continue
        existing = _month_completed(slug, first, last) if not force else None
        if existing:
            typer.secho(
                f"  [{first.strftime('%Y-%m')}] ✓ already completed "
                f"({existing['apps_yielded']} apps, {existing['files_downloaded']} files)",
                fg=typer.colors.GREEN,
            )
            n_skipped += 1
            continue

        typer.echo(f"  [{first.strftime('%Y-%m')}] running…")
        try:
            stats = run_council(
                council, date_from=first, date_to=last,
                do_list=do_list, do_detail=do_detail, do_docs=do_docs,
                detail_limit=detail_limit or None,
                docs_limit=docs_limit or None,
                allowed_type_codes=allowed,
            )
            typer.secho(
                f"  [{first.strftime('%Y-%m')}] ✓ done — list:{stats['list']} "
                f"detail:{stats['detail']} docs:{stats['docs']} files:{stats['doc_files']}",
                fg=typer.colors.GREEN,
            )
            n_done += 1
        except Exception as exc:  # noqa: BLE001
            typer.secho(
                f"  [{first.strftime('%Y-%m')}] ✗ FAILED: {exc}",
                fg=typer.colors.RED,
            )
            n_failed += 1
            # Don't bail — keep walking months so a transient hiccup
            # doesn't block the rest of the backfill.

    typer.echo("")
    typer.echo(f"summary: {n_done} done, {n_skipped} already-completed, {n_failed} failed")


@council_app.command("months")
def council_months(
    slug: str = typer.Argument(..., help="council slug"),
    date_from: str = typer.Option(None, "--from", help="start month YYYY-MM-01 (default: earliest in db)"),
    date_to: str = typer.Option(None, "--to", help="end month YYYY-MM-DD (default: today)"),
) -> None:
    """Per-month visualisation of scrape progress.

    Shows for every month in the range:
      ✓ COMPLETED — has a council_scrape_windows row with status='completed'
      … RUNNING — has a 'running' row but no completed yet
      ✗ FAILED — has a 'failed' row but no completed
      ○ PENDING — no scrape attempt for this exact month boundary

    Plus per-month app + doc counts so partial coverage is obvious.
    """
    today = date.today()
    if not date_from:
        with session_scope() as s:
            row = s.execute(text("""
                SELECT MIN(lodged_date) AS lo FROM council_applications WHERE council_slug = :slug
            """), {"slug": slug}).fetchone()
        df = (row.lo if row and row.lo else today).replace(day=1)
    else:
        df = date.fromisoformat(date_from)
    dt_to = date.fromisoformat(date_to) if date_to else today
    months = _iter_months(df, dt_to)

    # One round-trip: pull all relevant scrape_windows + per-month app counts.
    with session_scope() as s:
        windows = s.execute(text("""
            SELECT date_from, date_to, status, apps_yielded, files_downloaded
              FROM council_scrape_windows
             WHERE council_slug = :slug
               AND date_from BETWEEN :lo AND :hi
        """), {"slug": slug, "lo": df.isoformat(), "hi": dt_to.isoformat()}).fetchall()
        counts = s.execute(text("""
            SELECT YEAR(lodged_date) AS y,
                   MONTH(lodged_date) AS m,
                   COUNT(*) AS apps,
                   SUM(detail_fetched_at IS NOT NULL) AS det,
                   SUM(docs_fetched_at  IS NOT NULL) AS docs
              FROM council_applications
             WHERE council_slug = :slug
               AND lodged_date BETWEEN :lo AND :hi
             GROUP BY y, m
        """), {"slug": slug, "lo": df.isoformat(), "hi": dt_to.isoformat()}).fetchall()

    # Index by exact (date_from, date_to). For "month" rows we keep the
    # most relevant status: completed > running > failed > pending.
    STATUS_RANK = {"completed": 3, "running": 2, "failed": 1, "aborted": 0}
    by_window: dict[tuple[date, date], dict] = {}
    for w in windows:
        key = (w.date_from, w.date_to)
        cur = by_window.get(key)
        if cur is None or STATUS_RANK.get(w.status, -1) > STATUS_RANK.get(cur["status"], -1):
            by_window[key] = {"status": w.status, "apps": w.apps_yielded or 0, "files": w.files_downloaded or 0}

    counts_by_month = {(row.y, row.m): row for row in counts}

    # Drop any broad-window status — only per-month completion is
    # authoritative. We've seen broad scrapes report 'completed' while
    # actually missing months in the middle (rate-limit truncations etc.).
    typer.echo(
        f"{'month':<8} {'status':<11} {'apps':>5} {'detail':>6} {'docs':>5}"
    )
    for first, last in months:
        info = by_window.get((first, last))

        if info is None:
            badge = typer.style("○ pending  ", fg=typer.colors.WHITE, dim=True)
        elif info["status"] == "completed":
            badge = typer.style("✓ completed", fg=typer.colors.GREEN)
        elif info["status"] == "running":
            badge = typer.style("… running  ", fg=typer.colors.YELLOW)
        elif info["status"] == "failed":
            badge = typer.style("✗ failed   ", fg=typer.colors.RED)
        else:
            badge = typer.style(f"? {info['status']:<9}", fg=typer.colors.WHITE)

        # Per-month app/doc/detail counts — always shown, regardless of
        # window status. This is the real evidence of what's in the db.
        c = counts_by_month.get((first.year, first.month))
        apps_str = f"{int(c.apps):>5}" if c else "    —"
        det_str = f"{int(c.det or 0):>6}" if c else "     —"
        docs_str = f"{int(c.docs or 0):>5}" if c else "    —"

        typer.echo(
            f"{first.strftime('%Y-%m')}  {badge} {apps_str} {det_str} {docs_str}"
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


# ---------------- asic ----------------


@asic_app.command("lookup")
def asic_lookup(acn: str) -> None:
    """Look up a single ACN on ASIC Connect Online and persist the result."""
    from listo import asic as asic_mod

    detail = asic_mod.lookup_acn(acn)
    if detail is None:
        typer.echo(f"ACN {acn}: not found")
        raise typer.Exit(code=1)
    stats = asic_mod.persist_details([detail])
    typer.echo(
        f"{detail.name}  ACN {detail.acn}  ABN {detail.abn or '-'}  "
        f"{detail.status}  {detail.locality or '-'}  "
        f"(reg {detail.registration_date})"
    )
    typer.echo(f"  persisted: {stats}")


@asic_app.command("search")
def asic_search(
    name: str,
    types: str = typer.Option(
        "Australian Proprietary Company",
        help="Comma-separated Type values to fetch detail for. "
             "Pass 'all' to fetch every row that has an ACN.",
    ),
    sleep: float = typer.Option(3.0, help="Seconds between detail fetches."),
) -> None:
    """Search ASIC by name; fetch + persist detail for matching rows.

    Defaults to Australian Proprietary Company (Pty Ltd) only — the
    case the listo redev pipeline cares about.
    """
    from listo import asic as asic_mod

    if types.strip().lower() == "all":
        types_to_fetch = tuple({"Australian Proprietary Company",
                                "Australian Public Company",
                                "Registered Australian Body",
                                "Foreign Company"})
    else:
        types_to_fetch = tuple(t.strip() for t in types.split(",") if t.strip())

    rows, details = asic_mod.search_by_name(
        name, types_to_fetch=types_to_fetch, sleep_between=sleep,
    )
    typer.echo(f"name search {name!r}: {len(rows)} total rows")
    for r in rows:
        marker = "*" if r.acn and r.type_text in types_to_fetch else " "
        typer.echo(
            f"  {marker} {r.acn or '-':<10} {r.name[:40]:<40} "
            f"{r.type_text[:32]:<32} {r.status}"
        )
    if not details:
        typer.echo("no detail records fetched")
        return
    stats = asic_mod.persist_details(details)
    typer.echo(f"\npersisted {len(details)} detail records: {stats}")


@asic_app.command("status")
def asic_status() -> None:
    """Counts of `companies` rows, broken down by ASIC enrichment state."""
    with session_scope() as s:
        total = s.execute(text("SELECT COUNT(*) FROM companies")).scalar() or 0
        with_asic = s.execute(
            text("SELECT COUNT(*) FROM companies WHERE asic_fetched_at IS NOT NULL")
        ).scalar() or 0
        registered = s.execute(text(
            "SELECT COUNT(*) FROM companies WHERE asic_status = 'Registered'"
        )).scalar() or 0
        latest = s.execute(text(
            "SELECT MAX(asic_fetched_at) FROM companies"
        )).scalar()
        by_locality = s.execute(text("""
            SELECT asic_locality, COUNT(*) AS n
              FROM companies
             WHERE asic_locality IS NOT NULL
             GROUP BY asic_locality
             ORDER BY n DESC
             LIMIT 10
        """)).fetchall()
    typer.echo(f"companies total:        {total}")
    typer.echo(f"  with ASIC enrichment: {with_asic}")
    typer.echo(f"  Registered status:    {registered}")
    typer.echo(f"  last fetched:         {latest}")
    if by_locality:
        typer.echo("\ntop registered-office localities:")
        for row in by_locality:
            typer.echo(f"  {row.n:>4}  {row.asic_locality}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
