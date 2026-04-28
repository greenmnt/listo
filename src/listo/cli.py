from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import typer
from sqlalchemy import bindparam, func, select, text

from listo.db import session_scope
from listo.fetch import domain as domain_fetch
from listo.fetch import realestate as realestate_fetch
from listo.fetch.cookies import have_kasada_token, load_cookies_for
from listo.models import CrawlRun, Listing, Property, RawPage, Sale
from listo.parse import runner as parse_runner
from listo.suburbs import GOLD_COAST_SUBURBS, TARGET_SUBURBS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = typer.Typer(no_args_is_help=True, add_completion=False)
fetch_app = typer.Typer(no_args_is_help=True)
parse_app = typer.Typer(no_args_is_help=True)
match_app = typer.Typer(no_args_is_help=True)
enrich_app = typer.Typer(no_args_is_help=True)
app.add_typer(fetch_app, name="fetch")
app.add_typer(parse_app, name="parse")
app.add_typer(match_app, name="match")
app.add_typer(enrich_app, name="enrich")


# Default staleness windows by page_type. sold listings are append-only and
# don't change once recorded; buy listings turn over weekly.
_DEFAULT_MAX_AGE_DAYS = {
    "sold": 30,
    "buy": 7,
    "rent": 7,
}


def _suburb_list(name: str) -> list[tuple[str, str, str]]:
    """Resolve a named suburb list (gold_coast | target)."""
    if name == "gold_coast":
        return GOLD_COAST_SUBURBS
    if name == "target":
        return TARGET_SUBURBS
    raise typer.BadParameter(f"unknown suburb list {name!r}; choose gold_coast or target")


def _resolve(suburb: str) -> tuple[str, str]:
    """Find postcode + state for a single-suburb command. Searches both lists."""
    candidates = list(GOLD_COAST_SUBURBS) + list(TARGET_SUBURBS)
    matches = [(pc, st) for s, pc, st in candidates if s.lower() == suburb.lower()]
    if not matches:
        raise typer.BadParameter(
            f"{suburb!r} not in any suburb list — add it to src/listo/suburbs.py first."
        )
    if len(set(matches)) > 1:
        # Suburb name collides across states (e.g. Brighton VIC vs SA, Palm Beach NSW vs QLD).
        states = ", ".join(sorted({f"{pc}/{st}" for pc, st in matches}))
        raise typer.BadParameter(
            f"{suburb!r} is ambiguous (found in {states}). Use --postcode and --state explicitly."
        )
    return matches[0]


# ---------------- fetch -----------------


def _fetch_one(
    source: str,
    suburb: str,
    postcode: str,
    state: str,
    page_type: str,
    max_pages: int,
    max_age_days: int | None,
    force: bool,
) -> None:
    if source == "realestate":
        result = realestate_fetch.fetch_suburb(
            suburb, postcode, page_type, state=state,
            max_pages=max_pages, max_age_days=max_age_days, force=force,
        )
    elif source == "domain":
        result = domain_fetch.fetch_suburb(
            suburb, postcode, page_type, state=state,
            max_pages=max_pages, max_age_days=max_age_days, force=force,
        )
    else:
        raise typer.BadParameter(f"unknown source: {source}")
    typer.echo(
        f"{source} {page_type} {suburb} {postcode}/{state}: pages={result.pages_fetched} "
        f"last_page={result.last_page} status={result.status}"
        + (f" error={result.error}" if result.error else "")
    )


def _max_age_for(page_type: str, override: int | None) -> int:
    if override is not None:
        return override
    return _DEFAULT_MAX_AGE_DAYS.get(page_type, 30)


@fetch_app.command("realestate")
def fetch_realestate(
    suburb: str = typer.Option(..., "--suburb"),
    page_type: str = typer.Option("sold", "--page-type", help="sold | buy"),
    max_pages: int = typer.Option(80, "--max-pages"),
    max_age_days: Optional[int] = typer.Option(None, "--max-age-days",
        help="skip fetch if a successful run finished within this window (default 30 sold, 7 buy/rent)"),
    force: bool = typer.Option(False, "--force", help="bypass the dedup skip checks"),
) -> None:
    pc, st = _resolve(suburb)
    _fetch_one("realestate", suburb, pc, st, page_type, max_pages,
               _max_age_for(page_type, max_age_days), force)


@fetch_app.command("domain")
def fetch_domain(
    suburb: str = typer.Option(..., "--suburb"),
    page_type: str = typer.Option("sold", "--page-type", help="sold | buy"),
    max_pages: int = typer.Option(80, "--max-pages"),
    max_age_days: Optional[int] = typer.Option(None, "--max-age-days"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    pc, st = _resolve(suburb)
    _fetch_one("domain", suburb, pc, st, page_type, max_pages,
               _max_age_for(page_type, max_age_days), force)


@fetch_app.command("all")
def fetch_all(
    page_type: str = typer.Option("sold", "--page-type", help="sold | buy"),
    max_pages: int = typer.Option(80, "--max-pages"),
    sources: str = typer.Option("realestate,domain", "--sources"),
    suburb_list: str = typer.Option("gold_coast", "--suburb-list",
        help="gold_coast (75 suburbs) | target (~135 wealthy/coastal suburbs)"),
    max_age_days: Optional[int] = typer.Option(None, "--max-age-days"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    """Iterate every suburb in the chosen list against each source."""
    src_list = [s.strip() for s in sources.split(",") if s.strip()]
    suburbs = _suburb_list(suburb_list)
    age = _max_age_for(page_type, max_age_days)
    typer.echo(f"fetching {len(suburbs)} suburbs × {len(src_list)} sources, "
               f"page_type={page_type}, max_pages={max_pages}, max_age_days={age}, force={force}")
    for suburb, postcode, state in suburbs:
        for src in src_list:
            try:
                _fetch_one(src, suburb, postcode, state, page_type, max_pages, age, force)
            except Exception as e:  # noqa: BLE001
                typer.echo(f"ERROR {src} {suburb}: {e}", err=True)


# ---------------- parse -----------------


@parse_app.command("realestate")
def parse_realestate(limit: Optional[int] = typer.Option(None, "--limit")) -> None:
    stats = parse_runner.parse_unparsed(source="realestate", limit=limit)
    typer.echo(f"realestate: {stats}")


@parse_app.command("domain")
def parse_domain(limit: Optional[int] = typer.Option(None, "--limit")) -> None:
    stats = parse_runner.parse_unparsed(source="domain", limit=limit)
    typer.echo(f"domain: {stats}")


@parse_app.command("all")
def parse_all(limit: Optional[int] = typer.Option(None, "--limit")) -> None:
    stats = parse_runner.parse_unparsed(limit=limit)
    typer.echo(f"all: {stats}")


@parse_app.command("reparse")
def parse_reparse(source: Optional[str] = typer.Option(None, "--source")) -> None:
    """Force re-parse of all (or filtered) raw_pages — clears parsed_at first."""
    stats = parse_runner.reparse_all(source=source)
    typer.echo(f"reparse: {stats}")


# ---------------- status / coverage / match -----------------


@app.command("status")
def status() -> None:
    with session_scope() as s:
        rp = s.execute(select(func.count()).select_from(RawPage)).scalar() or 0
        rp_unparsed = s.execute(
            select(func.count()).select_from(RawPage).where(RawPage.parsed_at.is_(None))
        ).scalar() or 0
        rp_err = s.execute(
            select(func.count()).select_from(RawPage).where(RawPage.parse_error.isnot(None))
        ).scalar() or 0
        props = s.execute(select(func.count()).select_from(Property)).scalar() or 0
        listings = s.execute(select(func.count()).select_from(Listing)).scalar() or 0
        sales = s.execute(select(func.count()).select_from(Sale)).scalar() or 0
        runs_recent = s.execute(
            select(CrawlRun)
            .where(CrawlRun.started_at >= datetime.utcnow() - timedelta(days=1))
            .order_by(CrawlRun.started_at.desc())
            .limit(10)
        ).scalars().all()

    typer.echo(f"raw_pages:         {rp} (unparsed: {rp_unparsed}, errors: {rp_err})")
    typer.echo(f"properties:        {props}")
    typer.echo(f"listings:          {listings}")
    typer.echo(f"sales:             {sales}")
    typer.echo("recent crawl_runs (last 24h):")
    for r in runs_recent:
        typer.echo(
            f"  [{r.id}] {r.source} {r.page_type} {r.suburb} {r.postcode}: "
            f"{r.status} pages={r.pages_fetched} last_page={r.last_page}"
        )


@app.command("coverage")
def coverage(
    suburb_list: str = typer.Option("target", "--suburb-list"),
    page_type: str = typer.Option("sold", "--page-type"),
    max_age_days: int = typer.Option(30, "--max-age-days",
        help="rows finished within this window count as 'fresh'"),
) -> None:
    """Show what's been fetched per (source, suburb) for a chosen suburb list."""
    suburbs = _suburb_list(suburb_list)
    page_type_enum = {"sold": "search_sold", "buy": "search_buy", "rent": "search_rent"}[page_type]
    cutoff = datetime.utcnow() - timedelta(days=max_age_days)

    with session_scope() as s:
        rows = s.execute(text("""
            SELECT source, suburb, postcode,
                   MAX(finished_at) AS last_finished,
                   MAX(pages_fetched) AS last_pages,
                   MAX(status)         AS last_status
            FROM crawl_runs
            WHERE page_type = :pt
              AND status = 'done'
              AND finished_at >= :cutoff
            GROUP BY source, suburb, postcode
        """), {"pt": page_type_enum, "cutoff": cutoff}).fetchall()

    fresh = {(r.source, r.suburb.lower(), r.postcode): r for r in rows}

    sources = ["realestate", "domain"]
    by_source: dict[str, dict[str, int]] = {src: {"fresh": 0, "stale": 0, "missing": 0} for src in sources}

    typer.echo(f"\ncoverage for {len(suburbs)} suburbs × {sources}, page_type={page_type}, max_age_days={max_age_days}\n")
    typer.echo(f"{'suburb':<25} {'pc':<5} {'st':<3} {'realestate':<22} {'domain':<22}")
    typer.echo("-" * 80)
    for suburb, pc, st in suburbs:
        cells = []
        for src in sources:
            row = fresh.get((src, suburb.lower(), pc))
            if row:
                age = (datetime.utcnow() - row.last_finished).days
                cells.append(f"✓ {row.last_pages}p {age}d ago")
                by_source[src]["fresh"] += 1
            else:
                cells.append("· missing")
                by_source[src]["missing"] += 1
        typer.echo(f"{suburb:<25} {pc:<5} {st:<3} {cells[0]:<22} {cells[1]:<22}")
    typer.echo("\nsummary:")
    for src in sources:
        c = by_source[src]
        typer.echo(f"  {src:<11} fresh={c['fresh']:<4} missing={c['missing']:<4}")


@app.command("cookies")
def cookies_inspect(
    domain: str = typer.Argument(..., help="apex domain, e.g. realestate.com.au"),
) -> None:
    """List cookies present in Chromium for a given domain."""
    try:
        jar = load_cookies_for(domain)
    except RuntimeError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(1)
    if not jar:
        typer.echo(f"no cookies for {domain} — visit it in Chromium first")
        return
    typer.echo(f"{len(jar)} cookies for {domain} (Kasada token: {have_kasada_token(jar)}):")
    for name, value in sorted(jar.items()):
        preview = value if len(value) <= 50 else value[:47] + "..."
        typer.echo(f"  {name} = {preview}")


def _fmt_money(n) -> str:
    if n is None:
        return "?"
    return f"${int(n):,}"


def _fmt_state(status: str | None, sold_date, sold_price, listed_price) -> str:
    """Build a 'SOLD $X on Y' or 'LISTED $X' display string."""
    if sold_date is not None:
        return f"SOLD {_fmt_money(sold_price)} on {sold_date.isoformat()}"
    if status == "active":
        return f"LISTED {_fmt_money(listed_price)} (active)"
    if status == "sold":
        return f"SOLD {_fmt_money(sold_price or listed_price)} (date unknown)"
    if status:
        return f"{status.upper()} {_fmt_money(listed_price)}"
    return "?"


# Property types that indicate an EXISTING apartment building, not a viable
# redevelopment subject. If the "house" row has one of these types, the lot is
# almost certainly an apartment block changing hands rather than a duplex play.
_NON_HOUSE_TYPES = (
    "Block Of Units", "Block of Units",
    "Apartment", "Unit", "Flat",
    "Apartment / Unit / Flat",
    "New Apartments / Off the Plan",
    "Studio",
)


@match_app.command("duplexes")
def match_duplexes(
    min_house_lot: int = typer.Option(0, "--min-house-lot",
        help="filter: pre-redev house lot ≥ this many m² (0 = no filter)"),
    max_house_lot: int = typer.Option(0, "--max-house-lot",
        help="filter: pre-redev house lot ≤ this many m² (0 = no filter)"),
    strict_order: bool = typer.Option(True, "--strict-order/--no-strict-order",
        help="require house sale to precede unit sales (filters out existing apartment blocks)"),
    exclude_apartment_blocks: bool = typer.Option(True,
        "--exclude-apartment-blocks/--include-apartment-blocks",
        help=f"exclude lots where the 'house' is actually one of: {', '.join(_NON_HOUSE_TYPES)}"),
    max_units: int = typer.Option(4, "--max-units",
        help="exclude lots with more units than this (default 4 — covers duplex/triplex/quad). Set to 0 for no filter."),
) -> None:
    """Find lots that appear both as a single house and as multiple units.

    Default behaviour requires temporal ordering: the most recent house sale
    must predate the earliest unit sale (or no unit has sold yet). This filters
    out cases like 'apartment in an existing block sold in 2022, then the whole
    block sold in 2025' — that's not a redevelopment, it's just a building changing
    hands. Pass --no-strict-order to see all candidates regardless of order.

    Output shows:
      - sale price + date for the original house (if sold)
      - sale price + date OR current asking price for each unit (per status)
      - sources (which sites have data on this property)
      - sample URLs to the listings
    """
    sql = text(
        """
        WITH
          prop_listings AS (
            SELECT property_id,
                   MAX(land_size_m2)                            AS land_m2,
                   MAX(url)                                     AS sample_url,
                   MAX(status)                                  AS status,
                   MAX(price_max)                               AS list_price,
                   GROUP_CONCAT(DISTINCT source)                AS sources,
                   GROUP_CONCAT(DISTINCT property_type)         AS types
            FROM listings GROUP BY property_id
          ),
          prop_sales AS (
            SELECT property_id,
                   MIN(sold_date)                               AS first_sold_date,
                   MAX(sold_date)                               AS last_sold_date,
                   MAX(sold_price)                              AS last_sold_price
            FROM sales GROUP BY property_id
          )
        SELECT p.match_key,
               MAX(p.suburb)                                                                   AS suburb,
               MAX(p.postcode)                                                                 AS postcode,
               MIN(CONCAT(p.street_number, ' ', p.street_name))                                AS street,
               SUM(CASE WHEN p.unit_number = ''  THEN 1 ELSE 0 END)                            AS n_houses,
               SUM(CASE WHEN p.unit_number <> '' THEN 1 ELSE 0 END)                            AS n_units,
               MAX(CASE WHEN p.unit_number = ''  THEN pl.land_m2 END)                          AS house_lot_m2,
               GROUP_CONCAT(DISTINCT p.property_type)                                          AS prop_types,
               GROUP_CONCAT(DISTINCT pl.sources)                                               AS all_sources,
               GROUP_CONCAT(DISTINCT p.unit_number ORDER BY p.unit_number)                     AS units,
               -- house side: latest sale (the developer's purchase price)
               MIN(CASE WHEN p.unit_number = ''  THEN pl.sample_url END)                       AS house_url,
               MAX(CASE WHEN p.unit_number = ''  THEN pl.status END)                           AS house_status,
               MAX(CASE WHEN p.unit_number = ''  THEN pl.list_price END)                       AS house_list_price,
               MAX(CASE WHEN p.unit_number = ''  THEN ps.last_sold_date END)                   AS house_sold_date,
               MAX(CASE WHEN p.unit_number = ''  THEN ps.last_sold_price END)                  AS house_sold_price,
               -- one representative unit: earliest sale (when the redev first hit the market)
               MIN(CASE WHEN p.unit_number <> '' THEN pl.sample_url END)                       AS unit_url,
               MAX(CASE WHEN p.unit_number <> '' THEN pl.status END)                           AS unit_status,
               MAX(CASE WHEN p.unit_number <> '' THEN pl.list_price END)                       AS unit_list_price,
               MIN(CASE WHEN p.unit_number <> '' THEN ps.first_sold_date END)                  AS unit_sold_date,
               MIN(CASE WHEN p.unit_number <> '' THEN ps.last_sold_price END)                  AS unit_sold_price
        FROM properties p
        LEFT JOIN prop_listings pl ON pl.property_id = p.id
        LEFT JOIN prop_sales    ps ON ps.property_id = p.id
        GROUP BY p.match_key
        HAVING SUM(CASE WHEN p.unit_number = ''  THEN 1 ELSE 0 END) >= 1
           AND SUM(CASE WHEN p.unit_number <> '' THEN 1 ELSE 0 END) >= 1
           AND ( :max_units = 0 OR SUM(CASE WHEN p.unit_number <> '' THEN 1 ELSE 0 END) <= :max_units )
           -- lot-size filter is fail-open: if we don't have the lot data,
           -- the candidate still qualifies (the address pattern alone is
           -- enough signal — a "X St" + "Y/X St" pair with no land data is
           -- still likely a duplex play).
           AND ( :min_lot = 0
                 OR MAX(CASE WHEN p.unit_number = '' THEN pl.land_m2 END) IS NULL
                 OR MAX(CASE WHEN p.unit_number = '' THEN pl.land_m2 END) >= :min_lot )
           AND ( :max_lot = 0
                 OR MAX(CASE WHEN p.unit_number = '' THEN pl.land_m2 END) IS NULL
                 OR MAX(CASE WHEN p.unit_number = '' THEN pl.land_m2 END) <= :max_lot )
           AND (
                NOT :strict_order
                OR (
                    MAX(CASE WHEN p.unit_number = '' THEN ps.last_sold_date END) IS NOT NULL
                    AND (
                        MIN(CASE WHEN p.unit_number <> '' THEN ps.first_sold_date END) IS NULL
                        OR  MAX(CASE WHEN p.unit_number = ''  THEN ps.last_sold_date END)
                          < MIN(CASE WHEN p.unit_number <> '' THEN ps.first_sold_date END)
                    )
                )
           )
           AND (
                NOT :exclude_blocks
                OR MAX(CASE WHEN p.unit_number = '' THEN p.property_type END) IS NULL
                OR MAX(CASE WHEN p.unit_number = '' THEN p.property_type END) NOT IN :non_house_types
           )
        ORDER BY n_units DESC, n_houses DESC
        LIMIT 200
        """
    ).bindparams(bindparam("non_house_types", expanding=True))
    detail_sql = text(
        """
        SELECT p.id, p.unit_number, p.property_type, p.suburb, p.postcode,
               p.street_number, p.street_name, p.match_key,
               (SELECT MAX(land_size_m2) FROM listings WHERE property_id = p.id) AS land_m2,
               (SELECT MAX(sold_date)  FROM sales    WHERE property_id = p.id) AS last_sold_date,
               (SELECT MAX(sold_price) FROM sales    WHERE property_id = p.id) AS last_sold_price,
               (SELECT MAX(status)     FROM listings WHERE property_id = p.id) AS list_status,
               (SELECT MAX(price_max)  FROM listings WHERE property_id = p.id) AS list_price,
               (SELECT MAX(price_text) FROM listings WHERE property_id = p.id) AS list_price_text,
               (SELECT GROUP_CONCAT(DISTINCT source) FROM listings WHERE property_id = p.id) AS sources,
               (SELECT MIN(url) FROM listings WHERE property_id = p.id) AS url
        FROM properties p
        WHERE p.match_key = :mk
        ORDER BY p.unit_number
        """
    )
    with session_scope() as s:
        candidate_keys = s.execute(sql, {
            "min_lot": min_house_lot,
            "max_lot": max_house_lot,
            "max_units": max_units,
            "strict_order": strict_order,
            "exclude_blocks": exclude_apartment_blocks,
            "non_house_types": list(_NON_HOUSE_TYPES),
        }).fetchall()
        if not candidate_keys:
            typer.echo("no duplex candidates found")
            return

        label = " (strict temporal ordering)" if strict_order else " (all candidates, no order check)"
        typer.echo(f"{len(candidate_keys)} duplex candidates{label}, lot range [{min_house_lot},{max_house_lot or '∞'}]:\n")

        for cand in candidate_keys:
            details = s.execute(detail_sql, {"mk": cand.match_key}).fetchall()
            if not details:
                continue
            # Header from any row (suburb/postcode/street are the same across the match_key)
            header = details[0]
            lot = next((d.land_m2 for d in details if d.unit_number == "" and d.land_m2 is not None), None)
            typer.echo(f"  {header.suburb}, {header.postcode} — {header.street_number} {header.street_name}")
            typer.echo(f"      lot_m²={lot if lot is not None else '?'}  units={len(details)}")
            for d in details:
                label = "house" if d.unit_number == "" else f"unit {d.unit_number}"
                state = _fmt_state(d.list_status, d.last_sold_date, d.last_sold_price, d.list_price)
                typer.echo(f"      {label:<10} {state}  type={d.property_type or '?'}  src={d.sources or '?'}")
                if d.url:
                    typer.echo(f"                 {d.url}")
            typer.echo("")


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
        latest = s.execute(text("""
            SELECT series_id, series_label, month, rate_pct
            FROM mortgage_rates
            WHERE series_id IN (
                'FILRHLBVD','FILRHL3YF','FILRHLBVDI','FILRHL3YFI',     -- discounted/fixed (real-customer rates)
                'FILRHLBVS','FILRHLBVSI','FILRHLBVO'                    -- standard reference rates (rarely paid)
            )
              AND month = (SELECT MAX(month) FROM mortgage_rates WHERE series_id='FILRHLBVD')
            ORDER BY FIELD(series_id,
                'FILRHLBVD','FILRHL3YF','FILRHLBVDI','FILRHL3YFI',
                'FILRHLBVO','FILRHLBVS','FILRHLBVSI')
        """)).fetchall()
        typer.echo("  latest housing rates (** = use this in financial models):")
        primary = {"FILRHLBVD", "FILRHL3YF", "FILRHLBVDI", "FILRHL3YFI"}
        for r in latest:
            label = (r.series_label or "").replace("Lending rates; Housing loans; Banks; ", "")[:60]
            marker = "**" if r.series_id in primary else "  "
            typer.echo(f"  {marker}{r.series_id:<14} {r.rate_pct:>6.3f}%  {label}")


@enrich_app.command("planning")
def enrich_planning(
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run",
        help="dry-run lists candidates that would be enriched without making any HTTP calls"),
) -> None:
    """List or fetch DA records for duplex candidates.

    Currently dry-run only — prints which candidate addresses don't yet have
    DA data, which council they belong to, and which portal we'd query. The
    actual scraper will be wired in once we choose between PlanningAlerts API
    vs per-council Infor ePathway scraping.
    """
    from listo.councils import council_for_postcode

    sql = text(
        """
        SELECT p.match_key,
               MAX(p.suburb)                                      AS suburb,
               MAX(p.postcode)                                    AS postcode,
               MIN(CONCAT(p.street_number, ' ', p.street_name))   AS street,
               (SELECT COUNT(*) FROM dev_applications da
                WHERE da.match_key = p.match_key)                 AS da_rows
        FROM properties p
        GROUP BY p.match_key
        HAVING SUM(CASE WHEN p.unit_number = ''  THEN 1 ELSE 0 END) >= 1
           AND SUM(CASE WHEN p.unit_number <> '' THEN 1 ELSE 0 END) >= 1
        ORDER BY p.match_key
        LIMIT 200
        """
    )
    with session_scope() as s:
        rows = s.execute(sql).fetchall()

    needing_enrichment = [r for r in rows if r.da_rows == 0]
    have_data = [r for r in rows if r.da_rows > 0]

    typer.echo(f"\n{len(rows)} duplex candidates total — {len(have_data)} already have DA data, {len(needing_enrichment)} need enrichment\n")

    by_council: dict[str, list] = {}
    no_council: list = []
    for r in needing_enrichment:
        c = council_for_postcode(r.postcode)
        if c is None:
            no_council.append(r)
        else:
            by_council.setdefault(c.slug, []).append(r)

    for slug, rs in sorted(by_council.items()):
        from listo.councils import councils as _all
        portal = _all()[slug].da_portal
        typer.echo(f"  {slug}: {len(rs)} addresses — {portal.system} ({portal.url[:60]}...)")
        for r in rs[:5]:
            typer.echo(f"      {r.suburb}, {r.postcode} — {r.street}")
        if len(rs) > 5:
            typer.echo(f"      ... and {len(rs) - 5} more")

    if no_council:
        typer.echo(f"\n  no-council-config (skipped): {len(no_council)} addresses")
        for r in no_council[:5]:
            typer.echo(f"      {r.suburb}, {r.postcode} — {r.street}")
        if len(no_council) > 5:
            typer.echo(f"      ... and {len(no_council) - 5} more — add their council to data/council_portals.json to include")

    if dry_run:
        typer.echo("\n(dry-run — no HTTP calls. pass --no-dry-run to actually ingest.)")
        return

    # Real enrichment.
    from listo import da_ingest
    typer.echo("\nstarting DA enrichment (this drives a real browser; progress logs to stderr)...")
    stats = da_ingest.enrich_candidates(limit=None, download_documents=True)
    typer.echo(
        f"\nenrichment complete: candidates={stats.candidates_processed}, "
        f"applications inserted={stats.applications_inserted}, "
        f"docs downloaded={stats.documents_downloaded}, "
        f"docs skipped (already stored)={stats.documents_skipped_existing}, "
        f"errors={stats.errors}"
    )


if __name__ == "__main__":
    app()
