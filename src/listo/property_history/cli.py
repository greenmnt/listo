"""CLI subcommands for property-history scraping. Wired into the main
CLI as `listo property ...`."""
from __future__ import annotations

import logging
from dataclasses import dataclass

import typer
from sqlalchemy import select, text as sql_text

from listo.councils.parsing import split_council_address
from listo.db import session_scope
from listo.models import CouncilApplication
from listo.property_history import domain as domain_pdp
from listo.property_history import orchestrator
from listo.property_history import realestate as rea_pdp


property_app = typer.Typer(no_args_is_help=True, help="Property-history scraping (Domain + REA via Kasada-bypass Chrome)")

logger = logging.getLogger(__name__)


@property_app.command("fetch")
def fetch(
    address: str = typer.Option(None, "--address", "-a", help="freeform address, e.g. '124 Sunshine Pde, Miami QLD 4220'"),
    url: str = typer.Option(None, "--url", "-u", help="full PDP URL (Domain or REA)"),
    da: str = typer.Option(None, "--da", help="council application id, e.g. EDA/2021/97 — looks up its address"),
    council_slug: str = typer.Option("cogc", "--council", help="council slug for --da lookup"),
    sources: str = typer.Option("domain", "--sources", help="comma-separated source list: 'domain' / 'realestate' / 'all'"),
) -> None:
    """Fetch a property's PDP from the named sources (no discovery).

    Use `listo property history` for the full pipeline (discovery +
    listings).
    """
    enabled = {s.strip().lower() for s in sources.split(",") if s.strip()}
    if "all" in enabled:
        enabled = {"domain", "realestate"}
    unknown = enabled - {"domain", "realestate"}
    if unknown:
        raise typer.BadParameter(f"unknown sources {sorted(unknown)} — supported: 'domain', 'realestate', 'all'.")

    if sum(x is not None for x in (address, url, da)) != 1:
        raise typer.BadParameter("supply exactly one of --address / --url / --da")

    if da:
        address = _address_from_da(council_slug=council_slug, da_id=da)
        typer.echo(f"DA {da} → {address}")

    if "domain" in enabled:
        if url and "domain.com.au" in url:
            res = domain_pdp.fetch_and_persist(url)
        elif address:
            res = domain_pdp.fetch_by_address(address)
        else:
            res = None
        if res:
            typer.echo("\n[Domain]")
            _print_domain_result(res)

    if "realestate" in enabled:
        if url and "realestate.com.au" in url:
            r = rea_pdp.fetch_and_persist(url)
        elif address:
            r = rea_pdp.fetch_by_address(address)
        else:
            r = None
        if r:
            typer.echo("\n[Realestate]")
            _print_rea_result(r)


@property_app.command("history")
def history(
    address: str = typer.Option(None, "--address", "-a", help="freeform address"),
    da: str = typer.Option(None, "--da", help="council application id, e.g. EDA/2021/97"),
    council_slug: str = typer.Option("cogc", "--council"),
    skip_listings: bool = typer.Option(False, "--skip-listings", help="don't fetch sold-listing detail pages"),
) -> None:
    """Full pipeline: discover URLs via Google, fetch PDPs + listings, persist all."""
    if sum(x is not None for x in (address, da)) != 1:
        raise typer.BadParameter("supply exactly one of --address / --da")
    if da:
        address = _address_from_da(council_slug=council_slug, da_id=da)
        typer.echo(f"DA {da} → {address}")
    assert address is not None

    res = orchestrator.run(address, fetch_listings=not skip_listings)

    typer.echo("\n=== Discovery ===")
    typer.echo(f"  rea_pdps:        {len(res.discovery.rea_pdp_urls)}")
    for u in res.discovery.rea_pdp_urls: typer.echo(f"    · {u}")
    typer.echo(f"  rea_sold:        {len(res.discovery.rea_sold_urls)}")
    for u in res.discovery.rea_sold_urls: typer.echo(f"    · {u}")
    typer.echo(f"  domain_pdps:     {len(res.discovery.domain_pdp_urls)}")
    for u in res.discovery.domain_pdp_urls: typer.echo(f"    · {u}")
    typer.echo(f"  domain_listings: {len(res.discovery.domain_listing_urls)}")
    for u in res.discovery.domain_listing_urls: typer.echo(f"    · {u}")

    typer.echo("\n=== Fetched ===")
    typer.echo(f"  domain_pdps:      {res.counters.domain_pdps}")
    typer.echo(f"  rea_pdps:         {res.counters.rea_pdps}")
    typer.echo(f"  domain_listings:  {res.counters.domain_listings}")
    typer.echo(f"  rea_listings:     {res.counters.rea_listings}")

    if res.counters.errors:
        typer.echo(f"\n=== Errors ({len(res.counters.errors)}) ===")
        for e in res.counters.errors: typer.secho(f"  · {e}", fg=typer.colors.YELLOW)


# Description-regex filters per kind, mirroring api/src/classify.rs:79-95.
# Each entry is (positive_pattern, exclusion_pattern). The exclusion is
# applied as `description NOT REGEXP exclusion` so e.g. a 'triplex' DA
# isn't double-counted under both duplex and big_dev.
_KIND_REGEX: dict[str, tuple[str, str | None]] = {
    "duplex": (
        r"(?i)dual[[:space:]]+occupancy|duplex",
        r"(?i)triplex|fourplex|quadruplex|multi[[:space:]-]+unit|multi[[:space:]-]+dwelling",
    ),
    "big_dev": (
        r"(?i)triplex|fourplex|quadruplex|multi[[:space:]-]+unit|multi[[:space:]-]+dwelling|townhouse",
        None,
    ),
    "granny": (
        r"(?i)secondary[[:space:]]+dwelling|granny[[:space:]]+flat|auxiliary[[:space:]]+dwelling|ancillary[[:space:]]+dwelling",
        None,
    ),
    # 'redev' = duplex + big_dev (the multi-dwelling redev premise).
    "redev": (
        r"(?i)dual[[:space:]]+occupancy|duplex|triplex|fourplex|quadruplex|multi[[:space:]-]+unit|multi[[:space:]-]+dwelling|townhouse",
        None,
    ),
}


@dataclass
class _Target:
    app_pk: int
    application_id: str
    raw_address: str
    parsed_street: str
    parsed_suburb: str
    parsed_state: str
    parsed_postcode: str

    @property
    def search_address(self) -> str:
        return f"{self.parsed_street}, {self.parsed_suburb} {self.parsed_state} {self.parsed_postcode}"


def _select_targets(
    *,
    council_slug: str,
    type_codes: list[str],
    kind: str,
    approved_only: bool,
    date_from: str | None,
    date_to: str | None,
    skip_existing: bool,
) -> list[_Target]:
    """Pick DAs from council_applications and parse their addresses.

    `kind` selects the description-regex filter:
      duplex   — dual occupancy / duplex (excluding triplex+ keywords)
      big_dev  — triplex/fourplex/multi-unit/townhouse (or approved_units>=3)
      granny   — secondary / granny flat
      redev    — duplex + big_dev combined
      all      — no description filter

    Skips rows whose raw_address can't be parsed by split_council_address
    (those need manual cleanup; not worth blocking the batch on them).
    Optionally skips DAs whose street already has a domain/realestate
    property row (so reruns resume cheaply).
    """
    if kind != "all" and kind not in _KIND_REGEX:
        raise typer.BadParameter(
            f"--kind must be one of: duplex, big_dev, granny, redev, all (got {kind!r})"
        )

    types_in = ",".join(f"'{t.upper()}'" for t in type_codes)
    where_extra = []
    params: dict[str, object] = {"slug": council_slug}

    if kind == "all":
        kind_clause = "1=1"
    else:
        positive, exclusion = _KIND_REGEX[kind]
        params["kind_re"] = positive
        if kind == "big_dev":
            # Match the API: big_dev includes approved_units>=3 even when the
            # description doesn't have a redev keyword (subdivision approvals
            # often just say 'Material Change of Use 3 units').
            kind_clause = "(description REGEXP :kind_re OR approved_units >= 3)"
        else:
            kind_clause = "(description REGEXP :kind_re)"
        if exclusion is not None:
            params["kind_excl_re"] = exclusion
            kind_clause += " AND (description NOT REGEXP :kind_excl_re)"

    if approved_only:
        where_extra.append("LOWER(decision_outcome) LIKE '%approv%'")
    if date_from:
        where_extra.append("lodged_date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where_extra.append("lodged_date <= :date_to")
        params["date_to"] = date_to
    extra_sql = (" AND " + " AND ".join(where_extra)) if where_extra else ""
    sql = sql_text(f"""
        SELECT id, application_id, raw_address
          FROM council_applications
         WHERE council_slug = :slug
           AND type_code IN ({types_in})
           AND raw_address IS NOT NULL AND raw_address <> ''
           AND {kind_clause}
           {extra_sql}
         ORDER BY lodged_date DESC, id DESC
    """)

    out: list[_Target] = []
    with session_scope() as s:
        rows = s.execute(sql, params).fetchall()

    unparseable = 0
    for r in rows:
        street, suburb, postcode, state = split_council_address(r.raw_address)
        if not (street and suburb and postcode and state):
            unparseable += 1
            continue
        out.append(_Target(
            app_pk=int(r.id),
            application_id=r.application_id,
            raw_address=r.raw_address,
            parsed_street=street,
            parsed_suburb=suburb,
            parsed_state=state,
            parsed_postcode=postcode,
        ))
    if unparseable:
        logger.info("skipped %d rows where split_council_address couldn't parse raw_address", unparseable)

    if skip_existing and out:
        out = _drop_already_scraped(out)
    return out


def _drop_already_scraped(targets: list[_Target]) -> list[_Target]:
    """Remove targets whose address has BOTH Domain and REA rows already.

    Skips a candidate only when the same street+suburb prefix appears in
    both `domain_properties` AND `realestate_properties`. If only one
    source has it, we re-run so the missing source gets backfilled.

    Matching is by `display_address LIKE '<street>, <suburb>%'` (case-
    insensitive, prefix). Including the suburb avoids false skips when a
    street name repeats across suburbs (e.g. 'Smith Street'). The same
    prefix shape is used by the Rust API in
    api/src/service/applications.rs:56.

    Pulls all distinct display_addresses from both tables (small — a few
    hundred rows currently) and matches in Python; doing one query per
    candidate would be RTT-bound over the SSH tunnel.
    """
    if not targets:
        return targets
    with session_scope() as s:
        domain_addrs = [r[0] for r in s.execute(sql_text(
            "SELECT DISTINCT display_address FROM domain_properties"
        )).fetchall() if r[0]]
        rea_addrs = [r[0] for r in s.execute(sql_text(
            "SELECT DISTINCT display_address FROM realestate_properties"
        )).fetchall() if r[0]]

    domain_lc = [a.lower() for a in domain_addrs]
    rea_lc = [a.lower() for a in rea_addrs]

    kept: list[_Target] = []
    skipped_both = 0
    skipped_partial = 0  # informational: counted as kept (we'll re-run)
    for t in targets:
        prefix = f"{t.parsed_street}, {t.parsed_suburb}".lower()
        in_domain = any(a.startswith(prefix) for a in domain_lc)
        in_rea = any(a.startswith(prefix) for a in rea_lc)
        if in_domain and in_rea:
            skipped_both += 1
            continue
        if in_domain or in_rea:
            skipped_partial += 1
        kept.append(t)
    if skipped_both:
        logger.info(
            "skipped %d targets with BOTH domain+rea rows (use --include-existing to retry)",
            skipped_both,
        )
    if skipped_partial:
        logger.info(
            "kept %d targets where only one of domain/rea is on file (re-running to backfill)",
            skipped_partial,
        )
    return kept


@property_app.command("scrape-batch")
def scrape_batch(
    council_slug: str = typer.Option("cogc", "--council"),
    types: str = typer.Option("MCU", "--types", help="comma-separated council type codes (default MCU)"),
    kind: str = typer.Option("duplex", "--kind", help="duplex | big_dev | granny | redev | all (default: duplex only)"),
    approved_only: bool = typer.Option(True, "--approved/--all-outcomes", help="restrict to decision_outcome LIKE '%approv%'"),
    date_from: str = typer.Option(None, "--from", help="lodged_date >= YYYY-MM-DD"),
    date_to: str = typer.Option(None, "--to", help="lodged_date <= YYYY-MM-DD"),
    skip_existing: bool = typer.Option(True, "--skip-existing/--include-existing", help="skip streets already in domain_properties/realestate_properties"),
    limit: int = typer.Option(50, "--limit", help="cap candidates after filtering (0 = no cap)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="print targets without fetching"),
    skip_listings: bool = typer.Option(False, "--skip-listings", help="skip sold-listing detail fetches (faster, less complete)"),
) -> None:
    """Iterate council DAs of the chosen kind (default: duplex) and run the
    property-history orchestrator (Domain + REA + Google) for each.

    No LLM step. Uses raw_address parsed via split_council_address. Resume-safe
    by default — rerunning skips streets already in domain_properties /
    realestate_properties.
    """
    type_list = [t.strip().upper() for t in types.split(",") if t.strip()]
    if not type_list:
        raise typer.BadParameter("--types must be non-empty")

    typer.echo(f"selecting {kind} candidates ({','.join(type_list)}) from {council_slug}...")
    targets = _select_targets(
        council_slug=council_slug,
        type_codes=type_list,
        kind=kind,
        approved_only=approved_only,
        date_from=date_from,
        date_to=date_to,
        skip_existing=skip_existing,
    )
    typer.echo(f"  {len(targets)} candidates after filtering")

    if limit and len(targets) > limit:
        targets = targets[:limit]
        typer.echo(f"  capped to --limit {limit}")

    if dry_run:
        typer.echo("\nDRY RUN — would fetch:")
        for t in targets:
            typer.echo(f"  [{t.application_id}] {t.search_address}")
        return

    if not targets:
        typer.echo("nothing to do")
        return

    successes = 0
    failures = 0
    # Kasada circuit-breaker. When the warmed Chrome profile gets flagged,
    # REA stops returning real PDP data and every fetch comes back as
    # "no propertyProfile.property record found" — Domain keeps working
    # though, so the run looks healthy in counters but ~zero REA data
    # actually lands. We track DAs where Google found REA URLs but ZERO
    # parsed; if that happens KASADA_BREAKER_N times in a row, abort so
    # the user can re-seed the profile before burning more candidates.
    KASADA_BREAKER_N = 3
    rea_burnt_streak = 0
    for i, t in enumerate(targets, 1):
        typer.echo(f"\n[{i}/{len(targets)}] {t.application_id} → {t.search_address}")
        try:
            res = orchestrator.run(t.search_address, fetch_listings=not skip_listings)
        except Exception as exc:  # noqa: BLE001 — we want the loop to keep going
            typer.secho(f"  FAILED: {exc!r}", fg=typer.colors.RED)
            failures += 1
            continue
        successes += 1
        typer.echo(
            f"  domain_pdps={res.counters.domain_pdps} "
            f"rea_pdps={res.counters.rea_pdps} "
            f"domain_listings={res.counters.domain_listings} "
            f"rea_listings={res.counters.rea_listings}"
        )
        if res.counters.errors:
            for e in res.counters.errors:
                typer.secho(f"    · {e}", fg=typer.colors.YELLOW)

        # Kasada circuit-breaker — see comment above the loop. We define
        # "burnt" as: discovery surfaced REA URLs to fetch, but zero PDPs
        # got parsed AND every error mentions Kasada / propertyProfile.
        rea_urls_attempted = (
            len(res.discovery.rea_pdp_urls) + len(res.discovery.rea_sold_urls)
        )
        rea_kasada_signals = any(
            ("kasada" in e.lower() or "propertyProfile" in e or "CdpUnavailableError" in e)
            for e in res.counters.errors
        )
        burnt_now = (
            rea_urls_attempted > 0
            and res.counters.rea_pdps == 0
            and rea_kasada_signals
        )
        rea_burnt_streak = (rea_burnt_streak + 1) if burnt_now else 0
        if rea_burnt_streak >= KASADA_BREAKER_N:
            typer.secho(
                f"\nABORTING: {KASADA_BREAKER_N} consecutive DAs returned no REA PDPs "
                "despite finding URLs — Kasada has likely flagged the Chrome profile.",
                fg=typer.colors.RED, bold=True,
            )
            typer.echo(
                "Stop the run, re-seed the Chrome profile (delete "
                "~/.config/google-chrome-listo, relaunch Chrome with "
                "scripts/run-scrape.sh, visit realestate.com.au manually), "
                "then re-run scrape-batch — --skip-existing will only retry "
                "candidates with partial coverage."
            )
            raise typer.Exit(code=2)

    typer.echo(f"\nbatch done: {successes} ok, {failures} failed")


def _address_from_da(council_slug: str, da_id: str) -> str:
    """Convert a council DA into a comma-separated address string."""
    with session_scope() as s:
        app = s.execute(
            select(CouncilApplication).where(
                CouncilApplication.council_slug == council_slug,
                CouncilApplication.application_id == da_id,
            )
        ).scalar_one_or_none()
        if app is None:
            raise typer.BadParameter(f"no council_applications row for {council_slug}/{da_id}")

        # raw_address looks like 'Lot 376 RP21903, 124 Sunshine Parade, MIAMI  QLD  4220'.
        # Split off the leading lot/plan, keep "street, suburb STATE postcode".
        if not app.raw_address:
            raise typer.BadParameter(f"DA {da_id} has no raw_address")
        chunks = [c.strip() for c in app.raw_address.split(",")]
        if chunks and chunks[0].lower().startswith("lot "):
            chunks = chunks[1:]
        if not chunks:
            raise typer.BadParameter(f"can't extract street from {app.raw_address!r}")
        rejoined = ", ".join(chunks)
        # Collapse double spaces inside "MIAMI  QLD  4220".
        return " ".join(rejoined.split())


def _print_rea_result(res: rea_pdp.FetchReaPdpResult) -> None:
    typer.echo(f"  url:               {res.url}")
    typer.echo(f"  http:              {res.http_status}")
    typer.echo(f"  raw_pages.id:      {res.raw_page_id}")
    if res.error:
        typer.secho(f"  error:             {res.error}", fg=typer.colors.RED)
        return
    p = res.parsed
    assert p
    typer.echo(f"  realestate_property: {res.realestate_property_id} (rea id={p.rea_property_id})")
    typer.echo(f"  address:           {p.display_address}")
    typer.echo(
        f"  attrs:             {p.property_type or '?'} · "
        f"{p.bedrooms or '?'}br · {p.bathrooms or '?'}ba · "
        f"{p.car_spaces or '?'} car · {p.land_area_m2 or '?'}m²"
    )
    if p.valuation_mid:
        typer.echo(
            f"  estimate (REA):    ${p.valuation_low or 0:,}–${p.valuation_high or 0:,} "
            f"(mid ${p.valuation_mid:,}, {p.valuation_confidence})"
        )
    typer.echo(f"  pca link:          {p.pca_property_url or '—'}")
    typer.echo(f"  timeline events:   {len(p.timeline)}")
    for ev in p.timeline:
        typer.echo(f"    {ev.get('date','—'):<12}  {ev.get('eventType','?'):<8}  {str(ev.get('price') or '—'):>14}  {(ev.get('agency') or '').strip()}")


def _print_domain_result(res: domain_pdp.FetchPdpResult) -> None:
    typer.echo("")
    typer.echo(f"  url:               {res.url}")
    typer.echo(f"  http:              {res.http_status}")
    typer.echo(f"  raw_pages.id:      {res.raw_page_id}")
    if res.error:
        typer.secho(f"  error:             {res.error}", fg=typer.colors.RED)
        return

    p = res.parsed
    assert p
    typer.echo(f"  domain_property:   {res.domain_property_id} (propertyId={p.domain_property_id})")
    typer.echo(f"  address:           {p.display_address}")
    typer.echo(
        f"  attrs:             {p.property_type or '?'} · "
        f"{p.bedrooms or '?'}br · {p.bathrooms or '?'}ba · "
        f"{p.parking_spaces or '?'} car · "
        f"{p.land_area_m2 or '?'}m²"
    )
    if p.valuation_mid:
        typer.echo(
            f"  estimate (Domain): ${p.valuation_low:,}–${p.valuation_high:,} "
            f"(mid ${p.valuation_mid:,}, {p.valuation_confidence}, as of {p.valuation_date})"
        )
    typer.echo("")
    typer.echo(f"  timeline ({len(p.timeline)} events):")
    for ev in sorted(p.timeline, key=lambda e: e.get("eventDate", ""), reverse=True):
        date_s = (ev.get("eventDate") or "")[:10]
        cat = ev.get("category") or "?"
        price = ev.get("eventPrice")
        sold = (ev.get("saleMetadata") or {}).get("isSold")
        agent = (ev.get("agency") or {}).get("name") or ""
        desc = ev.get("priceDescription") or ""
        if isinstance(price, int) and price:
            price_s = f"${price:,}" if cat == "Sale" else f"${price}/wk"
        else:
            price_s = "—"
        marker = " ✅" if sold else (" 🚫" if cat == "Sale" and not sold else "")
        typer.echo(f"    {date_s}  {cat:<8}  {price_s:>11}  {desc:<14}  {agent}{marker}")
