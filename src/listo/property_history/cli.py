"""CLI subcommands for property-history scraping. Wired into the main
CLI as `listo property ...`."""
from __future__ import annotations

import logging

import typer
from sqlalchemy import select

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
