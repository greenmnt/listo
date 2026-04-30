"""Phase X — Google-search unit-prefixed addresses to detect built duplexes.

Background: an approved duplex/triplex DA might never go on the market for
sale (units are kept and rented, or the developer holds them). In that case
we have no `unit_sales` row on file, so the front end can't tell apart
'built but unsold' from 'never built / abandoned'.

This phase plugs the gap. For each approved DA without post-decision sales,
we issue Google searches for `<unit>/<street_number> <street_name>` (e.g.
'1/8 Sandy Court'). Any classified realestate.com.au or domain.com.au URL
proves the unit was listed at some point — so the duplex was built. Hits
land in `discovered_urls` keyed by the unit-prefixed search_address; the
API SQL reads this table to derive `built_status`.

Cheap and contained: it reuses `discover_for_address()` (already throttled,
already classifies URLs, already caches). One query per unit per source
(Domain + REA), so a duplex costs 4 Google calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

from sqlalchemy import text as sql_text

from listo.db import session_scope
from listo.property_history.orchestrator import _split_address
from listo.property_history import search as search_mod


logger = logging.getLogger(__name__)


@dataclass
class BuiltCheckStats:
    apps_visited: int = 0
    queries_run: int = 0
    units_with_evidence: int = 0
    apps_skipped_no_address: int = 0


def _select_candidates(
    *,
    council_slug: str | None,
    app_id_str: str | None,
    min_age_months: int,
    redo: bool,
) -> list[tuple[int, str, str, int]]:
    """Approved DAs without post-decision sales that we should Google-check.

    Returns rows of (app_pk, application_id, street_address, dwelling_count).

    Skips apps where:
      - decision_outcome doesn't include 'approv' (only check approved DAs)
      - decision_date too recent (< min_age_months) — the project may still
        be under construction; Google won't have it indexed yet
      - dwelling_count is missing or <= 1 (nothing to unit-prefix-search for)
      - we already have evidence cached for at least one unit
        (unless --redo)
    """
    cutoff = date.today() - timedelta(days=min_age_months * 30)
    conditions = [
        "ds.application_id = ca.id",
        "LOWER(ca.decision_outcome) LIKE '%approv%'",
        "ca.decision_date IS NOT NULL",
        "ca.decision_date <= :cutoff",
        "ds.dwelling_count > 1",
        "ds.street_address IS NOT NULL",
        "ds.street_address <> ''",
    ]
    params: dict[str, object] = {"cutoff": cutoff}
    if council_slug:
        conditions.append("ca.council_slug = :slug")
        params["slug"] = council_slug
    if app_id_str:
        conditions.append("ca.application_id = :appid")
        params["appid"] = app_id_str

    where = " AND ".join(conditions)
    sql = sql_text(f"""
        SELECT ca.id, ca.application_id, ds.street_address, ds.dwelling_count
          FROM da_summaries ds
          JOIN council_applications ca ON ca.id = ds.application_id
         WHERE {where}
         ORDER BY ca.decision_date ASC
    """)
    with session_scope() as s:
        rows = s.execute(sql, params).fetchall()

    out: list[tuple[int, str, str, int]] = []
    for r in rows:
        if not redo and _has_unit_evidence_cached(r.street_address, r.dwelling_count):
            continue
        out.append((r.id, r.application_id, r.street_address, r.dwelling_count))
    return out


def _has_unit_evidence_cached(street_address: str, dwelling_count: int) -> bool:
    """True if we've already searched any unit-prefixed form of this address.

    We just need to know we've TRIED — even a zero-result run still creates
    a search-query history we shouldn't repeat. Cache lookup is by prefix
    (`'1/<street>%'`) so it tolerates the suburb / postcode tail being
    canonicalised differently from search to search.
    """
    base = _street_only(street_address)
    if not base:
        return False
    sql = sql_text("""
        SELECT 1 FROM discovered_urls
         WHERE search_address LIKE :pat
         LIMIT 1
    """)
    with session_scope() as s:
        for unit in range(1, dwelling_count + 1):
            pat = f"{unit}/{base}%"
            row = s.execute(sql, {"pat": pat}).first()
            if row is not None:
                return True
    return False


def _street_only(street_address: str) -> str:
    """'15 Matasha Crescent, Pimpama QLD 4209' → '15 Matasha Crescent'."""
    return street_address.split(",", 1)[0].strip()


def run(
    *,
    council_slug: str | None = None,
    app_id_str: str | None = None,
    min_age_months: int = 6,
    redo: bool = False,
) -> BuiltCheckStats:
    """Run the unit-evidence check across one or many DAs."""
    stats = BuiltCheckStats()
    candidates = _select_candidates(
        council_slug=council_slug, app_id_str=app_id_str,
        min_age_months=min_age_months, redo=redo,
    )
    logger.info(
        "built-check: %d candidates (decision >= %d months old, no post sales)",
        len(candidates), min_age_months,
    )

    for app_pk, app_id, street_address, dwelling_count in candidates:
        stats.apps_visited += 1
        street = _street_only(street_address)
        if not street or not street[0].isdigit():
            logger.warning("  [%s] no usable street → skipping", app_id)
            stats.apps_skipped_no_address += 1
            continue
        # Use property_history's _split_address on the parent so we get
        # the canonical 'street name (long form)' + 'suburb hint' that
        # discover_for_address already understands. We then prepend the
        # unit prefix.
        parent_search, suburb_hint = _split_address(street_address)
        for unit in range(1, dwelling_count + 1):
            unit_address = f"{unit}/{parent_search}"
            logger.info("  [%s] searching: %s", app_id, unit_address)
            try:
                disc = search_mod.discover_for_address(
                    unit_address, suburb_hint=suburb_hint or None,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("  [%s] search failed for %s: %s", app_id, unit_address, exc)
                continue
            stats.queries_run += len(disc.queries)
            n_rea_dom = (
                len(disc.rea_pdp_urls) + len(disc.rea_sold_urls)
                + len(disc.domain_pdp_urls) + len(disc.domain_listing_urls)
            )

            # Third pass — unscoped quoted-phrase search. Catches agency
            # sites (rwbgrentals.com etc.) that index leased / sold / managed
            # listings outside REA + Domain, plus body-corp / valuer / utility
            # registers that mention the unit address. Any hit = built.
            general_q = f'"{unit_address}"' + (f" {suburb_hint}" if suburb_hint else "")
            try:
                gen = search_mod.google_search_general(general_q)
            except Exception as exc:  # noqa: BLE001
                logger.warning("  [%s] general search failed for %s: %s", app_id, unit_address, exc)
                gen = None
            n_general = 0
            if gen is not None:
                stats.queries_run += 1
                n_general = len(gen.urls)
                if n_general > 0:
                    search_mod.cache_urls_general(
                        search_address=unit_address, query=general_q, urls=gen.urls,
                    )

            total_hits = n_rea_dom + n_general
            if total_hits > 0:
                stats.units_with_evidence += 1
                logger.info(
                    "  [%s]   ↳ %s evidence: rea=%d/%d domain=%d/%d general=%d",
                    app_id, unit_address,
                    len(disc.rea_pdp_urls), len(disc.rea_sold_urls),
                    len(disc.domain_pdp_urls), len(disc.domain_listing_urls),
                    n_general,
                )
            else:
                logger.info("  [%s]   ↳ %s no hits", app_id, unit_address)

    return stats
