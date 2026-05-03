"""End-to-end property-history orchestration: discover → fetch → parse → persist.

Given an address, runs the full pipeline:

1. **Domain PDP** for the parent address (plain HTTP).
2. **Google search** for `site:realestate.com.au "<address>"` and
   `site:domain.com.au "<address>"`. Caches discovered URLs.
3. **REA PDP** for the parent (and any unit-prefixed children Google
   surfaced — '1/124', '2/124', etc.).
4. **Domain PDPs** for each unit-prefixed child Google surfaced.
5. **Listing-detail pages** for every `/sold/...` and Domain
   `/{slug}-{listingId}` URL Google found.

All raw HTML lands in `raw_pages` (gzipped). All parsed records land in
the per-source tables.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field

from listo.property_history import domain as domain_pdp
from listo.property_history import listings as listings_mod
from listo.property_history import realestate as rea_pdp
from listo.property_history import search as search_mod
from listo.property_history.search import mark_fetched


logger = logging.getLogger(__name__)


@dataclass
class RunCounters:
    domain_pdps: int = 0
    rea_pdps: int = 0
    domain_listings: int = 0
    rea_listings: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class PropertyHistoryRun:
    address: str
    discovery: search_mod.DiscoveryResult
    counters: RunCounters


def _split_address(address: str) -> tuple[str, str, str, str]:
    """Return (street, suburb, state, postcode) for a freeform address.

    The street is the quoted phrase we use when we have to fall back
    to a site:-scoped search. State + postcode get added to the
    initial generic Google query so the address is unambiguous on
    the first page of results.

    We expand any street-type abbreviation to the long form because
    Google indexes Domain's slug (long form) AND REA's slug (short
    form) cross-referenced — 'Parade' typically returns more matches
    than 'Pde'.
    """
    from listo.address import canonical_long_form

    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) < 2:
        return address.strip(), "", "", ""
    street = parts[0]
    tail = " ".join(parts[1:])
    m = re.search(r"(.+?)\s+([A-Z]{2,3})\s+(\d{4})$", tail)
    if m:
        suburb = m.group(1).strip()
        state = m.group(2).strip()
        postcode = m.group(3).strip()
    else:
        suburb = tail.strip()
        state = ""
        postcode = ""

    tokens = street.split()
    if tokens:
        tokens[-1] = canonical_long_form(tokens[-1])
    return " ".join(tokens), suburb, state, postcode


_PDP_SLUG_RE = re.compile(
    r"/(?:property|property-profile)/([a-z0-9][a-z0-9-]*)/?"
)
# REA sold listing: realestate.com.au/sold/property-<type>-<state>-<suburb>-<listing_id>
_REA_SOLD_PATH_RE = re.compile(
    r"^https?://www\.realestate\.com\.au/sold/([a-z0-9+-]+?)-\d{6,}\b", re.IGNORECASE,
)
# Domain listing: domain.com.au/<address-slug>-<listing_id>
_DOMAIN_LISTING_PATH_RE = re.compile(
    r"^https?://www\.domain\.com\.au/(?!property-profile/)([a-z0-9-]+?)-\d{6,}\b",
    re.IGNORECASE,
)


def _slug_matches_parent(url: str, search_address: str, suburb_hint: str | None) -> bool:
    """Return True if the discovered URL refers to the same street # /
    suburb as the parent DA. Filters out neighbours that Google
    surfaces alongside the target.

    Three URL shapes the discovery returns:
      - PDP:           /property/{slug} or /property-profile/{slug}
                       — slug starts with '<num>-' (or unit prefix)
      - Domain listing /{address-slug}-{listing_id}
                       — slug starts with '<num>-'
      - REA sold:      /sold/property-<type>-<state>-<suburb>-{listing_id}
                       — has suburb but NO street number, so we can
                         only match on suburb

    Permissive on parse failure: when the slug shape is unrecognised
    or the parent street # can't be extracted, we let the URL
    through so the orchestrator still gets to attempt the fetch.
    """
    m_num = re.match(r"^\s*(\d+)\b", search_address)
    parent_num = m_num.group(1) if m_num else None

    suburb_token = (
        suburb_hint.lower().replace(" ", "-") if suburb_hint else None
    )

    # 1) PDP / property-profile shape — match on street #
    m_slug = _PDP_SLUG_RE.search(url)
    if m_slug:
        slug = m_slug.group(1).lower()
        return _check_addr_slug(slug, parent_num, suburb_token)

    # 2) Domain listing shape — same street # match
    m_dom = _DOMAIN_LISTING_PATH_RE.match(url)
    if m_dom:
        return _check_addr_slug(m_dom.group(1).lower(), parent_num, suburb_token)

    # 3) REA sold shape — only suburb is in the URL; street # isn't
    m_sold = _REA_SOLD_PATH_RE.match(url)
    if m_sold:
        if not suburb_token:
            return True
        return suburb_token in m_sold.group(1).lower()

    return True  # unknown shape; don't filter aggressively


def _check_addr_slug(slug: str, parent_num: str | None, suburb_token: str | None) -> bool:
    """Shared check for PDP-style and Domain-listing slugs that begin
    with the street number (with optional unit prefix)."""
    if parent_num is None:
        return True
    body = slug
    body = re.sub(r"^unit-\d+[a-z]?-", "", body)        # REA: unit-2-25-...
    body = re.sub(r"^\d+[a-z]?-(?=\d+-)", "", body)     # Domain: 1-7-...
    m_body = re.match(r"^(\d+)[a-z]?-", body)
    if not m_body or m_body.group(1) != parent_num:
        return False
    if suburb_token and suburb_token not in body:
        return False
    return True


def run(address: str, *, fetch_listings: bool = True, throttle: float = 2.0) -> PropertyHistoryRun:
    """Execute the full pipeline for one address.

    `fetch_listings=False` skips listing-page fetches (useful when you
    only want PDP + sales-history). `throttle` is the inter-fetch delay
    on REA paths to be polite to Kasada.
    """
    counters = RunCounters()

    # -------- 1. Domain PDP for the parent address --------
    logger.info("=== Domain PDP: %s ===", address)
    try:
        d = domain_pdp.fetch_by_address(address)
        if d.error:
            counters.errors.append(f"domain_pdp parent: {d.error}")
        else:
            counters.domain_pdps += 1
    except Exception as exc:  # noqa: BLE001
        counters.errors.append(f"domain_pdp parent: {exc!r}")

    # -------- 2. Google discovery --------
    search_address, suburb_hint, state, postcode = _split_address(address)
    logger.info("=== Google discovery: %s ===", search_address)
    discovery = search_mod.discover_for_address(
        search_address,
        suburb_hint=suburb_hint or None,
        state=state or None,
        postcode=postcode or None,
    )
    logger.info(
        "  rea_pdps=%d  rea_sold=%d  domain_pdps=%d  domain_listings=%d (raw)",
        len(discovery.rea_pdp_urls),
        len(discovery.rea_sold_urls),
        len(discovery.domain_pdp_urls),
        len(discovery.domain_listing_urls),
    )
    # Google's results frequently mix the target property with its
    # neighbours (e.g. searching '7 Alec Ave' surfaces #4 and #11 too).
    # Without filtering, the orchestrator would fetch all of them and
    # tag the neighbour data under THIS DA's context, which is just
    # wrong. Keep only URLs whose slug matches the parent street_number
    # (or has a unit prefix to that same number — captures post-redev
    # children like 1/7 and 2/7 for duplex detection).
    rea_pdp_urls = [u for u in discovery.rea_pdp_urls
                    if _slug_matches_parent(u, search_address, suburb_hint)]
    rea_sold_urls = [u for u in discovery.rea_sold_urls
                     if _slug_matches_parent(u, search_address, suburb_hint)]
    domain_pdp_urls = [u for u in discovery.domain_pdp_urls
                       if _slug_matches_parent(u, search_address, suburb_hint)]
    domain_listing_urls = [u for u in discovery.domain_listing_urls
                           if _slug_matches_parent(u, search_address, suburb_hint)]
    n_dropped = (
        (len(discovery.rea_pdp_urls) - len(rea_pdp_urls))
        + (len(discovery.rea_sold_urls) - len(rea_sold_urls))
        + (len(discovery.domain_pdp_urls) - len(domain_pdp_urls))
        + (len(discovery.domain_listing_urls) - len(domain_listing_urls))
    )
    if n_dropped:
        logger.info(
            "  dropped %d url(s) whose slug didn't match parent street # ("
            "rea_pdps=%d rea_sold=%d domain_pdps=%d domain_listings=%d after filter)",
            n_dropped,
            len(rea_pdp_urls), len(rea_sold_urls),
            len(domain_pdp_urls), len(domain_listing_urls),
        )

    # -------- 3. REA PDPs (parent + unit children) --------
    for url in rea_pdp_urls:
        logger.info("REA PDP fetch: %s", url)
        time.sleep(throttle)
        try:
            r = rea_pdp.fetch_and_persist(url)
            mark_fetched(url)
            if r.error:
                counters.errors.append(f"rea_pdp {url}: {r.error}")
            else:
                counters.rea_pdps += 1
        except Exception as exc:  # noqa: BLE001
            counters.errors.append(f"rea_pdp {url}: {exc!r}")

    # -------- 4. Domain PDPs for unit-prefix children Google found --------
    # The parent Domain PDP was already fetched in step 1; this loop covers
    # the unit-prefixed children Google surfaced ('1/124', '2/124', etc.).
    # store_raw_page dedups on (url_hash, content_hash) so re-fetching the
    # parent here is harmless.
    for url in domain_pdp_urls:
        logger.info("Domain PDP fetch: %s", url)
        try:
            r = domain_pdp.fetch_and_persist(url)
            mark_fetched(url)
            if r.error:
                counters.errors.append(f"domain_pdp {url}: {r.error}")
            else:
                counters.domain_pdps += 1
        except Exception as exc:  # noqa: BLE001
            counters.errors.append(f"domain_pdp {url}: {exc!r}")

    # -------- 5. Listing-detail pages --------
    if fetch_listings:
        for url in rea_sold_urls:
            logger.info("REA listing fetch: %s", url)
            time.sleep(throttle)
            try:
                _, listing_id, parsed, err = listings_mod.fetch_rea_listing(url)
                mark_fetched(url)
                if err:
                    counters.errors.append(f"rea_listing {url}: {err}")
                elif listing_id:
                    counters.rea_listings += 1
            except Exception as exc:  # noqa: BLE001
                counters.errors.append(f"rea_listing {url}: {exc!r}")

        for url in domain_listing_urls:
            logger.info("Domain listing fetch: %s", url)
            try:
                _, listing_id, parsed, err = listings_mod.fetch_domain_listing(url)
                mark_fetched(url)
                if err:
                    counters.errors.append(f"domain_listing {url}: {err}")
                elif listing_id:
                    counters.domain_listings += 1
            except Exception as exc:  # noqa: BLE001
                counters.errors.append(f"domain_listing {url}: {exc!r}")

    return PropertyHistoryRun(
        address=address,
        discovery=discovery,
        counters=counters,
    )
