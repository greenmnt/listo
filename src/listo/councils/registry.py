"""Council registry: slug → list of (scraper backend, date coverage).

Some councils have one backend; others split history across two
(Newcastle: eTrack pre-Jan 2026, T1Cloud post-Jan 2026). The orchestrator
walks each backend in turn, restricting the scrape to the date window
each one covers.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from listo.councils.base import CouncilScraper


@dataclass(frozen=True)
class CouncilBackend:
    """One scraper instance scoped to a date window."""
    name: str                                # 'cogc_post_2017' / 'newcastle_etrack' …
    factory: callable                        # zero-arg → context-manager scraper
    covers_from: date | None = None
    covers_to: date | None = None

    def covers(self, *, date_from: date, date_to: date) -> bool:
        """Does this backend overlap with the requested window?"""
        if self.covers_to and date_from > self.covers_to:
            return False
        if self.covers_from and date_to < self.covers_from:
            return False
        return True

    def clamp(self, *, date_from: date, date_to: date) -> tuple[date, date]:
        """Clamp the requested window to this backend's coverage."""
        lo = max(date_from, self.covers_from) if self.covers_from else date_from
        hi = min(date_to, self.covers_to) if self.covers_to else date_to
        return lo, hi


@dataclass(frozen=True)
class CouncilDef:
    slug: str
    name: str
    state: str
    backends: tuple[CouncilBackend, ...]


# ---- factories ----

def _cogc_post_2017():
    from listo.councils.infor_epathway import COGC_CONFIG, InforEpathwayScraper
    cfg = type(COGC_CONFIG)(
        council_slug=COGC_CONFIG.council_slug,
        lists_url=COGC_CONFIG.lists_url,
        enquiry_lists=["Development applications after July 2017"],
        docs_portal_id_url=COGC_CONFIG.docs_portal_id_url,
        docs_portal_oid_url=COGC_CONFIG.docs_portal_oid_url,
    )
    return InforEpathwayScraper(cfg)


def _cogc_pre_2017():
    from listo.councils.infor_epathway import COGC_CONFIG, InforEpathwayScraper
    cfg = type(COGC_CONFIG)(
        council_slug=COGC_CONFIG.council_slug,
        lists_url=COGC_CONFIG.lists_url,
        enquiry_lists=["Development applications before July 2017"],
        docs_portal_id_url=COGC_CONFIG.docs_portal_id_url,
        docs_portal_oid_url=COGC_CONFIG.docs_portal_oid_url,
    )
    return InforEpathwayScraper(cfg)


def _newcastle_etrack():
    from listo.councils.techone_etrack import NEWCASTLE_CONFIG, TechOneEtrackScraper
    return TechOneEtrackScraper(NEWCASTLE_CONFIG)


def _newcastle_t1cloud():
    from listo.councils.techone_t1cloud import NEWCASTLE_CONFIG, TechOneT1CloudScraper
    return TechOneT1CloudScraper(NEWCASTLE_CONFIG)


COUNCILS: dict[str, CouncilDef] = {
    "cogc": CouncilDef(
        slug="cogc",
        name="City of Gold Coast",
        state="QLD",
        backends=(
            CouncilBackend(
                name="cogc_post_2017",
                factory=_cogc_post_2017,
                covers_from=date(2017, 7, 1),
            ),
            CouncilBackend(
                name="cogc_pre_2017",
                factory=_cogc_pre_2017,
                covers_to=date(2017, 6, 30),
            ),
        ),
    ),
    "newcastle": CouncilDef(
        slug="newcastle",
        name="City of Newcastle",
        state="NSW",
        backends=(
            CouncilBackend(
                name="newcastle_etrack",
                factory=_newcastle_etrack,
                covers_to=date(2026, 1, 31),
            ),
            CouncilBackend(
                name="newcastle_t1cloud",
                factory=_newcastle_t1cloud,
                covers_from=date(2026, 2, 1),
            ),
        ),
    ),
}


def get_council(slug: str) -> CouncilDef:
    if slug not in COUNCILS:
        raise KeyError(f"unknown council slug: {slug}. registered: {sorted(COUNCILS)}")
    return COUNCILS[slug]
