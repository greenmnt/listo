"""Council DA portal config + DA description parsers.

Loads data/council_portals.json and exposes typed accessors. Provides a
unit-count extractor that handles the keyword patterns we've seen in MCU
descriptions (DUAL OCCUPANCY, TRIPLEX, '3 units', etc.).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

_DATA_FILE = Path(__file__).resolve().parents[2] / "data" / "council_portals.json"


@dataclass(frozen=True)
class DaPortal:
    url: str
    system: str
    module_code: str | None
    decision_notice_pdfs: bool
    notes: str


@dataclass(frozen=True)
class Council:
    slug: str
    name: str
    state: str
    covers_postcodes: tuple[str, ...]
    da_portal: DaPortal
    planning_alerts_authority: str | None
    open_data_portal: str | None


@lru_cache(maxsize=1)
def _raw() -> dict:
    return json.loads(_DATA_FILE.read_text())


@lru_cache(maxsize=1)
def councils() -> dict[str, Council]:
    """Return {slug: Council}."""
    out: dict[str, Council] = {}
    for c in _raw()["councils"]:
        portal = c["da_portal"]
        out[c["slug"]] = Council(
            slug=c["slug"],
            name=c["name"],
            state=c["state"],
            covers_postcodes=tuple(c.get("covers_postcodes", [])),
            da_portal=DaPortal(
                url=portal["url"],
                system=portal["system"],
                module_code=portal.get("module_code"),
                decision_notice_pdfs=portal.get("decision_notice_pdfs", False),
                notes=portal.get("notes", ""),
            ),
            planning_alerts_authority=c.get("planning_alerts_authority"),
            open_data_portal=c.get("open_data_portal"),
        )
    return out


def council_for_postcode(postcode: str) -> Council | None:
    for c in councils().values():
        if postcode in c.covers_postcodes:
            return c
    return None


# ---------------- DA description parsing -----------------

# Word-number map for descriptions like "DUAL OCCUPANCY", "TRIPLEX", etc.
_WORD_TO_NUM = {
    "dual": 2, "duplex": 2,
    "triple": 3, "triplex": 3,
    "fourplex": 4, "quadruplex": 4,
}


# Patterns ordered most-specific first.
_UNIT_COUNT_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(\d+)\s*x?\s*(?:unit|dwelling|townhouse|apartment|lot)s?\b", re.I), "explicit_n"),
    (re.compile(r"\b(dual|triple|fourplex|quadruplex)\b", re.I), "word"),
    (re.compile(r"\b(duplex|triplex)\b", re.I), "word"),
    (re.compile(r"\b(?:multi(?:ple)?)[\s-]+(?:unit|dwelling|residential)\b", re.I), "multi_unknown"),
]


def extract_approved_units(description: str | None) -> int | None:
    """Return the unit count parsed from a DA description, or None if unknown.

    Examples:
      "MATERIAL CHANGE OF USE CODE MCU201700973 PN62487/01/DA2 DUAL OCCUPANCY" -> 2
      "Multi-unit residential (3 units)"                                       -> 3
      "Triplex development on Lot 21"                                          -> 3
    """
    if not description:
        return None
    for pat, kind in _UNIT_COUNT_PATTERNS:
        m = pat.search(description)
        if not m:
            continue
        if kind == "explicit_n":
            try:
                n = int(m.group(1))
            except (ValueError, IndexError):
                continue
            if 2 <= n <= 50:
                return n
        elif kind == "word":
            word = m.group(1).lower()
            if word in _WORD_TO_NUM:
                return _WORD_TO_NUM[word]
        elif kind == "multi_unknown":
            # Description says "multi-unit" but doesn't give a count. Caller can
            # treat None as "definitely multi, count unknown" by also checking
            # the description text directly.
            return None
    return None


# ---------------- internal property ID extraction -----------------

# Council 'PN<digits>' references. Same PN ties together every DA on a single lot.
_PN_PATTERN = re.compile(r"\bPN\s*(\d{4,7})\b", re.I)


def extract_internal_property_id(description: str | None) -> str | None:
    """Pull a council internal property id (e.g. 'PN62487') out of a description."""
    if not description:
        return None
    m = _PN_PATTERN.search(description)
    if not m:
        return None
    return f"PN{m.group(1)}"


# ---------------- type code -----------------

_TYPE_CODE_PATTERN = re.compile(r"^([A-Z]{2,4})/", re.I)


def extract_type_code(application_id: str | None) -> str | None:
    """Pull 'MCU' from 'MCU/2017/973', etc."""
    if not application_id:
        return None
    m = _TYPE_CODE_PATTERN.match(application_id)
    return m.group(1).upper() if m else None
