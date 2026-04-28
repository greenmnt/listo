from __future__ import annotations

import re
from dataclasses import dataclass

# Map full street-type word to its standard Australian abbreviation.
# Both the long form and the abbreviation normalize to the abbreviation.
_SUFFIX_MAP: dict[str, str] = {
    "avenue": "ave", "ave": "ave", "av": "ave",
    "street": "st", "st": "st",
    "road": "rd", "rd": "rd",
    "drive": "dr", "dr": "dr", "drv": "dr",
    "court": "ct", "ct": "ct", "crt": "ct",
    "parade": "pde", "pde": "pde", "pd": "pde",
    "terrace": "tce", "tce": "tce", "ter": "tce",
    "crescent": "cres", "cres": "cres", "cr": "cres",
    "boulevard": "bvd", "boulevarde": "bvd", "bvd": "bvd", "blvd": "bvd",
    "highway": "hwy", "hwy": "hwy",
    "place": "pl", "pl": "pl",
    "lane": "ln", "ln": "ln",
    "close": "cl", "cl": "cl",
    "way": "way", "wy": "way",
    "circuit": "cct", "cct": "cct", "cir": "cct",
    "esplanade": "esp", "esp": "esp",
    "promenade": "prom", "prom": "prom",
    "grove": "gr", "gr": "gr",
    "rise": "rise",
    "row": "row",
    "view": "view",
    "square": "sq", "sq": "sq",
    "loop": "loop",
}

_PUNCT = re.compile(r"[^\w\s/]+")
_WS = re.compile(r"\s+")
# A street number with trailing alpha suffix: "11a", "18b", "11abc". In AU
# property practice this nearly always denotes a half-of-duplex subdivision
# of the original lot, equivalent to "1/11" / "2/11" notation. Treating the
# alpha as part of the unit indicator (rather than a different lot) lets the
# match_key align across both notations.
_ALPHA_SUFFIX = re.compile(r"^(\d+)([a-z]+)$")


@dataclass(frozen=True)
class NormalizedAddress:
    unit_number: str
    street_number: str
    street_name: str
    street_norm: str
    suburb: str
    suburb_norm: str
    postcode: str
    match_key: str


def _norm_token(s: str) -> str:
    return _PUNCT.sub("", s).strip().lower()


def _norm_suburb(suburb: str) -> str:
    s = _PUNCT.sub("", suburb).lower()
    return _WS.sub(" ", s).strip()


def _norm_street(street_name: str) -> str:
    cleaned = _PUNCT.sub("", street_name).lower()
    cleaned = _WS.sub(" ", cleaned).strip()
    if not cleaned:
        return ""
    parts = cleaned.split(" ")
    last = parts[-1]
    if last in _SUFFIX_MAP:
        parts[-1] = _SUFFIX_MAP[last]
    return " ".join(parts)


_UNIT_MAX = 16  # matches properties.unit_number VARCHAR(16)
_STREET_NUM_MAX = 16  # matches properties.street_number VARCHAR(16)


def normalize_address(raw_street: str, suburb: str, postcode: str) -> NormalizedAddress:
    """Parse a raw street string into normalized address components.

    The match_key is the same for "17 Third Avenue" and "2/17 Third Avenue"
    at the same suburb/postcode — that is the whole point.
    """
    raw = _WS.sub(" ", raw_street.strip())

    unit_number = ""
    street_part = raw
    if "/" in raw:
        left, _, right = raw.partition("/")
        unit_number = _norm_token(left)[:_UNIT_MAX]   # silently truncate weird unit prefixes ("LA SABBIA PENTHOUSE/...")
        street_part = right.strip()

    tokens = street_part.split(" ", 1)
    if len(tokens) < 2:
        raw_street_number = _norm_token(tokens[0]) if tokens else ""
        street_name = ""
    else:
        raw_street_number = _norm_token(tokens[0])
        street_name = tokens[1].strip()

    # If street_number ends in alpha (e.g., "11a"), strip it onto the unit
    # indicator. "11a" alone -> unit="a"; "1/11a" -> unit="1a". Both end up
    # with match_key based on "11" so they cluster with the original lot.
    m = _ALPHA_SUFFIX.match(raw_street_number)
    if m:
        base, alpha = m.group(1), m.group(2)
        street_number = base[:_STREET_NUM_MAX]
        unit_number = (unit_number + alpha)[:_UNIT_MAX] if unit_number else alpha[:_UNIT_MAX]
    else:
        street_number = raw_street_number[:_STREET_NUM_MAX]

    street_norm = _norm_street(street_name)
    suburb_norm = _norm_suburb(suburb)
    pc = postcode.strip()

    match_key = f"{street_number}|{street_norm}|{suburb_norm}|{pc}"

    # Title-case the street name for display, leaving abbreviations alone if all-lower.
    display_street = " ".join(w.capitalize() for w in street_name.split()) if street_name else ""
    display_suburb = " ".join(w.capitalize() for w in suburb.split())

    return NormalizedAddress(
        unit_number=unit_number,
        street_number=street_number,
        street_name=display_street,
        street_norm=street_norm,
        suburb=display_suburb,
        suburb_norm=suburb_norm,
        postcode=pc,
        match_key=match_key,
    )
