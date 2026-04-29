"""Description / id / type parsing helpers reused across vendors.

Same logic as the previous top-level councils.py — pulled into the
councils package so vendor scrapers can import without circular deps.
"""
from __future__ import annotations

import re


_WORD_TO_NUM = {
    "dual": 2, "duplex": 2,
    "triple": 3, "triplex": 3,
    "fourplex": 4, "quadruplex": 4,
}


_UNIT_COUNT_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(\d+)\s*x?\s*(?:unit|dwelling|townhouse|apartment|lot)s?\b", re.I), "explicit_n"),
    (re.compile(r"\b(dual|triple|fourplex|quadruplex)\b", re.I), "word"),
    (re.compile(r"\b(duplex|triplex)\b", re.I), "word"),
    (re.compile(r"\b(?:multi(?:ple)?)[\s-]+(?:unit|dwelling|residential)\b", re.I), "multi_unknown"),
]


def extract_approved_units(description: str | None) -> int | None:
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
            return None
    return None


_PN_PATTERN = re.compile(r"\bPN\s*(\d{4,7})\b", re.I)


def extract_internal_property_id(description: str | None) -> str | None:
    if not description:
        return None
    m = _PN_PATTERN.search(description)
    return f"PN{m.group(1)}" if m else None


_TYPE_CODE_PATTERN = re.compile(r"^([A-Z]{2,4})/", re.I)


def extract_type_code(application_id: str | None) -> str | None:
    if not application_id:
        return None
    m = _TYPE_CODE_PATTERN.match(application_id)
    return m.group(1).upper() if m else None


_DATE_AU = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def parse_au_date(s: str | None):
    from datetime import date
    if not s:
        return None
    m = _DATE_AU.match(s.strip())
    if not m:
        return None
    d, mth, y = (int(g) for g in m.groups())
    try:
        return date(y, mth, d)
    except ValueError:
        return None


_SIZE_RE = re.compile(r"^([\d.]+)\s*([KMG]?b)$", re.I)


def parse_size_to_bytes(s: str | None) -> int | None:
    if not s:
        return None
    m = _SIZE_RE.match(s.strip())
    if not m:
        return None
    n = float(m.group(1))
    unit = m.group(2).lower()
    return int(n * {"b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3}.get(unit, 1))


def safe_filename(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")
    return s[:120] or "file"


def count_pdf_pages(path) -> int | None:
    try:
        from pypdf import PdfReader
    except ImportError:
        return None
    try:
        return len(PdfReader(str(path)).pages)
    except Exception:
        return None


# ---------------- address splitting ----------------

# Pull the "<number> <street name>" portion out of a council-style raw
# address ("Lot 61 RP172633, 22 Viscount Drive, TALLAI QLD 4213").
# Returns (street_address, suburb, postcode, state) or all-None on failure.
_ADDR_RE = re.compile(
    r",\s*(?P<street>[^,]+?),\s*(?P<suburb>[A-Z][A-Z \-']+)\s+(?P<state>QLD|NSW|VIC|TAS|WA|SA|NT|ACT)\s+(?P<pc>\d{4})\s*$"
)


def split_council_address(raw_address: str | None) -> tuple[str | None, str | None, str | None, str | None]:
    if not raw_address:
        return (None, None, None, None)
    m = _ADDR_RE.search(raw_address)
    if not m:
        return (None, None, None, None)
    return (
        m.group("street").strip(),
        m.group("suburb").strip().title(),
        m.group("pc"),
        m.group("state"),
    )
