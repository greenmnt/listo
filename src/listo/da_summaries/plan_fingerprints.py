"""Identity-token extractor for plan PDFs.

Many architects / draftspersons put their firm name only in the **logo**
on plans — text-extractable content has the URL, email, and licence
number but not the firm name. We capture those tokens as fingerprints
so they can later be resolved to a named entity by cross-referencing
other documents in the same DA (Supporting Documents' "Prepared by"
tables, Decision Reports' "Name – Role" lists, etc).

Each function returns `Fingerprint` records with span + bbox; the
harvester writes them to the `doc_fingerprints` table.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from listo.da_summaries.entity_evidence import PageLayout, layout_for_span


# ---------------------------------------------------------------- patterns


_URL_RE = re.compile(
    # WWW.BORISDESIGN.COM.AU / https://example.com.au / borisdesign.com.au
    r"\b(?:https?://)?(?:www\.)?"
    r"(?P<host>[a-z][a-z0-9\-]+(?:\.[a-z][a-z0-9\-]+)*\."
    r"(?:com\.au|net\.au|org\.au|com|net|org|design|architects?))\b"
    r"(?P<path>/[^\s]*)?",
    re.IGNORECASE,
)

_EMAIL_RE = re.compile(
    r"\b(?P<local>[a-z0-9._%+\-]+)@(?P<host>[a-z0-9.\-]+\.[a-z]{2,})\b",
    re.IGNORECASE,
)

# AU phone numbers (landline + mobile + 1300 / 1800)
_PHONE_RE = re.compile(
    r"\b(?:"
    r"\(0[2-9]\)\s*\d{4}\s*\d{4}|"        # (07) 5501 7200
    r"0[2-9]\s*\d{4}\s*\d{4}|"            # 07 5501 7200
    r"04\d{2}\s*\d{3}\s*\d{3}|"           # 0427 998 551
    r"1[38]00\s*\d{3}\s*\d{3}"            # 1300/1800 NNN NNN
    r")\b"
)

# Licences — multiple regulators. Keep separate group per kind so we
# can tag fingerprint_kind correctly.
_LICENCE_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("qbcc", re.compile(
        r"\bQ\.?B\.?C\.?C\.?\s*"
        r"(?:LICEN[CS]E|LIC\.?(?:\s*NO\.?)?)"
        r"[^\n]{0,40}?"
        r"(?:N[°o]?|No\.?|NUMBER|#)?\.?\s*[:.\-]?\s*"
        r"(\d[\d\s]*[A-Z]?)",
        re.IGNORECASE,
    )),
    ("qbsa", re.compile(
        r"\bQ\.?B\.?S\.?A\.?\s*"
        r"(?:LICEN[CS]E|LIC\.?(?:\s*NO\.?)?)"
        r"[^\n]{0,40}?"
        r"(?:N[°o]?|No\.?|NUMBER|#)?\.?\s*[:.\-]?\s*"
        r"(\d[\d\s]*[A-Z]?)",
        re.IGNORECASE,
    )),
    ("bsa", re.compile(
        r"\bBSA\s+(?:LICEN[CS]E|LIC\.?)\s*[:.\-]?\s*(\d[\d\s]*[A-Z]?)",
        re.IGNORECASE,
    )),
    ("vba", re.compile(
        r"\bVBA\s+(?:LICEN[CS]E|LIC\.?)\s*"
        r"(?:NUMBER|N[°o]?|No\.?|#)?\.?\s*[:.\-]?\s*"
        r"([A-Z]{1,4}[-\s]?[A-Z]{0,4}\s*\d[\d\s]*)",
        re.IGNORECASE,
    )),
    ("licence_other", re.compile(
        r"\b(?:NSW\s+)?BUILDERS?\s+LICEN[CS]E\s*"
        r"(?:N[°o]?|No\.?|NUMBER|#)?\.?\s*[:.\-]?\s*"
        r"(\d[\d\s]*[A-Z]?)",
        re.IGNORECASE,
    )),
]

_ACN_RE = re.compile(
    r"\bA\.?\s*C\.?\s*N\.?\s*[:.]?\s*(\d{3}\s*\d{3}\s*\d{3})\b",
    re.IGNORECASE,
)
_ABN_RE = re.compile(
    r"\bA\.?\s*B\.?\s*N\.?\s*[:.]?\s*(\d{2}\s*\d{3}\s*\d{3}\s*\d{3})\b",
    re.IGNORECASE,
)


# Domains we should ignore — generic / council / utility / non-firm.
_IGNORABLE_DOMAINS = frozenset({
    "goldcoast.qld.gov.au",
    "qld.gov.au",
    "gov.au",
    "asic.gov.au",
    "energex.com.au",
    "ergon.com.au",
    "urbanutilities.com.au",
    "qldwater.com.au",
    "australia.com",
    "google.com",
    "youtube.com",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
})


# ---------------------------------------------------------------- record type


@dataclass
class Fingerprint:
    kind: str                # 'url' | 'email' | 'phone' | 'qbcc' | …
    raw_value: str           # as found in text
    normalized_value: str    # canonical form for joining
    span_start: int
    span_end: int
    page_index: int
    layout: dict | None = None


# ---------------------------------------------------------------- normalisers


def _norm_url(host: str, path: str | None) -> str:
    """`WWW.BORISDESIGN.COM.AU` → `borisdesign.com.au`. Drop path."""
    h = host.lower().strip()
    if h.startswith("www."):
        h = h[4:]
    return h


def _norm_email(local: str, host: str) -> str:
    return f"{local.lower()}@{host.lower()}"


def _norm_phone(raw: str) -> str:
    return re.sub(r"\D", "", raw)


def _norm_licence(raw: str) -> str:
    """Strip whitespace; keep alphanumerics + dashes."""
    return re.sub(r"\s+", "", raw).upper()


def _norm_acn(raw: str) -> str | None:
    digits = re.sub(r"\D", "", raw)
    return digits if len(digits) == 9 else None


def _norm_abn(raw: str) -> str | None:
    digits = re.sub(r"\D", "", raw)
    return digits if len(digits) == 11 else None


# ---------------------------------------------------------------- extractor


def extract_fingerprints(page: PageLayout) -> list[Fingerprint]:
    """Walk a page's text and emit one Fingerprint per identifying token
    found. Layout (bbox + font) is computed for each match."""
    out: list[Fingerprint] = []
    text = page.text
    seen: set[tuple[str, str]] = set()  # (kind, normalized) — dedup per page

    def _emit(kind: str, raw: str, normalized: str, start: int, end: int) -> None:
        key = (kind, normalized)
        if key in seen:
            return
        seen.add(key)
        out.append(Fingerprint(
            kind=kind,
            raw_value=raw,
            normalized_value=normalized,
            span_start=start,
            span_end=end,
            page_index=page.page_index,
            layout=layout_for_span(page, start, end),
        ))

    # URLs
    for m in _URL_RE.finditer(text):
        host = m.group("host")
        path = m.group("path")
        normalized = _norm_url(host, path)
        if normalized in _IGNORABLE_DOMAINS:
            continue
        _emit("url", m.group(0), normalized, m.start(), m.end())

    # Emails
    for m in _EMAIL_RE.finditer(text):
        host = m.group("host").lower()
        if host in _IGNORABLE_DOMAINS:
            continue
        normalized = _norm_email(m.group("local"), host)
        _emit("email", m.group(0), normalized, m.start(), m.end())

    # Phones
    for m in _PHONE_RE.finditer(text):
        raw = m.group(0)
        normalized = _norm_phone(raw)
        if len(normalized) < 8:
            continue
        _emit("phone", raw, normalized, m.start(), m.end())

    # Licences (per kind)
    for kind, regex in _LICENCE_PATTERNS:
        for m in regex.finditer(text):
            raw = m.group(0)
            normalized = _norm_licence(m.group(1))
            if not normalized or not any(c.isdigit() for c in normalized):
                continue
            _emit(kind, raw, normalized, m.start(), m.end())

    # ACN / ABN
    for m in _ACN_RE.finditer(text):
        normalized = _norm_acn(m.group(1))
        if normalized:
            _emit("acn", m.group(0), normalized, m.start(), m.end())
    for m in _ABN_RE.finditer(text):
        normalized = _norm_abn(m.group(1))
        if normalized:
            _emit("abn", m.group(0), normalized, m.start(), m.end())

    return out
