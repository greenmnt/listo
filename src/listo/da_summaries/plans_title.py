"""Tier-0 regex parser for builder/architect title blocks on plan PDFs.

Plan PDFs from volume builders (Metricon, Plantation Homes, Coral Homes,
McDonald Jones, etc.) carry a uniform copyright-block on every drawing
sheet — name, address, ACN, builders licence, URL. This module pulls the
entity out of that block:

  Metricon homes owns copyright   ← strongest anchor — name on this line
  in this drawing.
  © COPYRIGHT 2019
  www.metricon.com.au
  A.C.N.053189469                ← machine-readable ACN
  QBSA LICENCE N°: 40992         ← QBSA/QBCC = Qld builders licence
  NSW BUILDERS LICENCE N°: 36654C

Role assignment is signal-driven, not doc_type-driven, because volume
builders also produce the "Architectural Plans" attachment in-house:

  - QBCC/QBSA/Builders Licence found → role = builder
  - else if name contains 'Architect' / 'Architecture' / 'Design'
    → role = architect
  - else → role = unknown (caller may default by doc_type)

Confidence is high when ACN + licence both corroborate; medium with just
one anchor; low if only the copyright line was found.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pymupdf  # type: ignore[import-untyped]

from listo.da_summaries.cogc_correspondence import _looks_like_company


logger = logging.getLogger(__name__)


# How many pages to scan looking for the title block. Cover sheets +
# drawing index usually live on pages 1-2; the title block starts at
# page 3 and repeats. We bail as soon as we find one.
_MAX_PAGES_TO_SCAN = 12


# Anchor 1: "Metricon homes owns copyright" — name precedes " owns copyright".
_OWNS_COPYRIGHT_RE = re.compile(
    r"^\s*([A-Z][\w\s&\-\.,'/]+?)\s+owns\s+copyright\b",
    re.IGNORECASE | re.MULTILINE,
)

# Anchor 2: "© THIS DRAWING IS TO REMAIN THE PROPERTY OF G.J. GARDNER HOMES…"
# Require the © symbol in the lookback so we don't match legal boilerplate
# like "THESE PLANS ARE PROTECTED BY COPY RIGHT AND ARE THE PROPERTY OF
# THE AUTHOR" which lacks ©. Names with internal dots ("G.J.") are
# allowed; capture ends at "." followed by newline.
_PROPERTY_OF_RE = re.compile(
    r"©\s*(?:THIS\s+)?(?:DRAWING|DESIGN|PLAN)S?"
    r"[\s\S]{0,80}?"
    r"\bPROPERTY\s+OF\s+"
    r"([A-Z][\w\s&\-\.,'/]+?)"
    r"\.\s*(?=\n|$|REPRODUCTION\b|ANY\s+UN)",
    re.IGNORECASE,
)

# Generic-placeholder names that the regex sometimes captures from
# legal boilerplate. Drop them.
_GENERIC_NAME_BLOCKLIST = frozenset({
    "the author", "the owner", "the proprietor", "the designer",
    "the copyright holder", "the company", "the builder",
})


# Anchor 3: tabular label-value blocks on plan title sheets.
#   client :          designed by :     drawn by :
#   Goldberg Constr.  DC                DC
#
# Labels live in their own pdf-spans with bboxes; values sit in nearby
# spans (right or below). PyMuPDF flat-text reads cells row-by-row and
# smashes labels+values together — only layout-aware parsing recovers
# the pairing. Allow up to 3 leading chars on the label (PyMuPDF
# occasionally misses the first letter of a span: "lient :" instead of
# "client :").
_TABULAR_LABEL_RE = re.compile(
    r"^.{0,3}?(?P<label>"
    r"client|builder|architect|designer|"
    r"designed?\s+by|drawn?\s+by|drafted?\s+by|"
    r"engineer|developer|owner|principal"
    r")\s*:\s*$",
    re.IGNORECASE,
)

# Map normalised label → entity role. Some labels we explicitly do NOT
# want a row for ("drawn by" is usually a person's initials).
# "client" is special — see `_role_for_client_value`.
_LABEL_TO_ROLE: dict[str, str | None] = {
    "client":       "client",       # placeholder; real role decided by value shape
    "builder":      "builder",
    "architect":    "architect",
    "designer":     "architect",
    "designed by":  "architect",
    "drawn by":     None,
    "drafted by":   None,
    "engineer":     "engineer",
    "developer":    "developer",
    "owner":        "owner",
    "principal":    None,
}


def _role_for_client_value(value: str) -> str:
    """A designer's `client :` field is ambiguous — could be the
    contracted builder (Goldberg Construction) or the homeowner
    (Simelhay & Imlach). Decide from the value's shape:
      - Company markers (Pty Ltd / Construction / Trust / etc.) → builder
      - Otherwise → owner
    """
    return "builder" if _looks_like_company(value) else "owner"

# "Inert" labels — recognised as labels (so we don't pick their text
# as a value for adjacent active labels) but produce no entity row.
# These are everywhere on plan title blocks.
_INERT_LABEL_RE = re.compile(
    r"^.{0,3}?(?:"
    r"date|title|scale|paper|north|status|revision|rev|issue|"
    r"sheet(?:\s*no)?|drawing(?:\s*no)?|project(?:\s*no)?|"
    r"job(?:\s*no)?|order(?:\s*no)?|file|amend(?:ment)?s?|"
    r"chk|check(?:ed)?(?:\s*by)?|approved(?:\s*by)?|"
    r"address|north\s*point"
    r")\s*:?\s*$",
    re.IGNORECASE,
)

# Reject obvious non-name values from the tabular extractor.
_NON_NAME_VALUE_RE = re.compile(
    r"^\s*("
    r"\d[\d\s.,]*[a-z]?|"                             # numeric incl. spaces/units ("25 24", "10.0 M", "1239")
    r"\d{1,4}[/\-.]\d{1,4}([/\-.]\d{2,4})?|"          # date ("18/02/2020")
    r"[A-Z]{1,3}|"                                    # 1-3 char initials ("DC", "KOJ")
    r"sheet\s+\d+|page\s+\d+|"                        # pagination
    r"\d+\s*:\s*\d+|"                                 # scale ("1:100")
    r"a\d+(?:[-_]\d+)?|"                              # drawing number ("A2-02")
    r"on\s+a\d+|"                                     # paper size ("on A3")
    r"job\s*(?:no|number)|order\s*(?:no|number)|"     # bare label residue
    r"rev(?:ision)?|status|paper|north|date|title|scale"
    r")\s*$",
    re.IGNORECASE,
)

# Common AU residential street suffixes. Values that match are
# addresses, not entity names. Two patterns:
#   - `<digits> <name> <suffix>`   (e.g., "5 Sunshine Parade")
#   - `<name> <suffix>`            (e.g., "Sunshine Parade" alone —
#                                   common when the title block puts
#                                   the street name in its own cell)
_ADDRESS_RE = re.compile(
    r"^\s*(?:\d+\s+\S.*\b|\S+(?:\s+\S+)*\s+)("
    r"st|street|rd|road|av|ave|avenue|cct|circuit|cl|close|"
    r"cr|cres|crescent|dr|drive|hwy|highway|la|lane|"
    r"pde|parade|pl|place|tce|terrace|wy|way|ct|court|sq|square|"
    r"esp|esplanade|bvd|boulevard|grn|green"
    r")\b\s*$",
    re.IGNORECASE,
)


def _norm_label(raw: str) -> str:
    """Collapse whitespace, lowercase. 'designed  By' → 'designed by'."""
    return re.sub(r"\s+", " ", raw).strip().lower()


# "<word> no" / "<word> number" / "<word> reference" — not an entity name.
_LABEL_RESIDUE_RE = re.compile(
    r"^\s*\w+\s+(?:no\.?|number|reference|ref\.?|code|id)\.?\s*$",
    re.IGNORECASE,
)


def _looks_like_entity_name(value: str) -> bool:
    """Strict positive filter: reject anything that doesn't look like a
    real entity name. Keeps the recall-vs-precision dial firmly on
    precision for the tabular extractor — the regex anchors get most
    of the recall, the tabular path is the fallback."""
    v = value.strip()
    if len(v) < 5:
        return False
    # Label-value smash ("Dws: N3", "Permit: 12345") leaves a colon in
    # the captured span. Real names don't.
    if ":" in v:
        return False
    # Reject "<word> No" / "<word> Number" / "<word> Ref" residues.
    if _LABEL_RESIDUE_RE.match(v):
        return False
    # Must be majority letters (rejects "10.0 M", "25 24" if they
    # somehow slipped past the numeric regex).
    letter_count = sum(1 for c in v if c.isalpha())
    if letter_count < len(v) * 0.5:
        return False
    # Either ≥2 alpha tokens OR contains a company marker.
    tokens = re.findall(r"\b[A-Za-z]+\b", v)
    if len(tokens) >= 2:
        return True
    if _looks_like_company(v):
        return True
    return False


def parse_tabular_title_block(page) -> list[PlansTitleBlock]:
    """Find `label : value` pairs in a tabular title block via bbox
    proximity. For each span whose text exactly matches a known label,
    pair it with the nearest non-label span that is either:
      - same row, immediately to the right, or
      - directly below with a similar x-start (next row, same column).

    `page` is a `entity_evidence.PageLayout`. Returns empty list if no
    label spans present."""
    out: list[PlansTitleBlock] = []
    if not getattr(page, "spans", None):
        return out

    # Pre-index: label spans + plain (non-label, non-empty) value candidates.
    label_spans: list[tuple[Any, str, str]] = []  # (span_meta, raw_label, role)
    for sp in page.spans:
        txt = page.text[sp.char_start:sp.char_end].strip()
        m = _TABULAR_LABEL_RE.match(txt)
        if not m:
            continue
        label = _norm_label(m.group("label"))
        role = _LABEL_TO_ROLE.get(label)
        if role is None:
            continue
        label_spans.append((sp, label, role))

    if not label_spans:
        return out

    seen_names: set[str] = set()
    for lsp, label, role in label_spans:
        l_x0, l_y0, l_x1, l_y1 = lsp.bbox
        l_h = max(l_y1 - l_y0, 1.0)

        candidates: list[tuple[float, Any, str]] = []
        for vs in page.spans:
            if vs is lsp:
                continue
            v_text = page.text[vs.char_start:vs.char_end].strip()
            if not v_text or len(v_text) < 4:
                continue
            # Skip other labels (active or inert).
            if _TABULAR_LABEL_RE.match(v_text):
                continue
            if _INERT_LABEL_RE.match(v_text):
                continue
            # Skip non-name values (numbers, dates, initials, etc.).
            if _NON_NAME_VALUE_RE.match(v_text):
                continue
            # Skip addresses ("15 Abalone Avenue", "5 Sonia Street").
            if _ADDRESS_RE.match(v_text):
                continue
            # Strict positive filter — must look like a real name.
            if not _looks_like_entity_name(v_text):
                continue

            v_x0, v_y0, v_x1, v_y1 = vs.bbox
            v_y_center = (v_y0 + v_y1) / 2
            l_y_center = (l_y0 + l_y1) / 2

            same_row = abs(l_y_center - v_y_center) < l_h * 0.7
            to_right = v_x0 >= l_x1 - 2

            below = (v_y0 >= l_y1 - 1) and (v_y0 <= l_y1 + 25)
            x_close = abs(v_x0 - l_x0) < 60

            if same_row and to_right:
                dist = (v_x0 - l_x1) + 0.0   # purely horizontal
                candidates.append((dist, vs, v_text))
            elif below and x_close:
                # Slight penalty so a same-row right neighbour wins ties.
                dist = (v_y0 - l_y1) + 5.0
                candidates.append((dist, vs, v_text))

        if not candidates:
            continue
        candidates.sort(key=lambda c: c[0])
        _, vs, raw_value = candidates[0]

        name = _clean_name(raw_value)
        key = name.lower()
        if (
            not name
            or len(name) < 4
            or key in seen_names
            or key in _GENERIC_NAME_BLOCKLIST
        ):
            continue
        seen_names.add(key)

        # Resolve the dynamic role for "client".
        emit_role = role
        if role == "client":
            emit_role = _role_for_client_value(name)

        out.append(PlansTitleBlock(
            name=name,
            role=emit_role,
            acn=None,
            abn=None,
            licence=None,
            confidence="medium",        # no licence/ACN corroboration
            page=0,                     # caller fills in
            span_start=vs.char_start,
            span_end=vs.char_end,
            page_text=page.text,
        ))

    return out

# "A.C.N.053189469", "ACN: 053 189 469", "A C N 053189469"
_ACN_RE = re.compile(
    r"\bA\.?\s*C\.?\s*N\.?\s*[:.]?\s*(\d{3}\s*\d{3}\s*\d{3})\b",
    re.IGNORECASE,
)

# "ABN 123 456 789 012" / "A.B.N.: 12345678901"
_ABN_RE = re.compile(
    r"\bA\.?\s*B\.?\s*N\.?\s*[:.]?\s*(\d{2}\s*\d{3}\s*\d{3}\s*\d{3})\b",
    re.IGNORECASE,
)

# Builders Licence — handles the format zoo:
#   - "QBSA LICENCE N°: 40992"             (modern Metricon)
#   - "Q.B.S.A. LIC.no.1091125"            (older dotted form, G.J. Gardner)
#   - "BSA Lic: 121 8091"                  (abbreviated, hand-typed by smaller firms)
#   - "QBCC Licence No: 87654"             (current Qld regulator name)
#   - "BUILDERS LICENCE N°: 36654C"        (NSW)
# Captured number can contain spaces (we strip them).
_BUILDERS_LICENCE_RE = re.compile(
    r"\b(?:Q\.?B\.?S\.?A\.?|Q\.?B\.?C\.?C\.?|BSA|BCC|BUILDERS?)\s*"
    r"\.?\s*"
    r"(?:LICEN[CS]E|LIC\.?(?:\s*NO\.?)?)"
    r"[^\n]{0,40}?"
    r"(?:N[°o]?|No\.?|#)?\.?\s*[:.]?\s*"
    r"(\d[\d ]*[A-Z]?)",
    re.IGNORECASE,
)

# Architect-firm signal: name contains one of these.
_ARCHITECT_NAME_RE = re.compile(
    r"\b(architects?|architecture|design(?:ers?)?)\b",
    re.IGNORECASE,
)


@dataclass
class PlansTitleBlock:
    name: str
    role: str           # 'builder' | 'architect' | 'unknown'
    acn: str | None     # 9 digits, no spaces
    abn: str | None     # 11 digits, no spaces
    licence: str | None # raw licence number string
    confidence: str     # 'high' | 'medium' | 'low'
    page: int           # 1-indexed page where the block was found
    # Char offsets of the matched name within the page text; populated
    # so callers can look up bbox/font for evidence collection.
    span_start: int = 0
    span_end: int = 0
    page_text: str = ""


def _normalise_acn(raw: str | None) -> str | None:
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    return digits if len(digits) == 9 else None


def _normalise_abn(raw: str | None) -> str | None:
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    return digits if len(digits) == 11 else None


def _classify_role(name: str, licence: str | None) -> str:
    """Architect-name marker wins over licence presence — many
    designers (Brad Ruddell Design, SMEKdesign) carry a builders /
    practitioner licence too, but their actual role is architect /
    designer. Only fall back to `builder` when the name has no
    design marker AND a licence is present."""
    if _ARCHITECT_NAME_RE.search(name):
        return "architect"
    if licence:
        return "builder"
    return "unknown"


def _confidence(acn: str | None, licence: str | None) -> str:
    score = (1 if acn else 0) + (1 if licence else 0)
    if score >= 2:
        return "high"
    if score == 1:
        return "medium"
    return "low"


# Acronyms / abbreviations that should stay all-caps after title-casing.
# Anything else gets .title()'d, so "GOLD COAST" → "Gold Coast".
_KEEP_UPPER = frozenset({
    "PTY", "LTD", "INC", "ABN", "ACN", "ACT", "NSW", "VIC", "QLD",
    "SA", "WA", "TAS", "NT", "AU", "USA", "UK", "GST",
    "AUS", "BSA", "QBSA", "QBCC", "BCC",
    "I", "II", "III", "IV",
})


def _clean_name(raw: str) -> str:
    """Title-case the entity name, preserving known acronyms (PTY, LTD)
    but lower-casing arbitrary all-caps words (GOLD COAST → Gold Coast).
    Internal dots in initialisms ("G.J.") are kept upper."""
    raw = re.sub(r"\s+", " ", raw).strip(" ,.;:-")
    parts = raw.split()
    out = []
    for p in parts:
        # Initialism with dots: "G.J.", "U.S.A." → keep upper.
        if re.fullmatch(r"(?:[A-Z]\.){2,}[A-Z]?\.?", p):
            out.append(p.upper())
            continue
        if p.upper() in _KEEP_UPPER:
            out.append(p.upper())
            continue
        out.append(p.title())
    return " ".join(out)


def _all_licences(text: str) -> list[str]:
    """Every builders-licence number found in the text, normalised
    (whitespace stripped). Used to disambiguate which entity owns which
    licence when a title block lists more than one."""
    out: list[str] = []
    for m in _BUILDERS_LICENCE_RE.finditer(text):
        n = re.sub(r"\s+", "", m.group(1))
        if n and n not in out:
            out.append(n)
    return out


def _licence_near(text: str, anchor_start: int, anchor_end: int,
                  window: int = 350) -> str | None:
    """Find the closest builders-licence number to the anchor. Looks
    BOTH directions because copyright statements and licence numbers
    appear in either order on different builders' title blocks."""
    lo = max(0, anchor_start - window)
    hi = min(len(text), anchor_end + window)
    chunk = text[lo: hi]
    best: tuple[int, str] | None = None
    for m in _BUILDERS_LICENCE_RE.finditer(chunk):
        n = re.sub(r"\s+", "", m.group(1))
        if not n:
            continue
        # Distance from anchor in original text coordinates
        match_start = lo + m.start()
        if anchor_start <= match_start <= anchor_end:
            dist = 0
        elif match_start < anchor_start:
            dist = anchor_start - m.end() - lo
        else:
            dist = match_start - anchor_end
        if best is None or dist < best[0]:
            best = (dist, n)
    return best[1] if best else None


def parse_plans_title_block(text: str) -> list[PlansTitleBlock]:
    """Return every entity found in the page text. May contain a builder
    AND a designer when both are present (G.J. Gardner + Brad Ruddell
    pattern). Empty list when no anchor matched."""
    text = text or ""
    out: list[PlansTitleBlock] = []
    seen_names: set[str] = set()

    # ACN/ABN are page-scoped — they usually appear once. Bind to whatever
    # licence sits closest. Today we just attach the first ACN/ABN to the
    # first entity that has a licence (good enough for the volume-builder
    # case which is the main user). Refine later if we see counter-examples.
    acn_m = _ACN_RE.search(text)
    abn_m = _ABN_RE.search(text)
    page_acn = _normalise_acn(acn_m.group(1)) if acn_m else None
    page_abn = _normalise_abn(abn_m.group(1)) if abn_m else None
    acn_attached = False
    abn_attached = False

    # Anchor 1: "[Name] owns copyright"
    for m in _OWNS_COPYRIGHT_RE.finditer(text):
        name = _clean_name(m.group(1))
        key = name.lower()
        if not name or key in seen_names or key in _GENERIC_NAME_BLOCKLIST:
            continue
        seen_names.add(key)
        licence = _licence_near(text, m.start(), m.end())
        acn = page_acn if (licence and not acn_attached) else None
        abn = page_abn if (licence and not abn_attached and not acn) else None
        if acn:
            acn_attached = True
        if abn:
            abn_attached = True
        out.append(PlansTitleBlock(
            name=name,
            role=_classify_role(name, licence),
            acn=acn, abn=abn, licence=licence,
            confidence=_confidence(acn, licence),
            page=0,
            span_start=m.start(1),
            span_end=m.end(1),
            page_text=text,
        ))

    # Anchor 2: "PROPERTY OF [Name]"
    for m in _PROPERTY_OF_RE.finditer(text):
        name = _clean_name(m.group(1))
        key = name.lower()
        if not name or key in seen_names or key in _GENERIC_NAME_BLOCKLIST:
            continue
        seen_names.add(key)
        licence = _licence_near(text, m.start(), m.end())
        acn = page_acn if (licence and not acn_attached) else None
        abn = page_abn if (licence and not abn_attached and not acn) else None
        if acn:
            acn_attached = True
        if abn:
            abn_attached = True
        out.append(PlansTitleBlock(
            name=name,
            role=_classify_role(name, licence),
            acn=acn, abn=abn, licence=licence,
            confidence=_confidence(acn, licence),
            page=0,
            span_start=m.start(1),
            span_end=m.end(1),
            page_text=text,
        ))

    return out


def parse_url_inferred_block(page) -> list[PlansTitleBlock]:
    """Anchor 4: use a URL on the page to identify the firm name on
    the same page.

    Strategy: extract URL hosts from the page text, derive each one's
    brand stem (`smekdesign.com.au` → `smekdesign`), then iterate
    every text span on the page. A span whose
    *alphanumeric-only-normalised* text equals the stem is the firm
    name — emit it with the verbatim span text. This catches plain-
    text title blocks where the firm name appears alone (`SMEKdesign`,
    `BORIS DESIGN`) with no copyright / licence-label / tabular anchor.

    Iterating whole-spans (not substring search) avoids the URL itself
    matching: `SMEKDESIGN.COM.AU` normalises to `smekdesigncomau`,
    not `smekdesign`.
    """
    from listo.da_summaries.plan_fingerprints import _URL_RE, _IGNORABLE_DOMAINS

    if not getattr(page, "spans", None):
        return []

    # Collect URL stems on this page.
    stems: set[str] = set()
    for m in _URL_RE.finditer(page.text):
        host = m.group("host").lower()
        if host.startswith("www."):
            host = host[4:]
        if host in _IGNORABLE_DOMAINS:
            continue
        stem = host.split(".")[0]
        if len(stem) >= 5:        # avoid tiny stems → spurious matches
            stems.add(stem)

    if not stems:
        return []

    out: list[PlansTitleBlock] = []
    seen_names: set[str] = set()
    for sp in page.spans:
        text = page.text[sp.char_start: sp.char_end].strip()
        if not text or len(text) < 4:
            continue
        normalised = re.sub(r"[^a-z0-9]", "", text.lower())
        if not normalised:
            continue
        # Exact match — span IS the firm brand stem
        # (e.g., "Boris Design" → 'borisdesign' == stem 'borisdesign').
        matched_stem: str | None = None
        if normalised in stems:
            matched_stem = normalised
        else:
            # Prefix match — span IS the brand head, URL appends a
            # suffix word (e.g., span "SMEK" prefix-matches stem
            # "smekdesign"). Require ≥3 extra chars in the stem so
            # a trivial 4-char span doesn't latch onto an unrelated
            # 5-char stem. Length-≥4 already enforced above.
            for stem in stems:
                if stem.startswith(normalised) and len(stem) >= len(normalised) + 3:
                    matched_stem = stem
                    break
            if matched_stem is None:
                continue
        key = text.lower()
        if key in seen_names:
            continue
        seen_names.add(key)

        licence = _licence_near(page.text, sp.char_start, sp.char_end)
        # Role classification combines span text + the *suffix* part of
        # the stem (extracted as its own word). For span "SMEK" matched
        # against stem "smekdesign", suffix = "design" — the
        # `\bdesign\b` test in _classify_role then fires correctly.
        role_input = text
        if matched_stem != normalised and len(matched_stem) > len(normalised):
            stem_suffix = matched_stem[len(normalised):]
            role_input = f"{text} {stem_suffix}"
        # Bind page-scoped ACN/ABN if present (only one usually appears
        # on a designer's title block, so attaching to the firm is fine).
        acn_m = _ACN_RE.search(page.text)
        abn_m = _ABN_RE.search(page.text)
        out.append(PlansTitleBlock(
            name=text,
            role=_classify_role(role_input, licence),
            acn=_normalise_acn(acn_m.group(1)) if acn_m else None,
            abn=_normalise_abn(abn_m.group(1)) if abn_m else None,
            licence=licence,
            confidence="medium",
            page=0,                # caller fills
            span_start=sp.char_start,
            span_end=sp.char_end,
            page_text=page.text,
        ))
    return out


def extract_from_plan_pdf(file_path: str) -> tuple[list[PlansTitleBlock], list]:
    """Open a plan PDF, run all three title-block parsers per page,
    and return `(hits, page_layouts)`.

    Anchors tried per page (in order):
      1. owns_copyright + property_of (text-only, with licence/ACN binding)
      2. tabular label-value pairs (bbox-aware, for "client : / designed by :")

    `hits[i].page` (1-indexed) maps into `page_layouts` (0-indexed) so
    the caller can look up bbox/font for `(span_start, span_end)` on
    the relevant page. Scans up to `_MAX_PAGES_TO_SCAN` pages and bails
    after the cover sheets once entities have been found."""
    from listo.da_summaries.entity_evidence import load_pdf_pages

    page_layouts = load_pdf_pages(file_path, max_pages=_MAX_PAGES_TO_SCAN)
    if not page_layouts:
        return [], []

    seen: set[str] = set()
    out: list[PlansTitleBlock] = []
    for pl in page_layouts:
        page_hits: list[PlansTitleBlock] = []
        page_hits.extend(parse_plans_title_block(pl.text))
        page_hits.extend(parse_tabular_title_block(pl))
        page_hits.extend(parse_url_inferred_block(pl))
        for hit in page_hits:
            key = hit.name.lower()
            if key in seen:
                continue
            seen.add(key)
            hit.page = pl.page_index + 1
            out.append(hit)
        if out and pl.page_index >= 4:
            break
    return out, page_layouts
