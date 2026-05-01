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

import pymupdf  # type: ignore[import-untyped]


logger = logging.getLogger(__name__)


# How many pages to scan looking for the title block. Cover sheets +
# drawing index usually live on pages 1-2; the title block starts at
# page 3 and repeats. We bail as soon as we find one.
_MAX_PAGES_TO_SCAN = 12


# "Metricon homes owns copyright"  — name is everything before " owns copyright".
# Loose on case + extra whitespace; strict on the literal phrase.
_OWNS_COPYRIGHT_RE = re.compile(
    r"^\s*([A-Z][\w\s&\-\.,'/]+?)\s+owns\s+copyright\b",
    re.IGNORECASE | re.MULTILINE,
)

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

# QBSA/QBCC/Builders Licence — Qld + general AU patterns.
_BUILDERS_LICENCE_RE = re.compile(
    r"\b(QBCC|QBSA|BUILDERS?\s+LICEN[CS]E)\b[\s\S]{0,40}?"
    r"N[°o]?\.?\s*[:.]?\s*([A-Z0-9\-]+)",
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
    if licence:
        return "builder"
    if _ARCHITECT_NAME_RE.search(name):
        return "architect"
    return "unknown"


def _confidence(acn: str | None, licence: str | None) -> str:
    score = (1 if acn else 0) + (1 if licence else 0)
    if score >= 2:
        return "high"
    if score == 1:
        return "medium"
    return "low"


def parse_plans_title_block(text: str) -> PlansTitleBlock | None:
    """Find a title block in already-extracted text. Returns None if no
    "owns copyright" anchor is present."""
    m = _OWNS_COPYRIGHT_RE.search(text or "")
    if not m:
        return None
    raw_name = m.group(1).strip()

    # Title-case but preserve common all-caps suffixes (PTY LTD).
    parts = raw_name.split()
    name = " ".join(p if p.isupper() and len(p) <= 4 else p.title() for p in parts)

    acn_m = _ACN_RE.search(text)
    abn_m = _ABN_RE.search(text)
    lic_m = _BUILDERS_LICENCE_RE.search(text)

    acn = _normalise_acn(acn_m.group(1)) if acn_m else None
    abn = _normalise_abn(abn_m.group(1)) if abn_m else None
    licence = lic_m.group(2).strip() if lic_m else None

    return PlansTitleBlock(
        name=name,
        role=_classify_role(name, licence),
        acn=acn,
        abn=abn,
        licence=licence,
        confidence=_confidence(acn, licence),
        page=0,  # caller fills in
    )


def extract_from_plan_pdf(file_path: str) -> PlansTitleBlock | None:
    """Open a plan PDF and scan up to _MAX_PAGES_TO_SCAN pages for the
    title block. Returns the first hit, with `page` set 1-indexed."""
    p = Path(file_path)
    if not p.exists():
        return None
    try:
        with pymupdf.open(p) as pdf:
            n = min(pdf.page_count, _MAX_PAGES_TO_SCAN)
            for i in range(n):
                t = pdf[i].get_text("text") or ""
                hit = parse_plans_title_block(t)
                if hit is not None:
                    hit.page = i + 1
                    return hit
    except Exception as exc:  # noqa: BLE001
        logger.warning("pymupdf failed scanning plan %s: %s", p, exc)
        return None
    return None
