"""Regex-tier parser for City of Gold Coast Council correspondence.

Council letters (Confirmation Notice, Decision Notice, Information
Request, Decision Notice Cover Letter) follow a fixed template. They
are far cleaner than the form-extracted blob because the council
typist split the applicant name from the c/- agent and from the
postal address.

This module:

1. Decides whether a chunk of extracted text is a COGC letter
   (`is_cogc_correspondence`). Strict header AND/OR footer match —
   keeps recall high without false positives on applicant-submitted
   reports that merely mention the council in passing.
2. Pulls structured fields out (`parse_cogc_letter`) — the recipient
   block at the top, the "Applicant name:" / "Applicant contact
   details:" pair when present, and the narrative
   "I refer to the development application lodged by: <name>" form.
3. Splits multi-party name strings ("Peter Dawson and Noela Roberts",
   "Daniel and Amber Knapp", "Mark & Skye Rustin") into one entry per
   person, with simple surname-inheritance for the "Daniel and Amber
   Knapp" case. Refuses to split obvious company names ("Storey and
   Castle Planning", "Homecorp Constructions").

The output is intentionally schema-light — just lists of cleaned
strings + a doc-kind tag. The CLI harvester turns these into
`application_entities` rows.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field


# ---------------------------------------------------------------- detection


# Strong fingerprints for COGC correspondence (header or footer). We
# require at least one — but each one alone is enough.
_COGC_FINGERPRINTS = (
    r"\bCouncil of the City of Gold Coast\b",
    r"\b07\s*5582\s*8866\b",
    r"\bmail@goldcoast\.qld\.gov\.au\b",
    r"\bCity Development Branch\b",
    r"\bPlanning\s*&\s*Environment Directorate\b",
)
_COGC_FINGERPRINT_RE = re.compile("|".join(_COGC_FINGERPRINTS), re.IGNORECASE)


def is_cogc_correspondence(text: str) -> bool:
    """True if the extracted text shows COGC-letterhead fingerprints."""
    if not text:
        return False
    return _COGC_FINGERPRINT_RE.search(text) is not None


# ---------------------------------------------------------------- doc kind


_DOC_KIND_PATTERNS: tuple[tuple[str, str], ...] = (
    # Order matters — the cover letter contains "Decision Notice" too.
    ("decision_notice_cover_letter", r"^\s*Decision Notice\s*$.*Please find enclosed the decision notice"),
    ("decision_notice",              r"Decision notice\s*[—-]\s*(?:approval|Express Development Application)"),
    ("confirmation_notice",          r"Confirmation Notice\s*[–-]\s*Assessment Manager"),
    ("information_request",          r"^\s*Information Request\s*$"),
    ("response_to_ir_cover_letter",  r"Response to (?:Information Request|IR)"),
)


def detect_doc_kind(text: str) -> str | None:
    for kind, pat in _DOC_KIND_PATTERNS:
        if re.search(pat, text, re.MULTILINE | re.DOTALL):
            return kind
    return None


# ---------------------------------------------------------------- name splitting


# Tokens that mark a string as a company / firm — do NOT split on "and".
_COMPANY_MARKERS = re.compile(
    r"\b("
    r"pty\s*ltd|pty\s*limited|limited|ltd|inc|corp|p\s*/\s*l|p/l|"
    r"trust|group|holdings|developments?|constructions?|builders?|homes?|"
    r"planning|consult(?:ing|ants?)|architects?|"
    r"&\s*co|&\s*sons|&\s*partners"
    r")\b",
    re.IGNORECASE,
)

# STRONG markers: always a separate legal entity. When one of these is
# in a sub-piece, that sub-piece is definitely separable from any
# co-named individuals ("The JNP Trust & Darren and Karen Mealing"
# splits into [trust, person, person]). Distinguished from the weaker
# markers above which often appear as descriptive suffixes inside
# firm names ("Storey and Castle Planning") where splitting is wrong.
_STRONG_COMPANY_MARKERS = re.compile(
    r"\b(pty\s*ltd|pty\s*limited|limited|ltd|inc|corp|p\s*/\s*l|p/l|"
    r"trust|holdings|partnership)\b",
    re.IGNORECASE,
)

# Strip trailing punctuation/whitespace artefacts.
_TRAILING_JUNK = re.compile(r"[\s,;:.\-]+$")


def _looks_like_company(s: str) -> bool:
    return bool(_COMPANY_MARKERS.search(s))


def _has_strong_company_marker(s: str) -> bool:
    return bool(_STRONG_COMPANY_MARKERS.search(s))


# Inline "C/-" / "C/o" / "Care of" — used when a recipient block was
# rendered on one line by the PDF (no newline before the agent), so
# parse_recipient_block couldn't split it. Requires whitespace on the
# left so we don't match mid-word.
_INLINE_CARE_OF_RE = re.compile(
    r"\s+(?:c\s*/\s*[\-o]|care\s+of)\s+",
    re.IGNORECASE,
)


def extract_inline_co_agent(raw: str) -> tuple[str, str | None]:
    """Pull an inline "C/- <agent>" off the right of a recipient string.

    Used when the PDF rendered the agent on the same physical line as
    the primary recipient ("Peter Dawson and Noela Roberts C/- Planit
    Consulting Pty Ltd"). Returns (primary_text, agent_text_or_None).

    The split is purely textual — the caller decides what role to assign.
    """
    if not raw:
        return raw, None
    m = _INLINE_CARE_OF_RE.search(raw)
    if not m:
        return raw, None
    primary = raw[: m.start()].strip()
    agent = raw[m.end():].strip()
    return primary, (agent or None)


def split_party_names(raw: str) -> list[str]:
    """Split a recipient name like "Peter Dawson and Noela Roberts" into parts.

    Behaviour:
      - "Peter Dawson and Noela Roberts" → ["Peter Dawson", "Noela Roberts"]
      - "Daniel Knapp and Amber Knapp"   → ["Daniel Knapp", "Amber Knapp"]
      - "Daniel and Amber Knapp"         → ["Daniel Knapp", "Amber Knapp"]
      - "Mark & Skye Rustin"             → ["Mark Rustin", "Skye Rustin"]
      - "John Badaloff, Wendy Ann Badaloff"
                                         → ["John Badaloff", "Wendy Ann Badaloff"]
      - "Storey and Castle Planning"     → ["Storey and Castle Planning"]
      - "Homecorp Constructions"         → ["Homecorp Constructions"]
      - "The JNP Trust & Darren and Karen Mealing"
                                         → ["The JNP Trust",
                                            "Darren Mealing", "Karen Mealing"]
      - "Bob Builder Pty Ltd and Jane Smith"
                                         → ["Bob Builder Pty Ltd", "Jane Smith"]
    """
    s = (raw or "").strip()
    s = _TRAILING_JUNK.sub("", s)
    if not s:
        return []

    # Comma-separated person list ("John Badaloff, Wendy Ann Badaloff")
    # — only treat as a list when each comma-separated chunk is itself
    # plausibly a person name (≥2 words, no company markers).
    if "," in s:
        comma_parts = [p.strip() for p in s.split(",") if p.strip()]
        if len(comma_parts) >= 2 and all(
            len(p.split()) >= 2 and not _looks_like_company(p) for p in comma_parts
        ):
            return comma_parts

    # Split on top-level " and " / " & " (case-insensitive, requires
    # whitespace boundaries so "Stephanie" / "Brandon" don't trigger).
    pieces = [
        p.strip()
        for p in re.split(r"\s+(?:and|&)\s+", s, flags=re.IGNORECASE)
        if p.strip()
    ]

    if len(pieces) < 2:
        return [s]

    # MIXED MODE — at least one piece carries a strong legal-entity
    # marker (Pty Ltd / Trust / Holdings / etc). That piece is its
    # own row; remaining pieces go through person-name processing
    # (surname inheritance from the longest person piece).
    if any(_has_strong_company_marker(p) for p in pieces):
        people = [p for p in pieces if not _has_strong_company_marker(p)]
        if people:
            fullest = max(people, key=lambda p: len(p.split()))
            if len(fullest.split()) >= 2:
                surname = fullest.split()[-1]
                people = [
                    p if len(p.split()) >= 2 else f"{p} {surname}"
                    for p in people
                ]
        # Reassemble in original order, swapping in the inherited
        # forms for the person pieces.
        out: list[str] = []
        person_iter = iter(people)
        for p in pieces:
            if _has_strong_company_marker(p):
                out.append(p)
            else:
                out.append(next(person_iter))
        return out

    # No strong markers. If the whole string still looks company-like
    # ("Storey and Castle Planning", "Homecorp Constructions"), keep
    # it whole — the conjunction is internal to a firm name.
    if _looks_like_company(s):
        return [s]

    # Pure-person path with surname inheritance.
    fullest = max(pieces, key=lambda p: len(p.split()))
    if len(fullest.split()) >= 2:
        surname = fullest.split()[-1]
        pieces = [
            p if len(p.split()) >= 2 else f"{p} {surname}"
            for p in pieces
        ]
    return pieces


def guess_entity_type(name: str) -> str:
    if _looks_like_company(name):
        return "company"
    # Bare ALL-CAPS short names ("DAWSON & ROBERTS") are usually
    # informal labels for a couple — flag as 'unknown' so callers
    # can decide whether to merge.
    if name.isupper() and len(name.split()) <= 3:
        return "unknown"
    return "individual"


# ---------------------------------------------------------------- recipient block


# "MERMAID BEACH QLD 4218" — last line of every recipient block. The
# suburb is upper-cased, two spaces around QLD are common in DOCX→PDF
# rendering. Postcode is 4 digits.
_POSTAL_TERMINATOR_RE = re.compile(
    r"^\s*([A-Z][A-Z\s'\-]+?)\s+(?:QLD|NSW|VIC|ACT|SA|WA|TAS|NT)\s+(\d{4})\s*$",
)
_OUR_REF_RE = re.compile(r"^\s*Our reference:\s*([^\s].*?)\s*$", re.MULTILINE)
_DEAR_RE = re.compile(r"^\s*Dear\s+(?:Sir|Madam|Mr|Mrs|Ms|Mx|All)", re.MULTILINE)
_CARE_OF_RE = re.compile(r"^\s*[Cc]\s*/\s*-?\s*(.+?)\s*$")


@dataclass
class RecipientBlock:
    primary_name: str | None = None
    care_of_agent: str | None = None
    address_lines: list[str] = field(default_factory=list)
    suburb: str | None = None
    postcode: str | None = None


def parse_recipient_block(text: str) -> RecipientBlock | None:
    """Pull the address-header recipient block out of a COGC letter.

    Strategy: anchor on `Our reference:` (always present, always near
    the top) and look forward for the first non-blank lines, stopping
    at the next blank line or `Dear Sir/Madam`.
    """
    m = _OUR_REF_RE.search(text)
    if not m:
        return None
    cursor = m.end()

    # Walk lines until we find the recipient block. Skip leading blanks.
    after = text[cursor:]
    lines = after.splitlines()

    block: list[str] = []
    started = False
    for raw in lines:
        line = raw.strip()
        if _DEAR_RE.match(raw):
            break
        if not line:
            if started:
                break
            continue
        # Sanity guard — if we somehow drift into the body, bail.
        if line.lower().startswith(
            ("i refer to", "please find", "this letter", "applicant name:")
        ):
            break
        started = True
        block.append(line)

    if not block:
        return None

    rb = RecipientBlock()

    # Last line MUST be a postal terminator (suburb + state + postcode).
    # Without one we're not looking at a real recipient block — most
    # likely the body of a structured Decision Notice attachment.
    last = block[-1]
    pm = _POSTAL_TERMINATOR_RE.match(last)
    if not pm:
        return None
    rb.suburb = pm.group(1).title().strip()
    rb.postcode = pm.group(2)
    body = block[:-1]

    # First line is the primary recipient name.
    if body:
        rb.primary_name = body[0]
        rest = body[1:]
    else:
        rest = []

    # If line 2 is "C/- ..." it's the agent — strip the prefix.
    if rest:
        cm = _CARE_OF_RE.match(rest[0])
        if cm:
            rb.care_of_agent = cm.group(1).strip()
            rest = rest[1:]

    rb.address_lines = rest
    return rb


# ---------------------------------------------------------------- structured Applicant


_APPLICANT_BLOCK_RE = re.compile(
    r"Applicant\s+name:\s*\n\s*(?P<name>.+?)\s*\n"
    r"\s*Applicant\s+contact\s+details:\s*\n\s*(?P<details>.+?)"
    r"(?=\n\s*\n|\n\s*Application\s+(?:details|number)|\n\s*Application\b|\Z)",
    re.IGNORECASE | re.DOTALL,
)


@dataclass
class ApplicantStructuredField:
    applicant_name: str | None = None
    contact_lines: list[str] = field(default_factory=list)
    care_of_agent: str | None = None


def parse_applicant_structured(text: str) -> ApplicantStructuredField | None:
    """Extract the 'Applicant name: ... / Applicant contact details: ...' block.

    Used by Confirmation Notice and Decision Notice attachments.
    """
    m = _APPLICANT_BLOCK_RE.search(text)
    if not m:
        return None

    out = ApplicantStructuredField(applicant_name=m.group("name").strip() or None)

    detail_lines = [ln.strip() for ln in m.group("details").splitlines() if ln.strip()]
    if detail_lines:
        cm = _CARE_OF_RE.match(detail_lines[0])
        if cm:
            out.care_of_agent = cm.group(1).strip()
            detail_lines = detail_lines[1:]
    out.contact_lines = detail_lines
    return out


# ---------------------------------------------------------------- narrative refer-by-name


_REFER_BY_NAME_RE = re.compile(
    r"I\s+refer\s+to\s+the\s+development\s+application\s+lodged\s+by:\s*\n"
    r"(?:Applicant\s+name:\s*\n)?"
    r"(?P<name>.+?)\s*\n"
    r"(?:Applicant\s+contact\s+details:[\s\S]*?\n)?"
    r"(?:.*?\n){0,8}?"
    r"\s*in\s+relation\s+to\s+development\s+of\s+land",
    re.IGNORECASE,
)


def parse_refer_by_name(text: str) -> str | None:
    """Extract the recipient name from the Information Request body.

    Pattern: "I refer to the development application lodged by:\\n<NAME>\\n
    in relation to development of land/premises described as:".
    """
    m = _REFER_BY_NAME_RE.search(text)
    if not m:
        return None
    cand = m.group("name").strip()
    # Skip the structured-form variant where "<NAME>" is actually the
    # "Applicant contact details:" header that snuck through.
    if cand.lower().startswith("applicant contact"):
        return None
    return cand or None


# ---------------------------------------------------------------- top-level


@dataclass
class ParsedCorrespondence:
    is_cogc: bool
    doc_kind: str | None
    recipient: RecipientBlock | None
    applicant_field: ApplicantStructuredField | None
    refer_by_name: str | None


def parse_cogc_letter(text: str) -> ParsedCorrespondence | None:
    """Top-level entry. Returns None if the text isn't a COGC letter."""
    if not is_cogc_correspondence(text):
        return None
    return ParsedCorrespondence(
        is_cogc=True,
        doc_kind=detect_doc_kind(text),
        recipient=parse_recipient_block(text),
        applicant_field=parse_applicant_structured(text),
        refer_by_name=parse_refer_by_name(text),
    )
