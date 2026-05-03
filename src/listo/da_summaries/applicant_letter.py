"""Regex-tier parser for applicant-authored letters TO council.

Council letters (handled by `cogc_correspondence`) are FROM the
council — they carry council letterhead fingerprints and a fixed
template (recipient block / Applicant fields / "I refer to ...").

Applicant letters travel the other direction. Town planners, civil
engineers, architects etc. write to council on their own letterhead.
These are *exactly* the consultant entities we want labelled, but
the COGC parser ignores them entirely.

Detection (strict gate, applied AFTER `is_cogc_correspondence` has
returned False — we never re-tag a council letter):

  - has a sign-off keyword (Yours sincerely / faithfully / Regards / etc.)
  - has a letterhead-style company line in the first 1.5 KB
  - is NOT a COGC letter (caller's responsibility)

Extraction:

  - **letterhead company** — the firm sending the letter. Pulled from
    the first 1.5 KB. ABN proximity (`<Company>\\nABN: ...`) wins;
    otherwise first matching company-suffix line.
  - **sign-off block** — the keyword + up to 6 following non-blank
    lines. Plausible person name (2-5 Title-Case words, qualifications
    stripped) + the next non-contact line as role.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


_SIGNOFF_RE = re.compile(
    r"\bYours\s+(?:sincerely|faithfully|truly)\b"
    r"|\bKind\s+regards\b"
    r"|\bBest\s+regards\b"
    r"|\bWarm\s+regards\b"
    r"|\bRegards\b",
    re.IGNORECASE,
)


# Strong council-author indicators. We need our own discriminator
# (independent of cogc_correspondence.is_cogc_correspondence) because
# `is_cogc` over-fires on applicant letters that simply *address* the
# council branch in the recipient block (e.g. "Chief Executive Officer
# / City Development Branch / Gold Coast City Council").
#
# A real council-AUTHORED letter has BOTH:
#   - The canonical header block (Date: / Contact: / Location: / Telephone:)
#     with "Our reference: MCU/.." (council's own application code)
#   - A "For the Chief Executive Officer" closing
# Either signal is enough to rule the doc council-authored.
_COUNCIL_AUTHOR_HEADER = re.compile(
    r"\bOur\s+reference\s*:\s*(?:MCU|COM|ROL|EDA|EXA|PDA|FDA|OPW|RDA|SBA)/\d",
    re.IGNORECASE,
)
_COUNCIL_AUTHOR_FOOTER = re.compile(
    r"For\s+the\s+(?:Chief\s+Executive\s+Officer|Mayor)",
    re.IGNORECASE,
)


def looks_like_council_authored(text: str) -> bool:
    """True if the doc has structural markers of a COUNCIL-authored
    letter — either the canonical 'Our reference: MCU/...' top-block
    OR the 'For the Chief Executive Officer' closing."""
    if not text:
        return False
    head = text[:1000]
    tail = text[-1500:]
    if _COUNCIL_AUTHOR_HEADER.search(head):
        return True
    if _COUNCIL_AUTHOR_FOOTER.search(tail):
        return True
    return False


# Company-suffix line: <Company words> + (Pty Ltd | Group | Consulting | …).
# Constrained: company prefix is 4-80 chars of letters/spaces/punct, must
# start with an uppercase letter. The suffix list is intentionally
# narrow — these are the words a planner / engineer / architect firm
# actually uses in its registered name.
_COMPANY_SUFFIX = (
    r"Pty\s*Ltd|Pty\s*Limited|Pty\.\s*Ltd\.?|"
    r"Limited|Ltd|"
    r"Group|"
    r"Consulting|Consultants|"
    r"Engineering|Engineers|"
    r"Architects|Architecture|"
    r"Planning|Planners"
)
_COMPANY_LINE_RE = re.compile(
    r"\b([A-Z][A-Za-z'&\-,\.\s]{3,80}?\s+(?:" + _COMPANY_SUFFIX + r"))(?:\s|$|,|\n)"
)

# ABN — every Australian business letterhead has one in the masthead.
_ABN_RE = re.compile(r"\bABN[:\s.]*\d{2}\s*\d{3}\s*\d{3}\s*\d{3}\b", re.IGNORECASE)


def has_signoff(text: str) -> bool:
    return _SIGNOFF_RE.search(text) is not None


def is_applicant_letter(text: str) -> bool:
    """True if this looks like an applicant→council letter.

    Uses an own-test (`looks_like_council_authored`) rather than the
    looser `is_cogc_correspondence`, because the latter over-fires on
    applicant letters that just *mention* the council branch in their
    recipient block.
    """
    if not text or not has_signoff(text):
        return False
    if looks_like_council_authored(text):
        return False
    head = text[:1500]
    if _COMPANY_LINE_RE.search(head) is None and _ABN_RE.search(head) is None:
        return False
    return True


# ---------------------------------------------------------------- helpers


def _looks_like_person(line: str) -> str | None:
    """Strip qualifications / titles and decide if `line` is a
    plausible person name. Returns the cleaned name or None.

    Handles patterns like:
      "Jesse Hardman (BE(Civil), MIEAust)"     → "Jesse Hardman"
      "Dr Rodney Ronalds"                       → "Dr Rodney Ronalds"
      "Dominee Rye, RPEQ MEIAust"               → "Dominee Rye"
      "Sigrid Pembroke"                         → "Sigrid Pembroke"
    """
    s = line.strip()
    if not s or len(s) > 80:
        return None
    # Reject common contact-info prefixes.
    if re.match(r"^(?:P\s*[:.]|E\s*[:.]|W\s*[:.]|M\s*[:.]|"
                r"Phone|Email|Web|Mobile|Tel|Fax|Direct|Address)\b",
                s, re.IGNORECASE):
        return None
    # Reject if it contains digits (sheet refs, contact numbers, etc.)
    if any(c.isdigit() for c in s):
        return None
    # Reject if it looks like a company suffix.
    if re.search(r"\b(?:" + _COMPANY_SUFFIX + r")\b", s, re.IGNORECASE):
        return None
    # Strip "(qualifications)" trailing parenthesis block.
    s = re.sub(r"\s*\(.*$", "", s).strip()
    # Strip trailing qualifications after a comma: "Dominee Rye, RPEQ"
    s = re.sub(r",\s+[A-Z][A-Z\s]+.*$", "", s).strip()
    # Strip trailing comma.
    s = s.rstrip(",").strip()
    # Title prefix (keep but check the rest).
    body = re.sub(r"^(?:Dr|Mr|Mrs|Ms)\.?\s+", "", s)
    words = body.split()
    if not (2 <= len(words) <= 5):
        return None
    # Each word must start with a capital letter (Title Case) — but
    # allow the second name to be all-caps surname.
    bad = [w for w in words if not (w[0].isupper())]
    if bad:
        return None
    return s


# Lines we should skip when scanning past a sign-off keyword (signature
# images often render as blank lines or admin bracketed text).
_SKIP_AFTER_SIGNOFF_RE = re.compile(
    r"^\s*$"                              # blank
    r"|^[\s\W]+$"                         # punctuation only
    r"|^Digitally\s+signed",              # docusign banner
    re.IGNORECASE,
)


def _company_line(line: str) -> str | None:
    """If `line` is itself a company-suffix-bearing string, return it
    cleaned up. Else None."""
    s = line.strip().rstrip(",").strip()
    if not s or len(s) > 100:
        return None
    if _COMPANY_LINE_RE.match(s):
        return s
    return None


def _extract_signoff_block(
    text: str,
) -> tuple[str | None, str | None, str | None]:
    """Find the LAST sign-off keyword and parse the immediately-
    following block. Returns (name, role, signoff_company) — any may
    be None.

    We search in the last 4000 chars (signoffs sit near the end of
    the letter). If multiple signoffs exist (e.g. a quoted email
    chain), the last one wins.

    The third return value covers the layout
        Yours sincerely,
        <Name>
        <Role>
        <Company Pty Ltd>
    where the company appears in the signoff itself rather than the
    letterhead (common for planning consultancies)."""
    tail = text[-4000:]
    matches = list(_SIGNOFF_RE.finditer(tail))
    if not matches:
        return None, None, None
    m = matches[-1]
    after = tail[m.end():].lstrip(",").lstrip()
    lines = [ln.rstrip() for ln in after.split("\n")]
    candidates = [ln for ln in lines[:12] if not _SKIP_AFTER_SIGNOFF_RE.match(ln)]
    if not candidates:
        return None, None, None

    # Layout A (planner-style): person → role → company
    # Layout B (engineering-firm-style): company → person → role
    # Scan up to 6 candidate lines; pick first plausible person.
    name = None
    name_idx = None
    signoff_company = None
    for i, ln in enumerate(candidates[:6]):
        if signoff_company is None:
            cl = _company_line(ln)
            if cl:
                signoff_company = cl
        person = _looks_like_person(ln)
        if person:
            name = person
            name_idx = i
            break
    if not name:
        # No person? still return any company we saw so the caller can
        # at least record the firm.
        return None, None, signoff_company

    role = None
    if name_idx is not None and name_idx + 1 < len(candidates):
        role_line = candidates[name_idx + 1].strip().rstrip(",")
        if 0 < len(role_line) <= 80 and not _SKIP_AFTER_SIGNOFF_RE.match(role_line):
            if not re.match(r"^(?:P\s*[:.]|E\s*[:.]|W\s*[:.]|M\s*[:.]|"
                            r"Phone|Email|Web|Mobile|Tel|Fax|Direct)\b",
                            role_line, re.IGNORECASE):
                role = role_line

    # Look further down for a trailing company line if we haven't seen one.
    if signoff_company is None and name_idx is not None:
        for ln in candidates[name_idx + 1: name_idx + 5]:
            cl = _company_line(ln)
            if cl:
                signoff_company = cl
                break

    return name, role, signoff_company


def _extract_letterhead_company(text: str) -> str | None:
    """Find the firm's name in the letterhead (first ~1500 chars).
    Two strategies, ordered by reliability:

    1. **ABN proximity** — Australian business letterheads put the
       firm name immediately above the ABN line. The line right
       before "ABN: ..." is the company.
    2. **First company-suffix line** — fall back to the first match
       of `<Words> Pty Ltd | Group | Consulting | ...`.
    """
    head = text[:1500]

    abn = _ABN_RE.search(head)
    if abn:
        pre = head[:abn.start()].rstrip()
        last_lines = [ln.strip() for ln in pre.split("\n") if ln.strip()][-3:]
        for ln in reversed(last_lines):
            # Reject pure address / ref-number lines.
            if re.match(r"^\d", ln):
                continue
            if re.match(r"^(?:Your|Our|Ref)\s+", ln, re.IGNORECASE):
                continue
            if re.search(r"\bP\.?O\.?\s*Box\b", ln, re.IGNORECASE):
                continue
            # Trim trailing comma.
            return ln.rstrip(",").strip()

    m = _COMPANY_LINE_RE.search(head)
    if m:
        return m.group(1).strip()

    return None


# ---------------------------------------------------------------- public API


@dataclass
class ParsedApplicantLetter:
    """Both `letterhead_company` and `signoff_company` are kept so the
    caller can attribute each emission to its source. They're often
    the same firm — the harvester de-duplicates downstream via the
    company natural-key upsert."""
    letterhead_company: str | None
    signoff_company: str | None
    signoff_name: str | None
    signoff_role: str | None


def _looks_like_template_artefact(name: str | None) -> bool:
    """Returns True for letterhead extractions that picked up the
    recipient block / template fragment instead of an actual firm
    name (e.g. 'Chief Executive Officer \\nCity Development')."""
    if not name:
        return False
    if "\n" in name:
        return True
    return bool(re.search(
        r"\b(?:Chief|Executive|Officer|Branch|Department)\b",
        name, re.IGNORECASE,
    ))


def parse_applicant_letter(text: str) -> ParsedApplicantLetter | None:
    """Parse an applicant-authored letter. Returns None if the text
    isn't recognisable as one (incl. when it looks council-authored).
    """
    if not is_applicant_letter(text):
        return None
    letterhead = _extract_letterhead_company(text)
    name, role, signoff_company = _extract_signoff_block(text)
    if _looks_like_template_artefact(letterhead):
        letterhead = None
    if not letterhead and not signoff_company and not name:
        return None
    return ParsedApplicantLetter(
        letterhead_company=letterhead,
        signoff_company=signoff_company,
        signoff_name=name,
        signoff_role=role,
    )
