"""Heuristic filter pass over `entity_evidence`.

The regex harvester emits *predictions* (`status='predicted'`). Many are
obviously not real entities (single-letter "P" / "T", numbers, header
tokens like "PROJECT" or "ADDRESS"). This module sweeps those out before
ML training, recording WHICH rule rejected each row so we can audit later.

Audit trail uses existing columns:
- `status` flipped from 'predicted' → 'rejected'
- `verifier` set to e.g. `'heuristic_v1'`  (versioned so we can roll
  forward without losing the v1 history)
- `verified_at` set to NOW()
- `notes` set to the rule_name that triggered

To re-run with a newer ruleset, bump VERSION below; rows already rejected
by the old version won't be re-touched (we only operate on
`status='predicted'`).

Spot-check workflow:
- Run `listo da filter-entities --dry-run` first to see counts per rule.
- Run for real to apply.
- Inspect what survived: `SELECT ... WHERE status='predicted'` and look
  for new patterns the filter should catch.
- Add a new rule, bump VERSION, re-run.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Callable

from sqlalchemy import text

from listo.db import session_scope


VERSION = "v1"
log = logging.getLogger(__name__)


@dataclass
class Rule:
    name: str
    check: Callable[[dict], bool]   # returns True if row should be REJECTED
    why: str


# Header / template / form-field words that are NEVER company/person names
# but get caught by the regex when it sees Title-Case text in a title block.
_HEADER_TOKENS = {
    "PROJECT", "ADDRESS", "LOT", "DATE", "AMENDMENTS", "CHKD", "CHECKED",
    "AREA", "SITE", "TLF", "PH", "FAX", "PO BOX", "SHEET", "DRG",
    "DESCRIPTION", "TITLE", "CLIENT", "NOTES", "ISSUE", "DESIGN",
    "SCALE", "PLAN", "APPROVED", "REJECTED", "BUILDER", "BUILDERS",
    "ARCHITECT", "OWNER", "APPLICANT", "AGENT", "SHEETS", "REV",
    "REVISION", "ORIGINAL", "COPY", "SIGNED", "WITNESS", "PRINT",
    "NAME", "ROLE", "JOB", "REF", "FILE", "DRAWING", "DRAWN", "AUTH",
    "PROPOSED", "EXISTING", "AMEND", "LEVEL", "TBC", "TBA", "NTS",
    "QLD", "NSW", "VIC", "TAS", "WA", "SA", "NT", "ACT",
}

# Tokens that often slip through as "name" but are clearly admin labels.
_ADMIN_PHRASES = re.compile(
    r"^(?:see|refer|ph|fax|tel|email|web|po\s*box|p\.?o\.?|c/-)\b",
    re.IGNORECASE,
)

# Pure street-suffix-only matches (regex sometimes grabs "Drive" or "Court"
# as a name when there's no preceding number).
_STREET_SUFFIX_ONLY = {
    "STREET", "ROAD", "AVENUE", "DRIVE", "COURT", "PLACE", "LANE",
    "PARADE", "TERRACE", "CRESCENT", "BOULEVARD", "BOULEVARDE", "WAY",
    "HIGHWAY", "CIRCUIT", "ESPLANADE", "PROMENADE", "CLOSE", "GROVE",
    "ROW", "VIEW", "SQUARE", "LOOP", "RISE",
}


def _name(r: dict) -> str:
    return (r.get("candidate_name") or "").strip()


RULES: list[Rule] = [
    Rule(
        name="too_short",
        why="< 4 chars: regex grabbed initials / single letters / 'P', 'Tlf' etc.",
        check=lambda r: len(_name(r)) < 4,
    ),
    Rule(
        name="all_digits",
        why="numeric only — sheet numbers, page refs, etc.",
        check=lambda r: _name(r).replace(" ", "").isdigit(),
    ),
    Rule(
        name="no_alpha",
        why="contains zero alphabetic characters — punctuation/digits only",
        check=lambda r: not any(c.isalpha() for c in _name(r)),
    ),
    Rule(
        name="header_token",
        why="exact match to a known title-block / form-field header word",
        check=lambda r: _name(r).upper() in _HEADER_TOKENS,
    ),
    Rule(
        name="street_suffix_only",
        why="street-type word with no street name attached (e.g. 'Court')",
        check=lambda r: _name(r).upper() in _STREET_SUFFIX_ONLY,
    ),
    Rule(
        name="admin_phrase_prefix",
        why="starts with admin label (See/Refer/Ph/PO Box/c/-)",
        check=lambda r: bool(_ADMIN_PHRASES.match(_name(r))),
    ),
    Rule(
        name="all_uppercase_long",
        why="all-caps run of >35 chars — typically a wrapped paragraph or block of text, not a name",
        check=lambda r: len(_name(r)) > 35 and _name(r).isupper() and " " in _name(r) and any(len(w) > 12 for w in _name(r).split()),
    ),
    Rule(
        name="enormous",
        why="> 80 chars — almost certainly a span boundary error capturing multiple lines",
        check=lambda r: len(_name(r)) > 80,
    ),
]


@dataclass
class FilterStats:
    inspected: int = 0
    rejected: int = 0
    kept: int = 0
    by_rule: dict[str, int] | None = None

    def __post_init__(self):
        if self.by_rule is None:
            self.by_rule = {}


def run(*, dry_run: bool = False, version: str | None = None) -> FilterStats:
    """Apply heuristics to all `predicted` rows in entity_evidence.

    Each row is checked against rules in order; first matching rule wins
    and produces the rejection. Rows with no matching rule remain
    `predicted` (unchanged).
    """
    ver = version or VERSION
    verifier_tag = f"heuristic_{ver}"
    log.info("entity-filter %s: dry_run=%s", verifier_tag, dry_run)

    with session_scope() as s:
        rows = s.execute(text(
            "SELECT id, candidate_name, candidate_role, extractor "
            "FROM entity_evidence WHERE status = 'predicted'"
        )).fetchall()

    log.info("inspecting %d predicted rows", len(rows))

    rejections: list[tuple[int, str, str]] = []  # (id, rule_name, rule_why)
    stats = FilterStats(inspected=len(rows))
    for r in rows:
        d = dict(r._mapping)
        for rule in RULES:
            try:
                hit = rule.check(d)
            except Exception:  # noqa: BLE001
                hit = False
            if hit:
                rejections.append((d["id"], rule.name, rule.why))
                stats.by_rule[rule.name] = stats.by_rule.get(rule.name, 0) + 1
                stats.rejected += 1
                break
        else:
            stats.kept += 1

    log.info("would reject %d / %d (kept: %d)", stats.rejected, stats.inspected, stats.kept)
    for name, n in sorted(stats.by_rule.items(), key=lambda kv: -kv[1]):
        log.info("  rule %s: %d", name, n)

    if dry_run or not rejections:
        return stats

    # Bulk-update in chunks, recording rule_name in `notes` so we can
    # diff later and re-run with a refined ruleset.
    with session_scope() as s:
        for i in range(0, len(rejections), 500):
            batch = rejections[i : i + 500]
            for rid, name, why in batch:
                s.execute(text(
                    "UPDATE entity_evidence "
                    "   SET status='rejected', verifier=:v, verified_at=NOW(), "
                    "       notes=CONCAT(:rule, ': ', :why) "
                    " WHERE id=:id"
                ), {"v": verifier_tag, "rule": name, "why": why, "id": rid})
        s.commit()
    log.info("wrote %d updates", len(rejections))
    return stats
