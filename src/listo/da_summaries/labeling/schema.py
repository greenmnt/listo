"""Fixed label schema for the LayoutLMv3 entity extractor.

Token-classification with BIO tags. Each entity role we care about
gets a `B-<ROLE>` (begin) and `I-<ROLE>` (inside) tag; everything else
is `O` (outside). Standard NER convention.

Why this exact set:
  - applicant / agent / owner / builder / architect — five roles that
    actually appear with meaningful frequency in `application_entities`.
  - engineer / surveyor / landscape — secondary consultants that show
    up in Specialist Reports + Delegated Planner Reports. Worth tagging
    distinctly so we can later filter to "redev developer team" vs
    "incidental consultants".
  - We do NOT include sub-roles like `town_planning_consultant` —
    these all collapse into `agent` for our purposes (matches the
    role assigned by the COGC correspondence parser today).

Bumping this schema means retraining from scratch — so we keep it
short and resist the urge to add fine-grained roles until we have
strong evidence they matter.
"""
from __future__ import annotations


# Canonical role names. Lowercase, snake_case. These are what we
# write into application_entities.role today, so the extractor is a
# drop-in replacement.
ROLES = (
    "applicant",
    "agent",
    "owner",
    "builder",
    "architect",
    "engineer",
    "surveyor",
    "landscape",
)


# Build BIO tag list. Order matters — index → label_id is fixed once
# a model is trained.
LABELS: list[str] = ["O"]
for r in ROLES:
    LABELS.append(f"B-{r.upper()}")
    LABELS.append(f"I-{r.upper()}")

LABEL2ID: dict[str, int] = {lbl: i for i, lbl in enumerate(LABELS)}
ID2LABEL: dict[int, str] = {i: lbl for lbl, i in LABEL2ID.items()}


# Some `application_entities.role` values we've already emitted that
# don't map cleanly to one of `ROLES` — fold or drop them here.
ROLE_NORMALISATION: dict[str, str | None] = {
    # consultants captured via plans/specialist parsers — fold to
    # `agent` for now (matches existing convention where Findasite
    # Town Planners is tagged `agent`)
    "consultant":   "agent",
    "developer":    "applicant",   # developer-as-applicant pattern
    "client":       None,           # ambiguous — drop, don't train on these rows
    "unknown":      None,
    "other":        None,
}


def normalise_role(raw: str | None) -> str | None:
    """Map a raw application_entities.role value to one of ROLES, or
    None if it should be dropped from the training set."""
    if raw is None:
        return None
    r = raw.strip().lower()
    if r in ROLES:
        return r
    return ROLE_NORMALISATION.get(r, None)


def role_to_bio(role: str, position: str) -> str:
    """`role='applicant'`, `position='B'` → `'B-APPLICANT'`."""
    return f"{position}-{role.upper()}"


def num_labels() -> int:
    return len(LABELS)
