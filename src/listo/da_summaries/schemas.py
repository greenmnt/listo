"""Pydantic schemas + completeness rule.

The `DocFacts` schema is what we ask Ollama to return as JSON. The same
shape carries through to `da_doc_summaries` and (with merge) to
`da_summaries`.
"""
from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field


PROMPT_VERSION = "v3"


class DwellingKind(StrEnum):
    HOUSE = "house"
    GRANNY = "granny"
    DUPLEX = "duplex"
    TRIPLEX = "triplex"
    TOWNHOUSES = "townhouses"
    APARTMENTS = "apartments"
    MIXED = "mixed"
    OTHER = "other"
    UNKNOWN = "unknown"


KIND_VALUES: set[str] = {k.value for k in DwellingKind}


EntityType = Literal["company", "individual", "trust", "unknown"]


class DocFacts(BaseModel):
    """What the LLM extracts from a single document.

    Every field is required-but-nullable (`Field(...)`) so the JSON
    schema marks them all as `required`. With Ollama's `format=schema`
    constraint, this forces the LLM to include every field in its
    output (using null when the fact isn't stated) rather than
    cherry-picking the easy ones.

    Applicant + owner have ACN/ABN/entity_type companions because those
    are the parties whose identity matters for cross-DA aggregation
    (a developer flips many lots — we want one row per ACN, not one
    per spelling variation). Builder/architect stay flat for now since
    their forms rarely include ACN.
    """

    applicant_name: str | None = Field(..., description="Primary applicant entity as written, including 'Pty Ltd'/'Limited' suffix; or null")
    applicant_acn: str | None = Field(..., description="9-digit ACN (Australian Company Number) of applicant if stated, digits only no spaces, or null")
    applicant_abn: str | None = Field(..., description="11-digit ABN of applicant if stated, digits only no spaces, or null")
    applicant_entity_type: EntityType = Field(..., description="company / individual / trust / unknown")
    applicant_agent_name: str | None = Field(..., description="The 'c/-' lodging agent (often a town planner like 'HPC Planning'), or null")
    builder_name: str | None = Field(..., description="Construction firm; often blank on initial Form 1; null if absent")
    architect_name: str | None = Field(..., description="Designer / drafter / architectural firm, or null")
    owner_name: str | None = Field(..., description="Property owner entity as written, or null")
    owner_acn: str | None = Field(..., description="9-digit ACN of owner if stated (digits only), or null")
    owner_abn: str | None = Field(..., description="11-digit ABN of owner if stated (digits only), or null")
    owner_entity_type: EntityType = Field(..., description="company / individual / trust / unknown")
    dwelling_count: int | None = Field(..., ge=0, le=500, description="Total dwellings on the lot after the proposal, or null")
    dwelling_kind: DwellingKind = Field(..., description="Pick the closest enum value; use 'unknown' if you cannot tell")
    project_description: str | None = Field(..., max_length=2000, description="One- or two-sentence summary of the proposal, or null")
    lot_on_plan: str | None = Field(..., description="e.g. 'Lot 3 RP12345', or null")
    street_address: str | None = Field(..., description="Full street address with suburb + postcode, or null")
    confidence: Literal["high", "medium", "low"] = Field(..., description="Self-rated confidence in the extraction overall")
    notes: str | None = Field(..., max_length=200, description="Any caveats; <100 chars; null if none")


def is_complete(
    *,
    dwelling_count: int | None,
    dwelling_kind: str | None,
    applicant_name: str | None,
    builder_name: str | None,
    architect_name: str | None,
) -> bool:
    """Returns True when a `da_summaries` row has enough data to be
    considered complete (i.e. doesn't need phase-2 escalation).

    Rules (kept loose enough that older / simpler DAs still pass):
    - dwelling_count must be non-null
    - dwelling_kind must be non-null and not 'unknown'
    - applicant_name must be non-null
    - at least one of (builder_name, architect_name) must be non-null
    """
    if dwelling_count is None:
        return False
    if not dwelling_kind or dwelling_kind == DwellingKind.UNKNOWN.value:
        return False
    if not applicant_name:
        return False
    if not (builder_name or architect_name):
        return False
    return True
