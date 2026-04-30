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


class BuildFeatures(BaseModel):
    """Physical / cost-driver attributes extracted from drawings, design
    statements, and specialist reports. Populated per-chunk in the
    build-features lane and merged in aggregate.

    Every field is null when the chunk doesn't mention it. Long-tail
    list fields (materials list, plant species, fittings) are stored as
    short strings rather than nested models — keeps the JSON schema
    simple for 7B models, which are flaky on nested objects.
    """

    gfa_m2: int | None = Field(..., ge=0, le=20_000, description="Gross floor area in square metres (sum of all enclosed floor areas) — read from a GFA / area schedule if present, else null")
    site_area_m2: int | None = Field(..., ge=0, le=200_000, description="Site / lot area in square metres if explicitly stated, else null")
    internal_area_m2: int | None = Field(..., ge=0, le=20_000, description="Internal / habitable area in m² if stated separately from GFA, else null")
    external_area_m2: int | None = Field(..., ge=0, le=20_000, description="Outdoor decks / verandas / patios / balconies area in m² if stated, else null")
    levels: int | None = Field(..., ge=0, le=20, description="Storeys above ground (e.g. 'two-storey duplex' → 2). 0 if explicitly single-storey unstated.")
    has_basement: bool | None = Field(..., description="True if a basement or sub-floor level is described; null when not mentioned")
    garage_spaces: int | None = Field(..., ge=0, le=30, description="Number of car spaces in the garage / carport, or null")
    bedrooms: int | None = Field(..., ge=0, le=50, description="Total bedrooms across all dwellings if stated, or null")
    bathrooms: int | None = Field(..., ge=0, le=50, description="Total bathrooms across all dwellings if stated, or null")
    materials_walls: str | None = Field(..., max_length=300, description="External wall materials/finishes — e.g. 'rendered block + timber cladding'. Null if not stated.")
    materials_roof: str | None = Field(..., max_length=200, description="Roof materials — e.g. 'colorbond, charcoal'. Null if not stated.")
    materials_floor: str | None = Field(..., max_length=200, description="Internal floor finishes — e.g. 'engineered oak + tiles to wet areas'. Null if not stated.")
    fittings_quality: Literal["budget", "mid", "premium", "luxury", "unknown"] = Field(..., description="Implied finish tier from fittings/appliances/inclusions described in this chunk; 'unknown' if no signal")
    fittings_notes: str | None = Field(..., max_length=400, description="Specific high-signal fittings e.g. 'Smeg appliances, stone benchtops, fireplace, ducted AC' — null if absent")
    landscaping_summary: str | None = Field(..., max_length=400, description="One-line summary of landscape design/spec — e.g. 'native + tropical mix, paved driveway, in-ground pool'. Null if absent.")
    plant_species: list[str] = Field(..., max_length=80, description="Plant species or common names listed in landscape plans (empty list if none). Use the most common form found.")
    has_pool: bool | None = Field(..., description="True if an in-ground or above-ground swimming pool is part of the proposal; null if not mentioned")
    confidence: Literal["high", "medium", "low"] = Field(..., description="Self-rated confidence in this chunk's extraction overall")
    notes: str | None = Field(..., max_length=300, description="Free-form caveats / things you noticed but didn't have a field for. Null if none.")


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
