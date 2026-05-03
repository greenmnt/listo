"""Document-context metadata that's fed to ML extractors as input tags.

Every document carries a small fixed metadata block that gives the
model jurisdictional + structural context:

  [KIND: ...]      ← workflow stage (submission/decision/...)
  [TYPE: ...]      ← raw doc_type ("3. Plans", "Decision Notice", ...)
  [COUNCIL: ...]   ← council slug (cogc / newcastle / ...)
  [STATE: ...]     ← AU state code (qld / nsw / vic)
  [VENDOR: ...]    ← portal vendor (infor_epathway / techone_etrack / ...)

These five tags let one model handle:
  - mixed doc kinds (training data shared across plans + letters + forms)
  - mixed councils (per-council quirks tagged, transferable across)
  - mixed states (state-level patterns like QBCC/VBA inherited)
  - mixed portal vendors (ePathway PDF rendering ≠ T1Cloud)

When we onboard a new council, the model already knows e.g. QLD-wide
patterns from existing COGC data — we just need a small fine-tuning
batch on the new council's specifics.

The values come from existing schema:
  - kind, type   → council_application_documents.doc_kind / .doc_type
  - council      → council_applications.council_slug
  - vendor       → council_applications.vendor
  - state        → councils.registry COUNCILS[slug].state

Used by the LayoutLMv3 extractor (when built) and any LLM-based
extractor that wants doc context in its prompt.
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text as sql_text


@dataclass
class DocMetadata:
    doc_id: int
    application_id: int
    kind: str        # doc_kind enum value
    type: str        # raw doc_type
    council: str     # council slug, e.g. 'cogc'
    state: str       # 'qld', 'nsw', etc. (lowercase)
    vendor: str      # 'infor_epathway', 'techone_etrack', etc.

    def as_tokens(self) -> list[str]:
        """Render as a list of bracketed tag tokens, ready to prepend
        to the model input sequence (each gets a zero bbox)."""
        return [
            f"[KIND: {self.kind or 'other'}]",
            f"[TYPE: {self.type or 'unknown'}]",
            f"[COUNCIL: {self.council}]",
            f"[STATE: {self.state}]",
            f"[VENDOR: {self.vendor}]",
        ]

    def as_prompt_block(self) -> str:
        """Render as a one-line prompt header for an LLM (Sonnet, qwen,
        etc.) — same info, prose-friendly format."""
        return (
            f"Document context — kind={self.kind}, type={self.type!r}, "
            f"council={self.council} ({self.state.upper()}), "
            f"portal={self.vendor}."
        )


def doc_metadata_for(s, doc_id: int) -> DocMetadata | None:
    """Look up a document's full metadata from the existing schema.
    Returns None if the doc id doesn't exist or the council slug isn't
    registered.
    """
    from listo.councils.registry import COUNCILS

    row = s.execute(sql_text("""
        SELECT
          d.id            AS doc_id,
          d.application_id AS application_id,
          d.doc_kind      AS kind,
          d.doc_type      AS type,
          ca.council_slug AS council,
          ca.vendor       AS vendor
        FROM council_application_documents d
        JOIN council_applications ca ON ca.id = d.application_id
        WHERE d.id = :doc_id
    """), {"doc_id": doc_id}).fetchone()
    if row is None:
        return None

    council_def = COUNCILS.get(row.council)
    state = council_def.state.lower() if council_def else "unknown"

    return DocMetadata(
        doc_id=row.doc_id,
        application_id=row.application_id,
        kind=row.kind or "other",
        type=row.type or "unknown",
        council=row.council,
        state=state,
        vendor=row.vendor,
    )
