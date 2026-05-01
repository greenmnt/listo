"""council_application_documents.doc_kind: temporal-stage classification

The DA workflow is fixed-shape:

    submission → ir_council → ir_response → further_info → decision

Knowing which stage each doc belongs to lets us derive an entity
timeline ('builder X appears in submission, then in ir_response we
see architect Y added') without storing change events explicitly.

The ENUM is populated by `listo.da_summaries.doc_kind.classify_doc_kind`,
applied via `listo da reclassify-docs` (idempotent).

Revision ID: 0021
Revises: 0020
Create Date: 2026-05-01
"""
from __future__ import annotations

from alembic import op


revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE council_application_documents
          ADD COLUMN doc_kind ENUM(
            'submission', 'amendment',
            'ir_council', 'ir_response',
            'further_info', 'decision', 'other'
          ) NULL,
          ADD KEY ix_doc_kind (application_id, doc_kind)
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE council_application_documents
          DROP KEY ix_doc_kind,
          DROP COLUMN doc_kind
    """)
