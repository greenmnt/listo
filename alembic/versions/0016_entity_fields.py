"""entity carry-through columns: ACN/ABN/entity_type/agent on summaries

The LLM now extracts richer entity fields per-doc — primary name with
'Pty Ltd' preserved, ACN, ABN, entity_type ('company'/'individual'/
'trust'/'unknown'), and the 'c/-' agent name. We carry them through
da_doc_summaries → da_summaries, then aggregate resolves the
companies table FKs.

Revision ID: 0016
Revises: 0015
Create Date: 2026-04-30

"""
from __future__ import annotations

from alembic import op


revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE da_doc_summaries
          ADD COLUMN applicant_acn         CHAR(9)     NULL AFTER applicant_name,
          ADD COLUMN applicant_abn         VARCHAR(11) NULL AFTER applicant_acn,
          ADD COLUMN applicant_entity_type VARCHAR(20) NULL AFTER applicant_abn,
          ADD COLUMN applicant_agent_name  VARCHAR(255) NULL AFTER applicant_entity_type,
          ADD COLUMN owner_acn             CHAR(9)     NULL AFTER owner_name,
          ADD COLUMN owner_abn             VARCHAR(11) NULL AFTER owner_acn,
          ADD COLUMN owner_entity_type     VARCHAR(20) NULL AFTER owner_abn
    """)

    op.execute("""
        ALTER TABLE da_summaries
          ADD COLUMN applicant_acn         CHAR(9)     NULL AFTER applicant_name,
          ADD COLUMN applicant_abn         VARCHAR(11) NULL AFTER applicant_acn,
          ADD COLUMN applicant_entity_type VARCHAR(20) NULL AFTER applicant_abn,
          ADD COLUMN applicant_agent_name  VARCHAR(255) NULL AFTER applicant_entity_type,
          ADD COLUMN owner_acn             CHAR(9)     NULL AFTER owner_name,
          ADD COLUMN owner_abn             VARCHAR(11) NULL AFTER owner_acn,
          ADD COLUMN owner_entity_type     VARCHAR(20) NULL AFTER owner_abn,
          ADD KEY ix_das_applicant_acn (applicant_acn),
          ADD KEY ix_das_owner_acn     (owner_acn)
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE da_summaries
          DROP KEY ix_das_applicant_acn,
          DROP KEY ix_das_owner_acn,
          DROP COLUMN applicant_acn,
          DROP COLUMN applicant_abn,
          DROP COLUMN applicant_entity_type,
          DROP COLUMN applicant_agent_name,
          DROP COLUMN owner_acn,
          DROP COLUMN owner_abn,
          DROP COLUMN owner_entity_type
    """)
    op.execute("""
        ALTER TABLE da_doc_summaries
          DROP COLUMN applicant_acn,
          DROP COLUMN applicant_abn,
          DROP COLUMN applicant_entity_type,
          DROP COLUMN applicant_agent_name,
          DROP COLUMN owner_acn,
          DROP COLUMN owner_abn,
          DROP COLUMN owner_entity_type
    """)
