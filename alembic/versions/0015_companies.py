"""companies table + entity FKs on da_summaries

We parse names like:

  'Great Southern Men Developments Pty Ltd (A.C.N 652 330 928) c/- HPC Planning'

into structured pieces — the primary entity, its ACN/ABN, and the
'care of' agent (often a town planner lodging on behalf of the
developer). The ACN is a stable identifier we can use to filter
projects by developer / builder later, even when the display name
varies between DAs.

Schema:
- `companies` — one row per (ACN, norm_name) pair we've ever seen.
  Same ACN with a slightly different display name → same row.
- `da_summaries` gets nullable company-FK columns for applicant,
  applicant_agent (the c/- consultant), builder, architect, owner.
  The legacy text columns (`applicant_name` etc.) stay alongside the
  FKs as the raw string from the LLM.

Revision ID: 0015
Revises: 0014
Create Date: 2026-04-30

"""
from __future__ import annotations

from alembic import op


revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE companies (
          id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          acn             CHAR(9)       NULL,                 -- 9-digit Australian Company Number
          abn             VARCHAR(11)   NULL,                 -- 11-digit Australian Business Number
          display_name    VARCHAR(255)  NOT NULL,             -- as-encountered, with Pty Ltd preserved
          norm_name       VARCHAR(255)  NOT NULL,             -- lowercase, suffix-stripped, for fuzzy match
          entity_type     VARCHAR(20)   NOT NULL DEFAULT 'unknown',  -- 'company' | 'trust' | 'individual' | 'unknown'
          first_seen_at   DATETIME(3)   NOT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_co_acn  (acn),
          UNIQUE KEY uq_co_abn  (abn),
          KEY ix_co_norm (norm_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # FKs on da_summaries. The text columns stay (raw from LLM) so we can
    # always re-parse without losing the original answer.
    op.execute("""
        ALTER TABLE da_summaries
          ADD COLUMN applicant_company_id        BIGINT UNSIGNED NULL,
          ADD COLUMN applicant_agent_company_id  BIGINT UNSIGNED NULL,
          ADD COLUMN builder_company_id          BIGINT UNSIGNED NULL,
          ADD COLUMN architect_company_id        BIGINT UNSIGNED NULL,
          ADD COLUMN owner_company_id            BIGINT UNSIGNED NULL,
          ADD CONSTRAINT fk_das_applicant_co
            FOREIGN KEY (applicant_company_id) REFERENCES companies(id) ON DELETE SET NULL,
          ADD CONSTRAINT fk_das_agent_co
            FOREIGN KEY (applicant_agent_company_id) REFERENCES companies(id) ON DELETE SET NULL,
          ADD CONSTRAINT fk_das_builder_co
            FOREIGN KEY (builder_company_id) REFERENCES companies(id) ON DELETE SET NULL,
          ADD CONSTRAINT fk_das_architect_co
            FOREIGN KEY (architect_company_id) REFERENCES companies(id) ON DELETE SET NULL,
          ADD CONSTRAINT fk_das_owner_co
            FOREIGN KEY (owner_company_id) REFERENCES companies(id) ON DELETE SET NULL,
          ADD KEY ix_das_applicant_co (applicant_company_id),
          ADD KEY ix_das_builder_co   (builder_company_id),
          ADD KEY ix_das_architect_co (architect_company_id)
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE da_summaries
          DROP FOREIGN KEY fk_das_applicant_co,
          DROP FOREIGN KEY fk_das_agent_co,
          DROP FOREIGN KEY fk_das_builder_co,
          DROP FOREIGN KEY fk_das_architect_co,
          DROP FOREIGN KEY fk_das_owner_co,
          DROP KEY ix_das_applicant_co,
          DROP KEY ix_das_builder_co,
          DROP KEY ix_das_architect_co,
          DROP COLUMN applicant_company_id,
          DROP COLUMN applicant_agent_company_id,
          DROP COLUMN builder_company_id,
          DROP COLUMN architect_company_id,
          DROP COLUMN owner_company_id
    """)
    op.execute("DROP TABLE IF EXISTS companies")
