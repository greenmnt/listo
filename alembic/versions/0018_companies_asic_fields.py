"""ASIC View-Details enrichment columns on `companies`.

The DA summariser populates `companies` from LLM-extracted entity
fields. This migration adds the fields we get back from ASIC's
public Registry Search 'View Details' page so a single `companies`
row can hold both the in-DA mention and the authoritative registry
snapshot.

Revision ID: 0018
Revises: 0017
Create Date: 2026-05-01
"""
from __future__ import annotations

from alembic import op


revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE companies
          ADD COLUMN asic_status            VARCHAR(20)  NULL AFTER entity_type,
          ADD COLUMN asic_company_type      VARCHAR(120) NULL AFTER asic_status,
          ADD COLUMN asic_locality          VARCHAR(120) NULL AFTER asic_company_type,
          ADD COLUMN asic_regulator         VARCHAR(80)  NULL AFTER asic_locality,
          ADD COLUMN asic_registration_date DATE         NULL AFTER asic_regulator,
          ADD COLUMN asic_next_review_date  DATE         NULL AFTER asic_registration_date,
          ADD COLUMN asic_fetched_at        DATETIME(3)  NULL AFTER asic_next_review_date,
          ADD KEY ix_co_asic_status   (asic_status),
          ADD KEY ix_co_asic_locality (asic_locality)
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE companies
          DROP KEY ix_co_asic_status,
          DROP KEY ix_co_asic_locality,
          DROP COLUMN asic_status,
          DROP COLUMN asic_company_type,
          DROP COLUMN asic_locality,
          DROP COLUMN asic_regulator,
          DROP COLUMN asic_registration_date,
          DROP COLUMN asic_next_review_date,
          DROP COLUMN asic_fetched_at
    """)
