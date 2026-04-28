"""add property_type to properties

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-28

"""
from __future__ import annotations

from alembic import op


revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE properties ADD COLUMN property_type VARCHAR(40) NULL AFTER state")


def downgrade() -> None:
    op.execute("ALTER TABLE properties DROP COLUMN property_type")
