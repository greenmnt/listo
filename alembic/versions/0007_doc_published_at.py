"""council_application_documents.published_at

Records the council's "Date published" timestamp for each document.
Sorting on this gives chronological submission order (application form
first, decision notice last) and lets the scraper pick which files to
download (typically first + last) without losing the full inventory.

Revision ID: 0007
Revises: 0006
Create Date: 2026-04-29

"""
from __future__ import annotations

from alembic import op


revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE council_application_documents "
        "ADD COLUMN published_at DATETIME(3) NULL AFTER downloaded_at, "
        "ADD KEY ix_cad_published_at (application_id, published_at)"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE council_application_documents "
        "DROP KEY ix_cad_published_at, "
        "DROP COLUMN published_at"
    )
