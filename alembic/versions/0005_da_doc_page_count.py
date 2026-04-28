"""add page_count to da_documents (for DA complexity score)

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-28

"""
from __future__ import annotations

from alembic import op


revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE da_documents ADD COLUMN page_count INT NULL AFTER file_size")
    op.execute("ALTER TABLE da_documents ADD COLUMN doc_oid VARCHAR(40) NULL AFTER doc_type")
    op.execute("CREATE INDEX ix_doc_oid ON da_documents (doc_oid)")


def downgrade() -> None:
    op.execute("DROP INDEX ix_doc_oid ON da_documents")
    op.execute("ALTER TABLE da_documents DROP COLUMN doc_oid")
    op.execute("ALTER TABLE da_documents DROP COLUMN page_count")
