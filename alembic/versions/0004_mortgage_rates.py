"""mortgage_rates table for RBA F5 ingest

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-28

"""
from __future__ import annotations

from alembic import op


revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE mortgage_rates (
          id           INT UNSIGNED NOT NULL AUTO_INCREMENT,
          series_id    VARCHAR(20)  NOT NULL,         -- RBA stable id e.g. 'FILRHLBVS'
          series_label VARCHAR(255) NULL,             -- human readable from CSV header
          month        DATE         NOT NULL,         -- end-of-month from RBA CSV
          rate_pct     DECIMAL(6,3) NOT NULL,         -- per cent per annum
          source       VARCHAR(20)  NOT NULL DEFAULT 'rba_f5',
          fetched_at   DATETIME(3)  NOT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_rate_series_month (series_id, month),
          KEY ix_rate_month (month),
          KEY ix_rate_series_month_rev (series_id, month DESC)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS mortgage_rates")
