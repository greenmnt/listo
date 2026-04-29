"""council_scrape_windows: track each (council, backend, date-window) attempt

Lets us answer 'have we finished Feb 2025 for COGC?' without inferring
from per-app timestamps. One row per scrape attempt; updated to
finished/failed when the walk ends.

Revision ID: 0008
Revises: 0007
Create Date: 2026-04-30

"""
from __future__ import annotations

from alembic import op


revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE council_scrape_windows (
          id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          council_slug      VARCHAR(40)  NOT NULL,
          vendor            VARCHAR(40)  NOT NULL,
          backend_name      VARCHAR(60)  NOT NULL,             -- e.g. 'cogc_post_2017' / 'cogc_post_2017_http'
          date_from         DATE         NOT NULL,
          date_to           DATE         NOT NULL,
          started_at        DATETIME(3)  NOT NULL,
          finished_at       DATETIME(3)  NULL,
          status            VARCHAR(20)  NOT NULL,             -- 'running' / 'completed' / 'failed' / 'aborted'
          pages_walked      INT          NOT NULL DEFAULT 0,
          apps_yielded      INT          NOT NULL DEFAULT 0,
          apps_with_docs    INT          NOT NULL DEFAULT 0,
          files_downloaded  INT          NOT NULL DEFAULT 0,
          error             TEXT         NULL,
          PRIMARY KEY (id),
          KEY ix_csw_council_window (council_slug, date_from, date_to),
          KEY ix_csw_status (council_slug, status, finished_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS council_scrape_windows")
