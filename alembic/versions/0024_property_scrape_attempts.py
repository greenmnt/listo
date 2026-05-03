"""property_scrape_attempts: track every Domain/REA direct-slug attempt

Without this, the dedup logic in scrape-batch can't tell "we never tried"
apart from "we tried and Domain/REA genuinely had nothing for this
address". Properties Domain doesn't index (mid-redev, vacant lots,
coverage gaps) keep getting re-scraped on every run, burning bandwidth
and Google quota for no payoff.

Schema is keyed on (source, display_address) — one attempt row per
address per source. Subsequent attempts upsert in place.

`result` values:
  'found'      → parser succeeded, data row exists in domain_/realestate_properties
  'not_found'  → server returned 404 (or a non-2xx that maps to "no such property")
  'error'      → transient failure (network, parse error, Kasada interstitial, etc.)
"""

import sqlalchemy as sa
from alembic import op


revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "property_scrape_attempts",
        sa.Column("id", sa.BigInteger().with_variant(
            sa.dialects.mysql.BIGINT(unsigned=True), "mysql"
        ), primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(20), nullable=False),
        sa.Column("display_address", sa.String(255), nullable=False),
        sa.Column("url", sa.String(1024), nullable=False),
        sa.Column("http_status", sa.SmallInteger(), nullable=True),
        sa.Column("result", sa.String(20), nullable=False),
        sa.Column("error_message", sa.String(500), nullable=True),
        sa.Column("attempted_at", sa.DateTime(fsp=3), nullable=False,
                  server_default=sa.func.now(3)),
        sa.UniqueConstraint(
            "source", "display_address",
            name="uq_property_scrape_attempts_source_addr",
        ),
        sa.Index(
            "ix_property_scrape_attempts_result",
            "source", "result",
        ),
    )


def downgrade() -> None:
    op.drop_table("property_scrape_attempts")
