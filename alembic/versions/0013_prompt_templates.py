"""prompt_templates + audit columns on da_doc_summaries

Stores the exact system + user template text that was active for each
`(prompt_version, template_key)` pair the first time it was used.
Templates are write-once per (version, key) — to change a template, bump
the version. This means we always know what prompt produced an output,
even years later.

Plus two new columns on `da_doc_summaries`:
- `template_key`: which prompt template was used ('da_form_1', 'decision_notice', …)
- `text_sha256`: hash of the input text the LLM saw, so we can verify
  reproducibility against the source PDF.

Revision ID: 0013
Revises: 0012
Create Date: 2026-04-30

"""
from __future__ import annotations

from alembic import op


revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE prompt_templates (
          id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          prompt_version  VARCHAR(20)  NOT NULL,
          template_key    VARCHAR(40)  NOT NULL,    -- 'da_form_1' | 'decision_notice' | 'specialist' | 'plans' | 'supporting' | 'generic'
          system_prompt   MEDIUMTEXT   NOT NULL,
          user_template   MEDIUMTEXT   NOT NULL,    -- includes literal {text} and {app_id} placeholders
          notes           TEXT         NULL,        -- e.g. 'covers DA Form 1 + Amended DA Form 1'
          first_used_at   DATETIME(3)  NOT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_pt (prompt_version, template_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    op.execute("""
        ALTER TABLE da_doc_summaries
          ADD COLUMN template_key VARCHAR(40) NULL AFTER prompt_version,
          ADD COLUMN text_sha256  BINARY(32)  NULL AFTER text_chars,
          ADD KEY ix_dds_template (prompt_version, template_key)
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE da_doc_summaries DROP KEY ix_dds_template")
    op.execute("ALTER TABLE da_doc_summaries DROP COLUMN text_sha256")
    op.execute("ALTER TABLE da_doc_summaries DROP COLUMN template_key")
    op.execute("DROP TABLE IF EXISTS prompt_templates")
