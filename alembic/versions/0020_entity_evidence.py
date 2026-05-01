"""entity_evidence: ML-ready training data for entity extraction

Every regex emission writes a row here with full provenance (source text +
character span + page geometry). This is the dataset we'll later use to
train layout-aware NER (LayoutLMv3 / ModernBERT) once we've got
~500 verified rows.

The table preserves rejected / superseded predictions across regex
versions so we can evaluate iteration impact and use false positives as
hard-negatives during training.

Revision ID: 0020
Revises: 0019
Create Date: 2026-05-01
"""
from __future__ import annotations

from alembic import op


revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE entity_evidence (
          id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          application_id  BIGINT UNSIGNED NOT NULL,
          source_doc_id   BIGINT UNSIGNED NULL,

          extractor       VARCHAR(40) NOT NULL,
            -- 'cogc_correspondence_regex_v1' | 'plans_title_regex_v1' | …
            -- bump the suffix when a regex behaviour changes so old
            -- predictions stay queryable for comparison.

          -- Provenance: the actual ML input.
          -- For correspondence: the cached extracted_text the regex saw.
          -- For plans: the single page's text where the match landed.
          source_text     MEDIUMTEXT NOT NULL,
          span_start      INT NOT NULL,
          span_end        INT NOT NULL,

          -- What the extractor emitted
          candidate_name  VARCHAR(255) NOT NULL,
          candidate_role  VARCHAR(30) NULL,
          confidence      VARCHAR(10) NULL,

          -- Geometry — populated when we have layout-aware extraction.
          -- Shape:
          --   { "page_index": int, "page_w": float, "page_h": float,
          --     "bbox": [x0,y0,x1,y1], "font": str, "size": float,
          --     "flags": int, "rotation": int }
          layout          JSON NULL,

          -- Human / LLM / external verification (null until reviewed).
          status          ENUM('predicted','verified','rejected','corrected')
                          NOT NULL DEFAULT 'predicted',
          truth_name      VARCHAR(255) NULL,
          truth_role      VARCHAR(30) NULL,
          verifier        VARCHAR(80) NULL,
            -- 'human:<user>' | 'llm:claude-sonnet-4.6' | 'asic_lookup' | …
          verified_at     DATETIME(3) NULL,
          notes           TEXT NULL,

          created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

          PRIMARY KEY (id),
          -- Idempotency: same extractor+doc+span overwrites the prediction
          -- (so re-runs don't duplicate). Different extractor versions get
          -- separate rows on purpose so we can compare them.
          UNIQUE KEY uq_ee_dedup (extractor, source_doc_id, span_start, span_end),
          KEY ix_ee_app    (application_id),
          KEY ix_ee_doc    (source_doc_id),
          KEY ix_ee_status (status, extractor),
          KEY ix_ee_extractor (extractor, created_at),
          CONSTRAINT fk_ee_app
            FOREIGN KEY (application_id) REFERENCES council_applications(id) ON DELETE CASCADE,
          CONSTRAINT fk_ee_doc
            FOREIGN KEY (source_doc_id)  REFERENCES council_application_documents(id) ON DELETE SET NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS entity_evidence")
