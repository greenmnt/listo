"""document_features: cached per-PDF characteristics + treatment class

We classify every downloaded document by features observable from the
PDF itself (page count, text volume, AcroForm widgets, producer
metadata) rather than relying on the council-side `doc_type` label,
which is noisy ('Forms' might or might not be an AcroForm; 'Specialist
Reports' might be a scanned image).

The `treatment` column is the routing key for the LLM pipeline:
- 'acroform_filled' → use da_form_1 template (widget block authoritative)
- 'narrative_long'  → specialist / decision_notice (by doc_type hint)
- 'narrative_short' → generic / decision_notice
- 'titleblock'      → plans template
- 'image_only'      → skip; no LLM
- 'unsupported'     → skip; not a PDF

Idempotent: one row per document_id; recomputed when --force is set.

Revision ID: 0014
Revises: 0013
Create Date: 2026-04-30

"""
from __future__ import annotations

from alembic import op


revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE document_features (
          document_id            BIGINT UNSIGNED NOT NULL,
          analyzed_at            DATETIME(3) NOT NULL,
          analyzer_version       VARCHAR(20) NOT NULL,    -- bump when classification rules change
          mime_type              VARCHAR(80) NULL,
          page_count             INT UNSIGNED NULL,
          total_text_chars       INT UNSIGNED NOT NULL DEFAULT 0,
          mean_chars_per_page    INT UNSIGNED NOT NULL DEFAULT 0,
          has_acroform           TINYINT(1)  NOT NULL DEFAULT 0,
          n_text_widgets         INT UNSIGNED NOT NULL DEFAULT 0,
          n_text_widgets_filled  INT UNSIGNED NOT NULL DEFAULT 0,
          n_checkbox_widgets     INT UNSIGNED NOT NULL DEFAULT 0,
          pdf_producer           VARCHAR(255) NULL,
          pdf_creator            VARCHAR(255) NULL,
          pdf_format             VARCHAR(40)  NULL,
          treatment              VARCHAR(40)  NOT NULL,
          extraction_notes       TEXT         NULL,
          PRIMARY KEY (document_id),
          KEY ix_df_treatment (treatment),
          CONSTRAINT fk_df_doc FOREIGN KEY (document_id)
            REFERENCES council_application_documents(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS document_features")
