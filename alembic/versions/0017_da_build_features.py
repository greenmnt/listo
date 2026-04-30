"""da_build_features: per-chunk physical/cost extraction (build-features lane)

One row per (application_id, document_id, prompt_version, template_key,
chunk_index). Used by the new `listo da features` runner; aggregated
into per-DA scalars by `aggregate.py` (next migration adds the
rolled-up columns to da_summaries).

Revision ID: 0017
Revises: 0016
Create Date: 2026-04-30
"""
from __future__ import annotations

from alembic import op


revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE da_build_features (
          id                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          application_id        BIGINT UNSIGNED NOT NULL,
          document_id           BIGINT UNSIGNED NOT NULL,
          doc_type              VARCHAR(120) NULL,
          prompt_version        VARCHAR(20)  NOT NULL,
          template_key          VARCHAR(40)  NOT NULL,
          model                 VARCHAR(80)  NOT NULL,
          chunk_index           INT UNSIGNED NOT NULL,
          page_start            INT UNSIGNED NOT NULL,
          page_end              INT UNSIGNED NOT NULL,
          extracted_at          DATETIME(3)  NOT NULL,
          extraction_method     VARCHAR(20)  NOT NULL,
          text_chars            INT UNSIGNED NULL,

          gfa_m2                INT          NULL,
          site_area_m2          INT          NULL,
          internal_area_m2      INT          NULL,
          external_area_m2      INT          NULL,
          levels                TINYINT      NULL,
          has_basement          TINYINT(1)   NULL,
          garage_spaces         TINYINT      NULL,
          bedrooms              SMALLINT     NULL,
          bathrooms             SMALLINT     NULL,

          materials_walls       VARCHAR(300) NULL,
          materials_roof        VARCHAR(200) NULL,
          materials_floor       VARCHAR(200) NULL,
          fittings_quality      VARCHAR(20)  NULL,
          fittings_notes        VARCHAR(400) NULL,

          landscaping_summary   VARCHAR(400) NULL,
          plant_species_json    JSON         NULL,
          has_pool              TINYINT(1)   NULL,

          confidence            VARCHAR(10)  NULL,
          notes                 VARCHAR(300) NULL,
          raw_response_json     JSON         NOT NULL,

          PRIMARY KEY (id),
          UNIQUE KEY uq_dbf_chunk (document_id, prompt_version, template_key, chunk_index),
          KEY ix_dbf_app (application_id),
          KEY ix_dbf_doc (document_id),
          KEY ix_dbf_pv (prompt_version),
          CONSTRAINT fk_dbf_app FOREIGN KEY (application_id)
            REFERENCES council_applications(id) ON DELETE CASCADE,
          CONSTRAINT fk_dbf_doc FOREIGN KEY (document_id)
            REFERENCES council_application_documents(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS da_build_features")
