"""da_summaries: per-doc + per-DA Ollama-extracted facts + business links

Three new tables:
- `da_doc_summaries`: per-document LLM extraction (one row per
  (document_id, prompt_version)). Tier-1 = first/last doc; tier-2 =
  escalated docs (Specialist Reports, Plans, Amended DA Form 1, etc.).
- `da_summaries`: aggregated per-DA view + DA-process stats. Drives the
  frontend display + carries a `status` enum for the escalation gate.
- `business_links`: builder/architect website URLs discovered via Google
  search through the existing CDP-Chrome rig.

Plus a `summarised_at` helper column on `council_applications` for
resume / pending-lookup.

Revision ID: 0012
Revises: 0011
Create Date: 2026-04-30

"""
from __future__ import annotations

from alembic import op


revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE da_doc_summaries (
          id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          document_id         BIGINT UNSIGNED NOT NULL,
          application_id      BIGINT UNSIGNED NOT NULL,
          doc_type            VARCHAR(120) NULL,
          doc_position        VARCHAR(10)  NOT NULL,            -- 'first' | 'last' | 'tier2'
          tier                TINYINT UNSIGNED NOT NULL DEFAULT 1,
          model               VARCHAR(80)  NOT NULL,
          prompt_version      VARCHAR(20)  NOT NULL,
          summarised_at       DATETIME(3)  NOT NULL,
          text_chars          INT UNSIGNED NULL,
          pages_used          VARCHAR(60)  NULL,
          extraction_method   VARCHAR(20)  NOT NULL DEFAULT 'pymupdf',
          extraction_notes    TEXT         NULL,
          applicant_name      VARCHAR(255) NULL,
          builder_name        VARCHAR(255) NULL,
          architect_name      VARCHAR(255) NULL,
          owner_name          VARCHAR(255) NULL,
          dwelling_count      SMALLINT     NULL,
          dwelling_kind       VARCHAR(40)  NULL,
          project_description TEXT         NULL,
          lot_on_plan         VARCHAR(120) NULL,
          street_address      VARCHAR(255) NULL,
          confidence          VARCHAR(10)  NULL,                -- 'high' | 'medium' | 'low'
          raw_response_json   JSON         NOT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_dds (document_id, prompt_version),
          KEY ix_dds_app (application_id),
          KEY ix_dds_doctype (doc_type),
          CONSTRAINT fk_dds_doc FOREIGN KEY (document_id)
            REFERENCES council_application_documents(id) ON DELETE CASCADE,
          CONSTRAINT fk_dds_app FOREIGN KEY (application_id)
            REFERENCES council_applications(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    op.execute("""
        CREATE TABLE da_summaries (
          application_id        BIGINT UNSIGNED NOT NULL,
          applicant_name        VARCHAR(255) NULL,
          builder_name          VARCHAR(255) NULL,
          architect_name        VARCHAR(255) NULL,
          owner_name            VARCHAR(255) NULL,
          dwelling_count        SMALLINT     NULL,
          dwelling_kind         VARCHAR(40)  NULL,
          project_description   TEXT         NULL,
          lot_on_plan           VARCHAR(120) NULL,
          street_address        VARCHAR(255) NULL,
          source_doc_ids_json   JSON         NULL,
          n_docs                INT UNSIGNED NOT NULL DEFAULT 0,
          n_docs_downloaded     INT UNSIGNED NOT NULL DEFAULT 0,
          total_bytes           BIGINT UNSIGNED NOT NULL DEFAULT 0,
          total_pages           INT UNSIGNED NOT NULL DEFAULT 0,
          n_information_requests INT UNSIGNED NOT NULL DEFAULT 0,
          n_amendments          INT UNSIGNED NOT NULL DEFAULT 0,
          n_specialist_reports  INT UNSIGNED NOT NULL DEFAULT 0,
          days_lodge_to_decide  INT          NULL,
          first_doc_at          DATETIME(3)  NULL,
          last_doc_at           DATETIME(3)  NULL,
          n_docs_summarised     INT UNSIGNED NOT NULL DEFAULT 0,
          status                VARCHAR(20)  NOT NULL DEFAULT 'incomplete',
          aggregated_at         DATETIME(3)  NOT NULL,
          PRIMARY KEY (application_id),
          KEY ix_das_status (status),
          CONSTRAINT fk_das_app FOREIGN KEY (application_id)
            REFERENCES council_applications(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    op.execute("""
        CREATE TABLE business_links (
          id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          business_name       VARCHAR(255) NOT NULL,
          display_name        VARCHAR(255) NOT NULL,
          business_role       VARCHAR(20)  NOT NULL,
          url                 VARCHAR(1024) NULL,
          url_kind            VARCHAR(20)  NULL,
          search_query        VARCHAR(255) NULL,
          search_engine       VARCHAR(20)  NOT NULL DEFAULT 'google',
          confidence          VARCHAR(10)  NULL,
          candidates_json     JSON         NULL,
          discovered_at       DATETIME(3)  NOT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_bl (business_name, business_role),
          KEY ix_bl_role (business_role)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    op.execute("""
        ALTER TABLE council_applications
          ADD COLUMN summarised_at DATETIME(3) NULL,
          ADD KEY ix_ca_pending_summary (summarised_at)
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE council_applications DROP KEY ix_ca_pending_summary")
    op.execute("ALTER TABLE council_applications DROP COLUMN summarised_at")
    op.execute("DROP TABLE IF EXISTS business_links")
    op.execute("DROP TABLE IF EXISTS da_summaries")
    op.execute("DROP TABLE IF EXISTS da_doc_summaries")
