"""dev_applications, da_documents, da_flags

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-28

"""
from __future__ import annotations

from alembic import op


revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE dev_applications (
          id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          council_slug      VARCHAR(40)  NOT NULL,
          application_id    VARCHAR(40)  NOT NULL,            -- 'MCU/2017/973'
          application_type  VARCHAR(80)  NULL,                -- 'Material Change of Use (Single Uses)'
          type_code         VARCHAR(8)   NULL,                -- 'MCU' / 'OPW' / 'BWK' / 'ROL'
          description       TEXT         NULL,
          approved_units    INT          NULL,                -- regex-extracted from description
          internal_property_id VARCHAR(40) NULL,              -- council 'PN<digits>' — stable per-lot key
          lot_on_plan       VARCHAR(80)  NULL,                -- 'Lot 21 M73854'
          raw_address       VARCHAR(255) NULL,                -- as printed by council
          match_key         VARCHAR(160) NULL,                -- normalized address join key to properties
          suburb            VARCHAR(80)  NULL,
          postcode          CHAR(4)      NULL,
          state             CHAR(3)      NULL,
          status            VARCHAR(40)  NULL,
          decision_outcome  VARCHAR(40)  NULL,                -- 'Approved' / 'Refused' / 'Withdrawn'
          decision_authority VARCHAR(80) NULL,
          lodged_date       DATE         NULL,
          decision_date     DATE         NULL,
          n_submissions     INT          NULL,                -- third-party objections received
          conditions_count  INT          NULL,                -- decision conditions (>20 = headache)
          applicant_name    VARCHAR(160) NULL,
          builder_name      VARCHAR(160) NULL,
          architect_name    VARCHAR(160) NULL,
          source_url        VARCHAR(500) NULL,
          first_seen_at     DATETIME(3)  NOT NULL,
          last_seen_at      DATETIME(3)  NOT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_da_council_app (council_slug, application_id),
          KEY ix_da_match_key (match_key),
          KEY ix_da_internal_property (internal_property_id),
          KEY ix_da_lodged (council_slug, lodged_date),
          KEY ix_da_type (type_code, lodged_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    op.execute("""
        CREATE TABLE da_documents (
          id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          application_id    BIGINT UNSIGNED NOT NULL,         -- FK to dev_applications.id
          doc_type          VARCHAR(80)  NULL,                -- 'Decision Notice' / 'DA Plans' / 'Assessment Report'
          title             VARCHAR(255) NULL,
          source_url        VARCHAR(500) NULL,
          file_path         VARCHAR(500) NULL,                -- where we stored a local copy
          content_hash      BINARY(32)   NULL,                -- sha256 of bytes
          mime_type         VARCHAR(60)  NULL,
          file_size         BIGINT       NULL,
          extracted_text    LONGTEXT     NULL,                -- output of pdf-to-text
          extraction_notes  TEXT         NULL,                -- 'used OCR' / 'text layer' / errors
          downloaded_at     DATETIME(3)  NULL,
          PRIMARY KEY (id),
          KEY ix_doc_app (application_id),
          KEY ix_doc_type (doc_type),
          CONSTRAINT fk_doc_app FOREIGN KEY (application_id) REFERENCES dev_applications(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    op.execute("""
        CREATE TABLE da_flags (
          id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          application_id    BIGINT UNSIGNED NOT NULL,
          flag_kind         VARCHAR(40)  NOT NULL,            -- 'overlay_bushfire' / 'overlay_flood' / 'submissions' / 'long_approval' / 'high_conditions' / 'referred_state'
          severity          ENUM('info','warn','high') NOT NULL DEFAULT 'warn',
          detail            VARCHAR(255) NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_flag (application_id, flag_kind),
          KEY ix_flag_kind (flag_kind),
          CONSTRAINT fk_flag_app FOREIGN KEY (application_id) REFERENCES dev_applications(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS da_flags")
    op.execute("DROP TABLE IF EXISTS da_documents")
    op.execute("DROP TABLE IF EXISTS dev_applications")
