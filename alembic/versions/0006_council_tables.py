"""council_applications, council_application_documents, council_requests
   plus loosen raw_pages/crawl_runs source ENUMs to VARCHAR

Drops dev_applications, da_flags, da_documents (replaced by the new
council_* tables; existing 94 doc rows are tied to the old shape and not
worth migrating — re-scrape will repopulate via content_hash dedup).

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-29

"""
from __future__ import annotations

from alembic import op


revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop in FK-dependency order: flags → documents → applications.
    op.execute("DROP TABLE IF EXISTS da_flags")
    op.execute("DROP TABLE IF EXISTS da_documents")
    op.execute("DROP TABLE IF EXISTS dev_applications")

    # raw_pages.source was ENUM('realestate','domain'). Council pages need
    # values like 'council_cogc' / 'council_newcastle'. ENUM → VARCHAR(40)
    # so new councils can be added without DDL. Existing rows preserved.
    op.execute("ALTER TABLE raw_pages MODIFY source VARCHAR(40) NOT NULL")
    op.execute("ALTER TABLE raw_pages MODIFY page_type VARCHAR(40) NOT NULL")
    op.execute("ALTER TABLE crawl_runs MODIFY source VARCHAR(40) NOT NULL")
    op.execute("ALTER TABLE crawl_runs MODIFY page_type VARCHAR(40) NOT NULL")

    # One row per DA across every council. Two JSON blobs preserve the raw
    # listing-row tags + raw detail-page label/value pairs as fetched, so
    # the structured columns can be re-derived without re-scraping if our
    # extraction logic improves.
    op.execute("""
        CREATE TABLE council_applications (
          id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          council_slug        VARCHAR(40)  NOT NULL,             -- 'cogc' / 'newcastle'
          vendor              VARCHAR(40)  NOT NULL,             -- 'infor_epathway' / 'techone_etrack' / 'techone_t1cloud'
          application_id      VARCHAR(60)  NOT NULL,             -- council's own number, e.g. 'MCU/2025/64' or 'DA2024/0123'
          application_url     VARCHAR(1024) NULL,                -- canonical detail URL
          type_code           VARCHAR(8)   NULL,                 -- 'MCU' / 'OPV' / 'ROL' / 'DA' …
          application_type    VARCHAR(120) NULL,                 -- 'Material Change of Use'
          description         TEXT         NULL,
          approved_units      INT          NULL,                 -- regex-extracted
          status              VARCHAR(60)  NULL,                 -- 'Lodged' / 'Under Assessment' / 'Approved' / 'Withdrawn' …
          decision_outcome    VARCHAR(60)  NULL,                 -- 'Approved' / 'Refused' / 'Withdrawn'
          decision_authority  VARCHAR(120) NULL,
          lodged_date         DATE         NULL,
          decision_date       DATE         NULL,
          n_submissions       INT          NULL,
          conditions_count    INT          NULL,
          applicant_name      VARCHAR(255) NULL,
          builder_name        VARCHAR(255) NULL,
          architect_name      VARCHAR(255) NULL,
          owner_name          VARCHAR(255) NULL,
          internal_property_id VARCHAR(60) NULL,                 -- council's own lot key (PN12345 etc.)
          lot_on_plan         VARCHAR(120) NULL,
          raw_address         VARCHAR(500) NULL,
          street_address      VARCHAR(255) NULL,
          suburb              VARCHAR(120) NULL,
          postcode            VARCHAR(4)   NULL,
          state               VARCHAR(3)   NULL,
          match_key           VARCHAR(255) NULL,                 -- normalized address join key (joins to future properties.match_key)
          raw_listing_row     JSON         NULL,                 -- every column from the search-results table
          raw_detail_fields   JSON         NULL,                 -- every label/value from the detail page
          list_first_seen_at  DATETIME(3)  NOT NULL,
          detail_fetched_at   DATETIME(3)  NULL,
          docs_fetched_at     DATETIME(3)  NULL,
          last_seen_at        DATETIME(3)  NOT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_council_app (council_slug, application_id),
          KEY ix_ca_match_key (match_key),
          KEY ix_ca_internal_property (internal_property_id),
          KEY ix_ca_lodged (council_slug, lodged_date),
          KEY ix_ca_type (type_code, lodged_date),
          KEY ix_ca_suburb (suburb, postcode),
          KEY ix_ca_pending_detail (detail_fetched_at, council_slug),
          KEY ix_ca_pending_docs (docs_fetched_at, council_slug)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # Per-document row. doc_oid = the council/portal-stable doc id when one
    # exists (ePathway 'A43780294'); content_hash dedups across re-runs.
    op.execute("""
        CREATE TABLE council_application_documents (
          id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          application_id    BIGINT UNSIGNED NOT NULL,
          doc_oid           VARCHAR(60)  NULL,                   -- portal-stable per-doc id
          doc_type          VARCHAR(120) NULL,
          title             VARCHAR(500) NULL,
          source_url        VARCHAR(1024) NULL,
          file_path         VARCHAR(500) NULL,
          content_hash      BINARY(32)   NULL,
          mime_type         VARCHAR(80)  NULL,
          file_size         BIGINT       NULL,
          page_count        INT          NULL,
          extracted_text    LONGTEXT     NULL,
          extraction_notes  TEXT         NULL,
          downloaded_at     DATETIME(3)  NULL,
          PRIMARY KEY (id),
          KEY ix_cad_app (application_id),
          KEY ix_cad_type (doc_type),
          KEY ix_cad_content_hash (content_hash),
          UNIQUE KEY uq_cad_app_oid (application_id, doc_oid),
          CONSTRAINT fk_cad_app FOREIGN KEY (application_id)
            REFERENCES council_applications(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # Every HTTP request we make to a council portal: list pages, detail
    # pages, document downloads, retries, failures. raw_pages stores
    # successful HTML bodies; this table is the full attempt log including
    # failures, redirects, and binary downloads. raw_page_id / document_id
    # join to the stored payload when one exists.
    op.execute("""
        CREATE TABLE council_requests (
          id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          council_slug      VARCHAR(40)  NOT NULL,
          vendor            VARCHAR(40)  NOT NULL,
          purpose           VARCHAR(40)  NOT NULL,                -- 'list' / 'detail' / 'docs_index' / 'doc_download'
          method            VARCHAR(8)   NOT NULL,
          url               VARCHAR(2048) NOT NULL,
          url_hash          BINARY(32)   NOT NULL,
          http_status       SMALLINT     NULL,                    -- NULL on connection error
          elapsed_ms        INT          NULL,
          bytes_received    BIGINT       NULL,
          content_hash      BINARY(32)   NULL,
          attempt_index     SMALLINT     NOT NULL DEFAULT 1,
          raw_page_id       BIGINT UNSIGNED NULL,
          document_id       BIGINT UNSIGNED NULL,
          application_id    BIGINT UNSIGNED NULL,
          error             TEXT         NULL,
          started_at        DATETIME(3)  NOT NULL,
          PRIMARY KEY (id),
          KEY ix_creq_started (council_slug, started_at),
          KEY ix_creq_url_hash (url_hash),
          KEY ix_creq_purpose (purpose, started_at),
          KEY ix_creq_app (application_id),
          CONSTRAINT fk_creq_raw FOREIGN KEY (raw_page_id)
            REFERENCES raw_pages(id) ON DELETE SET NULL,
          CONSTRAINT fk_creq_doc FOREIGN KEY (document_id)
            REFERENCES council_application_documents(id) ON DELETE SET NULL,
          CONSTRAINT fk_creq_app FOREIGN KEY (application_id)
            REFERENCES council_applications(id) ON DELETE SET NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS council_requests")
    op.execute("DROP TABLE IF EXISTS council_application_documents")
    op.execute("DROP TABLE IF EXISTS council_applications")
    # Note: ENUMs not restored on downgrade — the new VARCHAR is a strict
    # superset of the old ENUM domain, so existing rows remain valid.
