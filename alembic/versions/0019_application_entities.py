"""application_entities: many-to-many DA ↔ companies with role + provenance

The five fixed FK columns on `da_summaries` (applicant/owner/agent/builder/
architect) can't represent multi-party owners (e.g. a couple), don't carry
provenance (which doc said so), and have no slot for weaker relations
(witnesses, body corps, referral agencies, neighbours).

This migration adds an explicit join table — one row per (application,
company, role, source_doc) — so a property co-owned by Peter Dawson AND
Noela Roberts gets two distinct `companies` rows linked to the same DA
with `role='owner'`. The existing FK columns on `da_summaries` stay as
denormalised "best guess" pointers.

Revision ID: 0019
Revises: 0018
Create Date: 2026-05-01
"""
from __future__ import annotations

from alembic import op


revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE application_entities (
          id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          application_id  BIGINT UNSIGNED NOT NULL,
          company_id      BIGINT UNSIGNED NOT NULL,
          role            VARCHAR(30)     NOT NULL,
            -- 'applicant' | 'owner' | 'agent' | 'builder' | 'architect'
            -- | 'witness' | 'referral_agency' | 'objector' | 'other'
          is_primary      TINYINT(1)      NOT NULL DEFAULT 0,
          source_doc_id   BIGINT UNSIGNED NULL,
            -- council_application_documents.id; NULL for synthesised rows
            -- (e.g. derived from the listing-row blob).
          source_field    VARCHAR(80)     NULL,
            -- e.g. 'recipient_block', 'applicant_name_field',
            -- 'owner_details_field', 'da_form_owner_consent'.
          extractor       VARCHAR(40)     NOT NULL,
            -- 'cogc_correspondence_regex' | 'da_summary_llm' | 'manual' | ...
          confidence      VARCHAR(10)     NULL,
            -- 'high' | 'medium' | 'low'
          extracted_at    DATETIME(3)     NOT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_ae_dedup (application_id, company_id, role, source_doc_id, extractor),
          KEY ix_ae_app  (application_id, role),
          KEY ix_ae_co   (company_id, role),
          KEY ix_ae_doc  (source_doc_id),
          CONSTRAINT fk_ae_app
            FOREIGN KEY (application_id) REFERENCES council_applications(id) ON DELETE CASCADE,
          CONSTRAINT fk_ae_co
            FOREIGN KEY (company_id)     REFERENCES companies(id)            ON DELETE CASCADE,
          CONSTRAINT fk_ae_doc
            FOREIGN KEY (source_doc_id)  REFERENCES council_application_documents(id) ON DELETE SET NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS application_entities")
