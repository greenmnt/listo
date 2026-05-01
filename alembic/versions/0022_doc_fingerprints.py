"""doc_fingerprints: identity tokens (URL / email / licence / ACN) on docs

Many architectural / draftsperson title blocks put their firm name only
in the **logo** (image) — text-extractable PyMuPDF output for those
pages contains the URL, email, and licence number but not the firm
name itself. We capture those tokens as fingerprints, then resolve
them to named entities by cross-referencing other docs in the same DA
(Supporting Documents, Specialist Reports, Decision Reports) where the
firm name appears spelled out.

Distinct from `entity_evidence` because the semantics differ:
  - entity_evidence: "we think this name plays this role" (prediction)
  - doc_fingerprints: "this token appears here" (signal, no identity)

Resolution writes back `resolved_company_id` to bind a fingerprint to
a `companies` row once the owner is known.

Revision ID: 0022
Revises: 0021
Create Date: 2026-05-01
"""
from __future__ import annotations

from alembic import op


revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE doc_fingerprints (
          id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          application_id  BIGINT UNSIGNED NOT NULL,
          source_doc_id   BIGINT UNSIGNED NOT NULL,

          fingerprint_kind ENUM(
            'url', 'email', 'phone',
            'qbcc', 'qbsa', 'bsa', 'vba', 'licence_other',
            'acn', 'abn'
          ) NOT NULL,
          raw_value        VARCHAR(500) NOT NULL,
          normalized_value VARCHAR(255) NOT NULL,

          span_start  INT NULL,
          span_end    INT NULL,
          page_index  INT NULL,
          layout      JSON NULL,

          resolved_company_id  BIGINT UNSIGNED NULL,
          resolved_via         VARCHAR(40) NULL,
          resolved_at          DATETIME(3) NULL,
          created_at           DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

          PRIMARY KEY (id),
          UNIQUE KEY uq_dfp (source_doc_id, fingerprint_kind, normalized_value),
          KEY ix_dfp_normalized (fingerprint_kind, normalized_value),
          KEY ix_dfp_app_kind   (application_id, fingerprint_kind),
          KEY ix_dfp_resolved   (resolved_company_id),
          CONSTRAINT fk_dfp_app
            FOREIGN KEY (application_id) REFERENCES council_applications(id) ON DELETE CASCADE,
          CONSTRAINT fk_dfp_doc
            FOREIGN KEY (source_doc_id)  REFERENCES council_application_documents(id) ON DELETE CASCADE,
          CONSTRAINT fk_dfp_resolved_co
            FOREIGN KEY (resolved_company_id) REFERENCES companies(id) ON DELETE SET NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS doc_fingerprints")
