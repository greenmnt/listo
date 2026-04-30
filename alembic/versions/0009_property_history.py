"""property_history: domain_properties + domain_sales

Per-source PDP snapshots and timeline events. Phase 1 covers Domain only
(plain HTTP, no Kasada). Realestate.com.au and property.com.au will get
sibling tables in a follow-up migration once the Kasada bypass fetcher
is reincarnated.

Raw HTML continues to live in `raw_pages` (source='domain_property',
page_type='pdp'). These tables hold the *parsed* per-source view.

Revision ID: 0009
Revises: 0008
Create Date: 2026-04-30

"""
from __future__ import annotations

from alembic import op


revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE domain_properties (
          id                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          raw_page_id           BIGINT UNSIGNED NOT NULL,
          property_id           BIGINT UNSIGNED NULL,           -- FK to properties (lazy match)

          -- Domain identifiers
          domain_property_id    VARCHAR(64)  NOT NULL,          -- 'EP-0835-KO'
          domain_apollo_id      VARCHAR(255) NOT NULL,          -- relay 'UHJvcGVydHk6...'
          url_slug              VARCHAR(255) NOT NULL,          -- '124-sunshine-parade-miami-qld-4220'
          url                   VARCHAR(1024) NOT NULL,

          -- Address (decomposed, mirrors the Apollo `address` shape)
          display_address       VARCHAR(255) NOT NULL,
          unit_number           VARCHAR(16)  NOT NULL DEFAULT '',
          street_number         VARCHAR(16)  NOT NULL DEFAULT '',
          street_name           VARCHAR(120) NOT NULL DEFAULT '',
          street_type           VARCHAR(20)  NOT NULL DEFAULT '',
          suburb                VARCHAR(80)  NOT NULL,
          postcode              CHAR(4)      NOT NULL,
          state                 CHAR(3)      NOT NULL,
          lat                   DECIMAL(9,6) NULL,
          lng                   DECIMAL(9,6) NULL,

          -- Cadastral identifiers — duplex DAs that became "1/124"-style
          -- subdivisions will populate these on the unit children.
          lot_number            VARCHAR(20)  NULL,
          plan_number           VARCHAR(20)  NULL,

          -- Property attributes from the Apollo Property record
          property_type         VARCHAR(40)  NULL,              -- 'Duplex' / 'House' / etc.
          bedrooms              TINYINT UNSIGNED NULL,
          bathrooms             TINYINT UNSIGNED NULL,
          parking_spaces        TINYINT UNSIGNED NULL,
          land_area_m2          INT UNSIGNED NULL,
          internal_area_m2      INT UNSIGNED NULL,

          -- Domain's own valuation + rental estimate
          valuation_low         INT UNSIGNED NULL,
          valuation_mid         INT UNSIGNED NULL,
          valuation_high        INT UNSIGNED NULL,
          valuation_confidence  VARCHAR(40)  NULL,
          valuation_date        DATE         NULL,
          rent_estimate_weekly  INT UNSIGNED NULL,
          rent_yield_pct        DECIMAL(5,2) NULL,

          raw_property_json     JSON         NOT NULL,
          fetched_at            DATETIME(3)  NOT NULL,
          parsed_at             DATETIME(3)  NOT NULL,

          PRIMARY KEY (id),
          KEY ix_dp_property (property_id),
          KEY ix_dp_dpid (domain_property_id),
          KEY ix_dp_slug (url_slug),
          KEY ix_dp_suburb_pc (suburb, postcode),
          CONSTRAINT fk_dp_raw FOREIGN KEY (raw_page_id) REFERENCES raw_pages(id),
          CONSTRAINT fk_dp_prop FOREIGN KEY (property_id) REFERENCES properties(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    op.execute("""
        CREATE TABLE domain_sales (
          id                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          domain_property_id    BIGINT UNSIGNED NOT NULL,
          property_id           BIGINT UNSIGNED NULL,
          raw_page_id           BIGINT UNSIGNED NOT NULL,

          -- One row per PropertyTimelineEvent. `category` is Domain's own
          -- field — observed values include 'Sale' and 'Rental'.
          event_date            DATE         NULL,
          event_price           INT UNSIGNED NULL,
          category              VARCHAR(20)  NOT NULL,
          price_description     VARCHAR(120) NULL,             -- 'PRIVATE TREATY' / 'PER WEEK'
          is_sold               TINYINT(1)   NOT NULL DEFAULT 0,
          is_major_event        TINYINT(1)   NOT NULL DEFAULT 0,
          days_on_market        INT          NULL,
          agency_name           VARCHAR(160) NULL,
          agency_profile_url    VARCHAR(255) NULL,

          raw_event_json        JSON         NOT NULL,

          PRIMARY KEY (id),
          KEY ix_ds_dp (domain_property_id),
          KEY ix_ds_property (property_id),
          KEY ix_ds_date (event_date),
          KEY ix_ds_sold (is_sold, event_date),
          CONSTRAINT fk_ds_dp FOREIGN KEY (domain_property_id) REFERENCES domain_properties(id),
          CONSTRAINT fk_ds_raw FOREIGN KEY (raw_page_id) REFERENCES raw_pages(id),
          CONSTRAINT fk_ds_prop FOREIGN KEY (property_id) REFERENCES properties(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS domain_sales")
    op.execute("DROP TABLE IF EXISTS domain_properties")
