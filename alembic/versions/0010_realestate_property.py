"""realestate_properties + realestate_sales

PDP snapshots and timeline events from realestate.com.au. Fetched via the
user's running Chrome over CDP (Kasada bypass — see CLAUDE.md). REA's
`id` field is the same numeric identifier used by property.com.au, so we
skip a separate property.com.au scraper.

Revision ID: 0010
Revises: 0009
Create Date: 2026-04-30

"""
from __future__ import annotations

from alembic import op


revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE realestate_properties (
          id                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          raw_page_id           BIGINT UNSIGNED NOT NULL,
          property_id           BIGINT UNSIGNED NULL,           -- FK to properties (lazy match)

          -- REA / property.com.au identifiers (same numeric id across both sites)
          rea_property_id       BIGINT UNSIGNED NOT NULL,       -- 6157790
          url_slug              VARCHAR(255) NOT NULL,          -- '124-sunshine-pde-miami-qld-4220'
          url                   VARCHAR(1024) NOT NULL,
          pca_property_url      VARCHAR(1024) NULL,             -- direct property.com.au link

          -- Address (decomposed, mirrors REA's `address` shape)
          display_address       VARCHAR(255) NOT NULL,
          unit_number           VARCHAR(16)  NOT NULL DEFAULT '',
          street_number         VARCHAR(16)  NOT NULL DEFAULT '',
          street_name           VARCHAR(120) NOT NULL DEFAULT '',
          suburb                VARCHAR(80)  NOT NULL,
          postcode              CHAR(4)      NOT NULL,
          state                 CHAR(3)      NOT NULL,
          lat                   DECIMAL(9,6) NULL,
          lng                   DECIMAL(9,6) NULL,

          -- Property attributes from `propertyProfile.property.attributes`
          property_type         VARCHAR(40)  NULL,
          bedrooms              TINYINT UNSIGNED NULL,
          bathrooms             TINYINT UNSIGNED NULL,
          car_spaces            TINYINT UNSIGNED NULL,
          land_area_m2          INT UNSIGNED NULL,
          floor_area_m2         INT UNSIGNED NULL,
          year_built            SMALLINT UNSIGNED NULL,

          -- Market state
          status_label          VARCHAR(40)  NULL,              -- 'Off market' / 'For sale'
          market_status         VARCHAR(40)  NULL,              -- 'off_market' / 'on_market' (trackingMarketStatus)

          -- REA's AVM
          valuation_low         INT UNSIGNED NULL,
          valuation_mid         INT UNSIGNED NULL,
          valuation_high        INT UNSIGNED NULL,
          valuation_confidence  VARCHAR(40)  NULL,
          rent_estimate_weekly  INT UNSIGNED NULL,
          rent_yield_pct        DECIMAL(5,2) NULL,

          raw_property_json     JSON         NOT NULL,
          fetched_at            DATETIME(3)  NOT NULL,
          parsed_at             DATETIME(3)  NOT NULL,

          PRIMARY KEY (id),
          KEY ix_rep_pid (rea_property_id),
          KEY ix_rep_property (property_id),
          KEY ix_rep_slug (url_slug),
          KEY ix_rep_suburb_pc (suburb, postcode),
          CONSTRAINT fk_rep_raw FOREIGN KEY (raw_page_id) REFERENCES raw_pages(id),
          CONSTRAINT fk_rep_prop FOREIGN KEY (property_id) REFERENCES properties(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    op.execute("""
        CREATE TABLE realestate_sales (
          id                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          realestate_property_id BIGINT UNSIGNED NOT NULL,
          property_id           BIGINT UNSIGNED NULL,
          raw_page_id           BIGINT UNSIGNED NOT NULL,

          -- One row per `propertyTimeline` event.
          event_date            DATE         NULL,
          event_price           INT UNSIGNED NULL,
          price_text            VARCHAR(120) NULL,             -- '$1,200,000' or 'Contact agent'
          event_type            VARCHAR(40)  NOT NULL,         -- 'sold' / 'listing' / 'leased'
          agency_name           VARCHAR(160) NULL,
          listing_url           VARCHAR(1024) NULL,            -- listingEvent.url, when present

          raw_event_json        JSON         NOT NULL,

          PRIMARY KEY (id),
          KEY ix_res_rep (realestate_property_id),
          KEY ix_res_property (property_id),
          KEY ix_res_date (event_date),
          KEY ix_res_type (event_type, event_date),
          CONSTRAINT fk_res_rep FOREIGN KEY (realestate_property_id) REFERENCES realestate_properties(id),
          CONSTRAINT fk_res_raw FOREIGN KEY (raw_page_id) REFERENCES raw_pages(id),
          CONSTRAINT fk_res_prop FOREIGN KEY (property_id) REFERENCES properties(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS realestate_sales")
    op.execute("DROP TABLE IF EXISTS realestate_properties")
