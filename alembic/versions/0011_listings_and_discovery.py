"""realestate_listings + domain_listings + discovered_urls

Listing-detail pages (the per-transaction `/sold/...` and Domain
`/{slug}-{listingId}` URLs) carry the agent's full description, photos,
and listing-time price — meaningfully richer than the property-profile
timeline. Stored separately so we can keep the PDP table 1:1 with the
underlying property and let listings be N:1.

`discovered_urls` is a cache of URLs surfaced by Google `site:` searches
so we don't re-query Google when re-running a property fetch.

Revision ID: 0011
Revises: 0010
Create Date: 2026-04-30

"""
from __future__ import annotations

from alembic import op


revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE realestate_listings (
          id                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          raw_page_id           BIGINT UNSIGNED NOT NULL,
          realestate_property_id BIGINT UNSIGNED NULL,         -- FK once we resolve which property

          rea_listing_id        BIGINT UNSIGNED NOT NULL,      -- e.g. 203073654
          url                   VARCHAR(1024) NOT NULL,

          listing_kind          VARCHAR(20)  NOT NULL,         -- 'sold' / 'buy' / 'rent' (from URL prefix)
          listing_status        VARCHAR(40)  NULL,             -- in-page status
          display_address       VARCHAR(255) NOT NULL,
          property_type         VARCHAR(40)  NULL,

          price_text            VARCHAR(160) NULL,             -- 'Auction $1,200,000'
          sold_price            INT UNSIGNED NULL,
          sold_date             DATE         NULL,
          sale_method           VARCHAR(40)  NULL,             -- 'Private treaty' / 'Auction'

          bedrooms              TINYINT UNSIGNED NULL,
          bathrooms             TINYINT UNSIGNED NULL,
          car_spaces            TINYINT UNSIGNED NULL,
          land_area_m2          INT UNSIGNED NULL,
          floor_area_m2         INT UNSIGNED NULL,

          agency_name           VARCHAR(160) NULL,
          agent_name            VARCHAR(160) NULL,

          description           MEDIUMTEXT   NULL,             -- agent's listing description
          features_json         JSON         NULL,             -- ["Air conditioning", ...]
          photos_json           JSON         NULL,             -- [{url, caption}, ...]

          raw_listing_json      JSON         NOT NULL,
          fetched_at            DATETIME(3)  NOT NULL,
          parsed_at             DATETIME(3)  NOT NULL,

          PRIMARY KEY (id),
          UNIQUE KEY uq_rl_listing (rea_listing_id),
          KEY ix_rl_rep (realestate_property_id),
          KEY ix_rl_sold (sold_date, sold_price),
          CONSTRAINT fk_rl_raw FOREIGN KEY (raw_page_id) REFERENCES raw_pages(id),
          CONSTRAINT fk_rl_rep FOREIGN KEY (realestate_property_id) REFERENCES realestate_properties(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    op.execute("""
        CREATE TABLE domain_listings (
          id                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          raw_page_id           BIGINT UNSIGNED NOT NULL,
          domain_property_id    BIGINT UNSIGNED NULL,          -- FK once we resolve

          domain_listing_id     BIGINT UNSIGNED NOT NULL,      -- e.g. 2017154349
          url                   VARCHAR(1024) NOT NULL,

          listing_kind          VARCHAR(20)  NOT NULL,         -- 'sold' / 'buy' / 'rent'
          listing_status        VARCHAR(40)  NULL,
          display_address       VARCHAR(255) NOT NULL,
          property_type         VARCHAR(40)  NULL,

          price_text            VARCHAR(160) NULL,
          sold_price            INT UNSIGNED NULL,
          sold_date             DATE         NULL,
          sale_method           VARCHAR(40)  NULL,

          bedrooms              TINYINT UNSIGNED NULL,
          bathrooms             TINYINT UNSIGNED NULL,
          car_spaces            TINYINT UNSIGNED NULL,
          land_area_m2          INT UNSIGNED NULL,

          agency_name           VARCHAR(160) NULL,
          agent_name            VARCHAR(160) NULL,

          description           MEDIUMTEXT   NULL,
          features_json         JSON         NULL,
          photos_json           JSON         NULL,

          raw_listing_json      JSON         NOT NULL,
          fetched_at            DATETIME(3)  NOT NULL,
          parsed_at             DATETIME(3)  NOT NULL,

          PRIMARY KEY (id),
          UNIQUE KEY uq_dl_listing (domain_listing_id),
          KEY ix_dl_dp (domain_property_id),
          KEY ix_dl_sold (sold_date, sold_price),
          CONSTRAINT fk_dl_raw FOREIGN KEY (raw_page_id) REFERENCES raw_pages(id),
          CONSTRAINT fk_dl_dp FOREIGN KEY (domain_property_id) REFERENCES domain_properties(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    op.execute("""
        CREATE TABLE discovered_urls (
          id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          search_address    VARCHAR(255) NOT NULL,    -- normalised address used for search
          search_query      VARCHAR(255) NOT NULL,    -- 'site:realestate.com.au "..." Miami'
          url               VARCHAR(1024) NOT NULL,
          url_hash          BINARY(32)   NOT NULL,
          url_kind          VARCHAR(40)  NOT NULL,    -- 'rea_pdp' | 'rea_sold' | 'domain_pdp' | 'domain_listing'
          search_engine     VARCHAR(20)  NOT NULL,    -- 'google'
          discovered_at     DATETIME(3)  NOT NULL,
          fetched_at        DATETIME(3)  NULL,        -- when we fetched this URL
          PRIMARY KEY (id),
          UNIQUE KEY uq_du_url (url_hash),
          KEY ix_du_address (search_address),
          KEY ix_du_kind_unfetched (url_kind, fetched_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS discovered_urls")
    op.execute("DROP TABLE IF EXISTS domain_listings")
    op.execute("DROP TABLE IF EXISTS realestate_listings")
