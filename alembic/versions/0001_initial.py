"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-04-28

"""
from __future__ import annotations

from alembic import op


revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE raw_pages (
          id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          source        ENUM('realestate','domain') NOT NULL,
          page_type     ENUM('search_sold','search_buy','search_rent','listing') NOT NULL,
          url           VARCHAR(1024) NOT NULL,
          url_hash      BINARY(32) NOT NULL,
          suburb        VARCHAR(80) NULL,
          postcode      CHAR(4) NULL,
          page_index    INT NULL,
          http_status   SMALLINT NOT NULL,
          fetched_at    DATETIME(3) NOT NULL,
          content_hash  BINARY(32) NOT NULL,
          body_gz       MEDIUMBLOB NOT NULL,
          headers_json  JSON NOT NULL,
          parsed_at     DATETIME(3) NULL,
          parse_error   TEXT NULL,
          PRIMARY KEY (id),
          KEY ix_raw_source_type_fetched (source, page_type, fetched_at),
          KEY ix_raw_url_hash (url_hash),
          KEY ix_raw_content_hash (content_hash),
          KEY ix_raw_unparsed (parsed_at, source)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    op.execute(
        """
        CREATE TABLE properties (
          id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          match_key       VARCHAR(160) NOT NULL,
          unit_number     VARCHAR(16) NOT NULL DEFAULT '',
          street_number   VARCHAR(16) NOT NULL,
          street_name     VARCHAR(120) NOT NULL,
          street_norm     VARCHAR(120) NOT NULL,
          suburb          VARCHAR(80) NOT NULL,
          suburb_norm     VARCHAR(80) NOT NULL,
          postcode        CHAR(4) NOT NULL,
          state           CHAR(3) NOT NULL DEFAULT 'QLD',
          lat             DECIMAL(9,6) NULL,
          lng             DECIMAL(9,6) NULL,
          first_seen_at   DATETIME(3) NOT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_prop_full (match_key, unit_number),
          KEY ix_prop_match (match_key),
          KEY ix_prop_suburb_postcode (suburb_norm, postcode)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    op.execute(
        """
        CREATE TABLE listings (
          id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          property_id       BIGINT UNSIGNED NOT NULL,
          source            ENUM('realestate','domain') NOT NULL,
          source_listing_id VARCHAR(32) NOT NULL,
          raw_page_id       BIGINT UNSIGNED NOT NULL,
          listing_kind      ENUM('buy','rent','sold') NOT NULL,
          status            ENUM('active','sold','withdrawn','unknown') NOT NULL,
          price_text        VARCHAR(120) NULL,
          price_min         INT UNSIGNED NULL,
          price_max         INT UNSIGNED NULL,
          beds              TINYINT UNSIGNED NULL,
          baths             TINYINT UNSIGNED NULL,
          parking           TINYINT UNSIGNED NULL,
          property_type     VARCHAR(40) NULL,
          land_size_m2      INT UNSIGNED NULL,
          agent_name        VARCHAR(160) NULL,
          agency_name       VARCHAR(160) NULL,
          url               VARCHAR(1024) NOT NULL,
          first_seen_at     DATETIME(3) NOT NULL,
          last_seen_at      DATETIME(3) NOT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_listing_source (source, source_listing_id),
          KEY ix_listing_property (property_id),
          CONSTRAINT fk_listing_prop FOREIGN KEY (property_id) REFERENCES properties(id),
          CONSTRAINT fk_listing_raw  FOREIGN KEY (raw_page_id) REFERENCES raw_pages(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    op.execute(
        """
        CREATE TABLE sales (
          id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          property_id       BIGINT UNSIGNED NOT NULL,
          source            ENUM('realestate','domain') NOT NULL,
          source_listing_id VARCHAR(32) NULL,
          raw_page_id       BIGINT UNSIGNED NOT NULL,
          sold_date         DATE NULL,
          sold_price        INT UNSIGNED NULL,
          sale_method       VARCHAR(40) NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_sale (property_id, source, source_listing_id, sold_date),
          KEY ix_sale_property_date (property_id, sold_date),
          CONSTRAINT fk_sale_prop FOREIGN KEY (property_id) REFERENCES properties(id),
          CONSTRAINT fk_sale_raw  FOREIGN KEY (raw_page_id) REFERENCES raw_pages(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    op.execute(
        """
        CREATE TABLE crawl_runs (
          id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          source        ENUM('realestate','domain') NOT NULL,
          page_type     VARCHAR(20) NOT NULL,
          suburb        VARCHAR(80) NOT NULL,
          postcode      CHAR(4) NOT NULL,
          started_at    DATETIME(3) NOT NULL,
          finished_at   DATETIME(3) NULL,
          pages_fetched INT NOT NULL DEFAULT 0,
          last_page     INT NOT NULL DEFAULT 0,
          status        ENUM('running','done','failed','partial') NOT NULL,
          error         TEXT NULL,
          PRIMARY KEY (id),
          KEY ix_runs_resume (source, page_type, suburb, postcode, started_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS sales")
    op.execute("DROP TABLE IF EXISTS listings")
    op.execute("DROP TABLE IF EXISTS crawl_runs")
    op.execute("DROP TABLE IF EXISTS properties")
    op.execute("DROP TABLE IF EXISTS raw_pages")
