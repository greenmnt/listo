"""dedupe domain_/realestate_properties + add unique natural-key index

Both `domain_properties.persist` and `realestate_properties.persist` had
been doing plain `s.add()` with no upsert — every rerun of the property
scraper inserted a fresh row for the same Domain/REA listing, so the
canonical addresses ended up with 5-17 duplicate rows each. Frontend +
ML training counts get inflated proportionally.

Cleanup logic:
  1. For each natural key (`domain_property_id` / `rea_property_id`),
     pick the highest-id row as the "keeper" — it has the freshest
     `fetched_at` and the most up-to-date `raw_page_id` linkage.
  2. Repoint every FK child (`domain_listings`, `domain_sales`,
     `realestate_listings`, `realestate_sales`) from the loser rows
     to the keeper row.
  3. Delete the loser rows.
  4. Add UNIQUE indexes on the natural-key columns so future runs
     have something to ON DUPLICATE KEY UPDATE against.

`raw_pages` is intentionally untouched — every scrape's raw HTML
remains queryable for forensics, only the parsed-property dedup row
collapses.
"""

from alembic import op


revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ---- domain_properties ----
    # Repoint FK children to the keeper (max(id)) per natural key.
    for child_table in ("domain_listings", "domain_sales"):
        op.execute(f"""
            UPDATE {child_table} c
              JOIN domain_properties losers
                ON losers.id = c.domain_property_id
              JOIN (
                SELECT MAX(id) AS keeper_id, domain_property_id AS natkey
                  FROM domain_properties
                 WHERE domain_property_id IS NOT NULL
                 GROUP BY domain_property_id
                HAVING COUNT(*) > 1
              ) keepers ON keepers.natkey = losers.domain_property_id
               SET c.domain_property_id = keepers.keeper_id
             WHERE losers.id <> keepers.keeper_id;
        """)
    op.execute("""
        DELETE losers FROM domain_properties losers
          JOIN (
            SELECT MAX(id) AS keeper_id, domain_property_id AS natkey
              FROM domain_properties
             WHERE domain_property_id IS NOT NULL
             GROUP BY domain_property_id
            HAVING COUNT(*) > 1
          ) keepers ON keepers.natkey = losers.domain_property_id
         WHERE losers.id <> keepers.keeper_id;
    """)
    op.create_index(
        "uq_domain_properties_natkey",
        "domain_properties",
        ["domain_property_id"],
        unique=True,
    )

    # ---- realestate_properties ----
    for child_table in ("realestate_listings", "realestate_sales"):
        op.execute(f"""
            UPDATE {child_table} c
              JOIN realestate_properties losers
                ON losers.id = c.realestate_property_id
              JOIN (
                SELECT MAX(id) AS keeper_id, rea_property_id AS natkey
                  FROM realestate_properties
                 WHERE rea_property_id IS NOT NULL
                 GROUP BY rea_property_id
                HAVING COUNT(*) > 1
              ) keepers ON keepers.natkey = losers.rea_property_id
               SET c.realestate_property_id = keepers.keeper_id
             WHERE losers.id <> keepers.keeper_id;
        """)
    op.execute("""
        DELETE losers FROM realestate_properties losers
          JOIN (
            SELECT MAX(id) AS keeper_id, rea_property_id AS natkey
              FROM realestate_properties
             WHERE rea_property_id IS NOT NULL
             GROUP BY rea_property_id
            HAVING COUNT(*) > 1
          ) keepers ON keepers.natkey = losers.rea_property_id
         WHERE losers.id <> keepers.keeper_id;
    """)
    op.create_index(
        "uq_realestate_properties_natkey",
        "realestate_properties",
        ["rea_property_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_realestate_properties_natkey", table_name="realestate_properties")
    op.drop_index("uq_domain_properties_natkey", table_name="domain_properties")
    # Cannot un-dedupe: lost-row data is gone.
