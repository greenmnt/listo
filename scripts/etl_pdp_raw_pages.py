#!/usr/bin/env python3
"""Push just the PDP/listing raw_pages (Domain + REA property pages) from
laptop to server. These are the raw_pages that the properties remediation
needs to resolve raw_page_id FKs on domain_*/realestate_*.

The bulk of raw_pages (council_cogc, search-result pages) are NOT needed
for the duplex frontend view — they can come later.

Strips id so server assigns new autoincrement; INSERT IGNORE protects
against duplicates if rerun.
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text


LAPTOP_URL = "mysql+pymysql://listo:password@127.0.0.1/listo?charset=utf8mb4"
SERVER_URL = "mysql+pymysql://listo:password@127.0.0.1:3307/listo?charset=utf8mb4"


SOURCES = ("domain_property", "realestate_property", "domain_listing", "realestate_listing")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    log = logging.getLogger("etl_pdp")

    laptop = create_engine(LAPTOP_URL, future=True)
    server = create_engine(SERVER_URL, future=True)

    log.info("pulling PDP raw_pages from laptop (sources=%s)...", SOURCES)
    with laptop.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(
            text("SELECT * FROM raw_pages WHERE source IN :srcs").bindparams(
                __import__("sqlalchemy").bindparam("srcs", expanding=True)
            ),
            {"srcs": list(SOURCES)},
        ).fetchall()]
    log.info("got %d rows", len(rows))

    # Strip id so server assigns new autoincrement
    cleaned = [{k: v for k, v in r.items() if k != "id"} for r in rows]

    # We'll de-dup at the application level by checking (url_hash, content_hash)
    # on server BEFORE inserting (since the table has no UK on those columns).
    log.info("checking which rows already exist on server...")
    with server.connect() as c:
        existing = {(r._mapping["url_hash"], r._mapping["content_hash"])
                    for r in c.execute(text(
                        "SELECT url_hash, content_hash FROM raw_pages WHERE source IN :srcs"
                    ).bindparams(__import__("sqlalchemy").bindparam("srcs", expanding=True)),
                    {"srcs": list(SOURCES)}).fetchall()}
    log.info("server has %d existing PDP rows", len(existing))

    to_insert = [r for r in cleaned
                 if (r["url_hash"], r["content_hash"]) not in existing]
    log.info("inserting %d new rows (skipped %d duplicates)",
             len(to_insert), len(cleaned) - len(to_insert))

    if not to_insert:
        log.info("nothing to do."); return

    cols = list(to_insert[0].keys())
    placeholders = ", ".join(f":{c}" for c in cols)
    col_list = ", ".join(f"`{c}`" for c in cols)
    sql = f"INSERT INTO `raw_pages` ({col_list}) VALUES ({placeholders})"

    inserted = 0
    BATCH = 50
    with server.begin() as c:
        for i in range(0, len(to_insert), BATCH):
            r = c.execute(text(sql), to_insert[i : i + BATCH])
            inserted += r.rowcount or 0
            log.info("  progress: %d/%d (inserted %d)", i + len(to_insert[i:i+BATCH]), len(to_insert), inserted)
    log.info("done: %d inserted", inserted)


if __name__ == "__main__":
    main()
