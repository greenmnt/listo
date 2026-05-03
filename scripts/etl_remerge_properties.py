#!/usr/bin/env python3
"""Remediation step for the FK-aware merge: push the `properties` parent
table and re-resolve domain_*/realestate_* FKs through it.

Background: the first ETL pass set raw_page_id=NULL but left property_id
unset, so MySQL silently rejected every domain_properties /
realestate_properties INSERT IGNORE on the FK constraint property_id ->
properties(id). This script:

1. Pushes `properties` (UK on match_key — INSERT IGNORE-safe).
2. For domain_properties / realestate_properties / their sales+listings:
   resolve property_id via the laptop's natural match_key, lift the row
   over with the correct server property_id, set raw_page_id=NULL.

Idempotent. Can run while the main etl_remerge.py is still on raw_pages.
"""
from __future__ import annotations

import logging
import time

from sqlalchemy import create_engine, text


LAPTOP_URL = "mysql+pymysql://listo:password@127.0.0.1/listo?charset=utf8mb4"
SERVER_URL = "mysql+pymysql://listo:password@127.0.0.1:3307/listo?charset=utf8mb4"

log = logging.getLogger("etl_props")


def _all(eng, sql, **p):
    with eng.connect() as c:
        return [dict(r._mapping) for r in c.execute(text(sql), p).fetchall()]


def _insert_ignore(server, table, rows, label=None):
    if not rows:
        return 0
    cols = list(rows[0].keys())
    placeholders = ", ".join(f":{c}" for c in cols)
    col_list = ", ".join(f"`{c}`" for c in cols)
    sql = f"INSERT IGNORE INTO `{table}` ({col_list}) VALUES ({placeholders})"
    inserted = 0
    BATCH = 200
    with server.begin() as c:
        for i in range(0, len(rows), BATCH):
            r = c.execute(text(sql), rows[i : i + BATCH])
            inserted += r.rowcount or 0
    log.info("  %s: %d inserted of %d", label or table, inserted, len(rows))
    return inserted


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    laptop = create_engine(LAPTOP_URL, future=True)
    server = create_engine(SERVER_URL, future=True)

    # ---------- 1. properties ----------
    log.info("== properties ==")
    rows = _all(laptop, "SELECT * FROM properties")
    cleaned = [{k: v for k, v in r.items() if k != "id"} for r in rows]
    t0 = time.time()
    _insert_ignore(server, "properties", cleaned)
    log.info("== properties done in %.1fs ==", time.time() - t0)

    log.info("building server properties match_key map...")
    pmap = {r["match_key"]: r["id"] for r in _all(server, "SELECT id, match_key FROM properties")}
    log.info("server properties: %d", len(pmap))

    # On laptop, domain_properties / realestate_properties may have
    # property_id = NULL (never linked to the dormant `properties` table)
    # but they ALWAYS have a raw_page_id (NOT NULL FK). So we resolve
    # raw_page_id via the natural (url_hash, content_hash) on raw_pages,
    # and leave property_id alone (NULL is allowed).
    log.info("building server raw_pages url_hash map (this is large)...")
    rp_map: dict[bytes, int] = {}
    server_rp = _all(server, "SELECT id, url_hash, content_hash FROM raw_pages")
    for r in server_rp:
        # Prefer (url_hash, content_hash) for strict match; fall back to just url_hash.
        rp_map[(r["url_hash"], r["content_hash"])] = r["id"]
        rp_map.setdefault(("uh", r["url_hash"]), r["id"])
    log.info("  server raw_pages map size: %d", len(rp_map))

    def _resolve_raw_page(url_hash, content_hash):
        if url_hash is None:
            return None
        v = rp_map.get((url_hash, content_hash))
        if v is None:
            v = rp_map.get(("uh", url_hash))
        return v

    # ---------- 2. domain_properties ----------
    log.info("== domain_properties ==")
    rows = _all(laptop, """
        SELECT dp.*,
               p.match_key AS _natural_pk,
               rp.url_hash AS _uh, rp.content_hash AS _ch
          FROM domain_properties dp
          LEFT JOIN properties p ON p.id = dp.property_id
          JOIN raw_pages rp ON rp.id = dp.raw_page_id
    """)
    payload = []; sk_rp = 0
    for r in rows:
        rpid = _resolve_raw_page(r["_uh"], r["_ch"])
        if rpid is None:
            sk_rp += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["property_id"] = pmap.get(r["_natural_pk"])  # may be None — fine, column is nullable
        d["raw_page_id"] = rpid
        payload.append(d)
    log.info("  resolved %d/%d (skipped raw_page=%d)", len(payload), len(rows), sk_rp)
    _insert_ignore(server, "domain_properties", payload)

    # build map of server's domain_properties.id by domain_property_id (the natural)
    log.info("building server domain_properties map...")
    dp_map = {r["domain_property_id"]: r["id"] for r in _all(
        server, "SELECT id, domain_property_id FROM domain_properties WHERE domain_property_id IS NOT NULL")}
    log.info("server domain_properties: %d", len(dp_map))

    # ---------- 3. domain_sales (FK to properties.id, domain_properties.id, raw_pages.id) ----------
    log.info("== domain_sales ==")
    rows = _all(laptop, """
        SELECT ds.*,
               p.match_key AS _pk_natural,
               dp.domain_property_id AS _dp_natural,
               rp.url_hash AS _uh, rp.content_hash AS _ch
          FROM domain_sales ds
          LEFT JOIN properties p ON p.id = ds.property_id
          JOIN domain_properties dp ON dp.id = ds.domain_property_id
          JOIN raw_pages rp ON rp.id = ds.raw_page_id
    """)
    payload = []; sk_dp = 0; sk_rp = 0
    for r in rows:
        dpid = dp_map.get(r["_dp_natural"])
        rpid = _resolve_raw_page(r["_uh"], r["_ch"])
        if dpid is None: sk_dp += 1; continue
        if rpid is None: sk_rp += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["property_id"] = pmap.get(r["_pk_natural"])
        d["domain_property_id"] = dpid
        d["raw_page_id"] = rpid
        payload.append(d)
    log.info("  resolved %d/%d (skipped dp=%d, raw=%d)", len(payload), len(rows), sk_dp, sk_rp)
    _insert_ignore(server, "domain_sales", payload)

    # ---------- 4. domain_listings ----------
    log.info("== domain_listings ==")
    rows = _all(laptop, """
        SELECT dl.*,
               dp.domain_property_id AS _dp_natural,
               rp.url_hash AS _uh, rp.content_hash AS _ch
          FROM domain_listings dl
          JOIN domain_properties dp ON dp.id = dl.domain_property_id
          JOIN raw_pages rp ON rp.id = dl.raw_page_id
    """)
    payload = []
    for r in rows:
        dpid = dp_map.get(r["_dp_natural"])
        rpid = _resolve_raw_page(r["_uh"], r["_ch"])
        if dpid is None or rpid is None: continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["domain_property_id"] = dpid
        d["raw_page_id"] = rpid
        payload.append(d)
    _insert_ignore(server, "domain_listings", payload)

    # ---------- 5. realestate_properties ----------
    log.info("== realestate_properties ==")
    rows = _all(laptop, """
        SELECT rep.*, p.match_key AS _natural_pk,
               rp.url_hash AS _uh, rp.content_hash AS _ch
          FROM realestate_properties rep
          LEFT JOIN properties p ON p.id = rep.property_id
          JOIN raw_pages rp ON rp.id = rep.raw_page_id
    """)
    payload = []; sk_rp = 0
    for r in rows:
        rpid = _resolve_raw_page(r["_uh"], r["_ch"])
        if rpid is None: sk_rp += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["property_id"] = pmap.get(r["_natural_pk"])
        d["raw_page_id"] = rpid
        payload.append(d)
    log.info("  resolved %d/%d (skipped raw=%d)", len(payload), len(rows), sk_rp)
    _insert_ignore(server, "realestate_properties", payload)

    log.info("building server realestate_properties map...")
    rep_map = {r["rea_property_id"]: r["id"] for r in _all(
        server, "SELECT id, rea_property_id FROM realestate_properties WHERE rea_property_id IS NOT NULL")}
    log.info("server realestate_properties: %d", len(rep_map))

    # ---------- 6. realestate_sales ----------
    log.info("== realestate_sales ==")
    rows = _all(laptop, """
        SELECT rs.*,
               p.match_key AS _pk_natural,
               rep.rea_property_id AS _rp_natural,
               rp.url_hash AS _uh, rp.content_hash AS _ch
          FROM realestate_sales rs
          LEFT JOIN properties p ON p.id = rs.property_id
          JOIN realestate_properties rep ON rep.id = rs.realestate_property_id
          JOIN raw_pages rp ON rp.id = rs.raw_page_id
    """)
    payload = []; sk_rp_id = 0; sk_raw = 0
    for r in rows:
        rpid = rep_map.get(r["_rp_natural"])
        raw_id = _resolve_raw_page(r["_uh"], r["_ch"])
        if rpid is None: sk_rp_id += 1; continue
        if raw_id is None: sk_raw += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["property_id"] = pmap.get(r["_pk_natural"])
        d["realestate_property_id"] = rpid
        d["raw_page_id"] = raw_id
        payload.append(d)
    log.info("  resolved %d/%d (skipped rep=%d, raw=%d)", len(payload), len(rows), sk_rp_id, sk_raw)
    _insert_ignore(server, "realestate_sales", payload)

    # ---------- 7. realestate_listings ----------
    log.info("== realestate_listings ==")
    rows = _all(laptop, """
        SELECT rl.*,
               rep.rea_property_id AS _rp_natural,
               rp.url_hash AS _uh, rp.content_hash AS _ch
          FROM realestate_listings rl
          JOIN realestate_properties rep ON rep.id = rl.realestate_property_id
          JOIN raw_pages rp ON rp.id = rl.raw_page_id
    """)
    payload = []
    for r in rows:
        rpid = rep_map.get(r["_rp_natural"])
        raw_id = _resolve_raw_page(r["_uh"], r["_ch"])
        if rpid is None or raw_id is None: continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["realestate_property_id"] = rpid
        d["raw_page_id"] = raw_id
        payload.append(d)
    _insert_ignore(server, "realestate_listings", payload)

    log.info("done.")


if __name__ == "__main__":
    main()
