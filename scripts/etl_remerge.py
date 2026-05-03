#!/usr/bin/env python3
"""ETL the laptop's listo DB into the server's, with FK remapping.

The first merge attempt INSERT IGNORE'd raw rows including autoincrement
FKs, which silently corrupted every child table because laptop's IDs
collided with different server DAs. This script does the merge again,
correctly:

- Pulls each table from laptop with extra "natural key" columns joined
  in for any autoincrement FK (council_applications.id, council_application_documents.id,
  domain_properties.id, realestate_properties.id, companies.id).
- Resolves those natural keys to the server's autoincrement ids at
  INSERT time, then INSERT IGNORE's against each table's natural unique
  constraint. Idempotent.

Run from the repo root with the SSH tunnel up:

    ./scripts/server-tunnel.sh &      # in a separate terminal
    uv run python scripts/etl_remerge.py [--only table1,table2]

Pre-conditions:
  - Server has been restored from pre-merge backup and re-upgraded to
    alembic 0022 (so all the post-0008 child tables exist, empty).
  - Laptop's listo DB has the canonical post-2020 dataset.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Iterable

from sqlalchemy import create_engine, text


LAPTOP_URL = "mysql+pymysql://listo:password@127.0.0.1/listo?charset=utf8mb4"
SERVER_URL = "mysql+pymysql://listo:password@127.0.0.1:3307/listo?charset=utf8mb4"

BATCH = 200
log = logging.getLogger("etl")


def _chunked(rows: list[dict], n: int = BATCH) -> Iterable[list[dict]]:
    for i in range(0, len(rows), n):
        yield rows[i : i + n]


def _all(eng, sql: str, **params) -> list[dict]:
    with eng.connect() as c:
        return [dict(r._mapping) for r in c.execute(text(sql), params).fetchall()]


def _stream(eng, sql: str, page: int = 500, **params):
    """Yield batches of rows, paginated by LIMIT/OFFSET. Caller must include
    a stable ORDER BY in the SQL."""
    offset = 0
    while True:
        rows = _all(eng, sql + f" LIMIT {page} OFFSET {offset}", **params)
        if not rows:
            return
        yield rows
        if len(rows) < page:
            return
        offset += page


def _insert_ignore(server_eng, table: str, rows: list[dict], label: str = None) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    placeholders = ", ".join(f":{c}" for c in cols)
    col_list = ", ".join(f"`{c}`" for c in cols)
    sql = f"INSERT IGNORE INTO `{table}` ({col_list}) VALUES ({placeholders})"
    inserted = 0
    with server_eng.begin() as c:
        for batch in _chunked(rows):
            r = c.execute(text(sql), batch)
            inserted += r.rowcount or 0
    log.info("  %s: %d inserted of %d attempted", label or table, inserted, len(rows))
    return inserted


# ---------------------------------------------------------------------------
# Lookup maps for FK resolution (built from server side)
# ---------------------------------------------------------------------------

def build_app_map(server) -> dict[tuple[str, str], int]:
    log.info("building server council_applications natural-key map...")
    rows = _all(server, "SELECT id, council_slug, application_id FROM council_applications")
    return {(r["council_slug"], r["application_id"]): r["id"] for r in rows}


def build_doc_map(server) -> dict[tuple[int, str], int]:
    log.info("building server council_application_documents natural-key map...")
    rows = _all(server, "SELECT id, application_id, doc_oid FROM council_application_documents")
    return {(r["application_id"], r["doc_oid"]): r["id"] for r in rows if r["doc_oid"]}


def build_company_map(server) -> dict[tuple, int]:
    rows = _all(server, "SELECT id, acn, abn, norm_name FROM companies")
    out: dict[tuple, int] = {}
    for r in rows:
        if r["acn"]:
            out[("acn", r["acn"])] = r["id"]
        if r["abn"]:
            out[("abn", r["abn"])] = r["id"]
        if r["norm_name"]:
            out[("norm_name", r["norm_name"])] = r["id"]
    return out


def build_domain_prop_map(server) -> dict[str, int]:
    rows = _all(server, "SELECT id, domain_property_id FROM domain_properties WHERE domain_property_id IS NOT NULL")
    return {r["domain_property_id"]: r["id"] for r in rows}


def build_rea_prop_map(server) -> dict[str, int]:
    rows = _all(server, "SELECT id, rea_property_id FROM realestate_properties WHERE rea_property_id IS NOT NULL")
    return {r["rea_property_id"]: r["id"] for r in rows}


def build_raw_page_map(server) -> dict[bytes, int]:
    """Match by url_hash. Server may have many duplicates; we just need any matching id."""
    rows = _all(server, "SELECT id, url_hash, content_hash FROM raw_pages")
    out: dict[tuple, int] = {}
    for r in rows:
        # Prefer (url_hash, content_hash) for stronger match
        key = (r["url_hash"], r["content_hash"])
        out[key] = r["id"]
        # Fallback by just url_hash
        if r["url_hash"] not in out:
            out[("url_hash_only", r["url_hash"])] = r["id"]
    return out


def lookup_company(company_map, acn, abn, norm_name) -> int | None:
    for k in (("acn", acn), ("abn", abn), ("norm_name", norm_name)):
        if k[1]:
            v = company_map.get(k)
            if v is not None:
                return v
    return None


# ---------------------------------------------------------------------------
# Generic ETL by table
# ---------------------------------------------------------------------------

def etl_simple(laptop, server, table: str, exclude_cols: tuple[str, ...] = ("id",)) -> None:
    """SELECT * FROM table on laptop, drop excluded cols (typically autoincrement
    id), INSERT IGNORE on server. Use only for tables whose FKs are NOT
    autoincrement references (or which have no FKs). Server's natural unique
    key handles dedup."""
    log.info("== %s ==", table)
    rows = _all(laptop, f"SELECT * FROM `{table}`")
    if not rows:
        log.info("  empty on laptop, skipped"); return
    cleaned = [{k: v for k, v in r.items() if k not in exclude_cols} for r in rows]
    _insert_ignore(server, table, cleaned)


def etl_streamed(laptop, server, table: str, exclude_cols: tuple[str, ...] = ("id",), page: int = 500) -> None:
    """Streaming variant of etl_simple — for big tables (raw_pages)."""
    log.info("== %s (streamed) ==", table)
    cnt = _all(laptop, f"SELECT COUNT(*) AS n FROM `{table}`")[0]["n"]
    log.info("  %d rows on laptop, streaming...", cnt)
    total_in = 0
    total_seen = 0
    for batch in _stream(laptop, f"SELECT * FROM `{table}` ORDER BY id", page=page):
        cleaned = [{k: v for k, v in r.items() if k not in exclude_cols} for r in batch]
        total_in += _insert_ignore(server, table, cleaned, label=f"{table} batch")
        total_seen += len(batch)
        log.info("  progress: %d / %d  (inserted: %d)", total_seen, cnt, total_in)


def etl_council_application_documents(laptop, server, app_map) -> None:
    """FK remap: application_id (autoinc) → look up via natural (slug, app_id)."""
    log.info("== council_application_documents ==")
    rows = _all(laptop, """
        SELECT d.*, ca.council_slug AS _slug, ca.application_id AS _app_id
          FROM council_application_documents d
          JOIN council_applications ca ON ca.id = d.application_id
    """)
    payload = []; skipped = 0
    for r in rows:
        sid = app_map.get((r["_slug"], r["_app_id"]))
        if sid is None:
            skipped += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["application_id"] = sid
        payload.append(d)
    log.info("  resolved %d/%d (skipped %d)", len(payload), len(rows), skipped)
    _insert_ignore(server, "council_application_documents", payload)


def etl_da_summaries(laptop, server, app_map, company_map) -> None:
    """application_id PK + 5 company_id FK columns."""
    log.info("== da_summaries ==")
    rows = _all(laptop, """
        SELECT ds.*,
               ca.council_slug AS _slug, ca.application_id AS _app_id,
               cap.acn AS _cap_acn, cap.abn AS _cap_abn, cap.norm_name AS _cap_norm,
               cag.acn AS _cag_acn, cag.abn AS _cag_abn, cag.norm_name AS _cag_norm,
               cb.acn  AS _cb_acn,  cb.abn  AS _cb_abn,  cb.norm_name  AS _cb_norm,
               car.acn AS _car_acn, car.abn AS _car_abn, car.norm_name AS _car_norm,
               co.acn  AS _co_acn,  co.abn  AS _co_abn,  co.norm_name  AS _co_norm
          FROM da_summaries ds
          JOIN council_applications ca ON ca.id = ds.application_id
          LEFT JOIN companies cap ON cap.id = ds.applicant_company_id
          LEFT JOIN companies cag ON cag.id = ds.applicant_agent_company_id
          LEFT JOIN companies cb  ON cb.id  = ds.builder_company_id
          LEFT JOIN companies car ON car.id = ds.architect_company_id
          LEFT JOIN companies co  ON co.id  = ds.owner_company_id
    """)
    payload = []; skipped = 0
    for r in rows:
        sid = app_map.get((r["_slug"], r["_app_id"]))
        if sid is None:
            skipped += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_")}
        d["application_id"] = sid
        d["applicant_company_id"]       = lookup_company(company_map, r["_cap_acn"], r["_cap_abn"], r["_cap_norm"])
        d["applicant_agent_company_id"] = lookup_company(company_map, r["_cag_acn"], r["_cag_abn"], r["_cag_norm"])
        d["builder_company_id"]         = lookup_company(company_map, r["_cb_acn"],  r["_cb_abn"],  r["_cb_norm"])
        d["architect_company_id"]       = lookup_company(company_map, r["_car_acn"], r["_car_abn"], r["_car_norm"])
        d["owner_company_id"]           = lookup_company(company_map, r["_co_acn"],  r["_co_abn"],  r["_co_norm"])
        payload.append(d)
    log.info("  resolved %d/%d (skipped %d)", len(payload), len(rows), skipped)
    _insert_ignore(server, "da_summaries", payload)


def etl_with_doc_fk(laptop, server, app_map, doc_map, table: str, doc_fk_col: str = "source_doc_id") -> None:
    """Generic: rows reference council_applications.id AND council_application_documents.id."""
    log.info("== %s ==", table)
    rows = _all(laptop, f"""
        SELECT t.*, ca.council_slug AS _slug, ca.application_id AS _app_id,
               d.doc_oid AS _doc_oid
          FROM `{table}` t
          JOIN council_application_documents d ON d.id = t.{doc_fk_col}
          JOIN council_applications ca ON ca.id = d.application_id
    """)
    payload = []; sk_app = 0; sk_doc = 0
    for r in rows:
        app_id = app_map.get((r["_slug"], r["_app_id"]))
        if app_id is None:
            sk_app += 1; continue
        doc_id = doc_map.get((app_id, r["_doc_oid"]))
        if doc_id is None:
            sk_doc += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        if "application_id" in d:
            d["application_id"] = app_id
        d[doc_fk_col] = doc_id
        payload.append(d)
    log.info("  resolved %d/%d (skipped app=%d, doc=%d)", len(payload), len(rows), sk_app, sk_doc)
    _insert_ignore(server, table, payload)


def etl_with_app_fk(laptop, server, app_map, table: str, app_fk_col: str = "application_id") -> None:
    """Tables with just an application_id FK to council_applications."""
    log.info("== %s ==", table)
    rows = _all(laptop, f"""
        SELECT t.*, ca.council_slug AS _slug, ca.application_id AS _app_id
          FROM `{table}` t
          JOIN council_applications ca ON ca.id = t.{app_fk_col}
    """)
    payload = []; skipped = 0
    for r in rows:
        sid = app_map.get((r["_slug"], r["_app_id"]))
        if sid is None:
            skipped += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d[app_fk_col] = sid
        payload.append(d)
    log.info("  resolved %d/%d (skipped %d)", len(payload), len(rows), skipped)
    _insert_ignore(server, table, payload)


def etl_with_app_and_doc_fk_optional(laptop, server, app_map, doc_map, table: str,
                                     app_col: str = "application_id",
                                     doc_col: str = "source_doc_id") -> None:
    """Like etl_with_doc_fk but doc FK is optional (may be NULL on some rows)."""
    log.info("== %s ==", table)
    rows = _all(laptop, f"""
        SELECT t.*, ca.council_slug AS _slug, ca.application_id AS _app_id,
               d.doc_oid AS _doc_oid
          FROM `{table}` t
          JOIN council_applications ca ON ca.id = t.{app_col}
          LEFT JOIN council_application_documents d ON d.id = t.{doc_col}
    """)
    payload = []; sk_app = 0
    for r in rows:
        app_id = app_map.get((r["_slug"], r["_app_id"]))
        if app_id is None:
            sk_app += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d[app_col] = app_id
        # Optional doc FK
        if d.get(doc_col) is not None and r["_doc_oid"]:
            d[doc_col] = doc_map.get((app_id, r["_doc_oid"]))  # may be None — drop the row's doc_id rather than fail
        payload.append(d)
    log.info("  resolved %d/%d (skipped app=%d)", len(payload), len(rows), sk_app)
    _insert_ignore(server, table, payload)


def etl_application_entities(laptop, server, app_map, doc_map, company_map) -> None:
    log.info("== application_entities ==")
    rows = _all(laptop, """
        SELECT ae.*, ca.council_slug AS _slug, ca.application_id AS _app_id,
               c.acn AS _co_acn, c.abn AS _co_abn, c.norm_name AS _co_norm,
               d.doc_oid AS _doc_oid
          FROM application_entities ae
          JOIN council_applications ca ON ca.id = ae.application_id
          LEFT JOIN companies c ON c.id = ae.company_id
          LEFT JOIN council_application_documents d ON d.id = ae.source_doc_id
    """)
    payload = []; sk_app = 0; sk_co = 0
    for r in rows:
        app_id = app_map.get((r["_slug"], r["_app_id"]))
        if app_id is None:
            sk_app += 1; continue
        co_id = lookup_company(company_map, r["_co_acn"], r["_co_abn"], r["_co_norm"])
        if co_id is None:
            sk_co += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["application_id"] = app_id
        d["company_id"] = co_id
        # Optional source_doc_id
        if d.get("source_doc_id") is not None and r["_doc_oid"]:
            d["source_doc_id"] = doc_map.get((app_id, r["_doc_oid"]))
        payload.append(d)
    log.info("  resolved %d/%d (skipped app=%d, co=%d)", len(payload), len(rows), sk_app, sk_co)
    _insert_ignore(server, "application_entities", payload)


def etl_doc_fingerprints(laptop, server, app_map, doc_map, company_map) -> None:
    log.info("== doc_fingerprints ==")
    rows = _all(laptop, """
        SELECT df.*, ca.council_slug AS _slug, ca.application_id AS _app_id,
               d.doc_oid AS _doc_oid,
               c.acn AS _co_acn, c.abn AS _co_abn, c.norm_name AS _co_norm
          FROM doc_fingerprints df
          JOIN council_applications ca ON ca.id = df.application_id
          JOIN council_application_documents d ON d.id = df.source_doc_id
          LEFT JOIN companies c ON c.id = df.resolved_company_id
    """)
    payload = []; sk_app = 0; sk_doc = 0
    for r in rows:
        app_id = app_map.get((r["_slug"], r["_app_id"]))
        if app_id is None:
            sk_app += 1; continue
        doc_id = doc_map.get((app_id, r["_doc_oid"]))
        if doc_id is None:
            sk_doc += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["application_id"] = app_id
        d["source_doc_id"] = doc_id
        if d.get("resolved_company_id") is not None:
            d["resolved_company_id"] = lookup_company(company_map, r["_co_acn"], r["_co_abn"], r["_co_norm"])
        payload.append(d)
    log.info("  resolved %d/%d (skipped app=%d, doc=%d)", len(payload), len(rows), sk_app, sk_doc)
    _insert_ignore(server, "doc_fingerprints", payload)


def etl_domain_realestate(laptop, server) -> None:
    log.info("== domain_properties ==")
    # raw_page_id is FK but server may have a different id. We'll set it to NULL on import — it's metadata.
    rows = _all(laptop, "SELECT * FROM domain_properties")
    cleaned = []
    for r in rows:
        d = {k: v for k, v in r.items() if k != "id"}
        d["raw_page_id"] = None  # FK auditing artifact, drop
        cleaned.append(d)
    _insert_ignore(server, "domain_properties", cleaned)

    log.info("== domain_sales ==")
    dp_map = build_domain_prop_map(server)
    rows = _all(laptop, """
        SELECT ds.*, dp.domain_property_id AS _natural_dp
          FROM domain_sales ds JOIN domain_properties dp ON dp.id = ds.domain_property_id
    """)
    payload = []; skipped = 0
    for r in rows:
        sid = dp_map.get(r["_natural_dp"])
        if sid is None:
            skipped += 1; continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["domain_property_id"] = sid
        d["raw_page_id"] = None
        payload.append(d)
    log.info("  resolved %d/%d (skipped %d)", len(payload), len(rows), skipped)
    _insert_ignore(server, "domain_sales", payload)

    log.info("== domain_listings ==")
    rows = _all(laptop, """
        SELECT dl.*, dp.domain_property_id AS _natural_dp
          FROM domain_listings dl JOIN domain_properties dp ON dp.id = dl.domain_property_id
    """)
    payload = []
    for r in rows:
        sid = dp_map.get(r["_natural_dp"])
        if sid is None: continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["domain_property_id"] = sid
        if "raw_page_id" in d: d["raw_page_id"] = None
        payload.append(d)
    _insert_ignore(server, "domain_listings", payload)

    log.info("== realestate_properties ==")
    rows = _all(laptop, "SELECT * FROM realestate_properties")
    cleaned = []
    for r in rows:
        d = {k: v for k, v in r.items() if k != "id"}
        if "raw_page_id" in d: d["raw_page_id"] = None
        cleaned.append(d)
    _insert_ignore(server, "realestate_properties", cleaned)

    log.info("== realestate_sales ==")
    rp_map = build_rea_prop_map(server)
    rows = _all(laptop, """
        SELECT rs.*, rp.rea_property_id AS _natural_rp
          FROM realestate_sales rs JOIN realestate_properties rp ON rp.id = rs.realestate_property_id
    """)
    payload = []
    for r in rows:
        sid = rp_map.get(r["_natural_rp"])
        if sid is None: continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["realestate_property_id"] = sid
        if "raw_page_id" in d: d["raw_page_id"] = None
        payload.append(d)
    _insert_ignore(server, "realestate_sales", payload)

    log.info("== realestate_listings ==")
    rows = _all(laptop, """
        SELECT rl.*, rp.rea_property_id AS _natural_rp
          FROM realestate_listings rl JOIN realestate_properties rp ON rp.id = rl.realestate_property_id
    """)
    payload = []
    for r in rows:
        sid = rp_map.get(r["_natural_rp"])
        if sid is None: continue
        d = {k: v for k, v in r.items() if not k.startswith("_") and k != "id"}
        d["realestate_property_id"] = sid
        if "raw_page_id" in d: d["raw_page_id"] = None
        payload.append(d)
    _insert_ignore(server, "realestate_listings", payload)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated step names (defaults to all)")
    args = ap.parse_args()
    only = set(args.only.split(",")) if args.only else None

    laptop = create_engine(LAPTOP_URL, future=True)
    server = create_engine(SERVER_URL, future=True)

    with server.connect() as c:
        v = c.execute(text("SELECT version_num FROM alembic_version")).scalar()
        log.info("server alembic_version = %s", v)
        if v != "0022":
            log.error("server is not at 0022 — aborting"); sys.exit(2)

    def step(name: str, fn):
        if only and name not in only:
            return
        t0 = time.time()
        fn()
        log.info("== %s done in %.1fs ==", name, time.time() - t0)

    # Phase 1 — parents (no FK or natural-key only)
    step("council_applications", lambda: etl_simple(laptop, server, "council_applications"))
    app_map = build_app_map(server)
    log.info("server now has %d council_applications", len(app_map))

    step("companies", lambda: etl_simple(laptop, server, "companies"))
    company_map = build_company_map(server)
    log.info("server now has %d companies natural keys", len(company_map))

    step("mortgage_rates", lambda: etl_simple(laptop, server, "mortgage_rates"))

    # Phase 2 — documents (FK to council_apps)
    step("council_application_documents",
         lambda: etl_council_application_documents(laptop, server, app_map))
    doc_map = build_doc_map(server)
    log.info("server now has %d council_application_documents", len(doc_map))

    # Phase 3 — domain/realestate (separate FK universe, no council link)
    step("domain_realestate", lambda: etl_domain_realestate(laptop, server))

    # Phase 4 — discovered_urls (no FK)
    step("discovered_urls", lambda: etl_simple(laptop, server, "discovered_urls"))

    # Phase 5 — child tables with council/doc/company FKs
    step("da_summaries", lambda: etl_da_summaries(laptop, server, app_map, company_map))

    step("da_build_features", lambda: etl_with_app_and_doc_fk_optional(
        laptop, server, app_map, doc_map, "da_build_features",
        app_col="application_id", doc_col="document_id"))

    step("da_doc_summaries", lambda: etl_with_app_and_doc_fk_optional(
        laptop, server, app_map, doc_map, "da_doc_summaries",
        app_col="application_id", doc_col="document_id"))

    step("document_features", lambda: etl_with_doc_fk(
        laptop, server, app_map, doc_map, "document_features", doc_fk_col="document_id"))

    step("entity_evidence", lambda: etl_with_doc_fk(
        laptop, server, app_map, doc_map, "entity_evidence", doc_fk_col="source_doc_id"))

    step("doc_fingerprints", lambda: etl_doc_fingerprints(
        laptop, server, app_map, doc_map, company_map))

    step("application_entities", lambda: etl_application_entities(
        laptop, server, app_map, doc_map, company_map))

    # Phase 6 — raw_pages (large; runs last so we don't block earlier verification)
    step("raw_pages", lambda: etl_streamed(laptop, server, "raw_pages", page=500))

    log.info("ETL done.")


if __name__ == "__main__":
    main()
