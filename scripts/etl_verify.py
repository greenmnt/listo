#!/usr/bin/env python3
"""Post-merge verification: confirm the laptop→server ETL succeeded.

Checks (each printed PASS / FAIL with counts):

  FK1. Every FK column on the merged child tables resolves to a real
       parent row (no dangling references).
  FK2. For tables with a natural key (council_slug+application_id, or
       acn/abn/norm_name, or doc_oid+app_id), each merged row's resolved
       parent matches what it pointed to on the laptop. Catches the
       FK-remap bug we hit on the first attempt.
  CN.  Cardinality — server has at least as many rows as laptop in each
       merged table (server = laptop ∪ existing pre-merge rows).
  GATE. The duplex-frontend gate query (api/src/service/applications.rs
        list_applications, HAVING pre_price IS NOT NULL) returns a
        non-zero count and matches what laptop returns.

Run with the SSH tunnel up:

    ./scripts/server-tunnel.sh &
    uv run python scripts/etl_verify.py

Exits with status 1 if any check fails, 0 otherwise.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass

from sqlalchemy import create_engine, text


LAPTOP_URL = "mysql+pymysql://listo:password@127.0.0.1/listo?charset=utf8mb4"
SERVER_URL = "mysql+pymysql://listo:password@127.0.0.1:3307/listo?charset=utf8mb4"


@dataclass
class Result:
    name: str
    ok: bool
    detail: str


results: list[Result] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append(Result(name, ok, detail))
    marker = "PASS" if ok else "FAIL"
    print(f"  [{marker}] {name}{(' — ' + detail) if detail else ''}")


def scalar(eng, sql, **params):
    with eng.connect() as c:
        return c.execute(text(sql), params).scalar()


def all_rows(eng, sql, **params):
    with eng.connect() as c:
        return [dict(r._mapping) for r in c.execute(text(sql), params).fetchall()]


def main() -> int:
    laptop = create_engine(LAPTOP_URL, future=True)
    server = create_engine(SERVER_URL, future=True)

    print("\n=== FK integrity (no dangling references) ===\n")

    # Each tuple: (child_table, child_fk_col, parent_table, parent_pk_col, allow_null)
    fk_checks = [
        ("da_summaries",                  "application_id",          "council_applications",          "id", False),
        ("da_summaries",                  "applicant_company_id",    "companies",                     "id", True),
        ("da_summaries",                  "applicant_agent_company_id","companies",                   "id", True),
        ("da_summaries",                  "builder_company_id",      "companies",                     "id", True),
        ("da_summaries",                  "architect_company_id",    "companies",                     "id", True),
        ("da_summaries",                  "owner_company_id",        "companies",                     "id", True),
        ("da_doc_summaries",              "application_id",          "council_applications",          "id", False),
        ("da_doc_summaries",              "document_id",             "council_application_documents", "id", True),
        ("da_build_features",             "application_id",          "council_applications",          "id", False),
        ("da_build_features",             "document_id",             "council_application_documents", "id", True),
        ("entity_evidence",               "application_id",          "council_applications",          "id", False),
        ("entity_evidence",               "source_doc_id",           "council_application_documents", "id", False),
        ("doc_fingerprints",              "application_id",          "council_applications",          "id", False),
        ("doc_fingerprints",              "source_doc_id",           "council_application_documents", "id", False),
        ("doc_fingerprints",              "resolved_company_id",     "companies",                     "id", True),
        ("application_entities",          "application_id",          "council_applications",          "id", False),
        ("application_entities",          "company_id",              "companies",                     "id", False),
        ("application_entities",          "source_doc_id",           "council_application_documents", "id", True),
        ("council_application_documents", "application_id",          "council_applications",          "id", False),
        ("document_features",             "document_id",             "council_application_documents", "id", False),
        ("domain_properties",             "raw_page_id",             "raw_pages",                     "id", False),
        ("domain_properties",             "property_id",             "properties",                    "id", True),
        ("domain_sales",                  "domain_property_id",      "domain_properties",             "id", False),
        ("domain_sales",                  "raw_page_id",             "raw_pages",                     "id", False),
        ("domain_sales",                  "property_id",             "properties",                    "id", True),
        ("domain_listings",               "domain_property_id",      "domain_properties",             "id", False),
        ("domain_listings",               "raw_page_id",             "raw_pages",                     "id", False),
        ("realestate_properties",         "raw_page_id",             "raw_pages",                     "id", False),
        ("realestate_properties",         "property_id",             "properties",                    "id", True),
        ("realestate_sales",              "realestate_property_id",  "realestate_properties",         "id", False),
        ("realestate_sales",              "raw_page_id",             "raw_pages",                     "id", False),
        ("realestate_sales",              "property_id",             "properties",                    "id", True),
        ("realestate_listings",           "realestate_property_id",  "realestate_properties",         "id", False),
        ("realestate_listings",           "raw_page_id",             "raw_pages",                     "id", False),
    ]

    for child, fk, parent, pk, allow_null in fk_checks:
        sql = f"""
            SELECT COUNT(*) FROM `{child}` c
            LEFT JOIN `{parent}` p ON p.`{pk}` = c.`{fk}`
            WHERE c.`{fk}` IS NOT NULL AND p.`{pk}` IS NULL
        """
        try:
            dangling = scalar(server, sql)
        except Exception as e:
            check(f"{child}.{fk} -> {parent}.{pk}", False, f"query error: {e!r}")
            continue
        total = scalar(server, f"SELECT COUNT(*) FROM `{child}`") or 0
        non_null = scalar(server,
            f"SELECT COUNT(*) FROM `{child}` WHERE `{fk}` IS NOT NULL") or 0
        ok = (dangling == 0)
        detail = f"{dangling} dangling / {non_null} non-null / {total} total"
        check(f"{child}.{fk} -> {parent}.{pk}", ok, detail)

    print("\n=== Natural-key consistency (laptop vs server) ===\n")

    # da_summaries: each row's resolved (council_slug, application_id) should
    # match what the laptop's row resolved to. This is the key check that
    # catches the FK-remap corruption we hit the first time.
    laptop_da = all_rows(laptop, """
        SELECT ds.application_id AS lap_pk, ca.council_slug, ca.application_id AS app_id
          FROM da_summaries ds JOIN council_applications ca ON ca.id = ds.application_id
    """)
    laptop_natural = {(r["council_slug"], r["app_id"]) for r in laptop_da}

    server_da = all_rows(server, """
        SELECT ca.council_slug, ca.application_id AS app_id, ds.application_id AS srv_pk
          FROM da_summaries ds JOIN council_applications ca ON ca.id = ds.application_id
    """)
    server_natural = {(r["council_slug"], r["app_id"]) for r in server_da}

    missing = laptop_natural - server_natural
    extra   = server_natural - laptop_natural
    check("da_summaries: laptop ⊆ server (natural keys)",
          len(missing) == 0,
          f"{len(missing)} laptop natural keys missing on server" + (f" e.g. {list(missing)[:3]}" if missing else ""))

    # entity_evidence: laptop's (slug, app_id, source_doc.doc_oid) tuples ⊆ server's.
    print()
    laptop_ee = all_rows(laptop, """
        SELECT ca.council_slug, ca.application_id AS app_id, d.doc_oid
          FROM entity_evidence e
          JOIN council_application_documents d ON d.id = e.source_doc_id
          JOIN council_applications ca ON ca.id = d.application_id
    """)
    laptop_ee_set = {(r["council_slug"], r["app_id"], r["doc_oid"]) for r in laptop_ee}
    server_ee = all_rows(server, """
        SELECT ca.council_slug, ca.application_id AS app_id, d.doc_oid
          FROM entity_evidence e
          JOIN council_application_documents d ON d.id = e.source_doc_id
          JOIN council_applications ca ON ca.id = d.application_id
    """)
    server_ee_set = {(r["council_slug"], r["app_id"], r["doc_oid"]) for r in server_ee}
    missing_ee = laptop_ee_set - server_ee_set
    check("entity_evidence: laptop ⊆ server (natural keys)",
          len(missing_ee) == 0,
          f"{len(missing_ee)} missing")

    # doc_fingerprints: same shape
    laptop_df = all_rows(laptop, """
        SELECT ca.council_slug, ca.application_id AS app_id, d.doc_oid, df.fingerprint_kind, df.normalized_value
          FROM doc_fingerprints df
          JOIN council_application_documents d ON d.id = df.source_doc_id
          JOIN council_applications ca ON ca.id = d.application_id
    """)
    laptop_df_set = {(r["council_slug"], r["app_id"], r["doc_oid"], r["fingerprint_kind"], r["normalized_value"]) for r in laptop_df}
    server_df = all_rows(server, """
        SELECT ca.council_slug, ca.application_id AS app_id, d.doc_oid, df.fingerprint_kind, df.normalized_value
          FROM doc_fingerprints df
          JOIN council_application_documents d ON d.id = df.source_doc_id
          JOIN council_applications ca ON ca.id = d.application_id
    """)
    server_df_set = {(r["council_slug"], r["app_id"], r["doc_oid"], r["fingerprint_kind"], r["normalized_value"]) for r in server_df}
    missing_df = laptop_df_set - server_df_set
    check("doc_fingerprints: laptop ⊆ server (natural keys)",
          len(missing_df) == 0,
          f"{len(missing_df)} missing")

    print("\n=== Cardinality (server ≥ laptop for merged tables) ===\n")

    cardinality_tables = [
        "council_applications", "council_application_documents",
        "companies", "mortgage_rates", "raw_pages",
        "domain_properties", "domain_sales", "domain_listings",
        "realestate_properties", "realestate_sales", "realestate_listings",
        "discovered_urls", "properties",
        "da_summaries", "da_doc_summaries", "da_build_features",
        "document_features", "entity_evidence", "doc_fingerprints",
        "application_entities",
    ]
    for t in cardinality_tables:
        try:
            l = scalar(laptop, f"SELECT COUNT(*) FROM `{t}`")
            s = scalar(server, f"SELECT COUNT(*) FROM `{t}`")
        except Exception as e:
            check(f"cardinality: {t}", False, f"error: {e!r}")
            continue
        ok = (s >= l)
        check(f"cardinality: {t}", ok, f"laptop={l} server={s} (delta={s-l:+d})")

    print("\n=== Frontend duplex gate (HAVING pre_price IS NOT NULL) ===\n")

    gate_sql = """
        SELECT COUNT(*) FROM (
          SELECT
            (SELECT s.event_price FROM domain_sales s
              JOIN domain_properties dp ON dp.id = s.domain_property_id
             WHERE dp.display_address LIKE CONCAT(ds.street_address, '%')
               AND s.event_date < ca.lodged_date
               AND s.event_date >= DATE_SUB(ca.lodged_date, INTERVAL 10 YEAR)
               AND s.is_sold = 1 AND s.event_price IS NOT NULL
             ORDER BY s.event_date DESC LIMIT 1) AS pre_price_d,
            (SELECT s.event_price FROM realestate_sales s
              JOIN realestate_properties rp ON rp.id = s.realestate_property_id
             WHERE rp.display_address LIKE CONCAT(ds.street_address, '%')
               AND s.event_date < ca.lodged_date
               AND s.event_date >= DATE_SUB(ca.lodged_date, INTERVAL 10 YEAR)
               AND s.event_type = 'sold' AND s.event_price IS NOT NULL
             ORDER BY s.event_date DESC LIMIT 1) AS pre_price_r
          FROM council_applications ca
          LEFT JOIN da_summaries ds ON ds.application_id = ca.id
          WHERE (ca.description REGEXP '(?i)dual[[:space:]]+occupancy|duplex'
                 AND ca.description NOT REGEXP '(?i)triplex|fourplex|quadruplex|multi[[:space:]-]+unit|multi[[:space:]-]+dwelling')
        ) sub
        WHERE sub.pre_price_d IS NOT NULL OR sub.pre_price_r IS NOT NULL
    """
    try:
        l_gate = scalar(laptop, gate_sql)
        s_gate = scalar(server, gate_sql)
    except Exception as e:
        check("duplex frontend gate", False, f"query error: {e!r}")
    else:
        ok = (s_gate is not None and s_gate > 0 and s_gate >= l_gate)
        check("duplex frontend gate (laptop)", l_gate is not None and l_gate > 0,
              f"{l_gate} duplex DAs with pre_price")
        check("duplex frontend gate (server)", ok,
              f"{s_gate} duplex DAs with pre_price (laptop has {l_gate})")

    print()
    failed = [r for r in results if not r.ok]
    if failed:
        print(f"\nFAILED: {len(failed)}/{len(results)} checks failed.\n")
        for r in failed:
            print(f"  - {r.name}: {r.detail}")
        return 1
    print(f"\nALL PASS ({len(results)} checks).\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
