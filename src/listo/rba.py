"""RBA F5 'Indicator Lending Rates' ingest.

The CSV format (https://www.rba.gov.au/statistics/tables/csv/f5-data.csv):
    Row 0 :  "F5 INDICATOR LENDING RATES"           (banner)
    Row 1 :  "Title,<series labels comma-separated>"
    Row 2 :  "Description,..."
    Row 3 :  "Frequency,Monthly,Monthly,..."
    Row 4 :  "Type,Original,Original,..."
    Row 5 :  "Units,Per cent per annum,..."
    Row 6 :  blank
    Row 7 :  "Source,RBA,RBA,..."
    Row 8 :  "Publication date,..."
    Row 9 :  "Series ID,FILRSBVRT,FILRSBVOO,..."
    Row 10:  blank
    Row 11+: data rows: "DD/MM/YYYY,<rate>,<rate>,..." (empty cells where series didn't exist)

The first column of data rows is an end-of-month date (e.g. 31/01/1959).

This module downloads the CSV, parses it, and bulk-inserts rows into the
mortgage_rates table. Idempotent — re-running just upserts existing months.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import date, datetime
from io import StringIO

import httpx
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from listo.db import session_scope
from listo.models import MortgageRate

logger = logging.getLogger(__name__)

RBA_F5_URL = "https://www.rba.gov.au/statistics/tables/csv/f5-data.csv"


@dataclass
class IngestStats:
    series: int = 0
    months: int = 0
    rows_upserted: int = 0
    skipped_blank: int = 0


def _download_csv(url: str = RBA_F5_URL) -> str:
    """Fetch the F5 CSV. Plain HTTP — RBA serves it without bot defense."""
    resp = httpx.get(url, timeout=30, follow_redirects=True)
    resp.raise_for_status()
    body = resp.text
    if not body.lstrip().startswith("﻿") and "INDICATOR LENDING RATES" not in body[:200]:
        raise RuntimeError("Unexpected response — RBA may have changed the URL or returned HTML")
    return body


def _parse_csv(body: str) -> tuple[list[str], list[str], list[tuple[date, list[str]]]]:
    """Parse the F5 CSV into (series_ids, series_labels, [(month, raw_values_per_series)])."""
    reader = csv.reader(StringIO(body))
    rows = list(reader)
    if not rows:
        raise RuntimeError("empty CSV")

    # Locate header rows by their leading-cell label rather than index — RBA
    # has been known to insert / remove blank lines.
    by_label: dict[str, list[str]] = {}
    data_start: int | None = None
    for i, row in enumerate(rows):
        if not row:
            continue
        head = row[0].strip().lstrip("﻿")
        if head and not head[0].isdigit():
            by_label[head] = row
            continue
        if "/" in head and head[0].isdigit():
            data_start = i
            break

    if data_start is None:
        raise RuntimeError("Couldn't find data start in F5 CSV")
    if "Series ID" not in by_label or "Title" not in by_label:
        raise RuntimeError("Missing 'Series ID' or 'Title' header row")

    series_ids = by_label["Series ID"][1:]
    series_labels = by_label["Title"][1:]
    while len(series_labels) < len(series_ids):
        series_labels.append("")

    data: list[tuple[date, list[str]]] = []
    for row in rows[data_start:]:
        if not row or not row[0].strip():
            continue
        try:
            d = datetime.strptime(row[0].strip(), "%d/%m/%Y").date()
        except ValueError:
            continue  # malformed date — skip
        # Pad/trim to series length
        values = list(row[1:])
        if len(values) < len(series_ids):
            values.extend([""] * (len(series_ids) - len(values)))
        data.append((d, values[: len(series_ids)]))

    return series_ids, series_labels, data


def ingest(url: str = RBA_F5_URL) -> IngestStats:
    """Download F5 CSV and upsert into mortgage_rates."""
    logger.info("downloading RBA F5 from %s", url)
    body = _download_csv(url)
    series_ids, series_labels, data = _parse_csv(body)
    label_for = dict(zip(series_ids, series_labels))
    logger.info("parsed %d series × %d months", len(series_ids), len(data))

    stats = IngestStats(series=len(series_ids), months=len(data))
    now = datetime.utcnow()
    chunk: list[dict] = []
    CHUNK_SIZE = 1000

    def flush() -> None:
        if not chunk:
            return
        with session_scope() as s:
            insert_fn = mysql_insert if s.bind.dialect.name == "mysql" else sqlite_insert
            stmt = insert_fn(MortgageRate).values(chunk)
            if s.bind.dialect.name == "mysql":
                stmt = stmt.on_duplicate_key_update(
                    series_label=stmt.inserted.series_label,
                    rate_pct=stmt.inserted.rate_pct,
                    fetched_at=stmt.inserted.fetched_at,
                )
            else:
                stmt = stmt.on_conflict_do_update(
                    index_elements=["series_id", "month"],
                    set_=dict(
                        series_label=stmt.excluded.series_label,
                        rate_pct=stmt.excluded.rate_pct,
                        fetched_at=stmt.excluded.fetched_at,
                    ),
                )
            s.execute(stmt)
        stats.rows_upserted += len(chunk)
        chunk.clear()

    for month, values in data:
        for sid, raw in zip(series_ids, values):
            if not raw or not raw.strip():
                stats.skipped_blank += 1
                continue
            try:
                rate = float(raw.strip())
            except ValueError:
                stats.skipped_blank += 1
                continue
            chunk.append({
                "series_id": sid,
                "series_label": label_for.get(sid, "")[:255],
                "month": month,
                "rate_pct": rate,
                "source": "rba_f5",
                "fetched_at": now,
            })
            if len(chunk) >= CHUNK_SIZE:
                flush()
    flush()
    logger.info("upserted %d rows (skipped %d blank cells)", stats.rows_upserted, stats.skipped_blank)
    return stats


# Series ID convenience constants — used by the financial model.
#
# IMPORTANT: use the DISCOUNTED series, not the STANDARD series, when modelling
# what a real customer paid. "Standard Variable Rate" (SVR) is the published
# reference rate banks list on their websites — almost nobody actually pays it
# because every package deal discounts 1.5-2.5% off SVR. The Discounted series
# is the cross-bank average of what owner-occupiers actually pay.
OO_VARIABLE_DISCOUNTED = "FILRHLBVD"   # Owner-occupier discounted variable — DEFAULT for the financial model
OO_3YR_FIXED = "FILRHL3YF"             # Owner-occupier 3-year fixed
INV_VARIABLE_DISCOUNTED = "FILRHLBVDI" # Investor discounted variable
INV_3YR_FIXED = "FILRHL3YFI"           # Investor 3-year fixed
OO_INTEREST_ONLY = "FILRHLBVO"         # Owner-occupier IO — relevant for construction/holding periods

# The published reference rates — kept here for completeness but rarely the
# right number to use in modeling.
OO_VARIABLE_STANDARD = "FILRHLBVS"
INV_VARIABLE_STANDARD = "FILRHLBVSI"
