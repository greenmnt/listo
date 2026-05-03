"""Track Domain/REA direct-slug fetch outcomes per address.

Lets `_drop_already_scraped` distinguish "we never tried this address"
from "we tried and Domain/REA genuinely has no profile for it" — so
addresses Domain doesn't index don't get re-attempted on every batch.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import bindparam, text as sql_text

from listo.db import session_scope


_VALID_SOURCES = ("domain", "realestate")
_VALID_RESULTS = ("found", "not_found", "error")


def record_attempt(
    *,
    source: str,
    display_address: str,
    url: str,
    http_status: int | None,
    result: str,
    error_message: str | None = None,
) -> None:
    """Upsert a row into property_scrape_attempts.

    `display_address` should be the freeform address the orchestrator
    is fetching for (NOT the parsed display_address from the response,
    which only exists on success). Keyed on (source, address) — every
    re-run overwrites the prior attempt for the same address.
    """
    if source not in _VALID_SOURCES:
        raise ValueError(f"unknown source {source!r}, want one of {_VALID_SOURCES}")
    if result not in _VALID_RESULTS:
        raise ValueError(f"unknown result {result!r}, want one of {_VALID_RESULTS}")
    if not display_address:
        return  # nothing to record against
    with session_scope() as s:
        s.execute(sql_text("""
            INSERT INTO property_scrape_attempts
              (source, display_address, url, http_status, result,
               error_message, attempted_at)
            VALUES
              (:source, :display_address, :url, :http_status, :result,
               :error_message, :attempted_at)
            ON DUPLICATE KEY UPDATE
              url = VALUES(url),
              http_status = VALUES(http_status),
              result = VALUES(result),
              error_message = VALUES(error_message),
              attempted_at = VALUES(attempted_at)
        """), {
            "source": source,
            "display_address": display_address[:255],
            "url": url[:1024],
            "http_status": http_status,
            "result": result,
            "error_message": (error_message or "")[:500] or None,
            "attempted_at": datetime.utcnow(),
        })


def addresses_with_attempt(*, source: str, results: tuple[str, ...]) -> list[str]:
    """Return all `display_address` values that have an attempt with
    one of `results` for the given source. Used by `_drop_already_scraped`
    to treat 'not_found' attempts as covered.
    """
    if source not in _VALID_SOURCES:
        raise ValueError(f"unknown source {source!r}")
    if not results:
        return []
    with session_scope() as s:
        rows = s.execute(
            sql_text("""
                SELECT display_address FROM property_scrape_attempts
                 WHERE source = :source AND result IN :results
            """).bindparams(bindparam("results", expanding=True)),
            {"source": source, "results": list(results)},
        ).fetchall()
    return [r[0] for r in rows if r[0]]
