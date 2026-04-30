"""Phase 4 — Google-search builder/architect names; persist canonical websites.

Reuses `property_history.search.google_search` (already throttled at
8s/query and battle-tested for property URL discovery via the running
`:9222` CDP Chrome). For each unique (business_name, role) seen in
`da_summaries`, we run one Google search and pick the highest-confidence
candidate URL.

Skipping policy: any (business_name, role) already in `business_links`
is skipped (the unique key prevents accidental overwrites).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

from sqlalchemy import select, text as sql_text

from listo.db import session_scope
from listo.models import BusinessLink
from listo.property_history import search as ph_search


logger = logging.getLogger(__name__)


# Directory / aggregator domains — penalised in scoring.
DIRECTORY_DOMAINS = {
    "truelocal.com.au", "yellowpages.com.au", "hotfrog.com.au",
    "yelp.com.au", "oneflare.com.au", "australia.com",
    "localsearch.com.au", "businesslistingsaustralia.com",
    "searchfrog.com.au", "australianbusinessesonline.com.au",
    "homely.com.au", "houzz.com.au",  # platform listings, not the firm's own site
}
SOCIAL_DOMAINS = {
    "linkedin.com", "facebook.com", "instagram.com", "twitter.com",
    "x.com", "youtube.com", "tiktok.com",
}
QBCC_DOMAINS = {"qbcc.qld.gov.au", "myqbcc.qld.gov.au"}


@dataclass
class BusinessStats:
    queried: int = 0
    persisted_high: int = 0
    persisted_medium: int = 0
    persisted_low: int = 0
    persisted_no_match: int = 0


# ---------- helpers ----------


_NORM_RE = re.compile(r"[^a-z0-9]+")


def _normalise_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    return _NORM_RE.sub(" ", name.lower()).strip()


def _slug(name: str) -> str:
    return _NORM_RE.sub("", name.lower())


def _domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:  # noqa: BLE001
        return ""
    return host[4:] if host.startswith("www.") else host


def _classify_url(url: str) -> str:
    d = _domain(url)
    if d in SOCIAL_DOMAINS or any(d.endswith("." + s) for s in SOCIAL_DOMAINS):
        return "linkedin" if "linkedin" in d else ("facebook" if "facebook" in d else "other")
    if d in QBCC_DOMAINS:
        return "qbcc"
    if d.endswith(".com.au"):
        return "website"
    return "other"


def _score_candidate(url: str, *, business_name: str, role: str) -> int:
    d = _domain(url)
    if not d:
        return -10
    score = 0

    # Strong signal: domain contains a normalised slug of the business name
    name_slug = _slug(business_name)
    if name_slug and name_slug[:12] in d.replace(".", ""):
        score += 3

    # Role keywords in the domain
    role_keywords: dict[str, list[str]] = {
        "builder": ["build", "construct", "homes", "developments", "projects"],
        "architect": ["architect", "design", "studio", "drafting"],
        "applicant": [],
    }
    for kw in role_keywords.get(role, []):
        if kw in d:
            score += 2
            break

    # .com.au bias
    if d.endswith(".com.au"):
        score += 1

    # Penalties
    if d in DIRECTORY_DOMAINS:
        score -= 2
    if d in SOCIAL_DOMAINS or any(d.endswith("." + s) for s in SOCIAL_DOMAINS):
        score -= 1  # social isn't ideal but isn't terrible

    return score


def _confidence(score: int) -> str:
    if score >= 5:
        return "high"
    if score >= 3:
        return "medium"
    if score >= 1:
        return "low"
    return "no_match"


def _city_hint_for(business_name: str, role: str) -> str:
    """Most common suburb across DAs that name this business in this role."""
    col = "builder_name" if role == "builder" else "architect_name"
    with session_scope() as s:
        row = s.execute(sql_text(f"""
            SELECT ca.suburb, COUNT(*) AS n
              FROM da_summaries ds
              JOIN council_applications ca ON ca.id = ds.application_id
             WHERE ds.{col} = :name AND ca.suburb IS NOT NULL AND ca.suburb <> ''
             GROUP BY ca.suburb
             ORDER BY n DESC
             LIMIT 1
        """), {"name": business_name}).fetchone()
    return (row.suburb if row else "") or ""


# ---------- main ----------


def _select_pending(
    *,
    council_slug: str | None,
    limit: int | None,
    computer_index: int,
    computer_count: int,
    force: bool,
) -> list[tuple[str, str]]:
    """Return [(display_name, role), ...] not yet in business_links."""
    where = []
    params: dict[str, object] = {"count": computer_count, "index": computer_index}
    if council_slug:
        where.append("ca.council_slug = :slug")
        params["slug"] = council_slug

    where_sql = (" AND " + " AND ".join(where)) if where else ""

    # MD5 partition over the *normalised* name so the same business
    # always lands on the same machine regardless of how it's spelled
    # in different DAs.
    sql = sql_text(f"""
        SELECT DISTINCT name, role FROM (
          SELECT TRIM(ds.builder_name) AS name, 'builder' AS role,
                 LOWER(TRIM(ds.builder_name)) AS norm
            FROM da_summaries ds
            JOIN council_applications ca ON ca.id = ds.application_id
           WHERE ds.builder_name IS NOT NULL AND ds.builder_name <> ''
                 {where_sql}
          UNION ALL
          SELECT TRIM(ds.architect_name) AS name, 'architect' AS role,
                 LOWER(TRIM(ds.architect_name)) AS norm
            FROM da_summaries ds
            JOIN council_applications ca ON ca.id = ds.application_id
           WHERE ds.architect_name IS NOT NULL AND ds.architect_name <> ''
                 {where_sql}
        ) u
        WHERE CONV(SUBSTRING(MD5(u.norm), 1, 8), 16, 10) %% :count = :index
          {"" if force else "AND NOT EXISTS (SELECT 1 FROM business_links bl WHERE bl.business_name = u.norm AND bl.business_role = u.role)"}
        ORDER BY u.name
        {f'LIMIT {int(limit)}' if limit else ''}
    """)
    with session_scope() as s:
        rows = s.execute(sql, params).fetchall()
        return [(r.name, r.role) for r in rows]


def run(
    *,
    council_slug: str | None = None,
    limit: int | None = None,
    force: bool = False,
    computer_index: int = 0,
    computer_count: int = 1,
) -> BusinessStats:
    stats = BusinessStats()
    pending = _select_pending(
        council_slug=council_slug,
        limit=limit,
        computer_index=computer_index,
        computer_count=computer_count,
        force=force,
    )
    logger.info("phase 4: %d businesses to look up (machine %d/%d)",
                len(pending), computer_index, computer_count)

    for display_name, role in pending:
        norm = _normalise_name(display_name)
        if not norm:
            continue
        city_hint = _city_hint_for(display_name, role)
        query = f'site:.com.au "{display_name}"' + (f" {city_hint}" if city_hint else "")

        try:
            res = ph_search.google_search(query)
        except Exception as exc:  # noqa: BLE001
            logger.warning("google_search failed for %r: %s", display_name, exc)
            continue
        stats.queried += 1

        scored = [
            {"url": url, "domain": _domain(url), "score": _score_candidate(url, business_name=display_name, role=role)}
            for url in res.urls[:10]
        ]
        scored.sort(key=lambda x: x["score"], reverse=True)
        top = scored[0] if scored else None
        confidence = _confidence(top["score"]) if top else "no_match"

        url = top["url"] if (top and confidence != "no_match") else None
        url_kind = _classify_url(url) if url else None

        with session_scope() as s:
            # Upsert (delete + insert; the unique key is on normalised name + role).
            s.execute(sql_text("""
                DELETE FROM business_links
                 WHERE business_name = :norm AND business_role = :role
            """), {"norm": norm, "role": role})
            s.add(BusinessLink(
                business_name=norm,
                display_name=display_name[:255],
                business_role=role,
                url=url,
                url_kind=url_kind,
                search_query=query[:255],
                search_engine="google",
                confidence=confidence,
                candidates_json={"top": scored[:5]} if scored else None,
                discovered_at=datetime.utcnow(),
            ))
        if confidence == "high":
            stats.persisted_high += 1
        elif confidence == "medium":
            stats.persisted_medium += 1
        elif confidence == "low":
            stats.persisted_low += 1
        else:
            stats.persisted_no_match += 1
        logger.info("  [%s] %s — %s (%s)", role, display_name, url or "(no match)", confidence)

    return stats
