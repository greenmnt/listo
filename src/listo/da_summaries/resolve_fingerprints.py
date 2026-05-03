"""Resolve unbound URL / email fingerprints to named companies by
cross-referencing other docs in the same application.

The pattern: an architect's plan PDF carries only their logo (image)
plus a URL + email + licence in text. The Supporting Documents,
Decision Report, or Specialist Report for the same DA usually spells
out the firm name in prose ("Plans prepared by Boris Design", "Boris
Design – Architect", etc.).

This resolver:
  1. Reads each unresolved URL/email fingerprint.
  2. Derives a firm-word from the domain (`borisdesign.com.au` → `borisdesign`).
  3. Generates candidate spellings (raw, titlecased, split at known
     company-marker suffixes: `boris design`, `boris-design`, etc.).
  4. Searches the cached `extracted_text` of every OTHER doc in the
     same application for any candidate.
  5. If a candidate matches, upserts it as a `companies` row and writes
     `resolved_company_id` + `resolved_via='cross_doc_match'` back to
     every fingerprint with that domain in that app.

Docs whose `extracted_text` is NULL are extracted on the fly via
`extract_text_for_prompt` (and cached). The resolver therefore extends
text coverage to non-plan/non-correspondence docs (Supporting Documents,
Specialist Reports, Decision Reports) as a side effect — these are
exactly the docs whose text we need for entity resolution.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text as sql_text

from listo.da_summaries.aggregate import _upsert_company
from listo.da_summaries.text_extract import extract_text_for_prompt
from listo.db import session_scope


logger = logging.getLogger(__name__)


# Suffixes that often terminate a domain firm-word and mark a word
# boundary. `borisdesign` → `boris` + `design` because `design` is a
# known suffix.
_DOMAIN_SUFFIX_WORDS = (
    "architecture", "architects", "architect",
    "design", "designs", "designer", "designers",
    "studio", "studios",
    "planning", "planners",
    "consulting", "consultancy", "consultants",
    "homes", "constructions", "construction", "builders",
    "group", "holdings", "developments", "development",
    "drafting", "drafts",
    "surveying", "surveyors", "surveys", "survey",
    "engineering", "engineers", "engineer",
)


# Generic email hosts — when an email's domain is one of these the
# domain itself carries no firm signal; the local part is more likely
# to. `auskidesign@gmail.com` → seed = `auskidesign`.
_GENERIC_EMAIL_HOSTS = frozenset({
    "gmail.com", "googlemail.com",
    "hotmail.com", "hotmail.com.au", "hotmail.co.uk",
    "yahoo.com", "yahoo.com.au",
    "outlook.com", "outlook.com.au", "live.com", "live.com.au", "msn.com",
    "icloud.com", "me.com", "mac.com",
    "bigpond.com", "bigpond.com.au", "bigpond.net.au", "bigpond.net",
    "iinet.net.au", "internode.on.net", "tpg.com.au",
    "optusnet.com.au", "optus.com.au",
    "westnet.com.au", "dodo.com.au",
})

# Mailbox-style local parts that carry no firm signal — skip when the
# domain is generic (no signal anywhere) or in addition to using the
# domain when it isn't.
_GENERIC_EMAIL_LOCALS = frozenset({
    "info", "admin", "administrator", "contact", "hello", "mail",
    "email", "sales", "support", "office", "enquiries", "enquiry",
    "team", "staff", "reception", "general", "noreply", "no-reply",
})


def _strip_tld(domain: str) -> str:
    """borisdesign.com.au → borisdesign. metricon.com.au → metricon."""
    d = domain.lower()
    for tld in (".com.au", ".net.au", ".org.au", ".com", ".net", ".org"):
        if d.endswith(tld):
            d = d[: -len(tld)]
            break
    # Take the first segment (strip subdomains).
    return d.split(".")[0]


def _name_candidates(firm_word: str) -> list[str]:
    """Generate likely spellings of a firm name from a domain word.

    `borisdesign`        → ['Boris Design', ...]
    `bradruddelldesign`  → ['Bradruddell Design', 'Br Adruddell Design',
                            'Bra Druddell Design', 'Brad Ruddell Design', …]
    `metricon`           → ['Me Tricon', ..., 'Metricon']

    For domains ending in a known company suffix (design, architects,
    homes, …), we ONLY emit multi-word forms — falling back to a
    concatenated `Bradruddelldesign` would create a junk company. For
    domains without a known suffix (e.g. `metricon`), we fall back to
    the bare title-cased form so genuine single-word firms still
    resolve.

    Order: most-likely-canonical first; first sibling-doc match wins.
    """
    word = firm_word.lower().replace("-", "")
    if len(word) < 4:
        return []

    out: list[str] = []
    has_known_suffix = False

    for suf in _DOMAIN_SUFFIX_WORDS:
        if word.endswith(suf) and len(word) > len(suf):
            has_known_suffix = True
            prefix = word[: -len(suf)]
            # Primary: prefix + suffix as two title-cased words.
            out.append(f"{prefix.title()} {suf.title()}")
            # Secondary: every-position split within prefix.
            # Yields one of "Brad Ruddell Design" for `bradruddelldesign`.
            for i in range(2, len(prefix) - 1):
                p1, p2 = prefix[:i], prefix[i:]
                if len(p1) >= 2 and len(p2) >= 2:
                    out.append(f"{p1.title()} {p2.title()} {suf.title()}")
            # Tertiary: prefix-only candidate. Catches cases where a
            # sibling doc abbreviates to just the brand stem — e.g.
            # `smekdesign.com.au` referenced as `SMEK` in a consultant
            # table. Length ≥3 to avoid spurious 2-letter matches.
            if len(prefix) >= 3:
                out.append(prefix.title())
            break

    if not has_known_suffix:
        # Try splitting the whole word at every position — catches firms
        # like `bradanthony` even without a known suffix anchor.
        for i in range(2, len(word) - 1):
            p1, p2 = word[:i], word[i:]
            if len(p1) >= 2 and len(p2) >= 2:
                out.append(f"{p1.title()} {p2.title()}")

    # Bare title-cased + uppercase — LAST RESORT.
    # Tried after every split candidate so legit firms with spaced
    # spellings (Brad Ruddell Design) resolve correctly first; only
    # falls through for genuine single-word brand names (Metricon,
    # SMEKdesign, QUBD).
    out.append(firm_word.title())
    out.append(firm_word.upper())

    # De-dupe while preserving order.
    seen: set[str] = set()
    result: list[str] = []
    for c in out:
        k = c.lower()
        if k in seen:
            continue
        seen.add(k)
        result.append(c)
    return result


@dataclass
class ResolutionStats:
    fingerprints_seen: int = 0
    domains_seen: int = 0
    docs_text_extracted: int = 0
    domains_resolved: int = 0
    fingerprints_updated: int = 0


def _ensure_text(s, doc_id: int, file_path: str | None,
                 mime_type: str | None, doc_type: str | None,
                 cached_text: str | None) -> str | None:
    """Return doc text, extracting + caching if missing."""
    if cached_text:
        return cached_text
    if not file_path:
        return None
    ext = extract_text_for_prompt(
        file_path=file_path, mime_type=mime_type, doc_type=doc_type,
    )
    if not ext.text:
        return None
    s.execute(
        sql_text(
            "UPDATE council_application_documents "
            "SET extracted_text = :t WHERE id = :i"
        ),
        {"t": ext.text, "i": doc_id},
    )
    return ext.text


def _firm_seed(fingerprint_kind: str, normalized_value: str) -> str | None:
    """Pick the most-likely firm-word from a fingerprint.

    URL: take the domain firm-word (`borisdesign.com.au` → `borisdesign`).
    Email: prefer the domain firm-word; fall back to the local part
    when the domain is generic (`auskidesign@gmail.com` → `auskidesign`).
    Returns None if the fingerprint carries no firm signal at all
    (e.g., `info@gmail.com`).
    """
    if fingerprint_kind == "url":
        # If the URL itself is a generic email host (rare but possible
        # — pymupdf reads `auskidesign@gmail.com` as URL `gmail.com`
        # plus email both), it carries no firm signal.
        if normalized_value in _GENERIC_EMAIL_HOSTS:
            return None
        seed = _strip_tld(normalized_value)
        return seed if len(seed) >= 4 else None

    if fingerprint_kind == "email":
        local, _, domain = normalized_value.partition("@")
        if not domain:
            return None
        if domain in _GENERIC_EMAIL_HOSTS:
            # Domain has no firm signal — try the local part instead.
            cleaned = re.sub(r"[._\-+]", "", local)
            if not cleaned or cleaned in _GENERIC_EMAIL_LOCALS:
                return None
            return cleaned if len(cleaned) >= 4 else None
        # Non-generic domain — use the domain firm-word (consistent with
        # URL-fingerprint resolution).
        seed = _strip_tld(domain)
        return seed if len(seed) >= 4 else None

    return None


def _existing_match(
    app_companies: list, candidate: str,
) -> int | None:
    """If a `companies` row already linked to this app has a name that
    overlaps the candidate (substring either way), return its id.

    Prevents `Metricon` (URL-resolved) from creating a duplicate when
    `Metricon Homes` (owns_copyright-resolved) already exists.
    """
    cl = candidate.lower()
    for r in app_companies:
        nl = (r.display_name or "").lower()
        if not nl:
            continue
        if cl == nl or cl in nl or nl in cl:
            return r.id
    return None


def resolve_app(app_pk: int) -> ResolutionStats:
    """Resolve unbound URL/email fingerprints for one application."""
    stats = ResolutionStats()

    with session_scope() as s:
        fps = s.execute(sql_text("""
            SELECT id, fingerprint_kind, normalized_value, source_doc_id
            FROM doc_fingerprints
            WHERE application_id = :app
              AND fingerprint_kind IN ('url', 'email')
              AND resolved_company_id IS NULL
        """), {"app": app_pk}).fetchall()
        stats.fingerprints_seen = len(fps)
        if not fps:
            return stats

        # Group fingerprints by **firm seed**, so URL+email fingerprints
        # for the same firm collapse into one resolution attempt — even
        # when one is a generic-host email whose domain we ignore in
        # favour of the local part.
        by_seed: dict[str, list] = {}
        for fp in fps:
            seed = _firm_seed(fp.fingerprint_kind, fp.normalized_value)
            if not seed:
                continue
            by_seed.setdefault(seed, []).append(fp)
        stats.domains_seen = len(by_seed)

        # Pull every doc for this app once (we'll search them all).
        docs = s.execute(sql_text("""
            SELECT id, doc_type, file_path, mime_type, extracted_text
            FROM council_application_documents
            WHERE application_id = :app
        """), {"app": app_pk}).fetchall()

        # Mutable cache of resolved text per doc (after extraction).
        text_cache: dict[int, str | None] = {d.id: d.extracted_text for d in docs}

        for firm_word, group in by_seed.items():
            candidates = _name_candidates(firm_word)
            if not candidates:
                continue
            source_doc_ids = {fp.source_doc_id for fp in group}

            resolved_name: str | None = None
            for cand in candidates:
                # Build a tolerant regex: word boundaries around each
                # token, allow ANY non-word chars between them. So
                # `Image Design` matches "Image + Design", "Image-Design",
                # "ImageDesign", "Image Design" all alike.
                tokens = [re.escape(t) for t in cand.split() if t]
                if not tokens:
                    continue
                # `\bWord1\b\W*\bWord2\b\W*\bWord3\b` — each token MUST
                # be a word on its own. Without the inner `\b`, the
                # pattern would happily match a concatenated form like
                # `bradruddelldesign` against candidate `Bradruddell Design`.
                pattern = re.compile(
                    r"\b" + r"\b\W*\b".join(tokens) + r"\b",
                    re.IGNORECASE,
                )

                for d in docs:
                    if d.id in source_doc_ids:
                        continue
                    text = text_cache[d.id]
                    if text is None:
                        text = _ensure_text(
                            s, d.id, d.file_path, d.mime_type, d.doc_type, None,
                        )
                        text_cache[d.id] = text
                        if text is not None:
                            stats.docs_text_extracted += 1
                    if not text:
                        continue
                    m = pattern.search(text)
                    if m:
                        # Capture the verbatim form as it appears in the
                        # sibling doc — preserves "QUBD" vs "Qubd",
                        # "IMAGE + DESIGN" vs "Image Design", etc.
                        resolved_name = m.group(0).strip()
                        break
                if resolved_name:
                    break

            if not resolved_name:
                continue

            # Light cleanup: collapse internal whitespace.
            display_name = re.sub(r"\s+", " ", resolved_name).strip()

            # Prefer binding to an entity that already exists for this
            # app if the names overlap (Metricon vs Metricon Homes).
            app_companies = s.execute(sql_text("""
                SELECT DISTINCT c.id, c.display_name
                FROM application_entities ae
                JOIN companies c ON c.id = ae.company_id
                WHERE ae.application_id = :app
            """), {"app": app_pk}).fetchall()
            company_id = _existing_match(app_companies, display_name)

            if company_id is None:
                company_id = _upsert_company(
                    s,
                    display_name=display_name,
                    entity_type="company",
                )
                if company_id is None:
                    continue
            stats.domains_resolved += 1

            # Write back to every fingerprint with this domain.
            for fp in group:
                s.execute(
                    sql_text("""
                        UPDATE doc_fingerprints
                        SET resolved_company_id = :co,
                            resolved_via = :via,
                            resolved_at = :now
                        WHERE id = :id
                    """),
                    {
                        "co": company_id,
                        "via": "cross_doc_match",
                        "now": datetime.utcnow(),
                        "id": fp.id,
                    },
                )
                stats.fingerprints_updated += 1

    return stats


def resolve_all() -> ResolutionStats:
    """Run resolution across every application that has fingerprints."""
    total = ResolutionStats()
    with session_scope() as s:
        app_ids = [
            r.application_id for r in s.execute(sql_text(
                "SELECT DISTINCT application_id FROM doc_fingerprints "
                "WHERE resolved_company_id IS NULL"
            )).fetchall()
        ]
    for app_pk in app_ids:
        st = resolve_app(app_pk)
        total.fingerprints_seen     += st.fingerprints_seen
        total.domains_seen          += st.domains_seen
        total.docs_text_extracted   += st.docs_text_extracted
        total.domains_resolved      += st.domains_resolved
        total.fingerprints_updated  += st.fingerprints_updated
    return total
