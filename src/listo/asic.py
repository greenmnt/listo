"""ASIC Connect Online registry scraper.

Drives the public Registry Search at connectonline.asic.gov.au to look
up Australian companies by ACN or by name. The site is an Oracle ADF
SPA fronted by invisible reCAPTCHA — fresh patchright instances score
low and trip image challenges, so this module attaches to the user's
real Chrome via CDP (`http://localhost:9222`) where the warmed-up
fingerprint passes silently.

Two entry points:

- `lookup_acn(acn)`        — single ACN → company detail.
- `search_by_name(query)`  — walks every page of results, filters to
                             Australian Proprietary Companies, then
                             does a fresh ACN lookup per row to land
                             on its View Details page.

Each detail fetch happens in its own browser tab. ADF state and
silent reCAPTCHA failures both seem to be per-tab, so isolating each
fetch keeps the rest of a name-search batch clean.

Persistence reuses the existing `companies` table (matched by ACN);
ASIC fields are written into the `asic_*` columns added in 0018.
"""
from __future__ import annotations

import logging
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Iterator

from patchright.sync_api import BrowserContext, Page, sync_playwright
from sqlalchemy import select

from listo.db import session_scope
from listo.models import Company


logger = logging.getLogger(__name__)


CDP_URL = "http://localhost:9222"
LANDING_URL = "https://connectonline.asic.gov.au/RegistrySearch/"

# Selectors that match either the fresh-landing search panel
# (`:searchPanelLanding:dc1:s1:`) or the post-results inline search
# bar (`:generalSearchPanelFragment:s4:`). ADF chooses one or the
# other depending on whether the tab has searched before.
DROPDOWN_SUFFIXES = (
    ":searchPanelLanding:dc1:s1:searchTypesLovId",
    ":generalSearchPanelFragment:s4:searchTypesLovId",
)
TEXTBOX_SEL = (
    "[id$=':searchPanelLanding:dc1:s1:searchForTextId::content'], "
    "[id$=':generalSearchPanelFragment:s4:searchForTextId::content']"
)
BUTTON_SEL = (
    "[id$=':searchPanelLanding:dc1:s1:searchButtonId'], "
    "[id$=':generalSearchPanelFragment:s4:searchButtonId']"
)
RESULTS_TABLE_SEL = "[id$=':t1'][role='grid']"
NEXT_BUTTON_SEL = "[id$=':pagingNextButton']:not([id$='ButtonTwin'])"


# ---------------------------------------------------------------- dataclasses


@dataclass
class ResultRow:
    """One row of a name-search results table."""
    acn: str | None      # 9-digit number from hidden span; None for BN-only rows
    code: str            # 'ORG' / 'BN'
    name: str            # display name (may have leading '*' for former names)
    number_text: str     # 'ACN 165 787 173' / 'TAS BN01623140' / ''
    type_text: str       # 'Australian Proprietary Company' / 'Business Name' / ...
    status: str          # 'Registered' / 'Cancelled' / 'Deregistered'
    address: str         # registered office locality, often blank


@dataclass
class CompanyDetail:
    """Snapshot of an ASIC View-Details page for one entity."""
    acn: str
    name: str
    abn: str | None = None
    registration_date: date | None = None
    next_review_date: date | None = None
    status: str | None = None
    type: str | None = None
    locality: str | None = None
    regulator: str | None = None
    extra: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------- HTML parsing


# Chrome's DOM serialization lowercases attribute names, so the `_afrRK`
# we see in the raw HTTP response becomes `_afrrk` in `page.content()`.
# Match either case.
_ROW_RE = re.compile(
    r'<tr [^>]*_afr[Rr][Kk]="(\d+)"[^>]*class="af_table_data-row"[^>]*>(.*?)</tr>',
    re.DOTALL,
)
_NAME_RE = re.compile(r'orgName[^>]*>([^<]+)</a>', re.DOTALL)
_HIDDEN_SPAN_RE = re.compile(r'<span style="display:none">([^<]*)</span>')
_COL_RE = re.compile(
    r':c(\d)" class="af_column_data-cell">\s*<span[^>]*>([^<]*?)</span>',
    re.DOTALL,
)
_DETAIL_TABLE_RE = re.compile(
    r'<table[^>]*class="detailTable"[^>]*>(.*?)</table>', re.DOTALL,
)
_DETAIL_ROW_RE = re.compile(
    r'<tr><th>([^<]*?(?:<abbr[^>]*>([^<]+)</abbr>)?[^<]*)</th>\s*<td>(.*?)</td></tr>',
    re.DOTALL,
)
_DIGITS_ONLY_RE = re.compile(r"\D+")


def _parse_au_date(s: str | None) -> date | None:
    """Parse 'D/M/YYYY' (ASIC's date format) into a date object."""
    if not s:
        return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%-d/%-m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_results_page(html: str) -> list[ResultRow]:
    """Extract every `<tr>` row of the name-search results table."""
    rows: list[ResultRow] = []
    for m in _ROW_RE.finditer(html):
        body = m.group(2)
        hidden = _HIDDEN_SPAN_RE.findall(body)
        acn = hidden[0] if hidden and hidden[0].isdigit() else None
        code = hidden[1] if len(hidden) >= 2 else ""
        name_m = _NAME_RE.search(body)
        cols = {int(c): v.strip() for c, v in _COL_RE.findall(body)}
        rows.append(ResultRow(
            acn=acn,
            code=code,
            name=name_m.group(1).strip() if name_m else "",
            number_text=cols.get(3, ""),
            type_text=cols.get(6, ""),
            status=cols.get(1, ""),
            address=cols.get(4, ""),
        ))
    return rows


def parse_detail_page(html: str) -> CompanyDetail | None:
    """Extract the company-summary fields from a View-Details page."""
    m = _DETAIL_TABLE_RE.search(html)
    if not m:
        return None
    fields: dict[str, str] = {}
    for row in _DETAIL_ROW_RE.finditer(m.group(1)):
        label_full, abbr, td = row.groups()
        label = (abbr if abbr else label_full).rstrip(": ").strip().rstrip(":").strip()
        # Strip a11y-only `hiddenHint` spans, then any other tags, to get
        # just the visible value text.
        val = re.sub(r'<span class="hiddenHint">[^<]*</span>', '', td)
        val = re.sub(r'<[^>]+>', '', val)
        val = re.sub(r'\s+', ' ', val).strip().replace('&amp;', '&')
        if label and val:
            fields[label] = val
    if "Name" not in fields or "ACN" not in fields:
        return None
    abn = fields.get("ABN")
    if abn:
        abn_digits = _DIGITS_ONLY_RE.sub("", abn)
        abn = abn_digits if len(abn_digits) == 11 else None
    return CompanyDetail(
        acn=_DIGITS_ONLY_RE.sub("", fields["ACN"]),
        name=fields["Name"],
        abn=abn,
        registration_date=_parse_au_date(fields.get("Registration date")),
        next_review_date=_parse_au_date(fields.get("Next review date")),
        status=fields.get("Status"),
        type=fields.get("Type"),
        locality=fields.get("Locality of registered office"),
        regulator=fields.get("Regulator"),
        extra={k: v for k, v in fields.items() if k not in {
            "Name", "ACN", "ABN", "Registration date", "Next review date",
            "Status", "Type", "Locality of registered office", "Regulator",
        }},
    )


# ---------------------------------------------------------------- form driving


def _open_search(page: Page, query: str) -> None:
    """Land the page on results for `query` (Organisation & Business Names).

    Either a results table renders (multiple hits) or — for an exact
    ACN match — ASIC redirects straight to the View-Details page.
    """
    logger.info("ASIC search: %r", query)
    page.bring_to_front()
    # `domcontentloaded`; `networkidle` stalls when the user's Chrome
    # has many other tabs holding open keep-alive connections. Retry
    # transient DNS errors that happen under tab pressure.
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            page.goto(LANDING_URL, wait_until="domcontentloaded", timeout=60_000)
            break
        except Exception as e:
            last_err = e
            logger.warning("goto attempt %d failed: %s", attempt + 1, e)
            time.sleep(2)
    else:
        assert last_err is not None
        raise last_err
    page.wait_for_load_state("load", timeout=60_000)

    # Find the dropdown ADF rendered (landing vs post-results layout)
    # and wait for ADF to finish binding it.
    deadline = time.time() + 60
    dropdown_id: str | None = None
    while time.time() < deadline:
        dropdown_id = page.evaluate(
            "(suffixes) => { for (const sfx of suffixes) { "
            "const el = document.querySelector(`[id$=\"${sfx}\"]`); "
            "if (el && typeof AdfPage !== 'undefined' && AdfPage.PAGE) { "
            "  const cid = el.id.replace('::content',''); "
            "  if (AdfPage.PAGE.findComponent(cid)) return cid; "
            "}} return null; }",
            list(DROPDOWN_SUFFIXES),
            isolated_context=False,
        )
        if dropdown_id:
            break
        page.wait_for_timeout(250)
    else:
        raise TimeoutError("ADF search panel never finished binding")

    # autoSubmit=true on the dropdown; setting it via the ADF API
    # commits to the model and fires the partial refresh.
    page.evaluate(
        "(id) => AdfPage.PAGE.findComponent(id).setValue('1')",
        dropdown_id,
        isolated_context=False,
    )
    page.wait_for_timeout(2500)  # let the autoSubmit PPR settle

    # Type into the textbox like a real user — `page.fill` skips the
    # input pipeline, so ADF's inputText doesn't commit the value to
    # the model and the form submits empty.
    page.locator(TEXTBOX_SEL).first.click()
    page.keyboard.type(query, delay=20)
    page.keyboard.press("Tab")

    # `force=True`: Google's invisible reCAPTCHA renders an empty
    # overlay div during scoring that intercepts pointer events and
    # would otherwise hang the actionability check. Re-click once if
    # the results don't render — silent reCAPTCHA-score failures show
    # up as "click happened, nothing changed".
    for attempt in range(2):
        page.locator(BUTTON_SEL).first.click(force=True)
        try:
            page.wait_for_selector(
                f"{RESULTS_TABLE_SEL}, table.detailTable",
                timeout=30_000,
                state="attached",
            )
            return
        except Exception:
            if attempt == 0:
                logger.warning("search did not land — re-clicking Search")
                page.wait_for_timeout(2000)
                continue
            raise


def _go_to_next_page(page: Page) -> bool:
    """Click Next; return True if it advanced, False if disabled/missing.

    ADF reuses `_afrrk` row keys across pages, so we detect the page
    flip by the first row's name-link textContent changing.
    """
    btns = page.locator(NEXT_BUTTON_SEL)
    if btns.count() == 0:
        return False
    btn = btns.first
    if btn.evaluate("el => el.disabled || el.classList.contains('p_AFDisabled')"):
        return False
    name_before = page.evaluate(
        "() => { const a = document.querySelector(`[id$=':t1'][role='grid'] "
        "a[id$=':orgName']`); return a ? a.textContent : null; }",
        isolated_context=False,
    )
    btn.click()
    page.wait_for_function(
        "(before) => { const a = document.querySelector(`[id$=':t1'][role='grid'] "
        "a[id$=':orgName']`); return a && a.textContent !== before; }",
        arg=name_before,
        timeout=20_000,
    )
    page.wait_for_timeout(500)
    return True


# ---------------------------------------------------------------- public API


@contextmanager
def _attached_browser() -> Iterator[BrowserContext]:
    """Connect over CDP to the user's Chrome on :9222."""
    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(CDP_URL)
        try:
            yield browser.contexts[0]
        finally:
            browser.close()


def lookup_acn(acn: str, *, ctx: BrowserContext | None = None) -> CompanyDetail | None:
    """Search ASIC for a single ACN; return the parsed View-Details page.

    Opens its own tab so callers iterating over many ACNs don't share
    ADF state (per-tab) or reCAPTCHA accumulation (per-tab too,
    empirically — fresh tabs reset the silent-failure pattern).
    """
    acn_clean = _DIGITS_ONLY_RE.sub("", acn)
    if len(acn_clean) != 9:
        raise ValueError(f"expected 9-digit ACN, got {acn!r}")

    if ctx is None:
        with _attached_browser() as ctx2:
            return lookup_acn(acn_clean, ctx=ctx2)

    page = ctx.new_page()
    try:
        _open_search(page, acn_clean)
        if page.locator("table.detailTable").count() == 0:
            logger.warning("ACN %s: no detail page", acn_clean)
            return None
        return parse_detail_page(page.content())
    finally:
        page.close()


def search_by_name(
    query: str,
    *,
    ctx: BrowserContext | None = None,
    types_to_fetch: tuple[str, ...] = ("Australian Proprietary Company",),
    sleep_between: float = 3.0,
) -> tuple[list[ResultRow], list[CompanyDetail]]:
    """Walk every page of name-search results, then fetch the View-Details
    page for each row whose `Type` matches `types_to_fetch`.

    Returns `(all_rows, details)`. Rows that didn't match the type
    filter (Business Names, trusts, etc.) are still in `all_rows` so
    callers can inspect them.
    """
    if ctx is None:
        with _attached_browser() as ctx2:
            return search_by_name(
                query,
                ctx=ctx2,
                types_to_fetch=types_to_fetch,
                sleep_between=sleep_between,
            )

    page = ctx.new_page()
    all_rows: list[ResultRow] = []
    try:
        _open_search(page, query)
        # Exact-match shortcut: ASIC drops a single-result query straight
        # onto the detail page.
        if page.locator("table.detailTable").count() > 0:
            d = parse_detail_page(page.content())
            return [], [d] if d else []

        page_no = 1
        while True:
            rows = parse_results_page(page.content())
            logger.info("page %d: %d rows", page_no, len(rows))
            all_rows.extend(rows)
            if not _go_to_next_page(page):
                break
            page_no += 1
    finally:
        page.close()

    targets = [
        r for r in all_rows
        if r.acn and r.type_text in types_to_fetch
    ]
    logger.info(
        "name search %r: %d total rows, %d to fetch (types=%s)",
        query, len(all_rows), len(targets), types_to_fetch,
    )

    details: list[CompanyDetail] = []
    for i, r in enumerate(targets, 1):
        logger.info("[%d/%d] fetching %s %s", i, len(targets), r.acn, r.name)
        try:
            d = lookup_acn(r.acn, ctx=ctx)
        except Exception as e:
            logger.warning("[%d/%d] ACN %s failed: %s", i, len(targets), r.acn, e)
            continue
        if d:
            details.append(d)
        time.sleep(sleep_between)
    return all_rows, details


# ---------------------------------------------------------------- persistence


_COMPANY_SUFFIX_RE = re.compile(
    r"\b(pty\.?\s*ltd\.?|pty\.?\s*limited|limited|inc\.?|corp\.?|llc|ltd\.?)\b\.?\s*$",
    re.IGNORECASE,
)


def _norm_name(display: str) -> str:
    """Canonical name used as fuzzy join key when ACN isn't available.

    Mirrors the helper in `da_summaries/aggregate.py` so a `companies`
    row created by either path collides cleanly.
    """
    s = display.lower().strip()
    s = re.sub(r"\([^)]*\)", "", s)
    s = _COMPANY_SUFFIX_RE.sub("", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def upsert_company_from_asic(s, detail: CompanyDetail) -> int:
    """Insert or update a `companies` row from an ASIC detail snapshot.

    Match priority is ACN (always present in `CompanyDetail`). On
    update we always overwrite the `asic_*` columns since they're
    authoritative; we only set `display_name` if the existing row had
    a worse one (e.g. created from a free-text DA mention).

    Returns the company id.
    """
    existing = s.execute(
        select(Company).where(Company.acn == detail.acn)
    ).scalar_one_or_none()

    now = datetime.utcnow()
    if existing is None:
        # Try to pick up an earlier name-only match before inserting,
        # so the DA-summariser-created row gets enriched in place.
        norm = _norm_name(detail.name)
        existing = s.execute(
            select(Company).where(
                Company.acn.is_(None), Company.norm_name == norm
            ).limit(1)
        ).scalar_one_or_none()

    if existing is None:
        existing = Company(
            acn=detail.acn,
            abn=detail.abn,
            display_name=detail.name[:255],
            norm_name=_norm_name(detail.name)[:255],
            entity_type="company",
            first_seen_at=now,
        )
        s.add(existing)
    else:
        existing.acn = existing.acn or detail.acn
        if detail.abn and not existing.abn:
            existing.abn = detail.abn
        # ASIC's display name is authoritative — replace the LLM-extracted
        # one (which may have lost punctuation, capitalization, or the
        # 'PTY LTD' suffix).
        existing.display_name = detail.name[:255]
        existing.norm_name = _norm_name(detail.name)[:255]
        if existing.entity_type == "unknown":
            existing.entity_type = "company"

    existing.asic_status = detail.status
    existing.asic_company_type = detail.type
    existing.asic_locality = detail.locality
    existing.asic_regulator = detail.regulator
    existing.asic_registration_date = detail.registration_date
    existing.asic_next_review_date = detail.next_review_date
    existing.asic_fetched_at = now
    s.flush()
    return existing.id


def persist_details(details: list[CompanyDetail]) -> dict[str, int]:
    """Upsert a batch of `CompanyDetail` records into `companies`.

    Returns counts of `inserted` vs `updated` rows for reporting.
    """
    inserted = updated = 0
    with session_scope() as s:
        for d in details:
            before = s.execute(
                select(Company).where(Company.acn == d.acn)
            ).scalar_one_or_none()
            had_asic = before is not None and before.asic_fetched_at is not None
            upsert_company_from_asic(s, d)
            if had_asic:
                updated += 1
            else:
                inserted += 1
    return {"inserted": inserted, "updated": updated}
