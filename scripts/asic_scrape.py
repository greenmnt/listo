"""End-to-end ASIC scraper.

Connects to the user's real Chrome on :9222 (real fingerprint = invisible
reCAPTCHA passes), drives the ADF Registry Search form, walks the
results table across all pages, filters to Australian Proprietary
Companies, then re-queries each by ACN to land on its View Details
page and parses the company summary.

The scraper is split into independent steps so a partial run leaves
useful artefacts behind in data/asic_probe/.
"""
from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from patchright.sync_api import Page, sync_playwright


LANDING_URL = "https://connectonline.asic.gov.au/RegistrySearch/"

# Search-panel field selectors. The landing page uses
# `:searchPanelLanding:dc1:s1:` IDs; once a search has been run, ADF
# keeps the results layout and the search bar moves to
# `:generalSearchPanelFragment:s4:`. Match either by suffix.
DROPDOWN_SEL = (
    "[id$=':searchPanelLanding:dc1:s1:searchTypesLovId'], "
    "[id$=':generalSearchPanelFragment:s4:searchTypesLovId']"
)
TEXTBOX_SEL = (
    "[id$=':searchPanelLanding:dc1:s1:searchForTextId::content'], "
    "[id$=':generalSearchPanelFragment:s4:searchForTextId::content']"
)
BUTTON_SEL = (
    "[id$=':searchPanelLanding:dc1:s1:searchButtonId'], "
    "[id$=':generalSearchPanelFragment:s4:searchButtonId']"
)

# Results-table + paging selectors. ADF increments the region index
# (:r1:0:, :r1:1:, …) across navigations within the same session, so
# match by suffix rather than fixed index.
RESULTS_TABLE_SEL = "[id$=':t1'][role='grid']"
NEXT_BUTTON_SEL = "[id$=':pagingNextButton']"

OUT_DIR = Path("data/asic_probe")


@dataclass
class ResultRow:
    acn: str | None       # 9-digit number from hidden span (None for BN-only rows)
    code: str             # ORG / BN
    name: str             # display name
    number_text: str      # "ACN 165 787 173" / "TAS BN01623140" / ""
    type_text: str        # "Australian Proprietary Company" / "Business Name" / ...
    status: str           # "Registered" / "Cancelled" / "Deregistered"
    address: str          # registered office locality, often blank


@dataclass
class CompanyDetail:
    acn: str
    name: str
    abn: str | None = None
    registration_date: str | None = None
    next_review_date: str | None = None
    status: str | None = None
    type: str | None = None
    locality: str | None = None
    regulator: str | None = None
    extra: dict[str, str] = field(default_factory=dict)


# --- search-form driving --------------------------------------------------

def open_search(page: Page, query: str) -> None:
    """Land the page on results for `query` (Organisation & Business Names)."""
    print(f"  search: {query!r}")
    # Background tabs get throttled JS execution under Chrome — keep our
    # tab in the foreground so ADF runs at full speed.
    page.bring_to_front()
    # Use domcontentloaded; networkidle stalls when Chrome has many
    # other tabs holding open keep-alive connections. The user's running
    # Chrome occasionally throws DNS errors under tab pressure — retry.
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            page.goto(LANDING_URL, wait_until="domcontentloaded", timeout=60_000)
            break
        except Exception as e:
            last_err = e
            print(f"  goto attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    else:
        raise last_err
    page.wait_for_load_state("load", timeout=60_000)
    # Poll for ADF + the search panel component to be bound. Run the
    # check in main world (isolated_context=False) — patchright's default
    # isolation sees AdfPage as undefined and the wait silently ticks
    # forever.
    # Find which dropdown is currently rendered (landing vs results) and
    # wait for ADF to bind it.
    deadline = time.time() + 60
    dropdown_id: str | None = None
    while time.time() < deadline:
        dropdown_id = page.evaluate(
            "() => { for (const sfx of [':searchPanelLanding:dc1:s1:searchTypesLovId', "
            "':generalSearchPanelFragment:s4:searchTypesLovId']) { "
            "const el = document.querySelector(`[id$=\"${sfx}\"]`); "
            "if (el && typeof AdfPage !== 'undefined' && AdfPage.PAGE) { "
            "  const cid = el.id.replace('::content',''); "
            "  if (AdfPage.PAGE.findComponent(cid)) return cid; "
            "}} return null; }",
            isolated_context=False,
        )
        if dropdown_id:
            break
        page.wait_for_timeout(250)
    else:
        debug = OUT_DIR / f"adf_notbound_{int(time.time())}.html"
        debug.write_text(page.content(), encoding="utf-8")
        print(f"  ADF not bound; on {page.url} title={page.title()!r}; dumped {debug}")
        raise TimeoutError("ADF dropdown component never bound")
    page.evaluate(
        "(id) => AdfPage.PAGE.findComponent(id).setValue('1')",
        dropdown_id,
        isolated_context=False,
    )
    page.wait_for_timeout(2500)  # let the autoSubmit PPR settle

    page.locator(TEXTBOX_SEL).first.click()
    page.keyboard.type(query, delay=20)
    page.keyboard.press("Tab")

    # Click Search; if the results don't appear within the timeout
    # (silent reCAPTCHA-score failure), re-click once before giving up.
    # `force=True` bypasses the actionability wait, which sometimes hangs
    # because reCAPTCHA's invisible overlay div intercepts pointer events.
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
                print("  search did not return; re-clicking Search")
                page.wait_for_timeout(2000)
                continue
            debug = OUT_DIR / f"fail_{int(time.time())}.html"
            try:
                debug.write_text(page.content(), encoding="utf-8")
            except Exception as e:
                print(f"  also failed to dump html: {e}")
            print(f"  FAIL @ {page.url} title={page.title()!r}; dumped {debug}")
            raise


# --- results-table parsing ------------------------------------------------

# Chrome's DOM serialization lowercases attribute names, so the
# `_afrRK` we see in the HTTP response becomes `_afrrk` in
# `page.content()`. Match either.
ROW_RE = re.compile(
    r'<tr [^>]*_afr[Rr][Kk]="(\d+)"[^>]*class="af_table_data-row"[^>]*>(.*?)</tr>',
    re.DOTALL,
)
NAME_RE = re.compile(r'orgName[^>]*>([^<]+)</a>', re.DOTALL)
HIDDEN_SPAN_RE = re.compile(r'<span style="display:none">([^<]*)</span>')
COL_RE = re.compile(
    r':c(\d)" class="af_column_data-cell">\s*<span[^>]*>([^<]*?)</span>',
    re.DOTALL,
)


def parse_results_page(html: str) -> list[ResultRow]:
    rows: list[ResultRow] = []
    for m in ROW_RE.finditer(html):
        body = m.group(2)
        hidden = HIDDEN_SPAN_RE.findall(body)
        acn = hidden[0] if len(hidden) >= 1 and hidden[0].isdigit() else None
        code = hidden[1] if len(hidden) >= 2 else ""
        name_m = NAME_RE.search(body)
        cols = {int(c): v.strip() for c, v in COL_RE.findall(body)}
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


def total_results_count(html: str) -> int | None:
    m = re.search(
        r'<span style="font-weight:bold;;white-space:nowrap">(\d+)</span>\s*<[^>]*>\s*</td>\s*<td>\s*<span[^>]*>results found',
        html,
    )
    return int(m.group(1)) if m else None


def go_to_next_page(page: Page) -> bool:
    """Click Next; return True if it advanced, False if we were on the last page.

    ADF rebuilds the table on each page change, so we detect advance by
    watching the first row's _afrRK rowKey flip from its prior value.
    """
    btns = page.locator(NEXT_BUTTON_SEL)
    if btns.count() == 0:
        return False
    # Pick the non-twin button (there's a duplicate `Twin` paginator).
    btn = page.locator(f"{NEXT_BUTTON_SEL}:not([id$='ButtonTwin'])").first
    if btn.evaluate("el => el.disabled || el.classList.contains('p_AFDisabled')"):
        return False
    # Watch the first row's name link — ADF reuses _afrrk values across
    # pages, so we detect advance by the displayed text changing.
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


# --- detail-page parsing --------------------------------------------------

DETAIL_TABLE_RE = re.compile(
    r'<table[^>]*class="detailTable"[^>]*>(.*?)</table>', re.DOTALL,
)
DETAIL_ROW_RE = re.compile(
    r'<tr><th>([^<]*?(?:<abbr[^>]*>([^<]+)</abbr>)?[^<]*)</th>\s*<td>(.*?)</td></tr>',
    re.DOTALL,
)
ABN_RE = re.compile(r'>([\d ]{14,16})<')


def parse_detail_page(html: str) -> CompanyDetail | None:
    m = DETAIL_TABLE_RE.search(html)
    if not m:
        return None
    fields: dict[str, str] = {}
    for row in DETAIL_ROW_RE.finditer(m.group(1)):
        label_full, abbr, td = row.groups()
        label = (abbr if abbr else label_full).rstrip(": ").strip()
        if label.endswith(":"):
            label = label[:-1].strip()
        # Strip tags from value; keep only visible text.
        val = re.sub(r'<span class="hiddenHint">[^<]*</span>', '', td)
        val = re.sub(r'<[^>]+>', '', val)
        val = re.sub(r'\s+', ' ', val).strip()
        val = val.replace('&amp;', '&')
        if label and val:
            fields[label] = val
    if "Name" not in fields or "ACN" not in fields:
        return None
    return CompanyDetail(
        acn=fields["ACN"].replace(" ", ""),
        name=fields["Name"],
        abn=fields.get("ABN", "").replace(" ", "") or None,
        registration_date=fields.get("Registration date"),
        next_review_date=fields.get("Next review date"),
        status=fields.get("Status"),
        type=fields.get("Type"),
        locality=fields.get("Locality of registered office"),
        regulator=fields.get("Regulator"),
        extra={k: v for k, v in fields.items() if k not in {
            "Name", "ACN", "ABN", "Registration date", "Next review date",
            "Status", "Type", "Locality of registered office", "Regulator",
        }},
    )


# --- driver ---------------------------------------------------------------

def main(query: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp("http://localhost:9222")
        ctx = browser.contexts[0]
        page = ctx.new_page()
        try:
            open_search(page, query)
            # If ASIC dropped us straight onto a detail page (exact ACN),
            # short-circuit.
            if page.locator("table.detailTable").count() > 0:
                detail = parse_detail_page(page.content())
                print("DETAIL (single-hit):")
                print(json.dumps(detail.__dict__ if detail else None, indent=2))
                return

            all_rows: list[ResultRow] = []
            page_no = 1
            while True:
                html = page.content()
                (OUT_DIR / f"results_p{page_no}.html").write_text(html, encoding="utf-8")
                rows = parse_results_page(html)
                print(f"page {page_no}: {len(rows)} rows; total={total_results_count(html)}")
                all_rows.extend(rows)
                if not go_to_next_page(page):
                    break
                page_no += 1

            apc = [r for r in all_rows if r.type_text == "Australian Proprietary Company" and r.acn]
            print(f"\ntotal rows={len(all_rows)}, Australian Proprietary Company={len(apc)}")
            for r in apc:
                print(f"  {r.acn}  {r.name}  ({r.status})")

            # Walk each APC row in its OWN fresh tab. ADF state and
            # silent reCAPTCHA-score failures both seem to be per-tab,
            # so isolating each detail fetch keeps the others clean.
            # Sleep between iterations to be polite and to let any
            # invisible reCAPTCHA scoring catch its breath.
            details: list[CompanyDetail] = []
            for i, r in enumerate(apc, 1):
                print(f"\n[{i}/{len(apc)}] {r.acn} {r.name}")
                detail_page = ctx.new_page()
                try:
                    open_search(detail_page, r.acn)
                    if detail_page.locator("table.detailTable").count() == 0:
                        fail = OUT_DIR / f"detail_miss_{r.acn}.html"
                        fail.write_text(detail_page.content(), encoding="utf-8")
                        print(f"  WARN: no detail; dumped {fail}")
                        continue
                    d = parse_detail_page(detail_page.content())
                    if d:
                        details.append(d)
                        print(f"  -> {d.name} | {d.status} | {d.locality}")
                    (OUT_DIR / f"detail_{r.acn}.html").write_text(
                        detail_page.content(), encoding="utf-8"
                    )
                except Exception as e:
                    print(f"  ERR: {e}")
                finally:
                    detail_page.close()
                time.sleep(3)

            out_json = OUT_DIR / "details.json"
            out_json.write_text(json.dumps([d.__dict__ for d in details], indent=2))
            print(f"\nwrote {len(details)} records to {out_json}")
        finally:
            page.close()
            browser.close()


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Potter Projects Pty Ltd"
    main(q)
