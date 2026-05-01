"""TechnologyOne eTrack (eProperty) DA scraper — legacy ASP.NET WebForms.

Tenants:
  Newcastle pre-Feb 2026:
    https://cn-web.t1cloud.com/T1PRDefault/WebApps/eProperty/P1/eTrack/
    eTrackApplicationSearch.aspx?r=TCON.LG.WEBGUEST&f=%24P1.ETR.SEARCH.ENQ

This is a SCAFFOLD. The selectors/JS extraction below are best-guesses
based on the standard TechOne eProperty markup pattern; the first run
on a live page will need the user to either paste a sample HTML so we
can confirm the field names, or run with --headless=false and tweak.

Hooks into the same CouncilScraper protocol as infor_epathway, so the
orchestrator drives it identically.
"""
from __future__ import annotations

import hashlib
import logging
import mimetypes
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from patchright.sync_api import Page, sync_playwright

from listo.councils.base import (
    DaDetailRecord,
    DaDocumentRef,
    DaListingRow,
    DownloadedDocument,
    FetchRecord,
    RequestSink,
)
from listo.councils.parsing import (
    count_pdf_pages,
    extract_internal_property_id,
    extract_type_code,
    parse_au_date,
    safe_filename,
    split_council_address,
)


logger = logging.getLogger(__name__)


@dataclass
class TechOneEtrackConfig:
    council_slug: str
    search_url: str
    realm: str                                # 'TCON.LG.WEBGUEST' for Newcastle
    state: str = "NSW"


NEWCASTLE_CONFIG = TechOneEtrackConfig(
    council_slug="newcastle",
    search_url=(
        "https://cn-web.t1cloud.com/T1PRDefault/WebApps/eProperty/P1/eTrack/"
        "eTrackApplicationSearch.aspx?r=TCON.LG.WEBGUEST&f=%24P1.ETR.SEARCH.ENQ"
    ),
    realm="TCON.LG.WEBGUEST",
    state="NSW",
)


class TechOneEtrackScraper:
    vendor: str = "techone_etrack"

    def __init__(
        self,
        config: TechOneEtrackConfig,
        *,
        headless: bool = True,
        jitter_min: float = 1.5,
        jitter_max: float = 4.0,
    ):
        self.config = config
        self.council_slug = config.council_slug
        self._headless = headless
        self._jitter_min = jitter_min
        self._jitter_max = jitter_max
        self._pw = None
        self._ctx = None
        self._page: Page | None = None

    def __enter__(self):
        self._pw = sync_playwright().start()
        self._ctx = self._pw.chromium.launch_persistent_context(
            user_data_dir="",
            headless=self._headless,
            no_viewport=True,
            locale="en-AU",
            timezone_id="Australia/Sydney",
        )
        self._page = self._ctx.new_page()
        return self

    def __exit__(self, *_a):
        try:
            if self._ctx:
                self._ctx.close()
        finally:
            if self._pw:
                self._pw.stop()

    def _sleep(self):
        import random
        time.sleep(random.uniform(self._jitter_min, self._jitter_max))

    def _record_page(self, sink: RequestSink, *, purpose: str, started: datetime) -> int | None:
        page = self._page
        assert page is not None
        body = page.content().encode("utf-8")
        elapsed = int((datetime.utcnow() - started).total_seconds() * 1000)
        return sink.record(FetchRecord(
            purpose=purpose,
            method="GET",
            url=page.url,
            started_at=started,
            elapsed_ms=elapsed,
            http_status=200,
            bytes_received=len(body),
            body=body,
            body_is_html=True,
            content_type="text/html",
        ))

    # ---------------- search ----------------

    def iter_listings(
        self,
        *,
        date_from: date,
        date_to: date,
        sink: RequestSink,
        skip_application_ids: set[str] | None = None,
        allowed_type_codes: set[str] | None = None,
    ) -> Iterator[DaListingRow]:
        # SCAFFOLD: type-code filter not yet wired through this vendor;
        # accepted to satisfy the protocol so the orchestrator can pass
        # it without breaking. Implement when this scraper goes live.
        del allowed_type_codes
        page = self._page
        assert page is not None

        # SCAFFOLD — selectors below are inferred from standard eProperty
        # markup. Once we see the real page, swap these for the actual
        # control IDs (likely 'ctl00$Content$DateLodgedFrom' style).
        page.goto(self.config.search_url, wait_until="domcontentloaded", timeout=30000)

        # Click the 'Date Lodged' or 'Lodged Date Range' search tab if
        # the page presents tabs. eTrack search forms typically render as
        # a single page with optional sections rather than tabs.

        from_input = _first_locator(page, [
            "input[name$='LodgedFromDate']",
            "input[id$='LodgedFromDate']",
            "input[name$='DateFrom']",
            "input[id$='DateFrom']",
        ])
        to_input = _first_locator(page, [
            "input[name$='LodgedToDate']",
            "input[id$='LodgedToDate']",
            "input[name$='DateTo']",
            "input[id$='DateTo']",
        ])
        if from_input is None or to_input is None:
            raise RuntimeError(
                "techone_etrack: could not find lodgement-date inputs on "
                f"{page.url}. Inspect the live page and update the selector list."
            )

        from_input.fill(date_from.strftime("%d/%m/%Y"))
        to_input.fill(date_to.strftime("%d/%m/%Y"))

        self._sleep()
        started = datetime.utcnow()
        # Submit. eTrack uses an asp:Button — it might be id'd 'Search',
        # 'btnSearch', 'SearchButton' etc.
        submit = _first_locator(page, [
            "input[type=submit][value*=Search]",
            "input[type=submit][id$='Search']",
            "button:has-text('Search')",
        ])
        if submit is None:
            raise RuntimeError("techone_etrack: search button not found")
        submit.click(timeout=15000)
        page.wait_for_load_state("networkidle", timeout=30000)

        # Walk paginated results.
        while True:
            self._record_page(sink, purpose="list", started=started)
            yield from _parse_etrack_results(page, council_slug=self.council_slug, vendor=self.vendor, state=self.config.state)
            if not _click_etrack_next(page):
                return
            self._sleep()
            started = datetime.utcnow()

    # ---------------- detail ----------------

    def fetch_detail(self, listing: DaListingRow, sink: RequestSink) -> DaDetailRecord:
        page = self._page
        assert page is not None
        if not listing.application_url:
            raise RuntimeError(f"techone_etrack: no detail URL for {listing.application_id}")
        started = datetime.utcnow()
        page.goto(listing.application_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=20000)
        self._record_page(sink, purpose="detail", started=started)
        return _parse_etrack_detail(page, listing.application_id, council_slug=self.council_slug, vendor=self.vendor, state=self.config.state)

    # ---------------- documents ----------------

    def list_documents(self, detail: DaDetailRecord, sink: RequestSink) -> list[DaDocumentRef]:
        page = self._page
        assert page is not None
        # eProperty typically renders documents in a tabbed pane on the
        # detail page itself, or via a 'Documents' subpage. SCAFFOLD:
        # try clicking a 'Documents' tab; fall back to scanning the
        # current page for document anchors.
        try:
            page.click("text=Documents", timeout=3000)
            page.wait_for_load_state("networkidle", timeout=10000)
            started = datetime.utcnow()
            self._record_page(sink, purpose="docs_index", started=started)
        except Exception:
            pass

        rows = page.evaluate("""
          () => {
            // Look for <a> nodes that point at .pdf or include 'Document'.
            const anchors = Array.from(document.querySelectorAll('a'))
              .filter(a => /\\.pdf$|Document|attachment/i.test(a.href || ''));
            return anchors.map(a => ({
              href: a.href,
              text: (a.innerText || a.title || '').trim(),
            }));
          }
        """)
        return [
            DaDocumentRef(
                doc_oid=_doc_oid_from_url(r["href"]),
                title=r["text"] or None,
                source_url=r["href"],
            )
            for r in rows if r.get("href")
        ]

    def download(self, doc: DaDocumentRef, target_dir: Path, sink: RequestSink) -> DownloadedDocument:
        page = self._page
        assert page is not None
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        self._sleep()
        started = datetime.utcnow()
        try:
            with page.expect_download(timeout=60000) as dl_info:
                try:
                    page.goto(doc.source_url, timeout=60000)
                except Exception:
                    pass
            download = dl_info.value
            suggested = download.suggested_filename or (doc.doc_oid or "file")
            ext = Path(suggested).suffix or ".bin"
            fname = safe_filename(doc.doc_oid or suggested) + ext
            path = target_dir / fname
            download.save_as(str(path))
            size = path.stat().st_size
            with open(path, "rb") as f:
                ch = hashlib.sha256(f.read()).digest()
            mime, _ = mimetypes.guess_type(path.name)
            pages = count_pdf_pages(path) if (mime == "application/pdf" or ext.lower() == ".pdf") else None
            elapsed = int((datetime.utcnow() - started).total_seconds() * 1000)
            sink.record(FetchRecord(
                purpose="doc_download",
                method="GET",
                url=doc.source_url or "",
                started_at=started,
                elapsed_ms=elapsed,
                http_status=200,
                bytes_received=size,
                content_type=mime,
            ))
            return DownloadedDocument(
                doc_oid=doc.doc_oid,
                title=doc.title,
                doc_type=doc.doc_type,
                source_url=doc.source_url,
                file_path=str(path),
                file_size=size,
                mime_type=mime,
                content_hash=ch,
                page_count=pages,
            )
        except Exception as e:
            elapsed = int((datetime.utcnow() - started).total_seconds() * 1000)
            sink.record(FetchRecord(
                purpose="doc_download",
                method="GET",
                url=doc.source_url or "",
                started_at=started,
                elapsed_ms=elapsed,
                error=str(e),
            ))
            raise


# ---------------- parsing helpers ----------------


def _first_locator(page: Page, selectors: list[str]):
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            if loc.count() > 0 and loc.is_visible(timeout=500):
                return loc
        except Exception:
            continue
    return None


def _click_etrack_next(page: Page) -> bool:
    try:
        nxt = page.locator("a:has-text('Next')").first
        if nxt.count() == 0 or not nxt.is_visible():
            return False
        nxt.click(timeout=10000)
        page.wait_for_load_state("networkidle", timeout=20000)
        return True
    except Exception:
        return False


def _parse_etrack_results(page: Page, *, council_slug: str, vendor: str, state: str) -> list[DaListingRow]:
    """SCAFFOLD parser. eTrack typically renders an HTML <table> with
    one row per application; the first column is the application number
    rendered as a link. Column headers vary per tenant, but commonly:
    'Application Number' / 'Lodged' / 'Description' / 'Location' / 'Status'.
    """
    rows_data = page.evaluate("""
      () => {
        const tables = Array.from(document.querySelectorAll('table'));
        function hasAppHeader(t) {
          const headerRow = t.querySelector('tr');
          if (!headerRow) return false;
          const txt = headerRow.innerText.toLowerCase();
          return /application\\s*(number|no|id)/.test(txt) && /lodged|date/.test(txt);
        }
        const t = tables.find(hasAppHeader);
        if (!t) return { headers: [], rows: [] };
        const headerRow = t.querySelector('tr');
        const headers = Array.from(headerRow.querySelectorAll('th, td')).map(c => c.innerText.trim());
        const rows = Array.from(t.querySelectorAll('tr')).slice(1)
          .map(r => Array.from(r.querySelectorAll('td')).map(td => ({
            text: td.innerText.trim(),
            href: (td.querySelector('a') ? td.querySelector('a').href : null),
          })))
          .filter(c => c.length > 0);
        return { headers, rows };
      }
    """)

    headers = [h.lower() for h in rows_data.get("headers", [])]
    out: list[DaListingRow] = []
    for row in rows_data.get("rows", []):
        if not row:
            continue
        cells = {h: c["text"] for h, c in zip(headers, row)}
        app_id = row[0]["text"].strip()
        if not app_id:
            continue
        href = row[0].get("href")
        raw_address = (
            cells.get("location") or cells.get("property") or cells.get("address")
        )
        street, suburb, postcode, st = split_council_address(raw_address)
        out.append(DaListingRow(
            council_slug=council_slug,
            vendor=vendor,
            application_id=app_id,
            application_url=href,
            type_code=extract_type_code(app_id),
            application_type=cells.get("application type") or cells.get("type"),
            lodged_date=parse_au_date(cells.get("lodged") or cells.get("lodgement date") or cells.get("date lodged")),
            raw_address=raw_address,
            street_address=street,
            suburb=suburb,
            postcode=postcode,
            state=st or state,
            status=cells.get("status"),
            raw_row=cells,
        ))
    return out


_ETRACK_DETAIL_LABELS = (
    "Application No",
    "Application Number",
    "Description",
    "Lodged",
    "Status",
    "Location",
    "Property",
    "Applicant",
    "Decision",
    "Decision Date",
    "Determined",
)


def _parse_etrack_detail(page: Page, app_id_hint: str, *, council_slug: str, vendor: str, state: str) -> DaDetailRecord:
    """SCAFFOLD detail parser — extracts label/value pairs from any
    table-of-key-value-rows on the detail page. Most eProperty detail
    pages render the application metadata in such a table.
    """
    fields = page.evaluate("""
      () => {
        const out = {};
        // Pattern 1: <table> rows with two cells (label | value)
        document.querySelectorAll('table tr').forEach(r => {
          const tds = r.querySelectorAll('td, th');
          if (tds.length === 2) {
            const k = tds[0].innerText.trim().replace(/:$/, '');
            const v = tds[1].innerText.trim();
            if (k && v && !(k in out)) out[k] = v;
          }
        });
        // Pattern 2: <dt>/<dd>
        const dts = document.querySelectorAll('dt');
        dts.forEach(dt => {
          const k = dt.innerText.trim().replace(/:$/, '');
          const dd = dt.nextElementSibling;
          if (dd && dd.tagName === 'DD' && k) out[k] = dd.innerText.trim();
        });
        return out;
      }
    """)

    def find(*keys: str):
        for k in keys:
            for fk, fv in fields.items():
                if fk.lower() == k.lower():
                    return fv
        return None

    description = find("Description", "Application Description", "Proposed Use")
    raw_address = find("Location", "Property", "Property Address", "Address")
    street, suburb, postcode, st = split_council_address(raw_address)

    return DaDetailRecord(
        council_slug=council_slug,
        vendor=vendor,
        application_id=app_id_hint,
        application_url=page.url,
        application_type=find("Application Type", "Type"),
        type_code=extract_type_code(app_id_hint),
        description=description,
        status=find("Status"),
        decision_outcome=find("Decision", "Determination"),
        lodged_date=parse_au_date(find("Lodged", "Lodgement Date", "Date Lodged")),
        decision_date=parse_au_date(find("Decision Date", "Determined", "Date Determined")),
        applicant_name=find("Applicant", "Applicant Name"),
        owner_name=find("Owner", "Owner Name"),
        internal_property_id=extract_internal_property_id(description),
        lot_on_plan=find("Lot/DP", "Lot on Plan", "Lot/Plan"),
        raw_address=raw_address,
        street_address=street,
        suburb=suburb,
        postcode=postcode,
        state=st or state,
        raw_fields=fields,
    )


def _doc_oid_from_url(url: str) -> str | None:
    """Pull a stable doc id from an eProperty document URL.
    SCAFFOLD — patterns vary; fall back to a hash of the URL."""
    if not url:
        return None
    m = re.search(r"[?&](?:DocumentId|DocId|FileId|Id)=([A-Za-z0-9_\-]+)", url, re.I)
    if m:
        return m.group(1)
    # Last-segment fallback
    last = url.rstrip("/").rsplit("/", 1)[-1]
    return last[:60] if last else None
