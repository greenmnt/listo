"""TechnologyOne T1Cloud DA scraper — new SaaS SPA.

Tenants:
  Newcastle post-Feb 2026:
    https://cn.t1cloud.com/apps/Applications/Search/MyServices/Application_Search

This is the new T1 SaaS frontend. It's almost certainly an Angular/React
SPA backed by a JSON API; pre-rendered HTML won't have the data. Two
approaches:
  1) Use the JSON API directly (httpx) — requires reverse-engineering
     the auth/token flow.
  2) Drive playwright, wait for the SPA to render, then scrape the DOM
     and intercept XHR responses for raw data.

This SCAFFOLD takes the playwright + XHR-intercept approach. The
selectors below need confirmation against the live site (we currently
have only the URL, not a sample HTML/network trace).
"""
from __future__ import annotations

import hashlib
import json
import logging
import mimetypes
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
class TechOneT1CloudConfig:
    council_slug: str
    search_url: str
    state: str = "NSW"


NEWCASTLE_CONFIG = TechOneT1CloudConfig(
    council_slug="newcastle",
    search_url="https://cn.t1cloud.com/apps/Applications/Search/MyServices/Application_Search",
    state="NSW",
)


class TechOneT1CloudScraper:
    vendor: str = "techone_t1cloud"

    def __init__(
        self,
        config: TechOneT1CloudConfig,
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
        # XHR JSON responses captured during the current page lifetime
        self._captured_json: list[dict] = []

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
        self._page.on("response", self._on_response)
        return self

    def __exit__(self, *_a):
        try:
            if self._ctx:
                self._ctx.close()
        finally:
            if self._pw:
                self._pw.stop()

    def _on_response(self, response):
        """Capture JSON responses from API endpoints — these carry the
        actual application data (the SPA renders from them)."""
        try:
            ct = response.headers.get("content-type", "")
            if "application/json" not in ct.lower():
                return
            url = response.url
            # T1Cloud APIs typically live under /apps/<APP>/api/ or similar.
            if "/api/" not in url and "/Service/" not in url:
                return
            try:
                body = response.json()
            except Exception:
                return
            self._captured_json.append({"url": url, "body": body})
        except Exception:
            pass

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

        self._captured_json = []
        page.goto(self.config.search_url, wait_until="domcontentloaded", timeout=60000)
        # SPA hydration — wait for the search form to render.
        page.wait_for_load_state("networkidle", timeout=45000)

        # SCAFFOLD selectors. T1Cloud commonly uses Material-style inputs;
        # the placeholder/label likely contains "Lodged" or "Date".
        from_input = _first_locator(page, [
            "input[placeholder*='Lodged from' i]",
            "input[placeholder*='From' i][type='text']",
            "input[aria-label*='From' i][type='text']",
        ])
        to_input = _first_locator(page, [
            "input[placeholder*='Lodged to' i]",
            "input[placeholder*='To' i][type='text']",
            "input[aria-label*='To' i][type='text']",
        ])
        if from_input is None or to_input is None:
            raise RuntimeError(
                "techone_t1cloud: lodgement date inputs not found; inspect "
                f"{page.url} and update selectors."
            )

        from_input.fill(date_from.strftime("%d/%m/%Y"))
        to_input.fill(date_to.strftime("%d/%m/%Y"))

        submit = _first_locator(page, [
            "button:has-text('Search')",
            "button[type=submit]",
        ])
        if submit is None:
            raise RuntimeError("techone_t1cloud: search button not found")
        self._sleep()
        started = datetime.utcnow()
        submit.click(timeout=15000)
        page.wait_for_load_state("networkidle", timeout=45000)

        self._record_page(sink, purpose="list", started=started)

        # Prefer parsed JSON when available — it's authoritative.
        for app in _flatten_t1_application_json(self._captured_json):
            yield _t1_app_to_listing(app, council_slug=self.council_slug, vendor=self.vendor, state=self.config.state)

        # SPA pagination: detect a 'next page' button and re-emit. T1Cloud
        # commonly uses MUI pagination — selector: button[aria-label*=Next i].
        while _click_t1_next(page):
            self._captured_json = []
            page.wait_for_load_state("networkidle", timeout=30000)
            self._sleep()
            started = datetime.utcnow()
            self._record_page(sink, purpose="list", started=started)
            for app in _flatten_t1_application_json(self._captured_json):
                yield _t1_app_to_listing(app, council_slug=self.council_slug, vendor=self.vendor, state=self.config.state)

    # ---------------- detail ----------------

    def fetch_detail(self, listing: DaListingRow, sink: RequestSink) -> DaDetailRecord:
        page = self._page
        assert page is not None
        if not listing.application_url:
            raise RuntimeError(f"techone_t1cloud: no detail URL for {listing.application_id}")
        self._captured_json = []
        started = datetime.utcnow()
        page.goto(listing.application_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_load_state("networkidle", timeout=45000)
        self._record_page(sink, purpose="detail", started=started)

        # Reach into the captured JSON for the detail payload.
        detail_json = _find_t1_detail_json(self._captured_json, listing.application_id)
        if detail_json is None:
            # Fall back to scraping the rendered DOM for label/value pairs.
            return _t1_dom_detail(page, listing, state=self.config.state)
        return _t1_json_to_detail(detail_json, listing, state=self.config.state)

    # ---------------- documents ----------------

    def list_documents(self, detail: DaDetailRecord, sink: RequestSink) -> list[DaDocumentRef]:
        page = self._page
        assert page is not None
        # Find a 'Documents' tab / accordion section.
        try:
            page.click("text=Documents", timeout=3000)
            page.wait_for_load_state("networkidle", timeout=10000)
            started = datetime.utcnow()
            self._record_page(sink, purpose="docs_index", started=started)
        except Exception:
            pass

        # Documents typically render as anchors with .pdf hrefs or with
        # a download icon button. Match either.
        rows = page.evaluate("""
          () => Array.from(document.querySelectorAll('a, button')).filter(el => {
            const t = (el.innerText || el.title || '').toLowerCase();
            const href = (el.href || '').toLowerCase();
            return /\\.pdf|\\.doc|attachment|download/.test(href) ||
                   /download/.test(t);
          }).map(el => ({
            href: el.href || null,
            text: (el.innerText || el.title || '').trim(),
          }))
        """)
        out: list[DaDocumentRef] = []
        for r in rows:
            href = r.get("href")
            if not href:
                continue
            out.append(DaDocumentRef(
                doc_oid=_doc_oid_from_url(href),
                title=r.get("text") or None,
                source_url=href,
            ))
        return out

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


# ---------------- helpers ----------------


def _first_locator(page: Page, selectors: list[str]):
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            if loc.count() > 0 and loc.is_visible(timeout=500):
                return loc
        except Exception:
            continue
    return None


def _click_t1_next(page: Page) -> bool:
    try:
        nxt = page.locator(
            "button[aria-label*='Next' i], button:has-text('Next')"
        ).first
        if nxt.count() == 0 or not nxt.is_visible() or nxt.is_disabled():
            return False
        nxt.click(timeout=10000)
        return True
    except Exception:
        return False


def _flatten_t1_application_json(captured: list[dict]) -> list[dict]:
    """Look through captured JSON responses for ones that contain a list
    of applications. Heuristic: object/array with keys like
    'ApplicationNumber'/'AppNo'/'LodgementDate'.
    """
    out: list[dict] = []
    for entry in captured:
        body = entry.get("body")
        for record in _walk_json_for_apps(body):
            out.append(record)
    return out


def _walk_json_for_apps(node):
    if isinstance(node, list):
        for item in node:
            yield from _walk_json_for_apps(item)
    elif isinstance(node, dict):
        keys_lower = {k.lower() for k in node.keys()}
        if any(k in keys_lower for k in ("applicationnumber", "appno", "applicationno")):
            yield node
        for v in node.values():
            if isinstance(v, (list, dict)):
                yield from _walk_json_for_apps(v)


def _t1_app_to_listing(app: dict, *, council_slug: str, vendor: str, state: str) -> DaListingRow:
    def g(*keys: str):
        for k in keys:
            for ak, av in app.items():
                if ak.lower() == k.lower():
                    return av
        return None

    app_id = str(g("ApplicationNumber", "AppNo", "ApplicationNo") or "").strip()
    raw_address = g("PropertyAddress", "Location", "Address")
    street, suburb, postcode, st = split_council_address(raw_address)
    return DaListingRow(
        council_slug=council_slug,
        vendor=vendor,
        application_id=app_id,
        application_url=g("DetailUrl", "Url", "ApplicationUrl"),
        type_code=extract_type_code(app_id),
        application_type=g("ApplicationType", "Type"),
        lodged_date=parse_au_date(g("LodgementDate", "LodgedDate", "DateLodged")),
        raw_address=raw_address,
        street_address=street,
        suburb=suburb,
        postcode=postcode,
        state=st or state,
        status=g("Status"),
        raw_row=app,
    )


def _find_t1_detail_json(captured: list[dict], app_id: str) -> dict | None:
    """Pick the captured JSON response whose body identifies app_id.
    SCAFFOLD — heuristic match."""
    for entry in captured:
        body = entry.get("body")
        try:
            if json.dumps(body).find(app_id) >= 0:
                # Walk for the most-detailed dict containing app_id
                for cand in _walk_json_for_apps(body):
                    if str(cand.get("ApplicationNumber", "") or cand.get("AppNo", "")).strip() == app_id:
                        return cand
        except Exception:
            continue
    return None


def _t1_json_to_detail(app: dict, listing: DaListingRow, *, state: str) -> DaDetailRecord:
    def g(*keys: str):
        for k in keys:
            for ak, av in app.items():
                if ak.lower() == k.lower():
                    return av
        return None

    description = g("Description", "ProposedUse", "ApplicationDescription")
    raw_address = g("PropertyAddress", "Location", "Address") or listing.raw_address
    street, suburb, postcode, st = split_council_address(raw_address)
    return DaDetailRecord(
        council_slug=listing.council_slug,
        vendor=listing.vendor,
        application_id=listing.application_id,
        application_url=listing.application_url,
        application_type=g("ApplicationType", "Type"),
        type_code=extract_type_code(listing.application_id),
        description=description,
        status=g("Status"),
        decision_outcome=g("Decision", "Determination"),
        lodged_date=parse_au_date(g("LodgementDate", "LodgedDate", "DateLodged")),
        decision_date=parse_au_date(g("DecisionDate", "DeterminationDate")),
        applicant_name=g("Applicant", "ApplicantName"),
        owner_name=g("Owner", "OwnerName"),
        internal_property_id=extract_internal_property_id(description),
        lot_on_plan=g("LotPlan", "Lot/DP", "LotDP"),
        raw_address=raw_address,
        street_address=street,
        suburb=suburb,
        postcode=postcode,
        state=st or state,
        raw_fields=app,
    )


def _t1_dom_detail(page: Page, listing: DaListingRow, *, state: str) -> DaDetailRecord:
    """Fallback when no JSON detail payload was captured — scrape labels."""
    fields = page.evaluate("""
      () => {
        const out = {};
        document.querySelectorAll('dt').forEach(dt => {
          const dd = dt.nextElementSibling;
          if (dd && dd.tagName === 'DD') out[dt.innerText.trim().replace(/:$/, '')] = dd.innerText.trim();
        });
        document.querySelectorAll('label, .field-label, [class*=Label]').forEach(lbl => {
          const v = lbl.nextElementSibling;
          if (v) {
            const k = lbl.innerText.trim().replace(/:$/, '');
            const val = v.innerText.trim();
            if (k && val && !(k in out)) out[k] = val;
          }
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

    description = find("Description", "Proposed Use")
    raw_address = find("Property Address", "Location", "Address") or listing.raw_address
    street, suburb, postcode, st = split_council_address(raw_address)
    return DaDetailRecord(
        council_slug=listing.council_slug,
        vendor=listing.vendor,
        application_id=listing.application_id,
        application_url=page.url,
        application_type=find("Application Type", "Type"),
        type_code=extract_type_code(listing.application_id),
        description=description,
        status=find("Status"),
        lodged_date=parse_au_date(find("Lodged", "Lodgement Date")),
        decision_date=parse_au_date(find("Decision Date", "Determined")),
        applicant_name=find("Applicant"),
        owner_name=find("Owner"),
        internal_property_id=extract_internal_property_id(description),
        raw_address=raw_address,
        street_address=street,
        suburb=suburb,
        postcode=postcode,
        state=st or state,
        raw_fields=fields,
    )


def _doc_oid_from_url(url: str) -> str | None:
    if not url:
        return None
    import re as _re
    m = _re.search(r"[?&](?:DocumentId|DocId|FileId|Id)=([A-Za-z0-9_\-]+)", url, _re.I)
    if m:
        return m.group(1)
    last = url.rstrip("/").rsplit("/", 1)[-1]
    return last[:60] if last else None
