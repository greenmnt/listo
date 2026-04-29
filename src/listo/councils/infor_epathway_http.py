"""HTTP-based Infor ePathway DA scraper (v2).

Same `CouncilScraper` protocol as ``infor_epathway.py`` but talks raw
HTTP instead of driving a browser. Manages ASP.NET WebForms postback
state (``__VIEWSTATE`` / ``__VIEWSTATEGENERATOR`` / ``__EVENTVALIDATION``
/ ``__PREVIOUSPAGE``) across calls, parses HTML with selectolax.

Much faster and lighter than the browser version. The trade-off is
that we don't get JS execution — fine for ePathway because all the
state lives in hidden form fields and there's no JS challenge.

The Playwright version stays in place for COGC by default; use
``cogc_http`` (or any other slug pointed at this scraper) to opt into
the v2 backend.
"""
from __future__ import annotations

import hashlib
import logging
import mimetypes
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterator
from urllib.parse import urljoin

import httpx
from selectolax.parser import HTMLParser

from listo.councils.base import (
    DaDetailRecord,
    DaDocumentRef,
    DaListingRow,
    DownloadedDocument,
    FetchRecord,
    RequestSink,
)
from listo.councils.infor_epathway import (
    DEFAULT_DOC_DIR,
    InforEpathwayConfig,
    _extract_doc_label,
    _parse_published_at,
    _select_download_indices,
)
from listo.councils.parsing import (
    count_pdf_pages,
    extract_internal_property_id,
    extract_type_code,
    parse_au_date,
    parse_size_to_bytes,
    safe_filename,
    split_council_address,
)


logger = logging.getLogger(__name__)


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Postback / form-state helpers
# ---------------------------------------------------------------------------


def _extract_form_state(html: str) -> dict[str, str]:
    """Build a name→value dict for every <input>, <select>, <textarea>
    on the page that has a `name` attribute. We feed this back into the
    next POST so the server's WebForms state machine stays consistent.

    For <select>: take the value of the option marked selected (or the
    first option if none is marked).
    For <input type="checkbox"|"radio">: include only when checked.
    For <input type="submit"|"image"|"button">: skip — those are the
    things we choose to "press" by name on each postback.
    """
    doc = HTMLParser(html)
    fields: dict[str, str] = {}

    for el in doc.css("input"):
        name = el.attributes.get("name")
        if not name:
            continue
        itype = (el.attributes.get("type") or "text").lower()
        if itype in ("submit", "image", "button"):
            continue
        if itype in ("checkbox", "radio"):
            # Only include when actually checked
            if el.attributes.get("checked") is not None:
                fields[name] = el.attributes.get("value", "on") or "on"
            continue
        fields[name] = el.attributes.get("value", "") or ""

    for el in doc.css("textarea"):
        name = el.attributes.get("name")
        if name:
            fields[name] = (el.text() or "").strip()

    for el in doc.css("select"):
        name = el.attributes.get("name")
        if not name:
            continue
        # Selected option, or first option as fallback
        selected = el.css_first("option[selected]")
        if selected is None:
            selected = el.css_first("option")
        fields[name] = (selected.attributes.get("value", "") if selected else "") or ""

    return fields


def _form_action(html: str, base_url: str) -> str:
    """Resolve the form's action URL against base_url. Falls back to
    base_url if the form has no explicit action."""
    doc = HTMLParser(html)
    form = doc.css_first("form#aspnetForm") or doc.css_first("form")
    if form is None:
        return base_url
    action = form.attributes.get("action") or ""
    if not action:
        return base_url
    return urljoin(base_url, action)


# ---------------------------------------------------------------------------
# scraper
# ---------------------------------------------------------------------------


class InforEpathwayHttpScraper:
    """ePathway scraper that uses raw HTTP instead of a browser."""

    vendor: str = "infor_epathway"

    def __init__(
        self,
        config: InforEpathwayConfig,
        *,
        jitter_min: float = 1.0,
        jitter_max: float = 2.5,
    ):
        self.config = config
        self.council_slug = config.council_slug
        self._jitter_min = jitter_min
        self._jitter_max = jitter_max
        self._client: httpx.Client | None = None
        self._skip_ids: set[str] = set()
        # State carried across postbacks within one enquiry list walk:
        #   - results_url: the canonical search-results URL (Referer source)
        #   - results_html: most recent results-page HTML (for next postback state)
        self._results_url: str | None = None
        self._results_html: str | None = None

    # ---- context manager ----

    def __enter__(self):
        self._client = httpx.Client(
            http2=True,
            follow_redirects=True,
            timeout=30,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "en-AU,en;q=0.9",
            },
        )
        return self

    def __exit__(self, *_a):
        if self._client is not None:
            self._client.close()
            self._client = None

    # ---- helpers ----

    def _sleep(self) -> None:
        time.sleep(random.uniform(self._jitter_min, self._jitter_max))

    def _http(
        self,
        method: str,
        url: str,
        *,
        sink: RequestSink,
        purpose: str,
        referer: str | None = None,
        data: dict[str, str] | None = None,
        application_id: int | None = None,
        record: bool = True,
        stream: bool = False,
    ) -> httpx.Response:
        """Single HTTP call with sink logging. Returns the response (raw
        bytes available via ``.content``)."""
        assert self._client is not None
        headers: dict[str, str] = {}
        if referer:
            headers["Referer"] = referer
        if data is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        started = datetime.utcnow()
        try:
            if stream:
                # Used for binary downloads where we want to stream into a file.
                # The caller is responsible for reading .content / .iter_bytes
                # and closing the response.
                response = self._client.send(
                    self._client.build_request(method, url, headers=headers, data=data),
                    stream=True,
                )
            else:
                response = self._client.request(method, url, headers=headers, data=data)
            elapsed = int((datetime.utcnow() - started).total_seconds() * 1000)
            if record and not stream:
                ct = response.headers.get("content-type", "")
                body = response.content
                is_html = "text/html" in ct
                sink.record(FetchRecord(
                    purpose=purpose,
                    method=method,
                    url=str(response.url),
                    started_at=started,
                    elapsed_ms=elapsed,
                    http_status=response.status_code,
                    bytes_received=len(body),
                    body=body if is_html else None,
                    body_is_html=is_html,
                    content_type=ct,
                    application_id=application_id,
                ))
            return response
        except Exception as e:
            elapsed = int((datetime.utcnow() - started).total_seconds() * 1000)
            sink.record(FetchRecord(
                purpose=purpose,
                method=method,
                url=url,
                started_at=started,
                elapsed_ms=elapsed,
                error=str(e),
                application_id=application_id,
            ))
            raise

    # ---- search / pagination ----

    def iter_listings(
        self,
        *,
        date_from: date,
        date_to: date,
        sink: RequestSink,
        skip_application_ids: set[str] | None = None,
    ) -> Iterator[DaListingRow]:
        """Walk every enquiry list configured for this council."""
        self._skip_ids = skip_application_ids or set()
        for enquiry_label in self.config.enquiry_lists:
            yield from self._iter_one_list(enquiry_label, date_from, date_to, sink)

    def _iter_one_list(
        self,
        enquiry_label: str,
        date_from: date,
        date_to: date,
        sink: RequestSink,
    ) -> Iterator[DaListingRow]:
        logger.info(
            "[%s] (http) starting list walk: enquiry=%r window=%s..%s (skipping %d already-complete)",
            self.council_slug, enquiry_label, date_from, date_to, len(self._skip_ids),
        )

        results_url, results_html = self._submit_search_for_enquiry(
            enquiry_label, date_from, date_to, sink,
        )
        self._results_url = results_url
        self._results_html = results_html

        page_index = 1
        total_rows = 0
        while True:
            pager_text = _read_pager_text(results_html)
            rows = _parse_results_table(results_html)
            logger.info(
                "[%s] (http) results page %d: parsed %d rows%s",
                self.council_slug, page_index, len(rows),
                f" ({pager_text})" if pager_text else "",
            )

            for i, raw in enumerate(rows, start=1):
                listing = _row_to_listing(raw, council_slug=self.council_slug)
                if listing.application_id in self._skip_ids:
                    logger.info(
                        "[%s] (http) page %d row %d/%d: %s — already complete, skipping",
                        self.council_slug, page_index, i, len(rows),
                        listing.application_id,
                    )
                    total_rows += 1
                    yield listing
                    continue
                logger.info(
                    "[%s] (http) page %d row %d/%d: %s — %s — %s",
                    self.council_slug, page_index, i, len(rows),
                    listing.application_id,
                    (listing.application_type or "?")[:40],
                    (listing.suburb or listing.raw_address or "?")[:40],
                )
                try:
                    self._capture_detail_inline(listing, sink, results_url=results_url)
                except Exception as e:
                    logger.warning(
                        "[%s] (http)   ↳ detail capture FAILED for %s: %s",
                        self.council_slug, listing.application_id, e,
                    )
                total_rows += 1
                yield listing

            # Advance to next page via postback (image button has the
            # next-page navigation wired up server-side).
            next_html = self._click_next_page(results_url, results_html, sink)
            if next_html is None:
                logger.info(
                    "[%s] (http) no Next page — finished walk: %d pages, %d rows",
                    self.council_slug, page_index, total_rows,
                )
                return
            page_index += 1
            self._sleep()
            results_html = next_html
            self._results_html = results_html
            logger.info("[%s] (http) advanced to page %d", self.council_slug, page_index)

    def _submit_search_for_enquiry(
        self,
        enquiry_label: str,
        date_from: date,
        date_to: date,
        sink: RequestSink,
    ) -> tuple[str, str]:
        """Walk EnquiryLists → EnquirySearch → EnquirySummaryView.
        Returns (results_url, results_html)."""
        # 1. GET the enquiry-list selection page
        list_url = self.config.lists_url
        r = self._http("GET", list_url, sink=sink, purpose="list")
        list_html = r.text
        list_url_resolved = str(r.url)

        # 2. POST with the chosen enquiry list radio + Next button.
        # The radio's value is the enquiry list id (e.g. "102"). We
        # discover it by matching the visible label in the dropdown.
        enquiry_list_id = _find_enquiry_list_id(list_html, enquiry_label)
        if enquiry_list_id is None:
            raise RuntimeError(
                f"could not find enquiry list radio for {enquiry_label!r}"
            )

        fields = _extract_form_state(list_html)
        # Set the enquiry list dropdown to the chosen id
        fields = _set_dropdown(fields, list_html, "mEnquiryListsDropDownList", enquiry_list_id)
        # Press Next (the image-button on the entry page)
        next_button = _find_button_name(list_html, value_match="Next")
        if next_button:
            fields[f"{next_button}.x"] = "1"
            fields[f"{next_button}.y"] = "1"

        action = _form_action(list_html, list_url_resolved)
        r = self._http(
            "POST", action, sink=sink, purpose="list",
            data=fields, referer=list_url_resolved,
        )
        search_url = str(r.url)
        search_html = r.text

        # 3. POST: switch to "Date range search" tab via tab control
        #    postback. The tab control exposes its menu via a TabControl
        #    panel; the postback target is "...$mTabControl$tabControlMenu"
        #    with __EVENTARGUMENT being the tab index.
        tab_target, tab_index = _find_date_range_tab(search_html)
        if tab_target is None:
            raise RuntimeError("could not locate Date range search tab")
        fields = _extract_form_state(search_html)
        fields["__EVENTTARGET"] = tab_target
        fields["__EVENTARGUMENT"] = tab_index
        action = _form_action(search_html, search_url)
        r = self._http(
            "POST", action, sink=sink, purpose="list",
            data=fields, referer=search_url,
        )
        search_url = str(r.url)
        search_html = r.text

        # 4. Fill the date inputs and POST with the Search button.
        from_name = _find_input_name_ending(search_html, "$mFromDatePicker$dateTextBox")
        to_name = _find_input_name_ending(search_html, "$mToDatePicker$dateTextBox")
        if not (from_name and to_name):
            raise RuntimeError(
                "could not locate date inputs (mFromDatePicker / mToDatePicker)"
            )
        fields = _extract_form_state(search_html)
        fields[from_name] = date_from.strftime("%d/%m/%Y")
        fields[to_name] = date_to.strftime("%d/%m/%Y")
        fields["__EVENTTARGET"] = ""
        fields["__EVENTARGUMENT"] = ""
        # Press the Search submit button by name=value
        search_button = _find_button_name(search_html, value_match="Search")
        if search_button:
            fields[search_button] = "Search"
        action = _form_action(search_html, search_url)
        r = self._http(
            "POST", action, sink=sink, purpose="list",
            data=fields, referer=search_url,
        )
        return str(r.url), r.text

    def _click_next_page(
        self,
        results_url: str,
        results_html: str,
        sink: RequestSink,
    ) -> str | None:
        """Postback the next-page image button. Returns the new results
        HTML, or None if there is no next page."""
        next_name = _find_next_page_button(results_html)
        if not next_name:
            return None
        fields = _extract_form_state(results_html)
        fields["__EVENTTARGET"] = next_name
        fields["__EVENTARGUMENT"] = ""
        # Image-button click: include .x / .y too so older ASP.NET
        # tenants that detect via coordinate fields still process it.
        fields[f"{next_name}.x"] = "1"
        fields[f"{next_name}.y"] = "1"
        action = _form_action(results_html, results_url)
        try:
            r = self._http(
                "POST", action, sink=sink, purpose="list",
                data=fields, referer=results_url,
            )
        except Exception as e:
            logger.warning("[%s] (http) next-page POST failed: %s", self.council_slug, e)
            return None
        return r.text

    # ---- per-row capture ----

    def _capture_detail_inline(
        self,
        listing: DaListingRow,
        sink: RequestSink,
        *,
        results_url: str,
    ) -> None:
        """GET the detail URL with proper Referer; parse; walk docs portal."""
        url = listing.application_url or ""
        if not url.startswith("http") or "EnquiryDetailView.aspx" not in url:
            logger.info(
                "[%s] (http)   ↳ unexpected detail URL %r — skipping",
                self.council_slug, url[:120],
            )
            return

        self._sleep()
        r = self._http(
            "GET", url, sink=sink, purpose="detail",
            referer=results_url,
        )
        if r.status_code >= 400 or "Error.aspx" in str(r.url):
            logger.warning(
                "[%s] (http)   ↳ landed on error/non-detail page (%s) — skipping",
                self.council_slug, str(r.url)[:120],
            )
            return

        listing.application_url = str(r.url)
        listing.inline_detail = _parse_detail_page(
            r.text, listing.application_id,
            council_slug=self.council_slug, vendor=self.vendor,
            url=str(r.url),
        )

        try:
            listing.inline_documents = self._capture_docs_inline(listing, sink)
        except Exception as e:
            logger.warning(
                "[%s] (http)   ↳ docs capture FAILED for %s: %s",
                self.council_slug, listing.application_id, e,
            )
            listing.inline_documents = None

    def _capture_docs_inline(
        self,
        listing: DaListingRow,
        sink: RequestSink,
    ) -> list[DownloadedDocument]:
        """Visit the documents portal, parse #gvDocs, download first/last."""
        internal_id = _epathway_id_from_url(listing.application_url)
        if not internal_id:
            return []

        url = self.config.docs_portal_id_url.format(id=internal_id)
        r = self._http(
            "GET", url, sink=sink, purpose="docs_index",
            referer=listing.application_url,
        )
        rows = _parse_gvdocs(r.text)
        if not rows:
            logger.info(
                "[%s] (http)   ↳ docs portal: 0 documents",
                self.council_slug,
            )
            return []
        logger.info(
            "[%s] (http)   ↳ docs portal: %d documents listed",
            self.council_slug, len(rows),
        )

        # Build refs sorted chronologically; download first + last only.
        refs: list[DaDocumentRef] = []
        for raw in rows:
            pub = _parse_published_at(raw.get("date_published"))
            refs.append(DaDocumentRef(
                doc_oid=raw["oid"] or None,
                title=raw["name"] or None,
                doc_type=_extract_doc_label(raw["name"], listing.application_id),
                source_url=urljoin(url, raw["href"]) if raw["href"] else None,
                size_text=raw["size_text"] or None,
                published_at=pub,
            ))
        refs.sort(key=lambda r: (r.published_at is None, r.published_at or datetime.min))
        download_idx = _select_download_indices(len(refs))

        target_dir = DEFAULT_DOC_DIR / listing.application_id.replace("/", "_")
        target_dir.mkdir(parents=True, exist_ok=True)

        results: list[DownloadedDocument] = []
        for j, ref in enumerate(refs, start=1):
            do_download = (j - 1) in download_idx
            tag = "DOWNLOAD" if do_download else "metadata"
            logger.info(
                "[%s] (http)     ↳ doc %d/%d [%s] %s — %s (%s, pub=%s)",
                self.council_slug, j, len(refs), tag,
                ref.doc_oid or "?",
                (ref.title or "?")[:50],
                ref.size_text or "?",
                ref.published_at.isoformat(timespec='seconds') if ref.published_at else "—",
            )
            if do_download:
                try:
                    results.append(self._download_one(ref, target_dir, sink))
                    continue
                except Exception as e:
                    logger.warning(
                        "[%s] (http)     ↳ download failed for %s: %s — keeping metadata-only",
                        self.council_slug, ref.doc_oid, e,
                    )
            results.append(DownloadedDocument(
                doc_oid=ref.doc_oid,
                title=ref.title,
                doc_type=ref.doc_type,
                source_url=ref.source_url,
                file_path=None,
                file_size=parse_size_to_bytes(ref.size_text),
                mime_type=None,
                content_hash=None,
                page_count=None,
                published_at=ref.published_at,
            ))
        return results

    def _download_one(
        self,
        doc: DaDocumentRef,
        target_dir: Path,
        sink: RequestSink,
    ) -> DownloadedDocument:
        """GET the doc URL, save to disk, hash, return persistence record."""
        if not doc.source_url:
            raise RuntimeError(f"no source URL for doc {doc.doc_oid}")
        self._sleep()
        started = datetime.utcnow()
        # Stream so we don't hold a big PDF in memory while writing.
        with self._client.stream("GET", doc.source_url) as response:  # type: ignore[union-attr]
            response.raise_for_status()
            mime = response.headers.get("content-type", "").split(";")[0].strip() or None
            disposition = response.headers.get("content-disposition", "")
            ext = _ext_from_disposition(disposition) or _ext_from_mime(mime) or ".bin"
            fname = safe_filename(doc.doc_oid or "file") + ext
            path = target_dir / fname
            sha = hashlib.sha256()
            size = 0
            with open(path, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)
                    sha.update(chunk)
                    size += len(chunk)
        elapsed = int((datetime.utcnow() - started).total_seconds() * 1000)
        sink.record(FetchRecord(
            purpose="doc_download",
            method="GET",
            url=doc.source_url,
            started_at=started,
            elapsed_ms=elapsed,
            http_status=200,
            bytes_received=size,
            content_type=mime,
        ))
        pages = count_pdf_pages(path) if (mime == "application/pdf" or ext.lower() == ".pdf") else None
        return DownloadedDocument(
            doc_oid=doc.doc_oid,
            title=doc.title,
            doc_type=doc.doc_type,
            source_url=doc.source_url,
            file_path=str(path),
            file_size=size,
            mime_type=mime,
            content_hash=sha.digest(),
            page_count=pages,
            published_at=doc.published_at,
        )

    # ---- protocol fallbacks (mostly no-ops since list walk does everything) ----

    def fetch_detail(self, listing: DaListingRow, sink: RequestSink) -> DaDetailRecord:
        """Standalone detail fetch — used by the orchestrator's docs phase
        for legacy rows whose detail wasn't captured during the list walk."""
        url = listing.application_url
        if not url:
            raise RuntimeError(f"no URL for {listing.application_id}")
        r = self._http(
            "GET", url, sink=sink, purpose="detail",
            referer=self._results_url or self.config.lists_url,
        )
        return _parse_detail_page(
            r.text, listing.application_id,
            council_slug=self.council_slug, vendor=self.vendor,
            url=str(r.url),
        )

    def list_documents(
        self,
        detail: DaDetailRecord,
        sink: RequestSink,
    ) -> list[DaDocumentRef]:
        internal_id = _epathway_id_from_url(detail.application_url)
        if not internal_id:
            return []
        url = self.config.docs_portal_id_url.format(id=internal_id)
        r = self._http(
            "GET", url, sink=sink, purpose="docs_index",
            referer=detail.application_url,
        )
        rows = _parse_gvdocs(r.text)
        return [
            DaDocumentRef(
                doc_oid=raw["oid"] or None,
                title=raw["name"] or None,
                doc_type=_extract_doc_label(raw["name"], detail.application_id),
                source_url=urljoin(url, raw["href"]) if raw["href"] else None,
                size_text=raw["size_text"] or None,
                published_at=_parse_published_at(raw.get("date_published")),
            )
            for raw in rows
        ]

    def download(
        self,
        doc: DaDocumentRef,
        target_dir: Path,
        sink: RequestSink,
    ) -> DownloadedDocument:
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        return self._download_one(doc, target_dir, sink)


# ---------------------------------------------------------------------------
# parsing helpers
# ---------------------------------------------------------------------------


def _read_pager_text(html: str) -> str | None:
    m = re.search(r"Page\s+\d+\s+of\s+\d+", html)
    return m.group(0) if m else None


def _parse_results_table(html: str) -> list[dict]:
    """Parse the search-results table. Returns one dict per row."""
    doc = HTMLParser(html)
    table = doc.css_first("table#gridResults")
    if table is None:
        # The results table id may differ; look for any table whose
        # first <th> says 'Application number'.
        for t in doc.css("table"):
            ths = t.css("th")
            if ths and "application number" in (ths[0].text() or "").lower():
                table = t
                break
    if table is None:
        return []

    headers = [(th.text() or "").strip().lower() for th in table.css("th")]
    rows: list[dict] = []
    for tr in table.css("tr"):
        if tr.css_first("th"):
            continue  # header row
        tds = tr.css("td")
        if not tds:
            continue
        cells: dict[str, str] = {}
        for h, td in zip(headers, tds):
            cells[h] = (td.text() or "").strip()
        first_a = tds[0].css_first("a")
        href = first_a.attributes.get("href") if first_a else None
        rows.append({
            "cells": cells,
            "href": href,
            "headers": headers,
        })
    return rows


def _row_to_listing(raw: dict, *, council_slug: str) -> DaListingRow:
    cells = raw["cells"]
    href = raw["href"]
    app_id = (cells.get("application number") or "").strip()
    raw_address = cells.get("application location") or cells.get("location") or None
    street, suburb, postcode, state = split_council_address(raw_address)
    return DaListingRow(
        council_slug=council_slug,
        vendor="infor_epathway",
        application_id=app_id,
        application_url=urljoin(_RESULTS_BASE, href) if href else None,
        type_code=extract_type_code(app_id),
        application_type=cells.get("application type"),
        lodged_date=parse_au_date(cells.get("lodgement date")),
        raw_address=raw_address,
        street_address=street,
        suburb=suburb or cells.get("location suburb") or cells.get("suburb"),
        postcode=postcode,
        state=state or "QLD",
        raw_row=cells,
    )


# Best-effort base for relative hrefs in result rows. The actual base
# URL is the EnquirySummaryView page; relative hrefs like
# 'EnquiryDetailView.aspx?Id=...' resolve correctly against any URL in
# the same directory.
_RESULTS_BASE = (
    "https://cogc.cloud.infor.com/ePathway/epthprod/Web/GeneralEnquiry/"
    "EnquirySummaryView.aspx"
)


_DETAIL_FIELD_LABELS = (
    "Application number",
    "Application description",
    "Application type",
    "Lodgement date",
    "Application location",
    "Status",
)


def _parse_detail_page(
    html: str,
    application_id_hint: str,
    *,
    council_slug: str,
    vendor: str,
    url: str | None = None,
) -> DaDetailRecord:
    """Parse the EnquiryDetailView.aspx page into a DaDetailRecord."""
    doc = HTMLParser(html)

    # The detail page renders fields like:
    #   <span class="AlternateContentHeading">Application number</span>
    #   <td><div class='AlternateContentText'>MCU/2026/159</div></td>
    # We can grab them by matching headings to following text.
    fields: dict[str, str] = {}
    for span in doc.css("span.AlternateContentHeading, span.ContentHeading"):
        label = (span.text() or "").strip().rstrip(":")
        if not label:
            continue
        # The value is the nearest following div with the *ContentText class
        parent = span.parent
        if parent is None:
            continue
        value_node = parent.css_first("div.AlternateContentText, div.ContentText")
        if value_node is not None:
            fields[label] = (value_node.text() or "").strip()

    # Decision grid: <table id="gridResults"> with cols
    # Decision type / Decision date / Decision authority
    decision_outcome = decision_date_str = decision_authority = None
    lot_on_plan = None
    for grid in doc.css("table#gridResults"):
        ths = [(th.text() or "").strip() for th in grid.css("th")]
        if not ths:
            continue
        row = grid.css("tr.ContentPanel, tr.AlternateContentPanel")
        first_data = row[0] if row else None
        if not first_data:
            continue
        tds = [(td.text() or "").strip() for td in first_data.css("td")]
        if ths[0] == "Decision type" and len(tds) >= 1:
            decision_outcome = tds[0] or None
            decision_date_str = tds[1] if len(tds) > 1 else None
            decision_authority = tds[2] if len(tds) > 2 else None
        elif ths[0] == "Lot on Plan" and tds:
            lot_on_plan = tds[0] or None

    raw_address = fields.get("Application location")
    street, suburb, postcode, state = split_council_address(raw_address)
    description = fields.get("Application description")

    return DaDetailRecord(
        council_slug=council_slug,
        vendor=vendor,
        application_id=application_id_hint,
        application_url=url,
        application_type=fields.get("Application type"),
        type_code=extract_type_code(application_id_hint),
        description=description,
        status=fields.get("Status"),
        decision_outcome=decision_outcome,
        decision_authority=decision_authority,
        lodged_date=parse_au_date(fields.get("Lodgement date")),
        decision_date=parse_au_date(decision_date_str),
        internal_property_id=extract_internal_property_id(description),
        lot_on_plan=lot_on_plan,
        raw_address=raw_address,
        street_address=street,
        suburb=suburb,
        postcode=postcode,
        state=state or "QLD",
        raw_fields=fields,
    )


def _parse_gvdocs(html: str) -> list[dict]:
    """Parse the docs portal #gvDocs table. Note the Date published
    column is hidden by DataTables so we read textContent (selectolax's
    .text() ignores display:none, so we need to walk children
    explicitly to grab the date)."""
    doc = HTMLParser(html)
    table = doc.css_first("table#gvDocs")
    if table is None:
        return []
    rows: list[dict] = []
    for tr in table.css("tbody tr"):
        tds = tr.css("td")
        if len(tds) < 4:
            continue
        a = tds[0].css_first("a")
        # tds[4] is the hidden Date published. selectolax .text(deep=True)
        # doesn't respect display:none in the same way innerText does in
        # browsers, so we get the raw text.
        date_published = None
        if len(tds) > 4:
            date_published = (tds[4].text() or "").strip()
        rows.append({
            "oid": (tds[0].text() or "").strip(),
            "href": a.attributes.get("href") if a else None,
            "name": (tds[1].text() or "").strip(),
            "size_text": (tds[3].text() or "").strip(),
            "date_published": date_published,
        })
    return rows


# ---------------------------------------------------------------------------
# little helpers
# ---------------------------------------------------------------------------


def _find_enquiry_list_id(html: str, label: str) -> str | None:
    """The enquiry-list <select> has options like:
       <option value='102'>Development applications after July 2017</option>
    Match on label text and return the value."""
    doc = HTMLParser(html)
    sel = doc.css_first("select[name$='mEnquiryListsDropDownList']")
    if sel is None:
        return None
    for opt in sel.css("option"):
        if (opt.text() or "").strip() == label:
            return opt.attributes.get("value")
    return None


def _set_dropdown(
    fields: dict[str, str],
    html: str,
    name_suffix: str,
    new_value: str,
) -> dict[str, str]:
    """Find the <select> whose name ends with name_suffix and set its
    value in `fields`."""
    doc = HTMLParser(html)
    for sel in doc.css("select"):
        name = sel.attributes.get("name") or ""
        if name.endswith(name_suffix) or name_suffix in name:
            fields[name] = new_value
            return fields
    return fields


def _find_button_name(html: str, *, value_match: str) -> str | None:
    """Find the first <input type='submit'|'image'|'button'> whose
    value contains `value_match` (case-insensitive). Return its
    name attribute."""
    doc = HTMLParser(html)
    needle = value_match.lower()
    for el in doc.css("input"):
        itype = (el.attributes.get("type") or "").lower()
        if itype not in ("submit", "image", "button"):
            continue
        val = (el.attributes.get("value") or el.attributes.get("alt") or "").lower()
        if needle in val:
            return el.attributes.get("name")
    return None


def _find_input_name_ending(html: str, suffix: str) -> str | None:
    doc = HTMLParser(html)
    for el in doc.css("input"):
        name = el.attributes.get("name") or ""
        if name.endswith(suffix):
            return name
    return None


def _find_date_range_tab(html: str) -> tuple[str | None, str]:
    """Locate the Date range tab postback. Returns (tab_target, index)."""
    doc = HTMLParser(html)
    # Tabs are anchors with href like
    # javascript:__doPostBack('ctl00$...$mTabControl$tabControlMenu','2')
    # The tab labelled 'Date range search' is the one we want.
    pat = re.compile(r"__doPostBack\('([^']*tabControlMenu)'\s*,\s*'(\d+)'")
    for a in doc.css("a"):
        text = (a.text() or "").strip().lower()
        if "date range" not in text:
            continue
        href = a.attributes.get("href") or ""
        m = pat.search(href)
        if m:
            return m.group(1), m.group(2)
    return None, ""


def _find_next_page_button(html: str) -> str | None:
    """Find the postback name of the 'Next page' image button. Returns
    None when we're already on the last page."""
    doc = HTMLParser(html)
    for el in doc.css("input[type='image']"):
        name = el.attributes.get("name") or ""
        if "nextpage" in name.lower() or "nextPage" in name:
            return name
        title = (el.attributes.get("title") or "").lower()
        if "next page" in title:
            return name
    # Fallback: anchor with __doPostBack('...nextPageHyperLink',...)
    for a in doc.css("a"):
        href = a.attributes.get("href") or ""
        m = re.search(r"__doPostBack\('([^']*nextPage[^']*)'", href, re.I)
        if m:
            return m.group(1)
    return None


def _epathway_id_from_url(url: str | None) -> str | None:
    if not url:
        return None
    m = re.search(r"[?&]Id=(\d+)", url, re.I)
    return m.group(1) if m else None


_EXT_RE = re.compile(r'filename\*?=(?:UTF-8\'\'|")?([^";]+)', re.I)


def _ext_from_disposition(disposition: str) -> str | None:
    if not disposition:
        return None
    m = _EXT_RE.search(disposition)
    if not m:
        return None
    fname = m.group(1).strip(' "')
    if "." in fname:
        return "." + fname.rsplit(".", 1)[1].lower()
    return None


def _ext_from_mime(mime: str | None) -> str | None:
    if not mime:
        return None
    return mimetypes.guess_extension(mime)
