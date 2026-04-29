"""Infor ePathway DA scraper.

Drives the public DA register hosted on Infor ePathway. City of Gold
Coast uses two enquiry lists:
  102 — DA after July 2017
  115 — DA before July 2017

Date-range search is the primary entry point. The form has tabs
(Application number / Address / Date range / Lot on plan / Decision
date); we click the Date range tab and submit.

Documents live on a separate origin (integrations.goldcoast.qld.gov.au
for COGC) keyed by ePathway internal id. The detail page URL leaks the
internal id via `?Id=NNNNNNNN` which we extract.
"""
from __future__ import annotations

import hashlib
import logging
import mimetypes
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from patchright.sync_api import Page, sync_playwright

from listo.councils.base import (
    CouncilScraper,
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
    parse_size_to_bytes,
    safe_filename,
    split_council_address,
)


logger = logging.getLogger(__name__)

# Documents land under data/da_docs/<application_id>/. Each file is
# saved with a deterministic name based on the portal oid so re-runs
# overwrite cleanly without producing duplicates on disk.
DEFAULT_DOC_DIR = Path("data/da_docs")


@dataclass
class InforEpathwayConfig:
    """Per-council ePathway config. One council can have multiple
    enquiry lists (e.g. COGC's pre/post July 2017 split)."""
    council_slug: str
    lists_url: str                             # entry-point URL for the LAP module
    enquiry_lists: list[str]                   # labels of enquiry lists to walk (in order)
    docs_portal_id_url: str                    # template like 'https://.../default.aspx?id={id}'
    docs_portal_oid_url: str                   # template like 'https://.../default.aspx?oid={oid}'
    # Optional label → enquiry-list-id mapping. The HTTP backend uses
    # this to skip the entry-page radio dance and go directly to
    # EnquirySearch.aspx?EnquiryListId=N. The Playwright backend
    # ignores it (it clicks the radio by label text instead).
    enquiry_list_ids: dict[str, str] = field(default_factory=dict)


# ---- COGC ----

COGC_CONFIG = InforEpathwayConfig(
    council_slug="cogc",
    lists_url=(
        "https://cogc.cloud.infor.com/ePathway/ePthProd/web/GeneralEnquiry/"
        "EnquiryLists.aspx?ModuleCode=LAP"
    ),
    enquiry_lists=[
        "Development applications after July 2017",
        "Development applications before July 2017",
    ],
    enquiry_list_ids={
        "Development applications after July 2017": "102",
        "Development applications before July 2017": "115",
    },
    docs_portal_id_url="https://integrations.goldcoast.qld.gov.au/pdonline/default.aspx?id={id}",
    docs_portal_oid_url="https://integrations.goldcoast.qld.gov.au/pdonline/default.aspx?oid={oid}",
)


# ---- main scraper ----


class InforEpathwayScraper:
    vendor: str = "infor_epathway"

    def __init__(
        self,
        config: InforEpathwayConfig,
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
        # Two persistent tabs: main is the search-results / detail walk,
        # docs is the second-origin documents portal. Keeping them on
        # separate tabs lets us interleave per-row work without losing
        # the search-results state.
        self._page: Page | None = None
        self._docs_page: Page | None = None
        self._enquiry_list_active: str | None = None
        # Session re-establishment state. iter_listings stashes the
        # active enquiry label + date window here so a mid-walk recovery
        # can re-submit the same search without us having to thread
        # them through every helper.
        self._cached_enquiry_label: str | None = None
        self._date_from: date | None = None
        self._date_to: date | None = None
        self._skip_ids: set[str] = set()
        # Page number we're currently walking. Used by the popup-blocker
        # fallback in _capture_detail_inline to know how far to advance
        # after a forced re-submit.
        self._current_page_index: int = 1

    def __enter__(self):
        self._pw = sync_playwright().start()
        self._ctx = self._pw.chromium.launch_persistent_context(
            user_data_dir="",
            headless=self._headless,
            no_viewport=True,
            locale="en-AU",
            timezone_id="Australia/Brisbane",
            accept_downloads=True,
        )
        self._page = self._ctx.new_page()
        self._docs_page = self._ctx.new_page()
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

    def _record_page(
        self,
        sink: RequestSink,
        *,
        purpose: str,
        started: datetime,
        page: Page | None = None,
    ) -> int | None:
        """Snapshot a page → FetchRecord → sink. Returns raw_page_id.
        Defaults to the main page; pass docs_page to log a docs-portal
        snapshot."""
        if page is None:
            page = self._page
        assert page is not None
        body = page.content()
        body_b = body.encode("utf-8")
        elapsed = int((datetime.utcnow() - started).total_seconds() * 1000)
        return sink.record(FetchRecord(
            purpose=purpose,
            method="GET",
            url=page.url,
            started_at=started,
            elapsed_ms=elapsed,
            http_status=200,
            bytes_received=len(body_b),
            body=body_b,
            body_is_html=True,
            content_type="text/html",
        ))

    # ---------------- date-range search ----------------

    def iter_listings(
        self,
        *,
        date_from: date,
        date_to: date,
        sink: RequestSink,
        skip_application_ids: set[str] | None = None,
    ) -> Iterator[DaListingRow]:
        """Walk every enquiry list configured for this council, submitting
        a Date range search (lodged_date BETWEEN date_from AND date_to)
        and yielding parsed rows from each results page.

        skip_application_ids: applications already fully processed (docs
        downloaded). For each match the scraper yields a lightweight
        DaListingRow without clicking into the detail page — the
        orchestrator's upsert is a near-noop, but keeping the yield
        means list_first_seen_at gets a heartbeat update.
        """
        # Stash on instance so session re-establishment can re-submit
        # with the same dates without us having to thread them through.
        self._date_from = date_from
        self._date_to = date_to
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
        page = self._page
        assert page is not None

        logger.info(
            "[%s] starting list walk: enquiry=%r window=%s..%s (skipping %d already-complete)",
            self.council_slug, enquiry_label, date_from, date_to,
            len(self._skip_ids),
        )

        # Navigate to the date-range form for this enquiry list, then
        # submit. Both ops are encapsulated so session-expiry recovery
        # can call them again without re-running the whole iter.
        self._cached_enquiry_label = enquiry_label
        self._navigate_to_date_form(enquiry_label)
        self._submit_search(date_from, date_to)
        started = datetime.utcnow()

        page_index = 1
        self._current_page_index = page_index
        total_rows = 0
        while True:
            self._record_page(sink, purpose="list", started=started)
            table = _parse_results_table(page)
            listings = _table_to_listings(table, council_slug=self.council_slug)
            page_total = len(listings)

            pager_text = _read_pager_text(page)
            logger.info(
                "[%s] results page %d: parsed %d rows%s",
                self.council_slug, page_index, page_total,
                f" ({pager_text})" if pager_text else "",
            )

            # ePathway result anchors are javascript:__doPostBack(...) — not
            # navigable URLs. We have to click each row in-place to reach
            # the detail page, capture it, then navigate back. This means
            # list phase also fills inline_detail, and the orchestrator's
            # detail phase mostly has nothing to do for fresh runs.
            for row_index, listing in enumerate(listings, start=1):
                if listing.application_id in self._skip_ids:
                    logger.info(
                        "[%s] page %d row %d/%d: %s — already complete, skipping",
                        self.council_slug, page_index, row_index, page_total,
                        listing.application_id,
                    )
                    total_rows += 1
                    yield listing
                    continue

                logger.info(
                    "[%s] page %d row %d/%d: %s — %s — %s",
                    self.council_slug, page_index, row_index, page_total,
                    listing.application_id,
                    (listing.application_type or "?")[:40],
                    (listing.suburb or listing.raw_address or "?")[:40],
                )
                try:
                    self._capture_detail_inline(listing, sink)
                    if listing.inline_detail is not None:
                        logger.info(
                            "[%s]   ↳ detail captured (status=%s, decision=%s, units=%s)",
                            self.council_slug,
                            listing.inline_detail.status or "—",
                            listing.inline_detail.decision_outcome or "—",
                            _maybe_units(listing.inline_detail.description),
                        )
                    else:
                        logger.info("[%s]   ↳ detail skipped (no link found)", self.council_slug)
                except Exception as e:
                    logger.warning(
                        "[%s]   ↳ detail capture FAILED for %s: %s",
                        self.council_slug, listing.application_id, e,
                    )
                total_rows += 1
                yield listing

                # No per-row session check needed: with the new-tab
                # detail flow the main tab never navigates away from
                # results between rows. Only the Next-page click below
                # can hit a stale session (long pause, machine sleep).

            if not _click_next_page(page):
                logger.info(
                    "[%s] no Next page — finished walk: %d pages, %d rows",
                    self.council_slug, page_index, total_rows,
                )
                return

            page_index += 1
            self._current_page_index = page_index
            logger.info("[%s] advancing to page %d", self.council_slug, page_index)
            self._sleep()
            started = datetime.utcnow()

            # Liveness check after the Next-page postback. ePathway can
            # take a couple of seconds for the results table to render,
            # so retry the check before triggering an expensive
            # re-establishment.
            if not self._main_tab_alive_retry(retries=4, delay=2.0):
                logger.warning(
                    "[%s] main-tab session expired on Next-page click — re-establishing to page %d",
                    self.council_slug, page_index,
                )
                try:
                    self._re_establish_to_page(page_index)
                except Exception as e:
                    logger.error(
                        "[%s] re-establish FAILED at page %d: %s — ending walk for this enquiry list",
                        self.council_slug, page_index, e,
                    )
                    return
                started = datetime.utcnow()

    def _capture_detail_inline(self, listing: DaListingRow, sink: RequestSink) -> None:
        """Capture detail + docs for one application *without ever
        clicking the link on the main tab*.

        How: read the postback target and the entire form payload from
        the main tab, open a fresh tab via context.new_page() (which
        is never popup-blocked because it's an explicit programmatic
        page creation, not a window.open), inject a copy of the form
        in that new tab with __EVENTTARGET swapped to the row's
        postback target, and submit. The new tab navigates to the
        detail page; the main tab is never touched, so its results
        state is preserved across rows.

        Each successful call leaves one fully-complete app: detail +
        every document + docs_fetched_at all upserted by the
        orchestrator's per-row loop.
        """
        main_page = self._page
        assert main_page is not None

        # Find the row's link in the live results table. ePathway
        # result-row anchors are real <a href="EnquiryDetailView.aspx?
        # Id=...">MCU/.../...</a> elements; we match by application_id
        # text since it's highly unique on the page.
        link = main_page.locator(f"a:has-text('{listing.application_id}')").first
        if link.count() == 0:
            logger.info(
                "[%s]   ↳ no row link visible for %s — skipping",
                self.council_slug, listing.application_id,
            )
            return

        self._sleep()
        started = datetime.utcnow()

        # Ctrl/Cmd-click the link. The browser opens the destination in
        # a new tab — same as a user middle-click. This is essential
        # because:
        #   1. Plain page.goto() to the detail URL is rejected by
        #      ePathway (server-side check on sec-fetch-user / cookie
        #      handshake → redirect to Error.aspx).
        #   2. A modifier-click is treated as a real user-initiated
        #      navigation, so Referer / sec-fetch-user / cookies are
        #      all correct.
        #   3. Modifier-click leaves the main tab where it was — no
        #      go_back, no form replication, no popup-blocker risk.
        detail_page: "Page | None" = None
        try:
            with self._ctx.expect_page(timeout=20000) as new_page_info:
                link.click(modifiers=["ControlOrMeta"], timeout=15000)
            detail_page = new_page_info.value

            try:
                detail_page.wait_for_load_state("domcontentloaded", timeout=30000)
                detail_page.wait_for_function(
                    "() => /Application description|Lodgement date/i.test(document.body.innerText)",
                    timeout=20000,
                )
            except Exception:
                pass

            if "Error.aspx" in detail_page.url or _epathway_id_from_url(detail_page.url) is None:
                logger.warning(
                    "[%s]   ↳ landed on error/non-detail page (%s) — leaving for next walk",
                    self.council_slug, detail_page.url[:120],
                )
                return

            listing.application_url = detail_page.url
            self._record_page(sink, purpose="detail", started=started, page=detail_page)
            listing.inline_detail = _parse_detail_page(
                detail_page, listing.application_id,
                council_slug=self.council_slug, vendor=self.vendor,
            )

            try:
                listing.inline_documents = self._capture_docs_inline(listing, sink)
            except Exception as e:
                logger.warning(
                    "[%s]   ↳ docs capture FAILED for %s: %s — leaving for phase docs",
                    self.council_slug, listing.application_id, e,
                )
                listing.inline_documents = None
        finally:
            if detail_page is not None:
                try:
                    detail_page.close()
                except Exception:
                    pass

    def _capture_docs_inline(
        self,
        listing: DaListingRow,
        sink: RequestSink,
    ) -> list:
        """Open the COGC documents portal for this application in the
        second tab, parse #gvDocs, and download every linked file.
        Returns the list of DownloadedDocument records (possibly empty)."""
        from listo.councils.base import DaDocumentRef, DownloadedDocument

        docs_page = self._docs_page
        assert docs_page is not None

        internal_id = _epathway_id_from_url(listing.application_url)
        if not internal_id:
            logger.info(
                "[%s]   ↳ no internal Id in detail URL — skipping docs",
                self.council_slug,
            )
            return []

        url = self.config.docs_portal_id_url.format(id=internal_id)
        started = datetime.utcnow()
        docs_page.goto(url, wait_until="domcontentloaded", timeout=30000)
        # The page is server-rendered; if #gvDocs is going to appear it's
        # there at domcontentloaded. Short wait covers DataTables setup
        # for the populated case without burning 15s on every empty app.
        try:
            docs_page.wait_for_selector("#gvDocs", timeout=3000)
        except Exception:
            pass
        self._record_page(sink, purpose="docs_index", started=started, page=docs_page)

        # Combined parse + diagnostic so a 0-docs result tells us *why*:
        # table missing vs table empty vs row shape mismatch.
        # NOTE: the "Date published" column (tds[4]) is hidden by
        # DataTables (visible:false), which makes innerText return ''
        # for it. Use textContent on tds[4] so we still get the
        # ISO timestamp.
        diag = docs_page.evaluate("""
          () => {
            const t = document.getElementById('gvDocs');
            const out = {
              tableExists: !!t,
              tbodyRows: 0,
              rowsParsed: [],
              bodySnippet: ((document.body && document.body.innerText) || '').slice(0, 160),
            };
            if (!t) return out;
            const trs = Array.from(t.querySelectorAll('tbody tr'));
            out.tbodyRows = trs.length;
            out.rowsParsed = trs.map(r => {
              const tds = Array.from(r.querySelectorAll('td'));
              if (tds.length < 4) return null;
              const a = tds[0].querySelector('a');
              return {
                oid: tds[0].innerText.trim(),
                href: a ? a.href : null,
                name: tds[1].innerText.trim(),
                size_text: tds[3].innerText.trim(),
                date_published: tds.length > 4 ? (tds[4].textContent || '').trim() : null,
              };
            }).filter(x => x);
            return out;
          }
        """)
        if not diag["tableExists"]:
            logger.info(
                "[%s]   ↳ docs portal: no #gvDocs table (page snippet: %r)",
                self.council_slug, diag["bodySnippet"][:100],
            )
            return []
        if diag["tbodyRows"] == 0:
            logger.info(
                "[%s]   ↳ docs portal: 0 documents (table empty — likely not yet uploaded)",
                self.council_slug,
            )
            return []
        rows = diag["rowsParsed"]
        logger.info(
            "[%s]   ↳ docs portal: %d documents listed",
            self.council_slug, len(rows),
        )
        if not rows:
            logger.info(
                "[%s]   ↳ docs portal: tbody had %d rows but parser found 0 — row shape unexpected",
                self.council_slug, diag["tbodyRows"],
            )
            return []

        # Build refs and sort chronologically by published date so the
        # download policy (first + last) maps to "earliest submission +
        # latest amendment/decision". Docs without a parseable date go
        # to the end so they don't accidentally become "first".
        refs = []
        for r in rows:
            pub = _parse_published_at(r.get("date_published"))
            ref = DaDocumentRef(
                doc_oid=r["oid"] or None,
                title=r["name"] or None,
                doc_type=_extract_doc_label(r["name"], listing.application_id),
                source_url=r["href"],
                size_text=r["size_text"] or None,
                published_at=pub,
            )
            refs.append(ref)
        refs.sort(key=lambda r: (r.published_at is None, r.published_at or datetime.min))

        # Decide which refs get bytes downloaded vs metadata-only. The
        # default policy is "first + last by published_at" — typically
        # the application Form/bundle and the Decision Notice. Anything
        # in between (plans, supporting reports, internal correspondence)
        # is recorded as metadata only and can be retro-fetched later.
        download_indices = _select_download_indices(len(refs))
        target_dir = DEFAULT_DOC_DIR / listing.application_id.replace("/", "_")
        target_dir.mkdir(parents=True, exist_ok=True)

        results: list[DownloadedDocument] = []
        for j, ref in enumerate(refs, start=1):
            do_download = (j - 1) in download_indices
            tag = "DOWNLOAD" if do_download else "metadata"
            logger.info(
                "[%s]     ↳ doc %d/%d [%s] %s — %s (%s, pub=%s)",
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
                        "[%s]     ↳ download FAILED for %s: %s — keeping metadata-only",
                        self.council_slug, ref.doc_oid, e,
                    )
                    # fall through to metadata-only persist
            # Metadata-only record: file fields stay None, file_size is
            # parsed best-effort from the size_text on the portal so the
            # row carries a useful size estimate without bytes on disk.
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

    def _return_to_results(self, page: Page) -> None:
        """Navigate back to the search-results table from a detail page.
        ePathway preserves the results state on go_back so we don't need
        to re-submit the search. If go_back fails (already on results,
        history empty), fall back to no-op."""
        try:
            page.go_back(timeout=15000)
            page.wait_for_function(
                """() => {
                    const t = Array.from(document.querySelectorAll('table'));
                    return t.some(tb => /application number/i.test(tb.innerText));
                }""",
                timeout=15000,
            )
        except Exception:
            pass

    def _navigate_to_date_form(self, enquiry_label: str) -> None:
        page = self._page
        assert page is not None
        if self._enquiry_list_active == enquiry_label and self._on_date_form():
            return
        page.goto(self.config.lists_url, wait_until="domcontentloaded", timeout=30000)
        page.click(f"text={enquiry_label}", timeout=15000)
        page.click("input[value=Next]", timeout=15000)
        page.wait_for_selector("input[name$='mSearchButton']", timeout=20000)

        # Try several plausible labels for the date-range tab. The exact
        # text varies per tenant — COGC may use "Date range search",
        # "Date range", "Lodgement date range", etc.
        tab_clicked = False
        for tab_text in (
            "Date range search",
            "Lodgement date search",
            "Lodgement date range",
            "Date range",
            "Lodgement date",
            "Date Lodged",
        ):
            try:
                page.click(f"text={tab_text}", timeout=2000)
                tab_clicked = True
                break
            except Exception:
                continue

        # Try several plausible from-date input selectors. ePathway nests
        # the date textbox inside a DatePicker subcontrol, so the actual
        # name suffix is '$mFromDatePicker$dateTextBox'. Older / variant
        # tenants use simpler names — kept those as fallbacks.
        from_selectors = [
            "input[name$='mFromDatePicker$dateTextBox']",
            "input[name$='mLodgedFromDatePicker$dateTextBox']",
            "input[name$='mLodgementFromDatePicker$dateTextBox']",
            "input[name$='mFromDateTextBox']",
            "input[name$='mDateFromTextBox']",
            "input[name$='mLodgedFromDateTextBox']",
            "input[name$='mLodgementDateFromTextBox']",
            "input[id$='FromDate']",
            "input[id$='DateFrom']",
            "input[id$='FromDatePicker_dateTextBox']",
        ]
        for sel in from_selectors:
            try:
                page.wait_for_selector(sel, timeout=3000)
                self._from_date_selector = sel
                self._enquiry_list_active = enquiry_label
                return
            except Exception:
                continue

        # Nothing matched — dump what's actually on the page so we can
        # update the selector list for next time.
        diag = self._diagnose_form()
        raise RuntimeError(
            "infor_epathway: could not locate the lodgement-date input. "
            f"tab_clicked={tab_clicked}. "
            f"Page diagnostic:\n{diag}"
        )

    def _on_date_form(self) -> bool:
        page = self._page
        if page is None:
            return False
        sel = getattr(self, "_from_date_selector", None) or "input[name$='mFromDateTextBox']"
        try:
            return page.locator(sel).is_visible(timeout=500)
        except Exception:
            return False

    # ---------------- session lifecycle ----------------

    def _submit_search(self, date_from: date, date_to: date) -> None:
        """Fill the date inputs and submit the search. Caller must have
        already navigated to the date-range form (i.e. _from_date_selector
        is set)."""
        page = self._page
        assert page is not None
        from_sel = self._from_date_selector
        to_sel = (
            from_sel.replace("FromDate", "ToDate")
                    .replace("DateFrom", "DateTo")
                    .replace("From", "To")
        )
        page.locator(from_sel).first.fill("")
        page.locator(from_sel).first.fill(date_from.strftime("%d/%m/%Y"))
        page.locator(to_sel).first.fill("")
        page.locator(to_sel).first.fill(date_to.strftime("%d/%m/%Y"))
        self._sleep()
        page.click("input[name$='mSearchButton']", timeout=15000)
        try:
            page.wait_for_function(
                """() => {
                    const t = Array.from(document.querySelectorAll('table'));
                    if (t.some(tb => /application number/i.test(tb.innerText))) return true;
                    const body = document.body.innerText.toLowerCase();
                    return body.includes('no records') || body.includes('no results') || body.includes('no matching');
                }""",
                timeout=30000,
            )
        except Exception:
            pass

    def _main_tab_alive(self) -> bool:
        """Cheap liveness check on the search-results page. Returns False
        if the page no longer holds a results table OR we see explicit
        session-expiry / sign-in markers in the body."""
        page = self._page
        if page is None:
            return False
        try:
            return page.evaluate("""
              () => {
                const txt = (document.body && document.body.innerText) || '';
                if (/expired|session\\s+timed\\s*out|please\\s+sign\\s*in|please\\s+log\\s*in|invalid\\s+token/i.test(txt)) return false;
                return Array.from(document.querySelectorAll('table'))
                  .some(t => /application number/i.test(t.innerText));
              }
            """)
        except Exception:
            return False

    def _main_tab_alive_retry(self, retries: int = 3, delay: float = 1.5) -> bool:
        """Retry the liveness check a few times. ePathway responses
        sometimes take a couple of seconds to render the results table
        after a postback — without retry we'd false-fire the
        re-establishment path on every slow page."""
        for i in range(retries):
            if self._main_tab_alive():
                return True
            if i < retries - 1:
                time.sleep(delay)
        return False

    def _re_establish_to_page(self, target_page_index: int) -> None:
        """Re-navigate from the entry page, re-submit the saved search,
        and click Next back to `target_page_index`. Each Next click
        retries a couple of times to absorb transient missed clicks."""
        if self._date_from is None or self._date_to is None:
            raise RuntimeError("cannot re-establish: search dates were never recorded")
        # Force re-navigation by clearing the cached active label.
        active_label = self._cached_enquiry_label
        self._enquiry_list_active = None
        self._navigate_to_date_form(active_label)
        self._submit_search(self._date_from, self._date_to)
        for n in range(target_page_index - 1):
            ok = False
            for attempt in range(3):
                if _click_next_page(self._page):
                    ok = True
                    break
                logger.warning(
                    "[%s] re-establish: Next click on page %d→%d attempt %d failed, retrying",
                    self.council_slug, n + 1, n + 2, attempt + 1,
                )
                time.sleep(2)
            if not ok:
                raise RuntimeError(
                    f"re-establish failed: could not advance past page {n + 1} "
                    f"(target {target_page_index})"
                )
        logger.info(
            "[%s] re-established session at page %d",
            self.council_slug, target_page_index,
        )

    def _diagnose_form(self) -> str:
        """Dump every <a> link text and every <input> name on the current
        page. Used when our selector list misses — tells us what the
        actual labels/names are so we can fix the lists."""
        page = self._page
        if page is None:
            return "(no page)"
        try:
            data = page.evaluate("""
              () => ({
                url: location.href,
                links: Array.from(document.querySelectorAll('a, button, input[type=submit], input[type=button]'))
                  .map(el => (el.innerText || el.value || el.title || '').trim())
                  .filter(t => t && t.length < 80),
                inputs: Array.from(document.querySelectorAll('input, select, textarea'))
                  .map(el => ({
                    tag: el.tagName.toLowerCase(),
                    type: el.type || '',
                    name: el.name || '',
                    id: el.id || '',
                    placeholder: el.placeholder || '',
                  }))
                  .filter(i => i.name || i.id),
                tabs: Array.from(document.querySelectorAll('[role=tab], .tab, .tabs a, ul.tabs li'))
                  .map(el => (el.innerText || '').trim())
                  .filter(t => t),
              })
            """)
            lines = [f"url: {data['url']}"]
            if data.get("tabs"):
                lines.append(f"tabs ({len(data['tabs'])}):")
                for t in data["tabs"]:
                    lines.append(f"  - {t}")
            lines.append(f"links/buttons ({len(data['links'])}):")
            for t in sorted(set(data["links"])):
                lines.append(f"  - {t}")
            lines.append(f"inputs ({len(data['inputs'])}):")
            for i in data["inputs"]:
                lines.append(f"  - <{i['tag']} type={i['type']} name={i['name']} id={i['id']} ph={i['placeholder']!r}>")
            return "\n".join(lines)
        except Exception as e:
            return f"(diagnose failed: {e})"

    # ---------------- detail page ----------------

    def fetch_detail(
        self,
        listing: DaListingRow,
        sink: RequestSink,
    ) -> DaDetailRecord:
        page = self._page
        assert page is not None
        # If we have a direct URL use it; otherwise click the link from
        # the current results table (caller must have it on screen).
        started = datetime.utcnow()
        if listing.application_url:
            page.goto(listing.application_url, wait_until="domcontentloaded", timeout=30000)
        else:
            link = page.locator(f"a:has-text('{listing.application_id}')").first
            if link.count() == 0:
                raise RuntimeError(f"detail link for {listing.application_id} not visible")
            self._sleep()
            link.click(timeout=15000)
        try:
            page.wait_for_function(
                "() => /Application description|Lodgement date/i.test(document.body.innerText)",
                timeout=20000,
            )
        except Exception:
            pass

        self._record_page(sink, purpose="detail", started=started)
        return _parse_detail_page(page, listing.application_id, council_slug=self.council_slug, vendor=self.vendor)

    # ---------------- documents ----------------
    #
    # Most COGC scrapes will populate documents inline during the list
    # walk via _capture_docs_inline, so list_documents() / download()
    # below are mainly used as a fallback by the orchestrator's docs
    # phase (rows where inline capture failed mid-run, or older data).
    # All three paths share the same parser + downloader.

    def list_documents(
        self,
        detail: DaDetailRecord,
        sink: RequestSink,
    ) -> list[DaDocumentRef]:
        docs_page = self._docs_page
        assert docs_page is not None
        internal_id = _epathway_id_from_url(detail.application_url)
        if not internal_id:
            return []
        url = self.config.docs_portal_id_url.format(id=internal_id)
        started = datetime.utcnow()
        docs_page.goto(url, wait_until="domcontentloaded", timeout=30000)
        try:
            docs_page.wait_for_selector("#gvDocs tbody tr", timeout=15000)
        except Exception:
            self._record_page(sink, purpose="docs_index", started=started, page=docs_page)
            return []
        self._record_page(sink, purpose="docs_index", started=started, page=docs_page)

        rows = docs_page.evaluate("""
          () => {
            const t = document.getElementById('gvDocs');
            if (!t) return [];
            return Array.from(t.querySelectorAll('tbody tr')).map(r => {
              const tds = Array.from(r.querySelectorAll('td'));
              if (tds.length < 4) return null;
              const a = tds[0].querySelector('a');
              return {
                oid: tds[0].innerText.trim(),
                href: a ? a.href : null,
                name: tds[1].innerText.trim(),
                size_text: tds[3].innerText.trim(),
                date_published: tds.length > 4 ? (tds[4].textContent || '').trim() : null,
              };
            }).filter(x => x);
          }
        """)
        app_id_for_label = detail.application_id if detail else None
        return [
            DaDocumentRef(
                doc_oid=r["oid"] or None,
                title=r["name"] or None,
                doc_type=_extract_doc_label(r["name"], app_id_for_label),
                source_url=r["href"] or self.config.docs_portal_oid_url.format(oid=r["oid"]),
                size_text=r["size_text"] or None,
                published_at=_parse_published_at(r.get("date_published")),
            )
            for r in rows
        ]

    def download(
        self,
        doc: DaDocumentRef,
        target_dir: Path,
        sink: RequestSink,
    ) -> DownloadedDocument:
        return self._download_one(doc, target_dir, sink)

    def _download_one(
        self,
        doc: DaDocumentRef,
        target_dir: Path,
        sink: RequestSink,
    ) -> DownloadedDocument:
        """Download one document via the docs tab. Used by both the
        inline list walk and the standalone docs phase."""
        page = self._docs_page
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
                    # goto often raises ERR_ABORTED when the response is
                    # served as Content-Disposition: attachment. The
                    # expect_download wrapper resolves anyway.
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
                published_at=doc.published_at,
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


def _click_next_page(page: Page) -> bool:
    """Click the 'Next' pagination link if present and enabled. Returns
    False when we're on the last page (or no pager is rendered).

    ePathway tenants render the pager in many different ways:
      - text link 'Next'
      - <img alt='Next'> wrapped in an anchor
      - <input type=image alt='Next'>
      - <input type=image src='.../stage-next.gif' alt='Go to next page'>
      - single-character '>' anchor
      - numbered page anchors (click '2' from page 1, etc.)
    We try each in order and on a miss dump the relevant markup so we
    can extend the list against the real DOM.
    """
    # Detect current page from any 'Page X of Y' text — used both to
    # pick the next-page-number anchor and to know if we even should
    # have a next link to begin with.
    pager_state = _detect_pager_state(page)
    candidates: list[tuple[str, "object"]] = [
        ("a text=Next", page.locator("a:has-text('Next')").first),
        ("a img[alt*=Next]", page.locator("a:has(img[alt*='Next' i])").first),
        ("a img[src*=next]", page.locator("a:has(img[src*='next' i])").first),
        ("input[type=image] alt*=Next", page.locator("input[type='image'][alt*='Next' i]").first),
        ("input[type=image] src*=stage-next", page.locator("input[type='image'][src*='stage-next' i]").first),
        ("input[type=image] src*=next", page.locator("input[type='image'][src*='next' i]").first),
        ("input[name*=Next]", page.locator("input[name*='Next' i]").first),
        ("a aria-label*=Next", page.locator("a[aria-label*='Next' i], button[aria-label*='Next' i]").first),
        ("a text=>", page.locator("a:has-text('>')").first),
    ]
    if pager_state and pager_state.get("current") and pager_state.get("total"):
        cur = pager_state["current"]
        tot = pager_state["total"]
        if cur < tot:
            # Try clicking the literal next page number — many ePathway
            # tenants render '1 2 3 ... Next' as anchors with the digit
            # as text. is_visible filters out hidden non-pager matches.
            candidates.append((f"a text={cur + 1}", page.locator(f"a:has-text('{cur + 1}')").first))

    # Snapshot the current pager-text BEFORE clicking so we can wait
    # for the body to actually transition, instead of leaning on
    # networkidle (which never triggers on ePathway because tracking
    # scripts keep the network warm — that's where the 30s/page hit
    # was coming from).
    prev_pager_text = ""
    try:
        prev_pager_text = page.evaluate(
            "() => { const m=(document.body.innerText||'').match(/Page\\s+(\\d+)\\s+of/i); return m?m[1]:''; }"
        ) or ""
    except Exception:
        pass

    for label, loc in candidates:
        try:
            if loc.count() == 0:
                continue
            if not loc.is_visible():
                continue
            logger.debug("pager: clicking via '%s'", label)
            loc.click(timeout=10000)
            # Wait until the 'Page N of Y' indicator changes; that's a
            # cheap, reliable signal that the table re-rendered with
            # the next page's rows.
            try:
                page.wait_for_function(
                    """(prev) => {
                        const m = (document.body.innerText || '').match(/Page\\s+(\\d+)\\s+of/i);
                        return !!m && m[1] !== prev;
                    }""",
                    arg=prev_pager_text,
                    timeout=15000,
                )
            except Exception:
                # Fallback: cheap DOM-stable wait
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=5000)
                except Exception:
                    pass
            return True
        except Exception as e:
            logger.debug("pager: '%s' failed: %s", label, e)
            continue

    # We didn't find a clickable next. If the pager text shows we're not
    # on the last page, the list of selectors is incomplete — dump the
    # bottom-of-page markup so we can extend the candidate list.
    if pager_state and pager_state.get("current") and pager_state.get("total"):
        cur = pager_state["current"]
        tot = pager_state["total"]
        if cur < tot:
            dump = _dump_pager_markup(page)
            logger.error(
                "pager: stuck on page %d/%d but no candidate matched. Markup:\n%s",
                cur, tot, dump,
            )
    return False


def _detect_pager_state(page: Page) -> dict | None:
    try:
        return page.evaluate("""
          () => {
            const t = (document.body && document.body.innerText) || '';
            const m = t.match(/Page\\s+(\\d+)\\s+of\\s+(\\d+)/i);
            if (!m) return null;
            return { current: parseInt(m[1], 10), total: parseInt(m[2], 10) };
          }
        """)
    except Exception:
        return None


def _dump_pager_markup(page: Page) -> str:
    """Capture every anchor / input / button near the 'Page X of Y'
    string so we can write a matching selector. Returns a multi-line
    string suitable for log output."""
    try:
        data = page.evaluate("""
          () => {
            // Find any element whose direct text mentions 'Page X of Y'
            const all = Array.from(document.querySelectorAll('*'));
            const pagerNode = all.find(el => {
              const t = (el.innerText || '');
              if (!/Page\\s+\\d+\\s+of\\s+\\d+/i.test(t)) return false;
              // Prefer the deepest matching node
              return Array.from(el.children).every(c => !/Page\\s+\\d+\\s+of\\s+\\d+/i.test((c.innerText || '')));
            });
            const root = pagerNode ? (pagerNode.closest('table, tr, div') || pagerNode.parentElement) : null;
            const scope = root || document.body;
            return Array.from(scope.querySelectorAll('a, button, input'))
              .map(el => ({
                tag: el.tagName.toLowerCase(),
                type: el.type || '',
                name: el.name || '',
                id: el.id || '',
                href: el.href || '',
                text: ((el.innerText || el.value || el.title || '')).trim().slice(0, 40),
                src: el.src || '',
                alt: el.alt || '',
              }))
              .filter(x => x.text || x.alt || x.name || x.src);
          }
        """)
        lines = []
        for el in data or []:
            lines.append(
                f"  <{el['tag']} type={el['type']!r} name={el['name']!r} "
                f"id={el['id']!r} text={el['text']!r} alt={el['alt']!r} "
                f"src={el['src'][-60:]!r}>"
            )
        return "\n".join(lines) or "(no elements found near pager)"
    except Exception as e:
        return f"(dump failed: {e})"


def _read_pager_text(page: Page) -> str | None:
    """Extract pagination summary from the page footer if rendered.

    ePathway typically prints text like 'Page 3 of 11' or 'Showing 51-75
    of 280'. Returning that as a string lets us log progress like
    'page 3 (Showing 51-75 of 280)'.
    """
    try:
        return page.evaluate("""
          () => {
            const txt = document.body.innerText || '';
            const m1 = txt.match(/Page\\s+\\d+\\s+of\\s+\\d+/i);
            if (m1) return m1[0];
            const m2 = txt.match(/Showing\\s+\\d[\\d,]*\\s*-\\s*\\d[\\d,]*\\s+of\\s+\\d[\\d,]*/i);
            if (m2) return m2[0];
            const m3 = txt.match(/\\d+\\s+(?:matching|matches|results?|records?)\\s+found/i);
            if (m3) return m3[0];
            return null;
          }
        """)
    except Exception:
        return None


def _maybe_units(description: str | None) -> str:
    from listo.councils.parsing import extract_approved_units
    n = extract_approved_units(description)
    return str(n) if n else "—"


def _parse_published_at(s: str | None) -> datetime | None:
    """Parse the docs portal's 'Date published' value (ISO-8601 with 'T'
    separator, no timezone) into a naive datetime. Returns None on
    failure or empty input."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.strip())
    except Exception:
        return None


def _select_download_indices(n_docs: int) -> set[int]:
    """Return the 0-based indices (within a chronologically-sorted doc
    list) that the scraper should actually download. Default policy:
    the first and the last — i.e. the application bundle and the most
    recent amendment/decision. For 1 doc, just that one. For 0, none.
    """
    if n_docs <= 0:
        return set()
    if n_docs == 1:
        return {0}
    return {0, n_docs - 1}


def _extract_doc_label(name: str | None, application_id: str | None) -> str | None:
    """Pull a short type-ish label from a COGC docs-portal Name cell.

    The 'Name' column is verbose — it embeds the application id, the
    document type, and the property location all together, e.g.
    'MCU/2022/636 TP report and codes Lot 2 SP319853, 223 Burleigh
    Connection Road, BURLEIGH WATERS QLD 4220 Material Change of Use'.

    Strip the leading application id and everything from the first
    'Lot ...' onwards so we end up with 'TP report and codes'. Cap at
    100 chars to fit the doc_type column whatever weird names show up.
    """
    if not name:
        return None
    s = name.strip()
    if application_id and s.startswith(application_id):
        s = s[len(application_id):].lstrip(" -:")
    m = re.search(r"\s+Lot\s+\d", s)
    if m:
        s = s[:m.start()].rstrip()
    s = s.strip()
    if not s:
        return None
    return s[:100]


def _parse_results_table(page: Page) -> list[dict]:
    return page.evaluate("""
      () => {
        const tables = Array.from(document.querySelectorAll('table'));
        function isHeaderRow(r) {
          const cells = Array.from(r.querySelectorAll('th'));
          if (cells.length < 2) return false;
          const first = cells[0].innerText.trim().toLowerCase().replace(/\\s+/g,' ');
          return first === 'application number' || first.startsWith('application number');
        }
        let best = null;
        for (const t of tables) {
          const headerRow = Array.from(t.querySelectorAll('tr')).find(isHeaderRow);
          if (!headerRow) continue;
          const headers = Array.from(headerRow.querySelectorAll('th')).map(c => c.innerText.trim());
          const rows = Array.from(t.querySelectorAll('tr'))
            .filter(r => r !== headerRow)
            .map(r => Array.from(r.querySelectorAll('td')).map(td => ({
              text: td.innerText.trim(),
              href: (td.querySelector('a') ? td.querySelector('a').href : null),
            })))
            .filter(cells => cells.length > 0);
          best = { headers, rows };
          break;
        }
        return best || { headers: [], rows: [] };
      }
    """)


def _table_to_listings(table: dict, *, council_slug: str) -> list[DaListingRow]:
    headers = [h.lower() for h in table.get("headers", [])]
    rows: list[DaListingRow] = []
    for row in table.get("rows", []):
        if len(row) < 2:
            continue
        cells = {h: c["text"] for h, c in zip(headers, row)}
        href = row[0].get("href") if row else None
        app_id = row[0]["text"].strip()
        if not app_id:
            continue
        raw_address = cells.get("application location") or cells.get("location")
        street, suburb, postcode, state = split_council_address(raw_address)
        rows.append(DaListingRow(
            council_slug=council_slug,
            vendor="infor_epathway",
            application_id=app_id,
            application_url=href,
            type_code=extract_type_code(app_id),
            application_type=cells.get("application type"),
            lodged_date=parse_au_date(cells.get("lodgement date") or cells.get("lodged")),
            raw_address=raw_address,
            street_address=street,
            suburb=suburb or (cells.get("location suburb") or cells.get("suburb")),
            postcode=postcode,
            state=state or "QLD",
            raw_row=cells,
        ))
    return rows


_DETAIL_LABELS = (
    "Application number",
    "Application description",
    "Application type",
    "Lodgement date",
    "Application location",
    "Status",
    "Responsible officer",
)
_DETAIL_SECTIONS = ("\nDecision\n", "\nProperty\n", "\nWork flow", "\nResponsible officer\n", "\nDocuments\n")


def _parse_detail_page(page: Page, app_id_hint: str, *, council_slug: str, vendor: str) -> DaDetailRecord:
    body = page.locator("body").inner_text()
    fields: dict[str, str] = {}
    for lbl in _DETAIL_LABELS:
        idx = body.find(lbl)
        if idx < 0:
            continue
        start = idx + len(lbl)
        end = len(body)
        for other in _DETAIL_LABELS:
            if other == lbl:
                continue
            j = body.find(other, start)
            if 0 <= j < end:
                end = j
        for sec in _DETAIL_SECTIONS:
            j = body.find(sec, start)
            if 0 <= j < end:
                end = j
        value = body[start:end].strip()
        if value.startswith(":"):
            value = value[1:].strip()
        fields[lbl] = value

    grids = page.evaluate("""
      () => Array.from(document.querySelectorAll('table#gridResults'))
        .map(t => Array.from(t.rows).map(r => Array.from(r.cells).map(c => c.innerText.trim())))
    """)
    decision_outcome = decision_date_str = decision_authority = lot_on_plan = None
    for g in grids:
        if len(g) >= 2 and g[0] and g[0][0] == "Decision type":
            decision_outcome = g[1][0] if len(g[1]) > 0 else None
            decision_date_str = g[1][1] if len(g[1]) > 1 else None
            decision_authority = g[1][2] if len(g[1]) > 2 else None
        elif len(g) >= 2 and g[0] and g[0][0] == "Lot on Plan":
            lot_on_plan = g[1][0] if len(g[1]) > 0 else None

    raw_address = fields.get("Application location")
    street, suburb, postcode, state = split_council_address(raw_address)

    return DaDetailRecord(
        council_slug=council_slug,
        vendor=vendor,
        application_id=app_id_hint,
        application_url=page.url,
        application_type=fields.get("Application type"),
        type_code=extract_type_code(app_id_hint),
        description=fields.get("Application description"),
        status=fields.get("Status"),
        decision_outcome=decision_outcome,
        decision_authority=decision_authority,
        lodged_date=parse_au_date(fields.get("Lodgement date")),
        decision_date=parse_au_date(decision_date_str),
        internal_property_id=extract_internal_property_id(fields.get("Application description")),
        lot_on_plan=lot_on_plan,
        raw_address=raw_address,
        street_address=street,
        suburb=suburb,
        postcode=postcode,
        state=state or "QLD",
        raw_fields=fields,
    )


def _epathway_id_from_url(url: str | None) -> str | None:
    if not url:
        return None
    m = re.search(r"[?&]Id=(\d+)", url, re.I)
    return m.group(1) if m else None
