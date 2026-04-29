"""City of Gold Coast PD Online (Infor ePathway) DA scraper.

Drives the public DA register at
https://cogc.cloud.infor.com/ePathway/ePthProd/web/GeneralEnquiry/EnquiryLists.aspx?ModuleCode=LAP

Address-based search → results table → per-application detail page.

We use playwright (mutter-headless) because the site is ASP.NET WebForms with
__VIEWSTATE postbacks for tab switches. Manual httpx + ViewState juggling is
fragile; a real browser is more robust and the volume here is low (maybe a few
hundred candidate addresses to enrich).

Two enquiry list IDs are useful for our duplex use case:
  102 — DA after July 2017
  115 — DA before July 2017
"""

from __future__ import annotations

import hashlib
import logging
import mimetypes
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from patchright.sync_api import Page, sync_playwright

logger = logging.getLogger(__name__)

LISTS_URL = (
    "https://cogc.cloud.infor.com/ePathway/ePthProd/web/GeneralEnquiry/"
    "EnquiryLists.aspx?ModuleCode=LAP"
)
# Documents portal — separate origin. Identifies an application by ePathway id.
DOCS_PORTAL_URL = "https://integrations.goldcoast.qld.gov.au/pdonline/default.aspx?id={id}"
DOCS_OID_URL = "https://integrations.goldcoast.qld.gov.au/pdonline/default.aspx?oid={oid}"

# Where document files land on disk.
DEFAULT_DOC_DIR = Path("data/da_docs")

ENQUIRY_LIST_AFTER_JULY_2017 = "Development applications after July 2017"
ENQUIRY_LIST_BEFORE_JULY_2017 = "Development applications before July 2017"


@dataclass
class DaSearchResult:
    application_id: str          # 'MCU/2017/973'
    type_code: str | None        # 'MCU'
    application_type: str | None # 'Material Change of Use (Single Uses)'
    lodged_date: date | None
    raw_address: str | None
    suburb: str | None
    detail_href: str | None      # link to the detail page (relative or postback target)


@dataclass
class DaDocument:
    oid: str                     # 'A43780294' — stable per-doc id
    name: str | None             # e.g. 'DA SPECIALIST REPORTS 55 BREAKER STREET MAIN BEACH'
    type_label: str | None = None
    size_text: str | None = None  # '1.89Mb' / '255.97Kb' as shown by the portal
    source_url: str | None = None # the per-document URL


@dataclass
class DownloadedDocument:
    oid: str
    name: str | None
    file_path: str
    file_size: int
    mime_type: str | None
    content_hash: bytes      # sha256
    page_count: int | None


@dataclass
class DaDetailRecord:
    application_id: str
    application_type: str | None = None
    type_code: str | None = None
    description: str | None = None
    internal_property_id: str | None = None
    lot_on_plan: str | None = None
    raw_address: str | None = None
    suburb: str | None = None
    postcode: str | None = None
    state: str | None = "QLD"
    status: str | None = None
    decision_outcome: str | None = None
    decision_authority: str | None = None
    lodged_date: date | None = None
    decision_date: date | None = None
    applicant_name: str | None = None
    source_url: str | None = None
    raw_field_dump: dict = field(default_factory=dict)


_DATE_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_SIZE_RE = re.compile(r"^([\d.]+)\s*([KMG]?b)$", re.I)


def _parse_size_to_bytes(s: str | None) -> int | None:
    """'1.89Mb' -> 1981480; '255.97Kb' -> 262113. Returns None on parse failure."""
    if not s:
        return None
    m = _SIZE_RE.match(s.strip())
    if not m:
        return None
    n = float(m.group(1))
    unit = m.group(2).lower()
    mult = {"b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3}.get(unit, 1)
    return int(n * mult)


def _safe_filename(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")
    return s[:120] or "file"


def _parse_au_date(s: str | None) -> date | None:
    if not s:
        return None
    m = _DATE_RE.match(s.strip())
    if not m:
        return None
    d, mth, y = (int(g) for g in m.groups())
    try:
        return date(y, mth, d)
    except ValueError:
        return None


def _ensure_wayland() -> None:
    """Force WAYLAND_DISPLAY=wayland-99 so chromium connects to the headless
    mutter compositor instead of the user's real Wayland session.

    Without overriding, a normal shell already has WAYLAND_DISPLAY=wayland-0
    (the live GNOME session) and chromium happily opens windows there.
    The mutter compositor must already be running on wayland-99:
        mutter --headless --virtual-monitor=1920x1080 --wayland-display=wayland-99 &
    """
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    socket_path = f"{runtime_dir}/wayland-99"
    if not os.path.exists(socket_path):
        raise RuntimeError(
            f"mutter compositor socket not found at {socket_path}. Start it with:\n"
            "  mutter --headless --virtual-monitor=1920x1080 --wayland-display=wayland-99 &"
        )
    os.environ["WAYLAND_DISPLAY"] = "wayland-99"
    os.environ.pop("DISPLAY", None)


class GoldCoastPdOnline:
    """Stateful playwright session for one or more searches."""

    def __init__(self, *, headless: bool = False, jitter_min: float = 1.5, jitter_max: float = 4.0):
        _ensure_wayland()
        self._jitter_min = jitter_min
        self._jitter_max = jitter_max
        self._pw = None
        self._ctx = None
        self._page: Page | None = None
        self._headless = headless
        self._enquiry_list_active: str | None = None  # which enquiry list we're configured for

    def __enter__(self) -> "GoldCoastPdOnline":
        self._pw = sync_playwright().start()
        self._ctx = self._pw.chromium.launch_persistent_context(
            user_data_dir="",
            headless=self._headless,
            no_viewport=True,
            locale="en-AU",
            timezone_id="Australia/Brisbane",
            args=["--ozone-platform=wayland", "--enable-features=UseOzonePlatform"],
        )
        self._page = self._ctx.new_page()
        return self

    def __exit__(self, *_a) -> None:
        try:
            if self._ctx:
                self._ctx.close()
        finally:
            if self._pw:
                self._pw.stop()

    def _sleep(self) -> None:
        time.sleep(random.uniform(self._jitter_min, self._jitter_max))

    def _on_address_form(self) -> bool:
        """Cheap check: are we currently sitting on the search form's Address tab?"""
        page = self._page
        if page is None:
            return False
        try:
            return page.locator("input[name$='mStreetNumberTextBox']").is_visible(timeout=500)
        except Exception:
            return False

    def _navigate_to_search(self, enquiry_list_label: str) -> None:
        """Visit entry page → choose the enquiry list radio → Next → search form.

        Skips the navigation if we're already sitting on the right register's
        Address tab (cheap check via `mStreetNumberTextBox` visibility).
        """
        page = self._page
        assert page is not None
        if self._enquiry_list_active == enquiry_list_label and self._on_address_form():
            return
        page.goto(LISTS_URL, wait_until="domcontentloaded", timeout=30000)
        page.click(f"text={enquiry_list_label}", timeout=15000)
        page.click("input[value=Next]", timeout=15000)
        page.wait_for_selector("input[name$='mSearchButton']", timeout=20000)
        page.click("text=Address search", timeout=10000)
        page.wait_for_selector("input[name$='mStreetNumberTextBox']", timeout=20000)
        self._enquiry_list_active = enquiry_list_label

    def search_by_address(
        self,
        street_number: str,
        street_name: str,
        suburb: str,
        *,
        enquiry_list: str = ENQUIRY_LIST_AFTER_JULY_2017,
    ) -> list[DaSearchResult]:
        """Fill the address tab and submit a search; return parsed result rows."""
        page = self._page
        assert page is not None
        self._navigate_to_search(enquiry_list)

        # Field selectors (suffix `mTabControl$ctl09$...` for the Address tab).
        prefix = (
            "ctl00$MainBodyContent$mGeneralEnquirySearchControl$mTabControl$ctl09"
        )
        # Convert dollar-name to ID form for selectors:
        def field(s):
            return f"input[name='{prefix}${s}']"
        def select_field(s):
            return f"select[name='{prefix}${s}']"

        # Reset address fields then fill
        page.fill(field("mStreetNumberTextBox"), "")
        page.fill(field("mStreetNumberTextBox"), street_number)
        page.fill(field("mStreetNameTextBox"), "")
        page.fill(field("mStreetNameTextBox"), street_name)
        # Leave street type as '(any)'
        page.select_option(select_field("mStreetTypeDropDown"), value="(any)")
        page.fill(field("mSuburbTextBox"), "")
        page.fill(field("mSuburbTextBox"), suburb)

        self._sleep()
        page.click("input[name$='mSearchButton']", timeout=15000)
        # Wait for either the results table or a 'no results' message.
        # ePathway shows results inside #ctl00_MainBodyContent_mResultListPanel
        # and a 'no records' label otherwise. We just wait for either to land.
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
            pass  # fall through; parser will return [] if nothing's there

        return _parse_results_table(page)

    # ---------------- documents portal ----------------

    def _docs_portal_id(self, source_url: str) -> str | None:
        """Extract the ePathway internal id from a detail page URL.
        e.g. 'https://...EnquiryDetailView.aspx?Id=50884298&EnquiryListId=115' -> '50884298'.
        """
        m = re.search(r"[?&]Id=(\d+)", source_url, re.I)
        return m.group(1) if m else None

    def list_documents(self, internal_id: str) -> list[DaDocument]:
        """Open the documents portal for a given ePathway id and return all
        documents (across pagination — sets the page size to 100 first)."""
        page = self._page
        assert page is not None
        page.goto(DOCS_PORTAL_URL.format(id=internal_id), wait_until="domcontentloaded", timeout=30000)
        # Page size selector. Set to 100 to capture all docs in one go.
        try:
            page.wait_for_selector("select[name*='length'], select[aria-label*='entries']", timeout=10000)
            page.select_option("select[name*='length'], select[aria-label*='entries']", value="100")
            page.wait_for_function(
                "() => document.body.innerText.includes('documents')",
                timeout=15000,
            )
        except Exception:
            pass

        rows = page.evaluate("""
          () => {
            const tables = Array.from(document.querySelectorAll('table'));
            // Find the documents table — has a header containing 'Link' and 'Size'
            const t = tables.find(tb => {
              const txt = tb.innerText || '';
              return /\\bLink\\b/.test(txt) && /\\bSize\\b/.test(txt);
            });
            if (!t) return [];
            return Array.from(t.querySelectorAll('tbody tr')).map(r => {
              const tds = Array.from(r.querySelectorAll('td'));
              if (tds.length < 4) return null;
              const a = tds[0].querySelector('a');
              return {
                oid: tds[0].innerText.trim(),
                href: a ? a.href : null,
                name: tds[1].innerText.trim(),
                type_label: tds[2].innerText.trim(),
                size_text: tds[3].innerText.trim(),
              };
            }).filter(x => x);
          }
        """)
        docs = []
        for r in rows:
            docs.append(DaDocument(
                oid=r["oid"],
                name=r["name"] or None,
                type_label=r["type_label"] or None,
                size_text=r["size_text"] or None,
                source_url=r["href"] or DOCS_OID_URL.format(oid=r["oid"]),
            ))
        return docs

    def download_documents(
        self,
        docs: list[DaDocument],
        target_dir: Path | str,
    ) -> list[DownloadedDocument]:
        """Download every document into target_dir. Filenames are <oid><.ext>.
        Returns one DownloadedDocument per successful download."""
        page = self._page
        assert page is not None
        target = Path(target_dir)
        target.mkdir(parents=True, exist_ok=True)

        out: list[DownloadedDocument] = []
        for d in docs:
            self._sleep()
            try:
                with page.expect_download(timeout=60000) as dl_info:
                    # Just navigate to the per-document URL — server responds
                    # with a Content-Disposition: attachment.
                    try:
                        page.goto(d.source_url, timeout=60000)
                    except Exception:
                        # goto often raises 'net::ERR_ABORTED' when the response
                        # is a download. Ignore — expect_download will resolve.
                        pass
                download = dl_info.value
                suggested = download.suggested_filename or d.oid
                ext = Path(suggested).suffix or ".bin"
                fname = _safe_filename(d.oid) + ext
                path = target / fname
                download.save_as(str(path))
                size = path.stat().st_size
                with open(path, "rb") as f:
                    content_hash = hashlib.sha256(f.read()).digest()
                mime, _ = mimetypes.guess_type(path.name)
                page_count = _count_pdf_pages(path) if (mime == "application/pdf" or ext.lower() == ".pdf") else None
                out.append(DownloadedDocument(
                    oid=d.oid, name=d.name, file_path=str(path),
                    file_size=size, mime_type=mime, content_hash=content_hash,
                    page_count=page_count,
                ))
                logger.info("downloaded %s (%s, %d bytes, %s pages)", d.oid, mime, size, page_count)
            except Exception as e:
                logger.warning("download failed for %s: %s", d.oid, e)
        return out

    def get_detail(self, result: DaSearchResult) -> DaDetailRecord:
        """Click into a search result and parse the detail page."""
        page = self._page
        assert page is not None
        # Result row anchors are __doPostBack-style. Use the application_id text
        # to click (rendered as a link in the table).
        link = page.locator(f"a:has-text('{result.application_id}')").first
        if link.count() == 0:
            raise RuntimeError(f"detail link for {result.application_id} not visible — search results may have scrolled")
        self._sleep()
        link.click(timeout=15000)
        # Wait for detail-page hallmarks: the page renders pairs in a table
        # under headings like 'Application description' / 'Lodgement date'.
        try:
            page.wait_for_function(
                "() => /Application description|Lodgement date/i.test(document.body.innerText)",
                timeout=20000,
            )
        except Exception:
            pass

        rec = _parse_detail_page(page, result.application_id)
        rec.source_url = page.url
        # Back to results so the caller can iterate other links
        try:
            page.go_back(timeout=10000)
            page.wait_for_selector("input[name$='mSearchButton']", timeout=10000)
        except Exception:
            pass
        return rec


# ---------------- HTML parsing helpers ----------------

def _parse_results_table(page: Page) -> list[DaSearchResult]:
    """Read the results <table>. ePathway renders a sortable table with one row
    per application. The actual results table has a <th> row that contains
    EXACTLY the text 'Application number' (the search-form tab also uses this
    string, so we have to match precisely)."""
    rows_data = page.evaluate("""
      () => {
        // Find a table whose <th>-header row's first cell text is exactly
        // 'Application number' (or starts with — there may be sort markers).
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
          const headers = Array.from(headerRow.querySelectorAll('th'))
            .map(c => c.innerText.trim());
          // Data rows are siblings of the header row — pick all <tr> in this
          // table EXCEPT the header itself.
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
        return best;
      }
    """)
    if not rows_data or not rows_data["rows"]:
        return []
    headers = [h.lower() for h in rows_data["headers"]]
    out: list[DaSearchResult] = []
    for row in rows_data["rows"]:
        if len(row) < 2:
            continue
        # First non-empty cell with a link is the application number
        app_id = row[0]["text"]
        href = row[0]["href"]
        if not app_id:
            continue
        # Map other columns by header name
        cells = {h: c["text"] for h, c in zip(headers, row)}
        lodged = _parse_au_date(cells.get("lodgement date") or cells.get("lodged"))
        type_code = None
        m = re.match(r"^([A-Z]{2,4})/", app_id)
        if m:
            type_code = m.group(1)
        out.append(DaSearchResult(
            application_id=app_id,
            type_code=type_code,
            application_type=cells.get("application type"),
            lodged_date=lodged,
            raw_address=cells.get("application location") or cells.get("location"),
            suburb=cells.get("location suburb") or cells.get("suburb"),
            detail_href=href,
        ))
    return out


_DETAIL_LABELS = (
    # Order matters — earlier labels are bounded by later ones in the body text.
    "Application number",
    "Application description",
    "Application type",
    "Lodgement date",
    "Application location",
    "Status",
    "Responsible officer",
)
_DETAIL_SECTIONS = ("\nDecision\n", "\nProperty\n", "\nWork flow", "\nResponsible officer\n", "\nDocuments\n")


def _parse_detail_page(page: Page, app_id_hint: str) -> DaDetailRecord:
    """ePathway detail page renders top-of-page fields as label-glued-to-value
    strings (no colon, no separator), e.g. 'Application descriptionMATERIAL
    CHANGE OF USE CODE...'. Below those are gridResults tables for Decision,
    Property, and Work flow sections.
    """
    body = page.locator("body").inner_text()
    fields: dict[str, str] = {}
    for lbl in _DETAIL_LABELS:
        idx = body.find(lbl)
        if idx < 0:
            continue
        start = idx + len(lbl)
        # Bound the value by the earliest of: any other label, or a section header.
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
        # Strip the leading colon/whitespace if present
        if value.startswith(":"):
            value = value[1:].strip()
        fields[lbl] = value

    # Pull the gridResults tables for Decision / Property
    grids = page.evaluate("""
      () => Array.from(document.querySelectorAll('table#gridResults'))
        .map(t => Array.from(t.rows).map(r => Array.from(r.cells).map(c => c.innerText.trim())))
    """)
    decision_outcome = decision_date = decision_authority = lot_on_plan = None
    for g in grids:
        if len(g) >= 2 and g[0] and g[0][0] == "Decision type":
            decision_outcome = g[1][0] if len(g[1]) > 0 else None
            decision_date = g[1][1] if len(g[1]) > 1 else None
            decision_authority = g[1][2] if len(g[1]) > 2 else None
        elif len(g) >= 2 and g[0] and g[0][0] == "Lot on Plan":
            lot_on_plan = g[1][0] if len(g[1]) > 0 else None

    rec = DaDetailRecord(application_id=app_id_hint)
    rec.raw_field_dump = fields
    rec.application_type = fields.get("Application type")
    rec.description = fields.get("Application description")
    rec.lodged_date = _parse_au_date(fields.get("Lodgement date"))
    rec.raw_address = fields.get("Application location")
    rec.status = fields.get("Status")
    rec.decision_outcome = decision_outcome
    rec.decision_date = _parse_au_date(decision_date)
    rec.decision_authority = decision_authority
    rec.lot_on_plan = lot_on_plan
    m = re.match(r"^([A-Z]{2,4})/", app_id_hint)
    rec.type_code = m.group(1) if m else None
    if rec.description:
        pn = re.search(r"\bPN\s*(\d{4,7})\b", rec.description, re.I)
        if pn:
            rec.internal_property_id = f"PN{pn.group(1)}"
    return rec


# ---------------- pdf page counting ----------------

def _count_pdf_pages(path: Path) -> int | None:
    try:
        from pypdf import PdfReader
    except ImportError:
        return None
    try:
        reader = PdfReader(str(path))
        return len(reader.pages)
    except Exception as e:
        logger.debug("pypdf failed on %s: %s", path, e)
        return None


# ---------------- public iteration helper ----------------

def search_addresses(
    addresses: list[tuple[str, str, str]],
    *,
    enquiry_list: str = ENQUIRY_LIST_AFTER_JULY_2017,
    headless: bool = False,
) -> Iterator[tuple[tuple[str, str, str], list[DaSearchResult]]]:
    """For each (street_number, street_name, suburb), yield the search results.

    Use this when you want to handle persistence externally — e.g. iterating
    duplex candidates and upserting into dev_applications.
    """
    with GoldCoastPdOnline(headless=headless) as gc:
        for addr in addresses:
            try:
                results = gc.search_by_address(addr[0], addr[1], addr[2], enquiry_list=enquiry_list)
                yield addr, results
            except Exception as e:  # noqa: BLE001
                logger.warning("search failed for %s: %s", addr, e)
                yield addr, []
