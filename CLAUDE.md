# Listo — Gold Coast duplex/triplex redevelopment dataset

## Goal

Build a dataset to spot duplex/triplex redevelopments. Pattern: a house sells at "17 Third Avenue, Palm Beach" in 2023, then a "2/17 Third Avenue, Palm Beach" listing appears in 2026 — same lot, redeveloped. Back out the original sale price vs. the new unit price + holding cost + build cost to estimate developer profit.

Initial scope: Gold Coast (QLD postcodes 4207–4228) + Newcastle (NSW). Expanding to other AU coastal regions as more council scrapers are added.

## Architecture (current)

**DA-first inverted funnel.** We scrape every development application from council DA registers in a date window, then drill into each one for full detail + every attached document. Sale-side data (realestate.com.au PDPs, etc.) gets layered on later, looked up only for properties that appear in DAs of interest. The earlier "scrape all sales then look for redev patterns" approach has been archived under `src/listo/archive/`.

## Stack

- **Python 3.13** managed by `uv`. Run anything with `uv run listo <cmd>`.
- **MySQL** (DB `listo`, user `listo`, pw `password`, localhost). Connection in `src/listo/config.py`.
- **Playwright via patchright** (anti-detection fork) — used for council portals. Plain headless chromium; no Wayland/mutter gymnastics needed (Kasada was a realestate.com.au-specific problem).
- **SQLAlchemy 2 + Alembic** for schema. Migrations under `alembic/versions/`.

## What's built

### Schema (alembic 0001 → 0006)

| Table | Purpose |
|---|---|
| `raw_pages` | Every fetched HTML page, gzipped, with url_hash + content_hash for dedup. `source` column carries values like `council_cogc`, `council_newcastle`. |
| `council_applications` | One row per DA across every council. Council-agnostic (`council_slug` + `vendor` columns). Holds parsed structured fields plus `raw_listing_row` and `raw_detail_fields` JSON blobs that preserve the source page exactly. Per-stage timestamps (`list_first_seen_at`, `detail_fetched_at`, `docs_fetched_at`) drive resume. |
| `council_application_documents` | Every document downloaded from a council portal (PDF mostly): file_path, content_hash, page_count, mime_type, file_size. FK to `council_applications`. |
| `council_requests` | Every HTTP request we made to a council portal — successes and failures. Joins back to `raw_pages` (when an HTML body was stored) or `council_application_documents` (when a binary was downloaded). The full attempt log. |
| `crawl_runs` | Generic crawl-attempt tracker. Source/page_type are now VARCHAR(40) so council values fit. |
| `mortgage_rates` | RBA F5 series back to 1959 (variable, fixed, OO/investor) for financial modeling. |
| `properties`/`listings`/`sales` | Tables retained from the previous architecture; will be repopulated when the realestate PDP fetcher is added. Currently dormant. |

Dropped in 0006: `dev_applications`, `da_documents`, `da_flags` (replaced by the council_* tables).

### Modules

- **`councils/base.py`** — `CouncilScraper` protocol + dataclasses (`DaListingRow`, `DaDetailRecord`, `DaDocumentRef`, `DownloadedDocument`, `FetchRecord`, `RequestSink`).
- **`councils/registry.py`** — `COUNCILS` dict mapping slug → list of `CouncilBackend`. Each backend has a date-coverage window so multi-system councils (Newcastle eTrack pre-2026 / T1Cloud post-2026) auto-route by date.
- **`councils/orchestrator.py`** — drives a scraper through three phases (list → detail → docs). `DbRequestSink` writes raw_pages + council_requests as a side effect of every fetch. Phase 1/2/3 are individually skippable for staged scraping.
- **`councils/parsing.py`** — `extract_approved_units`, `extract_internal_property_id`, `extract_type_code`, `parse_au_date`, `split_council_address`. Shared across vendors.
- **`councils/browser.py`** — small playwright launch helper (currently unused; each vendor scraper still owns its own session because they have slightly different timezones/locales).
- **`councils/infor_epathway.py`** — Infor ePathway vendor (City of Gold Coast). Date-range search on the LAP module, two enquiry lists (post-July-2017 + pre-July-2017). Documents portal lives at `integrations.goldcoast.qld.gov.au` keyed by ePathway internal id.
- **`councils/techone_etrack.py`** — TechnologyOne eTrack vendor (Newcastle pre-Feb-2026). ASP.NET WebForms, similar in spirit to ePathway. **Selectors are best-guesses; will need tuning against a live page.**
- **`councils/techone_t1cloud.py`** — TechnologyOne T1Cloud vendor (Newcastle post-Feb-2026). SaaS SPA; intercepts XHR JSON responses for authoritative data with a DOM-scraping fallback. **Selectors are best-guesses; will need tuning against a live page.**
- **`rba.py`** — RBA F5 CSV ingest into `mortgage_rates`.
- **`asic.py`** — ASIC Connect Online registry scraper (ACN ↔ company name). Drives the Oracle ADF search via `connect_over_cdp("http://localhost:9222")` against the user's running Chrome — fresh patchright instances trip invisible reCAPTCHA, the warmed Chrome session passes silently. Each detail fetch happens in its own tab to isolate ADF state and reCAPTCHA scoring. Persists into the existing `companies` table (matched by ACN) using the `asic_*` enrichment columns from migration 0018.

### CLI

```
listo status                                              # raw_pages + council_applications + docs + requests counts
listo council list                                        # registered councils + which dates each backend covers
listo council scrape <slug> --from YYYY-MM-DD --to YYYY-MM-DD
                                                          # full pipeline: list → detail → docs
                                                          # add --list-only / --detail-only / --docs-only for one phase
                                                          # add --detail-limit N / --docs-limit N to cap a run
                                                          # --types defaults to MCU,COM,ROL,EDA,EXA,PDA,FDA (residential redev);
                                                          #   listings still recorded for excluded types — only detail+docs skipped.
                                                          #   pass --types all to fetch every category.
listo council resume <slug>                               # picks up detail+docs over the existing date span in db
listo enrich rates                                        # RBA F5 mortgage rates
listo asic lookup <ACN>                                   # one ACN → ASIC View Details → upsert companies row
listo asic search "<name>"                                # name search → walk all pages → fetch detail for every Australian Proprietary Company hit
                                                          # add --types "Type1,Type2" / --types all to widen the filter
listo asic status                                         # how many companies rows have ASIC enrichment, top localities
```

### Resume model

The list / detail / docs phases each set a per-stage timestamp on `council_applications`:

- list phase: `list_first_seen_at`
- detail phase: `detail_fetched_at`
- docs phase: `docs_fetched_at`

A re-run of any phase only operates on rows where the corresponding timestamp is NULL (inside the requested date window). Killing and restarting is safe — no `--force` needed.

Every HTTP fetch lands in `council_requests`, so debugging "what did we actually try to fetch" is a SQL query.

## Known issues / next moves

- **Newcastle scrapers are scaffolds.** Selectors in `techone_etrack.py` and `techone_t1cloud.py` are best-guesses based on standard vendor markup; they will need to be tuned against a live page (run with `headless=False` and watch, or paste sample HTML so we can fix the selectors before the first real run).
- **realestate.com.au PDP fetcher** is the next major piece. We validated the `/property/{slug}` endpoint loads cleanly and carries the full `ArgonautExchange` payload (sale history, attributes, neighbouring data) without auth and without Kasada walls. The plan: walk `council_applications` rows, derive a slug, fetch the PDP, parse → `properties` + `sales`.
- **Mutter/Kasada bypass** is no longer needed for the council scrapers and has been removed from the active code path. The `memory/project_kasada_bypass.md` note still applies if/when we revive realestate scraping.

## Common queries

```bash
# Per-council scrape progress
mysql -u listo -ppassword listo -e "
  SELECT council_slug, vendor,
         COUNT(*) AS apps,
         SUM(detail_fetched_at IS NOT NULL) AS with_detail,
         SUM(docs_fetched_at IS NOT NULL)   AS with_docs,
         MIN(lodged_date) AS earliest, MAX(lodged_date) AS latest
    FROM council_applications GROUP BY council_slug, vendor;
"

# Recent failures from the request log
mysql -u listo -ppassword listo -e "
  SELECT started_at, council_slug, purpose, http_status, error, url
    FROM council_requests
   WHERE error IS NOT NULL OR http_status >= 400
   ORDER BY started_at DESC LIMIT 20;
"

# Duplex-keyword DAs by suburb (rough)
mysql -u listo -ppassword listo -e "
  SELECT suburb, COUNT(*) AS n
    FROM council_applications
   WHERE description REGEXP 'DUAL OCCUPANCY|DUPLEX|TRIPLEX|MULTI[ -]?DWELLING'
   GROUP BY suburb ORDER BY n DESC LIMIT 30;
"
```

## File locations

- Main code: `src/listo/`
- Council scrapers: `src/listo/councils/`
- Archived realestate flow: `src/listo/archive/`
- Archived shell scripts: `scripts/archive/`
- Migrations: `alembic/versions/`
- DA PDFs: `data/da_docs/<application_id>/`
- Memory: `~/.claude/projects/-home-a-Desktop-greenmount-listo/memory/`
