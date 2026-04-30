# listo

Gold Coast duplex/triplex redevelopment dataset. See `CLAUDE.md` for full project context.

## Edge cases the redev matcher needs to handle

The naïve approach — "look up the DA address on realestate.com.au, expect to
find one pre-DA sale and two post-DA unit sales" — breaks on a few real
patterns we've already seen in the data:

- **Address change after redev.** Corner lots and lot mergers cause the new
  duplex to be sold under a *different street and/or number* than the DA.
  Worked example: `MCU/2021/532` + `MCU/2021/533` at *23 Hythe Street, Miami*
  (Lot 1 RP3147) merged with 1 Redondo Avenue and the units were sold as
  `1A/1 Redondo Ave` / `1/1 Redondo Ave` — the Hythe Street address vanished.
  → **Mitigation:** match on lot/plan (e.g. `Lot 1 RP3147`) before falling
  back to address; flag corner lots and adjoining-lot patterns from the
  cadastre for manual review.

- **Long approval history on a single lot.** Some lots accumulate years of
  permits — e.g. *49 Marion Street, Tugun* (Lot 116 RP32011) carries a 2015
  MCU, multiple minor changes, OPV / OPW, an Extension of Approval, plus
  the 2021 duplex MCU we'd target. The "interesting" DA isn't always the
  most recent one. → **Mitigation:** treat the *earliest dual-occupancy*
  MCU on the lot as the redev signal; later MIN / EXA records are usually
  amendments, not new redevs.

- **Paired DAs on one lot.** A single duplex can be lodged as two
  simultaneous MCUs, one per dwelling half (Hythe Street again). Counting
  approvals will double-count unless we collapse on lot+lodgement-date.

### Picking a clean test case

The **simplest** Gold Coast duplex test case currently in the database is
`MCU/2021/710` — *26 Nundah Avenue, Miami QLD 4220* (Lot 52 RP88447):

- Single MCU on the lot — no prior 2015 approval, no minor changes.
- Mid-block address (#26) — no corner-lot rebadging risk.
- Lodged 2021-12-21, approved 2022-05-16 (Code Assessment, Dual Occupancy).
- 17 documents archived.
- 4-year window since approval — build + first resale should be done by 2026.

Expected post-redev pattern on realestate: one sale of `26 Nundah Ave` before
late 2021, then sales of `1/26 Nundah Ave` and `2/26 Nundah Ave` from 2023+.

## Running the realestate.com.au scraper

realestate.com.au is protected by Kasada. The bypass requires a headless mutter compositor with GPU access — see `memory/project_kasada_bypass.md` for why other approaches fail.

### 1. Launch mutter (once per boot)

```bash
mutter --headless --virtual-monitor=1920x1080 --wayland-display=wayland-99 &
disown
export WAYLAND_DISPLAY=wayland-99
```

`src/listo/fetch/playwright_http.py` checks for the `wayland-99` socket and refuses to launch without it.

### 2. Launch the fetch pool

From the repo root:

```bash
nohup ./scripts/fetch_pool.sh > /tmp/listo-pool-realestate.log 2>&1 &
disown
```

Defaults: `WORKERS=4`, `SOURCES=realestate`, `SUBURB_LIST=gold_coast`, `PAGE_TYPE=sold`.

Override via env vars:

```bash
WORKERS=2 PAGE_TYPE=buy ./scripts/fetch_pool.sh
SOURCES=domain WORKERS=2 ./scripts/fetch_pool.sh   # domain runs as its own pool
```

Logs:
- Pool: `/tmp/listo-pool-realestate.log`
- Per-worker: `/tmp/listo-bucketed-realestate-w{i}of{n}.log`

The pool starts with `WORKERS` watchdogs and ratchets down (never up) if aggregate page-growth stays below `WORKERS * HEALTHY_PAGES_PER_MIN` for `RATCHET_MIN` minutes — the assumption being that high concurrency tripped a rate limit. Stop with `kill -TERM <pool-pid>`; the trap cascades to all workers and chromium processes.
