# listo

Gold Coast duplex/triplex redevelopment dataset. See `CLAUDE.md` for full project context.

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
