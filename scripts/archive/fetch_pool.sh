#!/usr/bin/env bash
# Adaptive parallel pool of fetch_watchdog.sh instances.
#
# Starts with WORKERS workers (default 4). Monitors aggregate raw_pages
# growth. If growth drops below the ratchet threshold for RATCHET_MIN
# minutes, kill the highest-numbered worker (N-1). Repeat all the way down
# to 1. Never scales back up — the assumption is that high N tripped some
# rate limit and we want to back off.
#
# Each worker gets a balanced slice of the suburb list via WORKER_INDEX /
# WORKER_COUNT. Page-level dedup means there's no double-fetching even if
# slices overlap.
#
# Usage:
#   ./scripts/fetch_pool.sh                                # 4 workers, gold_coast, sold, realestate
#   WORKERS=2 SOURCES=domain ./scripts/fetch_pool.sh
#
# Stop: SIGTERM the pool. It cascades down to all workers.

set -uo pipefail

WORKERS="${WORKERS:-4}"
MIN_WORKERS="${MIN_WORKERS:-1}"
SUBURB_LIST="${SUBURB_LIST:-gold_coast}"
PAGE_TYPE="${PAGE_TYPE:-sold}"
SOURCES="${SOURCES:-realestate}"           # default to realestate-only — domain runs as its own pool
RATCHET_MIN="${RATCHET_MIN:-15}"           # if growth poor for this many minutes, drop a worker
HEALTHY_PAGES_PER_MIN="${HEALTHY_PAGES_PER_MIN:-5}"  # per-worker expected baseline rate
COOLDOWN_SEC="${COOLDOWN_SEC:-60}"

DB_USER="${DB_USER:-listo}"
DB_PASS="${DB_PASS:-password}"
DB_NAME="${DB_NAME:-listo}"

POOL_LOG="/tmp/listo-pool-${SOURCES}.log"

declare -a WORKER_PIDS=()       # bash array of watchdog PIDs

# Cleanup any stale fetch processes left over from a previous run. Without
# this we end up with multiple pools / orphaned workers hammering the same
# site (which is what triggered Kasada's IP score increase last time —
# 7 ghost workers pushed concurrent requests up).
#
# Strategy: find every pool/watchdog/listo-fetch/CDP-chromium process that
# isn't part of OUR process group, signal each PGID with TERM, then KILL.
# We use process-group kill so descendants go down with their parents.
# Tempdirs from prior CDP runs are also wiped so /tmp doesn't fill up.
cleanup_stale_processes() {
  local self_pgid
  self_pgid="$(ps -o pgid= -p $$ | tr -d ' ')"
  echo "[$(date -Is)] startup cleanup: scanning for stale fetch processes (own PGID=$self_pgid)"

  local stale_pgids
  # ps -eo pgid,args puts pgid in $1 and the rest of the line in $2..$NF.
  # Pattern-match against the whole line ($0), not $2 — `bash ./scripts/...`
  # has "bash" in $2 and "./scripts/fetch_pool.sh" in $3, so $2-anchored
  # patterns silently miss the actual scripts.
  stale_pgids="$(
    ps -eo pgid,args | \
      awk -v self="$self_pgid" '
        ($0 ~ /fetch_pool\.sh/ ||
         $0 ~ /fetch_watchdog\.sh/ ||
         $0 ~ /listo fetch all/ ||
         $0 ~ /listo_cdp_/) &&
        $1 != self {print $1}
      ' | sort -u
  )"

  if [[ -z "$stale_pgids" ]]; then
    echo "[$(date -Is)] startup cleanup: nothing stale found"
  else
    echo "[$(date -Is)] startup cleanup: terminating PGIDs: $(echo $stale_pgids | tr '\n' ' ')"
    for pgid in $stale_pgids; do kill -TERM -"$pgid" 2>/dev/null || true; done
    sleep 3
    for pgid in $stale_pgids; do kill -KILL -"$pgid" 2>/dev/null || true; done
    sleep 1
  fi

  # Sweep up any orphan CDP-mode chromium tempdirs (older than 1 minute so
  # we don't blow away the dir of a process we just nuked but kernel hasn't
  # reaped). Keeps /tmp from filling up over months of restarts.
  find /tmp -maxdepth 1 -name "listo_cdp_*" -mmin +1 -exec rm -rf {} + 2>/dev/null || true
}
cleanup_stale_processes

db_count() {
  local sources_sql
  sources_sql="$(echo "$SOURCES" | sed "s/,/','/g")"
  mysql -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -BNe \
    "SELECT COUNT(*) FROM raw_pages WHERE source IN ('$sources_sql');" \
    2>/dev/null | tail -1
}

start_worker() {
  local idx="$1"
  local total="$2"
  # setsid puts the watchdog in its own session/process-group, so its PID
  # is also its PGID. Everything it spawns (uv → python → patchright node →
  # chromium) inherits that PGID. stop_worker can then signal the entire
  # tree at once via `kill -SIG -<pgid>`, which `pkill -P` (direct children
  # only) and plain `kill <pid>` (the watchdog itself only) cannot do.
  # </dev/null on stdin so setsid doesn't try to grab a controlling tty.
  setsid env WORKER_INDEX="$idx" WORKER_COUNT="$total" \
    SOURCES="$SOURCES" SUBURB_LIST="$SUBURB_LIST" PAGE_TYPE="$PAGE_TYPE" \
    LISTO_REQUEST_MIN_DELAY="${LISTO_REQUEST_MIN_DELAY:-}" \
    LISTO_REQUEST_MAX_DELAY="${LISTO_REQUEST_MAX_DELAY:-}" \
    ./scripts/fetch_watchdog.sh </dev/null >/dev/null 2>&1 &
  local pid=$!
  WORKER_PIDS+=("$pid")
  echo "[$(date -Is)] started worker $idx/$total as PGID $pid (log: /tmp/listo-bucketed-${SOURCES}-w${idx}of${total}.log)"
}

stop_worker() {
  local pid="$1"
  echo "[$(date -Is)] stopping worker PGID $pid (entire tree: watchdog → uv → python → chromium)..."
  # Negative PID = signal the whole process group. Reaches every descendant
  # regardless of how deep the fork tree is.
  kill -TERM -"$pid" 2>/dev/null || true
  sleep 3
  kill -KILL -"$pid" 2>/dev/null || true
}

cleanup() {
  echo "[$(date -Is)] pool received signal, stopping all workers..."
  for pid in "${WORKER_PIDS[@]}"; do
    stop_worker "$pid" || true
  done
  exit 0
}
trap cleanup INT TERM

echo "[$(date -Is)] pool starting: workers=$WORKERS sources=$SOURCES list=$SUBURB_LIST page_type=$PAGE_TYPE ratchet=${RATCHET_MIN}min" | tee -a "$POOL_LOG"

current_n="$WORKERS"
for ((i=0; i<current_n; i++)); do
  start_worker "$i" "$current_n"
  sleep 5  # stagger so they don't all warmup at the same instant
done

prev_count="$(db_count)"
prev_count=${prev_count:-0}
slow_minutes=0

while [[ "$current_n" -ge "$MIN_WORKERS" ]]; do
  sleep 60

  cur_count="$(db_count)"
  cur_count=${cur_count:-0}
  delta=$((cur_count - prev_count))
  threshold=$((current_n * HEALTHY_PAGES_PER_MIN))

  echo "[$(date -Is)] heartbeat: workers=$current_n delta=${delta}pages/min threshold=${threshold} slow=${slow_minutes}/${RATCHET_MIN}min" | tee -a "$POOL_LOG"

  if [[ "$delta" -ge "$threshold" ]]; then
    slow_minutes=0
  else
    slow_minutes=$((slow_minutes + 1))
  fi
  prev_count="$cur_count"

  if [[ "$slow_minutes" -ge "$RATCHET_MIN" && "$current_n" -gt "$MIN_WORKERS" ]]; then
    new_n=$((current_n - 1))
    echo "[$(date -Is)] RATCHET: $current_n → $new_n (sustained slow growth)" | tee -a "$POOL_LOG"

    # Stop the highest-numbered worker
    last_pid="${WORKER_PIDS[-1]}"
    stop_worker "$last_pid"
    unset 'WORKER_PIDS[-1]'

    current_n="$new_n"

    # Re-launch remaining workers with new WORKER_COUNT so they re-partition
    # the suburb list correctly. (Without this, worker 0 of 4 would still
    # think it owns suburbs[0::4] when there are only 3 workers.)
    echo "[$(date -Is)] re-partitioning remaining $current_n workers..." | tee -a "$POOL_LOG"
    for pid in "${WORKER_PIDS[@]}"; do
      stop_worker "$pid"
    done
    WORKER_PIDS=()
    sleep "$COOLDOWN_SEC"
    for ((i=0; i<current_n; i++)); do
      start_worker "$i" "$current_n"
      sleep 5
    done

    slow_minutes=0
    prev_count="$(db_count)"
    prev_count=${prev_count:-0}
  fi

  # Check if any workers have died unexpectedly
  alive=0
  new_pids=()
  for pid in "${WORKER_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      alive=$((alive + 1))
      new_pids+=("$pid")
    else
      echo "[$(date -Is)] worker PID $pid died unexpectedly" | tee -a "$POOL_LOG"
    fi
  done
  WORKER_PIDS=("${new_pids[@]}")
  if [[ "$alive" -eq 0 ]]; then
    echo "[$(date -Is)] all workers dead — exiting pool" | tee -a "$POOL_LOG"
    exit 1
  fi
  if [[ "$alive" -lt "$current_n" ]]; then
    current_n="$alive"
    echo "[$(date -Is)] pool size dropped to $current_n alive" | tee -a "$POOL_LOG"
  fi
done

echo "[$(date -Is)] reached MIN_WORKERS=$MIN_WORKERS, holding steady" | tee -a "$POOL_LOG"
# Hang on a single worker
wait
