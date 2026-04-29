#!/usr/bin/env bash
# Watchdog wrapper for `listo fetch all --bucketed`.
#
# Design:
#   - Loop forever, launching the listo command as a child process.
#   - Every 60s, check raw_pages count in MySQL.
#   - If count hasn't grown for STALL_MIN minutes, kill the child + chromium.
#   - Sleep COOLDOWN_SEC seconds, restart.
#   - Resume is automatic via page-level dedup (no --force).
#
# Usage:
#   ./scripts/fetch_watchdog.sh                                 # gold_coast, sold, bucketed
#   SUBURB_LIST=target ./scripts/fetch_watchdog.sh
#   PAGE_TYPE=buy ./scripts/fetch_watchdog.sh
#
# Stop: send SIGTERM to the watchdog itself; it'll kill the child + exit.

set -uo pipefail

# --- config ---
SUBURB_LIST="${SUBURB_LIST:-gold_coast}"
PAGE_TYPE="${PAGE_TYPE:-sold}"
SOURCES="${SOURCES:-realestate,domain}"
STALL_MIN="${STALL_MIN:-8}"           # restart if no new raw_pages for this many minutes
COOLDOWN_SEC="${COOLDOWN_SEC:-60}"    # rest between restarts
MIN_DELAY="${LISTO_REQUEST_MIN_DELAY:-1.0}"
MAX_DELAY="${LISTO_REQUEST_MAX_DELAY:-3.0}"
WORKER_INDEX="${WORKER_INDEX:-0}"
WORKER_COUNT="${WORKER_COUNT:-1}"

# Per-instance log + pidfile so multiple watchdogs (different sources or
# different worker indices in a parallel pool) don't trample each other.
INSTANCE_TAG="${INSTANCE_TAG:-$(echo "$SOURCES" | tr ',' '-')-w${WORKER_INDEX}of${WORKER_COUNT}}"
LOG="/tmp/listo-bucketed-${INSTANCE_TAG}.log"
PIDFILE="/tmp/listo-bucketed-${INSTANCE_TAG}.pid"
UV="${UV:-$HOME/.local/bin/uv}"

DB_USER="${DB_USER:-listo}"
DB_PASS="${DB_PASS:-password}"
DB_NAME="${DB_NAME:-listo}"

# --- helpers ---
db_count() {
  # Filter to this instance's source(s) so a stalled realestate watchdog
  # doesn't see domain's progress (and vice versa).
  local sources_sql
  sources_sql="$(echo "$SOURCES" | sed "s/,/','/g")"
  mysql -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -BNe \
    "SELECT COUNT(*) FROM raw_pages WHERE source IN ('$sources_sql');" \
    2>/dev/null | tail -1
}

kill_children() {
  local pid="${1:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "[$(date -Is)] killing PGID $pid (uv → python → patchright → chromium)"
    # Child was launched via setsid below, so $pid is also its PGID.
    # Negative-PID signals the whole group, killing every descendant.
    # Avoids `pkill -f chromium-1208` which would also nuke chromium owned
    # by sibling watchdogs (other workers in a pool).
    kill -TERM -"$pid" 2>/dev/null || true
    sleep 2
    kill -KILL -"$pid" 2>/dev/null || true
  fi
  rm -f "$PIDFILE"
}

cleanup() {
  echo "[$(date -Is)] watchdog received signal, cleaning up..."
  if [[ -f "$PIDFILE" ]]; then
    kill_children "$(cat "$PIDFILE")"
  fi
  exit 0
}
trap cleanup INT TERM

# --- main loop ---
echo "[$(date -Is)] watchdog starting: list=$SUBURB_LIST page_type=$PAGE_TYPE sources=$SOURCES stall=${STALL_MIN}min cooldown=${COOLDOWN_SEC}s"

attempt=0
while true; do
  attempt=$((attempt + 1))
  echo "[$(date -Is)] === attempt $attempt: launching listo fetch ==="

  # Launch in its own session/process-group via setsid so kill_children can
  # signal the whole tree (uv → python → patchright node → chromium) by
  # PGID, without affecting sibling watchdogs in a parallel pool.
  setsid env \
      LISTO_REQUEST_MIN_DELAY="$MIN_DELAY" \
      LISTO_REQUEST_MAX_DELAY="$MAX_DELAY" \
      "$UV" run listo fetch all \
        --page-type "$PAGE_TYPE" \
        --suburb-list "$SUBURB_LIST" \
        --bucketed \
        --sources "$SOURCES" \
        --worker-index "$WORKER_INDEX" \
        --worker-count "$WORKER_COUNT" \
      </dev/null >> "$LOG" 2>&1 &
  child=$!
  echo "$child" > "$PIDFILE"
  echo "[$(date -Is)] launched as PID $child (log: $LOG)"

  # Stall-detection loop.
  prev_count="$(db_count)"
  prev_count=${prev_count:-0}
  stall_minutes=0
  while kill -0 "$child" 2>/dev/null; do
    sleep 60
    cur_count="$(db_count)"
    cur_count=${cur_count:-0}
    if [[ "$cur_count" -gt "$prev_count" ]]; then
      delta=$((cur_count - prev_count))
      echo "[$(date -Is)] heartbeat: raw_pages=$cur_count (+$delta in last 60s) — stall counter reset"
      stall_minutes=0
      prev_count="$cur_count"
    else
      stall_minutes=$((stall_minutes + 1))
      echo "[$(date -Is)] heartbeat: raw_pages=$cur_count (no change) — stall=${stall_minutes}min/${STALL_MIN}min"
      if [[ "$stall_minutes" -ge "$STALL_MIN" ]]; then
        echo "[$(date -Is)] STALL DETECTED — killing and restarting"
        kill_children "$child"
        break
      fi
    fi
  done

  # Either child exited naturally OR we killed it for stall.
  if kill -0 "$child" 2>/dev/null; then
    kill_children "$child"
  else
    echo "[$(date -Is)] child PID $child exited"
  fi

  echo "[$(date -Is)] sleeping ${COOLDOWN_SEC}s before next attempt..."
  sleep "$COOLDOWN_SEC"
done
