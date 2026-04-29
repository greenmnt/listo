#!/usr/bin/env bash
# Watchdog wrapper for `listo fetch all --bucketed` with dongle IP rotation.
#
# Like fetch_watchdog.sh but on stall:
#   1. Records the current dongle IP as "bad" in the cache
#   2. Rotates the dongle (REBOOT_DEVICE → new public IP)
#   3. Restarts the fetcher on the fresh IP
#
# Pass-through args for multi-machine setups:
#   COMPUTER_INDEX / COMPUTER_COUNT — coarse split across physical machines
#   WORKER_INDEX   / WORKER_COUNT   — fine split within this machine
# E.g. with COMPUTER_COUNT=2 WORKER_COUNT=2 you get 4 distinct slots — the
# laptop runs (computer=0, workers=0,1) and the Pi runs (computer=1,
# workers=0,1).
#
# Usage:
#   LISTO_DONGLE_PASSWORD=Admin ./scripts/fetch_dongle_watchdog.sh
#   COMPUTER_INDEX=0 COMPUTER_COUNT=2 LISTO_DONGLE_PASSWORD=Admin ./scripts/fetch_dongle_watchdog.sh
#
# Stop: send SIGTERM to the watchdog itself; it'll kill the child + exit.

set -uo pipefail

# --- config ---
SUBURB_LIST="${SUBURB_LIST:-gold_coast}"
PAGE_TYPE="${PAGE_TYPE:-sold}"
SOURCES="${SOURCES:-realestate}"
STALL_MIN="${STALL_MIN:-8}"
COOLDOWN_SEC="${COOLDOWN_SEC:-30}"
# Proactive rotation: rotate IP after this many pages even if nothing's gone
# wrong. Empirically Kasada's risk score climbs over ~480 pages per IP before
# blocking, so rotating in the 300-400 range stays well clear of that wall.
# Each restart picks a random threshold in [PROACTIVE_MIN, PROACTIVE_MAX] so
# our cadence isn't a fixed pattern Kasada could fingerprint.
# Set PROACTIVE_MIN=0 to disable proactive rotation (reactive-only).
PROACTIVE_MIN="${PROACTIVE_MIN:-300}"
PROACTIVE_MAX="${PROACTIVE_MAX:-400}"
MIN_DELAY="${LISTO_REQUEST_MIN_DELAY:-1.0}"
MAX_DELAY="${LISTO_REQUEST_MAX_DELAY:-3.0}"
WORKER_INDEX="${WORKER_INDEX:-0}"
WORKER_COUNT="${WORKER_COUNT:-1}"
COMPUTER_INDEX="${COMPUTER_INDEX:-0}"
COMPUTER_COUNT="${COMPUTER_COUNT:-1}"
ENGINE="${ENGINE:-playwright}"
USE_CDP="${LISTO_CDP_ATTACH:-1}"

# Dongle: rotate on stall (1) or only at startup (0). When rotating fails
# (e.g. dongle is unplugged / no password), we still restart so the fetcher
# can keep trying.
ROTATE_ON_STALL="${ROTATE_ON_STALL:-1}"
ROTATE_ON_START="${ROTATE_ON_START:-0}"

# Per-instance log so multiple watchdogs don't trample each other.
INSTANCE_TAG="${INSTANCE_TAG:-c${COMPUTER_INDEX}of${COMPUTER_COUNT}-w${WORKER_INDEX}of${WORKER_COUNT}}"
# Single-log mode: if stdout is already pointing at a regular file (e.g.
# `nohup ./fetch_dongle_watchdog.sh > /tmp/dongle-w0.log 2>&1`), reuse THAT
# file as our LOG. Then watchdog heartbeats AND `listo fetch` output AND
# dongle helper output all land in one place — `tail -f /tmp/dongle-w0.log`
# shows everything. Otherwise fall back to a per-instance default and
# self-redirect to it.
stdout_target="$(readlink /proc/$$/fd/1 2>/dev/null || true)"
if [[ -f "$stdout_target" ]]; then
  LOG="$stdout_target"
else
  LOG="/tmp/listo-dongle-${INSTANCE_TAG}.log"
  exec >> "$LOG" 2>&1
fi
KERNEL_LOG="/tmp/listo-dongle-${INSTANCE_TAG}.kernel.log"
HEALTH_LOG="/tmp/listo-dongle-${INSTANCE_TAG}.health.log"
PIDFILE="/tmp/listo-dongle-${INSTANCE_TAG}.pid"
UV="${UV:-$HOME/.local/bin/uv}"

# On startup, rotate only if the current IP is in the cache marked bad.
# (As opposed to ROTATE_ON_START which rotates unconditionally.) This is the
# usual case — most starts happen on a fresh IP and we don't want to burn
# a 45s reboot for nothing, but if we restart while sitting on a known-bad
# IP, kick to a new one before fetching.
ROTATE_IF_BAD_ON_START="${ROTATE_IF_BAD_ON_START:-1}"

DB_USER="${DB_USER:-listo}"
DB_PASS="${DB_PASS:-password}"
DB_NAME="${DB_NAME:-listo}"

# --- helpers ---
db_count() {
  local sources_sql
  sources_sql="$(echo "$SOURCES" | sed "s/,/','/g")"
  mysql -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -BNe \
    "SELECT COUNT(*) FROM raw_pages WHERE source IN ('$sources_sql');" \
    2>/dev/null | tail -1
}

# Count of ERR_HTTP_RESPONSE_CODE_FAILURE occurrences in the child log.
# Each one is a deep-page 4xx (almost always Kasada returning 429 on a list
# page after the warmup challenge). One of these is enough to know the IP
# is flagged — no point waiting out the 8-min stall timer.
#
# Subtle: `grep -c` writes "0" to stdout AND exits status 1 when there are
# no matches. The naive `grep ... || echo 0` produces "0\n0" — bash then
# fails arithmetic on that. We use a local var with `|| n=0` which keeps
# the captured number on success and falls through to 0 on the exit-1 case.
log_block_count() {
  local n=0
  if [[ -f "$LOG" ]]; then
    n=$(grep -c "ERR_HTTP_RESPONSE_CODE_FAILURE" "$LOG" 2>/dev/null) || n=0
  fi
  echo "$n"
}

dongle_record_bad() {
  echo "[$(date -Is)] marking current IP bad in cache"
  "$UV" run python -m listo.fetch.dongle record-bad 2>&1 || true
}

dongle_record_good() {
  echo "[$(date -Is)] marking current IP good in cache"
  "$UV" run python -m listo.fetch.dongle record-good 2>&1 || true
}

dongle_rotate() {
  echo "[$(date -Is)] rotating dongle (this takes ~45s)"
  "$UV" run python -m listo.fetch.dongle rotate 2>&1 || true
}

dongle_rotate_if_bad() {
  echo "[$(date -Is)] checking if current IP is known-bad"
  "$UV" run python -m listo.fetch.dongle rotate-if-bad 2>&1 || true
}

dongle_health() {
  # One-line health snapshot for the heartbeat. Returns immediately on probe
  # failure (the watchdog should never hang on diagnostics).
  "$UV" run python -m listo.fetch.dongle health 2>/dev/null || echo "(health probe failed)"
}

kill_children() {
  local pid="${1:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "[$(date -Is)] killing PGID $pid"
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
  if [[ -n "${KERNEL_TAIL_PID:-}" ]] && kill -0 "$KERNEL_TAIL_PID" 2>/dev/null; then
    kill "$KERNEL_TAIL_PID" 2>/dev/null || true
  fi
  exit 0
}
trap cleanup INT TERM

# --- background diagnostic streams ---
# Tail kernel events for the dongle's USB/network interface into a dedicated
# log. Captures the exact moment of any reboot/disconnect with full kernel
# context (much better than after-the-fact `journalctl` queries since the
# events stream live and won't be lost to log rotation).
echo "[$(date -Is)] starting kernel-event stream → $KERNEL_LOG"
(
  echo "=== kernel stream started $(date -Is) ==="
  journalctl -kf --since now -o short-iso 2>&1 \
    | grep --line-buffered -iE "usb|enp0s20|cdc_ether|disconnect|reset|carrier|modem" \
    >> "$KERNEL_LOG"
) &
KERNEL_TAIL_PID=$!

# --- main loop ---
echo "[$(date -Is)] dongle-watchdog starting:"
echo "    list=$SUBURB_LIST page_type=$PAGE_TYPE sources=$SOURCES engine=$ENGINE"
echo "    computer=$COMPUTER_INDEX/$COMPUTER_COUNT  worker=$WORKER_INDEX/$WORKER_COUNT"
echo "    stall=${STALL_MIN}min  cooldown=${COOLDOWN_SEC}s"
echo "    proactive_rotate=${PROACTIVE_MIN}-${PROACTIVE_MAX} pages  rotate_on_stall=$ROTATE_ON_STALL"
echo "    log=$LOG"

if [[ "$ROTATE_ON_START" == "1" ]]; then
  dongle_rotate
elif [[ "$ROTATE_IF_BAD_ON_START" == "1" ]]; then
  dongle_rotate_if_bad
fi

attempt=0
while true; do
  attempt=$((attempt + 1))
  echo "[$(date -Is)] === attempt $attempt: launching listo fetch ==="

  # Pre-launch: if we rotated to (or are still sitting on) a known-bad IP,
  # rotate again. Cheap no-op when the IP is fine.
  if [[ "$attempt" -gt 1 && "$ROTATE_IF_BAD_ON_START" == "1" ]]; then
    dongle_rotate_if_bad
  fi
  echo "[$(date -Is)] $(dongle_health)"

  # Build the env for the child. LISTO_CDP_ATTACH carried through so CDP
  # mode survives across restarts.
  setsid env \
      LISTO_REQUEST_MIN_DELAY="$MIN_DELAY" \
      LISTO_REQUEST_MAX_DELAY="$MAX_DELAY" \
      LISTO_CDP_ATTACH="$USE_CDP" \
      "$UV" run listo fetch all \
        --page-type "$PAGE_TYPE" \
        --suburb-list "$SUBURB_LIST" \
        --bucketed \
        --sources "$SOURCES" \
        --engine "$ENGINE" \
        --worker-index "$WORKER_INDEX" \
        --worker-count "$WORKER_COUNT" \
        --computer-index "$COMPUTER_INDEX" \
        --computer-count "$COMPUTER_COUNT" \
      </dev/null 2>&1 &
  child=$!
  echo "$child" > "$PIDFILE"
  echo "[$(date -Is)] launched as PID $child"

  # Stall + proactive + HTTP-block detection loop.
  start_count="$(db_count)"
  start_count=${start_count:-0}
  prev_count="$start_count"
  start_block_count="$(log_block_count)"
  start_block_count=${start_block_count:-0}
  stall_minutes=0
  stalled=0
  proactive_trigger=0
  blocked_trigger=0
  # Pick a fresh proactive threshold per attempt so cadence isn't a fixed
  # pattern Kasada could fingerprint. PROACTIVE_MIN=0 disables.
  if [[ "$PROACTIVE_MIN" -gt 0 ]]; then
    rotate_after=$((PROACTIVE_MIN + RANDOM % (PROACTIVE_MAX - PROACTIVE_MIN + 1)))
    echo "[$(date -Is)] proactive rotation will trigger after $rotate_after pages this attempt"
  else
    rotate_after=0
  fi
  while kill -0 "$child" 2>/dev/null; do
    sleep 60
    # Fast path: any new ERR_HTTP_RESPONSE_CODE_FAILURE since launch means
    # Kasada is rejecting deep pages on this IP. Don't wait for the 8-min
    # stall timer — one error is enough.
    cur_block_count="$(log_block_count)"
    cur_block_count=${cur_block_count:-0}
    new_blocks=$((cur_block_count - start_block_count))
    if [[ "$new_blocks" -gt 0 ]]; then
      echo "[$(date -Is)] DEEP-PAGE 429 DETECTED ($new_blocks errors since launch) — rotating immediately"
      blocked_trigger=1
      kill_children "$child"
      break
    fi
    cur_count="$(db_count)"
    cur_count=${cur_count:-0}
    pages_this_attempt=$((cur_count - start_count))
    # Health snapshot — single line, easy to grep when correlating with a
    # disconnect later. Runs every minute on a 3s timeout so it can't hang.
    health="$(dongle_health)"
    echo "[$(date -Is)] $health" >> "$HEALTH_LOG"
    if [[ "$cur_count" -gt "$prev_count" ]]; then
      delta=$((cur_count - prev_count))
      echo "[$(date -Is)] heartbeat: raw_pages=$cur_count (+$delta this minute, $pages_this_attempt this attempt)  $health"
      stall_minutes=0
      prev_count="$cur_count"
    else
      stall_minutes=$((stall_minutes + 1))
      echo "[$(date -Is)] heartbeat: raw_pages=$cur_count ($pages_this_attempt this attempt, no change) — stall=${stall_minutes}/${STALL_MIN}min  $health"
      if [[ "$stall_minutes" -ge "$STALL_MIN" ]]; then
        echo "[$(date -Is)] STALL DETECTED"
        stalled=1
        kill_children "$child"
        break
      fi
    fi
    # Proactive trigger: enough pages on this IP, rotate before Kasada flags it.
    if [[ "$rotate_after" -gt 0 && "$pages_this_attempt" -ge "$rotate_after" ]]; then
      echo "[$(date -Is)] PROACTIVE ROTATION ($pages_this_attempt >= $rotate_after pages on this IP)"
      proactive_trigger=1
      kill_children "$child"
      break
    fi
  done

  # Either child exited naturally OR we killed it for stall/proactive.
  if kill -0 "$child" 2>/dev/null; then
    kill_children "$child"
  else
    echo "[$(date -Is)] child PID $child exited"
  fi

  # On stall OR HTTP-block: mark IP bad. On proactive: mark IP good.
  # Rotate the dongle in either case if rotation is enabled.
  if [[ ("$stalled" == "1" || "$blocked_trigger" == "1") && "$ROTATE_ON_STALL" == "1" ]]; then
    dongle_record_bad
    dongle_rotate
  elif [[ "$proactive_trigger" == "1" ]]; then
    dongle_record_good
    dongle_rotate
  fi

  echo "[$(date -Is)] sleeping ${COOLDOWN_SEC}s before next attempt..."
  sleep "$COOLDOWN_SEC"
done
