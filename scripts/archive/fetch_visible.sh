#!/usr/bin/env bash
# Run listo fetchers with VISIBLE chromium windows on the user's real wayland
# session. Use when the headless mutter+patchright path is being detected
# (Kasada now flags our headless fingerprint) — visible patchright on the
# real GUI session typically passes because the GPU + compositor are real
# and indistinguishable from a normal user's chromium.
#
# Trade-off: chromium windows pop on your screen. Don't run this while
# you're trying to use the desktop.
#
# Usage:
#   ./scripts/fetch_visible.sh                      # 2 workers, gold_coast, sold, realestate
#   WORKERS=1 ./scripts/fetch_visible.sh
#   SUBURB_LIST=palm_beach PAGE_TYPE=buy ./scripts/fetch_visible.sh
#
# Stop: Ctrl+C — cleans up all worker process groups.

set -uo pipefail

WORKERS="${WORKERS:-2}"
SUBURB_LIST="${SUBURB_LIST:-gold_coast}"
PAGE_TYPE="${PAGE_TYPE:-sold}"
SOURCES="${SOURCES:-realestate}"
MIN_DELAY="${LISTO_REQUEST_MIN_DELAY:-2.0}"
MAX_DELAY="${LISTO_REQUEST_MAX_DELAY:-5.0}"
UV="${UV:-$HOME/.local/bin/uv}"

# Detect the user's real wayland socket. We deliberately skip wayland-99
# (mutter — headless), since the whole point of this script is to use the
# real visible session.
RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
REAL_DISPLAY=""
for d in wayland-0 wayland-1 wayland-2; do
  sock="$RUNTIME_DIR/$d"
  if [[ -S "$sock" && "$d" != "wayland-99" ]]; then
    REAL_DISPLAY="$d"
    break
  fi
done
if [[ -z "$REAL_DISPLAY" ]]; then
  echo "ERROR: no real wayland socket found in $RUNTIME_DIR" >&2
  echo "(this script needs a visible GUI session, not the mutter virtual one)" >&2
  ls "$RUNTIME_DIR"/wayland-* 2>&1 >&2
  exit 1
fi

echo "[$(date -Is)] visible-window mode: WAYLAND_DISPLAY=$REAL_DISPLAY"
echo "[$(date -Is)]   workers=$WORKERS  list=$SUBURB_LIST  page_type=$PAGE_TYPE  sources=$SOURCES"
echo "[$(date -Is)]   delays=${MIN_DELAY}-${MAX_DELAY}s"
echo "[$(date -Is)] chromium windows will appear on your desktop. Ctrl+C to stop."

PIDS=()
cleanup() {
  echo ""
  echo "[$(date -Is)] stopping all workers..."
  for p in "${PIDS[@]}"; do
    # Each worker was launched via setsid, so PID == PGID. Negative kill
    # signals the whole tree (uv → python → patchright node → chromium).
    kill -TERM -"$p" 2>/dev/null || true
  done
  sleep 3
  for p in "${PIDS[@]}"; do
    kill -KILL -"$p" 2>/dev/null || true
  done
  exit 0
}
trap cleanup INT TERM

for ((i=0; i<WORKERS; i++)); do
  LOG="/tmp/listo-visible-w${i}of${WORKERS}.log"
  echo "[$(date -Is)] starting worker $i/$WORKERS — log: $LOG"
  setsid env \
    WAYLAND_DISPLAY="$REAL_DISPLAY" \
    LISTO_WAYLAND_DISPLAY="$REAL_DISPLAY" \
    LISTO_REQUEST_MIN_DELAY="$MIN_DELAY" \
    LISTO_REQUEST_MAX_DELAY="$MAX_DELAY" \
    "$UV" run listo fetch all \
      --page-type "$PAGE_TYPE" \
      --suburb-list "$SUBURB_LIST" \
      --bucketed \
      --sources "$SOURCES" \
      --worker-index "$i" \
      --worker-count "$WORKERS" \
    </dev/null > "$LOG" 2>&1 &
  PIDS+=("$!")
  sleep 5  # stagger so the windows don't appear all at once
done

echo "[$(date -Is)] all $WORKERS workers running"
wait
