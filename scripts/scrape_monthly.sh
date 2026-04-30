#!/usr/bin/env bash
# Launch N parallel workers that scrape a council month-by-month, each
# taking a disjoint slice of the date range.
#
# Usage:
#   scripts/scrape_monthly.sh <from-date> <to-date> [worker_count] [council_slug]
#
# Defaults: 4 workers, council 'cogc'.
#
# Each worker:
#   - walks the full month list
#   - claims only its modulo-bucket of months (worker_index % worker_count)
#   - skips months already marked `completed` in council_scrape_windows
#   - logs to /tmp/listo-worker-<i>.log
#
# Re-running this script after Ctrl-C / reboot is safe — completed months
# are skipped automatically.

set -euo pipefail

DATE_FROM="${1:-}"
DATE_TO="${2:-}"
WORKER_COUNT="${3:-4}"
COUNCIL_SLUG="${4:-cogc}"

if [[ -z "$DATE_FROM" || -z "$DATE_TO" ]]; then
  cat <<EOF
usage: $(basename "$0") <from-date> <to-date> [worker_count=4] [council_slug=cogc]

example:
  $(basename "$0") 2020-01-01 2026-04-30
  $(basename "$0") 2020-01-01 2026-04-30 8 cogc

dates are YYYY-MM-DD. The script splits the range one month at a time
across N parallel workers; each worker logs to /tmp/listo-worker-<i>.log.
EOF
  exit 64
fi

# cd into the repo root regardless of where the script was invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Heads-up: refuse to launch alongside broad-window scrapes that would
# fight for the same months. The user can `pkill` them and rerun.
if pgrep -af 'listo council scrape ' | grep -v 'scrape-monthly' >/dev/null; then
  echo "warning: broad 'listo council scrape' processes are still running:"
  pgrep -af 'listo council scrape ' | grep -v 'scrape-monthly'
  echo
  echo "they'll race with these workers. kill with:"
  echo "  pkill -f 'listo council scrape '"
  echo
  read -r -p "continue anyway? [y/N] " ans
  case "$ans" in
    y|Y|yes) ;;
    *) echo "aborted."; exit 1 ;;
  esac
fi

echo "council:  $COUNCIL_SLUG"
echo "range:    $DATE_FROM → $DATE_TO"
echo "workers:  $WORKER_COUNT"
echo "logs:     /tmp/listo-worker-{0..$((WORKER_COUNT-1))}.log"
echo

PIDS=()
for i in $(seq 0 $((WORKER_COUNT - 1))); do
  LOG="/tmp/listo-worker-${i}.log"
  : > "$LOG"
  nohup uv run listo council scrape-monthly "$COUNCIL_SLUG" \
    --from "$DATE_FROM" --to "$DATE_TO" \
    --worker-index "$i" --worker-count "$WORKER_COUNT" \
    > "$LOG" 2>&1 &
  PID=$!
  disown $PID
  PIDS+=($PID)
  echo "  worker $i  pid=$PID  log=$LOG"
done

echo
echo "live progress:"
echo "  uv run listo council months $COUNCIL_SLUG --from $DATE_FROM"
echo
echo "tail every worker:"
echo "  tail -f /tmp/listo-worker-*.log"
echo
echo "stop everything:"
echo "  kill ${PIDS[*]}"
