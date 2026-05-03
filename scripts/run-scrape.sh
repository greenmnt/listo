#!/usr/bin/env bash
# Restart Chrome on :9222 with the listo profile, source .env.server, and
# run `listo property scrape-batch`. Any args passed are forwarded to
# scrape-batch, e.g.:
#
#   ./scripts/run-scrape.sh --dry-run --limit 10
#   ./scripts/run-scrape.sh --limit 50
#   ./scripts/run-scrape.sh                       # full 1,127 candidates
#
# Assumes the SSH tunnel to the server DB is already running in another
# terminal: ./scripts/server-tunnel.sh
#
# After the script restarts Chrome, you may need to manually click through
# realestate.com.au's Kasada interstitial once if the listo profile is
# fresh or has expired cookies. The script pauses briefly to let you do
# that before kicking off the scrape.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Persistent profile (so Kasada cookies survive reboots — /tmp is tmpfs).
# Override with LISTO_CHROME_PROFILE=/some/other/dir if needed.
USER_DATA_DIR="${LISTO_CHROME_PROFILE:-$HOME/.config/google-chrome-listo}"
CDP_PORT=9222
CHROME_LOG=/tmp/listo-chrome.log

# 1. Close any existing Chrome listening on :9222
existing_pids=$(lsof -ti ":${CDP_PORT}" -sTCP:LISTEN 2>/dev/null || true)
if [[ -n "$existing_pids" ]]; then
    echo "Closing existing Chrome on :${CDP_PORT} (PIDs: $existing_pids)"
    kill $existing_pids 2>/dev/null || true
    for _ in 1 2 3 4 5; do
        sleep 1
        still=$(lsof -ti ":${CDP_PORT}" -sTCP:LISTEN 2>/dev/null || true)
        [[ -z "$still" ]] && break
    done
    still=$(lsof -ti ":${CDP_PORT}" -sTCP:LISTEN 2>/dev/null || true)
    if [[ -n "$still" ]]; then
        echo "  forcing kill -9 on $still"
        kill -9 $still 2>/dev/null || true
        sleep 1
    fi
fi

# 2. Locate a Chrome binary
CHROME_BIN="${CHROME_BIN:-$(command -v google-chrome || command -v google-chrome-stable || command -v chromium || command -v chromium-browser || true)}"
if [[ -z "$CHROME_BIN" ]]; then
    echo "ERROR: no chrome/chromium binary found. Install google-chrome or set CHROME_BIN=path/to/chrome" >&2
    exit 1
fi

# 3. Launch fresh Chrome with the listo profile
echo "Launching $CHROME_BIN"
echo "  --user-data-dir=$USER_DATA_DIR"
echo "  --remote-debugging-port=$CDP_PORT"
nohup "$CHROME_BIN" \
    --remote-debugging-port="$CDP_PORT" \
    --user-data-dir="$USER_DATA_DIR" \
    > "$CHROME_LOG" 2>&1 &
disown

# 4. Wait for CDP to come up
echo -n "  waiting for CDP..."
for i in 1 2 3 4 5 6 7 8 9 10; do
    if curl -s --max-time 1 "http://localhost:${CDP_PORT}/json/version" > /dev/null 2>&1; then
        echo " up after ${i}s"
        break
    fi
    sleep 1
    echo -n "."
done
if ! curl -s --max-time 1 "http://localhost:${CDP_PORT}/json/version" > /dev/null 2>&1; then
    echo
    echo "ERROR: Chrome CDP not reachable on :${CDP_PORT} - check $CHROME_LOG" >&2
    exit 1
fi

# 5. Source server-DB env (assumes tunnel is up)
cd "$REPO_ROOT"
if [[ ! -f .env.server ]]; then
    echo "ERROR: .env.server missing in $REPO_ROOT" >&2
    exit 1
fi
set -a
# shellcheck disable=SC1091
source .env.server
set +a
echo "  LISTO_DATABASE_URL=$LISTO_DATABASE_URL"

# 6. Verify the SSH tunnel is reachable before launching the scrape
if ! mysql -h 127.0.0.1 -P 3307 -u listo -ppassword -e "SELECT 1" listo > /dev/null 2>&1; then
    echo
    echo "ERROR: can't reach server DB on 127.0.0.1:3307." >&2
    echo "Start the tunnel in another terminal first:" >&2
    echo "  ./scripts/server-tunnel.sh" >&2
    exit 1
fi

# 7. Brief pause so the user can click through any Kasada interstitial in the
#    fresh Chrome before the scrape starts hitting realestate.com.au.
echo
echo "Chrome is up. If you see a Kasada / Cloudflare interstitial on REA,"
echo "click through it now. Starting scrape in 5s..."
sleep 5

# 8. Hand off to scrape-batch with any pass-through args
echo
echo "Running: uv run listo property scrape-batch $*"
exec uv run listo property scrape-batch "$@"
