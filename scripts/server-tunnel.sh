#!/usr/bin/env bash
# Open an SSH tunnel from this laptop's localhost:3307 to the server's MySQL
# (127.0.0.1:3306 on the server). While it's open, any client can connect to
# 127.0.0.1:3307 here and is talking to the server's database.
#
# Usage:
#   ./scripts/server-tunnel.sh           # foreground, Ctrl-C to stop
#
# Pair with `.env.server` (LISTO_DATABASE_URL=...:3307...) when running scrapers:
#   set -a; source .env.server; set +a
#   uv run listo property history --da EDA/2021/97
#
# Sanity check while it's running:
#   mysql -h 127.0.0.1 -P 3307 -u listo -ppassword listo \
#     -e "SELECT council_slug, COUNT(*) FROM council_applications GROUP BY council_slug;"

set -euo pipefail

REMOTE="ubuntu@65.21.199.218"
LOCAL_PORT=3307
REMOTE_HOST=127.0.0.1
REMOTE_PORT=3306

SSH_OPTS=(
    -N
    -L "${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT}"
    -i "${HOME}/.ssh/id_ed25519_kwaku"
    -o IdentitiesOnly=yes
    -o ServerAliveInterval=30
    -o ServerAliveCountMax=3
    -o ExitOnForwardFailure=yes
)

if command -v autossh >/dev/null 2>&1; then
    echo "Opening autossh tunnel: localhost:${LOCAL_PORT} -> ${REMOTE}:${REMOTE_PORT}"
    echo "(reconnects automatically; Ctrl-C to stop)"
    exec autossh -M 0 "${SSH_OPTS[@]}" "${REMOTE}"
else
    echo "autossh not installed - falling back to plain ssh."
    echo "  to install: sudo apt install -y autossh"
    echo "Opening ssh tunnel: localhost:${LOCAL_PORT} -> ${REMOTE}:${REMOTE_PORT}"
    echo "(no auto-reconnect; Ctrl-C to stop)"
    exec ssh "${SSH_OPTS[@]}" "${REMOTE}"
fi
