#!/usr/bin/env bash
# End-to-end analysis pipeline for one council DA — or, with no app-id,
# a batch over every unprocessed duplex candidate it can find.
#
# Per-DA pipeline:
#   1. summarise   (phase 1 — first + last doc → entities)
#   2. escalate    (phase 2 — tier-2 docs if entity row is incomplete)
#   3. features    (phase 2.5 — chunked build-feature extraction)
#   4. aggregate   (phase 3 — merge per-doc rows → da_summaries)
#   5. property fetch (Domain via httpx; Realestate via Chrome :9222)
#   6. property history (Google discovery + comparable PDPs)
#
# Steps 5-6 auto-skip when Chrome :9222 isn't up — the script prints
# instructions for warming it up and continues so the LLM phases still
# complete.
#
# Usage:
#   scripts/analyse_da.sh                     # batch: find + process all duplex candidates
#   scripts/analyse_da.sh --limit 3           # batch, cap at 3 candidates
#   scripts/analyse_da.sh MCU/2025/568        # single app
#   scripts/analyse_da.sh MCU/2025/568 qwen2.5:7b-instruct
#
# Env overrides:
#   LISTO_MODEL    — default qwen2.5:7b-instruct (also positional arg 2)
#   LISTO_DB_USER  — default listo
#   LISTO_DB_PASS  — default password
#   LISTO_DB_NAME  — default listo

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DB_USER="${LISTO_DB_USER:-listo}"
DB_PASS="${LISTO_DB_PASS:-password}"
DB_NAME="${LISTO_DB_NAME:-listo}"
MODEL_DEFAULT="${LISTO_MODEL:-qwen2.5:7b-instruct}"

# ---- arg parsing ---------------------------------------------------

APP_ID=""
MODEL="$MODEL_DEFAULT"
LIMIT=0   # 0 = no cap

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    --limit)
      LIMIT="${2:-}"
      [[ -z "$LIMIT" ]] && { echo "--limit needs a number" >&2; exit 64; }
      shift 2
      ;;
    --limit=*)
      LIMIT="${1#--limit=}"
      shift
      ;;
    --)
      shift; break
      ;;
    -*)
      echo "unknown flag: $1" >&2
      exit 64
      ;;
    *)
      if [[ -z "$APP_ID" ]]; then
        APP_ID="$1"
      else
        MODEL="$1"
      fi
      shift
      ;;
  esac
done

# ---- helpers -------------------------------------------------------

mysql_q() {
  # Quiet, no-headers query helper. Reads SQL from stdin.
  MYSQL_PWD="$DB_PASS" mysql -u "$DB_USER" -N -B "$DB_NAME"
}

fmt_elapsed() {
  # Format seconds → "Hh MMm SSs" / "MMm SSs" / "SSs".
  local s=$1
  if (( s >= 3600 )); then
    printf '%dh %02dm %02ds' $(( s / 3600 )) $(( (s % 3600) / 60 )) $(( s % 60 ))
  elif (( s >= 60 )); then
    printf '%dm %02ds' $(( s / 60 )) $(( s % 60 ))
  else
    printf '%ds' "$s"
  fi
}

time_phase() {
  # time_phase "label" cmd args...
  local label="$1"; shift
  local start
  start=$(date +%s)
  echo "-- $label --"
  if "$@"; then
    local rc=0
  else
    local rc=$?
  fi
  local elapsed=$(( $(date +%s) - start ))
  PHASE_TIMINGS+=("$(printf '  %-40s %s' "$label" "$(fmt_elapsed "$elapsed")")")
  echo "   ↳ $label took $(fmt_elapsed "$elapsed")"
  return "$rc"
}

list_duplex_candidates() {
  # Unprocessed duplex DAs (no da_summaries row).
  #
  # We require a Plans/Drawings doc AND a Supporting Document to be
  # *indexed* (listed in council_application_documents) but NOT
  # necessarily *downloaded* — phase 0 of the per-app pipeline pulls
  # any missing docs on demand via `listo council fetch-app-docs`.
  # This unlocks the long tail of DAs where the bulk scrape only
  # grabbed first+last and missed the substantive bundle.
  #
  # Ordering: oldest lodgement first (older parent sale → duplex has
  # had time to be built/listed/sold → complete pre→post signal).
  # Then prefer doc-rich apps so the LLM has more to work with.
  local lim_clause=""
  [[ "$LIMIT" -gt 0 ]] && lim_clause="LIMIT $LIMIT"

  mysql_q <<SQL
SELECT ca.application_id
  FROM council_applications ca
  JOIN council_application_documents d ON d.application_id = ca.id
  LEFT JOIN da_summaries ds ON ds.application_id = ca.id
 WHERE ds.application_id IS NULL
   AND ca.description REGEXP 'DUAL OCCUPANCY|DUPLEX'
 GROUP BY ca.id, ca.application_id
HAVING SUM(d.doc_type LIKE '%Drawing%'
        OR d.doc_type LIKE '%Plans%'
        OR d.doc_type LIKE '%Stamped Approved Plan%') >= 1
   AND SUM(d.doc_type LIKE '%Supporting Document%') >= 1
 ORDER BY
   ca.lodged_date ASC,
   (
     SUM(d.doc_type LIKE '%Drawing%' OR d.doc_type LIKE '%Plans%' OR d.doc_type LIKE '%Stamped Approved Plan%')
   + SUM(d.doc_type LIKE '%Supporting Document%')
   + SUM(d.doc_type LIKE '%Specialist Report%')
   ) DESC,
   COUNT(d.id) DESC
 $lim_clause;
SQL
}

run_pipeline() {
  local app_id="$1"
  local model="$2"

  echo
  echo "=================================================================="
  echo "  analyse $app_id  (model=$model)"
  echo "  started: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "=================================================================="

  PHASE_TIMINGS=()
  local pipeline_start
  pipeline_start=$(date +%s)

  # Phase 0 — make sure every listed doc has actually been downloaded.
  # The initial bulk scrape only pulls first+last per app (size cap), so
  # apps often have a Form 1 / Cover Letter that was indexed but never
  # fetched, leaving the LLM blind to applicant/builder names. This step
  # forces a full per-app re-fetch (LISTO_DOWNLOAD_ALL=1 internally).
  # Idempotent — already-downloaded docs are skipped by content_hash.
  local missing_docs
  missing_docs=$(MYSQL_PWD="$DB_PASS" mysql -u "$DB_USER" -N -B "$DB_NAME" <<SQL
SELECT COUNT(*)
  FROM council_application_documents cad
  JOIN council_applications ca ON ca.id = cad.application_id
 WHERE ca.application_id = '$app_id'
   AND cad.file_path IS NULL;
SQL
)
  if [[ "${missing_docs:-0}" -gt 0 ]]; then
    time_phase "phase 0: fetch missing docs ($missing_docs listed but undownloaded)" \
      uv run listo council fetch-app-docs "$app_id"
  else
    echo "-- phase 0: all docs on disk, skipping --"
    PHASE_TIMINGS+=("$(printf '  %-40s %s' "phase 0: fetch missing docs (skipped)" "0s")")
  fi
  echo

  time_phase "phase 1: summarise" \
    uv run listo da summarise --app-id "$app_id" --model "$model"
  echo

  # Seed da_summaries row so phase 2 + 2.5 can find it (their SELECTs
  # JOIN da_summaries — without this row they return 0 apps).
  time_phase "phase 3a: aggregate (seed)" \
    uv run listo da aggregate --app-id "$app_id"
  echo

  time_phase "phase 2: escalate (incomplete only)" \
    uv run listo da escalate --app-id "$app_id" --model "$model" --max-tier2-docs 5
  echo

  time_phase "phase 2.5: build features" \
    uv run listo da features --app-id "$app_id" --model "$model"
  echo

  time_phase "phase 3b: aggregate (finalise)" \
    uv run listo da aggregate --app-id "$app_id"
  echo

  if ss -ltn 2>/dev/null | grep -q ':9222 '; then
    time_phase "property fetch (domain + realestate)" \
      uv run listo property fetch --da "$app_id" --sources all
    echo
    time_phase "property history (google + comparables)" \
      uv run listo property history --da "$app_id" --skip-listings
    echo
    # Built-status check — Google for unit-prefixed addresses to detect
    # duplexes built but never sold (held / rented). Needs Chrome :9222.
    time_phase "built-status check (unit-prefixed Google)" \
      uv run listo da check-built --app-id "$app_id" --min-age-months 6
  else
    time_phase "property fetch (domain only — chrome :9222 down)" \
      uv run listo property fetch --da "$app_id" --sources domain
    echo
    echo "  Skipped: realestate fetch + comparable discovery + built-status check."
    echo "  to enable: launch chrome with"
    echo "    google-chrome --remote-debugging-port=9222 --user-data-dir=/tmp/listo-chrome"
    echo "  visit realestate.com.au + domain.com.au once each, then re-run this script."
  fi

  local total_elapsed=$(( $(date +%s) - pipeline_start ))
  echo
  echo "------------------------------------------------------------------"
  echo "  timings for $app_id"
  echo "------------------------------------------------------------------"
  for line in "${PHASE_TIMINGS[@]}"; do
    echo "$line"
  done
  printf '  %-40s %s\n' "TOTAL" "$(fmt_elapsed "$total_elapsed")"
  echo
  echo ">> done: $app_id"
}

# ---- main ----------------------------------------------------------

if [[ -n "$APP_ID" ]]; then
  run_pipeline "$APP_ID" "$MODEL"
  exit 0
fi

echo "== batch mode: discovering unprocessed duplex candidates =="
mapfile -t CANDIDATES < <(list_duplex_candidates)

if [[ "${#CANDIDATES[@]}" -eq 0 ]]; then
  echo "no duplex candidates found (every match already has a da_summaries row)."
  exit 0
fi

echo "found ${#CANDIDATES[@]} candidate(s):"
for app in "${CANDIDATES[@]}"; do
  echo "  · $app"
done
echo

ok=0
fail=0
for app in "${CANDIDATES[@]}"; do
  if run_pipeline "$app" "$MODEL"; then
    ok=$((ok + 1))
  else
    fail=$((fail + 1))
    echo "!! pipeline failed for $app — continuing batch"
  fi
done

echo
echo "=================================================================="
echo "  batch complete: $ok ok, $fail failed (of ${#CANDIDATES[@]})"
echo "=================================================================="
