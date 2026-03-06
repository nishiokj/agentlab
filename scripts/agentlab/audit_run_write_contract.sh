#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/agentlab/audit_run_write_contract.sh <run_dir> [output_md]

Description:
  Audits an experiment run directory against the runner write contract.
  Produces a markdown report with:
  - required/optional path presence
  - discovered schema_version values in JSON/JSONL files
  - SQLite table and runtime_kv key checks
  - legacy/compat footprint signals (facts/, run-level evidence/)
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required" >&2
  exit 1
fi

RUN_DIR_INPUT="$1"
OUTPUT_MD="${2:-}"

if [[ ! -d "$RUN_DIR_INPUT" ]]; then
  echo "error: run directory not found: $RUN_DIR_INPUT" >&2
  exit 1
fi

RUN_DIR="$(cd "$RUN_DIR_INPUT" && pwd)"
RUN_NAME="$(basename "$RUN_DIR")"
RUN_DB="$RUN_DIR/run.sqlite"
TS_UTC="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

if [[ -n "$OUTPUT_MD" ]]; then
  OUTPUT_MD_ABS="$(cd "$(dirname "$OUTPUT_MD")" && pwd)/$(basename "$OUTPUT_MD")"
else
  OUTPUT_MD_ABS="$RUN_DIR/write_contract_audit.md"
fi

readonly REQUIRED_PATHS=(
  "manifest.json"
  "resolved_experiment.json"
  "resolved_experiment.digest"
  "resolved_variants.json"
  "resolved_schedule.json"
  "run.sqlite"
)

readonly OPTIONAL_PATHS=(
  "trials"
  "artifacts"
  "benchmark/predictions.jsonl"
  "benchmark/scores.jsonl"
  "benchmark/adapter_manifest.json"
  "benchmark/summary.json"
  "runtime/operation_lease.json"
  "runtime/recovery_report.json"
  ".scratch"
  "runtime/worker_payload"
)

readonly LEGACY_COMPAT_PATHS=(
  "facts/run_manifest.json"
  "facts/trials.jsonl"
  "facts/metrics_long.jsonl"
  "facts/events.jsonl"
  "facts/variant_snapshots.jsonl"
  "evidence/evidence_records.jsonl"
  "evidence/task_chain_states.jsonl"
  "runtime/run_control.json"
  "runtime/run_session_state.json"
  "runtime/schedule_progress.json"
  "runtime/parallel_worker_control.json"
  "runtime/engine_lease.json"
)

readonly EXPECTED_SQLITE_TABLES=(
  "attempt_objects"
  "benchmark_prediction_rows"
  "benchmark_score_rows"
  "chain_state_rows"
  "event_rows"
  "evidence_rows"
  "lineage_heads"
  "lineage_versions"
  "metric_rows"
  "pending_trial_completions"
  "run_manifests"
  "runtime_kv"
  "runtime_ops"
  "slot_commit_records"
  "trial_rows"
  "variant_snapshot_rows"
)

readonly EXPECTED_RUNTIME_KEYS=(
  "engine_lease_v1"
  "run_control_v2"
  "run_session_state_v1"
  "schedule_progress_v2"
)

readonly OPTIONAL_RUNTIME_KEYS=(
  "parallel_worker_control_v1"
)

readonly KNOWN_RUN_SCHEMA_VERSIONS=(
  "agent_result_v1"
  "agent_task_v1"
  "agent_artifact_v1"
  "benchmark_adapter_manifest_v1"
  "benchmark_prediction_record_v1"
  "benchmark_score_record_v1"
  "benchmark_summary_v1"
  "benchmark_trial_preflight_v1"
  "control_plane_v1"
  "engine_lease_v1"
  "evidence_record_v1"
  "fork_manifest_v1"
  "hook_events_v1"
  "manifest_v1"
  "operation_lease_v1"
  "parallel_worker_control_v1"
  "pending_trial_completion_v1"
  "recovery_report_v1"
  "replay_manifest_v1"
  "resolved_schedule_v1"
  "resolved_variants_v1"
  "run_control_v2"
  "run_manifest_v1"
  "run_session_state_v1"
  "schedule_progress_v2"
  "state_inventory_v1"
  "task_chain_state_v1"
  "task_boundary_v2"
  "trace_manifest_v1"
  "trial_metadata_v1"
  "trial_state_v1"
  "workspace_bundle_v1"
  "workspace_diff_v1"
  "workspace_patch_v1"
  "workspace_seed_pack_v1"
  "workspace_snapshot_v1"
)

join_by() {
  local IFS="$1"
  shift
  echo "$*"
}

tmpdir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

present_file="$tmpdir/present_paths.txt"
missing_file="$tmpdir/missing_paths.txt"
optional_present_file="$tmpdir/optional_present_paths.txt"
legacy_present_file="$tmpdir/legacy_present_paths.txt"
schemas_found_file="$tmpdir/schemas_found.tsv"
schemas_unique_file="$tmpdir/schemas_unique.txt"
schemas_unknown_file="$tmpdir/schemas_unknown.txt"
sqlite_tables_file="$tmpdir/sqlite_tables.txt"
sqlite_missing_tables_file="$tmpdir/sqlite_missing_tables.txt"
sqlite_extra_tables_file="$tmpdir/sqlite_extra_tables.txt"
runtime_keys_file="$tmpdir/runtime_keys.txt"
runtime_missing_keys_file="$tmpdir/runtime_missing_keys.txt"
runtime_optional_keys_present_file="$tmpdir/runtime_optional_keys_present.txt"
runtime_optional_keys_missing_file="$tmpdir/runtime_optional_keys_missing.txt"
runtime_extra_keys_file="$tmpdir/runtime_extra_keys.txt"
table_counts_file="$tmpdir/table_counts.tsv"
top_tree_file="$tmpdir/top_tree.txt"

touch "$present_file" "$missing_file" "$optional_present_file" "$legacy_present_file"
touch "$schemas_found_file" "$schemas_unique_file" "$schemas_unknown_file"
touch "$sqlite_tables_file" "$sqlite_missing_tables_file" "$sqlite_extra_tables_file"
touch "$runtime_keys_file" "$runtime_missing_keys_file"
touch "$runtime_optional_keys_present_file" "$runtime_optional_keys_missing_file"
touch "$runtime_extra_keys_file" "$table_counts_file"

for rel in "${REQUIRED_PATHS[@]}"; do
  if [[ -e "$RUN_DIR/$rel" ]]; then
    echo "$rel" >> "$present_file"
  else
    echo "$rel" >> "$missing_file"
  fi
done

for rel in "${OPTIONAL_PATHS[@]}"; do
  if [[ -e "$RUN_DIR/$rel" ]]; then
    echo "$rel" >> "$optional_present_file"
  fi
done

for rel in "${LEGACY_COMPAT_PATHS[@]}"; do
  if [[ -e "$RUN_DIR/$rel" ]]; then
    echo "$rel" >> "$legacy_present_file"
  fi
done

find "$RUN_DIR" -mindepth 1 -maxdepth 3 | sed "s|^$RUN_DIR/||" | sort > "$top_tree_file"

while IFS= read -r -d '' file; do
  rel="${file#$RUN_DIR/}"
  case "$file" in
    *.json)
      ver="$(jq -r 'try .schema_version // empty' "$file" 2>/dev/null || true)"
      if [[ -n "$ver" ]]; then
        printf "%s\t%s\n" "$rel" "$ver" >> "$schemas_found_file"
      fi
      ;;
    *.jsonl)
      jq -Rr 'fromjson? | .schema_version? // empty' "$file" 2>/dev/null \
        | awk -v p="$rel" 'NF { print p "\t" $0 }' >> "$schemas_found_file"
      ;;
  esac
done < <(find "$RUN_DIR" -type f \( -name '*.json' -o -name '*.jsonl' \) -print0)

sort -u "$schemas_found_file" -o "$schemas_found_file"
cut -f2 "$schemas_found_file" | sort -u > "$schemas_unique_file"

known_versions_csv="$(join_by "," "${KNOWN_RUN_SCHEMA_VERSIONS[@]}")"
jq -Rn --arg known "$known_versions_csv" '
  ($known | split(",") | map(select(length>0))) as $knownSet
  | inputs
  | select(. != "")
  | select((. as $v | $knownSet | index($v)) == null)
' "$schemas_unique_file" > "$schemas_unknown_file"

if [[ -f "$RUN_DB" ]] && command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "$RUN_DB" "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;" > "$sqlite_tables_file"

  expected_tables_csv="$(join_by "," "${EXPECTED_SQLITE_TABLES[@]}")"
  jq -Rn --arg expected "$expected_tables_csv" '
    ($expected | split(",") | map(select(length>0))) as $expectedSet
    | inputs
    | select(. != "")
    | select((. as $t | $expectedSet | index($t)) == null)
  ' "$sqlite_tables_file" > "$sqlite_extra_tables_file"

  for t in "${EXPECTED_SQLITE_TABLES[@]}"; do
    if ! grep -Fxq "$t" "$sqlite_tables_file"; then
      echo "$t" >> "$sqlite_missing_tables_file"
    fi
  done

  for t in "${EXPECTED_SQLITE_TABLES[@]}"; do
    count="$(sqlite3 "$RUN_DB" "SELECT COUNT(*) FROM \"$t\";" 2>/dev/null || echo "ERR")"
    printf "%s\t%s\n" "$t" "$count" >> "$table_counts_file"
  done

  sqlite3 "$RUN_DB" "SELECT key FROM runtime_kv ORDER BY key;" > "$runtime_keys_file"

  for k in "${EXPECTED_RUNTIME_KEYS[@]}"; do
    if ! grep -Fxq "$k" "$runtime_keys_file"; then
      echo "$k" >> "$runtime_missing_keys_file"
    fi
  done

  for k in "${OPTIONAL_RUNTIME_KEYS[@]}"; do
    if grep -Fxq "$k" "$runtime_keys_file"; then
      echo "$k" >> "$runtime_optional_keys_present_file"
    else
      echo "$k" >> "$runtime_optional_keys_missing_file"
    fi
  done

  expected_keys_csv="$(join_by "," "${EXPECTED_RUNTIME_KEYS[@]}" "${OPTIONAL_RUNTIME_KEYS[@]}")"
  jq -Rn --arg expected "$expected_keys_csv" '
    ($expected | split(",") | map(select(length>0))) as $expectedSet
    | inputs
    | select(. != "")
    | select((. as $k | $expectedSet | index($k)) == null)
  ' "$runtime_keys_file" > "$runtime_extra_keys_file"
fi

{
  echo "# Run Write Contract Audit"
  echo
  echo "- generated_at_utc: $TS_UTC"
  echo "- run_dir: $RUN_DIR"
  echo "- run_name: $RUN_NAME"
  echo
  echo "## 1) Required Paths"
  if [[ -s "$present_file" ]]; then
    echo
    echo "Present:"
    sed 's/^/- /' "$present_file"
  fi
  if [[ -s "$missing_file" ]]; then
    echo
    echo "Missing:"
    sed 's/^/- /' "$missing_file"
  fi
  if [[ ! -s "$missing_file" ]]; then
    echo
    echo "- All required paths are present."
  fi
  echo
  echo "## 2) Optional/Transient Paths Present"
  if [[ -s "$optional_present_file" ]]; then
    sed 's/^/- /' "$optional_present_file"
  else
    echo "- none"
  fi
  echo
  echo "## 3) Legacy/Compat Paths Present"
  if [[ -s "$legacy_present_file" ]]; then
    sed 's/^/- /' "$legacy_present_file"
  else
    echo "- none"
  fi
  echo
  echo "## 4) Discovered schema_version Values"
  if [[ -s "$schemas_found_file" ]]; then
    echo
    echo "| File | schema_version |"
    echo "|---|---|"
    awk -F'\t' '{ printf("| %s | %s |\n", $1, $2); }' "$schemas_found_file"
  else
    echo "- none detected"
  fi
  echo
  echo "Unique schema versions:"
  if [[ -s "$schemas_unique_file" ]]; then
    sed 's/^/- /' "$schemas_unique_file"
  else
    echo "- none"
  fi
  echo
  echo "Unknown schema versions (not in known run contract):"
  if [[ -s "$schemas_unknown_file" ]]; then
    sed 's/^/- /' "$schemas_unknown_file"
  else
    echo "- none"
  fi
  echo
  echo "## 5) SQLite Contract"
  if [[ ! -f "$RUN_DB" ]]; then
    echo "- run.sqlite not found"
  elif ! command -v sqlite3 >/dev/null 2>&1; then
    echo "- sqlite3 unavailable; table checks skipped"
  else
    echo
    echo "Tables present:"
    if [[ -s "$sqlite_tables_file" ]]; then
      sed 's/^/- /' "$sqlite_tables_file"
    else
      echo "- none"
    fi
    echo
    echo "Missing expected tables:"
    if [[ -s "$sqlite_missing_tables_file" ]]; then
      sed 's/^/- /' "$sqlite_missing_tables_file"
    else
      echo "- none"
    fi
    echo
    echo "Extra tables (not in expected list):"
    if [[ -s "$sqlite_extra_tables_file" ]]; then
      sed 's/^/- /' "$sqlite_extra_tables_file"
    else
      echo "- none"
    fi
    echo
    echo "runtime_kv keys:"
    if [[ -s "$runtime_keys_file" ]]; then
      sed 's/^/- /' "$runtime_keys_file"
    else
      echo "- none"
    fi
    echo
    echo "Missing expected runtime_kv keys:"
    if [[ -s "$runtime_missing_keys_file" ]]; then
      sed 's/^/- /' "$runtime_missing_keys_file"
    else
      echo "- none"
    fi
    echo
    echo "Optional runtime_kv keys present:"
    if [[ -s "$runtime_optional_keys_present_file" ]]; then
      sed 's/^/- /' "$runtime_optional_keys_present_file"
    else
      echo "- none"
    fi
    echo
    echo "Optional runtime_kv keys missing:"
    if [[ -s "$runtime_optional_keys_missing_file" ]]; then
      sed 's/^/- /' "$runtime_optional_keys_missing_file"
    else
      echo "- none"
    fi
    echo
    echo "Extra runtime_kv keys:"
    if [[ -s "$runtime_extra_keys_file" ]]; then
      sed 's/^/- /' "$runtime_extra_keys_file"
    else
      echo "- none"
    fi
    echo
    echo "Table row counts:"
    echo
    echo "| table | rows |"
    echo "|---|---:|"
    if [[ -s "$table_counts_file" ]]; then
      awk -F'\t' '{ printf("| %s | %s |\n", $1, $2); }' "$table_counts_file"
    fi
  fi
  echo
  echo "## 6) Run Footprint (Depth <= 3)"
  if [[ -s "$top_tree_file" ]]; then
    echo
    echo '```text'
    cat "$top_tree_file"
    echo '```'
  else
    echo "- no files found under run dir"
  fi
} > "$OUTPUT_MD_ABS"

echo "$OUTPUT_MD_ABS"
