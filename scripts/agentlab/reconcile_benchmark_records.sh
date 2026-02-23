#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required"
  exit 1
fi

RUN_DIR="${1:-}"
if [[ -z "${RUN_DIR}" ]]; then
  RUN_DIR="$(find .lab/runs -mindepth 1 -maxdepth 1 -type d -name 'run_*' -print0 2>/dev/null | xargs -0 ls -td 2>/dev/null | head -n 1 || true)"
fi
if [[ -z "${RUN_DIR}" || ! -d "${RUN_DIR}" ]]; then
  echo "run directory not found"
  exit 1
fi

TRIALS_DIR="${RUN_DIR}/trials"
if [[ ! -d "${TRIALS_DIR}" ]]; then
  echo "missing trials dir: ${TRIALS_DIR}"
  exit 1
fi

BENCHMARK_DIR="${RUN_DIR}/benchmark"
mkdir -p "${BENCHMARK_DIR}"
PREDICTIONS_OUT="${BENCHMARK_DIR}/predictions.jsonl"
SCORES_OUT="${BENCHMARK_DIR}/scores.jsonl"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

collect_kind() {
  local label="$1"
  local filename="$2"
  local out_path="$3"
  local tmp_rows="${TMP_DIR}/${label}.rows.tsv"

  : > "${tmp_rows}"
  local -a missing_trials=()
  local -a trial_dirs=()
  mapfile -t trial_dirs < <(find "${TRIALS_DIR}" -mindepth 1 -maxdepth 1 -type d -name 'trial_*' | sort)

  if [[ "${#trial_dirs[@]}" -eq 0 ]]; then
    echo "no trial directories found under ${TRIALS_DIR}"
    return 1
  fi

  local trial_dir
  for trial_dir in "${trial_dirs[@]}"; do
    local candidate="${trial_dir}/out/${filename}"
    if [[ ! -f "${candidate}" ]]; then
      missing_trials+=("${trial_dir}")
      continue
    fi
    local fallback_trial_id
    fallback_trial_id="$(basename "${trial_dir}")"
    local raw
    raw="$(jq -rc --arg fallback "${fallback_trial_id}" '
      {
        trial_id: (.ids.trial_id // $fallback),
        schedule_index: (
          .ids.schedule_index
          // .ids.slot_index
          // .ext.schedule_index
          // .ext.slot_index
          // null
        ),
        record: .
      }
    ' "${candidate}")"

    local trial_id schedule_tag schedule_value record_json
    trial_id="$(jq -r '.trial_id // empty' <<< "${raw}")"
    if [[ -z "${trial_id}" ]]; then
      trial_id="${fallback_trial_id}"
    fi
    schedule_value="$(jq -r '.schedule_index // empty' <<< "${raw}")"
    if [[ "${schedule_value}" =~ ^[0-9]+$ ]]; then
      schedule_tag="0"
      schedule_value="$(printf '%020d' "${schedule_value}")"
    else
      schedule_tag="1"
      schedule_value="${trial_id}"
    fi
    record_json="$(jq -c '.record' <<< "${raw}")"
    printf '%s\t%s\t%s\t%s\n' "${schedule_tag}" "${schedule_value}" "${trial_id}" "${record_json}" >> "${tmp_rows}"
  done

  if [[ "${#missing_trials[@]}" -gt 0 ]]; then
    echo "missing ${label} files:"
    printf '  %s\n' "${missing_trials[@]}"
    return 1
  fi

  if [[ ! -s "${tmp_rows}" ]]; then
    echo "no ${label} files found"
    return 1
  fi

  local dupes
  dupes="$(cut -f3 "${tmp_rows}" | sort | uniq -d)"
  if [[ -n "${dupes}" ]]; then
    echo "duplicate trial IDs detected for ${label}:"
    printf '%s\n' "${dupes}"
    return 1
  fi

  LC_ALL=C sort -t $'\t' -k1,1 -k2,2 -k3,3 "${tmp_rows}" | cut -f4- > "${out_path}"
  echo "wrote ${label} records: ${out_path} ($(wc -l < "${out_path}") lines)"
}

collect_kind "prediction" "benchmark_prediction.json" "${PREDICTIONS_OUT}"
collect_kind "score" "benchmark_score.json" "${SCORES_OUT}"

echo "reconciled benchmark files for ${RUN_DIR}"
