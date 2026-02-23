#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

EXPERIMENT_PATH="${AGENTLAB_EXPERIMENT_PATH:-.lab/experiments/swebench_lite_curated.yaml}"
LOG_DIR="${AGENTLAB_RUN_LOG_DIR:-.lab/logs/curated_runs}"
mkdir -p "${LOG_DIR}"
RUN_LOG="${AGENTLAB_RUN_LOG:-${LOG_DIR}/run_curated_$(date +%Y%m%d_%H%M%S).log}"
PRE_EXISTING_RUNS_FILE="$(mktemp)"
find .lab/runs -mindepth 1 -maxdepth 1 -type d -name 'run_*' -print 2>/dev/null | sort > "${PRE_EXISTING_RUNS_FILE}" || true
trap 'rm -f "${PRE_EXISTING_RUNS_FILE}"' EXIT

if [[ -n "${AGENTLAB_LIMIT:-}" ]]; then
  node scripts/run-swebench-lite-experiment.mjs \
    --write-only \
    --experiment "${EXPERIMENT_PATH}" \
    --limit "${AGENTLAB_LIMIT}"
fi

if [[ "$#" -gt 0 ]]; then
  RUN_CMD=("$@")
else
  if command -v lab-cli >/dev/null 2>&1; then
    RUN_CMD=(lab-cli run "${EXPERIMENT_PATH}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
  elif [[ -x rust/target/release/lab-cli ]]; then
    RUN_CMD=(rust/target/release/lab-cli run "${EXPERIMENT_PATH}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
  else
    RUN_CMD=(cargo run --manifest-path rust/Cargo.toml -p lab-cli -- run "${EXPERIMENT_PATH}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
  fi
fi

echo "run log: ${RUN_LOG}"
echo "run command: ${RUN_CMD[*]}"

set +e
"${RUN_CMD[@]}" 2>&1 | tee "${RUN_LOG}"
RUN_STATUS=${PIPESTATUS[0]}
set -e

resolve_run_dir() {
  if [[ -n "${AGENTLAB_RUN_DIR:-}" ]]; then
    echo "${AGENTLAB_RUN_DIR}"
    return 0
  fi

  local run_id
  run_id="$(grep -Eo 'run_[a-zA-Z0-9_]+' "${RUN_LOG}" | tail -n 1 || true)"
  if [[ -n "${run_id}" && -d ".lab/runs/${run_id}" ]]; then
    echo ".lab/runs/${run_id}"
    return 0
  fi

  local -a new_runs=()
  while IFS= read -r candidate; do
    if ! grep -Fqx "${candidate}" "${PRE_EXISTING_RUNS_FILE}"; then
      new_runs+=("${candidate}")
    fi
  done < <(find .lab/runs -mindepth 1 -maxdepth 1 -type d -name 'run_*' -print | sort)
  if [[ "${#new_runs[@]}" -gt 0 ]]; then
    printf '%s\n' "${new_runs[@]}" | xargs ls -td 2>/dev/null | head -n 1
    return 0
  fi

  local newest
  newest="$(find .lab/runs -mindepth 1 -maxdepth 1 -type d -name 'run_*' -print0 2>/dev/null | xargs -0 ls -td 2>/dev/null | head -n 1 || true)"
  if [[ -n "${newest}" ]]; then
    echo "${newest}"
    return 0
  fi
  return 1
}

if [[ "${RUN_STATUS}" -ne 0 ]]; then
  echo "lab-cli exited with non-zero status: ${RUN_STATUS}"
  echo "logs preserved at ${RUN_LOG}"
  exit "${RUN_STATUS}"
fi

RUN_DIR="$(resolve_run_dir || true)"
if [[ -z "${RUN_DIR}" ]]; then
  echo "unable to resolve run directory after successful lab-cli exit"
  exit 1
fi

echo "run directory: ${RUN_DIR}"
RUN_CONTROL="${RUN_DIR}/runtime/run_control.json"
if [[ ! -f "${RUN_CONTROL}" ]]; then
  echo "missing run control file: ${RUN_CONTROL}"
  echo "diagnostics: listing runtime files"
  find "${RUN_DIR}" -maxdepth 3 -type f | sort
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for run-control status checks"
  exit 1
fi

RUN_STATE="$(jq -r '.status // empty' "${RUN_CONTROL}")"
echo "run status from run_control.json: ${RUN_STATE:-<empty>}"

if [[ "${RUN_STATE}" == "running" ]]; then
  echo "run_control.json still reports status=running after lab-cli exit"
  echo "diagnostics:"
  jq -C . "${RUN_CONTROL}" || cat "${RUN_CONTROL}"
  exit 1
fi

case "${RUN_STATE}" in
  completed|failed|cancelled|canceled)
    ;;
  *)
    echo "non-terminal or unknown run state: ${RUN_STATE:-<empty>}"
    jq -C . "${RUN_CONTROL}" || cat "${RUN_CONTROL}"
    exit 1
    ;;
esac

echo "run finished with terminal status ${RUN_STATE}; logs preserved at ${RUN_LOG}"
