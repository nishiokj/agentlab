#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "python interpreter not found (tried python3, python)"
    exit 1
  fi
fi

EXPERIMENT_PATH="${BENCH_EXPERIMENT_PATH:-.lab/experiments/bench_v0_per_task.yaml}"
DATASET_PATH="${BENCH_DATASET_PATH:-.lab/experiments/data/bench_v0.task_boundary_v2.jsonl}"
SUITE="${BENCH_SUITE:-v0}"
DATASET_LIMIT="${BENCH_DATASET_LIMIT:-1}"
DEFAULT_TASK_IMAGE="${BENCH_DEFAULT_TASK_IMAGE:-}"
DEFAULT_TASK_WORKSPACE="${BENCH_DEFAULT_TASK_WORKSPACE:-/agentlab/workspace}"
AGENT_ARTIFACT="${BENCH_AGENT_ARTIFACT:-.lab/agents/agent-runtime.tar.gz}"
RUN_EXECUTOR="${AGENTLAB_EXECUTOR:-local_docker}"
RUN_SMOKE="${BENCH_SMOKE_RUN:-1}"

if [[ -z "${DEFAULT_TASK_IMAGE}" ]]; then
  echo "missing BENCH_DEFAULT_TASK_IMAGE (required for bench v0 hard cutover export)"
  exit 1
fi

if [[ ! -f "${AGENT_ARTIFACT}" ]]; then
  echo "missing BENCH_AGENT_ARTIFACT: ${AGENT_ARTIFACT}"
  exit 1
fi

echo "building bench v0 task_boundary_v2 dataset: ${DATASET_PATH}"
"${PYTHON_BIN}" bench/integration/agentlab/export_bench_suite_to_jsonl.py \
  --suite "${SUITE}" \
  --output "${DATASET_PATH}" \
  --default-task-image "${DEFAULT_TASK_IMAGE}" \
  --require-task-image \
  --default-task-workspace "${DEFAULT_TASK_WORKSPACE}" \
  --limit "${DATASET_LIMIT}"

TMP_EXP="$(mktemp ".lab/experiments/_tmp_bench_v0_per_task.XXXXXX.yaml")"
trap 'rm -f "${TMP_EXP}"' EXIT

DATASET_ABS="$(cd "$(dirname "${DATASET_PATH}")" && pwd)/$(basename "${DATASET_PATH}")"
ARTIFACT_ABS="$(cd "$(dirname "${AGENT_ARTIFACT}")" && pwd)/$(basename "${AGENT_ARTIFACT}")"

sed \
  -e "s|^  path: .*|  path: ${DATASET_ABS}|" \
  -e "s|^  limit: .*|  limit: ${DATASET_LIMIT}|" \
  -e "s|^    artifact: .*|    artifact: ${ARTIFACT_ABS}|" \
  "${EXPERIMENT_PATH}" > "${TMP_EXP}"

if command -v lab-cli >/dev/null 2>&1; then
  PREFLIGHT_CMD=(lab-cli preflight "${TMP_EXP}")
  RUN_CMD=(lab-cli run "${TMP_EXP}" --executor "${RUN_EXECUTOR}")
elif [[ -x rust/target/release/lab-cli ]]; then
  PREFLIGHT_CMD=(rust/target/release/lab-cli preflight "${TMP_EXP}")
  RUN_CMD=(rust/target/release/lab-cli run "${TMP_EXP}" --executor "${RUN_EXECUTOR}")
else
  PREFLIGHT_CMD=(cargo run --manifest-path rust/Cargo.toml -p lab-cli -- preflight "${TMP_EXP}")
  RUN_CMD=(cargo run --manifest-path rust/Cargo.toml -p lab-cli -- run "${TMP_EXP}" --executor "${RUN_EXECUTOR}")
fi

echo "preflight command: ${PREFLIGHT_CMD[*]}"
"${PREFLIGHT_CMD[@]}"

if [[ "${RUN_SMOKE}" == "1" ]]; then
  echo "smoke run command: ${RUN_CMD[*]}"
  "${RUN_CMD[@]}"
else
  echo "skipping smoke run (set BENCH_SMOKE_RUN=1 to execute)"
fi
