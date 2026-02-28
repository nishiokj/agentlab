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

EXPERIMENT_PATH="${AGENTLAB_EXPERIMENT_PATH:-.lab/experiments/terminal_bench2_harbor.yaml}"
DATASET_PATH="${HARBOR_AGENTLAB_DATASET_PATH:-.lab/experiments/data/terminal_bench2_harbor.task_boundary_v2.jsonl}"
HARBOR_TASKS_ROOT="${HARBOR_TASKS_ROOT:-}"
HARBOR_DATASET_REGISTRY="${HARBOR_DATASET_REGISTRY:-}"
HARBOR_DATASET_REGISTRY_ROOT="${HARBOR_DATASET_REGISTRY_ROOT:-}"
HARBOR_REQUIRE_TASK_IMAGE="${HARBOR_REQUIRE_TASK_IMAGE:-0}"
HARBOR_DEFAULT_TASK_IMAGE="${HARBOR_DEFAULT_TASK_IMAGE:-}"
HARBOR_DEFAULT_TASK_WORKSPACE="${HARBOR_DEFAULT_TASK_WORKSPACE:-}"
export HARBOR_EVALUATOR_CMD="${HARBOR_EVALUATOR_CMD:-}"
export HARBOR_EVALUATOR_CMD_JSON="${HARBOR_EVALUATOR_CMD_JSON:-}"

build_dataset=0
if [[ -n "${HARBOR_TASKS_ROOT}" || -n "${HARBOR_DATASET_REGISTRY}" ]]; then
  build_dataset=1
fi

if [[ "${build_dataset}" -eq 1 ]]; then
  EXPORT_CMD=(
    "${PYTHON_BIN}"
    adapters/harbor/export_harbor_to_agentlab_jsonl.py
    --output
    "${DATASET_PATH}"
  )
  if [[ -n "${HARBOR_TASKS_ROOT}" ]]; then
    EXPORT_CMD+=(--tasks-root "${HARBOR_TASKS_ROOT}")
  fi
  if [[ -n "${HARBOR_DATASET_REGISTRY}" ]]; then
    EXPORT_CMD+=(--registry-json "${HARBOR_DATASET_REGISTRY}")
    if [[ -n "${HARBOR_DATASET_REGISTRY_ROOT}" ]]; then
      EXPORT_CMD+=(--registry-root "${HARBOR_DATASET_REGISTRY_ROOT}")
    fi
  fi
  if [[ -n "${AGENTLAB_LIMIT:-}" ]]; then
    EXPORT_CMD+=(--limit "${AGENTLAB_LIMIT}")
  fi
  if [[ "${HARBOR_REQUIRE_TASK_IMAGE}" == "1" ]]; then
    EXPORT_CMD+=(--require-task-image)
  fi
  if [[ -n "${HARBOR_DEFAULT_TASK_IMAGE}" ]]; then
    EXPORT_CMD+=(--default-task-image "${HARBOR_DEFAULT_TASK_IMAGE}")
  fi
  if [[ -n "${HARBOR_DEFAULT_TASK_WORKSPACE}" ]]; then
    EXPORT_CMD+=(--default-task-workspace "${HARBOR_DEFAULT_TASK_WORKSPACE}")
  fi
  echo "building Harbor dataset: ${EXPORT_CMD[*]}"
  "${EXPORT_CMD[@]}"
fi

if [[ ! -f "${DATASET_PATH}" ]]; then
  echo "missing mapped dataset: ${DATASET_PATH}"
  echo "set HARBOR_TASKS_ROOT or HARBOR_DATASET_REGISTRY to auto-build before run"
  exit 1
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

echo "run command: ${RUN_CMD[*]}"
"${RUN_CMD[@]}"
