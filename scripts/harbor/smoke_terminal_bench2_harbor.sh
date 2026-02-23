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

TASKS_ROOT="${HARBOR_SMOKE_TASKS_ROOT:-scripts/harbor/fixtures/tb2_smoke_task}"
SMOKE_DATASET="${HARBOR_SMOKE_DATASET:-.lab/experiments/data/terminal_bench2_harbor_smoke.task_boundary_v2.jsonl}"
SMOKE_EXPERIMENT="${HARBOR_SMOKE_EXPERIMENT:-.lab/experiments/terminal_bench2_harbor_smoke.yaml}"
PER_TASK_EXPERIMENT="${HARBOR_PER_TASK_EXPERIMENT:-.lab/experiments/terminal_bench2_harbor_per_task.yaml}"
DEFAULT_TASK_IMAGE="${HARBOR_DEFAULT_TASK_IMAGE:-python:3.11-slim}"
RUN_SMOKE="${HARBOR_SMOKE_RUN:-0}"
RUN_PER_TASK="${HARBOR_SMOKE_RUN_PER_TASK:-0}"
PER_TASK_ARTIFACT="${HARBOR_AGENT_ARTIFACT:-.lab/agents/agent-runtime.tar.gz}"
export HARBOR_EVALUATOR_CMD="${HARBOR_EVALUATOR_CMD:-}"
export HARBOR_EVALUATOR_CMD_JSON="${HARBOR_EVALUATOR_CMD_JSON:-}"

echo "building smoke dataset: ${SMOKE_DATASET}"
"${PYTHON_BIN}" scripts/harbor/export_harbor_to_agentlab_jsonl.py \
  --tasks-root "${TASKS_ROOT}" \
  --output "${SMOKE_DATASET}" \
  --require-task-image \
  --default-task-image "${DEFAULT_TASK_IMAGE}" \
  --limit 1

TMP_PER_TASK_EXP="$(mktemp ".lab/experiments/_tmp_terminal_bench2_harbor_per_task.XXXXXX.yaml")"
trap 'rm -f "${TMP_PER_TASK_EXP}"' EXIT
sed \
  -e "s|path: .*|path: data/terminal_bench2_harbor_smoke.task_boundary_v2.jsonl|" \
  "${PER_TASK_EXPERIMENT}" > "${TMP_PER_TASK_EXP}"

if command -v lab-cli >/dev/null 2>&1; then
  DESCRIBE_CMD=(lab-cli describe "${SMOKE_EXPERIMENT}")
  DESCRIBE_PER_TASK_CMD=(lab-cli describe "${TMP_PER_TASK_EXP}")
  RUN_CMD=(lab-cli run "${SMOKE_EXPERIMENT}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
  RUN_PER_TASK_CMD=(lab-cli run "${TMP_PER_TASK_EXP}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
elif [[ -x rust/target/release/lab-cli ]]; then
  DESCRIBE_CMD=(rust/target/release/lab-cli describe "${SMOKE_EXPERIMENT}")
  DESCRIBE_PER_TASK_CMD=(rust/target/release/lab-cli describe "${TMP_PER_TASK_EXP}")
  RUN_CMD=(rust/target/release/lab-cli run "${SMOKE_EXPERIMENT}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
  RUN_PER_TASK_CMD=(rust/target/release/lab-cli run "${TMP_PER_TASK_EXP}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
else
  DESCRIBE_CMD=(cargo run --manifest-path rust/Cargo.toml -p lab-cli -- describe "${SMOKE_EXPERIMENT}")
  DESCRIBE_PER_TASK_CMD=(cargo run --manifest-path rust/Cargo.toml -p lab-cli -- describe "${TMP_PER_TASK_EXP}")
  RUN_CMD=(cargo run --manifest-path rust/Cargo.toml -p lab-cli -- run "${SMOKE_EXPERIMENT}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
  RUN_PER_TASK_CMD=(cargo run --manifest-path rust/Cargo.toml -p lab-cli -- run "${TMP_PER_TASK_EXP}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
fi

echo "describing smoke experiment: ${DESCRIBE_CMD[*]}"
"${DESCRIBE_CMD[@]}"

echo "describing per-task experiment: ${DESCRIBE_PER_TASK_CMD[*]}"
"${DESCRIBE_PER_TASK_CMD[@]}"

if [[ "${RUN_SMOKE}" == "1" ]]; then
  echo "running smoke experiment: ${RUN_CMD[*]}"
  "${RUN_CMD[@]}"
else
  echo "skipping smoke run (set HARBOR_SMOKE_RUN=1 to execute)"
fi

if [[ "${RUN_PER_TASK}" == "1" ]]; then
  if [[ ! -f "${PER_TASK_ARTIFACT}" ]]; then
    echo "missing per-task artifact: ${PER_TASK_ARTIFACT}"
    exit 1
  fi
  sed -i.bak "s|artifact: .*|artifact: ${PER_TASK_ARTIFACT}|" "${TMP_PER_TASK_EXP}"
  rm -f "${TMP_PER_TASK_EXP}.bak"

  if command -v lab-cli >/dev/null 2>&1; then
    RUN_PER_TASK_CMD=(lab-cli run "${TMP_PER_TASK_EXP}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
  elif [[ -x rust/target/release/lab-cli ]]; then
    RUN_PER_TASK_CMD=(rust/target/release/lab-cli run "${TMP_PER_TASK_EXP}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
  else
    RUN_PER_TASK_CMD=(cargo run --manifest-path rust/Cargo.toml -p lab-cli -- run "${TMP_PER_TASK_EXP}" --executor "${AGENTLAB_EXECUTOR:-local_docker}")
  fi

  echo "running per-task experiment: ${RUN_PER_TASK_CMD[*]}"
  "${RUN_PER_TASK_CMD[@]}"
else
  echo "skipping per-task run (set HARBOR_SMOKE_RUN_PER_TASK=1 to execute)"
fi
