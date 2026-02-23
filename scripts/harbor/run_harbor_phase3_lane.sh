#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

LANE="${1:-}"
if [[ "${LANE}" != "pinned" && "${LANE}" != "canary" ]]; then
  echo "usage: scripts/harbor/run_harbor_phase3_lane.sh [pinned|canary]"
  exit 2
fi

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "python interpreter not found (tried python3, python)"
    exit 2
  fi
fi

REQ_FILE="${HARBOR_REQUIREMENTS_FILE:-scripts/harbor/requirements-harbor-${LANE}.txt}"
ENFORCE_DEP_SPECS="${HARBOR_ENFORCE_DEP_SPECS:-1}"
REQUIRE_EVALUATOR_CMD="${HARBOR_REQUIRE_EVALUATOR_CMD:-1}"
RUN_SMOKE_DESCRIBE="${HARBOR_RUN_SMOKE_DESCRIBE:-0}"
SKIP_PIP_BOOTSTRAP="${HARBOR_SKIP_PIP_BOOTSTRAP:-0}"
PIP_SPECS_RAW="${HARBOR_PIP_SPECS:-}"

VENV_DIR="$(mktemp -d "${TMPDIR:-/tmp}/harbor_phase3_${LANE}.XXXXXX")"
cleanup() {
  rm -rf "${VENV_DIR}"
}
trap cleanup EXIT

VENV_CREATE_CMD=("${PYTHON_BIN}" -m venv "${VENV_DIR}")
echo "creating venv for lane '${LANE}': ${VENV_CREATE_CMD[*]}"
"${VENV_CREATE_CMD[@]}"

VENV_PY="${VENV_DIR}/bin/python"
VENV_PIP="${VENV_DIR}/bin/pip"

if [[ "${SKIP_PIP_BOOTSTRAP}" == "1" ]]; then
  echo "skipping pip/setuptools/wheel upgrade (HARBOR_SKIP_PIP_BOOTSTRAP=1)"
else
  echo "upgrading pip/setuptools/wheel (best effort)"
  if ! "${VENV_PIP}" install --upgrade pip setuptools wheel; then
    echo "warning: pip bootstrap upgrade failed; continuing with existing venv tooling"
  fi
fi

install_from_specs() {
  local specs=()
  while IFS= read -r line; do
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "${line}" ]] && continue
    specs+=("${line}")
  done < <(printf '%s\n' "${PIP_SPECS_RAW}" | tr ',' '\n')

  if [[ "${#specs[@]}" -eq 0 ]]; then
    return 1
  fi

  echo "installing Harbor pip specs for lane '${LANE}': ${specs[*]}"
  "${VENV_PIP}" install "${specs[@]}"
  return 0
}

install_from_requirements() {
  if [[ ! -f "${REQ_FILE}" ]]; then
    if [[ "${ENFORCE_DEP_SPECS}" == "1" ]]; then
      echo "missing requirements file for lane '${LANE}': ${REQ_FILE}"
      echo "action: create ${REQ_FILE} or set HARBOR_PIP_SPECS"
      exit 2
    fi
    echo "requirements file not found for lane '${LANE}', skipping Harbor dep install"
    return 0
  fi

  if ! grep -Eq '^[[:space:]]*[^#[:space:]].*$' "${REQ_FILE}"; then
    if [[ "${ENFORCE_DEP_SPECS}" == "1" ]]; then
      echo "no Harbor dependency specs found in ${REQ_FILE}"
      echo "action: add pinned/canary Harbor package specs or set HARBOR_PIP_SPECS"
      exit 2
    fi
    echo "requirements file ${REQ_FILE} is empty/comment-only, skipping Harbor dep install"
    return 0
  fi

  echo "installing Harbor deps from ${REQ_FILE}"
  "${VENV_PIP}" install -r "${REQ_FILE}"
}

if ! install_from_specs; then
  install_from_requirements
fi

echo "running Harbor unit tests"
"${VENV_PY}" -m unittest discover -s scripts/harbor/tests -p 'test_*.py'

echo "running Harbor script syntax checks"
PYTHONPYCACHEPREFIX="${VENV_DIR}/pycache" "${VENV_PY}" -m py_compile \
  scripts/harbor/export_harbor_to_agentlab_jsonl.py \
  scripts/harbor/harbor_benchmark_adapter.py \
  scripts/harbor/check_harbor_adapter_compat.py

PROBE_CMD=("${VENV_PY}" scripts/harbor/check_harbor_adapter_compat.py --python-bin "${VENV_PY}")
if [[ "${REQUIRE_EVALUATOR_CMD}" == "1" ]]; then
  PROBE_CMD+=(--require-evaluator-cmd --expect-external-evaluator)
fi
echo "running Harbor compatibility probe: ${PROBE_CMD[*]}"
"${PROBE_CMD[@]}"

if [[ "${RUN_SMOKE_DESCRIBE}" == "1" ]]; then
  echo "running Harbor smoke describe checks"
  PYTHON_BIN="${VENV_PY}" scripts/harbor/smoke_terminal_bench2_harbor.sh
else
  echo "skipping smoke describe checks (set HARBOR_RUN_SMOKE_DESCRIBE=1 to enable)"
fi

echo "Harbor Phase 3 lane '${LANE}' completed"
