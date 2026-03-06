#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  validate-dx-evidence.sh --phase <phase0|phase1|phase2> [--file <path>] [--mode <lint|strict>]

Options:
  --phase   Required phase selector.
  --file    Optional override path to invariants.md.
  --mode    Validation mode: lint (default) or strict.
EOF
}

trim() {
  local s="$*"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

is_placeholder() {
  local value
  value="$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')"
  case "$value" in
    ""|"TBD"|"TODO"|"N/A"|"-")
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

phase=""
file=""
mode="lint"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --phase)
      shift
      phase="${1:-}"
      ;;
    --file)
      shift
      file="${1:-}"
      ;;
    --mode)
      shift
      mode="${1:-}"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ -z "$phase" ]]; then
  echo "error: --phase is required" >&2
  usage >&2
  exit 2
fi

if [[ "$mode" != "lint" && "$mode" != "strict" ]]; then
  echo "error: --mode must be lint or strict (got '$mode')" >&2
  exit 2
fi

declare -a required_ids=()
case "$phase" in
  phase0)
    required_ids=(P0-I01 P0-I02 P0-I03 P0-I04 P0-I05 P0-I06)
    ;;
  phase1)
    required_ids=(P1-I01 P1-I02 P1-I03 P1-I04 P1-I05 P1-I06)
    ;;
  phase2)
    required_ids=(P2-I01 P2-I02 P2-I03)
    ;;
  *)
    echo "error: unknown phase '$phase'" >&2
    exit 2
    ;;
esac

if [[ -z "$file" ]]; then
  file="docs/evidence/dx/${phase}/invariants.md"
fi

if [[ ! -f "$file" ]]; then
  echo "error: invariants file not found: $file" >&2
  exit 1
fi

declare -A seen=()
declare -A status_by_id=()
declare -A evidence_by_id=()
declare -A test_by_id=()
declare -A reviewer_by_id=()

errors=0
row_count=0

while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  row_count=$((row_count + 1))
  IFS='|' read -r _ c1 c2 c3 c4 c5 c6 _ <<< "$line"

  id="$(trim "$c1")"
  status="$(trim "$c2")"
  status="$(printf '%s' "$status" | tr '[:lower:]' '[:upper:]')"
  test_name="$(trim "$c3")"
  evidence_path="$(trim "$c4")"
  reviewer="$(trim "$c5")"

  if [[ -n "${seen[$id]:-}" ]]; then
    echo "error: duplicate invariant id '$id' in $file" >&2
    errors=$((errors + 1))
    continue
  fi
  seen["$id"]=1

  if [[ ! "$id" =~ ^P[0-9]+-I[0-9]{2}$ ]]; then
    echo "error: malformed invariant id '$id' in $file" >&2
    errors=$((errors + 1))
  fi

  case "$status" in
    PASS|PENDING|BLOCKED)
      ;;
    *)
      echo "error: invalid status '$status' for $id (expected PASS|PENDING|BLOCKED)" >&2
      errors=$((errors + 1))
      ;;
  esac

  if [[ -z "$test_name" ]]; then
    echo "error: empty test field for $id" >&2
    errors=$((errors + 1))
  fi
  if [[ -z "$evidence_path" ]]; then
    echo "error: empty evidence field for $id" >&2
    errors=$((errors + 1))
  fi
  if [[ -z "$reviewer" ]]; then
    echo "error: empty reviewer field for $id" >&2
    errors=$((errors + 1))
  fi

  if [[ "$evidence_path" == /* ]]; then
    echo "error: evidence path for $id must be relative (got absolute path '$evidence_path')" >&2
    errors=$((errors + 1))
  fi

  if [[ "$status" == "PASS" ]]; then
    if is_placeholder "$test_name"; then
      echo "error: PASS row $id has placeholder test field '$test_name'" >&2
      errors=$((errors + 1))
    fi
    if is_placeholder "$reviewer"; then
      echo "error: PASS row $id has placeholder reviewer field '$reviewer'" >&2
      errors=$((errors + 1))
    fi
    if [[ ! -f "$evidence_path" ]]; then
      echo "error: PASS row $id evidence file does not exist: $evidence_path" >&2
      errors=$((errors + 1))
    fi
  fi

  status_by_id["$id"]="$status"
  evidence_by_id["$id"]="$evidence_path"
  test_by_id["$id"]="$test_name"
  reviewer_by_id["$id"]="$reviewer"
done < <(grep -E '^\|[[:space:]]*P[0-9]+-I[0-9]{2}[[:space:]]*\|' "$file" || true)

if (( row_count == 0 )); then
  echo "error: no invariant rows found in $file" >&2
  exit 1
fi

for id in "${required_ids[@]}"; do
  if [[ -z "${seen[$id]:-}" ]]; then
    echo "error: missing required invariant row '$id' in $file" >&2
    errors=$((errors + 1))
    continue
  fi
  if [[ "$mode" == "strict" ]]; then
    if [[ "${status_by_id[$id]}" != "PASS" ]]; then
      echo "error: strict mode requires $id=PASS (got ${status_by_id[$id]})" >&2
      errors=$((errors + 1))
    fi
    if [[ ! -f "${evidence_by_id[$id]}" ]]; then
      echo "error: strict mode requires evidence file for $id: ${evidence_by_id[$id]}" >&2
      errors=$((errors + 1))
    fi
    if is_placeholder "${test_by_id[$id]}"; then
      echo "error: strict mode requires non-placeholder test for $id" >&2
      errors=$((errors + 1))
    fi
    if is_placeholder "${reviewer_by_id[$id]}"; then
      echo "error: strict mode requires non-placeholder reviewer for $id" >&2
      errors=$((errors + 1))
    fi
  fi
done

if (( errors > 0 )); then
  echo "DX evidence validation failed for phase '$phase' (mode=$mode, errors=$errors)." >&2
  exit 1
fi

echo "DX evidence validation passed: phase=$phase mode=$mode file=$file rows=$row_count"
for id in "${required_ids[@]}"; do
  echo "  - $id: ${status_by_id[$id]}"
done
