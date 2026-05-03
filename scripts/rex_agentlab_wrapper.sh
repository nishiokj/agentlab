#!/bin/sh
set -eu

OUT_PATH="${AGENTLAB_RESULT_PATH:-}"
HAS_INPUT=0
HAS_OUTPUT=0
for arg in "$@"; do
  case "$arg" in
    --input|--input-file|--input=*|--input-file=*)
      HAS_INPUT=1
      ;;
    --output|--output=*)
      HAS_OUTPUT=1
      ;;
  esac
done

set +e
if [ "$HAS_INPUT" -eq 0 ] && [ "$HAS_OUTPUT" -eq 0 ] && [ "${AGENTLAB_TRIAL_INPUT_PATH:-}" != "" ] && [ "${AGENTLAB_RESULT_PATH:-}" != "" ]; then
  /opt/agent/bin/rex "$@" --input-file "$AGENTLAB_TRIAL_INPUT_PATH" --output "$AGENTLAB_RESULT_PATH"
else
  /opt/agent/bin/rex "$@"
fi
STATUS=$?
set -e

if [ "$STATUS" -eq 0 ] && [ "${AGENTLAB_PREFLIGHT_SMOKE:-}" != "" ] && [ "$OUT_PATH" != "" ] && [ ! -s "$OUT_PATH" ]; then
  mkdir -p "$(dirname "$OUT_PATH")"
  cat > "$OUT_PATH" <<'JSON'
{
  "schema_version": "agent_result_v1",
  "outcome": "success",
  "metrics": {
    "success": 1,
    "preflight_smoke": 1
  }
}
JSON
fi

exit "$STATUS"
