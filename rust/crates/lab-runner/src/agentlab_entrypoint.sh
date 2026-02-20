#!/bin/sh
set -eu

required_env() {
  key="$1"
  eval "value=\${$key:-}"
  if [ -z "$value" ]; then
    echo "missing required env: $key" >&2
    exit 64
  fi
}

required_env "AGENTLAB_TRIAL_INPUT"
required_env "AGENTLAB_TRIAL_OUTPUT"
required_env "AGENTLAB_TRIAL_EVENTS"
required_env "AGENTLAB_HARNESS_COMMAND"
required_env "AGENTLAB_AGENTLABD_START_REQUEST"
required_env "AGENTLAB_AGENTLABD_START_RESPONSE"

mkdir -p "$(dirname "$AGENTLAB_TRIAL_OUTPUT")"
mkdir -p "$(dirname "$AGENTLAB_TRIAL_EVENTS")"
mkdir -p "$(dirname "$AGENTLAB_AGENTLABD_START_RESPONSE")"

launch_mode="${AGENTLAB_LAUNCH_MODE:-file}"
if [ "$launch_mode" != "file" ] && [ "$launch_mode" != "stdio" ]; then
  echo "unsupported AGENTLAB_LAUNCH_MODE: $launch_mode" >&2
  exit 64
fi

if [ ! -f "$AGENTLAB_AGENTLABD_START_REQUEST" ]; then
  echo "missing AGENTLABD start request: $AGENTLAB_AGENTLABD_START_REQUEST" >&2
  exit 65
fi

if ! grep -q '"type"[[:space:]]*:[[:space:]]*"StartTrial"' "$AGENTLAB_AGENTLABD_START_REQUEST"; then
  echo "invalid AGENTLABD start request payload" >&2
  exit 66
fi

printf '%s' "{\"schema_version\":\"agentlabd_rpc_v1\",\"response\":{\"type\":\"StartTrial\",\"accepted\":true,\"session_id\":\"${AGENTLAB_TRIAL_ID:-trial}\",\"started_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}" > "$AGENTLAB_AGENTLABD_START_RESPONSE"

if [ -n "${AGENTLAB_SETUP_COMMAND:-}" ]; then
  sh -lc "$AGENTLAB_SETUP_COMMAND"
fi

set +e
if [ "$launch_mode" = "stdio" ]; then
  tmp_output="$AGENTLAB_TRIAL_OUTPUT.tmp"
  cat "$AGENTLAB_TRIAL_INPUT" | sh -lc "$AGENTLAB_HARNESS_COMMAND" >"$tmp_output" 2>"$AGENTLAB_TRIAL_EVENTS"
  status="$?"
  if [ -f "$tmp_output" ]; then
    mv "$tmp_output" "$AGENTLAB_TRIAL_OUTPUT"
  fi
else
  sh -lc "$AGENTLAB_HARNESS_COMMAND"
  status="$?"
fi
set -e

if [ ! -f "$AGENTLAB_TRIAL_OUTPUT" ]; then
  printf '%s' '{"schema_version":"agent_result_v1","outcome":"error","error":{"code":"missing_trial_output","message":"harness did not write AGENTLAB_TRIAL_OUTPUT"}}' > "$AGENTLAB_TRIAL_OUTPUT"
fi

exit "$status"
