#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUST_DIR="$ROOT_DIR/rust"
TARGET_FILE="$RUST_DIR/crates/lab-runner/src/lib.rs"

if [[ ! -f "$TARGET_FILE" ]]; then
  echo "Target file not found: $TARGET_FILE" >&2
  exit 2
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required but not found on PATH" >&2
  exit 2
fi

TMP_BACKUP="$(mktemp "${TMPDIR:-/tmp}/lab_runner_mutation_backup.XXXXXX")"
cp "$TARGET_FILE" "$TMP_BACKUP"
cleanup() {
  cp "$TMP_BACKUP" "$TARGET_FILE"
  rm -f "$TMP_BACKUP"
}
trap cleanup EXIT

mutate_once() {
  local search="$1"
  local replace="$2"
  SEARCH="$search" REPLACE="$replace" perl -0777 -i -pe '
    my $search = $ENV{"SEARCH"};
    my $replace = $ENV{"REPLACE"};
    my $count = s/\Q$search\E/$replace/s;
    if ($count != 1) {
      die "expected exactly one replacement, got $count\n";
    }
  ' "$TARGET_FILE"
}

run_case() {
  local name="$1"
  local search="$2"
  local replace="$3"
  local test_filter="$4"
  local log_file="$5"

  cp "$TMP_BACKUP" "$TARGET_FILE"
  if ! mutate_once "$search" "$replace"; then
    echo "ERROR: failed to apply mutation for case: $name" >&2
    return 2
  fi

  if (cd "$RUST_DIR" && cargo test -p lab-runner "$test_filter" >"$log_file" 2>&1); then
    echo "SURVIVED: $name"
    return 1
  fi
  echo "KILLED:   $name"
  return 0
}

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/lab_runner_mutation_logs.XXXXXX")"
SURVIVORS=0

record_case() {
  local status=0
  if run_case "$@"; then
    status=0
  else
    status=$?
  fi
  if [[ "$status" -eq 1 ]]; then
    SURVIVORS=$((SURVIVORS + 1))
    return 0
  fi
  if [[ "$status" -eq 2 ]]; then
    echo "Mutation gate aborted due to mutation setup error." >&2
    exit 2
  fi
  return 0
}

record_case \
  "local capacity off-by-one (>= -> >)" \
  "if state.in_flight_by_ticket.len() >= self.inner.max_in_flight {" \
  "if state.in_flight_by_ticket.len() > self.inner.max_in_flight {" \
  "p2c_local_thread_worker_backend_enforces_capacity_and_polls_completions" \
  "$WORK_DIR/mutant_01.log"

record_case \
  "local completion bookkeeping bypassed (consume -> raw push)" \
  "completions.push(self.consume_completion(completion)?);" \
  "completions.push(completion);" \
  "p5b_local_worker_high_churn_drains_in_flight_bookkeeping" \
  "$WORK_DIR/mutant_02.log"

record_case \
  "local capacity classifier broken (prefix/contains both miss)" \
  "message.starts_with(LOCAL_WORKER_CAPACITY_ERROR_PREFIX) || message.contains(\"at capacity\")" \
  "message.starts_with(\"local worker backend capacity saturated\") || message.contains(\"queue full\")" \
  "p5b_submit_backpressure_classifies_capacity_as_retryable" \
  "$WORK_DIR/mutant_03.log"

record_case \
  "local backpressure path bypassed (Ok(None) -> Err)" \
  "Err(err) if is_worker_backend_capacity_error(&err) => Ok(None)," \
  "Err(err) if is_worker_backend_capacity_error(&err) => Err(err)," \
  "p5b_submit_backpressure_classifies_capacity_as_retryable" \
  "$WORK_DIR/mutant_04.log"

record_case \
  "remote worker_id mismatch guard inverted (!= -> ==)" \
  "if completion.ticket.worker_id != submission.worker_id {" \
  "if completion.ticket.worker_id == submission.worker_id {" \
  "p2d_remote_backend_rejects_mismatched_completion_worker_id" \
  "$WORK_DIR/mutant_05.log"

record_case \
  "remote trial_id mismatch guard inverted (!= -> ==)" \
  "if completion.ticket.trial_id != submission.trial_id {" \
  "if completion.ticket.trial_id == submission.trial_id {" \
  "p2d_remote_backend_rejects_mismatched_completion_contracts" \
  "$WORK_DIR/mutant_06.log"

record_case \
  "remote schedule_idx mismatch guard inverted (!= -> ==)" \
  "if completion.schedule_idx != submission.schedule_idx {" \
  "if completion.schedule_idx == submission.schedule_idx {" \
  "p7_remote_backend_concurrent_submit_poll_drains_and_cleans_state" \
  "$WORK_DIR/mutant_07.log"

record_case \
  "remote duplicate seq comparator inverted (== -> !=)" \
  "&& completion_seq == existing.completion_seq;" \
  "&& completion_seq != existing.completion_seq;" \
  "p2d_remote_backend_dedupes_duplicate_delivery_by_completion_seq" \
  "$WORK_DIR/mutant_08.log"

record_case \
  "remote dedupe cache eviction disabled (> -> <)" \
  "while state.completion_key_by_ticket.len() > max_tickets {" \
  "while state.completion_key_by_ticket.len() < max_tickets {" \
  "p2d_remote_backend_completion_dedupe_cache_is_bounded" \
  "$WORK_DIR/mutant_09.log"

record_case \
  "remote completion delivery dropped (Deliver arm no-op)" \
  "RemoteCompletionValidation::Deliver => accepted.push(completion)," \
  "RemoteCompletionValidation::Deliver => {}," \
  "p2d_remote_backend_fake_harness_round_trips_protocol_contract" \
  "$WORK_DIR/mutant_10.log"

record_case \
  "remote submit schema guard inverted (!= -> ==)" \
  "if response.schema_version != REMOTE_SUBMIT_SCHEMA_V1 {" \
  "if response.schema_version == REMOTE_SUBMIT_SCHEMA_V1 {" \
  "p2d_remote_backend_fake_harness_round_trips_protocol_contract" \
  "$WORK_DIR/mutant_11.log"

record_case \
  "remote submit trial_id guard inverted (!= -> ==)" \
  "if response.ticket.trial_id != dispatch.trial_id {" \
  "if response.ticket.trial_id == dispatch.trial_id {" \
  "p2d_remote_backend_fake_harness_round_trips_protocol_contract" \
  "$WORK_DIR/mutant_12.log"

record_case \
  "remote poll schema guard inverted (!= -> ==)" \
  "if response.schema_version != REMOTE_POLL_SCHEMA_V1 {" \
  "if response.schema_version == REMOTE_POLL_SCHEMA_V1 {" \
  "p2d_remote_backend_fake_harness_round_trips_protocol_contract" \
  "$WORK_DIR/mutant_13.log"

record_case \
  "remote pause worker_id guard inverted (!= -> ==)" \
  "if response.ack.worker_id != worker_id {" \
  "if response.ack.worker_id == worker_id {" \
  "p2d_remote_backend_fake_harness_round_trips_protocol_contract" \
  "$WORK_DIR/mutant_14.log"

record_case \
  "remote pause label guard inverted (!= -> ==)" \
  "if response.ack.label != label {" \
  "if response.ack.label == label {" \
  "p2d_remote_backend_fake_harness_round_trips_protocol_contract" \
  "$WORK_DIR/mutant_15.log"

record_case \
  "remote pause acceptance guard inverted (!accepted -> accepted)" \
  "if !response.ack.accepted {" \
  "if response.ack.accepted {" \
  "p2d_remote_backend_fake_harness_round_trips_protocol_contract" \
  "$WORK_DIR/mutant_16.log"

record_case \
  "remote pause active-trial membership check inverted" \
  "if !expected_trials.contains(response.ack.trial_id.as_str()) {" \
  "if expected_trials.contains(response.ack.trial_id.as_str()) {" \
  "p2d_remote_backend_fake_harness_round_trips_protocol_contract" \
  "$WORK_DIR/mutant_17.log"

record_case \
  "remote stop acceptance guard inverted (!accepted -> accepted)" \
  "if !response.accepted {" \
  "if response.accepted {" \
  "p2d_remote_backend_fake_harness_round_trips_protocol_contract" \
  "$WORK_DIR/mutant_18.log"

record_case \
  "remote remove_active_submission bypassed" \
  "Self::remove_active_submission_for_ticket(&mut state, ticket_id);" \
  "let _ = ticket_id;" \
  "p7_remote_backend_concurrent_submit_poll_drains_and_cleans_state" \
  "$WORK_DIR/mutant_19.log"

record_case \
  "remote retry attempts clamped wrong (max -> min)" \
  "let attempts = self.retry_settings.max_attempts.max(1);" \
  "let attempts = self.retry_settings.max_attempts.min(1);" \
  "p2d_remote_backend_retries_retryable_submit_errors" \
  "$WORK_DIR/mutant_20.log"

record_case \
  "remote retry condition inverted (attempt < attempts -> >)" \
  "if retryable && attempt < attempts {" \
  "if retryable && attempt > attempts {" \
  "p2d_remote_backend_retries_typed_retryable_submit_errors" \
  "$WORK_DIR/mutant_21.log"

cp "$TMP_BACKUP" "$TARGET_FILE"
echo
echo "Mutation logs: $WORK_DIR"
if [[ "$SURVIVORS" -gt 0 ]]; then
  echo "Mutation gate FAILED: $SURVIVORS surviving mutant(s)." >&2
  exit 1
fi
echo "Mutation gate PASSED: all mutants killed."
