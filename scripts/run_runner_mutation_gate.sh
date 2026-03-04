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

# ── Scheduling Guards ──

record_case \
  "schedule randomized Fisher-Yates swap removed" \
  "slots.swap(i, j);" \
  "/* slots.swap(i, j); */" \
  "schedule_randomized_different_seeds_produce_different_orders" \
  "$WORK_DIR/mutant_22.log"

# ── Validation Guards ──

record_case \
  "validate_required_fields empty string check removed (is_empty -> false)" \
  "Some(Value::String(s)) => s.trim().is_empty()," \
  "Some(Value::String(s)) => false," \
  "validate_required_fields_v1_whitespace_experiment_id_fails" \
  "$WORK_DIR/mutant_23.log"

record_case \
  "validate_required_fields zero replications check removed (== Some(0) -> false)" \
  "n.as_u64() == Some(0)
                }
                _ => false," \
  "false
                }
                _ => false," \
  "validate_required_fields_v1_zero_replications_fails" \
  "$WORK_DIR/mutant_24.log"

record_case \
  "sanitize_name_for_path empty fallback changed (experiment -> default)" \
  "\"experiment\".to_string()
    } else {
        trimmed.to_string()" \
  "\"default\".to_string()
    } else {
        trimmed.to_string()" \
  "sanitize_name_for_path_all_special_returns_experiment" \
  "$WORK_DIR/mutant_25.log"

record_case \
  "experiment_max_concurrency clamp removed (max(1) -> max(0))" \
  "(raw.max(1)).min(usize::MAX as u64) as usize" \
  "(raw.max(0)).min(usize::MAX as u64) as usize" \
  "experiment_max_concurrency_clamps_zero_to_one" \
  "$WORK_DIR/mutant_26.log"

# ── Path & Contract Guards ──

record_case \
  "strip_contract_prefix slash boundary removed (starts_with('/') -> true)" \
  "if rest.starts_with('/') {
        Some(rest)
    } else {
        None" \
  "if true {
        Some(rest)
    } else {
        None" \
  "strip_contract_prefix_no_slash_boundary_returns_none" \
  "$WORK_DIR/mutant_27.log"

record_case \
  "resolve_contract_path_components In mapped to Out" \
  "return Some((ContractPathRoot::In, rest));" \
  "return Some((ContractPathRoot::Out, rest));" \
  "resolve_contract_path_components_maps_all_roots" \
  "$WORK_DIR/mutant_28.log"

record_case \
  "is_workspace_evidence_excluded node_modules check removed" \
  "if name == \"node_modules\"" \
  "if name == \"__never_match__\"" \
  "workspace_evidence_excluded_node_modules" \
  "$WORK_DIR/mutant_29.log"

record_case \
  "validate_workspace_relative_path dot-dot check removed" \
  "Component::ParentDir => {
                return Err(anyhow!(\"path cannot contain '..'\"));" \
  "Component::ParentDir => {
                normalized.push(\"..\");" \
  "validate_workspace_relative_path_rejects_dot_dot" \
  "$WORK_DIR/mutant_30.log"

record_case \
  "validate_container_workspace_path boundary check inverted" \
  "if !(path == AGENTLAB_CONTRACT_WORKSPACE_DIR" \
  "if (path == AGENTLAB_CONTRACT_WORKSPACE_DIR" \
  "validate_container_workspace_path_exact_match" \
  "$WORK_DIR/mutant_31.log"

record_case \
  "find_project_root_from_run_dir parent chain shortened (3 parents -> 2)" \
  ".and_then(|p| p.parent()) // .lab
        .and_then(|p| p.parent()) // root" \
  ".and_then(|p| p.parent()) // root
        .and_then(|_| None) // MUTANT: killed chain" \
  "find_project_root_from_run_dir_three_level_navigation" \
  "$WORK_DIR/mutant_32.log"

# ── State Management Guards ──

record_case \
  "recover_reconciled_status maps unknown to completed instead of interrupted" \
  "_ => \"interrupted\"," \
  "_ => \"completed\"," \
  "recover_reconciled_status_maps_unknown_to_interrupted" \
  "$WORK_DIR/mutant_33.log"

record_case \
  "continue_run allows completed status" \
  "\"completed\" => return Err(anyhow!(\"run already completed" \
  "\"completed\" => { /* MUTANT: allow completed */ }" \
  "continue_run_rejects_completed_status" \
  "$WORK_DIR/mutant_34.log"

record_case \
  "RunControlGuard drop writes completed instead of failed" \
  "let _ = write_run_control_v2(&self.run_dir, &self.run_id, \"failed\", &[], None);" \
  "let _ = write_run_control_v2(&self.run_dir, &self.run_id, \"completed\", &[], None);" \
  "run_control_guard_marks_failed_on_drop" \
  "$WORK_DIR/mutant_35.log"

record_case \
  "TrialStateGuard drop writes completed instead of failed/aborted" \
  "let _ = write_trial_state(
                &self.trial_dir,
                &self.trial_id,
                \"failed\",
                None,
                None,
                Some(\"aborted\")," \
  "let _ = write_trial_state(
                &self.trial_dir,
                &self.trial_id,
                \"completed\",
                None,
                None,
                None," \
  "trial_state_guard_marks_aborted_on_drop" \
  "$WORK_DIR/mutant_36.log"

# ── Retry & Scheduling Guards ──

record_case \
  "should_retry_outcome empty triggers always returns false" \
  "return outcome == \"error\" || exit_status != \"0\";" \
  "return false;" \
  "should_retry_outcome_error_always_retried_default" \
  "$WORK_DIR/mutant_37.log"

record_case \
  "should_retry_outcome error match removed" \
  "\"error\" if outcome == \"error\" => return true," \
  "\"error\" if outcome == \"error\" => {}," \
  "should_retry_outcome_error_with_error_trigger" \
  "$WORK_DIR/mutant_38.log"

record_case \
  "should_retry_outcome failure match removed" \
  "\"failure\" if exit_status != \"0\" => return true," \
  "\"failure\" if exit_status != \"0\" => {}," \
  "should_retry_outcome_failure_with_failure_trigger" \
  "$WORK_DIR/mutant_39.log"

record_case \
  "should_retry_outcome timeout match removed" \
  "\"timeout\" if outcome == \"timeout\" => return true," \
  "\"timeout\" if outcome == \"timeout\" => {}," \
  "should_retry_outcome_timeout_with_timeout_trigger" \
  "$WORK_DIR/mutant_40.log"

# ── Trial & Fork Guards ──

record_case \
  "trial_index_from_trial_id prefix changed (trial_ -> task_)" \
  ".strip_prefix(\"trial_\")" \
  ".strip_prefix(\"task_\")" \
  "trial_index_from_trial_id_parses_standard_format" \
  "$WORK_DIR/mutant_41.log"

record_case \
  "trial_index_from_trial_id zero filter removed (> 0 -> >= 0)" \
  ".filter(|idx| *idx > 0)" \
  ".filter(|idx| *idx >= 0)" \
  "trial_index_from_trial_id_handles_zero" \
  "$WORK_DIR/mutant_42.log"

# ── Benchmark Guards ──

record_case \
  "benchmark verdict pass mapped to failure instead of success" \
  "\"pass\" => Some(\"success\")," \
  "\"pass\" => Some(\"failure\")," \
  "benchmark_verdict_maps_to_trial_outcome" \
  "$WORK_DIR/mutant_43.log"

record_case \
  "benchmark verdict fail mapped to success instead of failure" \
  "\"fail\" => Some(\"failure\")," \
  "\"fail\" => Some(\"success\")," \
  "benchmark_verdict_maps_to_trial_outcome" \
  "$WORK_DIR/mutant_44.log"

# ── Variant Resolution Guards ──

record_case \
  "resolve_variant_plan baseline bindings default changed (empty object -> null)" \
  "let baseline_bindings = json_value
            .pointer(\"/baseline/bindings\")
            .cloned()
            .unwrap_or(json!({}));" \
  "let baseline_bindings = json_value
            .pointer(\"/baseline/bindings\")
            .cloned()
            .unwrap_or(json!(null));" \
  "resolve_variant_plan_legacy_variant_bindings_default_to_empty_object" \
  "$WORK_DIR/mutant_45.log"

# ── DeterministicCommitter Guards ──

record_case \
  "deterministic committer stale check inverted (< -> >)" \
  "if schedule_idx < self.next_commit_idx {" \
  "if schedule_idx > self.next_commit_idx {" \
  "deterministic_committer_enqueue_stale_index_errors" \
  "$WORK_DIR/mutant_47.log"

record_case \
  "deterministic committer pending dedup check removed" \
  "if existing_key == pending_key {
                return Ok(false);
            }" \
  "if false {
                return Ok(false);
            }" \
  "deterministic_committer_enqueue_duplicate_returns_false" \
  "$WORK_DIR/mutant_48.log"

# ── Build Runtime Contract Env Guards ──

record_case \
  "build_runtime_contract_env clean contract short-circuit removed" \
  "if clean_contract_v1 {
        return BTreeMap::new();
    }" \
  "if false {
        return BTreeMap::new();
    }" \
  "build_runtime_contract_env_clean_contract_returns_empty" \
  "$WORK_DIR/mutant_49.log"

# ── Schedule Progress Guards ──

record_case \
  "schedule_progress_path wrong filename" \
  "run_dir.join(\"runtime\").join(\"schedule_progress.json\")" \
  "run_dir.join(\"runtime\").join(\"schedule_state.json\")" \
  "schedule_progress_path_uses_runtime_dir" \
  "$WORK_DIR/mutant_50.log"

record_case \
  "slot_commit_journal_path wrong filename" \
  "run_dir.join(\"runtime\").join(\"slot_commit_journal.jsonl\")" \
  "run_dir.join(\"runtime\").join(\"slot_commit_log.jsonl\")" \
  "slot_commit_journal_path_correct_structure" \
  "$WORK_DIR/mutant_51.log"

# ── as_portable_rel Guards ──

# ── workspace_evidence exclusion scope ──

record_case \
  "workspace_evidence prefix exclusion disabled" \
  "if WORKSPACE_EVIDENCE_EXCLUDE_PREFIXES
        .iter()
        .any(|prefix| rel.starts_with(prefix))
    {
        return true;
    }" \
  "if WORKSPACE_EVIDENCE_EXCLUDE_PREFIXES
        .iter()
        .any(|prefix| rel.starts_with(prefix))
    {
        /* MUTANT: disabled */
    }" \
  "workspace_evidence_excluded_logs_prefix" \
  "$WORK_DIR/mutant_53.log"

# ── load_experiment_input Guards ──

record_case \
  "load_experiment_input manifest overrides check removed" \
  "if overrides_path.is_some() {
                return Err(anyhow!(
                    \"overrides are not supported when running from a built package directory\"" \
  "if false {
                return Err(anyhow!(
                    \"overrides are not supported when running from a built package directory\"" \
  "load_experiment_input_directory_package_rejects_overrides" \
  "$WORK_DIR/mutant_54.log"

# ── output_peer_path Guards ──

record_case \
  "output_peer_path ignores parent and always returns filename" \
  "if let Some(parent) = output.parent() {
        return parent.join(file_name).to_string_lossy().to_string();
    }
    file_name.to_string()" \
  "file_name.to_string()" \
  "output_peer_path_replaces_filename" \
  "$WORK_DIR/mutant_55.log"

# ── highest_attempt_by_schedule Guards ──

record_case \
  "highest_attempt_by_schedule always stores 0 instead of max" \
  "if record.attempt > *entry {
            *entry = record.attempt;
        }" \
  "if record.attempt > *entry {
            *entry = 0;
        }" \
  "highest_attempt_by_schedule_tracks_max_attempt_per_index" \
  "$WORK_DIR/mutant_56.log"

# ── resolve_trial_timeout_ms Guards ──

record_case \
  "resolve_trial_timeout_ms ignores input and always uses default" \
  "input
        .pointer(\"/policy/timeout_ms\")
        .and_then(|v| v.as_u64())
        .or(invocation_default_timeout_ms)" \
  "invocation_default_timeout_ms" \
  "resolve_trial_timeout_ms_prefers_input_over_default" \
  "$WORK_DIR/mutant_57.log"

# ── find_project_root Guards ──

record_case \
  "find_project_root ignores .lab and always returns input" \
  "if p.file_name().and_then(|s| s.to_str()) == Some(\".lab\") {
            return p.parent().unwrap_or(experiment_dir).to_path_buf();
        }" \
  "if false {
            return p.parent().unwrap_or(experiment_dir).to_path_buf();
        }" \
  "find_project_root_returns_parent_of_dot_lab" \
  "$WORK_DIR/mutant_58.log"

# ── commit_key_for_slot_completion determinism ──

record_case \
  "commit_key_for_slot_completion format changed" \
  "format!(\"{}:{}:{}\", slot.schedule_index, slot.trial_id, slot.status)" \
  "format!(\"{}:{}\", slot.schedule_index, slot.trial_id)" \
  "deterministic_committer_commit_key_for_slot_completion_deterministic" \
  "$WORK_DIR/mutant_59.log"

cp "$TMP_BACKUP" "$TARGET_FILE"
echo
echo "Mutation logs: $WORK_DIR"
if [[ "$SURVIVORS" -gt 0 ]]; then
  echo "Mutation gate FAILED: $SURVIVORS surviving mutant(s)." >&2
  exit 1
fi
echo "Mutation gate PASSED: all mutants killed."
