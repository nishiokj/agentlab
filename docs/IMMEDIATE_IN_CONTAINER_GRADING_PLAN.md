# Immediate In-Container Task Grading Plan

Date: 2026-02-23  
Scope: `rust/crates/lab-runner/src/lib.rs`

## Goal

Grade each task immediately when it completes, inside the same task container/workspace used for agent execution.

This replaces end-of-run batch grading as the primary path.

## Current Behavior

1. Worker runs agent in container in `run_builtin_adapter_container(...)` (`rust/crates/lab-runner/src/lib.rs:9364`).
2. Worker returns `TrialExecutionResult` through `TrialCompletion.artifacts` in `execute_parallel_worker_trial(...)` (`rust/crates/lab-runner/src/lib.rs:5763`).
3. Coordinator commits evidence/facts in `RunCoordinator::commit_trial_slot(...)` (`rust/crates/lab-runner/src/lib.rs:5229`).
4. Benchmark grading is batch post-processing at end of run via `process_benchmark_outputs(...)` (`rust/crates/lab-runner/src/lib.rs:7100`), invoked from:
   - `continue_run(...)` (`rust/crates/lab-runner/src/lib.rs:2712`)
   - `run_experiment_with_behavior(...)` (`rust/crates/lab-runner/src/lib.rs:6404`)

## Target Behavior

1. Agent executes in the trial container.
2. Grader executes in the same container and workspace before teardown.
3. Worker returns trial outputs plus benchmark prediction/score rows.
4. Coordinator writes prediction/score rows immediately at deterministic commit time.
5. End-of-run phase only builds/refreshes `benchmark/summary.json`.

## How Grader Runs In Same Container

Use a single `docker run` wrapper command in `run_builtin_adapter_container(...)`:

1. Run setup command if configured.
2. Run agent command.
3. Capture agent exit status.
4. Run grader command in same container.
5. Exit with explicit grading policy outcome.

No second container launch and no `docker exec` sidecar required.

## Grader Inputs In Container

These are already mounted/prepared by `TrialPaths` and `prepare_io_paths(...)`:

1. `/agentlab/in/task.json`
2. `/agentlab/in/bindings.json`
3. `/agentlab/in/policy.json`
4. `/agentlab/workspace`
5. `/agentlab/out/result.json`
6. `/agentlab/state/events.jsonl`

Runtime identifiers already available from `build_runtime_contract_env(...)`:

1. `AGENTLAB_RUN_ID`
2. `AGENTLAB_TRIAL_ID`
3. `AGENTLAB_TASK_ID`
4. `AGENTLAB_REPL_IDX`
5. `AGENTLAB_TASK_IMAGE` (when present)

Add explicit grader output env vars:

1. `AGENTLAB_BENCHMARK_PREDICTION_PATH=/agentlab/out/benchmark_prediction.json`
2. `AGENTLAB_BENCHMARK_SCORE_PATH=/agentlab/out/benchmark_score.json`
3. `AGENTLAB_AGENT_EXIT_STATUS=<captured status>`

## Worker Return Payload Changes

Extend `TrialExecutionResult` (`rust/crates/lab-runner/src/lib.rs:4448`) with:

1. `deferred_benchmark_prediction_records: Vec<Value>`
2. `deferred_benchmark_score_records: Vec<Value>`

In `TrialExecutor::execute_slot(...)`:

1. After `materialize_trial_result(...)` (`rust/crates/lab-runner/src/lib.rs:4858`), read grader output files from trial `out/`.
2. Validate row schemas:
   - `benchmark_prediction_record_v1.jsonschema`
   - `benchmark_score_record_v1.jsonschema`
3. Store validated rows in new deferred benchmark fields.

No extra transport work is needed because `execute_parallel_worker_trial(...)` already serializes `TrialExecutionResult` into `TrialCompletion.artifacts`.

## Coordinator Commit Changes

In `RunCoordinator::commit_trial_slot(...)` (`rust/crates/lab-runner/src/lib.rs:5229`):

1. Append benchmark prediction rows to `.lab/runs/<run_id>/benchmark/predictions.jsonl`.
2. Append benchmark score rows to `.lab/runs/<run_id>/benchmark/scores.jsonl`.
3. Keep this inside the same durability section as evidence/fact row appends.
4. Flush, then advance `schedule_progress`.

This preserves deterministic ordering and durability guarantees.

## End-Of-Run Changes

Replace batch adapter execution for scoring with summary-only behavior:

1. Build `benchmark/summary.json` from committed `scores.jsonl`.
2. Keep schema validation on summary output.
3. Remove or gate `process_benchmark_outputs(...)` batch command path.

## Failure Policy

Required explicit rules:

1. If agent fails and policy allows, grader may still run to produce `fail/error`.
2. If grader command fails or score row missing, classify slot as deterministic `grade_error`.
3. Commit ordering remains by `schedule_idx` via deterministic committer.

## Data Flow Summary

1. Worker executes agent and grader in same container.
2. Worker returns trial + benchmark rows.
3. Coordinator commits rows in order.
4. Progress advances only after durability boundary.
5. Summary is generated from committed score rows.

## Key Code Touch Points

1. `run_builtin_adapter_container(...)` (`rust/crates/lab-runner/src/lib.rs:9364`)
2. `build_runtime_contract_env(...)` (`rust/crates/lab-runner/src/lib.rs:9147`)
3. `TrialExecutionResult` (`rust/crates/lab-runner/src/lib.rs:4448`)
4. `TrialExecutor::execute_slot(...)` (`rust/crates/lab-runner/src/lib.rs:4587`)
5. `execute_parallel_worker_trial(...)` (`rust/crates/lab-runner/src/lib.rs:5763`)
6. `RunCoordinator::commit_trial_slot(...)` (`rust/crates/lab-runner/src/lib.rs:5229`)
7. End-of-run benchmark hook sites:
   - `continue_run(...)` (`rust/crates/lab-runner/src/lib.rs:2712`)
   - `run_experiment_with_behavior(...)` (`rust/crates/lab-runner/src/lib.rs:6404`)
