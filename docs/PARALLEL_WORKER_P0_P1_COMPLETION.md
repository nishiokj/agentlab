# Parallel Worker Cutover Progress

Date: 2026-02-22

## `P0` Foundation Freeze Completed

Baseline freeze artifacts:

1. Benchmark-adaptation trial-shape fixture:
   - `rust/crates/lab-runner/testdata/p0_benchmark_adaptation_trial_shape.json`
2. Baseline fixture validation test:
   - `p0_freeze_benchmark_adaptation_trial_shape_fixture_parses`
3. Contract gate tests that lock pre-cutover control-plane behavior shape:
   - `p1_run_control_writer_emits_v2_payload_and_legacy_single_active_mirror`
   - `p1_run_control_v2_schema_accepts_writer_payload`

## `P1` Contract Freeze Completed

Runner contract surface added:

1. Worker boundary types and trait in runner core:
   - `TrialDispatch`
   - `WorkerTicket`
   - `TrialCompletion`
   - `WorkerPauseAck`
   - `WorkerBackend`
2. `run_control_v2` schema + writer migration:
   - `schemas/run_control_v2.jsonschema`
   - `write_run_control_v2(...)` emits `schema_version: run_control_v2`
3. V2 run-control helpers for active-trial and adapter-control reads:
   - `run_control_active_trial_ids(...)`
   - `run_control_active_adapter_for_trial(...)`
4. Concurrency policy parsing in `parse_policies(...)`:
   - `design.policies.concurrency.max_in_flight_per_variant`
   - `design.policies.concurrency.require_chain_lease`
5. Resolved experiment schemas updated for new concurrency policy fields:
   - `schemas/resolved_experiment_v0_4.jsonschema`
   - `schemas/resolved_experiment_v0_5.jsonschema`

## `P2A` Coordinator Skeleton Completed

Scheduler loop now has explicit coordinator phases:

1. Dispatch:
   - `RunCoordinator::dispatch_slot(...)`
2. Commit skipped slot:
   - `RunCoordinator::commit_skipped_pruned_slot(...)`
3. Commit completed slot:
   - `RunCoordinator::commit_trial_slot(...)`

`execute_schedule_engine(...)` now routes the loop through these coordinator methods.

## `P2B` TrialExecutor Boundary Completed

Per-slot trial execution body is extracted behind:

1. `TrialExecutor::execute_slot(...)`
2. `TrialExecutionResult` completion payload (`trial_id`, `slot_status`)

Behavior remains unchanged; loop orchestration is separated from trial execution internals.

## `P2E` Test Harness Lane Completed

Determinism and pause/resume scaffolding delivered:

1. Determinism fixture:
   - `rust/crates/lab-runner/testdata/p2e_determinism_fixture.json`
2. Out-of-order completion simulator test harness:
   - `OutOfOrderCompletionSimulator` + `drain_ready_completions_in_schedule_order(...)`
3. Determinism tests:
   - `p2e_out_of_order_completion_simulator_replays_fixture_ticks`
   - `p2e_determinism_fixture_commits_contiguously_despite_out_of_order_arrivals`
4. Multi-flight pause/resume scaffolding tests on `run_control_v2`:
   - `p2e_pause_scaffolding_marks_interrupted_when_multi_flight_pause_fails`
   - `p2e_resume_scaffolding_requires_trial_id_when_multi_flight_is_active`

## `P4` Cutover Integration Gate Completed

Coordinator execution now integrates backend dispatch + deterministic commit:

1. `execute_schedule_engine(...)` routes through a parallel engine when:
   - `design.max_concurrency > 1`
   - policy state is `isolate_per_trial`
2. Parallel engine wiring added:
   - `LocalThreadWorkerBackend` submission/poll loop
   - in-flight ticket tracking
   - per-variant in-flight gating via `design.policies.concurrency.max_in_flight_per_variant`
   - out-of-order completion buffering + deterministic contiguous commit via `DeterministicCommitter`
3. Trial worker payloads now commit through coordinator:
   - deferred sink rows
   - deferred evidence rows
   - deferred chain-state rows
4. Failure/pruning accounting moved to commit-time (`RunCoordinator::commit_trial_slot(...)`) so coordinator remains single global-state owner.
5. Runtime concurrency is now consumed from experiment contract:
   - `experiment_max_concurrency(...)` reads `/design/max_concurrency`

Validation test:

- `p4_cutover_uses_parallel_engine_path_for_isolate_policy`

## `P5A` Recovery Hardening Completed

Continue/recovery flow now deterministically reconciles stale active trials:

1. `run_control_v2.active_trials` parser added:
   - `run_control_active_trials(...)`
2. Continue path captures recovered active trials before re-entering scheduler.
3. Scheduler seeds recovered in-flight slots as deterministic `worker_lost` failures by `schedule_idx` before dispatching new work.
4. Pause fan-out behavior for multi-flight active runs:
   - `pause_run(..., trial_id=None, ...)` now attempts pause across all active trials.
   - Partial pause failures mark run as `interrupted` and persist unpaused survivors in `active_trials`.
5. Recovery failure accounting/pruning occurs through normal commit path to keep ordering and resume semantics stable.

Validation test:

- `p5a_recovered_active_trials_commit_as_worker_lost_deterministically`
- `p5a_pause_run_fans_out_to_all_active_trials`
- `p5a_pause_run_partial_fanout_sets_interrupted_and_keeps_survivor_active`

## `P5B` Capacity Hardening Completed

Capacity/backpressure behavior is now hardened:

1. Local backend supports explicit capacity ceiling:
   - `AGENTLAB_LOCAL_WORKER_MAX_IN_FLIGHT`
   - Effective in-flight cap can be stricter than requested concurrency.
2. Capacity events are treated as scheduler backpressure:
   - Dispatch retries instead of failing the run when backend returns capacity saturation.
3. Burst completion drains are bounded and lossless:
   - `poll_completions(...)` drains in bounded batches and continues across polls.

Validation tests:

- `p5b_local_worker_capacity_ceiling_resolves_with_warning`
- `p5b_submit_backpressure_classifies_capacity_as_retryable`
- `p5b_local_worker_backend_drains_burst_completions_without_loss`

## `P6` Hard Cleanup + Migration Completed

Hard cleanup removes remaining pre-cutover assumptions:

1. Serial execution path removed:
   - `execute_schedule_engine_serial(...)` deleted.
   - `execute_schedule_engine(...)` now routes through one parallel coordinator path.
2. `run_control_v1` single-active assumptions removed:
   - Legacy mirror fields (`active_trial_id`, `active_adapter`) no longer written.
   - Legacy fallback readers removed from active-trial/control helpers.
3. Run-control schema migrated:
   - `schemas/run_control_v2.jsonschema` no longer allows legacy mirror fields.
4. Obsolete tests/fixtures migrated:
   - Tests now use `active_trials` as authoritative multi-flight state.
   - Added regression test:
     - `p6_run_control_v2_writer_emits_active_trials_without_legacy_mirrors`

## `P7` Final Validation Completed

Final validation gates for cutover behavior are now locked:

1. Concurrency cap integration check:
   - `p7_concurrency_cap_honors_max_in_flight_four`
2. Determinism parity check for ordering-normalized final aggregates:
   - `p7_parallel_and_serial_equivalent_final_aggregates_ordering_normalized`
3. Release gate check for hard-cut policy boundary:
   - `p7_release_gate_rejects_non_isolate_state_policy`

These tests validate `P7` acceptance criteria around runtime concurrency enforcement,
deterministic commit behavior, and hard-cut execution boundaries.

## Remote Worker Concrete Lane Started

Initial concrete remote implementation now lands behind existing `WorkerBackend` contracts:

1. Added HTTP-backed remote protocol implementation:
   - `HttpRemoteWorkerProtocol`
   - Request paths:
     - `v1/worker/submit`
     - `v1/worker/poll`
     - `v1/worker/pause`
     - `v1/worker/stop`
2. Added remote auth token resolution:
   - `resolve_remote_bearer_token(...)` resolves optional bearer token from env var name.
3. Scheduler routing now supports remote backend execution path:
   - Remote no longer hard-fails at run startup.
   - `execute_schedule_engine(...)` routes to parallel worker engine when remote executor is requested.
4. Protocol hardening:
   - `RemoteWorkerBackend` now validates response `schema_version` for submit/poll/pause/stop.

Validation tests:

- `p2d_remote_backend_rejects_schema_version_mismatch`
- `resolve_remote_bearer_token_reads_env_when_present`
- `resolve_remote_bearer_token_skips_unset_and_errors_for_missing_env`
