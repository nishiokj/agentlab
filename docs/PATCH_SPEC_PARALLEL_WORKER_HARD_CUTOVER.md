# Patch Spec: Parallel Worker Runner Hard Cutover

Status: In progress (`P0` + `P1` completed on 2026-02-22)  
Date: 2026-02-22  
Owner: `lab-runner` (core), `lab-cli` (surface), `schemas` (contracts)

## 1. Intent

Hard-cut the runner from single-flight execution to a worker-based run engine that is:

1. Fully parallelizable locally.
2. Scalable to remote/cloud worker execution.
3. Deterministic and resumable under crash/retry conditions.
4. Benchmark-agnostic and file-contract-driven.
5. Safe from shared mutable state sprawl.

This spec updates only runner architecture. It does not modify benchmark spec documents.

## 2. Inputs and Contracts (Extracted Baseline)

This runner model explicitly reuses benchmark-adaptation inputs from `jesus/docs/specs/BENCHMARK_ADAPTER_PATCH_SPEC.md`:

1. Dataset task rows provide runtime task fields (not benchmark-specific code paths), including `task.image` and `task.workspace`.
2. Optional frozen agent artifact (`agent_artifact_v1`) is copied into trial runtime.
3. Optional grading script + per-task grading assets produce `grade.json`.
4. Single-container-per-trial execution remains valid.
5. Preflight remains the guardrail for static checks, image readiness, and smoke checks.

Runner requirement: these are generic file contracts. The scheduler and worker model must never branch on benchmark identity.

## 3. Why Hard Cut Is Required

Current runtime remains structurally serial:

1. Serial schedule loop: `rust/crates/lab-runner/src/lib.rs:2354`.
2. Blocking trial call in loop: `rust/crates/lab-runner/src/lib.rs:2627`.
3. `design.max_concurrency` is not consumed for scheduling.
4. `run_control_v1` models a single `active_trial_id`: `rust/crates/lab-runner/src/lib.rs:519`.

The code and docs already acknowledge the gap:

1. `README.md:355`.
2. `README.md:356`.
3. `docs/VARIANT_EXECUTABLE_RUNTIME_SPEC.md:32`.
4. `docs/VARIANT_EXECUTABLE_RUNTIME_SPEC.md:33`.

## 4. Hard-Cut Rules

1. No legacy serial-only schedule path remains in production code.
2. `design.max_concurrency` is enforced by scheduler logic.
3. Workers never mutate global run state.
4. Workers never append run-level facts/progress directly.
5. `run` and `continue` use one execution engine path.
6. No benchmark-name/task-prefix branching in execution behavior.

## 5. Execution Model (From Trial Unit Upward)

## 5.1 Trial Unit

A trial is one immutable dispatch payload:

1. `schedule_idx`, `slot`, `trial_id`.
2. Resolved runtime profile.
3. Task payload/materialization data.
4. Effective policy.

Worker returns immutable completion payload:

1. Terminal status/classification.
2. Trial-level artifacts/evidence payloads.
3. Metrics/event/snapshot rows.
4. Runtime summary.

## 5.2 Trial Lifecycle Inside Worker

Worker trial lifecycle supports both legacy runtime and benchmark-adaptation runtime contracts:

1. Resolve trial image source (`task.image` when configured, else global image fallback).
2. Create/start container.
3. Stage task input and dependencies.
4. Stage frozen agent artifact when configured.
5. Run agent phase.
6. Run grading phase when configured.
7. Collect `/out` files, including `result.json` and optional `grade.json`.
8. Tear down container.
9. Return completion payload to coordinator.

Important: this lifecycle is worker-local. It does not write run-level progress or sink streams.

## 5.3 Preflight Contract

Runner supports preflight phases as execution prerequisites:

1. Static validation (artifact/schema/paths/permissions).
2. Image readiness checks (including pull/availability).
3. Smoke execution check.

Preflight is orthogonal to scheduler implementation and remains benchmark-agnostic.

## 6. Target Architecture

## 6.1 Components

1. `RunCoordinator`: single mutable-state owner.
2. `Scheduler`: dispatch eligibility and ordering.
3. `WorkerBackend`: execution transport boundary.
4. `TrialExecutor`: per-trial execution body.
5. `Committer`: single-writer state + sink persistence.

## 6.2 Ownership Boundaries

Coordinator exclusively owns:

1. `schedule_progress`.
2. `trial_index`.
3. `consecutive_failures`.
4. `pruned_variants`.
5. `chain_states`.
6. Run control state.
7. All `RunSink` append/flush calls and evidence/progress writes.

Workers own only per-trial local state.

## 6.3 Worker Boundary Interface

```rust
trait WorkerBackend: Send + Sync {
    fn submit(&self, dispatch: TrialDispatch) -> anyhow::Result<WorkerTicket>;
    fn poll_completions(
        &self,
        timeout: std::time::Duration,
    ) -> anyhow::Result<Vec<TrialCompletion>>;
    fn request_pause(&self, worker_id: &str, label: &str) -> anyhow::Result<PauseAck>;
    fn request_stop(&self, worker_id: &str, reason: &str) -> anyhow::Result<()>;
}
```

Implementations:

1. `LocalThreadWorkerBackend` (required in this cutover).
2. `RemoteWorkerBackend` (interface required now; implementation can land incrementally behind same contract).

## 7. Scheduling, Concurrency, and Determinism

## 7.1 Dispatch Constraints

Dispatch allowed only when all pass:

1. Global cap: `in_flight < design.max_concurrency`.
2. Variant cap: `in_flight_by_variant[v] < variant.max_parallel_trials` (default unbounded, globally bounded).
3. Chain lease: max one in-flight trial per `(variant_id, chain_id)` for stateful policies.
4. Variant not pruned.

## 7.2 Deterministic Commit Discipline

Completions can arrive out of order; commits cannot:

1. Buffer completion by `schedule_idx`.
2. Commit only contiguous sequence starting at `next_commit_idx`.
3. Append sink/evidence/progress in commit order.
4. Flush sink on commit boundary.

Result: deterministic run history independent of race timing.

## 7.3 Retry/Pruning Ownership

1. Retry loop remains inside per-trial execution envelope.
2. Pruning decisions are coordinator-only at commit time.
3. In-flight trials for a just-pruned variant are allowed to finish; new dispatches blocked.

## 8. Run Control and Resume Model

## 8.1 `run_control_v2`

Replace `run_control_v1` single-active model with multi-flight control model:

1. `status`.
2. `active_trials` map:
   - `trial_id`
   - `worker_id`
   - `schedule_idx`
   - `variant_id`
   - `started_at`
   - control metadata
3. `updated_at`.
4. Optional pause metadata.

`trial_state.json` remains per-trial terminal state record.

## 8.2 Continue/Resume

Continue semantics:

1. Reload persisted progress.
2. Rehydrate in-flight state from `run_control_v2.active_trials`.
3. Reconcile orphans (reattach if alive; otherwise fail deterministically as `worker_lost`).
4. Resume through same coordinator loop as fresh run.

No alternate continue scheduler implementation is allowed.

## 9. Edge Case Matrix

1. Duplicate completion for committed slot: idempotent drop.
2. Completion for unknown ticket: protocol fault, backend quarantine, run fail.
3. Crash after append before progress write: idempotent commit key required.
4. Crash after progress write before sink flush: flush-on-commit boundary required.
5. Pause requested while dispatching: stop dispatch first, then pause active workers.
6. Pause timeout for subset of workers: run becomes `interrupted`, persist survivors.
7. Stale active trial in control but missing on disk during continue: fail orphan and continue.
8. Malformed `result.json` or `grade.json`: classify as result/grade error, commit deterministically.
9. Out-of-disk during artifact persistence: deterministic failure class.
10. Remote duplicate delivery: dedupe by `(run_id, schedule_idx, trial_id, completion_seq)`.
11. State-policy chain overlap attempted under concurrency: blocked by chain lease.
12. `max_concurrency` exceeds backend practical capacity: backend can apply stricter ceiling with explicit warning.

## 10. Obsolete Paths To Remove

1. Serial monolithic execution loop in `execute_schedule_engine(...)`.
2. `run_control_v1` single-active write/read assumptions.
3. Any scheduler path that ignores `design.max_concurrency`.
4. Any direct worker path that mutates global progress or run sink output.

## 11. Parallelizable Phase DAG (Dependencies + Join Gates)

## 11.1 DAG Overview

The implementation is intentionally not one lane after `P1`:

1. `P0 -> P1 -> {P2A, P2B, P2C, P2D, P2E}`
2. `P2A -> {P3A, P3C}`
3. `P2B -> P3B`
4. `{P3A, P3B, P2C, P3C} -> P4`
5. `P4 -> {P5A, P5B}`
6. `{P5A, P5B, P2E} -> P6`
7. `P6 -> P7`

## 11.2 Phase Table

| Phase | Deliverables | Depends On | Parallelizable With |
|---|---|---|---|
| `P0` Foundation Freeze | Baseline behavior capture, benchmark-adaptation fixture run, acceptance gate lock | None | None |
| `P1` Contract Freeze | `WorkerBackend` types, dispatch/completion contracts, `run_control_v2` schema, concurrency policy parse | `P0` | None |
| `P2A` Coordinator Core | Dispatch/poll loop skeleton with in-flight accounting and no legacy scheduler edits yet | `P1` | `P2B`, `P2C`, `P2D`, `P2E` |
| `P2B` TrialExecutor Boundary | Extract trial body into `TrialExecutor` with behavior parity | `P1` | `P2A`, `P2C`, `P2D`, `P2E` |
| `P2C` Local Backend Pool | Bounded `LocalThreadWorkerBackend`, ticket map, completion polling | `P1` | `P2A`, `P2B`, `P2D`, `P2E` |
| `P2D` Remote Contract Surface | Remote backend protocol surface + fake backend harness + contract tests | `P1` | `P2A`, `P2B`, `P2C`, `P2E` |
| `P2E` Test Harness Lane | Determinism fixtures, out-of-order completion simulators, pause/resume scaffolding | `P1` | `P2A`, `P2B`, `P2C`, `P2D` |
| `P3A` Deterministic Committer | Ordered commit buffer, idempotent commit keys, sink flush discipline | `P2A` | `P3B`, `P3C`, `P2D`, `P2E` |
| `P3B` Benchmark-Adaptation Wiring | Per-task image, frozen agent artifact staging, optional grading, preflight hooks in `TrialExecutor` | `P2B` | `P3A`, `P3C`, `P2D`, `P2E` |
| `P3C` Multi-Flight Control Plane | `run_control_v2.active_trials`, CLI rendering for multiple in-flight workers | `P2A` | `P3A`, `P3B`, `P2D`, `P2E` |
| `P4` Cutover Integration Gate | Integrate coordinator, local backend, trial executor, committer; route both `run` and `continue` to one engine | `P3A`, `P3B`, `P3C`, `P2C` | `P2D`, `P2E` can continue hardening tests/contracts |
| `P5A` Recovery Hardening | Pause fan-out, orphan reconciliation, deterministic worker-lost handling | `P4` | `P5B` |
| `P5B` Capacity Hardening | Backpressure, backend capacity ceiling behavior, queue drain correctness under burst completions | `P4` | `P5A` |
| `P6` Hard Cleanup + Migration | Delete serial path, delete `run_control_v1` assumptions, remove obsolete tests, migrate fixtures | `P5A`, `P5B`, `P2E` | Remote backend implementation can proceed on stable contract |
| `P7` Final Validation | Full integration matrix, perf/concurrency checks, determinism parity, release gate | `P6` | None |

## 11.3 Explicit Parallel Entry Points

Use these as real team split points:

1. After `P1`, start five independent lanes: `P2A`, `P2B`, `P2C`, `P2D`, `P2E`.
2. After `P2A` and `P2B`, split again: `P3A`, `P3B`, `P3C` in parallel.
3. After `P4`, run two hardening lanes in parallel: `P5A` and `P5B`.
4. Keep `P2D` and `P2E` active across `P4` and `P5` so contract and determinism testing are not on the critical path.

Critical join gates:

1. Gate G1: `P1` complete before any `P2*`.
2. Gate G2: `P4` blocked until `P3A + P3B + P3C + P2C` complete.
3. Gate G3: `P6` blocked until `P5A + P5B + P2E` complete.

## 12. Required File-Level Changes

1. `rust/crates/lab-runner/src/lib.rs`
2. `rust/crates/lab-runner/src/sink.rs`
3. `rust/crates/lab-cli/src/main.rs`
4. `schemas/run_control_v2.jsonschema` (new)
5. `schemas/schedule_progress_v2.jsonschema` (if commit-id/versioning added)
6. `README.md` and runner architecture docs

## 13. Test Plan

1. Unit: dispatch gating (global/variant/chain constraints).
2. Unit: deterministic ordered commit from out-of-order completions.
3. Unit: duplicate completion idempotency.
4. Unit: pruning behavior with in-flight trials.
5. Integration: concurrency cap honored (`max_concurrency=4` yields up to 4 in flight).
6. Integration: pause/resume with multiple active workers.
7. Integration: crash + continue deterministic recovery.
8. Integration: parallel and serial-equivalent final aggregates (ordering-normalized).
9. Integration: benchmark-adaptation trial (per-task image + artifact + grading) under parallel execution.
10. Contract: schema validation for control/progress payloads.

## 14. Acceptance Gates

Cutover is complete only when all are true:

1. Scheduler enforces `design.max_concurrency` at runtime.
2. No serial-only production execution path remains.
3. Multi-flight `run_control_v2.active_trials` is authoritative.
4. Workers cannot mutate global state or append run-level facts.
5. `run` and `continue` execute through the same coordinator path.
6. Parallel execution works for:
   - cross-variant concurrency
   - same-variant replication concurrency when policy allows
7. Benchmark-adaptation trial contracts run unchanged under worker model.
8. Remote backend interface is stable behind `WorkerBackend`.

## 15. Risks and Mitigations

1. Nondeterministic outputs: enforce schedule-index commit ordering and idempotent keys.
2. Shared-state corruption: single-owner coordinator model.
3. Pause/resume regressions: explicit active-worker map + timeout semantics.
4. Memory growth: bounded in-flight + buffered completion drain in order.
5. Backend protocol drift: contract tests + fake backend integration suite.
