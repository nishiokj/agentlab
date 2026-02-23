# Patch Spec: Durable Experiment Recovery (Experiment Durable, Task Disposable)

## Goal

Make experiment execution durable across runner/process/host failure without requiring task-level continuity.

This patch explicitly prioritizes:

1. Durable **experiment progress**.
2. Exactly-once **slot commit publication**.
3. At-least-once **task execution** (restarts are acceptable).

This patch does **not** require durable in-flight task/process memory.

---

## Why This Patch

Current behavior already persists substantial run state, but recovery still has hard gaps:

1. `continue_run` rejects runs in `running` status, even when the original owner is dead.
2. `operation.lock` is create/delete only; stale lock takeover is not modeled.
3. Slot facts are appended to JSONL directly; crash points can still create duplicate/partial physical rows.
4. Recovery path marks recovered active trials as `worker_lost`, but no explicit recover command or full reconciliation contract exists.

The result: users can still lose momentum at the experiment level even though trial artifacts are persisted.

---

## Scope

### In scope

1. Durable run-owner lease and explicit stale-owner takeover.
2. Explicit user-facing recover flow (no hidden auto-recovery required).
3. Slot-level commit protocol with durable journal and exactly-once publication semantics.
4. Recovery reconciliation algorithm for `running` or inconsistent run state.
5. Analysis read contract that only includes committed slot publications.

### Out of scope

1. Durable in-flight task memory or process reattachment as a required behavior.
2. State-policy recovery for mutable cross-trial chain state (`persist_per_task`, `accumulate`) in this patch.
3. Remote worker protocol redesign beyond required recovery metadata propagation.

---

## Product Contract

### Durable Experiment Contract

A run is durable iff:

1. Every committed `schedule_idx` remains committed after crash/restart.
2. `next_schedule_index` can be reconstructed exactly from committed records.
3. In-flight (uncommitted) slots may be retried.
4. User can recover and continue without restarting the full experiment.

### Disposable Task Contract

1. Task execution is allowed to restart.
2. Task attempts are not required to be unique.
3. Exactly-once guarantee applies to **committed slot publication**, not process attempt count.

---

## User Experience

No implicit auto-recovery is required for correctness.

### New command: `lab recover`

```bash
lab recover --run-dir .lab/runs/<run_id> [--force] [--json]
```

Behavior:

1. Acquires operation lease (stale-aware).
2. Validates run ownership lease staleness (or `--force`).
3. Reconciles committed slots and progress cursor.
4. Clears unrecoverable in-flight ownership, marks run `interrupted`.
5. Writes recovery report artifact.

Output includes:

1. `run_id`
2. `previous_status`
3. `recovered_status`
4. `rewound_to_schedule_idx`
5. `active_trials_released`
6. `committed_slots_verified`
7. `notes`

### Continue flow

```bash
lab continue --run-dir .lab/runs/<run_id>
```

Rules:

1. `continue` remains for terminal continuable statuses (`failed`, `paused`, `interrupted`).
2. `continue` on `running` returns actionable error: run `lab recover` first (or pass future explicit takeover flag if added).

### Resume flow (unchanged semantics)

`resume` remains trial-checkpoint continuation from paused trial artifacts, not run crash recovery.

---

## State Model Changes

## 1) Run owner lease (new)

Path:

1. `.lab/runs/<run_id>/runtime/engine_lease.json`

Schema: `engine_lease_v1` (new schema file).

Required fields:

1. `schema_version = "engine_lease_v1"`
2. `run_id`
3. `owner_id` (UUID)
4. `pid`
5. `hostname`
6. `started_at`
7. `heartbeat_at`
8. `expires_at`
9. `epoch` (monotonic fencing token)

Rules:

1. Runner heartbeat updates lease every `T_heartbeat` (default 2s).
2. Lease considered stale when `now > expires_at`.
3. `recover` may adopt stale lease by writing same file with `epoch+1` atomically.

## 2) Operation lease replaces bare lock

Replace `runtime/operation.lock` with:

1. `.lab/runs/<run_id>/runtime/operation_lease.json`

Schema: `operation_lease_v1`.

Fields:

1. `operation_id`
2. `op_type` (`continue|recover|pause|kill|resume|fork|replay`)
3. `owner_pid`
4. `owner_host`
5. `acquired_at`
6. `expires_at`

Rules:

1. Acquire via atomic create.
2. If exists and stale, steal with explicit stale marker.
3. If exists and fresh, fail with `operation_in_progress`.

---

## Commit Durability Model

Commit unit is **slot publication** (`schedule_idx`).

## 1) Slot commit journal (new)

Path:

1. `.lab/runs/<run_id>/runtime/slot_commit_journal.jsonl`

Schema per row: `slot_commit_record_v1`.

Record types:

1. `intent`
2. `commit`
3. `abort`

Common fields:

1. `run_id`
2. `schedule_idx`
3. `slot_commit_id`
4. `trial_id`
5. `attempt`
6. `recorded_at`

`intent` fields:

1. `expected_rows.trials`
2. `expected_rows.metrics`
3. `expected_rows.events`
4. `expected_rows.variant_snapshots`
5. `expected_rows.evidence`
6. `expected_rows.chain_states`
7. `payload_digest`

`commit` fields:

1. `written_rows.*` (same structure)
2. `facts_fsync_completed` (bool)
3. `runtime_fsync_completed` (bool)

## 2) Fact row identity changes (required)

Add to every persisted fact/evidence row:

1. `schedule_idx`
2. `slot_commit_id`
3. `attempt`
4. `row_seq` (per-row-type sequence within slot)

This applies to:

1. `facts/trials.jsonl`
2. `facts/metrics_long.jsonl`
3. `facts/events.jsonl`
4. `facts/variant_snapshots.jsonl`
5. `evidence/evidence_records.jsonl`
6. `evidence/task_chain_states.jsonl`
7. `benchmark/predictions.jsonl`
8. `benchmark/scores.jsonl`

## 3) Schedule progress schema bump

Add `schedule_progress_v2` with:

1. `schema_version = "schedule_progress_v2"`
2. `completed_slots[]` entry includes:
   1. `schedule_index`
   2. `trial_id`
   3. `status`
   4. `slot_commit_id`
   5. `attempt`
3. `next_schedule_index` remains authoritative cursor.

---

## Required Write Ordering (No Hand-Waving)

For each completed slot:

1. Build full deferred payload and deterministic `slot_commit_id`.
2. Append `intent` record to `slot_commit_journal.jsonl`; fsync file + directory.
3. Append all fact/evidence/benchmark rows with `slot_commit_id`; fsync each touched file + parent dir.
4. Append `commit` record to journal; fsync journal + parent dir.
5. Atomically write `schedule_progress_v2` with committed slot and advanced cursor; fsync runtime dir.
6. Update `run_control` active set/status atomically.

If crash occurs:

1. `intent` without `commit` => slot uncommitted and eligible for rerun.
2. `commit` present but `schedule_progress` behind => recovery advances cursor idempotently.
3. Rows without committed `slot_commit_id` are ignored by analysis contract.

---

## Recovery Algorithm

`lab recover --run-dir <run_dir>`:

1. Acquire stale-aware operation lease.
2. Load and validate:
   1. `run_control`
   2. `run_session_state`
   3. `schedule_progress_v2` (or migrate v1 -> v2 in memory)
   4. `slot_commit_journal`
   5. `engine_lease`
3. Validate ownership:
   1. If owner lease fresh and not `--force`, fail with `run_owner_alive`.
   2. If stale or forced, adopt lease (`epoch+1`).
4. Build `committed_by_schedule_idx` from journal `commit` rows.
5. Reconcile progress:
   1. Find first `schedule_idx` where `schedule_progress` and `committed_by_schedule_idx` diverge.
   2. Rewind `next_schedule_index` to first divergent index.
   3. Truncate `completed_slots` after rewind boundary.
6. Resolve active trials from `run_control.active_trials`:
   1. If their `schedule_idx` is committed, drop from active set.
   2. Else mark trial state `failed` with `exit_reason = "worker_lost_recovered"` and leave slot uncommitted for rerun.
7. Write:
   1. reconciled `schedule_progress_v2`
   2. `run_control` status `interrupted`, `active_trials={}`
   3. `runtime/recovery_report.json`
8. Return summary to user; run is now continuable.

---

## Analysis Contract Change

Analysis must only read committed publications.

Rule:

1. A fact row is visible iff its `slot_commit_id` has a corresponding `commit` journal record and `schedule_idx < next_schedule_index` (or equivalent committed set membership).

This prevents duplicate physical rows from affecting derived metrics.

---

## Continue Semantics After Patch

`continue` must:

1. Load `schedule_progress_v2` + commit journal.
2. Refuse `running` unless explicit takeover path is requested (or user ran `recover`).
3. Resume from `next_schedule_index`.
4. Preserve deterministic schedule ordering.
5. Never require full experiment restart for stale-owner crashes.

---

## Persisting Location and Writer Ownership

Yes, where persistence happens matters.

Required rules:

1. Only the runner coordinator commit path writes run-level facts/evidence/progress/journal.
2. Workers/harness write trial-local outputs only (`trials/<trial_id>/out/...`).
3. Commit persistence occurs on the same durability plane as run metadata (same filesystem semantics).
4. `rename`-atomic + fsync semantics are required for durability claims.

If filesystem semantics are weak/non-atomic (certain network filesystems), runner must:

1. Emit durability downgrade warning.
2. Mark attestation durability grade accordingly.

---

## Schema/Artifact Changes

New schemas:

1. `schemas/engine_lease_v1.jsonschema`
2. `schemas/operation_lease_v1.jsonschema`
3. `schemas/slot_commit_record_v1.jsonschema`
4. `schemas/schedule_progress_v2.jsonschema`
5. `schemas/recovery_report_v1.jsonschema`

Updated schemas:

1. `schemas/run_control_v2.jsonschema` (or introduce `run_control_v3` if adding lease linkage fields)
2. Fact row schemas once formalized (if currently code-defined only, formalize now)

New artifacts:

1. `runtime/engine_lease.json`
2. `runtime/operation_lease.json`
3. `runtime/slot_commit_journal.jsonl`
4. `runtime/recovery_report.json`

---

## Implementation Plan

## Phase 1: Recovery Surface + Leases

1. Add `lab recover` command.
2. Add engine lease writer/heartbeat in runner loop.
3. Replace `operation.lock` with stale-aware operation lease.

## Phase 2: Slot Commit Journal

1. Add journal writer and schema validation.
2. Add `slot_commit_id` and `schedule_idx` to all deferred rows.
3. Wire strict write ordering with fsync barriers.

## Phase 3: Progress v2 + Reconciliation

1. Add `schedule_progress_v2`.
2. Add v1->v2 read migration.
3. Implement deterministic reconcile algorithm.

## Phase 4: Analysis Visibility Gate

1. Update analysis views to include committed-slot filter.
2. Verify metrics parity uninterrupted vs crash+recover+continue.

---

## Acceptance Criteria

1. Crash at any commit step never requires full experiment restart.
2. `lab recover` converts stale `running` run into continuable state deterministically.
3. No committed slot is lost after recovery.
4. No uncommitted slot is treated as committed.
5. Crash+recover+continue produces identical final committed slot set to uninterrupted run.
6. Duplicate physical rows (if any) do not alter analysis outputs.

---

## Test Matrix (Required)

Fault-injection tests must crash runner at each point:

1. Before `intent` write.
2. After `intent`, before fact append.
3. After fact append, before `commit`.
4. After `commit`, before `schedule_progress_v2`.
5. After `schedule_progress_v2`, before `run_control`.

For each crash point:

1. Run `lab recover`.
2. Run `lab continue`.
3. Assert final committed slot set equals uninterrupted baseline.
4. Assert analysis outputs match baseline.

Additional tests:

1. Stale operation lease takeover works and is fenced by epoch.
2. Active trial orphaned during crash is rerun, not treated committed.
3. `resume` behavior remains checkpoint/fork scoped and does not replace `recover`.

---

## Non-Negotiable Invariants

1. Exactly-once applies to slot publication, not attempt execution.
2. A slot is committed iff journal contains `commit` for its `slot_commit_id`.
3. `schedule_progress_v2.next_schedule_index` must be derivable from committed slot prefix.
4. Runner is single writer for run-level durable state.
5. Users can always recover and continue a stale `running` experiment without starting over.
