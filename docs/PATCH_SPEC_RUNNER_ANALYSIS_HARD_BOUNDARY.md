# Patch Spec: Runner/Analysis Hard Boundary (No Backcompat)

## Goal

Make `lab-runner` an execution orchestrator only.

- Runner writes append-only trial facts through a storage interface.
- Analysis/DuckDB reads persisted facts on demand.
- Remove runner-owned analysis aggregates and full-run in-memory summary vectors.
- No backwards compatibility path to legacy analysis files (`analysis/summary.json`, `analysis/comparisons.json`).

---

## Why This Patch

Current flow mixes boundaries:

- Runner tracks analysis state in RAM (`trial_summaries`, `event_counts`, `trial_event_counts`).
- Runner calls analysis writer in-loop, which rewrites full JSONL tables repeatedly.
- This creates O(n^2) work and unbounded per-run RAM growth with trial count.

This patch removes that coupling.

---

## Scope

### In scope

- Define and adopt a runner storage sink interface.
- Convert runner to append-only fact writes.
- Delete runner analysis RAM tracking and rebuild logic.
- Move aggregation responsibility to analysis query layer.
- Keep DuckDB views queryable from persisted facts at any time.

### Out of scope

- Legacy file compatibility shims.
- Retaining old summary/comparisons file contracts.
- Remote executor implementation details (interface allows it; full remote support is separate).

---

## Hard Boundary

### Runner owns

- Scheduling/execution control flow.
- Trial lifecycle state machine.
- Writing immutable fact records (append-only).
- Minimal bounded runtime state required for execution correctness.

### Analysis owns

- Aggregations (variant success rates, comparisons, rankings, trends).
- DuckDB view definitions and materialization policy.
- Query APIs (`lab views`, `lab query`, `lab trend`, etc.).

---

## Explicit Deletions

Delete these runner-side analysis paths and RAM structures.

### `rust/crates/lab-runner/src/lib.rs`

1. Delete in-memory analysis accumulators:
- `trial_summaries: Vec<Value>`
- `event_counts: BTreeMap<String, BTreeMap<String, usize>>`
- `trial_event_counts: BTreeMap<String, BTreeMap<String, usize>>`

2. Delete rebuild path:
- `rebuild_all_trial_summaries(...)`

3. Delete runner-level score patching pass over summaries:
- `apply_score_records_to_trial_summaries(...)`

4. Delete runner calls to analysis batch writer:
- `write_analysis(...)` from fresh run path
- `write_analysis(...)` from continue path
- `write_analysis(...)` from per-trial loop

5. Remove `summarize_trial(...)` dependency from runner.

### `rust/crates/lab-analysis/src/lib.rs`

1. Delete batch write API used by runner:
- `write_analysis(...)`
- `write_analysis_tables(...)`

2. Remove any assumption that analysis tables are rewritten as full snapshots.

3. Keep/expand read/query layer only:
- DuckDB view SQL
- query functions
- optional explicit materialization routines

### `rust/crates/lab-cli/src/main.rs`

1. Remove remaining legacy assumptions about precomputed analysis summaries.
2. Source run-level metrics from canonical fact-derived views/tables only.

---

## New Storage Interface Boundary

Add a runner-owned sink abstraction in `lab-runner`:

`rust/crates/lab-runner/src/sink.rs` (new)

```rust
pub trait RunSink {
    fn write_run_manifest(&mut self, run: &RunManifestRecord) -> Result<()>;
    fn append_trial_record(&mut self, row: &TrialRecord) -> Result<()>;
    fn append_metric_rows(&mut self, rows: &[MetricRow]) -> Result<()>;
    fn append_event_rows(&mut self, rows: &[EventRow]) -> Result<()>;
    fn append_variant_snapshot(&mut self, rows: &[VariantSnapshotRow]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}
```

Initial implementation:

- `JsonlRunSink` -> append-only JSONL files under `.lab/runs/<run_id>/facts/`.

Future implementations enabled by boundary:

- `DuckDbRunSink`
- `SqliteRunSink`
- `PostgresRunSink`
- object/blob sink

Runner does not know destination internals.

---

## Canonical Persisted Facts (Append-Only)

Write these files once per record append, never full-table rewrite:

- `facts/trials.jsonl`
- `facts/metrics_long.jsonl`
- `facts/events.jsonl` (or split by trial/variant views in analysis layer)
- `facts/variant_snapshots.jsonl`

Optional run metadata:

- `facts/run_manifest.json`

All downstream analysis is derived from these fact streams.

---

## Exact Write Stages

### Stage 0: Run bootstrap

Runner writes:

- `manifest.json`
- `resolved_experiment.json` + digest
- `resolved_variants.json` + digest
- `resolved_schedule.json` + digest
- sink bootstrap metadata (`facts/run_manifest.json`)

### Stage 1: Trial start

Runner writes control/state artifacts required for execution:

- `trial_state.json` -> `running`
- `trial_input.json`
- `trial_metadata.json` (including `variant_digest`)

No analysis aggregation in this stage.

### Stage 2: Trial completion

Runner appends facts via `RunSink`:

- one `TrialRecord` row
- zero or more `MetricRow` rows
- zero or more `EventRow` rows
- optional `VariantSnapshotRow` (if needed for resolved runtime evidence)

Runner then updates execution state (`schedule_progress`, `trial_state`, `run_control`).

No full read-modify-write of all previous trials.

### Stage 3: Analysis query/materialization (on demand)

Analysis layer:

- queries fact JSONL directly using DuckDB `read_json_auto(...)`, or
- materializes `analysis/agentlab.duckdb` when explicitly requested.

Runner does not trigger heavy aggregation passes.

---

## RAM Cleanup Requirements

Per-run memory must be bounded by execution control state, not trial count.

### Allowed in RAM

- current trial runtime state
- scheduler cursor/progress
- pruning/retry counters
- chain/checkpoint state for active logic
- small bounded caches (if needed, explicitly bounded)

### Forbidden in RAM

- full history vector of trial summaries
- variant-wide aggregate maps derived from all completed trials
- loading old trial outputs just to continue running

Acceptance rule:

- memory usage should not grow linearly with number of completed trials in a single run, excluding unavoidable per-trial execution working buffers.

---

## DuckDB Layer Plan

### Input model

DuckDB layer reads from `facts/*.jsonl` as the source of truth.

### Views

Define views over fact files:

- `trials`
- `metrics_long`
- `events`
- `variant_summary` (derived view, not physically rewritten by runner)
- `task_variant_matrix` (task x variant comparison)
- `run_progress` (live completion stats)

### Materialization policy

- Default: query directly from JSONL (no forced rematerialization per trial).
- Optional explicit command: `lab views refresh` to rebuild a persistent DuckDB DB.
- Optional background refresh mode is analysis-owned, not runner-owned.

---

## Migration / Cutover Plan

### Phase 1: Interface + append-only writes

1. Add `RunSink` trait + `JsonlRunSink`.
2. Write fact rows in runner trial loop.
3. Keep legacy analysis code compiled but unused.

### Phase 2: Remove old analysis coupling

1. Remove runner calls to `summarize_trial`/`write_analysis`.
2. Remove runner RAM analysis accumulators.
3. Remove `rebuild_all_trial_summaries` and summary patching path.

### Phase 3: Analysis hard cut

1. Remove `write_analysis`/`write_analysis_tables` APIs from `lab-analysis`.
2. Move/replace any callers with query-time derivation.
3. Ensure CLI views use fact-derived views only.

### Phase 4: Cleanup and validation

1. Remove dead files/branches/tests for legacy summary outputs.
2. Update docs/specs to reflect fact-first architecture.

---

## Acceptance Criteria

1. Runner no longer allocates trial-history analysis vectors/maps.
2. Runner no longer calls batch analysis writers.
3. Runner only appends per-trial facts and updates execution state.
4. DuckDB queries can run during active execution against persisted facts.
5. No per-trial full-table rewrite occurs.
6. No legacy summary/comparisons compatibility path remains.

---

## Test Plan

1. Unit: `JsonlRunSink` append semantics and schema validation.
2. Unit: runner loop writes exactly one trial row per completed trial.
3. Integration: long run (e.g. 500+ trials) shows stable memory profile.
4. Integration: query mid-run returns partial but consistent aggregates.
5. Integration: continue-run appends new facts without reloading full prior trial history.

---

## Risks and Mitigations

1. Risk: fact schema churn breaks queries.
- Mitigation: versioned fact row schemas + strict validation tests.

2. Risk: query performance on large JSONL.
- Mitigation: optional analysis-owned DuckDB refresh/materialization; partitioned fact files if needed.

3. Risk: duplicate/partial rows on crash.
- Mitigation: atomic append discipline and idempotent keys (`run_id`,`trial_id`,`row_type`,`seq`).

