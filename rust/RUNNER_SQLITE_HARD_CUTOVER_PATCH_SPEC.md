# Runner Persistence Hard Cutover Patch Spec (SQLite Canonical)

Status: Proposed  
Owner: `lab-runner`  
Scope: Hard cutover only. No dual write. No compatibility shim.

## 1. Hard Decisions

1. Canonical runtime persistence is a single SQLite database per run: `run_dir/run.sqlite`.
2. JSONL facts, runtime JSON state files, and evidence JSONL are removed as canonical stores.
3. Full workspace copies under `trials/*/workspace` and `evidence/chains/*/*workspace*` are removed.
4. Large data is persisted by reference through a content addressed object store, not by repeated copies.
5. `lab-analysis` reads from SQLite canonical data (directly or via ephemeral in memory DuckDB). No persisted `analysis/agentlab.duckdb`.

## 2. SQLite Typing Answer

SQLite has types, but uses dynamic typing with storage classes (`NULL`, `INTEGER`, `REAL`, `TEXT`, `BLOB`).

For this cutover we will enforce types aggressively with:

1. `STRICT` tables (SQLite 3.37+).
2. `CHECK` constraints for enum like fields and value ranges.
3. JSON columns stored as `TEXT` + `CHECK(json_valid(column))`.
4. `FOREIGN KEY` constraints with `PRAGMA foreign_keys=ON`.

Result: we keep SQLite flexibility but get strong schema discipline close to typed DB behavior.

## 3. Current Write Surface To Replace

These are current write entry points in `crates/lab-runner/src/lib.rs` and `src/sink.rs`:

1. Run/runtime JSON files:
   - `write_run_control_v2`
   - `write_run_session_state`
   - `write_schedule_progress`
   - `append_slot_commit_record`
   - `persist_pending_trial_completions`
   - `write_engine_lease`
   - `write_parallel_worker_control_state`
2. Trial filesystem materialization:
   - `TrialPaths::prepare` (`in/state/out/deps/workspace/tmp`)
   - `prepare_io_paths` (`in/trial_input.json`, `in/task.json`, `in/bindings.json`, `in/dependencies.json`, `in/policy.json`)
   - `materialize_trial_result` (`out/result.json` copied to root `result.json`)
   - `write_state_inventory`
3. Evidence/chain writes:
   - `append_jsonl` to `evidence/evidence_records.jsonl` and `evidence/task_chain_states.jsonl`
   - workspace snapshots/diffs/patch JSON files in `trial/evidence`
   - chain root and step full workspace copies
4. Facts sink JSONL:
   - `RunSink` / `JsonlRunSink` writing `facts/trials.jsonl`, `metrics_long.jsonl`, `events.jsonl`, `variant_snapshots.jsonl`, `run_manifest.json`.

## 4. Target Canonical Data Model

All tables are `STRICT`.

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE runs (
  run_id TEXT PRIMARY KEY,
  experiment_key TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('running','paused','interrupted','failed','completed','killed','preflight_failed')),
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  materialization_mode TEXT NOT NULL CHECK(materialization_mode IN ('none','metadata_only','outputs_only','full')),
  baseline_variant_id TEXT NOT NULL,
  workload_type TEXT NOT NULL,
  resolved_experiment_json TEXT NOT NULL CHECK(json_valid(resolved_experiment_json)),
  resolved_experiment_digest TEXT NOT NULL
) STRICT;

CREATE TABLE run_runtime_state (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
  next_schedule_index INTEGER NOT NULL,
  next_trial_index INTEGER NOT NULL,
  total_slots INTEGER NOT NULL,
  schedule_json TEXT NOT NULL CHECK(json_valid(schedule_json)),
  completed_slots_json TEXT NOT NULL CHECK(json_valid(completed_slots_json)),
  pruned_variants_json TEXT NOT NULL CHECK(json_valid(pruned_variants_json)),
  consecutive_failures_json TEXT NOT NULL CHECK(json_valid(consecutive_failures_json)),
  updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE run_leases (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
  operation_lease_json TEXT CHECK(operation_lease_json IS NULL OR json_valid(operation_lease_json)),
  engine_lease_json TEXT CHECK(engine_lease_json IS NULL OR json_valid(engine_lease_json)),
  parallel_worker_control_json TEXT CHECK(parallel_worker_control_json IS NULL OR json_valid(parallel_worker_control_json)),
  updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE variants (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  variant_id TEXT NOT NULL,
  is_baseline INTEGER NOT NULL CHECK(is_baseline IN (0,1)),
  args_json TEXT NOT NULL CHECK(json_valid(args_json)),
  bindings_json TEXT NOT NULL CHECK(json_valid(bindings_json)),
  env_json TEXT NOT NULL CHECK(json_valid(env_json)),
  PRIMARY KEY (run_id, variant_id)
) STRICT;

CREATE TABLE trial_slots (
  slot_key TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  schedule_idx INTEGER NOT NULL,
  variant_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  task_index INTEGER NOT NULL,
  repl_idx INTEGER NOT NULL,
  chain_label TEXT NOT NULL,
  chain_key TEXT NOT NULL,
  UNIQUE (run_id, schedule_idx),
  FOREIGN KEY (run_id, variant_id) REFERENCES variants(run_id, variant_id)
) STRICT;

CREATE TABLE trial_attempts (
  attempt_key TEXT PRIMARY KEY,
  slot_key TEXT NOT NULL REFERENCES trial_slots(slot_key) ON DELETE CASCADE,
  attempt_no INTEGER NOT NULL,
  trial_id TEXT NOT NULL,
  slot_status TEXT NOT NULL CHECK(slot_status IN ('completed','failed','skipped_pruned')),
  outcome TEXT,
  status_code TEXT,
  failure_classification TEXT,
  started_at_ms INTEGER NOT NULL,
  ended_at_ms INTEGER NOT NULL,
  duration_ms REAL NOT NULL,
  row_payload_digest TEXT NOT NULL,
  UNIQUE (slot_key, attempt_no),
  UNIQUE (trial_id)
) STRICT;

CREATE TABLE trial_records (
  attempt_key TEXT NOT NULL REFERENCES trial_attempts(attempt_key) ON DELETE CASCADE,
  row_seq INTEGER NOT NULL,
  baseline_id TEXT NOT NULL,
  workload_type TEXT NOT NULL,
  primary_metric_name TEXT NOT NULL,
  primary_metric_value_json TEXT NOT NULL CHECK(json_valid(primary_metric_value_json)),
  metrics_json TEXT NOT NULL CHECK(json_valid(metrics_json)),
  bindings_json TEXT NOT NULL CHECK(json_valid(bindings_json)),
  hook_events_total INTEGER NOT NULL,
  has_hook_events INTEGER NOT NULL CHECK(has_hook_events IN (0,1)),
  PRIMARY KEY (attempt_key, row_seq)
) STRICT;

CREATE TABLE metric_rows (
  attempt_key TEXT NOT NULL REFERENCES trial_attempts(attempt_key) ON DELETE CASCADE,
  row_seq INTEGER NOT NULL,
  metric_name TEXT NOT NULL,
  metric_value_json TEXT NOT NULL CHECK(json_valid(metric_value_json)),
  metric_source TEXT,
  PRIMARY KEY (attempt_key, row_seq)
) STRICT;

CREATE TABLE event_rows (
  attempt_key TEXT NOT NULL REFERENCES trial_attempts(attempt_key) ON DELETE CASCADE,
  row_seq INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  ts TEXT,
  payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
  PRIMARY KEY (attempt_key, row_seq)
) STRICT;

CREATE TABLE variant_snapshot_rows (
  attempt_key TEXT NOT NULL REFERENCES trial_attempts(attempt_key) ON DELETE CASCADE,
  row_seq INTEGER NOT NULL,
  binding_name TEXT NOT NULL,
  binding_value_json TEXT NOT NULL CHECK(json_valid(binding_value_json)),
  binding_value_text TEXT NOT NULL,
  PRIMARY KEY (attempt_key, row_seq)
) STRICT;

CREATE TABLE objects (
  object_ref TEXT PRIMARY KEY,
  sha256_hex TEXT NOT NULL UNIQUE,
  size_bytes INTEGER NOT NULL,
  media_type TEXT NOT NULL,
  storage_relpath TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE attempt_objects (
  attempt_key TEXT NOT NULL REFERENCES trial_attempts(attempt_key) ON DELETE CASCADE,
  role TEXT NOT NULL,
  object_ref TEXT NOT NULL REFERENCES objects(object_ref),
  PRIMARY KEY (attempt_key, role)
) STRICT;

CREATE TABLE lineage_heads (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  chain_key TEXT NOT NULL,
  latest_version_id TEXT NOT NULL,
  step_index INTEGER NOT NULL,
  PRIMARY KEY (run_id, chain_key)
) STRICT;

CREATE TABLE lineage_versions (
  version_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  chain_key TEXT NOT NULL,
  step_index INTEGER NOT NULL,
  trial_id TEXT NOT NULL,
  parent_version_id TEXT,
  pre_snapshot_ref TEXT,
  post_snapshot_ref TEXT,
  diff_incremental_ref TEXT,
  diff_cumulative_ref TEXT,
  patch_incremental_ref TEXT,
  patch_cumulative_ref TEXT
) STRICT;

CREATE TABLE benchmark_predictions (
  attempt_key TEXT NOT NULL REFERENCES trial_attempts(attempt_key) ON DELETE CASCADE,
  row_seq INTEGER NOT NULL,
  prediction_json TEXT NOT NULL CHECK(json_valid(prediction_json)),
  PRIMARY KEY (attempt_key, row_seq)
) STRICT;

CREATE TABLE benchmark_scores (
  attempt_key TEXT NOT NULL REFERENCES trial_attempts(attempt_key) ON DELETE CASCADE,
  row_seq INTEGER NOT NULL,
  score_json TEXT NOT NULL CHECK(json_valid(score_json)),
  PRIMARY KEY (attempt_key, row_seq)
) STRICT;
```

## 5. Idempotent Key Strategy

1. `slot_key = sha256(run_id + schedule_idx + variant_id + task_id + repl_idx)`.
2. `attempt_key = sha256(slot_key + attempt_no + payload_digest)`.
3. `version_id = sha256(run_id + chain_key + step_index + trial_id)`.
4. `object_ref = artifact://sha256/<hex>` (existing format retained).
5. All writes are `INSERT ... ON CONFLICT DO UPDATE` or `DO NOTHING` where appropriate.

## 6. Sink Interface Replacement

Current `RunSink` in `crates/lab-runner/src/sink.rs` is JSONL append oriented.  
Hard cutover means this abstraction becomes harmful because it separates fact writes from runtime state writes.

### Decision

1. Delete `JsonlRunSink`.
2. Replace `RunSink` with concrete transactional store: `SqliteRunStore`.
3. `SqliteRunStore` owns one `rusqlite::Connection` and explicit transaction boundaries.
4. Slot commit is a single DB transaction:
   - write trial/evidence/metrics/events/benchmark rows
   - update `run_runtime_state`
   - update run status metadata
   - commit atomically

### New API (in `lab-runner`)

```rust
pub struct SqliteRunStore { /* conn, prepared statements */ }

impl SqliteRunStore {
    pub fn open(run_dir: &Path, run_id: &str, resolved: &Value, execution: &RunExecutionOptions) -> Result<Self>;
    pub fn begin_slot_commit(&mut self, schedule_idx: usize, attempt: usize, slot_commit_id: &str) -> Result<()>;
    pub fn write_trial_bundle(&mut self, bundle: &TrialPersistenceBundle) -> Result<()>;
    pub fn finalize_slot_commit(&mut self, progress: &ScheduleProgress) -> Result<()>;
    pub fn update_run_status(&mut self, status: &str, active_trials: &[RunControlActiveTrial], pause: Option<&RunControlPauseMetadata>) -> Result<()>;
}
```

Notes:

1. `flush()` no longer exists. `COMMIT` is durability barrier.
2. Run control and run session become DB rows, not JSON files.
3. `BEGIN IMMEDIATE` is used for slot commit serialization.

## 7. Filesystem Shape After Cutover

Canonical:

1. `<run_dir>/run.sqlite`
2. `<run_dir>/objects/sha256/<digest>/blob` (or project global object store if configured)

Ephemeral only:

1. `<run_dir>/scratch/...` for live process IO/mounts.
2. Scratch is always eligible for deletion and excluded from audit footprint.

Removed:

1. `facts/*`
2. `runtime/*.json` and `runtime/*.jsonl`
3. `evidence/evidence_records.jsonl`
4. `evidence/task_chain_states.jsonl`
5. `evidence/chains/*/*workspace*` full directory snapshots
6. `trials/*/{in,state,out,deps,workspace,tmp}` as persisted artifacts
7. duplicated root files: `trials/*/trial_input.json`, `trials/*/result.json`
8. `analysis/agentlab.duckdb` persisted file

## 8. Patch Set (Detailed)

## P0: Add SQLite persistence module

Files:

1. Add `crates/lab-runner/src/persistence/mod.rs`
2. Add `crates/lab-runner/src/persistence/schema_v2.sql`
3. Add `crates/lab-runner/src/persistence/sqlite_store.rs`
4. Update `crates/lab-runner/Cargo.toml`:
   - `rusqlite = { version = "...", features = ["bundled"] }`

Implementation:

1. Create schema bootstrap on run open.
2. Implement prepared statements for all hot write paths.
3. Ensure `PRAGMA journal_mode=WAL`, `synchronous=FULL`, `temp_store=MEMORY`.

## P1: Replace runtime JSON write functions with DB updates

In `crates/lab-runner/src/lib.rs`:

1. Replace `write_run_control_v2` calls with `SqliteRunStore::update_run_status`.
2. Replace `write_run_session_state` with insertion into `runs` + run options columns.
3. Replace `write_schedule_progress` with update to `run_runtime_state`.
4. Replace `append_slot_commit_record` and `persist_pending_trial_completions` with `slot_commit` and `pending_completion` tables.
5. Replace engine lease JSON with `run_leases.engine_lease_json`.
6. Replace parallel worker control JSON with `run_leases.parallel_worker_control_json`.

## P2: Remove JSONL facts sink and route through DB transaction

Files:

1. Delete `crates/lab-runner/src/sink.rs` JSONL implementation.
2. Introduce `TrialPersistenceBundle` DTO in persistence module.
3. Update `RunCoordinator::commit_trial_slot` to write bundle via store.

Implementation:

1. Remove `JsonlRunSink::new` and all `append_*` calls.
2. Persist `TrialRecord`, `MetricRow`, `EventRow`, `VariantSnapshotRow` as normalized SQL rows.
3. Keep row ordering using `(attempt_key, row_seq)`.

## P3: Stop persisting trial materialization directories

In `lib.rs`:

1. Replace `TrialPaths::prepare` persistent directories with scratch directories.
2. `prepare_io_paths` writes only to scratch.
3. Remove canonical copy-back (`materialize_trial_result`) to trial root path.
4. Persist input/output/event payloads as object refs (`objects`, `attempt_objects`).
5. Remove `apply_materialization_policy` for canonical persistence; keep only scratch cleanup.

## P4: Replace chain workspace copies with lineage refs only

In `lib.rs` around chain handling:

1. Remove `copy_dir_filtered` calls that write chain root and step workspace directories.
2. Keep snapshot manifest and diff generation, but persist only object refs.
3. Write lineage progression to `lineage_versions` and `lineage_heads`.
4. Restore behavior (when state policy requires it) reads from refs and reconstructs into scratch workspace only.

## P5: CLI artifact and runs listing updates

In `crates/lab-cli/src/main.rs`:

1. `run_artifacts_to_json` returns DB paths/keys instead of JSONL paths.
2. `runs` command summary reads counts and outcomes from `run.sqlite` (not `facts/trials.jsonl`).
3. In flight scoreboard falls back to runtime state from DB.

## P6: Analysis layer hard cutover

In `crates/lab-analysis/src/lib.rs`:

1. Remove dependency on `facts/*.jsonl` and `runtime/*.json`.
2. Source all views from SQLite tables.
3. If DuckDB views are retained, materialize in memory from SQLite source only (no persisted analysis DB).
4. Remove `ensure_fact_files`, `ensure_runtime_files`, and run local `analysis/agentlab.duckdb` writes.

## P7: Replay/Fork/Resume adaptation

In `lib.rs`:

1. Replace trial input loading from `trials/<id>/trial_input.json` with DB lookup by `trial_id` and `attempt_objects(role='trial_input')`.
2. Replace checkpoint token discovery from filesystem with lineage/version tables.
3. Replace fork/replay manifests with DB rows (`forks`, `replays` tables or JSON payload columns under runtime ops table).

## P8: Delete legacy write code and dead tests

1. Remove file based helper functions that are no longer used:
   - JSON state writers
   - JSONL append utilities
   - materialization cleanup policy tied to persisted trial dirs
2. Replace tests that assert file existence with DB assertions.

## 9. Transaction and Durability Rules

1. Every slot commit runs in one SQLite transaction.
2. Ordering:
   - insert/update `trial_attempts` and dependent rows
   - update lineage
   - update run progress/status
   - commit
3. On crash before commit: no partial slot visible.
4. On crash after commit: fully durable with WAL+FULL sync.

## 10. Backward Compatibility Policy

No compatibility fallback in runtime path.

1. Runner only supports `run.sqlite` format after cutover.
2. Existing legacy run directories are treated as read only historical artifacts.
3. Optional offline migration tool can be provided later as a separate command, not in runner hot path.

## 11. Acceptance Criteria

1. Fresh run writes exactly one canonical file (`run.sqlite`) plus object blobs.
2. No `facts/`, `runtime/*.json`, `runtime/*.jsonl`, `evidence/*.jsonl`, `trials/*/workspace` are created.
3. Restart/resume recovers entirely from DB state.
4. Slot commit idempotency verified by rerunning same commit payload with no duplicate rows.
5. Disk footprint reduction:
   - eliminate repeated workspace copy classes from audit (`trials.workspace_copy`, `evidence.chain_root_workspace_copy`, `evidence.chain_step_workspace_copy` near zero).
6. `lab views/query/runs` still return equivalent semantics from DB backed reads.

## 12. Test Plan

1. Unit tests:
   - key generation determinism
   - object ref dedupe
   - strict schema validation failures
2. Integration tests:
   - run -> interrupt -> continue
   - parallel schedule with deterministic commit order
   - replay/fork/resume DB roundtrip
3. Regression tests:
   - no persisted trial workspace dirs
   - no JSONL facts files
   - no runtime JSON control files
4. Property checks:
   - `run_runtime_state.next_schedule_index == max(committed schedule_idx)+1`
   - each `trial_attempts` has matching metric/event/trial bundle cardinality.

