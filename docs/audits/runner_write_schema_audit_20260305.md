# Runner Write/Schema Audit (Experiments)

Date: 2026-03-05  
Scope: `rust/crates/lab-runner` runtime/build outputs, `rust/crates/lab-runner/src/persistence/schema_v2.sql`, and schema files in `schemas/`.

## 1) Write Planes (Current Reality)

The runner currently writes across multiple planes:

1. Build package plane: `.lab/builds/...` sealed package artifacts.
2. Run metadata plane: run root files (`manifest.json`, resolved files, variants/schedule).
3. Structured ledger plane: `run.sqlite` + WAL/SHM (canonical row/state store).
4. Artifact blob plane: `artifacts/sha256/<digest>/blob` content-addressed storage.
5. Trial materialization plane: `trials/<trial_id>/...` human-readable per-trial files.
6. Runtime staging plane: `.scratch/...` and `runtime/worker_payload/...` transient scratch/payload.
7. Legacy/compat plane: `facts/`, run-level `evidence/`, runtime JSON mirror filenames.

This is why output appears scattered: canonical state and materialized/debug outputs coexist, with legacy compatibility still present.

## 2) File Write Contract

### 2.1 Build Package Writes (`build_experiment_package`)

| Path pattern | Writer (code) | Format/schema | Lifecycle | Notes |
|---|---|---|---|---|
| `.lab/builds/<id_ts>/tasks/tasks.jsonl` | `build_experiment_package` (`lib.rs:8662`) | JSONL task rows (`task_boundary_v2` rows from dataset) | durable | Input dataset copied into package |
| `.lab/builds/<id_ts>/files/...` | `rewrite_runtime_paths_for_package` path-copy flow (`lib.rs:8676+`) | free-form files | durable | Runtime dependency/support files |
| `.lab/builds/<id_ts>/agent_builds/...` | same (`lib.rs:8657`) | free-form artifacts (tar/dir) | durable | Agent artifact payloads |
| `.lab/builds/<id_ts>/resolved_experiment.json` | `atomic_write_json_pretty` (`lib.rs:8738`) | resolved experiment JSON | durable | No dedicated `resolved_variants/schedule` schema here; experiment schema family exists |
| `.lab/builds/<id_ts>/checksums.json` | `atomic_write_json_pretty` (`lib.rs:8768`) | `sealed_package_checksums_v2` | durable | Has explicit schema_version field |
| `.lab/builds/<id_ts>/package.lock` | `atomic_write_json_pretty` (`lib.rs:8774`) | `sealed_package_lock_v1` | durable | Digest lock |
| `.lab/builds/<id_ts>/manifest.json` | `atomic_write_json_pretty` (`lib.rs:8788`) | `sealed_run_package_v2` | durable | Sealed package manifest |

### 2.2 Run Root Writes (`run_experiment_with_behavior`)

| Path pattern | Writer (code) | Format/schema | Canonical? | Lifecycle | Notes |
|---|---|---|---|---|---|
| `run_<id>/manifest.json` | `atomic_write_json_pretty` (`lib.rs:8845`) | `manifest_v1` | yes (file) | durable | Run header metadata |
| `run_<id>/resolved_experiment.json` | `atomic_write_json_pretty` (`lib.rs:8832`) | resolved experiment JSON | yes (file) | durable | |
| `run_<id>/resolved_experiment.digest` | `atomic_write_bytes` (`lib.rs:8834`) | raw digest bytes | yes (file) | durable | |
| `run_<id>/resolved_variants.json` | `write_resolved_variants` (`lib.rs:11751`) | `resolved_variants_v1` | yes (file) | durable | |
| `run_<id>/resolved_schedule.json` | `write_resolved_schedule` (`lib.rs:11762`) | `resolved_schedule_v1` | yes (file) | durable | |
| `run_<id>/run.sqlite` (+ `-wal`, `-shm`) | `SqliteRunStore::open` (`sqlite_store.rs:140`) | SQLite DB (`schema_v2.sql`) | yes (sqlite) | durable | Main structured store |

### 2.3 Artifact Blob Writes

| Path pattern | Writer (code) | Format/schema | Canonical? | Lifecycle | Notes |
|---|---|---|---|---|---|
| `run_<id>/artifacts/sha256/<hex>/blob` | `ArtifactStore::put_bytes/put_file` (`lab-core/src/lib.rs:149`) | raw bytes, addressed by digest | yes (blob store) | durable | Evidence and IO payload objects |

### 2.4 Trial Materialization Writes

| Path pattern | Writer (code) | Format/schema | Lifecycle | Notes |
|---|---|---|---|---|
| `trials/<trial>/trial_state.json` | `write_trial_state` (`lib.rs:1848`) | `trial_state_v1` | durable | Per-trial status |
| `trials/<trial>/trial_metadata.json` | `atomic_write_json_pretty` (`lib.rs:6238`) | `trial_metadata_v1` | durable | Trial summary metadata |
| `trials/<trial>/benchmark_preflight.json` | `atomic_write_json_pretty` (`lib.rs:5878`) | `benchmark_trial_preflight_v1` | durable | Frozen input digest + preflight metadata |
| `trials/<trial>/state_inventory.json` | `write_state_inventory` (`lib.rs:16150`) | `state_inventory_v1` (intended) | durable | Writer/schema mismatch noted below |
| `trials/<trial>/harness_stdout.log` | adapter run path (`lib.rs:6473`) | plain text | durable | |
| `trials/<trial>/harness_stderr.log` | adapter run path (`lib.rs:6474`) | plain text | durable | |
| `trials/<trial>/trace_manifest.json` | `atomic_write_json_pretty` (`lib.rs:6358`) | `trace_manifest_v1` | optional durable | Written if trace available |
| `trials/<trial>/evidence/workspace_pre_snapshot.json` | `atomic_write_json_pretty` (`lib.rs:6275`) | `workspace_snapshot_v1` | durable | |
| `trials/<trial>/evidence/workspace_post_snapshot.json` | `atomic_write_json_pretty` (`lib.rs:6423`) | `workspace_snapshot_v1` | durable | |
| `trials/<trial>/evidence/workspace_diff_incremental.json` | `atomic_write_json_pretty` (`lib.rs:6437`) | `workspace_diff_v1` | durable | |
| `trials/<trial>/evidence/workspace_diff_cumulative.json` | `atomic_write_json_pretty` (`lib.rs:6438`) | `workspace_diff_v1` | durable | |
| `trials/<trial>/evidence/workspace_patch_incremental.json` | `atomic_write_json_pretty` (`lib.rs:6439`) | `workspace_patch_v1` | durable | |
| `trials/<trial>/evidence/workspace_patch_cumulative.json` | `atomic_write_json_pretty` (`lib.rs:6440`) | `workspace_patch_v1` | durable | |
| `trials/<trial>/artifacts/benchmark_frozen_agent_input/trial_input.json` | `fs::copy` (`lib.rs:5858`) | copy of trial input (`agent_task_v1` payload) | durable | Frozen benchmark input |
| `trials/<trial>/result.json` | `materialize_trial_result` (`lib.rs:15984`) | copy of agent output | optional durable | Canonicalized result copy |

### 2.5 Benchmark Output Writes (Run-level)

| Path pattern | Writer (code) | Format/schema | Lifecycle | Notes |
|---|---|---|---|---|
| `benchmark/predictions.jsonl` | `process_benchmark_outputs` (`lib.rs:11540`) | JSONL `benchmark_prediction_record_v1` | durable | Validated against schema on processing |
| `benchmark/scores.jsonl` | `process_benchmark_outputs` (`lib.rs:11541`) | JSONL `benchmark_score_record_v1` | durable | Validated against schema on processing |
| `benchmark/adapter_manifest.json` | `atomic_write_json_pretty` (`lib.rs:11559`) | `benchmark_adapter_manifest_v1` | durable | Schema-validated |
| `benchmark/summary.json` | `atomic_write_json_pretty` (`lib.rs:11563`) | `benchmark_summary_v1` | durable | Schema-validated |

### 2.6 Runtime Staging / Transient Writes

| Path pattern | Writer (code) | Format/schema | Lifecycle | Notes |
|---|---|---|---|---|
| `.scratch/<trial>_<pid>_<seq>/{in,workspace,state,deps,out,tmp}` | `TrialPaths::prepare` (`lib.rs:13986`) | mixed files | transient | Per-trial execution sandbox |
| `.scratch/.../in/trial_input.json` | IO prep (`lib.rs:15942`) | `agent_task_v1` payload | transient | |
| `.scratch/.../out/result.json` | agent output | `agent_result_v1` payload | transient | |
| `.scratch/.../out/trajectory.jsonl` | agent trajectory/hook events | JSONL events | transient | |
| `.scratch/.../state/lab_control.json` | `write_adapter_control_action` (`lib.rs:16005`) | `control_plane_v1` | transient | |
| `runtime/worker_payload/<trial>/evidence_records.jsonl` | `execute_parallel_worker_trial` (`lib.rs:7754`) | pre-commit `evidence_record_v1` rows | transient | Can be left behind on interruption |
| `runtime/worker_payload/<trial>/task_chain_states.jsonl` | same (`lib.rs:7755`) | pre-commit `task_chain_state_v1` rows | transient | Can be left behind on interruption |
| `runtime/operation_lease.json` | `acquire_run_operation_lease` (`lib.rs:1031`) | `operation_lease_v1` | transient lock | File lock still file-based |
| `runtime/recovery_report.json` | recovery (`lib.rs:2485`) | `recovery_report_v1` | transient/debug | Written during recover path |

### 2.7 Legacy / Compatibility File Shapes

These path names remain in code or old runs:

- `facts/{run_manifest.json,trials.jsonl,metrics_long.jsonl,events.jsonl,variant_snapshots.jsonl}`  
  Current sink is SQLite-backed (`JsonlRunSink = SqliteRunStore` at `sink.rs:124`). These files persist in older runs.
- run-level `evidence/{evidence_records.jsonl,task_chain_states.jsonl}`  
  Still path-created in execution flows (`lib.rs:8870`, `lib.rs:2260`), but writes are routed to SQLite when identity fields exist.
- Runtime mirror filenames (`runtime/run_control.json`, `run_session_state.json`, `schedule_progress.json`, `parallel_worker_control.json`, `engine_lease.json`)  
  `load_json_file` maps these names to SQLite runtime keys first (`lib.rs:3539`), then file fallback.
- Declared but effectively dead file journal paths:  
  `runtime/slot_commit_journal.jsonl`, `runtime/pending_trial_completions.jsonl` (`lib.rs:1220`, `lib.rs:1224`) are not active write targets in production flow (only path tests).

## 3) SQLite Contract (`run.sqlite`)

Schema source: `rust/crates/lab-runner/src/persistence/schema_v2.sql`.

### 3.1 Runtime KV and Control Tables

- `runtime_kv` (`key`, `value_json`, `updated_at_ms`)  
  Runtime keys currently used:  
  `run_control_v2`, `run_session_state_v1`, `schedule_progress_v2`, `parallel_worker_control_v1`, `engine_lease_v1`.
- `run_manifests`
- `slot_commit_records`
- `pending_trial_completions`
- `runtime_ops`

### 3.2 Row Tables

- `trial_rows`
- `metric_rows`
- `event_rows`
- `variant_snapshot_rows`
- `evidence_rows`
- `chain_state_rows`
- `benchmark_prediction_rows`
- `benchmark_score_rows`
- `attempt_objects`
- `lineage_versions`
- `lineage_heads`

### 3.3 Important Routing Behavior

- `append_jsonl` routes JSONL rows into SQLite only if identity fields exist (`run_id`, `schedule_idx`, `attempt`, `row_seq`, `slot_commit_id`) (`lib.rs:14188`, `lib.rs:14201`).
- Slot commit pipeline annotates identity fields before commit (`lib.rs:6819`, `lib.rs:7021`).
- If identity is missing, `append_jsonl` falls back to raw file append (`lib.rs:14224`).

## 4) Schema Coverage Matrix (File Outputs vs Schema Files)

### 4.1 File outputs with explicit JSON schema files present in `schemas/`

- `manifest_v1`
- `run_control_v2`
- `schedule_progress_v2`
- `operation_lease_v1`
- `engine_lease_v1`
- `state_inventory_v1`
- `evidence_record_v1`
- `task_chain_state_v1`
- `benchmark_prediction_record_v1`
- `benchmark_score_record_v1`
- `benchmark_adapter_manifest_v1`
- `benchmark_summary_v1`
- `agent_task_v1`
- `agent_result_v1`
- `trace_manifest_v1`
- `hook_events_v1`
- `recovery_report_v1`

### 4.2 File outputs with schema_version in payload but no matching schema file in `schemas/`

- `trial_state_v1`
- `trial_metadata_v1`
- `benchmark_trial_preflight_v1`
- `resolved_variants_v1`
- `resolved_schedule_v1`
- `workspace_snapshot_v1`
- `workspace_diff_v1`
- `workspace_patch_v1`
- `workspace_bundle_v1`
- `run_manifest_v1`
- `replay_manifest_v1`
- `fork_manifest_v1`
- `pending_trial_completion_v1`
- `parallel_worker_control_v1`

## 5) Known Coordination/Slop Findings

1. Mixed persistence modes across runs (`facts/evidence` legacy files vs SQLite canonical rows) create inconsistent directory footprints.
2. Runtime compatibility path names still exist even when canonical writes moved to SQLite.
3. Dead path functions (`slot_commit_journal.jsonl`, `pending_trial_completions.jsonl`) remain and imply obsolete file contracts.
4. `runtime/worker_payload` and `.scratch` are transient by design but can remain when runs are interrupted.
5. `state_inventory` writer/schema mismatch exists:
   - writer emits `agent_runtime_identity` (`lib.rs:16137`)
   - schema requires `harness_identity` (`schemas/state_inventory_v1.jsonschema`)

## 6) Minimal Verification Commands (Post-run)

Use these to compare expected contract vs actual run output:

```bash
# 1) Top-level footprint for a run
cd /path/to/run_<id>
find . -maxdepth 3 -mindepth 1 | sort

# 2) JSON files with declared schema_version
find . -type f \( -name '*.json' -o -name '*.jsonl' \) -print0 \
  | xargs -0 -I{} sh -c 'first=$(head -n1 "{}" 2>/dev/null); echo "$first" | jq -r ".schema_version // empty" >/dev/null 2>&1 && echo "{} $(echo "$first" | jq -r ".schema_version // empty")"'

# 3) SQLite table inventory
sqlite3 run.sqlite ".tables"
sqlite3 run.sqlite "select key from runtime_kv order by key;"
```

## 7) Machine-Checkable Contract Audit (New)

Use the run contract checker to diff expected vs actual writes for a completed or interrupted run:

```bash
cd /Users/jevinnishioka/Desktop/Experiments
scripts/agentlab/audit_run_write_contract.sh /absolute/path/to/.lab/runs/run_<id>
```

Output:

- Writes `/absolute/path/to/.lab/runs/run_<id>/write_contract_audit.md`
- Reports:
  - required run files present/missing
  - optional/transient folders present (`.scratch`, `runtime/worker_payload`, etc.)
  - legacy/compat footprint presence (`facts/*`, run-level `evidence/*`, runtime JSON mirrors)
  - discovered `schema_version` values across JSON/JSONL files
  - SQLite table inventory + missing/extra tables
  - `runtime_kv` expected keys, optional keys, and extras
  - per-table row counts

## 8) Cutover Status (Implemented)

As of this audit update, runner write behavior was hardened in code:

- Removed runtime JSON file fallback for canonical runtime state keys (`run_control`, `run_session_state`, `schedule_progress`, `parallel_worker_control`, `engine_lease`): reads now resolve from SQLite runtime state only.
- Removed `append_jsonl` fallback-to-file behavior for mapped row tables; rows now must carry SQLite identity fields and route to SQLite.
- Removed dead path contract helpers for:
  - `runtime/slot_commit_journal.jsonl`
  - `runtime/pending_trial_completions.jsonl`
- Removed `JsonlRunSink` compatibility alias in favor of `SqliteRunStore`.
- Added missing schema files for emitted `schema_version` values, including:
  - `trial_state_v1`, `trial_metadata_v1`, `benchmark_trial_preflight_v1`
  - `resolved_variants_v1`, `resolved_schedule_v1`
  - `workspace_snapshot_v1`, `workspace_diff_v1`, `workspace_patch_v1`, `workspace_bundle_v1`
  - `run_manifest_v1`, `run_session_state_v1`, `parallel_worker_control_v1`
  - `replay_manifest_v1`, `fork_manifest_v1`, `pending_trial_completion_v1`
  - `control_plane_v1`, `task_boundary_v2`
  - `sealed_package_checksums_v2`, `sealed_package_lock_v1`, `sealed_run_package_v2`
- Added write-time schema contract existence checks:
  - if payload declares `schema_version`, corresponding `schemas/<schema_version>.jsonschema` must exist.
