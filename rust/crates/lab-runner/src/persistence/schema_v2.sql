PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS runtime_kv (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL CHECK(json_valid(value_json)),
  updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS run_manifests (
  run_id TEXT PRIMARY KEY,
  manifest_json TEXT NOT NULL CHECK(json_valid(manifest_json)),
  updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS slot_commit_records (
  run_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  record_type TEXT NOT NULL,
  slot_commit_id TEXT NOT NULL,
  record_json TEXT NOT NULL CHECK(json_valid(record_json)),
  recorded_at_ms INTEGER NOT NULL,
  PRIMARY KEY (run_id, schedule_idx, attempt, record_type)
) STRICT;

CREATE TABLE IF NOT EXISTS pending_trial_completions (
  run_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  trial_result_json TEXT NOT NULL CHECK(json_valid(trial_result_json)),
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (run_id, schedule_idx)
) STRICT;

CREATE TABLE IF NOT EXISTS trial_rows (
  run_id TEXT NOT NULL,
  trial_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  row_seq INTEGER NOT NULL,
  slot_commit_id TEXT NOT NULL,
  baseline_id TEXT NOT NULL,
  workload_type TEXT NOT NULL,
  variant_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  repl_idx INTEGER NOT NULL,
  outcome TEXT NOT NULL,
  primary_metric_name TEXT NOT NULL,
  primary_metric_value_json TEXT NOT NULL CHECK(json_valid(primary_metric_value_json)),
  metrics_json TEXT NOT NULL CHECK(json_valid(metrics_json)),
  bindings_json TEXT NOT NULL CHECK(json_valid(bindings_json)),
  hook_events_total INTEGER NOT NULL,
  has_hook_events INTEGER NOT NULL CHECK(has_hook_events IN (0,1)),
  row_json TEXT NOT NULL CHECK(json_valid(row_json)),
  PRIMARY KEY (run_id, trial_id, schedule_idx, attempt, row_seq)
) STRICT;

CREATE TABLE IF NOT EXISTS metric_rows (
  run_id TEXT NOT NULL,
  trial_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  row_seq INTEGER NOT NULL,
  slot_commit_id TEXT NOT NULL,
  variant_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  repl_idx INTEGER NOT NULL,
  outcome TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  metric_value_json TEXT NOT NULL CHECK(json_valid(metric_value_json)),
  metric_source TEXT,
  row_json TEXT NOT NULL CHECK(json_valid(row_json)),
  PRIMARY KEY (run_id, trial_id, schedule_idx, attempt, row_seq)
) STRICT;

CREATE TABLE IF NOT EXISTS event_rows (
  run_id TEXT NOT NULL,
  trial_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  row_seq INTEGER NOT NULL,
  slot_commit_id TEXT NOT NULL,
  variant_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  repl_idx INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  ts TEXT,
  payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
  row_json TEXT NOT NULL CHECK(json_valid(row_json)),
  PRIMARY KEY (run_id, trial_id, schedule_idx, attempt, row_seq)
) STRICT;

CREATE TABLE IF NOT EXISTS variant_snapshot_rows (
  run_id TEXT NOT NULL,
  trial_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  row_seq INTEGER NOT NULL,
  slot_commit_id TEXT NOT NULL,
  variant_id TEXT NOT NULL,
  baseline_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  repl_idx INTEGER NOT NULL,
  binding_name TEXT NOT NULL,
  binding_value_json TEXT NOT NULL CHECK(json_valid(binding_value_json)),
  binding_value_text TEXT NOT NULL,
  row_json TEXT NOT NULL CHECK(json_valid(row_json)),
  PRIMARY KEY (run_id, trial_id, schedule_idx, attempt, row_seq)
) STRICT;

CREATE TABLE IF NOT EXISTS evidence_rows (
  run_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  row_seq INTEGER NOT NULL,
  slot_commit_id TEXT NOT NULL,
  row_json TEXT NOT NULL CHECK(json_valid(row_json)),
  PRIMARY KEY (run_id, schedule_idx, attempt, row_seq)
) STRICT;

CREATE TABLE IF NOT EXISTS chain_state_rows (
  run_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  row_seq INTEGER NOT NULL,
  slot_commit_id TEXT NOT NULL,
  row_json TEXT NOT NULL CHECK(json_valid(row_json)),
  PRIMARY KEY (run_id, schedule_idx, attempt, row_seq)
) STRICT;

CREATE TABLE IF NOT EXISTS benchmark_prediction_rows (
  run_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  row_seq INTEGER NOT NULL,
  slot_commit_id TEXT NOT NULL,
  row_json TEXT NOT NULL CHECK(json_valid(row_json)),
  PRIMARY KEY (run_id, schedule_idx, attempt, row_seq)
) STRICT;

CREATE TABLE IF NOT EXISTS benchmark_score_rows (
  run_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  row_seq INTEGER NOT NULL,
  slot_commit_id TEXT NOT NULL,
  row_json TEXT NOT NULL CHECK(json_valid(row_json)),
  PRIMARY KEY (run_id, schedule_idx, attempt, row_seq)
) STRICT;

CREATE TABLE IF NOT EXISTS attempt_objects (
  run_id TEXT NOT NULL,
  trial_id TEXT NOT NULL,
  schedule_idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  role TEXT NOT NULL,
  object_ref TEXT NOT NULL,
  metadata_json TEXT CHECK(metadata_json IS NULL OR json_valid(metadata_json)),
  recorded_at_ms INTEGER NOT NULL,
  PRIMARY KEY (run_id, trial_id, schedule_idx, attempt, role)
) STRICT;

CREATE TABLE IF NOT EXISTS lineage_versions (
  version_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  chain_key TEXT NOT NULL,
  step_index INTEGER NOT NULL,
  trial_id TEXT NOT NULL,
  parent_version_id TEXT,
  pre_snapshot_ref TEXT,
  post_snapshot_ref TEXT,
  diff_incremental_ref TEXT,
  diff_cumulative_ref TEXT,
  patch_incremental_ref TEXT,
  patch_cumulative_ref TEXT,
  workspace_ref TEXT,
  checkpoint_labels_json TEXT NOT NULL CHECK(json_valid(checkpoint_labels_json))
) STRICT;

CREATE TABLE IF NOT EXISTS lineage_heads (
  run_id TEXT NOT NULL,
  chain_key TEXT NOT NULL,
  latest_version_id TEXT NOT NULL,
  step_index INTEGER NOT NULL,
  latest_workspace_ref TEXT,
  PRIMARY KEY (run_id, chain_key)
) STRICT;

CREATE TABLE IF NOT EXISTS runtime_ops (
  run_id TEXT NOT NULL,
  op_kind TEXT NOT NULL,
  op_id TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (run_id, op_kind, op_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_trial_rows_variant ON trial_rows (run_id, variant_id);
CREATE INDEX IF NOT EXISTS idx_trial_rows_task ON trial_rows (run_id, task_id);
CREATE INDEX IF NOT EXISTS idx_metric_rows_name ON metric_rows (run_id, metric_name);
CREATE INDEX IF NOT EXISTS idx_slot_commits_schedule ON slot_commit_records (run_id, schedule_idx);
CREATE INDEX IF NOT EXISTS idx_attempt_objects_trial_role
  ON attempt_objects (run_id, trial_id, role, attempt DESC);
CREATE INDEX IF NOT EXISTS idx_lineage_versions_trial
  ON lineage_versions (run_id, trial_id, step_index DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_ops_kind
  ON runtime_ops (run_id, op_kind, updated_at_ms DESC);
