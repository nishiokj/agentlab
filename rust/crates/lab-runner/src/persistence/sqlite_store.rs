use crate::validate_schema_contract_value;
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use lab_core::sha256_bytes;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::path::{Path, PathBuf};

const SCHEMA_SQL: &str = include_str!("schema_v2.sql");
pub const RUN_SQLITE_FILE: &str = "run.sqlite";

#[derive(Debug, Clone, Copy)]
pub enum JsonRowTable {
    Evidence,
    ChainState,
    BenchmarkPrediction,
    BenchmarkScore,
}

#[derive(Debug)]
pub struct TrialRowInsert<'a> {
    pub run_id: &'a str,
    pub trial_id: &'a str,
    pub schedule_idx: usize,
    pub attempt: usize,
    pub row_seq: usize,
    pub slot_commit_id: &'a str,
    pub baseline_id: &'a str,
    pub workload_type: &'a str,
    pub variant_id: &'a str,
    pub task_id: &'a str,
    pub repl_idx: usize,
    pub outcome: &'a str,
    pub primary_metric_name: &'a str,
    pub primary_metric_value: &'a Value,
    pub metrics: &'a Value,
    pub bindings: &'a Value,
    pub hook_events_total: usize,
    pub has_hook_events: bool,
    pub row_json: &'a Value,
}

#[derive(Debug)]
pub struct MetricRowInsert<'a> {
    pub run_id: &'a str,
    pub trial_id: &'a str,
    pub schedule_idx: usize,
    pub attempt: usize,
    pub row_seq: usize,
    pub slot_commit_id: &'a str,
    pub variant_id: &'a str,
    pub task_id: &'a str,
    pub repl_idx: usize,
    pub outcome: &'a str,
    pub metric_name: &'a str,
    pub metric_value: &'a Value,
    pub metric_source: Option<&'a str>,
    pub row_json: &'a Value,
}

#[derive(Debug)]
pub struct EventRowInsert<'a> {
    pub run_id: &'a str,
    pub trial_id: &'a str,
    pub schedule_idx: usize,
    pub attempt: usize,
    pub row_seq: usize,
    pub slot_commit_id: &'a str,
    pub variant_id: &'a str,
    pub task_id: &'a str,
    pub repl_idx: usize,
    pub seq: usize,
    pub event_type: &'a str,
    pub ts: Option<&'a str>,
    pub payload: &'a Value,
    pub row_json: &'a Value,
}

#[derive(Debug)]
pub struct VariantSnapshotRowInsert<'a> {
    pub run_id: &'a str,
    pub trial_id: &'a str,
    pub schedule_idx: usize,
    pub attempt: usize,
    pub row_seq: usize,
    pub slot_commit_id: &'a str,
    pub variant_id: &'a str,
    pub baseline_id: &'a str,
    pub task_id: &'a str,
    pub repl_idx: usize,
    pub binding_name: &'a str,
    pub binding_value: &'a Value,
    pub binding_value_text: &'a str,
    pub row_json: &'a Value,
}

pub fn run_sqlite_path(run_dir: &Path) -> PathBuf {
    run_dir.join(RUN_SQLITE_FILE)
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn json_text(value: &Value) -> Result<String> {
    serde_json::to_string(value).context("serialize json")
}

fn parse_json_text(raw: String) -> Result<Value> {
    serde_json::from_str(&raw).context("parse json")
}

fn as_i64(v: usize) -> i64 {
    v as i64
}

fn extract_str<'a>(value: &'a Value, pointer: &str) -> Result<&'a str> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing string field '{}'", pointer))
}

fn extract_usize(value: &Value, pointer: &str) -> Result<usize> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .ok_or_else(|| anyhow!("missing integer field '{}'", pointer))
}

fn extract_str_opt<'a>(value: &'a Value, pointer: &str) -> Option<&'a str> {
    value.pointer(pointer).and_then(Value::as_str)
}

pub struct SqliteRunStore {
    conn: Connection,
}

impl SqliteRunStore {
    pub fn open(run_dir: &Path) -> Result<Self> {
        if !run_dir.exists() {
            std::fs::create_dir_all(run_dir).with_context(|| {
                format!(
                    "create run directory for sqlite store: {}",
                    run_dir.display()
                )
            })?;
        }
        let db_path = run_sqlite_path(run_dir);
        let conn = Connection::open(&db_path)
            .with_context(|| format!("open sqlite database {}", db_path.display()))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA foreign_keys=ON;
             PRAGMA temp_store=MEMORY;",
        )
        .context("configure sqlite pragmas")?;
        conn.execute_batch(SCHEMA_SQL)
            .context("bootstrap sqlite schema")?;
        Ok(Self { conn })
    }

    pub fn put_runtime_json(&mut self, key: &str, value: &Value) -> Result<()> {
        validate_schema_contract_value(value, format!("runtime_kv key '{}'", key).as_str())?;
        let payload = json_text(value)?;
        self.conn.execute(
            "INSERT INTO runtime_kv (key, value_json, updated_at_ms)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET
               value_json=excluded.value_json,
               updated_at_ms=excluded.updated_at_ms",
            params![key, payload, now_ms()],
        )?;
        Ok(())
    }

    pub fn get_runtime_json(&self, key: &str) -> Result<Option<Value>> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT value_json FROM runtime_kv WHERE key=?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?;
        raw.map(parse_json_text).transpose()
    }

    pub fn put_run_manifest(&mut self, run_id: &str, manifest: &Value) -> Result<()> {
        validate_schema_contract_value(
            manifest,
            format!("run_manifest row for run '{}'", run_id).as_str(),
        )?;
        let payload = json_text(manifest)?;
        self.conn.execute(
            "INSERT INTO run_manifests (run_id, manifest_json, updated_at_ms)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(run_id) DO UPDATE SET
               manifest_json=excluded.manifest_json,
               updated_at_ms=excluded.updated_at_ms",
            params![run_id, payload, now_ms()],
        )?;
        Ok(())
    }

    pub fn upsert_slot_commit_record(&mut self, record: &Value) -> Result<()> {
        let run_id = extract_str(record, "/run_id")?;
        let schedule_idx = extract_usize(record, "/schedule_idx")?;
        let attempt = extract_usize(record, "/attempt")?;
        let record_type = extract_str(record, "/record_type")?;
        let slot_commit_id = extract_str(record, "/slot_commit_id")?;
        let payload = json_text(record)?;
        self.conn.execute(
            "INSERT INTO slot_commit_records
             (run_id, schedule_idx, attempt, record_type, slot_commit_id, record_json, recorded_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(run_id, schedule_idx, attempt, record_type) DO UPDATE SET
               slot_commit_id=excluded.slot_commit_id,
               record_json=excluded.record_json,
               recorded_at_ms=excluded.recorded_at_ms",
            params![
                run_id,
                as_i64(schedule_idx),
                as_i64(attempt),
                record_type,
                slot_commit_id,
                payload,
                now_ms()
            ],
        )?;
        Ok(())
    }

    pub fn load_slot_commit_records(&self, run_id: &str) -> Result<Vec<Value>> {
        let mut stmt = self.conn.prepare(
            "SELECT record_json
             FROM slot_commit_records
             WHERE run_id=?1
             ORDER BY schedule_idx, attempt,
               CASE record_type WHEN 'intent' THEN 0 ELSE 1 END",
        )?;
        let mut rows = stmt.query(params![run_id])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let raw: String = row.get(0)?;
            out.push(parse_json_text(raw)?);
        }
        Ok(out)
    }

    pub fn first_run_id_with_slot_commits(&self) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT run_id FROM slot_commit_records ORDER BY recorded_at_ms LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn replace_pending_trial_completions(
        &mut self,
        run_id: &str,
        rows: &[Value],
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM pending_trial_completions WHERE run_id=?1",
            params![run_id],
        )?;
        for row in rows {
            validate_schema_contract_value(
                row,
                format!("pending_trial_completions row for run '{}'", run_id).as_str(),
            )?;
            let schedule_idx = extract_usize(row, "/schedule_idx")?;
            let trial_result = row
                .get("trial_result")
                .ok_or_else(|| anyhow!("pending completion missing /trial_result"))?;
            tx.execute(
                "INSERT INTO pending_trial_completions
                 (run_id, schedule_idx, trial_result_json, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    run_id,
                    as_i64(schedule_idx),
                    json_text(trial_result)?,
                    now_ms()
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn load_pending_trial_completions(&self, run_id: &str) -> Result<Vec<Value>> {
        let mut stmt = self.conn.prepare(
            "SELECT schedule_idx, trial_result_json
             FROM pending_trial_completions
             WHERE run_id=?1
             ORDER BY schedule_idx",
        )?;
        let mut rows = stmt.query(params![run_id])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let schedule_idx: i64 = row.get(0)?;
            let trial_result_raw: String = row.get(1)?;
            out.push(serde_json::json!({
                "schema_version": "pending_trial_completion_v1",
                "schedule_idx": schedule_idx,
                "trial_result": parse_json_text(trial_result_raw)?,
            }));
        }
        Ok(out)
    }

    pub fn first_run_id_with_pending_completions(&self) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT run_id FROM pending_trial_completions ORDER BY updated_at_ms LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn upsert_attempt_object(
        &mut self,
        run_id: &str,
        trial_id: &str,
        schedule_idx: usize,
        attempt: usize,
        role: &str,
        object_ref: &str,
        metadata: Option<&Value>,
    ) -> Result<()> {
        let metadata_json = metadata.map(json_text).transpose()?;
        self.conn.execute(
            "INSERT INTO attempt_objects (
               run_id, trial_id, schedule_idx, attempt, role, object_ref, metadata_json, recorded_at_ms
             ) VALUES (
               ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
             )
             ON CONFLICT(run_id, trial_id, schedule_idx, attempt, role) DO UPDATE SET
               object_ref=excluded.object_ref,
               metadata_json=excluded.metadata_json,
               recorded_at_ms=excluded.recorded_at_ms",
            params![
                run_id,
                trial_id,
                as_i64(schedule_idx),
                as_i64(attempt),
                role,
                object_ref,
                metadata_json,
                now_ms()
            ],
        )?;
        Ok(())
    }

    pub fn latest_attempt_object_ref(
        &self,
        run_id: &str,
        trial_id: &str,
        role: &str,
    ) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT object_ref
                 FROM attempt_objects
                 WHERE run_id=?1 AND trial_id=?2 AND role=?3
                 ORDER BY attempt DESC, recorded_at_ms DESC
                 LIMIT 1",
                params![run_id, trial_id, role],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn latest_lineage_version_id_for_trial(
        &self,
        run_id: &str,
        trial_id: &str,
    ) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT version_id
                 FROM lineage_versions
                 WHERE run_id=?1 AND trial_id=?2
                 ORDER BY step_index DESC
                 LIMIT 1",
                params![run_id, trial_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn lineage_workspace_ref_by_version(&self, version_id: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT workspace_ref
                 FROM lineage_versions
                 WHERE version_id=?1",
                params![version_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn upsert_runtime_operation(
        &mut self,
        run_id: &str,
        op_kind: &str,
        op_id: &str,
        payload: &Value,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO runtime_ops (run_id, op_kind, op_id, payload_json, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(run_id, op_kind, op_id) DO UPDATE SET
               payload_json=excluded.payload_json,
               updated_at_ms=excluded.updated_at_ms",
            params![run_id, op_kind, op_id, json_text(payload)?, now_ms()],
        )?;
        Ok(())
    }

    fn upsert_lineage_from_chain_state_row(&mut self, row: &Value) -> Result<()> {
        let run_id = extract_str_opt(row, "/run_id")
            .or_else(|| extract_str_opt(row, "/ids/run_id"))
            .ok_or_else(|| anyhow!("missing run_id in chain state row"))?;
        let trial_id = extract_str_opt(row, "/ids/trial_id")
            .ok_or_else(|| anyhow!("missing /ids/trial_id in chain state row"))?;
        let chain_key = extract_str_opt(row, "/chain_id")
            .ok_or_else(|| anyhow!("missing /chain_id in chain state row"))?;
        let step_index = extract_usize(row, "/step_index")?;
        let pre_snapshot_ref = extract_str_opt(row, "/snapshots/prev_ref");
        let post_snapshot_ref = extract_str_opt(row, "/snapshots/post_ref");
        let diff_incremental_ref = extract_str_opt(row, "/diffs/incremental_ref");
        let diff_cumulative_ref = extract_str_opt(row, "/diffs/cumulative_ref");
        let patch_incremental_ref = extract_str_opt(row, "/diffs/patch_incremental_ref");
        let patch_cumulative_ref = extract_str_opt(row, "/diffs/patch_cumulative_ref");
        let workspace_ref = extract_str_opt(row, "/ext/latest_workspace_ref")
            .or_else(|| extract_str_opt(row, "/ext/workspace_ref"));

        let token = format!("{run_id}|{chain_key}|{step_index}|{trial_id}");
        let version_id = sha256_bytes(token.as_bytes());

        let parent_version_id: Option<String> = self
            .conn
            .query_row(
                "SELECT latest_version_id
                 FROM lineage_heads
                 WHERE run_id=?1 AND chain_key=?2",
                params![run_id, chain_key],
                |db_row| db_row.get(0),
            )
            .optional()?;

        let checkpoint_labels = row
            .pointer("/checkpoint_labels")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));

        self.conn.execute(
            "INSERT INTO lineage_versions (
               version_id, run_id, chain_key, step_index, trial_id, parent_version_id,
               pre_snapshot_ref, post_snapshot_ref,
               diff_incremental_ref, diff_cumulative_ref,
               patch_incremental_ref, patch_cumulative_ref,
               workspace_ref, checkpoint_labels_json
             ) VALUES (
               ?1, ?2, ?3, ?4, ?5, ?6,
               ?7, ?8,
               ?9, ?10,
               ?11, ?12,
               ?13, ?14
             )
             ON CONFLICT(version_id) DO UPDATE SET
               parent_version_id=excluded.parent_version_id,
               pre_snapshot_ref=excluded.pre_snapshot_ref,
               post_snapshot_ref=excluded.post_snapshot_ref,
               diff_incremental_ref=excluded.diff_incremental_ref,
               diff_cumulative_ref=excluded.diff_cumulative_ref,
               patch_incremental_ref=excluded.patch_incremental_ref,
               patch_cumulative_ref=excluded.patch_cumulative_ref,
               workspace_ref=excluded.workspace_ref,
               checkpoint_labels_json=excluded.checkpoint_labels_json",
            params![
                version_id,
                run_id,
                chain_key,
                as_i64(step_index),
                trial_id,
                parent_version_id,
                pre_snapshot_ref,
                post_snapshot_ref,
                diff_incremental_ref,
                diff_cumulative_ref,
                patch_incremental_ref,
                patch_cumulative_ref,
                workspace_ref,
                json_text(&checkpoint_labels)?
            ],
        )?;

        self.conn.execute(
            "INSERT INTO lineage_heads (run_id, chain_key, latest_version_id, step_index, latest_workspace_ref)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(run_id, chain_key) DO UPDATE SET
               latest_version_id=excluded.latest_version_id,
               step_index=excluded.step_index,
               latest_workspace_ref=excluded.latest_workspace_ref",
            params![run_id, chain_key, version_id, as_i64(step_index), workspace_ref],
        )?;
        Ok(())
    }

    fn upsert_attempt_objects_from_evidence_row(&mut self, row: &Value) -> Result<()> {
        let run_id = extract_str_opt(row, "/run_id")
            .or_else(|| extract_str_opt(row, "/ids/run_id"))
            .ok_or_else(|| anyhow!("missing run_id in evidence row"))?;
        let Some(trial_id) = extract_str_opt(row, "/ids/trial_id") else {
            return Ok(());
        };
        let Some(schedule_idx) = extract_usize(row, "/schedule_idx").ok() else {
            return Ok(());
        };
        let Some(attempt) = extract_usize(row, "/attempt").ok() else {
            return Ok(());
        };
        let Some(evidence) = row.pointer("/evidence").and_then(Value::as_object) else {
            return Ok(());
        };

        for role in [
            "trial_input_ref",
            "trial_output_ref",
            "hook_events_ref",
            "stdout_ref",
            "stderr_ref",
            "workspace_pre_ref",
            "workspace_post_ref",
            "diff_incremental_ref",
            "diff_cumulative_ref",
            "patch_incremental_ref",
            "patch_cumulative_ref",
            "workspace_bundle_ref",
        ] {
            let Some(object_ref) = evidence.get(role).and_then(Value::as_str) else {
                continue;
            };
            let normalized_role = role.trim_end_matches("_ref");
            self.upsert_attempt_object(
                run_id,
                trial_id,
                schedule_idx,
                attempt,
                normalized_role,
                object_ref,
                Some(row),
            )?;
        }
        Ok(())
    }

    pub fn upsert_trial_row(&mut self, row: TrialRowInsert<'_>) -> Result<()> {
        self.conn.execute(
            "INSERT INTO trial_rows (
               run_id, trial_id, schedule_idx, attempt, row_seq, slot_commit_id,
               baseline_id, workload_type, variant_id, task_id, repl_idx, outcome,
               primary_metric_name, primary_metric_value_json, metrics_json, bindings_json,
               hook_events_total, has_hook_events, row_json
             ) VALUES (
               ?1, ?2, ?3, ?4, ?5, ?6,
               ?7, ?8, ?9, ?10, ?11, ?12,
               ?13, ?14, ?15, ?16,
               ?17, ?18, ?19
             )
             ON CONFLICT(run_id, trial_id, schedule_idx, attempt, row_seq) DO UPDATE SET
               slot_commit_id=excluded.slot_commit_id,
               baseline_id=excluded.baseline_id,
               workload_type=excluded.workload_type,
               variant_id=excluded.variant_id,
               task_id=excluded.task_id,
               repl_idx=excluded.repl_idx,
               outcome=excluded.outcome,
               primary_metric_name=excluded.primary_metric_name,
               primary_metric_value_json=excluded.primary_metric_value_json,
               metrics_json=excluded.metrics_json,
               bindings_json=excluded.bindings_json,
               hook_events_total=excluded.hook_events_total,
               has_hook_events=excluded.has_hook_events,
               row_json=excluded.row_json",
            params![
                row.run_id,
                row.trial_id,
                as_i64(row.schedule_idx),
                as_i64(row.attempt),
                as_i64(row.row_seq),
                row.slot_commit_id,
                row.baseline_id,
                row.workload_type,
                row.variant_id,
                row.task_id,
                as_i64(row.repl_idx),
                row.outcome,
                row.primary_metric_name,
                json_text(row.primary_metric_value)?,
                json_text(row.metrics)?,
                json_text(row.bindings)?,
                as_i64(row.hook_events_total),
                if row.has_hook_events { 1_i64 } else { 0_i64 },
                json_text(row.row_json)?,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_metric_row(&mut self, row: MetricRowInsert<'_>) -> Result<()> {
        self.conn.execute(
            "INSERT INTO metric_rows (
               run_id, trial_id, schedule_idx, attempt, row_seq, slot_commit_id,
               variant_id, task_id, repl_idx, outcome,
               metric_name, metric_value_json, metric_source, row_json
             ) VALUES (
               ?1, ?2, ?3, ?4, ?5, ?6,
               ?7, ?8, ?9, ?10,
               ?11, ?12, ?13, ?14
             )
             ON CONFLICT(run_id, trial_id, schedule_idx, attempt, row_seq) DO UPDATE SET
               slot_commit_id=excluded.slot_commit_id,
               variant_id=excluded.variant_id,
               task_id=excluded.task_id,
               repl_idx=excluded.repl_idx,
               outcome=excluded.outcome,
               metric_name=excluded.metric_name,
               metric_value_json=excluded.metric_value_json,
               metric_source=excluded.metric_source,
               row_json=excluded.row_json",
            params![
                row.run_id,
                row.trial_id,
                as_i64(row.schedule_idx),
                as_i64(row.attempt),
                as_i64(row.row_seq),
                row.slot_commit_id,
                row.variant_id,
                row.task_id,
                as_i64(row.repl_idx),
                row.outcome,
                row.metric_name,
                json_text(row.metric_value)?,
                row.metric_source,
                json_text(row.row_json)?,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_event_row(&mut self, row: EventRowInsert<'_>) -> Result<()> {
        self.conn.execute(
            "INSERT INTO event_rows (
               run_id, trial_id, schedule_idx, attempt, row_seq, slot_commit_id,
               variant_id, task_id, repl_idx, seq, event_type, ts, payload_json, row_json
             ) VALUES (
               ?1, ?2, ?3, ?4, ?5, ?6,
               ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14
             )
             ON CONFLICT(run_id, trial_id, schedule_idx, attempt, row_seq) DO UPDATE SET
               slot_commit_id=excluded.slot_commit_id,
               variant_id=excluded.variant_id,
               task_id=excluded.task_id,
               repl_idx=excluded.repl_idx,
               seq=excluded.seq,
               event_type=excluded.event_type,
               ts=excluded.ts,
               payload_json=excluded.payload_json,
               row_json=excluded.row_json",
            params![
                row.run_id,
                row.trial_id,
                as_i64(row.schedule_idx),
                as_i64(row.attempt),
                as_i64(row.row_seq),
                row.slot_commit_id,
                row.variant_id,
                row.task_id,
                as_i64(row.repl_idx),
                as_i64(row.seq),
                row.event_type,
                row.ts,
                json_text(row.payload)?,
                json_text(row.row_json)?,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_variant_snapshot_row(&mut self, row: VariantSnapshotRowInsert<'_>) -> Result<()> {
        self.conn.execute(
            "INSERT INTO variant_snapshot_rows (
               run_id, trial_id, schedule_idx, attempt, row_seq, slot_commit_id,
               variant_id, baseline_id, task_id, repl_idx, binding_name,
               binding_value_json, binding_value_text, row_json
             ) VALUES (
               ?1, ?2, ?3, ?4, ?5, ?6,
               ?7, ?8, ?9, ?10, ?11,
               ?12, ?13, ?14
             )
             ON CONFLICT(run_id, trial_id, schedule_idx, attempt, row_seq) DO UPDATE SET
               slot_commit_id=excluded.slot_commit_id,
               variant_id=excluded.variant_id,
               baseline_id=excluded.baseline_id,
               task_id=excluded.task_id,
               repl_idx=excluded.repl_idx,
               binding_name=excluded.binding_name,
               binding_value_json=excluded.binding_value_json,
               binding_value_text=excluded.binding_value_text,
               row_json=excluded.row_json",
            params![
                row.run_id,
                row.trial_id,
                as_i64(row.schedule_idx),
                as_i64(row.attempt),
                as_i64(row.row_seq),
                row.slot_commit_id,
                row.variant_id,
                row.baseline_id,
                row.task_id,
                as_i64(row.repl_idx),
                row.binding_name,
                json_text(row.binding_value)?,
                row.binding_value_text,
                json_text(row.row_json)?,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_json_row(&mut self, table: JsonRowTable, row: &Value) -> Result<()> {
        let run_id = extract_str(row, "/run_id")?;
        let schedule_idx = extract_usize(row, "/schedule_idx")?;
        let attempt = extract_usize(row, "/attempt")?;
        let row_seq = extract_usize(row, "/row_seq")?;
        let slot_commit_id = extract_str(row, "/slot_commit_id")?;
        let payload = json_text(row)?;
        let (table_name, sql) = match table {
            JsonRowTable::Evidence => (
                "evidence_rows",
                "INSERT INTO evidence_rows
                 (run_id, schedule_idx, attempt, row_seq, slot_commit_id, row_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(run_id, schedule_idx, attempt, row_seq) DO UPDATE SET
                   slot_commit_id=excluded.slot_commit_id,
                   row_json=excluded.row_json",
            ),
            JsonRowTable::ChainState => (
                "chain_state_rows",
                "INSERT INTO chain_state_rows
                 (run_id, schedule_idx, attempt, row_seq, slot_commit_id, row_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(run_id, schedule_idx, attempt, row_seq) DO UPDATE SET
                   slot_commit_id=excluded.slot_commit_id,
                   row_json=excluded.row_json",
            ),
            JsonRowTable::BenchmarkPrediction => (
                "benchmark_prediction_rows",
                "INSERT INTO benchmark_prediction_rows
                 (run_id, schedule_idx, attempt, row_seq, slot_commit_id, row_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(run_id, schedule_idx, attempt, row_seq) DO UPDATE SET
                   slot_commit_id=excluded.slot_commit_id,
                   row_json=excluded.row_json",
            ),
            JsonRowTable::BenchmarkScore => (
                "benchmark_score_rows",
                "INSERT INTO benchmark_score_rows
                 (run_id, schedule_idx, attempt, row_seq, slot_commit_id, row_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(run_id, schedule_idx, attempt, row_seq) DO UPDATE SET
                   slot_commit_id=excluded.slot_commit_id,
                   row_json=excluded.row_json",
            ),
        };
        self.conn
            .execute(
                sql,
                params![
                    run_id,
                    as_i64(schedule_idx),
                    as_i64(attempt),
                    as_i64(row_seq),
                    slot_commit_id,
                    payload
                ],
            )
            .with_context(|| format!("upsert row in {}", table_name))?;
        match table {
            JsonRowTable::Evidence => {
                self.upsert_attempt_objects_from_evidence_row(row)?;
            }
            JsonRowTable::ChainState => {
                self.upsert_lineage_from_chain_state_row(row)?;
            }
            JsonRowTable::BenchmarkPrediction | JsonRowTable::BenchmarkScore => {}
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn has_lineage_for_trial(&self, run_id: &str, trial_id: &str) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT count(*)
             FROM lineage_versions
             WHERE run_id=?1 AND trial_id=?2",
            params![run_id, trial_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    #[cfg(test)]
    pub fn latest_runtime_operation(&self, run_id: &str, op_kind: &str) -> Result<Option<Value>> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT payload_json
                 FROM runtime_ops
                 WHERE run_id=?1 AND op_kind=?2
                 ORDER BY updated_at_ms DESC
                 LIMIT 1",
                params![run_id, op_kind],
                |row| row.get(0),
            )
            .optional()?;
        raw.map(parse_json_text).transpose()
    }

    #[cfg(test)]
    pub fn row_count(&self, table: &str) -> Result<i64> {
        let sql = format!("SELECT count(*) FROM {}", table);
        let count = self.conn.query_row(&sql, [], |row| row.get(0))?;
        Ok(count)
    }
}
