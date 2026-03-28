use crate::experiment::state::SlotCommitRecord;
use crate::model::RUNTIME_KEY_RUN_CONTROL;
use crate::package::validate::validate_schema_contract_value;
use crate::persistence::rows::{
    infer_run_dir_from_path, json_row_table_from_path, path_uses_sqlite_json_row_ingest,
    row_has_sqlite_identity_fields, EventRow, MetricRow, RunManifestRecord, TrialRecord,
    VariantSnapshotRow,
};
use crate::persistence::store::{
    EventRowInsert, MetricRowInsert, SqliteRunStore as BackingSqliteStore, TrialRowInsert,
    VariantSnapshotRowInsert,
};
use anyhow::{anyhow, Result};
use lab_core::ensure_dir;
use serde_json::{json, Value};
use std::fs;
use std::io::Write;
use std::path::Path;

pub trait RunSink {
    fn write_run_manifest(&mut self, run: &RunManifestRecord) -> Result<()>;
    fn append_trial_record(&mut self, row: &TrialRecord) -> Result<()>;
    fn append_metric_rows(&mut self, rows: &[MetricRow]) -> Result<()>;
    fn append_event_rows(&mut self, rows: &[EventRow]) -> Result<()>;
    fn append_variant_snapshot(&mut self, rows: &[VariantSnapshotRow]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

#[derive(Default)]
pub(crate) struct BufferedRunSink {
    pub(crate) trial_records: Vec<TrialRecord>,
    pub(crate) metric_rows: Vec<MetricRow>,
    pub(crate) event_rows: Vec<EventRow>,
    pub(crate) variant_snapshot_rows: Vec<VariantSnapshotRow>,
}

impl RunSink for BufferedRunSink {
    fn write_run_manifest(&mut self, _run: &RunManifestRecord) -> Result<()> {
        Ok(())
    }

    fn append_trial_record(&mut self, row: &TrialRecord) -> Result<()> {
        self.trial_records.push(row.clone());
        Ok(())
    }

    fn append_metric_rows(&mut self, rows: &[MetricRow]) -> Result<()> {
        self.metric_rows.extend(rows.iter().cloned());
        Ok(())
    }

    fn append_event_rows(&mut self, rows: &[EventRow]) -> Result<()> {
        self.event_rows.extend(rows.iter().cloned());
        Ok(())
    }

    fn append_variant_snapshot(&mut self, rows: &[VariantSnapshotRow]) -> Result<()> {
        self.variant_snapshot_rows.extend(rows.iter().cloned());
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct SqliteRunJournal {
    inner: BackingSqliteStore,
}

impl SqliteRunJournal {
    pub fn new(run_dir: &Path) -> Result<Self> {
        Ok(Self {
            inner: BackingSqliteStore::open(run_dir)?,
        })
    }
}

impl RunSink for SqliteRunJournal {
    fn write_run_manifest(&mut self, run: &RunManifestRecord) -> Result<()> {
        let payload = serde_json::to_value(run)?;
        validate_schema_contract_value(&payload, "run manifest row")?;
        self.inner.put_run_manifest(&run.run_id, &payload)
    }

    fn append_trial_record(&mut self, row: &TrialRecord) -> Result<()> {
        self.inner.upsert_trial_row(TrialRowInsert {
            run_id: &row.run_id,
            trial_id: &row.trial_id,
            schedule_idx: row.schedule_idx,
            attempt: row.attempt,
            row_seq: row.row_seq,
            slot_commit_id: &row.slot_commit_id,
            baseline_id: &row.baseline_id,
            workload_type: &row.workload_type,
            variant_id: &row.variant_id,
            task_id: &row.task_id,
            repl_idx: row.repl_idx,
            outcome: &row.outcome,
            primary_metric_name: &row.primary_metric_name,
            primary_metric_value: &row.primary_metric_value,
            metrics: &row.metrics,
            bindings: &row.bindings,
            hook_events_total: row.hook_events_total,
            has_hook_events: row.has_hook_events,
            row_json: &serde_json::to_value(row)?,
        })
    }

    fn append_metric_rows(&mut self, rows: &[MetricRow]) -> Result<()> {
        for row in rows {
            self.inner.upsert_metric_row(MetricRowInsert {
                run_id: &row.run_id,
                trial_id: &row.trial_id,
                schedule_idx: row.schedule_idx,
                attempt: row.attempt,
                row_seq: row.row_seq,
                slot_commit_id: &row.slot_commit_id,
                variant_id: &row.variant_id,
                task_id: &row.task_id,
                repl_idx: row.repl_idx,
                outcome: &row.outcome,
                metric_name: &row.metric_name,
                metric_value: &row.metric_value,
                metric_source: row.metric_source.as_deref(),
                row_json: &serde_json::to_value(row)?,
            })?;
        }
        Ok(())
    }

    fn append_event_rows(&mut self, rows: &[EventRow]) -> Result<()> {
        for row in rows {
            self.inner.upsert_event_row(EventRowInsert {
                run_id: &row.run_id,
                trial_id: &row.trial_id,
                schedule_idx: row.schedule_idx,
                attempt: row.attempt,
                row_seq: row.row_seq,
                slot_commit_id: &row.slot_commit_id,
                variant_id: &row.variant_id,
                task_id: &row.task_id,
                repl_idx: row.repl_idx,
                seq: row.seq,
                event_type: &row.event_type,
                ts: row.ts.as_deref(),
                payload: &row.payload,
                row_json: &serde_json::to_value(row)?,
            })?;
        }
        Ok(())
    }

    fn append_variant_snapshot(&mut self, rows: &[VariantSnapshotRow]) -> Result<()> {
        for row in rows {
            self.inner
                .upsert_variant_snapshot_row(VariantSnapshotRowInsert {
                    run_id: &row.run_id,
                    trial_id: &row.trial_id,
                    schedule_idx: row.schedule_idx,
                    attempt: row.attempt,
                    row_seq: row.row_seq,
                    slot_commit_id: &row.slot_commit_id,
                    variant_id: &row.variant_id,
                    baseline_id: &row.baseline_id,
                    task_id: &row.task_id,
                    repl_idx: row.repl_idx,
                    binding_name: &row.binding_name,
                    binding_value: &row.binding_value,
                    binding_value_text: &row.binding_value_text,
                    row_json: &serde_json::to_value(row)?,
                })?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

pub(crate) fn append_slot_commit_record(run_dir: &Path, record: &SlotCommitRecord) -> Result<()> {
    let record_json = serde_json::to_value(record)?;
    validate_schema_contract_value(&record_json, "slot commit record")?;
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.upsert_slot_commit_record(&record_json)
}

pub(crate) fn load_slot_commit_records(run_dir: &Path) -> Result<Vec<SlotCommitRecord>> {
    let store = BackingSqliteStore::open(run_dir)?;
    let run_id = store
        .get_runtime_json(RUNTIME_KEY_RUN_CONTROL)?
        .and_then(|value| {
            value
                .pointer("/run_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| store.first_run_id_with_slot_commits().ok().flatten())
        .unwrap_or_default();
    if run_id.is_empty() {
        return Ok(Vec::new());
    }
    let values = store.load_slot_commit_records(&run_id)?;
    let mut rows = Vec::with_capacity(values.len());
    for value in values {
        rows.push(serde_json::from_value::<SlotCommitRecord>(value)?);
    }
    Ok(rows)
}

pub(crate) fn append_jsonl_file(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}

pub(crate) fn append_jsonl(path: &Path, value: &Value) -> Result<()> {
    let mut row = value.clone();
    if let (Some(run_dir), Some(table)) = (
        infer_run_dir_from_path(path),
        json_row_table_from_path(path),
    ) {
        if !path_uses_sqlite_json_row_ingest(&run_dir, path) {
            validate_schema_contract_value(
                &row,
                format!("jsonl row append for {}", path.display()).as_str(),
            )?;
            return append_jsonl_file(path, &row);
        }
        if row.pointer("/run_id").is_none() {
            if let Some(control) =
                BackingSqliteStore::open(&run_dir)?.get_runtime_json(RUNTIME_KEY_RUN_CONTROL)?
            {
                if let Some(run_id) = control.pointer("/run_id").and_then(Value::as_str) {
                    if let Some(obj) = row.as_object_mut() {
                        obj.insert("run_id".to_string(), json!(run_id));
                    }
                }
            }
        }
        validate_schema_contract_value(
            &row,
            format!("jsonl row append for {}", path.display()).as_str(),
        )?;
        if row_has_sqlite_identity_fields(&row) {
            let mut store = BackingSqliteStore::open(&run_dir)?;
            return store.upsert_json_row(table, &row);
        }
        return Err(anyhow!(
            "jsonl append rejected for {}: missing sqlite identity fields (run_id, schedule_idx, attempt, row_seq, slot_commit_id)",
            path.display()
        ));
    }
    Err(anyhow!(
        "jsonl append rejected for {}: path is not mapped to a sqlite json row table",
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::store::run_sqlite_path;
    use rusqlite::Connection;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        std::env::temp_dir().join(format!("agentlab_runner_sink_{}_{}", label, nanos))
    }

    #[test]
    fn sqlite_sink_persists_rows() {
        let run_dir = temp_root("sqlite_sink");
        fs::create_dir_all(&run_dir).expect("create run dir");
        let mut sink = SqliteRunJournal::new(&run_dir).expect("sink should initialize");
        sink.write_run_manifest(&RunManifestRecord {
            schema_version: "run_manifest_v1".to_string(),
            run_id: "run_123".to_string(),
            created_at: "2026-02-22T00:00:00Z".to_string(),
            workload_type: "agent_eval".to_string(),
            baseline_id: "base".to_string(),
            variant_ids: vec!["base".to_string(), "candidate".to_string()],
        })
        .expect("manifest should write");
        sink.append_trial_record(&TrialRecord {
            run_id: "run_123".to_string(),
            trial_id: "trial_1".to_string(),
            schedule_idx: 0,
            slot_commit_id: "slot_test".to_string(),
            attempt: 1,
            row_seq: 0,
            baseline_id: "base".to_string(),
            workload_type: "agent_eval".to_string(),
            variant_id: "base".to_string(),
            task_index: 0,
            task_id: "task_1".to_string(),
            repl_idx: 0,
            outcome: "success".to_string(),
            success: true,
            status_code: "0".to_string(),
            integration_level: "cli_basic".to_string(),
            network_mode_requested: "none".to_string(),
            network_mode_effective: "none".to_string(),
            primary_metric_name: "resolved".to_string(),
            primary_metric_value: json!(1.0),
            metrics: json!({"status_code":"0","resolved":1.0}),
            bindings: json!({"temp":0.2}),
            hook_events_total: 0,
            has_hook_events: false,
        })
        .expect("trial row should write");
        sink.append_metric_rows(&[MetricRow {
            run_id: "run_123".to_string(),
            trial_id: "trial_1".to_string(),
            schedule_idx: 0,
            slot_commit_id: "slot_test".to_string(),
            attempt: 1,
            row_seq: 0,
            variant_id: "base".to_string(),
            task_id: "task_1".to_string(),
            repl_idx: 0,
            outcome: "success".to_string(),
            metric_name: "resolved".to_string(),
            metric_value: json!(1.0),
            metric_source: Some("grader".to_string()),
        }])
        .expect("metric row should write");
        sink.flush().expect("flush should succeed");

        let db_path = run_sqlite_path(&run_dir);
        assert!(db_path.exists(), "sqlite database should exist");
        let conn = Connection::open(&db_path).expect("sqlite connection should open");
        let manifest_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_manifests WHERE run_id = ?1",
                ["run_123"],
                |row| row.get(0),
            )
            .expect("manifest count should load");
        assert_eq!(manifest_count, 1);
        let trial_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM trial_rows WHERE run_id = ?1",
                ["run_123"],
                |row| row.get(0),
            )
            .expect("trial count should load");
        assert_eq!(trial_count, 1);
    }
}
