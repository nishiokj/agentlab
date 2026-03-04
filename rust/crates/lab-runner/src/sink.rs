use crate::persistence::sqlite_store::{
    EventRowInsert, MetricRowInsert, SqliteRunStore as BackingSqliteStore, TrialRowInsert,
    VariantSnapshotRowInsert,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifestRecord {
    pub schema_version: String,
    pub run_id: String,
    pub created_at: String,
    pub workload_type: String,
    pub baseline_id: String,
    pub variant_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrialRecord {
    pub run_id: String,
    pub trial_id: String,
    pub schedule_idx: usize,
    pub slot_commit_id: String,
    pub attempt: usize,
    pub row_seq: usize,
    pub baseline_id: String,
    pub workload_type: String,
    pub variant_id: String,
    pub task_index: usize,
    pub task_id: String,
    pub repl_idx: usize,
    pub outcome: String,
    pub success: bool,
    pub status_code: String,
    pub container_mode: bool,
    pub integration_level: String,
    pub network_mode_requested: String,
    pub network_mode_effective: String,
    pub primary_metric_name: String,
    pub primary_metric_value: Value,
    pub metrics: Value,
    pub bindings: Value,
    pub hook_events_total: usize,
    pub has_hook_events: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricRow {
    pub run_id: String,
    pub trial_id: String,
    pub schedule_idx: usize,
    pub slot_commit_id: String,
    pub attempt: usize,
    pub row_seq: usize,
    pub variant_id: String,
    pub task_id: String,
    pub repl_idx: usize,
    pub outcome: String,
    pub metric_name: String,
    pub metric_value: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRow {
    pub run_id: String,
    pub trial_id: String,
    pub schedule_idx: usize,
    pub slot_commit_id: String,
    pub attempt: usize,
    pub row_seq: usize,
    pub variant_id: String,
    pub task_id: String,
    pub repl_idx: usize,
    pub seq: usize,
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantSnapshotRow {
    pub run_id: String,
    pub trial_id: String,
    pub schedule_idx: usize,
    pub slot_commit_id: String,
    pub attempt: usize,
    pub row_seq: usize,
    pub variant_id: String,
    pub baseline_id: String,
    pub task_id: String,
    pub repl_idx: usize,
    pub binding_name: String,
    pub binding_value: Value,
    pub binding_value_text: String,
}

pub trait RunSink {
    fn write_run_manifest(&mut self, run: &RunManifestRecord) -> Result<()>;
    fn append_trial_record(&mut self, row: &TrialRecord) -> Result<()>;
    fn append_metric_rows(&mut self, rows: &[MetricRow]) -> Result<()>;
    fn append_event_rows(&mut self, rows: &[EventRow]) -> Result<()>;
    fn append_variant_snapshot(&mut self, rows: &[VariantSnapshotRow]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

pub struct SqliteRunStore {
    inner: BackingSqliteStore,
}

impl SqliteRunStore {
    pub fn new(run_dir: &Path) -> Result<Self> {
        Ok(Self {
            inner: BackingSqliteStore::open(run_dir)?,
        })
    }
}

// Temporary compatibility alias while caller code transitions to SqliteRunStore naming.
pub type JsonlRunSink = SqliteRunStore;

impl RunSink for SqliteRunStore {
    fn write_run_manifest(&mut self, run: &RunManifestRecord) -> Result<()> {
        let payload = serde_json::to_value(run)?;
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
        // SQLite transactions are committed by each statement in this sink path.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::sqlite_store::{run_sqlite_path, SqliteRunStore as BackingStore};
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
        let mut sink = SqliteRunStore::new(&run_dir).expect("sink should initialize");
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
            container_mode: true,
            integration_level: "cli_basic".to_string(),
            network_mode_requested: "none".to_string(),
            network_mode_effective: "none".to_string(),
            primary_metric_name: "resolved".to_string(),
            primary_metric_value: json!(1.0),
            metrics: json!({"status_code":"0","resolved":1.0}),
            bindings: json!({"temp":0.2}),
            hook_events_total: 1,
            has_hook_events: true,
        })
        .expect("trial row should append");
        sink.flush().expect("flush should succeed");

        let db_path = run_sqlite_path(&run_dir);
        assert!(db_path.exists());
        let db = BackingStore::open(&run_dir).expect("open sqlite");
        assert_eq!(db.row_count("run_manifests").expect("count"), 1);
        assert_eq!(db.row_count("trial_rows").expect("count"), 1);
    }
}
