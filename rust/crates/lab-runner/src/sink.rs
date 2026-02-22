use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const FACTS_DIR: &str = "facts";
const FACTS_TRIALS_FILE: &str = "trials.jsonl";
const FACTS_METRICS_LONG_FILE: &str = "metrics_long.jsonl";
const FACTS_EVENTS_FILE: &str = "events.jsonl";
const FACTS_VARIANT_SNAPSHOTS_FILE: &str = "variant_snapshots.jsonl";
const FACTS_RUN_MANIFEST_FILE: &str = "run_manifest.json";

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

pub struct JsonlRunSink {
    run_manifest_path: PathBuf,
    trials_writer: BufWriter<File>,
    metrics_writer: BufWriter<File>,
    events_writer: BufWriter<File>,
    variant_snapshots_writer: BufWriter<File>,
}

impl JsonlRunSink {
    pub fn new(run_dir: &Path) -> Result<Self> {
        let facts_dir = run_dir.join(FACTS_DIR);
        fs::create_dir_all(&facts_dir)?;

        Ok(Self {
            run_manifest_path: facts_dir.join(FACTS_RUN_MANIFEST_FILE),
            trials_writer: open_append(facts_dir.join(FACTS_TRIALS_FILE))?,
            metrics_writer: open_append(facts_dir.join(FACTS_METRICS_LONG_FILE))?,
            events_writer: open_append(facts_dir.join(FACTS_EVENTS_FILE))?,
            variant_snapshots_writer: open_append(facts_dir.join(FACTS_VARIANT_SNAPSHOTS_FILE))?,
        })
    }
}

impl RunSink for JsonlRunSink {
    fn write_run_manifest(&mut self, run: &RunManifestRecord) -> Result<()> {
        fs::write(&self.run_manifest_path, serde_json::to_vec_pretty(run)?)?;
        Ok(())
    }

    fn append_trial_record(&mut self, row: &TrialRecord) -> Result<()> {
        append_row(&mut self.trials_writer, row)
    }

    fn append_metric_rows(&mut self, rows: &[MetricRow]) -> Result<()> {
        for row in rows {
            append_row(&mut self.metrics_writer, row)?;
        }
        Ok(())
    }

    fn append_event_rows(&mut self, rows: &[EventRow]) -> Result<()> {
        for row in rows {
            append_row(&mut self.events_writer, row)?;
        }
        Ok(())
    }

    fn append_variant_snapshot(&mut self, rows: &[VariantSnapshotRow]) -> Result<()> {
        for row in rows {
            append_row(&mut self.variant_snapshots_writer, row)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.trials_writer.flush()?;
        self.metrics_writer.flush()?;
        self.events_writer.flush()?;
        self.variant_snapshots_writer.flush()?;
        Ok(())
    }
}

fn open_append(path: PathBuf) -> Result<BufWriter<File>> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    Ok(BufWriter::new(file))
}

fn append_row<T: Serialize>(writer: &mut BufWriter<File>, row: &T) -> Result<()> {
    serde_json::to_writer(&mut *writer, row)?;
    writer.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        std::env::temp_dir().join(format!("agentlab_runner_sink_{}_{}", label, nanos))
    }

    #[test]
    fn jsonl_sink_appends_fact_rows() {
        let run_dir = temp_root("append");
        fs::create_dir_all(&run_dir).expect("create run dir");
        let mut sink = JsonlRunSink::new(&run_dir).expect("sink should initialize");
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
        sink.append_metric_rows(&[
            MetricRow {
                run_id: "run_123".to_string(),
                trial_id: "trial_1".to_string(),
                variant_id: "base".to_string(),
                task_id: "task_1".to_string(),
                repl_idx: 0,
                outcome: "success".to_string(),
                metric_name: "resolved".to_string(),
                metric_value: json!(1.0),
                metric_source: Some("primary".to_string()),
            },
            MetricRow {
                run_id: "run_123".to_string(),
                trial_id: "trial_1".to_string(),
                variant_id: "base".to_string(),
                task_id: "task_1".to_string(),
                repl_idx: 0,
                outcome: "success".to_string(),
                metric_name: "status_code".to_string(),
                metric_value: json!("0"),
                metric_source: None,
            },
        ])
        .expect("metric rows should append");
        sink.append_event_rows(&[EventRow {
            run_id: "run_123".to_string(),
            trial_id: "trial_1".to_string(),
            variant_id: "base".to_string(),
            task_id: "task_1".to_string(),
            repl_idx: 0,
            seq: 0,
            event_type: "tool_call".to_string(),
            ts: Some("2026-02-22T00:00:01Z".to_string()),
            payload: json!({"event_type":"tool_call"}),
        }])
        .expect("event rows should append");
        sink.append_variant_snapshot(&[VariantSnapshotRow {
            run_id: "run_123".to_string(),
            trial_id: "trial_1".to_string(),
            variant_id: "base".to_string(),
            baseline_id: "base".to_string(),
            task_id: "task_1".to_string(),
            repl_idx: 0,
            binding_name: "temp".to_string(),
            binding_value: json!(0.2),
            binding_value_text: "0.2".to_string(),
        }])
        .expect("variant snapshots should append");
        sink.flush().expect("flush should succeed");

        let facts_dir = run_dir.join("facts");
        assert!(facts_dir.join("run_manifest.json").exists());
        assert_eq!(
            fs::read_to_string(facts_dir.join("trials.jsonl"))
                .expect("trials file should exist")
                .lines()
                .count(),
            1
        );
        assert_eq!(
            fs::read_to_string(facts_dir.join("metrics_long.jsonl"))
                .expect("metrics file should exist")
                .lines()
                .count(),
            2
        );
        assert_eq!(
            fs::read_to_string(facts_dir.join("events.jsonl"))
                .expect("events file should exist")
                .lines()
                .count(),
            1
        );
        assert_eq!(
            fs::read_to_string(facts_dir.join("variant_snapshots.jsonl"))
                .expect("variant snapshots file should exist")
                .lines()
                .count(),
            1
        );
        let _ = fs::remove_dir_all(run_dir);
    }
}
