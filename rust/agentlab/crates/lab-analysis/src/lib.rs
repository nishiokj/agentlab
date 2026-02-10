use anyhow::Result;
use lab_core::ensure_dir;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;

pub fn summarize_trial(
    run_id: &str,
    trial_output: &Value,
    trial_id: &str,
    workload_type: &str,
    variant_id: &str,
    task_idx: usize,
    task_id: &str,
    repl: usize,
    status: String,
    container_mode: bool,
    integration_level: &str,
    network_mode_requested: &str,
    network_mode_effective: &str,
) -> Value {
    let outcome = trial_output
        .get("outcome")
        .and_then(|v| v.as_str())
        .unwrap_or("error");
    let (primary_metric_name, primary_metric_value) =
        if let Some(obj) = trial_output.get("objective").and_then(|v| v.as_object()) {
            let name = obj
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("primary_metric")
                .to_string();
            let value = obj.get("value").cloned().unwrap_or(json!(null));
            (name, value)
        } else {
            let fallback = if outcome == "success" { 1.0 } else { 0.0 };
            ("success".to_string(), json!(fallback))
        };
    let mut metrics = trial_output.get("metrics").cloned().unwrap_or(json!({}));
    if let Some(obj) = metrics.as_object_mut() {
        obj.insert("status_code".to_string(), json!(status));
    }
    json!({
        "run_id": run_id,
        "trial_id": trial_id,
        "workload_type": workload_type,
        "variant_id": variant_id,
        "task_index": task_idx,
        "task_id": task_id,
        "repl_idx": repl,
        "outcome": outcome,
        "success": outcome == "success",
        "container_mode": container_mode,
        "integration_level": integration_level,
        "network_mode_requested": network_mode_requested,
        "network_mode_effective": network_mode_effective,
        "primary_metric_name": primary_metric_name,
        "primary_metric_value": primary_metric_value,
        "metrics": metrics,
    })
}

pub fn write_analysis(
    analysis_dir: &Path,
    summaries: &[Value],
    baseline_id: &str,
    event_counts: &BTreeMap<String, BTreeMap<String, usize>>,
    trial_event_counts: &BTreeMap<String, BTreeMap<String, usize>>,
) -> Result<()> {
    let mut outcomes: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for s in summaries {
        let vid = s
            .get("variant_id")
            .and_then(|v| v.as_str())
            .unwrap_or("base");
        outcomes.entry(vid.to_string()).or_default().push(s);
    }

    let mut summary_map = BTreeMap::new();
    for (variant, rows) in outcomes.iter() {
        let total = rows.len() as f64;
        let successes = rows
            .iter()
            .filter(|r| r.get("outcome").and_then(|v| v.as_str()) == Some("success"))
            .count() as f64;
        let success_rate = if total > 0.0 { successes / total } else { 0.0 };
        let primary_metric_name = rows
            .iter()
            .find_map(|r| r.get("primary_metric_name").and_then(|v| v.as_str()))
            .unwrap_or("success");
        let mut pm_sum = 0.0f64;
        let mut pm_n = 0usize;
        for r in rows {
            if let Some(v) = r.get("primary_metric_value").and_then(|v| v.as_f64()) {
                pm_sum += v;
                pm_n += 1;
            }
        }
        let primary_metric_mean = if pm_n > 0 { pm_sum / pm_n as f64 } else { 0.0 };
        summary_map.insert(
            variant.clone(),
            json!({
                "total": total,
                "success_rate": success_rate,
                "primary_metric_name": primary_metric_name,
                "primary_metric_mean": primary_metric_mean,
                "event_counts": event_counts.get(variant).cloned().unwrap_or_default()
            }),
        );
    }

    let summary = json!({
        "schema_version": "analysis_summary_v1",
        "baseline_id": baseline_id,
        "variants": summary_map,
    });
    fs::write(
        analysis_dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;

    let mut comparisons = Vec::new();
    for (variant, data) in summary_map.iter() {
        if variant == baseline_id {
            continue;
        }
        let base = summary_map.get(baseline_id).cloned().unwrap_or(json!({}));
        comparisons.push(json!({
            "baseline": baseline_id,
            "variant": variant,
            "baseline_success_rate": base.get("success_rate").cloned().unwrap_or(json!(0.0)),
            "variant_success_rate": data.get("success_rate").cloned().unwrap_or(json!(0.0)),
        }));
    }

    let comparisons_json = json!({
        "schema_version": "analysis_comparisons_v1",
        "comparisons": comparisons
    });
    fs::write(
        analysis_dir.join("comparisons.json"),
        serde_json::to_vec_pretty(&comparisons_json)?,
    )?;

    write_analysis_tables(
        analysis_dir,
        summaries,
        baseline_id,
        &summary_map,
        event_counts,
        trial_event_counts,
    )?;

    Ok(())
}

fn write_analysis_tables(
    analysis_dir: &Path,
    summaries: &[Value],
    baseline_id: &str,
    summary_map: &BTreeMap<String, Value>,
    event_counts: &BTreeMap<String, BTreeMap<String, usize>>,
    trial_event_counts: &BTreeMap<String, BTreeMap<String, usize>>,
) -> Result<()> {
    let tables_dir = analysis_dir.join("tables");
    ensure_dir(&tables_dir)?;

    let mut trials = fs::File::create(tables_dir.join("trials.jsonl"))?;
    let mut metrics_long = fs::File::create(tables_dir.join("metrics_long.jsonl"))?;
    let mut events_by_trial = fs::File::create(tables_dir.join("event_counts_by_trial.jsonl"))?;
    let mut events_by_variant = fs::File::create(tables_dir.join("event_counts_by_variant.jsonl"))?;
    let mut variant_summary = fs::File::create(tables_dir.join("variant_summary.jsonl"))?;

    for s in summaries {
        let trial_id = s
            .get("trial_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let hook_counts = trial_event_counts
            .get(&trial_id)
            .cloned()
            .unwrap_or_default();
        let hook_total: usize = hook_counts.values().sum();
        let mut trial_row = s.clone();
        if let Some(obj) = trial_row.as_object_mut() {
            obj.insert("baseline_id".to_string(), json!(baseline_id));
            obj.insert("hook_events_total".to_string(), json!(hook_total));
            obj.insert("has_hook_events".to_string(), json!(hook_total > 0));
        }
        serde_json::to_writer(&mut trials, &trial_row)?;
        writeln!(&mut trials)?;

        if let Some(metrics) = s.get("metrics").and_then(|v| v.as_object()) {
            for (metric_name, metric_value) in metrics {
                let row = json!({
                    "run_id": s.get("run_id").cloned().unwrap_or(json!(null)),
                    "trial_id": s.get("trial_id").cloned().unwrap_or(json!(null)),
                    "variant_id": s.get("variant_id").cloned().unwrap_or(json!(null)),
                    "task_id": s.get("task_id").cloned().unwrap_or(json!(null)),
                    "repl_idx": s.get("repl_idx").cloned().unwrap_or(json!(null)),
                    "outcome": s.get("outcome").cloned().unwrap_or(json!(null)),
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                });
                serde_json::to_writer(&mut metrics_long, &row)?;
                writeln!(&mut metrics_long)?;
            }
        }
        if let (Some(name), Some(value)) = (
            s.get("primary_metric_name").and_then(|v| v.as_str()),
            s.get("primary_metric_value"),
        ) {
            let row = json!({
                "run_id": s.get("run_id").cloned().unwrap_or(json!(null)),
                "trial_id": s.get("trial_id").cloned().unwrap_or(json!(null)),
                "variant_id": s.get("variant_id").cloned().unwrap_or(json!(null)),
                "task_id": s.get("task_id").cloned().unwrap_or(json!(null)),
                "repl_idx": s.get("repl_idx").cloned().unwrap_or(json!(null)),
                "outcome": s.get("outcome").cloned().unwrap_or(json!(null)),
                "metric_name": name,
                "metric_value": value,
                "metric_source": "primary"
            });
            serde_json::to_writer(&mut metrics_long, &row)?;
            writeln!(&mut metrics_long)?;
        }
    }

    for (trial_id, counts) in trial_event_counts {
        for (event_type, count) in counts {
            let row = json!({
                "trial_id": trial_id,
                "event_type": event_type,
                "count": count
            });
            serde_json::to_writer(&mut events_by_trial, &row)?;
            writeln!(&mut events_by_trial)?;
        }
    }

    for (variant_id, counts) in event_counts {
        for (event_type, count) in counts {
            let row = json!({
                "variant_id": variant_id,
                "event_type": event_type,
                "count": count
            });
            serde_json::to_writer(&mut events_by_variant, &row)?;
            writeln!(&mut events_by_variant)?;
        }
    }

    for (variant_id, data) in summary_map {
        let row = json!({
            "baseline_id": baseline_id,
            "variant_id": variant_id,
            "total": data.get("total").cloned().unwrap_or(json!(0)),
            "success_rate": data.get("success_rate").cloned().unwrap_or(json!(0.0)),
            "primary_metric_name": data.get("primary_metric_name").cloned().unwrap_or(json!("success")),
            "primary_metric_mean": data.get("primary_metric_mean").cloned().unwrap_or(json!(0.0)),
            "event_counts": data.get("event_counts").cloned().unwrap_or(json!({})),
        });
        serde_json::to_writer(&mut variant_summary, &row)?;
        writeln!(&mut variant_summary)?;
    }

    let duckdb_sql = r#"-- Run from analysis directory:
-- duckdb .lab/runs/<run_id>/analysis/agentlab.duckdb < tables/load_duckdb.sql
INSTALL json;
LOAD json;

CREATE OR REPLACE VIEW trials AS
SELECT * FROM read_json_auto('tables/trials.jsonl', format='newline_delimited');

CREATE OR REPLACE VIEW metrics_long AS
SELECT * FROM read_json_auto('tables/metrics_long.jsonl', format='newline_delimited');

CREATE OR REPLACE VIEW event_counts_by_trial AS
SELECT * FROM read_json_auto('tables/event_counts_by_trial.jsonl', format='newline_delimited');

CREATE OR REPLACE VIEW event_counts_by_variant AS
SELECT * FROM read_json_auto('tables/event_counts_by_variant.jsonl', format='newline_delimited');

CREATE OR REPLACE VIEW variant_summary AS
SELECT * FROM read_json_auto('tables/variant_summary.jsonl', format='newline_delimited');
"#;
    fs::write(tables_dir.join("load_duckdb.sql"), duckdb_sql)?;

    Ok(())
}
