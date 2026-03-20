use anyhow::{anyhow, Result};
use chrono::Utc;
use lab_core::{ensure_dir, sha256_file};
use serde_json::{json, Value};
use std::fs;
use std::path::Path;

use crate::config::atomic_write_json_pretty;
use crate::model::BenchmarkConfig;
use crate::trial::grade::task_grading_enabled;

pub(crate) fn stage_benchmark_trial_preflight(
    benchmark_config: &BenchmarkConfig,
    trial_dir: &Path,
    run_id: &str,
    trial_id: &str,
    schedule_idx: usize,
    variant_id: &str,
    task_payload: &Value,
    environment_image: Option<&str>,
    trial_input_path: &Path,
) -> Result<()> {
    if benchmark_config.grader.is_none() {
        return Ok(());
    }

    let task_id = task_payload
        .get("id")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("benchmark preflight: task payload missing non-empty id"))?;
    let environment_image = environment_image
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    let grading_enabled = task_grading_enabled(task_payload);
    if !grading_enabled {
        return Err(anyhow!(
            "benchmark preflight: grading.enabled=false was removed in Milestone 4; every benchmark task must emit mapped_grader_output.json"
        ));
    }

    let frozen_dir = trial_dir
        .join("artifacts")
        .join("benchmark_frozen_agent_input");
    ensure_dir(&frozen_dir)?;
    let frozen_input_path = frozen_dir.join("trial_input.json");
    fs::copy(trial_input_path, &frozen_input_path)?;
    let frozen_input_digest = sha256_file(&frozen_input_path)?;

    let preflight = json!({
        "schema_version": "benchmark_trial_preflight_v1",
        "run_id": run_id,
        "trial_id": trial_id,
        "schedule_idx": schedule_idx,
        "variant_id": variant_id,
        "task_id": task_id,
        "environment_image": environment_image,
        "grading": {
            "enabled": grading_enabled,
        },
        "frozen_agent_artifacts": {
            "trial_input_path": frozen_input_path,
            "trial_input_digest": frozen_input_digest,
        },
        "checked_at": Utc::now().to_rfc3339(),
    });
    atomic_write_json_pretty(&trial_dir.join("benchmark_preflight.json"), &preflight)
}
