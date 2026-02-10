use anyhow::{anyhow, Result};
use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessManifest {
    pub schema_version: String,
    pub integration_level: String,
    pub step: Option<ManifestStep>,
    pub hooks: Option<ManifestHooks>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestStep {
    pub semantics: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestHooks {
    pub schema_version: String,
    pub events_path: String,
    pub header_event_emitted: Option<bool>,
}

#[derive(Debug)]
pub struct HookValidationError {
    pub message: String,
    pub line: Option<usize>,
    pub seq: Option<i64>,
    pub event_type: Option<String>,
}

impl std::fmt::Display for HookValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for HookValidationError {}

pub fn load_manifest(path: &Path) -> Result<HarnessManifest> {
    let data = std::fs::read_to_string(path)?;
    let manifest: HarnessManifest = serde_json::from_str(&data)?;
    Ok(manifest)
}

pub fn validate_hooks(
    manifest: &HarnessManifest,
    events_path: &Path,
    schema: &JSONSchema,
) -> Result<()> {
    let file = File::open(events_path)?;
    let reader = BufReader::new(file);

    let mut last_seq: Option<i64> = None;
    let mut step_started = false;
    let mut last_step_index: Option<i64> = None;
    let mut seen_steps = false;
    let mut waiting_for_ack: Option<i64> = None;
    let mut stop_seen = false;

    for (idx, line) in reader.lines().enumerate() {
        let line_no = idx + 1;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line).map_err(|e| HookValidationError {
            message: format!("invalid JSON at line {}: {}", line_no, e),
            line: Some(line_no),
            seq: None,
            event_type: None,
        })?;

        if let Err(errors) = schema.validate(&value) {
            let mut msgs = Vec::new();
            for e in errors {
                msgs.push(e.to_string());
            }
            return Err(HookValidationError {
                message: format!(
                    "schema validation failed at line {}: {}",
                    line_no,
                    msgs.join("; ")
                ),
                line: Some(line_no),
                seq: value.get("seq").and_then(|v| v.as_i64()),
                event_type: value
                    .get("event_type")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            }
            .into());
        }

        let seq = value
            .get("seq")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| HookValidationError {
                message: format!("missing seq at line {}", line_no),
                line: Some(line_no),
                seq: None,
                event_type: None,
            })?;

        if let Some(prev) = last_seq {
            if seq <= prev {
                return Err(HookValidationError {
                    message: format!("non-monotonic seq at line {}: {} <= {}", line_no, seq, prev),
                    line: Some(line_no),
                    seq: Some(seq),
                    event_type: value
                        .get("event_type")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                }
                .into());
            }
        }
        last_seq = Some(seq);

        let event_type = value
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if stop_seen && event_type == "agent_step_start" {
            return Err(HookValidationError {
                message: format!("agent_step_start after stop at line {}", line_no),
                line: Some(line_no),
                seq: Some(seq),
                event_type: Some(event_type.to_string()),
            }
            .into());
        }

        match event_type {
            "agent_step_start" => {
                seen_steps = true;
                let step_index = value
                    .get("step_index")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| HookValidationError {
                        message: format!("missing step_index at line {}", line_no),
                        line: Some(line_no),
                        seq: Some(seq),
                        event_type: Some(event_type.to_string()),
                    })?;
                if waiting_for_ack.is_some() {
                    return Err(HookValidationError {
                        message: format!("agent_step_start before control_ack at line {}", line_no),
                        line: Some(line_no),
                        seq: Some(seq),
                        event_type: Some(event_type.to_string()),
                    }
                    .into());
                }
                if step_started {
                    return Err(HookValidationError {
                        message: format!("nested agent_step_start at line {}", line_no),
                        line: Some(line_no),
                        seq: Some(seq),
                        event_type: Some(event_type.to_string()),
                    }
                    .into());
                }
                if let Some(last) = last_step_index {
                    if step_index != last + 1 {
                        return Err(HookValidationError {
                            message: format!("non-sequential step_index at line {}", line_no),
                            line: Some(line_no),
                            seq: Some(seq),
                            event_type: Some(event_type.to_string()),
                        }
                        .into());
                    }
                }
                step_started = true;
                last_step_index = Some(step_index);
            }
            "agent_step_end" => {
                seen_steps = true;
                let step_index = value
                    .get("step_index")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| HookValidationError {
                        message: format!("missing step_index at line {}", line_no),
                        line: Some(line_no),
                        seq: Some(seq),
                        event_type: Some(event_type.to_string()),
                    })?;
                if !step_started {
                    return Err(HookValidationError {
                        message: format!("agent_step_end without start at line {}", line_no),
                        line: Some(line_no),
                        seq: Some(seq),
                        event_type: Some(event_type.to_string()),
                    }
                    .into());
                }
                if let Some(last) = last_step_index {
                    if step_index != last {
                        return Err(HookValidationError {
                            message: format!(
                                "agent_step_end step_index mismatch at line {}",
                                line_no
                            ),
                            line: Some(line_no),
                            seq: Some(seq),
                            event_type: Some(event_type.to_string()),
                        }
                        .into());
                    }
                }
                step_started = false;
                waiting_for_ack = Some(step_index);
            }
            "control_ack" => {
                let step_index = value
                    .get("step_index")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| HookValidationError {
                        message: format!("missing step_index at line {}", line_no),
                        line: Some(line_no),
                        seq: Some(seq),
                        event_type: Some(event_type.to_string()),
                    })?;
                if let Some(waiting) = waiting_for_ack {
                    if step_index != waiting {
                        return Err(HookValidationError {
                            message: format!("control_ack step_index mismatch at line {}", line_no),
                            line: Some(line_no),
                            seq: Some(seq),
                            event_type: Some(event_type.to_string()),
                        }
                        .into());
                    }
                    waiting_for_ack = None;
                } else {
                    return Err(HookValidationError {
                        message: format!(
                            "control_ack without pending step_end at line {}",
                            line_no
                        ),
                        line: Some(line_no),
                        seq: Some(seq),
                        event_type: Some(event_type.to_string()),
                    }
                    .into());
                }
                if value.get("action_observed").and_then(|v| v.as_str()) == Some("stop") {
                    stop_seen = true;
                }
            }
            "model_call_end" | "tool_call_end" => {
                if seen_steps {
                    let step_index = value.get("step_index").and_then(|v| v.as_i64());
                    if step_index.is_none() {
                        return Err(HookValidationError {
                            message: format!(
                                "missing step_index for causal event at line {}",
                                line_no
                            ),
                            line: Some(line_no),
                            seq: Some(seq),
                            event_type: Some(event_type.to_string()),
                        }
                        .into());
                    }
                }
            }
            _ => {}
        }
    }

    if waiting_for_ack.is_some() {
        return Err(anyhow!("missing control_ack after final agent_step_end"));
    }

    // Manifest sanity checks
    if matches!(
        manifest.integration_level.as_str(),
        "cli_events" | "otel" | "sdk_control" | "sdk_full"
    ) {
        if manifest.hooks.is_none() && manifest.integration_level != "otel" {
            return Err(anyhow!("hooks manifest required for integration_level"));
        }
    }

    Ok(())
}
