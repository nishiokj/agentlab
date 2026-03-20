use anyhow::{anyhow, Result};
use lab_schemas::compile_schema;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::config::*;
use crate::model::*;

pub(crate) fn validate_required_fields(json_value: &Value) -> Result<()> {
    if json_value
        .pointer("/version")
        .and_then(|value| value.as_str())
        .is_some_and(|value| value.trim() == "1.0")
    {
        return Err(anyhow!("legacy experiment version '1.0' is not supported"));
    }
    for (pointer, message) in [
        (
            "/runtime/agent",
            "use runtime.agent_runtime plus policy.task_sandbox only",
        ),
        (
            "/runtime/sandbox",
            "use runtime.agent_runtime plus policy.task_sandbox only",
        ),
        (
            "/runtime/policy",
            "use runtime.agent_runtime plus policy.task_sandbox only",
        ),
        (
            "/runtime/agent_runtime/io",
            "commands consume the trial contract directly; no runner IO remapping is supported",
        ),
        (
            "/runtime/agent_runtime/workspace_patches",
            "workspace patches were removed; task-owned inputs must come from task rows or packaged artifacts",
        ),
        (
            "/runtime/agent_runtime/launch",
            "launch indirection was removed; use runtime.agent_runtime.{artifact,image,command,env}",
        ),
        (
            "/runtime/agent_runtime/env_from_host",
            "use $NAME runtime bindings resolved from variant bindings or lab run --env/--env-file",
        ),
        (
            "/runtime/agent_runtime/binding_args",
            "commands are literal argv; project bindings directly in runtime.agent_runtime.command",
        ),
        (
            "/runtime/agent_runtime/support_files",
            "runtime support file staging was removed; package files in the agent artifact or benchmark-owned sealed assets",
        ),
        (
            "/runtime/agent_runtime/secret_env",
            "use $NAME runtime bindings resolved from variant bindings or lab run --env/--env-file",
        ),
        (
            "/runtime/dependencies/file_staging",
            "host-path file staging was removed; package files in the agent artifact or task rows",
        ),
        (
            "/runtime/dependencies/assets",
            "dependency asset staging was removed; task-owned inputs must be embedded in task rows",
        ),
        (
            "/runtime/dependencies/secret_files",
            "secret file staging was removed; inject secrets at launch time, not through authored host paths",
        ),
        (
            "/benchmark/grader/support_files",
            "benchmark grader support_files was removed; reference grader files directly in grader.command or use runner-owned built-ins",
        ),
        (
            "/benchmark/adapter/support_files",
            "benchmark adapter support_files was removed; benchmark assets must be runner-owned sealed assets",
        ),
    ] {
        if json_value.pointer(pointer).is_some() {
            return Err(anyhow!(
                "{} was removed in the hard cutover; {}",
                pointer,
                message
            ));
        }
    }
    let required: &[&str] = &[
        "/experiment/workload_type",
        "/design/replications",
        "/policy/timeout_ms",
        "/policy/task_sandbox/network",
        "/baseline/variant_id",
    ];
    let mut missing = Vec::new();
    for pointer in required {
        let value = json_value.pointer(pointer);
        let is_missing = match value {
            None => true,
            Some(Value::String(s)) => s.is_empty(),
            Some(Value::Number(n)) => {
                n.as_u64() == Some(0)
                    && (*pointer == "/design/replications" || *pointer == "/policy/timeout_ms")
            }
            _ => false,
        };
        if is_missing {
            missing.push(*pointer);
        }
    }
    if json_value.pointer("/runtime/agent_runtime").is_none() {
        missing.push("/runtime/agent_runtime");
    }
    if json_value.pointer("/policy/task_sandbox").is_none() {
        missing.push("/policy/task_sandbox");
    }
    let has_command = match json_value.pointer("/runtime/agent_runtime/command") {
        Some(Value::String(s)) => !s.trim().is_empty(),
        Some(Value::Array(parts)) if !parts.is_empty() => parts
            .iter()
            .all(|part| part.as_str().map(|s| !s.trim().is_empty()).unwrap_or(false)),
        _ => false,
    };
    if !has_command {
        missing.push("/runtime/agent_runtime/command");
    }
    let artifact = json_value
        .pointer("/runtime/agent_runtime/artifact")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty());
    if artifact.is_none() {
        missing.push("/runtime/agent_runtime/artifact");
    }
    let image = json_value
        .pointer("/runtime/agent_runtime/image")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty());
    if image.is_none() {
        missing.push("/runtime/agent_runtime/image");
    }
    if json_value
        .pointer("/benchmark/grader/command")
        .and_then(Value::as_array)
        .is_none_or(|command| command.is_empty())
    {
        missing.push("/benchmark/grader/command");
    }
    let experiment_id = json_value
        .pointer("/experiment/id")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if experiment_id.is_empty() {
        missing.push("/experiment/id");
    }
    let baseline_id = json_value
        .pointer("/baseline/variant_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if baseline_id.is_empty() {
        missing.push("/baseline/variant_id");
    }
    if !missing.is_empty() {
        missing.sort_unstable();
        missing.dedup();
        return Err(anyhow!(
            "missing required experiment fields: {}",
            missing.join(", ")
        ));
    }

    let image_source = json_value
        .pointer("/benchmark/image_source")
        .and_then(Value::as_str)
        .unwrap_or("experiment");
    match image_source {
        "experiment" | "per_task" => {}
        other => {
            return Err(anyhow!(
                "benchmark.image_source must be 'experiment' or 'per_task' (found '{}')",
                other
            ));
        }
    }
    if image_source == "per_task" {
        let workload_type = json_value
            .pointer("/experiment/workload_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if workload_type != "container" {
            return Err(anyhow!(
                "benchmark.image_source=per_task requires experiment.workload_type=container"
            ));
        }
        if artifact.is_none() {
            return Err(anyhow!(
                "benchmark.image_source=per_task still requires runtime.agent_runtime.artifact"
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_schema_contract_value(value: &Value, context: &str) -> Result<()> {
    let Some(schema_version) = value.pointer("/schema_version").and_then(Value::as_str) else {
        return Ok(());
    };
    let schema_name = format!("{}.jsonschema", schema_version);
    compile_schema(&schema_name).map_err(|err| {
        anyhow!(
            "missing schema contract for schema_version '{}' in {} (expected schemas/{}): {}",
            schema_version,
            context,
            schema_name,
            err
        )
    })?;
    Ok(())
}

pub(crate) fn load_experiment_overrides(overrides_path: &Path) -> Result<ExperimentOverrides> {
    let overrides_schema = compile_schema("experiment_overrides_v1.jsonschema")?;
    let overrides_data = fs::read_to_string(overrides_path)?;
    let overrides_json: Value = serde_json::from_str(&overrides_data)?;
    if let Err(errors) = overrides_schema.validate(&overrides_json) {
        let mut msgs = Vec::new();
        for e in errors {
            msgs.push(e.to_string());
        }
        return Err(anyhow!(
            "overrides schema validation failed ({}): {}",
            overrides_path.display(),
            msgs.join("; ")
        ));
    }
    let overrides: ExperimentOverrides = serde_json::from_value(overrides_json)?;
    if overrides.schema_version != "experiment_overrides_v1" {
        return Err(anyhow!(
            "unsupported overrides schema_version: {}",
            overrides.schema_version
        ));
    }
    Ok(overrides)
}

pub(crate) fn load_knob_manifest(manifest_path: &Path) -> Result<KnobManifest> {
    let manifest_schema = compile_schema("knob_manifest_v1.jsonschema")?;
    let manifest_data = fs::read_to_string(manifest_path)?;
    let manifest_json: Value = serde_json::from_str(&manifest_data)?;
    if let Err(errors) = manifest_schema.validate(&manifest_json) {
        let mut msgs = Vec::new();
        for e in errors {
            msgs.push(e.to_string());
        }
        return Err(anyhow!(
            "knob manifest schema validation failed ({}): {}",
            manifest_path.display(),
            msgs.join("; ")
        ));
    }
    let manifest: KnobManifest = serde_json::from_value(manifest_json)?;
    if manifest.schema_version != "knob_manifest_v1" {
        return Err(anyhow!(
            "unsupported knob manifest schema_version: {}",
            manifest.schema_version
        ));
    }
    Ok(manifest)
}

pub(crate) fn validate_knob_value(knob: &KnobDef, value: &Value) -> Result<()> {
    if !value_matches_type(value, &knob.value_type) {
        return Err(anyhow!(
            "override value type mismatch for knob {}: expected {}, got {}",
            knob.id,
            knob.value_type,
            value_type_name(value)
        ));
    }
    if let Some(options) = knob.options.as_ref() {
        if !options.iter().any(|option| option == value) {
            return Err(anyhow!(
                "override value for knob {} is not in allowed options",
                knob.id
            ));
        }
    }
    if let Some(minimum) = knob.minimum {
        if let Some(v) = value.as_f64() {
            if v < minimum {
                return Err(anyhow!(
                    "override value for knob {} is below minimum {}",
                    knob.id,
                    minimum
                ));
            }
        }
    }
    if let Some(maximum) = knob.maximum {
        if let Some(v) = value.as_f64() {
            if v > maximum {
                return Err(anyhow!(
                    "override value for knob {} is above maximum {}",
                    knob.id,
                    maximum
                ));
            }
        }
    }
    Ok(())
}

pub fn validate_knob_overrides(manifest_path: &Path, overrides_path: &Path) -> Result<()> {
    let manifest = load_knob_manifest(manifest_path)?;
    let overrides = load_experiment_overrides(overrides_path)?;
    let mut by_id: BTreeMap<String, KnobDef> = BTreeMap::new();
    for knob in manifest.knobs {
        by_id.insert(knob.id.clone(), knob);
    }
    for (id, value) in overrides.values.iter() {
        let knob = by_id
            .get(id)
            .ok_or_else(|| anyhow!("override references unknown knob id: {}", id))?;
        validate_knob_value(knob, value)?;
    }
    Ok(())
}
