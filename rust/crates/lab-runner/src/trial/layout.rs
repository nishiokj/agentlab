use anyhow::Result;
use lab_core::{ensure_dir, AGENTLAB_CONTRACT_IN_DIR, AGENTLAB_CONTRACT_OUT_DIR};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::atomic_write_json_pretty;
use crate::experiment::runner::{
    map_contract_path_to_host, ContractPathHostRoots, ContractPathMode,
};
use crate::experiment::runtime::{AgentRuntimeConfig, DEFAULT_TASK_WORKDIR_FALLBACK};
use crate::model::MaterializationMode;
use crate::trial::execution::resolve_container_image_digest;
use crate::trial::prepare::TrialPaths;
use crate::util::{copy_dir_preserve_contents, copy_file_if_exists, remove_path_if_exists};

pub(crate) fn materialize_trial_result(trial_dir: &Path, output_path: &Path) -> Result<PathBuf> {
    let canonical_output = trial_dir.join("result.json");
    if output_path != canonical_output {
        if canonical_output.exists() {
            let _ = fs::remove_file(&canonical_output);
        }
        if output_path.exists() {
            if let Some(parent) = canonical_output.parent() {
                ensure_dir(parent)?;
            }
            fs::copy(output_path, &canonical_output)?;
        }
    }
    Ok(canonical_output)
}

pub(crate) fn materialize_trial_runtime_layout(
    trial_dir: &Path,
    paths: &TrialPaths,
    mode: MaterializationMode,
) -> Result<()> {
    match mode {
        MaterializationMode::Full => {
            copy_dir_preserve_contents(&paths.in_dir, &trial_dir.join("in"))?;
            copy_dir_preserve_contents(&paths.out, &trial_dir.join("out"))?;
            copy_dir_preserve_contents(&paths.state, &trial_dir.join("state"))?;
            copy_dir_preserve_contents(&paths.workspace, &trial_dir.join("workspace"))?;
            copy_dir_preserve_contents(&paths.tmp, &trial_dir.join("tmp"))?;
            copy_file_if_exists(
                &paths.runtime.trial_input,
                &trial_dir.join("trial_input.json"),
            )?;
            copy_file_if_exists(
                &paths.out.join("harness_manifest.json"),
                &trial_dir.join("harness_manifest.json"),
            )?;
            let _ = materialize_trial_result(trial_dir, &paths.runtime.result)?;
        }
        MaterializationMode::OutputsOnly => {
            copy_dir_preserve_contents(&paths.out, &trial_dir.join("out"))?;
            copy_file_if_exists(
                &paths.out.join("harness_manifest.json"),
                &trial_dir.join("harness_manifest.json"),
            )?;
            let _ = materialize_trial_result(trial_dir, &paths.runtime.result)?;
        }
        MaterializationMode::MetadataOnly | MaterializationMode::None => {}
    }
    apply_materialization_policy(trial_dir, mode)
}

fn apply_materialization_policy(trial_dir: &Path, mode: MaterializationMode) -> Result<()> {
    match mode {
        MaterializationMode::Full => return Ok(()),
        MaterializationMode::OutputsOnly => {
            for dir_name in ["workspace", "state", "tmp", "artifacts"] {
                remove_path_if_exists(&trial_dir.join(dir_name))?;
            }
        }
        MaterializationMode::MetadataOnly | MaterializationMode::None => {
            for dir_name in ["workspace", "state", "tmp", "artifacts", "out"] {
                remove_path_if_exists(&trial_dir.join(dir_name))?;
            }
            remove_path_if_exists(&trial_dir.join("trial_input.json"))?;
            remove_path_if_exists(&trial_dir.join("result.json"))?;
            remove_path_if_exists(&trial_dir.join("harness_manifest.json"))?;
            remove_path_if_exists(&trial_dir.join("trace_manifest.json"))?;
            if matches!(mode, MaterializationMode::None) {
                remove_path_if_exists(&trial_dir.join("state_inventory.json"))?;
            }
        }
    }
    Ok(())
}

pub(crate) fn resolve_agent_runtime_manifest_path(paths: &TrialPaths) -> Result<PathBuf> {
    map_contract_path_to_host(
        &format!("{}/harness_manifest.json", AGENTLAB_CONTRACT_OUT_DIR),
        &ContractPathHostRoots::from_trial_paths(paths),
        ContractPathMode::ContainerMount,
    )
}

pub(crate) fn write_state_inventory(
    trial_dir: &Path,
    json_value: &Value,
    agent_runtime: &AgentRuntimeConfig,
    _paths: &TrialPaths,
    exec_digest: &str,
    effective_network_mode: &str,
    invocation_source: &str,
    task_sandbox_image: Option<&str>,
    task_sandbox_workdir: Option<&str>,
) -> Result<()> {
    let sanitization_profile = json_value
        .pointer("/design/sanitization_profile")
        .and_then(|v| v.as_str())
        .unwrap_or("hermetic_functional");
    let integration_level = agent_runtime.integration_level.as_str();
    let mode_requested = json_value
        .pointer("/policy/task_sandbox/network")
        .and_then(|v| v.as_str())
        .unwrap_or("none");
    let mode_effective = effective_network_mode;
    let enforcement_effective = if mode_requested == "none" {
        "docker_none"
    } else {
        "unknown"
    };
    let workspace_path = task_sandbox_workdir.unwrap_or(DEFAULT_TASK_WORKDIR_FALLBACK);

    let mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workdir", "path": workspace_path, "writable": true}),
        json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
        json!({"name": "tmp", "path": "/tmp", "writable": true}),
    ];
    let mut agent_runtime_mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workdir", "path": workspace_path, "writable": true}),
        json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
        json!({"name": "tmp", "path": "/tmp", "writable": true}),
    ];
    agent_runtime_mounts.push(json!({
        "name": "agent_bundle",
        "path": "/opt/agent",
        "writable": false
    }));
    let mut task_sandbox_mounts = vec![
        json!({"name": "in", "path": AGENTLAB_CONTRACT_IN_DIR, "writable": false}),
        json!({"name": "workdir", "path": workspace_path, "writable": true}),
        json!({"name": "out", "path": AGENTLAB_CONTRACT_OUT_DIR, "writable": true}),
        json!({"name": "tmp", "path": "/tmp", "writable": true}),
    ];
    let sandbox_profile = json_value
        .pointer("/policy/task_sandbox/profile")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    if sandbox_profile == "swebench_testbed" {
        task_sandbox_mounts.push(json!({
            "name": "testbed",
            "path": "/testbed",
            "writable": true
        }));
    }
    let agent_runtime_image = Some(agent_runtime.image.as_str());
    let agent_runtime_image_digest = agent_runtime_image.and_then(resolve_container_image_digest);
    let task_sandbox_image_digest = task_sandbox_image.and_then(resolve_container_image_digest);

    let state = json!({
        "schema_version": "state_inventory_v1",
        "sanitization_profile": sanitization_profile,
        "integration_level": integration_level,
        "mounts": mounts,
        "network": {
            "mode_requested": mode_requested,
            "mode_effective": mode_effective,
            "allowed_hosts": json_value
                .pointer("/policy/task_sandbox/allowed_hosts")
                .cloned()
                .unwrap_or(json!([])),
            "enforcement_effective": enforcement_effective,
            "egress_self_test": {
                "performed": false,
                "cases": []
            }
        },
        "harness_identity": {
            "name": agent_runtime.command_raw.first().cloned().unwrap_or("unknown".to_string()),
            "exec_digest": exec_digest,
            "entry_command": agent_runtime.command_raw.clone()
        },
        "planes": {
            "agent_runtime": {
                "executor": "docker",
                "image": agent_runtime_image,
                "image_digest": agent_runtime_image_digest,
                "workdir": workspace_path,
                "mounts": agent_runtime_mounts,
                "network_mode": agent_runtime.network
            },
            "task_sandbox": {
                "executor": "docker",
                "image": task_sandbox_image,
                "image_digest": task_sandbox_image_digest,
                "workdir": workspace_path,
                "mounts": task_sandbox_mounts,
                "network_mode": mode_effective
            }
        },
        "ext": {
            "agent_runtime_identity": {
                "invocation_source": invocation_source
            }
        },
        "violations": {
            "state_leak": false,
            "profile_invariant_violation": false,
            "notes": []
        }
    });
    atomic_write_json_pretty(&trial_dir.join("state_inventory.json"), &state)?;
    Ok(())
}
