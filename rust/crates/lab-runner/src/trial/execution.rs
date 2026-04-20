use anyhow::{anyhow, Result};
use chrono::Utc;
use lab_core::{ensure_dir, sha256_file, AGENTLAB_CONTRACT_IN_DIR, AGENTLAB_CONTRACT_OUT_DIR};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::backend::docker::{
    ContainerHandle, ContainerMount, ContainerSpec, DockerRuntime, ExecSpec,
};
use crate::experiment::runner::{
    map_contract_path_to_host, ContractPathHostRoots, ContractPathMode,
};
use crate::experiment::runtime::AgentRuntimeConfig;
use crate::model::{
    BenchmarkGraderConfig, GradingStrategy, PreparedTrialIo, ResolvedMountReference,
    AGENTLAB_ENV_AGENT_EXIT_STATUS, MAPPED_GRADER_OUTPUT_FILENAME, RAW_GRADER_OUTPUT_FILENAME,
};
use crate::trial::artifacts::{
    artifact_type_from_trial_input_path, extract_candidate_artifact_record,
    load_trial_output_resilient,
};
use crate::trial::env::{
    benchmark_grader_expected_output_filename, benchmark_grader_uses_mapper, build_exec_env,
    resolve_benchmark_conclusion_mapper_command, resolve_benchmark_grader_command,
    resolve_grading_phase, resolve_runtime_agent_command, ResolvedGradingPhase,
};
use crate::trial::grade::{
    build_grading_sandbox_plan, build_hidden_asset_bindings, materialize_injected_grader_bundle,
    reveal_hidden_assets, stash_hidden_assets, validate_benchmark_grading_contract,
    write_grader_input_file,
};
use crate::trial::prepare::TrialPaths;
use crate::trial::spec::TaskMaterializationKind;
use crate::trial::state::{
    new_trial_attempt_state, reconcile_trial_attempt_as_abandoned, set_trial_attempt_phase,
    write_trial_attempt_state, AgentPhaseRecord, ContractFileState, GraderMappingPhaseRecord,
    GradingPhaseRecord, GradingSandboxState, TaskSandboxPlan, TaskSandboxState, TrialAttemptState,
    TrialPhase,
};
use crate::util::output_error_detail;
use lab_schemas::compile_schema;

#[derive(Clone)]
pub(crate) struct AdapterRunRequest<'a> {
    pub(crate) runtime_experiment: &'a Value,
    pub(crate) runtime: &'a AgentRuntimeConfig,
    pub(crate) variant_args: &'a [String],
    pub(crate) runtime_env: &'a BTreeMap<String, String>,
    pub(crate) runtime_overrides_env: &'a BTreeMap<String, String>,
    pub(crate) trial_paths: &'a TrialPaths,
    pub(crate) dynamic_mounts: &'a [ResolvedMountReference],
    pub(crate) io_paths: &'a PreparedTrialIo,
    pub(crate) network_mode: &'a str,
    pub(crate) benchmark_grader: Option<&'a BenchmarkGraderConfig>,
    pub(crate) benchmark_grading_enabled: bool,
    pub(crate) run_id: &'a str,
    pub(crate) task_image: &'a str,
    pub(crate) task_workdir: &'a str,
    pub(crate) task_materialization_kind: TaskMaterializationKind,
    pub(crate) agent_artifact: Option<&'a Path>,
}

pub(crate) struct TrialRuntimeOutcome {
    pub(crate) agent_exit_status: String,
    pub(crate) trial_output: Value,
    pub(crate) result_parse_error: Option<String>,
    pub(crate) trial_conclusion_row: Option<Value>,
    pub(crate) deferred_trial_conclusion_records: Vec<Value>,
    pub(crate) grade_error_reason: Option<String>,
}

struct AgentStageOutcome {
    agent_exit_status: String,
    trial_output: Value,
    result_parse_error: Option<String>,
}

struct GradingStageOutcome {
    trial_conclusion_row: Option<Value>,
    deferred_trial_conclusion_records: Vec<Value>,
    grade_error_reason: Option<String>,
}

const INJECTED_BUNDLE_SOURCE_MOUNT_PATH: &str = "/agentlab/_materialize/injected_bundle_src";

fn finalize_trial_runtime(
    trial_dir: &Path,
    attempt_state: &mut TrialAttemptState,
    agent_outcome: AgentStageOutcome,
    grading_outcome: GradingStageOutcome,
) -> Result<TrialRuntimeOutcome> {
    set_trial_attempt_phase(trial_dir, attempt_state, TrialPhase::CommitPending)?;
    Ok(TrialRuntimeOutcome {
        agent_exit_status: agent_outcome.agent_exit_status,
        trial_output: agent_outcome.trial_output,
        result_parse_error: agent_outcome.result_parse_error,
        trial_conclusion_row: grading_outcome.trial_conclusion_row,
        deferred_trial_conclusion_records: grading_outcome.deferred_trial_conclusion_records,
        grade_error_reason: grading_outcome.grade_error_reason,
    })
}

pub(crate) fn execute_trial_runtime(
    trial_dir: &Path,
    schedule_idx: usize,
    attempt_no: u32,
    request: &AdapterRunRequest<'_>,
    task_id: &str,
    variant_id: &str,
    repl_idx: usize,
    task_sandbox_plan: &TaskSandboxPlan,
) -> Result<TrialRuntimeOutcome> {
    validate_benchmark_grading_contract(request)?;
    let docker = DockerRuntime::connect()?;
    docker.ensure_image(&task_sandbox_plan.image)?;
    let hidden_asset_bindings = request
        .benchmark_grader
        .map(build_hidden_asset_bindings)
        .transpose()?
        .unwrap_or_default();
    let injected_grading_phase = if request.benchmark_grading_enabled {
        if let Some(grader) = request.benchmark_grader {
            if matches!(grader.strategy, GradingStrategy::Injected) {
                resolve_benchmark_grader_command(request)?
                    .as_ref()
                    .map(|command| resolve_grading_phase(request, grader, command))
                    .transpose()?
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    let mut attempt_state = new_trial_attempt_state(
        trial_dir,
        schedule_idx,
        attempt_no,
        task_id,
        variant_id,
        repl_idx,
        &request.trial_paths.in_dir,
        &request.trial_paths.out,
    );
    write_trial_attempt_state(trial_dir, &attempt_state)?;

    let mut task_container: Option<ContainerHandle> = None;
    let mut grading_container: Option<ContainerHandle> = None;

    let execution = (|| -> Result<TrialRuntimeOutcome> {
        set_trial_attempt_phase(
            trial_dir,
            &mut attempt_state,
            TrialPhase::AgentMaterializing,
        )?;

        let task_handle = materialize_task_sandbox(
            &docker,
            request,
            task_sandbox_plan,
            injected_grading_phase.as_ref(),
        )?;
        if !hidden_asset_bindings.is_empty() {
            stash_hidden_assets(
                &docker,
                &task_handle,
                trial_dir,
                &hidden_asset_bindings,
                task_sandbox_plan.time_limit_ms,
            )?;
        }
        //Lots of clones here. Why? ###Codex 
        let task_sandbox = TaskSandboxState {
            container_id: task_handle.container_id.clone(),
            image: task_sandbox_plan.image.clone(),
            workdir: task_sandbox_plan.workdir.clone(),
            materialization: task_sandbox_plan.materialization.clone(),
        };
        attempt_state.task_sandbox = Some(task_sandbox.clone());
        write_trial_attempt_state(trial_dir, &attempt_state)?;
        task_container = Some(task_handle.clone());

        set_trial_attempt_phase(trial_dir, &mut attempt_state, TrialPhase::AgentRunning)?;

        let agent_started_at = Utc::now().to_rfc3339();
        //Is there overlap here with ExecSpec? seems like workingDir is used twice, is agent path also a component of command? ###Codex
        let agent_exec = docker.exec(
            &task_handle,
            &ExecSpec {
                command: resolve_runtime_agent_command(request)?,
                env: build_exec_env(request, request.task_workdir, None, true),
                workdir: Some(request.task_workdir.to_string()),
            },
        )?;
        let agent_stream = docker.stream_exec_output(
            &agent_exec,
            &trial_dir.join("harness_stdout.log"),
            &trial_dir.join("harness_stderr.log"),
            Some(Duration::from_millis(task_sandbox_plan.time_limit_ms)),
        )?;
        let agent_status =
            docker
                .wait_exec(&agent_exec)
                .unwrap_or(crate::backend::docker::ExecStatus {
                    exit_code: None,
                    running: false,
                });
        let agent_ended_at = Utc::now().to_rfc3339();

        let (trial_output, result_parse_error) =
            load_trial_output_resilient(&request.io_paths.result_host)?;
        let result_state = classify_contract_file_state(
            &request.io_paths.result_host,
            result_parse_error.as_deref(),
        );

        let candidate_artifact = extract_candidate_artifact_record(
            &trial_output,
            artifact_type_from_trial_input_path(&request.io_paths.trial_input_host)?,
        );
        let agent_phase = AgentPhaseRecord {
            started_at: agent_started_at.clone(),
            ended_at: agent_ended_at.clone(),
            exit_code: agent_status.exit_code,
            signal: if agent_stream.timed_out {
                Some("KILL".to_string())
            } else {
                None
            },
            timed_out: agent_stream.timed_out,
            result_state,
            stdout_path: trial_dir
                .join("harness_stdout.log")
                .to_string_lossy()
                .to_string(),
            stderr_path: trial_dir
                .join("harness_stderr.log")
                .to_string_lossy()
                .to_string(),
        };
        attempt_state.agent_phase = Some(agent_phase);
        attempt_state.candidate_artifact = Some(candidate_artifact);
        set_trial_attempt_phase(trial_dir, &mut attempt_state, TrialPhase::AgentFinished)?;

        let agent_exit_status = if agent_stream.timed_out {
            "timeout".to_string()
        } else {
            agent_status
                .exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string())
        };

        let mut trial_conclusion_row = None;
        let mut deferred_trial_conclusion_records = Vec::new();
        let mut grade_error_reason = None;
        let agent_outcome = AgentStageOutcome {
            agent_exit_status: agent_exit_status.clone(),
            trial_output: trial_output.clone(),
            result_parse_error: result_parse_error.clone(),
        };

        if request.benchmark_grading_enabled {
            write_grader_input_file(
                request.io_paths,
                &serde_json::from_slice(&fs::read(&request.io_paths.trial_input_host)?)?,
                &trial_output,
                request.trial_paths,
                request.task_workdir,
                &agent_exit_status,
                result_parse_error.as_deref(),
                &agent_started_at,
                &agent_ended_at,
                None,
                None,
            )?;

            let Some(grader_command) = resolve_benchmark_grader_command(request)? else {
                return finalize_trial_runtime(
                    trial_dir,
                    &mut attempt_state,
                    agent_outcome,
                    GradingStageOutcome {
                        trial_conclusion_row,
                        deferred_trial_conclusion_records,
                        grade_error_reason: Some(
                            "mapped_grader_output_missing: benchmark grader command not resolved"
                                .to_string(),
                        ),
                    },
                );
            };
            let grader = request
                .benchmark_grader
                .ok_or_else(|| anyhow!("benchmark grading enabled without grader config"))?;
            let grading_phase_resolved = resolve_grading_phase(request, grader, &grader_command)?;
            let grading_plan = build_grading_sandbox_plan(grader, &grading_phase_resolved)?;

            set_trial_attempt_phase(
                trial_dir,
                &mut attempt_state,
                TrialPhase::GraderMaterializing,
            )?;
            let grading_handle = match grader.strategy {
                GradingStrategy::InTaskImage => {
                    if !hidden_asset_bindings.is_empty() {
                        reveal_hidden_assets(
                            &docker,
                            &task_handle,
                            trial_dir,
                            &hidden_asset_bindings,
                            task_sandbox_plan.time_limit_ms,
                        )?;
                    }
                    task_handle.clone()
                }
                GradingStrategy::Injected => {
                    materialize_injected_grader_bundle(
                        &docker,
                        &task_handle,
                        trial_dir,
                        &grading_phase_resolved,
                        task_sandbox_plan.time_limit_ms,
                    )?;
                    task_handle.clone()
                }
                GradingStrategy::Separate => {
                    let handle =
                        materialize_grading_sandbox(&docker, request, &grading_phase_resolved)?;
                    grading_container = Some(handle.clone());
                    handle
                }
            };
            let grading_sandbox = GradingSandboxState {
                container_id: grading_handle.container_id.clone(),
                strategy: grader.strategy.clone(),
                workdir: grading_phase_resolved.workdir.clone(),
            };
            attempt_state.grading_sandbox = Some(grading_sandbox.clone());
            write_trial_attempt_state(trial_dir, &attempt_state)?;

            set_trial_attempt_phase(trial_dir, &mut attempt_state, TrialPhase::GraderRunning)?;
            let grader_started_at = Utc::now().to_rfc3339();
            let grader_exec = docker.exec(
                &grading_handle,
                &ExecSpec {
                    command: grading_phase_resolved.command.clone(),
                    env: build_exec_env(
                        request,
                        &grading_phase_resolved.workdir,
                        Some((AGENTLAB_ENV_AGENT_EXIT_STATUS, agent_exit_status.as_str())),
                        false,
                    ),
                    workdir: Some(grading_phase_resolved.workdir.clone()),
                },
            )?;
            let grader_stream = docker.stream_exec_output(
                &grader_exec,
                &trial_dir.join("grader_stdout.log"),
                &trial_dir.join("grader_stderr.log"),
                Some(Duration::from_millis(task_sandbox_plan.time_limit_ms)),
            )?;
            let grader_status =
                docker
                    .wait_exec(&grader_exec)
                    .unwrap_or(crate::backend::docker::ExecStatus {
                        exit_code: None,
                        running: false,
                    });
            let grader_ended_at = Utc::now().to_rfc3339();

            let expected_output_path =
                request
                    .trial_paths
                    .out
                    .join(benchmark_grader_expected_output_filename(
                        request.benchmark_grader,
                    ));
            let raw_output_state = classify_contract_file_state(&expected_output_path, None);
            attempt_state.grading_phase = Some(GradingPhaseRecord {
                started_at: grader_started_at,
                ended_at: grader_ended_at,
                exit_code: grader_status.exit_code,
                signal: if grader_stream.timed_out {
                    Some("KILL".to_string())
                } else {
                    None
                },
                timed_out: grader_stream.timed_out,
                raw_output_state,
                stdout_path: trial_dir
                    .join("grader_stdout.log")
                    .to_string_lossy()
                    .to_string(),
                stderr_path: trial_dir
                    .join("grader_stderr.log")
                    .to_string_lossy()
                    .to_string(),
            });
            write_trial_attempt_state(trial_dir, &attempt_state)?;

            if benchmark_grader_uses_mapper(request.benchmark_grader) {
                set_trial_attempt_phase(trial_dir, &mut attempt_state, TrialPhase::GraderMapping)?;
                let Some(mapper_command) =
                    resolve_benchmark_conclusion_mapper_command(request, grader)?
                else {
                    return Err(anyhow!("mapper mode grader missing mapper command"));
                };
                let mapper_started_at = Utc::now().to_rfc3339();
                let mapper_exec = docker.exec(
                    &grading_handle,
                    &ExecSpec {
                        command: mapper_command,
                        env: build_exec_env(request, &grading_phase_resolved.workdir, None, false),
                        workdir: Some(grading_phase_resolved.workdir.clone()),
                    },
                )?;
                let _mapper_stream = docker.stream_exec_output(
                    &mapper_exec,
                    &trial_dir.join("mapper_stdout.log"),
                    &trial_dir.join("mapper_stderr.log"),
                    Some(Duration::from_millis(task_sandbox_plan.time_limit_ms)),
                )?;
                let _ = docker.wait_exec(&mapper_exec);
                let mapper_ended_at = Utc::now().to_rfc3339();
                let mapped_output_path =
                    request.trial_paths.out.join(MAPPED_GRADER_OUTPUT_FILENAME);
                let mapped_validation_error =
                    validate_json_schema("trial_conclusion_v1.jsonschema", &mapped_output_path)
                        .err()
                        .map(|err| err.to_string());
                let mapped_output_state = classify_contract_file_state(
                    &mapped_output_path,
                    mapped_validation_error.as_deref(),
                );
                attempt_state.mapping_phase = Some(GraderMappingPhaseRecord {
                    started_at: mapper_started_at,
                    ended_at: mapper_ended_at,
                    mapped_output_state,
                    stdout_path: trial_dir
                        .join("mapper_stdout.log")
                        .to_string_lossy()
                        .to_string(),
                    stderr_path: trial_dir
                        .join("mapper_stderr.log")
                        .to_string_lossy()
                        .to_string(),
                });
                write_trial_attempt_state(trial_dir, &attempt_state)?;
                match validate_json_schema("trial_conclusion_v1.jsonschema", &mapped_output_path) {
                    Ok(row) => {
                        deferred_trial_conclusion_records.push(row.clone());
                        trial_conclusion_row = Some(row);
                    }
                    Err(err) => {
                        grade_error_reason = Some(format!("mapped_grader_output_invalid: {}", err));
                    }
                }
            } else {
                let mapped_output_path =
                    request.trial_paths.out.join(MAPPED_GRADER_OUTPUT_FILENAME);
                match validate_json_schema("trial_conclusion_v1.jsonschema", &mapped_output_path) {
                    Ok(row) => {
                        deferred_trial_conclusion_records.push(row.clone());
                        trial_conclusion_row = Some(row);
                    }
                    Err(err) => {
                        grade_error_reason = Some(format!("mapped_grader_output_invalid: {}", err));
                    }
                }
            }

            if trial_conclusion_row.is_none() && grade_error_reason.is_none() {
                let expected = if benchmark_grader_uses_mapper(request.benchmark_grader) {
                    RAW_GRADER_OUTPUT_FILENAME
                } else {
                    MAPPED_GRADER_OUTPUT_FILENAME
                };
                grade_error_reason = Some(format!("mapped_grader_output_missing: {}", expected));
            }

            let _ = grading_plan;
        }

        finalize_trial_runtime(
            trial_dir,
            &mut attempt_state,
            agent_outcome,
            GradingStageOutcome {
                trial_conclusion_row,
                deferred_trial_conclusion_records,
                grade_error_reason,
            },
        )
    })();

    let cleanup_grading = grading_container
        .as_ref()
        .map(|handle| docker.remove_container(handle, true));
    let cleanup_task = task_container
        .as_ref()
        .map(|handle| docker.remove_container(handle, true));
    if let Some(result) = cleanup_grading {
        let _ = result;
    }
    if let Some(result) = cleanup_task {
        let _ = result;
    }

    if execution.is_err() {
        let _ = reconcile_trial_attempt_as_abandoned(trial_dir);
    }
    execution
}

fn materialize_task_sandbox(
    docker: &DockerRuntime,
    request: &AdapterRunRequest<'_>,
    plan: &TaskSandboxPlan,
    injected_phase: Option<&ResolvedGradingPhase>,
) -> Result<ContainerHandle> {
    let mut extra_mounts = Vec::new();
    if let Some(bundle_host_path) =
        injected_phase.and_then(|phase| phase.injected_bundle_host_path.as_ref())
    {
        extra_mounts.push(ResolvedMountReference {
            host_path: bundle_host_path.clone(),
            mount_path: INJECTED_BUNDLE_SOURCE_MOUNT_PATH.to_string(),
        });
    }
    let mut spec = build_container_spec(
        request,
        &plan.image,
        &plan.workdir,
        plan.network_mode.as_str(),
        true,
        &extra_mounts,
    );
    spec.platform = resolve_container_platform(&plan.image).map(|value| value.to_string());
    let handle = docker.create_container(&spec)?;
    docker.start_container(&handle)?;
    Ok(handle)
}

fn materialize_grading_sandbox(
    docker: &DockerRuntime,
    request: &AdapterRunRequest<'_>,
    resolved: &ResolvedGradingPhase,
) -> Result<ContainerHandle> {
    let mut spec = build_container_spec(
        request,
        &resolved.image,
        &resolved.workdir,
        request.network_mode,
        false,
        &resolved.extra_mounts,
    );
    spec.platform = resolve_container_platform(&resolved.image).map(|value| value.to_string());
    let handle = docker.create_container(&spec)?;
    docker.start_container(&handle)?;
    Ok(handle)
}

pub(crate) fn build_container_spec(
    request: &AdapterRunRequest<'_>,
    image: &str,
    workdir: &str,
    network_mode: &str,
    include_agent_artifact: bool,
    extra_mounts: &[ResolvedMountReference],
) -> ContainerSpec {
    let mut mounts = vec![
        ContainerMount {
            host_path: request.trial_paths.in_dir.clone(),
            container_path: AGENTLAB_CONTRACT_IN_DIR.to_string(),
            read_only: true,
        },
        ContainerMount {
            host_path: request.trial_paths.out.clone(),
            container_path: AGENTLAB_CONTRACT_OUT_DIR.to_string(),
            read_only: false,
        },
    ];
    mounts.extend(request.dynamic_mounts.iter().map(|mount| ContainerMount {
        host_path: mount.host_path.clone(),
        container_path: mount.mount_path.clone(),
        read_only: true,
    }));
    mounts.extend(extra_mounts.iter().map(|mount| ContainerMount {
        host_path: mount.host_path.clone(),
        container_path: mount.mount_path.clone(),
        read_only: true,
    }));
    if include_agent_artifact {
        if let Some(bundle) = request.agent_artifact {
            if let Ok(bundle_root) = resolve_agent_artifact_mount_dir(bundle) {
                mounts.push(ContainerMount {
                    host_path: bundle_root,
                    container_path: "/opt/agent".to_string(),
                    read_only: true,
                });
            }
        }
    }
    let mut tmpfs = BTreeMap::new();
    tmpfs.insert("/tmp".to_string(), "rw".to_string());
    if include_agent_artifact {
        tmpfs.insert("/opt/bench".to_string(), "rw".to_string());
    }

    let cpu_count = request
        .runtime_experiment
        .pointer("/policy/task_sandbox/resources/cpu_count")
        .and_then(Value::as_u64);
    let memory_mb = request
        .runtime_experiment
        .pointer("/policy/task_sandbox/resources/memory_mb")
        .and_then(Value::as_u64);

    let mut spec = ContainerSpec::idle(image.to_string());
    spec.workdir = Some(workdir.to_string());
    spec.mounts = mounts;
    spec.tmpfs = tmpfs;
    spec.network_mode = if network_mode == "none" {
        Some("none".to_string())
    } else {
        None
    };
    spec.security_opt = if request
        .runtime_experiment
        .pointer("/policy/task_sandbox/hardening/no_new_privileges")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        vec!["no-new-privileges".to_string()]
    } else {
        Vec::new()
    };
    spec.cap_drop = if request
        .runtime_experiment
        .pointer("/policy/task_sandbox/hardening/drop_all_caps")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        vec!["ALL".to_string()]
    } else {
        Vec::new()
    };
    spec.cpu_count = cpu_count;
    spec.memory_mb = memory_mb;
    spec
}

fn classify_contract_file_state(path: &Path, validation_error: Option<&str>) -> ContractFileState {
    if !path.exists() || path.metadata().map(|meta| meta.len()).unwrap_or(0) == 0 {
        ContractFileState::Missing
    } else if validation_error.is_some() {
        ContractFileState::PresentInvalid
    } else {
        ContractFileState::Valid
    }
}

fn validate_json_schema(schema_name: &str, path: &Path) -> Result<Value> {
    if !path.exists() {
        return Err(anyhow!("{} missing: {}", schema_name, path.display()));
    }
    let raw = fs::read_to_string(path)?;
    let value: Value = serde_json::from_str(&raw)?;
    let schema = compile_schema(schema_name)?;
    if let Err(errors) = schema.validate(&value) {
        let msgs = errors
            .map(|err| err.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(anyhow!("schema validation failed: {}", msgs));
    }
    Ok(value)
}

// ---------------------------------------------------------------------------
// Container/artifact helpers (moved from runtime.rs)
// ---------------------------------------------------------------------------

pub(crate) fn validate_container_workspace_path(path: &str) -> Result<()> {
    let p = Path::new(path);
    if !p.is_absolute() {
        return Err(anyhow!("mount_path must be absolute"));
    }
    for component in p.components() {
        if matches!(component, Component::ParentDir) {
            return Err(anyhow!("mount_path cannot contain '..'"));
        }
    }
    Ok(())
}

pub(crate) fn resolve_task_sandbox_image(request: &AdapterRunRequest<'_>) -> Result<String> {
    let image = request.task_image.trim();
    if image.is_empty() {
        return Err(anyhow!("task image is required for task sandbox"));
    }
    Ok(image.to_string())
}

pub(crate) fn resolve_container_workspace<'a>(
    request: &'a AdapterRunRequest<'_>,
) -> Result<&'a str> {
    let workdir = request.task_workdir.trim();
    if workdir.is_empty() {
        return Err(anyhow!("task workdir is required for task sandbox"));
    }
    Ok(workdir)
}

pub(crate) fn resolve_container_platform(image: &str) -> Option<&'static str> {
    let normalized = image.strip_prefix("swebench/").unwrap_or(image);
    if normalized.starts_with("sweb.eval.x86_64.") {
        return Some("linux/amd64");
    }
    if normalized.starts_with("sweb.eval.aarch64.") || normalized.starts_with("sweb.eval.arm64.") {
        return Some("linux/arm64");
    }
    None
}

pub(crate) fn resolve_container_image_digest(image: &str) -> Option<String> {
    let runtime = DockerRuntime::connect().ok()?;
    let metadata = runtime.ensure_image(image).ok()?;
    metadata
        .repo_digests
        .first()
        .and_then(|value| value.rsplit_once('@').map(|(_, digest)| digest.to_string()))
        .or(metadata.image_id)
}

pub(crate) fn agent_artifact_cache_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) fn repair_agent_artifact_layout(unpacked_dir: &Path) -> Result<()> {
    let packages_root = unpacked_dir.join("packages");
    let nested_packages_root = packages_root.join("packages");
    if !packages_root.is_dir() || !nested_packages_root.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(&nested_packages_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let shim_path = packages_root.join(&name);
        if shim_path.exists() {
            continue;
        }
        let nested_rel = Path::new("packages").join(&name);
        let nested_abs = packages_root.join(&nested_rel);
        if !nested_abs.exists() {
            continue;
        }
        symlink(&nested_rel, &shim_path).map_err(|err| {
            anyhow!(
                "failed to create artifact layout shim {} -> {}: {}",
                shim_path.display(),
                nested_rel.display(),
                err
            )
        })?;
    }
    Ok(())
}

pub(crate) fn resolve_agent_artifact_mount_dir(artifact: &Path) -> Result<PathBuf> {
    if artifact.is_dir() {
        return Ok(fs::canonicalize(artifact).unwrap_or_else(|_| artifact.to_path_buf()));
    }
    if !artifact.exists() {
        return Err(anyhow!(
            "runtime.agent_runtime.artifact not found: {}",
            artifact.display()
        ));
    }
    if !artifact.is_file() {
        return Err(anyhow!(
            "runtime.agent_runtime.artifact must be a file or directory: {}",
            artifact.display()
        ));
    }
    let artifact_path = fs::canonicalize(artifact).unwrap_or_else(|_| artifact.to_path_buf());
    let artifact_name = artifact_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let tar_flag = if artifact_name.ends_with(".tar.gz") || artifact_name.ends_with(".tgz") {
        "-xzf"
    } else if artifact_name.ends_with(".tar") {
        "-xf"
    } else {
        return Err(anyhow!(
            "runtime.agent_runtime.artifact '{}' must be a directory or .tar/.tar.gz archive",
            artifact_path.display()
        ));
    };

    let digest = sha256_file(&artifact_path)?;
    let digest_path_component = digest.replace(':', "_");
    let cache_root = artifact_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".agentlab_artifact_cache");
    ensure_dir(&cache_root)?;
    let unpacked_dir = cache_root.join(&digest_path_component);
    let ready_marker = unpacked_dir.join(".agentlab_ready");
    if ready_marker.exists() {
        repair_agent_artifact_layout(&unpacked_dir)?;
        return Ok(unpacked_dir);
    }

    let _guard = agent_artifact_cache_lock()
        .lock()
        .map_err(|_| anyhow!("agent artifact cache lock poisoned"))?;
    if ready_marker.exists() {
        repair_agent_artifact_layout(&unpacked_dir)?;
        return Ok(unpacked_dir);
    }

    if unpacked_dir.exists() {
        fs::remove_dir_all(&unpacked_dir)?;
    }
    let staging_dir = cache_root.join(format!(
        "{}.tmp.{}.{}",
        digest_path_component,
        std::process::id(),
        Utc::now().timestamp_micros()
    ));
    if staging_dir.exists() {
        let _ = fs::remove_dir_all(&staging_dir);
    }
    ensure_dir(&staging_dir)?;
    let unpack_out = Command::new("tar")
        .args([
            tar_flag,
            artifact_path.to_string_lossy().as_ref(),
            "-C",
            staging_dir.to_string_lossy().as_ref(),
        ])
        .output()?;
    if !unpack_out.status.success() {
        let _ = fs::remove_dir_all(&staging_dir);
        return Err(anyhow!(
            "failed to unpack runtime.agent_runtime.artifact {}: {}",
            artifact_path.display(),
            output_error_detail(&unpack_out),
        ));
    }
    if let Err(err) = fs::rename(&staging_dir, &unpacked_dir) {
        let _ = fs::remove_dir_all(&staging_dir);
        return Err(anyhow!(
            "failed to finalize unpacked runtime.agent_runtime.artifact {} into {}: {}",
            artifact_path.display(),
            unpacked_dir.display(),
            err
        ));
    }
    repair_agent_artifact_layout(&unpacked_dir)?;
    fs::write(&ready_marker, digest.as_bytes())?;
    Ok(unpacked_dir)
}

pub(crate) fn map_container_path_to_host(path: &str, paths: &TrialPaths) -> Result<PathBuf> {
    map_contract_path_to_host(
        path,
        &ContractPathHostRoots::from_trial_paths(paths),
        ContractPathMode::ContainerMount,
    )
}
