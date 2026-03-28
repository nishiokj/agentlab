#[cfg(test)]
mod tests {
    use super::*;

    // Standard library
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::fs;
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    // External crates
    use anyhow::Result;
    use chrono::Utc;
    use lab_schemas::compile_schema;
    use serde::Deserialize;
    use serde_json::{json, Value};

    // lab_core
    use lab_core::{
        canonical_json_digest, ensure_dir, sha256_bytes, sha256_file, ArtifactStore,
        AGENTLAB_CONTRACT_IN_DIR, AGENTLAB_CONTRACT_OUT_DIR,
        AGENTLAB_ENV_GRADER_INPUT_PATH, AGENTLAB_ENV_REPL_IDX, AGENTLAB_ENV_RESULT_PATH,
        AGENTLAB_ENV_RUN_ID, AGENTLAB_ENV_TASK_ID, AGENTLAB_ENV_TIMEOUT_MS,
        AGENTLAB_ENV_TRIAL_ID, AGENTLAB_ENV_TRIAL_INPUT_PATH, AGENTLAB_ENV_VARIANT_ID,
        AGENTLAB_GRADER_INPUT_PATH, AGENTLAB_MAPPED_GRADER_OUTPUT_PATH,
        AGENTLAB_RAW_GRADER_OUTPUT_PATH, AGENTLAB_RESULT_PATH,
        AGENTLAB_RUNNER_SUPPORT_REL_DIR, AGENTLAB_TASK_WORKDIR_PLACEHOLDER,
        AGENTLAB_TRAJECTORY_PATH, AGENTLAB_TRIAL_INPUT_PATH,
    };

    // Crate modules
    use crate::config::*;
    use crate::experiment::commit::{
        make_slot_commit_id, load_jsonl_value_rows, DeterministicCommitter, RunCoordinator,
    };
    use crate::experiment::control::*;
    use crate::experiment::lease::{
        acquire_run_operation_lease, engine_lease_is_stale, operation_lease_is_stale,
        EngineLeaseRecord, OperationLeaseRecord, RunOperationType,
    };
    use crate::experiment::preflight::*;
    use crate::experiment::runner::*;
    use crate::experiment::runtime::*;
    use crate::experiment::state::*;
    use crate::model::*;
    use crate::package::authoring::*;
    use crate::package::compile::*;
    use crate::package::sealed::*;
    use crate::package::staging::*;
    use crate::package::validate::*;
    use crate::persistence::journal::*;
    use crate::persistence::rows::*;
    use crate::persistence::store::*;
    use crate::persistence::store::SqliteRunStore as BackingSqliteStore;
    use crate::trial::artifacts::load_trial_output_resilient;
    use crate::trial::env::{build_exec_env, resolve_runtime_agent_command};
    use crate::trial::execution::{
        map_container_path_to_host, resolve_agent_artifact_mount_dir, resolve_container_platform,
        validate_container_workspace_path,
    };
    use crate::trial::grade::benchmark_retry_inputs;
    use crate::trial::execution::AdapterRunRequest;
    use crate::trial::preflight::stage_benchmark_trial_preflight;
    use crate::trial::prepare::{
        build_runtime_contract_env, build_trial_input, normalize_task_prompt_aliases,
        prepare_task_environment, resolve_trial_io_host_path, resolve_trial_timeout_ms,
        PreparedTaskEnvironment, TrialPaths,
    };
    use crate::trial::spec::{
        parse_task_boundary_from_packaged_task, parse_task_row,
        TaskBoundaryMaterialization, TaskMaterializationKind, TaskMaterializationSpec,
    };
    use crate::trial::state::{
        write_trial_state, AttemptFsLayout, AttemptSlotRef, TaskSandboxState, TrialAttemptKey,
        TrialAttemptState, TrialPhase, TrialStateGuard,
    };
    use crate::util::*;

    type BenchmarkAdapterConfig = BenchmarkGraderConfig;
    const AGENTLAB_CONTRACT_STATE_DIR: &str = "/agentlab/state";
    const AGENTLAB_CONTRACT_WORKSPACE_DIR: &str = "/agentlab/workspace";

    struct TempDirGuard {
        path: PathBuf,
    }

    impl TempDirGuard {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "{}_{}_{}",
                prefix,
                std::process::id(),
                Utc::now().timestamp_micros()
            ));
            ensure_dir(&path).expect("temp dir");
            Self { path }
        }
    }

    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn create_run_dir(prefix: &str, run_id: &str) -> (TempDirGuard, PathBuf) {
        let root = TempDirGuard::new(prefix);
        let run_dir = root.path.join(".lab").join("runs").join(run_id);
        ensure_dir(&run_dir).expect("run dir");
        (root, run_dir)
    }

    fn prepared_trial_io_fixture(output_host: PathBuf, events_host: PathBuf) -> PreparedTrialIo {
        PreparedTrialIo {
            trial_input_host: PathBuf::from("/tmp/trial_input.json"),
            grader_input_host: PathBuf::from("/tmp/grader_input.json"),
            result_host: output_host.clone(),
            events_host,
            trial_input_path: AGENTLAB_TRIAL_INPUT_PATH.to_string(),
            grader_input_path: AGENTLAB_GRADER_INPUT_PATH.to_string(),
            result_path: AGENTLAB_RESULT_PATH.to_string(),
            raw_grader_output_path: AGENTLAB_RAW_GRADER_OUTPUT_PATH.to_string(),
            mapped_grader_output_path: AGENTLAB_MAPPED_GRADER_OUTPUT_PATH.to_string(),
            trajectory_path: AGENTLAB_TRAJECTORY_PATH.to_string(),
            input_host: PathBuf::from("/tmp/trial_input.json"),
            output_host,
        }
    }

    fn prepared_trial_io_fixture_with_contract_paths(
        trial_input_path: &str,
        grader_input_path: &str,
        result_path: &str,
        raw_grader_output_path: &str,
        mapped_grader_output_path: &str,
        trajectory_path: &str,
    ) -> PreparedTrialIo {
        PreparedTrialIo {
            trial_input_host: PathBuf::from("/tmp/trial_input.json"),
            grader_input_host: PathBuf::from("/tmp/grader_input.json"),
            result_host: PathBuf::from("/out"),
            events_host: PathBuf::from("/events"),
            trial_input_path: trial_input_path.to_string(),
            grader_input_path: grader_input_path.to_string(),
            result_path: result_path.to_string(),
            raw_grader_output_path: raw_grader_output_path.to_string(),
            mapped_grader_output_path: mapped_grader_output_path.to_string(),
            trajectory_path: trajectory_path.to_string(),
            input_host: PathBuf::from("/tmp/trial_input.json"),
            output_host: PathBuf::from("/out"),
        }
    }

    fn harness_success_command() -> Vec<String> {
        vec![
            "sh".to_string(),
            "-lc".to_string(),
            "printf '%s' '{\"schema_version\":\"agent_result_v1\",\"outcome\":\"success\",\"checkpoints\":[]}'".to_string(),
        ]
    }

    fn harness_success_output_command() -> Vec<String> {
        vec![
            "/bin/sh".to_string(),
            "/opt/agent/bin/write_success_result.sh".to_string(),
        ]
    }

    fn agent_execution_fixture(image: Option<&str>) -> AgentExecutionConfig {
        AgentExecutionConfig {
            executor: Some(AgentExecutionExecutor::Docker),
            image: image.map(|value| value.to_string()),
            network: "none".to_string(),
        }
    }

    fn legacy_contract_runtime_fixture() -> AgentRuntimeConfig {
        AgentRuntimeConfig {
            adapter_ref: AgentAdapterRef::default(),
            command_raw: vec!["sh".to_string(), "-lc".to_string(), "echo ok".to_string()],
            image: "img:latest".to_string(),
            network: "none".to_string(),
            sandbox_image: Some("img:latest".to_string()),
            image_source: ImageSource::Global,
            execution: agent_execution_fixture(Some("img:latest")),
            agent_artifact: PathBuf::from("/tmp/agent-artifact"),
            agent_artifact_digest: None,
            agent_artifact_resolved_path: None,
            io: AgentRuntimeIoConfig {
                input_arg: "--input".to_string(),
                output_arg: "--output".to_string(),
            },
            integration_level: "cli_basic".to_string(),
            launch_mode: AgentLaunchMode::File,
            env: BTreeMap::new(),
            env_from_host: vec![],
            workspace_patches: Vec::new(),
            trajectory_path: None,
            causal_extraction: None,
            default_timeout_ms: None,
            tracing_mode: None,
            force_container: true,
            dependency_file_staging: Vec::new(),
            dependency_services: Vec::new(),
        }
    }

    fn command_contains_flag_value(command: &[String], flag: &str, value: &str) -> bool {
        command
            .windows(2)
            .any(|pair| pair[0] == flag && pair[1] == value)
    }

    fn scratch_workspace() -> WorkspaceSpec {
        WorkspaceSpec {
            mode: WorkspaceMode::Scratch,
            base: WorkspaceBaseSpec {
                kind: WorkspaceBaseKind::Empty,
                dataset_pack_ref: None,
                repo: None,
                commit: None,
            },
            overlays: Vec::new(),
            aux_mounts: Vec::new(),
        }
    }

    fn runtime_task_boundary(
        task_payload: Value,
        task_image: &str,
        task_workdir: &str,
        time_limit_ms: Option<u64>,
    ) -> TaskBoundaryMaterialization {
        let task_id = task_payload
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| "task_0".to_string());
        let materialization = TaskMaterializationSpec {
            kind: TaskMaterializationKind::TaskImage,
            task_bundle_ref: None,
        };
        let declaration = json!({
            "schema_version": "task_row_v1",
            "id": task_id,
            "image": task_image,
            "workdir": task_workdir,
            "time_limit_ms": time_limit_ms,
            "task": task_payload.clone(),
            "materialization": { "kind": "task_image" }
        });
        TaskBoundaryMaterialization {
            declaration,
            task_payload,
            workspace: scratch_workspace(),
            dependencies: json!({}),
            materialization,
            task_id,
            task_image: task_image.to_string(),
            task_workdir: task_workdir.to_string(),
            time_limit_ms,
        }
    }

    fn runtime_task_boundary_from_row(task_row: Value) -> TaskBoundaryMaterialization {
        let parsed = parse_task_row(&task_row).expect("task row");
        let declaration = serde_json::to_value(&parsed).expect("task row value");
        TaskBoundaryMaterialization {
            declaration,
            task_payload: parsed.task.clone(),
            workspace: scratch_workspace(),
            dependencies: json!({}),
            materialization: parsed.materialization.clone(),
            task_id: parsed.task_id(0),
            task_image: parsed.image.clone(),
            task_workdir: parsed.workdir.clone(),
            time_limit_ms: parsed.time_limit_ms,
        }
    }

    fn task_row_value(
        task_id: &str,
        image: &str,
        workdir: &str,
        time_limit_ms: Option<u64>,
    ) -> Value {
        json!({
            "schema_version": "task_row_v1",
            "id": task_id,
            "image": image,
            "workdir": workdir,
            "time_limit_ms": time_limit_ms,
            "task": { "id": task_id },
            "materialization": { "kind": "task_image" }
        })
    }

    fn base_image_bundle_task_row(
        task_id: &str,
        image: &str,
        workdir: &str,
        task_bundle_ref: &str,
    ) -> Value {
        json!({
            "schema_version": "task_row_v1",
            "id": task_id,
            "image": image,
            "workdir": workdir,
            "task": { "id": task_id },
            "materialization": {
                "kind": "base_image_bundle",
                "task_bundle_ref": task_bundle_ref
            }
        })
    }

    fn runtime_sandbox(image_source: &str, image: Option<&str>) -> Value {
        let mut sandbox = json!({
            "executor": "docker",
            "image_source": image_source,
            "profile": "workspace_write",
            "network": "none",
        });
        if let Some(image) = image {
            sandbox
                .as_object_mut()
                .expect("sandbox object")
                .insert("image".to_string(), json!(image));
        }
        sandbox
    }

    fn ensure_test_agent_bundle(project_root: &Path, bundle_name: &str) -> PathBuf {
        let bundle_root = project_root.join(".lab").join("agents").join(bundle_name);
        let bin_dir = bundle_root.join("bin");
        ensure_dir(&bin_dir).expect("test bundle bin dir");
        for name in ["sh", "python", "python3", "node", "rex"] {
            fs::copy("/bin/sh", bin_dir.join(name)).expect("copy test bundle executable");
        }
        let write_success_script = bin_dir.join("write_success_result.sh");
        fs::write(
            &write_success_script,
            concat!(
                "#!/bin/sh\n",
                "printf '%s' '{\"schema_version\":\"agent_result_v1\",\"outcome\":\"success\",\"checkpoints\":[]}' > /agentlab/out/result.json\n"
            ),
        )
        .expect("write test bundle success script");
        #[cfg(unix)]
        {
            let mut perms = fs::metadata(&write_success_script)
                .expect("script metadata")
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&write_success_script, perms).expect("script permissions");
        }
        bundle_root
    }

    fn run_git(cwd: &Path, args: &[&str]) {
        let output = Command::new("git")
            .current_dir(cwd)
            .args(args)
            .output()
            .expect("run git command");
        assert!(
            output.status.success(),
            "git {:?} failed in {}:\nstdout:\n{}\nstderr:\n{}",
            args,
            cwd.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn git_stdout(cwd: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .current_dir(cwd)
            .args(args)
            .output()
            .expect("run git command");
        assert!(
            output.status.success(),
            "git {:?} failed in {}:\nstdout:\n{}\nstderr:\n{}",
            args,
            cwd.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("git stdout utf8")
            .trim()
            .to_string()
    }

    fn create_file_git_origin(root: &Path, prefix: &str) -> (String, String) {
        let source = root.join(format!("{}_src", prefix));
        ensure_dir(&source).expect("git source dir");
        run_git(&source, &["init"]);
        run_git(&source, &["config", "user.email", "tests@example.com"]);
        run_git(&source, &["config", "user.name", "Lab Runner Tests"]);
        ensure_dir(&source.join("src")).expect("git source nested dir");
        fs::write(source.join("README.md"), "hello from origin\n").expect("git readme");
        fs::write(source.join("src/lib.txt"), "seeded from origin\n").expect("git source file");
        run_git(&source, &["add", "."]);
        run_git(&source, &["commit", "-m", "initial"]);
        let commit = git_stdout(&source, &["rev-parse", "HEAD"]);

        let origin = root.join(format!("{}_origin.git", prefix));
        let output = Command::new("git")
            .args([
                "clone",
                "--bare",
                source.to_string_lossy().as_ref(),
                origin.to_string_lossy().as_ref(),
            ])
            .output()
            .expect("clone bare origin");
        assert!(
            output.status.success(),
            "git clone --bare failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let origin_url = format!(
            "file://{}",
            origin.canonicalize().expect("canonical origin").display()
        );
        (origin_url, commit)
    }

    fn container_execution() -> RunExecutionOptions {
        RunExecutionOptions {
            executor: Some(ExecutorKind::LocalDocker),
            ..RunExecutionOptions::default()
        }
    }

    fn docker_runtime_available() -> bool {
        crate::backend::docker::DockerRuntime::connect()
            .and_then(|runtime| runtime.ping())
            .is_ok()
    }

    fn ensure_docker_test_image(image: &str) {
        crate::backend::docker::DockerRuntime::connect()
            .expect("docker runtime")
            .ensure_image(image)
            .expect("container image");
    }

    fn build_docker_test_image(root: &Path, tag_suffix: &str, dockerfile: &str) -> String {
        ensure_docker_test_image("python:3.11-slim");
        let dockerfile_path = root.join("Dockerfile");
        fs::write(&dockerfile_path, dockerfile).expect("dockerfile");
        let tag = format!(
            "agentlab-test-{}-{}:{}",
            sanitize_for_fs(tag_suffix),
            std::process::id(),
            Utc::now().timestamp_micros()
        );
        let output = Command::new("docker")
            .args(["build", "-t", &tag, root.to_string_lossy().as_ref()])
            .output()
            .expect("docker build");
        assert!(
            output.status.success(),
            "docker build failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        tag
    }

    fn write_resolved_experiment(
        run_dir: &Path,
        integration_level: &str,
        include_events_path: bool,
    ) {
        let _ = include_events_path;
        let project_root = find_project_root(run_dir);
        let bundle_root = ensure_test_agent_bundle(&project_root, "rex-current");

        let resolved = json!({
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": { "path": "tasks.jsonl" },
            "design": { "sanitization_profile": "hermetic_functional", "replications": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent_runtime": {
                    "command": harness_success_command(),
                    "artifact": bundle_root.to_string_lossy().to_string(),
                    "image": "python:3.11-slim",
                    "integration_level": integration_level
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": {
                    "profile": "workspace_write",
                    "network": "none"
                }
            }
        });
        atomic_write_json_pretty(&run_dir.join("resolved_experiment.json"), &resolved)
            .expect("write resolved");
        let (variants, baseline_id) = resolve_variant_plan(&resolved).expect("variant plan");
        write_resolved_variants(run_dir, &resolved, &baseline_id, &variants)
            .expect("write resolved variants");
    }

    fn write_resolved_experiment_with_command(
        run_dir: &Path,
        integration_level: &str,
        command: Vec<String>,
    ) {
        let project_root = find_project_root(run_dir);
        let bundle_root = ensure_test_agent_bundle(&project_root, "rex-current");
        let resolved = json!({
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": { "path": "tasks.jsonl" },
            "design": { "sanitization_profile": "hermetic_functional", "replications": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent_runtime": {
                    "command": command,
                    "artifact": bundle_root.to_string_lossy().to_string(),
                    "image": "python:3.11-slim",
                    "integration_level": integration_level
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": {
                    "profile": "workspace_write",
                    "network": "none"
                }
            }
        });
        atomic_write_json_pretty(&run_dir.join("resolved_experiment.json"), &resolved)
            .expect("write resolved");
        let (variants, baseline_id) = resolve_variant_plan(&resolved).expect("variant plan");
        write_resolved_variants(run_dir, &resolved, &baseline_id, &variants)
            .expect("write resolved variants");
    }

    fn write_task_row_dataset(path: &Path, task_id: &str) {
        fs::write(
            path,
            format!(
                concat!(
                    "{{\"schema_version\":\"task_row_v1\",",
                    "\"id\":\"{}\",",
                    "\"image\":\"python:3.11-slim\",",
                    "\"workdir\":\"/workspace/task\",",
                    "\"task\":{{\"id\":\"{}\"}},",
                    "\"materialization\":{{\"kind\":\"task_image\"}}}}\n"
                ),
                task_id, task_id
            ),
        )
        .expect("task row dataset");
    }

    fn write_packaged_task_dataset(path: &Path, task_id: &str) {
        write_task_row_dataset(path, task_id);
    }

    fn seed_parent_trial(
        run_dir: &Path,
        trial_id: &str,
        checkpoints: Value,
        trial_status: &str,
        pause_label: Option<&str>,
    ) -> PathBuf {
        let trial_dir = run_dir.join("trials").join(trial_id);
        ensure_dir(&trial_dir).expect("trial dir");
        ensure_dir(&trial_dir.join("workspace")).expect("workspace");
        ensure_dir(&trial_dir.join("state")).expect("state");

        fs::write(
            trial_dir.join("workspace").join("fixture.txt"),
            "workspace fixture",
        )
        .expect("workspace fixture");
        let trial_input = json!({
            "schema_version": "agent_task_v1",
            "ids": { "trial_id": trial_id, "variant_id": "base", "task_id": "task_1", "repl_idx": 0 },
            "task": {
                "id": "task_1"
            },
            "ext": {
                "task_spec": {
                    "environment": {
                        "image": "python:3.11-slim"
                    },
                    "workspace": {
                        "mode": "scratch",
                        "base": { "kind": "empty" },
                        "overlays": [],
                        "aux_mounts": []
                    },
                    "dependencies": {},
                    "limits": {}
                }
            },
            "bindings": {
                "existing": "value"
            },
            "runtime": {
                "paths": {
                    "workspace": trial_dir.join("workspace").to_string_lossy().to_string(),
                    "state": trial_dir.join("state").to_string_lossy().to_string(),
                    "out": trial_dir.join("out").to_string_lossy().to_string(),
                    "tmp": trial_dir.join("tmp").to_string_lossy().to_string()
                },
                "network": { "mode_requested": "none" }
            }
        });
        atomic_write_json_pretty(&trial_dir.join("trial_input.json"), &trial_input)
            .expect("trial input");

        let trial_output = json!({
            "schema_version": "agent_result_v1",
            "outcome": "success",
            "checkpoints": checkpoints
        });
        atomic_write_json_pretty(&trial_dir.join("result.json"), &trial_output)
            .expect("trial output");

        write_trial_state(
            &trial_dir,
            trial_id,
            trial_status,
            pause_label,
            pause_label,
            if trial_status == "paused" {
                Some("paused_by_user")
            } else {
                None
            },
        )
        .expect("trial state");

        let run_id = run_dir
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("run")
            .to_string();
        let artifact_store = ArtifactStore::new(run_dir.join("artifacts"));
        let input_ref = artifact_store
            .put_bytes(&serde_json::to_vec_pretty(&trial_input).expect("trial input bytes"))
            .expect("input ref");
        let output_ref = artifact_store
            .put_bytes(&serde_json::to_vec_pretty(&trial_output).expect("trial output bytes"))
            .expect("output ref");
        let workspace_ref = artifact_store
            .put_bytes(b"workspace_placeholder")
            .expect("workspace ref");
        let checkpoint_labels = trial_output
            .get("checkpoints")
            .and_then(Value::as_array)
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| {
                        row.get("logical_name")
                            .and_then(Value::as_str)
                            .or_else(|| row.get("path").and_then(Value::as_str))
                    })
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut store = BackingSqliteStore::open(run_dir).expect("store");
        store
            .upsert_attempt_object(&run_id, trial_id, 0, 1, "trial_input", &input_ref, None)
            .expect("attempt input");
        store
            .upsert_attempt_object(&run_id, trial_id, 0, 1, "trial_output", &output_ref, None)
            .expect("attempt output");
        store
            .upsert_json_row(
                JsonRowTable::ChainState,
                &json!({
                    "schema_version": "task_chain_state_v1",
                    "run_id": run_id,
                    "chain_id": "base::task_1",
                    "step_index": 0,
                    "ids": {
                        "trial_id": trial_id,
                        "variant_id": "base",
                        "task_id": "task_1",
                        "repl_idx": 0
                    },
                    "snapshots": {
                        "chain_root_ref": workspace_ref,
                        "prev_ref": workspace_ref,
                        "post_ref": workspace_ref
                    },
                    "diffs": {
                        "incremental_ref": output_ref,
                        "cumulative_ref": output_ref,
                        "patch_incremental_ref": output_ref,
                        "patch_cumulative_ref": output_ref
                    },
                    "checkpoint_labels": checkpoint_labels,
                    "ext": {
                        "latest_workspace_ref": workspace_ref
                    },
                    "schedule_idx": 0,
                    "attempt": 1,
                    "row_seq": 0,
                    "slot_commit_id": "seed_slot_commit"
                }),
            )
            .expect("seed lineage");

        trial_dir
    }

    fn active_control_for_trial(trial_dir: &Path) -> ActiveAdapterControl {
        let control_path = trial_dir.join("state").join("lab_control.json");
        let payload = json!({
            "schema_version": "control_plane_v1",
            "seq": 0,
            "action": "continue",
            "label": null,
            "requested_at": Utc::now().to_rfc3339(),
            "requested_by": "run_loop",
        });
        atomic_write_json_pretty(&control_path, &payload).expect("control file");
        ActiveAdapterControl {
            adapter_id: BUILTIN_COMMAND_ADAPTER_ID.to_string(),
            adapter_version: BUILTIN_COMMAND_ADAPTER_VERSION.to_string(),
            command_path: control_path.to_string_lossy().to_string(),
            events_path: Some(
                trial_dir
                    .join("state")
                    .join("events.jsonl")
                    .to_string_lossy()
                    .to_string(),
            ),
        }
    }

    fn write_test_run_control(
        run_dir: &Path,
        run_id: &str,
        status: &str,
        active_trial_id: Option<&str>,
        active_control: Option<&ActiveAdapterControl>,
    ) {
        let active_trials = active_trial_id
            .map(|trial_id| {
                vec![RunControlActiveTrial {
                    trial_id: trial_id.to_string(),
                    worker_id: "worker_1".to_string(),
                    schedule_idx: None,
                    variant_id: None,
                    started_at: Some(Utc::now().to_rfc3339()),
                    control: active_control.cloned(),
                }]
            })
            .unwrap_or_default();
        write_run_control_v2(run_dir, run_id, status, &active_trials, None).expect("run control");
    }

    fn seed_continuable_container_run(prefix: &str) -> (TempDirGuard, PathBuf) {
        let (root, run_dir) = create_run_dir(prefix, "run_1");
        write_resolved_experiment_with_command(
            &run_dir,
            "cli_basic",
            harness_success_output_command(),
        );
        write_packaged_task_dataset(&run_dir.join("tasks.jsonl"), "task_1");
        write_test_run_control(&run_dir, "run_1", "paused", None, None);

        let resolved = load_json_file(&run_dir.join("resolved_experiment.json")).expect("resolved");
        let schedule = build_trial_schedule(
            1,
            1,
            1,
            parse_policies(&resolved).scheduling,
            experiment_random_seed(&resolved),
        );
        let schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: schedule.len(),
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule,
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        write_schedule_progress(&run_dir, &schedule_progress).expect("schedule progress");
        write_run_session_state(
            &run_dir,
            "run_1",
            &RunBehavior::default(),
            &container_execution(),
        )
        .expect("run session");

        (root, run_dir)
    }

    fn load_sqlite_json_row(run_dir: &Path, table: &str, run_id: &str) -> Value {
        let conn = rusqlite::Connection::open(run_sqlite_path(run_dir)).expect("open sqlite");
        let sql = format!(
            "SELECT row_json FROM {} WHERE run_id=?1 ORDER BY schedule_idx, attempt, row_seq LIMIT 1",
            table
        );
        let raw: String = conn
            .query_row(&sql, [run_id], |row| row.get(0))
            .expect("row json");
        serde_json::from_str(&raw).expect("decode row json")
    }

    fn spawn_pause_ack_writer(
        control_path: PathBuf,
        events_path: PathBuf,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut seen_versions = std::collections::BTreeSet::new();
            while Instant::now() < deadline {
                let bytes = match fs::read(&control_path) {
                    Ok(b) => b,
                    Err(_) => {
                        thread::sleep(Duration::from_millis(20));
                        continue;
                    }
                };
                let value: Value = match serde_json::from_slice(&bytes) {
                    Ok(v) => v,
                    Err(_) => {
                        thread::sleep(Duration::from_millis(20));
                        continue;
                    }
                };
                let action = value
                    .pointer("/action")
                    .and_then(|v| v.as_str())
                    .unwrap_or("continue");
                if action != "checkpoint" && action != "stop" {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }

                let version = sha256_bytes(&bytes);
                if !seen_versions.insert(version.clone()) {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }

                if let Some(parent) = events_path.parent() {
                    let _ = ensure_dir(parent);
                }
                let ack = json!({
                    "event_type": "control_ack",
                    "action_observed": action,
                    "control_version": version
                });
                if let Ok(mut file) = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&events_path)
                {
                    let _ = writeln!(file, "{}", ack);
                }
                if action == "stop" {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
        })
    }

    fn create_trial_paths_fixture(prefix: &str) -> (TempDirGuard, TrialPaths) {
        let root = TempDirGuard::new(prefix);
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        fs::write(exp_dir.join("README.md"), "fixture").expect("exp fixture");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let paths = TrialPaths::new(&trial_dir, &exp_dir).expect("trial paths");
        paths.prepare(true).expect("prepare");
        (root, paths)
    }

    #[test]
    fn trial_paths_drop_cleans_scratch_without_explicit_cleanup() {
        let root = TempDirGuard::new("agentlab_trial_paths_drop_cleanup");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        fs::write(exp_dir.join("README.md"), "fixture").expect("exp fixture");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");

        let scratch_dir = {
            let paths = TrialPaths::new(&trial_dir, &exp_dir).expect("trial paths");
            paths.prepare(true).expect("prepare");
            fs::write(paths.out.join("result.json"), "{}").expect("write result");
            assert!(
                paths.scratch_dir.exists(),
                "scratch dir should exist while trial paths live"
            );
            paths.scratch_dir.clone()
        };

        assert!(
            !scratch_dir.exists(),
            "trial path drop should cleanup scratch dir: {}",
            scratch_dir.display()
        );
    }

    #[test]
    fn contract_path_mapper_resolves_container_contract_paths() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_contract_mapper_container");
        let cases = vec![
            (
                format!("{}/trial_input.json", AGENTLAB_CONTRACT_IN_DIR),
                paths.in_dir.join("trial_input.json"),
            ),
            (
                format!("{}/events.jsonl", AGENTLAB_CONTRACT_STATE_DIR),
                paths.state.join("events.jsonl"),
            ),
            (
                format!("{}/result.json", AGENTLAB_CONTRACT_OUT_DIR),
                paths.out.join("result.json"),
            ),
        ];
        for (raw, expected) in cases {
            let resolved = map_container_path_to_host(&raw, &paths).expect("resolve path");
            assert_eq!(resolved, expected, "path mismatch for {}", raw);
        }

        let err = map_container_path_to_host("/stateful/not_state", &paths).expect_err("reject");
        assert!(
            err.to_string().contains("unsupported container mount path"),
            "unexpected error: {}",
            err
        );

        let err = map_container_path_to_host("/state/events.jsonl", &paths).expect_err("reject");
        assert!(
            err.to_string().contains("unsupported container mount path"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn contract_path_mapper_enforces_mode_specific_paths() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_contract_mapper_modes");
        let staged_support = task_workdir_support_destination_path("pkg.json");
        let resolved = resolve_trial_io_host_path(&staged_support, &paths).expect("support file");
        assert_eq!(
            resolved,
            paths
                .workspace
                .join(AGENTLAB_RUNNER_SUPPORT_REL_DIR)
                .join("pkg.json")
        );

        let err = resolve_trial_io_host_path("/unknown/pkg.json", &paths).expect_err("reject");
        assert!(
            err.to_string().contains("unsupported container mount path"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn contract_path_mapper_rejects_legacy_dataset_runtime_io_paths_in_container_mode() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_contract_mapper_dataset_legacy");
        let err = resolve_trial_io_host_path("/dataset/tasks.jsonl", &paths)
            .expect_err("reject legacy dataset runtime io path");
        assert!(
            err.to_string().contains("unsupported container mount path"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn contract_path_mapper_resolves_event_paths_and_rejects_invalid_roots() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_contract_mapper_events");
        let trial_dir = paths.in_dir.parent().expect("trial dir").to_path_buf();

        let in_path = format!("{}/trial_input.json", AGENTLAB_CONTRACT_IN_DIR);
        let resolved_in = resolve_event_path_for_trial(&in_path, &trial_dir).expect("in path");
        assert_eq!(resolved_in, trial_dir.join("in").join("trial_input.json"));

        let err = resolve_event_path_for_trial("/dataset/tasks.jsonl", &trial_dir)
            .expect_err("reject legacy dataset path");
        assert!(
            err.to_string()
                .contains("unsupported runtime event path for trial"),
            "unexpected error: {}",
            err
        );

        let err = resolve_event_path_for_trial("/harness/logs/events.jsonl", &trial_dir)
            .expect_err("reject");
        assert!(
            err.to_string()
                .contains("unsupported runtime event path for trial"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn write_state_inventory_container_excludes_legacy_dataset_mount() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_state_inventory_container");
        let runtime = legacy_contract_runtime_fixture();
        let experiment = json!({
            "version": "0.3",
            "design": { "sanitization_profile": "hermetic_functional" },
            "runtime": { "policy": { "network": { "mode": "none", "allowed_hosts": [] } } }
        });
        write_state_inventory(
            &paths.trial_dir,
            &experiment,
            &runtime,
            &paths,
            "sha256:test",
            "none",
            "runtime_agent",
            Some("img:latest"),
            Some("/workspace/task"),
        )
        .expect("write state inventory");
        let inventory = load_json_file(&paths.trial_dir.join("state_inventory.json"))
            .expect("load state inventory");
        let mounts = inventory
            .pointer("/mounts")
            .and_then(|v| v.as_array())
            .expect("mounts");
        let names = mounts
            .iter()
            .filter_map(|row| row.get("name").and_then(|v| v.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["in", "workspace", "state", "out", "tmp"]);
        assert!(
            !mounts.iter().any(|row| {
                row.get("path").and_then(|v| v.as_str()) == Some("/dataset")
                    || row.get("name").and_then(|v| v.as_str()) == Some("dataset")
            }),
            "legacy dataset mount unexpectedly present: {:?}",
            mounts
        );
        assert!(inventory.pointer("/planes/agent_runtime").is_some());
        assert!(inventory.pointer("/planes/task_sandbox").is_some());
        let agent_runtime_mounts = inventory
            .pointer("/planes/agent_runtime/mounts")
            .and_then(|v| v.as_array())
            .expect("agent runtime mounts");
        let task_sandbox_mounts = inventory
            .pointer("/planes/task_sandbox/mounts")
            .and_then(|v| v.as_array())
            .expect("task sandbox mounts");
        assert!(
            !agent_runtime_mounts
                .iter()
                .any(|row| { row.get("name").and_then(|v| v.as_str()) == Some("deps") }),
            "agent runtime deps mount unexpectedly present: {:?}",
            agent_runtime_mounts
        );
        assert!(
            !task_sandbox_mounts
                .iter()
                .any(|row| { row.get("name").and_then(|v| v.as_str()) == Some("deps") }),
            "task sandbox deps mount unexpectedly present: {:?}",
            task_sandbox_mounts
        );
    }

    #[test]
    fn write_state_inventory_local_excludes_legacy_dataset_mount() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_state_inventory_local");
        let runtime = legacy_contract_runtime_fixture();
        let experiment = json!({
            "version": "0.3",
            "design": { "sanitization_profile": "hermetic_functional" },
            "runtime": { "policy": { "network": { "mode": "none", "allowed_hosts": [] } } }
        });
        write_state_inventory(
            &paths.trial_dir,
            &experiment,
            &runtime,
            &paths,
            "sha256:test",
            "full",
            "runtime_agent",
            Some("img:latest"),
            Some("/workspace/task"),
        )
        .expect("write state inventory");
        let inventory = load_json_file(&paths.trial_dir.join("state_inventory.json"))
            .expect("load state inventory");
        let mounts = inventory
            .pointer("/mounts")
            .and_then(|v| v.as_array())
            .expect("mounts");
        let names = mounts
            .iter()
            .filter_map(|row| row.get("name").and_then(|v| v.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec!["in", "workspace", "state", "deps", "out", "tmp"]
        );
        assert!(
            !mounts
                .iter()
                .any(|row| row.get("name").and_then(|v| v.as_str()) == Some("dataset")),
            "legacy dataset mount unexpectedly present: {:?}",
            mounts
        );
        assert!(inventory.pointer("/planes/agent_runtime").is_some());
        assert!(inventory.pointer("/planes/task_sandbox").is_some());
    }

    #[test]
    fn write_state_inventory_container_reports_agent_bundle_mount_when_present() {
        let (root, paths) = create_trial_paths_fixture("agentlab_state_inventory_bundle");
        let mut runtime = legacy_contract_runtime_fixture();
        let bundle_dir = root.path.join("agent_bundle");
        ensure_dir(&bundle_dir).expect("bundle dir");
        runtime.agent_artifact = bundle_dir;
        let experiment = json!({
            "version": "0.3",
            "design": { "sanitization_profile": "hermetic_functional" },
            "runtime": { "policy": { "network": { "mode": "none", "allowed_hosts": [] } } }
        });
        write_state_inventory(
            &paths.trial_dir,
            &experiment,
            &runtime,
            &paths,
            "sha256:test",
            "none",
            "runtime_agent",
            Some("img:latest"),
            Some("/workspace/task"),
        )
        .expect("write state inventory");
        let inventory = load_json_file(&paths.trial_dir.join("state_inventory.json"))
            .expect("load state inventory");
        let agent_runtime_mounts = inventory
            .pointer("/planes/agent_runtime/mounts")
            .and_then(|v| v.as_array())
            .expect("agent runtime mounts");
        assert!(
            agent_runtime_mounts
                .iter()
                .any(|row| row.get("name").and_then(|v| v.as_str()) == Some("agent_bundle")),
            "agent bundle mount should be present when runtime.agent.bundle is configured: {:?}",
            agent_runtime_mounts
        );
    }

    #[test]
    fn run_session_state_roundtrip_normalizes_execution_options() {
        let (_root, run_dir) = create_run_dir("agentlab_run_session_state", "run_1");
        let behavior = RunBehavior {
            network_mode_override: Some("full".to_string()),
            require_network_none: false,
        };
        let execution = RunExecutionOptions {
            executor: Some(ExecutorKind::LocalDocker),
            materialize: None,
            runtime_env: BTreeMap::new(),
            runtime_env_files: Vec::new(),
        };
        write_run_session_state(&run_dir, "run_1", &behavior, &execution).expect("write state");
        let state = load_run_session_state(&run_dir).expect("load state");
        assert_eq!(state.schema_version, "run_session_state_v1");
        assert_eq!(state.run_id, "run_1");
        assert_eq!(
            state.behavior.network_mode_override.as_deref(),
            Some("full")
        );
        assert_eq!(state.execution.executor, Some(ExecutorKind::LocalDocker));
        assert_eq!(state.execution.materialize, Some(MaterializationMode::Full));
    }

    #[test]
    fn continue_run_accepts_paused_and_interrupted_terminal_statuses() {
        for status in ["paused", "interrupted"] {
            let (_root, run_dir) = create_run_dir("agentlab_continue_statuses", "run_1");
            write_test_run_control(&run_dir, "run_1", status, None, None);

            let err =
                continue_run(&run_dir).expect_err("continue should reach run session state load");
            assert!(
                err.to_string()
                    .contains("run_session_state_v1 not found in sqlite runtime_kv"),
                "status {} produced unexpected error: {}",
                status,
                err
            );
        }
    }

    #[test]
    fn continue_run_uses_persisted_behavior() {
        let (_root, run_dir) = create_run_dir("agentlab_continue_persisted_behavior", "run_1");
        let dataset_path = run_dir.join("tasks.jsonl");
        write_packaged_task_dataset(&dataset_path, "task_1");
        let resolved = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": {
                "path": "tasks.jsonl",
                "provider": "local_jsonl",
                "suite_id": "s",
                "schema_version": "v1",
                "split_id": "dev",
                "limit": 1
            },
            "design": {
                "sanitization_profile": "hermetic_functional",
                "comparison": "paired",
                "replications": 1,
                "random_seed": 1,
                "shuffle_tasks": false,
                "max_concurrency": 1
            },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent": {
                    "command": harness_success_command(),
                    "bundle": ".lab/agents/rex-current.tar.gz",
                    "integration_level": "cli_basic",
                    "io": { "input_arg": "--input", "output_arg": "--output" }
                },
                "sandbox": {
                    "executor": "docker",
                    "image_source": "global",
                    "image": "img",
                    "profile": "workspace_write",
                    "network": "full"
                },
                "policy": { "timeout_ms": 600000 }
            }
        });
        atomic_write_json_pretty(&run_dir.join("resolved_experiment.json"), &resolved)
            .expect("resolved");
        write_test_run_control(&run_dir, "run_1", "failed", None, None);
        let schedule = build_trial_schedule(1, 1, 1, parse_policies(&resolved).scheduling, 1);
        let schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: schedule.len(),
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule,
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        write_schedule_progress(&run_dir, &schedule_progress).expect("progress");
        let behavior = RunBehavior {
            network_mode_override: None,
            require_network_none: true,
        };
        write_run_session_state(&run_dir, "run_1", &behavior, &container_execution())
            .expect("run session");

        let err = continue_run(&run_dir).expect_err("continue should honor persisted behavior");
        assert!(
            err.to_string()
                .contains("run-experiment requires network mode 'none'"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn continue_run_e2e_executes_container_trial_and_persists_runtime_state() {
        let (_root, run_dir) = seed_continuable_container_run("agentlab_continue_e2e_runtime");

        let result = continue_run(&run_dir).expect("continue run");
        assert_eq!(result.run_id, "run_1");

        let control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        let trial_state = load_json_file(
            &run_dir
                .join("trials")
                .join("trial_1")
                .join("trial_state.json"),
        )
        .expect("trial state");
        if control.pointer("/status").and_then(Value::as_str) != Some("completed") {
            panic!("control={:?} trial_state={:?}", control, trial_state);
        }

        let schedule_progress = load_schedule_progress(&run_dir).expect("schedule progress");
        let trial_output =
            load_json_file(&run_dir.join("trials").join("trial_1").join("result.json"))
                .expect("trial output");
        assert_eq!(schedule_progress.next_schedule_index, 1);
        assert_eq!(schedule_progress.completed_slots.len(), 1);
        assert_eq!(schedule_progress.completed_slots[0].schedule_index, 0);
        assert_eq!(schedule_progress.completed_slots[0].trial_id, "trial_1");
        if schedule_progress.completed_slots[0].status != "completed" {
            panic!(
                "slot={:?} trial_state={:?} trial_output={:?}",
                schedule_progress.completed_slots[0], trial_state, trial_output
            );
        }
        assert_eq!(schedule_progress.completed_slots[0].attempt, 1);
        assert!(
            !schedule_progress.completed_slots[0]
                .slot_commit_id
                .is_empty(),
            "slot commit id should be persisted"
        );

        let store = BackingSqliteStore::open(&run_dir).expect("store");
        assert_eq!(store.row_count("evidence_rows").expect("evidence count"), 1);
        assert_eq!(
            store
                .row_count("chain_state_rows")
                .expect("chain state count"),
            1
        );
        assert!(
            store
                .latest_attempt_object_ref("run_1", "trial_1", "trial_input")
                .expect("trial_input ref")
                .is_some(),
            "trial input should be persisted into attempt objects"
        );
        assert!(
            store
                .latest_attempt_object_ref("run_1", "trial_1", "trial_output")
                .expect("trial_output ref")
                .is_some(),
            "trial output should be persisted into attempt objects"
        );
        assert!(
            store
                .has_lineage_for_trial("run_1", "trial_1")
                .expect("lineage"),
            "chain state should materialize lineage rows"
        );
        assert!(
            !run_dir
                .join("runtime")
                .join("worker_payload")
                .join("trial_1")
                .exists(),
            "worker payload spool should be cleaned up after commit"
        );
    }

    #[test]
    fn continue_run_e2e_commits_slot_identity_on_sqlite_json_rows() {
        let (_root, run_dir) = seed_continuable_container_run("agentlab_continue_e2e_sqlite");

        continue_run(&run_dir).expect("continue run");

        let evidence = load_sqlite_json_row(&run_dir, "evidence_rows", "run_1");
        assert_eq!(
            evidence.pointer("/run_id").and_then(Value::as_str),
            Some("run_1")
        );
        assert_eq!(
            evidence.pointer("/schedule_idx").and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            evidence.pointer("/attempt").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            evidence.pointer("/row_seq").and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            evidence.pointer("/ids/trial_id").and_then(Value::as_str),
            Some("trial_1")
        );
        assert!(
            evidence
                .pointer("/slot_commit_id")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty()),
            "evidence row should be annotated with slot identity"
        );

        let chain_state = load_sqlite_json_row(&run_dir, "chain_state_rows", "run_1");
        assert_eq!(
            chain_state.pointer("/run_id").and_then(Value::as_str),
            Some("run_1")
        );
        assert_eq!(
            chain_state.pointer("/schedule_idx").and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            chain_state.pointer("/attempt").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            chain_state.pointer("/row_seq").and_then(Value::as_u64),
            Some(0)
        );
        assert!(
            chain_state
                .pointer("/slot_commit_id")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty()),
            "chain state row should be annotated with slot identity"
        );
    }

    #[test]
    fn resolve_agent_runtime_parses_launch_mode_stdio() {
        let root = TempDirGuard::new("agentlab_launch_mode_parse");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent": {
                    "command": ["sh", "-lc", "echo ok"],
                    "bundle": ".lab/agents/rex-current.tar.gz",
                    "integration_level": "cli_basic",
                    "launch": { "mode": "stdio" }
                },
                "sandbox": runtime_sandbox("global", Some("img")),
                "policy": { "timeout_ms": 600000 }
            }
        });

        let agent_runtime =
            resolve_agent_runtime(&spec, &exp_dir, &root.path).expect("resolve runtime");
        assert_eq!(agent_runtime.launch_mode, AgentLaunchMode::Stdio);
    }

    #[test]
    fn resolve_agent_runtime_custom_image_supports_command_override_string() {
        let root = TempDirGuard::new("agentlab_command_override_string");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent": {
                    "command": "rex",
                    "bundle": ".lab/agents/rex-current.tar.gz"
                },
                "sandbox": runtime_sandbox("global", Some("img")),
                "policy": { "timeout_ms": 600000 }
            }
        });

        let agent_runtime =
            resolve_agent_runtime(&spec, &exp_dir, &root.path).expect("resolve runtime");
        assert_eq!(agent_runtime.command_raw, vec!["rex"]);
    }

    #[test]
    fn resolve_agent_runtime_per_task_requires_artifact() {
        let root = TempDirGuard::new("agentlab_per_task_requires_artifact");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent": {
                    "command": ["rex", "run"]
                },
                "sandbox": runtime_sandbox("per_task", None),
                "policy": { "timeout_ms": 600000 }
            }
        });

        let err = match resolve_agent_runtime(&spec, &exp_dir, &root.path) {
            Ok(_) => panic!("missing artifact should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("runtime.agent.bundle is required"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_agent_runtime_rejects_legacy_aliases() {
        let root = TempDirGuard::new("agentlab_command_aliases");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent": {
                    "command": ["rex", "run-agent-loop"],
                    "aliases": {
                        "rex": ["bun", "./scripts/rex.js"]
                    },
                    "bundle": ".lab/agents/rex-current.tar.gz"
                },
                "sandbox": runtime_sandbox("global", Some("img")),
                "policy": { "timeout_ms": 600000 }
            }
        });

        let err = match resolve_agent_runtime(&spec, &exp_dir, &root.path) {
            Ok(_) => panic!("legacy aliases should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("hard cut"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn build_runtime_contract_env_includes_agentlabd_keys() {
        let io = prepared_trial_io_fixture(
            PathBuf::from("/tmp/out.json"),
            PathBuf::from("/tmp/events.jsonl"),
        );
        let input = json!({
            "ids": {
                "trial_id": "trial_1",
                "variant_id": "control",
                "task_id": "task_1",
                "repl_idx": 0
            }
        });
        let env = build_runtime_contract_env("run_1", &input, &io, None, Some(12345));
        assert_eq!(
            env.get(AGENTLAB_ENV_TRIAL_INPUT_PATH).map(String::as_str),
            Some(AGENTLAB_TRIAL_INPUT_PATH)
        );
        assert_eq!(
            env.get(AGENTLAB_ENV_GRADER_INPUT_PATH).map(String::as_str),
            Some(AGENTLAB_GRADER_INPUT_PATH)
        );
        assert_eq!(
            env.get(AGENTLAB_ENV_RESULT_PATH).map(String::as_str),
            Some(AGENTLAB_RESULT_PATH)
        );
    }

    #[test]
    fn build_runtime_contract_env_includes_paths_for_minimal_input() {
        let io = prepared_trial_io_fixture(
            PathBuf::from("/tmp/out.json"),
            PathBuf::from("/tmp/events.jsonl"),
        );
        let input = json!({ "ids": { "trial_id": "trial_1" } });
        let env = build_runtime_contract_env("run_1", &input, &io, None, Some(12345));
        assert!(
            env.contains_key(AGENTLAB_ENV_TRIAL_INPUT_PATH),
            "runtime env should always include AGENTLAB_* paths after the hard cutover"
        );
    }

    #[test]
    fn resolve_harness_rejects_runtime_dependency_file_staging() {
        let root = TempDirGuard::new("agentlab_reject_runtime_file_staging");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent_runtime": {
                    "command": ["sh", "-lc", "echo ok"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest",
                    "env": {"A":"B"}
                },
                "dependencies": {
                    "file_staging": [
                        {
                            "source_from_host": "./secrets/graphd.db",
                            "destination_path": format!("{}/.graphd/graphd.db", AGENTLAB_CONTRACT_STATE_DIR),
                            "required": true
                        }
                    ]
                }
            }
        });

        let err = match resolve_agent_runtime(&spec, &exp_dir, &root.path) {
            Ok(_) => panic!("runtime.dependencies.file_staging must be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("runtime.dependencies.file_staging is not supported"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_harness_rejects_benchmark_grader_support_files() {
        let root = TempDirGuard::new("agentlab_reject_benchmark_support_files");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "benchmark": {
                "grader": {
                    "command": ["python3", "grader.py"],
                    "support_files": [
                        {
                            "source_from_host": "./bench",
                            "destination_path": "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/bench"
                        }
                    ]
                }
            },
            "runtime": {
                "agent_runtime": {
                    "command": ["sh", "-lc", "echo ok"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest"
                }
            }
        });

        let err = match resolve_agent_runtime(&spec, &exp_dir, &root.path) {
            Ok(_) => panic!("benchmark.grader.support_files must fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("benchmark.grader.support_files is not supported"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_harness_rejects_secret_files() {
        let root = TempDirGuard::new("agentlab_secret_files_rejected");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent_runtime": {
                    "command": ["sh", "-lc", "echo ok"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest"
                },
                "dependencies": {
                    "secret_files": [
                        {
                            "source_from_host": "./secrets/api.key",
                            "destination_path": task_workdir_support_destination_path("api.key")
                        }
                    ]
                }
            }
        });

        let err = match resolve_agent_runtime(&spec, &exp_dir, &root.path) {
            Ok(_) => panic!("runtime.dependencies.secret_files must be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("runtime.dependencies.secret_files is not supported"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_harness_rejects_secret_env_aliases() {
        let root = TempDirGuard::new("agentlab_secret_env_aliases");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        let spec = json!({
            "runtime": {
                "agent_runtime": {
                    "command": ["sh", "-lc", "echo ok"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest",
                    "secret_env": ["ANTHROPIC_API_KEY"]
                },
                "dependencies": {}
            }
        });

        let err = match resolve_agent_runtime(&spec, &exp_dir, &root.path) {
            Ok(_) => panic!("should reject legacy aliases"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("runtime.agent_runtime.secret_env is not supported"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn replay_grade_maps_by_integration_level() {
        assert_eq!(replay_grade_for_integration("sdk_full"), "strict");
        assert_eq!(replay_grade_for_integration("sdk_control"), "checkpointed");
        assert_eq!(replay_grade_for_integration("cli_events"), "best_effort");
        assert_eq!(replay_grade_for_integration("cli_basic"), "best_effort");
    }

    #[test]
    fn run_operation_lease_is_exclusive() {
        let run_dir = std::env::temp_dir().join(format!(
            "agentlab_lock_test_{}_{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        ensure_dir(&run_dir).expect("temp run dir");

        let lock1 = acquire_run_operation_lease(&run_dir, RunOperationType::Continue)
            .expect("first lock must succeed");
        let err = acquire_run_operation_lease(&run_dir, RunOperationType::Continue)
            .expect_err("second lock must fail");
        assert!(
            err.to_string().contains("operation_in_progress"),
            "unexpected lock error: {}",
            err
        );
        drop(lock1);
        let lock2 = acquire_run_operation_lease(&run_dir, RunOperationType::Continue)
            .expect("lock should be re-acquirable");
        drop(lock2);
        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn fork_selector_parser_accepts_supported_kinds() {
        match parse_fork_selector("checkpoint:ckpt_a").expect("checkpoint selector") {
            ForkSelector::Checkpoint(v) => assert_eq!(v, "ckpt_a"),
            _ => panic!("expected checkpoint"),
        }
        match parse_fork_selector("step:12").expect("step selector") {
            ForkSelector::Step(v) => assert_eq!(v, 12),
            _ => panic!("expected step"),
        }
        match parse_fork_selector("event_seq:34").expect("event_seq selector") {
            ForkSelector::EventSeq(v) => assert_eq!(v, 34),
            _ => panic!("expected event_seq"),
        }
        assert!(parse_fork_selector("bad").is_err());
        assert!(parse_fork_selector("unknown:1").is_err());
    }

    #[test]
    fn adapter_control_ack_received_matches_action_and_control_version() {
        let root = std::env::temp_dir().join(format!(
            "agentlab_ack_test_{}_{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        ensure_dir(&root).expect("temp dir");
        let events_path = root.join("harness_events.jsonl");
        let line = r#"{"event_type":"control_ack","seq":9,"step_index":2,"control_version":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","action_observed":"stop"}"#;
        atomic_write_bytes(&events_path, format!("{}\n", line).as_bytes()).expect("write events");

        assert!(adapter_control_ack_received(
            &events_path,
            "stop",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        .expect("parse ack"));
        assert!(!adapter_control_ack_received(
            &events_path,
            "checkpoint",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        .expect("parse ack"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_resume_selector_prefers_requested_label() {
        let (_root, run_dir) = create_run_dir("agentlab_resume_sel_test", "run_1");
        seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([
                {"path": format!("{}/ckpt_a", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "a", "step": 1},
                {"path": format!("{}/ckpt_b", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "b", "step": 2}
            ]),
            "paused",
            Some("a"),
        );
        let selector =
            resolve_resume_selector(&run_dir, "run_1", "trial_1", Some("a")).expect("selector");
        assert_eq!(selector, "checkpoint:a");
    }

    #[test]
    fn resolve_resume_selector_defaults_to_latest_step() {
        let (_root, run_dir) = create_run_dir("agentlab_resume_default_test", "run_1");
        seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([
                {"path": format!("{}/ckpt_a", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "a", "step": 3},
                {"path": format!("{}/ckpt_b", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "b", "step": 5}
            ]),
            "paused",
            Some("b"),
        );
        let selector =
            resolve_resume_selector(&run_dir, "run_1", "trial_1", None).expect("selector");
        assert_eq!(selector, "checkpoint:b");
    }

    #[test]
    fn resolve_resume_selector_errors_when_label_not_found() {
        let (_root, run_dir) = create_run_dir("agentlab_resume_missing_label_test", "run_1");
        seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/ckpt_a", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "a", "step": 1}]),
            "paused",
            Some("a"),
        );
        let err = resolve_resume_selector(&run_dir, "run_1", "trial_1", Some("missing"))
            .expect_err("should fail");
        assert!(
            err.to_string().contains("resume_checkpoint_not_found"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn parse_fork_selector_rejects_empty_checkpoint_name() {
        let err = match parse_fork_selector("checkpoint: ") {
            Ok(_) => panic!("empty checkpoint should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("checkpoint name empty"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_selector_checkpoint_non_strict_uses_lineage_token_when_available() {
        let (_root, run_dir) = create_run_dir("agentlab_fork_selector_path_missing", "run_1");
        let trial_dir = seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp_missing", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 3}]),
            "completed",
            None,
        );
        let output = json!({
            "checkpoints": [
                {"path": format!("{}/cp_missing", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 3}
            ]
        });
        let selector = parse_fork_selector("checkpoint:cp1").expect("selector");
        let source = resolve_selector_checkpoint(&selector, Some(&output), &trial_dir, false)
            .expect("selector resolution");
        assert!(
            source
                .as_deref()
                .is_some_and(|token| token.starts_with("lineage:")),
            "expected lineage token, got {:?}",
            source
        );
    }

    #[test]
    fn resolve_selector_checkpoint_strict_uses_lineage_not_fs_path() {
        let (_root, run_dir) = create_run_dir("agentlab_fork_selector_strict_missing", "run_1");
        let trial_dir = seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp_missing", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 3}]),
            "completed",
            None,
        );
        let output = json!({
            "checkpoints": [
                {"path": format!("{}/cp_missing", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 3}
            ]
        });
        let selector = parse_fork_selector("checkpoint:cp1").expect("selector");
        let token = resolve_selector_checkpoint(&selector, Some(&output), &trial_dir, true)
            .expect("strict resolution should succeed with lineage");
        assert!(
            token
                .as_deref()
                .is_some_and(|value| value.starts_with("lineage:")),
            "unexpected token: {:?}",
            token
        );
    }

    #[test]
    fn replay_trial_requires_prepared_environment_manifest_and_rejects_trial_input_fallback() {
        let (_root, run_dir) =
            create_run_dir("agentlab_replay_no_legacy_dataset_trial_dir", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        write_packaged_task_dataset(&run_dir.join("tasks.jsonl"), "task_1");
        let parent_trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "completed", None);
        assert!(
            !parent_trial_dir.join("dataset").exists(),
            "hard cutover: parent trial should not carry legacy dataset dir"
        );

        assert!(
            parent_trial_dir.join("trial_input.json").exists(),
            "seeded trial input should exist so replay cannot quietly fall back to it"
        );
        let err = match replay_trial(&run_dir, "trial_1", false) {
            Ok(_) => panic!("replay should require prepared_task_environment metadata"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("prepared_task_environment"),
            "replay should fail on missing prepared environment manifest, got: {}",
            msg
        );
        assert!(
            msg.contains("trial_1"),
            "replay failure should identify the affected trial, got: {}",
            msg
        );
    }

    #[test]
    fn fork_trial_requires_prepared_environment_manifest_without_input_only_fallback() {
        let (_root, run_dir) = create_run_dir("agentlab_fork_input_fallback", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp_missing", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 1}]),
            "completed",
            None,
        );
        let conn = rusqlite::Connection::open(run_sqlite_path(&run_dir)).expect("open sqlite");
        conn.execute("DELETE FROM lineage_versions", [])
            .expect("delete lineage versions");
        conn.execute("DELETE FROM lineage_heads", [])
            .expect("delete lineage heads");

        let err = match fork_trial(
            &run_dir,
            "trial_1",
            "checkpoint:cp1",
            &BTreeMap::new(),
            false,
        ) {
            Ok(_) => panic!("fork should require prepared_task_environment metadata"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("prepared_task_environment"),
            "fork should fail on missing prepared environment manifest, got: {}",
            msg
        );
        assert!(
            !msg.contains("input_only"),
            "fork should not advertise legacy input_only fallback, got: {}",
            msg
        );
    }

    #[test]
    fn fork_trial_strict_requires_sdk_full_integration_level() {
        let (_root, run_dir) = create_run_dir("agentlab_fork_strict_level", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp1", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 1}]),
            "completed",
            None,
        );

        let err = fork_trial(
            &run_dir,
            "trial_1",
            "checkpoint:cp1",
            &BTreeMap::new(),
            true,
        )
        .err()
        .expect("strict fork should fail for non-sdk_full");
        assert!(
            err.to_string()
                .contains("strict fork requires integration_level sdk_full"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn fork_trial_strict_fails_when_selected_checkpoint_is_unavailable() {
        let (_root, run_dir) = create_run_dir("agentlab_fork_strict_checkpoint", "run_1");
        write_resolved_experiment(&run_dir, "sdk_full", true);
        seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp_missing", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 1}]),
            "completed",
            None,
        );
        let conn = rusqlite::Connection::open(run_sqlite_path(&run_dir)).expect("open sqlite");
        conn.execute("DELETE FROM lineage_versions", [])
            .expect("delete lineage versions");
        conn.execute("DELETE FROM lineage_heads", [])
            .expect("delete lineage heads");

        let err = fork_trial(
            &run_dir,
            "trial_1",
            "checkpoint:cp1",
            &BTreeMap::new(),
            true,
        )
        .err()
        .expect("strict fork should fail when checkpoint bytes are unavailable");
        assert!(
            err.to_string().contains("strict_source_unavailable"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn pause_run_rejects_target_trial_that_is_not_active() {
        let (_root, run_dir) = create_run_dir("agentlab_pause_not_active", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        let control = active_control_for_trial(&trial_dir);
        write_test_run_control(
            &run_dir,
            "run_1",
            "running",
            Some("trial_1"),
            Some(&control),
        );

        let err = pause_run(&run_dir, Some("trial_2"), Some("pause"), 1)
            .err()
            .expect("pause should reject non-active target");
        assert!(
            err.to_string().contains("pause_target_not_active"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resume_run_requires_run_to_be_paused() {
        let (_root, run_dir) = create_run_dir("agentlab_resume_not_paused", "run_1");
        write_resolved_experiment(&run_dir, "sdk_full", true);
        let trial_dir = seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp1", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 1}]),
            "paused",
            Some("cp1"),
        );
        ensure_dir(&trial_dir.join("state").join("cp1")).expect("checkpoint path");
        let control = active_control_for_trial(&trial_dir);
        write_test_run_control(
            &run_dir,
            "run_1",
            "running",
            Some("trial_1"),
            Some(&control),
        );

        let err = resume_trial(&run_dir, None, None, &BTreeMap::new(), false)
            .err()
            .expect("resume should fail for non-paused run");
        assert!(
            err.to_string().contains("resume_non_paused"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resume_run_requires_trial_state_to_be_paused() {
        let (_root, run_dir) = create_run_dir("agentlab_resume_trial_state", "run_1");
        write_resolved_experiment(&run_dir, "sdk_full", true);
        let trial_dir = seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([{"path": format!("{}/cp1", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp1", "step": 1}]),
            "completed",
            None,
        );
        ensure_dir(&trial_dir.join("state").join("cp1")).expect("checkpoint path");
        let control = active_control_for_trial(&trial_dir);
        write_test_run_control(&run_dir, "run_1", "paused", Some("trial_1"), Some(&control));

        let err = resume_trial(&run_dir, None, None, &BTreeMap::new(), false)
            .err()
            .expect("resume should fail when trial state is not paused");
        assert!(
            err.to_string().contains("resume_trial_not_paused"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resume_trial_requires_prepared_environment_manifest_for_fork_resume() {
        let (_root, run_dir) = create_run_dir("agentlab_resume_success", "run_1");
        write_resolved_experiment(&run_dir, "sdk_full", true);
        let trial_dir = seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([
                {"path": format!("{}/cp_old", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp_old", "step": 1},
                {"path": format!("{}/cp_resume", AGENTLAB_CONTRACT_STATE_DIR), "logical_name": "cp_resume", "step": 2}
            ]),
            "paused",
            Some("cp_resume"),
        );
        ensure_dir(&trial_dir.join("state").join("cp_resume")).expect("checkpoint path");
        let control = active_control_for_trial(&trial_dir);
        write_test_run_control(&run_dir, "run_1", "paused", Some("trial_1"), Some(&control));

        let mut set_bindings = BTreeMap::new();
        set_bindings.insert("resume.override".to_string(), json!(42));
        let err = match resume_trial(&run_dir, None, None, &set_bindings, false) {
            Ok(_) => panic!("resume should require prepared_task_environment metadata"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("prepared_task_environment"),
            "resume should fail on missing prepared environment manifest, got: {}",
            msg
        );
    }

    #[test]
    fn validate_required_fields_passes_on_complete_spec() {
        let spec = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent_runtime": {
                    "command": ["node", "h.js"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest"
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": { "network": "none" }
            }
        });
        validate_required_fields(&spec).expect("valid spec should pass");
    }

    #[test]
    fn validate_required_fields_reports_all_missing() {
        let spec = json!({
            "experiment": { "id": "e", "name": "n" },
            "dataset": { "path": "tasks.jsonl" },
            "design": {},
            "baseline": {},
            "runtime": {},
            "policy": { "task_sandbox": {} }
        });
        let err = validate_required_fields(&spec).expect_err("should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("/experiment/workload_type"),
            "missing workload_type: {}",
            msg
        );
        assert!(
            msg.contains("/design/replications"),
            "missing replications: {}",
            msg
        );
        assert!(
            msg.contains("/runtime/agent_runtime"),
            "missing runtime.agent_runtime: {}",
            msg
        );
        assert!(
            msg.contains("/policy/task_sandbox/network"),
            "missing task_sandbox.network: {}",
            msg
        );
        assert!(
            msg.contains("/runtime/agent_runtime/command"),
            "missing runtime.agent_runtime.command: {}",
            msg
        );
        assert!(
            msg.contains("/runtime/agent_runtime/artifact"),
            "missing artifact: {}",
            msg
        );
        assert!(
            msg.contains("/runtime/agent_runtime/image"),
            "missing image: {}",
            msg
        );
        assert!(
            msg.contains("/policy/timeout_ms"),
            "missing timeout: {}",
            msg
        );
        assert!(
            msg.contains("/baseline/variant_id"),
            "missing baseline variant_id: {}",
            msg
        );
    }

    #[test]
    fn validate_required_fields_allows_missing_integration_level() {
        let spec = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent_runtime": {
                    "command": ["node", "h.js"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest"
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": { "network": "none" }
            }
        });
        validate_required_fields(&spec).expect("missing integration_level should default");
    }

    #[test]
    fn validate_required_fields_requires_image_for_container_mode() {
        let spec = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent_runtime": {
                    "command": ["node", "/app/h.js"],
                    "artifact": ".lab/agents/rex-current.tar.gz"
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": { "network": "none" }
            }
        });
        let err = validate_required_fields(&spec).expect_err("should fail");
        assert!(
            err.to_string().contains("/runtime/agent_runtime/image"),
            "missing runtime.agent_runtime.image: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_allows_missing_task_sandbox_profile() {
        let spec = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent_runtime": {
                    "command": ["node", "/app/h.js"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest"
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": { "network": "none" }
            }
        });
        validate_required_fields(&spec).expect("task_sandbox.profile should default");
    }

    #[test]
    fn validate_required_fields_requires_artifact_for_agent_runtime() {
        let spec = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent_runtime": {
                    "command": ["node", "/app/h.js"],
                    "image": "ghcr.io/acme/agent-runtime:latest"
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": { "network": "none" }
            }
        });
        let err = validate_required_fields(&spec).expect_err("missing artifact should fail");
        assert!(
            err.to_string().contains("/runtime/agent_runtime/artifact"),
            "expected missing artifact error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_rejects_removed_agent_runtime_support_files() {
        let spec = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent_runtime": {
                    "command": ["node", "/app/h.js"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest",
                    "support_files": [{"packaged_path": "deps/tool.py"}]
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": { "network": "none" }
            }
        });
        let err = validate_required_fields(&spec)
            .expect_err("runtime.agent_runtime.support_files should be rejected");
        assert!(
            err.to_string()
                .contains("/runtime/agent_runtime/support_files was removed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_variant_plan_ignores_version_field() {
        let spec = json!({
            "version": "1.0",
            "baseline": { "variant_id": "base", "args": ["--temperature", "0.7"] },
            "variant_plan": [
                { "variant_id": "hot", "args": ["--temperature", "0.9"] }
            ]
        });
        let (variants, baseline_id) = resolve_variant_plan(&spec).expect("variant plan");
        assert_eq!(baseline_id, "base");
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[1].id, "hot");
    }

    #[test]
    fn resolve_variant_plan_accepts_variants_alias() {
        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variants": [
                { "id": "old", "config": { "temperature": 0.7 } }
            ]
        });
        let (variants, baseline_id) = resolve_variant_plan(&spec).expect("variants alias");
        assert_eq!(baseline_id, "base");
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[1].id, "old");
        assert_eq!(variants[1].bindings["temperature"], json!(0.7));
    }

    #[test]
    fn resolve_variant_plan_rejects_bad_variant_plan_entry() {
        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [
                { "bindings": { "temperature": 0.8 } },
                { "variant_id": "t2", "bindings": [] }
            ]
        });

        let err = resolve_variant_plan(&spec).expect_err("bad variant plan should fail");
        assert!(
            err.to_string().contains("variant_plan[0]"),
            "unexpected error: {}",
            err
        );

        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [
                { "variant_id": "t2", "bindings": [] }
            ]
        });
        let err = resolve_variant_plan(&spec).expect_err("bad variant bindings type should fail");
        assert!(
            err.to_string().contains("variant_plan[0].bindings"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_variant_plan_uses_baseline_when_no_variant_plan_present() {
        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} }
        });

        let (variants, baseline_id) = resolve_variant_plan(&spec).expect("baseline only");
        assert_eq!(baseline_id, "base");
        assert_eq!(variants.len(), 1);
        assert_eq!(variants[0].id, "base");
    }

    #[test]
    fn load_run_variants_falls_back_to_experiment_when_manifest_missing() {
        let (_root, run_dir) = create_run_dir("agentlab_variants_fallback", "run_1");
        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [{ "variant_id": "alt", "bindings": { "temperature": 1.2 } }]
        });

        let (variants, baseline_id) =
            load_run_variants(&run_dir, &spec).expect("load fallback variants");
        assert_eq!(baseline_id, "base");
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0].id, "base");
        assert_eq!(variants[1].id, "alt");
    }

    #[test]
    fn load_run_variants_prefers_resolved_manifest_over_experiment() {
        let (_root, run_dir) = create_run_dir("agentlab_variants_manifest_preferred", "run_1");
        let project_root = find_project_root(&run_dir);
        let bundle_root = ensure_test_agent_bundle(&project_root, "rex-current");
        let original = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [{ "variant_id": "alt", "bindings": { "temperature": 1.2 } }],
            "runtime": {
                "agent": {
                    "command": harness_success_command(),
                    "bundle": bundle_root.to_string_lossy().to_string(),
                    "io": { "input_arg": "--input", "output_arg": "--output" }
                },
                "sandbox": runtime_sandbox("global", Some("img")),
                "policy": { "timeout_ms": 600000 }
            }
        });
        let (resolved_variants, resolved_baseline) =
            resolve_variant_plan(&original).expect("resolve variants");
        write_resolved_variants(&run_dir, &original, &resolved_baseline, &resolved_variants)
            .expect("write manifest");

        let changed = json!({
            "baseline": { "variant_id": "changed", "bindings": {} },
            "variant_plan": [{ "variant_id": "new", "bindings": { "temperature": 0.2 } }]
        });
        let (loaded_variants, loaded_baseline) =
            load_run_variants(&run_dir, &changed).expect("load manifest variants");

        assert_eq!(loaded_baseline, "base");
        assert_eq!(loaded_variants.len(), 2);
        assert_eq!(loaded_variants[0].id, "base");
        assert_eq!(loaded_variants[1].id, "alt");
    }

    #[test]
    fn load_run_variants_rejects_manifest_without_variant_digest() {
        let (_root, run_dir) = create_run_dir("agentlab_variants_manifest_missing_digest", "run_1");
        fs::write(
            run_dir.join("resolved_variants.json"),
            serde_json::to_vec_pretty(&json!({
                "schema_version": "resolved_variants_v1",
                "generated_at": "2026-03-10T00:00:00Z",
                "baseline_id": "base",
                "variants": [
                    {
                        "id": "base",
                        "bindings": {},
                        "args": [],
                        "env": {},
                        "image": null,
                        "runtime_overrides": null
                    }
                ]
            }))
            .expect("serialize manifest"),
        )
        .expect("write manifest");

        let err = load_run_variants(&run_dir, &json!({})).expect_err("missing variant_digest");
        assert!(err.to_string().contains("variant_digest"), "{}", err);
    }

    #[test]
    fn variant_digest_changes_with_variant_configuration() {
        let base = Variant {
            id: "base".to_string(),
            bindings: json!({}),
            args: vec!["--temperature".to_string(), "0.7".to_string()],
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let mut changed = base.clone();
        changed.args = vec!["--temperature".to_string(), "1.2".to_string()];

        let base_digest = variant_digest(&base).expect("base digest");
        let changed_digest = variant_digest(&changed).expect("changed digest");
        assert_ne!(base_digest, changed_digest);
    }

    #[test]
    fn resolve_variant_plan_parses_runtime_overrides() {
        let spec = json!({
            "baseline": {
                "variant_id": "base",
                "bindings": {},
                "runtime_overrides": {
                    "policy": {
                        "timeout_ms": 123000
                    }
                }
            },
            "variant_plan": [
                {
                    "variant_id": "treatment",
                    "bindings": {},
                    "runtime_overrides": {
                        "agent": {
                            "custom_image": {
                                "image": "example:variant"
                            }
                        }
                    }
                }
            ]
        });

        let (variants, baseline_id) = resolve_variant_plan(&spec).expect("variant plan");
        assert_eq!(baseline_id, "base");
        assert_eq!(variants.len(), 2);
        assert!(variants[0].runtime_overrides.is_some());
        assert!(variants[1].runtime_overrides.is_some());
    }

    #[test]
    fn resolve_variant_plan_rejects_invalid_runtime_overrides_shape() {
        let spec = json!({
            "baseline": {
                "variant_id": "base",
                "bindings": {},
                "runtime_overrides": "bad"
            }
        });
        let err = resolve_variant_plan(&spec).expect_err("baseline runtime_overrides should fail");
        assert!(
            err.to_string().contains("/baseline/runtime_overrides"),
            "unexpected error: {}",
            err
        );

        let spec = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [
                {
                    "variant_id": "treatment",
                    "bindings": {},
                    "runtime_overrides": "bad"
                }
            ]
        });
        let err = resolve_variant_plan(&spec).expect_err("variant runtime_overrides should fail");
        assert!(
            err.to_string()
                .contains("/variant_plan[0].runtime_overrides"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn resolve_runtime_for_variant_merges_runtime_overrides() {
        let base = json!({
            "runtime": {
                "agent": {
                    "mode": "custom_image",
                    "custom_image": {
                        "image": "base:image",
                        "entrypoint": ["echo", "base"]
                    },
                    "overrides": {
                        "env": {
                            "A": "1",
                            "B": "2"
                        }
                    }
                },
                "policy": {
                    "timeout_ms": 600000
                }
            }
        });
        let variant = Variant {
            id: "treatment".to_string(),
            bindings: json!({}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: Some(json!({
                "agent": {
                    "custom_image": {
                        "image": "treatment:image"
                    },
                    "overrides": {
                        "env": {
                            "B": "override",
                            "C": "3"
                        }
                    }
                },
                "policy": {
                    "timeout_ms": 900000
                }
            })),
        };

        let merged = resolve_runtime_for_variant(&base, &variant).expect("merge");
        assert_eq!(
            merged
                .pointer("/runtime/agent/custom_image/image")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "treatment:image"
        );
        assert_eq!(
            merged
                .pointer("/runtime/agent/custom_image/entrypoint")
                .and_then(|v| v.as_array())
                .map(|v| v.len())
                .unwrap_or(0),
            2
        );
        assert_eq!(
            merged
                .pointer("/runtime/agent/overrides/env/A")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "1"
        );
        assert_eq!(
            merged
                .pointer("/runtime/agent/overrides/env/B")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "override"
        );
        assert_eq!(
            merged
                .pointer("/runtime/agent/overrides/env/C")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "3"
        );
        assert_eq!(
            merged
                .pointer("/runtime/policy/timeout_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            900000
        );
    }

    #[test]
    fn resolve_runtime_for_variant_surfaces_removed_dependency_file_staging() {
        let base = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "control" },
            "runtime": {
                "agent_runtime": {
                    "command": ["sh", "-lc", "echo ok"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest"
                },
                "dependencies": {
                    "file_staging": [
                        {
                            "source_from_host": "files/benchmark.py",
                            "destination_path": "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/benchmark.py",
                            "required": true,
                            "read_only": true
                        },
                        {
                            "source_from_host": "files/defaults.json",
                            "destination_path": "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/defaults.json",
                            "required": true,
                            "read_only": true
                        }
                    ]
                }
            },
            "policy": { "timeout_ms": 600000, "task_sandbox": { "network": "none" } }
        });
        let variant = Variant {
            id: "treatment".to_string(),
            bindings: json!({}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: Some(json!({
                "dependencies": {
                    "file_staging": [
                        {
                            "source_from_host": "files/alt-defaults.json",
                            "destination_path": "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/defaults.json",
                            "required": true,
                            "read_only": true
                        },
                        {
                            "source_from_host": "files/codex-auth.json",
                            "destination_path": "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/.config/rex/codex-auth.json",
                            "required": true,
                            "read_only": true
                        }
                    ]
                }
            })),
        };

        let merged = resolve_runtime_for_variant(&base, &variant).expect("merge");
        let err = validate_required_fields(&merged)
            .expect_err("merged variant should reject removed runtime.dependencies.file_staging");
        assert!(
            err.to_string()
                .contains("/runtime/dependencies/file_staging was removed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_requires_benchmark_grader_command() {
        let spec = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "benchmark": {
                "policy": { "task_model": "independent" }
            },
            "runtime": {
                "agent_runtime": {
                    "command": ["node", "h.js"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest"
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": { "network": "none" }
            }
        });
        let err = validate_required_fields(&spec).expect_err("should fail");
        assert!(
            err.to_string().contains("/benchmark/grader/command"),
            "missing benchmark grader command: {}",
            err
        );
    }

    #[test]
    fn p0_freeze_benchmark_adaptation_trial_shape_fixture_parses() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../testdata/p0_benchmark_adaptation_trial_shape.json"
        ))
        .expect("fixture json");
        let resolved = fixture
            .pointer("/resolved_experiment")
            .cloned()
            .expect("resolved fixture");
        let benchmark = parse_benchmark_config(&resolved);
        assert_eq!(benchmark.policy.task_model, TaskModel::Dependent);
        assert_eq!(benchmark.policy.scoring_lifecycle, "predict_then_score");
        assert_eq!(
            benchmark.policy.required_evidence_classes,
            vec!["agent_patch".to_string(), "grader_report".to_string()]
        );
        let dataset_task = fixture
            .pointer("/dataset_task_row")
            .cloned()
            .expect("dataset task row");
        let boundary = runtime_task_boundary_from_row(dataset_task);
        assert_eq!(
            boundary
                .task_payload
                .pointer("/id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "swebench__django__12345"
        );
        assert_eq!(boundary.task_image, "ghcr.io/acme/swebench-task:20260222");
        assert_eq!(boundary.task_workdir, "/testbed");
        assert_eq!(boundary.time_limit_ms, Some(1_800_000));
    }

    #[test]
    fn parse_benchmark_config_reads_typed_grader_contract() {
        let spec = json!({
            "benchmark": {
                "grader": {
                    "strategy": "injected",
                    "command": ["python3", "./grader.py"],
                    "conclusion": {
                        "mode": "mapper",
                        "mapper": "./mappers/normalize.py"
                    },
                    "injected": {
                        "bundle": "./graders/bundle.tar.gz",
                        "copy_dest": "/opt/grader"
                    }
                }
            }
        });

        let benchmark = parse_benchmark_config(&spec);
        let grader = benchmark.grader.expect("grader config");
        assert_eq!(grader.strategy, GradingStrategy::Injected);
        assert_eq!(grader.command, vec!["python3", "./grader.py"]);
        assert_eq!(grader.conclusion.mode, GraderConclusionMode::Mapper);
        assert_eq!(
            grader.conclusion.mapper.as_deref(),
            Some("./mappers/normalize.py")
        );
        let injected = grader.injected.expect("injected config");
        assert_eq!(injected.bundle, "./graders/bundle.tar.gz");
        assert_eq!(injected.copy_dest, "/opt/grader");
        assert!(grader.separate.is_none());
    }

    #[test]
    fn p6_run_control_v2_writer_emits_active_trials_without_legacy_mirrors() {
        let (_root, run_dir) = create_run_dir("agentlab_run_control_v2_writer", "run_1");
        write_test_run_control(&run_dir, "run_1", "running", Some("trial_1"), None);
        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");

        assert_eq!(
            run_control
                .pointer("/schema_version")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "run_control_v2"
        );
        assert_eq!(
            run_control
                .pointer("/active_trials/trial_1/trial_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "trial_1"
        );
        assert!(
            run_control.pointer("/active_trial_id").is_none(),
            "legacy /active_trial_id should be removed in P6 cleanup"
        );
        assert!(
            run_control.pointer("/active_adapter").is_none(),
            "legacy /active_adapter should be removed in P6 cleanup"
        );
    }

    #[test]
    fn p1_run_control_v2_schema_accepts_writer_payload() {
        let (_root, run_dir) = create_run_dir("agentlab_run_control_v2_schema", "run_1");
        write_test_run_control(&run_dir, "run_1", "running", Some("trial_1"), None);
        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        let schema = compile_schema("run_control_v2.jsonschema").expect("schema");
        match schema.validate(&run_control) {
            Ok(_) => {}
            Err(errors) => {
                let mut messages = Vec::new();
                for err in errors {
                    messages.push(err.to_string());
                }
                panic!(
                    "run_control_v2 schema validation failed: {}",
                    messages.join(" | ")
                );
            }
        };
    }

    #[test]
    fn p1_run_control_helpers_read_active_trial_and_control_from_v2_shape() {
        let run_control = json!({
            "schema_version": "run_control_v2",
            "run_id": "run_1",
            "status": "running",
            "active_trials": {
                "trial_alpha": {
                    "trial_id": "trial_alpha",
                    "worker_id": "worker_1",
                    "schedule_idx": 7,
                    "variant_id": "base",
                    "started_at": "2026-02-22T00:00:00Z",
                    "control": {
                        "id": "builtin.command_contract",
                        "version": "v1",
                        "command_path": "/tmp/control.json",
                        "events_path": "/tmp/events.jsonl"
                    }
                }
            },
            "updated_at": "2026-02-22T00:00:00Z"
        });

        let ids = run_control_active_trial_ids(&run_control);
        assert_eq!(ids, vec!["trial_alpha".to_string()]);
        let control = run_control_active_adapter_for_trial(&run_control, "trial_alpha")
            .expect("active adapter control");
        assert_eq!(
            control
                .pointer("/command_path")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "/tmp/control.json"
        );
    }

    #[derive(Debug, Clone)]
    struct DeterminismCompletion {
        schedule_idx: usize,
        classification: String,
    }

    #[derive(Debug, Deserialize)]
    struct P2EDeterminismFixture {
        schema_version: String,
        arrivals: Vec<P2EDeterminismArrival>,
        expected_commit_schedule_idx: Vec<usize>,
    }

    #[derive(Debug, Deserialize)]
    struct P2EDeterminismArrival {
        tick: usize,
        schedule_idx: usize,
        trial_id: String,
        classification: String,
    }

    struct OutOfOrderCompletionSimulator {
        by_tick: BTreeMap<usize, Vec<DeterminismCompletion>>,
    }

    impl OutOfOrderCompletionSimulator {
        fn from_fixture(fixture: &P2EDeterminismFixture) -> Self {
            let mut by_tick: BTreeMap<usize, Vec<DeterminismCompletion>> = BTreeMap::new();
            for row in fixture.arrivals.iter() {
                by_tick
                    .entry(row.tick)
                    .or_default()
                    .push(DeterminismCompletion {
                        schedule_idx: row.schedule_idx,
                        classification: row.classification.clone(),
                    });
            }
            Self { by_tick }
        }

        fn max_tick(&self) -> usize {
            self.by_tick.keys().copied().max().unwrap_or(0)
        }

        fn poll_tick(&mut self, tick: usize) -> Vec<DeterminismCompletion> {
            self.by_tick.remove(&tick).unwrap_or_default()
        }
    }

    fn load_p2e_determinism_fixture() -> P2EDeterminismFixture {
        let fixture: P2EDeterminismFixture =
            serde_json::from_str(include_str!("../testdata/p2e_determinism_fixture.json"))
                .expect("p2e fixture json");
        assert_eq!(fixture.schema_version, "p2e_determinism_fixture_v1");
        fixture
    }

    fn drain_ready_completions_in_schedule_order(
        pending: &mut BTreeMap<usize, DeterminismCompletion>,
        next_commit_idx: &mut usize,
    ) -> Vec<DeterminismCompletion> {
        let mut ready = Vec::new();
        loop {
            let Some(completion) = pending.remove(next_commit_idx) else {
                break;
            };
            *next_commit_idx += 1;
            ready.push(completion);
        }
        ready
    }

    #[test]
    fn p5b_local_worker_capacity_ceiling_resolves_with_warning() {
        let (effective, warning) = resolve_local_worker_max_in_flight(8, Some(3));
        assert_eq!(effective, 3);
        assert!(
            warning
                .as_deref()
                .unwrap_or("")
                .contains("capacity ceiling applied"),
            "expected capacity warning, got: {:?}",
            warning
        );

        let (effective_noop, warning_noop) = resolve_local_worker_max_in_flight(2, Some(4));
        assert_eq!(effective_noop, 2);
        assert!(warning_noop.is_none());
    }

    #[test]
    fn p2e_out_of_order_completion_simulator_replays_fixture_ticks() {
        let fixture = load_p2e_determinism_fixture();
        let mut simulator = OutOfOrderCompletionSimulator::from_fixture(&fixture);

        let tick0 = simulator.poll_tick(0);
        assert_eq!(tick0.len(), 2);
        assert_eq!(tick0[0].schedule_idx, 2);
        assert_eq!(tick0[0].classification, "arrive_2");
        assert_eq!(tick0[1].schedule_idx, 0);
        assert_eq!(tick0[1].classification, "arrive_0");

        let tick1 = simulator.poll_tick(1);
        assert_eq!(tick1.len(), 2);
        assert_eq!(tick1[0].schedule_idx, 3);
        assert_eq!(tick1[0].classification, "arrive_3");
        assert_eq!(tick1[1].schedule_idx, 1);
        assert_eq!(tick1[1].classification, "arrive_1");

        let tick2 = simulator.poll_tick(2);
        assert!(tick2.is_empty(), "fixture should have no tick=2 arrivals");
    }

    #[test]
    fn p2e_determinism_fixture_commits_contiguously_despite_out_of_order_arrivals() {
        let fixture = load_p2e_determinism_fixture();
        let mut simulator = OutOfOrderCompletionSimulator::from_fixture(&fixture);
        let max_tick = simulator.max_tick();

        let mut pending: BTreeMap<usize, DeterminismCompletion> = BTreeMap::new();
        let mut next_commit_idx = 0usize;
        let mut committed_schedule_idx = Vec::new();
        for tick in 0..=max_tick {
            for completion in simulator.poll_tick(tick) {
                pending.insert(completion.schedule_idx, completion);
            }
            let ready =
                drain_ready_completions_in_schedule_order(&mut pending, &mut next_commit_idx);
            for completion in ready {
                committed_schedule_idx.push(completion.schedule_idx);
            }
        }
        let trailing =
            drain_ready_completions_in_schedule_order(&mut pending, &mut next_commit_idx);
        for completion in trailing {
            committed_schedule_idx.push(completion.schedule_idx);
        }

        assert_eq!(
            committed_schedule_idx, fixture.expected_commit_schedule_idx,
            "commits must be deterministic and contiguous by schedule_idx"
        );
        assert!(
            pending.is_empty(),
            "pending completion buffer should fully drain by final commit"
        );
    }

    fn write_run_control_v2_multi_active_fixture(run_dir: &Path, status: &str, trials: &[&str]) {
        let mut active_trials = serde_json::Map::new();
        for (idx, trial_id) in trials.iter().enumerate() {
            active_trials.insert(
                (*trial_id).to_string(),
                json!({
                    "trial_id": trial_id,
                    "worker_id": format!("worker_{}", idx),
                    "schedule_idx": idx,
                    "variant_id": "base",
                    "started_at": "2026-02-22T00:00:00Z",
                    "control": {
                        "id": BUILTIN_COMMAND_ADAPTER_ID,
                        "version": BUILTIN_COMMAND_ADAPTER_VERSION,
                        "command_path": format!("/tmp/{}.control.json", trial_id),
                        "events_path": format!("/tmp/{}.events.jsonl", trial_id)
                    }
                }),
            );
        }
        let payload = json!({
            "schema_version": "run_control_v2",
            "run_id": "run_1",
            "status": status,
            "active_trials": active_trials,
            "updated_at": "2026-02-22T00:00:00Z"
        });
        let mut store = BackingSqliteStore::open(run_dir).expect("open sqlite store");
        store
            .put_runtime_json(RUNTIME_KEY_RUN_CONTROL, &payload)
            .expect("run control fixture");
    }

    #[test]
    fn p2e_pause_scaffolding_marks_interrupted_when_multi_flight_pause_fails() {
        let (_root, run_dir) = create_run_dir("agentlab_p2e_pause_scaffold", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        write_run_control_v2_multi_active_fixture(&run_dir, "running", &["trial_a", "trial_b"]);

        let err = match pause_run(&run_dir, None, Some("checkpoint"), 1) {
            Ok(_) => {
                panic!("pause fan-out should fail when fixture trial dirs/controls are absent")
            }
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("pause_partial_failure"),
            "unexpected error: {}",
            err
        );
        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "interrupted"
        );
    }

    #[test]
    fn p2e_resume_scaffolding_requires_trial_id_when_multi_flight_is_active() {
        let (_root, run_dir) = create_run_dir("agentlab_p2e_resume_scaffold", "run_1");
        write_run_control_v2_multi_active_fixture(&run_dir, "paused", &["trial_a", "trial_b"]);

        let err = match resume_trial(&run_dir, None, None, &BTreeMap::new(), false) {
            Ok(_) => {
                panic!("resume without trial_id should fail when multiple active trials exist")
            }
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("resume_multiple_active_trials"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p3a_deterministic_committer_buffers_out_of_order_and_dedupes_commits() {
        let (_root, run_dir) = create_run_dir("agentlab_p3a_committer", "run_1");
        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 3,
            next_schedule_index: 0,
            next_trial_index: 2,
            schedule: vec![
                TrialSlot {
                    variant_idx: 0,
                    task_idx: 0,
                    repl_idx: 0,
                },
                TrialSlot {
                    variant_idx: 0,
                    task_idx: 1,
                    repl_idx: 0,
                },
                TrialSlot {
                    variant_idx: 0,
                    task_idx: 2,
                    repl_idx: 0,
                },
            ],
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let mut run_sink = SqliteRunJournal::new(&run_dir).expect("sink");
        let mut committer = DeterministicCommitter::from_progress(&schedule_progress, &[]);
        let policy_config = PolicyConfig::default();
        let evidence_records_path = run_dir.join("runtime").join("p3a_evidence.jsonl");
        let chain_state_path = run_dir.join("runtime").join("p3a_chain_state.jsonl");
        let benchmark_conclusions_path = run_dir.join("runtime").join("p3a_conclusions.jsonl");
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();

        let inserted = committer
            .enqueue_trial(
                1,
                TrialExecutionResult::minimal("trial_2".to_string(), "completed", Some(0)),
            )
            .expect("enqueue idx=1");
        assert!(inserted, "first enqueue should be accepted");
        assert_eq!(
            committer
                .drain_ready(
                    &run_dir,
                    &policy_config,
                    &evidence_records_path,
                    &chain_state_path,
                    &benchmark_conclusions_path,
                    &mut schedule_progress,
                    2,
                    &mut pruned_variants,
                    &mut consecutive_failures,
                    &mut run_sink
                )
                .expect("drain"),
            0,
            "idx=1 cannot commit until idx=0 arrives"
        );

        committer
            .enqueue_trial(
                0,
                TrialExecutionResult::minimal("trial_1".to_string(), "completed", Some(0)),
            )
            .expect("enqueue idx=0");
        assert_eq!(
            committer
                .drain_ready(
                    &run_dir,
                    &policy_config,
                    &evidence_records_path,
                    &chain_state_path,
                    &benchmark_conclusions_path,
                    &mut schedule_progress,
                    2,
                    &mut pruned_variants,
                    &mut consecutive_failures,
                    &mut run_sink
                )
                .expect("drain"),
            2,
            "contiguous commit should drain idx=0 and idx=1"
        );
        assert_eq!(
            schedule_progress
                .completed_slots
                .iter()
                .map(|slot| slot.schedule_index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        let duplicate_committed = committer
            .enqueue_trial(
                1,
                TrialExecutionResult::minimal("trial_2".to_string(), "completed", Some(0)),
            )
            .expect("enqueue duplicate committed");
        assert!(
            !duplicate_committed,
            "duplicate completion for committed slot must be idempotently dropped"
        );
    }

    #[test]
    fn p3b_benchmark_preflight_stages_frozen_input_and_records_task_image() {
        let root = TempDirGuard::new("agentlab_p3b_preflight");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");
        let trial_input_path = trial_dir.join("trial_input.json");
        atomic_write_json_pretty(
            &trial_input_path,
            &json!({
                "schema_version": "agent_task_v1",
                "ids": { "trial_id": "trial_1" }
            }),
        )
        .expect("trial input");

        let benchmark = BenchmarkConfig {
            policy: BenchmarkPolicyConfig::default(),
            grader: Some(BenchmarkGraderConfig::in_task_image(vec![
                "echo".to_string(),
                "ok".to_string(),
            ])),
            adapter: None,
        };
        stage_benchmark_trial_preflight(
            &benchmark,
            &trial_dir,
            "run_1",
            "trial_1",
            4,
            "candidate",
            &json!({
                "id": "task_9"
            }),
            Some("ghcr.io/acme/task:20260222"),
            &trial_input_path,
        )
        .expect("preflight");

        let preflight =
            load_json_file(&trial_dir.join("benchmark_preflight.json")).expect("preflight json");
        assert_eq!(
            preflight
                .pointer("/environment_image")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "ghcr.io/acme/task:20260222"
        );
        assert_eq!(
            preflight
                .pointer("/grading/enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            true
        );
        assert!(
            trial_dir
                .join("artifacts")
                .join("benchmark_frozen_agent_input")
                .join("trial_input.json")
                .exists(),
            "frozen trial_input must be staged for grading/replay"
        );
    }

    #[test]
    fn p3b_benchmark_preflight_rejects_grading_opt_out_for_benchmarks() {
        let root = TempDirGuard::new("agentlab_p3b_preflight_grading_gate");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");
        let trial_input_path = trial_dir.join("trial_input.json");
        atomic_write_json_pretty(
            &trial_input_path,
            &json!({
                "schema_version": "agent_task_v1",
                "ids": { "trial_id": "trial_1" }
            }),
        )
        .expect("trial input");

        let benchmark = BenchmarkConfig {
            policy: BenchmarkPolicyConfig::default(),
            grader: Some(BenchmarkGraderConfig::in_task_image(vec![
                "echo".to_string(),
                "ok".to_string(),
            ])),
            adapter: None,
        };
        let err = stage_benchmark_trial_preflight(
            &benchmark,
            &trial_dir,
            "run_1",
            "trial_1",
            4,
            "candidate",
            &json!({
                "id": "task_9",
                "grading": { "enabled": false }
            }),
            Some("ghcr.io/acme/task:20260222"),
            &trial_input_path,
        )
        .expect_err("benchmark grading opt-out should be rejected");
        assert!(
            err.to_string().contains("grading.enabled=false"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p3c_run_control_v2_writer_supports_multi_flight_active_trials() {
        let (_root, run_dir) = create_run_dir("agentlab_p3c_run_control", "run_1");
        let active_trials = vec![
            RunControlActiveTrial {
                trial_id: "trial_1".to_string(),
                worker_id: "worker_a".to_string(),
                schedule_idx: Some(1),
                variant_id: Some("base".to_string()),
                started_at: Some("2026-02-22T00:00:00Z".to_string()),
                control: None,
            },
            RunControlActiveTrial {
                trial_id: "trial_2".to_string(),
                worker_id: "worker_b".to_string(),
                schedule_idx: Some(2),
                variant_id: Some("candidate".to_string()),
                started_at: Some("2026-02-22T00:00:01Z".to_string()),
                control: None,
            },
        ];
        write_run_control_v2(&run_dir, "run_1", "running", &active_trials, None)
            .expect("write run control v2");
        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/active_trials/trial_1/schedule_idx")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            1
        );
        assert_eq!(
            run_control
                .pointer("/active_trials/trial_2/variant_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "candidate"
        );
        assert!(
            run_control.pointer("/active_trial_id").is_none(),
            "legacy active_trial_id mirror field should be removed"
        );
        assert!(
            run_control.pointer("/active_adapter").is_none(),
            "legacy active_adapter mirror field should be removed"
        );
    }

    #[test]
    fn p4_cutover_uses_parallel_engine_path_for_isolate_policy() {
        let (_root, run_dir) = create_run_dir("agentlab_p4_parallel_path", "run_1");
        write_run_control_v2(&run_dir, "run_1", "paused", &[], None).expect("run control");
        let trials_dir = run_dir.join("trials");
        let evidence_dir = run_dir.join("evidence");
        ensure_dir(&trials_dir).expect("trials dir");
        ensure_dir(&evidence_dir).expect("evidence dir");
        let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
        let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&task_chain_states_path, "").expect("chain rows");

        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 0,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: Vec::new(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let mut trial_index = 0_usize;
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut run_sink = SqliteRunJournal::new(&run_dir).expect("sink");
        execute_schedule_engine(
            ScheduleEngineMode::ContinueRun,
            &run_dir,
            "run_1",
            "agent_runtime",
            &run_dir,
            &run_dir.join("dataset.jsonl"),
            &[],
            &[],
            &[],
            &PolicyConfig::default(),
            &BenchmarkConfig::default(),
            &[],
            &RunBehavior::default(),
            MaterializationMode::Full,
            &TaskBoundaryPolicy::default(),
            &trials_dir,
            &evidence_dir,
            &evidence_records_path,
            &task_chain_states_path,
            &mut schedule_progress,
            &mut trial_index,
            &mut consecutive_failures,
            &mut pruned_variants,
            &[],
            "base",
            &mut run_sink,
            2,
        )
        .expect("parallel engine should no-op cleanly");

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control
                .pointer("/status")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "running"
        );
        let active_trials = run_control
            .pointer("/active_trials")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        assert!(
            active_trials.is_empty(),
            "parallel engine should end with no active trials"
        );
    }

    #[test]
    fn p5a_recovered_active_trials_commit_as_worker_lost_deterministically() {
        let (_root, run_dir) = create_run_dir("agentlab_p5a_worker_lost", "run_1");
        let trials_dir = run_dir.join("trials");
        let evidence_dir = run_dir.join("evidence");
        ensure_dir(&trials_dir).expect("trials dir");
        ensure_dir(&evidence_dir).expect("evidence dir");
        let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
        let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&task_chain_states_path, "").expect("chain rows");

        let variants = vec![Variant {
            id: "base".to_string(),
            bindings: json!({}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        }];
        let schedule = vec![TrialSlot {
            variant_idx: 0,
            task_idx: 0,
            repl_idx: 0,
        }];
        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 1,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: schedule.clone(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let recovered_active_trials = vec![RunControlActiveTrial {
            trial_id: "trial_orphan".to_string(),
            worker_id: "worker_dead".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: None,
        }];
        let mut trial_index = 0_usize;
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut run_sink = SqliteRunJournal::new(&run_dir).expect("sink");
        let policy_config = PolicyConfig {
            pruning_max_consecutive_failures: Some(1),
            ..PolicyConfig::default()
        };
        execute_schedule_engine(
            ScheduleEngineMode::ContinueRun,
            &run_dir,
            "run_1",
            "agent_runtime",
            &run_dir,
            &run_dir.join("dataset.jsonl"),
            &variants,
            &[json!({"id":"task_1"})],
            &schedule,
            &policy_config,
            &BenchmarkConfig::default(),
            &[],
            &RunBehavior::default(),
            MaterializationMode::Full,
            &TaskBoundaryPolicy::default(),
            &trials_dir,
            &evidence_dir,
            &evidence_records_path,
            &task_chain_states_path,
            &mut schedule_progress,
            &mut trial_index,
            &mut consecutive_failures,
            &mut pruned_variants,
            &recovered_active_trials,
            "base",
            &mut run_sink,
            1,
        )
        .expect("parallel recovery handling");

        assert_eq!(schedule_progress.next_schedule_index, 1);
        assert_eq!(schedule_progress.completed_slots.len(), 1);
        assert_eq!(schedule_progress.completed_slots[0].schedule_index, 0);
        assert_eq!(
            schedule_progress.completed_slots[0].trial_id,
            "trial_orphan"
        );
        assert_eq!(schedule_progress.completed_slots[0].status, "failed");
        assert_eq!(consecutive_failures.get(&0).copied().unwrap_or(0), 1);
        assert!(pruned_variants.contains(&0));
    }

    #[test]
    fn p7_pause_run_rejects_active_trial_without_runtime_container_state() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_pause_legacy_active_trial", "run_1");
        let _trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        let active_trials = vec![RunControlActiveTrial {
            trial_id: "trial_1".to_string(),
            worker_id: "worker_parallel_1".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: None,
        }];
        write_run_control_v2(&run_dir, "run_1", "running", &active_trials, None).expect("control");

        let err =
            pause_run(&run_dir, None, Some("worker_pause"), 2).expect_err("pause should fail");
        assert!(
            err.to_string().contains("pause_missing_runtime_container"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p7_pause_run_uses_persisted_runtime_container_when_adapter_control_missing() {
        if !docker_runtime_available() {
            return;
        }
        ensure_docker_test_image("python:3.11-slim");

        let (_root, run_dir) = create_run_dir("agentlab_p7_pause_runtime_state", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);

        let docker = crate::backend::docker::DockerRuntime::connect().expect("docker runtime");
        let handle = docker
            .create_container(&crate::backend::docker::ContainerSpec::idle(
                "python:3.11-slim",
            ))
            .expect("create idle container");
        docker
            .start_container(&handle)
            .expect("start idle container");

        trial::state::write_trial_attempt_state(
            &trial_dir,
            &runtime_trial_attempt_state_with_task_container(
                TrialPhase::AgentRunning,
                &handle.container_id,
            ),
        )
        .expect("write runtime state");

        let active_trials = vec![RunControlActiveTrial {
            trial_id: "trial_1".to_string(),
            worker_id: "worker_parallel_1".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: None,
        }];
        write_run_control_v2(&run_dir, "run_1", "running", &active_trials, None).expect("control");

        let paused =
            pause_run(&run_dir, None, Some("docker_pause"), 2).expect("pause should succeed");
        assert_eq!(paused.trial_id, "trial_1");
        assert!(paused.checkpoint_acked);
        assert!(paused.stop_acked);

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control.pointer("/status").and_then(Value::as_str),
            Some("paused")
        );

        let trial_state = load_json_file(&trial_dir.join("trial_state.json")).expect("trial state");
        assert_eq!(
            trial_state.pointer("/status").and_then(Value::as_str),
            Some("paused")
        );

        let runtime_state =
            trial::state::load_trial_attempt_state(&trial_dir).expect("runtime state");
        assert_eq!(runtime_state.state.phase, TrialPhase::Paused);

        let inspected = docker
            .inspect_container(&handle)
            .expect("inspect paused container");
        assert_eq!(inspected.status.as_deref(), Some("paused"));

        let _ = docker.remove_container(&handle, true);
    }

    #[test]
    fn p7_kill_run_uses_persisted_runtime_container_when_adapter_control_missing() {
        if !docker_runtime_available() {
            return;
        }
        ensure_docker_test_image("python:3.11-slim");

        let (_root, run_dir) = create_run_dir("agentlab_p7_kill_runtime_state", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);

        let docker = crate::backend::docker::DockerRuntime::connect().expect("docker runtime");
        let handle = docker
            .create_container(&crate::backend::docker::ContainerSpec::idle(
                "python:3.11-slim",
            ))
            .expect("create idle container");
        docker
            .start_container(&handle)
            .expect("start idle container");

        trial::state::write_trial_attempt_state(
            &trial_dir,
            &runtime_trial_attempt_state_with_task_container(
                TrialPhase::AgentRunning,
                &handle.container_id,
            ),
        )
        .expect("write runtime state");

        let active_trials = vec![RunControlActiveTrial {
            trial_id: "trial_1".to_string(),
            worker_id: "worker_parallel_1".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: None,
        }];
        write_run_control_v2(&run_dir, "run_1", "running", &active_trials, None).expect("control");

        let killed = kill_run(&run_dir).expect("kill should succeed");
        assert_eq!(killed.killed_trials, vec!["trial_1".to_string()]);

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control.pointer("/status").and_then(Value::as_str),
            Some("killed")
        );

        let trial_state = load_json_file(&trial_dir.join("trial_state.json")).expect("trial state");
        assert_eq!(
            trial_state.pointer("/status").and_then(Value::as_str),
            Some("killed")
        );

        let runtime_state =
            trial::state::load_trial_attempt_state(&trial_dir).expect("runtime state");
        assert_eq!(runtime_state.state.phase, TrialPhase::Killed);

        let inspect_err = docker
            .inspect_container(&handle)
            .expect_err("killed container should be removed");
        assert!(
            inspect_err.to_string().contains("not found")
                || inspect_err.to_string().contains("404"),
            "unexpected inspect error: {}",
            inspect_err
        );
    }

    #[test]
    fn p7_kill_run_does_not_fallback_to_adapter_when_runtime_state_lacks_container_ids() {
        let (_root, run_dir) =
            create_run_dir("agentlab_p7_kill_runtime_missing_container", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);

        trial::state::write_trial_attempt_state(
            &trial_dir,
            &runtime_trial_attempt_state_fixture(TrialPhase::AgentRunning),
        )
        .expect("write runtime state");

        let active_trials = vec![RunControlActiveTrial {
            trial_id: "trial_1".to_string(),
            worker_id: "worker_parallel_1".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: None,
        }];
        write_run_control_v2(&run_dir, "run_1", "running", &active_trials, None).expect("control");

        let err = kill_run(&run_dir).expect_err("kill should fail");
        assert!(
            err.to_string().contains("kill_missing_runtime_container"),
            "unexpected error: {}",
            err
        );

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control.pointer("/status").and_then(Value::as_str),
            Some("interrupted")
        );
        let active = run_control
            .pointer("/active_trials")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        assert_eq!(active.len(), 1);
        assert!(active.contains_key("trial_1"));

        let trial_state = load_json_file(&trial_dir.join("trial_state.json")).expect("trial state");
        assert_eq!(
            trial_state.pointer("/status").and_then(Value::as_str),
            Some("running")
        );
    }

    #[test]
    fn p7_kill_run_partial_runtime_failure_sets_interrupted_and_keeps_active_trial() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_kill_partial_runtime_failure", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);

        let active_trials = vec![RunControlActiveTrial {
            trial_id: "trial_1".to_string(),
            worker_id: "worker_parallel_1".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: None,
        }];
        write_run_control_v2(&run_dir, "run_1", "running", &active_trials, None).expect("control");

        let err = kill_run(&run_dir).expect_err("kill should fail");
        assert!(
            err.to_string().contains("kill_partial_failure"),
            "unexpected error: {}",
            err
        );

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control.pointer("/status").and_then(Value::as_str),
            Some("interrupted")
        );
        let active = run_control
            .pointer("/active_trials")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        assert_eq!(active.len(), 1);
        assert!(active.contains_key("trial_1"));

        let trial_state = load_json_file(&trial_dir.join("trial_state.json")).expect("trial state");
        assert_eq!(
            trial_state.pointer("/status").and_then(Value::as_str),
            Some("running")
        );
    }

    #[test]
    fn p7_resume_trial_unpauses_persisted_runtime_container_without_forking() {
        if !docker_runtime_available() {
            return;
        }
        ensure_docker_test_image("python:3.11-slim");

        let (_root, run_dir) = create_run_dir("agentlab_p7_resume_runtime_state", "run_1");
        write_resolved_experiment(&run_dir, "cli_events", true);
        let trial_dir = seed_parent_trial(
            &run_dir,
            "trial_1",
            json!([]),
            "paused",
            Some("docker_pause"),
        );

        let docker = crate::backend::docker::DockerRuntime::connect().expect("docker runtime");
        let handle = docker
            .create_container(&crate::backend::docker::ContainerSpec::idle(
                "python:3.11-slim",
            ))
            .expect("create idle container");
        docker
            .start_container(&handle)
            .expect("start idle container");
        docker
            .pause_container(&handle)
            .expect("pause idle container");

        let mut runtime_state = runtime_trial_attempt_state_with_task_container(
            TrialPhase::Paused,
            &handle.container_id,
        );
        runtime_state.paused_from_phase = Some(TrialPhase::AgentRunning);
        trial::state::write_trial_attempt_state(&trial_dir, &runtime_state)
            .expect("write runtime state");

        let active_trials = vec![RunControlActiveTrial {
            trial_id: "trial_1".to_string(),
            worker_id: "worker_parallel_1".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: None,
        }];
        write_run_control_v2(&run_dir, "run_1", "paused", &active_trials, None).expect("control");

        let resumed = resume_trial(&run_dir, None, None, &BTreeMap::new(), false)
            .expect("resume should succeed");
        assert_eq!(resumed.trial_id, "trial_1");
        assert!(matches!(resumed.mode, ResumeMode::RuntimeUnpause));
        assert!(resumed.selector.is_none());
        assert!(resumed.fork.is_none());

        let run_control = load_json_file(&run_control_path(&run_dir)).expect("run control");
        assert_eq!(
            run_control.pointer("/status").and_then(Value::as_str),
            Some("running")
        );

        let trial_state = load_json_file(&trial_dir.join("trial_state.json")).expect("trial state");
        assert_eq!(
            trial_state.pointer("/status").and_then(Value::as_str),
            Some("running")
        );

        let runtime_state =
            trial::state::load_trial_attempt_state(&trial_dir).expect("runtime state");
        assert_eq!(runtime_state.state.phase, TrialPhase::AgentRunning);
        assert_eq!(runtime_state.state.paused_from_phase, None);

        let inspected = docker
            .inspect_container(&handle)
            .expect("inspect resumed container");
        assert_eq!(inspected.status.as_deref(), Some("running"));

        let _ = docker.remove_container(&handle, true);
    }

    fn p7_trial_result_with_trial_record(schedule_idx: usize) -> TrialExecutionResult {
        let trial_id = format!("trial_{}", schedule_idx + 1);
        let mut result = TrialExecutionResult::minimal(trial_id.clone(), "completed", Some(0));
        result.deferred_trial_records.push(TrialRecord {
            run_id: "run_1".to_string(),
            trial_id,
            schedule_idx,
            slot_commit_id: String::new(),
            attempt: 0,
            row_seq: 0,
            baseline_id: "base".to_string(),
            workload_type: "agent_harness".to_string(),
            variant_id: "base".to_string(),
            task_index: schedule_idx,
            task_id: format!("task_{}", schedule_idx),
            repl_idx: 0,
            outcome: "success".to_string(),
            success: true,
            status_code: "0".to_string(),
            integration_level: "cli_basic".to_string(),
            network_mode_requested: "none".to_string(),
            network_mode_effective: "none".to_string(),
            primary_metric_name: "success".to_string(),
            primary_metric_value: json!(1.0),
            metrics: json!({"success": 1.0, "status_code": "0"}),
            bindings: json!({}),
            hook_events_total: 0,
            has_hook_events: false,
        });
        result
    }

    struct FlushFailRunSink;

    impl RunSink for FlushFailRunSink {
        fn write_run_manifest(&mut self, _run: &RunManifestRecord) -> Result<()> {
            Ok(())
        }

        fn append_trial_record(&mut self, _row: &TrialRecord) -> Result<()> {
            Ok(())
        }

        fn append_metric_rows(&mut self, _rows: &[MetricRow]) -> Result<()> {
            Ok(())
        }

        fn append_event_rows(&mut self, _rows: &[EventRow]) -> Result<()> {
            Ok(())
        }

        fn append_variant_snapshot(&mut self, _rows: &[VariantSnapshotRow]) -> Result<()> {
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Err(anyhow::anyhow!("flush_failed"))
        }
    }

    #[test]
    fn p7_commit_trial_slot_does_not_advance_progress_when_flush_fails() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_commit_flush_fail", "run_1");
        ensure_dir(&run_dir.join("runtime")).expect("runtime dir");
        let evidence_records_path = run_dir.join("runtime").join("p7_evidence.jsonl");
        let chain_state_path = run_dir.join("runtime").join("p7_chain_state.jsonl");
        let benchmark_conclusions_path = run_dir.join("runtime").join("p7_conclusions.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&chain_state_path, "").expect("chain rows");

        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 1,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: vec![TrialSlot {
                variant_idx: 0,
                task_idx: 0,
                repl_idx: 0,
            }],
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: "2026-02-22T00:00:00Z".to_string(),
        };
        write_schedule_progress(&run_dir, &schedule_progress).expect("progress");

        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut slot_attempts: HashMap<usize, usize> = HashMap::new();
        let trial_result =
            TrialExecutionResult::minimal("trial_1".to_string(), "completed", Some(0));
        let mut sink = FlushFailRunSink;
        let err = RunCoordinator::commit_trial_slot(
            &run_dir,
            &PolicyConfig::default(),
            &evidence_records_path,
            &chain_state_path,
            &benchmark_conclusions_path,
            &mut schedule_progress,
            0,
            1,
            &mut pruned_variants,
            &mut consecutive_failures,
            &trial_result,
            &mut sink,
            &mut slot_attempts,
        )
        .expect_err("flush failure should abort slot commit");
        assert!(
            err.to_string().contains("flush_failed"),
            "unexpected error: {}",
            err
        );
        assert_eq!(schedule_progress.next_schedule_index, 0);
        assert!(
            schedule_progress.completed_slots.is_empty(),
            "slot should not be committed when sink flush fails"
        );
        assert!(pruned_variants.is_empty());
        assert!(consecutive_failures.is_empty());

        let persisted = load_schedule_progress(&run_dir).expect("load persisted progress");
        assert_eq!(persisted.next_schedule_index, 0);
        assert!(persisted.completed_slots.is_empty());
    }

    #[test]
    fn p7_commit_trial_slot_persists_trial_conclusion_rows() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_commit_trial_conclusions", "run_1");
        ensure_dir(&run_dir.join("runtime")).expect("runtime dir");
        let evidence_records_path = run_dir.join("runtime").join("p7_evidence.jsonl");
        let chain_state_path = run_dir.join("runtime").join("p7_chain_state.jsonl");
        let benchmark_conclusions_path = run_dir.join("runtime").join("p7_conclusions.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&chain_state_path, "").expect("chain rows");
        fs::write(&benchmark_conclusions_path, "").expect("conclusion rows");

        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v2".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 1,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: vec![TrialSlot {
                variant_idx: 0,
                task_idx: 0,
                repl_idx: 0,
            }],
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: "2026-02-22T00:00:00Z".to_string(),
        };
        write_schedule_progress(&run_dir, &schedule_progress).expect("progress");

        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut slot_attempts: HashMap<usize, usize> = HashMap::new();
        let mut trial_result =
            TrialExecutionResult::minimal("trial_1".to_string(), "completed", Some(0));
        trial_result.deferred_trial_conclusion_records.push(json!({
            "schema_version": "trial_conclusion_v1",
            "payload": { "resolved": 1.0 },
            "reported_outcome": "success",
            "primary_metric": { "name": "resolved", "value": 1.0 },
            "grader": { "name": "test_grader", "strategy": "in_task_image" }
        }));

        let mut run_sink = BufferedRunSink::default();
        RunCoordinator::commit_trial_slot(
            &run_dir,
            &PolicyConfig::default(),
            &evidence_records_path,
            &chain_state_path,
            &benchmark_conclusions_path,
            &mut schedule_progress,
            0,
            1,
            &mut pruned_variants,
            &mut consecutive_failures,
            &trial_result,
            &mut run_sink,
            &mut slot_attempts,
        )
        .expect("commit trial slot");

        let store = BackingSqliteStore::open(&run_dir).expect("open sqlite store");
        assert_eq!(
            store
                .row_count("benchmark_conclusion_rows")
                .expect("conclusion row count"),
            1,
            "expected one persisted trial conclusion row"
        );
        let row = load_sqlite_json_row(&run_dir, "benchmark_conclusion_rows", "run_1");
        assert_eq!(
            row.pointer("/schema_version").and_then(Value::as_str),
            Some("trial_conclusion_v1")
        );
        assert_eq!(
            row.pointer("/schedule_idx").and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(row.pointer("/attempt").and_then(Value::as_u64), Some(1));
        assert_eq!(row.pointer("/row_seq").and_then(Value::as_u64), Some(0));
        assert!(
            row.pointer("/slot_commit_id")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty()),
            "slot_commit_id should be annotated onto persisted conclusion rows"
        );
    }

    #[test]
    fn p7_commit_trial_slot_marks_runtime_state_committed() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_commit_runtime_state", "run_1");
        ensure_dir(&run_dir.join("runtime")).expect("runtime dir");
        let evidence_records_path = run_dir.join("runtime").join("p7_evidence.jsonl");
        let chain_state_path = run_dir.join("runtime").join("p7_chain_state.jsonl");
        let benchmark_conclusions_path = run_dir.join("runtime").join("p7_conclusions.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&chain_state_path, "").expect("chain rows");
        fs::write(&benchmark_conclusions_path, "").expect("conclusion rows");

        let trial_dir = run_dir.join("trials").join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");
        trial::state::write_trial_attempt_state(
            &trial_dir,
            &runtime_trial_attempt_state_fixture(TrialPhase::CommitPending),
        )
        .expect("write runtime state");

        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v2".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 1,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: vec![TrialSlot {
                variant_idx: 0,
                task_idx: 0,
                repl_idx: 0,
            }],
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: "2026-02-22T00:00:00Z".to_string(),
        };
        write_schedule_progress(&run_dir, &schedule_progress).expect("progress");

        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut slot_attempts: HashMap<usize, usize> = HashMap::new();
        let trial_result =
            TrialExecutionResult::minimal("trial_1".to_string(), "completed", Some(0));
        let mut run_sink = BufferedRunSink::default();
        RunCoordinator::commit_trial_slot(
            &run_dir,
            &PolicyConfig::default(),
            &evidence_records_path,
            &chain_state_path,
            &benchmark_conclusions_path,
            &mut schedule_progress,
            0,
            1,
            &mut pruned_variants,
            &mut consecutive_failures,
            &trial_result,
            &mut run_sink,
            &mut slot_attempts,
        )
        .expect("commit trial slot");

        let persisted = trial::state::load_trial_attempt_state(&trial_dir).expect("load state");
        assert_eq!(persisted.state.phase, TrialPhase::Committed);
    }

    fn p7_commit_trial_rows_for_arrival_order(
        prefix: &str,
        arrival_order: &[usize],
    ) -> (Vec<String>, Vec<usize>) {
        let (_root, run_dir) = create_run_dir(prefix, "run_1");
        let slot_count = arrival_order.len();
        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: slot_count,
            next_schedule_index: 0,
            next_trial_index: slot_count,
            schedule: (0..slot_count)
                .map(|idx| TrialSlot {
                    variant_idx: 0,
                    task_idx: idx,
                    repl_idx: 0,
                })
                .collect(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let policy_config = PolicyConfig::default();
        let evidence_records_path = run_dir.join("runtime").join("p7_evidence.jsonl");
        let chain_state_path = run_dir.join("runtime").join("p7_chain_state.jsonl");
        let benchmark_conclusions_path = run_dir.join("runtime").join("p7_conclusions.jsonl");
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut run_sink = BufferedRunSink::default();
        let mut committer = DeterministicCommitter::from_progress(&schedule_progress, &[]);

        for schedule_idx in arrival_order {
            let inserted = committer
                .enqueue_trial(
                    *schedule_idx,
                    p7_trial_result_with_trial_record(*schedule_idx),
                )
                .expect("enqueue trial");
            assert!(inserted, "arrival order should not contain duplicates");
            let _ = committer
                .drain_ready(
                    &run_dir,
                    &policy_config,
                    &evidence_records_path,
                    &chain_state_path,
                    &benchmark_conclusions_path,
                    &mut schedule_progress,
                    slot_count,
                    &mut pruned_variants,
                    &mut consecutive_failures,
                    &mut run_sink,
                )
                .expect("drain ready");
        }
        let _ = committer
            .drain_ready(
                &run_dir,
                &policy_config,
                &evidence_records_path,
                &chain_state_path,
                &benchmark_conclusions_path,
                &mut schedule_progress,
                slot_count,
                &mut pruned_variants,
                &mut consecutive_failures,
                &mut run_sink,
            )
            .expect("final drain");

        let committed_trial_ids = run_sink
            .trial_records
            .iter()
            .map(|row| row.trial_id.clone())
            .collect::<Vec<_>>();
        let committed_schedule_idx = schedule_progress
            .completed_slots
            .iter()
            .map(|slot| slot.schedule_index)
            .collect::<Vec<_>>();
        (committed_trial_ids, committed_schedule_idx)
    }

    #[test]
    fn p7_parallel_and_serial_equivalent_final_aggregates_ordering_normalized() {
        let serial_arrivals = [0usize, 1, 2, 3];
        let parallel_arrivals = [2usize, 0, 3, 1];

        let (serial_trial_ids, serial_commit_idx) =
            p7_commit_trial_rows_for_arrival_order("agentlab_p7_serial_parity", &serial_arrivals);
        let (parallel_trial_ids, parallel_commit_idx) = p7_commit_trial_rows_for_arrival_order(
            "agentlab_p7_parallel_parity",
            &parallel_arrivals,
        );

        assert_eq!(serial_commit_idx, vec![0, 1, 2, 3]);
        assert_eq!(parallel_commit_idx, serial_commit_idx);
        assert_eq!(
            parallel_trial_ids, serial_trial_ids,
            "ordering-normalized final aggregates should match serial-equivalent output"
        );
    }

    #[test]
    fn p7_persisted_pending_completion_survives_restart_and_drains_after_head_slot() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_pending_recovery", "run_1");
        let slot_count = 2usize;
        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: slot_count,
            next_schedule_index: 0,
            next_trial_index: slot_count,
            schedule: (0..slot_count)
                .map(|idx| TrialSlot {
                    variant_idx: 0,
                    task_idx: idx,
                    repl_idx: 0,
                })
                .collect(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let policy_config = PolicyConfig::default();
        let evidence_records_path = run_dir.join("runtime").join("p7_pending_evidence.jsonl");
        let chain_state_path = run_dir.join("runtime").join("p7_pending_chain_state.jsonl");
        let benchmark_conclusions_path =
            run_dir.join("runtime").join("p7_pending_conclusions.jsonl");
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut run_sink = BufferedRunSink::default();

        // First process lifetime: slot 1 finishes before slot 0, so it is buffered pending.
        let mut committer = DeterministicCommitter::from_progress(&schedule_progress, &[]);
        committer
            .enqueue_trial(1, p7_trial_result_with_trial_record(1))
            .expect("enqueue slot 1 result");
        let committed = committer
            .drain_ready(
                &run_dir,
                &policy_config,
                &evidence_records_path,
                &chain_state_path,
                &benchmark_conclusions_path,
                &mut schedule_progress,
                slot_count,
                &mut pruned_variants,
                &mut consecutive_failures,
                &mut run_sink,
            )
            .expect("drain should buffer slot 1");
        assert_eq!(committed, 0, "slot 1 cannot commit before slot 0");
        assert_eq!(schedule_progress.next_schedule_index, 0);
        let pending_records = committer.pending_trial_completion_records();
        persist_pending_trial_completions(&run_dir, &pending_records).expect("persist pending");

        // Simulate restart: reload persisted pending completion, then recover slot 0 as worker_lost.
        let journal_records = load_slot_commit_records(&run_dir).expect("load journal");
        let mut restarted =
            DeterministicCommitter::from_progress(&schedule_progress, &journal_records);
        let persisted = load_pending_trial_completion_records(&run_dir).expect("load pending");
        assert!(
            persisted.contains_key(&1),
            "slot 1 pending completion should persist across restart"
        );
        for (schedule_idx, result) in persisted {
            restarted
                .enqueue_trial(schedule_idx, result)
                .expect("re-enqueue persisted completion");
        }
        restarted
            .enqueue_trial(
                0,
                TrialExecutionResult::worker_lost(
                    "trial_1".to_string(),
                    Some(0),
                    Some("worker_lost".to_string()),
                ),
            )
            .expect("enqueue recovered slot 0");

        let committed_after_restart = restarted
            .drain_ready(
                &run_dir,
                &policy_config,
                &evidence_records_path,
                &chain_state_path,
                &benchmark_conclusions_path,
                &mut schedule_progress,
                slot_count,
                &mut pruned_variants,
                &mut consecutive_failures,
                &mut run_sink,
            )
            .expect("drain after restart");
        assert_eq!(
            committed_after_restart, 2,
            "slot 0 and persisted slot 1 should both commit"
        );
        assert_eq!(schedule_progress.next_schedule_index, 2);
        assert_eq!(
            schedule_progress
                .completed_slots
                .iter()
                .map(|slot| slot.schedule_index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(
            run_sink
                .trial_records
                .iter()
                .any(|row| row.schedule_idx == 1 && row.trial_id == "trial_2"),
            "persisted slot 1 completion should appear in committed facts after restart"
        );
    }

    #[test]
    fn p7_release_gate_rejects_non_isolate_state_policy() {
        let (_root, run_dir) = create_run_dir("agentlab_p7_release_gate", "run_1");
        write_run_control_v2(&run_dir, "run_1", "paused", &[], None).expect("run control");
        let trials_dir = run_dir.join("trials");
        let evidence_dir = run_dir.join("evidence");
        ensure_dir(&trials_dir).expect("trials dir");
        ensure_dir(&evidence_dir).expect("evidence dir");
        let evidence_records_path = evidence_dir.join("evidence_records.jsonl");
        let task_chain_states_path = evidence_dir.join("task_chain_states.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&task_chain_states_path, "").expect("chain rows");

        let mut schedule_progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: "run_1".to_string(),
            total_slots: 0,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: Vec::new(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let mut trial_index = 0_usize;
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut run_sink = SqliteRunJournal::new(&run_dir).expect("sink");
        let policy_config = PolicyConfig {
            state: StatePolicy::PersistPerTask,
            ..PolicyConfig::default()
        };
        let err = execute_schedule_engine(
            ScheduleEngineMode::ContinueRun,
            &run_dir,
            "run_1",
            "agent_runtime",
            &run_dir,
            &run_dir.join("dataset.jsonl"),
            &[],
            &[],
            &[],
            &policy_config,
            &BenchmarkConfig::default(),
            &[],
            &RunBehavior::default(),
            MaterializationMode::Full,
            &TaskBoundaryPolicy::default(),
            &trials_dir,
            &evidence_dir,
            &evidence_records_path,
            &task_chain_states_path,
            &mut schedule_progress,
            &mut trial_index,
            &mut consecutive_failures,
            &mut pruned_variants,
            &[],
            "base",
            &mut run_sink,
            4,
        )
        .expect_err("non-isolate policy should be rejected by hard cutover release gate");
        assert!(
            err.to_string().contains("supports only isolate_per_trial"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn parse_task_boundary_extracts_runtime_fields() {
        let task = json!({
            "schema_version": "task_row_v1",
            "id": "task_1",
            "image": "python:3.11-slim",
            "workdir": "/workspace/task",
            "time_limit_ms": 120_000,
            "task": {
                "id": "task_1",
                "prompt": "solve this"
            },
            "materialization": {
                "kind": "task_image"
            }
        });

        let parsed = parse_task_boundary_from_packaged_task(&task).expect("parse boundary");
        assert_eq!(
            parsed
                .task_payload
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "task_1"
        );
        assert_eq!(parsed.task_image, "python:3.11-slim");
        assert_eq!(parsed.task_workdir, "/workspace/task");
        assert!(parsed.workspace.overlays.is_empty());
        assert!(parsed.workspace.aux_mounts.is_empty());
        assert_eq!(parsed.time_limit_ms, Some(120_000));
    }

    #[test]
    fn parse_task_boundary_parses_workspace_base_dataset_pack() {
        let task = json!({
            "schema_version": "task_row_v1",
            "id": "task_1",
            "image": "swebench/sweb.eval.x86_64.astropy__astropy-12907:latest",
            "workdir": "/testbed",
            "task": {
                "id": "task_1",
                "prompt": "solve this"
            },
            "materialization": {
                "kind": "base_image_bundle",
                "task_bundle_ref": "tasks/task_1"
            }
        });

        let parsed = parse_task_boundary_from_packaged_task(&task).expect("workspace base");
        assert_eq!(
            parsed.materialization.kind,
            TaskMaterializationKind::BaseImageBundle
        );
        assert_eq!(parsed.task_workdir, "/testbed");
        assert_eq!(
            parsed.materialization.task_bundle_ref.as_deref(),
            Some("tasks/task_1")
        );
    }

    #[test]
    fn parse_task_boundary_rejects_unsupported_keys() {
        let task = json!({
            "schema_version": "task_row_v1",
            "id": "task_1",
            "image": "python:3.11-slim",
            "workdir": "/workspace/task",
            "task": { "id": "task_1" },
            "materialization": {
                "kind": "task_image"
            },
            "benchmark_kind": "custom_magic"
        });
        let err = parse_task_boundary_from_packaged_task(&task).expect_err("should fail");
        assert!(
            err.to_string().contains("unknown field")
                || err.to_string().contains("unsupported key"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn prepare_task_environment_materializes_base_image_bundle_into_workspace() {
        let root = TempDirGuard::new("agentlab_base_image_bundle_prepare");
        let bundle_dir = root
            .path
            .join("tasks")
            .join("task_bundles")
            .join("task_1_bundle");
        ensure_dir(&bundle_dir.join("src")).expect("bundle dir");
        fs::write(bundle_dir.join("src/main.py"), "print('ok')\n").expect("bundle file");

        let task = json!({
            "schema_version": "task_row_v1",
            "id": "task_1",
            "image": "python:3.11-slim",
            "workdir": "/workspace/task",
            "materialization": {
                "kind": "base_image_bundle",
                "task_bundle_ref": "tasks/task_bundles/task_1_bundle"
            },
            "task": {
                "id": "task_1",
                "prompt": "solve it"
            }
        });
        let task_boundary = parse_task_boundary_from_packaged_task(&task).expect("task boundary");
        let variant = preflight_test_variant();
        let runtime = legacy_contract_runtime_fixture();
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");

        let prepared = prepare_task_environment(
            &root.path,
            &trial_dir,
            "run_1",
            "trial_1",
            &json!({ "policy": { "timeout_ms": 30000 } }),
            &variant,
            0,
            0,
            &task_boundary,
            &runtime,
        )
        .expect("prepare task environment");

        assert_eq!(
            fs::read_to_string(prepared.trial_paths.workspace.join("src/main.py"))
                .expect("materialized bundle file"),
            "print('ok')\n"
        );
        assert!(
            prepared.dynamic_mounts.is_empty(),
            "base_image_bundle should not produce legacy aux mounts"
        );
        let task_sandbox_plan = prepared
            .manifest
            .task_sandbox_plan
            .as_ref()
            .expect("task sandbox plan");
        assert_eq!(task_sandbox_plan.image, "python:3.11-slim");
        assert_eq!(task_sandbox_plan.workdir, "/workspace/task");
        assert_eq!(
            task_sandbox_plan.materialization.kind,
            TaskMaterializationKind::BaseImageBundle
        );
        assert_eq!(task_sandbox_plan.io_mounts.in_dir, AGENTLAB_CONTRACT_IN_DIR);
        assert_eq!(
            task_sandbox_plan.io_mounts.out_dir,
            AGENTLAB_CONTRACT_OUT_DIR
        );
        assert_eq!(
            task_sandbox_plan.artifact_mount.container_artifact_dir,
            "/opt/agent"
        );
        assert_eq!(task_sandbox_plan.time_limit_ms, 30_000);
    }

    #[test]
    fn build_agent_task_uses_run_id_and_limits_without_embedding_setup_manifest() {
        let root = TempDirGuard::new("agentlab_task_boundary_trial_input");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp");
        fs::write(exp_dir.join("harness.sh"), "#!/bin/sh\n").expect("harness");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial");
        let paths = TrialPaths::new(&trial_dir, &exp_dir).expect("paths");
        paths.prepare(true).expect("prepare");

        let json_value = json!({
            "design": { "sanitization_profile": "hermetic_functional" },
            "runtime": {
                "agent": {
                    "command": ["sh", "-lc", "echo ok"],
                    "bundle": ".lab/agents/rex-current.tar.gz"
                },
                "sandbox": {
                    "executor": "docker",
                    "image_source": "global",
                    "image": "img",
                    "profile": "workspace_write",
                    "network": "none"
                },
                "dependencies": { "services": [] },
                "policy": {
                    "timeout_ms": 600000
                }
            }
        });
        let variant = Variant {
            id: "baseline".to_string(),
            bindings: json!({ "model": "demo" }),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let task_boundary = runtime_task_boundary(
            json!({ "id": "task_1", "prompt": "x" }),
            "python:3.11-slim",
            AGENTLAB_CONTRACT_WORKSPACE_DIR,
            Some(90_000),
        );

        let input = build_trial_input(
            &json_value,
            "run_actual_1",
            "trial_1",
            &variant,
            0,
            0,
            &task_boundary,
        );

        assert_eq!(
            input
                .pointer("/ids/run_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "run_actual_1"
        );
        assert_eq!(
            input
                .pointer("/runtime/time_limit_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            90000
        );
        assert!(
            input.pointer("/bindings").is_none(),
            "agent-facing trial_input must not carry variant bindings"
        );
        assert!(
            input.pointer("/ext/task_spec").is_none(),
            "agent-facing trial_input must not carry a task setup manifest"
        );
    }

    #[test]
    fn normalize_task_prompt_aliases_deduplicates_identical_fields() {
        let task = json!({
            "id": "swebench_astropy_astropy_12907",
            "input": { "prompt": "same prompt", "repo": "astropy/astropy" },
            "prompt": "same prompt",
            "swebench": {
                "input": { "prompt": "same prompt", "base_commit": "abc123" }
            }
        });

        let normalized = normalize_task_prompt_aliases(&task);
        assert_eq!(
            normalized
                .pointer("/input/prompt")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "same prompt"
        );
        assert!(
            normalized.pointer("/prompt").is_none(),
            "top-level duplicated prompt should be removed"
        );
        assert!(
            normalized.pointer("/swebench/input/prompt").is_none(),
            "nested duplicated prompt should be removed"
        );
        assert_eq!(
            normalized
                .pointer("/swebench/input/base_commit")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "abc123"
        );
    }

    #[test]
    fn normalize_task_prompt_aliases_preserves_distinct_prompt_fields() {
        let task = json!({
            "id": "task_1",
            "input": { "prompt": "canonical prompt" },
            "prompt": "different top-level prompt",
            "swebench": {
                "input": { "prompt": "different nested prompt" }
            }
        });

        let normalized = normalize_task_prompt_aliases(&task);
        assert_eq!(
            normalized
                .pointer("/input/prompt")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "canonical prompt"
        );
        assert_eq!(
            normalized
                .pointer("/prompt")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "different top-level prompt"
        );
        assert_eq!(
            normalized
                .pointer("/swebench/input/prompt")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "different nested prompt"
        );
    }

    // -----------------------------------------------------------------------
    // build_trial_schedule tests
    // -----------------------------------------------------------------------

    #[test]
    fn schedule_variant_sequential_orders_variant_then_task_then_repl() {
        let slots = build_trial_schedule(2, 3, 2, SchedulingPolicy::VariantSequential, 1);
        assert_eq!(slots.len(), 12); // 2 variants * 3 tasks * 2 repls

        // First 6 slots should be variant 0
        for slot in &slots[0..6] {
            assert_eq!(slot.variant_idx, 0);
        }
        // Last 6 slots should be variant 1
        for slot in &slots[6..12] {
            assert_eq!(slot.variant_idx, 1);
        }

        // Within variant 0: task 0 repl 0, task 0 repl 1, task 1 repl 0, ...
        assert_eq!(slots[0].task_idx, 0);
        assert_eq!(slots[0].repl_idx, 0);
        assert_eq!(slots[1].task_idx, 0);
        assert_eq!(slots[1].repl_idx, 1);
        assert_eq!(slots[2].task_idx, 1);
        assert_eq!(slots[2].repl_idx, 0);
    }

    #[test]
    fn schedule_paired_interleaved_orders_task_then_variant_then_repl() {
        let slots = build_trial_schedule(2, 3, 2, SchedulingPolicy::PairedInterleaved, 1);
        assert_eq!(slots.len(), 12);

        // First 4 slots should all be task 0 (2 variants * 2 repls)
        for slot in &slots[0..4] {
            assert_eq!(slot.task_idx, 0);
        }
        // Within task 0: variant 0 repl 0, variant 0 repl 1, variant 1 repl 0, variant 1 repl 1
        assert_eq!(slots[0].variant_idx, 0);
        assert_eq!(slots[0].repl_idx, 0);
        assert_eq!(slots[1].variant_idx, 0);
        assert_eq!(slots[1].repl_idx, 1);
        assert_eq!(slots[2].variant_idx, 1);
        assert_eq!(slots[2].repl_idx, 0);
        assert_eq!(slots[3].variant_idx, 1);
        assert_eq!(slots[3].repl_idx, 1);
    }

    #[test]
    fn schedule_paired_interleaved_pairs_variants_on_same_task() {
        // Key A/B test property: for each task, all variants run before moving to next task
        let slots = build_trial_schedule(3, 4, 1, SchedulingPolicy::PairedInterleaved, 1);
        assert_eq!(slots.len(), 12); // 3 variants * 4 tasks * 1 repl

        for task_idx in 0..4 {
            let task_slots: Vec<_> = slots.iter().filter(|s| s.task_idx == task_idx).collect();
            assert_eq!(task_slots.len(), 3); // one per variant
            let variant_ids: Vec<_> = task_slots.iter().map(|s| s.variant_idx).collect();
            assert_eq!(variant_ids, vec![0, 1, 2]);
        }
    }

    #[test]
    fn schedule_randomized_contains_all_slots() {
        let slots = build_trial_schedule(2, 3, 2, SchedulingPolicy::Randomized, 42);
        assert_eq!(slots.len(), 12);

        // Every (variant, task, repl) triple should appear exactly once
        let mut seen = HashSet::new();
        for slot in &slots {
            let key = (slot.variant_idx, slot.task_idx, slot.repl_idx);
            assert!(seen.insert(key), "duplicate slot: {:?}", key);
        }
        assert_eq!(seen.len(), 12);
    }

    #[test]
    fn schedule_randomized_is_deterministic_with_same_seed() {
        let a = build_trial_schedule(2, 4, 2, SchedulingPolicy::Randomized, 1337);
        let b = build_trial_schedule(2, 4, 2, SchedulingPolicy::Randomized, 1337);
        for (sa, sb) in a.iter().zip(b.iter()) {
            assert_eq!(sa.variant_idx, sb.variant_idx);
            assert_eq!(sa.task_idx, sb.task_idx);
            assert_eq!(sa.repl_idx, sb.repl_idx);
        }
    }

    #[test]
    fn schedule_randomized_different_seed_produces_different_order() {
        let a = build_trial_schedule(2, 4, 2, SchedulingPolicy::Randomized, 1);
        let b = build_trial_schedule(2, 4, 2, SchedulingPolicy::Randomized, 2);
        // With 16 slots, the probability of identical ordering is negligible
        let same = a.iter().zip(b.iter()).all(|(sa, sb)| {
            sa.variant_idx == sb.variant_idx
                && sa.task_idx == sb.task_idx
                && sa.repl_idx == sb.repl_idx
        });
        assert!(!same, "different seeds should produce different orderings");
    }

    #[test]
    fn schedule_single_variant_single_task_single_repl() {
        for policy in [
            SchedulingPolicy::VariantSequential,
            SchedulingPolicy::PairedInterleaved,
            SchedulingPolicy::Randomized,
        ] {
            let slots = build_trial_schedule(1, 1, 1, policy, 1);
            assert_eq!(slots.len(), 1);
            assert_eq!(slots[0].variant_idx, 0);
            assert_eq!(slots[0].task_idx, 0);
            assert_eq!(slots[0].repl_idx, 0);
        }
    }

    #[test]
    fn schedule_empty_when_zero_tasks() {
        let slots = build_trial_schedule(2, 0, 3, SchedulingPolicy::VariantSequential, 1);
        assert!(slots.is_empty());
    }

    // -----------------------------------------------------------------------
    // should_retry_outcome tests
    // -----------------------------------------------------------------------

    #[test]
    fn retry_with_empty_retry_on_retries_any_failure() {
        // Empty retry_on means retry on any non-success
        assert!(should_retry_outcome("error", "0", &[]));
        assert!(should_retry_outcome("success", "1", &[])); // exit nonzero
        assert!(!should_retry_outcome("success", "0", &[])); // success — no retry
    }

    #[test]
    fn retry_on_error_only_retries_error_outcome() {
        let triggers = vec!["error".to_string()];
        assert!(should_retry_outcome("error", "0", &triggers));
        assert!(should_retry_outcome("error", "1", &triggers));
        assert!(!should_retry_outcome("success", "0", &triggers));
        assert!(!should_retry_outcome("success", "1", &triggers)); // exit nonzero but not "error"
    }

    #[test]
    fn retry_on_failure_retries_nonzero_exit() {
        let triggers = vec!["failure".to_string()];
        assert!(should_retry_outcome("success", "1", &triggers));
        assert!(should_retry_outcome("error", "137", &triggers));
        assert!(!should_retry_outcome("success", "0", &triggers));
        assert!(!should_retry_outcome("error", "0", &triggers)); // error outcome but exit 0
    }

    #[test]
    fn retry_on_timeout_retries_timeout_outcome() {
        let triggers = vec!["timeout".to_string()];
        assert!(should_retry_outcome("timeout", "0", &triggers));
        assert!(should_retry_outcome("timeout", "1", &triggers));
        assert!(!should_retry_outcome("error", "0", &triggers));
        assert!(!should_retry_outcome("success", "0", &triggers));
    }

    #[test]
    fn retry_on_multiple_triggers() {
        let triggers = vec!["error".to_string(), "timeout".to_string()];
        assert!(should_retry_outcome("error", "0", &triggers));
        assert!(should_retry_outcome("timeout", "0", &triggers));
        assert!(!should_retry_outcome("success", "1", &triggers)); // failure not in triggers
    }

    #[test]
    fn benchmark_verdict_maps_to_trial_outcome() {
        assert_eq!(benchmark_verdict_to_trial_outcome("pass"), Some("success"));
        assert_eq!(benchmark_verdict_to_trial_outcome("fail"), Some("failure"));
        assert_eq!(
            benchmark_verdict_to_trial_outcome("missing"),
            Some("missing")
        );
        assert_eq!(benchmark_verdict_to_trial_outcome("error"), Some("error"));
        assert_eq!(benchmark_verdict_to_trial_outcome("unknown"), None);
    }

    #[test]
    fn trial_conclusion_outcome_maps_to_trial_outcome() {
        assert_eq!(
            trial_conclusion_outcome_to_trial_outcome("success"),
            Some("success")
        );
        assert_eq!(
            trial_conclusion_outcome_to_trial_outcome("failure"),
            Some("failure")
        );
        assert_eq!(
            trial_conclusion_outcome_to_trial_outcome("timeout"),
            Some("timeout")
        );
        assert_eq!(
            trial_conclusion_outcome_to_trial_outcome("pass"),
            Some("success")
        );
        assert_eq!(trial_conclusion_outcome_to_trial_outcome("unknown"), None);
    }

    #[test]
    fn benchmark_retry_inputs_ignore_agent_exit_when_mapped_output_is_valid() {
        let (outcome, exit_status) = benchmark_retry_inputs(
            true,
            &json!({ "outcome": "error" }),
            Some(&json!({
                "schema_version": "trial_conclusion_v1",
                "reported_outcome": "success"
            })),
            None,
            "137",
        );
        assert_eq!(outcome, "success");
        assert_eq!(exit_status, "0");
    }

    #[test]
    fn benchmark_retry_inputs_treat_missing_mapped_output_as_error() {
        let (outcome, exit_status) = benchmark_retry_inputs(
            true,
            &json!({ "outcome": "success" }),
            None,
            Some("mapped_grader_output_missing: /agentlab/out/mapped_grader_output.json"),
            "0",
        );
        assert_eq!(outcome, "error");
        assert_eq!(exit_status, "0");
    }

    #[test]
    fn check_dataset_task_ids_rejects_benchmark_grading_opt_out() {
        let benchmark = BenchmarkConfig {
            policy: BenchmarkPolicyConfig::default(),
            grader: Some(BenchmarkGraderConfig::in_task_image(vec![
                "python3".to_string(),
                "/opt/grader/run.py".to_string(),
            ])),
            adapter: None,
        };
        let mut task = task_row_value("task_1", "python:3.11-slim", "/workspace/task", None);
        task.pointer_mut("/task")
            .and_then(Value::as_object_mut)
            .expect("task object")
            .insert("grading".to_string(), json!({ "enabled": false }));
        let checks = check_dataset_task_ids(&[task], &benchmark, &[]);
        let grading_gate = checks
            .iter()
            .find(|check| {
                check
                    .message
                    .contains("Milestone 4 requires mapped grading output")
            })
            .expect("grading opt-out check");
        assert!(
            !grading_gate.passed,
            "grading opt-out should fail validation"
        );
        assert_eq!(grading_gate.severity, PreflightSeverity::Error);
    }

    // -----------------------------------------------------------------------
    // parse_policies tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_policies_defaults_when_no_policies_section() {
        let spec = json!({
            "design": {
                "replications": 1,
                "random_seed": 1
            }
        });
        let config = parse_policies(&spec);
        assert_eq!(config.scheduling, SchedulingPolicy::VariantSequential);
        assert_eq!(config.state, StatePolicy::IsolatePerTrial);
        assert_eq!(config.retry_max_attempts, 1);
        assert!(config.retry_on.is_empty());
        assert!(config.pruning_max_consecutive_failures.is_none());
        assert_eq!(config.concurrency.max_in_flight_per_variant, None);
        assert!(config.concurrency.require_chain_lease);
    }

    #[test]
    fn parse_policies_reads_all_fields() {
        let spec = json!({
            "design": {
                "policies": {
                    "scheduling": "paired_interleaved",
                    "state": "persist_per_task",
                    "retry": {
                        "max_attempts": 3,
                        "retry_on": ["error", "timeout"]
                    },
                    "pruning": {
                        "max_consecutive_failures": 5
                    },
                    "concurrency": {
                        "max_in_flight_per_variant": 2,
                        "require_chain_lease": false
                    }
                }
            }
        });
        let config = parse_policies(&spec);
        assert_eq!(config.scheduling, SchedulingPolicy::PairedInterleaved);
        assert_eq!(config.state, StatePolicy::PersistPerTask);
        assert_eq!(config.retry_max_attempts, 3);
        assert_eq!(config.retry_on, vec!["error", "timeout"]);
        assert_eq!(config.pruning_max_consecutive_failures, Some(5));
        assert_eq!(config.concurrency.max_in_flight_per_variant, Some(2));
        assert!(!config.concurrency.require_chain_lease);
    }

    #[test]
    fn parse_policies_handles_randomized_scheduling() {
        let spec = json!({
            "design": {
                "policies": {
                    "scheduling": "randomized",
                    "state": "accumulate",
                    "retry": { "max_attempts": 1 }
                }
            }
        });
        let config = parse_policies(&spec);
        assert_eq!(config.scheduling, SchedulingPolicy::Randomized);
        assert_eq!(config.state, StatePolicy::Accumulate);
    }

    #[test]
    fn parse_policies_unknown_scheduling_defaults_to_variant_sequential() {
        let spec = json!({
            "design": {
                "policies": {
                    "scheduling": "unknown_value",
                    "state": "unknown_state",
                    "retry": { "max_attempts": 1 }
                }
            }
        });
        let config = parse_policies(&spec);
        assert_eq!(config.scheduling, SchedulingPolicy::VariantSequential);
        assert_eq!(config.state, StatePolicy::IsolatePerTrial);
        assert!(config.concurrency.require_chain_lease);
    }

    #[test]
    fn parse_policies_missing_retry_defaults_to_one_attempt() {
        let spec = json!({
            "design": {
                "policies": {
                    "scheduling": "variant_sequential",
                    "state": "isolate_per_trial"
                }
            }
        });
        let config = parse_policies(&spec);
        assert_eq!(config.retry_max_attempts, 1);
        assert!(config.retry_on.is_empty());
        assert!(config.concurrency.require_chain_lease);
    }

    #[test]
    fn parse_policies_reads_concurrency_fields() {
        let spec = json!({
            "design": {
                "policies": {
                    "concurrency": {
                        "max_in_flight_per_variant": 4,
                        "require_chain_lease": true
                    }
                }
            }
        });

        let config = parse_policies(&spec);
        assert_eq!(config.concurrency.max_in_flight_per_variant, Some(4));
        assert!(config.concurrency.require_chain_lease);
    }

    #[test]
    fn inv02_timeout_policy_propagates_to_runtime_env() {
        let io = prepared_trial_io_fixture(
            PathBuf::from("/tmp/out.json"),
            PathBuf::from("/tmp/events.jsonl"),
        );
        let input = json!({
            "ids": {
                "trial_id": "trial_1",
                "variant_id": "base",
                "task_id": "task_1",
                "repl_idx": 0
            },
            "policy": {
                "timeout_ms": 456000
            }
        });
        let timeout_ms = resolve_trial_timeout_ms(&input);
        let env = build_runtime_contract_env("run_1", &input, &io, None, timeout_ms);
        assert_eq!(
            env.get(AGENTLAB_ENV_TIMEOUT_MS).map(String::as_str),
            Some("456000")
        );
    }

    #[test]
    fn inv03_preflight_fails_below_min_disk_headroom() {
        let check = check_disk_headroom_with_threshold(Path::new("."), u64::MAX);
        assert!(
            !check.passed,
            "disk check should fail when threshold is too high"
        );
        assert!(check.message.contains("required="));
        assert!(check.message.contains("available="));
    }

    #[test]
    fn inv03_preflight_passes_at_or_above_min_disk_headroom() {
        let check = check_disk_headroom_with_threshold(Path::new("."), 1);
        assert!(check.passed, "disk check should pass for tiny threshold");
    }

    #[test]
    fn preflight_parse_parallelism_clamps_and_rejects_zero() {
        assert_eq!(parse_parallelism("1"), Some(1));
        assert_eq!(
            parse_parallelism("128"),
            Some(MAX_PREFLIGHT_IMAGE_PROBE_PARALLELISM)
        );
        assert_eq!(parse_parallelism("0"), None);
        assert_eq!(parse_parallelism("abc"), None);
    }

    #[test]
    fn preflight_bounded_probe_preserves_order_and_bounds_concurrency() {
        let images = vec![
            "img_a".to_string(),
            "img_b".to_string(),
            "img_c".to_string(),
            "img_d".to_string(),
            "img_e".to_string(),
        ];
        let in_flight = std::sync::Arc::new(AtomicUsize::new(0));
        let max_in_flight = std::sync::Arc::new(AtomicUsize::new(0));
        let results = run_bounded_image_probes(&images, "test_probe", |idx, image| {
            let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            max_in_flight.fetch_max(current, Ordering::SeqCst);
            // Make completion order non-deterministic while checking stable output ordering.
            std::thread::sleep(Duration::from_millis(((images.len() - idx) * 2) as u64));
            in_flight.fetch_sub(1, Ordering::SeqCst);
            format!("{}:{}", idx, image)
        });

        assert_eq!(
            results,
            vec![
                "0:img_a".to_string(),
                "1:img_b".to_string(),
                "2:img_c".to_string(),
                "3:img_d".to_string(),
                "4:img_e".to_string(),
            ]
        );
        let allowed_parallelism = preflight_image_probe_parallelism().min(images.len()).max(1);
        assert!(
            max_in_flight.load(Ordering::SeqCst) <= allowed_parallelism,
            "bounded image probes exceeded allowed parallelism"
        );
    }

    fn preflight_test_runtime_profile(
        image_source: ImageSource,
        sandbox_image: Option<&str>,
    ) -> VariantRuntimeProfile {
        let mut agent_runtime = legacy_contract_runtime_fixture();
        agent_runtime.command_raw = vec!["rex".to_string()];
        agent_runtime.image = sandbox_image.unwrap_or("python:3.11-slim").to_string();
        agent_runtime.network = "none".to_string();
        agent_runtime.sandbox_image = sandbox_image.map(|value| value.to_string());
        agent_runtime.image_source = image_source;
        agent_runtime.execution = agent_execution_fixture(Some("python:3.11-slim"));

        VariantRuntimeProfile {
            experiment: json!({}),
            variant_args: Vec::new(),
            agent_runtime,
            agent_runtime_env: BTreeMap::new(),
            invocation_source: "test".to_string(),
            configured_network_mode: "none".to_string(),
            effective_network_mode: "none".to_string(),
        }
    }

    #[test]
    fn hermetic_preflight_requires_agent_runtime_image() {
        let variant = preflight_test_variant();
        let mut profile = preflight_test_runtime_profile(ImageSource::Global, Some("img:latest"));
        profile.agent_runtime.image.clear();

        let checks = check_agent_runtime_hermetic_for_variants(&[variant], &[profile]);
        assert_eq!(checks.len(), 1);
        assert!(!checks[0].passed, "{:?}", checks[0]);
        assert!(
            checks[0]
                .message
                .contains("runtime.agent_runtime.image is required"),
            "unexpected message: {}",
            checks[0].message
        );
    }

    #[test]
    fn dangerous_mode_preflight_rejects_dangerous_command_tokens() {
        let variant = preflight_test_variant();
        let mut profile = preflight_test_runtime_profile(ImageSource::Global, Some("img:latest"));
        profile.agent_runtime.command_raw = vec![
            "rex".to_string(),
            "run".to_string(),
            "--dangerous".to_string(),
        ];

        let checks = check_dangerous_mode_forbidden_for_variants(&[variant], &[profile]);
        assert_eq!(checks.len(), 1);
        assert!(!checks[0].passed, "{:?}", checks[0]);
        assert!(
            checks[0].message.contains("--dangerous"),
            "unexpected message: {}",
            checks[0].message
        );
    }

    #[test]
    fn dangerous_mode_preflight_rejects_variant_appended_flags() {
        let variant = preflight_test_variant();
        let mut profile = preflight_test_runtime_profile(ImageSource::Global, Some("img:latest"));
        profile.variant_args = vec!["--dangerous".to_string()];

        let checks = check_dangerous_mode_forbidden_for_variants(&[variant], &[profile]);
        assert_eq!(checks.len(), 1);
        assert!(!checks[0].passed, "{:?}", checks[0]);
        assert!(
            checks[0].message.contains("--dangerous"),
            "unexpected message: {}",
            checks[0].message
        );
    }

    #[test]
    fn dangerous_mode_preflight_rejects_embedded_flags_in_string_command() {
        let variant = preflight_test_variant();
        let mut profile = preflight_test_runtime_profile(ImageSource::Global, Some("img:latest"));
        profile.agent_runtime.command_raw = vec!["rex run --dangerous".to_string()];

        let checks = check_dangerous_mode_forbidden_for_variants(&[variant], &[profile]);
        assert_eq!(checks.len(), 1);
        assert!(!checks[0].passed, "{:?}", checks[0]);
        assert!(
            checks[0].message.contains("--dangerous"),
            "unexpected message: {}",
            checks[0].message
        );
    }

    #[test]
    fn resolve_run_isolation_grade_rejects_dangerous_variant_args() {
        let mut profile = preflight_test_runtime_profile(ImageSource::Global, Some("img:latest"));
        profile.variant_args = vec!["--dangerous".to_string()];
        assert_eq!(
            resolve_run_isolation_grade(&[profile], &RunBehavior::default()),
            "invalid"
        );
    }

    #[test]
    fn resolve_run_isolation_grade_marks_missing_agent_image_invalid() {
        let mut profile = preflight_test_runtime_profile(ImageSource::Global, Some("img:latest"));
        profile.agent_runtime.image.clear();
        assert_eq!(
            resolve_run_isolation_grade(&[profile], &RunBehavior::default()),
            "invalid"
        );
    }

    #[test]
    fn resolve_run_isolation_grade_marks_scientific_container_runs_hermetic() {
        let profile = preflight_test_runtime_profile(ImageSource::Global, Some("img:latest"));
        let behavior = RunBehavior::default();

        assert_eq!(
            resolve_run_isolation_grade(&[profile], &behavior),
            "hermetic"
        );
    }

    fn preflight_test_variant() -> Variant {
        Variant {
            id: "test_variant".to_string(),
            bindings: json!({}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        }
    }

    #[test]
    fn preflight_resolve_images_reports_missing_global_image() {
        let mut profile = preflight_test_runtime_profile(ImageSource::Global, None);
        profile.agent_runtime.image.clear();
        let check = resolve_preflight_images(
            "container_ready",
            &profile,
            &[],
            None,
            "global image missing",
        )
        .expect_err("missing global image should fail");
        assert_eq!(check.name, "container_ready");
        assert!(!check.passed);
        assert!(check.message.contains("global image missing"));
    }

    #[test]
    fn preflight_resolve_images_falls_back_to_global_image_when_tasks_absent() {
        let profile = preflight_test_runtime_profile(ImageSource::Global, Some("python:3.11-slim"));

        let images =
            resolve_preflight_images("container_ready", &profile, &[], None, "unused")
                .expect("global image should resolve");

        assert_eq!(images, vec!["python:3.11-slim".to_string()]);
    }

    #[test]
    fn preflight_resolve_images_reports_per_task_scan_errors() {
        let profile = preflight_test_runtime_profile(ImageSource::PerTask, None);
        let scan = PerTaskImageScanResult {
            unique_images: Vec::new(),
            missing_task_ids: Vec::new(),
            parse_errors: vec!["line 1: malformed".to_string()],
        };
        let check =
            resolve_preflight_images("container_ready", &profile, &[], Some(&scan), "unused")
                .expect_err("parse errors should fail");
        assert!(!check.passed);
        assert!(check
            .message
            .contains("failed to parse packaged task_row_v1 rows"));
    }

    #[test]
    fn preflight_resolve_images_prefers_per_task_images_over_task_image_sentinel() {
        let mut profile = preflight_test_runtime_profile(ImageSource::PerTask, Some("task_image"));
        profile.agent_runtime.image = "task_image".to_string();
        let scan = PerTaskImageScanResult {
            unique_images: vec![
                "swebench/task-a:latest".to_string(),
                "swebench/task-b:latest".to_string(),
            ],
            missing_task_ids: Vec::new(),
            parse_errors: Vec::new(),
        };

        let images =
            resolve_preflight_images("container_ready", &profile, &[], Some(&scan), "unused")
                .expect("per-task images should resolve");

        assert_eq!(
            images,
            vec![
                "swebench/task-a:latest".to_string(),
                "swebench/task-b:latest".to_string(),
            ]
        );
    }

    #[test]
    fn preflight_probe_output_blockers_detect_avx_incompatibility_warning() {
        let blockers = detect_known_probe_output_blockers(
            "",
            "warn: CPU lacks AVX support, strange crashes may occur.",
        );
        assert_eq!(blockers.len(), 1);
        assert!(blockers[0].contains("CPU lacks AVX support"));
    }

    #[test]
    fn preflight_probe_output_blockers_detect_missing_tool_registry_warning() {
        let blockers = detect_known_probe_output_blockers(
            "[harness] Agent 'coding' references tool 'Skill' which is not available",
            "",
        );
        assert_eq!(blockers.len(), 1);
        assert!(blockers[0].contains("references tool 'Skill'"));
    }

    #[test]
    fn preflight_agent_runtime_reachable_reports_missing_required_env_var() {
        let root = TempDirGuard::new("agentlab_preflight_missing_required_env");
        let variant = preflight_test_variant();
        let mut profile = preflight_test_runtime_profile(ImageSource::Global, Some("img:latest"));
        profile.agent_runtime.env_from_host = vec!["OPENAI_API_KEY".to_string()];
        profile.agent_runtime_env.clear();

        let check =
            check_agent_runtime_reachable_with_scan(&profile, &variant, &[], None, &root.path);

        assert_eq!(check.name, "agent_runtime_reachable");
        assert!(!check.passed, "{:?}", check);
        assert!(check.message.contains("missing required runtime env var"));
        assert!(check.message.contains("OPENAI_API_KEY"));
    }

    #[test]
    fn preflight_contract_smoke_result_validation_rejects_missing_payload() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_preflight_missing_result");
        let failures = validate_preflight_result_payload(&paths.out.join("result.json"));
        assert!(
            failures
                .iter()
                .any(|failure| failure.contains("contract smoke did not write result payload")),
            "unexpected failures: {:?}",
            failures
        );
    }

    #[test]
    fn preflight_blocking_error_filter_is_check_specific() {
        let checks = vec![
            PreflightCheck {
                name: "container_ready",
                passed: true,
                severity: PreflightSeverity::Error,
                message: "ok".to_string(),
            },
            PreflightCheck {
                name: "benchmark_grader_reachable",
                passed: false,
                severity: PreflightSeverity::Warning,
                message: "warn".to_string(),
            },
            PreflightCheck {
                name: "container_ready",
                passed: false,
                severity: PreflightSeverity::Error,
                message: "failed".to_string(),
            },
        ];
        assert!(has_blocking_preflight_error(&checks, "container_ready"));
        assert!(!has_blocking_preflight_error(
            &checks,
            "benchmark_grader_reachable"
        ));
    }

    fn inv07_spec_with_runtime_bindings() -> Value {
        json!({
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_runtime" },
            "dataset": { "path": "tasks.jsonl", "provider": "local_jsonl", "suite_id": "s", "split_id": "dev", "limit": 1 },
            "design": { "sanitization_profile": "hermetic_functional", "comparison": "paired", "replications": 1, "random_seed": 1, "shuffle_tasks": false, "max_concurrency": 1 },
            "baseline": { "variant_id": "base", "bindings": { "model_provider": "openai", "model": "gpt-5" } },
            "variant_plan": [
                { "variant_id": "alt", "bindings": { "model_provider": "anthropic", "model": "claude-sonnet-4" } }
            ],
            "runtime": {
                "agent_runtime": {
                    "command": ["rex", "run", "--provider", "$model_provider", "--model", "$model"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "img",
                    "env": {
                        "OPENAI_API_KEY": "$OPENAI_API_KEY",
                        "STATIC_FLAG": "1"
                    }
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": {
                    "profile": "default",
                    "network": "none"
                }
            }
        })
    }

    fn inv07_resolve_runtime_profiles(
        spec: &Value,
        exp_dir: &Path,
        runtime_env: BTreeMap<String, String>,
    ) -> (Vec<Variant>, Vec<VariantRuntimeProfile>) {
        let (variants, _) = resolve_variant_plan(spec).expect("variant plan");
        let execution = RunExecutionOptions {
            runtime_env,
            ..RunExecutionOptions::default()
        };
        let mut profiles = Vec::new();
        for variant in &variants {
            profiles.push(
                resolve_variant_runtime_profile(
                    spec,
                    variant,
                    exp_dir,
                    &RunBehavior::default(),
                    &execution,
                )
                .expect("runtime profile"),
            );
        }
        (variants, profiles)
    }

    #[test]
    fn inv07_runtime_bindings_resolve_variant_values_into_command() {
        let root = TempDirGuard::new("agentlab_inv07_variant_runtime_bindings");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        fs::write(exp_dir.join("tasks.jsonl"), "{\"id\":\"task_1\"}\n").expect("dataset");
        let spec = inv07_spec_with_runtime_bindings();
        let mut runtime_env = BTreeMap::new();
        runtime_env.insert("OPENAI_API_KEY".to_string(), "test-token".to_string());
        let (_variants, profiles) = inv07_resolve_runtime_profiles(&spec, &exp_dir, runtime_env);

        assert_eq!(
            profiles[0].agent_runtime.command_raw,
            vec![
                "rex".to_string(),
                "run".to_string(),
                "--provider".to_string(),
                "openai".to_string(),
                "--model".to_string(),
                "gpt-5".to_string()
            ]
        );
        assert_eq!(
            profiles[1].agent_runtime.command_raw,
            vec![
                "rex".to_string(),
                "run".to_string(),
                "--provider".to_string(),
                "anthropic".to_string(),
                "--model".to_string(),
                "claude-sonnet-4".to_string()
            ]
        );
    }

    #[test]
    fn inv07_runtime_bindings_resolve_launch_env_into_public_env() {
        let root = TempDirGuard::new("agentlab_inv07_launch_env_binding");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        fs::write(exp_dir.join("tasks.jsonl"), "{\"id\":\"task_1\"}\n").expect("dataset");
        let spec = inv07_spec_with_runtime_bindings();
        let mut runtime_env = BTreeMap::new();
        runtime_env.insert("OPENAI_API_KEY".to_string(), "test-token".to_string());
        let (_variants, profiles) = inv07_resolve_runtime_profiles(&spec, &exp_dir, runtime_env);

        assert_eq!(
            profiles[0]
                .agent_runtime_env
                .get("OPENAI_API_KEY")
                .map(String::as_str),
            Some("test-token")
        );
        assert_eq!(
            profiles[0]
                .agent_runtime_env
                .get("STATIC_FLAG")
                .map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn inv07_runtime_bindings_fail_when_required_launch_env_is_missing() {
        let root = TempDirGuard::new("agentlab_inv07_missing_launch_env");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp dir");
        fs::write(exp_dir.join("tasks.jsonl"), "{\"id\":\"task_1\"}\n").expect("dataset");
        let spec = inv07_spec_with_runtime_bindings();
        let (variants, _) = resolve_variant_plan(&spec).expect("variant plan");
        let err = match resolve_variant_runtime_profile(
            &spec,
            &variants[0],
            &exp_dir,
            &RunBehavior::default(),
            &RunExecutionOptions::default(),
        ) {
            Ok(_) => panic!("missing runtime env should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("missing runtime binding $OPENAI_API_KEY"),
            "unexpected error: {}",
            err
        );
    }

    fn inv06_write_resolved_experiment(
        run_dir: &Path,
        dataset_path: &str,
        run_id: &str,
        run_status: &str,
    ) -> Value {
        let project_root = find_project_root(run_dir);
        let bundle_root = ensure_test_agent_bundle(&project_root, "rex-current");
        let resolved = json!({
            "version": "0.3",
            "experiment": { "id": "e", "name": "n", "workload_type": "agent_harness" },
            "dataset": { "path": dataset_path, "provider": "local_jsonl", "suite_id": "s", "schema_version": "v1", "split_id": "dev", "limit": 1 },
            "design": { "sanitization_profile": "hermetic_functional", "comparison": "paired", "replications": 1, "random_seed": 1, "shuffle_tasks": false, "max_concurrency": 1 },
            "baseline": { "variant_id": "base", "bindings": {} },
            "runtime": {
                "agent": {
                    "command": [
                        "sh",
                        "-lc",
                        "printf '%s' '{\"schema_version\":\"agent_result_v1\",\"outcome\":\"success\",\"checkpoints\":[]}'"
                    ],
                    "bundle": bundle_root.to_string_lossy().to_string(),
                    "io": { "input_arg": "--input", "output_arg": "--output" }
                },
                "sandbox": runtime_sandbox("global", Some("img")),
                "policy": { "timeout_ms": 600000 }
            }
        });
        atomic_write_json_pretty(&run_dir.join("resolved_experiment.json"), &resolved)
            .expect("resolved experiment");
        let (variants, baseline_id) = resolve_variant_plan(&resolved).expect("variant plan");
        write_resolved_variants(run_dir, &resolved, &baseline_id, &variants)
            .expect("write variants");
        write_run_control_v2(run_dir, run_id, run_status, &[], None).expect("run control");
        write_run_session_state(
            run_dir,
            run_id,
            &RunBehavior::default(),
            &container_execution(),
        )
        .expect("run session");
        let schedule = build_trial_schedule(1, 1, 1, parse_policies(&resolved).scheduling, 1);
        let progress = ScheduleProgress {
            schema_version: "schedule_progress_v1".to_string(),
            run_id: run_id.to_string(),
            total_slots: schedule.len(),
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule,
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        write_schedule_progress(run_dir, &progress).expect("schedule progress");
        resolved
    }

    #[test]
    fn inv06_recover_then_continue_succeeds_with_minimal_env() {
        let (_root, run_dir) = create_run_dir("agentlab_inv06_recover_continue", "run_1");
        let dataset_path = run_dir.join("tasks.jsonl");
        fs::write(&dataset_path, "{\"id\":\"task_1\"}\n").expect("dataset");
        inv06_write_resolved_experiment(&run_dir, "tasks.jsonl", "run_1", "running");
        write_schedule_progress(
            &run_dir,
            &ScheduleProgress {
                schema_version: "schedule_progress_v1".to_string(),
                run_id: "run_1".to_string(),
                total_slots: 0,
                next_schedule_index: 0,
                next_trial_index: 0,
                schedule: Vec::new(),
                completed_slots: Vec::new(),
                pruned_variants: Vec::new(),
                consecutive_failures: BTreeMap::new(),
                updated_at: Utc::now().to_rfc3339(),
            },
        )
        .expect("schedule progress");

        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        let active = vec![RunControlActiveTrial {
            trial_id: "trial_1".to_string(),
            worker_id: "worker_1".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: Some(active_control_for_trial(&trial_dir)),
        }];
        write_run_control_v2(&run_dir, "run_1", "running", &active, None).expect("run control");

        let recovered = recover_run(&run_dir, true).expect("recover");
        assert_eq!(recovered.recovered_status, "interrupted");

        let continue_err =
            continue_run(&run_dir).expect_err("continue should reach deterministic terminal guard");
        assert!(
            continue_err.to_string().contains("nothing to continue"),
            "unexpected continue error: {}",
            continue_err
        );
    }

    #[test]
    fn recover_run_releases_untracked_runtime_active_trial() {
        let (_root, run_dir) = create_run_dir("agentlab_recover_runtime_only_active", "run_1");
        let dataset_path = run_dir.join("tasks.jsonl");
        fs::write(&dataset_path, "{\"id\":\"task_1\"}\n").expect("dataset");
        inv06_write_resolved_experiment(&run_dir, "tasks.jsonl", "run_1", "running");

        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        trial::state::write_trial_attempt_state(
            &trial_dir,
            &runtime_trial_attempt_state_fixture(TrialPhase::AgentRunning),
        )
        .expect("write runtime state");
        write_run_control_v2(&run_dir, "run_1", "running", &[], None).expect("run control");

        let recovered = recover_run(&run_dir, true).expect("recover");
        assert_eq!(recovered.active_trials_released, 1);

        let runtime_state = trial::state::load_trial_attempt_state(&trial_dir).expect("runtime");
        assert_eq!(runtime_state.state.phase, TrialPhase::Abandoned);
        let trial_state = load_json_file(&trial_dir.join("trial_state.json")).expect("trial state");
        assert_eq!(trial_state["status"], "failed");
        assert_eq!(trial_state["exit_reason"], "worker_lost_recovered");
    }

    #[test]
    fn recover_run_prefers_durable_paused_runtime_state_over_stale_run_control() {
        let (_root, run_dir) = create_run_dir("agentlab_recover_runtime_paused", "run_1");
        let dataset_path = run_dir.join("tasks.jsonl");
        fs::write(&dataset_path, "{\"id\":\"task_1\"}\n").expect("dataset");
        inv06_write_resolved_experiment(&run_dir, "tasks.jsonl", "run_1", "running");

        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "paused", Some("cp1"));
        let mut runtime_state = runtime_trial_attempt_state_fixture(TrialPhase::Paused);
        runtime_state.paused_from_phase = Some(TrialPhase::AgentRunning);
        trial::state::write_trial_attempt_state(&trial_dir, &runtime_state)
            .expect("write runtime state");

        let active = vec![RunControlActiveTrial {
            trial_id: "trial_1".to_string(),
            worker_id: "worker_1".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("base".to_string()),
            started_at: Some(Utc::now().to_rfc3339()),
            control: Some(active_control_for_trial(&trial_dir)),
        }];
        write_run_control_v2(&run_dir, "run_1", "running", &active, None).expect("run control");

        let recovered = recover_run(&run_dir, true).expect("recover");
        assert_eq!(recovered.active_trials_released, 0);

        let persisted = trial::state::load_trial_attempt_state(&trial_dir).expect("runtime");
        assert_eq!(persisted.state.phase, TrialPhase::Paused);
        assert_eq!(
            persisted.state.paused_from_phase,
            Some(TrialPhase::AgentRunning)
        );
        let trial_state = load_json_file(&trial_dir.join("trial_state.json")).expect("trial state");
        assert_eq!(trial_state["status"], "paused");
    }

    #[test]
    fn recover_run_reconciles_commit_pending_runtime_state_from_committed_slot() {
        let (_root, run_dir) = create_run_dir("agentlab_recover_commit_pending", "run_1");
        let dataset_path = run_dir.join("tasks.jsonl");
        fs::write(&dataset_path, "{\"id\":\"task_1\"}\n").expect("dataset");
        inv06_write_resolved_experiment(&run_dir, "tasks.jsonl", "run_1", "running");

        let trial_dir = seed_parent_trial(&run_dir, "trial_1", json!([]), "running", None);
        ensure_dir(&run_dir.join("runtime")).expect("runtime dir");
        let evidence_records_path = run_dir.join("runtime").join("recover_evidence.jsonl");
        let chain_state_path = run_dir.join("runtime").join("recover_chain_state.jsonl");
        let benchmark_conclusions_path = run_dir.join("runtime").join("recover_conclusions.jsonl");
        fs::write(&evidence_records_path, "").expect("evidence rows");
        fs::write(&chain_state_path, "").expect("chain rows");
        fs::write(&benchmark_conclusions_path, "").expect("conclusion rows");

        let mut schedule_progress = load_schedule_progress(&run_dir).expect("schedule progress");
        let mut pruned_variants: HashSet<usize> = HashSet::new();
        let mut consecutive_failures: BTreeMap<usize, usize> = BTreeMap::new();
        let mut slot_attempts: HashMap<usize, usize> = HashMap::new();
        let trial_result =
            TrialExecutionResult::minimal("trial_1".to_string(), "completed", Some(0));
        let mut run_sink = BufferedRunSink::default();
        RunCoordinator::commit_trial_slot(
            &run_dir,
            &PolicyConfig::default(),
            &evidence_records_path,
            &chain_state_path,
            &benchmark_conclusions_path,
            &mut schedule_progress,
            0,
            1,
            &mut pruned_variants,
            &mut consecutive_failures,
            &trial_result,
            &mut run_sink,
            &mut slot_attempts,
        )
        .expect("commit trial slot");

        trial::state::write_trial_attempt_state(
            &trial_dir,
            &runtime_trial_attempt_state_fixture(TrialPhase::CommitPending),
        )
        .expect("write runtime state");

        let recovered = recover_run(&run_dir, true).expect("recover");
        assert_eq!(recovered.active_trials_released, 0);

        let persisted = trial::state::load_trial_attempt_state(&trial_dir).expect("runtime");
        assert_eq!(persisted.state.phase, TrialPhase::Committed);
    }

    #[test]
    fn inv06_continue_handles_relative_and_absolute_dataset_paths() {
        let root = TempDirGuard::new("agentlab_inv06_dataset_paths");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).expect("exp");
        let abs_dataset = root.path.join("dataset_abs.jsonl");
        fs::write(&abs_dataset, "{\"id\":\"task_1\"}\n").expect("abs dataset");

        for use_absolute in [false, true] {
            let dataset_path = if use_absolute {
                abs_dataset.to_string_lossy().to_string()
            } else {
                "tasks.jsonl".to_string()
            };
            let spec = json!({
                "dataset": { "path": dataset_path }
            });
            let resolved = resolve_dataset_path(&spec, &exp_dir).expect("dataset path");
            let expected = if use_absolute {
                abs_dataset.clone()
            } else {
                exp_dir.join("tasks.jsonl")
            };
            assert_eq!(
                resolved, expected,
                "dataset mode absolute={} should resolve correctly",
                use_absolute
            );
        }
    }

    #[test]
    fn inv06_load_tasks_honors_zero_limit() {
        let root = TempDirGuard::new("agentlab_inv06_load_tasks_limit_zero");
        let dataset_path = root.path.join("tasks.jsonl");
        fs::write(
            &dataset_path,
            concat!(
                "{\"schema_version\":\"task_row_v1\",\"id\":\"task_1\",\"image\":\"python:3.11-slim\",\"workdir\":\"/workspace/task\",\"task\":{\"id\":\"task_1\"},\"materialization\":{\"kind\":\"task_image\"}}\n",
                "{\"schema_version\":\"task_row_v1\",\"id\":\"task_2\",\"image\":\"python:3.11-slim\",\"workdir\":\"/workspace/task\",\"task\":{\"id\":\"task_2\"},\"materialization\":{\"kind\":\"task_image\"}}\n",
                "{\"schema_version\":\"task_row_v1\",\"id\":\"task_3\",\"image\":\"python:3.11-slim\",\"workdir\":\"/workspace/task\",\"task\":{\"id\":\"task_3\"},\"materialization\":{\"kind\":\"task_image\"}}\n"
            ),
        )
        .expect("dataset");
        let spec = json!({
            "dataset": { "limit": 0 }
        });

        let tasks = load_tasks(&dataset_path, &spec).expect("load tasks");
        assert!(
            tasks.is_empty(),
            "dataset.limit=0 should produce zero loaded tasks"
        );
    }

    #[test]
    fn inv06_count_tasks_honors_zero_limit() {
        let root = TempDirGuard::new("agentlab_inv06_count_tasks_limit_zero");
        let dataset_path = root.path.join("tasks.jsonl");
        fs::write(
            &dataset_path,
            "{\"id\":\"task_1\"}\n{\"id\":\"task_2\"}\n{\"id\":\"task_3\"}\n",
        )
        .expect("dataset");
        let spec = json!({
            "dataset": { "limit": 0 }
        });

        let count = count_tasks(&dataset_path, &spec).expect("count tasks");
        assert_eq!(count, 0, "dataset.limit=0 should produce zero task count");
    }

    #[test]
    fn inv06_load_task_rows_for_build_reads_task_rows() {
        let root = TempDirGuard::new("agentlab_inv06_load_task_rows_for_build");
        let dataset_path = root.path.join("task_rows.jsonl");
        write_task_row_dataset(&dataset_path, "task_1");
        let spec = json!({
            "dataset": { "limit": 1 }
        });

        let tasks = load_task_rows_for_build(&dataset_path, &spec).expect("load task rows");
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].get("schema_version").and_then(Value::as_str),
            Some("task_row_v1")
        );
        assert_eq!(
            tasks[0].pointer("/id").and_then(Value::as_str),
            Some("task_1")
        );
    }

    #[test]
    fn inv06_build_load_task_rows_rejects_public_task_spec_rows() {
        let root = TempDirGuard::new("agentlab_inv06_build_rejects_public_task_spec");
        let dataset_path = root.path.join("task_spec.jsonl");
        fs::write(
            &dataset_path,
            "{\"task\":{\"id\":\"task_1\"},\"environment\":{\"image\":\"python:3.11-slim\"},\"workspace\":{\"mode\":\"scratch\",\"base\":{\"kind\":\"empty\"},\"overlays\":[],\"aux_mounts\":[]},\"dependencies\":{},\"limits\":{}}\n",
        )
        .expect("dataset");
        let spec = json!({
            "dataset": { "limit": 1 }
        });

        let err = load_task_rows_for_build(&dataset_path, &spec)
            .expect_err("build should reject task spec");
        assert!(
            err.to_string().contains("task_row_v1"),
            "unexpected runtime error: {}",
            err
        );
    }

    #[test]
    fn inv06_runtime_load_tasks_rejects_legacy_task_declaration_rows() {
        let root = TempDirGuard::new("agentlab_inv06_runtime_rejects_legacy_task_declaration");
        let dataset_path = root.path.join("tasks.jsonl");
        fs::write(
            &dataset_path,
            "{\"schema_version\":\"task_declaration_v1\",\"task\":{\"id\":\"task_1\"},\"environment\":{\"image\":\"python:3.11-slim\"},\"workspace\":{\"mode\":\"scratch\",\"base\":{\"kind\":\"empty\"},\"overlays\":[],\"aux_mounts\":[]},\"dependencies\":{},\"limits\":{}}\n",
        )
        .expect("dataset");
        let spec = json!({
            "dataset": { "limit": 1 }
        });

        let err =
            load_tasks(&dataset_path, &spec).expect_err("runtime should reject legacy declaration");
        assert!(
            err.to_string().contains("task_row_v1"),
            "unexpected runtime error: {}",
            err
        );
    }

    #[test]
    fn copy_dir_filtered_preserves_directory_symlinks_without_recursing() {
        let root = TempDirGuard::new("agentlab_copy_dir_filtered_symlink");
        let workspace = root.path.join("workspace");
        ensure_dir(&workspace).expect("workspace");
        fs::write(workspace.join("keep.txt"), "keep").expect("keep");
        symlink(Path::new("."), workspace.join("loop")).expect("loop symlink");

        let copied = root.path.join("copied");
        copy_dir_filtered(&workspace, &copied, &[]).expect("copy");

        let copied_loop = copied.join("loop");
        let metadata = fs::symlink_metadata(&copied_loop).expect("copied symlink metadata");
        assert!(
            metadata.file_type().is_symlink(),
            "{:?}",
            metadata.file_type()
        );
        assert_eq!(
            fs::read_link(&copied_loop).expect("copied symlink target"),
            PathBuf::from(".")
        );
    }

    #[test]
    fn outputs_only_materialization_preserves_out_dir_after_scratch_cleanup() {
        let root = TempDirGuard::new("agentlab_outputs_only_materialization");
        let run_dir = root.path.join(".lab").join("runs").join("run_1");
        let trial_dir = run_dir.join("trials").join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");

        let trial_paths = TrialPaths::new(&trial_dir, &root.path).expect("trial paths");
        trial_paths.prepare(false).expect("prepare trial paths");
        fs::write(
            trial_paths.runtime.result.clone(),
            "{\"schema_version\":\"agent_result_v1\",\"outcome\":\"success\"}\n",
        )
        .expect("write result");
        fs::write(
            trial_paths.out.join("agent_report.json"),
            "{\"cwd\":\"/agentlab/workspace\"}\n",
        )
        .expect("write agent report");

        materialize_trial_runtime_layout(
            &trial_dir,
            &trial_paths,
            MaterializationMode::OutputsOnly,
        )
        .expect("materialize outputs");
        trial_paths.cleanup_scratch().expect("cleanup scratch");

        assert!(
            trial_dir.join("out").join("agent_report.json").exists(),
            "out directory should be preserved after scratch cleanup"
        );
        assert!(
            trial_dir.join("result.json").exists(),
            "canonical result.json should be materialized into the stable trial dir"
        );
    }

    #[test]
    fn cleanup_scratch_removes_read_only_dependency_tree() {
        let root = TempDirGuard::new("agentlab_cleanup_scratch_read_only");
        let run_dir = root.path.join(".lab").join("runs").join("run_1");
        let trial_dir = run_dir.join("trials").join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");

        let trial_paths = TrialPaths::new(&trial_dir, &root.path).expect("trial paths");
        trial_paths.prepare(false).expect("prepare trial paths");

        let support_dir = trial_paths
            .workspace
            .join(AGENTLAB_RUNNER_SUPPORT_REL_DIR)
            .join("bench")
            .join("integration")
            .join("agentlab");
        ensure_dir(&support_dir).expect("support dir");
        fs::write(
            support_dir.join("bench_benchmark_adapter.py"),
            "#!/usr/bin/env python3\nprint('ok')\n",
        )
        .expect("support file");
        set_staged_path_read_only(&trial_paths.workspace.join(AGENTLAB_RUNNER_SUPPORT_REL_DIR))
            .expect("mark support tree read only");

        trial_paths.cleanup_scratch().expect("cleanup scratch");

        assert!(
            !trial_paths.scratch_dir.exists(),
            "scratch dir should be removed even when staged support files are read only"
        );
    }

    #[test]
    fn outputs_only_materialization_preserves_directory_symlinks_without_recursing() {
        let root = TempDirGuard::new("agentlab_outputs_only_symlink_materialization");
        let run_dir = root.path.join(".lab").join("runs").join("run_1");
        let trial_dir = run_dir.join("trials").join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");

        let trial_paths = TrialPaths::new(&trial_dir, &root.path).expect("trial paths");
        trial_paths.prepare(false).expect("prepare trial paths");
        fs::write(
            trial_paths.runtime.result.clone(),
            "{\"schema_version\":\"agent_result_v1\",\"outcome\":\"success\"}\n",
        )
        .expect("write result");
        fs::write(trial_paths.out.join("keep.txt"), "keep").expect("write out file");
        symlink(Path::new("."), trial_paths.out.join("loop")).expect("loop symlink");

        materialize_trial_runtime_layout(
            &trial_dir,
            &trial_paths,
            MaterializationMode::OutputsOnly,
        )
        .expect("materialize outputs");

        let materialized_loop = trial_dir.join("out").join("loop");
        let metadata = fs::symlink_metadata(&materialized_loop).expect("materialized symlink");
        assert!(
            metadata.file_type().is_symlink(),
            "{:?}",
            metadata.file_type()
        );
        assert_eq!(
            fs::read_link(&materialized_loop).expect("materialized symlink target"),
            PathBuf::from(".")
        );
    }

    #[test]
    fn inv04_agent_artifact_mount_cache_unpacks_tar_once() {
        let root = TempDirGuard::new("agentlab_inv04_artifact_mount_cache");
        let artifact_src = root.path.join("artifact_src");
        ensure_dir(&artifact_src).expect("artifact src");
        fs::write(artifact_src.join("agent.txt"), "agent payload").expect("artifact payload");
        let artifact_tar = root.path.join("agent-runtime.tar.gz");
        let tar_status = Command::new("tar")
            .args([
                "-czf",
                artifact_tar.to_string_lossy().as_ref(),
                "-C",
                artifact_src.to_string_lossy().as_ref(),
                ".",
            ])
            .status()
            .expect("create tar");
        assert!(tar_status.success(), "failed to create artifact tarball");

        let first_mount = resolve_agent_artifact_mount_dir(&artifact_tar).expect("first unpack");
        assert!(
            !first_mount.to_string_lossy().contains(':'),
            "artifact mount cache path must be colon-safe for docker bind mounts: {}",
            first_mount.display()
        );
        assert!(
            first_mount.join("agent.txt").exists(),
            "unpacked artifact payload missing"
        );

        let second_mount = resolve_agent_artifact_mount_dir(&artifact_tar)
            .expect("second unpack should be cached");
        assert_eq!(
            first_mount, second_mount,
            "artifact mount path should be stable across repeated calls"
        );
        assert!(
            second_mount.join(".agentlab_ready").exists(),
            "cached artifact should include ready marker"
        );
    }

    #[test]
    fn inv04_agent_artifact_mount_cache_repairs_nested_packages_layout() {
        let root = TempDirGuard::new("agentlab_inv04_artifact_layout_repair");
        let artifact_src = root.path.join("artifact_src");
        ensure_dir(&artifact_src.join("node_modules")).expect("node_modules dir");
        ensure_dir(
            &artifact_src
                .join("packages")
                .join("packages")
                .join("infra")
                .join("comms-bus"),
        )
        .expect("nested comms-bus dir");
        fs::write(
            artifact_src
                .join("packages")
                .join("packages")
                .join("infra")
                .join("comms-bus")
                .join("package.json"),
            "{}",
        )
        .expect("package marker");
        symlink(
            Path::new("../packages/infra/comms-bus"),
            artifact_src.join("node_modules").join("comms-bus"),
        )
        .expect("broken layout symlink");

        let artifact_tar = root.path.join("agent-runtime.tar.gz");
        let tar_status = Command::new("tar")
            .args([
                "-czf",
                artifact_tar.to_string_lossy().as_ref(),
                "-C",
                artifact_src.to_string_lossy().as_ref(),
                ".",
            ])
            .status()
            .expect("create tar");
        assert!(tar_status.success(), "failed to create artifact tarball");

        let mount_dir = resolve_agent_artifact_mount_dir(&artifact_tar).expect("mount dir");
        assert!(
            mount_dir
                .join("packages")
                .join("infra")
                .join("comms-bus")
                .join("package.json")
                .exists(),
            "expected compatibility shim at packages/infra"
        );
        assert!(
            mount_dir
                .join("node_modules")
                .join("comms-bus")
                .join("package.json")
                .exists(),
            "node_modules/comms-bus symlink should resolve after repair"
        );
    }

    #[test]
    fn resolve_container_platform_maps_swebench_architecture_tags() {
        assert_eq!(
            resolve_container_platform("swebench/sweb.eval.x86_64.astropy__astropy-12907:latest"),
            Some("linux/amd64")
        );
        assert_eq!(
            resolve_container_platform("sweb.eval.arm64.astropy__astropy-12907:latest"),
            Some("linux/arm64")
        );
        assert_eq!(resolve_container_platform("python:3.11-slim"), None);
    }

    fn create_dx_authoring_fixture(prefix: &str) -> TempDirGuard {
        let root = TempDirGuard::new(prefix);
        let dataset_dir = root.path.join(".lab").join("experiments").join("data");
        ensure_dir(&dataset_dir).expect("dataset dir");
        let workspace_base_digest = "f".repeat(64);
        let workspace_base_pack = root
            .path
            .join(".lab")
            .join("dataset_packs")
            .join("sha256")
            .join(&workspace_base_digest);
        ensure_dir(&workspace_base_pack).expect("workspace base pack dir");
        fs::write(workspace_base_pack.join("README.md"), "seed").expect("workspace base content");
        let bench_v0_row =
            r#"{"schema_version":"task_row_v1","id":"TASK001","image":"python:3.11-slim","workdir":"/workspace/task","time_limit_ms":600000,"task":{"id":"TASK001"},"materialization":{"kind":"task_image"}}"#
                .to_string();
        fs::write(dataset_dir.join("bench_v0.task_spec.jsonl"), &bench_v0_row)
            .expect("dataset row");
        let swebench_row = concat!(
            r#"{"schema_version":"task_row_v1","id":"swebench_astropy_astropy_12907","image":"swebench/sweb.eval.x86_64.astropy__astropy-12907:latest","workdir":"/testbed","task":{"id":"swebench_astropy_astropy_12907","benchmark":{"adapter_id":"swebench_task_container_grader","name":"swebench_lite_curated","split":"test"},"swebench":{"input":{"repo":"astropy/astropy","instance_id":"astropy__astropy-12907","base_commit":"deadbeef"}}},"materialization":{"kind":"task_image"}}"#
        );
        fs::write(
            dataset_dir.join("swebench_lite_curated.task_spec.jsonl"),
            swebench_row,
        )
        .expect("swebench dataset row");

        let artifact_bin = root
            .path
            .join(".lab")
            .join("agents")
            .join("rex-minimal-linux-dir")
            .join("bin");
        ensure_dir(&artifact_bin).expect("artifact dir");
        let artifact_entrypoint = artifact_bin.join("rex");
        fs::write(&artifact_entrypoint, "#!/bin/sh\necho rex\n").expect("artifact binary");
        #[cfg(unix)]
        {
            let mut perms = fs::metadata(&artifact_entrypoint)
                .expect("artifact metadata")
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&artifact_entrypoint, perms).expect("artifact executable perms");
        }

        let overrides_dir = root.path.join(".lab").join("experiments").join("overrides");
        ensure_dir(&overrides_dir).expect("overrides dir");
        fs::write(
            overrides_dir.join("providers.a.ts"),
            "export const P='A';\n",
        )
        .expect("patch A");
        fs::write(
            overrides_dir.join("providers.b.ts"),
            "export const P='B';\n",
        )
        .expect("patch B");
        fs::write(
            overrides_dir.join("defaults.bench-lmstudio-headless.json"),
            "{\n  \"models\": {\"default\": \"\"}\n}\n",
        )
        .expect("defaults config");
        fs::write(
            root.path.join("defaults.bench-lmstudio-headless.json"),
            "{\n  \"models\": {\"default\": \"\"}\n}\n",
        )
        .expect("root defaults config");
        let codex_auth_dir = overrides_dir.join(".config").join("nova");
        ensure_dir(&codex_auth_dir).expect("codex auth dir");
        fs::write(codex_auth_dir.join("codex-auth.json"), "{}\n").expect("codex auth");

        let benchmark_grader_dir = root.path.join("bench").join("integration").join("agentlab");
        ensure_dir(&benchmark_grader_dir).expect("benchmark grader dir");
        fs::write(
            benchmark_grader_dir.join("bench_benchmark_adapter.py"),
            "#!/usr/bin/env python3\nprint('ok')\n",
        )
        .expect("benchmark adapter");
        let swebench_adapter_dir = root.path.join("adapters").join("swebench");
        ensure_dir(&swebench_adapter_dir).expect("swebench adapter dir");
        fs::write(
            swebench_adapter_dir.join("swebench_task_container_grader.py"),
            "#!/usr/bin/env python3\nprint('ok')\n",
        )
        .expect("swebench benchmark adapter");
        fs::write(
            swebench_adapter_dir.join("_swebench_meta.py"),
            "def extract_swebench_meta(payload):\n    return {\"repo\": None, \"instance_id\": None, \"base_commit\": None}\n",
        )
        .expect("swebench meta helper");
        root
    }

    fn minimal_dx_spec() -> Value {
        json!({
            "experiment": {
                "id": "bench_v0_qwen35b_a3b_only",
                "name": "Bench v0: Qwen3.5 35B A3B",
                "tags": ["bench-v0", "single-variant"]
            },
            "benchmark": "bench_v0",
            "limit": 20,
            "agent": {
                "artifact": "rex-minimal-linux-dir",
                "image": "python:3.11-slim",
                "command": [
                    "rex",
                    "run",
                    "--dangerous",
                    "--config",
                    "defaults.bench-lmstudio-headless.json",
                    "--provider",
                    "$model_provider",
                    "--model",
                    "$model"
                ],
                "env": { "MEMORY_DAEMON_URL": "" },
                "source_commit": "deadbeef"
            },
            "baseline": {
                "id": "qwen_35b_a3b",
                "bindings": { "model_provider": "lmstudio", "model": "qwen3.5-35b-a3b" }
            },
            "overrides": {
                "network": "full"
            }
        })
    }

    fn minimal_new_dx_spec() -> Value {
        json!({
            "experiment": {
                "id": "bench_v0_multi_build",
                "name": "Bench v0 Multi Build",
                "tags": ["bench-v0", "multi-build"]
            },
            "benchmark": "bench_v0",
            "limit": 10,
            "agent_builds": [
                {
                    "id": "rex_default",
                    "artifact": "rex-minimal-linux-dir",
                    "image": "python:3.11-slim",
                    "command": [
                        "rex",
                        "run",
                        "--dangerous",
                        "--config",
                        "defaults.bench-lmstudio-headless.json",
                        "--provider",
                        "$model_provider",
                        "--model",
                        "$model"
                    ]
                },
                {
                    "id": "rex_alt",
                    "artifact": "rex-minimal-linux-dir",
                    "image": "python:3.11-slim",
                    "command": [
                        "rex",
                        "run",
                        "--alternate",
                        "--config",
                        "defaults.bench-lmstudio-headless.json",
                        "--provider",
                        "$model_provider",
                        "--model",
                        "$model"
                    ]
                }
            ],
            "variants": [
                {
                    "id": "qwen",
                    "baseline": true,
                    "agent_ref": "rex_default",
                    "env": { "BASELINE_ONLY": "1" },
                    "config": { "model_provider": "lmstudio", "model": "qwen3.5-35b-a3b" }
                },
                {
                    "id": "sonnet",
                    "agent_ref": "rex_alt",
                    "env": { "ANTHROPIC_REGION": "us" },
                    "config": { "model_provider": "anthropic", "model": "claude-sonnet-4" }
                }
            ],
            "overrides": {
                "network": "full"
            }
        })
    }

    fn minimal_swebench_dx_spec() -> Value {
        json!({
            "experiment": {
                "id": "swebench_lite_qwen35b_a3b_only",
                "name": "SWE-bench Lite: Qwen3.5 35B A3B",
                "tags": ["swebench-lite", "single-variant"]
            },
            "benchmark": "swebench_lite",
            "limit": 20,
            "agent": {
                "artifact": "rex-minimal-linux-dir",
                "image": "python:3.11-slim",
                "command": [
                    "rex",
                    "run",
                    "--dangerous",
                    "--provider",
                    "$model_provider",
                    "--model",
                    "$model"
                ],
                "env": { "MEMORY_DAEMON_URL": "" },
                "source_commit": "deadbeef"
            },
            "baseline": {
                "id": "qwen_35b_a3b",
                "bindings": { "model_provider": "lmstudio", "model": "qwen3.5-35b-a3b" }
            },
            "overrides": {
                "network": "full"
            }
        })
    }

    fn write_executable_script(path: &Path, contents: &str) {
        fs::write(path, contents).expect("write script");
        #[cfg(unix)]
        {
            let mut perms = fs::metadata(path).expect("script metadata").permissions();
            perms.set_mode(0o755);
            fs::set_permissions(path, perms).expect("script executable perms");
        }
    }

    fn write_preflight_result_agent(path: &Path) {
        write_executable_script(
            path,
            concat!(
                "#!/usr/bin/env python3\n",
                "from __future__ import annotations\n",
                "import pathlib\n",
                "import sys\n",
                "\n",
                "def main() -> int:\n",
                "    out = None\n",
                "    args = sys.argv[1:]\n",
                "    idx = 0\n",
                "    while idx < len(args):\n",
                "        if args[idx] == '--output' and idx + 1 < len(args):\n",
                "            out = args[idx + 1]\n",
                "            idx += 2\n",
                "            continue\n",
                "        idx += 1\n",
                "    if not out:\n",
                "        raise SystemExit('missing --output')\n",
                "    target = pathlib.Path(out)\n",
                "    target.parent.mkdir(parents=True, exist_ok=True)\n",
                "    target.write_text('{\"schema_version\":\"agent_result_v1\",\"outcome\":\"success\"}\\n', encoding='utf-8')\n",
                "    return 0\n",
                "\n",
                "raise SystemExit(main())\n",
            ),
        );
    }

    fn write_preflight_benchmark_grader(path: &Path) {
        write_executable_script(
            path,
            concat!(
                "#!/usr/bin/env python3\n",
                "from __future__ import annotations\n",
                "import json\n",
                "import os\n",
                "import pathlib\n",
                "\n",
                "def _write(path: str, payload: dict) -> None:\n",
                "    target = pathlib.Path(path)\n",
                "    target.parent.mkdir(parents=True, exist_ok=True)\n",
                "    target.write_text(json.dumps(payload, separators=(',', ':')) + '\\n', encoding='utf-8')\n",
                "\n",
                "ids = {\n",
                "    'run_id': 'run_preflight',\n",
                "    'trial_id': 'trial_preflight',\n",
                "    'variant_id': 'variant_preflight',\n",
                "    'task_id': os.environ.get('AGENTLAB_TASK_ID', 'task_preflight'),\n",
                "    'repl_idx': 0,\n",
                "}\n",
                "identity = {\n",
                "    'schedule_idx': 0,\n",
                "    'slot_commit_id': 'slot_preflight',\n",
                "    'attempt': 1,\n",
                "    'row_seq': 0,\n",
                "}\n",
                "benchmark = {\n",
                "    'adapter_id': 'test_adapter',\n",
                "    'name': 'test_bench',\n",
                "    'split': 'test',\n",
                "}\n",
                "_write('/agentlab/out/mapped_grader_output.json', {\n",
                "    'schema_version': 'trial_conclusion_v1',\n",
                "    'payload': {'resolved': 1.0},\n",
                "    'reported_outcome': 'success',\n",
                "    'primary_metric': {'name': 'resolved', 'value': 1.0},\n",
                "    'grader': {'name': 'test_grader', 'strategy': 'in_task_image'},\n",
                "})\n",
            ),
        );
    }

    #[test]
    fn p0_i01_and_p0_i05_dx_registry_resolution_is_complete_and_deterministic() {
        let root = create_dx_authoring_fixture("agentlab_p0_registry_resolution");
        let spec = minimal_dx_spec();
        let resolved_a =
            normalize_experiment_authoring(spec.clone(), &root.path, &root.path).expect("first");
        let resolved_b =
            normalize_experiment_authoring(spec.clone(), &root.path, &root.path).expect("second");
        assert_eq!(
            resolved_a, resolved_b,
            "normalized output must be deterministic"
        );

        assert_eq!(
            resolved_a
                .pointer("/dataset/suite_id")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "bench_v0"
        );
        assert_eq!(
            resolved_a
                .pointer("/benchmark/adapter/command/1")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/bench_benchmark_adapter.py"
        );
        assert_eq!(
            resolved_a
                .pointer("/design/comparison")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "none"
        );
        assert_eq!(
            resolved_a
                .pointer("/design/policies/scheduling")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "variant_sequential"
        );
        let artifact_digest = resolved_a
            .pointer("/runtime/agent/bundle_digest")
            .and_then(Value::as_str)
            .unwrap_or("");
        assert!(
            artifact_digest.starts_with("sha256:"),
            "artifact digest missing: {}",
            artifact_digest
        );
    }

    #[test]
    fn normalize_authoring_keeps_public_command_and_env_explicit() {
        let root = create_dx_authoring_fixture("agentlab_dx_first_class_agent_fields");
        let spec = json!({
            "experiment": {
                "id": "bench_v0_first_class_agent_fields",
                "name": "Bench v0 First-Class Agent Fields",
                "tags": ["bench-v0", "dx"]
            },
            "benchmark": "bench_v0",
            "agent": {
                "artifact": "rex-minimal-linux-dir",
                "image": "python:3.11-slim",
                "command": [
                    "rex",
                    "run",
                    "--dangerous",
                    "--config",
                    "defaults.bench-lmstudio-headless.json",
                    "--provider",
                    "$model_provider",
                    "--api-key",
                    "$ZAI_CODER_API_KEY"
                ],
                "env": {
                    "REX_CONFIG_PATH": "defaults.bench-lmstudio-headless.json"
                }
            },
            "baseline": {
                "id": "glm_5",
                "bindings": { "model_provider": "z.ai-coder", "model": "glm-5" }
            }
        });
        let resolved =
            normalize_experiment_authoring(spec, &root.path, &root.path).expect("normalize");

        let command = resolved
            .pointer("/runtime/agent_runtime/command")
            .and_then(Value::as_array)
            .expect("runtime command array");
        let command_tokens = command.iter().filter_map(Value::as_str).collect::<Vec<_>>();
        assert!(
            command_tokens
                .windows(2)
                .any(|w| w[0] == "--config" && w[1] == "defaults.bench-lmstudio-headless.json"),
            "command should keep public --config token as-authored: {:?}",
            command_tokens
        );
        assert!(
            command_tokens
                .windows(2)
                .any(|w| w[0] == "--api-key" && w[1] == "$ZAI_CODER_API_KEY"),
            "command should keep public runtime binding in argv: {:?}",
            command_tokens
        );
        assert_eq!(
            resolved
                .pointer("/runtime/agent_runtime/env/REX_CONFIG_PATH")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "defaults.bench-lmstudio-headless.json"
        );
    }

    #[test]
    fn normalize_authoring_uses_runner_repo_for_bench_builtin_runtime_assets() {
        let root = create_dx_authoring_fixture("agentlab_dx_bench_builtin_runtime_assets_root");
        fs::write(
            root.path.join("defaults.bench-lmstudio-headless.json"),
            "{\n  \"models\": {\"default\": \"\"}\n}\n",
        )
        .expect("root defaults config");
        fs::write(root.path.join("providers.a.ts"), "export const P='A';\n")
            .expect("root provider A");
        fs::write(root.path.join("providers.b.ts"), "export const P='B';\n")
            .expect("root provider B");
        let patch_source = root
            .path
            .join("packages")
            .join("core")
            .join("types")
            .join("src");
        ensure_dir(&patch_source).expect("patch source dir");
        fs::write(
            patch_source.join("providers.ts"),
            "export const PROVIDERS = [];\n",
        )
        .expect("patch source");
        ensure_dir(&root.path.join("bench").join("agentlab")).expect("local bench dir");
        fs::write(
            root.path
                .join("bench")
                .join("agentlab")
                .join("placeholder.txt"),
            "local bench placeholder",
        )
        .expect("local bench file");
        let spec = minimal_new_dx_spec();
        let resolved =
            normalize_experiment_authoring(spec, &root.path, &root.path).expect("normalize");

        let source = resolved
            .pointer("/benchmark/grader/_runtime_assets/0/build_source_path")
            .and_then(Value::as_str)
            .expect("bench support file source");
        let expected = builtin_benchmark_assets_root()
            .expect("builtin assets root")
            .join("bench");

        assert_eq!(PathBuf::from(source), expected);
        assert_ne!(PathBuf::from(source), root.path.join("bench"));
    }

    #[test]
    fn normalize_authoring_supports_swebench_lite_builtin_registry() {
        let root = create_dx_authoring_fixture("agentlab_dx_swebench_builtin");
        fs::write(root.path.join("providers.a.ts"), "export const P='A';\n")
            .expect("root provider A");
        fs::write(root.path.join("providers.b.ts"), "export const P='B';\n")
            .expect("root provider B");
        let patch_source = root
            .path
            .join("packages")
            .join("core")
            .join("types")
            .join("src");
        ensure_dir(&patch_source).expect("patch source dir");
        fs::write(
            patch_source.join("providers.ts"),
            "export const PROVIDERS = [];\n",
        )
        .expect("patch source");
        let resolved =
            normalize_experiment_authoring(minimal_swebench_dx_spec(), &root.path, &root.path)
                .expect("normalize");

        assert_eq!(
            resolved
                .pointer("/dataset/suite_id")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "swebench_lite_curated"
        );
        assert!(
            resolved
                .pointer("/dataset/path")
                .and_then(Value::as_str)
                .unwrap_or("")
                .ends_with(".lab/experiments/data/swebench_lite_curated.task_spec.jsonl"),
            "unexpected swebench dataset path: {:?}",
            resolved.pointer("/dataset/path")
        );
        assert_eq!(
            resolved
                .pointer("/benchmark/grader/command/1")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/swebench/swebench_task_container_grader.py"
        );
        assert_eq!(
            resolved
                .pointer("/benchmark/policy/scoring_lifecycle")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "integrated_score"
        );
        assert_eq!(
            resolved
                .pointer("/benchmark/grader/_runtime_assets/0/runtime_path")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/swebench"
        );
        let source = resolved
            .pointer("/benchmark/grader/_runtime_assets/0/build_source_path")
            .and_then(Value::as_str)
            .expect("swebench support file source");
        let expected = builtin_benchmark_assets_root()
            .expect("builtin assets root")
            .join("adapters")
            .join("swebench");
        assert_eq!(PathBuf::from(source), expected);
        assert_ne!(
            PathBuf::from(source),
            root.path.join("adapters").join("swebench")
        );
    }

    #[test]
    fn normalize_authoring_supports_agent_builds_and_variant_agent_refs() {
        let root = create_dx_authoring_fixture("agentlab_dx_v2_agent_builds");
        let spec = minimal_new_dx_spec();
        let resolved =
            normalize_experiment_authoring(spec, &root.path, &root.path).expect("normalize");

        assert_eq!(
            resolved
                .pointer("/baseline/variant_id")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "qwen"
        );
        assert_eq!(
            resolved
                .pointer("/runtime/agent_runtime/command/6")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "$model_provider"
        );
        assert_eq!(
            resolved
                .pointer("/runtime/agent_runtime/command/8")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "$model"
        );
        assert_eq!(
            resolved
                .pointer("/variant_plan/0/variant_id")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "sonnet"
        );
        let baseline_command = resolved
            .pointer("/runtime/agent_runtime/command")
            .and_then(Value::as_array)
            .expect("baseline command");
        let baseline_tokens = baseline_command
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert!(
            baseline_tokens
                .windows(2)
                .any(|w| { w[0] == "--config" && w[1] == "defaults.bench-lmstudio-headless.json" }),
            "baseline command should keep authored relative config path: {:?}",
            baseline_tokens
        );
        assert!(
            baseline_tokens
                .windows(2)
                .any(|w| w[0] == "--provider" && w[1] == "$model_provider"),
            "baseline command should keep public provider binding in argv: {:?}",
            baseline_tokens
        );
        assert_eq!(
            resolved
                .pointer("/variant_plan/0/runtime_overrides/agent_runtime/command/2")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "--alternate"
        );
        assert_eq!(
            resolved
                .pointer("/variant_plan/0/runtime_overrides/agent_runtime/env/ANTHROPIC_REGION")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "us"
        );
        let override_command = resolved
            .pointer("/variant_plan/0/runtime_overrides/agent_runtime/command")
            .and_then(Value::as_array)
            .expect("override command");
        let override_tokens = override_command
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert!(
            override_tokens
                .windows(2)
                .any(|w| { w[0] == "--config" && w[1] == "defaults.bench-lmstudio-headless.json" }),
            "variant override command should keep authored relative config path: {:?}",
            override_tokens
        );
        assert!(
            override_tokens
                .windows(2)
                .any(|w| w[0] == "--provider" && w[1] == "$model_provider"),
            "variant override command should keep public provider binding in argv: {:?}",
            override_tokens
        );
    }

    #[test]
    fn normalize_authoring_rejects_bindings_to_args_alias() {
        let root = create_dx_authoring_fixture("agentlab_dx_bindings_to_args_alias");
        let mut spec = minimal_dx_spec();
        let agent = spec
            .pointer_mut("/agent")
            .and_then(Value::as_object_mut)
            .expect("agent object");
        agent.remove("arg_map");
        agent.insert(
            "bindings_to_args".to_string(),
            json!([
                { "binding": "model_provider", "flag": "--provider" },
                { "binding": "model", "flag": "--model" }
            ]),
        );

        let err = normalize_experiment_authoring(spec, &root.path, &root.path)
            .expect_err("bindings_to_args alias should be rejected");
        assert!(
            err.to_string().contains("agent.bindings_to_args"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn build_experiment_package_rewrites_runtime_sources() {
        let root = create_dx_authoring_fixture("agentlab_build_package");
        let mut spec = minimal_new_dx_spec();
        if let Some(builds) = spec
            .pointer_mut("/agent_builds")
            .and_then(Value::as_array_mut)
        {
            for build in builds {
                if let Some(obj) = build.as_object_mut() {
                    obj.remove("provider_env");
                }
            }
        }
        let spec_path = root.path.join("experiment.yaml");
        fs::write(&spec_path, serde_yaml::to_string(&spec).expect("yaml")).expect("write spec");

        let out_dir = root.path.join("package");
        let build =
            build_experiment_package(&spec_path, None, Some(&out_dir)).expect("build package");
        assert!(build.manifest_path.exists(), "manifest missing");
        assert!(build.checksums_path.exists(), "checksums missing");
        assert!(
            build.package_dir.join(STAGING_MANIFEST_FILE).exists(),
            "runtime staging manifest missing"
        );

        let manifest = load_json_file(&build.manifest_path).expect("manifest json");
        assert_eq!(
            manifest
                .pointer("/schema_version")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "sealed_run_package_v2"
        );
        assert_eq!(
            manifest
                .pointer("/resolved_experiment/dataset/path")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "tasks/tasks.jsonl"
        );
        let packaged_tasks =
            load_jsonl_value_rows(&build.package_dir.join("tasks").join("tasks.jsonl"))
                .expect("packaged tasks");
        assert_eq!(packaged_tasks.len(), 1);
        let packaged_task_row = parse_task_row(&packaged_tasks[0]).expect("packaged task row");
        assert_eq!(packaged_task_row.schema_version, "task_row_v1");
        assert_eq!(
            packaged_task_row.materialization.kind,
            TaskMaterializationKind::BaseImageBundle
        );
        let bundle_ref = packaged_task_row
            .materialization
            .task_bundle_ref
            .as_deref()
            .expect("bundle ref");
        assert!(
            build.package_dir.join(bundle_ref).exists(),
            "packaged task bundle missing: {}",
            bundle_ref
        );
        let artifact = manifest
            .pointer("/resolved_experiment/runtime/agent_runtime/artifact")
            .and_then(Value::as_str)
            .unwrap_or("");
        assert!(
            artifact.starts_with("agent_builds/"),
            "artifact path should be packaged, got {}",
            artifact
        );
        assert_eq!(
            manifest
                .pointer("/resolved_experiment/runtime/agent_runtime/command/4")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/defaults.bench-lmstudio-headless.json"
        );
        assert!(
            build
                .package_dir
                .join(PACKAGED_RUNTIME_ASSETS_DIR)
                .join("defaults.bench-lmstudio-headless.json")
                .exists(),
            "relative command path should be copied into packaged runtime assets"
        );
        let staging_manifest = load_json_file(&build.package_dir.join(STAGING_MANIFEST_FILE))
            .expect("staging manifest");
        assert_eq!(
            staging_manifest
                .pointer("/schema_version")
                .and_then(Value::as_str)
                .unwrap_or(""),
            STAGING_MANIFEST_SCHEMA_VERSION
        );
        assert!(
            staging_manifest
                .pointer("/variants/qwen")
                .and_then(Value::as_array)
                .is_some_and(|entries| entries.iter().any(|entry| {
                    entry.pointer("/runtime_path").and_then(Value::as_str)
                        == Some("__AGENTLAB_TASK_WORKDIR__/.agentlab/support/bench")
                })),
            "qwen variant should include benchmark support directory staging entry"
        );
        assert!(
            staging_manifest
                .pointer("/variants/qwen")
                .and_then(Value::as_array)
                .is_some_and(|entries| entries.iter().any(|entry| {
                    entry.pointer("/runtime_path")
                        .and_then(Value::as_str)
                        == Some("__AGENTLAB_TASK_WORKDIR__/.agentlab/support/defaults.bench-lmstudio-headless.json")
                        && entry.pointer("/packaged_path").and_then(Value::as_str)
                            == Some("runtime_assets/defaults.bench-lmstudio-headless.json")
                })),
            "qwen variant should include rewritten runtime config staging entry"
        );
        assert!(
            build
                .package_dir
                .join(bundle_ref)
                .join("README.md")
                .exists(),
            "task-owned workspace inputs should be sealed into the task bundle"
        );

        let summary = describe_experiment(&build.package_dir).expect("describe package");
        assert_eq!(summary.exp_id, "bench_v0_multi_build");
        assert_eq!(summary.task_count, 1);
    }

    #[test]
    fn build_experiment_package_accepts_legacy_bench_builtin_grader_paths() {
        let root = create_dx_authoring_fixture("agentlab_build_package_legacy_bench_builtin");
        let spec = minimal_dx_spec();
        let spec_path = root.path.join("experiment.yaml");
        fs::write(&spec_path, serde_yaml::to_string(&spec).expect("yaml")).expect("write spec");

        let out_dir = root.path.join("package");
        let build =
            build_experiment_package(&spec_path, None, Some(&out_dir)).expect("build package");
        let manifest = load_json_file(&build.manifest_path).expect("manifest json");

        assert_eq!(
            manifest
                .pointer("/resolved_experiment/benchmark/grader/command/1")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/bench/integration/agentlab/bench_benchmark_adapter.py"
        );
        let staging_manifest = load_json_file(&build.package_dir.join(STAGING_MANIFEST_FILE))
            .expect("staging manifest");
        assert!(
            staging_manifest
                .pointer("/variants/qwen_35b_a3b")
                .and_then(Value::as_array)
                .is_some_and(|entries| entries.iter().any(|entry| {
                    entry.pointer("/runtime_path").and_then(Value::as_str)
                        == Some("__AGENTLAB_TASK_WORKDIR__/.agentlab/support/bench")
                })),
            "benchmark grader support directory should be staged for the legacy baseline variant"
        );
    }

    #[test]
    fn rewrite_benchmark_paths_for_package_stages_conclusion_mapper_into_task_workdir_support() {
        let root = TempDirGuard::new("agentlab_stage_benchmark_mapper");
        let exp_dir = root.path.join("exp");
        let package_dir = root.path.join("package");
        ensure_dir(&exp_dir).expect("exp dir");
        ensure_dir(&package_dir).expect("package dir");
        ensure_dir(&exp_dir.join("scripts")).expect("scripts dir");
        ensure_dir(&exp_dir.join("mappers")).expect("mappers dir");
        fs::write(
            exp_dir.join("scripts").join("grader.py"),
            "#!/usr/bin/env python3\n",
        )
        .expect("grader script");
        fs::write(
            exp_dir.join("mappers").join("normalize.py"),
            "#!/usr/bin/env python3\n",
        )
        .expect("mapper script");

        let mut benchmark_root = json!({
            "grader": {
                "command": ["python3", "./scripts/grader.py"],
                "conclusion": {
                    "mode": "mapper",
                    "mapper": "./mappers/normalize.py"
                }
            }
        });
        let mut file_copies = BTreeMap::new();
        let mut file_counter = 0usize;
        let mut public_path_copies = BTreeMap::new();
        let mut staging_manifest_entries = Vec::new();

        rewrite_benchmark_paths_for_package(
            &mut benchmark_root,
            &exp_dir,
            &package_dir,
            &mut file_copies,
            &mut file_counter,
            &mut public_path_copies,
            &mut staging_manifest_entries,
        )
        .expect("rewrite benchmark paths");

        assert_eq!(
            benchmark_root
                .pointer("/grader/conclusion/mapper")
                .and_then(Value::as_str),
            Some("__AGENTLAB_TASK_WORKDIR__/.agentlab/support/mappers/normalize.py")
        );
        assert!(
            package_dir
                .join(PACKAGED_RUNTIME_ASSETS_DIR)
                .join("mappers")
                .join("normalize.py")
                .exists(),
            "mapper should be staged into packaged runtime assets"
        );
        assert!(
            staging_manifest_entries.iter().any(|entry| {
                entry.runtime_path
                    == "__AGENTLAB_TASK_WORKDIR__/.agentlab/support/mappers/normalize.py"
                    && entry.packaged_path == "runtime_assets/mappers/normalize.py"
            }),
            "staging manifest should include mapper contract path"
        );
    }

    #[test]
    fn build_experiment_package_uses_builtin_dataset_path_override() {
        let root = create_dx_authoring_fixture("agentlab_build_dataset_path_override");
        let custom_dir = root.path.join("custom");
        ensure_dir(&custom_dir).expect("custom dataset dir");
        fs::write(
            custom_dir.join("tasks_override.jsonl"),
            concat!(
                r#"{"schema_version":"task_row_v1","id":"TASK_OVERRIDE","image":"python:3.11-slim","workdir":"/workspace/task","task":{"id":"TASK_OVERRIDE"},"materialization":{"kind":"task_image"}}"#,
                "\n"
            ),
        )
        .expect("override dataset");

        let mut spec = minimal_dx_spec();
        set_json_pointer_value(
            &mut spec,
            "/dataset",
            json!({ "path": "custom/tasks_override.jsonl" }),
        )
        .expect("set dataset override");
        let spec_path = root.path.join("experiment.yaml");
        fs::write(&spec_path, serde_yaml::to_string(&spec).expect("yaml")).expect("write spec");

        let build = build_experiment_package(&spec_path, None, Some(&root.path.join("package")))
            .expect("build package");
        let packaged_tasks =
            load_jsonl_value_rows(&build.package_dir.join("tasks").join("tasks.jsonl"))
                .expect("packaged tasks");
        assert_eq!(packaged_tasks.len(), 1);
        let packaged_task_row = parse_task_row(&packaged_tasks[0]).expect("packaged task row");
        assert_eq!(packaged_task_row.id.as_str(), "TASK_OVERRIDE");
    }

    #[test]
    fn build_experiment_package_fails_fast_on_invalid_task_row() {
        let root = create_dx_authoring_fixture("agentlab_build_package_invalid_task_spec");
        fs::write(
            root.path
                .join(".lab")
                .join("experiments")
                .join("data")
                .join("bench_v0.task_spec.jsonl"),
            "{\"id\":\"TASK001\",\"image\":\"python:3.11-slim\",\"workdir\":\"/workspace/task\",\"task\":{\"id\":\"TASK001\"},\"materialization\":{\"kind\":\"task_image\"}}\n",
        )
        .expect("invalid task row dataset");
        let spec = minimal_dx_spec();
        let spec_path = root.path.join("experiment.yaml");
        fs::write(&spec_path, serde_yaml::to_string(&spec).expect("yaml")).expect("write spec");

        let err = build_experiment_package(&spec_path, None, Some(&root.path.join("package")))
            .expect_err("invalid task row should fail build");
        assert!(
            err.to_string().contains("task_row_v1"),
            "unexpected build error: {}",
            err
        );
    }

    #[test]
    fn compile_tasks_for_package_seals_workspace_inputs_into_task_bundles() {
        let root = TempDirGuard::new("agentlab_compile_tasks_for_package");
        let dataset_bundle_src = root.path.join("dataset_bundle_src");
        ensure_dir(&dataset_bundle_src).expect("dataset bundle src");
        fs::write(dataset_bundle_src.join("README.md"), "dataset pack\n").expect("pack file");

        let git_bundle_src = root.path.join("git_bundle_src");
        ensure_dir(&git_bundle_src).expect("git bundle src");
        fs::write(git_bundle_src.join("README.md"), "git checkout bundle\n").expect("git bundle");

        let package_dir = root.path.join("package");
        ensure_dir(&package_dir).expect("package dir");
        let dataset_path = root.path.join("tasks.jsonl");
        fs::write(&dataset_path, "").expect("dataset file");

        let task_values = vec![
            base_image_bundle_task_row(
                "task_dataset",
                "python:3.11-slim",
                "/workspace/task",
                dataset_bundle_src.to_string_lossy().as_ref(),
            ),
            base_image_bundle_task_row(
                "task_git",
                "python:3.11-slim",
                "/workspace/task",
                git_bundle_src.to_string_lossy().as_ref(),
            ),
        ];
        let packaged_tasks = compile_tasks_for_package(
            &task_values,
            &root.path,
            &root.path,
            &dataset_path,
            &package_dir,
        )
        .expect("compile packaged tasks");
        assert_eq!(packaged_tasks.len(), 2);
        let dataset_row = parse_task_row(&packaged_tasks[0]).expect("dataset row");
        let git_row = parse_task_row(&packaged_tasks[1]).expect("git row");

        assert_eq!(
            fs::read_to_string(
                package_dir
                    .join(
                        dataset_row
                            .materialization
                            .task_bundle_ref
                            .as_deref()
                            .expect("dataset bundle ref")
                    )
                    .join("README.md")
            )
            .expect("packaged dataset bundle"),
            "dataset pack\n"
        );

        assert!(
            package_dir
                .join(
                    git_row
                        .materialization
                        .task_bundle_ref
                        .as_deref()
                        .expect("git bundle ref")
                )
                .join("README.md")
                .exists(),
            "explicit task bundle contents should be sealed into the package"
        );
    }

    #[test]
    fn build_experiment_package_rejects_external_exec_shim_artifact() {
        let root = create_dx_authoring_fixture("agentlab_build_reject_external_exec");
        let artifact_bin = root
            .path
            .join(".lab")
            .join("agents")
            .join("rex-external-exec")
            .join("bin");
        ensure_dir(&artifact_bin).expect("artifact dir");
        write_executable_script(
            &artifact_bin.join("rex"),
            "#!/usr/bin/env sh\nexec /usr/local/bin/bun /workspace/packages/apps/launcher/index.ts \"$@\"\n",
        );

        let mut spec = minimal_new_dx_spec();
        if let Some(builds) = spec
            .pointer_mut("/agent_builds")
            .and_then(Value::as_array_mut)
        {
            for build in builds {
                if let Some(obj) = build.as_object_mut() {
                    obj.insert("artifact".to_string(), json!("rex-external-exec"));
                }
            }
        }
        let spec_path = root.path.join("experiment.yaml");
        fs::write(&spec_path, serde_yaml::to_string(&spec).expect("yaml")).expect("write spec");

        let err = build_experiment_package(&spec_path, None, Some(&root.path.join("package")))
            .expect_err("external shim artifact should fail");
        assert!(
            err.to_string().contains("image-resident path"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn build_experiment_package_rejects_opt_agent_script_delegate() {
        let root = create_dx_authoring_fixture("agentlab_build_reject_opt_agent_script");
        let artifact_bin = root
            .path
            .join(".lab")
            .join("agents")
            .join("rex-opt-agent-script")
            .join("bin");
        ensure_dir(&artifact_bin).expect("artifact dir");
        write_executable_script(
            &artifact_bin.join("rex"),
            "#!/usr/bin/env sh\nexec /opt/agent/bin/bun /opt/agent/packages/apps/launcher/dist/index.js \"$@\"\n",
        );

        let mut spec = minimal_new_dx_spec();
        if let Some(builds) = spec
            .pointer_mut("/agent_builds")
            .and_then(Value::as_array_mut)
        {
            for build in builds {
                if let Some(obj) = build.as_object_mut() {
                    obj.insert("artifact".to_string(), json!("rex-opt-agent-script"));
                }
            }
        }
        let spec_path = root.path.join("experiment.yaml");
        fs::write(&spec_path, serde_yaml::to_string(&spec).expect("yaml")).expect("write spec");

        let err = build_experiment_package(&spec_path, None, Some(&root.path.join("package")))
            .expect_err("artifact script delegate should fail");
        assert!(
            err.to_string().contains("readable script path"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn build_experiment_package_accepts_explicit_artifact_command_without_entrypoint_shim() {
        let root = create_dx_authoring_fixture("agentlab_build_explicit_artifact_command");
        let artifact_root = root
            .path
            .join(".lab")
            .join("agents")
            .join("rex-explicit-command");
        let artifact_bin = artifact_root.join("bin");
        let script_dir = artifact_root
            .join("packages")
            .join("apps")
            .join("launcher")
            .join("dist");
        ensure_dir(&artifact_bin).expect("artifact bin dir");
        ensure_dir(&script_dir).expect("script dir");
        write_executable_script(&artifact_bin.join("bun"), "#!/bin/sh\nexit 0\n");
        write_executable_script(
            &artifact_bin.join("rex"),
            "#!/usr/bin/env sh\nexec /usr/local/bin/bun /workspace/packages/apps/launcher/index.ts \"$@\"\n",
        );
        fs::write(script_dir.join("index.js"), "console.log('ok');\n").expect("launcher");

        let mut spec = minimal_new_dx_spec();
        if let Some(builds) = spec
            .pointer_mut("/agent_builds")
            .and_then(Value::as_array_mut)
        {
            for build in builds {
                if let Some(obj) = build.as_object_mut() {
                    obj.insert("artifact".to_string(), json!("rex-explicit-command"));
                    obj.insert(
                        "command".to_string(),
                        json!([
                            "/opt/agent/bin/bun",
                            "/opt/agent/packages/apps/launcher/dist/index.js"
                        ]),
                    );
                }
            }
        }
        let spec_path = root.path.join("experiment.yaml");
        fs::write(&spec_path, serde_yaml::to_string(&spec).expect("yaml")).expect("write spec");

        build_experiment_package(&spec_path, None, Some(&root.path.join("package")))
            .expect("explicit artifact command should pass");
    }

    #[test]
    fn build_experiment_package_rejects_artifact_not_referenced_by_command() {
        let root = create_dx_authoring_fixture("agentlab_build_reject_no_executable");
        let artifact_root = root
            .path
            .join(".lab")
            .join("agents")
            .join("rex-empty-artifact");
        ensure_dir(&artifact_root).expect("artifact dir");
        fs::write(artifact_root.join("README.md"), "no executables here").expect("readme");

        let mut spec = minimal_new_dx_spec();
        if let Some(builds) = spec
            .pointer_mut("/agent_builds")
            .and_then(Value::as_array_mut)
        {
            for build in builds {
                if let Some(obj) = build.as_object_mut() {
                    obj.insert("artifact".to_string(), json!("rex-empty-artifact"));
                }
            }
        }
        let spec_path = root.path.join("experiment.yaml");
        fs::write(&spec_path, serde_yaml::to_string(&spec).expect("yaml")).expect("write spec");

        let err = build_experiment_package(&spec_path, None, Some(&root.path.join("package")))
            .expect_err("non-executable artifact should fail");
        assert!(
            err.to_string()
                .contains("did not resolve to artifact executable"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p0_i06_preflight_grader_reachability_rejects_forbidden_opt_bench_path() {
        if !docker_runtime_available() {
            eprintln!("skipping p0_i06 test: docker daemon unavailable");
            return;
        }
        ensure_docker_test_image("python:3.11-slim");

        let benchmark_config = BenchmarkConfig {
            policy: BenchmarkPolicyConfig::default(),
            grader: Some(BenchmarkGraderConfig::in_task_image(vec![
                "python3".to_string(),
                "/opt/bench/bench_benchmark_adapter.py".to_string(),
            ])),
            adapter: None,
        };
        let mut runtime_profile =
            preflight_test_runtime_profile(ImageSource::Global, Some("python:3.11-slim"));
        runtime_profile.agent_runtime.io = AgentRuntimeIoConfig {
            input_arg: "--input-file".to_string(),
            output_arg: "--output".to_string(),
        };
        let tasks = vec![task_row_value(
            "TASK001",
            "python:3.11-slim",
            "/workspace/task",
            None,
        )];

        let variant = preflight_test_variant();
        let root = TempDirGuard::new("agentlab_p0_grader_reachability_forbidden");
        let check = check_benchmark_grader_reachable(
            &benchmark_config,
            &runtime_profile,
            &variant,
            &tasks,
            &root.path,
        );
        assert!(
            !check.passed,
            "preflight must fail when grader script path is under forbidden /opt/bench"
        );
        assert!(
            check
                .message
                .contains("forbidden benchmark adapter script path"),
            "unexpected message: {}",
            check.message
        );
    }

    #[test]
    fn p0_i06_preflight_grader_reachability_allows_runner_staged_deps_script_path() {
        if !docker_runtime_available() {
            eprintln!("skipping p0_i06 staged-script test: docker daemon unavailable");
            return;
        }
        ensure_docker_test_image("python:3.11-slim");

        let benchmark_config = BenchmarkConfig {
            policy: BenchmarkPolicyConfig::default(),
            grader: Some(BenchmarkGraderConfig::in_task_image(vec![
                "python3".to_string(),
                task_workdir_support_destination_path("bench_benchmark_adapter.py"),
            ])),
            adapter: None,
        };
        let mut runtime_profile =
            preflight_test_runtime_profile(ImageSource::Global, Some("python:3.11-slim"));
        let variant = preflight_test_variant();
        let root = TempDirGuard::new("agentlab_p0_grader_reachability_staged");
        let staged_agent = root.path.join("preflight_agent.py");
        let staged_grader = root.path.join("bench_benchmark_adapter.py");
        write_preflight_result_agent(&staged_agent);
        write_preflight_benchmark_grader(&staged_grader);
        runtime_profile.agent_runtime.command_raw = vec![
            "python3".to_string(),
            task_workdir_support_destination_path("preflight_agent.py"),
        ];
        runtime_profile.agent_runtime.dependency_file_staging = vec![
            DependencyFileStagingSpec {
                source_from_host: staged_agent,
                destination_path: task_workdir_support_destination_path("preflight_agent.py"),
                required: true,
                read_only: true,
            },
            DependencyFileStagingSpec {
                source_from_host: staged_grader,
                destination_path: task_workdir_support_destination_path(
                    "bench_benchmark_adapter.py",
                ),
                required: true,
                read_only: true,
            },
        ];
        let check = check_benchmark_grader_reachable(
            &benchmark_config,
            &runtime_profile,
            &variant,
            &[],
            &root.path,
        );
        assert!(
            check.passed,
            "runner-staged script path should not be required in task image: {}",
            check.message
        );
    }

    #[test]
    fn p0_i06_preflight_grader_reachability_supports_swebench_grader_probe_contract() {
        if !docker_runtime_available() {
            eprintln!("skipping p0_i06 swebench grader probe test: docker daemon unavailable");
            return;
        }
        ensure_docker_test_image("python:3.11-slim");

        let benchmark_config = BenchmarkConfig {
            policy: BenchmarkPolicyConfig::default(),
            grader: Some(BenchmarkGraderConfig::in_task_image(vec![
                "python3".to_string(),
                task_workdir_support_destination_path("swebench_task_container_grader.py"),
            ])),
            adapter: None,
        };
        let mut runtime_profile =
            preflight_test_runtime_profile(ImageSource::Global, Some("python:3.11-slim"));
        let variant = preflight_test_variant();
        let root = TempDirGuard::new("agentlab_p0_swebench_grader_reachability");
        let staged_agent = root.path.join("preflight_agent.py");
        write_preflight_result_agent(&staged_agent);
        runtime_profile.agent_runtime.command_raw = vec![
            "python3".to_string(),
            task_workdir_support_destination_path("preflight_agent.py"),
        ];
        runtime_profile.agent_runtime.dependency_file_staging = vec![
            DependencyFileStagingSpec {
                source_from_host: staged_agent,
                destination_path: task_workdir_support_destination_path("preflight_agent.py"),
                required: true,
                read_only: true,
            },
            DependencyFileStagingSpec {
                source_from_host: PathBuf::from(env!("CARGO_MANIFEST_DIR").replace(
                    "/rust/crates/lab-runner",
                    "/adapters/swebench/swebench_task_container_grader.py",
                )),
                destination_path: task_workdir_support_destination_path(
                    "swebench_task_container_grader.py",
                ),
                required: true,
                read_only: true,
            },
            DependencyFileStagingSpec {
                source_from_host: PathBuf::from(env!("CARGO_MANIFEST_DIR").replace(
                    "/rust/crates/lab-runner",
                    "/adapters/swebench/_swebench_meta.py",
                )),
                destination_path: task_workdir_support_destination_path("_swebench_meta.py"),
                required: true,
                read_only: true,
            },
        ];
        let tasks = vec![json!({
            "schema_version": "task_row_v1",
            "id": "swebench_astropy_astropy_12907",
            "image": "swebench/sweb.eval.x86_64.astropy__astropy-12907:latest",
            "workdir": "/testbed",
            "task": {
                "id": "swebench_astropy_astropy_12907",
                "benchmark": {
                    "adapter_id": "swebench_task_container_grader",
                    "name": "swebench_lite_curated",
                    "split": "test"
                },
                "swebench": {
                    "input": {
                        "repo": "astropy/astropy",
                        "instance_id": "astropy__astropy-12907",
                        "base_commit": "deadbeef"
                    }
                }
            },
            "materialization": {
                "kind": "task_image"
            }
        })];
        let check = check_benchmark_grader_reachable(
            &benchmark_config,
            &runtime_profile,
            &variant,
            &tasks,
            &root.path,
        );
        assert!(
            check.passed,
            "swebench grader contract smoke should pass with staged agent result: {}",
            check.message
        );
    }

    #[test]
    fn p0_container_mount_args_use_contract_io_mounts_without_host_workspace_bind() {
        let (root, paths) = create_trial_paths_fixture("agentlab_p0_no_dataset_mount");
        let runtime = legacy_contract_runtime_fixture();
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let runtime_experiment = json!({
            "runtime": {
                "policy": {
                    "sandbox": {
                        "hardening": {
                            "no_new_privileges": true,
                            "drop_all_caps": true
                        }
                    }
                }
            }
        });
        let dynamic_mounts = vec![ResolvedMountReference {
            host_path: root.path.join("fixture-pack"),
            mount_path: format!("{}/dataset_pack", AGENTLAB_CONTRACT_WORKSPACE_DIR),
        }];
        fs::write(&dynamic_mounts[0].host_path, "fixture").expect("fixture pack");
        let request = AdapterRunRequest {
            runtime_experiment: &runtime_experiment,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &paths,
            dynamic_mounts: &dynamic_mounts,
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: None,
            benchmark_grading_enabled: false,
            run_id: "run_1",
            task_image: "python:3.11-slim",
            task_workdir: "/workspace/task",
            task_materialization_kind: TaskMaterializationKind::TaskImage,
            agent_artifact: None,
        };
        let spec = crate::trial::execution::build_container_spec(
            &request,
            request.task_image,
            "/workspace/task",
            request.network_mode,
            false,
            &[],
        );
        let mounts = &spec.mounts;
        assert!(
            mounts
                .iter()
                .any(|mount| mount.container_path == AGENTLAB_CONTRACT_IN_DIR && mount.read_only),
            "missing in-dir mount: {:?}",
            mounts
        );
        assert!(
            !mounts
                .iter()
                .any(|mount| mount.container_path == "/workspace/task"),
            "task container should not bind the host workspace into the task workdir: {:?}",
            mounts
        );
        assert!(
            mounts
                .iter()
                .any(|mount| mount.container_path == AGENTLAB_CONTRACT_OUT_DIR && !mount.read_only),
            "missing out-dir mount: {:?}",
            mounts
        );
        assert!(
            !mounts
                .iter()
                .any(|mount| mount.container_path == "/dataset"),
            "legacy /dataset mount should not be present: {:?}",
            mounts
        );
    }

    #[test]
    fn p0_base_image_bundle_avoids_host_workspace_bind_mount() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_p0_base_image_bundle_mount");
        let runtime = legacy_contract_runtime_fixture();
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let runtime_experiment = json!({});
        let request = AdapterRunRequest {
            runtime_experiment: &runtime_experiment,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &paths,
            dynamic_mounts: &[],
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: None,
            benchmark_grading_enabled: false,
            run_id: "run_1",
            task_image: "python:3.11-slim",
            task_workdir: "/workspace/task",
            task_materialization_kind: TaskMaterializationKind::BaseImageBundle,
            agent_artifact: None,
        };
        let spec = crate::trial::execution::build_container_spec(
            &request,
            request.task_image,
            "/workspace/task",
            request.network_mode,
            false,
            &[],
        );
        let mounts = &spec.mounts;
        assert!(
            !mounts
                .iter()
                .any(|mount| mount.container_path == "/workspace/task"
                    || mount.container_path == AGENTLAB_CONTRACT_WORKSPACE_DIR),
            "base_image_bundle should copy into the task workdir instead of keeping host workspace binds: {:?}",
            mounts
        );
    }

    #[test]
    fn p0_i03_injected_container_env_includes_agent_path() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_p0_path_env");
        let mut runtime = legacy_contract_runtime_fixture();
        runtime.command_raw = vec!["rex".to_string(), "run".to_string()];
        runtime.image = "image:latest".to_string();
        runtime.sandbox_image = Some("image:latest".to_string());
        runtime.execution = agent_execution_fixture(Some("image:latest"));
        runtime.agent_artifact = PathBuf::from("/tmp/agent-artifact");
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let empty_json = json!({});
        let request = AdapterRunRequest {
            runtime_experiment: &empty_json,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &paths,
            dynamic_mounts: &[],
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: None,
            benchmark_grading_enabled: false,
            run_id: "run_1",
            task_image: "image:latest",
            task_workdir: "/workspace/task",
            task_materialization_kind: TaskMaterializationKind::TaskImage,
            agent_artifact: Some(runtime.agent_artifact.as_path()),
        };
        let args = build_exec_env(&request, "/workspace/task", None, true);
        assert!(
            args.get("PATH").is_some_and(|value| {
                value
                    == AGENT_ARTIFACT_PATH_ENV_VALUE
                        .split_once('=')
                        .map(|(_, value)| value)
                        .unwrap_or_default()
            }),
            "PATH injection env missing: {:?}",
            args
        );
    }

    #[test]
    fn runtime_command_workspace_binding_tracks_declared_task_workdir() {
        let rendered = resolve_agent_runtime_command(
            &["agent".to_string(), "$WORKSPACE".to_string()],
            &json!({}),
            &BTreeMap::new(),
        )
        .expect("render command");
        assert_eq!(rendered[1], TASK_WORKDIR_TEMPLATE_PLACEHOLDER);

        let (_root, paths) = create_trial_paths_fixture("agentlab_runtime_workspace_binding");
        let mut runtime = legacy_contract_runtime_fixture();
        runtime.command_raw = rendered;
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let empty_json = json!({});
        let request = AdapterRunRequest {
            runtime_experiment: &empty_json,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &BTreeMap::new(),
            runtime_overrides_env: &BTreeMap::new(),
            trial_paths: &paths,
            dynamic_mounts: &[],
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: None,
            benchmark_grading_enabled: false,
            run_id: "run_1",
            task_image: "python:3.11-slim",
            task_workdir: "/workspace/task",
            task_materialization_kind: TaskMaterializationKind::TaskImage,
            agent_artifact: None,
        };
        let resolved = resolve_runtime_agent_command(&request).expect("resolve runtime command");
        assert_eq!(
            resolved,
            vec!["agent".to_string(), "/workspace/task".to_string()]
        );
    }

    #[test]
    fn build_exec_env_replaces_workspace_placeholder() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_workspace_env_placeholder");
        let runtime = legacy_contract_runtime_fixture();
        let mut runtime_env = BTreeMap::new();
        runtime_env.insert(
            "CONFIG_DIR".to_string(),
            format!("{}/config", TASK_WORKDIR_TEMPLATE_PLACEHOLDER),
        );
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let empty_json = json!({});
        let request = AdapterRunRequest {
            runtime_experiment: &empty_json,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &BTreeMap::new(),
            trial_paths: &paths,
            dynamic_mounts: &[],
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: None,
            benchmark_grading_enabled: false,
            run_id: "run_1",
            task_image: "python:3.11-slim",
            task_workdir: "/workspace/task",
            task_materialization_kind: TaskMaterializationKind::TaskImage,
            agent_artifact: None,
        };
        let args = build_exec_env(&request, "/workspace/task", None, true);
        assert!(
            args.get("CONFIG_DIR") == Some(&"/workspace/task/config".to_string()),
            "workspace placeholder should resolve in container env: {:?}",
            args
        );
        assert!(
            args.get("WORKSPACE") == Some(&"/workspace/task".to_string()),
            "WORKSPACE env should match the declared task workdir: {:?}",
            args
        );
    }

    #[test]
    fn preflight_benchmark_smoke_ignores_grade_error_marker_when_mapped_output_is_valid() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_preflight_marker_ignore");
        atomic_write_json_pretty(
            &paths.out.join(MAPPED_GRADER_OUTPUT_FILENAME),
            &json!({
                "schema_version": "trial_conclusion_v1",
                "payload": { "resolved": 1.0 },
                "reported_outcome": "success",
                "primary_metric": { "name": "resolved", "value": 1.0 },
                "grader": { "name": "test_grader", "strategy": "in_task_image" }
            }),
        )
        .expect("mapped output");
        fs::write(
            paths.out.join(BENCHMARK_GRADE_ERROR_FILENAME),
            "mapper_command_failed:1\n",
        )
        .expect("grade marker");
        let runtime = legacy_contract_runtime_fixture();
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let empty_json = json!({});
        let grader = BenchmarkGraderConfig::in_task_image(vec![
            "python3".to_string(),
            task_workdir_support_destination_path("grader.py"),
        ]);
        let request = AdapterRunRequest {
            runtime_experiment: &empty_json,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &paths,
            dynamic_mounts: &[],
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: Some(&grader),
            benchmark_grading_enabled: true,
            run_id: "run_1",
            task_image: "python:3.11-slim",
            task_workdir: "/workspace/task",
            task_materialization_kind: TaskMaterializationKind::TaskImage,
            agent_artifact: None,
        };
        let failures = validate_preflight_benchmark_smoke_outputs(
            &request,
            &BENCHMARK_GRADING_POLICY_EXIT_CODE.to_string(),
        );
        assert!(
            failures.is_empty(),
            "valid mapped output should suppress grade-error marker failures: {:?}",
            failures
        );
    }

    #[test]
    fn validate_benchmark_grading_contract_accepts_hidden_asset_isolation_plan() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_hidden_asset_guard");
        let runtime = legacy_contract_runtime_fixture();
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let empty_json = json!({});
        let grader = BenchmarkGraderConfig {
            strategy: GradingStrategy::InTaskImage,
            command: vec![
                "python3".to_string(),
                task_workdir_support_destination_path("grader.py"),
            ],
            conclusion: GraderConclusionConfig::default(),
            in_task_image: Some(InTaskImageGradingConfig {
                hidden_paths: vec!["/testbed/.hidden".to_string()],
                revealed_paths: vec!["/testbed/.hidden".to_string()],
            }),
            injected: None,
            separate: None,
        };
        let request = AdapterRunRequest {
            runtime_experiment: &empty_json,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &paths,
            dynamic_mounts: &[],
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: Some(&grader),
            benchmark_grading_enabled: true,
            run_id: "run_1",
            task_image: "python:3.11-slim",
            task_workdir: "/testbed",
            task_materialization_kind: TaskMaterializationKind::TaskImage,
            agent_artifact: None,
        };

        crate::trial::grade::validate_benchmark_grading_contract(&request)
            .expect("hidden asset isolation should now be supported");
    }

    #[test]
    fn validate_benchmark_grading_contract_rejects_mismatched_hidden_asset_visibility_lists() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_hidden_asset_guard_lengths");
        let runtime = legacy_contract_runtime_fixture();
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let empty_json = json!({});
        let grader = BenchmarkGraderConfig {
            strategy: GradingStrategy::InTaskImage,
            command: vec![
                "python3".to_string(),
                task_workdir_support_destination_path("grader.py"),
            ],
            conclusion: GraderConclusionConfig::default(),
            in_task_image: Some(InTaskImageGradingConfig {
                hidden_paths: vec!["/testbed/.hidden".to_string()],
                revealed_paths: vec![
                    "/testbed/.hidden".to_string(),
                    "/testbed/.hidden_extra".to_string(),
                ],
            }),
            injected: None,
            separate: None,
        };
        let request = AdapterRunRequest {
            runtime_experiment: &empty_json,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &paths,
            dynamic_mounts: &[],
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: Some(&grader),
            benchmark_grading_enabled: true,
            run_id: "run_1",
            task_image: "python:3.11-slim",
            task_workdir: "/testbed",
            task_materialization_kind: TaskMaterializationKind::TaskImage,
            agent_artifact: None,
        };

        let err = crate::trial::grade::validate_benchmark_grading_contract(&request)
            .expect_err("mismatched hidden/revealed lengths should fail");
        assert!(
            err.to_string().contains("matching lengths"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p7_execute_trial_runtime_base_image_bundle_copies_workspace_in_and_out() {
        if !docker_runtime_available() {
            eprintln!("skipping base-image-bundle runtime test: docker daemon unavailable");
            return;
        }
        ensure_docker_test_image("python:3.11-slim");

        let root = TempDirGuard::new("agentlab_p7_base_image_bundle_runtime");
        let bundle_dir = root.path.join("task_bundle");
        ensure_dir(&bundle_dir.join("src")).expect("bundle src");
        fs::write(bundle_dir.join("src/main.py"), "print('ok')\n").expect("bundle source");

        let agent_bundle = ensure_test_agent_bundle(&root.path, "base-image-bundle-agent");
        write_executable_script(
            &agent_bundle.join("bin/agent.sh"),
            concat!(
                "#!/bin/sh\n",
                "set -e\n",
                "find \"$WORKSPACE\" -maxdepth 2 -print >&2 || true\n",
                "test -f \"$WORKSPACE/src/main.py\"\n",
                "printf 'generated\\n' > \"$WORKSPACE/generated.txt\"\n",
                "printf '%s' '{\"schema_version\":\"agent_result_v1\",\"outcome\":\"success\",\"checkpoints\":[]}' > /agentlab/out/result.json\n",
            ),
        );

        let mut runtime = legacy_contract_runtime_fixture();
        runtime.command_raw = vec!["/bin/sh".to_string(), "/opt/agent/bin/agent.sh".to_string()];
        runtime.image = "python:3.11-slim".to_string();
        runtime.sandbox_image = Some("python:3.11-slim".to_string());
        runtime.execution = agent_execution_fixture(Some("python:3.11-slim"));
        runtime.agent_artifact = agent_bundle.clone();

        let task = base_image_bundle_task_row(
            "task_1",
            "python:3.11-slim",
            "/workspace/task",
            bundle_dir.to_string_lossy().as_ref(),
        );
        let task_boundary = parse_task_boundary_from_packaged_task(&task).expect("task boundary");
        let variant = preflight_test_variant();
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");
        let runtime_experiment = json!({
            "policy": {
                "task_sandbox": {
                    "hardening": {
                        "no_new_privileges": true,
                        "drop_all_caps": true
                    }
                }
            }
        });

        let prepared = prepare_task_environment(
            &root.path,
            &trial_dir,
            "run_1",
            "trial_1",
            &runtime_experiment,
            &variant,
            0,
            0,
            &task_boundary,
            &runtime,
        )
        .expect("prepare task environment");
        let task_sandbox_plan = prepared
            .manifest
            .task_sandbox_plan
            .clone()
            .expect("task sandbox plan");
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let request = AdapterRunRequest {
            runtime_experiment: &runtime_experiment,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &prepared.trial_paths,
            dynamic_mounts: &prepared.dynamic_mounts,
            io_paths: &prepared.io_paths,
            network_mode: "none",
            benchmark_grader: None,
            benchmark_grading_enabled: false,
            run_id: "run_1",
            task_image: &task_boundary.task_image,
            task_workdir: &task_boundary.task_workdir,
            task_materialization_kind: task_boundary.materialization.kind.clone(),
            agent_artifact: Some(runtime.agent_artifact.as_path()),
        };

        let outcome = crate::trial::execution::execute_trial_runtime(
            &trial_dir,
            0,
            1,
            &request,
            &task_boundary.task_id,
            &variant.id,
            0,
            &task_sandbox_plan,
        )
        .expect("execute trial runtime");

        assert_eq!(
            outcome.agent_exit_status,
            "0",
            "agent stdout:\n{}\nagent stderr:\n{}",
            fs::read_to_string(trial_dir.join("harness_stdout.log")).unwrap_or_default(),
            fs::read_to_string(trial_dir.join("harness_stderr.log")).unwrap_or_default()
        );
        assert_eq!(
            fs::read_to_string(prepared.trial_paths.workspace.join("generated.txt"))
                .expect("generated workspace file"),
            "generated\n"
        );
    }

    #[test]
    fn p7_execute_trial_runtime_hides_in_task_image_assets_until_grading() {
        if !docker_runtime_available() {
            eprintln!("skipping in-task-image hidden asset test: docker daemon unavailable");
            return;
        }

        let root = TempDirGuard::new("agentlab_p7_hidden_asset_runtime");
        let image = build_docker_test_image(
            &root.path,
            "hidden-assets",
            concat!(
                "FROM python:3.11-slim\n",
                "RUN mkdir -p /workspace/task/.hidden\n",
                "RUN python3 - <<'PY'\n",
                "from pathlib import Path\n",
                "Path('/workspace/task/.hidden/grader.py').write_text(",
                "\"from pathlib import Path\\n\"",
                "\"agent_file = Path('/workspace/task/agent_visible.txt')\\n\"",
                "\"if not agent_file.exists():\\n    raise SystemExit('missing agent output')\\n\"",
                "\"Path('/agentlab/out/mapped_grader_output.json').write_text('{\\\"schema_version\\\":\\\"trial_conclusion_v1\\\",\\\"payload\\\":{\\\"resolved\\\":1.0},\\\"reported_outcome\\\":\\\"success\\\",\\\"primary_metric\\\":{\\\"name\\\":\\\"resolved\\\",\\\"value\\\":1.0},\\\"grader\\\":{\\\"name\\\":\\\"test_grader\\\",\\\"strategy\\\":\\\"in_task_image\\\"}}')\\n\"",
                ")\n",
                "PY\n",
                "WORKDIR /workspace/task\n",
            ),
        );

        let agent_bundle = ensure_test_agent_bundle(&root.path, "hidden-assets-agent");
        write_executable_script(
            &agent_bundle.join("bin/agent.sh"),
            concat!(
                "#!/bin/sh\n",
                "set -e\n",
                "if [ -e \"$WORKSPACE/.hidden/grader.py\" ]; then\n",
                "  echo 'hidden grader asset leaked into agent step' >&2\n",
                "  exit 17\n",
                "fi\n",
                "printf 'agent-visible\\n' > \"$WORKSPACE/agent_visible.txt\"\n",
                "printf '%s' '{\"schema_version\":\"agent_result_v1\",\"outcome\":\"success\",\"checkpoints\":[]}' > /agentlab/out/result.json\n",
            ),
        );

        let mut runtime = legacy_contract_runtime_fixture();
        runtime.command_raw = vec!["/bin/sh".to_string(), "/opt/agent/bin/agent.sh".to_string()];
        runtime.image = image.clone();
        runtime.sandbox_image = Some(image.clone());
        runtime.execution = agent_execution_fixture(Some(&image));
        runtime.agent_artifact = agent_bundle.clone();

        let grader = BenchmarkGraderConfig {
            strategy: GradingStrategy::InTaskImage,
            command: vec![
                "python3".to_string(),
                "/workspace/task/.hidden/grader.py".to_string(),
            ],
            conclusion: GraderConclusionConfig::default(),
            in_task_image: Some(InTaskImageGradingConfig {
                hidden_paths: vec!["/workspace/task/.hidden/grader.py".to_string()],
                revealed_paths: vec!["/workspace/task/.hidden/grader.py".to_string()],
            }),
            injected: None,
            separate: None,
        };
        let task = task_row_value("task_hidden", &image, "/workspace/task", Some(30_000));
        let task_boundary = parse_task_boundary_from_packaged_task(&task).expect("task boundary");
        let variant = preflight_test_variant();
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).expect("trial dir");
        let runtime_experiment = json!({
            "policy": {
                "task_sandbox": {
                    "hardening": {
                        "no_new_privileges": true,
                        "drop_all_caps": true
                    }
                }
            }
        });

        let prepared = prepare_task_environment(
            &root.path,
            &trial_dir,
            "run_1",
            "trial_1",
            &runtime_experiment,
            &variant,
            0,
            0,
            &task_boundary,
            &runtime,
        )
        .expect("prepare task environment");
        let task_sandbox_plan = prepared
            .manifest
            .task_sandbox_plan
            .clone()
            .expect("task sandbox plan");
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let request = AdapterRunRequest {
            runtime_experiment: &runtime_experiment,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &prepared.trial_paths,
            dynamic_mounts: &prepared.dynamic_mounts,
            io_paths: &prepared.io_paths,
            network_mode: "none",
            benchmark_grader: Some(&grader),
            benchmark_grading_enabled: true,
            run_id: "run_1",
            task_image: &task_boundary.task_image,
            task_workdir: &task_boundary.task_workdir,
            task_materialization_kind: task_boundary.materialization.kind.clone(),
            agent_artifact: Some(runtime.agent_artifact.as_path()),
        };

        let outcome = crate::trial::execution::execute_trial_runtime(
            &trial_dir,
            0,
            1,
            &request,
            &task_boundary.task_id,
            &variant.id,
            0,
            &task_sandbox_plan,
        )
        .expect("execute trial runtime");

        assert_eq!(
            outcome.agent_exit_status,
            "0",
            "agent stdout:\n{}\nagent stderr:\n{}",
            fs::read_to_string(trial_dir.join("harness_stdout.log")).unwrap_or_default(),
            fs::read_to_string(trial_dir.join("harness_stderr.log")).unwrap_or_default()
        );
        assert!(
            outcome.trial_conclusion_row.is_some(),
            "grader should produce a mapped conclusion; grader stdout:\n{}\ngrader stderr:\n{}\nmapper stdout:\n{}\nmapper stderr:\n{}\ngrade_error_reason={:?}",
            fs::read_to_string(trial_dir.join("grader_stdout.log")).unwrap_or_default(),
            fs::read_to_string(trial_dir.join("grader_stderr.log")).unwrap_or_default(),
            fs::read_to_string(trial_dir.join("mapper_stdout.log")).unwrap_or_default(),
            fs::read_to_string(trial_dir.join("mapper_stderr.log")).unwrap_or_default(),
            outcome.grade_error_reason
        );
        assert_eq!(
            fs::read_to_string(prepared.trial_paths.workspace.join("agent_visible.txt"))
                .expect("agent-visible workspace file"),
            "agent-visible\n"
        );
    }

    #[test]
    fn validate_benchmark_grading_contract_rejects_missing_grader_command() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_missing_grader_command");
        let runtime = legacy_contract_runtime_fixture();
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let empty_json = json!({});
        let grader = BenchmarkGraderConfig {
            strategy: GradingStrategy::InTaskImage,
            command: Vec::new(),
            conclusion: GraderConclusionConfig::default(),
            in_task_image: Some(InTaskImageGradingConfig::default()),
            injected: None,
            separate: None,
        };
        let request = AdapterRunRequest {
            runtime_experiment: &empty_json,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &paths,
            dynamic_mounts: &[],
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: Some(&grader),
            benchmark_grading_enabled: true,
            run_id: "run_1",
            task_image: "python:3.11-slim",
            task_workdir: "/workspace/task",
            task_materialization_kind: TaskMaterializationKind::TaskImage,
            agent_artifact: None,
        };

        let err = crate::trial::grade::validate_benchmark_grading_contract(&request)
            .expect_err("missing grader command should be rejected");
        assert!(
            err.to_string().contains("no grader command resolved"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_benchmark_grading_contract_rejects_missing_mapper_command() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_missing_mapper_command");
        let runtime = legacy_contract_runtime_fixture();
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let empty_json = json!({});
        let grader = BenchmarkGraderConfig {
            strategy: GradingStrategy::InTaskImage,
            command: vec![
                "python3".to_string(),
                task_workdir_support_destination_path("grader.py"),
            ],
            conclusion: GraderConclusionConfig {
                mode: GraderConclusionMode::Mapper,
                mapper: None,
            },
            in_task_image: Some(InTaskImageGradingConfig::default()),
            injected: None,
            separate: None,
        };
        let request = AdapterRunRequest {
            runtime_experiment: &empty_json,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &paths,
            dynamic_mounts: &[],
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: Some(&grader),
            benchmark_grading_enabled: true,
            run_id: "run_1",
            task_image: "python:3.11-slim",
            task_workdir: "/workspace/task",
            task_materialization_kind: TaskMaterializationKind::TaskImage,
            agent_artifact: None,
        };

        let err = crate::trial::grade::validate_benchmark_grading_contract(&request)
            .expect_err("missing mapper command should be rejected");
        assert!(
            err.to_string()
                .contains("benchmark.grader.conclusion.mapper is required"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p0_i03_swebench_container_commands_request_explicit_platform() {
        let (_root, paths) = create_trial_paths_fixture("agentlab_p0_swebench_platform");
        let mut runtime = legacy_contract_runtime_fixture();
        runtime.command_raw = vec!["rex".to_string(), "run".to_string()];
        runtime.image = "swebench/sweb.eval.x86_64.astropy__astropy-12907:latest".to_string();
        runtime.sandbox_image =
            Some("swebench/sweb.eval.x86_64.astropy__astropy-12907:latest".to_string());
        runtime.execution = agent_execution_fixture(Some(
            "swebench/sweb.eval.x86_64.astropy__astropy-12907:latest",
        ));
        runtime.agent_artifact = PathBuf::from("/tmp/agent-artifact");
        let runtime_env = BTreeMap::new();
        let overrides = BTreeMap::new();
        let io_paths = prepared_trial_io_fixture(
            paths.out.join("result.json"),
            paths.state.join("events.jsonl"),
        );
        let empty_json = json!({});
        let request = AdapterRunRequest {
            runtime_experiment: &empty_json,
            runtime: &runtime,
            variant_args: &[],
            runtime_env: &runtime_env,
            runtime_overrides_env: &overrides,
            trial_paths: &paths,
            dynamic_mounts: &[],
            io_paths: &io_paths,
            network_mode: "none",
            benchmark_grader: None,
            benchmark_grading_enabled: false,
            run_id: "run_1",
            task_image: "swebench/sweb.eval.x86_64.astropy__astropy-12907:latest",
            task_workdir: "/testbed",
            task_materialization_kind: TaskMaterializationKind::TaskImage,
            agent_artifact: Some(runtime.agent_artifact.as_path()),
        };
        let mut spec = crate::trial::execution::build_container_spec(
            &request,
            "swebench/sweb.eval.x86_64.astropy__astropy-12907:latest",
            "/testbed",
            request.network_mode,
            false,
            &[],
        );
        spec.platform = resolve_container_platform(request.task_image).map(str::to_string);
        assert!(
            spec.platform.as_deref() == Some("linux/amd64"),
            "task sandbox spec missing explicit platform: {:?}",
            spec.platform
        );

        assert!(
            !spec
                .mounts
                .iter()
                .any(|mount| mount.container_path == "/opt/agent"),
            "task sandbox spec must not mount the agent bundle: {:?}",
            spec.mounts
        );
    }

    #[test]
    fn p0_i04_artifact_digest_pin_rejects_mutation() {
        let root = TempDirGuard::new("agentlab_p0_artifact_digest_pin");
        let artifact_dir = root.path.join("artifact");
        ensure_dir(&artifact_dir).expect("artifact dir");
        fs::write(artifact_dir.join("agent.txt"), "v1").expect("artifact v1");
        let digest_before = compute_artifact_content_digest(&artifact_dir).expect("digest before");
        fs::write(artifact_dir.join("agent.txt"), "v2").expect("artifact v2");
        let mut runtime = legacy_contract_runtime_fixture();
        runtime.command_raw = vec!["rex".to_string()];
        runtime.image = "image".to_string();
        runtime.sandbox_image = Some("image".to_string());
        runtime.execution = agent_execution_fixture(Some("image"));
        runtime.agent_artifact = artifact_dir.clone();
        runtime.agent_artifact_digest = Some(format!("sha256:{}", digest_before));
        runtime.agent_artifact_resolved_path = Some(artifact_dir.clone());
        let err = validate_agent_artifact_pin(&runtime).expect_err("digest mismatch expected");
        assert!(
            err.to_string().contains("digest mismatch"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn p0_i06_and_p1_i06_canonical_example_has_no_boundary_leaks_or_cp_hacks() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .canonicalize()
            .expect("repo root");
        let canonical = repo_root
            .join(".lab")
            .join("experiments")
            .join("bench_v0_qwen35b_a3b_only.yaml");
        let content = fs::read_to_string(&canonical).expect("canonical experiment fixture");
        assert!(
            !content.contains("/opt/agent/bin/"),
            "canonical fixture must not require internal /opt/agent/bin path"
        );
        assert!(
            !content.contains("cp /agentlab/deps"),
            "canonical fixture must not contain shell cp glue"
        );
    }

    // ───────────────────────────────────────────────────────────────────
    // Batch 1: Pure Functions & Data Utilities
    // ───────────────────────────────────────────────────────────────────

    #[test]
    fn sanitize_name_for_path_strips_special_chars() {
        assert_eq!(
            sanitize_name_for_path("hello world.v2/3"),
            "hello_world_v2_3"
        );
    }

    #[test]
    fn sanitize_name_for_path_all_special_returns_experiment() {
        assert_eq!(sanitize_name_for_path("@#$%^&"), "experiment");
    }

    #[test]
    fn sanitize_name_for_path_trims_leading_trailing_underscores() {
        assert_eq!(sanitize_name_for_path("__hello__"), "hello");
    }

    #[test]
    fn sanitize_name_for_path_preserves_alphanumeric_hyphen() {
        assert_eq!(sanitize_name_for_path("a-b_c"), "a-b_c");
    }

    #[test]
    fn sanitize_name_for_path_empty_returns_experiment() {
        assert_eq!(sanitize_name_for_path(""), "experiment");
    }

    #[test]
    fn sanitize_name_for_path_unicode_to_underscores() {
        let result = sanitize_name_for_path("café");
        assert!(!result.contains('é'), "unicode char should be replaced");
    }

    #[test]
    fn sanitize_name_for_path_numbers_preserved() {
        assert_eq!(sanitize_name_for_path("test123"), "test123");
    }

    #[test]
    fn sanitize_name_for_path_mixed_special_and_alpha() {
        let result = sanitize_name_for_path("my@experiment.v2");
        assert!(!result.contains('@'));
        assert!(!result.contains('.'));
        assert!(result.contains("my"));
        assert!(result.contains("v2"));
    }

    #[test]
    fn sanitize_name_for_path_only_underscores() {
        assert_eq!(sanitize_name_for_path("___"), "experiment");
    }

    #[test]
    fn sanitize_name_for_path_single_char() {
        assert_eq!(sanitize_name_for_path("x"), "x");
    }

    #[test]
    fn experiment_workload_type_reads_explicit_value() {
        let spec = json!({"version": "0.5", "experiment": {"workload_type": "agent_runtime"}});
        assert_eq!(experiment_workload_type(&spec).unwrap(), "agent_runtime");
    }

    #[test]
    fn experiment_workload_type_empty_string_fails() {
        let spec = json!({"experiment": {"workload_type": "  "}});
        let err = experiment_workload_type(&spec).unwrap_err();
        assert!(err
            .to_string()
            .contains("missing /experiment/workload_type"));
    }

    #[test]
    fn experiment_workload_type_missing_field_fails() {
        let spec = json!({"experiment": {"id": "e1"}});
        assert!(experiment_workload_type(&spec).is_err());
    }

    #[test]
    fn experiment_workload_type_trimmed() {
        let spec = json!({"version": "0.5", "experiment": {"workload_type": "  agent_runtime  "}});
        assert_eq!(experiment_workload_type(&spec).unwrap(), "agent_runtime");
    }

    #[test]
    fn experiment_random_seed_legacy_reads_design_random_seed() {
        let spec = json!({"design": {"random_seed": 99}});
        assert_eq!(experiment_random_seed(&spec), 99);
    }

    #[test]
    fn experiment_random_seed_defaults_to_one() {
        assert_eq!(experiment_random_seed(&json!({})), 1);
    }

    #[test]
    fn experiment_max_concurrency_clamps_zero_to_one() {
        let spec = json!({"design": {"max_concurrency": 0}});
        assert_eq!(experiment_max_concurrency(&spec), 1);
    }

    #[test]
    fn experiment_max_concurrency_defaults_to_one() {
        assert_eq!(experiment_max_concurrency(&json!({})), 1);
    }

    #[test]
    fn experiment_max_concurrency_preserves_large_value() {
        let spec = json!({"design": {"max_concurrency": 128}});
        assert_eq!(experiment_max_concurrency(&spec), 128);
    }

    #[test]
    fn experiment_max_concurrency_negative_as_json_defaults() {
        let spec = json!({"design": {"max_concurrency": -1}});
        assert_eq!(experiment_max_concurrency(&spec), 1);
    }

    #[test]
    fn configured_network_mode_reads_policy_path() {
        let spec = json!({"policy": {"task_sandbox": {"network": "host"}}});
        assert_eq!(configured_network_mode(&spec).unwrap(), "host");
    }

    #[test]
    fn configured_network_mode_missing_fails() {
        assert!(configured_network_mode(&json!({"policy": {}})).is_err());
    }

    #[test]
    fn configured_network_mode_reads_value() {
        let spec = json!({"policy": {"task_sandbox": {"network": "none"}}});
        assert_eq!(configured_network_mode(&spec).unwrap(), "none");
    }

    #[test]
    fn trial_index_from_trial_id_parses_standard_format() {
        assert_eq!(trial_index_from_trial_id("trial_5"), Some(5));
    }

    #[test]
    fn trial_index_from_trial_id_rejects_non_numeric() {
        assert_eq!(trial_index_from_trial_id("trial_abc"), None);
    }

    #[test]
    fn trial_index_from_trial_id_rejects_no_prefix() {
        assert_eq!(trial_index_from_trial_id("5"), None);
    }

    #[test]
    fn trial_index_from_trial_id_handles_zero() {
        assert_eq!(trial_index_from_trial_id("trial_0"), None);
    }

    #[test]
    fn trial_index_from_trial_id_large_number() {
        assert_eq!(trial_index_from_trial_id("trial_999999"), Some(999999));
    }

    #[test]
    fn trial_index_from_trial_id_empty_suffix() {
        assert_eq!(trial_index_from_trial_id("trial_"), None);
    }

    #[test]
    fn recover_reconciled_status_maps_completed() {
        assert_eq!(recover_reconciled_status("completed"), "completed");
    }

    #[test]
    fn recover_reconciled_status_maps_killed() {
        assert_eq!(recover_reconciled_status("killed"), "killed");
    }

    #[test]
    fn recover_reconciled_status_maps_unknown_to_interrupted() {
        assert_eq!(recover_reconciled_status("running"), "interrupted");
        assert_eq!(recover_reconciled_status("unknown"), "interrupted");
    }

    #[test]
    fn recover_reconciled_status_paused_to_interrupted() {
        assert_eq!(recover_reconciled_status("paused"), "interrupted");
    }

    #[test]
    fn recover_reconciled_status_failed_to_interrupted() {
        assert_eq!(recover_reconciled_status("failed"), "interrupted");
    }

    #[test]
    fn recover_reconciled_status_empty_to_interrupted() {
        assert_eq!(recover_reconciled_status(""), "interrupted");
    }

    #[test]
    fn as_portable_rel_converts_backslashes() {
        assert_eq!(as_portable_rel(Path::new("a\\b\\c")), "a/b/c");
    }

    #[test]
    fn as_portable_rel_preserves_forward_slashes() {
        assert_eq!(as_portable_rel(Path::new("a/b/c")), "a/b/c");
    }

    #[test]
    fn as_portable_rel_mixed_separators() {
        assert_eq!(as_portable_rel(Path::new("a\\b/c\\d")), "a/b/c/d");
    }

    #[test]
    fn as_portable_rel_empty_path() {
        assert_eq!(as_portable_rel(Path::new("")), "");
    }

    #[test]
    fn strip_contract_prefix_exact_match_returns_empty() {
        assert_eq!(strip_contract_prefix("/in", "/in"), Some(""));
    }

    #[test]
    fn strip_contract_prefix_with_subpath_returns_rest() {
        assert_eq!(
            strip_contract_prefix("/in/data.json", "/in"),
            Some("/data.json")
        );
    }

    #[test]
    fn strip_contract_prefix_partial_match_returns_none() {
        assert_eq!(strip_contract_prefix("/infoo", "/in"), None);
    }

    #[test]
    fn strip_contract_prefix_no_slash_boundary_returns_none() {
        assert_eq!(
            strip_contract_prefix("/agentlab/inbox", "/agentlab/in"),
            None
        );
    }

    #[test]
    fn strip_contract_prefix_longer_prefix_returns_none() {
        assert!(strip_contract_prefix("/in", "/in/extra").is_none());
    }

    #[test]
    fn strip_contract_prefix_completely_different_returns_none() {
        assert!(strip_contract_prefix("/out/data", "/in").is_none());
    }

    #[test]
    fn resolve_contract_path_components_maps_all_roots() {
        let cases = vec![
            (AGENTLAB_CONTRACT_IN_DIR, ContractPathRoot::In),
            (AGENTLAB_CONTRACT_OUT_DIR, ContractPathRoot::Out),
        ];
        for (dir, expected_root) in cases {
            let path = format!("{}/file.txt", dir);
            let (root, rest) = resolve_contract_path_components(&path)
                .unwrap_or_else(|| panic!("should resolve {}", dir));
            assert_eq!(root, expected_root, "root mismatch for {}", dir);
            assert_eq!(rest, "/file.txt", "rest mismatch for {}", dir);
        }
    }

    #[test]
    fn resolve_contract_path_components_unknown_root_returns_none() {
        assert!(resolve_contract_path_components("/unknown/path").is_none());
    }

    #[test]
    fn resolve_contract_path_components_exact_root() {
        let (root, rest) = resolve_contract_path_components(AGENTLAB_CONTRACT_IN_DIR).unwrap();
        assert_eq!(root, ContractPathRoot::In);
        assert_eq!(rest, "");
    }

    #[test]
    fn is_dx_contract_authoring_agent_section() {
        assert!(is_dx_contract_authoring(
            &json!({"agent": {"command": ["echo"]}})
        ));
    }

    #[test]
    fn is_dx_contract_authoring_overrides_section() {
        assert!(is_dx_contract_authoring(&json!({"overrides": "path"})));
    }

    #[test]
    fn is_dx_contract_authoring_baseline_id() {
        assert!(is_dx_contract_authoring(&json!({"baseline": {"id": "b1"}})));
    }

    #[test]
    fn is_dx_contract_authoring_benchmark_string() {
        assert!(is_dx_contract_authoring(&json!({"benchmark": "bench_v0"})));
    }

    #[test]
    fn is_dx_contract_authoring_variants_section() {
        assert!(is_dx_contract_authoring(&json!({"variants": []})));
    }

    #[test]
    fn is_dx_contract_authoring_false_for_empty() {
        assert!(!is_dx_contract_authoring(&json!({})));
    }

    #[test]
    fn is_dx_contract_authoring_false_for_legacy() {
        assert!(!is_dx_contract_authoring(&json!({
            "experiment": {"workload_type": "agent_runtime"},
            "runtime": {"agent": {"command": ["echo"]}}
        })));
    }

    // ───────────────────────────────────────────────────────────────────
    // Batch 2: Path & Contract Resolution
    // ───────────────────────────────────────────────────────────────────

    fn test_contract_roots(trial_dir: &Path) -> ContractPathHostRoots {
        ContractPathHostRoots::from_trial_dir(trial_dir)
    }

    #[test]
    fn map_contract_path_container_mode_maps_in_dir() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let result = map_contract_path_to_host(
            &format!("{}/task.json", AGENTLAB_CONTRACT_IN_DIR),
            &roots,
            ContractPathMode::ContainerMount,
        )
        .unwrap();
        assert_eq!(result, trial.join("in").join("task.json"));
    }

    #[test]
    fn map_contract_path_runtime_events_mode_maps_out_dir() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let result = map_contract_path_to_host(
            &format!("{}/events.jsonl", AGENTLAB_CONTRACT_OUT_DIR),
            &roots,
            ContractPathMode::RuntimeEvents,
        )
        .unwrap();
        assert_eq!(result, trial.join("out").join("events.jsonl"));
    }

    #[test]
    fn map_contract_path_container_mode_maps_out_dir() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let result = map_contract_path_to_host(
            &format!("{}/result.json", AGENTLAB_CONTRACT_OUT_DIR),
            &roots,
            ContractPathMode::ContainerMount,
        )
        .unwrap();
        assert_eq!(result, trial.join("out").join("result.json"));
    }

    #[test]
    fn map_contract_path_container_mode_maps_task_support_dir() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let result = map_contract_path_to_host(
            &task_workdir_support_destination_path("dep.tar"),
            &roots,
            ContractPathMode::ContainerMount,
        )
        .unwrap();
        assert_eq!(
            result,
            trial
                .join("workspace")
                .join(AGENTLAB_RUNNER_SUPPORT_REL_DIR)
                .join("dep.tar")
        );
    }

    #[test]
    fn map_contract_path_container_mode_maps_task_workdir_placeholder() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let result = map_contract_path_to_host(
            &format!("{}/src/main.py", AGENTLAB_TASK_WORKDIR_PLACEHOLDER),
            &roots,
            ContractPathMode::ContainerMount,
        )
        .unwrap();
        assert_eq!(result, trial.join("workspace").join("src").join("main.py"));
    }

    #[test]
    fn map_contract_path_container_mode_rejects_empty() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let err = map_contract_path_to_host("", &roots, ContractPathMode::ContainerMount)
            .expect_err("should reject empty");
        assert!(
            err.to_string().contains("empty"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn map_contract_path_container_mode_rejects_relative() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let err =
            map_contract_path_to_host("relative/path", &roots, ContractPathMode::ContainerMount)
                .expect_err("should reject relative");
        assert!(
            err.to_string().contains("absolute"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn map_contract_path_container_mode_trims_whitespace() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let padded = format!("  {}  ", AGENTLAB_CONTRACT_IN_DIR);
        let result =
            map_contract_path_to_host(&padded, &roots, ContractPathMode::ContainerMount).unwrap();
        assert_eq!(result, trial.join("in"));
    }

    #[test]
    fn map_contract_path_runtime_events_mode_rejects_task_support_placeholder() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let err = map_contract_path_to_host(
            &task_workdir_support_destination_path("data.bin"),
            &roots,
            ContractPathMode::RuntimeEvents,
        )
        .expect_err("RuntimeEvents should reject task workdir placeholders");
        assert!(
            err.to_string().contains("absolute"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn map_contract_path_runtime_events_allows_state() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let result = map_contract_path_to_host(
            &format!("{}/events.jsonl", AGENTLAB_CONTRACT_STATE_DIR),
            &roots,
            ContractPathMode::RuntimeEvents,
        )
        .unwrap();
        assert_eq!(result, trial.join("state").join("events.jsonl"));
    }

    #[test]
    fn map_contract_path_nested_subpath_resolves() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let result = map_contract_path_to_host(
            &format!("{}/nested/deep/file.json", AGENTLAB_CONTRACT_IN_DIR),
            &roots,
            ContractPathMode::ContainerMount,
        )
        .unwrap();
        assert_eq!(
            result,
            trial
                .join("in")
                .join("nested")
                .join("deep")
                .join("file.json")
        );
    }

    #[test]
    fn map_contract_path_double_slash_in_subpath() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        let result = map_contract_path_to_host(
            &format!("{}//file.json", AGENTLAB_CONTRACT_IN_DIR),
            &roots,
            ContractPathMode::ContainerMount,
        )
        .unwrap();
        assert!(result.to_string_lossy().contains("file.json"));
    }

    #[test]
    fn map_contract_path_container_mode_unknown_path_fails() {
        let trial = PathBuf::from("/tmp/trial_1");
        let roots = test_contract_roots(&trial);
        assert!(map_contract_path_to_host(
            "/unknown/root/file",
            &roots,
            ContractPathMode::ContainerMount
        )
        .is_err());
    }

    #[test]
    fn mode_allows_root_container_mount_allows_all() {
        for root in [ContractPathRoot::In, ContractPathRoot::Out] {
            assert!(
                mode_allows_root(ContractPathMode::ContainerMount, root),
                "ContainerMount should allow {:?}",
                root
            );
        }
    }

    #[test]
    fn mode_allows_root_runtime_events_allows_out() {
        assert!(mode_allows_root(
            ContractPathMode::RuntimeEvents,
            ContractPathRoot::Out
        ));
    }

    #[test]
    fn find_project_root_from_run_dir_standard_depth() {
        let root = TempDirGuard::new("find_root_std");
        let run_dir = root.path.join(".lab").join("runs").join("run_001");
        ensure_dir(&run_dir).unwrap();
        let found = find_project_root_from_run_dir(&run_dir).unwrap();
        assert_eq!(found, root.path);
    }

    #[test]
    fn find_project_root_from_run_dir_too_shallow_fails() {
        assert!(find_project_root_from_run_dir(Path::new("shallow")).is_err());
    }

    #[test]
    fn contract_path_host_roots_from_trial_dir_creates_expected_dirs() {
        let trial_dir = PathBuf::from("/tmp/trial_1");
        let roots = ContractPathHostRoots::from_trial_dir(&trial_dir);
        assert_eq!(roots.in_dir, trial_dir.join("in"));
        assert_eq!(roots.out_dir, trial_dir.join("out"));
        assert_eq!(roots.workspace_dir, trial_dir.join("workspace"));
    }

    #[test]
    fn resolve_event_path_for_trial_out_events_resolves() {
        let trial = PathBuf::from("/tmp/trial_1");
        let result = resolve_event_path_for_trial(
            &format!("{}/events.jsonl", AGENTLAB_CONTRACT_OUT_DIR),
            &trial,
        )
        .unwrap();
        assert_eq!(result, trial.join("out").join("events.jsonl"));
    }

    #[test]
    fn resolve_event_path_for_trial_rejects_task_support_placeholder() {
        let trial = PathBuf::from("/tmp/trial_1");
        assert!(resolve_event_path_for_trial(
            &task_workdir_support_destination_path("data.bin"),
            &trial
        )
        .is_err());
    }

    #[test]
    fn validate_container_workspace_path_rejects_non_workspace_root() {
        let err = validate_container_workspace_path("/some/other/path").expect_err("should reject");
        assert!(
            err.to_string().contains("mount_path must be under"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_container_workspace_path_exact_match() {
        validate_container_workspace_path(AGENTLAB_CONTRACT_WORKSPACE_DIR).unwrap();
    }

    #[test]
    fn validate_container_workspace_path_subpath() {
        let path = format!("{}/src/main.py", AGENTLAB_CONTRACT_WORKSPACE_DIR);
        validate_container_workspace_path(&path).unwrap();
    }

    #[test]
    fn validate_container_workspace_path_rejects_dot_dot() {
        let path = format!("{}/../escape", AGENTLAB_CONTRACT_WORKSPACE_DIR);
        assert!(validate_container_workspace_path(&path).is_err());
    }

    // ───────────────────────────────────────────────────────────────────
    // Batch 3: Experiment Validation & Normalization
    // ───────────────────────────────────────────────────────────────────

    fn legacy_experiment_base() -> Value {
        json!({
            "experiment": {"workload_type": "agent_runtime"},
            "design": {"sanitization_profile": "standard", "replications": 1},
            "runtime": {
                "policy": {"timeout_ms": 60000},
                "sandbox": {
                    "executor": "docker",
                    "image_source": "global",
                    "image": "img:latest",
                    "profile": "workspace_write",
                    "network": "none"
                },
                "agent": {"command": ["python", "main.py"], "bundle": ".lab/agents/rex-current.tar.gz"}
            },
            "baseline": {"variant_id": "baseline"}
        })
    }

    #[test]
    fn validate_required_fields_batch3_rejects_legacy_v1_shape() {
        let spec = json!({
            "version": "1.0",
            "experiment": {"id": "e1", "name": "test"},
            "dataset": {"path": "tasks.jsonl"},
            "design": {"replications": 1},
            "baseline": {"variant_id": "baseline"},
            "runtime": {"image": "img:latest", "command": ["python", "main.py"]}
        });
        let err = validate_required_fields(&spec).unwrap_err();
        assert!(err.to_string().contains("legacy experiment version '1.0'"));
    }

    #[test]
    fn validate_required_fields_legacy_empty_workload_type_fails() {
        let mut spec = legacy_experiment_base();
        spec["experiment"]["workload_type"] = json!("");
        let err =
            validate_required_fields(&spec).expect_err("legacy runtime.agent should be rejected");
        assert!(err.to_string().contains("/runtime/agent was removed"));
    }

    #[test]
    fn validate_required_fields_legacy_zero_timeout_fails() {
        let mut spec = legacy_experiment_base();
        spec["runtime"]["policy"]["timeout_ms"] = json!(0);
        let err =
            validate_required_fields(&spec).expect_err("legacy runtime.agent should be rejected");
        assert!(err.to_string().contains("/runtime/agent was removed"));
    }

    #[test]
    fn validate_required_fields_legacy_rejects_removed_mode_field() {
        let mut spec = legacy_experiment_base();
        spec["runtime"]["agent"]["mode"] = json!("container");
        let err = validate_required_fields(&spec).expect_err("should reject /runtime/agent/mode");
        assert!(
            err.to_string().contains("/runtime/agent was removed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_legacy_rejects_known_agent_ref() {
        let mut spec = legacy_experiment_base();
        spec["runtime"]["agent"]["known_agent_ref"] = json!("codex");
        let err = validate_required_fields(&spec).expect_err("should reject known_agent_ref");
        assert!(
            err.to_string().contains("/runtime/agent was removed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_legacy_rejects_custom_image() {
        let mut spec = legacy_experiment_base();
        spec["runtime"]["agent"]["custom_image"] = json!("img:v2");
        let err = validate_required_fields(&spec).expect_err("should reject custom_image");
        assert!(
            err.to_string().contains("/runtime/agent was removed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_legacy_rejects_adapter() {
        let mut spec = legacy_experiment_base();
        spec["runtime"]["agent"]["adapter"] = json!("custom_adapter");
        let err = validate_required_fields(&spec).expect_err("should reject adapter");
        assert!(
            err.to_string().contains("/runtime/agent was removed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_per_task_image_requires_container() {
        let mut spec = legacy_experiment_base();
        spec["runtime"]["sandbox"]["image_source"] = json!("per_task");
        spec["runtime"]["sandbox"]["image"] = Value::Null;
        spec["runtime"]["sandbox"]["executor"] = json!("local");
        let err = validate_required_fields(&spec).expect_err("per_task needs container");
        assert!(
            err.to_string().contains("/runtime/agent was removed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_per_task_image_requires_artifact() {
        let mut spec = legacy_experiment_base();
        spec["runtime"]["sandbox"]["image_source"] = json!("per_task");
        spec["runtime"]["sandbox"]["image"] = Value::Null;
        spec["runtime"]["agent"]
            .as_object_mut()
            .unwrap()
            .remove("bundle");
        let err = validate_required_fields(&spec).expect_err("per_task needs artifact");
        assert!(
            err.to_string().contains("/runtime/agent was removed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_invalid_image_source_fails() {
        let mut spec = legacy_experiment_base();
        spec["runtime"]["sandbox"]["image_source"] = json!("custom");
        let err = validate_required_fields(&spec).expect_err("invalid image_source");
        assert!(
            err.to_string().contains("/runtime/agent was removed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_legacy_valid_spec_passes() {
        let err = validate_required_fields(&legacy_experiment_base())
            .expect_err("legacy runtime.agent should be rejected");
        assert!(err.to_string().contains("/runtime/agent was removed"));
    }

    #[test]
    fn validate_required_fields_legacy_missing_command_fails() {
        let mut spec = legacy_experiment_base();
        spec["runtime"]["agent"]
            .as_object_mut()
            .unwrap()
            .remove("command");
        let err =
            validate_required_fields(&spec).expect_err("legacy runtime.agent should be rejected");
        assert!(err.to_string().contains("/runtime/agent was removed"));
    }

    #[test]
    fn validate_required_fields_legacy_missing_replications_fails() {
        let mut spec = legacy_experiment_base();
        spec["design"]
            .as_object_mut()
            .unwrap()
            .remove("replications");
        let err =
            validate_required_fields(&spec).expect_err("legacy runtime.agent should be rejected");
        assert!(err.to_string().contains("/runtime/agent was removed"));
    }

    #[test]
    fn validate_required_fields_legacy_missing_network_mode_fails() {
        let mut spec = legacy_experiment_base();
        spec["runtime"]["sandbox"]
            .as_object_mut()
            .unwrap()
            .remove("network");
        let err =
            validate_required_fields(&spec).expect_err("legacy runtime.agent should be rejected");
        assert!(err.to_string().contains("/runtime/agent was removed"));
    }

    #[test]
    fn validate_required_fields_allows_defaulted_authoring_fields() {
        let spec = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "control" },
            "runtime": {
                "agent_runtime": {
                    "command": ["sh", "-lc", "echo ok"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest"
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": { "network": "none" }
            }
        });
        validate_required_fields(&spec)
            .expect("defaulted sanitization_profile and task_sandbox.profile should be optional");
    }

    #[test]
    fn validate_required_fields_rejects_removed_runtime_file_staging_surface() {
        let spec = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "control" },
            "runtime": {
                "agent_runtime": {
                    "command": ["sh", "-lc", "echo ok"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest"
                },
                "dependencies": {
                    "file_staging": [{
                        "source_from_host": "./secrets/token.txt",
                        "destination_path": "/agentlab/deps/token.txt"
                    }]
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": { "network": "none" }
            }
        });
        let err = validate_required_fields(&spec)
            .expect_err("runtime.dependencies.file_staging should be rejected");
        assert!(
            err.to_string()
                .contains("/runtime/dependencies/file_staging was removed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_required_fields_rejects_removed_benchmark_support_files_surface() {
        let spec = json!({
            "experiment": { "workload_type": "agent_runtime" },
            "design": { "replications": 1 },
            "baseline": { "variant_id": "control" },
            "benchmark": {
                "grader": {
                    "command": ["python3", "grader.py"],
                    "support_files": [{
                        "source_from_host": "./bench",
                        "destination_path": "/agentlab/deps/bench"
                    }]
                }
            },
            "runtime": {
                "agent_runtime": {
                    "command": ["sh", "-lc", "echo ok"],
                    "artifact": ".lab/agents/rex-current.tar.gz",
                    "image": "ghcr.io/acme/agent-runtime:latest"
                }
            },
            "policy": {
                "timeout_ms": 600000,
                "task_sandbox": { "network": "none" }
            }
        });
        let err = validate_required_fields(&spec)
            .expect_err("benchmark.grader.support_files should be rejected");
        assert!(
            err.to_string()
                .contains("/benchmark/grader/support_files was removed"),
            "unexpected error: {}",
            err
        );
    }

    // ───────────────────────────────────────────────────────────────────
    // Batch 4: Fork/Resume/Replay Selectors & Checkpoints
    // ───────────────────────────────────────────────────────────────────

    #[test]
    fn parse_fork_selector_checkpoint_valid() {
        match parse_fork_selector("checkpoint:cp1").unwrap() {
            ForkSelector::Checkpoint(name) => assert_eq!(name, "cp1"),
            _ => panic!("expected Checkpoint"),
        }
    }

    #[test]
    fn parse_fork_selector_step_valid() {
        match parse_fork_selector("step:42").unwrap() {
            ForkSelector::Step(s) => assert_eq!(s, 42),
            _ => panic!("expected Step"),
        }
    }

    #[test]
    fn parse_fork_selector_event_seq_valid() {
        match parse_fork_selector("event_seq:100").unwrap() {
            ForkSelector::EventSeq(s) => assert_eq!(s, 100),
            _ => panic!("expected EventSeq"),
        }
    }

    #[test]
    fn parse_fork_selector_missing_colon_fails() {
        assert!(parse_fork_selector("checkpoint_name").is_err());
    }

    #[test]
    fn parse_fork_selector_unknown_kind_fails() {
        match parse_fork_selector("snapshot:x") {
            Ok(_) => panic!("should fail for unknown kind"),
            Err(err) => assert!(err.to_string().contains("checkpoint|step|event_seq")),
        }
    }

    #[test]
    fn parse_fork_selector_step_non_integer_fails() {
        assert!(parse_fork_selector("step:abc").is_err());
    }

    #[test]
    fn parse_fork_selector_event_seq_non_integer_fails() {
        assert!(parse_fork_selector("event_seq:xyz").is_err());
    }

    #[test]
    fn parse_fork_selector_step_negative_fails() {
        assert!(parse_fork_selector("step:-1").is_err());
    }

    #[test]
    fn parse_fork_selector_checkpoint_with_colons() {
        match parse_fork_selector("checkpoint:a:b:c").unwrap() {
            ForkSelector::Checkpoint(name) => assert_eq!(name, "a:b:c"),
            _ => panic!("expected Checkpoint"),
        }
    }

    #[test]
    fn parse_fork_selector_step_zero_accepted() {
        match parse_fork_selector("step:0").unwrap() {
            ForkSelector::Step(s) => assert_eq!(s, 0),
            _ => panic!("expected Step(0)"),
        }
    }

    #[test]
    fn parse_fork_selector_checkpoint_with_slashes() {
        match parse_fork_selector("checkpoint:/path/to/cp").unwrap() {
            ForkSelector::Checkpoint(name) => assert_eq!(name, "/path/to/cp"),
            _ => panic!("expected Checkpoint"),
        }
    }

    #[test]
    fn parse_fork_selector_empty_checkpoint_value_fails() {
        assert!(parse_fork_selector("checkpoint:").is_err());
    }

    #[test]
    fn parse_fork_selector_whitespace_checkpoint_value_fails() {
        assert!(parse_fork_selector("checkpoint:   ").is_err());
    }

    #[test]
    fn parse_fork_selector_large_step_value() {
        match parse_fork_selector("step:999999999").unwrap() {
            ForkSelector::Step(s) => assert_eq!(s, 999999999),
            _ => panic!("expected Step"),
        }
    }

    #[test]
    fn parse_fork_selector_empty_string_fails() {
        assert!(parse_fork_selector("").is_err());
    }

    #[test]
    fn resolve_selector_checkpoint_by_name_finds_match() {
        let root = TempDirGuard::new("resolve_cp_name");
        let trial_dir = root.path.join("trial_1");
        let state_dir = trial_dir.join("state");
        ensure_dir(&state_dir).unwrap();
        let cp_path = format!("{}/checkpoint_1.json", AGENTLAB_CONTRACT_STATE_DIR);
        fs::write(state_dir.join("checkpoint_1.json"), "{}").unwrap();
        let output = json!({"checkpoints": [{"logical_name": "cp1", "path": &cp_path, "step": 1}]});
        let result = resolve_selector_checkpoint(
            &ForkSelector::Checkpoint("cp1".to_string()),
            Some(&output),
            &trial_dir,
            true,
        )
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn resolve_selector_checkpoint_by_name_no_match_strict_fails() {
        let root = TempDirGuard::new("resolve_cp_strict_fail");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).unwrap();
        let output = json!({"checkpoints": []});
        let err = resolve_selector_checkpoint(
            &ForkSelector::Checkpoint("missing".to_string()),
            Some(&output),
            &trial_dir,
            true,
        )
        .expect_err("strict should fail");
        assert!(err.to_string().contains("strict_source_unavailable"));
    }

    #[test]
    fn resolve_selector_checkpoint_by_name_no_match_nonstrict_returns_none() {
        let root = TempDirGuard::new("resolve_cp_nonstrict");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).unwrap();
        let output = json!({"checkpoints": []});
        let result = resolve_selector_checkpoint(
            &ForkSelector::Checkpoint("missing".to_string()),
            Some(&output),
            &trial_dir,
            false,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn resolve_selector_checkpoint_by_step_highest_lte() {
        let root = TempDirGuard::new("resolve_cp_step");
        let trial_dir = root.path.join("trial_1");
        let state_dir = trial_dir.join("state");
        ensure_dir(&state_dir).unwrap();
        let cp5_path = format!("{}/cp5.json", AGENTLAB_CONTRACT_STATE_DIR);
        fs::write(state_dir.join("cp5.json"), "{}").unwrap();
        let output = json!({"checkpoints": [
            {"logical_name": "cp3", "path": &format!("{}/cp3.json", AGENTLAB_CONTRACT_STATE_DIR), "step": 3},
            {"logical_name": "cp5", "path": &cp5_path, "step": 5},
            {"logical_name": "cp8", "path": &format!("{}/cp8.json", AGENTLAB_CONTRACT_STATE_DIR), "step": 8}
        ]});
        let result =
            resolve_selector_checkpoint(&ForkSelector::Step(5), Some(&output), &trial_dir, false)
                .unwrap();
        assert!(result.is_some());
        assert!(result.unwrap().contains("cp5"));
    }

    #[test]
    fn resolve_selector_checkpoint_by_step_no_qualifying_strict_fails() {
        let root = TempDirGuard::new("resolve_cp_step_strict");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).unwrap();
        let output = json!({"checkpoints": [
            {"logical_name": "cp3", "path": "/state/cp3.json", "step": 3},
            {"logical_name": "cp5", "path": "/state/cp5.json", "step": 5}
        ]});
        assert!(resolve_selector_checkpoint(
            &ForkSelector::Step(1),
            Some(&output),
            &trial_dir,
            true
        )
        .is_err());
    }

    #[test]
    fn resolve_selector_checkpoint_by_event_seq_highest_lte() {
        let root = TempDirGuard::new("resolve_cp_event_seq");
        let trial_dir = root.path.join("trial_1");
        let state_dir = trial_dir.join("state");
        ensure_dir(&state_dir).unwrap();
        let cp_path = format!("{}/cp10.json", AGENTLAB_CONTRACT_STATE_DIR);
        fs::write(state_dir.join("cp10.json"), "{}").unwrap();
        let output = json!({"checkpoints": [
            {"logical_name": "cp5", "path": &format!("{}/cp5.json", AGENTLAB_CONTRACT_STATE_DIR), "step": 5},
            {"logical_name": "cp10", "path": &cp_path, "step": 10},
            {"logical_name": "cp20", "path": &format!("{}/cp20.json", AGENTLAB_CONTRACT_STATE_DIR), "step": 20}
        ]});
        let result = resolve_selector_checkpoint(
            &ForkSelector::EventSeq(15),
            Some(&output),
            &trial_dir,
            false,
        )
        .unwrap();
        assert!(result.is_some());
        assert!(result.unwrap().contains("cp10"));
    }

    #[test]
    fn resolve_selector_checkpoint_no_output_strict_fails() {
        let root = TempDirGuard::new("resolve_cp_no_output_strict");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).unwrap();
        assert!(resolve_selector_checkpoint(
            &ForkSelector::Checkpoint("any".to_string()),
            None,
            &trial_dir,
            true
        )
        .is_err());
    }

    #[test]
    fn resolve_selector_checkpoint_no_output_nonstrict_returns_none() {
        let root = TempDirGuard::new("resolve_cp_no_output_nonstrict");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).unwrap();
        assert!(resolve_selector_checkpoint(
            &ForkSelector::Checkpoint("any".to_string()),
            None,
            &trial_dir,
            false
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn resolve_selector_checkpoint_empty_checkpoints_strict_fails() {
        let root = TempDirGuard::new("resolve_cp_empty_strict");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).unwrap();
        assert!(resolve_selector_checkpoint(
            &ForkSelector::Step(5),
            Some(&json!({"checkpoints": []})),
            &trial_dir,
            true
        )
        .is_err());
    }

    #[test]
    fn resolve_selector_checkpoint_empty_checkpoints_nonstrict_returns_none() {
        let root = TempDirGuard::new("resolve_cp_empty_nonstrict");
        let trial_dir = root.path.join("trial_1");
        ensure_dir(&trial_dir).unwrap();
        assert!(resolve_selector_checkpoint(
            &ForkSelector::Step(5),
            Some(&json!({"checkpoints": []})),
            &trial_dir,
            false
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn adapter_control_ack_received_missing_file_returns_false() {
        let root = TempDirGuard::new("ack_missing");
        assert!(
            !adapter_control_ack_received(&root.path.join("events.jsonl"), "pause", "v1").unwrap()
        );
    }

    #[test]
    fn adapter_control_ack_received_wrong_action_returns_false() {
        let root = TempDirGuard::new("ack_wrong_action");
        let events_path = root.path.join("events.jsonl");
        fs::write(
            &events_path,
            r#"{"event_type":"control_ack","action_observed":"resume","control_version":"v1"}"#,
        )
        .unwrap();
        assert!(!adapter_control_ack_received(&events_path, "pause", "v1").unwrap());
    }

    #[test]
    fn adapter_control_ack_received_wrong_version_returns_false() {
        let root = TempDirGuard::new("ack_wrong_version");
        let events_path = root.path.join("events.jsonl");
        fs::write(
            &events_path,
            r#"{"event_type":"control_ack","action_observed":"pause","control_version":"v2"}"#,
        )
        .unwrap();
        assert!(!adapter_control_ack_received(&events_path, "pause", "v1").unwrap());
    }

    #[test]
    fn adapter_control_ack_received_skips_invalid_json_lines() {
        let root = TempDirGuard::new("ack_invalid_json");
        let events_path = root.path.join("events.jsonl");
        fs::write(&events_path, "not valid json\n{\"event_type\":\"control_ack\",\"action_observed\":\"pause\",\"control_version\":\"v1\"}\n").unwrap();
        assert!(adapter_control_ack_received(&events_path, "pause", "v1").unwrap());
    }

    #[test]
    fn adapter_control_ack_received_skips_empty_lines() {
        let root = TempDirGuard::new("ack_empty_lines");
        let events_path = root.path.join("events.jsonl");
        fs::write(&events_path, "\n\n{\"event_type\":\"control_ack\",\"action_observed\":\"pause\",\"control_version\":\"v1\"}\n\n").unwrap();
        assert!(adapter_control_ack_received(&events_path, "pause", "v1").unwrap());
    }

    #[test]
    fn adapter_control_ack_received_skips_non_control_ack_events() {
        let root = TempDirGuard::new("ack_other_events");
        let events_path = root.path.join("events.jsonl");
        fs::write(&events_path, "{\"event_type\":\"step\",\"data\":\"x\"}\n{\"event_type\":\"control_ack\",\"action_observed\":\"pause\",\"control_version\":\"v1\"}\n").unwrap();
        assert!(adapter_control_ack_received(&events_path, "pause", "v1").unwrap());
    }

    #[test]
    fn read_control_seq_missing_file_returns_zero() {
        let root = TempDirGuard::new("ctrl_seq_missing");
        assert_eq!(
            read_control_seq(&root.path.join("control.json")).unwrap(),
            0
        );
    }

    #[test]
    fn read_control_seq_missing_seq_field_returns_zero() {
        let root = TempDirGuard::new("ctrl_seq_no_field");
        let path = root.path.join("control.json");
        atomic_write_json_pretty(&path, &json!({"status": "running"})).unwrap();
        assert_eq!(read_control_seq(&path).unwrap(), 0);
    }

    #[test]
    fn read_control_seq_reads_valid_seq() {
        let root = TempDirGuard::new("ctrl_seq_valid");
        let path = root.path.join("control.json");
        atomic_write_json_pretty(&path, &json!({"seq": 42})).unwrap();
        assert_eq!(read_control_seq(&path).unwrap(), 42);
    }

    #[test]
    fn apply_variant_binding_overrides_adds_new_keys() {
        let mut variant = Variant {
            id: "baseline".to_string(),
            bindings: json!({"existing": "value"}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let mut overrides = BTreeMap::new();
        overrides.insert("new_key".to_string(), json!("new_value"));
        apply_variant_binding_overrides(&mut variant, &overrides).unwrap();
        assert_eq!(variant.bindings["new_key"], json!("new_value"));
    }

    #[test]
    fn apply_variant_binding_overrides_overwrites_existing() {
        let mut variant = Variant {
            id: "baseline".to_string(),
            bindings: json!({"key": "old"}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let mut overrides = BTreeMap::new();
        overrides.insert("key".to_string(), json!("new"));
        apply_variant_binding_overrides(&mut variant, &overrides).unwrap();
        assert_eq!(variant.bindings["key"], json!("new"));
    }

    #[test]
    fn apply_variant_binding_overrides_preserves_untouched_keys() {
        let mut variant = Variant {
            id: "baseline".to_string(),
            bindings: json!({"keep": "this", "change": "old"}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let mut overrides = BTreeMap::new();
        overrides.insert("change".to_string(), json!("new"));
        apply_variant_binding_overrides(&mut variant, &overrides).unwrap();
        assert_eq!(variant.bindings["keep"], json!("this"));
        assert_eq!(variant.bindings["change"], json!("new"));
    }

    #[test]
    fn apply_variant_binding_overrides_empty_map_is_noop() {
        let mut variant = Variant {
            id: "baseline".to_string(),
            bindings: json!({"key": "value"}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let original = variant.bindings.clone();
        apply_variant_binding_overrides(&mut variant, &BTreeMap::new()).unwrap();
        assert_eq!(variant.bindings, original);
    }

    #[test]
    fn apply_variant_binding_overrides_creates_bindings_object_if_missing() {
        let mut variant = Variant {
            id: "baseline".to_string(),
            bindings: Value::Null,
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let mut overrides = BTreeMap::new();
        overrides.insert("key".to_string(), json!("value"));
        apply_variant_binding_overrides(&mut variant, &overrides).unwrap();
        assert_eq!(variant.bindings["key"], json!("value"));
    }

    #[test]
    fn apply_variant_binding_overrides_nested_key() {
        let mut variant = Variant {
            id: "baseline".to_string(),
            bindings: json!({}),
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let mut overrides = BTreeMap::new();
        overrides.insert("nested.deep.key".to_string(), json!(42));
        apply_variant_binding_overrides(&mut variant, &overrides).unwrap();
        assert_eq!(variant.bindings["nested"]["deep"]["key"], json!(42));
    }

    // ───────────────────────────────────────────────────────────────────
    // Batch 5: Policy Parsing & Scheduling Edge Cases
    // ───────────────────────────────────────────────────────────────────

    #[test]
    fn schedule_variant_sequential_multi_replication() {
        let slots = build_trial_schedule(2, 3, 2, SchedulingPolicy::VariantSequential, 0);
        assert_eq!(slots.len(), 12);
        assert!(slots[..6].iter().all(|s| s.variant_idx == 0));
        assert!(slots[6..].iter().all(|s| s.variant_idx == 1));
    }

    #[test]
    fn schedule_paired_interleaved_multi_replication() {
        let slots = build_trial_schedule(2, 3, 2, SchedulingPolicy::PairedInterleaved, 0);
        assert_eq!(slots.len(), 12);
        assert_eq!(slots[0].task_idx, 0);
        assert_eq!(slots[0].variant_idx, 0);
    }

    #[test]
    fn schedule_randomized_large_is_deterministic_with_seed() {
        let a = build_trial_schedule(5, 10, 3, SchedulingPolicy::Randomized, 12345);
        let b = build_trial_schedule(5, 10, 3, SchedulingPolicy::Randomized, 12345);
        assert_eq!(a.len(), 150);
        for (i, (sa, sb)) in a.iter().zip(b.iter()).enumerate() {
            assert_eq!(
                (sa.variant_idx, sa.task_idx, sa.repl_idx),
                (sb.variant_idx, sb.task_idx, sb.repl_idx),
                "mismatch at slot {}",
                i
            );
        }
    }

    #[test]
    fn schedule_randomized_zero_seed_still_deterministic() {
        let a = build_trial_schedule(2, 3, 1, SchedulingPolicy::Randomized, 0);
        let b = build_trial_schedule(2, 3, 1, SchedulingPolicy::Randomized, 0);
        for (sa, sb) in a.iter().zip(b.iter()) {
            assert_eq!(
                (sa.variant_idx, sa.task_idx, sa.repl_idx),
                (sb.variant_idx, sb.task_idx, sb.repl_idx)
            );
        }
    }

    #[test]
    fn schedule_randomized_max_seed_does_not_overflow() {
        let slots = build_trial_schedule(2, 2, 1, SchedulingPolicy::Randomized, u64::MAX);
        assert_eq!(slots.len(), 4);
    }

    #[test]
    fn schedule_single_variant_many_tasks() {
        let slots = build_trial_schedule(1, 100, 1, SchedulingPolicy::VariantSequential, 0);
        assert_eq!(slots.len(), 100);
        assert!(slots.iter().all(|s| s.variant_idx == 0));
    }

    #[test]
    fn schedule_many_variants_single_task() {
        let slots = build_trial_schedule(50, 1, 1, SchedulingPolicy::VariantSequential, 0);
        assert_eq!(slots.len(), 50);
        for (i, slot) in slots.iter().enumerate() {
            assert_eq!(slot.variant_idx, i);
        }
    }

    #[test]
    fn schedule_slot_count_equals_product() {
        for policy in [
            SchedulingPolicy::VariantSequential,
            SchedulingPolicy::PairedInterleaved,
            SchedulingPolicy::Randomized,
        ] {
            let slots = build_trial_schedule(3, 4, 2, policy, 42);
            assert_eq!(slots.len(), 3 * 4 * 2, "policy {:?}", policy);
        }
    }

    #[test]
    fn schedule_randomized_different_seeds_produce_different_orders() {
        let a = build_trial_schedule(3, 5, 1, SchedulingPolicy::Randomized, 111);
        let b = build_trial_schedule(3, 5, 1, SchedulingPolicy::Randomized, 222);
        let same = a
            .iter()
            .zip(b.iter())
            .all(|(sa, sb)| sa.variant_idx == sb.variant_idx && sa.task_idx == sb.task_idx);
        assert!(!same, "different seeds should produce different orderings");
    }

    #[test]
    fn schedule_variant_sequential_order_is_variant_first() {
        let slots = build_trial_schedule(2, 3, 1, SchedulingPolicy::VariantSequential, 0);
        assert_eq!((slots[0].variant_idx, slots[0].task_idx), (0, 0));
        assert_eq!((slots[1].variant_idx, slots[1].task_idx), (0, 1));
        assert_eq!((slots[2].variant_idx, slots[2].task_idx), (0, 2));
        assert_eq!((slots[3].variant_idx, slots[3].task_idx), (1, 0));
    }

    #[test]
    fn schedule_paired_interleaved_order_is_task_first() {
        let slots = build_trial_schedule(2, 3, 1, SchedulingPolicy::PairedInterleaved, 0);
        assert_eq!((slots[0].task_idx, slots[0].variant_idx), (0, 0));
        assert_eq!((slots[1].task_idx, slots[1].variant_idx), (0, 1));
        assert_eq!((slots[2].task_idx, slots[2].variant_idx), (1, 0));
    }

    #[test]
    fn parse_policies_retry_on_empty_array() {
        let spec = json!({"design": {"policies": {"retry": {"max_attempts": 3, "retry_on": []}}}});
        let config = parse_policies(&spec);
        assert_eq!(config.retry_max_attempts, 3);
        assert!(config.retry_on.is_empty());
    }

    #[test]
    fn parse_policies_retry_on_multiple_triggers() {
        let spec = json!({"design": {"policies": {"retry": {"max_attempts": 2, "retry_on": ["error", "timeout"]}}}});
        let config = parse_policies(&spec);
        assert_eq!(config.retry_on.len(), 2);
    }

    #[test]
    fn parse_policies_concurrency_max_in_flight() {
        let spec =
            json!({"design": {"policies": {"concurrency": {"max_in_flight_per_variant": 4}}}});
        assert_eq!(
            parse_policies(&spec).concurrency.max_in_flight_per_variant,
            Some(4)
        );
    }

    #[test]
    fn parse_policies_concurrency_require_chain_lease() {
        let spec = json!({"design": {"policies": {"concurrency": {"require_chain_lease": false}}}});
        assert!(!parse_policies(&spec).concurrency.require_chain_lease);
    }

    #[test]
    fn parse_policies_task_boundary_require_workspace_materialization_false() {
        let spec = json!({"design": {"policies": {"task_boundary": {"require_workspace_materialization": false}}}});
        assert!(
            !parse_policies(&spec)
                .task_boundary
                .require_workspace_materialization
        );
    }

    #[test]
    fn parse_policies_pruning_max_consecutive_failures() {
        let spec = json!({"design": {"policies": {"pruning": {"max_consecutive_failures": 5}}}});
        assert_eq!(
            parse_policies(&spec).pruning_max_consecutive_failures,
            Some(5)
        );
    }

    #[test]
    fn parse_policies_pruning_default_none() {
        assert!(parse_policies(&json!({"design": {"policies": {}}}))
            .pruning_max_consecutive_failures
            .is_none());
    }

    #[test]
    fn parse_policies_scheduling_paired_interleaved() {
        assert_eq!(
            parse_policies(&json!({"design": {"policies": {"scheduling": "paired_interleaved"}}}))
                .scheduling,
            SchedulingPolicy::PairedInterleaved
        );
    }

    #[test]
    fn parse_policies_scheduling_randomized() {
        assert_eq!(
            parse_policies(&json!({"design": {"policies": {"scheduling": "randomized"}}}))
                .scheduling,
            SchedulingPolicy::Randomized
        );
    }

    #[test]
    fn parse_policies_scheduling_default_variant_sequential() {
        assert_eq!(
            parse_policies(&json!({"design": {"policies": {}}})).scheduling,
            SchedulingPolicy::VariantSequential
        );
    }

    #[test]
    fn parse_policies_no_policies_section_uses_defaults() {
        let config = parse_policies(&json!({}));
        assert_eq!(config.scheduling, SchedulingPolicy::VariantSequential);
        assert_eq!(config.retry_max_attempts, 1);
        assert!(config.retry_on.is_empty());
    }

    #[test]
    fn parse_policies_retry_max_attempts() {
        assert_eq!(
            parse_policies(&json!({"design": {"policies": {"retry": {"max_attempts": 5}}}}))
                .retry_max_attempts,
            5
        );
    }

    #[test]
    fn should_retry_outcome_error_always_retried_default() {
        assert!(should_retry_outcome("error", "0", &[]));
    }

    #[test]
    fn should_retry_outcome_success_never_retried() {
        assert!(!should_retry_outcome("success", "0", &[]));
    }

    #[test]
    fn should_retry_outcome_timeout_with_timeout_trigger() {
        assert!(should_retry_outcome(
            "timeout",
            "0",
            &["timeout".to_string()]
        ));
    }

    #[test]
    fn should_retry_outcome_timeout_without_trigger() {
        assert!(!should_retry_outcome(
            "timeout",
            "0",
            &["error".to_string()]
        ));
    }

    #[test]
    fn should_retry_outcome_failure_with_failure_trigger() {
        assert!(should_retry_outcome(
            "completed",
            "1",
            &["failure".to_string()]
        ));
    }

    #[test]
    fn should_retry_outcome_nonzero_exit_default_retried() {
        assert!(should_retry_outcome("completed", "1", &[]));
    }

    #[test]
    fn should_retry_outcome_error_with_error_trigger() {
        assert!(should_retry_outcome("error", "0", &["error".to_string()]));
    }

    #[test]
    fn should_retry_outcome_error_without_error_trigger() {
        assert!(!should_retry_outcome(
            "error",
            "0",
            &["timeout".to_string()]
        ));
    }

    #[test]
    fn should_retry_outcome_success_with_triggers_never_retried() {
        assert!(!should_retry_outcome(
            "success",
            "0",
            &["error".to_string(), "timeout".to_string()]
        ));
    }

    #[test]
    fn normalize_schedule_progress_fills_missing_attempt() {
        let mut progress = ScheduleProgress {
            schema_version: String::new(),
            run_id: "run_001".to_string(),
            total_slots: 1,
            next_schedule_index: 1,
            next_trial_index: 1,
            schedule: vec![],
            completed_slots: vec![SlotCompletion {
                schedule_index: 0,
                trial_id: "trial_1".to_string(),
                status: "completed".to_string(),
                slot_commit_id: "abc".to_string(),
                attempt: 0,
            }],
            pruned_variants: vec![],
            consecutive_failures: BTreeMap::new(),
            updated_at: String::new(),
        };
        normalize_schedule_progress(&mut progress);
        assert_eq!(progress.completed_slots[0].attempt, 1);
    }

    #[test]
    fn normalize_schedule_progress_fills_missing_commit_id() {
        let mut progress = ScheduleProgress {
            schema_version: String::new(),
            run_id: "run_001".to_string(),
            total_slots: 1,
            next_schedule_index: 1,
            next_trial_index: 1,
            schedule: vec![],
            completed_slots: vec![SlotCompletion {
                schedule_index: 0,
                trial_id: "trial_1".to_string(),
                status: "completed".to_string(),
                slot_commit_id: "".to_string(),
                attempt: 1,
            }],
            pruned_variants: vec![],
            consecutive_failures: BTreeMap::new(),
            updated_at: String::new(),
        };
        normalize_schedule_progress(&mut progress);
        assert!(progress.completed_slots[0]
            .slot_commit_id
            .starts_with("legacy_"));
    }

    #[test]
    fn normalize_schedule_progress_sets_schema_version() {
        let mut progress = ScheduleProgress {
            schema_version: "old".to_string(),
            run_id: "run_001".to_string(),
            total_slots: 0,
            next_schedule_index: 0,
            next_trial_index: 0,
            schedule: vec![],
            completed_slots: vec![],
            pruned_variants: vec![],
            consecutive_failures: BTreeMap::new(),
            updated_at: String::new(),
        };
        normalize_schedule_progress(&mut progress);
        assert_eq!(progress.schema_version, "schedule_progress_v2");
    }

    #[test]
    fn load_schedule_progress_rejects_v1_schema() {
        let root = TempDirGuard::new("sched_v1");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        let progress = json!({"schema_version": "schedule_progress_v1", "run_id": "run_001", "total_slots": 0, "next_schedule_index": 0, "next_trial_index": 0, "schedule": [], "completed_slots": [], "pruned_variants": [], "consecutive_failures": {}, "use_container": false, "updated_at": ""});
        let mut store = BackingSqliteStore::open(&run_dir).unwrap();
        store
            .put_runtime_json(RUNTIME_KEY_SCHEDULE_PROGRESS, &progress)
            .unwrap();
        assert!(load_schedule_progress(&run_dir)
            .unwrap_err()
            .to_string()
            .contains("unsupported"));
    }

    #[test]
    fn write_and_load_schedule_progress_roundtrip() {
        let root = TempDirGuard::new("sched_roundtrip");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        let progress = ScheduleProgress {
            schema_version: "schedule_progress_v2".to_string(),
            run_id: "run_001".to_string(),
            total_slots: 6,
            next_schedule_index: 3,
            next_trial_index: 3,
            schedule: vec![TrialSlot {
                variant_idx: 0,
                task_idx: 0,
                repl_idx: 0,
            }],
            completed_slots: vec![SlotCompletion {
                schedule_index: 0,
                trial_id: "trial_1".to_string(),
                status: "completed".to_string(),
                slot_commit_id: "abc123".to_string(),
                attempt: 1,
            }],
            pruned_variants: vec![],
            consecutive_failures: BTreeMap::new(),
            updated_at: "2024-01-01T00:00:00Z".to_string(),
        };
        write_schedule_progress(&run_dir, &progress).unwrap();
        let loaded = load_schedule_progress(&run_dir).unwrap();
        assert_eq!(loaded.run_id, "run_001");
        assert_eq!(loaded.total_slots, 6);
    }

    #[test]
    fn legacy_slot_commit_id_deterministic() {
        let slot = SlotCompletion {
            schedule_index: 0,
            trial_id: "trial_1".to_string(),
            status: "completed".to_string(),
            slot_commit_id: String::new(),
            attempt: 1,
        };
        assert_eq!(
            legacy_slot_commit_id("run_001", &slot),
            legacy_slot_commit_id("run_001", &slot)
        );
    }

    #[test]
    fn legacy_slot_commit_id_different_for_different_slots() {
        let a = SlotCompletion {
            schedule_index: 0,
            trial_id: "trial_1".to_string(),
            status: "completed".to_string(),
            slot_commit_id: String::new(),
            attempt: 1,
        };
        let b = SlotCompletion {
            schedule_index: 1,
            trial_id: "trial_2".to_string(),
            status: "completed".to_string(),
            slot_commit_id: String::new(),
            attempt: 1,
        };
        assert_ne!(
            legacy_slot_commit_id("run_001", &a),
            legacy_slot_commit_id("run_001", &b)
        );
    }

    #[test]
    fn default_slot_attempt_returns_one() {
        assert_eq!(default_slot_attempt(), 1);
    }

    // ───────────────────────────────────────────────────────────────────
    // Batch 6: Variant Resolution & Runtime Profiles
    // ───────────────────────────────────────────────────────────────────

    #[test]
    fn variant_digest_deterministic() {
        let v = Variant {
            id: "v1".to_string(),
            bindings: json!({"key": "value"}),
            args: vec![],
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        assert_eq!(variant_digest(&v).unwrap(), variant_digest(&v).unwrap());
    }

    #[test]
    fn variant_digest_changes_with_bindings() {
        let v1 = Variant {
            id: "v1".to_string(),
            bindings: json!({"key": "a"}),
            args: vec![],
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        let v2 = Variant {
            id: "v1".to_string(),
            bindings: json!({"key": "b"}),
            args: vec![],
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        };
        assert_ne!(variant_digest(&v1).unwrap(), variant_digest(&v2).unwrap());
    }

    #[test]
    fn variant_digest_changes_with_env() {
        let mut env1 = BTreeMap::new();
        env1.insert("FOO".to_string(), "bar".to_string());
        let mut env2 = BTreeMap::new();
        env2.insert("FOO".to_string(), "baz".to_string());
        let v1 = Variant {
            id: "v1".to_string(),
            bindings: json!({}),
            args: vec![],
            env: env1,
            image: None,
            runtime_overrides: None,
        };
        let v2 = Variant {
            id: "v1".to_string(),
            bindings: json!({}),
            args: vec![],
            env: env2,
            image: None,
            runtime_overrides: None,
        };
        assert_ne!(variant_digest(&v1).unwrap(), variant_digest(&v2).unwrap());
    }

    #[test]
    fn variant_digest_changes_with_image() {
        let v1 = Variant {
            id: "v1".to_string(),
            bindings: json!({}),
            args: vec![],
            env: BTreeMap::new(),
            image: Some("img:v1".to_string()),
            runtime_overrides: None,
        };
        let v2 = Variant {
            id: "v1".to_string(),
            bindings: json!({}),
            args: vec![],
            env: BTreeMap::new(),
            image: Some("img:v2".to_string()),
            runtime_overrides: None,
        };
        assert_ne!(variant_digest(&v1).unwrap(), variant_digest(&v2).unwrap());
    }

    #[test]
    fn write_resolved_variants_persists_behavior_surface_digests() {
        let root = TempDirGuard::new("agentlab_variant_behavior_digests");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir).expect("run dir");
        let project_root = find_project_root(&run_dir);
        let bundle_root = ensure_test_agent_bundle(&project_root, "rex-current");
        let resolved = json!({
            "baseline": { "variant_id": "base", "bindings": {} },
            "variant_plan": [
                {
                    "variant_id": "alt",
                    "bindings": { "temperature": 1.2 },
                    "runtime_overrides": {
                        "agent": {
                            "command": ["rex", "run", "--alternate"],
                            "env": { "PARALLEL_TOOLS": "1" }
                        }
                    }
                }
            ],
            "runtime": {
                "agent": {
                    "command": harness_success_command(),
                    "bundle": bundle_root.to_string_lossy().to_string(),
                    "io": { "input_arg": "--input", "output_arg": "--output" }
                },
                "sandbox": runtime_sandbox("global", Some("img")),
                "policy": { "timeout_ms": 600000 }
            }
        });
        let (variants, baseline_id) = resolve_variant_plan(&resolved).expect("variant plan");
        write_resolved_variants(&run_dir, &resolved, &baseline_id, &variants)
            .expect("write resolved variants");

        let manifest =
            load_json_file(&run_dir.join("resolved_variants.json")).expect("resolved variants");
        let manifest_variants = manifest
            .pointer("/variants")
            .and_then(Value::as_array)
            .expect("variant array");
        assert_eq!(manifest_variants.len(), variants.len());
        for (idx, variant) in variants.iter().enumerate() {
            let expected = resolved_variant_behavior_digest(&resolved, variant)
                .expect("behavior surface digest");
            assert_eq!(
                manifest_variants[idx]
                    .get("variant_digest")
                    .and_then(Value::as_str),
                Some(expected.as_str())
            );
        }
    }

    #[test]
    fn find_variant_by_id_finds_match() {
        let variants = vec![
            Variant {
                id: "baseline".to_string(),
                bindings: json!({}),
                args: vec![],
                env: BTreeMap::new(),
                image: None,
                runtime_overrides: None,
            },
            Variant {
                id: "treatment".to_string(),
                bindings: json!({"temp": 0.5}),
                args: vec![],
                env: BTreeMap::new(),
                image: None,
                runtime_overrides: None,
            },
        ];
        assert_eq!(
            find_variant_by_id(&variants, "treatment").unwrap().id,
            "treatment"
        );
    }

    #[test]
    fn find_variant_by_id_empty_id_returns_first() {
        let variants = vec![Variant {
            id: "baseline".to_string(),
            bindings: json!({}),
            args: vec![],
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        }];
        assert_eq!(find_variant_by_id(&variants, "").unwrap().id, "baseline");
    }

    #[test]
    fn find_variant_by_id_missing_fails() {
        let variants = vec![Variant {
            id: "baseline".to_string(),
            bindings: json!({}),
            args: vec![],
            env: BTreeMap::new(),
            image: None,
            runtime_overrides: None,
        }];
        assert!(find_variant_by_id(&variants, "missing").is_err());
    }

    #[test]
    fn resolve_variant_plan_single_baseline_only() {
        let spec = json!({"baseline": {"variant_id": "baseline", "bindings": {"x": 1}}});
        let (variants, baseline_id) = resolve_variant_plan(&spec).unwrap();
        assert_eq!(baseline_id, "baseline");
        assert_eq!(variants.len(), 1);
    }

    #[test]
    fn resolve_variant_plan_baseline_plus_treatments() {
        let spec = json!({"baseline": {"variant_id": "baseline", "bindings": {}}, "variant_plan": [{"variant_id": "v1", "bindings": {"key": "a"}}, {"variant_id": "v2", "bindings": {"key": "b"}}]});
        let (variants, _) = resolve_variant_plan(&spec).unwrap();
        assert_eq!(variants.len(), 3);
    }

    #[test]
    fn resolve_variant_plan_variant_bindings_preserved() {
        let spec = json!({"baseline": {"variant_id": "baseline", "bindings": {"temp": 0.5}}, "variant_plan": [{"variant_id": "v1", "bindings": {"temp": 0.9}}]});
        let (variants, _) = resolve_variant_plan(&spec).unwrap();
        assert_eq!(variants[0].bindings["temp"], json!(0.5));
        assert_eq!(variants[1].bindings["temp"], json!(0.9));
    }

    #[test]
    fn resolve_variant_plan_empty_bindings_default_to_object() {
        let spec = json!({"baseline": {"variant_id": "baseline"}});
        let (variants, _) = resolve_variant_plan(&spec).unwrap();
        assert!(variants[0].bindings.is_object());
    }

    // ───────────────────────────────────────────────────────────────────
    // Batch 7: Run State & Leasing
    // ───────────────────────────────────────────────────────────────────

    #[test]
    fn operation_lease_is_stale_expired_returns_true() {
        let record = OperationLeaseRecord {
            schema_version: "v1".to_string(),
            operation_id: "op1".to_string(),
            op_type: "run".to_string(),
            owner_pid: 12345,
            owner_host: "localhost".to_string(),
            acquired_at: "2024-01-01T00:00:00Z".to_string(),
            expires_at: "2024-01-01T00:05:00Z".to_string(),
            stale_takeover_of: None,
        };
        let now = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:10:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(operation_lease_is_stale(&record, now));
    }

    #[test]
    fn operation_lease_is_stale_fresh_returns_false() {
        let record = OperationLeaseRecord {
            schema_version: "v1".to_string(),
            operation_id: "op1".to_string(),
            op_type: "run".to_string(),
            owner_pid: 12345,
            owner_host: "localhost".to_string(),
            acquired_at: "2024-01-01T00:00:00Z".to_string(),
            expires_at: "2024-01-01T00:10:00Z".to_string(),
            stale_takeover_of: None,
        };
        let now = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:05:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(!operation_lease_is_stale(&record, now));
    }

    #[test]
    fn engine_lease_is_stale_expired_returns_true() {
        let record = EngineLeaseRecord {
            schema_version: "v1".to_string(),
            run_id: "run_001".to_string(),
            owner_id: "o1".to_string(),
            pid: 12345,
            hostname: "localhost".to_string(),
            started_at: "2024-01-01T00:00:00Z".to_string(),
            heartbeat_at: "2024-01-01T00:00:00Z".to_string(),
            expires_at: "2024-01-01T00:05:00Z".to_string(),
            epoch: 0,
        };
        let now = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:10:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(engine_lease_is_stale(&record, now));
    }

    #[test]
    fn engine_lease_is_stale_fresh_returns_false() {
        let record = EngineLeaseRecord {
            schema_version: "v1".to_string(),
            run_id: "run_001".to_string(),
            owner_id: "o1".to_string(),
            pid: 12345,
            hostname: "localhost".to_string(),
            started_at: "2024-01-01T00:00:00Z".to_string(),
            heartbeat_at: "2024-01-01T00:00:00Z".to_string(),
            expires_at: "2024-01-01T00:10:00Z".to_string(),
            epoch: 0,
        };
        let now = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:05:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(!engine_lease_is_stale(&record, now));
    }

    #[test]
    fn write_run_control_v2_running_status() {
        let root = TempDirGuard::new("run_ctrl_running");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        write_run_control_v2(&run_dir, "run_001", "running", &[], None).unwrap();
        let loaded = load_json_file(&run_control_path(&run_dir)).unwrap();
        assert_eq!(loaded["status"], "running");
    }

    #[test]
    fn write_run_control_v2_paused_with_label() {
        let root = TempDirGuard::new("run_ctrl_paused");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        let pause = RunControlPauseMetadata {
            label: "user_pause".to_string(),
            requested_at: "2024-01-01T00:00:00Z".to_string(),
            requested_by: None,
        };
        write_run_control_v2(&run_dir, "run_001", "paused", &[], Some(&pause)).unwrap();
        let loaded = load_json_file(&run_control_path(&run_dir)).unwrap();
        assert_eq!(loaded["status"], "paused");
        assert_eq!(loaded["pause"]["label"], "user_pause");
    }

    #[test]
    fn write_run_control_v2_active_trials_serialized() {
        let root = TempDirGuard::new("run_ctrl_active");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        let trials = vec![RunControlActiveTrial {
            trial_id: "trial_1".to_string(),
            worker_id: "worker_a".to_string(),
            schedule_idx: Some(0),
            variant_id: Some("baseline".to_string()),
            started_at: Some("2024-01-01T00:00:00Z".to_string()),
            control: None,
        }];
        write_run_control_v2(&run_dir, "run_001", "running", &trials, None).unwrap();
        let loaded = load_json_file(&run_control_path(&run_dir)).unwrap();
        let active = loaded["active_trials"].as_object().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active["trial_1"]["trial_id"], "trial_1");
    }

    #[test]
    fn write_run_control_v2_schema_version() {
        let root = TempDirGuard::new("run_ctrl_schema");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        write_run_control_v2(&run_dir, "run_001", "running", &[], None).unwrap();
        assert_eq!(
            load_json_file(&run_control_path(&run_dir)).unwrap()["schema_version"],
            "run_control_v2"
        );
    }

    #[test]
    fn write_run_control_v2_run_id_persisted() {
        let root = TempDirGuard::new("run_ctrl_id");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        write_run_control_v2(&run_dir, "my_run_123", "running", &[], None).unwrap();
        assert_eq!(
            load_json_file(&run_control_path(&run_dir)).unwrap()["run_id"],
            "my_run_123"
        );
    }

    #[test]
    fn write_run_control_v2_updated_at_present() {
        let root = TempDirGuard::new("run_ctrl_updated");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        write_run_control_v2(&run_dir, "run_001", "running", &[], None).unwrap();
        assert!(
            !load_json_file(&run_control_path(&run_dir)).unwrap()["updated_at"]
                .as_str()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn write_run_control_v2_persists_in_sqlite_without_runtime_file() {
        let root = TempDirGuard::new("run_ctrl_sqlite_only");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        write_run_control_v2(&run_dir, "run_sqlite", "running", &[], None).unwrap();
        assert!(
            !run_control_path(&run_dir).exists(),
            "runtime/run_control.json should not be the canonical write target"
        );
        let store = BackingSqliteStore::open(&run_dir).expect("open sqlite store");
        let persisted = store
            .get_runtime_json(RUNTIME_KEY_RUN_CONTROL)
            .expect("load run control from sqlite")
            .expect("run control row should exist");
        assert_eq!(
            persisted.pointer("/run_id").and_then(Value::as_str),
            Some("run_sqlite")
        );
        assert_eq!(
            persisted.pointer("/status").and_then(Value::as_str),
            Some("running")
        );
    }

    #[test]
    fn append_jsonl_evidence_rows_route_to_sqlite_store() {
        let root = TempDirGuard::new("append_jsonl_sqlite_route");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        write_run_control_v2(&run_dir, "run_evidence", "running", &[], None).unwrap();
        let evidence_path = run_dir.join("runtime").join("evidence_records.jsonl");
        append_jsonl(
            &evidence_path,
            &json!({
                "run_id": "run_evidence",
                "schedule_idx": 0,
                "attempt": 1,
                "row_seq": 0,
                "slot_commit_id": "slot_x",
                "kind": "test"
            }),
        )
        .expect("append_jsonl should route into sqlite");
        let store = BackingSqliteStore::open(&run_dir).expect("open sqlite store");
        assert_eq!(store.row_count("evidence_rows").expect("row count"), 1);
    }

    #[test]
    fn append_jsonl_without_slot_identity_errors() {
        let root = TempDirGuard::new("append_jsonl_missing_identity");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        write_run_control_v2(&run_dir, "run_fallback", "running", &[], None).unwrap();
        let evidence_path = run_dir.join("runtime").join("evidence_records.jsonl");
        let err = append_jsonl(
            &evidence_path,
            &json!({
                "schema_version": "evidence_record_v1",
                "ids": {
                    "trial_id": "trial_1"
                }
            }),
        )
        .expect_err("append_jsonl must reject rows without sqlite slot identity");
        assert!(
            err.to_string().contains("missing sqlite identity fields"),
            "unexpected error: {}",
            err
        );

        let store = BackingSqliteStore::open(&run_dir).expect("open sqlite store");
        assert_eq!(
            store.row_count("evidence_rows").expect("row count"),
            0,
            "rows without slot identity must not be routed into sqlite evidence table"
        );
        assert!(
            !evidence_path.exists(),
            "no fallback file should be created"
        );
    }

    #[test]
    fn write_trial_state_running() {
        let root = TempDirGuard::new("trial_state_running");
        write_trial_state(&root.path, "trial_1", "running", None, None, None).unwrap();
        let loaded = load_json_file(&root.path.join("trial_state.json")).unwrap();
        assert_eq!(loaded["status"], "running");
        assert_eq!(loaded["trial_id"], "trial_1");
    }

    fn runtime_trial_attempt_state_fixture(phase: TrialPhase) -> TrialAttemptState {
        TrialAttemptState {
            key: TrialAttemptKey {
                schedule_idx: 0,
                attempt: 1,
            },
            slot: AttemptSlotRef {
                schedule_idx: 0,
                variant_id: "variant_a".to_string(),
                task_id: "task_a".to_string(),
                repl_idx: 0,
            },
            phase,
            paused_from_phase: None,
            fs: AttemptFsLayout {
                attempt_dir: "/tmp/attempt".to_string(),
                in_dir: "/tmp/in".to_string(),
                out_dir: "/tmp/out".to_string(),
                telemetry_mounts: Vec::new(),
                logs_dir: "/tmp/logs".to_string(),
            },
            task_sandbox: None,
            grading_sandbox: None,
            agent_phase: None,
            grading_phase: None,
            mapping_phase: None,
            candidate_artifact: None,
        }
    }

    fn runtime_trial_attempt_state_with_task_container(
        phase: TrialPhase,
        container_id: &str,
    ) -> TrialAttemptState {
        let mut state = runtime_trial_attempt_state_fixture(phase);
        state.task_sandbox = Some(TaskSandboxState {
            container_id: container_id.to_string(),
            image: "python:3.11-slim".to_string(),
            workdir: "/workspace/task".to_string(),
            materialization: TaskMaterializationSpec {
                kind: TaskMaterializationKind::TaskImage,
                task_bundle_ref: None,
            },
        });
        state
    }

    #[test]
    fn trial_runtime_state_reconciles_abandoned_and_committed() {
        let root = TempDirGuard::new("trial_runtime_state_reconcile");
        trial::state::write_trial_attempt_state(
            &root.path,
            &runtime_trial_attempt_state_fixture(TrialPhase::AgentRunning),
        )
        .expect("write runtime state");

        trial::state::reconcile_trial_attempt_as_abandoned(&root.path)
            .expect("reconcile abandoned");
        let abandoned = trial::state::load_trial_attempt_state(&root.path).expect("load abandoned");
        assert_eq!(abandoned.state.phase, TrialPhase::Abandoned);

        trial::state::reconcile_trial_attempt_as_committed(&root.path)
            .expect("reconcile committed");
        let committed = trial::state::load_trial_attempt_state(&root.path).expect("load committed");
        assert_eq!(committed.state.phase, TrialPhase::Committed);

        trial::state::reconcile_trial_attempt_as_abandoned(&root.path)
            .expect("reconcile abandoned after commit");
        let still_committed =
            trial::state::load_trial_attempt_state(&root.path).expect("load after commit");
        assert_eq!(still_committed.state.phase, TrialPhase::Committed);
    }

    #[test]
    fn trial_runtime_state_preserves_paused_and_killed_from_abandon_reconcile() {
        let root = TempDirGuard::new("trial_runtime_state_terminal_reconcile");

        trial::state::write_trial_attempt_state(
            &root.path,
            &runtime_trial_attempt_state_fixture(TrialPhase::AgentRunning),
        )
        .expect("write runtime state");
        trial::state::reconcile_trial_attempt_as_paused(&root.path).expect("pause reconcile");
        trial::state::reconcile_trial_attempt_as_abandoned(&root.path)
            .expect("abandon paused reconcile");
        let paused = trial::state::load_trial_attempt_state(&root.path).expect("load paused");
        assert_eq!(paused.state.phase, TrialPhase::Paused);

        trial::state::reconcile_trial_attempt_as_killed(&root.path).expect("kill reconcile");
        trial::state::reconcile_trial_attempt_as_abandoned(&root.path)
            .expect("abandon killed reconcile");
        let killed = trial::state::load_trial_attempt_state(&root.path).expect("load killed");
        assert_eq!(killed.state.phase, TrialPhase::Killed);
    }

    #[test]
    fn trial_runtime_state_restores_paused_phase_on_resume_reconcile() {
        let root = TempDirGuard::new("trial_runtime_state_resume_reconcile");

        trial::state::write_trial_attempt_state(
            &root.path,
            &runtime_trial_attempt_state_fixture(TrialPhase::GraderRunning),
        )
        .expect("write runtime state");
        trial::state::reconcile_trial_attempt_as_paused(&root.path).expect("pause reconcile");

        let paused = trial::state::load_trial_attempt_state(&root.path).expect("load paused");
        assert_eq!(paused.state.phase, TrialPhase::Paused);
        assert_eq!(
            paused.state.paused_from_phase,
            Some(TrialPhase::GraderRunning)
        );

        trial::state::reconcile_trial_attempt_as_resumed(&root.path).expect("resume reconcile");
        let resumed = trial::state::load_trial_attempt_state(&root.path).expect("load resumed");
        assert_eq!(resumed.state.phase, TrialPhase::GraderRunning);
        assert_eq!(resumed.state.paused_from_phase, None);
    }

    #[test]
    fn write_trial_state_paused_with_label() {
        let root = TempDirGuard::new("trial_state_paused");
        write_trial_state(
            &root.path,
            "trial_1",
            "paused",
            Some("checkpoint_pause"),
            None,
            None,
        )
        .unwrap();
        let loaded = load_json_file(&root.path.join("trial_state.json")).unwrap();
        assert_eq!(loaded["pause_label"], "checkpoint_pause");
    }

    #[test]
    fn write_trial_state_completed() {
        let root = TempDirGuard::new("trial_state_completed");
        write_trial_state(&root.path, "trial_1", "completed", None, None, None).unwrap();
        assert_eq!(
            load_json_file(&root.path.join("trial_state.json")).unwrap()["status"],
            "completed"
        );
    }

    #[test]
    fn write_trial_state_failed_with_exit_reason() {
        let root = TempDirGuard::new("trial_state_failed");
        write_trial_state(&root.path, "trial_1", "failed", None, None, Some("timeout")).unwrap();
        let loaded = load_json_file(&root.path.join("trial_state.json")).unwrap();
        assert_eq!(loaded["exit_reason"], "timeout");
    }

    #[test]
    fn write_trial_state_schema_version() {
        let root = TempDirGuard::new("trial_state_schema");
        write_trial_state(&root.path, "trial_1", "running", None, None, None).unwrap();
        assert_eq!(
            load_json_file(&root.path.join("trial_state.json")).unwrap()["schema_version"],
            "trial_state_v1"
        );
    }

    #[test]
    fn write_trial_state_with_checkpoint() {
        let root = TempDirGuard::new("trial_state_cp");
        write_trial_state(
            &root.path,
            "trial_1",
            "paused",
            None,
            Some("checkpoint_5"),
            None,
        )
        .unwrap();
        assert_eq!(
            load_json_file(&root.path.join("trial_state.json")).unwrap()["checkpoint_selected"],
            "checkpoint_5"
        );
    }

    #[test]
    fn run_control_guard_marks_failed_on_drop() {
        let root = TempDirGuard::new("guard_drop_fail");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        {
            let _guard = RunControlGuard::new(&run_dir, "run_001");
        }
        assert_eq!(
            load_json_file(&run_control_path(&run_dir)).unwrap()["status"],
            "failed"
        );
    }

    #[test]
    fn run_control_guard_complete_prevents_drop_fail() {
        let root = TempDirGuard::new("guard_complete");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        {
            let mut guard = RunControlGuard::new(&run_dir, "run_001");
            guard.complete("completed").unwrap();
        }
        assert_eq!(
            load_json_file(&run_control_path(&run_dir)).unwrap()["status"],
            "completed"
        );
    }

    #[test]
    fn trial_state_guard_marks_aborted_on_drop() {
        let root = TempDirGuard::new("trial_guard_drop");
        {
            let _guard = TrialStateGuard::new(&root.path, "trial_1");
        }
        let loaded = load_json_file(&root.path.join("trial_state.json")).unwrap();
        assert_eq!(loaded["status"], "failed");
        assert_eq!(loaded["exit_reason"], "aborted");
    }

    #[test]
    fn trial_state_guard_complete_prevents_drop_abort() {
        let root = TempDirGuard::new("trial_guard_complete");
        {
            let mut guard = TrialStateGuard::new(&root.path, "trial_1");
            guard.complete("completed", None).unwrap();
        }
        assert_eq!(
            load_json_file(&root.path.join("trial_state.json")).unwrap()["status"],
            "completed"
        );
    }

    #[test]
    fn create_unique_run_dir_creates_expected_structure() {
        let root = TempDirGuard::new("unique_run");
        let (run_id, run_dir) = create_unique_run_dir(&root.path).unwrap();
        assert!(run_dir.exists());
        assert!(run_id.starts_with("run_"));
    }

    #[test]
    fn create_unique_run_dir_unique_ids() {
        let root = TempDirGuard::new("unique_run_ids");
        let (id_a, _) = create_unique_run_dir(&root.path).unwrap();
        let (id_b, _) = create_unique_run_dir(&root.path).unwrap();
        assert_ne!(id_a, id_b);
    }

    #[test]
    fn run_control_path_correct() {
        assert_eq!(
            run_control_path(&PathBuf::from("/tmp/run_001")),
            PathBuf::from("/tmp/run_001/runtime/run_control.json")
        );
    }

    #[test]
    fn write_run_session_state_roundtrip() {
        let root = TempDirGuard::new("session_roundtrip");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        let behavior = RunBehavior {
            network_mode_override: Some("bridge".to_string()),
            require_network_none: false,
        };
        let execution = RunExecutionOptions {
            executor: Some(ExecutorKind::LocalDocker),
            materialize: Some(MaterializationMode::Full),
            runtime_env: BTreeMap::new(),
            runtime_env_files: Vec::new(),
        };
        write_run_session_state(&run_dir, "run_001", &behavior, &execution).unwrap();
        let loaded = load_run_session_state(&run_dir).unwrap();
        assert_eq!(loaded.run_id, "run_001");
        assert_eq!(loaded.schema_version, "run_session_state_v1");
    }

    #[test]
    fn run_session_state_preserves_behavior() {
        let root = TempDirGuard::new("session_behavior");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        let behavior = RunBehavior {
            network_mode_override: Some("host".to_string()),
            require_network_none: true,
        };
        write_run_session_state(
            &run_dir,
            "run_002",
            &behavior,
            &RunExecutionOptions {
                executor: None,
                materialize: None,
                runtime_env: BTreeMap::new(),
                runtime_env_files: Vec::new(),
            },
        )
        .unwrap();
        let loaded = load_run_session_state(&run_dir).unwrap();
        assert_eq!(
            loaded.behavior.network_mode_override,
            Some("host".to_string())
        );
        assert!(loaded.behavior.require_network_none);
    }

    #[test]
    fn run_session_state_preserves_execution_options() {
        let root = TempDirGuard::new("session_execution");
        let run_dir = root.path.join("run");
        ensure_dir(&run_dir.join("runtime")).unwrap();
        let behavior = RunBehavior {
            network_mode_override: None,
            require_network_none: false,
        };
        let execution = RunExecutionOptions {
            executor: Some(ExecutorKind::LocalDocker),
            materialize: Some(MaterializationMode::MetadataOnly),
            runtime_env: BTreeMap::new(),
            runtime_env_files: Vec::new(),
        };
        write_run_session_state(&run_dir, "run_003", &behavior, &execution).unwrap();
        assert_eq!(
            load_run_session_state(&run_dir).unwrap().execution.executor,
            Some(ExecutorKind::LocalDocker)
        );
    }

    #[test]
    fn run_control_active_trials_parses_v2() {
        let control = json!({"schema_version": "run_control_v2", "active_trials": {"trial_1": {"trial_id": "trial_1", "worker_id": "worker_a", "schedule_idx": 0, "control": null}}});
        let trials = run_control_active_trials(&control);
        assert_eq!(trials.len(), 1);
        assert_eq!(trials[0].trial_id, "trial_1");
    }

    #[test]
    fn run_control_active_trials_empty_when_none() {
        assert!(run_control_active_trials(&json!({"schema_version": "run_control_v2"})).is_empty());
    }

    // ───────────────────────────────────────────────────────────────────
    // Batch 8-10: Build Package, Benchmark, Worker Backend, Utilities
    // ───────────────────────────────────────────────────────────────────

    #[test]
    fn atomic_write_bytes_creates_file() {
        let root = TempDirGuard::new("aw_bytes_create");
        let path = root.path.join("test.bin");
        atomic_write_bytes(&path, b"hello").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"hello");
    }

    #[test]
    fn atomic_write_bytes_overwrites_existing() {
        let root = TempDirGuard::new("aw_bytes_overwrite");
        let path = root.path.join("test.bin");
        atomic_write_bytes(&path, b"old").unwrap();
        atomic_write_bytes(&path, b"new").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"new");
    }

    #[test]
    fn atomic_write_bytes_creates_parent_dirs() {
        let root = TempDirGuard::new("aw_parent");
        let path = root.path.join("nested").join("deep").join("file.txt");
        atomic_write_bytes(&path, b"content").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"content");
    }

    #[test]
    fn atomic_write_json_pretty_roundtrip() {
        let root = TempDirGuard::new("aw_json_roundtrip");
        let path = root.path.join("test.json");
        let value = json!({"key": "value", "number": 42});
        atomic_write_json_pretty(&path, &value).unwrap();
        assert_eq!(load_json_file(&path).unwrap(), value);
    }

    #[test]
    fn load_json_file_missing_fails() {
        let root = TempDirGuard::new("load_json_missing");
        assert!(load_json_file(&root.path.join("missing.json")).is_err());
    }

    #[test]
    fn load_json_file_invalid_json_fails() {
        let root = TempDirGuard::new("load_json_invalid");
        let path = root.path.join("bad.json");
        fs::write(&path, "not valid json {{{").unwrap();
        assert!(load_json_file(&path).is_err());
    }

    #[test]
    fn load_json_file_valid_roundtrip() {
        let root = TempDirGuard::new("load_json_valid");
        let path = root.path.join("data.json");
        let value = json!({"array": [1, 2, 3], "nested": {"key": "value"}});
        atomic_write_json_pretty(&path, &value).unwrap();
        assert_eq!(load_json_file(&path).unwrap(), value);
    }

    #[test]
    fn set_json_pointer_value_creates_nested() {
        let mut root = json!({});
        set_json_pointer_value(&mut root, "/a/b/c", json!("deep")).unwrap();
        assert_eq!(root["a"]["b"]["c"], json!("deep"));
    }

    #[test]
    fn set_json_pointer_value_overwrites_existing() {
        let mut root = json!({"a": {"b": "old"}});
        set_json_pointer_value(&mut root, "/a/b", json!("new")).unwrap();
        assert_eq!(root["a"]["b"], json!("new"));
    }

    #[test]
    fn set_json_pointer_value_replaces_root() {
        let mut root = json!({"old": "data"});
        set_json_pointer_value(&mut root, "", json!("replaced")).unwrap();
        assert_eq!(root, json!("replaced"));
    }

    #[test]
    fn set_json_pointer_value_invalid_pointer_fails() {
        assert!(set_json_pointer_value(&mut json!({}), "no_slash", json!(1)).is_err());
    }

    #[test]
    fn set_json_pointer_value_deep_nested_creates_intermediates() {
        let mut root = json!({});
        set_json_pointer_value(&mut root, "/a/b/c/d", json!(42)).unwrap();
        assert_eq!(root["a"]["b"]["c"]["d"], json!(42));
    }

    #[test]
    fn copy_path_into_package_file() {
        let root = TempDirGuard::new("copy_pkg_file");
        let src = root.path.join("source.txt");
        let dst = root.path.join("dest").join("source.txt");
        fs::write(&src, "content").unwrap();
        copy_path_into_package(&src, &dst).unwrap();
        assert_eq!(fs::read_to_string(&dst).unwrap(), "content");
    }

    #[test]
    fn copy_path_into_package_dir() {
        let root = TempDirGuard::new("copy_pkg_dir");
        let src_dir = root.path.join("source_dir");
        ensure_dir(&src_dir).unwrap();
        fs::write(src_dir.join("file.txt"), "content").unwrap();
        let dst_dir = root.path.join("dest_dir");
        copy_path_into_package(&src_dir, &dst_dir).unwrap();
        assert_eq!(
            fs::read_to_string(dst_dir.join("file.txt")).unwrap(),
            "content"
        );
    }

    #[test]
    fn stage_source_into_package_absolute_path() {
        let root = TempDirGuard::new("stage_abs");
        let exp_dir = root.path.join("exp");
        let pkg_dir = root.path.join("pkg");
        ensure_dir(&exp_dir).unwrap();
        ensure_dir(&pkg_dir).unwrap();
        let src = root.path.join("artifact.tar");
        fs::write(&src, "data").unwrap();
        let mut copies = BTreeMap::new();
        let mut counter = 0usize;
        let rel = stage_source_into_package(
            src.to_str().unwrap(),
            &exp_dir,
            &pkg_dir,
            "agent_builds",
            "build",
            &mut copies,
            &mut counter,
        )
        .unwrap();
        assert!(!rel.is_empty());
        assert_eq!(counter, 1);
    }

    #[test]
    fn stage_source_into_package_relative_path() {
        let root = TempDirGuard::new("stage_rel");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).unwrap();
        fs::write(exp_dir.join("agent.tar"), "data").unwrap();
        let pkg_dir = root.path.join("pkg");
        ensure_dir(&pkg_dir).unwrap();
        let mut copies = BTreeMap::new();
        let mut counter = 0usize;
        assert!(!stage_source_into_package(
            "agent.tar",
            &exp_dir,
            &pkg_dir,
            "agent_builds",
            "build",
            &mut copies,
            &mut counter
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn stage_source_into_package_missing_source_fails() {
        let root = TempDirGuard::new("stage_missing");
        let exp_dir = root.path.join("exp");
        let pkg_dir = root.path.join("pkg");
        ensure_dir(&exp_dir).unwrap();
        ensure_dir(&pkg_dir).unwrap();
        let mut copies = BTreeMap::new();
        let mut counter = 0usize;
        assert!(stage_source_into_package(
            "nonexistent.tar",
            &exp_dir,
            &pkg_dir,
            "agent_builds",
            "build",
            &mut copies,
            &mut counter
        )
        .is_err());
    }

    #[test]
    fn stage_source_into_package_directory_copied() {
        let root = TempDirGuard::new("stage_dir");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).unwrap();
        let src_dir = exp_dir.join("agent_dir");
        ensure_dir(&src_dir).unwrap();
        fs::write(src_dir.join("main.py"), "print('hello')").unwrap();
        let pkg_dir = root.path.join("pkg");
        ensure_dir(&pkg_dir).unwrap();
        let mut copies = BTreeMap::new();
        let mut counter = 0usize;
        let rel = stage_source_into_package(
            "agent_dir",
            &exp_dir,
            &pkg_dir,
            "agent_builds",
            "build",
            &mut copies,
            &mut counter,
        )
        .unwrap();
        assert!(pkg_dir.join(rel.trim_start_matches('/')).exists());
    }

    #[test]
    fn stage_source_deduplicates_same_source() {
        let root = TempDirGuard::new("stage_dedup");
        let exp_dir = root.path.join("exp");
        ensure_dir(&exp_dir).unwrap();
        fs::write(exp_dir.join("artifact.tar"), "data").unwrap();
        let pkg_dir = root.path.join("pkg");
        ensure_dir(&pkg_dir).unwrap();
        let mut copies = BTreeMap::new();
        let mut counter = 0usize;
        let rel1 = stage_source_into_package(
            "artifact.tar",
            &exp_dir,
            &pkg_dir,
            "agent_builds",
            "build",
            &mut copies,
            &mut counter,
        )
        .unwrap();
        let rel2 = stage_source_into_package(
            "artifact.tar",
            &exp_dir,
            &pkg_dir,
            "agent_builds",
            "build",
            &mut copies,
            &mut counter,
        )
        .unwrap();
        assert_eq!(rel1, rel2);
        assert_eq!(counter, 1);
    }

    #[test]
    fn normalize_task_prompt_aliases_no_aliases_noop() {
        let task = json!({"input": {"prompt": "hello"}, "metadata": "x"});
        let result = normalize_task_prompt_aliases(&task);
        assert_eq!(result["input"]["prompt"], "hello");
    }

    #[test]
    fn replay_grade_for_integration_cli_basic() {
        assert_eq!(replay_grade_for_integration("cli_basic"), "best_effort");
    }

    #[test]
    fn replay_grade_for_integration_sdk_full() {
        assert_eq!(replay_grade_for_integration("sdk_full"), "strict");
    }

    #[test]
    fn replay_grade_for_integration_sdk_control() {
        assert_eq!(replay_grade_for_integration("sdk_control"), "checkpointed");
    }

    #[test]
    fn replay_grade_for_integration_cli_events() {
        assert_eq!(replay_grade_for_integration("cli_events"), "best_effort");
    }

    #[test]
    fn replay_grade_for_integration_otel() {
        assert_eq!(replay_grade_for_integration("otel"), "best_effort");
    }

    #[test]
    fn replay_grade_for_integration_unknown_level() {
        assert_eq!(
            replay_grade_for_integration("something_unknown"),
            "best_effort"
        );
    }

    // ── Batch 6 extension: resolve_variant_plan ──

    #[test]
    fn resolve_variant_plan_legacy_baseline_plus_two_treatments() {
        let exp = json!({
            "baseline": { "variant_id": "ctrl", "bindings": { "lr": 0.01 } },
            "variant_plan": [
                { "variant_id": "fast", "bindings": { "lr": 0.1 } },
                { "variant_id": "slow", "bindings": { "lr": 0.001 } }
            ]
        });
        let (variants, baseline_id) = resolve_variant_plan(&exp).unwrap();
        assert_eq!(baseline_id, "ctrl");
        assert_eq!(variants.len(), 3);
        assert_eq!(variants[0].id, "ctrl");
        assert_eq!(variants[1].id, "fast");
        assert_eq!(variants[2].id, "slow");
        assert_eq!(variants[1].bindings["lr"], json!(0.1));
    }

    #[test]
    fn resolve_variant_plan_legacy_no_variant_plan_returns_baseline_only() {
        let exp = json!({
            "baseline": { "variant_id": "base", "bindings": { "x": 1 } }
        });
        let (variants, baseline_id) = resolve_variant_plan(&exp).unwrap();
        assert_eq!(baseline_id, "base");
        assert_eq!(variants.len(), 1);
    }

    #[test]
    fn resolve_variant_plan_legacy_variant_bindings_default_to_empty_object() {
        let exp = json!({
            "baseline": { "variant_id": "b" },
            "variant_plan": [{ "variant_id": "v1" }]
        });
        let (variants, _) = resolve_variant_plan(&exp).unwrap();
        assert!(variants[1].bindings.is_object());
        assert_eq!(variants[1].bindings.as_object().unwrap().len(), 0);
    }

    #[test]
    fn resolve_variant_plan_legacy_variant_array_bindings_fails() {
        let exp = json!({
            "baseline": { "variant_id": "b" },
            "variant_plan": [{ "variant_id": "v1", "bindings": [1, 2] }]
        });
        assert!(resolve_variant_plan(&exp).is_err());
    }

    #[test]
    fn resolve_variant_plan_missing_baseline_variant_id_fails() {
        let exp = json!({ "baseline": { "bindings": {} } });
        assert!(resolve_variant_plan(&exp).is_err());
    }

    #[test]
    fn resolve_variant_plan_legacy_runtime_overrides_attached() {
        let exp = json!({
            "baseline": { "variant_id": "b", "runtime_overrides": { "agent": { "timeout_ms": 5000 } } },
            "variant_plan": []
        });
        let (variants, _) = resolve_variant_plan(&exp).unwrap();
        assert!(variants[0].runtime_overrides.is_some());
        assert_eq!(
            variants[0]
                .runtime_overrides
                .as_ref()
                .unwrap()
                .pointer("/agent/timeout_ms"),
            Some(&json!(5000))
        );
    }

    #[test]
    fn resolve_variant_plan_legacy_runtime_overrides_must_be_object() {
        let exp = json!({
            "baseline": { "variant_id": "b", "runtime_overrides": "bad" }
        });
        assert!(resolve_variant_plan(&exp).is_err());
    }

    #[test]
    fn resolve_variant_plan_variant_without_id_fails() {
        let exp = json!({
            "baseline": { "variant_id": "b" },
            "variant_plan": [{ "bindings": {} }]
        });
        assert!(resolve_variant_plan(&exp).is_err());
    }

    #[test]
    fn resolve_variant_plan_accepts_config_alias_for_bindings() {
        let exp = json!({
            "baseline": { "variant_id": "b" },
            "variant_plan": [{ "variant_id": "v1", "config": { "k": "v" } }]
        });
        let (variants, _) = resolve_variant_plan(&exp).unwrap();
        assert_eq!(variants[1].bindings["k"], json!("v"));
    }

    #[test]
    fn resolve_variant_plan_legacy_baseline_image_promotes_to_runtime_override() {
        let exp = json!({
            "baseline": { "variant_id": "b", "image": "custom:latest" }
        });
        let (variants, _) = resolve_variant_plan(&exp).unwrap();
        assert!(variants[0].runtime_overrides.is_some());
        assert_eq!(
            variants[0]
                .runtime_overrides
                .as_ref()
                .unwrap()
                .pointer("/agent/image"),
            Some(&json!("custom:latest"))
        );
    }

    // ── Batch 6 extension: build_runtime_contract_env ──

    #[test]
    fn build_runtime_contract_env_projects_contract_keys_without_task_image_from_agent_input() {
        let input = json!({
            "ids": { "trial_id": "t1", "variant_id": "v1", "task_id": "task_a", "repl_idx": 2 },
            "task": { "id": "task_a" },
            "environment": { "image": "poison/from-agent-input:latest" },
            "policy": { "timeout_ms": 30000 },
            "ext": {
                "task_spec": {
                    "environment": { "image": "myimg:1" },
                    "workspace": {
                        "mode": "scratch",
                        "base": { "kind": "empty" },
                        "overlays": [],
                        "aux_mounts": []
                    },
                    "dependencies": {},
                    "limits": {}
                }
            }
        });
        let io = prepared_trial_io_fixture_with_contract_paths(
            "/agentlab/in/trial_input.json",
            "/agentlab/in/grader_input.json",
            "/agentlab/out/result.json",
            "/agentlab/out/raw_grader_output.json",
            "/agentlab/out/mapped_grader_output.json",
            "/agentlab/out/trajectory.jsonl",
        );
        let env = build_runtime_contract_env("run_1", &input, &io, None, Some(30000));
        assert_eq!(env.get(AGENTLAB_ENV_RUN_ID).unwrap(), "run_1");
        assert_eq!(env.get(AGENTLAB_ENV_TRIAL_ID).unwrap(), "t1");
        assert_eq!(env.get(AGENTLAB_ENV_VARIANT_ID).unwrap(), "v1");
        assert_eq!(env.get(AGENTLAB_ENV_TASK_ID).unwrap(), "task_a");
        assert_eq!(env.get(AGENTLAB_ENV_REPL_IDX).unwrap(), "2");
        assert_eq!(env.get(AGENTLAB_ENV_TIMEOUT_MS).unwrap(), "30000");
        assert_eq!(
            env.get(AGENTLAB_ENV_TRIAL_INPUT_PATH).unwrap(),
            "/agentlab/in/trial_input.json"
        );
        assert!(
            !env.contains_key(AGENTLAB_ENV_TASK_IMAGE),
            "task image must come from PreparedTaskEnvironment, not agent-facing input"
        );
    }

    #[test]
    fn build_runtime_contract_env_minimal_input_still_projects_contract_keys() {
        let input = json!({ "ids": { "trial_id": "t1" } });
        let io = prepared_trial_io_fixture_with_contract_paths(
            "/agentlab/in/trial_input.json",
            "/agentlab/in/grader_input.json",
            "/agentlab/out/result.json",
            "/agentlab/out/raw_grader_output.json",
            "/agentlab/out/mapped_grader_output.json",
            "/agentlab/out/trajectory.jsonl",
        );
        let env = build_runtime_contract_env("run_1", &input, &io, None, Some(5000));
        assert_eq!(
            env.get(AGENTLAB_ENV_TRIAL_INPUT_PATH).unwrap(),
            "/agentlab/in/trial_input.json"
        );
    }

    #[test]
    fn build_runtime_contract_env_no_timeout_omits_key() {
        let input = json!({ "ids": { "trial_id": "t1" } });
        let io = prepared_trial_io_fixture_with_contract_paths(
            "/in/trial_input.json",
            "/in/grader_input.json",
            "/out/result.json",
            "/out/raw_grader_output.json",
            "/out/mapped_grader_output.json",
            "/out/trajectory.jsonl",
        );
        let env = build_runtime_contract_env("run_1", &input, &io, None, None);
        assert!(!env.contains_key(AGENTLAB_ENV_TIMEOUT_MS));
    }

    #[test]
    fn build_runtime_contract_env_no_task_image_omits_key() {
        let input = json!({ "ids": { "trial_id": "t1" }, "task": {} });
        let io = prepared_trial_io_fixture_with_contract_paths(
            "/in/trial_input.json",
            "/in/grader_input.json",
            "/out/result.json",
            "/out/raw_grader_output.json",
            "/out/mapped_grader_output.json",
            "/out/trajectory.jsonl",
        );
        let env = build_runtime_contract_env("run_1", &input, &io, None, None);
        assert!(!env.contains_key(AGENTLAB_ENV_TASK_IMAGE));
    }

    // ── Batch 6 extension: resolve_trial_timeout_ms ──

    #[test]
    fn resolve_trial_timeout_ms_reads_policy_field() {
        let input = json!({ "policy": { "timeout_ms": 60000 } });
        assert_eq!(resolve_trial_timeout_ms(&input), Some(60000));
    }

    #[test]
    fn resolve_trial_timeout_ms_both_missing_returns_none() {
        let input = json!({});
        assert_eq!(resolve_trial_timeout_ms(&input), None);
    }

    // ── Batch 8: sealed/build loader split ──

    fn write_empty_runtime_staging_manifest(package_dir: &Path) -> String {
        let path = package_dir.join(STAGING_MANIFEST_FILE);
        fs::write(
            &path,
            serde_json::to_string(&json!({
                "schema_version": STAGING_MANIFEST_SCHEMA_VERSION,
                "variants": {}
            }))
            .expect("staging manifest json"),
        )
        .expect("write staging manifest");
        sha256_file(&path).expect("staging manifest digest")
    }

    fn write_runtime_staging_manifest(package_dir: &Path, payload: &Value) {
        fs::write(
            package_dir.join(STAGING_MANIFEST_FILE),
            serde_json::to_string(payload).expect("staging manifest json"),
        )
        .expect("write staging manifest");
    }

    #[test]
    fn load_sealed_package_for_run_directory_without_manifest_fails() {
        let guard = TempDirGuard::new("load_exp_no_manifest");
        let err = load_sealed_package_for_run(&guard.path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("run_input_invalid_kind"),
            "expected run_input_invalid_kind error, got: {msg}"
        );
    }

    #[test]
    fn load_sealed_package_for_run_missing_file_fails() {
        let path = Path::new("/nonexistent/experiment.yaml");
        assert!(load_sealed_package_for_run(path).is_err());
    }

    #[test]
    fn load_authoring_input_for_build_rejects_manifest() {
        let guard = TempDirGuard::new("load_exp_manifest_as_build_input");
        let manifest = guard.path.join("manifest.json");
        fs::write(&manifest, r#"{}"#).unwrap();
        let err = load_authoring_input_for_build(&manifest, None).unwrap_err();
        assert!(err.to_string().contains("build_input_invalid_kind"));
    }

    #[test]
    fn load_sealed_package_for_run_directory_package_with_manifest_loads() {
        let guard = TempDirGuard::new("load_exp_pkg");
        let manifest = guard.path.join("manifest.json");
        let checksums = guard.path.join("checksums.json");
        fs::write(
            guard.path.join("resolved_experiment.json"),
            r#"{"version":"0.5","experiment":{"id":"e1","workload_type":"agent_runtime"}}"#,
        )
        .unwrap();
        let resolved_digest = sha256_file(&guard.path.join("resolved_experiment.json")).unwrap();
        let staging_digest = write_empty_runtime_staging_manifest(&guard.path);
        let files = json!({
            "resolved_experiment.json": resolved_digest,
            STAGING_MANIFEST_FILE: staging_digest
        });
        let package_digest = canonical_json_digest(&files);
        fs::write(
            &checksums,
            serde_json::to_string(&json!({
                "schema_version": "sealed_package_checksums_v2",
                "files": files
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            guard.path.join("package.lock"),
            format!(
                "{{\"schema_version\":\"sealed_package_lock_v1\",\"package_digest\":\"{}\"}}",
                package_digest
            ),
        )
        .unwrap();
        fs::write(
            &manifest,
            format!(
                "{{\"schema_version\":\"sealed_run_package_v2\",\"created_at\":\"2026-03-04T00:00:00Z\",\"resolved_experiment\":{{\"version\":\"0.5\",\"experiment\":{{\"id\":\"e1\",\"workload_type\":\"agent_runtime\"}}}},\"checksums_ref\":\"checksums.json\",\"package_digest\":\"{}\"}}",
                package_digest
            ),
        )
        .unwrap();
        let loaded = load_sealed_package_for_run(&guard.path).unwrap();
        assert_eq!(loaded.json_value.pointer("/version"), Some(&json!("0.5")));
        assert_eq!(
            loaded.json_value.pointer("/experiment/id"),
            Some(&json!("e1"))
        );
    }

    #[test]
    fn load_sealed_package_for_run_rejects_legacy_v1_experiment() {
        let guard = TempDirGuard::new("load_exp_pkg_v1_reject");
        let manifest = guard.path.join("manifest.json");
        let checksums = guard.path.join("checksums.json");
        fs::write(
            guard.path.join("resolved_experiment.json"),
            r#"{"version":"1.0","experiment":{"id":"e1"}}"#,
        )
        .unwrap();
        let resolved_digest = sha256_file(&guard.path.join("resolved_experiment.json")).unwrap();
        let staging_digest = write_empty_runtime_staging_manifest(&guard.path);
        let files = json!({
            "resolved_experiment.json": resolved_digest,
            STAGING_MANIFEST_FILE: staging_digest
        });
        let package_digest = canonical_json_digest(&files);
        fs::write(
            &checksums,
            serde_json::to_string(&json!({
                "schema_version": "sealed_package_checksums_v2",
                "files": files
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            guard.path.join("package.lock"),
            format!(
                "{{\"schema_version\":\"sealed_package_lock_v1\",\"package_digest\":\"{}\"}}",
                package_digest
            ),
        )
        .unwrap();
        fs::write(
            &manifest,
            format!(
                "{{\"schema_version\":\"sealed_run_package_v2\",\"created_at\":\"2026-03-04T00:00:00Z\",\"resolved_experiment\":{{\"version\":\"1.0\",\"experiment\":{{\"id\":\"e1\"}}}},\"checksums_ref\":\"checksums.json\",\"package_digest\":\"{}\"}}",
                package_digest
            ),
        )
        .unwrap();

        let loaded = load_sealed_package_for_run(&guard.path).expect("load sealed package");
        assert_eq!(
            loaded
                .json_value
                .pointer("/version")
                .and_then(Value::as_str)
                .unwrap_or(""),
            "1.0"
        );
    }

    #[test]
    fn load_sealed_package_for_run_ignores_manifest_resolved_experiment_payload() {
        let guard = TempDirGuard::new("load_exp_pkg_manifest_tamper");
        let resolved_path = guard.path.join("resolved_experiment.json");
        fs::write(
            &resolved_path,
            r#"{"version":"0.5","experiment":{"id":"from_checksums","workload_type":"agent_runtime"}}"#,
        )
        .unwrap();
        let resolved_digest = sha256_file(&resolved_path).unwrap();
        let staging_digest = write_empty_runtime_staging_manifest(&guard.path);
        fs::write(
            guard.path.join("checksums.json"),
            serde_json::to_string(&json!({
                "schema_version": "sealed_package_checksums_v2",
                "files": {
                    "resolved_experiment.json": resolved_digest,
                    STAGING_MANIFEST_FILE: staging_digest
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let package_digest = canonical_json_digest(&json!({
            "resolved_experiment.json": resolved_digest,
            STAGING_MANIFEST_FILE: staging_digest
        }));
        fs::write(
            guard.path.join("package.lock"),
            format!(
                "{{\"schema_version\":\"sealed_package_lock_v1\",\"package_digest\":\"{}\"}}",
                package_digest
            ),
        )
        .unwrap();
        fs::write(
            guard.path.join("manifest.json"),
            format!(
                "{{\"schema_version\":\"sealed_run_package_v2\",\"created_at\":\"2026-03-04T00:00:00Z\",\"resolved_experiment\":{{\"version\":\"0.5\",\"experiment\":{{\"id\":\"tampered_manifest\",\"workload_type\":\"agent_runtime\"}}}},\"checksums_ref\":\"checksums.json\",\"package_digest\":\"{}\"}}",
                package_digest
            ),
        )
        .unwrap();

        let loaded = load_sealed_package_for_run(&guard.path).unwrap();
        assert_eq!(
            loaded.json_value.pointer("/experiment/id"),
            Some(&json!("from_checksums"))
        );
    }

    #[test]
    fn load_sealed_package_for_run_rejects_checksum_mismatch() {
        let guard = TempDirGuard::new("load_exp_pkg_bad_checksum");
        fs::write(guard.path.join("resolved_experiment.json"), "{}").unwrap();
        let _staging_digest = write_empty_runtime_staging_manifest(&guard.path);
        fs::write(
            guard.path.join("checksums.json"),
            format!(
                "{{\"schema_version\":\"sealed_package_checksums_v2\",\"files\":{{\"resolved_experiment.json\":\"deadbeef\",\"{}\":\"deadbeef\"}}}}",
                STAGING_MANIFEST_FILE
            ),
        )
        .unwrap();
        let package_digest = canonical_json_digest(&json!({
            "resolved_experiment.json": "deadbeef",
            STAGING_MANIFEST_FILE: "deadbeef"
        }));
        fs::write(
            guard.path.join("package.lock"),
            format!(
                "{{\"schema_version\":\"sealed_package_lock_v1\",\"package_digest\":\"{}\"}}",
                package_digest
            ),
        )
        .unwrap();
        fs::write(
            guard.path.join("manifest.json"),
            format!(
                "{{\"schema_version\":\"sealed_run_package_v2\",\"created_at\":\"2026-03-04T00:00:00Z\",\"resolved_experiment\":{{}},\"checksums_ref\":\"checksums.json\",\"package_digest\":\"{}\"}}",
                package_digest
            ),
        )
        .unwrap();
        let err = load_sealed_package_for_run(&guard.path).unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn load_staging_specs_from_package_rejects_missing_variant_entries() {
        let guard = TempDirGuard::new("load_staging_specs_missing_variant");
        write_runtime_staging_manifest(
            &guard.path,
            &json!({
                "schema_version": STAGING_MANIFEST_SCHEMA_VERSION,
                "variants": {
                    "control": []
                }
            }),
        );

        let err = load_staging_specs_from_package(&guard.path, "treatment")
            .expect_err("missing variant entry should fail");
        assert!(
            err.to_string()
                .contains("runtime staging manifest missing entries for variant 'treatment'"),
            "{}",
            err
        );
    }

    #[test]
    fn load_staging_specs_from_package_rejects_destination_outside_contract_roots() {
        let guard = TempDirGuard::new("load_staging_specs_bad_destination");
        let packaged = guard.path.join("deps").join("defaults.json");
        ensure_dir(packaged.parent().unwrap()).expect("deps dir");
        fs::write(&packaged, "{}").expect("packaged runtime deps");
        write_runtime_staging_manifest(
            &guard.path,
            &json!({
                "schema_version": STAGING_MANIFEST_SCHEMA_VERSION,
                "variants": {
                    "control": [{
                        "original_relative_path": "overrides/defaults.json",
                        "packaged_path": "runtime_assets/defaults.json",
                        "runtime_path": "/tmp/defaults.json",
                        "required": true,
                        "read_only": true
                    }]
                }
            }),
        );

        let err = load_staging_specs_from_package(&guard.path, "control")
            .expect_err("invalid destination path should fail");
        assert!(
            err.to_string().contains(
                "must be under __AGENTLAB_TASK_WORKDIR__/.agentlab/support or /agentlab/state"
            ),
            "{}",
            err
        );
    }

    // ── Batch 10: DeterministicCommitter ──

    #[test]
    fn deterministic_committer_from_empty_progress() {
        let progress = ScheduleProgress {
            schema_version: "schedule_progress_v2".to_string(),
            run_id: "r1".to_string(),
            total_slots: 10,
            next_schedule_index: 0,
            next_trial_index: 1,
            schedule: Vec::new(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let committer = DeterministicCommitter::from_progress(&progress, &[]);
        assert_eq!(committer.next_commit_idx, 0);
        assert!(committer.committed_keys.is_empty());
        assert!(committer.pending_by_schedule.is_empty());
    }

    #[test]
    fn deterministic_committer_from_progress_with_completed_slots() {
        let progress = ScheduleProgress {
            schema_version: "schedule_progress_v2".to_string(),
            run_id: "r1".to_string(),
            total_slots: 10,
            next_schedule_index: 3,
            next_trial_index: 4,
            schedule: Vec::new(),
            completed_slots: vec![
                SlotCompletion {
                    schedule_index: 0,
                    trial_id: "trial_1".into(),
                    status: "completed".into(),
                    slot_commit_id: "c1".into(),
                    attempt: 1,
                },
                SlotCompletion {
                    schedule_index: 1,
                    trial_id: "trial_2".into(),
                    status: "completed".into(),
                    slot_commit_id: "c2".into(),
                    attempt: 1,
                },
                SlotCompletion {
                    schedule_index: 2,
                    trial_id: "trial_3".into(),
                    status: "failed".into(),
                    slot_commit_id: "c3".into(),
                    attempt: 1,
                },
            ],
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let committer = DeterministicCommitter::from_progress(&progress, &[]);
        assert_eq!(committer.next_commit_idx, 3);
        assert_eq!(committer.committed_keys.len(), 3);
    }

    #[test]
    fn deterministic_committer_enqueue_skipped_at_next_idx() {
        let progress = ScheduleProgress {
            schema_version: "schedule_progress_v2".to_string(),
            run_id: "r".to_string(),
            total_slots: 5,
            next_schedule_index: 0,
            next_trial_index: 1,
            schedule: Vec::new(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let mut committer = DeterministicCommitter::from_progress(&progress, &[]);
        let enqueued = committer.enqueue_skipped(0).unwrap();
        assert!(enqueued);
        assert_eq!(committer.pending_by_schedule.len(), 1);
    }

    #[test]
    fn deterministic_committer_enqueue_duplicate_returns_false() {
        let progress = ScheduleProgress {
            schema_version: "schedule_progress_v2".to_string(),
            run_id: "r".to_string(),
            total_slots: 5,
            next_schedule_index: 0,
            next_trial_index: 1,
            schedule: Vec::new(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let mut committer = DeterministicCommitter::from_progress(&progress, &[]);
        committer.enqueue_skipped(0).unwrap();
        let second = committer.enqueue_skipped(0).unwrap();
        assert!(!second, "duplicate enqueue should return false");
    }

    #[test]
    fn deterministic_committer_enqueue_stale_index_errors() {
        let progress = ScheduleProgress {
            schema_version: "schedule_progress_v2".to_string(),
            run_id: "r".to_string(),
            total_slots: 5,
            next_schedule_index: 5,
            next_trial_index: 6,
            schedule: Vec::new(),
            completed_slots: Vec::new(),
            pruned_variants: Vec::new(),
            consecutive_failures: BTreeMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let mut committer = DeterministicCommitter::from_progress(&progress, &[]);
        let err = committer.enqueue_skipped(2).unwrap_err();
        assert!(err.to_string().contains("stale completion"));
    }

    #[test]
    fn deterministic_committer_commit_key_for_slot_completion_deterministic() {
        let slot = SlotCompletion {
            schedule_index: 7,
            trial_id: "trial_8".to_string(),
            status: "completed".to_string(),
            slot_commit_id: "xyz".to_string(),
            attempt: 1,
        };
        let key = DeterministicCommitter::commit_key_for_slot_completion(&slot);
        assert_eq!(key, "7:trial_8:completed");
    }

    // ── Batch 10: highest_attempt_by_schedule ──

    #[test]
    fn highest_attempt_by_schedule_empty_returns_empty() {
        let result = highest_attempt_by_schedule(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn highest_attempt_by_schedule_tracks_max_attempt_per_index() {
        let records = vec![
            SlotCommitRecord {
                schema_version: "v1".to_string(),
                record_type: "slot_commit".to_string(),
                run_id: "r1".to_string(),
                schedule_idx: 0,
                slot_commit_id: "c1".to_string(),
                trial_id: "t1".to_string(),
                slot_status: "completed".to_string(),
                attempt: 1,
                recorded_at: Utc::now().to_rfc3339(),
                payload_digest: None,
                expected_rows: None,
                written_rows: None,
                facts_fsync_completed: None,
                runtime_fsync_completed: None,
            },
            SlotCommitRecord {
                schema_version: "v1".to_string(),
                record_type: "slot_commit".to_string(),
                run_id: "r1".to_string(),
                schedule_idx: 0,
                slot_commit_id: "c2".to_string(),
                trial_id: "t1".to_string(),
                slot_status: "failed".to_string(),
                attempt: 3,
                recorded_at: Utc::now().to_rfc3339(),
                payload_digest: None,
                expected_rows: None,
                written_rows: None,
                facts_fsync_completed: None,
                runtime_fsync_completed: None,
            },
            SlotCommitRecord {
                schema_version: "v1".to_string(),
                record_type: "slot_commit".to_string(),
                run_id: "r1".to_string(),
                schedule_idx: 1,
                slot_commit_id: "c3".to_string(),
                trial_id: "t2".to_string(),
                slot_status: "completed".to_string(),
                attempt: 2,
                recorded_at: Utc::now().to_rfc3339(),
                payload_digest: None,
                expected_rows: None,
                written_rows: None,
                facts_fsync_completed: None,
                runtime_fsync_completed: None,
            },
        ];
        let result = highest_attempt_by_schedule(&records);
        assert_eq!(*result.get(&0).unwrap(), 3);
        assert_eq!(*result.get(&1).unwrap(), 2);
    }

    // ── Batch 10: output_peer_path ──

    #[test]
    fn output_peer_path_replaces_filename() {
        assert_eq!(
            output_peer_path("/agentlab/out/result.json", "prediction.json"),
            "/agentlab/out/prediction.json"
        );
    }

    #[test]
    fn output_peer_path_no_parent_returns_filename() {
        assert_eq!(
            output_peer_path("result.json", "prediction.json"),
            "prediction.json"
        );
    }

    // ── Batch 10: find_project_root ──

    #[test]
    fn find_project_root_returns_parent_of_dot_lab() {
        let guard = TempDirGuard::new("find_root_lab");
        let lab_dir = guard.path.join(".lab");
        fs::create_dir_all(&lab_dir).unwrap();
        let result = find_project_root(&lab_dir);
        assert_eq!(result, guard.path);
    }

    #[test]
    fn find_project_root_no_dot_lab_returns_input() {
        let guard = TempDirGuard::new("find_root_none");
        let result = find_project_root(&guard.path);
        assert_eq!(result, guard.path);
    }

    // ── Mutation gate support tests ──

    #[test]
    fn validate_required_fields_v1_whitespace_experiment_id_fails() {
        let mut spec = json!({
            "version": "1.0",
            "experiment": {"id": "e1", "name": "test"},
            "dataset": {"path": "tasks.jsonl"},
            "design": {"replications": 1},
            "baseline": {"variant_id": "baseline"},
            "runtime": {"image": "img:latest", "command": ["python", "main.py"]}
        });
        spec["experiment"]["id"] = json!("  ");
        let err = validate_required_fields(&spec).unwrap_err();
        assert!(
            err.to_string().contains("legacy experiment version '1.0'"),
            "err: {err}"
        );
    }

    #[test]
    fn validate_required_fields_v1_whitespace_baseline_variant_id_fails() {
        let mut spec = json!({
            "version": "1.0",
            "experiment": {"id": "e1", "name": "test"},
            "dataset": {"path": "tasks.jsonl"},
            "design": {"replications": 1},
            "baseline": {"variant_id": "baseline"},
            "runtime": {"image": "img:latest", "command": ["python", "main.py"]}
        });
        spec["baseline"]["variant_id"] = json!("  ");
        let err = validate_required_fields(&spec).unwrap_err();
        assert!(
            err.to_string().contains("legacy experiment version '1.0'"),
            "err: {err}"
        );
    }
}
