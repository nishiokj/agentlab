use anyhow::{anyhow, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

pub const AGENTLAB_CONTRACT_ROOT: &str = "/agentlab";
pub const AGENTLAB_CONTRACT_IN_DIR: &str = "/agentlab/in";
pub const AGENTLAB_CONTRACT_OUT_DIR: &str = "/agentlab/out";
pub const AGENTLAB_CONTRACT_STATE_DIR: &str = "/agentlab/state";
pub const AGENTLAB_CONTRACT_WORKSPACE_DIR: &str = "/agentlab/workspace";
pub const AGENTLAB_CONTRACT_BIN_DIR: &str = "/agentlab/bin";
pub const AGENTLAB_CONTRACT_DEPS_DIR: &str = "/agentlab/deps";

pub const AGENTLAB_TRIAL_INPUT_PATH: &str = "/agentlab/in/trial_input.json";
pub const AGENTLAB_TRIAL_OUTPUT_PATH: &str = "/agentlab/out/trial_output.json";
pub const AGENTLAB_TRIAL_EVENTS_PATH: &str = "/agentlab/state/events.jsonl";
pub const AGENTLAB_CONTROL_PATH: &str = "/agentlab/state/lab_control.json";
pub const AGENTLAB_TASK_PATH: &str = "/agentlab/in/task.json";
pub const AGENTLAB_BINDINGS_PATH: &str = "/agentlab/in/bindings.json";
pub const AGENTLAB_DEPENDENCIES_PATH: &str = "/agentlab/in/dependencies.json";
pub const AGENTLAB_POLICY_PATH: &str = "/agentlab/in/policy.json";
pub const AGENTLAB_RESULT_PATH: &str = "/agentlab/out/result.json";
pub const AGENTLAB_TRAJECTORY_PATH: &str = "/agentlab/out/trajectory.jsonl";
pub const AGENTLAB_RUNNER_ENTRYPOINT_PATH: &str = "/agentlab/bin/entrypoint";
pub const AGENTLAB_HARNESS_INVOCATION_PATH: &str = "/agentlab/in/harness_invocation.json";
pub const AGENTLAB_AGENTLABD_START_REQUEST_PATH: &str =
    "/agentlab/state/agentlabd_start_trial.request.json";
pub const AGENTLAB_AGENTLABD_START_RESPONSE_PATH: &str =
    "/agentlab/state/agentlabd_start_trial.response.json";

pub const AGENTLAB_ENV_TRIAL_INPUT: &str = "AGENTLAB_TRIAL_INPUT";
pub const AGENTLAB_ENV_TRIAL_OUTPUT: &str = "AGENTLAB_TRIAL_OUTPUT";
pub const AGENTLAB_ENV_TRIAL_EVENTS: &str = "AGENTLAB_TRIAL_EVENTS";
pub const AGENTLAB_ENV_TIMEOUT_MS: &str = "AGENTLAB_TIMEOUT_MS";
pub const AGENTLAB_ENV_LAUNCH_MODE: &str = "AGENTLAB_LAUNCH_MODE";
pub const AGENTLAB_ENV_RUN_ID: &str = "AGENTLAB_RUN_ID";
pub const AGENTLAB_ENV_TRIAL_ID: &str = "AGENTLAB_TRIAL_ID";
pub const AGENTLAB_ENV_VARIANT_ID: &str = "AGENTLAB_VARIANT_ID";
pub const AGENTLAB_ENV_TASK_ID: &str = "AGENTLAB_TASK_ID";
pub const AGENTLAB_ENV_REPL_IDX: &str = "AGENTLAB_REPL_IDX";
pub const AGENTLAB_ENV_AGENTLABD_START_REQUEST: &str = "AGENTLAB_AGENTLABD_START_REQUEST";
pub const AGENTLAB_ENV_AGENTLABD_START_RESPONSE: &str = "AGENTLAB_AGENTLABD_START_RESPONSE";
pub const AGENTLAB_ENV_TASK_PATH: &str = "AGENTLAB_TASK_PATH";
pub const AGENTLAB_ENV_BINDINGS_PATH: &str = "AGENTLAB_BINDINGS_PATH";
pub const AGENTLAB_ENV_DEPENDENCIES_PATH: &str = "AGENTLAB_DEPENDENCIES_PATH";
pub const AGENTLAB_ENV_POLICY_PATH: &str = "AGENTLAB_POLICY_PATH";
pub const AGENTLAB_ENV_RESULT_PATH: &str = "AGENTLAB_RESULT_PATH";
pub const AGENTLAB_ENV_TRAJECTORY_PATH: &str = "AGENTLAB_TRAJECTORY_PATH";

#[derive(Debug, Clone)]
pub struct RunnerRuntimeHostPaths {
    pub in_dir: PathBuf,
    pub out_dir: PathBuf,
    pub state_dir: PathBuf,
    pub workspace_dir: PathBuf,
    pub deps_dir: PathBuf,
    pub tmp_dir: PathBuf,
    pub task: PathBuf,
    pub bindings: PathBuf,
    pub dependencies: PathBuf,
    pub policy: PathBuf,
    pub result: PathBuf,
    pub trajectory: PathBuf,
    pub trial_input: PathBuf,
    pub trial_output: PathBuf,
    pub trial_events: PathBuf,
    pub control: PathBuf,
    pub entrypoint_dir: PathBuf,
    pub entrypoint: PathBuf,
    pub harness_invocation: PathBuf,
    pub agentlabd_start_request: PathBuf,
    pub agentlabd_start_response: PathBuf,
}

pub fn runner_runtime_host_paths(trial_dir: &Path) -> RunnerRuntimeHostPaths {
    let in_dir = trial_dir.join("in");
    let out_dir = trial_dir.join("out");
    let state_dir = trial_dir.join("state");
    let workspace_dir = trial_dir.join("workspace");
    let deps_dir = trial_dir.join("deps");
    let entrypoint_dir = state_dir.join("agentlab_bin");
    RunnerRuntimeHostPaths {
        in_dir: in_dir.clone(),
        out_dir: out_dir.clone(),
        state_dir: state_dir.clone(),
        workspace_dir: workspace_dir.clone(),
        deps_dir: deps_dir.clone(),
        tmp_dir: trial_dir.join("tmp"),
        task: in_dir.join("task.json"),
        bindings: in_dir.join("bindings.json"),
        dependencies: in_dir.join("dependencies.json"),
        policy: in_dir.join("policy.json"),
        result: out_dir.join("result.json"),
        trajectory: out_dir.join("trajectory.jsonl"),
        trial_input: in_dir.join("trial_input.json"),
        trial_output: out_dir.join("trial_output.json"),
        trial_events: state_dir.join("events.jsonl"),
        control: state_dir.join("lab_control.json"),
        entrypoint: entrypoint_dir.join("entrypoint"),
        entrypoint_dir,
        harness_invocation: in_dir.join("harness_invocation.json"),
        agentlabd_start_request: state_dir.join("agentlabd_start_trial.request.json"),
        agentlabd_start_response: state_dir.join("agentlabd_start_trial.response.json"),
    }
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(sha256_bytes(&buf))
}

pub fn canonical_json(value: &Value) -> String {
    canonical_json_inner(value)
}

fn canonical_json_inner(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => serde_json::to_string(s).unwrap_or_else(|_| format!("\"{}\"", s)),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(canonical_json_inner).collect();
            format!("[{}]", items.join(","))
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut parts = Vec::with_capacity(keys.len());
            for k in keys {
                let v = map.get(k).unwrap();
                let ks = serde_json::to_string(k).unwrap();
                let vs = canonical_json_inner(v);
                parts.push(format!("{}:{}", ks, vs));
            }
            format!("{{{}}}", parts.join(","))
        }
    }
}

pub fn canonical_json_digest(value: &Value) -> String {
    let canonical = canonical_json(value);
    sha256_bytes(canonical.as_bytes())
}

pub fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    Ok(())
}

pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn put_bytes(&self, bytes: &[u8]) -> Result<String> {
        let digest = sha256_bytes(bytes);
        let hex = digest.strip_prefix("sha256:").unwrap_or("unknown");
        let dir = self.root.join("sha256").join(hex);
        ensure_dir(&dir)?;
        let path = dir.join("blob");
        if !path.exists() {
            fs::write(&path, bytes)?;
        }
        Ok(format!("artifact://sha256/{}", hex))
    }

    pub fn put_file(&self, path: &Path) -> Result<String> {
        let bytes = fs::read(path)?;
        self.put_bytes(&bytes)
    }

    pub fn read_ref(&self, artifact_ref: &str) -> Result<Vec<u8>> {
        let hex = artifact_ref
            .strip_prefix("artifact://sha256/")
            .ok_or_else(|| anyhow!("invalid artifact ref"))?;
        let path = self.root.join("sha256").join(hex).join("blob");
        Ok(fs::read(path)?)
    }
}

pub fn hashchain(prev: Option<&str>, line: &str) -> String {
    let mut hasher = Sha256::new();
    if let Some(p) = prev {
        hasher.update(p.as_bytes());
    }
    hasher.update(line.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}
