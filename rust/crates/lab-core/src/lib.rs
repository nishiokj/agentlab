use anyhow::{anyhow, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

pub const AGENTLAB_CONTRACT_IN_DIR: &str = "/agentlab/in";
pub const AGENTLAB_CONTRACT_OUT_DIR: &str = "/agentlab/out";
pub const AGENTLAB_CONTRACT_METRICS_DIR: &str = "/agentlab/metrics";
pub const AGENTLAB_CONTRACT_GRADER_AUX_DIR: &str = "/agentlab/in/grader";
pub const AGENTLAB_CONTRACT_RUNTIME_AUX_DIR: &str = "/agentlab/in/runtime";
pub const AGENTLAB_TASK_WORKDIR_PLACEHOLDER: &str = "__AGENTLAB_TASK_WORKDIR__";
pub const AGENTLAB_RUNNER_SUPPORT_REL_DIR: &str = ".agentlab/support";

pub const AGENTLAB_TRIAL_INPUT_PATH: &str = "/agentlab/in/trial_input.json";
pub const AGENTLAB_GRADER_INPUT_PATH: &str = "/agentlab/in/grader_input.json";
pub const AGENTLAB_RESULT_PATH: &str = "/agentlab/out/result.json";
pub const AGENTLAB_RAW_GRADER_OUTPUT_PATH: &str = "/agentlab/out/raw_grader_output.json";
pub const AGENTLAB_MAPPED_GRADER_OUTPUT_PATH: &str = "/agentlab/out/mapped_grader_output.json";
pub const AGENTLAB_TRAJECTORY_PATH: &str = "/agentlab/out/trajectory.jsonl";

pub const AGENTLAB_ENV_TIMEOUT_MS: &str = "AGENTLAB_TIMEOUT_MS";
pub const AGENTLAB_ENV_RUN_ID: &str = "AGENTLAB_RUN_ID";
pub const AGENTLAB_ENV_TRIAL_ID: &str = "AGENTLAB_TRIAL_ID";
pub const AGENTLAB_ENV_VARIANT_ID: &str = "AGENTLAB_VARIANT_ID";
pub const AGENTLAB_ENV_TASK_ID: &str = "AGENTLAB_TASK_ID";
pub const AGENTLAB_ENV_REPL_IDX: &str = "AGENTLAB_REPL_IDX";
pub const AGENTLAB_ENV_TRIAL_INPUT_PATH: &str = "AGENTLAB_TRIAL_INPUT_PATH";
pub const AGENTLAB_ENV_GRADER_INPUT_PATH: &str = "AGENTLAB_GRADER_INPUT_PATH";
pub const AGENTLAB_ENV_RESULT_PATH: &str = "AGENTLAB_RESULT_PATH";
pub const AGENTLAB_ENV_RAW_GRADER_OUTPUT_PATH: &str = "AGENTLAB_RAW_GRADER_OUTPUT_PATH";
pub const AGENTLAB_ENV_MAPPED_GRADER_OUTPUT_PATH: &str = "AGENTLAB_MAPPED_GRADER_OUTPUT_PATH";
pub const AGENTLAB_ENV_TRAJECTORY_PATH: &str = "AGENTLAB_TRAJECTORY_PATH";

#[derive(Debug, Clone)]
pub struct RunnerRuntimeHostPaths {
    pub in_dir: PathBuf,
    pub out_dir: PathBuf,
    pub state_dir: PathBuf,
    pub workspace_dir: PathBuf,
    pub tmp_dir: PathBuf,
    pub grader_input: PathBuf,
    pub result: PathBuf,
    pub raw_grader_output: PathBuf,
    pub mapped_grader_output: PathBuf,
    pub trajectory: PathBuf,
    pub trial_input: PathBuf,
    pub control: PathBuf,
}

pub fn runner_runtime_host_paths(trial_dir: &Path) -> RunnerRuntimeHostPaths {
    let in_dir = trial_dir.join("in");
    let out_dir = trial_dir.join("out");
    let state_dir = trial_dir.join("state");
    let workspace_dir = trial_dir.join("workspace");
    RunnerRuntimeHostPaths {
        in_dir: in_dir.clone(),
        out_dir: out_dir.clone(),
        state_dir: state_dir.clone(),
        workspace_dir: workspace_dir.clone(),
        tmp_dir: trial_dir.join("tmp"),
        grader_input: in_dir.join("grader_input.json"),
        result: out_dir.join("result.json"),
        raw_grader_output: out_dir.join("raw_grader_output.json"),
        mapped_grader_output: out_dir.join("mapped_grader_output.json"),
        trajectory: out_dir.join("trajectory.jsonl"),
        trial_input: in_dir.join("trial_input.json"),
        control: in_dir.join("runtime").join("lab_control.json"),
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
