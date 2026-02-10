use anyhow::{anyhow, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

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
