use anyhow::{anyhow, Result};
use include_dir::{include_dir, Dir};
use jsonschema::{Draft, JSONSchema};
use serde_json::Value;
use std::fs;
use std::path::Path;

static SCHEMAS_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/../../../schemas");

pub fn schema_names() -> Vec<String> {
    SCHEMAS_DIR
        .files()
        .filter_map(|f| {
            f.path()
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
        })
        .collect()
}

pub fn load_schema(name: &str) -> Result<Value> {
    if let Some(file) = SCHEMAS_DIR.get_file(name) {
        let data = std::str::from_utf8(file.contents())?;
        return Ok(serde_json::from_str(data)?);
    }

    // Dev fallback: allow newly added schema files before this crate is rebuilt.
    let fs_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../schemas")
        .join(name);
    if fs_path.exists() {
        let data = fs::read_to_string(fs_path)?;
        return Ok(serde_json::from_str(&data)?);
    }

    Err(anyhow!("schema not found: {}", name))
}

pub fn compile_schema(name: &str) -> Result<JSONSchema> {
    let schema = load_schema(name)?;
    let schema = Box::leak(Box::new(schema));
    let compiled = JSONSchema::options()
        .with_draft(Draft::Draft7)
        .compile(schema)?;
    Ok(compiled)
}
