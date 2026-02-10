use anyhow::Result;
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::Path;
use zip::write::FileOptions;

pub fn write_attestation(run_dir: &Path, payload: serde_json::Value) -> Result<()> {
    let path = run_dir.join("attestation.json");
    fs::write(path, serde_json::to_vec_pretty(&payload)?)?;
    Ok(())
}

pub fn default_attestation(
    resolved_digest: &str,
    image_digest: Option<&str>,
    grades: serde_json::Value,
    events_heads: Vec<(String, String)>,
    harness: serde_json::Value,
    trace_mode: &str,
) -> serde_json::Value {
    let heads: Vec<serde_json::Value> = events_heads
        .into_iter()
        .map(|(trial_id, head)| json!({"trial_id": trial_id, "head": head}))
        .collect();
    json!({
        "schema_version": "attestation_v1",
        "resolved_experiment_digest": resolved_digest,
        "image_digest": image_digest,
        "events_hashchain_heads": heads,
        "grades": grades,
        "harness": harness,
        "trace_ingestion": trace_mode,
    })
}

pub fn build_debug_bundle(run_dir: &Path, out_path: &Path) -> Result<()> {
    let file = fs::File::create(out_path)?;
    let mut zip = zip::ZipWriter::new(file);
    let opts = FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

    let include_paths = vec![
        run_dir.join("manifest.json"),
        run_dir.join("resolved_experiment.json"),
        run_dir.join("resolved_experiment.digest"),
        run_dir.join("attestation.json"),
    ];

    for p in include_paths {
        if p.exists() {
            let name = p.strip_prefix(run_dir).unwrap().to_string_lossy();
            zip.start_file(name, opts)?;
            let data = fs::read(&p)?;
            zip.write_all(&data)?;
        }
    }

    let trials_dir = run_dir.join("trials");
    if trials_dir.exists() {
        for entry in walkdir::WalkDir::new(&trials_dir) {
            let entry = entry?;
            if entry.file_type().is_file() {
                let path = entry.path();
                let name = path.strip_prefix(run_dir).unwrap().to_string_lossy();
                zip.start_file(name, opts)?;
                let data = fs::read(path)?;
                zip.write_all(&data)?;
            }
        }
    }

    zip.finish()?;
    Ok(())
}
