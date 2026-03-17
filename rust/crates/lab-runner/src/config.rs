use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use lab_core::{canonical_json_digest, ensure_dir, sha256_bytes, sha256_file};
use lab_schemas::compile_schema;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Component, Path, PathBuf};

use crate::persistence::sqlite_store::SqliteRunStore as BackingSqliteStore;
use crate::types::*;

// ---------------------------------------------------------------------------
// Atomic write helpers
// ---------------------------------------------------------------------------

pub(crate) fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let ts = Utc::now().timestamp_micros();
    let pid = std::process::id();
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("tmpfile");
    let tmp = path.with_file_name(format!(".{}.tmp.{}.{}", name, pid, ts));
    let mut file = fs::File::create(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

pub(crate) fn atomic_write_json_pretty(path: &Path, value: &Value) -> Result<()> {
    validate_schema_contract_value(value, format!("json write {}", path.display()).as_str())?;
    let bytes = serde_json::to_vec_pretty(value)?;
    atomic_write_bytes(path, &bytes)
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

pub(crate) fn normalize_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

pub(crate) fn canonicalize_best_effort(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| normalize_path(path))
}

// ---------------------------------------------------------------------------
// JSON file loading
// ---------------------------------------------------------------------------

pub(crate) fn load_json_file(path: &Path) -> Result<Value> {
    let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    let key = match file_name {
        "run_control.json" => Some(RUNTIME_KEY_RUN_CONTROL),
        "run_session_state.json" => Some(RUNTIME_KEY_RUN_SESSION_STATE),
        "schedule_progress.json" => Some(RUNTIME_KEY_SCHEDULE_PROGRESS),
        "parallel_worker_control.json" => Some(RUNTIME_KEY_PARALLEL_WORKER_CONTROL),
        "engine_lease.json" => Some(RUNTIME_KEY_ENGINE_LEASE),
        _ => None,
    };
    if let Some(key) = key {
        let run_dir = path.parent().and_then(|p| p.parent()).ok_or_else(|| {
            anyhow!(
                "cannot resolve run_dir for runtime key '{}' from {}",
                key,
                path.display()
            )
        })?;
        let store = BackingSqliteStore::open(run_dir)?;
        return store.get_runtime_json(key)?.ok_or_else(|| {
            anyhow!(
                "runtime state '{}' not found in sqlite for {}",
                key,
                run_dir.display()
            )
        });
    }
    if path.exists() {
        let bytes = fs::read(path)?;
        return Ok(serde_json::from_slice(&bytes)?);
    }
    Err(anyhow!("json file not found: {}", path.display()))
}

// ---------------------------------------------------------------------------
// Experiment field helpers
// ---------------------------------------------------------------------------

pub(crate) fn experiment_workload_type(json_value: &Value) -> Result<String> {
    if let Some(value) = json_value
        .pointer("/experiment/workload_type")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        return Ok(value.to_string());
    }
    Err(anyhow!("missing /experiment/workload_type"))
}

pub(crate) fn experiment_random_seed(json_value: &Value) -> u64 {
    json_value
        .pointer("/design/random_seed")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
}

pub(crate) fn experiment_max_concurrency(json_value: &Value) -> usize {
    let raw = json_value
        .pointer("/design/max_concurrency")
        .and_then(|v| v.as_u64())
        .unwrap_or(1);
    (raw.max(1)).min(usize::MAX as u64) as usize
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

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
        .unwrap_or("");
    if artifact.is_empty() {
        missing.push("/runtime/agent_runtime/artifact");
    }
    let image = json_value
        .pointer("/runtime/agent_runtime/image")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .unwrap_or("");
    if image.is_empty() {
        missing.push("/runtime/agent_runtime/image");
    }
    let sandbox_network = json_value
        .pointer("/policy/task_sandbox/network")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .unwrap_or("");
    if sandbox_network.is_empty() {
        missing.push("/policy/task_sandbox/network");
    }
    if json_value.pointer("/benchmark").is_some() {
        let has_grader_command = match json_value.pointer("/benchmark/grader/command") {
            Some(Value::Array(parts)) if !parts.is_empty() => parts
                .iter()
                .all(|part| part.as_str().map(|s| !s.trim().is_empty()).unwrap_or(false)),
            _ => false,
        };
        if !has_grader_command {
            missing.push("/benchmark/grader/command");
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        let lines = missing
            .iter()
            .map(|p| format!("  - {}", p))
            .collect::<Vec<_>>();
        Err(anyhow!(
            "experiment.yaml missing required fields:\n{}",
            lines.join("\n")
        ))
    }
}

// ---------------------------------------------------------------------------
// String parsing helpers
// ---------------------------------------------------------------------------

pub(crate) fn parse_string_array_field(value: Option<&Value>, field: &str) -> Result<Vec<String>> {
    match value {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => {
            let mut parsed = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                let token = item
                    .as_str()
                    .ok_or_else(|| anyhow!("{}[{}] must be a string", field, idx))?;
                if token.trim().is_empty() {
                    return Err(anyhow!("{}[{}] must not be empty", field, idx));
                }
                parsed.push(token.to_string());
            }
            Ok(parsed)
        }
        Some(_) => Err(anyhow!("{} must be a string[]", field)),
    }
}

pub(crate) fn parse_string_map_field(
    value: Option<&Value>,
    field: &str,
) -> Result<BTreeMap<String, String>> {
    match value {
        None => Ok(BTreeMap::new()),
        Some(Value::Object(map)) => {
            let mut parsed = BTreeMap::new();
            for (key, value) in map {
                if key.trim().is_empty() {
                    return Err(anyhow!("{} contains an empty key", field));
                }
                let as_str = value
                    .as_str()
                    .ok_or_else(|| anyhow!("{}['{}'] must be a string", field, key))?;
                parsed.insert(key.clone(), as_str.to_string());
            }
            Ok(parsed)
        }
        Some(_) => Err(anyhow!("{} must be an object<string,string>", field)),
    }
}

pub(crate) fn parse_optional_nonempty_string(
    value: Option<&Value>,
    field: &str,
) -> Result<Option<String>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Some(_) => Err(anyhow!("{} must be a string", field)),
    }
}

// ---------------------------------------------------------------------------
// Package path resolution & integrity
// ---------------------------------------------------------------------------

pub(crate) fn require_exact_object_keys(
    value: &Value,
    allowed: &[&str],
    context: &str,
) -> Result<()> {
    let obj = value
        .as_object()
        .ok_or_else(|| anyhow!("{} must be an object", context))?;
    for key in obj.keys() {
        if !allowed.iter().any(|expected| *expected == key) {
            return Err(anyhow!("{} contains unknown key '{}'", context, key));
        }
    }
    for key in allowed {
        if !obj.contains_key(*key) {
            return Err(anyhow!("{} missing required key '{}'", context, key));
        }
    }
    Ok(())
}

pub(crate) fn resolve_package_path_under_root(
    package_dir: &Path,
    rel_path: &str,
    field_name: &str,
) -> Result<PathBuf> {
    let trimmed = rel_path.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{} must be a non-empty relative path", field_name));
    }
    if Path::new(trimmed).is_absolute() {
        return Err(anyhow!("{} must be relative to package root", field_name));
    }
    let resolved = normalize_path(&package_dir.join(trimmed));
    let root = canonicalize_best_effort(package_dir);
    let resolved_cmp = canonicalize_best_effort(&resolved);
    if !resolved_cmp.starts_with(&root) {
        return Err(anyhow!(
            "{} escapes package root: '{}' (root: {})",
            field_name,
            rel_path,
            root.display()
        ));
    }
    Ok(resolved)
}

pub(crate) fn verify_sealed_package_integrity(
    package_dir: &Path,
    manifest: &Value,
) -> Result<Value> {
    require_exact_object_keys(
        manifest,
        &[
            "schema_version",
            "created_at",
            "resolved_experiment",
            "checksums_ref",
            "package_digest",
        ],
        "sealed package manifest",
    )?;
    if manifest.pointer("/schema_version").and_then(Value::as_str) != Some("sealed_run_package_v2")
    {
        return Err(anyhow!(
            "preflight_failed: manifest schema_version must be 'sealed_run_package_v2'"
        ));
    }
    let checksums_ref = manifest
        .pointer("/checksums_ref")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("sealed package manifest missing checksums_ref"))?;
    let checksums_path =
        resolve_package_path_under_root(package_dir, checksums_ref, "checksums_ref")?;
    let checksums = load_json_file(&checksums_path)?;
    if checksums.pointer("/schema_version").and_then(Value::as_str)
        != Some("sealed_package_checksums_v2")
    {
        return Err(anyhow!(
            "preflight_failed: checksums schema_version must be 'sealed_package_checksums_v2'"
        ));
    }
    let files = checksums
        .pointer("/files")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("preflight_failed: checksums.json missing object field 'files'"))?;
    for (rel, expected_digest) in files {
        let expected = expected_digest.as_str().ok_or_else(|| {
            anyhow!(
                "preflight_failed: checksums entry '{}' must be a string digest",
                rel
            )
        })?;
        let file_path = resolve_package_path_under_root(package_dir, rel, "checksums.files")?;
        if !file_path.is_file() {
            return Err(anyhow!(
                "preflight_failed: checksummed file missing: {}",
                file_path.display()
            ));
        }
        let actual = sha256_file(&file_path)?;
        if !expected.eq_ignore_ascii_case(actual.as_str()) {
            return Err(anyhow!(
                "preflight_failed: checksum mismatch for '{}' (expected {}, got {})",
                rel,
                expected,
                actual
            ));
        }
    }
    if !files.contains_key("resolved_experiment.json") {
        return Err(anyhow!(
            "preflight_failed: checksums must include 'resolved_experiment.json'"
        ));
    }
    if !files.contains_key(STAGING_MANIFEST_FILE) {
        return Err(anyhow!(
            "preflight_failed: checksums must include '{}'",
            STAGING_MANIFEST_FILE
        ));
    }
    let computed_digest = canonical_json_digest(
        checksums
            .pointer("/files")
            .ok_or_else(|| anyhow!("preflight_failed: checksums missing files object"))?,
    );
    let manifest_digest = manifest
        .pointer("/package_digest")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("sealed package manifest missing package_digest"))?;
    if computed_digest != manifest_digest {
        return Err(anyhow!(
            "preflight_failed: package digest mismatch (manifest={}, computed={})",
            manifest_digest,
            computed_digest
        ));
    }
    let lock_path = package_dir.join("package.lock");
    let lock = load_json_file(&lock_path).map_err(|err| {
        anyhow!(
            "preflight_failed: package.lock missing or unreadable at {}: {}",
            lock_path.display(),
            err
        )
    })?;
    if lock.pointer("/package_digest").and_then(Value::as_str) != Some(manifest_digest) {
        return Err(anyhow!(
            "preflight_failed: package.lock digest does not match manifest package_digest"
        ));
    }
    let resolved_path = resolve_package_path_under_root(
        package_dir,
        "resolved_experiment.json",
        "checksums.files",
    )?;
    let resolved_experiment = load_json_file(&resolved_path).map_err(|err| {
        anyhow!(
            "preflight_failed: resolved_experiment.json missing or unreadable at {}: {}",
            resolved_path.display(),
            err
        )
    })?;
    let staging_manifest_path =
        resolve_package_path_under_root(package_dir, STAGING_MANIFEST_FILE, "checksums.files")?;
    load_json_file(&staging_manifest_path).map_err(|err| {
        anyhow!(
            "preflight_failed: {} missing or unreadable at {}: {}",
            STAGING_MANIFEST_FILE,
            staging_manifest_path.display(),
            err
        )
    })?;
    Ok(resolved_experiment)
}

pub(crate) fn load_sealed_package_for_run(path: &Path) -> Result<LoadedExperimentInput> {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let (manifest_path, exp_dir) = if canonical.is_dir() {
        let manifest = canonical.join("manifest.json");
        if !manifest.is_file() {
            return Err(anyhow!(
                "run_input_invalid_kind: expected sealed package dir or manifest"
            ));
        }
        (manifest, canonical)
    } else if canonical
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "manifest.json")
    {
        let exp_dir = canonical
            .parent()
            .ok_or_else(|| anyhow!("manifest has no parent directory"))?
            .to_path_buf();
        (canonical, exp_dir)
    } else {
        return Err(anyhow!(
            "run_input_invalid_kind: expected sealed package dir or manifest"
        ));
    };
    let manifest = load_json_file(&manifest_path)?;
    let json_value = verify_sealed_package_integrity(&exp_dir, &manifest)?;
    let project_root = find_project_root(&exp_dir)
        .canonicalize()
        .unwrap_or_else(|_| find_project_root(&exp_dir));
    Ok(LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root,
    })
}

// ---------------------------------------------------------------------------
// Project root
// ---------------------------------------------------------------------------

pub fn find_project_root(experiment_dir: &Path) -> PathBuf {
    let mut cur = Some(experiment_dir);
    while let Some(p) = cur {
        if p.file_name().and_then(|s| s.to_str()) == Some(".lab") {
            return p.parent().unwrap_or(experiment_dir).to_path_buf();
        }
        cur = p.parent();
    }
    experiment_dir.to_path_buf()
}

// ---------------------------------------------------------------------------
// Policy parsing & config resolution
// ---------------------------------------------------------------------------

pub(crate) fn parse_policies(json_value: &Value) -> PolicyConfig {
    let policies = json_value.pointer("/design/policies");
    let Some(p) = policies else {
        return PolicyConfig::default();
    };

    let scheduling = match p.pointer("/scheduling").and_then(|v| v.as_str()) {
        Some("paired_interleaved") => SchedulingPolicy::PairedInterleaved,
        Some("randomized") => SchedulingPolicy::Randomized,
        _ => SchedulingPolicy::VariantSequential,
    };
    let state = parse_state_policy_value(p.pointer("/state").and_then(|v| v.as_str()))
        .unwrap_or(StatePolicy::IsolatePerTrial);
    let retry_max_attempts = p
        .pointer("/retry/max_attempts")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as usize;
    let retry_on = p
        .pointer("/retry/retry_on")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let pruning_max_consecutive_failures = p
        .pointer("/pruning/max_consecutive_failures")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    let max_in_flight_per_variant = p
        .pointer("/concurrency/max_in_flight_per_variant")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    let require_chain_lease = p
        .pointer("/concurrency/require_chain_lease")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    PolicyConfig {
        scheduling,
        state,
        retry_max_attempts,
        retry_on,
        pruning_max_consecutive_failures,
        task_boundary: TaskBoundaryPolicy::default(),
        concurrency: ConcurrencyPolicyConfig {
            max_in_flight_per_variant,
            require_chain_lease,
        },
    }
}

pub(crate) fn parse_task_model(value: Option<&str>) -> TaskModel {
    match value {
        Some("dependent") => TaskModel::Dependent,
        _ => TaskModel::Independent,
    }
}

pub(crate) fn parse_state_policy_value(value: Option<&str>) -> Option<StatePolicy> {
    match value {
        Some("isolate_per_trial") => Some(StatePolicy::IsolatePerTrial),
        Some("persist_per_task") => Some(StatePolicy::PersistPerTask),
        Some("accumulate") => Some(StatePolicy::Accumulate),
        _ => None,
    }
}

pub(crate) fn parse_benchmark_config(json_value: &Value) -> BenchmarkConfig {
    let benchmark_root = json_value.pointer("/benchmark");
    let Some(root) = benchmark_root else {
        return BenchmarkConfig::default();
    };

    let policy = root.pointer("/policy");
    let mut policy_config = BenchmarkPolicyConfig::default();
    if let Some(p) = policy {
        policy_config.task_model =
            parse_task_model(p.pointer("/task_model").and_then(|v| v.as_str()));
        if let Some(v) = p.pointer("/scoring_lifecycle").and_then(|v| v.as_str()) {
            policy_config.scoring_lifecycle = v.to_string();
        }
        if let Some(v) = p.pointer("/evaluator_mode").and_then(|v| v.as_str()) {
            policy_config.evaluator_mode = v.to_string();
        }
        if let Some(v) = p.pointer("/chain_failure_policy").and_then(|v| v.as_str()) {
            policy_config.chain_failure_policy = v.to_string();
        }
        if let Some(arr) = p
            .pointer("/required_evidence_classes")
            .and_then(|v| v.as_array())
        {
            policy_config.required_evidence_classes = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
    }

    let grader = root.pointer("/grader").and_then(|g| {
        let parse_string_array = |value: Option<&Value>| {
            value
                .and_then(|raw| raw.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| {
                            v.as_str().and_then(|s| {
                                let trimmed = s.trim();
                                if trimmed.is_empty() {
                                    None
                                } else {
                                    Some(trimmed.to_string())
                                }
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        let parse_optional_string = |value: Option<&Value>| {
            value.and_then(Value::as_str).and_then(|raw| {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
        };

        let command = parse_string_array(g.pointer("/command"));
        if command.is_empty() {
            return None;
        }

        let strategy = match g.pointer("/strategy").and_then(Value::as_str) {
            Some("injected") => GradingStrategy::Injected,
            Some("separate") => GradingStrategy::Separate,
            _ => GradingStrategy::InTaskImage,
        };
        let conclusion = GraderConclusionConfig {
            mode: match g.pointer("/conclusion/mode").and_then(Value::as_str) {
                Some("mapper") => GraderConclusionMode::Mapper,
                _ => GraderConclusionMode::Direct,
            },
            mapper: parse_optional_string(g.pointer("/conclusion/mapper")),
        };
        let in_task_image = g
            .pointer("/in_task_image")
            .map(|value| InTaskImageGradingConfig {
                hidden_paths: parse_string_array(value.get("hidden_paths")),
                revealed_paths: parse_string_array(value.get("revealed_paths")),
            });
        let injected = match (
            parse_optional_string(g.pointer("/injected/bundle")),
            parse_optional_string(g.pointer("/injected/copy_dest")),
        ) {
            (Some(bundle), Some(copy_dest)) => Some(InjectedGradingConfig { bundle, copy_dest }),
            _ => None,
        };
        let separate = match (
            parse_optional_string(g.pointer("/separate/image")),
            parse_optional_string(g.pointer("/separate/workdir")),
        ) {
            (Some(image), Some(workdir)) => Some(SeparateGradingConfig { image, workdir }),
            _ => None,
        };
        let is_in_task_image = matches!(strategy, GradingStrategy::InTaskImage);

        Some(BenchmarkGraderConfig {
            strategy,
            command,
            conclusion,
            in_task_image: if is_in_task_image {
                Some(in_task_image.unwrap_or_default())
            } else {
                in_task_image
            },
            injected,
            separate,
        })
    });

    #[cfg(test)]
    let adapter = grader.clone();
    BenchmarkConfig {
        policy: policy_config,
        grader,
        #[cfg(test)]
        adapter,
    }
}

pub(crate) fn resolve_effective_task_policy(
    experiment_policy: &PolicyConfig,
    benchmark_policy: &BenchmarkPolicyConfig,
    task_payload: &Value,
) -> EffectiveTaskPolicy {
    let override_obj = task_payload
        .get("policy_override")
        .and_then(|v| v.as_object());

    let state_override = override_obj
        .and_then(|o| o.get("state_policy"))
        .and_then(|v| v.as_str())
        .and_then(|s| parse_state_policy_value(Some(s)));
    let task_model_override = override_obj
        .and_then(|o| o.get("task_model"))
        .and_then(|v| v.as_str())
        .map(|s| parse_task_model(Some(s)));
    let scoring_lifecycle_override = override_obj
        .and_then(|o| o.get("scoring_lifecycle"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let chain_failure_override = override_obj
        .and_then(|o| o.get("chain_failure_policy"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let required_evidence_override = override_obj
        .and_then(|o| o.get("required_evidence_classes"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        });

    EffectiveTaskPolicy {
        state_policy: state_override.unwrap_or(experiment_policy.state),
        task_model: task_model_override.unwrap_or(benchmark_policy.task_model),
        scoring_lifecycle: scoring_lifecycle_override
            .unwrap_or_else(|| benchmark_policy.scoring_lifecycle.clone()),
        required_evidence_classes: required_evidence_override
            .unwrap_or_else(|| benchmark_policy.required_evidence_classes.clone()),
        chain_failure_policy: chain_failure_override
            .unwrap_or_else(|| benchmark_policy.chain_failure_policy.clone()),
    }
}

pub(crate) fn validate_required_evidence_classes(
    record: &Value,
    required: &[String],
) -> Result<()> {
    if required.is_empty() {
        return Ok(());
    }
    for class_name in required {
        let pointer = format!("/evidence/{}", class_name);
        let value = record.pointer(&pointer);
        let missing = match value {
            None => true,
            Some(Value::Null) => true,
            Some(Value::String(s)) => s.trim().is_empty(),
            _ => false,
        };
        if missing {
            return Err(anyhow!(
                "missing required evidence class '{}'; pointer {}",
                class_name,
                pointer
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

// ---------------------------------------------------------------------------
// Schedule building & progress tracking
// ---------------------------------------------------------------------------

pub(crate) fn build_trial_schedule(
    variant_count: usize,
    task_count: usize,
    replications: usize,
    policy: SchedulingPolicy,
    random_seed: u64,
) -> Vec<TrialSlot> {
    let mut slots = Vec::with_capacity(variant_count * task_count * replications);

    match policy {
        SchedulingPolicy::VariantSequential => {
            for v in 0..variant_count {
                for t in 0..task_count {
                    for r in 0..replications {
                        slots.push(TrialSlot {
                            variant_idx: v,
                            task_idx: t,
                            repl_idx: r,
                        });
                    }
                }
            }
        }
        SchedulingPolicy::PairedInterleaved => {
            for t in 0..task_count {
                for v in 0..variant_count {
                    for r in 0..replications {
                        slots.push(TrialSlot {
                            variant_idx: v,
                            task_idx: t,
                            repl_idx: r,
                        });
                    }
                }
            }
        }
        SchedulingPolicy::Randomized => {
            // Build variant_sequential order then shuffle deterministically
            for v in 0..variant_count {
                for t in 0..task_count {
                    for r in 0..replications {
                        slots.push(TrialSlot {
                            variant_idx: v,
                            task_idx: t,
                            repl_idx: r,
                        });
                    }
                }
            }
            // Deterministic Fisher-Yates using LCG seeded by random_seed
            let mut rng_state: u64 = random_seed;
            for i in (1..slots.len()).rev() {
                // LCG: state = state * 6364136223846793005 + 1442695040888963407
                rng_state = rng_state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let j = (rng_state >> 33) as usize % (i + 1);
                slots.swap(i, j);
            }
        }
    }

    slots
}

pub(crate) fn legacy_slot_commit_id(run_id: &str, slot: &SlotCompletion) -> String {
    let raw = format!(
        "legacy:{}:{}:{}:{}",
        run_id, slot.schedule_index, slot.trial_id, slot.status
    );
    let digest = sha256_bytes(raw.as_bytes());
    format!("legacy_{}", &digest[..24])
}

pub(crate) fn normalize_schedule_progress(progress: &mut ScheduleProgress) {
    progress.schema_version = "schedule_progress_v2".to_string();
    for slot in &mut progress.completed_slots {
        if slot.attempt == 0 {
            slot.attempt = 1;
        }
        if slot.slot_commit_id.trim().is_empty() {
            slot.slot_commit_id = legacy_slot_commit_id(&progress.run_id, slot);
        }
    }
}

pub(crate) fn load_schedule_progress(run_dir: &Path) -> Result<ScheduleProgress> {
    let store = BackingSqliteStore::open(run_dir)?;
    let Some(value) = store.get_runtime_json(RUNTIME_KEY_SCHEDULE_PROGRESS)? else {
        return Err(anyhow!(
            "schedule_progress_v2 not found in sqlite runtime_kv for {}",
            run_dir.display()
        ));
    };
    let mut progress: ScheduleProgress = serde_json::from_value(value)?;
    if progress.schema_version != "schedule_progress_v2" {
        return Err(anyhow!(
            "unsupported schedule_progress schema_version '{}' for {}",
            progress.schema_version,
            run_dir.display()
        ));
    }
    normalize_schedule_progress(&mut progress);
    Ok(progress)
}

pub(crate) fn write_schedule_progress(run_dir: &Path, progress: &ScheduleProgress) -> Result<()> {
    let mut next = progress.clone();
    normalize_schedule_progress(&mut next);
    let value = serde_json::to_value(next)?;
    let mut store = BackingSqliteStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_SCHEDULE_PROGRESS, &value)
}

// ---------------------------------------------------------------------------
// Resolved variants & schedule paths
// ---------------------------------------------------------------------------

pub(crate) fn resolved_variants_path(run_dir: &Path) -> PathBuf {
    run_dir.join("resolved_variants.json")
}

pub(crate) fn resolved_schedule_path(run_dir: &Path) -> PathBuf {
    run_dir.join("resolved_schedule.json")
}

pub(crate) fn write_resolved_variants(
    run_dir: &Path,
    experiment: &Value,
    baseline_id: &str,
    variants: &[Variant],
) -> Result<()> {
    let variants = variants
        .iter()
        .map(|variant| resolved_variant_manifest_entry(experiment, variant))
        .collect::<Result<Vec<_>>>()?;
    let manifest = ResolvedVariantsManifest {
        schema_version: "resolved_variants_v1".to_string(),
        generated_at: Utc::now().to_rfc3339(),
        baseline_id: baseline_id.to_string(),
        variants,
    };
    let value = serde_json::to_value(&manifest)?;
    atomic_write_json_pretty(&resolved_variants_path(run_dir), &value)
}

pub(crate) fn write_resolved_schedule(run_dir: &Path, schedule: &[TrialSlot]) -> Result<()> {
    let manifest = ResolvedScheduleManifest {
        schema_version: "resolved_schedule_v1".to_string(),
        generated_at: Utc::now().to_rfc3339(),
        total_slots: schedule.len(),
        schedule: schedule.to_vec(),
    };
    let value = serde_json::to_value(&manifest)?;
    atomic_write_json_pretty(&resolved_schedule_path(run_dir), &value)
}

// ---------------------------------------------------------------------------
// Run variants loading
// ---------------------------------------------------------------------------

pub(crate) fn load_run_variants(
    run_dir: &Path,
    experiment: &Value,
) -> Result<(Vec<Variant>, String)> {
    let manifest_path = resolved_variants_path(run_dir);
    if !manifest_path.exists() {
        return resolve_variant_plan(experiment);
    }

    let manifest: ResolvedVariantsManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if manifest.schema_version != "resolved_variants_v1" {
        return Err(anyhow!(
            "unsupported resolved variants schema_version in {}: {}",
            manifest_path.display(),
            manifest.schema_version
        ));
    }
    if manifest.variants.is_empty() {
        return Err(anyhow!(
            "resolved variants manifest has no variants: {}",
            manifest_path.display()
        ));
    }
    if !manifest
        .variants
        .iter()
        .any(|variant| variant.variant.id == manifest.baseline_id)
    {
        return Err(anyhow!(
            "resolved variants manifest baseline '{}' not found in variants: {}",
            manifest.baseline_id,
            manifest_path.display()
        ));
    }
    Ok((
        manifest
            .variants
            .into_iter()
            .map(|variant| variant.variant)
            .collect(),
        manifest.baseline_id,
    ))
}

// ---------------------------------------------------------------------------
// Retry & outcome helpers
// ---------------------------------------------------------------------------

pub(crate) fn should_retry_outcome(outcome: &str, exit_status: &str, retry_on: &[String]) -> bool {
    if retry_on.is_empty() {
        // When retry_on is unspecified, retry on any non-success
        return outcome == "error" || exit_status != "0";
    }
    for trigger in retry_on {
        match trigger.as_str() {
            "error" if outcome == "error" => return true,
            "failure" if exit_status != "0" => return true,
            "timeout" if outcome == "timeout" => return true,
            _ => {}
        }
    }
    false
}

pub(crate) fn benchmark_verdict_to_trial_outcome(verdict: &str) -> Option<&'static str> {
    match verdict {
        "pass" => Some("success"),
        "fail" => Some("failure"),
        "missing" => Some("missing"),
        "error" => Some("error"),
        _ => None,
    }
}

pub(crate) fn trial_conclusion_outcome_to_trial_outcome(outcome: &str) -> Option<&'static str> {
    match outcome {
        "success" => Some("success"),
        "failure" => Some("failure"),
        "missing" => Some("missing"),
        "error" => Some("error"),
        "timeout" => Some("timeout"),
        other => benchmark_verdict_to_trial_outcome(other),
    }
}

// ---------------------------------------------------------------------------
// Variant helpers
// ---------------------------------------------------------------------------

pub(crate) fn variant_bindings_for_summary(variant: &Variant) -> Value {
    if !variant.args.is_empty() || !variant.env.is_empty() || variant.image.is_some() {
        return json!({
            "args": variant.args,
            "env": variant.env,
            "image": variant.image,
        });
    }
    variant.bindings.clone()
}

pub(crate) fn variant_digest(variant: &Variant) -> Result<String> {
    let value = serde_json::to_value(variant)?;
    Ok(canonical_json_digest(&value))
}

pub(crate) fn resolved_variant_behavior_surface(
    experiment: &Value,
    variant: &Variant,
) -> Result<Value> {
    let mut runtime = experiment
        .pointer("/runtime")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !runtime.is_object() {
        return Err(anyhow!(
            "invalid /runtime in resolved experiment: expected object"
        ));
    }
    if let Some(runtime_overrides) = variant.runtime_overrides.as_ref() {
        if !runtime_overrides.is_object() {
            return Err(anyhow!(
                "variant '{}' runtime_overrides must be an object",
                variant.id
            ));
        }
        merge_json_value(&mut runtime, runtime_overrides);
    }
    Ok(json!({
        "bindings": variant.bindings.clone(),
        "args": variant.args.clone(),
        "env": variant.env.clone(),
        "image": variant.image.clone(),
        "runtime": runtime,
    }))
}

pub(crate) fn resolved_variant_behavior_digest(
    experiment: &Value,
    variant: &Variant,
) -> Result<String> {
    Ok(canonical_json_digest(&resolved_variant_behavior_surface(
        experiment, variant,
    )?))
}

fn resolved_variant_manifest_entry(
    experiment: &Value,
    variant: &Variant,
) -> Result<ResolvedVariant> {
    Ok(ResolvedVariant {
        variant_digest: resolved_variant_behavior_digest(experiment, variant)?,
        variant: variant.clone(),
    })
}

pub(crate) fn resolve_variant_plan(json_value: &Value) -> Result<(Vec<Variant>, String)> {
    let baseline = json_value
        .pointer("/baseline/variant_id")
        .or_else(|| json_value.pointer("/baseline/id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing /baseline/variant_id or /baseline/id"))?
        .to_string();

    let mut variants = Vec::new();
    let baseline_bindings = json_value
        .pointer("/baseline/bindings")
        .or_else(|| json_value.pointer("/baseline/config"))
        .cloned()
        .unwrap_or(json!({}));
    if !baseline_bindings.is_object() {
        return Err(anyhow!("invalid /baseline/bindings: expected object"));
    }
    let mut baseline_runtime_overrides = match json_value.pointer("/baseline/runtime_overrides") {
        None | Some(Value::Null) => None,
        Some(Value::Object(_)) => json_value.pointer("/baseline/runtime_overrides").cloned(),
        Some(_) => return Err(anyhow!("/baseline/runtime_overrides must be an object")),
    };
    if let Some(image) =
        parse_optional_nonempty_string(json_value.pointer("/baseline/image"), "/baseline/image")?
    {
        let mut overrides = baseline_runtime_overrides.unwrap_or_else(|| json!({}));
        set_json_pointer_value(&mut overrides, "/agent/image", json!(image))?;
        baseline_runtime_overrides = Some(overrides);
    }
    variants.push(Variant {
        id: baseline.clone(),
        bindings: baseline_bindings,
        args: Vec::new(),
        env: BTreeMap::new(),
        image: None,
        runtime_overrides: baseline_runtime_overrides,
    });

    let (variant_list, variant_path): (&[Value], &str) =
        if let Some(value) = json_value.pointer("/variant_plan") {
            (
                value
                    .as_array()
                    .map(|v| v.as_slice())
                    .ok_or_else(|| anyhow!("/variant_plan must be an array of variant objects"))?,
                "/variant_plan",
            )
        } else if let Some(value) = json_value.pointer("/variants") {
            (
                value
                    .as_array()
                    .map(|v| v.as_slice())
                    .ok_or_else(|| anyhow!("/variants must be an array of variant objects"))?,
                "/variants",
            )
        } else {
            (&[], "/variant_plan")
        };
    for (idx, item) in variant_list.iter().enumerate() {
        let id = item
            .get("variant_id")
            .or_else(|| item.get("id"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "{}[{}] must include non-empty string variant_id",
                    variant_path,
                    idx
                )
            })?
            .to_string();
        let bindings = item
            .get("bindings")
            .or_else(|| item.get("config"))
            .cloned()
            .unwrap_or(json!({}));
        if !bindings.is_object() {
            return Err(anyhow!(
                "{}[{}].bindings must be an object",
                variant_path,
                idx
            ));
        }
        let mut runtime_overrides = match item.get("runtime_overrides") {
            None | Some(Value::Null) => None,
            Some(Value::Object(_)) => item.get("runtime_overrides").cloned(),
            Some(_) => {
                return Err(anyhow!(
                    "{}[{}].runtime_overrides must be an object",
                    variant_path,
                    idx
                ))
            }
        };
        if let Some(image) = parse_optional_nonempty_string(
            item.get("image"),
            &format!("/variant_plan[{}].image", idx),
        )? {
            let mut overrides = runtime_overrides.unwrap_or_else(|| json!({}));
            set_json_pointer_value(&mut overrides, "/agent/image", json!(image))?;
            runtime_overrides = Some(overrides);
        }
        variants.push(Variant {
            id,
            bindings,
            args: Vec::new(),
            env: BTreeMap::new(),
            image: None,
            runtime_overrides,
        });
    }
    Ok((variants, baseline))
}

// ---------------------------------------------------------------------------
// JSON merge & pointer helpers
// ---------------------------------------------------------------------------

pub(crate) fn merge_json_value(base: &mut Value, patch: &Value) {
    match (base, patch) {
        (Value::Object(base_map), Value::Object(patch_map)) => {
            for (key, patch_value) in patch_map {
                if let Some(base_value) = base_map.get_mut(key) {
                    merge_json_value(base_value, patch_value);
                } else {
                    base_map.insert(key.clone(), patch_value.clone());
                }
            }
        }
        (base_slot, patch_value) => {
            *base_slot = patch_value.clone();
        }
    }
}

pub(crate) fn value_matches_type(value: &Value, t: &str) -> bool {
    match t {
        "string" => value.is_string(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => false,
    }
}

pub(crate) fn value_type_name(value: &Value) -> &'static str {
    if value.is_string() {
        "string"
    } else if value.is_boolean() {
        "boolean"
    } else if value.is_number() {
        "number"
    } else if value.is_array() {
        "array"
    } else if value.is_object() {
        "object"
    } else {
        "null"
    }
}

fn decode_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

pub(crate) fn set_json_pointer_value(
    root: &mut Value,
    pointer: &str,
    new_value: Value,
) -> Result<()> {
    if pointer.is_empty() || pointer == "/" {
        *root = new_value;
        return Ok(());
    }
    if !pointer.starts_with('/') {
        return Err(anyhow!("json_pointer must start with '/': {}", pointer));
    }

    let tokens: Vec<String> = pointer
        .split('/')
        .skip(1)
        .map(decode_pointer_token)
        .collect();
    if tokens.is_empty() {
        *root = new_value;
        return Ok(());
    }

    let mut cur = root;
    for token in tokens.iter().take(tokens.len() - 1) {
        match cur {
            Value::Object(map) => {
                let entry = map.entry(token.clone()).or_insert_with(|| json!({}));
                cur = entry;
            }
            Value::Array(arr) => {
                let idx: usize = token.parse().map_err(|_| {
                    anyhow!(
                        "json_pointer token '{}' is not a valid array index in {}",
                        token,
                        pointer
                    )
                })?;
                if idx >= arr.len() {
                    return Err(anyhow!(
                        "json_pointer array index {} out of bounds in {}",
                        idx,
                        pointer
                    ));
                }
                cur = &mut arr[idx];
            }
            _ => {
                return Err(anyhow!(
                    "json_pointer traversal hit non-container at token '{}' in {}",
                    token,
                    pointer
                ));
            }
        }
    }

    let last = tokens.last().unwrap();
    match cur {
        Value::Object(map) => {
            map.insert(last.clone(), new_value);
            Ok(())
        }
        Value::Array(arr) => {
            let idx: usize = last.parse().map_err(|_| {
                anyhow!(
                    "json_pointer token '{}' is not a valid array index in {}",
                    last,
                    pointer
                )
            })?;
            if idx >= arr.len() {
                return Err(anyhow!(
                    "json_pointer array index {} out of bounds in {}",
                    idx,
                    pointer
                ));
            }
            arr[idx] = new_value;
            Ok(())
        }
        _ => Err(anyhow!(
            "json_pointer target is not an object/array for {}",
            pointer
        )),
    }
}

// ---------------------------------------------------------------------------
// Runtime variant resolution
// ---------------------------------------------------------------------------

pub(crate) fn resolve_runtime_for_variant(experiment: &Value, variant: &Variant) -> Result<Value> {
    let mut resolved = experiment.clone();
    let Some(runtime_overrides) = variant.runtime_overrides.as_ref() else {
        return Ok(resolved);
    };

    let mut runtime = resolved
        .pointer("/runtime")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !runtime.is_object() {
        return Err(anyhow!("invalid /runtime: expected object"));
    }
    merge_json_value(&mut runtime, runtime_overrides);
    set_json_pointer_value(&mut resolved, "/runtime", runtime)?;
    Ok(resolved)
}

pub(crate) fn find_variant_by_id<'a>(
    variants: &'a [Variant],
    variant_id: &str,
) -> Result<&'a Variant> {
    let trimmed = variant_id.trim();
    if trimmed.is_empty() {
        return variants
            .first()
            .ok_or_else(|| anyhow!("no variants available in experiment"));
    }
    variants
        .iter()
        .find(|variant| variant.id == trimmed)
        .ok_or_else(|| anyhow!("variant '{}' not found in experiment", trimmed))
}

// ---------------------------------------------------------------------------
// Experiment overrides & knobs
// ---------------------------------------------------------------------------

pub(crate) fn apply_experiment_overrides(
    mut experiment: Value,
    overrides_path: &Path,
    project_root: &Path,
) -> Result<Value> {
    let overrides = load_experiment_overrides(overrides_path)?;
    if overrides.values.is_empty() {
        return Ok(experiment);
    }

    let manifest_rel = overrides
        .manifest_path
        .clone()
        .unwrap_or_else(|| ".lab/knobs/manifest.json".to_string());
    let manifest_path = if Path::new(&manifest_rel).is_absolute() {
        PathBuf::from(&manifest_rel)
    } else {
        project_root.join(&manifest_rel)
    };
    let manifest = load_knob_manifest(&manifest_path)?;

    let mut by_id: BTreeMap<String, KnobDef> = BTreeMap::new();
    for knob in manifest.knobs {
        by_id.insert(knob.id.clone(), knob);
    }

    for (id, value) in overrides.values.iter() {
        let knob = by_id
            .get(id)
            .ok_or_else(|| anyhow!("override references unknown knob id: {}", id))?;
        validate_knob_value(knob, value)?;
        set_json_pointer_value(&mut experiment, &knob.json_pointer, value.clone())?;
    }

    Ok(experiment)
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
        if !options.iter().any(|opt| opt == value) {
            return Err(anyhow!(
                "override value for knob {} is not in allowed options",
                knob.id
            ));
        }
    }

    if let Some(min) = knob.minimum {
        if let Some(v) = value.as_f64() {
            if v < min {
                return Err(anyhow!(
                    "override value for knob {} is below minimum {}",
                    knob.id,
                    min
                ));
            }
        }
    }
    if let Some(max) = knob.maximum {
        if let Some(v) = value.as_f64() {
            if v > max {
                return Err(anyhow!(
                    "override value for knob {} is above maximum {}",
                    knob.id,
                    max
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

// ---------------------------------------------------------------------------
// Dataset & tasks
// ---------------------------------------------------------------------------

pub(crate) fn resolve_dataset_path_in_package(
    json_value: &Value,
    package_dir: &Path,
) -> Result<PathBuf> {
    let rel = json_value
        .pointer("/dataset/path")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("dataset.path missing"))?;
    resolve_package_path_under_root(package_dir, rel, "dataset.path")
}

pub(crate) fn load_task_rows_for_build(path: &Path, json_value: &Value) -> Result<Vec<Value>> {
    let limit = json_value
        .pointer("/dataset/limit")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    if limit == Some(0) {
        return Ok(Vec::new());
    }
    let dataset_ref = json_value
        .pointer("/dataset/path")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let dataset_suite = json_value
        .pointer("/dataset/suite_id")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>");
    let file = fs::File::open(path).with_context(|| {
        format!(
            "failed to open dataset file '{}' (resolved from dataset.path='{}', dataset.suite_id='{}')",
            path.display(),
            dataset_ref,
            dataset_suite
        )
    })?;
    let reader = BufReader::new(file);
    let mut tasks = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if limit.is_some_and(|max| tasks.len() >= max) {
            break;
        }
        let task: Value = serde_json::from_str(trimmed)?;
        let task_id = task
            .pointer("/task/id")
            .or_else(|| task.pointer("/id"))
            .and_then(Value::as_str)
            .unwrap_or("<unknown_task>");
        if crate::parse_task_row(&task).is_err() {
            return Err(anyhow!(
                "dataset row {} task '{}' is not a valid task_row_v1",
                idx + 1,
                task_id
            ));
        }
        tasks.push(task);
    }
    Ok(tasks)
}

pub(crate) fn load_tasks(path: &Path, json_value: &Value) -> Result<Vec<Value>> {
    let limit = json_value
        .pointer("/dataset/limit")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    if limit == Some(0) {
        return Ok(Vec::new());
    }
    let file = fs::File::open(path).with_context(|| {
        format!(
            "failed to open dataset file '{}' referenced by dataset.path during build",
            path.display()
        )
    })?;
    let reader = BufReader::new(file);
    let mut tasks = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if limit.is_some_and(|max| tasks.len() >= max) {
            break;
        }
        let task: Value = serde_json::from_str(trimmed)?;
        let task_id = task
            .pointer("/task/id")
            .or_else(|| task.pointer("/id"))
            .and_then(Value::as_str)
            .unwrap_or("<unknown_task>");
        if crate::parse_task_row(&task).is_err() {
            return Err(anyhow!(
                "dataset row {} task '{}' is not a valid packaged task_row_v1",
                idx + 1,
                task_id
            ));
        }
        tasks.push(task);
    }
    Ok(tasks)
}
