pub fn preflight_experiment(path: &Path) -> Result<PreflightReport> {
    emit_preflight_log(format!("resolving sealed package {}", path.display()));
    let preflight_started = Instant::now();
    let LoadedExperimentInput {
        json_value,
        exp_dir,
        project_root,
    } = load_sealed_package_for_run(path)?;
    validate_required_fields(&json_value)?;

    let dataset_path = resolve_dataset_path_in_package(&json_value, &exp_dir)?;
    let tasks = load_tasks(&dataset_path, &json_value)?;
    let (variants, _baseline_id) = resolve_variant_plan(&json_value)?;
    emit_preflight_log(format!(
        "resolved {} task(s) and {} variant(s); running checks",
        tasks.len(),
        variants.len()
    ));
    emit_preflight_log(format!(
        "[PROFILE] config_resolution (yaml parse + dataset load + variant resolve) took {:.3}s",
        preflight_started.elapsed().as_secs_f64()
    ));
    let benchmark_config = parse_benchmark_config(&json_value);
    let _runtime_resolve_t = Instant::now();
    let mut variant_runtime_profiles = Vec::with_capacity(variants.len());
    for variant in &variants {
        variant_runtime_profiles.push(resolve_variant_runtime_profile(
            &json_value,
            variant,
            &exp_dir,
            false,
            &RunBehavior::default(),
            &RunExecutionOptions::default(),
        )?);
    }
    emit_preflight_log(format!(
        "[PROFILE] runtime_profile_resolution ({} variants) took {:.3}s",
        variants.len(),
        _runtime_resolve_t.elapsed().as_secs_f64()
    ));
    let checks = collect_preflight_checks(
        &json_value,
        &exp_dir,
        &exp_dir,
        &project_root,
        &tasks,
        &benchmark_config,
        &variants,
        &variant_runtime_profiles,
    );

    let passed = checks
        .iter()
        .all(|c| c.passed || matches!(c.severity, PreflightSeverity::Warning));
    let failed_count = checks
        .iter()
        .filter(|check| !check.passed && matches!(check.severity, PreflightSeverity::Error))
        .count();
    let warning_count = checks
        .iter()
        .filter(|check| !check.passed && matches!(check.severity, PreflightSeverity::Warning))
        .count();
    emit_preflight_log(format!(
        "completed {} checks in {:.1}s (warnings={}, failures={})",
        checks.len(),
        preflight_started.elapsed().as_secs_f32(),
        warning_count,
        failed_count
    ));

    Ok(PreflightReport { passed, checks })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ContainerReadinessCheckKey {
    container_mode: bool,
    image_source: ImageSource,
    container_image: Option<String>,
}

#[derive(Debug, Default)]
struct PerTaskImageScanResult {
    unique_images: Vec<String>,
    missing_task_ids: Vec<String>,
    parse_errors: Vec<String>,
}

fn format_preview(items: &[String], limit: usize) -> String {
    if items.is_empty() {
        return "(none)".to_string();
    }
    let shown = items.iter().take(limit).cloned().collect::<Vec<_>>();
    if items.len() <= limit {
        shown.join(", ")
    } else {
        format!("{}, ... (+{} more)", shown.join(", "), items.len() - limit)
    }
}

fn annotate_preflight_check_with_variants(
    check: &mut PreflightCheck,
    variant_ids: &[String],
    total_variants: usize,
) {
    if total_variants <= 1 || variant_ids.is_empty() {
        return;
    }
    let prefix = if variant_ids.len() == 1 {
        format!("variant '{}': ", variant_ids[0])
    } else {
        format!("variants [{}]: ", variant_ids.join(", "))
    };
    check.message = format!("{}{}", prefix, check.message);
}

fn check_benchmark_grader_reachable_for_variants(
    benchmark_config: &BenchmarkConfig,
    variants: &[Variant],
    variant_runtime_profiles: &[VariantRuntimeProfile],
    tasks: &[Value],
    per_task_scan: Option<&PerTaskImageScanResult>,
    is_clean: bool,
    project_root: &Path,
) -> Vec<PreflightCheck> {
    if variants.len() != variant_runtime_profiles.len() {
        return vec![PreflightCheck {
            name: "benchmark_grader_reachable",
            passed: false,
            severity: PreflightSeverity::Error,
            message: "internal error: variant/runtime profile count mismatch".to_string(),
        }];
    }
    variants
        .iter()
        .zip(variant_runtime_profiles.iter())
        .map(|(variant, runtime_profile)| {
            let mut check = check_benchmark_grader_reachable_with_scan(
                benchmark_config,
                runtime_profile,
                variant,
                tasks,
                per_task_scan,
                is_clean,
                project_root,
            );
            check.message = format!("variant '{}': {}", variant.id, check.message);
            check
        })
        .collect()
}

fn check_container_ready_for_variants(
    variants: &[Variant],
    variant_runtime_profiles: &[VariantRuntimeProfile],
    tasks: &[Value],
    per_task_scan: Option<&PerTaskImageScanResult>,
    skip_shell_probe: bool,
) -> Vec<PreflightCheck> {
    if variants.is_empty() || variant_runtime_profiles.is_empty() {
        return Vec::new();
    }
    let mut grouped: BTreeMap<ContainerReadinessCheckKey, Vec<usize>> = BTreeMap::new();
    for (idx, profile) in variant_runtime_profiles.iter().enumerate() {
        let key = ContainerReadinessCheckKey {
            container_mode: profile.container_mode,
            image_source: profile.agent_runtime.image_source,
            container_image: profile.agent_runtime.container_image.clone(),
        };
        grouped.entry(key).or_default().push(idx);
    }

    let mut checks = Vec::new();
    for indices in grouped.values() {
        let representative = indices[0];
        let mut scoped_checks = check_container_ready(
            &variant_runtime_profiles[representative],
            tasks,
            per_task_scan,
            skip_shell_probe,
        );
        let variant_ids = indices
            .iter()
            .filter_map(|idx| variants.get(*idx).map(|variant| variant.id.clone()))
            .collect::<Vec<_>>();
        for check in &mut scoped_checks {
            annotate_preflight_check_with_variants(check, &variant_ids, variants.len());
        }
        checks.extend(scoped_checks);
    }
    checks
}

fn collect_per_task_images_for_preflight(tasks: &[Value]) -> PerTaskImageScanResult {
    let mut unique_images = HashSet::new();
    let mut result = PerTaskImageScanResult::default();
    for (idx, task) in tasks.iter().enumerate() {
        let boundary = match parse_task_boundary_from_dataset_task(task) {
            Ok(boundary) => boundary,
            Err(err) => {
                result
                    .parse_errors
                    .push(format!("line {}: {}", idx + 1, err));
                continue;
            }
        };
        let task_id = boundary
            .task_payload
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("line {}", idx + 1));
        match boundary
            .task_image
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(image) => {
                if unique_images.insert(image.to_string()) {
                    result.unique_images.push(image.to_string());
                }
            }
            None => result.missing_task_ids.push(task_id),
        }
    }
    result.unique_images.sort();
    result
}

fn resolve_preflight_images(
    check_name: &'static str,
    runtime_profile: &VariantRuntimeProfile,
    tasks: &[Value],
    per_task_scan: Option<&PerTaskImageScanResult>,
    missing_global_image_message: &'static str,
) -> Result<Vec<String>, PreflightCheck> {
    match runtime_profile.agent_runtime.image_source {
        ImageSource::Global => match runtime_profile.agent_runtime.container_image.as_deref() {
            Some(image) => Ok(vec![image.to_string()]),
            None => Err(PreflightCheck {
                name: check_name,
                passed: false,
                severity: PreflightSeverity::Error,
                message: missing_global_image_message.to_string(),
            }),
        },
        ImageSource::PerTask => {
            let owned_scan;
            let scan = if let Some(scan) = per_task_scan {
                scan
            } else {
                owned_scan = collect_per_task_images_for_preflight(tasks);
                &owned_scan
            };
            if !scan.parse_errors.is_empty() {
                return Err(PreflightCheck {
                    name: check_name,
                    passed: false,
                    severity: PreflightSeverity::Error,
                    message: format!(
                        "failed to parse task image boundary rows: {}",
                        format_preview(&scan.parse_errors, 3)
                    ),
                });
            }
            if !scan.missing_task_ids.is_empty() {
                return Err(PreflightCheck {
                    name: check_name,
                    passed: false,
                    severity: PreflightSeverity::Error,
                    message: format!(
                        "tasks missing task.image in per-task mode: {}",
                        format_preview(&scan.missing_task_ids, 5)
                    ),
                });
            }
            if scan.unique_images.is_empty() {
                return Err(PreflightCheck {
                    name: check_name,
                    passed: false,
                    severity: PreflightSeverity::Error,
                    message: "no task images found in dataset for per-task image mode".to_string(),
                });
            }
            Ok(scan.unique_images.clone())
        }
    }
}

fn has_blocking_preflight_error(checks: &[PreflightCheck], name: &str) -> bool {
    checks.iter().any(|check| {
        check.name == name && !check.passed && matches!(check.severity, PreflightSeverity::Error)
    })
}

fn resolve_min_free_bytes() -> Result<u64> {
    match env::var(AGENTLAB_MIN_FREE_BYTES_ENV) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(anyhow!(
                    "{} must be a positive integer when set",
                    AGENTLAB_MIN_FREE_BYTES_ENV
                ));
            }
            let parsed = trimmed.parse::<u64>().map_err(|_| {
                anyhow!(
                    "{} must be a positive integer when set (got: {})",
                    AGENTLAB_MIN_FREE_BYTES_ENV,
                    raw
                )
            })?;
            if parsed == 0 {
                return Err(anyhow!(
                    "{} must be > 0 when set",
                    AGENTLAB_MIN_FREE_BYTES_ENV
                ));
            }
            Ok(parsed)
        }
        Err(env::VarError::NotPresent) => Ok(DEFAULT_MIN_FREE_BYTES),
        Err(err) => Err(anyhow!(
            "failed reading {}: {}",
            AGENTLAB_MIN_FREE_BYTES_ENV,
            err
        )),
    }
}

fn free_bytes_for_path(path: &Path) -> Result<u64> {
    let probe = path.to_string_lossy().to_string();
    let out = Command::new("df").args(["-Pk", probe.as_str()]).output()?;
    if !out.status.success() {
        return Err(anyhow!(
            "df -Pk {} failed: {}",
            probe,
            output_error_detail(&out)
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = stdout
        .lines()
        .rev()
        .find(|candidate| {
            let trimmed = candidate.trim();
            !trimmed.is_empty() && !trimmed.starts_with("Filesystem")
        })
        .ok_or_else(|| anyhow!("unable to parse df output for {}", probe))?;
    let fields = line.split_whitespace().collect::<Vec<_>>();
    if fields.len() < 4 {
        return Err(anyhow!(
            "unable to parse df output for {} (expected >=4 columns): {}",
            probe,
            line
        ));
    }
    let available_kb = fields[3].parse::<u64>().map_err(|_| {
        anyhow!(
            "unable to parse available blocks from df output for {}: {}",
            probe,
            line
        )
    })?;
    Ok(available_kb.saturating_mul(1024))
}

fn dir_size_bytes(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0u64;
    let walker = walkdir::WalkDir::new(path).into_iter();
    for entry in walker {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        total = total.saturating_add(entry.metadata()?.len());
    }
    Ok(total)
}

fn enforce_runtime_disk_headroom(probe_path: &Path, min_free_bytes: u64) -> Result<()> {
    let available = free_bytes_for_path(probe_path)?;
    if available < min_free_bytes {
        return Err(anyhow!(
            "runtime disk headroom breached at '{}': required={} available={}",
            probe_path.display(),
            min_free_bytes,
            available
        ));
    }
    Ok(())
}

fn enforce_runtime_run_size_budget(run_dir: &Path, max_run_bytes: u64) -> Result<()> {
    let used = dir_size_bytes(run_dir)?;
    if used > max_run_bytes {
        return Err(anyhow!(
            "runtime run size budget exceeded at '{}': max={} used={}",
            run_dir.display(),
            max_run_bytes,
            used
        ));
    }
    Ok(())
}

fn check_disk_headroom_with_threshold(probe_path: &Path, min_free_bytes: u64) -> PreflightCheck {
    let name = "disk_headroom";
    let available = match free_bytes_for_path(probe_path) {
        Ok(bytes) => bytes,
        Err(err) => {
            return PreflightCheck {
                name,
                passed: false,
                severity: PreflightSeverity::Error,
                message: format!(
                    "failed to determine free disk bytes at '{}': {}",
                    probe_path.display(),
                    err
                ),
            }
        }
    };
    if available < min_free_bytes {
        return PreflightCheck {
            name,
            passed: false,
            severity: PreflightSeverity::Error,
            message: format!(
                "insufficient free disk bytes at '{}': required={} available={}",
                probe_path.display(),
                min_free_bytes,
                available
            ),
        };
    }
    PreflightCheck {
        name,
        passed: true,
        severity: PreflightSeverity::Error,
        message: format!(
            "disk headroom ok at '{}': required={} available={}",
            probe_path.display(),
            min_free_bytes,
            available
        ),
    }
}

fn check_disk_headroom(probe_path: &Path) -> PreflightCheck {
    let name = "disk_headroom";
    match resolve_min_free_bytes() {
        Ok(min_free_bytes) => check_disk_headroom_with_threshold(probe_path, min_free_bytes),
        Err(err) => PreflightCheck {
            name,
            passed: false,
            severity: PreflightSeverity::Error,
            message: err.to_string(),
        },
    }
}

fn parse_provider_env_mapping_value(
    value: Option<&Value>,
    field_name: &str,
) -> Result<BTreeMap<String, String>> {
    let mut mapping = BTreeMap::new();
    let Some(raw) = value else {
        return Ok(mapping);
    };
    match raw {
        Value::Object(obj) => {
            for (provider, env_name_raw) in obj {
                let provider_trimmed = provider.trim();
                if provider_trimmed.is_empty() {
                    return Err(anyhow!("{} contains an empty provider key", field_name));
                }
                let env_name = env_name_raw
                    .as_str()
                    .ok_or_else(|| anyhow!("{}['{}'] must be a string", field_name, provider))?
                    .trim()
                    .to_string();
                if env_name.is_empty() {
                    return Err(anyhow!(
                        "{}['{}'] must be a non-empty env var name",
                        field_name,
                        provider
                    ));
                }
                mapping.insert(provider_trimmed.to_string(), env_name);
            }
        }
        Value::Array(items) => {
            for (idx, item) in items.iter().enumerate() {
                let token = item
                    .as_str()
                    .ok_or_else(|| anyhow!("{}[{}] must be a string", field_name, idx))?
                    .trim();
                if token.is_empty() {
                    return Err(anyhow!("{}[{}] must not be empty", field_name, idx));
                }
                let (provider, env_name) = token.split_once('=').ok_or_else(|| {
                    anyhow!(
                        "{}[{}] must use provider=ENV format (got '{}')",
                        field_name,
                        idx,
                        token
                    )
                })?;
                if provider.trim().is_empty() || env_name.trim().is_empty() {
                    return Err(anyhow!(
                        "{}[{}] must use non-empty provider and env var names",
                        field_name,
                        idx
                    ));
                }
                mapping.insert(provider.trim().to_string(), env_name.trim().to_string());
            }
        }
        _ => {
            return Err(anyhow!(
                "{} must be an object<string,string> or string[]",
                field_name
            ))
        }
    }
    Ok(mapping)
}

fn resolve_provider_env_mapping(json_value: &Value) -> Result<BTreeMap<String, String>> {
    let mut mapping = BTreeMap::new();
    for (pointer, field_name) in [
        (
            "/runtime/policy/provider_env",
            "runtime.policy.provider_env",
        ),
        (
            "/runtime/policy/provider_env_map",
            "runtime.policy.provider_env_map",
        ),
    ] {
        let next = parse_provider_env_mapping_value(json_value.pointer(pointer), field_name)?;
        mapping.extend(next);
    }
    Ok(mapping)
}

fn check_provider_model_wiring(
    json_value: &Value,
    variants: &[Variant],
    variant_runtime_profiles: &[VariantRuntimeProfile],
) -> PreflightCheck {
    let name = "provider_model_wiring";
    if variants.len() != variant_runtime_profiles.len() {
        return PreflightCheck {
            name,
            passed: false,
            severity: PreflightSeverity::Error,
            message: format!(
                "variant/runtime profile mismatch: variants={} runtime_profiles={}",
                variants.len(),
                variant_runtime_profiles.len()
            ),
        };
    }

    let mut bindings = Vec::new();
    for (idx, variant) in variants.iter().enumerate() {
        let Some(raw_provider) = variant
            .bindings
            .get("model_provider")
            .and_then(Value::as_str)
            .map(str::trim)
        else {
            continue;
        };
        if raw_provider.is_empty() {
            return PreflightCheck {
                name,
                passed: false,
                severity: PreflightSeverity::Error,
                message: format!(
                    "variant '{}' has an empty bindings.model_provider",
                    variant.id
                ),
            };
        }
        bindings.push((idx, variant.id.clone(), raw_provider.to_string()));
    }

    if bindings.is_empty() {
        return PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Warning,
            message: "no bindings.model_provider values found across variants".to_string(),
        };
    }

    let mapping = match resolve_provider_env_mapping(json_value) {
        Ok(mapping) => mapping,
        Err(err) => {
            return PreflightCheck {
                name,
                passed: false,
                severity: PreflightSeverity::Error,
                message: err.to_string(),
            }
        }
    };

    let mut missing_provider_mapping = Vec::new();
    let mut missing_provider_env = Vec::new();
    for (idx, variant_id, provider) in bindings {
        let Some(env_name) = mapping.get(&provider) else {
            missing_provider_mapping.push(format!("{} (variant '{}')", provider, variant_id));
            continue;
        };
        let has_env = variant_runtime_profiles
            .get(idx)
            .and_then(|profile| profile.agent_runtime_env.get(env_name))
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        if !has_env {
            missing_provider_env.push(format!(
                "{} requires env '{}' (variant '{}')",
                provider, env_name, variant_id
            ));
        }
    }

    if !missing_provider_mapping.is_empty() {
        return PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Warning,
            message: format!(
                "unmapped bindings.model_provider values (file auth or local model?): {}",
                format_preview(&missing_provider_mapping, 4)
            ),
        };
    }
    if !missing_provider_env.is_empty() {
        return PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Warning,
            message: format!(
                "provider env vars missing from resolved runtime env: {}",
                format_preview(&missing_provider_env, 4)
            ),
        };
    }

    let providers = mapping.keys().cloned().collect::<Vec<_>>();
    PreflightCheck {
        name,
        passed: true,
        severity: PreflightSeverity::Error,
        message: format!(
            "all model_provider bindings resolved via provider env mapping (providers: {})",
            format_preview(&providers, 5)
        ),
    }
}

fn collect_preflight_checks(
    json_value: &Value,
    exp_dir: &Path,
    disk_probe_path: &Path,
    project_root: &Path,
    tasks: &[Value],
    benchmark_config: &BenchmarkConfig,
    variants: &[Variant],
    variant_runtime_profiles: &[VariantRuntimeProfile],
) -> Vec<PreflightCheck> {
    let mut checks = Vec::new();
    if variants.is_empty() {
        checks.push(PreflightCheck {
            name: "variant_runtime_profiles",
            passed: false,
            severity: PreflightSeverity::Error,
            message: "no variants available in experiment".to_string(),
        });
        return checks;
    }
    if variant_runtime_profiles.is_empty() {
        checks.push(PreflightCheck {
            name: "variant_runtime_profiles",
            passed: false,
            severity: PreflightSeverity::Error,
            message: "failed to resolve runtime profile for baseline variant".to_string(),
        });
        return checks;
    }
    let is_clean = is_clean_contract_experiment(json_value);
    let needs_per_task_scan = variant_runtime_profiles.iter().any(|profile| {
        profile.container_mode && profile.agent_runtime.image_source == ImageSource::PerTask
    });
    let per_task_scan = if needs_per_task_scan {
        Some(collect_per_task_images_for_preflight(tasks))
    } else {
        None
    };
    let skip_container_shell_probe = benchmark_config.adapter.is_some() && !is_clean;

    // Structural checks (probe_trial_input, dataset_task_ids) are now
    // validated at build time in build_swebench_curated_ab_experiment.mjs.
    // Only machine-state-dependent checks remain here.
    checks.extend(check_dataset_task_ids(
        tasks,
        benchmark_config,
        variant_runtime_profiles,
    ));

    // PROFILING: throwaway stage timing
    macro_rules! timed_check {
        ($label:expr, $body:expr) => {{
            let _t = Instant::now();
            let result = $body;
            emit_preflight_log(format!(
                "[PROFILE] {} took {:.3}s",
                $label,
                _t.elapsed().as_secs_f64()
            ));
            result
        }};
    }

    emit_preflight_log("running check: disk_headroom");
    timed_check!(
        "disk_headroom",
        checks.push(check_disk_headroom(disk_probe_path))
    );
    emit_preflight_log("running check: provider_model_wiring");
    timed_check!(
        "provider_model_wiring",
        checks.push(check_provider_model_wiring(
            json_value,
            variants,
            variant_runtime_profiles,
        ))
    );
    emit_preflight_log("running check: container_ready");
    timed_check!(
        "container_ready",
        checks.extend(check_container_ready_for_variants(
            variants,
            variant_runtime_profiles,
            tasks,
            per_task_scan.as_ref(),
            skip_container_shell_probe,
        ))
    );
    if has_blocking_preflight_error(&checks, "container_ready") {
        checks.push(PreflightCheck {
            name: "agent_runtime_reachable",
            passed: true,
            severity: PreflightSeverity::Warning,
            message: "skipped because container_ready reported blocking failures".to_string(),
        });
        checks.push(PreflightCheck {
            name: "benchmark_grader_reachable",
            passed: true,
            severity: PreflightSeverity::Warning,
            message: "skipped because container_ready reported blocking failures".to_string(),
        });
    } else {
        emit_preflight_log("running check: agent_runtime_reachable");
        timed_check!(
            "agent_runtime_reachable",
            checks.extend(check_agent_runtime_reachable_for_variants(
                variants,
                variant_runtime_profiles,
                tasks,
                per_task_scan.as_ref(),
                project_root,
            ))
        );
        if has_blocking_preflight_error(&checks, "agent_runtime_reachable") {
            checks.push(PreflightCheck {
                name: "benchmark_grader_reachable",
                passed: true,
                severity: PreflightSeverity::Warning,
                message:
                    "skipped because agent_runtime_reachable reported blocking failures"
                        .to_string(),
            });
        } else {
        emit_preflight_log("running check: benchmark_grader_reachable");
        timed_check!(
            "benchmark_grader_reachable",
            checks.extend(check_benchmark_grader_reachable_for_variants(
                benchmark_config,
                variants,
                variant_runtime_profiles,
                tasks,
                per_task_scan.as_ref(),
                is_clean,
                project_root,
            ))
        );
        }
    }
    emit_preflight_log("running check: dependency_files_exist");
    timed_check!(
        "dependency_files_exist",
        checks.extend(check_dependency_files_exist(json_value, exp_dir))
    );
    emit_preflight_log("running check: workspace_patch_sources_exist");
    timed_check!(
        "workspace_patch_sources_exist",
        checks.extend(check_workspace_patch_sources_exist(
            variants,
            variant_runtime_profiles,
        ))
    );
    checks
}

fn check_dataset_task_ids(
    tasks: &[Value],
    benchmark_config: &BenchmarkConfig,
    variant_runtime_profiles: &[VariantRuntimeProfile],
) -> Vec<PreflightCheck> {
    let mut checks = Vec::new();
    let mut seen_ids: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut malformed_boundary_rows = Vec::new();
    let mut missing_ids = Vec::new();
    let mut grading_disabled_lines = Vec::new();
    let mut missing_materialization_lines = Vec::new();
    let has_benchmark = benchmark_config.adapter.is_some();
    let require_materialization = variant_runtime_profiles
        .iter()
        .any(|profile| profile.agent_runtime.image_source == ImageSource::PerTask);

    for (idx, task) in tasks.iter().enumerate() {
        let line_num = idx + 1;
        let parsed = match parse_task_boundary_from_dataset_task(task) {
            Ok(parsed) => parsed,
            Err(err) => {
                malformed_boundary_rows.push(format!("line {}: {}", line_num, err));
                continue;
            }
        };
        let id = parsed.task_payload.get("id").and_then(|v| v.as_str());

        match id {
            Some(id_str) if !id_str.is_empty() => {
                seen_ids
                    .entry(id_str.to_string())
                    .or_default()
                    .push(line_num);
            }
            _ => {
                missing_ids.push(line_num);
            }
        }

        if has_benchmark {
            if let Some(enabled) = parsed
                .task_payload
                .pointer("/grading/enabled")
                .and_then(|v| v.as_bool())
            {
                if !enabled {
                    grading_disabled_lines.push(line_num);
                }
            }
        }
        if require_materialization
            && parsed.workspace_seed.is_none()
            && parsed.workspace_files.is_empty()
            && parsed.mount_references.is_empty()
        {
            missing_materialization_lines.push(line_num);
        }
    }

    if !malformed_boundary_rows.is_empty() {
        checks.push(PreflightCheck {
            name: "dataset_task_ids",
            passed: false,
            severity: PreflightSeverity::Error,
            message: format!(
                "malformed task boundary rows (expected strict task_boundary_v2): {}",
                format_preview(&malformed_boundary_rows, 3)
            ),
        });
    }

    if !missing_ids.is_empty() {
        checks.push(PreflightCheck {
            name: "dataset_task_ids",
            passed: false,
            severity: PreflightSeverity::Error,
            message: format!("tasks missing 'id' field at lines: {:?}", missing_ids),
        });
    }
    if !missing_materialization_lines.is_empty() {
        checks.push(PreflightCheck {
            name: "dataset_task_ids",
            passed: false,
            severity: PreflightSeverity::Error,
            message: format!(
                "tasks missing workspace materialization (workspace_seed, workspace_files, or mount_references) at lines: {:?}",
                missing_materialization_lines
            ),
        });
    }

    let duplicates: Vec<_> = seen_ids
        .iter()
        .filter(|(_, lines)| lines.len() > 1)
        .collect();
    if !duplicates.is_empty() {
        let dup_details: Vec<String> = duplicates
            .iter()
            .map(|(id, lines)| format!("'{}' at lines {:?}", id, lines))
            .collect();
        checks.push(PreflightCheck {
            name: "dataset_task_ids",
            passed: false,
            severity: PreflightSeverity::Error,
            message: format!("duplicate task IDs: {}", dup_details.join(", ")),
        });
    }

    if missing_ids.is_empty()
        && duplicates.is_empty()
        && malformed_boundary_rows.is_empty()
        && missing_materialization_lines.is_empty()
    {
        checks.push(PreflightCheck {
            name: "dataset_task_ids",
            passed: true,
            severity: PreflightSeverity::Error,
            message: format!("all {} tasks have unique IDs", tasks.len()),
        });
    }

    if !grading_disabled_lines.is_empty() {
        checks.push(PreflightCheck {
            name: "dataset_task_ids",
            passed: true,
            severity: PreflightSeverity::Warning,
            message: format!(
                "benchmark configured but grading.enabled=false at lines: {:?} — these tasks will not be scored",
                grading_disabled_lines
            ),
        });
    }

    checks
}

fn check_benchmark_grader_reachable(
    benchmark_config: &BenchmarkConfig,
    runtime_profile: &VariantRuntimeProfile,
    variant: &Variant,
    tasks: &[Value],
    is_clean: bool,
    project_root: &Path,
) -> PreflightCheck {
    check_benchmark_grader_reachable_with_scan(
        benchmark_config,
        runtime_profile,
        variant,
        tasks,
        None,
        is_clean,
        project_root,
    )
}

fn check_agent_runtime_reachable_for_variants(
    variants: &[Variant],
    variant_runtime_profiles: &[VariantRuntimeProfile],
    tasks: &[Value],
    per_task_scan: Option<&PerTaskImageScanResult>,
    project_root: &Path,
) -> Vec<PreflightCheck> {
    if variants.len() != variant_runtime_profiles.len() {
        return vec![PreflightCheck {
            name: "agent_runtime_reachable",
            passed: false,
            severity: PreflightSeverity::Error,
            message: "internal error: variant/runtime profile count mismatch".to_string(),
        }];
    }

    let mut checks = Vec::with_capacity(variant_runtime_profiles.len());
    for (variant, runtime_profile) in variants.iter().zip(variant_runtime_profiles.iter()) {
        let mut check = check_agent_runtime_reachable_with_scan(
            runtime_profile,
            variant,
            tasks,
            per_task_scan,
            project_root,
        );
        check.message = format!("variant '{}': {}", variant.id, check.message);
        checks.push(check);
    }
    checks
}

fn check_agent_runtime_reachable_with_scan(
    runtime_profile: &VariantRuntimeProfile,
    variant: &Variant,
    tasks: &[Value],
    per_task_scan: Option<&PerTaskImageScanResult>,
    project_root: &Path,
) -> PreflightCheck {
    let name = "agent_runtime_reachable";
    if !runtime_profile.container_mode {
        return PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Warning,
            message: "local process mode — container agent bootstrap not applicable".to_string(),
        };
    }

    let images = match resolve_preflight_images(
        name,
        runtime_profile,
        tasks,
        per_task_scan,
        "no container image resolved for agent runtime",
    ) {
        Ok(images) => images,
        Err(check) => return check,
    };
    emit_preflight_log(format!(
        "agent_runtime_reachable: probing {} image(s) via shared runtime launch construction",
        images.len()
    ));
    let failures = run_bounded_image_probes(&images, "agent_runtime_reachable", |idx, image| {
        if should_emit_image_probe_progress(idx + 1, images.len()) {
            emit_preflight_log(format!(
                "agent_runtime_reachable: image {}/{} ({})",
                idx + 1,
                images.len(),
                image
            ));
        }
        let context =
            match build_preflight_probe_context(runtime_profile, variant, tasks, image, project_root)
            {
                Ok(context) => context,
                Err(err) => return Some(format!("{} ({})", image, err)),
            };
        let request = build_preflight_probe_request(&context, runtime_profile, None, false);
        let resolved_command = match resolve_runtime_agent_command(&request) {
            Ok(command) => command,
            Err(err) => return Some(format!("{} ({})", image, err)),
        };
        match run_preflight_container_probe(&request, image, &resolved_command) {
            Ok(report) => {
                let blockers = detect_known_probe_output_blockers(&report);
                if blockers.is_empty() {
                    None
                } else {
                    Some(format!(
                        "{} (probe {} emitted deterministic compatibility blockers: {})",
                        image,
                        shell_join(&report.probe_command),
                        format_preview(&blockers, 3)
                    ))
                }
            }
            Err(err) => Some(format!("{} ({})", image, err)),
        }
    });

    let failures: Vec<String> = failures.into_iter().flatten().collect();
    if !failures.is_empty() {
        return PreflightCheck {
            name,
            passed: false,
            severity: PreflightSeverity::Error,
            message: format!(
                "runtime agent launch is not reachable in required images: {}",
                format_preview(&failures, 3)
            ),
        };
    }

    PreflightCheck {
        name,
        passed: true,
        severity: PreflightSeverity::Error,
        message: if images.len() == 1 {
            format!("runtime agent launch reachable in image '{}'", images[0])
        } else {
            format!(
                "runtime agent launch reachable in all {} required images",
                images.len(),
            )
        },
    }
}

fn check_benchmark_grader_reachable_with_scan(
    benchmark_config: &BenchmarkConfig,
    runtime_profile: &VariantRuntimeProfile,
    variant: &Variant,
    tasks: &[Value],
    per_task_scan: Option<&PerTaskImageScanResult>,
    is_clean: bool,
    project_root: &Path,
) -> PreflightCheck {
    let name = "benchmark_grader_reachable";

    let adapter = match benchmark_config.adapter.as_ref() {
        Some(a) => a,
        None => {
            return PreflightCheck {
                name,
                passed: true,
                severity: PreflightSeverity::Warning,
                message: "no benchmark adapter configured — grading skipped".to_string(),
            };
        }
    };

    if is_clean {
        return PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Warning,
            message: "clean_contract_v1 experiment — benchmark grading will be silently skipped"
                .to_string(),
        };
    }

    if !runtime_profile.container_mode {
        return PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Warning,
            message:
                "local process mode — inline grading won't run, only post-run batch processing"
                    .to_string(),
        };
    }

    let images = match resolve_preflight_images(
        name,
        runtime_profile,
        tasks,
        per_task_scan,
        "benchmark adapter configured but no container image specified",
    ) {
        Ok(images) => images,
        Err(check) => return check,
    };
    emit_preflight_log(format!(
        "benchmark_grader_reachable: probing {} image(s) via shared runtime launch construction",
        images.len()
    ));
    let failures = run_bounded_image_probes(&images, "benchmark_grader_reachable", |idx, image| {
        if should_emit_image_probe_progress(idx + 1, images.len()) {
            emit_preflight_log(format!(
                "benchmark_grader_reachable: image {}/{} ({})",
                idx + 1,
                images.len(),
                image
            ));
        }
        let context =
            match build_preflight_probe_context(runtime_profile, variant, tasks, image, project_root)
            {
                Ok(context) => context,
                Err(err) => return Some(format!("{} ({})", image, err)),
            };
        let request =
            build_preflight_probe_request(&context, runtime_profile, Some(adapter), true);
        let grader_command = match resolve_benchmark_grader_command(&request) {
            Ok(Some(command)) => command,
            Ok(None) => return None,
            Err(err) => return Some(format!("{} ({})", image, err)),
        };
        if let Err(err) = seed_preflight_probe_result(&context.io_paths.output_host) {
            return Some(format!("{} ({})", image, err));
        }
        match run_preflight_container_probe(&request, image, &grader_command) {
            Ok(_) => None,
            Err(err) => Some(format!("{} ({})", image, err)),
        }
    });

    let failures: Vec<String> = failures.into_iter().flatten().collect();
    if !failures.is_empty() {
        return PreflightCheck {
            name,
            passed: false,
            severity: PreflightSeverity::Error,
            message: format!(
                "benchmark grader launch is not reachable in required images: {}",
                format_preview(&failures, 3)
            ),
        };
    }

    PreflightCheck {
        name,
        passed: true,
        severity: PreflightSeverity::Error,
        message: if images.len() == 1 {
            format!("benchmark grader launch reachable in image '{}'", images[0])
        } else {
            format!(
                "benchmark grader launch reachable in all {} required images",
                images.len()
            )
        },
    }
}

fn is_runner_staged_script_path(path: &str) -> bool {
    path == AGENTLAB_CONTRACT_DEPS_DIR
        || path.starts_with(&format!("{}/", AGENTLAB_CONTRACT_DEPS_DIR))
        || path == AGENTLAB_CONTRACT_STATE_DIR
        || path.starts_with(&format!("{}/", AGENTLAB_CONTRACT_STATE_DIR))
}

fn check_container_ready(
    runtime_profile: &VariantRuntimeProfile,
    tasks: &[Value],
    per_task_scan: Option<&PerTaskImageScanResult>,
    skip_shell_probe: bool,
) -> Vec<PreflightCheck> {
    let name = "container_ready";

    if !runtime_profile.container_mode {
        return vec![PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Error,
            message: "local process mode — Docker checks skipped".to_string(),
        }];
    }

    let mut checks = Vec::new();
    let active_container_context = Command::new("docker")
        .args(["context", "show"])
        .output()
        .ok()
        .and_then(|out| {
            if out.status.success() {
                let value = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if value.is_empty() {
                    None
                } else {
                    Some(value)
                }
            } else {
                None
            }
        });

    // Check Docker daemon reachability
    let _docker_total = Instant::now();
    let _docker_daemon_t = Instant::now();
    emit_preflight_log("container_ready: checking Docker daemon reachability");
    let docker_ok = match std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        Ok(status) if status.success() => {
            checks.push(PreflightCheck {
                name,
                passed: true,
                severity: PreflightSeverity::Error,
                message: if let Some(context) = active_container_context.as_deref() {
                    format!("Docker daemon is reachable (context='{}')", context)
                } else {
                    "Docker daemon is reachable".to_string()
                },
            });
            true
        }
        Ok(_) | Err(_) => {
            checks.push(PreflightCheck {
                name,
                passed: false,
                severity: PreflightSeverity::Error,
                message: "Docker daemon is not reachable — container execution will fail"
                    .to_string(),
            });
            false
        }
    };

    emit_preflight_log(format!(
        "[PROFILE] container_ready/daemon_check took {:.3}s",
        _docker_daemon_t.elapsed().as_secs_f64()
    ));

    if !docker_ok {
        return checks;
    }

    let images = match resolve_preflight_images(
        name,
        runtime_profile,
        tasks,
        per_task_scan,
        "no container image resolved for container execution",
    ) {
        Ok(images) => images,
        Err(check) => {
            checks.push(check);
            return checks;
        }
    };

    emit_preflight_log(format!(
        "container_ready: probing {} image(s) for availability",
        images.len()
    ));
    let _image_probe_t = Instant::now();
    let mut missing_images = Vec::new();
    for (idx, image) in images.iter().enumerate() {
        if should_emit_image_probe_progress(idx + 1, images.len()) {
            emit_preflight_log(format!(
                "container_ready: image availability {}/{} ({})",
                idx + 1,
                images.len(),
                image
            ));
        }
        if let Err(err) = ensure_container_image_ready(image) {
            missing_images.push(format!("{} ({})", image, err));
        }
    }
    emit_preflight_log(format!(
        "[PROFILE] container_ready/image_probe ({} images) took {:.3}s",
        images.len(),
        _image_probe_t.elapsed().as_secs_f64()
    ));
    if !missing_images.is_empty() {
        checks.push(PreflightCheck {
            name,
            passed: false,
            severity: PreflightSeverity::Error,
            message: format!(
                "container images not available: {}",
                format_preview(&missing_images, 3)
            ),
        });
        return checks;
    }
    checks.push(PreflightCheck {
        name,
        passed: true,
        severity: PreflightSeverity::Error,
        message: if runtime_profile.agent_runtime.image_source == ImageSource::PerTask {
            format!(
                "all {} per-task images available for execution",
                images.len()
            )
        } else {
            format!("image '{}' available for execution", images[0])
        },
    });

    if skip_shell_probe {
        checks.push(PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Error,
            message: "shell probe deferred to benchmark_grader_reachable".to_string(),
        });
        emit_preflight_log(format!(
            "[PROFILE] container_ready/total took {:.3}s",
            _docker_total.elapsed().as_secs_f64()
        ));
        return checks;
    }

    let mut shell_missing = Vec::new();
    emit_preflight_log(format!(
        "container_ready: probing /bin/sh in {} image(s)",
        images.len()
    ));
    let _shell_probe_t = Instant::now();
    let shell_probe_failures =
        run_bounded_image_probes(&images, "container_ready/shell_probe", |idx, image| {
            if should_emit_image_probe_progress(idx + 1, images.len()) {
                emit_preflight_log(format!(
                    "container_ready: shell probe {}/{} ({})",
                    idx + 1,
                    images.len(),
                    image
                ));
            }
            match Command::new("docker")
                .arg("run")
                .arg("--rm")
                .args(resolve_container_platform(image).map(|platform| ["--platform", platform]).into_iter().flatten())
                .args(["--entrypoint", "/bin/sh", image, "-c", "exit 0"])
                .output()
            {
                Ok(out) if out.status.success() => None,
                Ok(out) => Some(format!("{} ({})", image, output_error_detail(&out))),
                Err(err) => Some(format!("{} ({})", image, err)),
            }
        });
    for failure in shell_probe_failures.into_iter().flatten() {
        shell_missing.push(failure);
    }
    emit_preflight_log(format!(
        "[PROFILE] container_ready/shell_probe ({} images) took {:.3}s",
        images.len(),
        _shell_probe_t.elapsed().as_secs_f64()
    ));
    if shell_missing.is_empty() {
        checks.push(PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Error,
            message: if runtime_profile.agent_runtime.image_source == ImageSource::PerTask {
                format!("/bin/sh available in all {} per-task images", images.len())
            } else {
                "/bin/sh available in image".to_string()
            },
        });
    } else {
        checks.push(PreflightCheck {
            name,
            passed: false,
            severity: PreflightSeverity::Error,
            message: format!(
                "/bin/sh not available in required images: {} — entrypoint wrapper will fail",
                format_preview(&shell_missing, 3)
            ),
        });
    }

    emit_preflight_log(format!(
        "[PROFILE] container_ready/total took {:.3}s",
        _docker_total.elapsed().as_secs_f64()
    ));
    checks
}

fn seed_preflight_probe_result(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    fs::write(path, b"{\"output\":{\"patch\":\"\"}}\n")?;
    Ok(())
}

fn check_dependency_files_exist(json_value: &Value, exp_dir: &Path) -> Vec<PreflightCheck> {
    let name = "dependency_files_exist";
    let staging = match parse_dependency_file_staging(json_value, exp_dir) {
        Ok(s) => s,
        Err(e) => {
            return vec![PreflightCheck {
                name,
                passed: false,
                severity: PreflightSeverity::Error,
                message: format!("failed to parse dependency staging config: {}", e),
            }];
        }
    };

    if staging.is_empty() {
        return vec![PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Error,
            message: "no dependency file staging configured".to_string(),
        }];
    }

    let mut checks = Vec::new();
    for spec in &staging {
        let exists = spec.source_from_host.exists();
        if spec.required {
            checks.push(PreflightCheck {
                name,
                passed: exists,
                severity: PreflightSeverity::Error,
                message: if exists {
                    format!("required file exists: {}", spec.source_from_host.display())
                } else {
                    format!(
                        "required file missing: {} — trial execution will fail",
                        spec.source_from_host.display()
                    )
                },
            });
        } else if !exists {
            checks.push(PreflightCheck {
                name,
                passed: true,
                severity: PreflightSeverity::Warning,
                message: format!("optional file missing: {}", spec.source_from_host.display()),
            });
        }
    }

    if checks.is_empty() {
        checks.push(PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Error,
            message: format!("all {} dependency staging entries present", staging.len()),
        });
    }

    checks
}

fn check_workspace_patch_sources_exist(
    variants: &[Variant],
    variant_runtime_profiles: &[VariantRuntimeProfile],
) -> Vec<PreflightCheck> {
    let name = "workspace_patch_sources_exist";
    if variants.is_empty() || variant_runtime_profiles.is_empty() {
        return Vec::new();
    }
    let mut checks = Vec::new();
    let mut any_patch = false;
    for (idx, profile) in variant_runtime_profiles.iter().enumerate() {
        let variant_id = variants
            .get(idx)
            .map(|v| v.id.clone())
            .unwrap_or_else(|| format!("variant_{}", idx));
        for patch in &profile.agent_runtime.workspace_patches {
            any_patch = true;
            let exists = patch.source_from_host.exists();
            checks.push(PreflightCheck {
                name,
                passed: exists,
                severity: PreflightSeverity::Error,
                message: if exists {
                    format!(
                        "variant '{}': workspace patch source exists: {}",
                        variant_id,
                        patch.source_from_host.display()
                    )
                } else {
                    format!(
                        "variant '{}': workspace patch source missing: {}",
                        variant_id,
                        patch.source_from_host.display()
                    )
                },
            });
        }
    }
    if !any_patch {
        checks.push(PreflightCheck {
            name,
            passed: true,
            severity: PreflightSeverity::Error,
            message: "no workspace_patches configured".to_string(),
        });
    }
    checks
}

// ---------------------------------------------------------------------------
// Trial scheduling
// ---------------------------------------------------------------------------
fn benchmark_identity_from_manifest(
    manifest: &Value,
) -> Result<(String, String, Option<String>, String)> {
    let adapter_id = manifest
        .pointer("/adapter_id")
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("benchmark adapter manifest missing /adapter_id"))?
        .to_string();
    let name = manifest
        .pointer("/benchmark/name")
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("benchmark adapter manifest missing /benchmark/name"))?
        .to_string();
    let version = manifest
        .pointer("/benchmark/version")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let split = manifest
        .pointer("/benchmark/split")
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("benchmark adapter manifest missing /benchmark/split"))?
        .to_string();
    Ok((adapter_id, name, version, split))
}

fn read_jsonl_records(path: &Path) -> Result<Vec<Value>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        rows.push(serde_json::from_str::<Value>(trimmed)?);
    }
    Ok(rows)
}
fn validate_json_file_against_schema(schema_name: &str, path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(anyhow!(
            "required artifact missing for schema {}: {}",
            schema_name,
            path.display()
        ));
    }
    let schema = compile_schema(schema_name)?;
    let raw = fs::read_to_string(path)?;
    let value: Value = serde_json::from_str(&raw)?;
    if let Err(errors) = schema.validate(&value) {
        let msgs = errors.map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
        return Err(anyhow!(
            "schema validation failed ({}) {}: {}",
            schema_name,
            path.display(),
            msgs
        ));
    }
    Ok(())
}

fn validate_jsonl_against_schema(schema_name: &str, path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(anyhow!(
            "required artifact missing for schema {}: {}",
            schema_name,
            path.display()
        ));
    }
    let schema = compile_schema(schema_name)?;
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(trimmed)
            .map_err(|e| anyhow!("invalid json line {} in {}: {}", idx + 1, path.display(), e))?;
        match schema.validate(&value) {
            Ok(_) => {}
            Err(errors) => {
                let msgs = errors.map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
                return Err(anyhow!(
                    "schema validation failed ({}) {} line {}: {}",
                    schema_name,
                    path.display(),
                    idx + 1,
                    msgs
                ));
            }
        };
    }
    Ok(())
}

fn build_benchmark_summary(run_id: &str, manifest: &Value, score_rows: &[Value]) -> Result<Value> {
    let (adapter_id, name, version, split) = benchmark_identity_from_manifest(manifest)?;
    let evaluator = manifest
        .pointer("/evaluator")
        .cloned()
        .ok_or_else(|| anyhow!("benchmark adapter manifest missing /evaluator"))?;

    let mut totals = BTreeMap::from([
        ("pass".to_string(), 0usize),
        ("fail".to_string(), 0usize),
        ("missing".to_string(), 0usize),
        ("error".to_string(), 0usize),
    ]);
    let mut by_variant: BTreeMap<String, Vec<&Value>> = BTreeMap::new();

    for row in score_rows {
        let verdict = row
            .pointer("/verdict")
            .and_then(|v| v.as_str())
            .unwrap_or("error")
            .to_string();
        *totals.entry(verdict).or_default() += 1;
        let variant_id = row
            .pointer("/ids/variant_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        by_variant.entry(variant_id).or_default().push(row);
    }

    let mut variants = Vec::new();
    for (variant_id, rows) in by_variant {
        let total = rows.len();
        let pass = rows
            .iter()
            .filter(|r| r.pointer("/verdict").and_then(|v| v.as_str()) == Some("pass"))
            .count();
        let fail = rows
            .iter()
            .filter(|r| r.pointer("/verdict").and_then(|v| v.as_str()) == Some("fail"))
            .count();
        let missing = rows
            .iter()
            .filter(|r| r.pointer("/verdict").and_then(|v| v.as_str()) == Some("missing"))
            .count();
        let error = rows
            .iter()
            .filter(|r| r.pointer("/verdict").and_then(|v| v.as_str()) == Some("error"))
            .count();
        let pass_rate = if total > 0 {
            pass as f64 / total as f64
        } else {
            0.0
        };
        let primary_metric_name = rows
            .iter()
            .find_map(|r| {
                r.pointer("/primary_metric_name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| "resolved".to_string());
        let mut pm_sum = 0.0f64;
        let mut pm_count = 0usize;
        for row in rows {
            if let Some(v) = row
                .pointer("/primary_metric_value")
                .and_then(|v| v.as_f64())
            {
                pm_sum += v;
                pm_count += 1;
            }
        }
        let primary_metric_mean = if pm_count > 0 {
            pm_sum / pm_count as f64
        } else {
            0.0
        };
        variants.push(json!({
            "variant_id": variant_id,
            "total": total,
            "pass": pass,
            "fail": fail,
            "missing": missing,
            "error": error,
            "pass_rate": pass_rate,
            "primary_metric_name": primary_metric_name,
            "primary_metric_mean": primary_metric_mean
        }));
    }

    let mut benchmark = serde_json::Map::new();
    benchmark.insert("adapter_id".to_string(), json!(adapter_id));
    benchmark.insert("name".to_string(), json!(name));
    benchmark.insert("split".to_string(), json!(split));
    if let Some(version) = version {
        benchmark.insert("version".to_string(), json!(version));
    }

    Ok(json!({
        "schema_version": "benchmark_summary_v1",
        "created_at": Utc::now().to_rfc3339(),
        "run_id": run_id,
        "benchmark": Value::Object(benchmark),
        "evaluator": evaluator,
        "totals": {
            "trials": score_rows.len(),
            "pass": totals.get("pass").copied().unwrap_or(0),
            "fail": totals.get("fail").copied().unwrap_or(0),
            "missing": totals.get("missing").copied().unwrap_or(0),
            "error": totals.get("error").copied().unwrap_or(0)
        },
        "variants": variants
    }))
}

fn synthesize_benchmark_manifest_from_scores(score_rows: &[Value]) -> Option<Value> {
    let first = score_rows.first()?;
    let adapter_id = first
        .pointer("/benchmark/adapter_id")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())?;
    let benchmark_name = first
        .pointer("/benchmark/name")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())?;
    let benchmark_split = first
        .pointer("/benchmark/split")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())?;
    let benchmark_version = first
        .pointer("/benchmark/version")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let evaluator = first.pointer("/evaluator").cloned().unwrap_or_else(|| {
        json!({
            "name": "unknown",
            "mode": "custom"
        })
    });

    let mut benchmark = serde_json::Map::new();
    benchmark.insert("name".to_string(), json!(benchmark_name));
    benchmark.insert("split".to_string(), json!(benchmark_split));
    if let Some(version) = benchmark_version {
        benchmark.insert("version".to_string(), json!(version));
    }

    Some(json!({
        "schema_version": "benchmark_adapter_manifest_v1",
        "adapter_id": adapter_id,
        "adapter_version": "unknown",
        "benchmark": Value::Object(benchmark),
        "execution_mode": "integrated_score",
        "record_schemas": {
            "prediction": "benchmark_prediction_record_v1",
            "score": "benchmark_score_record_v1"
        },
        "evaluator": evaluator
    }))
}

fn default_benchmark_manifest(adapter: &BenchmarkAdapterConfig, score_rows: &[Value]) -> Value {
    if let Some(manifest) = adapter.manifest.clone() {
        return manifest;
    }
    if let Some(manifest) = synthesize_benchmark_manifest_from_scores(score_rows) {
        return manifest;
    }
    let fallback_adapter_id = adapter
        .command
        .first()
        .map(|s| s.as_str())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("benchmark_adapter");
    json!({
        "schema_version": "benchmark_adapter_manifest_v1",
        "adapter_id": fallback_adapter_id,
        "adapter_version": "unknown",
        "benchmark": {
            "name": "unknown",
            "split": "unknown"
        },
        "execution_mode": "integrated_score",
        "record_schemas": {
            "prediction": "benchmark_prediction_record_v1",
            "score": "benchmark_score_record_v1"
        },
        "evaluator": {
            "name": "unknown",
            "mode": "custom"
        }
    })
}

fn process_benchmark_outputs(
    _project_root: &Path,
    run_dir: &Path,
    run_id: &str,
    adapter: &BenchmarkAdapterConfig,
    _evidence_records_path: &Path,
    _task_chain_states_path: &Path,
) -> Result<PathBuf> {
    let benchmark_dir = run_dir.join("benchmark");
    ensure_dir(&benchmark_dir)?;
    let manifest_path = benchmark_dir.join("adapter_manifest.json");
    let predictions_path = benchmark_dir.join("predictions.jsonl");
    let scores_path = benchmark_dir.join("scores.jsonl");
    let summary_path = benchmark_dir.join("summary.json");

    if !predictions_path.exists() {
        atomic_write_bytes(&predictions_path, b"")?;
    }
    if !scores_path.exists() {
        atomic_write_bytes(&scores_path, b"")?;
    }

    validate_jsonl_against_schema(
        "benchmark_prediction_record_v1.jsonschema",
        &predictions_path,
    )?;
    validate_jsonl_against_schema("benchmark_score_record_v1.jsonschema", &scores_path)?;

    let scores = read_jsonl_records(&scores_path)?;
    let manifest = default_benchmark_manifest(adapter, &scores);
    atomic_write_json_pretty(&manifest_path, &manifest)?;
    validate_json_file_against_schema("benchmark_adapter_manifest_v1.jsonschema", &manifest_path)?;

    let summary = build_benchmark_summary(run_id, &manifest, &scores)?;
    atomic_write_json_pretty(&summary_path, &summary)?;
    validate_json_file_against_schema("benchmark_summary_v1.jsonschema", &summary_path)?;

    Ok(scores_path)
}
// --- Schedule progress tracking for resumable runs ---
// ---------------------------------------------------------------------------
fn resolve_dataset_path(json_value: &Value, exp_dir: &Path) -> Result<PathBuf> {
    let rel = json_value
        .pointer("/dataset/path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("dataset.path missing"))?;
    let path = exp_dir.join(rel);
    Ok(path)
}
fn count_tasks(path: &Path, json_value: &Value) -> Result<usize> {
    let limit = json_value
        .pointer("/dataset/limit")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    if limit == Some(0) {
        return Ok(0);
    }
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut count = 0usize;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if limit.is_some_and(|max| count >= max) {
            break;
        }
        count += 1;
    }
    Ok(count)
}

const TASK_BOUNDARY_V2_SCHEMA_VERSION: &str = "task_boundary_v2";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkspaceFileSpec {
    path: String,
    content: String,
    #[serde(default)]
    encoding: Option<String>,
    #[serde(default)]
    executable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MountReferenceSpec {
    dataset_pack_ref: String,
    mount_path: String,
    #[serde(default)]
    read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkspaceSeedSpec {
    dataset_pack_ref: String,
}
