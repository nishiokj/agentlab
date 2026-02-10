use anyhow::Result;
use clap::{Parser, Subcommand};
use serde_json::{json, Value};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "lab", version = "0.3.0", about = "AgentLab Rust CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Run {
        experiment: PathBuf,
        #[arg(long)]
        container: bool,
        #[arg(long)]
        overrides: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    RunDev {
        experiment: PathBuf,
        #[arg(long)]
        setup: Option<String>,
        #[arg(long)]
        overrides: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    RunExperiment {
        experiment: PathBuf,
        #[arg(long)]
        build_image: bool,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        overrides: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    Describe {
        experiment: PathBuf,
        #[arg(long)]
        overrides: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    KnobsInit {
        #[arg(long, default_value = ".lab/knobs/manifest.json")]
        manifest: PathBuf,
        #[arg(long, default_value = ".lab/knobs/overrides.json")]
        overrides: PathBuf,
        #[arg(long)]
        force: bool,
    },
    KnobsValidate {
        #[arg(long, default_value = ".lab/knobs/manifest.json")]
        manifest: PathBuf,
        #[arg(long, default_value = ".lab/knobs/overrides.json")]
        overrides: PathBuf,
        #[arg(long)]
        json: bool,
    },
    SchemaValidate {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        json: bool,
    },
    HooksValidate {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        events: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Publish {
        #[arg(long)]
        run_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    ImageBuild {
        experiment: PathBuf,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        dockerfile: Option<PathBuf>,
        #[arg(long)]
        context: Option<PathBuf>,
        #[arg(long)]
        overrides: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    Init {
        #[arg(long)]
        in_place: bool,
        #[arg(long)]
        language: Option<String>,
        #[arg(long, default_value = "agent_harness")]
        workload_type: String,
        #[arg(long)]
        container: bool,
        #[arg(long)]
        force: bool,
    },
    Clean {
        #[arg(long)]
        init: bool,
        #[arg(long)]
        runs: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let json_mode = command_json_mode(&cli.command);
    let result = run_command(cli.command);
    match result {
        Ok(Some(payload)) => {
            emit_json(&payload);
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(err) => {
            if json_mode {
                emit_json(&json_error("command_failed", err.to_string(), json!({})));
                std::process::exit(1);
            }
            Err(err)
        }
    }
}

fn run_command(command: Commands) -> Result<Option<Value>> {
    match command {
        Commands::Run {
            experiment,
            container,
            overrides,
            json,
        } => {
            let summary =
                lab_runner::describe_experiment_with_overrides(&experiment, overrides.as_deref())?;
            let result = lab_runner::run_experiment_with_overrides(
                &experiment,
                container,
                overrides.as_deref(),
            )?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "run",
                    "summary": summary_to_json(&summary),
                    "run": run_result_to_json(&result),
                    "container": container
                })));
            }
            print_summary(&summary);
            println!("run_id: {}", result.run_id);
            println!("run_dir: {}", result.run_dir.display());
        }
        Commands::RunDev {
            experiment,
            setup,
            overrides,
            json,
        } => {
            let summary =
                lab_runner::describe_experiment_with_overrides(&experiment, overrides.as_deref())?;
            let setup_for_json = setup.clone();
            let result = lab_runner::run_experiment_dev_with_overrides(
                &experiment,
                setup.clone(),
                overrides.as_deref(),
            )?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "run-dev",
                    "summary": summary_to_json(&summary),
                    "run": run_result_to_json(&result),
                    "dev_setup": setup_for_json,
                    "dev_network_mode": "full"
                })));
            }
            print_summary(&summary);
            if let Some(s) = &setup {
                println!("dev_setup: {}", s);
            } else {
                println!("dev_setup: none");
            }
            println!("dev_network_mode: full");
            println!("run_id: {}", result.run_id);
            println!("run_dir: {}", result.run_dir.display());
        }
        Commands::RunExperiment {
            experiment,
            build_image,
            tag,
            overrides,
            json,
        } => {
            let summary =
                lab_runner::describe_experiment_with_overrides(&experiment, overrides.as_deref())?;
            let mut docker_build_status: Option<String> = None;
            if build_image {
                let image = tag.or(summary.image.clone()).ok_or_else(|| {
                    anyhow::anyhow!("missing image tag (pass --tag or set runtime.sandbox.image)")
                })?;
                let (dockerfile_path, context_path) =
                    lab_runner::resolve_docker_build_with_overrides(
                        &experiment,
                        overrides.as_deref(),
                    )?;
                if !dockerfile_path.exists() {
                    return Err(anyhow::anyhow!(format!(
                        "dockerfile not found: {}",
                        dockerfile_path.display()
                    )));
                }
                if !context_path.exists() {
                    return Err(anyhow::anyhow!(format!(
                        "context dir not found: {}",
                        context_path.display()
                    )));
                }
                let status = lab_runner::run_docker_build(&image, &dockerfile_path, &context_path)?;
                if !json {
                    println!("docker_build: {}", status);
                }
                docker_build_status = Some(status);
            }
            let result = lab_runner::run_experiment_strict_with_overrides(
                &experiment,
                overrides.as_deref(),
            )?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "run-experiment",
                    "summary": summary_to_json(&summary),
                    "run": run_result_to_json(&result),
                    "experiment_network_requirement": "none",
                    "docker_build_status": docker_build_status
                })));
            }
            print_summary(&summary);
            println!("experiment_network_requirement: none");
            println!("run_id: {}", result.run_id);
            println!("run_dir: {}", result.run_dir.display());
        }
        Commands::Describe {
            experiment,
            overrides,
            json,
        } => {
            let summary =
                lab_runner::describe_experiment_with_overrides(&experiment, overrides.as_deref())?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "describe",
                    "summary": summary_to_json(&summary)
                })));
            }
            print_summary(&summary);
        }
        Commands::KnobsInit {
            manifest,
            overrides,
            force,
        } => {
            write_knob_files(&manifest, &overrides, force)?;
            println!("wrote: {}", manifest.display());
            println!("wrote: {}", overrides.display());
            println!(
                "next: lab knobs-validate --manifest {} --overrides {}",
                manifest.display(),
                overrides.display()
            );
        }
        Commands::KnobsValidate {
            manifest,
            overrides,
            json,
        } => {
            lab_runner::validate_knob_overrides(&manifest, &overrides)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "knobs-validate",
                    "valid": true,
                    "manifest": manifest.display().to_string(),
                    "overrides": overrides.display().to_string()
                })));
            }
            println!("ok");
        }
        Commands::SchemaValidate { schema, file, json } => {
            let compiled = lab_schemas::compile_schema(&schema)?;
            let data = std::fs::read_to_string(file)?;
            let value: serde_json::Value = serde_json::from_str(&data)?;
            if let Err(errors) = compiled.validate(&value) {
                for e in errors {
                    eprintln!("schema error: {}", e);
                }
                std::process::exit(1);
            }
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "schema-validate",
                    "valid": true,
                    "schema": schema
                })));
            }
            println!("ok");
        }
        Commands::HooksValidate {
            manifest,
            events,
            json,
        } => {
            let man = lab_hooks::load_manifest(&manifest)?;
            let schema = lab_schemas::compile_schema("hook_events_v1.jsonschema")?;
            lab_hooks::validate_hooks(&man, &events, &schema)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "hooks-validate",
                    "valid": true,
                    "manifest": manifest.display().to_string(),
                    "events": events.display().to_string()
                })));
            }
            println!("ok");
        }
        Commands::Publish { run_dir, out, json } => {
            let out_path = out.unwrap_or(run_dir.join("debug_bundles").join("bundle.zip"));
            std::fs::create_dir_all(out_path.parent().unwrap())?;
            lab_provenance::build_debug_bundle(&run_dir, &out_path)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "publish",
                    "bundle": out_path.display().to_string(),
                    "run_dir": run_dir.display().to_string()
                })));
            }
            println!("bundle: {}", out_path.display());
        }
        Commands::ImageBuild {
            experiment,
            tag,
            dockerfile,
            context,
            overrides,
            json,
        } => {
            let summary =
                lab_runner::describe_experiment_with_overrides(&experiment, overrides.as_deref())?;
            let image = tag.or(summary.image.clone()).ok_or_else(|| {
                anyhow::anyhow!("missing image tag (pass --tag or set runtime.sandbox.image)")
            })?;
            let (mut dockerfile_path, mut context_path) =
                lab_runner::resolve_docker_build_with_overrides(&experiment, overrides.as_deref())?;
            if let Some(df) = dockerfile {
                dockerfile_path = df;
            }
            if let Some(ctx) = context {
                context_path = ctx;
            }
            if !dockerfile_path.exists() {
                return Err(anyhow::anyhow!(format!(
                    "dockerfile not found: {}",
                    dockerfile_path.display()
                )));
            }
            if !context_path.exists() {
                return Err(anyhow::anyhow!(format!(
                    "context dir not found: {}",
                    context_path.display()
                )));
            }
            let status = lab_runner::run_docker_build(&image, &dockerfile_path, &context_path)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "image-build",
                    "image": image,
                    "dockerfile": dockerfile_path.display().to_string(),
                    "context": context_path.display().to_string(),
                    "docker_build": status
                })));
            }
            println!("docker_build: {}", status);
        }
        Commands::Init {
            in_place,
            language,
            workload_type,
            container,
            force,
        } => {
            let cwd = std::env::current_dir()?;
            let root = cwd;
            let lab_dir = root.join(".lab");
            std::fs::create_dir_all(&lab_dir)?;
            let bin_name = std::env::args()
                .next()
                .and_then(|s| {
                    std::path::Path::new(&s)
                        .file_name()
                        .map(|p| p.to_string_lossy().to_string())
                })
                .unwrap_or_else(|| "lab".to_string());

            let (exp_path, tasks_path) = if in_place {
                (root.join("experiment.yaml"), root.join("tasks.jsonl"))
            } else {
                (lab_dir.join("experiment.yaml"), lab_dir.join("tasks.jsonl"))
            };

            if !force {
                if exp_path.exists() || tasks_path.exists() {
                    return Err(anyhow::anyhow!(format!(
                        "init files already exist (use --force or --in-place): {}, {}",
                        exp_path.display(),
                        tasks_path.display()
                    )));
                }
            }

            let detected = detect_language(&root);
            let lang = language.unwrap_or(detected);
            if workload_type != "agent_harness" && workload_type != "trainer" {
                return Err(anyhow::anyhow!(
                    "invalid --workload-type '{}'; expected 'agent_harness' or 'trainer'",
                    workload_type
                ));
            }
            let (image, harness_cmd, warn) = defaults_for_language(&lang, &root, &workload_type);
            let sandbox_block = if container {
                format!(
                    "  sandbox:\n    mode: container\n    engine: docker\n    image: {}\n    root_read_only: true\n    run_as_user: \"1000:1000\"\n    hardening:\n      no_new_privileges: true\n      drop_all_caps: true\n    resources:\n      cpu_count: 2\n      memory_mb: 2048\n",
                    image
                )
            } else {
                "  sandbox:\n    mode: local\n".to_string()
            };

            let (primary_metrics, secondary_metrics) = if workload_type == "trainer" {
                ("[primary_metric]", "[train_loss, val_loss, wall_time_s]")
            } else {
                ("[success]", "[latency_ms]")
            };
            let exp_yaml = format!(
                "version: '0.3'\nexperiment:\n  id: exp_local\n  name: AgentLab Experiment\n  description: Generated by lab init\n  workload_type: {}\n  owner: you\n  tags: [example]\ndataset:\n  suite_id: local_suite\n  provider: local_jsonl\n  path: {}\n  schema_version: task_jsonl_v1\n  split_id: dev\n  limit: 50\ndesign:\n  sanitization_profile: hermetic_functional_v2\n  comparison: paired\n  replications: 1\n  random_seed: 1337\n  shuffle_tasks: true\n  max_concurrency: 1\nanalysis_plan:\n  primary_metrics: {}\n  secondary_metrics: {}\n  missingness:\n    policy: paired_drop\n    record_reasons: true\n  tests:\n    success: {{ method: paired_bootstrap, ci: 0.95, resamples: 1000 }}\n    latency_ms: {{ method: paired_bootstrap, ci: 0.95, resamples: 1000 }}\n  multiple_comparisons:\n    method: none\n  reporting:\n    effect_sizes: [risk_diff, median_diff]\n    show_task_level_table: true\nbaseline:\n  variant_id: base\n  bindings: {{}}\nvariant_plan: []\nruntime:\n  harness:\n    mode: cli\n    command: {}\n    integration_level: cli_basic\n    input_path: /out/trial_input.json\n    output_path: /out/trial_output.json\n    control_plane:\n      mode: file\n      path: /state/lab_control.json\n{}  network:\n    mode: none\n    allowed_hosts: []\nvalidity:\n  fail_on_state_leak: true\n  fail_on_profile_invariant_violation: true\n",
                workload_type,
                if in_place { "tasks.jsonl" } else { "tasks.jsonl" },
                primary_metrics,
                secondary_metrics,
                harness_cmd,
                sandbox_block
            );

            std::fs::write(&exp_path, exp_yaml)?;
            let tasks_template = if workload_type == "trainer" {
                "{ \"id\": \"job_0\", \"training\": { \"dataset\": \"dataset_a\", \"model\": \"resnet18\", \"epochs\": 5, \"seed\": 1337 } }\n{ \"id\": \"job_1\", \"training\": { \"dataset\": \"dataset_a\", \"model\": \"resnet18\", \"epochs\": 5, \"seed\": 1338 } }\n"
            } else {
                "{ \"id\": \"task_0\", \"prompt\": \"Say hello\" }\n{ \"id\": \"task_1\", \"prompt\": \"Return the number 2\" }\n"
            };
            std::fs::write(&tasks_path, tasks_template)?;
            let knobs_manifest = root.join(".lab").join("knobs").join("manifest.json");
            let knobs_overrides = root.join(".lab").join("knobs").join("overrides.json");
            write_knob_files(&knobs_manifest, &knobs_overrides, force)?;

            let exp_show = exp_path.strip_prefix(&root).unwrap_or(&exp_path).display();
            let tasks_show = tasks_path
                .strip_prefix(&root)
                .unwrap_or(&tasks_path)
                .display();
            let knobs_manifest_show = knobs_manifest
                .strip_prefix(&root)
                .unwrap_or(&knobs_manifest)
                .display();
            let knobs_overrides_show = knobs_overrides
                .strip_prefix(&root)
                .unwrap_or(&knobs_overrides)
                .display();
            println!("wrote: {}", exp_show);
            println!("wrote: {}", tasks_show);
            println!("wrote: {}", knobs_manifest_show);
            println!("wrote: {}", knobs_overrides_show);
            if !in_place {
                println!("note: experiment lives under .lab/; harness paths resolve relative to repo root");
            }
            if let Some(w) = warn {
                println!("assumption: {}", w);
            }
            println!("workload_type: {}", workload_type);
            let exp_arg = exp_path.strip_prefix(&root).unwrap_or(&exp_path).display();
            println!("next: {} describe {}", bin_name, exp_arg);
            println!(
                "next: {} describe {} --overrides {}",
                bin_name, exp_arg, knobs_overrides_show
            );
            if container {
                println!(
                    "next: {} run-dev {} --overrides {} --setup \"<install command>\"",
                    bin_name, exp_arg, knobs_overrides_show
                );
                println!(
                    "next: {} run-experiment {} --overrides {} --build-image",
                    bin_name, exp_arg, knobs_overrides_show
                );
            } else {
                println!(
                    "next: {} run {} --overrides {}",
                    bin_name, exp_arg, knobs_overrides_show
                );
            }
        }
        Commands::Clean { init, runs } => {
            let root = std::env::current_dir()?;
            let lab_dir = root.join(".lab");
            if init {
                let candidates = vec![
                    root.join("experiment.yaml"),
                    root.join("tasks.jsonl"),
                    lab_dir.join("experiment.yaml"),
                    lab_dir.join("tasks.jsonl"),
                ];
                for p in candidates {
                    if p.exists() {
                        let _ = std::fs::remove_file(&p);
                        println!("removed: {}", p.display());
                    }
                }
            }
            if runs {
                let runs_dir = lab_dir.join("runs");
                if runs_dir.exists() {
                    std::fs::remove_dir_all(&runs_dir)?;
                    println!("removed: {}", runs_dir.display());
                }
            }
        }
    }
    Ok(None)
}

fn emit_json(value: &Value) {
    match serde_json::to_string(value) {
        Ok(s) => println!("{}", s),
        Err(_) => println!(
            "{{\"ok\":false,\"error\":{{\"code\":\"serialization_error\",\"message\":\"failed to serialize JSON payload\",\"details\":{{}}}}}}"
        ),
    }
}

fn json_error(code: &str, message: String, details: Value) -> Value {
    json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message,
            "details": details
        }
    })
}

fn command_json_mode(command: &Commands) -> bool {
    match command {
        Commands::Run { json, .. }
        | Commands::RunDev { json, .. }
        | Commands::RunExperiment { json, .. }
        | Commands::Describe { json, .. }
        | Commands::KnobsValidate { json, .. }
        | Commands::SchemaValidate { json, .. }
        | Commands::HooksValidate { json, .. }
        | Commands::Publish { json, .. }
        | Commands::ImageBuild { json, .. } => *json,
        _ => false,
    }
}

fn run_result_to_json(result: &lab_runner::RunResult) -> Value {
    json!({
        "run_id": result.run_id,
        "run_dir": result.run_dir.display().to_string()
    })
}

fn summary_to_json(summary: &lab_runner::ExperimentSummary) -> Value {
    json!({
        "experiment": summary.exp_id,
        "workload_type": summary.workload_type,
        "dataset": summary.dataset_path.display().to_string(),
        "tasks": summary.task_count,
        "replications": summary.replications,
        "variant_plan_entries": summary.variant_count,
        "total_trials": summary.total_trials,
        "harness": summary.harness_command,
        "integration_level": summary.integration_level,
        "container_mode": summary.container_mode,
        "image": summary.image,
        "network": summary.network_mode,
        "events_path": summary.events_path,
        "tracing": summary.tracing_mode,
        "control_path": summary.control_path,
        "harness_script_resolved": summary.harness_script_resolved.as_ref().map(|p| p.display().to_string()),
        "harness_script_exists": summary.harness_script_exists
    })
}

fn print_summary(summary: &lab_runner::ExperimentSummary) {
    println!("experiment: {}", summary.exp_id);
    println!("workload_type: {}", summary.workload_type);
    println!("dataset: {}", summary.dataset_path.display());
    println!("tasks: {}", summary.task_count);
    println!("replications: {}", summary.replications);
    println!("variant_plan_entries: {}", summary.variant_count);
    println!("total_trials: {}", summary.total_trials);
    println!("harness: {:?}", summary.harness_command);
    println!("integration_level: {}", summary.integration_level);
    println!("container_mode: {}", summary.container_mode);
    if let Some(image) = &summary.image {
        println!("image: {}", image);
    }
    println!("network: {}", summary.network_mode);
    if let Some(events) = &summary.events_path {
        println!("events_path: {}", events);
    }
    if let Some(mode) = &summary.tracing_mode {
        println!("tracing: {}", mode);
    }
    println!("control_path: {}", summary.control_path);
    if let Some(p) = &summary.harness_script_resolved {
        println!("harness_script_resolved: {}", p.display());
        println!("harness_script_exists: {}", summary.harness_script_exists);
    }
}

fn detect_language(root: &std::path::Path) -> String {
    if root.join("package.json").exists()
        || root.join("agentlab/harness.js").exists()
        || root.join("harness.js").exists()
        || root.join("agentlab_demo_harness.js").exists()
        || root.join("src/harness.js").exists()
    {
        return "node".to_string();
    }
    if root.join("harness.py").exists()
        || root.join("main.py").exists()
        || root.join("src/harness.py").exists()
    {
        return "python".to_string();
    }
    if root.join("Cargo.toml").exists() {
        return "rust".to_string();
    }
    if root.join("go.mod").exists() {
        return "go".to_string();
    }
    if root.join("pyproject.toml").exists() || root.join("requirements.txt").exists() {
        return "python".to_string();
    }
    "unknown".to_string()
}

fn defaults_for_language(
    lang: &str,
    root: &std::path::Path,
    workload_type: &str,
) -> (String, String, Option<String>) {
    let pick = |candidates: &[&str]| -> Option<String> {
        for c in candidates {
            let p = root.join(c);
            if p.exists() {
                return Some(c.to_string());
            }
        }
        None
    };
    if workload_type == "trainer" {
        let train_node = pick(&["scripts/train.ts", "train.js", "src/train.js"]);
        let train_py = pick(&["train.py", "src/train.py"]);
        return match lang {
            "node" => {
                if let Some(file) = train_node {
                    if file == "scripts/train.ts" {
                        (
                            "oven/bun:1".to_string(),
                            "[\"bun\",\"./scripts/train.ts\"]".to_string(),
                            None,
                        )
                    } else {
                        (
                            "node:20-bullseye".to_string(),
                            format!("[\"node\",\"./{}\"]", file),
                            None,
                        )
                    }
                } else {
                    (
                        "node:20-bullseye".to_string(),
                        "[\"node\",\"./train.js\"]".to_string(),
                        Some("trainer workload assumes ./train.js entrypoint; update runtime.harness.command if needed".to_string()),
                    )
                }
            }
            "python" => {
                let has_train_py = train_py.is_some();
                (
                    "python:3.11-slim".to_string(),
                    format!(
                        "[\"python3\",\"./{}\"]",
                        train_py.unwrap_or_else(|| "train.py".to_string())
                    ),
                    if !has_train_py {
                        Some(
                            "trainer workload assumes ./train.py entrypoint; update runtime.harness.command if needed"
                                .to_string(),
                        )
                    } else {
                        None
                    },
                )
            }
            "go" => (
                "golang:1.22".to_string(),
                "[\"./train\"]".to_string(),
                Some(
                    "trainer workload assumes ./train binary entrypoint; update runtime.harness.command if needed"
                        .to_string(),
                ),
            ),
            "rust" => (
                "rust:1.76".to_string(),
                "[\"./train\"]".to_string(),
                Some(
                    "trainer workload assumes ./train binary entrypoint; update runtime.harness.command if needed"
                        .to_string(),
                ),
            ),
            _ => (
                "ubuntu:22.04".to_string(),
                "[\"./train\"]".to_string(),
                Some(
                    "trainer workload assumes ./train entrypoint; update runtime.harness.command if needed"
                        .to_string(),
                ),
            ),
        };
    }
    let node_file = pick(&[
        "agentlab/harness.js",
        "harness.js",
        "agentlab_demo_harness.js",
        "src/harness.js",
    ]);
    let py_file = pick(&["harness.py", "main.py", "src/harness.py"]);
    match lang {
        "node" => {
            if let Some(file) = node_file.clone() {
                if file == "agentlab_demo_harness.js" {
                    (
                        "node:20-bullseye".to_string(),
                        "[\"node\",\"./agentlab_demo_harness.js\",\"run\"]".to_string(),
                        None,
                    )
                } else {
                    (
                        "node:20-bullseye".to_string(),
                        format!("[\"node\",\"./{}\",\"run\"]", file),
                        Some("assumed 'run' subcommand; update runtime.harness.command if your CLI differs".to_string()),
                    )
                }
            } else {
                (
                    "node:20-bullseye".to_string(),
                    "[\"node\",\"./harness.js\",\"run\"]".to_string(),
                    Some("runtime.harness.command set to ./harness.js; update if your harness file differs".to_string()),
                )
            }
        }
        "python" => (
            "python:3.11-slim".to_string(),
            format!(
                "[\"python3\",\"./{}\",\"run\"]",
                py_file.clone().unwrap_or_else(|| "harness.py".to_string())
            ),
            if py_file.is_none() {
                Some("runtime.harness.command set to ./harness.py; update if your harness file differs".to_string())
            } else {
                None
            },
        ),
        "go" => (
            "golang:1.22".to_string(),
            "[\"./harness\",\"run\"]".to_string(),
            Some(
                "runtime.harness.command set to ./harness; update to your built binary path"
                    .to_string(),
            ),
        ),
        "rust" => (
            "rust:1.76".to_string(),
            "[\"./harness\",\"run\"]".to_string(),
            Some(
                "runtime.harness.command set to ./harness; update to your built binary path"
                    .to_string(),
            ),
        ),
        _ => (
            "ubuntu:22.04".to_string(),
            "[\"./harness\",\"run\"]".to_string(),
            Some(
                "runtime.harness.command set to ./harness; update to your actual entrypoint"
                    .to_string(),
            ),
        ),
    }
}

fn write_knob_files(
    manifest: &std::path::Path,
    overrides: &std::path::Path,
    force: bool,
) -> Result<()> {
    if let Some(parent) = manifest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if let Some(parent) = overrides.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if force || !manifest.exists() {
        let manifest_template = r#"{
  "schema_version": "knob_manifest_v1",
  "knobs": [
    {
      "id": "design.replications",
      "label": "Replications",
      "json_pointer": "/design/replications",
      "type": "integer",
      "minimum": 1,
      "maximum": 100,
      "role": "core",
      "scientific_role": "control",
      "autotune": { "enabled": true, "requires_human_approval": false }
    },
    {
      "id": "dataset.limit",
      "label": "Dataset Limit",
      "json_pointer": "/dataset/limit",
      "type": "integer",
      "minimum": 1,
      "role": "core",
      "scientific_role": "control"
    },
    {
      "id": "runtime.network.mode",
      "label": "Network Mode",
      "json_pointer": "/runtime/network/mode",
      "type": "string",
      "options": ["none", "full", "allowlist_enforced"],
      "role": "infra",
      "scientific_role": "invariant"
    },
    {
      "id": "runtime.harness.integration_level",
      "label": "Integration Level",
      "json_pointer": "/runtime/harness/integration_level",
      "type": "string",
      "options": ["cli_basic", "cli_events", "otel", "sdk_control", "sdk_full"],
      "role": "harness",
      "scientific_role": "confound"
    },
    {
      "id": "runtime.harness.command",
      "label": "Harness Command",
      "json_pointer": "/runtime/harness/command",
      "type": "array",
      "role": "harness",
      "scientific_role": "treatment",
      "autotune": { "enabled": false, "requires_human_approval": true }
    }
  ]
}
"#;
        std::fs::write(manifest, manifest_template)?;
    }

    if force || !overrides.exists() {
        let manifest_rel = if manifest.is_absolute() {
            if let Ok(cwd) = std::env::current_dir() {
                if let Ok(rel) = manifest.strip_prefix(&cwd) {
                    rel.to_string_lossy().to_string()
                } else {
                    manifest.display().to_string()
                }
            } else {
                manifest.display().to_string()
            }
        } else {
            manifest.to_string_lossy().to_string()
        };
        let overrides_template = format!(
            "{{\n  \"schema_version\": \"experiment_overrides_v1\",\n  \"manifest_path\": \"{}\",\n  \"values\": {{\n    \"design.replications\": 1\n  }}\n}}\n",
            manifest_rel.replace('\\', "\\\\")
        );
        std::fs::write(overrides, overrides_template)?;
    }

    Ok(())
}
