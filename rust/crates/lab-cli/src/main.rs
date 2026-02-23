use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "lab", version = "0.3.0", about = "AgentLab Rust CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ExecutorArg {
    #[value(name = "local_docker")]
    LocalDocker,
    #[value(name = "local_process")]
    LocalProcess,
    #[value(name = "remote")]
    Remote,
}

impl From<ExecutorArg> for lab_runner::ExecutorKind {
    fn from(value: ExecutorArg) -> Self {
        match value {
            ExecutorArg::LocalDocker => lab_runner::ExecutorKind::LocalDocker,
            ExecutorArg::LocalProcess => lab_runner::ExecutorKind::LocalProcess,
            ExecutorArg::Remote => lab_runner::ExecutorKind::Remote,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum MaterializeArg {
    #[value(name = "none")]
    None,
    #[value(name = "metadata_only")]
    MetadataOnly,
    #[value(name = "outputs_only")]
    OutputsOnly,
    #[value(name = "full")]
    Full,
}

impl From<MaterializeArg> for lab_runner::MaterializationMode {
    fn from(value: MaterializeArg) -> Self {
        match value {
            MaterializeArg::None => lab_runner::MaterializationMode::None,
            MaterializeArg::MetadataOnly => lab_runner::MaterializationMode::MetadataOnly,
            MaterializeArg::OutputsOnly => lab_runner::MaterializationMode::OutputsOnly,
            MaterializeArg::Full => lab_runner::MaterializationMode::Full,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum InitProfileArg {
    #[value(name = "agent-eval")]
    AgentEval,
    #[value(name = "ab-test")]
    AbTest,
    #[value(name = "sweep")]
    Sweep,
    #[value(name = "regression")]
    Regression,
    #[value(name = "local-dev")]
    LocalDev,
}

#[derive(Subcommand)]
enum Commands {
    Run {
        experiment: PathBuf,
        #[arg(long)]
        container: bool,
        #[arg(long, value_enum)]
        executor: Option<ExecutorArg>,
        #[arg(long, value_enum)]
        materialize: Option<MaterializeArg>,
        #[arg(long)]
        remote_endpoint: Option<String>,
        #[arg(long)]
        remote_token_env: Option<String>,
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
        overrides: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    Replay {
        #[arg(long)]
        run_dir: PathBuf,
        #[arg(long)]
        trial_id: String,
        #[arg(long)]
        strict: bool,
        #[arg(long)]
        json: bool,
    },
    Fork {
        #[arg(long)]
        run_dir: PathBuf,
        #[arg(long)]
        from_trial: String,
        #[arg(long)]
        at: String,
        #[arg(long = "set")]
        set_values: Vec<String>,
        #[arg(long)]
        strict: bool,
        #[arg(long)]
        json: bool,
    },
    Pause {
        #[arg(long)]
        run_dir: PathBuf,
        #[arg(long)]
        trial_id: Option<String>,
        #[arg(long)]
        label: Option<String>,
        #[arg(long, default_value_t = 60)]
        timeout_seconds: u64,
        #[arg(long)]
        json: bool,
    },
    #[command(about = "Resume a paused trial by forking from its checkpoint state")]
    Resume {
        #[arg(long)]
        run_dir: PathBuf,
        #[arg(long)]
        trial_id: Option<String>,
        #[arg(long)]
        label: Option<String>,
        #[arg(long = "set")]
        set_values: Vec<String>,
        #[arg(long)]
        strict: bool,
        #[arg(long)]
        json: bool,
    },
    #[command(about = "Continue a terminal run from the next schedule slot")]
    Continue {
        #[arg(long)]
        run_dir: PathBuf,
        #[arg(long)]
        json: bool,
    },
    #[command(about = "Recover a durable run after stale owner crash/interruption")]
    Recover {
        #[arg(long)]
        run_dir: PathBuf,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        json: bool,
    },
    #[command(about = "Kill a running or paused experiment immediately")]
    Kill {
        run: String,
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
    Views {
        run: String,
        view: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
    #[command(about = "Live refresh for a view (defaults to run_progress)")]
    ViewsLive {
        run: String,
        view: Option<String>,
        #[arg(long, default_value_t = 2)]
        interval_seconds: u64,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        once: bool,
        #[arg(long)]
        no_clear: bool,
    },
    Query {
        run: String,
        sql: String,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
    #[command(about = "Live per-task scoreboard grouped by variant")]
    Scoreboard {
        run: String,
        #[arg(long, default_value_t = 2)]
        interval_seconds: u64,
        #[arg(long, default_value_t = 8)]
        metric_limit: usize,
        #[arg(long)]
        once: bool,
        #[arg(long)]
        no_clear: bool,
    },
    Trend {
        experiment_id: String,
        #[arg(long)]
        task: Option<String>,
        #[arg(long)]
        variant: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
    },
    Runs {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
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
    Init {
        #[arg(long)]
        in_place: bool,
        #[arg(long)]
        force: bool,
        #[arg(long, value_enum)]
        profile: Option<InitProfileArg>,
    },
    Preflight {
        experiment: PathBuf,
        #[arg(long)]
        overrides: Option<PathBuf>,
        #[arg(long)]
        json: bool,
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
            executor,
            materialize,
            remote_endpoint,
            remote_token_env,
            overrides,
            json,
        } => {
            let summary =
                lab_runner::describe_experiment_with_overrides(&experiment, overrides.as_deref())?;
            let execution = lab_runner::RunExecutionOptions {
                executor: executor.map(Into::into),
                materialize: materialize.map(Into::into),
                remote_endpoint,
                remote_token_env,
            };
            let result = lab_runner::run_experiment_with_options_and_overrides(
                &experiment,
                container,
                overrides.as_deref(),
                execution.clone(),
            )?;
            if json {
                let post_run = try_post_run_stats_json(&result.run_dir);
                return Ok(Some(json!({
                    "ok": true,
                    "command": "run",
                    "summary": summary_to_json(&summary),
                    "run": run_result_to_json(&result),
                    "artifacts": run_artifacts_to_json(&result),
                    "container": container,
                    "executor": execution.executor.map(|e| e.as_str()),
                    "materialize": execution.materialize.map(|m| m.as_str()),
                    "remote_endpoint": execution.remote_endpoint,
                    "remote_token_env": execution.remote_token_env,
                    "post_run_stats": post_run
                })));
            }
            print_summary(&summary);
            println!("run_id: {}", result.run_id);
            println!("run_dir: {}", result.run_dir.display());
            try_print_post_run_stats(&result.run_dir, &result.run_id);
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
                let post_run = try_post_run_stats_json(&result.run_dir);
                return Ok(Some(json!({
                    "ok": true,
                    "command": "run-dev",
                    "summary": summary_to_json(&summary),
                    "run": run_result_to_json(&result),
                    "artifacts": run_artifacts_to_json(&result),
                    "dev_setup": setup_for_json,
                    "dev_network_mode": "full",
                    "post_run_stats": post_run
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
            try_print_post_run_stats(&result.run_dir, &result.run_id);
        }
        Commands::RunExperiment {
            experiment,
            overrides,
            json,
        } => {
            let summary =
                lab_runner::describe_experiment_with_overrides(&experiment, overrides.as_deref())?;
            let result = lab_runner::run_experiment_strict_with_overrides(
                &experiment,
                overrides.as_deref(),
            )?;
            if json {
                let post_run = try_post_run_stats_json(&result.run_dir);
                return Ok(Some(json!({
                    "ok": true,
                    "command": "run-experiment",
                    "summary": summary_to_json(&summary),
                    "run": run_result_to_json(&result),
                    "experiment_network_requirement": "none",
                    "post_run_stats": post_run
                })));
            }
            print_summary(&summary);
            println!("experiment_network_requirement: none");
            println!("run_id: {}", result.run_id);
            println!("run_dir: {}", result.run_dir.display());
            try_print_post_run_stats(&result.run_dir, &result.run_id);
        }
        Commands::Replay {
            run_dir,
            trial_id,
            strict,
            json,
        } => {
            let result = lab_runner::replay_trial(&run_dir, &trial_id, strict)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "replay",
                    "replay": replay_result_to_json(&result),
                })));
            }
            println!("replay_id: {}", result.replay_id);
            println!("replay_dir: {}", result.replay_dir.display());
            println!("parent_trial_id: {}", result.parent_trial_id);
            println!("strict: {}", result.strict);
            println!("replay_grade: {}", result.replay_grade);
            println!("harness_status: {}", result.harness_status);
        }
        Commands::Fork {
            run_dir,
            from_trial,
            at,
            set_values,
            strict,
            json,
        } => {
            let set_bindings = parse_set_bindings(&set_values)?;
            let result = lab_runner::fork_trial(&run_dir, &from_trial, &at, &set_bindings, strict)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "fork",
                    "fork": fork_result_to_json(&result),
                })));
            }
            println!("fork_id: {}", result.fork_id);
            println!("fork_dir: {}", result.fork_dir.display());
            println!("parent_trial_id: {}", result.parent_trial_id);
            println!("selector: {}", result.selector);
            println!("strict: {}", result.strict);
            println!(
                "source_checkpoint: {}",
                result.source_checkpoint.as_deref().unwrap_or("none")
            );
            println!("fallback_mode: {}", result.fallback_mode);
            println!("replay_grade: {}", result.replay_grade);
            println!("harness_status: {}", result.harness_status);
        }
        Commands::Pause {
            run_dir,
            trial_id,
            label,
            timeout_seconds,
            json,
        } => {
            let result = lab_runner::pause_run(
                &run_dir,
                trial_id.as_deref(),
                label.as_deref(),
                timeout_seconds,
            )?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "pause",
                    "pause": pause_result_to_json(&result),
                })));
            }
            println!("run_id: {}", result.run_id);
            println!("trial_id: {}", result.trial_id);
            println!("label: {}", result.label);
            println!("checkpoint_acked: {}", result.checkpoint_acked);
            println!("stop_acked: {}", result.stop_acked);
        }
        Commands::Resume {
            run_dir,
            trial_id,
            label,
            set_values,
            strict,
            json,
        } => {
            let set_bindings = parse_set_bindings(&set_values)?;
            let result = lab_runner::resume_trial(
                &run_dir,
                trial_id.as_deref(),
                label.as_deref(),
                &set_bindings,
                strict,
            )?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "resume",
                    "resume": resume_result_to_json(&result),
                })));
            }
            println!("trial_id: {}", result.trial_id);
            println!("selector: {}", result.selector);
            println!("fork_id: {}", result.fork.fork_id);
            println!("fork_dir: {}", result.fork.fork_dir.display());
            println!("replay_grade: {}", result.fork.replay_grade);
            println!("harness_status: {}", result.fork.harness_status);
        }
        Commands::Continue { run_dir, json } => {
            let result = lab_runner::continue_run(&run_dir)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "continue",
                    "run": run_result_to_json(&result),
                })));
            }
            println!("run_id: {}", result.run_id);
            println!("run_dir: {}", result.run_dir.display());
        }
        Commands::Recover {
            run_dir,
            force,
            json,
        } => {
            let result = lab_runner::recover_run(&run_dir, force)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "recover",
                    "recover": recover_result_to_json(&result),
                })));
            }
            println!("run_id: {}", result.run_id);
            println!("previous_status: {}", result.previous_status);
            println!("recovered_status: {}", result.recovered_status);
            println!(
                "rewound_to_schedule_idx: {}",
                result.rewound_to_schedule_idx
            );
            println!("active_trials_released: {}", result.active_trials_released);
            println!(
                "committed_slots_verified: {}",
                result.committed_slots_verified
            );
            if result.notes.is_empty() {
                println!("notes: (none)");
            } else {
                println!("notes: {}", result.notes.join(" | "));
            }
        }
        Commands::Kill { run, json } => {
            let run_dir = resolve_run_dir_arg(&run)?;
            let result = lab_runner::kill_run(&run_dir)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "kill",
                    "run_id": result.run_id,
                    "run_dir": result.run_dir.display().to_string(),
                    "previous_status": result.previous_status,
                    "killed_trials": result.killed_trials,
                })));
            }
            println!("killed: {}", result.run_id);
            println!("run_dir: {}", result.run_dir.display());
            println!("previous_status: {}", result.previous_status);
            if result.killed_trials.is_empty() {
                println!("killed_trials: (none active)");
            } else {
                println!("killed_trials: {}", result.killed_trials.join(", "));
            }
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
        Commands::Views {
            run,
            view,
            all,
            limit,
            json,
            csv,
        } => {
            if json && csv {
                return Err(anyhow::anyhow!("--json and --csv are mutually exclusive"));
            }
            if all && view.is_some() {
                return Err(anyhow::anyhow!(
                    "--all cannot be combined with a specific view name"
                ));
            }
            let run_dir = resolve_run_dir_arg(&run)?;
            let view_set = lab_analysis::run_view_set(&run_dir)?.as_str().to_string();
            let view_names = lab_analysis::list_views(&run_dir)?;

            if all {
                if json {
                    let mut payload = serde_json::Map::new();
                    for name in &view_names {
                        let table = lab_analysis::query_view(&run_dir, name, limit)?;
                        payload.insert(name.clone(), query_table_to_json(&table));
                    }
                    return Ok(Some(json!({
                        "ok": true,
                        "command": "views",
                        "run_dir": run_dir.display().to_string(),
                        "view_set": view_set,
                        "views": Value::Object(payload),
                    })));
                }
                if csv {
                    for name in &view_names {
                        let table = lab_analysis::query_view(&run_dir, name, limit)?;
                        print_query_table_csv(&table);
                    }
                    return Ok(None);
                }
                println!("run_dir: {}", run_dir.display());
                println!("view_set: {}", view_set);
                for name in &view_names {
                    println!("\n== {} ==", name);
                    let table = lab_analysis::query_view(&run_dir, name, limit)?;
                    print_query_table(&table);
                }
                return Ok(None);
            }

            if let Some(view_name) = view {
                let normalized = normalize_view_name(&view_name);
                let table = lab_analysis::query_view(&run_dir, &normalized, limit)?;
                if json {
                    return Ok(Some(json!({
                        "ok": true,
                        "command": "views",
                        "run_dir": run_dir.display().to_string(),
                        "view_set": view_set,
                        "view": normalized,
                        "result": query_table_to_json(&table),
                    })));
                }
                if csv {
                    print_query_table_csv(&table);
                    return Ok(None);
                }
                println!("run_dir: {}", run_dir.display());
                println!("view_set: {}", view_set);
                println!("view: {}", normalized);
                print_query_table(&table);
                return Ok(None);
            }

            // View listing (no specific view selected)
            let listing_table = lab_analysis::QueryTable {
                columns: vec!["view_name".to_string()],
                rows: view_names
                    .iter()
                    .map(|n| vec![Value::String(n.clone())])
                    .collect(),
            };
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "views",
                    "run_dir": run_dir.display().to_string(),
                    "view_set": view_set,
                    "available_views": view_names,
                })));
            }
            if csv {
                print_query_table_csv(&listing_table);
                return Ok(None);
            }
            println!("run_dir: {}", run_dir.display());
            println!("view_set: {}", view_set);
            for name in view_names {
                println!("{}", name);
            }
        }
        Commands::ViewsLive {
            run,
            view,
            interval_seconds,
            limit,
            once,
            no_clear,
        } => {
            let run_dir = resolve_run_dir_arg(&run)?;
            let sleep_interval = Duration::from_secs(interval_seconds.max(1));
            let resolved_view = view
                .as_deref()
                .map(normalize_view_name)
                .unwrap_or_else(|| "run_progress".to_string());
            let resolved_limit = limit.max(1);
            loop {
                let table = lab_analysis::query_view(&run_dir, &resolved_view, resolved_limit)?;
                if !no_clear {
                    print!("\x1B[2J\x1B[H");
                    let _ = std::io::stdout().flush();
                }
                println!("run_dir: {}", run_dir.display());
                println!("status: {}", read_run_status(&run_dir));
                println!("updated_unix_s: {}", unix_now_seconds());
                println!("view: {}", resolved_view);
                println!("limit: {}", resolved_limit);
                println!(
                    "refresh_interval_seconds: {} (Ctrl-C to stop)",
                    sleep_interval.as_secs()
                );
                println!();
                print_query_table(&table);

                if once {
                    break;
                }
                std::thread::sleep(sleep_interval);
            }
        }
        Commands::Query {
            run,
            sql,
            json,
            csv,
        } => {
            if json && csv {
                return Err(anyhow::anyhow!("--json and --csv are mutually exclusive"));
            }
            let run_dir = resolve_run_dir_arg(&run)?;
            let table = lab_analysis::query_run(&run_dir, &sql)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "query",
                    "run_dir": run_dir.display().to_string(),
                    "sql": sql,
                    "result": query_table_to_json(&table),
                })));
            }
            if csv {
                print_query_table_csv(&table);
                return Ok(None);
            }
            print_query_table(&table);
        }
        Commands::Scoreboard {
            run,
            interval_seconds,
            metric_limit,
            once,
            no_clear,
        } => {
            let run_dir = resolve_run_dir_arg(&run)?;
            let sleep_interval = Duration::from_secs(interval_seconds.max(1));
            loop {
                let table = build_live_scoreboard_table(&run_dir, metric_limit)?;
                let variants = scoreboard_variant_ids(&table);
                if !no_clear {
                    print!("\x1B[2J\x1B[H");
                    let _ = std::io::stdout().flush();
                }
                println!("run_dir: {}", run_dir.display());
                println!("status: {}", read_run_status(&run_dir));
                println!("updated_unix_s: {}", unix_now_seconds());
                if variants.is_empty() {
                    println!("variants: (none yet)");
                } else {
                    println!("variants: {}", variants.join(", "));
                }
                println!(
                    "refresh_interval_seconds: {} (Ctrl-C to stop)",
                    sleep_interval.as_secs()
                );
                println!();
                print_scoreboard_grouped_by_variant(&table);

                if once {
                    break;
                }
                std::thread::sleep(sleep_interval);
            }
        }
        Commands::Trend {
            experiment_id,
            task,
            variant,
            json,
            csv,
        } => {
            if json && csv {
                return Err(anyhow::anyhow!("--json and --csv are mutually exclusive"));
            }
            let project_root = resolve_project_root(std::env::current_dir()?.as_path());
            let table = lab_analysis::query_trend(
                &project_root,
                &experiment_id,
                task.as_deref(),
                variant.as_deref(),
            )?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "trend",
                    "project_root": project_root.display().to_string(),
                    "experiment_id": experiment_id,
                    "task": task,
                    "variant": variant,
                    "result": query_table_to_json(&table),
                })));
            }
            if csv {
                print_query_table_csv(&table);
                return Ok(None);
            }
            println!("project_root: {}", project_root.display());
            println!("experiment_id: {}", experiment_id);
            if let Some(task_id) = task {
                println!("task: {}", task_id);
            }
            if let Some(variant_id) = variant {
                println!("variant: {}", variant_id);
            }
            print_query_table(&table);
        }
        Commands::Runs { json, csv } => {
            if json && csv {
                return Err(anyhow::anyhow!("--json and --csv are mutually exclusive"));
            }
            let project_root = resolve_project_root(std::env::current_dir()?.as_path());
            let table = build_runs_table(&project_root)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "runs",
                    "project_root": project_root.display().to_string(),
                    "result": query_table_to_json(&table),
                })));
            }
            if csv {
                print_query_table_csv(&table);
                return Ok(None);
            }
            print_query_table(&table);
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
        Commands::Init {
            in_place,
            force,
            profile,
        } => {
            let Some(profile) = profile else {
                println!("available profiles:");
                println!("  - agent-eval  : single-variant agent evaluation in container mode");
                println!("  - ab-test     : baseline vs treatment paired comparison");
                println!("  - sweep       : independent parameter sweep over variant_plan");
                println!("  - regression  : fixed-suite pass-rate tracking over time");
                println!("  - local-dev   : fast local iteration (single worker)");
                println!("usage: lab init --profile <name>");
                return Ok(None);
            };

            let cwd = std::env::current_dir()?;
            let root = cwd;
            let lab_dir = root.join(".lab");
            std::fs::create_dir_all(&lab_dir)?;

            let exp_path = if in_place {
                root.join("experiment.yaml")
            } else {
                lab_dir.join("experiment.yaml")
            };

            if !force && exp_path.exists() {
                return Err(anyhow::anyhow!(format!(
                    "init file already exists (use --force): {}",
                    exp_path.display()
                )));
            }

            let exp_yaml = init_profile_template(profile);
            std::fs::write(&exp_path, exp_yaml)?;

            let exp_show = exp_path.strip_prefix(&root).unwrap_or(&exp_path).display();
            println!("wrote: {}", exp_show);
            println!(
                "next: edit {} (fill in dataset path + runtime command/image)",
                exp_show
            );
            println!("next: lab describe {}", exp_show);
        }
        Commands::Preflight {
            experiment,
            overrides,
            json,
        } => {
            let report = lab_runner::preflight_experiment(&experiment, overrides.as_deref())?;
            if json {
                return Ok(Some(json!({
                    "ok": report.passed,
                    "command": "preflight",
                    "checks": report.checks.iter().map(|c| json!({
                        "name": c.name,
                        "passed": c.passed,
                        "severity": match c.severity {
                            lab_runner::PreflightSeverity::Error => "error",
                            lab_runner::PreflightSeverity::Warning => "warning",
                        },
                        "message": c.message,
                    })).collect::<Vec<_>>()
                })));
            }
            print_preflight_report(&report);
            if !report.passed {
                std::process::exit(1);
            }
        }
        Commands::Clean { init, runs } => {
            let root = std::env::current_dir()?;
            let lab_dir = root.join(".lab");
            if init {
                let candidates = vec![
                    root.join("experiment.yaml"),
                    lab_dir.join("experiment.yaml"),
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

fn init_profile_template(profile: InitProfileArg) -> &'static str {
    match profile {
        InitProfileArg::AgentEval => {
            "version: \"0.5\"
experiment:
  id: my_eval
  name: My Agent Evaluation
  workload_type: agent_runtime
dataset:
  suite_id: local_suite
  provider: local_jsonl
  path: tasks.jsonl
  schema_version: task_jsonl_v1
  split_id: dev
  limit: 50
design:
  sanitization_profile: hermetic_functional
  comparison: paired
  replications: 3
  random_seed: 42
  shuffle_tasks: true
  max_concurrency: 1
baseline:
  variant_id: control
  bindings: {}
variant_plan: []
runtime:
  agent:
    command: [python, harness.py]
    image: my-harness:latest
  policy:
    timeout_ms: 300000
    network:
      mode: none
      allowed_hosts: []
    sandbox:
      mode: container
      resources:
        cpu_count: 2
        memory_mb: 2048
validity:
  fail_on_state_leak: true
  fail_on_profile_invariant_violation: true
"
        }
        InitProfileArg::AbTest => {
            "version: \"0.5\"
experiment:
  id: my_ab_test
  name: Baseline vs Treatment
  workload_type: agent_runtime
dataset:
  suite_id: local_suite
  provider: local_jsonl
  path: tasks.jsonl
  schema_version: task_jsonl_v1
  split_id: dev
  limit: 100
design:
  sanitization_profile: hermetic_functional
  comparison: paired
  replications: 5
  random_seed: 42
  shuffle_tasks: true
  max_concurrency: 1
baseline:
  variant_id: control
  bindings: {}
variant_plan:
  - variant_id: treatment
    bindings:
      model: claude-4
runtime:
  agent:
    command: [python, harness.py]
    image: my-harness:latest
  policy:
    timeout_ms: 300000
    network:
      mode: none
      allowed_hosts: []
    sandbox:
      mode: container
      resources:
        cpu_count: 2
        memory_mb: 2048
validity:
  fail_on_state_leak: true
  fail_on_profile_invariant_violation: true
"
        }
        InitProfileArg::Sweep => {
            "version: \"0.5\"
experiment:
  id: my_sweep
  name: Parameter Sweep
  workload_type: agent_runtime
dataset:
  suite_id: local_suite
  provider: local_jsonl
  path: tasks.jsonl
  schema_version: task_jsonl_v1
  split_id: dev
  limit: 100
design:
  sanitization_profile: hermetic_functional
  comparison: unpaired
  replications: 1
  random_seed: 42
  shuffle_tasks: true
  max_concurrency: 1
baseline:
  variant_id: control
  bindings: {}
variant_plan:
  - variant_id: t07
    bindings:
      temperature: 0.7
  - variant_id: t09
    bindings:
      temperature: 0.9
runtime:
  agent:
    command: [python, harness.py]
    image: my-harness:latest
  policy:
    timeout_ms: 300000
    network:
      mode: none
      allowed_hosts: []
    sandbox:
      mode: container
      resources:
        cpu_count: 2
        memory_mb: 2048
validity:
  fail_on_state_leak: true
  fail_on_profile_invariant_violation: true
"
        }
        InitProfileArg::Regression => {
            "version: \"0.5\"
experiment:
  id: my_regression
  name: Regression Tracking
  workload_type: agent_runtime
dataset:
  suite_id: local_suite
  provider: local_jsonl
  path: tasks.jsonl
  schema_version: task_jsonl_v1
  split_id: dev
  limit: 50
design:
  sanitization_profile: hermetic_functional
  comparison: paired
  replications: 3
  random_seed: 42
  shuffle_tasks: true
  max_concurrency: 1
baseline:
  variant_id: control
  bindings: {}
variant_plan: []
runtime:
  agent:
    command: [python, harness.py]
    image: my-harness:latest
  policy:
    timeout_ms: 300000
    network:
      mode: none
      allowed_hosts: []
    sandbox:
      mode: container
      resources:
        cpu_count: 2
        memory_mb: 2048
validity:
  fail_on_state_leak: true
  fail_on_profile_invariant_violation: true
"
        }
        InitProfileArg::LocalDev => {
            "version: \"0.5\"
experiment:
  id: my_local_dev
  name: Local Development
  workload_type: agent_runtime
dataset:
  suite_id: local_suite
  provider: local_jsonl
  path: tasks.jsonl
  schema_version: task_jsonl_v1
  split_id: dev
  limit: 10
design:
  sanitization_profile: hermetic_functional
  comparison: paired
  replications: 1
  random_seed: 42
  shuffle_tasks: true
  max_concurrency: 1
baseline:
  variant_id: control
  bindings: {}
variant_plan: []
runtime:
  agent:
    command: [python, harness.py]
  policy:
    timeout_ms: 120000
    network:
      mode: full
      allowed_hosts: []
    sandbox:
      mode: local
validity:
  fail_on_state_leak: true
  fail_on_profile_invariant_violation: true
"
        }
    }
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
        | Commands::Replay { json, .. }
        | Commands::Fork { json, .. }
        | Commands::Pause { json, .. }
        | Commands::Resume { json, .. }
        | Commands::Continue { json, .. }
        | Commands::Recover { json, .. }
        | Commands::Kill { json, .. }
        | Commands::Describe { json, .. }
        | Commands::Views { json, .. }
        | Commands::Query { json, .. }
        | Commands::Trend { json, .. }
        | Commands::Runs { json, .. }
        | Commands::KnobsValidate { json, .. }
        | Commands::SchemaValidate { json, .. }
        | Commands::HooksValidate { json, .. }
        | Commands::Publish { json, .. }
        | Commands::Preflight { json, .. } => *json,
        _ => false,
    }
}

fn run_result_to_json(result: &lab_runner::RunResult) -> Value {
    json!({
        "run_id": result.run_id,
        "run_dir": result.run_dir.display().to_string()
    })
}

fn run_artifacts_to_json(result: &lab_runner::RunResult) -> Value {
    let evidence = result.run_dir.join("evidence");
    let benchmark = result.run_dir.join("benchmark");
    let summary_path = benchmark.join("summary.json");
    json!({
        "evidence_records_path": evidence.join("evidence_records.jsonl").display().to_string(),
        "task_chain_states_path": evidence.join("task_chain_states.jsonl").display().to_string(),
        "benchmark_dir": benchmark.display().to_string(),
        "benchmark_summary_path": if summary_path.exists() {
            Some(summary_path.display().to_string())
        } else {
            None::<String>
        }
    })
}

fn replay_result_to_json(result: &lab_runner::ReplayResult) -> Value {
    json!({
        "replay_id": result.replay_id,
        "replay_dir": result.replay_dir.display().to_string(),
        "parent_trial_id": result.parent_trial_id,
        "strict": result.strict,
        "replay_grade": result.replay_grade,
        "harness_status": result.harness_status,
    })
}

fn fork_result_to_json(result: &lab_runner::ForkResult) -> Value {
    json!({
        "fork_id": result.fork_id,
        "fork_dir": result.fork_dir.display().to_string(),
        "parent_trial_id": result.parent_trial_id,
        "selector": result.selector,
        "strict": result.strict,
        "source_checkpoint": result.source_checkpoint,
        "fallback_mode": result.fallback_mode,
        "replay_grade": result.replay_grade,
        "harness_status": result.harness_status,
    })
}

fn pause_result_to_json(result: &lab_runner::PauseResult) -> Value {
    json!({
        "run_id": result.run_id,
        "trial_id": result.trial_id,
        "label": result.label,
        "checkpoint_acked": result.checkpoint_acked,
        "stop_acked": result.stop_acked,
    })
}

fn resume_result_to_json(result: &lab_runner::ResumeResult) -> Value {
    json!({
        "trial_id": result.trial_id,
        "selector": result.selector,
        "fork": fork_result_to_json(&result.fork),
    })
}

fn recover_result_to_json(result: &lab_runner::RecoverResult) -> Value {
    json!({
        "run_id": result.run_id,
        "previous_status": result.previous_status,
        "recovered_status": result.recovered_status,
        "rewound_to_schedule_idx": result.rewound_to_schedule_idx,
        "active_trials_released": result.active_trials_released,
        "committed_slots_verified": result.committed_slots_verified,
        "notes": result.notes,
    })
}

fn parse_set_bindings(values: &[String]) -> Result<BTreeMap<String, Value>> {
    let mut out = BTreeMap::new();
    for raw in values {
        let (key, val_raw) = raw
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!(format!("invalid --set '{}': expected k=v", raw)))?;
        if key.trim().is_empty() {
            return Err(anyhow::anyhow!(format!(
                "invalid --set '{}': key cannot be empty",
                raw
            )));
        }
        let parsed =
            serde_json::from_str::<Value>(val_raw).unwrap_or(Value::String(val_raw.to_string()));
        out.insert(key.to_string(), parsed);
    }
    Ok(out)
}

fn summary_to_json(summary: &lab_runner::ExperimentSummary) -> Value {
    json!({
        "experiment": summary.exp_id,
        "workload_type": summary.workload_type,
        "dataset": summary.dataset_path.display().to_string(),
        "tasks": summary.task_count,
        "replications": summary.replications,
        "variant_count": summary.variant_count,
        "total_trials": summary.total_trials,
        "agent_runtime": summary.agent_runtime_command,
        "image": summary.image,
        "network": summary.network_mode,
        "trajectory_path": summary.trajectory_path,
        "causal_extraction": summary.causal_extraction,
        "scheduling": summary.scheduling,
        "state_policy": summary.state_policy,
        "comparison": summary.comparison,
        "retry_max_attempts": summary.retry_max_attempts,
        "preflight_warnings": summary.preflight_warnings
    })
}

fn print_summary(summary: &lab_runner::ExperimentSummary) {
    println!("experiment: {}", summary.exp_id);
    println!("workload_type: {}", summary.workload_type);
    println!("dataset: {}", summary.dataset_path.display());
    println!("tasks: {}", summary.task_count);
    println!("replications: {}", summary.replications);
    println!("variant_count: {}", summary.variant_count);
    println!("total_trials: {}", summary.total_trials);
    println!("agent_runtime: {:?}", summary.agent_runtime_command);
    if let Some(image) = &summary.image {
        println!("image: {}", image);
    }
    println!("network: {}", summary.network_mode);
    if let Some(path) = &summary.trajectory_path {
        println!("trajectory_path: {}", path);
    }
    if let Some(mode) = &summary.causal_extraction {
        println!("causal_extraction: {}", mode);
    }
    if !summary.preflight_warnings.is_empty() {
        println!("preflight_warnings:");
        for w in &summary.preflight_warnings {
            println!("  - {}", w);
        }
    }
}

fn print_preflight_report(report: &lab_runner::PreflightReport) {
    for check in &report.checks {
        let icon = if check.passed {
            "PASS"
        } else {
            match check.severity {
                lab_runner::PreflightSeverity::Error => "FAIL",
                lab_runner::PreflightSeverity::Warning => "WARN",
            }
        };
        println!("[{}] {}: {}", icon, check.name, check.message);
    }
    if report.passed {
        println!("\npreflight: all checks passed");
    } else {
        println!("\npreflight: FAILED — resolve errors above before running");
    }
}

fn resolve_run_dir_arg(run: &str) -> Result<PathBuf> {
    let raw = PathBuf::from(run);
    if raw.exists() {
        return raw
            .canonicalize()
            .map_err(|_| anyhow::anyhow!(format!("run path not found: {}", raw.display())));
    }

    let cwd = std::env::current_dir()?;
    let from_cwd = cwd.join(".lab").join("runs").join(run);
    if from_cwd.exists() {
        return from_cwd
            .canonicalize()
            .map_err(|_| anyhow::anyhow!(format!("run path not found: {}", from_cwd.display())));
    }

    let project_root = resolve_project_root(cwd.as_path());
    let from_project = project_root.join(".lab").join("runs").join(run);
    if from_project.exists() {
        return from_project.canonicalize().map_err(|_| {
            anyhow::anyhow!(format!("run path not found: {}", from_project.display()))
        });
    }

    Err(anyhow::anyhow!(format!(
        "run '{}' not found (expected path or run id under .lab/runs)",
        run
    )))
}

fn resolve_project_root(start: &Path) -> PathBuf {
    let mut cur = Some(start);
    while let Some(path) = cur {
        if path.join(".lab").exists() {
            return path.to_path_buf();
        }
        cur = path.parent();
    }
    start.to_path_buf()
}

fn build_live_scoreboard_table(
    run_dir: &Path,
    metric_limit: usize,
) -> Result<lab_analysis::QueryTable> {
    let limit = metric_limit.max(1).min(32);
    let metric_names = fetch_scoreboard_metric_names(run_dir, limit)?;
    let sql = build_scoreboard_sql(&metric_names);
    lab_analysis::query_run(run_dir, &sql)
}

fn fetch_scoreboard_metric_names(run_dir: &Path, metric_limit: usize) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT metric_name
         FROM metrics_long m
         WHERE m.metric_name <> 'status_code'
           AND m.metric_name <> 'success'
           AND NOT EXISTS (
             SELECT 1
             FROM trials t
             WHERE t.primary_metric_name = m.metric_name
           )
         GROUP BY metric_name
         ORDER BY metric_name
         LIMIT {}",
        metric_limit
    );
    let table = lab_analysis::query_run(run_dir, &sql)?;
    let mut out = Vec::new();
    for row in table.rows {
        if let Some(name) = row.first().and_then(Value::as_str) {
            if !name.trim().is_empty() {
                out.push(name.to_string());
            }
        }
    }
    Ok(out)
}

fn build_scoreboard_sql(metric_names: &[String]) -> String {
    let mut columns = Vec::new();
    for metric_name in metric_names {
        let alias = format!("{}_mean", sanitize_scoreboard_alias(metric_name));
        columns.push(format!(
            "(SELECT round(m.mean_metric, 4)
             FROM metric_agg m
             WHERE m.variant_id = b.variant_id
               AND m.task_id = b.task_id
               AND m.metric_name = {}) AS {}",
            sql_string_literal(metric_name),
            sql_identifier(&alias)
        ));
    }
    let dynamic_cols = if columns.is_empty() {
        String::new()
    } else {
        format!(",\n    {}", columns.join(",\n    "))
    };
    format!(
        "WITH base AS (
            SELECT
                variant_id,
                task_id,
                count(*) AS n_trials,
                round(avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS success_rate,
                round(avg(try_cast(primary_metric_value AS DOUBLE)), 4) AS primary_metric_mean
            FROM trials
            GROUP BY variant_id, task_id
        ),
        metric_agg AS (
            SELECT
                variant_id,
                task_id,
                metric_name,
                avg(try_cast(metric_value AS DOUBLE)) AS mean_metric
            FROM metrics_long
            GROUP BY variant_id, task_id, metric_name
        )
        SELECT
            b.variant_id,
            b.task_id,
            b.n_trials,
            b.success_rate,
            b.primary_metric_mean{}
        FROM base b
        ORDER BY b.variant_id, b.task_id",
        dynamic_cols
    )
}

fn sanitize_scoreboard_alias(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn terminal_width() -> usize {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        let mut ws = MaybeUninit::<libc::winsize>::uninit();
        let ret = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, ws.as_mut_ptr()) };
        if ret == 0 {
            let ws = unsafe { ws.assume_init() };
            if ws.ws_col > 0 {
                return ws.ws_col as usize;
            }
        }
    }
    120
}

fn print_scoreboard_grouped_by_variant(table: &lab_analysis::QueryTable) {
    let term_w = terminal_width();

    let Some(variant_col_idx) = table.columns.iter().position(|c| c == "variant_id") else {
        print_scoreboard_table(table, term_w);
        return;
    };

    let mut per_variant: BTreeMap<String, Vec<Vec<Value>>> = BTreeMap::new();
    for row in &table.rows {
        let variant = row
            .get(variant_col_idx)
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let mut compact_row = Vec::with_capacity(row.len().saturating_sub(1));
        for (idx, cell) in row.iter().enumerate() {
            if idx != variant_col_idx {
                compact_row.push(cell.clone());
            }
        }
        per_variant.entry(variant).or_default().push(compact_row);
    }

    let mut columns = Vec::with_capacity(table.columns.len().saturating_sub(1));
    for (idx, col) in table.columns.iter().enumerate() {
        if idx != variant_col_idx {
            columns.push(col.clone());
        }
    }

    if per_variant.is_empty() {
        print_scoreboard_table(
            &lab_analysis::QueryTable {
                columns,
                rows: Vec::new(),
            },
            term_w,
        );
        return;
    }

    for (variant, rows) in per_variant {
        println!("== variant: {} ==", variant);
        print_scoreboard_table(
            &lab_analysis::QueryTable {
                columns: columns.clone(),
                rows,
            },
            term_w,
        );
        println!();
    }
}

fn scoreboard_variant_ids(table: &lab_analysis::QueryTable) -> Vec<String> {
    let Some(variant_col_idx) = table.columns.iter().position(|c| c == "variant_id") else {
        return Vec::new();
    };
    let mut variants = BTreeSet::new();
    for row in &table.rows {
        let variant = row
            .get(variant_col_idx)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or("unknown");
        variants.insert(variant.to_string());
    }
    variants.into_iter().collect()
}

/// Width-aware table printer for the scoreboard.
///
/// Strategy when the table exceeds terminal width:
/// 1. Switch from ` | ` separators to `  ` (saves 1 char per column boundary).
/// 2. Cap every column to a max width derived from available space.
/// 3. If still too wide, drop rightmost metric columns until it fits.
fn print_scoreboard_table(table: &lab_analysis::QueryTable, term_width: usize) {
    if table.columns.is_empty() {
        println!("(ok)");
        return;
    }

    let rendered_rows: Vec<Vec<String>> = table
        .rows
        .iter()
        .map(|row| row.iter().map(render_json_cell).collect::<Vec<String>>())
        .collect();

    let numeric_cols: Vec<bool> = (0..table.columns.len())
        .map(|col_idx| {
            let mut has_number = false;
            for row in &table.rows {
                match row.get(col_idx) {
                    Some(Value::Number(_)) => has_number = true,
                    Some(Value::Null) => {}
                    _ => return false,
                }
            }
            has_number
        })
        .collect();

    // Natural (uncapped) widths per column.
    let mut natural_widths: Vec<usize> = table.columns.iter().map(|c| c.chars().count()).collect();
    for row in &rendered_rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx < natural_widths.len() {
                natural_widths[idx] = natural_widths[idx].max(cell.chars().count());
            }
        }
    }

    // Determine how many columns we can actually show.
    // We protect the first few "core" columns (task_id, n_trials, success_rate, primary_metric_mean)
    // and drop metric columns from the right when space is tight.
    let core_count = table
        .columns
        .iter()
        .position(|c| c.ends_with("_mean") && c != "primary_metric_mean")
        .unwrap_or(table.columns.len());

    let (visible_count, sep, widths) =
        fit_columns_to_width(&natural_widths, core_count, term_width);

    // Render header
    let header: String = table.columns[..visible_count]
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            pad_cell(
                &truncate_cell(col, widths[idx]),
                widths[idx],
                numeric_cols.get(idx).copied().unwrap_or(false),
            )
        })
        .collect::<Vec<_>>()
        .join(sep);
    println!("{}", header);

    // Separator line
    let dash_join = if sep == " | " { "-+-" } else { "--" };
    let separator: String = widths[..visible_count]
        .iter()
        .map(|w| "-".repeat(*w))
        .collect::<Vec<_>>()
        .join(dash_join);
    println!("{}", separator);

    // Rows
    for row in &rendered_rows {
        let line: String = row[..visible_count.min(row.len())]
            .iter()
            .enumerate()
            .map(|(idx, cell)| {
                let ra = numeric_cols.get(idx).copied().unwrap_or(false);
                pad_cell(&truncate_cell(cell, widths[idx]), widths[idx], ra)
            })
            .collect::<Vec<_>>()
            .join(sep);
        println!("{}", line);
    }

    if visible_count < table.columns.len() {
        println!(
            "({} rows, {} cols hidden — widen terminal or use --metric-limit)",
            table.rows.len(),
            table.columns.len() - visible_count
        );
    } else {
        println!("({} rows)", table.rows.len());
    }
}

/// Pick separator style, cap column widths, and optionally drop trailing columns to fit `term_width`.
/// Returns (visible_column_count, separator_str, capped_widths).
fn fit_columns_to_width(
    natural_widths: &[usize],
    core_count: usize,
    term_width: usize,
) -> (usize, &'static str, Vec<usize>) {
    let n = natural_widths.len();
    if n == 0 {
        return (0, " | ", Vec::new());
    }

    // Try wide separators first (" | " = 3 chars), then compact ("  " = 2 chars).
    for sep in [" | ", "  "] {
        let sep_w = sep.len();
        // Try showing all columns, then progressively drop from the right (but never below core_count).
        let min_visible = core_count.min(n);
        for visible in (min_visible..=n).rev() {
            let sep_total = if visible > 1 {
                (visible - 1) * sep_w
            } else {
                0
            };
            let avail_for_cols = term_width.saturating_sub(sep_total);
            if avail_for_cols < visible {
                continue; // not even 1 char per column
            }
            let widths = cap_widths(&natural_widths[..visible], avail_for_cols);
            let total: usize = widths.iter().sum::<usize>() + sep_total;
            if total <= term_width {
                return (visible, sep, widths);
            }
        }
    }

    // Absolute fallback: show core columns, compact sep, hard-capped.
    let visible = core_count.min(n).max(1);
    let sep = "  ";
    let sep_total = if visible > 1 { (visible - 1) * 2 } else { 0 };
    let avail = term_width.saturating_sub(sep_total);
    let widths = cap_widths(&natural_widths[..visible], avail);
    (visible, sep, widths)
}

/// Distribute `budget` characters across columns, shrinking the widest ones first.
/// Minimum 4 chars per column.
fn cap_widths(natural: &[usize], budget: usize) -> Vec<usize> {
    let n = natural.len();
    if n == 0 {
        return Vec::new();
    }
    let total: usize = natural.iter().sum();
    if total <= budget {
        return natural.to_vec();
    }
    // Uniform cap: iteratively lower the ceiling until total fits.
    let min_w = 4_usize;
    // Binary search for the right cap.
    let mut lo = min_w;
    let mut hi = *natural.iter().max().unwrap_or(&budget);
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let used: usize = natural.iter().map(|&w| w.min(mid)).sum();
        if used <= budget {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    natural.iter().map(|&w| w.min(lo).max(min_w)).collect()
}

fn read_run_status(run_dir: &Path) -> String {
    let path = run_dir.join("runtime").join("run_control.json");
    if !path.exists() {
        return "unknown".to_string();
    }
    let raw = match std::fs::read_to_string(&path) {
        Ok(v) => v,
        Err(_) => return "unknown".to_string(),
    };
    let parsed: Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(_) => return "unknown".to_string(),
    };
    let status = parsed
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let active_trials = match parsed.get("active_trials").and_then(Value::as_object) {
        Some(active_trials) => active_trials,
        None => return status,
    };
    if active_trials.is_empty() {
        return status;
    }
    let mut workers = BTreeSet::new();
    for entry in active_trials.values() {
        if let Some(worker_id) = entry.get("worker_id").and_then(Value::as_str) {
            workers.insert(worker_id.to_string());
        }
    }
    if workers.is_empty() {
        return format!("{} (active_trials={})", status, active_trials.len());
    }
    if workers.len() <= 3 {
        let worker_list = workers.into_iter().collect::<Vec<_>>().join(",");
        return format!(
            "{} (active_trials={}, workers={})",
            status,
            active_trials.len(),
            worker_list
        );
    }
    format!(
        "{} (active_trials={}, workers={} total)",
        status,
        active_trials.len(),
        workers.len()
    )
}

fn unix_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn normalize_view_name(input: &str) -> String {
    let normalized = input.trim().replace('-', "_");
    match normalized.as_str() {
        "paired_diffs" => "paired_outcomes".to_string(),
        other => other.to_string(),
    }
}

fn query_table_to_json(table: &lab_analysis::QueryTable) -> Value {
    let mut objects = Vec::with_capacity(table.rows.len());
    for row in &table.rows {
        let mut obj = serde_json::Map::new();
        for (idx, column) in table.columns.iter().enumerate() {
            obj.insert(column.clone(), row.get(idx).cloned().unwrap_or(Value::Null));
        }
        objects.push(Value::Object(obj));
    }
    json!({
        "columns": table.columns,
        "rows": objects,
        "row_count": table.rows.len()
    })
}

fn print_query_table(table: &lab_analysis::QueryTable) {
    if table.columns.is_empty() {
        println!("(ok)");
        return;
    }

    let rendered_rows: Vec<Vec<String>> = table
        .rows
        .iter()
        .map(|row| row.iter().map(render_json_cell).collect::<Vec<String>>())
        .collect();

    let numeric_cols: Vec<bool> = (0..table.columns.len())
        .map(|col_idx| {
            let mut has_number = false;
            for row in &table.rows {
                match row.get(col_idx) {
                    Some(Value::Number(_)) => has_number = true,
                    Some(Value::Null) => {}
                    _ => return false,
                }
            }
            has_number
        })
        .collect();

    let mut widths: Vec<usize> = table.columns.iter().map(|c| c.chars().count()).collect();
    for row in &rendered_rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx < widths.len() {
                widths[idx] = widths[idx].max(cell.chars().count()).min(80);
            }
        }
    }

    let header = table
        .columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            pad_cell(
                col,
                widths[idx],
                numeric_cols.get(idx).copied().unwrap_or(false),
            )
        })
        .collect::<Vec<_>>()
        .join(" | ");
    println!("{}", header);
    let separator = widths
        .iter()
        .map(|w| "-".repeat(*w))
        .collect::<Vec<_>>()
        .join("-+-");
    println!("{}", separator);

    for row in rendered_rows {
        let line = row
            .iter()
            .enumerate()
            .map(|(idx, cell)| {
                let ra = numeric_cols.get(idx).copied().unwrap_or(false);
                if idx < widths.len() {
                    pad_cell(&truncate_cell(cell, widths[idx]), widths[idx], ra)
                } else {
                    cell.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(" | ");
        println!("{}", line);
    }
    println!("({} rows)", table.rows.len());
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn print_query_table_csv(table: &lab_analysis::QueryTable) {
    let header = table
        .columns
        .iter()
        .map(|c| csv_escape(c))
        .collect::<Vec<_>>()
        .join(",");
    println!("{}", header);
    for row in &table.rows {
        let line = row
            .iter()
            .map(|v| csv_escape(&render_json_cell(v)))
            .collect::<Vec<_>>()
            .join(",");
        println!("{}", line);
    }
}

fn render_json_cell(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn truncate_cell(value: &str, width: usize) -> String {
    let value_len = value.chars().count();
    if value_len <= width {
        return value.to_string();
    }
    if width <= 1 {
        return ".".to_string();
    }
    let mut out: String = value.chars().take(width - 1).collect();
    out.push('.');
    out
}

fn pad_cell(value: &str, width: usize, right_align: bool) -> String {
    let value_len = value.chars().count();
    if value_len >= width {
        value.to_string()
    } else if right_align {
        format!("{:padding$}{value}", "", padding = width - value_len)
    } else {
        format!("{value}{:padding$}", "", padding = width - value_len)
    }
}

fn try_print_post_run_stats(run_dir: &Path, run_id: &str) {
    let Some((view_set, table)) = try_load_headline(run_dir) else {
        return;
    };
    println!();
    println!("--- post-run stats ({}) ---", view_set.as_str());
    print_query_table(&table);
    println!();
    println!("next steps:");
    println!("  lab views {}", run_id);
    println!("  lab views {} --all", run_id);
    println!("  lab query {} \"SELECT * FROM trials\"", run_id);
}

fn try_post_run_stats_json(run_dir: &Path) -> Value {
    let Some((view_set, table)) = try_load_headline(run_dir) else {
        return Value::Null;
    };
    json!({
        "view_set": view_set.as_str(),
        "headline": query_table_to_json(&table),
    })
}

fn try_load_headline(run_dir: &Path) -> Option<(lab_analysis::ViewSet, lab_analysis::QueryTable)> {
    let view_set = lab_analysis::run_view_set(run_dir).ok()?;
    let headline = view_set.headline_view()?;
    let table = lab_analysis::query_view(run_dir, headline, 20).ok()?;
    Some((view_set, table))
}

fn build_runs_table(project_root: &Path) -> Result<lab_analysis::QueryTable> {
    let runs_dir = project_root.join(".lab").join("runs");
    if !runs_dir.exists() {
        return Ok(lab_analysis::QueryTable {
            columns: vec![
                "run_id".into(),
                "experiment".into(),
                "created_at".into(),
                "variants".into(),
                "pass_rate".into(),
            ],
            rows: vec![],
        });
    }

    struct RunRow {
        run_id: String,
        experiment: String,
        created_at: String,
        variants: usize,
        pass_rate: Option<f64>,
    }

    let mut entries: Vec<RunRow> = Vec::new();
    for entry in std::fs::read_dir(&runs_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let run_path = entry.path();

        // manifest.json → run_id, created_at
        let manifest_path = run_path.join("manifest.json");
        let (run_id, created_at) = if manifest_path.exists() {
            let raw = std::fs::read_to_string(&manifest_path).unwrap_or_default();
            let val: Value = serde_json::from_str(&raw).unwrap_or(json!({}));
            (
                val.get("run_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                val.get("created_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            )
        } else {
            let dirname = entry.file_name().to_string_lossy().to_string();
            (dirname, String::new())
        };

        // resolved_experiment.json → experiment id
        let resolved_path = run_path.join("resolved_experiment.json");
        let experiment = if resolved_path.exists() {
            let raw = std::fs::read_to_string(&resolved_path).unwrap_or_default();
            let val: Value = serde_json::from_str(&raw).unwrap_or(json!({}));
            val.pointer("/experiment/id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        } else {
            String::new()
        };

        // Canonical: facts/trials.jsonl (variant_summary is query-time derived).
        let trials_facts_path = run_path.join("facts").join("trials.jsonl");
        let (variants, pass_rate) = if trials_facts_path.exists() {
            let raw = std::fs::read_to_string(&trials_facts_path).unwrap_or_default();
            let mut baseline_id = String::new();
            let mut variant_ids: BTreeSet<String> = BTreeSet::new();
            let mut baseline_total = 0usize;
            let mut baseline_successes = 0usize;
            for line in raw.lines().filter(|line| !line.trim().is_empty()) {
                let row: Value = serde_json::from_str(line).unwrap_or(json!({}));
                let variant_id = row
                    .get("variant_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if variant_id.is_empty() {
                    continue;
                }
                variant_ids.insert(variant_id.clone());
                if baseline_id.is_empty() {
                    baseline_id = row
                        .get("baseline_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                }
                if !baseline_id.is_empty() && variant_id == baseline_id {
                    baseline_total += 1;
                    if row.get("outcome").and_then(Value::as_str) == Some("success") {
                        baseline_successes += 1;
                    }
                }
            }
            let pr = if baseline_total > 0 {
                Some(baseline_successes as f64 / baseline_total as f64)
            } else {
                None
            };
            (variant_ids.len(), pr)
        } else {
            (0, None)
        };

        entries.push(RunRow {
            run_id,
            experiment,
            created_at,
            variants,
            pass_rate,
        });
    }

    entries.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    let rows: Vec<Vec<Value>> = entries
        .into_iter()
        .map(|e| {
            vec![
                Value::String(e.run_id),
                Value::String(e.experiment),
                Value::String(e.created_at),
                json!(e.variants),
                match e.pass_rate {
                    Some(pr) => json!((pr * 10000.0).round() / 10000.0),
                    None => Value::Null,
                },
            ]
        })
        .collect();

    Ok(lab_analysis::QueryTable {
        columns: vec![
            "run_id".into(),
            "experiment".into(),
            "created_at".into(),
            "variants".into(),
            "pass_rate".into(),
        ],
        rows,
    })
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
      "id": "runtime.policy.network.mode",
      "label": "Network Mode",
      "json_pointer": "/runtime/policy/network/mode",
      "type": "string",
      "options": ["none", "full", "allowlist_enforced"],
      "role": "infra",
      "scientific_role": "invariant"
    },
    {
      "id": "runtime.agent.command",
      "label": "Agent Command",
      "json_pointer": "/runtime/agent/command",
      "type": "json",
      "role": "agent",
      "scientific_role": "treatment"
    },
    {
      "id": "runtime.agent.image",
      "label": "Agent Image",
      "json_pointer": "/runtime/agent/image",
      "type": "string",
      "role": "infra",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "agentlab_cli_{}_{}_{}",
            label,
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn read_run_status_renders_multiflight_active_trials() {
        let run_dir = temp_dir("run_status");
        let runtime_dir = run_dir.join("runtime");
        std::fs::create_dir_all(&runtime_dir).expect("runtime dir");
        let control = json!({
            "schema_version": "run_control_v2",
            "run_id": "run_1",
            "status": "running",
            "active_trials": {
                "trial_1": {
                    "trial_id": "trial_1",
                    "worker_id": "worker_2",
                    "schedule_idx": 1,
                    "variant_id": "base",
                    "started_at": "2026-02-22T00:00:00Z",
                    "control": null
                },
                "trial_2": {
                    "trial_id": "trial_2",
                    "worker_id": "worker_1",
                    "schedule_idx": 2,
                    "variant_id": "candidate",
                    "started_at": "2026-02-22T00:00:01Z",
                    "control": null
                }
            },
            "updated_at": "2026-02-22T00:00:02Z"
        });
        std::fs::write(
            runtime_dir.join("run_control.json"),
            serde_json::to_vec_pretty(&control).expect("serialize control"),
        )
        .expect("write control");

        let status = read_run_status(&run_dir);
        assert_eq!(
            status,
            "running (active_trials=2, workers=worker_1,worker_2)"
        );

        let _ = std::fs::remove_dir_all(&run_dir);
    }
}
