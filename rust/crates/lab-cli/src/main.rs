use anyhow::{anyhow, Result};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod tui;

#[derive(Parser)]
#[command(name = "lab", version = "0.3.0", about = "AgentLab Rust CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
}

#[derive(Subcommand)]
enum Commands {
    Build {
        experiment: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        overrides: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    BuildRun {
        experiment: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        overrides: Option<PathBuf>,
        #[arg(long, value_enum)]
        materialize: Option<MaterializeArg>,
        #[arg(long = "env", value_name = "KEY=VALUE", action = ArgAction::Append)]
        runtime_env: Vec<String>,
        #[arg(long = "env-file", value_name = "PATH", action = ArgAction::Append)]
        runtime_env_file: Vec<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    Run {
        package: PathBuf,
        #[arg(long, value_enum)]
        materialize: Option<MaterializeArg>,
        #[arg(long = "env", value_name = "KEY=VALUE", action = ArgAction::Append)]
        runtime_env: Vec<String>,
        #[arg(long = "env-file", value_name = "PATH", action = ArgAction::Append)]
        runtime_env_file: Vec<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    RunExperiment {
        package: PathBuf,
        #[arg(long = "env", value_name = "KEY=VALUE", action = ArgAction::Append)]
        runtime_env: Vec<String>,
        #[arg(long = "env-file", value_name = "PATH", action = ArgAction::Append)]
        runtime_env_file: Vec<PathBuf>,
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
        #[arg(long = "env", value_name = "KEY=VALUE", action = ArgAction::Append)]
        runtime_env: Vec<String>,
        #[arg(long = "env-file", value_name = "PATH", action = ArgAction::Append)]
        runtime_env_file: Vec<PathBuf>,
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
        package: PathBuf,
        #[arg(long)]
        json: bool,
    },
    #[command(about = "Inspect resolved variants for a run; pass VARIANT and optionally --against to show or diff")]
    Variants {
        run: String,
        variant: Option<String>,
        #[arg(long)]
        against: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
        #[arg(long, alias = "markdown")]
        md: bool,
        #[arg(long)]
        html: bool,
    },
    Views {
        run: String,
        view: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long = "max-rows")]
        max_rows: Option<usize>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        csv: bool,
        #[arg(long, alias = "markdown")]
        md: bool,
        #[arg(long)]
        html: bool,
    },
    #[command(
        about = "Live refresh for a view; omit run/view in a TTY to browse active runs and views"
    )]
    ViewsLive {
        run: Option<String>,
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
        package: PathBuf,
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

const STALE_BINARY_WATCH_RELATIVE_PATHS: &[&str] = &[
    "rust/Cargo.toml",
    "rust/Cargo.lock",
    "rust/crates/lab-cli/Cargo.toml",
    "rust/crates/lab-cli/src",
    "rust/crates/lab-runner/Cargo.toml",
    "rust/crates/lab-runner/src",
];

#[derive(Clone, Copy, Debug)]
enum ViewQueryPlan {
    Source(&'static str),
    AbComparisonSummary,
    Scoreboard,
}

#[derive(Clone, Copy, Debug)]
struct StandardViewDef {
    name: &'static str,
    purpose: &'static str,
    plan: ViewQueryPlan,
    aliases: &'static [&'static str],
}

#[derive(Clone, Debug)]
enum ResolvedViewPlan {
    Source(String),
    AbComparisonSummary,
    Scoreboard,
}

#[derive(Clone, Debug)]
struct ResolvedView {
    name: String,
    source: Option<String>,
    plan: ResolvedViewPlan,
    standardize_ab_terms: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TableRenderFormat {
    Text,
    Csv,
    Markdown,
    Html,
}

#[derive(Clone, Debug, Default)]
struct RunControlSummary {
    status: String,
    status_display: String,
    live_summary: String,
    active_trials: usize,
    is_active: bool,
}

#[derive(Clone, Debug)]
struct RunInventoryEntry {
    run_id: String,
    run_dir: PathBuf,
    experiment: String,
    started_at: String,
    started_at_display: String,
    control: RunControlSummary,
}

#[derive(Clone, Debug)]
struct RunMetrics {
    variants: usize,
    pass_rate: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ViewsBrowserScreen {
    RunPicker,
    ViewPicker,
    Viewer,
}

const STANDARD_VIEWS_CORE_ONLY: &[StandardViewDef] = &[
    StandardViewDef {
        name: "run_progress",
        purpose: "Run-level completion and pass-rate snapshot.",
        plan: ViewQueryPlan::Source("run_progress"),
        aliases: &["status", "progress"],
    },
    StandardViewDef {
        name: "variant_summary",
        purpose: "Per-variant success and primary metric summary.",
        plan: ViewQueryPlan::Source("variant_summary"),
        aliases: &["variants", "summary_by_variant"],
    },
    StandardViewDef {
        name: "task_variant_matrix",
        purpose: "Task-by-variant pass rates for quick gap scanning.",
        plan: ViewQueryPlan::Source("task_variant_matrix"),
        aliases: &["task_matrix", "matrix"],
    },
    StandardViewDef {
        name: "scoreboard",
        purpose: "Per-task scoreboard grouped by variant with metric aggregates.",
        plan: ViewQueryPlan::Scoreboard,
        aliases: &["board", "scores"],
    },
];

const STANDARD_VIEWS_AB_TEST: &[StandardViewDef] = &[
    StandardViewDef {
        name: "run_progress",
        purpose: "Run-level completion and pass-rate snapshot.",
        plan: ViewQueryPlan::Source("run_progress"),
        aliases: &["status", "progress"],
    },
    StandardViewDef {
        name: "variant_summary",
        purpose: "Per-variant success and primary metric summary.",
        plan: ViewQueryPlan::Source("variant_summary"),
        aliases: &["variants", "summary_by_variant"],
    },
    StandardViewDef {
        name: "comparison_summary",
        purpose: "Single-row AB summary (rates, deltas, effect size, McNemar).",
        plan: ViewQueryPlan::AbComparisonSummary,
        aliases: &[
            "summary",
            "overview",
            "paired_outcomes",
            "paired_diffs",
            "win_loss_tie",
            "effect_size",
            "mcnemar_contingency",
            "task_diffs",
        ],
    },
    StandardViewDef {
        name: "task_outcomes",
        purpose: "Task-level outcome/result side-by-side for variant_a vs variant_b.",
        plan: ViewQueryPlan::Source("ab_task_outcomes"),
        aliases: &[
            "outcome_compare",
            "ab_task_outcomes",
            "task_outcome_compare",
            "ab_task_table",
        ],
    },
    StandardViewDef {
        name: "task_metrics",
        purpose: "Task-level metric deltas with aligned trials and outcome change.",
        plan: ViewQueryPlan::Source("ab_task_metrics_side_by_side"),
        aliases: &[
            "task_compare",
            "task_comparison",
            "by_task",
            "task_table",
            "ab_task_metrics_side_by_side",
        ],
    },
    StandardViewDef {
        name: "turn_compare",
        purpose: "Turn-level side-by-side comparison (model, status, token deltas).",
        plan: ViewQueryPlan::Source("ab_turn_side_by_side"),
        aliases: &[
            "turn_diff",
            "turn_compare",
            "turn_side_by_side",
            "trace_turns",
            "ab_turn_side_by_side",
        ],
    },
    StandardViewDef {
        name: "trace",
        purpose: "Trace row side-by-side comparison for event-level diagnostics.",
        plan: ViewQueryPlan::Source("ab_trace_row_side_by_side"),
        aliases: &[
            "trace",
            "trace_diff",
            "trace_compare",
            "trace_side_by_side",
            "ab_trace_row_side_by_side",
        ],
    },
    StandardViewDef {
        name: "scoreboard",
        purpose: "Per-task scoreboard grouped by variant with metric aggregates.",
        plan: ViewQueryPlan::Scoreboard,
        aliases: &["board", "scores"],
    },
];

const STANDARD_VIEWS_MULTI_VARIANT: &[StandardViewDef] = &[
    StandardViewDef {
        name: "run_progress",
        purpose: "Run-level completion and pass-rate snapshot.",
        plan: ViewQueryPlan::Source("run_progress"),
        aliases: &["status", "progress"],
    },
    StandardViewDef {
        name: "variant_summary",
        purpose: "Per-variant success and primary metric summary.",
        plan: ViewQueryPlan::Source("variant_summary"),
        aliases: &["variants", "summary_by_variant"],
    },
    StandardViewDef {
        name: "variant_ranking",
        purpose: "Ranking by pass-rate and primary metric vs reference variant.",
        plan: ViewQueryPlan::Source("variant_ranking"),
        aliases: &["ranking", "leaderboard"],
    },
    StandardViewDef {
        name: "pairwise_compare",
        purpose: "Pairwise win/loss/tie counts across variant pairs.",
        plan: ViewQueryPlan::Source("pairwise_comparisons"),
        aliases: &["pairwise", "pairwise_comparisons"],
    },
    StandardViewDef {
        name: "task_variant_matrix",
        purpose: "Task-by-variant pass rates for quick gap scanning.",
        plan: ViewQueryPlan::Source("task_variant_matrix"),
        aliases: &["task_matrix", "matrix", "heatmap"],
    },
    StandardViewDef {
        name: "scoreboard",
        purpose: "Per-task scoreboard grouped by variant with metric aggregates.",
        plan: ViewQueryPlan::Scoreboard,
        aliases: &["board", "scores"],
    },
];

const STANDARD_VIEWS_PARAMETER_SWEEP: &[StandardViewDef] = &[
    StandardViewDef {
        name: "run_progress",
        purpose: "Run-level completion and pass-rate snapshot.",
        plan: ViewQueryPlan::Source("run_progress"),
        aliases: &["status", "progress"],
    },
    StandardViewDef {
        name: "variant_summary",
        purpose: "Per-variant success and primary metric summary.",
        plan: ViewQueryPlan::Source("variant_summary"),
        aliases: &["variants", "summary_by_variant"],
    },
    StandardViewDef {
        name: "config_ranking",
        purpose: "Top configurations by primary metric and pass-rate.",
        plan: ViewQueryPlan::Source("best_config"),
        aliases: &["best_config", "ranking", "top_configs"],
    },
    StandardViewDef {
        name: "parameter_effects",
        purpose: "Average metric by parameter value.",
        plan: ViewQueryPlan::Source("parameter_metric"),
        aliases: &["parameter_metric", "parameter_impact"],
    },
    StandardViewDef {
        name: "parameter_sensitivity",
        purpose: "Variance/range sensitivity by parameter.",
        plan: ViewQueryPlan::Source("sensitivity"),
        aliases: &["sensitivity"],
    },
    StandardViewDef {
        name: "scoreboard",
        purpose: "Per-task scoreboard grouped by variant with metric aggregates.",
        plan: ViewQueryPlan::Scoreboard,
        aliases: &["board", "scores"],
    },
];

const STANDARD_VIEWS_REGRESSION: &[StandardViewDef] = &[
    StandardViewDef {
        name: "run_progress",
        purpose: "Run-level completion and pass-rate snapshot.",
        plan: ViewQueryPlan::Source("run_progress"),
        aliases: &["status", "progress"],
    },
    StandardViewDef {
        name: "variant_summary",
        purpose: "Per-variant success and primary metric summary.",
        plan: ViewQueryPlan::Source("variant_summary"),
        aliases: &["variants", "summary_by_variant"],
    },
    StandardViewDef {
        name: "run_trend",
        purpose: "Pass-rate trend per run and variant.",
        plan: ViewQueryPlan::Source("pass_rate_trend"),
        aliases: &["trend", "pass_rate_trend"],
    },
    StandardViewDef {
        name: "flaky_tasks",
        purpose: "Tasks with unstable outcomes across replications.",
        plan: ViewQueryPlan::Source("flaky_tasks"),
        aliases: &["flaky"],
    },
    StandardViewDef {
        name: "failure_clusters",
        purpose: "Failure concentration by task-group prefix.",
        plan: ViewQueryPlan::Source("failure_clusters"),
        aliases: &["clusters"],
    },
    StandardViewDef {
        name: "scoreboard",
        purpose: "Per-task scoreboard grouped by variant with metric aggregates.",
        plan: ViewQueryPlan::Scoreboard,
        aliases: &["board", "scores"],
    },
];

fn repo_root_for_stale_binary_guard() -> Option<PathBuf> {
    let candidate = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
    if candidate.exists() {
        Some(candidate)
    } else {
        None
    }
}

fn latest_mtime_in_path(path: &Path) -> Result<Option<(SystemTime, PathBuf)>> {
    if !path.exists() {
        return Ok(None);
    }
    if path.file_name().and_then(|name| name.to_str()) == Some("tests.rs") {
        return Ok(None);
    }
    let meta = std::fs::metadata(path).map_err(|err| {
        anyhow!(
            "failed to stat stale-binary watch path {}: {}",
            path.display(),
            err
        )
    })?;
    if meta.is_file() {
        let modified = meta
            .modified()
            .map_err(|err| anyhow!("failed to read mtime for {}: {}", path.display(), err))?;
        return Ok(Some((modified, path.to_path_buf())));
    }
    if !meta.is_dir() {
        return Ok(None);
    }

    let mut newest: Option<(SystemTime, PathBuf)> = None;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).map_err(|err| {
            anyhow!(
                "failed to read stale-binary watch dir {}: {}",
                dir.display(),
                err
            )
        })?;
        for entry in entries {
            let entry = entry
                .map_err(|err| anyhow!("failed to read dir entry in {}: {}", dir.display(), err))?;
            let entry_path = entry.path();
            let entry_meta = entry
                .metadata()
                .map_err(|err| anyhow!("failed to stat {}: {}", entry_path.display(), err))?;
            if entry_meta.is_dir() {
                stack.push(entry_path);
                continue;
            }
            if !entry_meta.is_file() {
                continue;
            }
            if entry_path.file_name().and_then(|name| name.to_str()) == Some("tests.rs") {
                continue;
            }
            let modified = entry_meta.modified().map_err(|err| {
                anyhow!("failed to read mtime for {}: {}", entry_path.display(), err)
            })?;
            let replace = newest
                .as_ref()
                .map(|(current, _)| modified > *current)
                .unwrap_or(true);
            if replace {
                newest = Some((modified, entry_path));
            }
        }
    }
    Ok(newest)
}

fn newest_watch_mtime(repo_root: &Path) -> Result<Option<(SystemTime, PathBuf)>> {
    let mut newest: Option<(SystemTime, PathBuf)> = None;
    for rel in STALE_BINARY_WATCH_RELATIVE_PATHS {
        let candidate = repo_root.join(rel);
        let Some((modified, path)) = latest_mtime_in_path(&candidate)? else {
            continue;
        };
        let replace = newest
            .as_ref()
            .map(|(current, _)| modified > *current)
            .unwrap_or(true);
        if replace {
            newest = Some((modified, path));
        }
    }
    Ok(newest)
}

fn stale_binary_guard_error(
    exe_path: &Path,
    exe_mtime: SystemTime,
    source_path: &Path,
    source_mtime: SystemTime,
) -> anyhow::Error {
    let exe_secs = exe_mtime
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    let source_secs = source_mtime
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    anyhow!(
        "stale lab-cli binary detected: executable '{}' (mtime={}s) is older than source '{}' (mtime={}s). Rebuild with `cargo build -p lab-cli --release` and rerun.",
        exe_path.display(),
        exe_secs,
        source_path.display(),
        source_secs
    )
}

fn enforce_cli_binary_freshness(
    exe_path: &Path,
    exe_mtime: SystemTime,
    newest_source: Option<(SystemTime, PathBuf)>,
) -> Result<()> {
    let Some((source_mtime, source_path)) = newest_source else {
        return Ok(());
    };
    if source_mtime > exe_mtime {
        return Err(stale_binary_guard_error(
            exe_path,
            exe_mtime,
            &source_path,
            source_mtime,
        ));
    }
    Ok(())
}

fn ensure_cli_binary_is_fresh() -> Result<()> {
    let Some(repo_root) = repo_root_for_stale_binary_guard() else {
        return Ok(());
    };
    let exe_path = std::env::current_exe()
        .map_err(|err| anyhow!("failed to resolve current executable path: {}", err))?;
    let exe_mtime = std::fs::metadata(&exe_path)
        .and_then(|meta| meta.modified())
        .map_err(|err| {
            anyhow!(
                "failed to read executable mtime for {}: {}",
                exe_path.display(),
                err
            )
        })?;
    enforce_cli_binary_freshness(&exe_path, exe_mtime, newest_watch_mtime(&repo_root)?)
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let json_mode = command_json_mode(&cli.command);
    let result = ensure_cli_binary_is_fresh().and_then(|_| run_command(cli.command));
    match result {
        Ok(Some(payload)) => {
            emit_json(&payload);
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(err) => {
            if json_mode {
                let code = if err.to_string().contains("stale lab-cli binary detected") {
                    "stale_binary"
                } else {
                    "command_failed"
                };
                emit_json(&json_error(code, err.to_string(), json!({})));
                std::process::exit(1);
            }
            Err(err)
        }
    }
}

fn run_command(command: Commands) -> Result<Option<Value>> {
    match command {
        Commands::Build {
            experiment,
            out,
            overrides,
            json,
        } => {
            if !json {
                eprintln!("building package from: {}", experiment.display());
            }
            let build = lab_runner::build_experiment_package(
                &experiment,
                overrides.as_deref(),
                out.as_deref(),
            )?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "build",
                    "package_dir": build.package_dir.display().to_string(),
                    "manifest_path": build.manifest_path.display().to_string(),
                    "checksums_path": build.checksums_path.display().to_string(),
                })));
            }
            println!("package_dir: {}", build.package_dir.display());
            println!("manifest: {}", build.manifest_path.display());
            println!("checksums: {}", build.checksums_path.display());
        }
        Commands::BuildRun {
            experiment,
            out,
            overrides,
            materialize,
            runtime_env,
            runtime_env_file,
            json,
        } => {
            if !json {
                eprintln!("building package from: {}", experiment.display());
            }
            let build = lab_runner::build_experiment_package(
                &experiment,
                overrides.as_deref(),
                out.as_deref(),
            )?;
            let execution = build_run_execution_options(
                materialize,
                &runtime_env,
                &runtime_env_file,
            )?;
            let summary = lab_runner::describe_experiment(&build.package_dir)?;
            if !json {
                print_summary(&summary);
                eprintln!("launching run...");
            }
            let result = lab_runner::run_experiment_with_options(
                &build.package_dir,
                execution.clone(),
            )?;
            if json {
                let post_run = try_post_run_stats_json(&result.run_dir);
                return Ok(Some(json!({
                    "ok": true,
                    "command": "build-run",
                    "package_dir": build.package_dir.display().to_string(),
                    "manifest_path": build.manifest_path.display().to_string(),
                    "checksums_path": build.checksums_path.display().to_string(),
                    "summary": summary_to_json(&summary),
                    "run": run_result_to_json(&result),
                    "artifacts": run_artifacts_to_json(&result),
                    "materialize": execution.materialize.map(|m| m.as_str()),
                    "post_run_stats": post_run
                })));
            }
            println!("package_dir: {}", build.package_dir.display());
            println!("manifest: {}", build.manifest_path.display());
            println!("checksums: {}", build.checksums_path.display());
            println!("run_id: {}", result.run_id);
            println!("run_dir: {}", result.run_dir.display());
            try_print_post_run_stats(&result.run_dir, &result.run_id);
        }
        Commands::Run {
            package,
            materialize,
            runtime_env,
            runtime_env_file,
            json,
        } => {
            if !json {
                eprintln!("loading package: {}", package.display());
            }
            let summary = lab_runner::describe_experiment(&package)?;
            let execution = build_run_execution_options(
                materialize,
                &runtime_env,
                &runtime_env_file,
            )?;
            if !json {
                print_summary(&summary);
                eprintln!("launching run...");
            }
            let result = lab_runner::run_experiment_with_options(&package, execution.clone())?;
            if json {
                let post_run = try_post_run_stats_json(&result.run_dir);
                return Ok(Some(json!({
                    "ok": true,
                    "command": "run",
                    "summary": summary_to_json(&summary),
                    "run": run_result_to_json(&result),
                    "artifacts": run_artifacts_to_json(&result),
                    "materialize": execution.materialize.map(|m| m.as_str()),
                    "post_run_stats": post_run
                })));
            }
            println!("run_id: {}", result.run_id);
            println!("run_dir: {}", result.run_dir.display());
            try_print_post_run_stats(&result.run_dir, &result.run_id);
        }
        Commands::RunExperiment {
            package,
            runtime_env,
            runtime_env_file,
            json,
        } => {
            if !json {
                eprintln!("loading package: {}", package.display());
            }
            let summary = lab_runner::describe_experiment(&package)?;
            if !json {
                print_summary(&summary);
                eprintln!("launching strict experiment run...");
            }
            let execution = build_run_execution_options(
                None,
                &runtime_env,
                &runtime_env_file,
            )?;
            let result = lab_runner::run_experiment_strict_with_options(&package, execution)?;
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
        Commands::Continue {
            run_dir,
            runtime_env,
            runtime_env_file,
            json,
        } => {
            let execution = build_run_execution_options(
                None,
                &runtime_env,
                &runtime_env_file,
            )?;
            let result = lab_runner::continue_run_with_options(&run_dir, execution)?;
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
        Commands::Describe { package, json } => {
            let summary = lab_runner::describe_experiment(&package)?;
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "describe",
                    "summary": summary_to_json(&summary)
                })));
            }
            print_summary(&summary);
        }
        Commands::Variants {
            run,
            variant,
            against,
            json,
            csv,
            md,
            html,
        } => {
            if [json, csv, md, html].into_iter().filter(|flag| *flag).count() > 1 {
                return Err(anyhow!(
                    "--json, --csv, --md, and --html are mutually exclusive"
                ));
            }
            if variant.is_none() && against.is_some() {
                return Err(anyhow!("--against requires a variant id"));
            }

            let run_dir = resolve_run_dir_arg(&run)?;
            let inspection = load_variant_inspection_set(&run_dir)?;
            let render_format = table_render_format(csv, md, html);

            match (variant.as_deref(), against.as_deref()) {
                (None, None) => {
                    if json {
                        return Ok(Some(json!({
                            "ok": true,
                            "command": "variants",
                            "mode": "list",
                            "run_dir": run_dir.display().to_string(),
                            "experiment_id": inspection.experiment_id,
                            "baseline_id": inspection.baseline_id,
                            "variants": inspection
                                .variants
                                .iter()
                                .map(variant_inspection_to_json)
                                .collect::<Vec<_>>()
                        })));
                    }
                    let table = build_variants_list_table(&inspection);
                    render_variants_table("variants", &table, render_format);
                }
                (Some(variant_id), None) => {
                    let item = find_variant_inspection(&inspection, variant_id)?;
                    if json {
                        return Ok(Some(json!({
                            "ok": true,
                            "command": "variants",
                            "mode": "show",
                            "run_dir": run_dir.display().to_string(),
                            "experiment_id": inspection.experiment_id,
                            "baseline_id": inspection.baseline_id,
                            "variant": variant_inspection_to_json(item)
                        })));
                    }
                    let table = build_variant_show_table(item);
                    render_variants_table(
                        &format!("variant {}", item.id),
                        &table,
                        render_format,
                    );
                }
                (Some(variant_id), Some(against_id)) => {
                    let left = find_variant_inspection(&inspection, variant_id)?;
                    let right = find_variant_inspection(&inspection, against_id)?;
                    let diffs = diff_variant_surfaces(left, right);
                    if json {
                        return Ok(Some(json!({
                            "ok": true,
                            "command": "variants",
                            "mode": "diff",
                            "run_dir": run_dir.display().to_string(),
                            "experiment_id": inspection.experiment_id,
                            "baseline_id": inspection.baseline_id,
                            "left": variant_inspection_to_json(left),
                            "right": variant_inspection_to_json(right),
                            "diff": diffs
                                .iter()
                                .map(|entry| json!({
                                    "path": entry.path,
                                    "left": entry.left,
                                    "right": entry.right,
                                }))
                                .collect::<Vec<_>>()
                        })));
                    }
                    let table = build_variant_diff_table(left, right, &diffs);
                    render_variants_table(
                        &format!("variant diff {} vs {}", left.id, right.id),
                        &table,
                        render_format,
                    );
                }
                (None, Some(_)) => unreachable!("validated above"),
            }
        }
        Commands::Views {
            run,
            view,
            all,
            max_rows,
            json,
            csv,
            md,
            html,
        } => {
            let format_flags = [json, csv, md, html]
                .into_iter()
                .filter(|flag| *flag)
                .count();
            if format_flags > 1 {
                return Err(anyhow::anyhow!(
                    "--json, --csv, --md, and --html are mutually exclusive"
                ));
            }
            if all && view.is_some() {
                return Err(anyhow::anyhow!(
                    "--all cannot be combined with a specific view name"
                ));
            }
            let run_dir = resolve_run_dir_arg(&run)?;
            let run_view_set = lab_analysis::run_view_set(&run_dir)?;
            let view_set = run_view_set.as_str().to_string();
            let raw_view_names = lab_analysis::list_views(&run_dir)?;
            let standard_views = standard_views_for_set(run_view_set);
            let row_limit = max_rows.unwrap_or(0);
            let render_format = table_render_format(csv, md, html);

            if all {
                if json {
                    let mut payload = serde_json::Map::new();
                    for def in standard_views {
                        let resolved = resolved_view_from_def(run_view_set, def);
                        let table = query_resolved_view(&run_dir, &resolved, row_limit)?;
                        payload.insert(def.name.to_string(), query_table_to_json(&table));
                    }
                    return Ok(Some(json!({
                        "ok": true,
                        "command": "views",
                        "run_dir": run_dir.display().to_string(),
                        "view_set": view_set,
                        "view_count": standard_views.len(),
                        "raw_view_count": raw_view_names.len(),
                        "views": Value::Object(payload),
                    })));
                }
                let mut rendered: Vec<(ResolvedView, lab_analysis::QueryTable)> =
                    Vec::with_capacity(standard_views.len());
                for def in standard_views {
                    let resolved = resolved_view_from_def(run_view_set, def);
                    let table = query_resolved_view(&run_dir, &resolved, row_limit)?;
                    rendered.push((resolved, table));
                }
                if matches!(render_format, TableRenderFormat::Csv) {
                    for (_, table) in rendered {
                        print_query_table_csv(&table);
                    }
                    return Ok(None);
                }
                if matches!(render_format, TableRenderFormat::Markdown) {
                    print_views_markdown_document(&run_dir, &view_set, &rendered);
                    return Ok(None);
                }
                if matches!(render_format, TableRenderFormat::Html) {
                    print_views_html_document(&run_dir, &view_set, &rendered);
                    return Ok(None);
                }
                println!("run_dir: {}", run_dir.display());
                println!("view_set: {}", view_set);
                for (resolved, table) in rendered {
                    println!("\n== {} ==", resolved.name);
                    if !print_special_split_view(&run_dir, &resolved.name, &table) {
                        print_query_table(&table);
                    }
                }
                return Ok(None);
            }

            if let Some(view_name) = view {
                let resolved = resolve_requested_view(run_view_set, &raw_view_names, &view_name)?;
                let table = query_resolved_view(&run_dir, &resolved, row_limit)?;
                if json {
                    return Ok(Some(json!({
                        "ok": true,
                        "command": "views",
                        "run_dir": run_dir.display().to_string(),
                        "view_set": view_set,
                        "view": resolved.name,
                        "source_view": resolved.source,
                        "result": query_table_to_json(&table),
                    })));
                }
                if matches!(render_format, TableRenderFormat::Csv) {
                    print_query_table_csv(&table);
                    return Ok(None);
                }
                if matches!(render_format, TableRenderFormat::Markdown) {
                    print_single_view_markdown(&run_dir, &view_set, &resolved, &table);
                    return Ok(None);
                }
                if matches!(render_format, TableRenderFormat::Html) {
                    print_single_view_html(&run_dir, &view_set, &resolved, &table);
                    return Ok(None);
                }
                println!("run_dir: {}", run_dir.display());
                println!("view_set: {}", view_set);
                println!("view: {}", resolved.name);
                if let Some(source) = resolved.source.as_deref() {
                    if source != resolved.name {
                        println!("source_view: {}", source);
                    }
                }
                if !print_special_split_view(&run_dir, &resolved.name, &table) {
                    print_query_table(&table);
                }
                return Ok(None);
            }

            // Standardized view listing (no specific view selected)
            let listing_table = lab_analysis::QueryTable {
                columns: vec![
                    "view_name".to_string(),
                    "source_view".to_string(),
                    "purpose".to_string(),
                ],
                rows: standard_views
                    .iter()
                    .map(|def| {
                        vec![
                            Value::String(def.name.to_string()),
                            Value::String(standard_view_source_label(def).to_string()),
                            Value::String(def.purpose.to_string()),
                        ]
                    })
                    .collect(),
            };
            if json {
                return Ok(Some(json!({
                    "ok": true,
                    "command": "views",
                    "run_dir": run_dir.display().to_string(),
                    "view_set": view_set,
                    "available_views": standard_views.iter().map(|def| json!({
                        "name": def.name,
                        "source_view": standard_view_source_label(def),
                        "purpose": def.purpose,
                    })).collect::<Vec<_>>(),
                    "raw_view_count": raw_view_names.len(),
                })));
            }
            if matches!(render_format, TableRenderFormat::Csv) {
                print_query_table_csv(&listing_table);
                return Ok(None);
            }
            if matches!(render_format, TableRenderFormat::Markdown) {
                print_table_markdown(&listing_table);
                return Ok(None);
            }
            if matches!(render_format, TableRenderFormat::Html) {
                print_table_html_document("available_views", &listing_table);
                return Ok(None);
            }
            println!("run_dir: {}", run_dir.display());
            println!("view_set: {}", view_set);
            print_query_table(&listing_table);
            let hidden = raw_view_names.len().saturating_sub(standard_views.len());
            println!();
            println!(
                "standardized view surface: {} views ({} internal/raw views hidden by default)",
                standard_views.len(),
                hidden
            );
            println!("tip: use `lab query <run> \"SELECT * FROM <raw_view>\"` for raw internals");
        }
        Commands::ViewsLive {
            run,
            view,
            interval_seconds,
            limit,
            once,
            no_clear,
        } => {
            let sleep_interval = Duration::from_secs(interval_seconds.max(1));
            let resolved_limit = limit.max(1);
            let use_tui = !once && !no_clear && stdout_is_tty();
            let run_dir = match run.as_deref() {
                Some(run_arg) => Some(resolve_run_dir_arg(run_arg)?),
                None => None,
            };

            if use_tui {
                let project_root = resolve_project_root(std::env::current_dir()?.as_path());
                run_interactive_views_browser(
                    &project_root,
                    run_dir,
                    view.as_deref(),
                    sleep_interval,
                    resolved_limit,
                )?;
            } else {
                let run_dir = run_dir.ok_or_else(|| {
                    anyhow::anyhow!(
                        "run is required when interactive TUI selection is unavailable; pass a run id/path or use a TTY without --once/--no-clear"
                    )
                })?;
                let run_view_set = lab_analysis::run_view_set(&run_dir)?;
                let raw_view_names = lab_analysis::list_views(&run_dir)?;
                let resolved_view = match view.as_deref() {
                    Some(requested) => {
                        resolve_requested_view(run_view_set, &raw_view_names, requested)?
                    }
                    None => resolve_requested_view(run_view_set, &raw_view_names, "run_progress")?,
                };
                // Plain text fallback (--once, --no-clear, non-TTY)
                loop {
                    let table = query_resolved_view(&run_dir, &resolved_view, resolved_limit)?;
                    if !no_clear {
                        print!("\x1B[2J\x1B[H");
                        let _ = std::io::stdout().flush();
                    }
                    println!("run_dir: {}", run_dir.display());
                    println!("status: {}", read_run_status(&run_dir));
                    println!("updated_unix_s: {}", unix_now_seconds());
                    println!("view: {}", resolved_view.name);
                    if let Some(source) = resolved_view.source.as_deref() {
                        if source != resolved_view.name {
                            println!("source_view: {}", source);
                        }
                    }
                    println!("limit: {}", resolved_limit);
                    println!(
                        "refresh_interval_seconds: {} (Ctrl-C to stop)",
                        sleep_interval.as_secs()
                    );
                    println!();
                    if !print_special_split_view(&run_dir, &resolved_view.name, &table) {
                        print_query_table(&table);
                    }

                    if once {
                        break;
                    }
                    std::thread::sleep(sleep_interval);
                }
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
                println!("  - agent-eval  : single-variant isolated agent evaluation");
                println!(
                    "  - ab-test     : paired two-variant comparison (variant_a vs variant_b)"
                );
                println!("  - sweep       : independent parameter sweep over variants");
                println!("  - regression  : fixed-suite pass-rate tracking over time");
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
            println!("next: lab build {} --out .lab/builds/<name>", exp_show);
            println!("next: lab describe .lab/builds/<name>");
        }
        Commands::Preflight { package, json } => {
            if !json {
                eprintln!("running preflight: {}", package.display());
            }
            let report = lab_runner::preflight_experiment(&package)?;
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
            "experiment:
  id: my_eval
  name: My Agent Evaluation
  workload_type: agent_runtime
dataset:
  suite_id: local_suite
  provider: local_jsonl
  path: tasks.jsonl
  schema_version: task_spec_v1
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
  agent_runtime:
    command: [python, harness.py]
    artifact: ./agents/my-agent-runtime.tar.gz
    image: ghcr.io/acme/agent-runtime:latest
    network: none
    root_read_only: true
policy:
  timeout_ms: 300000
  task_sandbox:
    profile: default
    network: none
validity:
  fail_on_state_leak: true
  fail_on_profile_invariant_violation: true
"
        }
        InitProfileArg::AbTest => {
            "experiment:
  id: my_ab_test
  name: Paired Variant Comparison
  workload_type: agent_runtime
dataset:
  suite_id: local_suite
  provider: local_jsonl
  path: tasks.jsonl
  schema_version: task_spec_v1
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
  variant_id: variant_a
  bindings: {}
variant_plan:
  - variant_id: variant_b
    bindings:
      model: claude-4
runtime:
  agent_runtime:
    command: [python, harness.py]
    artifact: ./agents/my-agent-runtime.tar.gz
    image: ghcr.io/acme/agent-runtime:latest
    network: none
    root_read_only: true
policy:
  timeout_ms: 300000
  task_sandbox:
    profile: default
    network: none
validity:
  fail_on_state_leak: true
  fail_on_profile_invariant_violation: true
"
        }
        InitProfileArg::Sweep => {
            "experiment:
  id: my_sweep
  name: Parameter Sweep
  workload_type: agent_runtime
dataset:
  suite_id: local_suite
  provider: local_jsonl
  path: tasks.jsonl
  schema_version: task_spec_v1
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
  agent_runtime:
    command: [python, harness.py]
    artifact: ./agents/my-agent-runtime.tar.gz
    image: ghcr.io/acme/agent-runtime:latest
    network: none
    root_read_only: true
policy:
  timeout_ms: 300000
  task_sandbox:
    profile: default
    network: none
validity:
  fail_on_state_leak: true
  fail_on_profile_invariant_violation: true
"
        }
        InitProfileArg::Regression => {
            "experiment:
  id: my_regression
  name: Regression Tracking
  workload_type: agent_runtime
dataset:
  suite_id: local_suite
  provider: local_jsonl
  path: tasks.jsonl
  schema_version: task_spec_v1
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
  agent_runtime:
    command: [python, harness.py]
    artifact: ./agents/my-agent-runtime.tar.gz
    image: ghcr.io/acme/agent-runtime:latest
    network: none
    root_read_only: true
policy:
  timeout_ms: 300000
  task_sandbox:
    profile: default
    network: none
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
        Commands::Build { json, .. }
        | Commands::BuildRun { json, .. }
        | Commands::Run { json, .. }
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
    let sqlite = result.run_dir.join("run.sqlite");
    let objects = result.run_dir.join("objects");
    let benchmark = result.run_dir.join("benchmark");
    let summary_path = benchmark.join("summary.json");
    json!({
        "run_sqlite_path": sqlite.display().to_string(),
        "objects_dir": objects.display().to_string(),
        "benchmark_dir": benchmark.display().to_string(),
        "benchmark_summary_path": if summary_path.exists() {
            Some(summary_path.display().to_string())
        } else {
            None::<String>
        }
    })
}

fn load_run_control(run_dir: &Path) -> Option<Value> {
    let sqlite_path = run_dir.join("run.sqlite");
    if !sqlite_path.exists() {
        return None;
    }
    let conn = Connection::open(sqlite_path).ok()?;
    let raw: Option<String> = conn
        .query_row(
            "SELECT value_json FROM runtime_kv WHERE key=?1",
            params!["run_control_v2"],
            |row| row.get(0),
        )
        .optional()
        .ok()?;
    raw.and_then(|payload| serde_json::from_str::<Value>(&payload).ok())
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

fn parse_runtime_env_bindings(values: &[String]) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for raw in values {
        let (key_raw, value_raw) = raw
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid --env '{}': expected KEY=VALUE", raw))?;
        let key = key_raw.trim();
        if key.is_empty() {
            return Err(anyhow!("invalid --env '{}': key cannot be empty", raw));
        }
        out.insert(key.to_string(), value_raw.to_string());
    }
    Ok(out)
}

fn build_run_execution_options(
    materialize: Option<MaterializeArg>,
    runtime_env: &[String],
    runtime_env_files: &[PathBuf],
) -> Result<lab_runner::RunExecutionOptions> {
    Ok(lab_runner::RunExecutionOptions {
        materialize: materialize.map(Into::into),
        runtime_env: parse_runtime_env_bindings(runtime_env)?,
        runtime_env_files: runtime_env_files.to_vec(),
    })
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

#[derive(Debug, Clone)]
struct VariantInspectionSet {
    experiment_id: Option<String>,
    baseline_id: String,
    variants: Vec<VariantInspection>,
}

#[derive(Debug, Clone)]
struct VariantInspection {
    id: String,
    is_baseline: bool,
    variant_digest: String,
    agent_ref: Option<String>,
    raw_variant: Value,
    behavior_surface: Value,
    code_surface: Value,
}

#[derive(Debug, Clone)]
struct JsonDiffRow {
    path: String,
    left: Value,
    right: Value,
}

fn load_variant_inspection_set(run_dir: &Path) -> Result<VariantInspectionSet> {
    let resolved_path = run_dir.join("resolved_experiment.json");
    let resolved_experiment = read_json_file(&resolved_path)
        .ok_or_else(|| anyhow!("missing or invalid {}", resolved_path.display()))?;
    let variants_path = run_dir.join("resolved_variants.json");
    let manifest = read_json_file(&variants_path)
        .ok_or_else(|| anyhow!("missing or invalid {}", variants_path.display()))?;

    let baseline_id = manifest
        .pointer("/baseline_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| anyhow!("resolved_variants.json missing baseline_id"))?;
    let variant_values = manifest
        .pointer("/variants")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("resolved_variants.json missing variants array"))?;
    let experiment_id = resolved_experiment
        .pointer("/experiment/id")
        .or_else(|| resolved_experiment.pointer("/id"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let variants = variant_values
        .iter()
        .map(|variant| build_variant_inspection(&resolved_experiment, &baseline_id, variant))
        .collect::<Result<Vec<_>>>()?;

    Ok(VariantInspectionSet {
        experiment_id,
        baseline_id,
        variants,
    })
}

fn build_variant_inspection(
    resolved_experiment: &Value,
    baseline_id: &str,
    variant: &Value,
) -> Result<VariantInspection> {
    let id = variant
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("variant entry missing id"))?
        .to_string();
    let behavior_surface = build_variant_behavior_surface(resolved_experiment, variant)?;
    let code_surface = build_variant_code_surface(&behavior_surface);
    let variant_digest = variant
        .get("variant_digest")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("variant '{}' missing variant_digest", id))?;
    let agent_ref = variant
        .get("agent_ref")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            behavior_surface
                .pointer("/runtime/agent_ref")
                .and_then(Value::as_str)
                .map(str::to_string)
        });

    Ok(VariantInspection {
        id: id.clone(),
        is_baseline: id == baseline_id,
        variant_digest,
        agent_ref,
        raw_variant: variant.clone(),
        behavior_surface,
        code_surface,
    })
}

fn build_variant_behavior_surface(resolved_experiment: &Value, variant: &Value) -> Result<Value> {
    let mut runtime = resolved_experiment
        .pointer("/runtime")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !runtime.is_object() {
        return Err(anyhow!("invalid /runtime in resolved_experiment.json: expected object"));
    }
    if let Some(runtime_overrides) = variant.get("runtime_overrides") {
        if runtime_overrides.is_null() {
            return Ok(json!({
                "bindings": variant.get("bindings").cloned().unwrap_or_else(|| json!({})),
                "args": variant.get("args").cloned().unwrap_or_else(|| json!([])),
                "env": variant.get("env").cloned().unwrap_or_else(|| json!({})),
                "image": variant.get("image").cloned().unwrap_or(Value::Null),
                "agent_ref": variant.get("agent_ref").cloned().unwrap_or(Value::Null),
                "runtime": runtime,
            }));
        }
        if !runtime_overrides.is_object() {
            return Err(anyhow!(
                "variant '{}' runtime_overrides must be an object",
                variant.get("id").and_then(Value::as_str).unwrap_or("unknown")
            ));
        }
        merge_json_value(&mut runtime, runtime_overrides);
    }

    Ok(json!({
        "bindings": variant.get("bindings").cloned().unwrap_or_else(|| json!({})),
        "args": variant.get("args").cloned().unwrap_or_else(|| json!([])),
        "env": variant.get("env").cloned().unwrap_or_else(|| json!({})),
        "image": variant.get("image").cloned().unwrap_or(Value::Null),
        "agent_ref": variant.get("agent_ref").cloned().unwrap_or(Value::Null),
        "runtime": runtime,
    }))
}

fn build_variant_code_surface(behavior_surface: &Value) -> Value {
    let runtime = behavior_surface.pointer("/runtime").unwrap_or(&Value::Null);
    let mut out = Map::new();
    insert_first_pointer(
        runtime,
        &["/agent_runtime/artifact", "/agent/bundle"],
        "artifact",
        &mut out,
    );
    insert_first_pointer(
        runtime,
        &[
            "/agent_runtime/artifact_digest",
            "/agent/bundle_digest",
        ],
        "artifact_digest",
        &mut out,
    );
    insert_first_pointer(
        runtime,
        &[
            "/agent_runtime/artifact_resolved_path",
            "/agent/bundle_resolved_path",
        ],
        "artifact_resolved_path",
        &mut out,
    );
    insert_first_pointer(
        runtime,
        &["/agent_runtime/image", "/sandbox/image", "/agent/image"],
        "image",
        &mut out,
    );
    insert_first_pointer(
        runtime,
        &["/agent_runtime/command", "/agent/command"],
        "command",
        &mut out,
    );
    insert_first_pointer(
        runtime,
        &["/agent_runtime/env_from_host", "/agent/env_from_host"],
        "env_from_host",
        &mut out,
    );
    insert_first_pointer(
        runtime,
        &["/agent_runtime/binding_args", "/agent/binding_args"],
        "binding_args",
        &mut out,
    );
    insert_first_pointer(
        runtime,
        &["/agent_runtime/source_commit", "/agent/source_commit"],
        "source_commit",
        &mut out,
    );
    insert_first_pointer(
        runtime,
        &["/agent_runtime/source_branch", "/agent/source_branch"],
        "source_branch",
        &mut out,
    );
    insert_first_pointer(
        runtime,
        &["/agent_runtime/source_impl", "/agent/source_impl"],
        "source_impl",
        &mut out,
    );
    insert_first_pointer(
        runtime,
        &["/agent_runtime/dirty", "/agent/dirty"],
        "dirty",
        &mut out,
    );
    Value::Object(out)
}

fn insert_first_pointer(
    root: &Value,
    pointers: &[&str],
    key: &str,
    out: &mut Map<String, Value>,
) {
    if let Some(value) = pointer_first(root, pointers) {
        out.insert(key.to_string(), value.clone());
    }
}

fn pointer_first<'a>(root: &'a Value, pointers: &[&str]) -> Option<&'a Value> {
    for pointer in pointers {
        if let Some(value) = root.pointer(pointer) {
            if !value.is_null() {
                return Some(value);
            }
        }
    }
    None
}

fn find_variant_inspection<'a>(
    inspection: &'a VariantInspectionSet,
    variant_id: &str,
) -> Result<&'a VariantInspection> {
    let wanted = variant_id.trim();
    inspection
        .variants
        .iter()
        .find(|variant| variant.id == wanted)
        .ok_or_else(|| anyhow!("variant '{}' not found in resolved_variants.json", variant_id))
}

fn build_variants_list_table(inspection: &VariantInspectionSet) -> lab_analysis::QueryTable {
    let rows = inspection
        .variants
        .iter()
        .map(|variant| {
            vec![
                Value::String(if variant.is_baseline { "yes" } else { "no" }.to_string()),
                Value::String(variant.id.clone()),
                Value::String(variant.variant_digest.clone()),
                variant
                    .code_surface
                    .get("artifact_digest")
                    .cloned()
                    .unwrap_or(Value::Null),
                variant
                    .code_surface
                    .get("image")
                    .cloned()
                    .unwrap_or(Value::Null),
                variant
                    .agent_ref
                    .as_ref()
                    .map(|value| Value::String(value.clone()))
                    .unwrap_or(Value::Null),
            ]
        })
        .collect();

    lab_analysis::QueryTable {
        columns: vec![
            "baseline".to_string(),
            "variant_id".to_string(),
            "variant_digest".to_string(),
            "artifact_digest".to_string(),
            "image".to_string(),
            "agent_ref".to_string(),
        ],
        rows,
    }
}

fn build_variant_show_table(variant: &VariantInspection) -> lab_analysis::QueryTable {
    let mut rows = Vec::new();
    rows.push(vec![json!("variant_id"), json!(variant.id)]);
    rows.push(vec![json!("is_baseline"), json!(variant.is_baseline)]);
    rows.push(vec![json!("variant_digest"), json!(variant.variant_digest)]);
    rows.push(vec![json!("agent_ref"), json!(variant.agent_ref)]);
    rows.push(vec![json!("code_surface"), variant.code_surface.clone()]);
    rows.push(vec![json!("bindings"), variant.behavior_surface["bindings"].clone()]);
    rows.push(vec![json!("args"), variant.behavior_surface["args"].clone()]);
    rows.push(vec![json!("env"), variant.behavior_surface["env"].clone()]);
    rows.push(vec![json!("image"), variant.behavior_surface["image"].clone()]);
    rows.push(vec![json!("runtime"), variant.behavior_surface["runtime"].clone()]);
    rows.push(vec![json!("raw_variant"), variant.raw_variant.clone()]);

    lab_analysis::QueryTable {
        columns: vec!["field".to_string(), "value".to_string()],
        rows,
    }
}

fn diff_variant_surfaces(left: &VariantInspection, right: &VariantInspection) -> Vec<JsonDiffRow> {
    let mut rows = Vec::new();
    collect_json_diffs(
        "",
        &left.behavior_surface,
        &right.behavior_surface,
        &mut rows,
    );
    rows
}

fn collect_json_diffs(path: &str, left: &Value, right: &Value, out: &mut Vec<JsonDiffRow>) {
    if left == right {
        return;
    }

    match (left, right) {
        (Value::Object(left_map), Value::Object(right_map)) => {
            let mut keys = BTreeSet::new();
            keys.extend(left_map.keys().cloned());
            keys.extend(right_map.keys().cloned());
            for key in keys {
                let next_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };
                match (left_map.get(&key), right_map.get(&key)) {
                    (Some(next_left), Some(next_right)) => {
                        collect_json_diffs(&next_path, next_left, next_right, out);
                    }
                    (Some(next_left), None) => out.push(JsonDiffRow {
                        path: next_path,
                        left: next_left.clone(),
                        right: Value::Null,
                    }),
                    (None, Some(next_right)) => out.push(JsonDiffRow {
                        path: next_path,
                        left: Value::Null,
                        right: next_right.clone(),
                    }),
                    (None, None) => {}
                }
            }
        }
        (Value::Array(_), Value::Array(_)) => out.push(JsonDiffRow {
            path: path.to_string(),
            left: left.clone(),
            right: right.clone(),
        }),
        _ => out.push(JsonDiffRow {
            path: path.to_string(),
            left: left.clone(),
            right: right.clone(),
        }),
    }
}

fn build_variant_diff_table(
    left: &VariantInspection,
    right: &VariantInspection,
    diffs: &[JsonDiffRow],
) -> lab_analysis::QueryTable {
    let mut rows = vec![vec![
        json!("variant_digest"),
        json!(left.variant_digest),
        json!(right.variant_digest),
    ]];
    rows.extend(diffs.iter().map(|entry| {
        vec![
            Value::String(entry.path.clone()),
            entry.left.clone(),
            entry.right.clone(),
        ]
    }));
    lab_analysis::QueryTable {
        columns: vec!["path".to_string(), left.id.clone(), right.id.clone()],
        rows,
    }
}

fn render_variants_table(
    title: &str,
    table: &lab_analysis::QueryTable,
    render_format: TableRenderFormat,
) {
    match render_format {
        TableRenderFormat::Csv => print_query_table_csv(table),
        TableRenderFormat::Markdown => print_table_markdown(table),
        TableRenderFormat::Html => print_table_html_document(title, table),
        TableRenderFormat::Text => print_query_table(table),
    }
}

fn variant_inspection_to_json(variant: &VariantInspection) -> Value {
    json!({
        "id": variant.id,
        "is_baseline": variant.is_baseline,
        "variant_digest": variant.variant_digest,
        "agent_ref": variant.agent_ref,
        "code_surface": variant.code_surface,
        "behavior_surface": variant.behavior_surface,
        "raw_variant": variant.raw_variant,
    })
}

fn merge_json_value(base: &mut Value, patch: &Value) {
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

fn table_render_format(csv: bool, md: bool, html: bool) -> TableRenderFormat {
    if csv {
        TableRenderFormat::Csv
    } else if md {
        TableRenderFormat::Markdown
    } else if html {
        TableRenderFormat::Html
    } else {
        TableRenderFormat::Text
    }
}

fn standard_views_for_set(view_set: lab_analysis::ViewSet) -> &'static [StandardViewDef] {
    match view_set {
        lab_analysis::ViewSet::CoreOnly => STANDARD_VIEWS_CORE_ONLY,
        lab_analysis::ViewSet::AbTest => STANDARD_VIEWS_AB_TEST,
        lab_analysis::ViewSet::MultiVariant => STANDARD_VIEWS_MULTI_VARIANT,
        lab_analysis::ViewSet::ParameterSweep => STANDARD_VIEWS_PARAMETER_SWEEP,
        lab_analysis::ViewSet::Regression => STANDARD_VIEWS_REGRESSION,
    }
}

fn standard_view_source_label(def: &StandardViewDef) -> &'static str {
    match def.plan {
        ViewQueryPlan::Source(source) => source,
        ViewQueryPlan::AbComparisonSummary => "win_loss_tie+effect_size+mcnemar_contingency",
        ViewQueryPlan::Scoreboard => "scoreboard (dynamic)",
    }
}

fn normalize_view_key(input: &str) -> String {
    input.trim().replace('-', "_").to_ascii_lowercase()
}

fn find_raw_view_name(raw_view_names: &[String], key: &str) -> Option<String> {
    raw_view_names
        .iter()
        .find(|name| normalize_view_key(name) == key)
        .cloned()
}

fn resolved_view_from_def(view_set: lab_analysis::ViewSet, def: &StandardViewDef) -> ResolvedView {
    match def.plan {
        ViewQueryPlan::Source(source) => ResolvedView {
            name: def.name.to_string(),
            source: Some(source.to_string()),
            plan: ResolvedViewPlan::Source(source.to_string()),
            standardize_ab_terms: matches!(view_set, lab_analysis::ViewSet::AbTest),
        },
        ViewQueryPlan::AbComparisonSummary => ResolvedView {
            name: def.name.to_string(),
            source: Some(standard_view_source_label(def).to_string()),
            plan: ResolvedViewPlan::AbComparisonSummary,
            standardize_ab_terms: false,
        },
        ViewQueryPlan::Scoreboard => ResolvedView {
            name: def.name.to_string(),
            source: None,
            plan: ResolvedViewPlan::Scoreboard,
            standardize_ab_terms: false,
        },
    }
}

fn resolve_requested_view(
    view_set: lab_analysis::ViewSet,
    raw_view_names: &[String],
    requested: &str,
) -> Result<ResolvedView> {
    let normalized = normalize_view_key(requested);
    if normalized.is_empty() {
        return Err(anyhow::anyhow!("view name cannot be empty"));
    }

    for def in standard_views_for_set(view_set) {
        if normalize_view_key(def.name) == normalized
            || def
                .aliases
                .iter()
                .any(|alias| normalize_view_key(alias) == normalized)
        {
            return Ok(resolved_view_from_def(view_set, def));
        }
    }

    let legacy_key = normalize_view_name(requested);
    if let Some(raw_name) = find_raw_view_name(raw_view_names, &legacy_key) {
        return Ok(ResolvedView {
            name: raw_name.clone(),
            source: Some(raw_name.clone()),
            plan: ResolvedViewPlan::Source(raw_name),
            standardize_ab_terms: false,
        });
    }
    if let Some(raw_name) = find_raw_view_name(raw_view_names, &normalized) {
        return Ok(ResolvedView {
            name: raw_name.clone(),
            source: Some(raw_name.clone()),
            plan: ResolvedViewPlan::Source(raw_name),
            standardize_ab_terms: false,
        });
    }

    let available = standard_views_for_set(view_set)
        .iter()
        .map(|def| def.name)
        .collect::<Vec<_>>()
        .join(", ");
    Err(anyhow::anyhow!(format!(
        "unknown view '{}'. standardized views for {}: {}",
        requested,
        view_set.as_str(),
        available
    )))
}

fn query_resolved_view(
    run_dir: &Path,
    resolved: &ResolvedView,
    limit: usize,
) -> Result<lab_analysis::QueryTable> {
    let table = match &resolved.plan {
        ResolvedViewPlan::Source(source) => query_source_view(run_dir, source, limit)?,
        ResolvedViewPlan::AbComparisonSummary => query_ab_comparison_summary(run_dir)?,
        ResolvedViewPlan::Scoreboard => query_scoreboard(run_dir)?,
    };

    if resolved.standardize_ab_terms {
        return Ok(standardize_ab_table_columns(&table));
    }
    Ok(table)
}

fn run_interactive_views_browser(
    project_root: &Path,
    initial_run_dir: Option<PathBuf>,
    initial_view: Option<&str>,
    sleep_interval: Duration,
    limit: usize,
) -> Result<()> {
    let mut term = tui::Term::new()?;
    let can_return_to_run_picker = initial_run_dir.is_none();
    let mut run_entries = collect_run_inventory(project_root)?;
    let mut current_run_dir = initial_run_dir;
    let mut current_view = None;
    let mut selected_run_idx = 0usize;
    let mut selected_view_idx = 0usize;

    if let Some(run_dir) = current_run_dir.as_ref() {
        if let Some(idx) = run_entries
            .iter()
            .position(|entry| entry.run_dir == *run_dir)
        {
            selected_run_idx = idx;
        }
        if let Some(requested_view) = initial_view {
            let run_view_set = lab_analysis::run_view_set(run_dir)?;
            let raw_view_names = lab_analysis::list_views(run_dir)?;
            let resolved = resolve_requested_view(run_view_set, &raw_view_names, requested_view)?;
            selected_view_idx = standard_views_for_set(run_view_set)
                .iter()
                .position(|def| def.name == resolved.name)
                .unwrap_or(0);
            current_view = Some(resolved);
        }
    }

    let mut screen = match (&current_run_dir, &current_view) {
        (Some(_), Some(_)) => ViewsBrowserScreen::Viewer,
        (Some(_), None) => ViewsBrowserScreen::ViewPicker,
        (None, _) => ViewsBrowserScreen::RunPicker,
    };
    term.set_selected(match screen {
        ViewsBrowserScreen::RunPicker => selection_for_len(
            selected_run_idx,
            run_entries
                .iter()
                .filter(|entry| entry.control.is_active)
                .count(),
        ),
        ViewsBrowserScreen::ViewPicker => Some(selected_view_idx),
        ViewsBrowserScreen::Viewer => Some(0),
    });

    loop {
        match screen {
            ViewsBrowserScreen::RunPicker => {
                run_entries = collect_run_inventory(project_root)?;
                let active_run_entries = run_entries
                    .iter()
                    .filter(|entry| entry.control.is_active)
                    .cloned()
                    .collect::<Vec<_>>();
                selected_run_idx = clamp_index(
                    if let Some(run_dir) = current_run_dir.as_ref() {
                        active_run_entries
                            .iter()
                            .position(|entry| entry.run_dir == *run_dir)
                            .unwrap_or(selected_run_idx)
                    } else {
                        selected_run_idx
                    },
                    active_run_entries.len(),
                );
                let run_items = build_run_browser_items(&active_run_entries);
                term.set_selected(selection_for_len(
                    selected_run_idx,
                    active_run_entries.len(),
                ));
                term.draw(&tui::Screen::RunBrowser(tui::RunBrowserState {
                    items: &run_items,
                    refresh_secs: sleep_interval.as_secs(),
                }))?;

                match term.poll(sleep_interval)? {
                    tui::Action::Quit => break,
                    tui::Action::Back => break,
                    tui::Action::Select => {
                        if let Some(entry) = active_run_entries.get(selected_run_idx) {
                            current_run_dir = Some(entry.run_dir.clone());
                            selected_view_idx = 0;
                            screen = ViewsBrowserScreen::ViewPicker;
                            term.set_selected(Some(0));
                        }
                    }
                    tui::Action::ScrollUp => {
                        term.scroll_up();
                        selected_run_idx = term.selected().unwrap_or(0);
                    }
                    tui::Action::ScrollDown => {
                        term.scroll_down(active_run_entries.len());
                        selected_run_idx = term.selected().unwrap_or(0);
                    }
                    tui::Action::PageUp => {
                        term.page_up();
                        selected_run_idx = term.selected().unwrap_or(0);
                    }
                    tui::Action::PageDown => {
                        term.page_down(active_run_entries.len());
                        selected_run_idx = term.selected().unwrap_or(0);
                    }
                    tui::Action::Refresh | tui::Action::Tick => {}
                }
            }
            ViewsBrowserScreen::ViewPicker => {
                let run_dir = current_run_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("interactive view picker requires a selected run"))?;
                let run_entry = lookup_run_inventory(&run_entries, run_dir)
                    .unwrap_or_else(|| inspect_run_inventory_entry(run_dir));
                let run_view_set = lab_analysis::run_view_set(run_dir)?;
                let standard_views = standard_views_for_set(run_view_set);
                selected_view_idx = clamp_index(selected_view_idx, standard_views.len());
                let view_items = build_view_browser_items(run_view_set);
                term.set_selected(selection_for_len(selected_view_idx, standard_views.len()));
                term.draw(&tui::Screen::ViewBrowser(tui::ViewBrowserState {
                    run_id: &run_entry.run_id,
                    experiment: &run_entry.experiment,
                    started_at: &run_entry.started_at_display,
                    status: &run_entry.control.status_display,
                    items: &view_items,
                    refresh_secs: sleep_interval.as_secs(),
                }))?;

                match term.poll(sleep_interval)? {
                    tui::Action::Quit => break,
                    tui::Action::Back => {
                        if can_return_to_run_picker {
                            screen = ViewsBrowserScreen::RunPicker;
                            let active_len = run_entries
                                .iter()
                                .filter(|entry| entry.control.is_active)
                                .count();
                            term.set_selected(selection_for_len(selected_run_idx, active_len));
                        } else {
                            break;
                        }
                    }
                    tui::Action::Select => {
                        if let Some(def) = standard_views.get(selected_view_idx) {
                            current_view = Some(resolved_view_from_def(run_view_set, def));
                            screen = ViewsBrowserScreen::Viewer;
                            term.set_selected(Some(0));
                        }
                    }
                    tui::Action::ScrollUp => {
                        term.scroll_up();
                        selected_view_idx = term.selected().unwrap_or(0);
                    }
                    tui::Action::ScrollDown => {
                        term.scroll_down(standard_views.len());
                        selected_view_idx = term.selected().unwrap_or(0);
                    }
                    tui::Action::PageUp => {
                        term.page_up();
                        selected_view_idx = term.selected().unwrap_or(0);
                    }
                    tui::Action::PageDown => {
                        term.page_down(standard_views.len());
                        selected_view_idx = term.selected().unwrap_or(0);
                    }
                    tui::Action::Refresh | tui::Action::Tick => {}
                }
            }
            ViewsBrowserScreen::Viewer => {
                let run_dir = current_run_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("interactive viewer requires a selected run"))?;
                let run_entry = lookup_run_inventory(&run_entries, run_dir)
                    .unwrap_or_else(|| inspect_run_inventory_entry(run_dir));
                let run_view_set = lab_analysis::run_view_set(run_dir)?;
                let raw_view_names = lab_analysis::list_views(run_dir)?;
                let resolved_view = match current_view.clone() {
                    Some(view) => view,
                    None => resolve_requested_view(run_view_set, &raw_view_names, "run_progress")?,
                };
                current_view = Some(resolved_view.clone());

                let table = query_resolved_view(run_dir, &resolved_view, limit)?;
                let (display, legend, split_labels) =
                    if resolved_view.name == "trace" && has_ab_trace_columns(&table) {
                        let (d, l, s) = prepare_trace_split_view(&table);
                        (d, l, Some(s))
                    } else {
                        let (filtered, raw_legend) = elide_constant_columns(&table);
                        let display = shorten_display_columns(&filtered);
                        let legend: Vec<(String, String)> = raw_legend
                            .into_iter()
                            .map(|(k, v)| (shorten_column_name(&k), v))
                            .collect();
                        (display, legend, None)
                    };

                let hints = [
                    tui::KeyHint {
                        key: "Esc",
                        label: "views",
                    },
                    tui::KeyHint {
                        key: "q",
                        label: "quit",
                    },
                    tui::KeyHint {
                        key: "r",
                        label: "refresh",
                    },
                ];
                let split_refs = split_labels.as_ref().map(|(l, r)| (l.as_str(), r.as_str()));
                term.draw(&tui::Screen::LiveView(tui::ViewState {
                    run_id: &run_entry.run_id,
                    status: &run_entry.control.status_display,
                    started_at: &run_entry.started_at_display,
                    view_name: &resolved_view.name,
                    interval_secs: sleep_interval.as_secs(),
                    table: &display,
                    progress: read_run_progress(run_dir),
                    legend: &legend,
                    split_labels: split_refs,
                    hints: &hints,
                }))?;

                match term.poll(sleep_interval)? {
                    tui::Action::Quit => break,
                    tui::Action::Back => {
                        selected_view_idx = standard_views_for_set(run_view_set)
                            .iter()
                            .position(|def| def.name == resolved_view.name)
                            .unwrap_or(0);
                        screen = ViewsBrowserScreen::ViewPicker;
                        term.set_selected(Some(selected_view_idx));
                    }
                    tui::Action::ScrollUp => term.scroll_up(),
                    tui::Action::ScrollDown => term.scroll_down(display.rows.len()),
                    tui::Action::PageUp => term.page_up(),
                    tui::Action::PageDown => term.page_down(display.rows.len()),
                    tui::Action::Select | tui::Action::Refresh | tui::Action::Tick => {}
                }
            }
        }
    }

    Ok(())
}

fn selection_for_len(index: usize, len: usize) -> Option<usize> {
    if len == 0 {
        None
    } else {
        Some(clamp_index(index, len))
    }
}

fn clamp_index(index: usize, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        index.min(len.saturating_sub(1))
    }
}

fn build_run_browser_items(entries: &[RunInventoryEntry]) -> Vec<tui::RunBrowserItem> {
    entries
        .iter()
        .map(|entry| tui::RunBrowserItem {
            run_id: entry.run_id.clone(),
            experiment: display_or_dash(&entry.experiment),
            started_at: entry.started_at_display.clone(),
            status: entry.control.status.clone(),
            status_detail: entry.control.status_display.clone(),
            active_trials: entry.control.active_trials,
        })
        .collect()
}

fn build_view_browser_items(view_set: lab_analysis::ViewSet) -> Vec<tui::ViewBrowserItem> {
    standard_views_for_set(view_set)
        .iter()
        .map(|def| tui::ViewBrowserItem {
            name: def.name.to_string(),
            source_view: standard_view_source_label(def).to_string(),
            purpose: def.purpose.to_string(),
        })
        .collect()
}

fn lookup_run_inventory(
    entries: &[RunInventoryEntry],
    run_dir: &Path,
) -> Option<RunInventoryEntry> {
    entries
        .iter()
        .find(|entry| entry.run_dir == run_dir)
        .cloned()
}

fn query_source_view(
    run_dir: &Path,
    source_view: &str,
    limit: usize,
) -> Result<lab_analysis::QueryTable> {
    if limit == 0 {
        let sql = format!("SELECT * FROM {}", sql_identifier(source_view));
        return lab_analysis::query_run(run_dir, &sql);
    }
    lab_analysis::query_view(run_dir, source_view, limit)
}

fn query_ab_comparison_summary(run_dir: &Path) -> Result<lab_analysis::QueryTable> {
    let sql = "WITH delta AS (
            SELECT
                coalesce(max(CASE WHEN delta_type = 'regression' THEN n END), 0) AS variant_a_better_n,
                coalesce(max(CASE WHEN delta_type = 'improvement' THEN n END), 0) AS variant_b_better_n,
                coalesce(max(CASE WHEN delta_type = 'same' THEN n END), 0) AS same_outcome_n,
                coalesce(max(CASE WHEN delta_type = 'changed' THEN n END), 0) AS changed_outcome_n,
                coalesce(max(CASE WHEN delta_type = 'regression' THEN pct END), 0.0) AS variant_a_better_pct,
                coalesce(max(CASE WHEN delta_type = 'improvement' THEN pct END), 0.0) AS variant_b_better_pct,
                coalesce(max(CASE WHEN delta_type = 'same' THEN pct END), 0.0) AS same_outcome_pct,
                coalesce(max(CASE WHEN delta_type = 'changed' THEN pct END), 0.0) AS changed_outcome_pct
            FROM win_loss_tie
        ),
        effect AS (
            SELECT
                baseline_rate AS variant_a_rate,
                treatment_rate AS variant_b_rate,
                absolute_diff AS variant_b_minus_variant_a,
                cohens_h,
                magnitude
            FROM effect_size
        ),
        mcnemar AS (
            SELECT
                both_pass,
                base_only AS variant_a_only,
                treat_only AS variant_b_only,
                both_fail,
                mcnemar_chi2
            FROM mcnemar_contingency
        )
        SELECT
            effect.variant_a_rate,
            effect.variant_b_rate,
            effect.variant_b_minus_variant_a,
            delta.variant_a_better_n,
            delta.variant_b_better_n,
            delta.same_outcome_n,
            delta.changed_outcome_n,
            delta.variant_a_better_pct,
            delta.variant_b_better_pct,
            delta.same_outcome_pct,
            delta.changed_outcome_pct,
            mcnemar.both_pass,
            mcnemar.variant_a_only,
            mcnemar.variant_b_only,
            mcnemar.both_fail,
            mcnemar.mcnemar_chi2,
            effect.cohens_h,
            effect.magnitude
        FROM effect
        CROSS JOIN delta
        CROSS JOIN mcnemar";
    lab_analysis::query_run(run_dir, sql)
}

fn standardize_ab_table_columns(table: &lab_analysis::QueryTable) -> lab_analysis::QueryTable {
    lab_analysis::QueryTable {
        columns: table
            .columns
            .iter()
            .map(|name| standardize_ab_column_name(name))
            .collect(),
        rows: table.rows.clone(),
    }
}

fn standardize_ab_column_name(name: &str) -> String {
    match name {
        "baseline_id" | "a_variant_id" => "variant_a_id".to_string(),
        "treatment_id" | "b_variant_id" => "variant_b_id".to_string(),
        "treatment_variant_count" => "comparison_variant_count".to_string(),
        "baseline_outcome" => "variant_a_outcome".to_string(),
        "treatment_outcome" => "variant_b_outcome".to_string(),
        "baseline_metric" => "variant_a_metric".to_string(),
        "treatment_metric" => "variant_b_metric".to_string(),
        "baseline_rate" => "variant_a_rate".to_string(),
        "treatment_rate" => "variant_b_rate".to_string(),
        "base_only" => "variant_a_only".to_string(),
        "treat_only" => "variant_b_only".to_string(),
        "a_trial_id" => "variant_a_trial_id".to_string(),
        "b_trial_id" => "variant_b_trial_id".to_string(),
        other => {
            if let Some(rest) = other.strip_prefix("a_") {
                return format!("variant_a_{}", rest);
            }
            if let Some(rest) = other.strip_prefix("b_") {
                return format!("variant_b_{}", rest);
            }
            if let Some(rest) = other.strip_prefix("d_") {
                return format!("delta_{}", rest);
            }
            other.to_string()
        }
    }
}

struct ScoreboardMeta {
    experiment_id: String,
    baseline_id: String,
    comparison: String,
}

fn read_scoreboard_metadata(run_dir: &Path) -> ScoreboardMeta {
    let path = run_dir.join("resolved_experiment.json");
    let resolved = match std::fs::read_to_string(&path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
    {
        Some(v) => v,
        None => {
            return ScoreboardMeta {
                experiment_id: String::new(),
                baseline_id: String::new(),
                comparison: String::new(),
            }
        }
    };

    let experiment_id = resolved
        .pointer("/id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let baseline_id = resolved
        .pointer("/baseline/variant_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let comparison = resolved
        .pointer("/design/policies/comparison")
        .and_then(Value::as_str)
        .or_else(|| {
            resolved
                .pointer("/design/comparison")
                .and_then(Value::as_str)
        })
        .unwrap_or("")
        .to_string();

    ScoreboardMeta {
        experiment_id,
        baseline_id,
        comparison,
    }
}

/// Fetch per-variant bindings as a compact display string.
/// Returns a map of variant_id → "key1=val1, key2=val2" (empty string if no bindings).
fn fetch_variant_bindings(run_dir: &Path) -> BTreeMap<String, String> {
    let sql = "SELECT variant_id, first(bindings) AS bindings FROM trials GROUP BY variant_id";
    let table = match lab_analysis::query_run(run_dir, sql) {
        Ok(t) => t,
        Err(_) => return BTreeMap::new(),
    };
    let mut out = BTreeMap::new();
    for row in &table.rows {
        let variant = row
            .first()
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let bindings_val = row.get(1).cloned().unwrap_or(Value::Null);
        let compact = match &bindings_val {
            Value::Object(map) => map
                .iter()
                .map(|(k, v)| {
                    let val_str = match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    format!("{}={}", k, truncate_cell(&val_str, 24))
                })
                .collect::<Vec<_>>()
                .join(", "),
            Value::Null => String::new(),
            other => {
                let s = other.to_string();
                if s == "null" {
                    String::new()
                } else {
                    truncate_cell(&s, 60).to_string()
                }
            }
        };
        if !variant.is_empty() {
            out.insert(variant, compact);
        }
    }
    out
}

fn build_live_scoreboard_table(
    run_dir: &Path,
    metric_limit: usize,
) -> Result<lab_analysis::QueryTable> {
    let limit = metric_limit.clamp(1, 32);
    let metric_names = fetch_scoreboard_metric_names(run_dir, limit)?;
    let sql = build_scoreboard_sql(&metric_names);
    lab_analysis::query_run(run_dir, &sql)
}

fn build_inflight_scoreboard_table(run_dir: &Path) -> Option<lab_analysis::QueryTable> {
    let parsed = load_run_control(run_dir)?;
    let active_trials = parsed.get("active_trials").and_then(Value::as_object)?;
    if active_trials.is_empty() {
        return None;
    }

    let mut rows: Vec<(i64, String, Vec<Value>)> = Vec::with_capacity(active_trials.len());
    for (trial_key, entry) in active_trials {
        let trial_id = entry
            .get("trial_id")
            .and_then(Value::as_str)
            .unwrap_or(trial_key)
            .to_string();
        let variant_id = entry
            .get("variant_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let schedule_idx = entry
            .get("schedule_idx")
            .and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|u| u as i64)));
        let worker_id = entry
            .get("worker_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let started_at = entry
            .get("started_at")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();

        let row = vec![
            Value::String(variant_id),
            Value::String(trial_id.clone()),
            schedule_idx.map_or(Value::Null, |idx| json!(idx)),
            Value::String(worker_id),
            Value::String(started_at),
            Value::String("in_flight".to_string()),
        ];
        rows.push((schedule_idx.unwrap_or(i64::MAX), trial_id, row));
    }

    rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let sorted_rows = rows.into_iter().map(|(_, _, row)| row).collect();

    Some(lab_analysis::QueryTable {
        columns: vec![
            "variant_id".to_string(),
            "trial_id".to_string(),
            "schedule_idx".to_string(),
            "worker_id".to_string(),
            "started_at".to_string(),
            "lifecycle".to_string(),
        ],
        rows: sorted_rows,
    })
}

fn query_scoreboard(run_dir: &Path) -> Result<lab_analysis::QueryTable> {
    let table = build_live_scoreboard_table(run_dir, 8)?;
    if !table.rows.is_empty() {
        return Ok(table);
    }
    if let Some(inflight) = build_inflight_scoreboard_table(run_dir) {
        return Ok(inflight);
    }
    Ok(table)
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

fn print_scoreboard_grouped_by_variant(
    table: &lab_analysis::QueryTable,
    variant_bindings: &BTreeMap<String, String>,
) {
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

    for (variant, rows) in &per_variant {
        let bindings_str = variant_bindings
            .get(variant.as_str())
            .filter(|b| !b.is_empty());
        match bindings_str {
            Some(b) => println!("== {} ({}) ==", variant, b),
            None => println!("== {} ==", variant),
        }
        print_scoreboard_table(
            &lab_analysis::QueryTable {
                columns: columns.clone(),
                rows: rows.clone(),
            },
            term_w,
        );
        println!();
    }
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
        let mid = (lo + hi).div_ceil(2);
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
    summarize_run_control(load_run_control(run_dir).as_ref()).status_display
}

fn read_run_progress(run_dir: &Path) -> Option<(usize, usize)> {
    let sqlite_path = run_dir.join("run.sqlite");
    let conn = Connection::open(sqlite_path).ok()?;
    let raw: String = conn
        .query_row(
            "SELECT value_json FROM runtime_kv WHERE key=?1",
            params!["schedule_progress_v2"],
            |row| row.get(0),
        )
        .optional()
        .ok()??;
    let value: Value = serde_json::from_str(&raw).ok()?;
    let total = value.get("total_slots")?.as_u64()? as usize;
    let completed = value.get("completed_slots")?.as_array()?.len();
    if total == 0 {
        return None;
    }
    Some((completed, total))
}

fn stdout_is_tty() -> bool {
    unsafe { libc::isatty(libc::STDOUT_FILENO) == 1 }
}

fn unix_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Transform the trace side-by-side view into a compact split layout:
/// two minimal column sets (event, turn, tool, status-dot) separated by ┃,
/// with variant IDs as panel labels above each table half.
fn prepare_trace_split_view(
    table: &lab_analysis::QueryTable,
) -> (
    lab_analysis::QueryTable,
    Vec<(String, String)>,
    (String, String),
) {
    let idx = |name: &str| table.columns.iter().position(|c| c == name);
    let get = |row: &[Value], col: Option<usize>| -> Value {
        col.and_then(|i| row.get(i)).cloned().unwrap_or(Value::Null)
    };
    let task_id = idx("task_id");
    let a_id = idx("variant_a_id");
    let b_id = idx("variant_b_id");
    let a_event = idx("variant_a_event_type");
    let b_event = idx("variant_b_event_type");
    let a_turn = idx("variant_a_turn_index");
    let b_turn = idx("variant_b_turn_index");
    let a_tool = idx("variant_a_tool");
    let b_tool = idx("variant_b_tool");
    let a_status = idx("variant_a_status");
    let b_status = idx("variant_b_status");

    // Panel labels from first row
    let (left_label, right_label) = table
        .rows
        .first()
        .map(|first| {
            let a = a_id
                .and_then(|i| first.get(i))
                .and_then(Value::as_str)
                .unwrap_or("variant a")
                .to_string();
            let b = b_id
                .and_then(|i| first.get(i))
                .and_then(Value::as_str)
                .unwrap_or("variant b")
                .to_string();
            (a, b)
        })
        .unwrap_or_else(|| ("variant a".into(), "variant b".into()));

    let to_dot = |row: &[Value], col: Option<usize>| -> Value {
        match col
            .and_then(|i| row.get(i))
            .and_then(Value::as_str)
            .unwrap_or("")
        {
            "" => Value::Null,
            s if s.contains("success") || s == "ok" || s.starts_with('2') || s == "pass" => {
                Value::String("●".to_string())
            }
            _ => Value::String("✗".to_string()),
        }
    };

    // Same column names on both sides — the panel labels tell you which is which
    let columns = vec![
        "task".into(),
        "event".into(),
        "turn".into(),
        "tool".into(),
        "st".into(),
        "┃".into(),
        "event".into(),
        "turn".into(),
        "tool".into(),
        "st".into(),
    ];

    let rows: Vec<Vec<Value>> = table
        .rows
        .iter()
        .map(|row| {
            vec![
                get(row, task_id),
                get(row, a_event),
                get(row, a_turn),
                get(row, a_tool),
                to_dot(row, a_status),
                Value::String("┃".to_string()),
                get(row, b_event),
                get(row, b_turn),
                get(row, b_tool),
                to_dot(row, b_status),
            ]
        })
        .collect();

    let compact = lab_analysis::QueryTable { columns, rows };

    // Manually elide task if constant (can't use elide_constant_columns —
    // it would also eat the ┃ separator column since it's constant).
    let task_col = 0;
    let task_is_constant = compact.rows.len() > 1 && {
        let first = compact.rows[0].get(task_col);
        compact.rows.iter().all(|r| r.get(task_col) == first)
    };

    let (filtered, legend) = if task_is_constant {
        let val = compact.rows[0]
            .get(task_col)
            .map(render_json_cell)
            .unwrap_or_default();
        let legend = vec![("task".to_string(), val)];
        let columns = compact.columns[1..].to_vec();
        let rows = compact
            .rows
            .into_iter()
            .map(|mut r| {
                r.remove(0);
                r
            })
            .collect();
        (lab_analysis::QueryTable { columns, rows }, legend)
    } else {
        (compact, Vec::new())
    };

    (filtered, legend, (left_label, right_label))
}

fn has_ab_trace_columns(table: &lab_analysis::QueryTable) -> bool {
    let has = |name: &str| table.columns.iter().any(|c| c == name);
    has("variant_a_event_type") && has("variant_b_event_type")
}

/// Shorten column names for display. Strips verbose prefixes that waste
/// horizontal space in tables without losing meaning (`a_`/`b_` already
/// encodes the variant side).
fn shorten_column_name(name: &str) -> String {
    if let Some(rest) = name.strip_prefix("variant_a_") {
        return format!("a_{rest}");
    }
    if let Some(rest) = name.strip_prefix("variant_b_") {
        return format!("b_{rest}");
    }
    if let Some(rest) = name.strip_prefix("delta_") {
        return format!("d_{rest}");
    }
    name.to_string()
}

fn shorten_display_columns(table: &lab_analysis::QueryTable) -> lab_analysis::QueryTable {
    lab_analysis::QueryTable {
        columns: table
            .columns
            .iter()
            .map(|c| shorten_column_name(c))
            .collect(),
        rows: table.rows.clone(),
    }
}

fn normalize_view_name(input: &str) -> String {
    let normalized = input.trim().replace('-', "_");
    match normalized.as_str() {
        "paired_diffs" => "paired_outcomes".to_string(),
        "task_compare" | "task_comparison" | "by_task" | "task_table" | "ab_task_table" => {
            "ab_task_metrics_side_by_side".to_string()
        }
        "trace_diff" | "trace_compare" | "trace_side_by_side" => {
            "ab_trace_row_side_by_side".to_string()
        }
        "turn_diff" | "turn_compare" | "turn_side_by_side" | "trace_turns" => {
            "ab_turn_side_by_side".to_string()
        }
        "outcome_compare" => "ab_task_outcomes".to_string(),
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

/// Detect columns where every row has the same value.
/// Returns a filtered table (constant columns removed) and the elided (name, value) pairs.
fn elide_constant_columns(
    table: &lab_analysis::QueryTable,
) -> (lab_analysis::QueryTable, Vec<(String, String)>) {
    if table.rows.len() <= 1 || table.columns.len() <= 1 {
        return (table.clone(), Vec::new());
    }

    let mut elided = Vec::new();
    let mut keep_indices = Vec::new();

    for (col_idx, col_name) in table.columns.iter().enumerate() {
        let first_val = table
            .rows
            .first()
            .and_then(|row| row.get(col_idx))
            .cloned()
            .unwrap_or(Value::Null);

        let all_same = table
            .rows
            .iter()
            .all(|row| row.get(col_idx).cloned().unwrap_or(Value::Null) == first_val);

        if all_same {
            elided.push((col_name.clone(), render_json_cell(&first_val)));
        } else {
            keep_indices.push(col_idx);
        }
    }

    if elided.is_empty() {
        return (table.clone(), Vec::new());
    }

    let new_columns: Vec<String> = keep_indices
        .iter()
        .map(|&idx| table.columns[idx].clone())
        .collect();
    let new_rows: Vec<Vec<Value>> = table
        .rows
        .iter()
        .map(|row| {
            keep_indices
                .iter()
                .map(|&idx| row.get(idx).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();

    (
        lab_analysis::QueryTable {
            columns: new_columns,
            rows: new_rows,
        },
        elided,
    )
}

fn print_query_table(table: &lab_analysis::QueryTable) {
    if table.columns.is_empty() {
        println!("(ok)");
        return;
    }

    let (filtered, elided) = elide_constant_columns(table);
    let display = shorten_display_columns(&filtered);

    if !elided.is_empty() {
        let meta_parts: Vec<String> = elided
            .iter()
            .map(|(k, v)| {
                let display_v = truncate_cell(v, 40);
                format!("{}={}", shorten_column_name(k), display_v)
            })
            .collect();
        println!("{}", meta_parts.join("  "));
        println!();
    }

    let term_w = terminal_width();
    if should_chunk_query_table(&display, term_w) {
        print_query_table_in_column_chunks(&display, term_w);
    } else {
        print_scoreboard_table(&display, term_w);
    }
}

fn print_special_split_view(
    run_dir: &Path,
    view_name: &str,
    table: &lab_analysis::QueryTable,
) -> bool {
    match view_name {
        "scoreboard" => {
            let meta = read_scoreboard_metadata(run_dir);
            if !meta.experiment_id.is_empty() {
                print!("experiment: {}", meta.experiment_id);
                if !meta.comparison.is_empty() {
                    print!("  comparison: {}", meta.comparison);
                }
                println!();
            }
            if !meta.baseline_id.is_empty() {
                println!("reference_variant: {}", meta.baseline_id);
            }
            let variant_bindings = fetch_variant_bindings(run_dir);
            print_scoreboard_grouped_by_variant(table, &variant_bindings);
            true
        }
        "task_outcomes" | "ab_task_outcomes" => {
            print_ab_task_outcomes_table(table);
            true
        }
        "trace" | "trace_compare" | "ab_trace_row_side_by_side" => {
            print_trace_compare_by_task(table);
            true
        }
        "turn_compare" | "ab_turn_side_by_side" => {
            print_variant_prefixed_tables(
                table,
                &["task_id", "repl_idx", "turn_index"],
                "variant_a_",
                "variant_b_",
                "variant_a_turns",
                "variant_b_turns",
            );
            true
        }
        _ => false,
    }
}

fn print_ab_task_outcomes_table(table: &lab_analysis::QueryTable) {
    let ordered = project_query_table_by_column_priority(
        table,
        &[
            "task_id",
            "variant_a_outcome",
            "a_outcome",
            "variant_b_outcome",
            "b_outcome",
            "variant_a_result_score",
            "a_result_score",
            "variant_b_result_score",
            "b_result_score",
            "variant_a_trial_id",
            "a_trial_id",
            "variant_b_trial_id",
            "b_trial_id",
            "delta_result_score",
            "d_result_score",
            "outcome_change",
            "repl_idx",
            "variant_a_id",
            "a_variant_id",
            "variant_b_id",
            "b_variant_id",
        ],
    );
    print_query_table_no_elision(&ordered);
}

#[derive(Clone, Debug)]
struct TraceSection {
    task_id: String,
    repl_idx: String,
    variant_a_id: String,
    variant_b_id: String,
    variant_a_trial_id: String,
    variant_b_trial_id: String,
    variant_a_table: lab_analysis::QueryTable,
    variant_b_table: lab_analysis::QueryTable,
}

fn first_non_null_column_value(table: &lab_analysis::QueryTable, column_name: &str) -> String {
    let Some(idx) = table.columns.iter().position(|c| c == column_name) else {
        return String::new();
    };
    for row in &table.rows {
        let value = row.get(idx).unwrap_or(&Value::Null);
        match value {
            Value::Null => {}
            Value::String(s) if s.trim().is_empty() => {}
            Value::String(s) => return s.to_string(),
            other => return render_json_cell(other),
        }
    }
    String::new()
}

fn build_trace_side_table(
    table: &lab_analysis::QueryTable,
    prefix: &str,
) -> lab_analysis::QueryTable {
    let desired = vec![
        ("row_seq".to_string(), "row"),
        (format!("{}event_type", prefix), "evt"),
        (format!("{}turn_index", prefix), "turn"),
        (format!("{}model", prefix), "model"),
        (format!("{}tool", prefix), "tool"),
        (format!("{}status", prefix), "st"),
        (format!("{}call_id", prefix), "call"),
    ];
    let mut indices = Vec::new();
    let mut columns = Vec::new();
    let mut event_idx_in_projection = None;
    for (column_name, short_name) in desired {
        if let Some(idx) = table.columns.iter().position(|c| c == &column_name) {
            if !indices.contains(&idx) {
                if short_name == "evt" {
                    event_idx_in_projection = Some(indices.len());
                }
                indices.push(idx);
                columns.push(short_name.to_string());
            }
        }
    }
    let rows = table
        .rows
        .iter()
        .map(|row| {
            indices
                .iter()
                .map(|idx| row.get(*idx).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        })
        .filter(|projected| {
            // Drop rows where this side has no event payload at all.
            // This removes FULL OUTER JOIN null spam when only the opposite variant emitted a row.
            match event_idx_in_projection
                .and_then(|idx| projected.get(idx))
                .unwrap_or(&Value::Null)
            {
                Value::Null => false,
                Value::String(s) if s.trim().is_empty() => false,
                _ => true,
            }
        })
        .collect::<Vec<_>>();
    lab_analysis::QueryTable { columns, rows }
}

fn build_trace_sections(table: &lab_analysis::QueryTable) -> Vec<TraceSection> {
    let task_col = table.columns.iter().position(|c| c == "task_id");
    let repl_col = table.columns.iter().position(|c| c == "repl_idx");
    let (Some(task_col), Some(repl_col)) = (task_col, repl_col) else {
        return Vec::new();
    };

    let mut grouped: BTreeMap<(String, String), Vec<Vec<Value>>> = BTreeMap::new();
    for row in &table.rows {
        let task = row
            .get(task_col)
            .map(render_json_cell)
            .unwrap_or_else(|| "unknown".to_string());
        let repl = row
            .get(repl_col)
            .map(render_json_cell)
            .unwrap_or_else(|| "unknown".to_string());
        grouped.entry((task, repl)).or_default().push(row.clone());
    }

    grouped
        .into_iter()
        .map(|((task_id, repl_idx), rows)| {
            let grouped_table = lab_analysis::QueryTable {
                columns: table.columns.clone(),
                rows,
            };
            TraceSection {
                task_id,
                repl_idx,
                variant_a_id: first_non_null_column_value(&grouped_table, "variant_a_id"),
                variant_b_id: first_non_null_column_value(&grouped_table, "variant_b_id"),
                variant_a_trial_id: first_non_null_column_value(
                    &grouped_table,
                    "variant_a_trial_id",
                ),
                variant_b_trial_id: first_non_null_column_value(
                    &grouped_table,
                    "variant_b_trial_id",
                ),
                variant_a_table: build_trace_side_table(&grouped_table, "variant_a_"),
                variant_b_table: build_trace_side_table(&grouped_table, "variant_b_"),
            }
        })
        .collect()
}

fn print_trace_compare_by_task(table: &lab_analysis::QueryTable) {
    let sections = build_trace_sections(table);
    if sections.is_empty() {
        print_query_table(table);
        return;
    }

    for section in sections {
        println!("== task={} repl={} ==", section.task_id, section.repl_idx);
        if !section.variant_a_id.is_empty() || !section.variant_a_trial_id.is_empty() {
            println!(
                "variant_a: {}  trial: {}",
                if section.variant_a_id.is_empty() {
                    "unknown"
                } else {
                    section.variant_a_id.as_str()
                },
                if section.variant_a_trial_id.is_empty() {
                    "unknown"
                } else {
                    section.variant_a_trial_id.as_str()
                }
            );
        }
        if !section.variant_b_id.is_empty() || !section.variant_b_trial_id.is_empty() {
            println!(
                "variant_b: {}  trial: {}",
                if section.variant_b_id.is_empty() {
                    "unknown"
                } else {
                    section.variant_b_id.as_str()
                },
                if section.variant_b_trial_id.is_empty() {
                    "unknown"
                } else {
                    section.variant_b_trial_id.as_str()
                }
            );
        }
        println!();
        println!("-- variant_a --");
        print_query_table(&section.variant_a_table);
        println!();
        println!("-- variant_b --");
        print_query_table(&section.variant_b_table);
        println!();
    }
}

fn print_query_table_no_elision(table: &lab_analysis::QueryTable) {
    if table.columns.is_empty() {
        println!("(ok)");
        return;
    }
    let term_w = terminal_width();
    if should_chunk_query_table(table, term_w) {
        print_query_table_in_column_chunks(table, term_w);
    } else {
        print_scoreboard_table(table, term_w);
    }
}

fn project_query_table_by_column_priority(
    table: &lab_analysis::QueryTable,
    priority_cols: &[&str],
) -> lab_analysis::QueryTable {
    let mut indices = Vec::new();
    for name in priority_cols {
        if let Some(idx) = table.columns.iter().position(|col| col == name) {
            if !indices.contains(&idx) {
                indices.push(idx);
            }
        }
    }
    for idx in 0..table.columns.len() {
        if !indices.contains(&idx) {
            indices.push(idx);
        }
    }
    project_query_table_columns(table, &indices)
}

fn print_variant_prefixed_tables(
    table: &lab_analysis::QueryTable,
    shared_priority_cols: &[&str],
    left_prefix: &str,
    right_prefix: &str,
    left_title: &str,
    right_title: &str,
) {
    let mut shared_indices = Vec::new();
    for &name in shared_priority_cols {
        if let Some(idx) = table.columns.iter().position(|col| col == name) {
            if !shared_indices.contains(&idx) {
                shared_indices.push(idx);
            }
        }
    }

    let left_indices: Vec<usize> = table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| col.starts_with(left_prefix).then_some(idx))
        .collect();
    let right_indices: Vec<usize> = table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| col.starts_with(right_prefix).then_some(idx))
        .collect();

    if left_indices.is_empty() || right_indices.is_empty() {
        print_query_table(table);
        return;
    }

    let left_table = project_query_table_columns_with_prefix_trim(
        table,
        &shared_indices,
        &left_indices,
        left_prefix,
    );
    let right_table = project_query_table_columns_with_prefix_trim(
        table,
        &shared_indices,
        &right_indices,
        right_prefix,
    );

    println!("== {} ==", left_title);
    print_query_table(&left_table);
    println!();
    println!("== {} ==", right_title);
    print_query_table(&right_table);
}

fn project_query_table_columns_with_prefix_trim(
    table: &lab_analysis::QueryTable,
    shared_indices: &[usize],
    side_indices: &[usize],
    side_prefix: &str,
) -> lab_analysis::QueryTable {
    let mut combined_indices = shared_indices.to_vec();
    combined_indices.extend(side_indices.iter().copied());

    let columns = combined_indices
        .iter()
        .filter_map(|idx| table.columns.get(*idx))
        .map(|col| {
            col.strip_prefix(side_prefix)
                .map(str::to_string)
                .unwrap_or_else(|| col.clone())
        })
        .collect::<Vec<_>>();

    let rows = table
        .rows
        .iter()
        .map(|row| {
            combined_indices
                .iter()
                .map(|idx| row.get(*idx).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    lab_analysis::QueryTable { columns, rows }
}

fn should_chunk_query_table(table: &lab_analysis::QueryTable, term_w: usize) -> bool {
    let col_count = table.columns.len();
    if col_count <= 12 {
        return false;
    }
    // If the minimum printable width is already over budget, avoid degenerate 3-4 char headers.
    let min_required = col_count.saturating_mul(6) + col_count.saturating_sub(1).saturating_mul(2);
    min_required > term_w || col_count > 18
}

fn print_query_table_in_column_chunks(table: &lab_analysis::QueryTable, term_w: usize) {
    let anchor_indices = choose_query_table_anchor_indices(&table.columns);
    let mut is_anchor = vec![false; table.columns.len()];
    for idx in &anchor_indices {
        if *idx < is_anchor.len() {
            is_anchor[*idx] = true;
        }
    }
    let trailing_indices: Vec<usize> = (0..table.columns.len())
        .filter(|idx| !is_anchor[*idx])
        .collect();

    let anchor_count = anchor_indices.len().max(1);
    // Keep chunk width readable: prefer fewer columns so headers remain distinguishable.
    // Empirically, 4-6 total columns avoids "var." / "del." collisions.
    let base_max_total_cols = if term_w < 120 {
        4
    } else if term_w < 170 {
        5
    } else {
        6
    };
    let max_cols_for_readable = base_max_total_cols.max(anchor_count + 1);
    let chunk_payload_cols = max_cols_for_readable.saturating_sub(anchor_count).max(1);
    let total_chunks = trailing_indices.len().div_ceil(chunk_payload_cols).max(1);

    if trailing_indices.is_empty() {
        print_scoreboard_table(table, term_w);
        return;
    }

    for (chunk_idx, payload_chunk) in trailing_indices.chunks(chunk_payload_cols).enumerate() {
        if chunk_idx > 0 {
            println!();
        }
        let mut selected_indices = anchor_indices.clone();
        selected_indices.extend(payload_chunk.iter().copied());
        selected_indices.sort_unstable();

        let projected = project_query_table_columns(table, &selected_indices);
        println!("-- column chunk {}/{} --", chunk_idx + 1, total_chunks);
        print_scoreboard_table(&projected, term_w);
    }
}

fn choose_query_table_anchor_indices(columns: &[String]) -> Vec<usize> {
    let mut out = Vec::new();
    let priorities = [
        "task_id",
        "repl_idx",
        "turn_index",
        "row_seq",
        "variant_a_id",
        "variant_b_id",
        "variant_a_trial_id",
        "variant_b_trial_id",
        "trial_id",
        "variant_id",
    ];
    for name in priorities {
        if out.len() >= 3 {
            break;
        }
        if let Some(idx) = columns.iter().position(|col| col == name) {
            if !out.contains(&idx) {
                out.push(idx);
            }
        }
    }
    if out.is_empty() {
        out.push(0);
        if columns.len() > 1 {
            out.push(1);
        }
    }
    out
}

fn project_query_table_columns(
    table: &lab_analysis::QueryTable,
    indices: &[usize],
) -> lab_analysis::QueryTable {
    let columns = indices
        .iter()
        .filter_map(|idx| table.columns.get(*idx).cloned())
        .collect::<Vec<_>>();
    let rows = table
        .rows
        .iter()
        .map(|row| {
            indices
                .iter()
                .map(|idx| row.get(*idx).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    lab_analysis::QueryTable { columns, rows }
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

fn markdown_escape_cell(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('\r', "")
        .replace('\n', "<br>")
}

fn render_query_table_markdown(table: &lab_analysis::QueryTable) -> String {
    if table.columns.is_empty() {
        return "(ok)".to_string();
    }
    let header = format!(
        "| {} |",
        table
            .columns
            .iter()
            .map(|col| markdown_escape_cell(col))
            .collect::<Vec<_>>()
            .join(" | ")
    );
    let separator = format!(
        "| {} |",
        table
            .columns
            .iter()
            .map(|_| "---")
            .collect::<Vec<_>>()
            .join(" | ")
    );
    let mut lines = vec![header, separator];
    for row in &table.rows {
        let cells = table
            .columns
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                let value = row.get(idx).unwrap_or(&Value::Null);
                markdown_escape_cell(&render_json_cell(value))
            })
            .collect::<Vec<_>>();
        lines.push(format!("| {} |", cells.join(" | ")));
    }
    lines.join("\n")
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn render_query_table_html_fragment(table: &lab_analysis::QueryTable) -> String {
    if table.columns.is_empty() {
        return "<p>(ok)</p>".to_string();
    }
    let mut out = String::new();
    out.push_str("<table><thead><tr>");
    for col in &table.columns {
        out.push_str("<th>");
        out.push_str(&html_escape(col));
        out.push_str("</th>");
    }
    out.push_str("</tr></thead><tbody>");
    for row in &table.rows {
        out.push_str("<tr>");
        for (idx, _) in table.columns.iter().enumerate() {
            let value = row.get(idx).unwrap_or(&Value::Null);
            out.push_str("<td>");
            out.push_str(&html_escape(&render_json_cell(value)));
            out.push_str("</td>");
        }
        out.push_str("</tr>");
    }
    out.push_str("</tbody></table>");
    out
}

fn print_table_markdown(table: &lab_analysis::QueryTable) {
    println!("{}", render_query_table_markdown(table));
}

fn print_table_html_document(title: &str, table: &lab_analysis::QueryTable) {
    println!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{}</title><style>body{{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;padding:20px;line-height:1.4}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #bbb;padding:6px 8px;text-align:left;vertical-align:top}}th{{background:#f5f5f5;position:sticky;top:0}}tr:nth-child(even) td{{background:#fafafa}}</style></head><body><h1>{}</h1>{}</body></html>",
        html_escape(title),
        html_escape(title),
        render_query_table_html_fragment(table)
    );
}

fn is_trace_view(resolved: &ResolvedView) -> bool {
    if resolved.name == "trace" || resolved.name == "trace_compare" {
        return true;
    }
    matches!(
        resolved.source.as_deref(),
        Some("ab_trace_row_side_by_side")
    )
}

fn render_trace_sections_markdown(table: &lab_analysis::QueryTable) -> Option<String> {
    let sections = build_trace_sections(table);
    if sections.is_empty() {
        return None;
    }
    let mut out = String::new();
    for section in sections {
        out.push_str(&format!(
            "### task `{}` repl `{}`\n\n",
            markdown_escape_cell(&section.task_id),
            markdown_escape_cell(&section.repl_idx)
        ));
        out.push_str(&format!(
            "- variant_a: `{}`  trial: `{}`\n",
            markdown_escape_cell(if section.variant_a_id.is_empty() {
                "unknown"
            } else {
                section.variant_a_id.as_str()
            }),
            markdown_escape_cell(if section.variant_a_trial_id.is_empty() {
                "unknown"
            } else {
                section.variant_a_trial_id.as_str()
            })
        ));
        out.push_str(&format!(
            "- variant_b: `{}`  trial: `{}`\n\n",
            markdown_escape_cell(if section.variant_b_id.is_empty() {
                "unknown"
            } else {
                section.variant_b_id.as_str()
            }),
            markdown_escape_cell(if section.variant_b_trial_id.is_empty() {
                "unknown"
            } else {
                section.variant_b_trial_id.as_str()
            })
        ));
        out.push_str("\n#### variant_a\n\n");
        out.push_str(&render_query_table_markdown(&section.variant_a_table));
        out.push_str("\n\n#### variant_b\n\n");
        out.push_str(&render_query_table_markdown(&section.variant_b_table));
        out.push_str("\n\n");
    }
    Some(out)
}

fn render_trace_sections_html(table: &lab_analysis::QueryTable) -> Option<String> {
    let sections = build_trace_sections(table);
    if sections.is_empty() {
        return None;
    }
    let mut out = String::new();
    for section in sections {
        out.push_str("<section class=\"trace-task\">");
        out.push_str("<h3>task <code>");
        out.push_str(&html_escape(&section.task_id));
        out.push_str("</code> repl <code>");
        out.push_str(&html_escape(&section.repl_idx));
        out.push_str("</code></h3>");
        out.push_str("<p><strong>variant_a:</strong> <code>");
        out.push_str(&html_escape(if section.variant_a_id.is_empty() {
            "unknown"
        } else {
            section.variant_a_id.as_str()
        }));
        out.push_str("</code> <strong>trial:</strong> <code>");
        out.push_str(&html_escape(if section.variant_a_trial_id.is_empty() {
            "unknown"
        } else {
            section.variant_a_trial_id.as_str()
        }));
        out.push_str("</code></p>");
        out.push_str("<p><strong>variant_b:</strong> <code>");
        out.push_str(&html_escape(if section.variant_b_id.is_empty() {
            "unknown"
        } else {
            section.variant_b_id.as_str()
        }));
        out.push_str("</code> <strong>trial:</strong> <code>");
        out.push_str(&html_escape(if section.variant_b_trial_id.is_empty() {
            "unknown"
        } else {
            section.variant_b_trial_id.as_str()
        }));
        out.push_str("</code></p>");
        out.push_str("<div class=\"trace-grid\"><div><h4>variant_a</h4>");
        out.push_str(&render_query_table_html_fragment(&section.variant_a_table));
        out.push_str("</div><div><h4>variant_b</h4>");
        out.push_str(&render_query_table_html_fragment(&section.variant_b_table));
        out.push_str("</div></div></section>");
    }
    Some(out)
}

fn print_single_view_markdown(
    run_dir: &Path,
    view_set: &str,
    resolved: &ResolvedView,
    table: &lab_analysis::QueryTable,
) {
    println!("# lab view");
    println!();
    println!("run_dir: `{}`", run_dir.display());
    println!();
    println!("view_set: `{}`", view_set);
    println!();
    println!("view: `{}`", resolved.name);
    if let Some(source) = resolved.source.as_deref() {
        if source != resolved.name {
            println!();
            println!("source_view: `{}`", source);
        }
    }
    println!();
    if is_trace_view(resolved) {
        if let Some(rendered) = render_trace_sections_markdown(table) {
            println!("{}", rendered.trim_end());
            return;
        }
    }
    println!("{}", render_query_table_markdown(table));
}

fn print_single_view_html(
    run_dir: &Path,
    view_set: &str,
    resolved: &ResolvedView,
    table: &lab_analysis::QueryTable,
) {
    let mut out = String::new();
    out.push_str(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>lab view</title><style>body{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;padding:20px;line-height:1.4}table{border-collapse:collapse;width:100%}th,td{border:1px solid #bbb;padding:6px 8px;text-align:left;vertical-align:top}th{background:#f5f5f5;position:sticky;top:0}tr:nth-child(even) td{background:#fafafa}code{background:#f3f3f3;padding:1px 4px;border-radius:4px}.trace-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;align-items:start}.trace-task{margin-top:20px;padding-top:4px;border-top:1px solid #ddd}</style></head><body>",
    );
    out.push_str("<h1>lab view</h1>");
    out.push_str("<p><strong>run_dir:</strong> <code>");
    out.push_str(&html_escape(&run_dir.display().to_string()));
    out.push_str("</code></p>");
    out.push_str("<p><strong>view_set:</strong> <code>");
    out.push_str(&html_escape(view_set));
    out.push_str("</code></p>");
    out.push_str("<p><strong>view:</strong> <code>");
    out.push_str(&html_escape(&resolved.name));
    out.push_str("</code></p>");
    if let Some(source) = resolved.source.as_deref() {
        if source != resolved.name {
            out.push_str("<p><strong>source_view:</strong> <code>");
            out.push_str(&html_escape(source));
            out.push_str("</code></p>");
        }
    }
    if is_trace_view(resolved) {
        if let Some(rendered) = render_trace_sections_html(table) {
            out.push_str(&rendered);
        } else {
            out.push_str(&render_query_table_html_fragment(table));
        }
    } else {
        out.push_str(&render_query_table_html_fragment(table));
    }
    out.push_str("</body></html>");
    println!("{}", out);
}

fn print_views_markdown_document(
    run_dir: &Path,
    view_set: &str,
    rendered: &[(ResolvedView, lab_analysis::QueryTable)],
) {
    println!("# lab views");
    println!();
    println!("run_dir: `{}`", run_dir.display());
    println!();
    println!("view_set: `{}`", view_set);
    for (resolved, table) in rendered {
        println!();
        println!("## {}", resolved.name);
        if let Some(source) = resolved.source.as_deref() {
            if source != resolved.name {
                println!();
                println!("source_view: `{}`", source);
            }
        }
        println!();
        if is_trace_view(resolved) {
            if let Some(rendered) = render_trace_sections_markdown(table) {
                println!("{}", rendered.trim_end());
                continue;
            }
        }
        println!("{}", render_query_table_markdown(table));
    }
}

fn print_views_html_document(
    run_dir: &Path,
    view_set: &str,
    rendered: &[(ResolvedView, lab_analysis::QueryTable)],
) {
    let mut out = String::new();
    out.push_str(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>lab views</title><style>body{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;padding:20px;line-height:1.4}table{border-collapse:collapse;width:100%;margin-bottom:26px}th,td{border:1px solid #bbb;padding:6px 8px;text-align:left;vertical-align:top}th{background:#f5f5f5;position:sticky;top:0}tr:nth-child(even) td{background:#fafafa}code{background:#f3f3f3;padding:1px 4px;border-radius:4px}h2{margin-top:32px}.trace-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;align-items:start}.trace-task{margin-top:20px;padding-top:4px;border-top:1px solid #ddd}</style></head><body>",
    );
    out.push_str("<h1>lab views</h1>");
    out.push_str("<p><strong>run_dir:</strong> <code>");
    out.push_str(&html_escape(&run_dir.display().to_string()));
    out.push_str("</code></p>");
    out.push_str("<p><strong>view_set:</strong> <code>");
    out.push_str(&html_escape(view_set));
    out.push_str("</code></p>");
    for (resolved, table) in rendered {
        out.push_str("<h2>");
        out.push_str(&html_escape(&resolved.name));
        out.push_str("</h2>");
        if let Some(source) = resolved.source.as_deref() {
            if source != resolved.name {
                out.push_str("<p><strong>source_view:</strong> <code>");
                out.push_str(&html_escape(source));
                out.push_str("</code></p>");
            }
        }
        if is_trace_view(resolved) {
            if let Some(rendered) = render_trace_sections_html(table) {
                out.push_str(&rendered);
            } else {
                out.push_str(&render_query_table_html_fragment(table));
            }
        } else {
            out.push_str(&render_query_table_html_fragment(table));
        }
    }
    out.push_str("</body></html>");
    println!("{}", out);
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
    let entries = collect_run_inventory(project_root)?;
    let rows = entries
        .into_iter()
        .map(|entry| {
            let metrics = read_run_metrics(&entry.run_dir);
            vec![
                Value::String(entry.control.status_display),
                Value::String(entry.started_at_display),
                Value::String(entry.run_id),
                Value::String(display_or_dash(&entry.experiment)),
                Value::String(entry.control.live_summary),
                json!(metrics.variants),
                match metrics.pass_rate {
                    Some(pr) => json!((pr * 10000.0).round() / 10000.0),
                    None => Value::Null,
                },
            ]
        })
        .collect();

    Ok(lab_analysis::QueryTable {
        columns: vec![
            "status".into(),
            "started_at".into(),
            "run_id".into(),
            "experiment".into(),
            "live".into(),
            "variants".into(),
            "pass_rate".into(),
        ],
        rows,
    })
}

fn collect_run_inventory(project_root: &Path) -> Result<Vec<RunInventoryEntry>> {
    let runs_dir = project_root.join(".lab").join("runs");
    if !runs_dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&runs_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        entries.push(inspect_run_inventory_entry(&entry.path()));
    }

    entries.sort_by(|a, b| {
        b.control
            .is_active
            .cmp(&a.control.is_active)
            .then_with(|| b.started_at.cmp(&a.started_at))
            .then_with(|| a.run_id.cmp(&b.run_id))
    });
    Ok(entries)
}

fn inspect_run_inventory_entry(run_dir: &Path) -> RunInventoryEntry {
    let dir_name = run_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown_run")
        .to_string();
    let manifest = read_json_file(&run_dir.join("manifest.json")).unwrap_or_else(|| json!({}));
    let resolved =
        read_json_file(&run_dir.join("resolved_experiment.json")).unwrap_or_else(|| json!({}));

    let run_id = manifest
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or(&dir_name)
        .to_string();
    let started_at = manifest
        .get("created_at")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .or_else(|| timestamp_from_run_id(&run_id))
        .unwrap_or_default();
    let experiment = resolved
        .pointer("/experiment/id")
        .or_else(|| resolved.pointer("/id"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let control = summarize_run_control(load_run_control(run_dir).as_ref());

    RunInventoryEntry {
        run_id,
        run_dir: run_dir.to_path_buf(),
        experiment,
        started_at_display: format_timestamp_for_display(&started_at),
        started_at,
        control,
    }
}

fn read_json_file(path: &Path) -> Option<Value> {
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

fn summarize_run_control(parsed: Option<&Value>) -> RunControlSummary {
    let Some(parsed) = parsed else {
        return RunControlSummary {
            status: "unknown".to_string(),
            status_display: "unknown".to_string(),
            live_summary: "idle".to_string(),
            active_trials: 0,
            is_active: false,
        };
    };

    let status = parsed
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let active_trials_map = parsed
        .get("active_trials")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let active_trials = active_trials_map.len();
    let workers = active_trials_map
        .values()
        .filter_map(|entry| entry.get("worker_id").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let worker_list = workers.iter().cloned().collect::<Vec<_>>();
    let worker_suffix = if worker_list.is_empty() {
        None
    } else if worker_list.len() <= 3 {
        Some(worker_list.join(","))
    } else {
        Some(format!("{} total", worker_list.len()))
    };

    let status_display = if active_trials == 0 {
        status.clone()
    } else if let Some(worker_text) = worker_suffix.as_deref() {
        format!(
            "{} (active_trials={}, workers={})",
            status, active_trials, worker_text
        )
    } else {
        format!("{} (active_trials={})", status, active_trials)
    };
    let live_summary = if active_trials == 0 {
        "idle".to_string()
    } else if worker_list.is_empty() {
        format!("{} active", active_trials)
    } else if worker_list.len() <= 3 {
        format!("{} active / {}", active_trials, worker_list.join(","))
    } else {
        format!("{} active / {} workers", active_trials, worker_list.len())
    };
    let is_active = matches!(status.as_str(), "running" | "paused") || active_trials > 0;

    RunControlSummary {
        status,
        status_display,
        live_summary,
        active_trials,
        is_active,
    }
}

fn read_run_metrics(run_dir: &Path) -> RunMetrics {
    let sqlite_path = run_dir.join("run.sqlite");
    if sqlite_path.exists() {
        if let Ok(conn) = Connection::open(&sqlite_path) {
            let variants = conn
                .query_row(
                    "SELECT count(DISTINCT variant_id) FROM trial_rows",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0_i64) as usize;
            let baseline_id: Option<String> = conn
                .query_row("SELECT baseline_id FROM trial_rows LIMIT 1", [], |row| {
                    row.get(0)
                })
                .optional()
                .unwrap_or(None);
            let pass_rate = match baseline_id {
                Some(baseline) => conn
                    .query_row(
                        "SELECT avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END)
                         FROM trial_rows
                         WHERE variant_id = ?1",
                        params![baseline],
                        |row| row.get::<_, Option<f64>>(0),
                    )
                    .unwrap_or(None),
                None => None,
            };
            return RunMetrics {
                variants,
                pass_rate,
            };
        }
    }

    let trials_facts_path = run_dir.join("facts").join("trials.jsonl");
    if !trials_facts_path.exists() {
        return RunMetrics {
            variants: 0,
            pass_rate: None,
        };
    }

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

    RunMetrics {
        variants: variant_ids.len(),
        pass_rate: if baseline_total > 0 {
            Some(baseline_successes as f64 / baseline_total as f64)
        } else {
            None
        },
    }
}

fn format_timestamp_for_display(value: &str) -> String {
    if value.trim().is_empty() {
        return "unknown".to_string();
    }
    let display = value.replacen('T', " ", 1).trim().to_string();
    if let Some(prefix) = display.strip_suffix("+00:00") {
        format!("{}Z", prefix)
    } else {
        display
    }
}

fn timestamp_from_run_id(run_id: &str) -> Option<String> {
    let rest = run_id.strip_prefix("run_")?;
    let mut parts = rest.split('_');
    let date = parts.next()?;
    let time = parts.next()?;
    let micros = parts.next()?;
    if date.len() != 8
        || time.len() != 6
        || micros.len() != 6
        || !date.chars().all(|ch| ch.is_ascii_digit())
        || !time.chars().all(|ch| ch.is_ascii_digit())
        || !micros.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }

    Some(format!(
        "{}-{}-{} {}:{}:{}.{}",
        &date[0..4],
        &date[4..6],
        &date[6..8],
        &time[0..2],
        &time[2..4],
        &time[4..6],
        micros
    ))
}

fn display_or_dash(value: &str) -> String {
    if value.trim().is_empty() {
        "-".to_string()
    } else {
        value.to_string()
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
      "id": "policy.task_sandbox.network",
      "label": "Network Mode",
      "json_pointer": "/policy/task_sandbox/network",
      "type": "string",
      "options": ["none", "full", "allowlist_enforced"],
      "role": "infra",
      "scientific_role": "invariant"
    },
    {
      "id": "runtime.agent_runtime.command",
      "label": "Agent Command",
      "json_pointer": "/runtime/agent_runtime/command",
      "type": "json",
      "role": "agent",
      "scientific_role": "treatment"
    },
    {
      "id": "runtime.agent_runtime.artifact",
      "label": "Agent Artifact",
      "json_pointer": "/runtime/agent_runtime/artifact",
      "type": "string",
      "role": "infra",
      "scientific_role": "treatment",
      "autotune": { "enabled": false, "requires_human_approval": true }
    },
    {
      "id": "runtime.agent_runtime.image",
      "label": "Agent Image",
      "json_pointer": "/runtime/agent_runtime/image",
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
    use rusqlite::Connection as SqliteConnection;

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
    fn enforce_cli_binary_freshness_blocks_stale_executable() {
        let exe_path = PathBuf::from("/tmp/lab-cli");
        let exe_mtime = UNIX_EPOCH + Duration::from_secs(100);
        let src_mtime = UNIX_EPOCH + Duration::from_secs(101);
        let err = enforce_cli_binary_freshness(
            &exe_path,
            exe_mtime,
            Some((
                src_mtime,
                PathBuf::from("/repo/rust/crates/lab-runner/src/lib.rs"),
            )),
        )
        .expect_err("stale binary should be rejected");
        let msg = err.to_string();
        assert!(msg.contains("stale lab-cli binary detected"), "{}", msg);
        assert!(msg.contains("cargo build -p lab-cli --release"), "{}", msg);
    }

    #[test]
    fn enforce_cli_binary_freshness_allows_up_to_date_executable() {
        let exe_path = PathBuf::from("/tmp/lab-cli");
        let exe_mtime = UNIX_EPOCH + Duration::from_secs(200);
        let src_mtime = UNIX_EPOCH + Duration::from_secs(199);
        enforce_cli_binary_freshness(
            &exe_path,
            exe_mtime,
            Some((
                src_mtime,
                PathBuf::from("/repo/rust/crates/lab-runner/src/lib.rs"),
            )),
        )
        .expect("fresh binary should pass");
    }

    fn count_temp_sqlite_export_dirs_for_run(run_id: &str) -> usize {
        let prefix = format!("agentlab_sqlite_export_{}_", run_id);
        std::fs::read_dir(std::env::temp_dir())
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with(&prefix) && entry.file_type().ok()?.is_dir() {
                    Some(())
                } else {
                    None
                }
            })
            .count()
    }

    fn seed_sqlite_run_for_analysis_query(run_dir: &Path) {
        let sqlite_path = run_dir.join("run.sqlite");
        let conn = SqliteConnection::open(&sqlite_path).expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE trial_rows(row_json TEXT NOT NULL);
             CREATE TABLE metric_rows(row_json TEXT NOT NULL);
             CREATE TABLE event_rows(row_json TEXT NOT NULL);
             CREATE TABLE variant_snapshot_rows(row_json TEXT NOT NULL);
             CREATE TABLE slot_commit_records(
                 schedule_idx INTEGER NOT NULL,
                 attempt INTEGER NOT NULL,
                 record_type TEXT NOT NULL,
                 record_json TEXT NOT NULL
             );
             CREATE TABLE runtime_kv(
                 key TEXT PRIMARY KEY,
                 value_json TEXT NOT NULL
             );",
        )
        .expect("create sqlite schema");
        conn.execute(
            "INSERT INTO trial_rows(row_json) VALUES (?1)",
            [r#"{"run_id":"run_test","trial_id":"trial_1","variant_id":"base","task_id":"task_1","outcome":"success","slot_commit_id":"slot_1","schedule_idx":0}"#],
        )
        .expect("insert trial row");
        conn.execute(
            "INSERT INTO metric_rows(row_json) VALUES (?1)",
            [r#"{"run_id":"run_test","trial_id":"trial_1","variant_id":"base","task_id":"task_1","metric_name":"latency_ms","metric_value":12.3,"slot_commit_id":"slot_1","schedule_idx":0}"#],
        )
        .expect("insert metric row");
        conn.execute(
            "INSERT INTO event_rows(row_json) VALUES (?1)",
            [r#"{"run_id":"run_test","trial_id":"trial_1","variant_id":"base","task_id":"task_1","event_type":"model_call_end","slot_commit_id":"slot_1","schedule_idx":0}"#],
        )
        .expect("insert event row");
        conn.execute(
            "INSERT INTO variant_snapshot_rows(row_json) VALUES (?1)",
            [r#"{"run_id":"run_test","variant_id":"base","task_id":"task_1","slot_commit_id":"slot_1","schedule_idx":0}"#],
        )
        .expect("insert variant snapshot row");
        conn.execute(
            "INSERT INTO slot_commit_records(schedule_idx, attempt, record_type, record_json) VALUES (?1, ?2, ?3, ?4)",
            (
                0_i64,
                0_i64,
                "commit",
                r#"{"record_type":"commit","schedule_idx":0,"slot_commit_id":"slot_1","attempt":0}"#,
            ),
        )
        .expect("insert slot commit record");
    }

    fn seed_runtime_run_control(run_dir: &Path, control: &Value) {
        let sqlite_path = run_dir.join("run.sqlite");
        let conn = SqliteConnection::open(&sqlite_path).expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS runtime_kv(
                 key TEXT PRIMARY KEY,
                 value_json TEXT NOT NULL
             );",
        )
        .expect("create runtime_kv");
        conn.execute(
            "INSERT INTO runtime_kv(key, value_json) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
            (
                "run_control_v2",
                serde_json::to_string(control).expect("serialize control"),
            ),
        )
        .expect("write runtime run_control_v2");
    }

    fn seed_variant_run(run_dir: &Path) {
        std::fs::create_dir_all(run_dir).expect("run dir");
        std::fs::write(
            run_dir.join("resolved_experiment.json"),
            serde_json::to_vec_pretty(&json!({
                "experiment": { "id": "exp_variants" },
                "runtime": {
                    "agent_runtime": {
                        "artifact": "baseline.tar.gz",
                        "artifact_digest": "sha256:base",
                        "image": "img:base",
                        "command": ["rex", "run"]
                    },
                    "policy": {
                        "timeout_ms": 600000
                    }
                }
            }))
            .expect("serialize resolved experiment"),
        )
        .expect("write resolved experiment");
        std::fs::write(
            run_dir.join("resolved_variants.json"),
            serde_json::to_vec_pretty(&json!({
                "schema_version": "resolved_variants_v1",
                "generated_at": "2026-03-10T00:00:00Z",
                "baseline_id": "baseline",
                "variants": [
                    {
                        "id": "baseline",
                        "variant_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "bindings": { "model": "glm-5" },
                        "args": [],
                        "env": {},
                        "image": null,
                        "runtime_overrides": null
                    },
                    {
                        "id": "candidate",
                        "variant_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "bindings": { "model": "glm-5" },
                        "args": [],
                        "env": {},
                        "image": null,
                        "agent_ref": "candidate_build",
                        "runtime_overrides": {
                            "agent_runtime": {
                                "artifact": "candidate.tar.gz",
                                "artifact_digest": "sha256:candidate",
                                "image": "img:candidate",
                                "env": { "PARALLEL_TOOLS": "1" }
                            }
                        }
                    }
                ]
            }))
            .expect("serialize resolved variants"),
        )
        .expect("write resolved variants");
    }

    #[test]
    fn load_variant_inspection_set_reads_stored_variant_digests() {
        let run_dir = temp_dir("variant_inspection");
        seed_variant_run(&run_dir);

        let inspection = load_variant_inspection_set(&run_dir).expect("load inspection");
        assert_eq!(inspection.experiment_id.as_deref(), Some("exp_variants"));
        assert_eq!(inspection.baseline_id, "baseline");
        assert_eq!(inspection.variants.len(), 2);

        let baseline = find_variant_inspection(&inspection, "baseline").expect("baseline");
        let candidate = find_variant_inspection(&inspection, "candidate").expect("candidate");

        assert!(baseline.is_baseline);
        assert_eq!(
            baseline
                .code_surface
                .get("artifact_digest")
                .and_then(Value::as_str),
            Some("sha256:base")
        );
        assert_eq!(
            candidate
                .code_surface
                .get("artifact_digest")
                .and_then(Value::as_str),
            Some("sha256:candidate")
        );
        assert_eq!(candidate.agent_ref.as_deref(), Some("candidate_build"));
        assert_eq!(
            baseline.variant_digest,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            candidate.variant_digest,
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        assert_eq!(
            candidate
                .behavior_surface
                .pointer("/runtime/agent_runtime/env/PARALLEL_TOOLS")
                .and_then(Value::as_str),
            Some("1")
        );

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn diff_variant_surfaces_reports_nested_runtime_changes() {
        let run_dir = temp_dir("variant_diff");
        seed_variant_run(&run_dir);
        let inspection = load_variant_inspection_set(&run_dir).expect("load inspection");
        let baseline = find_variant_inspection(&inspection, "baseline").expect("baseline");
        let candidate = find_variant_inspection(&inspection, "candidate").expect("candidate");

        let diffs = diff_variant_surfaces(baseline, candidate);
        assert!(
            diffs.iter().any(|entry| entry.path == "runtime.agent_runtime.artifact_digest"),
            "expected artifact digest diff, got {:?}",
            diffs
        );
        assert!(
            diffs.iter().any(|entry| entry.path == "agent_ref"),
            "expected agent_ref diff, got {:?}",
            diffs
        );

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn load_variant_inspection_set_rejects_missing_variant_digest() {
        let run_dir = temp_dir("variant_missing_digest");
        seed_variant_run(&run_dir);
        let mut manifest =
            read_json_file(&run_dir.join("resolved_variants.json")).expect("resolved variants");
        let variants = manifest
            .pointer_mut("/variants")
            .and_then(Value::as_array_mut)
            .expect("variant array");
        variants[0]
            .as_object_mut()
            .expect("variant object")
            .remove("variant_digest");
        std::fs::write(
            run_dir.join("resolved_variants.json"),
            serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
        )
        .expect("write manifest");

        let err = load_variant_inspection_set(&run_dir).expect_err("missing variant_digest");
        assert!(err.to_string().contains("missing variant_digest"), "{}", err);

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn build_variants_list_table_exposes_baseline_and_variant_digest() {
        let run_dir = temp_dir("variant_list_table");
        seed_variant_run(&run_dir);
        let inspection = load_variant_inspection_set(&run_dir).expect("load inspection");
        let table = build_variants_list_table(&inspection);

        assert_eq!(
            table.columns,
            vec![
                "baseline".to_string(),
                "variant_id".to_string(),
                "variant_digest".to_string(),
                "artifact_digest".to_string(),
                "image".to_string(),
                "agent_ref".to_string(),
            ]
        );
        assert_eq!(table.rows.len(), 2);
        assert_eq!(table.rows[0][0], Value::String("yes".to_string()));
        assert_eq!(table.rows[0][1], Value::String("baseline".to_string()));
        assert_eq!(table.rows[1][5], Value::String("candidate_build".to_string()));

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn query_run_sqlite_cleans_temp_exports_and_keeps_real_run_id_in_metadata() {
        let run_dir = temp_dir("sqlite_query_cleanup");
        std::fs::create_dir_all(&run_dir).expect("run dir");
        seed_sqlite_run_for_analysis_query(&run_dir);

        let run_id = run_dir
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("run");
        let before = count_temp_sqlite_export_dirs_for_run(run_id);

        let table =
            lab_analysis::query_run(&run_dir, "SELECT run_id FROM analysis_metadata LIMIT 1")
                .expect("query run");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0][0], Value::String(run_id.to_string()));

        let after = count_temp_sqlite_export_dirs_for_run(run_id);
        assert_eq!(before, after, "sqlite temp export dirs should be cleaned");

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn read_run_status_renders_multiflight_active_trials() {
        let run_dir = temp_dir("run_status");
        std::fs::create_dir_all(&run_dir).expect("run dir");
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
        seed_runtime_run_control(&run_dir, &control);

        let status = read_run_status(&run_dir);
        assert_eq!(
            status,
            "running (active_trials=2, workers=worker_1,worker_2)"
        );

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn summarize_run_control_exposes_live_summary_and_activity_flag() {
        let control = json!({
            "schema_version": "run_control_v2",
            "run_id": "run_1",
            "status": "running",
            "active_trials": {
                "trial_1": {
                    "trial_id": "trial_1",
                    "worker_id": "worker_a",
                    "schedule_idx": 0,
                    "variant_id": "base",
                    "started_at": "2026-03-09T17:00:00Z",
                    "control": null
                }
            },
            "updated_at": "2026-03-09T17:00:02Z"
        });

        let summary = summarize_run_control(Some(&control));
        assert_eq!(summary.status, "running");
        assert_eq!(summary.active_trials, 1);
        assert_eq!(
            summary.status_display,
            "running (active_trials=1, workers=worker_a)"
        );
        assert_eq!(summary.live_summary, "1 active / worker_a");
        assert!(summary.is_active);
    }

    #[test]
    fn inspect_run_inventory_entry_reads_manifest_timestamp_and_experiment() {
        let run_dir = temp_dir("inventory_entry");
        std::fs::create_dir_all(&run_dir).expect("run dir");
        std::fs::write(
            run_dir.join("manifest.json"),
            r#"{
  "schema_version": "manifest_v1",
  "run_id": "run_123",
  "created_at": "2026-03-09T17:33:12Z"
}"#,
        )
        .expect("manifest");
        std::fs::write(
            run_dir.join("resolved_experiment.json"),
            r#"{
  "experiment": {
    "id": "exp_browser"
  }
}"#,
        )
        .expect("resolved");

        let entry = inspect_run_inventory_entry(&run_dir);
        assert_eq!(entry.run_id, "run_123");
        assert_eq!(entry.experiment, "exp_browser");
        assert_eq!(entry.started_at, "2026-03-09T17:33:12Z");
        assert_eq!(entry.started_at_display, "2026-03-09 17:33:12Z");
        assert_eq!(entry.control.status, "unknown");

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn build_inflight_scoreboard_table_reads_active_trials_when_facts_are_empty() {
        let run_dir = temp_dir("inflight_scoreboard");
        std::fs::create_dir_all(&run_dir).expect("run dir");
        let control = json!({
            "schema_version": "run_control_v2",
            "run_id": "run_1",
            "status": "running",
            "active_trials": {
                "trial_2": {
                    "trial_id": "trial_2",
                    "worker_id": "worker_2",
                    "schedule_idx": 1,
                    "variant_id": "codex_spark",
                    "started_at": "2026-02-22T00:00:01Z",
                    "control": null
                },
                "trial_1": {
                    "trial_id": "trial_1",
                    "worker_id": "worker_1",
                    "schedule_idx": 0,
                    "variant_id": "glm_5",
                    "started_at": "2026-02-22T00:00:00Z",
                    "control": null
                }
            },
            "updated_at": "2026-02-22T00:00:02Z"
        });
        seed_runtime_run_control(&run_dir, &control);

        let table =
            build_inflight_scoreboard_table(&run_dir).expect("in-flight scoreboard should exist");
        assert_eq!(
            table.columns,
            vec![
                "variant_id".to_string(),
                "trial_id".to_string(),
                "schedule_idx".to_string(),
                "worker_id".to_string(),
                "started_at".to_string(),
                "lifecycle".to_string(),
            ]
        );
        assert_eq!(table.rows.len(), 2);
        assert_eq!(table.rows[0][0], Value::String("glm_5".to_string()));
        assert_eq!(table.rows[0][1], Value::String("trial_1".to_string()));
        assert_eq!(table.rows[0][5], Value::String("in_flight".to_string()));
        assert_eq!(table.rows[1][0], Value::String("codex_spark".to_string()));
        assert_eq!(table.rows[1][1], Value::String("trial_2".to_string()));

        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn normalize_view_name_supports_ab_aliases() {
        assert_eq!(normalize_view_name("paired-diffs"), "paired_outcomes");
        assert_eq!(
            normalize_view_name("task-compare"),
            "ab_task_metrics_side_by_side"
        );
        assert_eq!(
            normalize_view_name("trace-diff"),
            "ab_trace_row_side_by_side"
        );
        assert_eq!(normalize_view_name("turn-diff"), "ab_turn_side_by_side");
        assert_eq!(normalize_view_name("outcome-compare"), "ab_task_outcomes");
    }

    #[test]
    fn resolve_requested_view_maps_aliases_to_standard_ab_views() {
        let raw = vec![
            "run_progress".to_string(),
            "ab_task_metrics_side_by_side".to_string(),
            "ab_trace_row_side_by_side".to_string(),
            "ab_turn_side_by_side".to_string(),
        ];
        let resolved = resolve_requested_view(lab_analysis::ViewSet::AbTest, &raw, "task-compare")
            .expect("resolve task-compare");
        assert_eq!(resolved.name, "task_metrics");
        assert_eq!(
            resolved.source.as_deref(),
            Some("ab_task_metrics_side_by_side")
        );

        let resolved = resolve_requested_view(lab_analysis::ViewSet::AbTest, &raw, "trace-diff")
            .expect("resolve trace-diff");
        assert_eq!(resolved.name, "trace");
        assert_eq!(
            resolved.source.as_deref(),
            Some("ab_trace_row_side_by_side")
        );
    }

    #[test]
    fn standardize_ab_column_name_rewrites_mixed_terms() {
        assert_eq!(
            standardize_ab_column_name("baseline_outcome"),
            "variant_a_outcome"
        );
        assert_eq!(
            standardize_ab_column_name("treatment_outcome"),
            "variant_b_outcome"
        );
        assert_eq!(
            standardize_ab_column_name("a_result_score"),
            "variant_a_result_score"
        );
        assert_eq!(
            standardize_ab_column_name("b_result_score"),
            "variant_b_result_score"
        );
        assert_eq!(
            standardize_ab_column_name("d_result_score"),
            "delta_result_score"
        );
        assert_eq!(standardize_ab_column_name("a_variant_id"), "variant_a_id");
        assert_eq!(standardize_ab_column_name("b_variant_id"), "variant_b_id");
    }

    #[test]
    fn build_trace_sections_compacts_variant_columns() {
        let table = lab_analysis::QueryTable {
            columns: vec![
                "task_id".to_string(),
                "repl_idx".to_string(),
                "variant_a_id".to_string(),
                "variant_b_id".to_string(),
                "variant_a_trial_id".to_string(),
                "variant_b_trial_id".to_string(),
                "row_seq".to_string(),
                "variant_a_event_type".to_string(),
                "variant_b_event_type".to_string(),
                "variant_a_turn_index".to_string(),
                "variant_b_turn_index".to_string(),
                "variant_a_model".to_string(),
                "variant_b_model".to_string(),
                "variant_a_tool".to_string(),
                "variant_b_tool".to_string(),
                "variant_a_status".to_string(),
                "variant_b_status".to_string(),
                "variant_a_call_id".to_string(),
                "variant_b_call_id".to_string(),
            ],
            rows: vec![vec![
                Value::String("TASK001".to_string()),
                json!(0),
                Value::String("a".to_string()),
                Value::String("b".to_string()),
                Value::String("trial_a".to_string()),
                Value::String("trial_b".to_string()),
                json!(1),
                Value::String("model_call_end".to_string()),
                Value::String("tool_call_end".to_string()),
                json!(0),
                Value::Null,
                Value::String("m-a".to_string()),
                Value::Null,
                Value::Null,
                Value::String("Bash".to_string()),
                Value::String("ok".to_string()),
                Value::String("ok".to_string()),
                Value::String("call-a".to_string()),
                Value::String("call-b".to_string()),
            ]],
        };

        let sections = build_trace_sections(&table);
        assert_eq!(sections.len(), 1);
        let section = &sections[0];
        assert_eq!(section.task_id, "TASK001");
        assert_eq!(section.repl_idx, "0");
        assert_eq!(section.variant_a_id, "a");
        assert_eq!(section.variant_b_id, "b");
        assert_eq!(
            section.variant_a_table.columns,
            vec!["row", "evt", "turn", "model", "tool", "st", "call"]
        );
        assert_eq!(
            section.variant_b_table.columns,
            vec!["row", "evt", "turn", "model", "tool", "st", "call"]
        );
    }

    #[test]
    fn build_trace_sections_drops_null_only_side_rows() {
        let table = lab_analysis::QueryTable {
            columns: vec![
                "task_id".to_string(),
                "repl_idx".to_string(),
                "variant_a_id".to_string(),
                "variant_b_id".to_string(),
                "variant_a_trial_id".to_string(),
                "variant_b_trial_id".to_string(),
                "row_seq".to_string(),
                "variant_a_event_type".to_string(),
                "variant_b_event_type".to_string(),
                "variant_a_turn_index".to_string(),
                "variant_b_turn_index".to_string(),
                "variant_a_model".to_string(),
                "variant_b_model".to_string(),
                "variant_a_tool".to_string(),
                "variant_b_tool".to_string(),
                "variant_a_status".to_string(),
                "variant_b_status".to_string(),
                "variant_a_call_id".to_string(),
                "variant_b_call_id".to_string(),
            ],
            rows: vec![
                vec![
                    Value::String("TASK001".to_string()),
                    json!(0),
                    Value::String("a".to_string()),
                    Value::String("b".to_string()),
                    Value::String("trial_a".to_string()),
                    Value::String("trial_b".to_string()),
                    json!(1),
                    Value::String("model_call_end".to_string()),
                    Value::Null,
                    json!(0),
                    Value::Null,
                    Value::String("m-a".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::String("ok".to_string()),
                    Value::Null,
                    Value::String("call-a".to_string()),
                    Value::Null,
                ],
                vec![
                    Value::String("TASK001".to_string()),
                    json!(0),
                    Value::String("a".to_string()),
                    Value::String("b".to_string()),
                    Value::String("trial_a".to_string()),
                    Value::String("trial_b".to_string()),
                    json!(2),
                    Value::Null,
                    Value::String("tool_call_end".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::String("Bash".to_string()),
                    Value::Null,
                    Value::String("ok".to_string()),
                    Value::Null,
                    Value::String("call-b".to_string()),
                ],
            ],
        };
        let sections = build_trace_sections(&table);
        assert_eq!(sections.len(), 1);
        let section = &sections[0];
        assert_eq!(section.variant_a_table.rows.len(), 1);
        assert_eq!(section.variant_b_table.rows.len(), 1);
    }

    #[test]
    fn trace_markdown_renderer_emits_pure_markdown() {
        let table = lab_analysis::QueryTable {
            columns: vec![
                "task_id".to_string(),
                "repl_idx".to_string(),
                "variant_a_id".to_string(),
                "variant_b_id".to_string(),
                "variant_a_trial_id".to_string(),
                "variant_b_trial_id".to_string(),
                "row_seq".to_string(),
                "variant_a_event_type".to_string(),
                "variant_b_event_type".to_string(),
                "variant_a_turn_index".to_string(),
                "variant_b_turn_index".to_string(),
                "variant_a_model".to_string(),
                "variant_b_model".to_string(),
                "variant_a_tool".to_string(),
                "variant_b_tool".to_string(),
                "variant_a_status".to_string(),
                "variant_b_status".to_string(),
                "variant_a_call_id".to_string(),
                "variant_b_call_id".to_string(),
            ],
            rows: vec![vec![
                Value::String("TASK001".to_string()),
                json!(0),
                Value::String("a".to_string()),
                Value::String("b".to_string()),
                Value::String("trial_a".to_string()),
                Value::String("trial_b".to_string()),
                json!(1),
                Value::String("model_call_end".to_string()),
                Value::String("model_call_end".to_string()),
                json!(0),
                json!(0),
                Value::String("m-a".to_string()),
                Value::String("m-b".to_string()),
                Value::Null,
                Value::Null,
                Value::String("ok".to_string()),
                Value::String("ok".to_string()),
                Value::String("call-a".to_string()),
                Value::String("call-b".to_string()),
            ]],
        };
        let rendered = render_trace_sections_markdown(&table).expect("trace markdown");
        assert!(rendered.contains("#### variant_a"));
        assert!(rendered.contains("#### variant_b"));
        assert!(!rendered.contains("<table>"));
        assert!(!rendered.contains("<td"));
    }

    #[test]
    fn choose_query_table_anchor_indices_prefers_task_context_columns() {
        let columns = vec![
            "delta_tokens_in".to_string(),
            "task_id".to_string(),
            "variant_b_outcome".to_string(),
            "repl_idx".to_string(),
            "turn_index".to_string(),
            "variant_a_id".to_string(),
            "variant_b_id".to_string(),
        ];
        let anchors = choose_query_table_anchor_indices(&columns);
        assert_eq!(anchors, vec![1, 3, 4]);
    }

    #[test]
    fn should_chunk_query_table_for_wide_views() {
        let columns = (0..24)
            .map(|idx| format!("col_{}", idx))
            .collect::<Vec<_>>();
        let table = lab_analysis::QueryTable {
            columns,
            rows: vec![vec![Value::String("x".to_string()); 24]],
        };
        assert!(should_chunk_query_table(&table, 120));
        assert!(should_chunk_query_table(&table, 200));
    }

    #[test]
    fn project_query_table_columns_with_prefix_trim_trims_variant_prefix() {
        let table = lab_analysis::QueryTable {
            columns: vec![
                "task_id".to_string(),
                "repl_idx".to_string(),
                "variant_a_trial_id".to_string(),
                "variant_a_event_type".to_string(),
                "variant_b_trial_id".to_string(),
            ],
            rows: vec![vec![
                Value::String("TASK001".to_string()),
                json!(0),
                Value::String("trial_1".to_string()),
                Value::String("model_call_start".to_string()),
                Value::String("trial_2".to_string()),
            ]],
        };

        let projected =
            project_query_table_columns_with_prefix_trim(&table, &[0, 1], &[2, 3], "variant_a_");
        assert_eq!(
            projected.columns,
            vec![
                "task_id".to_string(),
                "repl_idx".to_string(),
                "trial_id".to_string(),
                "event_type".to_string(),
            ]
        );
        assert_eq!(projected.rows.len(), 1);
    }

    #[test]
    fn project_query_table_by_column_priority_reorders_and_keeps_remaining() {
        let table = lab_analysis::QueryTable {
            columns: vec![
                "a_result_score".to_string(),
                "task_id".to_string(),
                "b_outcome".to_string(),
                "a_outcome".to_string(),
                "d_result_score".to_string(),
            ],
            rows: vec![vec![
                json!(1.0),
                Value::String("TASK001".to_string()),
                Value::String("failure".to_string()),
                Value::String("success".to_string()),
                json!(-1.0),
            ]],
        };
        let projected =
            project_query_table_by_column_priority(&table, &["task_id", "a_outcome", "b_outcome"]);
        assert_eq!(
            projected.columns,
            vec![
                "task_id".to_string(),
                "a_outcome".to_string(),
                "b_outcome".to_string(),
                "a_result_score".to_string(),
                "d_result_score".to_string(),
            ]
        );
        assert_eq!(projected.rows.len(), 1);
    }
}
