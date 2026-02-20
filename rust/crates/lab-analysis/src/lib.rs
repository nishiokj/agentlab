use anyhow::{anyhow, Context, Result};
#[cfg(feature = "duckdb_engine")]
use duckdb::Connection;
use include_dir::{include_dir, Dir};
use lab_core::ensure_dir;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

static VIEW_BUNDLES: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/views");

const TABLE_TRIALS: &str = "trials.jsonl";
const TABLE_METRICS_LONG: &str = "metrics_long.jsonl";
const TABLE_EVENT_COUNTS_BY_TRIAL: &str = "event_counts_by_trial.jsonl";
const TABLE_EVENT_COUNTS_BY_VARIANT: &str = "event_counts_by_variant.jsonl";
const TABLE_VARIANT_SUMMARY: &str = "variant_summary.jsonl";
const TABLE_BINDINGS_LONG: &str = "bindings_long.jsonl";

const ANALYSIS_DB_FILE: &str = "agentlab.duckdb";
const LOAD_SQL_FILE: &str = "load_duckdb.sql";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewSet {
    CoreOnly,
    AbTest,
    MultiVariant,
    ParameterSweep,
    Regression,
}

impl ViewSet {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CoreOnly => "core_only",
            Self::AbTest => "ab_test",
            Self::MultiVariant => "multi_variant",
            Self::ParameterSweep => "parameter_sweep",
            Self::Regression => "regression",
        }
    }

    fn bundle_file(self) -> Option<&'static str> {
        match self {
            Self::CoreOnly => None,
            Self::AbTest => Some("ab_test.sql"),
            Self::MultiVariant => Some("multi_variant.sql"),
            Self::ParameterSweep => Some("parameter_sweep.sql"),
            Self::Regression => Some("regression.sql"),
        }
    }
}

#[derive(Debug, Clone)]
struct ExperimentDesign {
    comparison: String,
    scheduling: String,
    variant_count: usize,
}

#[derive(Debug, Clone)]
struct RunAnalysisContext {
    run_dir: PathBuf,
    analysis_dir: PathBuf,
    tables_dir: PathBuf,
    comparison_policy: String,
    scheduling_policy: String,
    view_set: ViewSet,
}

#[derive(Debug, Clone)]
pub struct QueryTable {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub fn summarize_trial(
    run_id: &str,
    trial_output: &Value,
    trial_id: &str,
    workload_type: &str,
    variant_id: &str,
    task_idx: usize,
    task_id: &str,
    repl: usize,
    bindings: &Value,
    status: String,
    container_mode: bool,
    integration_level: &str,
    network_mode_requested: &str,
    network_mode_effective: &str,
) -> Value {
    let outcome = trial_output
        .get("outcome")
        .and_then(|v| v.as_str())
        .unwrap_or("error");
    let (primary_metric_name, primary_metric_value) =
        if let Some(obj) = trial_output.get("objective").and_then(|v| v.as_object()) {
            let name = obj
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("primary_metric")
                .to_string();
            let value = obj.get("value").cloned().unwrap_or(json!(null));
            (name, value)
        } else {
            let fallback = if outcome == "success" { 1.0 } else { 0.0 };
            ("success".to_string(), json!(fallback))
        };
    let mut metrics = trial_output.get("metrics").cloned().unwrap_or(json!({}));
    if let Some(obj) = metrics.as_object_mut() {
        obj.insert("status_code".to_string(), json!(status));
    }
    json!({
        "run_id": run_id,
        "trial_id": trial_id,
        "workload_type": workload_type,
        "variant_id": variant_id,
        "task_index": task_idx,
        "task_id": task_id,
        "repl_idx": repl,
        "outcome": outcome,
        "success": outcome == "success",
        "container_mode": container_mode,
        "integration_level": integration_level,
        "network_mode_requested": network_mode_requested,
        "network_mode_effective": network_mode_effective,
        "primary_metric_name": primary_metric_name,
        "primary_metric_value": primary_metric_value,
        "metrics": metrics,
        "bindings": bindings,
    })
}

pub fn write_analysis(
    analysis_dir: &Path,
    summaries: &[Value],
    baseline_id: &str,
    event_counts: &BTreeMap<String, BTreeMap<String, usize>>,
    trial_event_counts: &BTreeMap<String, BTreeMap<String, usize>>,
) -> Result<()> {
    let mut outcomes: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for s in summaries {
        let vid = s
            .get("variant_id")
            .and_then(|v| v.as_str())
            .unwrap_or("base");
        outcomes.entry(vid.to_string()).or_default().push(s);
    }

    let mut summary_map = BTreeMap::new();
    for (variant, rows) in &outcomes {
        let total = rows.len() as f64;
        let successes = rows
            .iter()
            .filter(|r| r.get("outcome").and_then(|v| v.as_str()) == Some("success"))
            .count() as f64;
        let success_rate = if total > 0.0 { successes / total } else { 0.0 };
        let primary_metric_name = rows
            .iter()
            .find_map(|r| r.get("primary_metric_name").and_then(|v| v.as_str()))
            .unwrap_or("success");
        let mut pm_sum = 0.0f64;
        let mut pm_n = 0usize;
        for r in rows {
            if let Some(v) = r.get("primary_metric_value").and_then(|v| v.as_f64()) {
                pm_sum += v;
                pm_n += 1;
            }
        }
        let primary_metric_mean = if pm_n > 0 { pm_sum / pm_n as f64 } else { 0.0 };
        summary_map.insert(
            variant.clone(),
            json!({
                "total": total,
                "success_rate": success_rate,
                "primary_metric_name": primary_metric_name,
                "primary_metric_mean": primary_metric_mean,
                "event_counts": event_counts.get(variant).cloned().unwrap_or_default()
            }),
        );
    }

    let summary = json!({
        "schema_version": "analysis_summary_v1",
        "baseline_id": baseline_id,
        "variants": summary_map,
    });
    fs::write(
        analysis_dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;

    let mut comparisons = Vec::new();
    for (variant, data) in &summary_map {
        if variant == baseline_id {
            continue;
        }
        let base = summary_map.get(baseline_id).cloned().unwrap_or(json!({}));
        comparisons.push(json!({
            "baseline": baseline_id,
            "variant": variant,
            "baseline_success_rate": base.get("success_rate").cloned().unwrap_or(json!(0.0)),
            "variant_success_rate": data.get("success_rate").cloned().unwrap_or(json!(0.0)),
        }));
    }

    let comparisons_json = json!({
        "schema_version": "analysis_comparisons_v1",
        "comparisons": comparisons
    });
    fs::write(
        analysis_dir.join("comparisons.json"),
        serde_json::to_vec_pretty(&comparisons_json)?,
    )?;

    write_analysis_tables(
        analysis_dir,
        summaries,
        baseline_id,
        &summary_map,
        event_counts,
        trial_event_counts,
    )?;

    Ok(())
}

pub fn run_view_set(run_dir: &Path) -> Result<ViewSet> {
    let context = load_run_context(run_dir)?;
    Ok(context.view_set)
}

fn duckdb_disabled_error(op: &str) -> anyhow::Error {
    anyhow!(
        "DuckDB support is disabled in this build; '{}' is unavailable (enable feature 'duckdb_engine' on lab-analysis)",
        op
    )
}

#[cfg(feature = "duckdb_engine")]
pub fn list_views(run_dir: &Path) -> Result<Vec<String>> {
    let context = load_run_context(run_dir)?;
    materialize_run_duckdb(&context)?;
    let conn = open_run_connection(&context)?;
    let table = execute_select_query(
        &conn,
        "SELECT table_name
         FROM information_schema.views
         WHERE table_schema = 'main'
         ORDER BY table_name",
    )?;
    let mut out = Vec::new();
    for row in table.rows {
        if let Some(name) = row.first().and_then(Value::as_str) {
            out.push(name.to_string());
        }
    }
    Ok(out)
}

#[cfg(not(feature = "duckdb_engine"))]
pub fn list_views(_run_dir: &Path) -> Result<Vec<String>> {
    Err(duckdb_disabled_error("views"))
}

pub fn query_view(run_dir: &Path, view_name: &str, limit: usize) -> Result<QueryTable> {
    if !is_safe_identifier(view_name) {
        return Err(anyhow!(
            "invalid view name '{}': only [A-Za-z0-9_] is allowed",
            view_name
        ));
    }
    let safe_limit = if limit == 0 { 100 } else { limit };
    let sql = format!(
        "SELECT * FROM {} LIMIT {}",
        quote_identifier(view_name),
        safe_limit
    );
    query_run(run_dir, &sql)
}

#[cfg(feature = "duckdb_engine")]
pub fn query_run(run_dir: &Path, sql: &str) -> Result<QueryTable> {
    let normalized = validate_read_only_sql(sql)?;
    let context = load_run_context(run_dir)?;
    materialize_run_duckdb(&context)?;
    let conn = open_run_connection(&context)?;
    execute_select_query(&conn, &normalized)
}

#[cfg(not(feature = "duckdb_engine"))]
pub fn query_run(_run_dir: &Path, _sql: &str) -> Result<QueryTable> {
    Err(duckdb_disabled_error("query"))
}

#[cfg(feature = "duckdb_engine")]
pub fn query_trend(
    project_root: &Path,
    experiment_id: &str,
    task_id: Option<&str>,
    variant_id: Option<&str>,
) -> Result<QueryTable> {
    let experiment_id = experiment_id.trim();
    if experiment_id.is_empty() {
        return Err(anyhow!("experiment_id cannot be empty"));
    }

    let db_path = materialize_project_duckdb(project_root)?;
    let conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open project DuckDB {}", db_path.display()))?;
    load_json_extension(&conn)?;

    let mut conditions = vec![format!("r.experiment_id = {}", sql_literal(experiment_id))];
    if let Some(task) = task_id {
        if !task.trim().is_empty() {
            conditions.push(format!("t.task_id = {}", sql_literal(task.trim())));
        }
    }
    if let Some(variant) = variant_id {
        if !variant.trim().is_empty() {
            conditions.push(format!("t.variant_id = {}", sql_literal(variant.trim())));
        }
    }

    let sql = format!(
        "SELECT
            t.run_id,
            t.variant_id,
            round(avg(CASE WHEN t.outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate,
            count(*) AS n_trials
         FROM all_trials t
         JOIN all_runs r USING (run_id)
         WHERE {}
         GROUP BY t.run_id, t.variant_id
         ORDER BY t.run_id, t.variant_id",
        conditions.join(" AND ")
    );
    execute_select_query(&conn, &sql)
}

#[cfg(not(feature = "duckdb_engine"))]
pub fn query_trend(
    _project_root: &Path,
    _experiment_id: &str,
    _task_id: Option<&str>,
    _variant_id: Option<&str>,
) -> Result<QueryTable> {
    Err(duckdb_disabled_error("trend"))
}

fn write_analysis_tables(
    analysis_dir: &Path,
    summaries: &[Value],
    baseline_id: &str,
    summary_map: &BTreeMap<String, Value>,
    event_counts: &BTreeMap<String, BTreeMap<String, usize>>,
    trial_event_counts: &BTreeMap<String, BTreeMap<String, usize>>,
) -> Result<()> {
    let tables_dir = analysis_dir.join("tables");
    ensure_dir(&tables_dir)?;

    let mut trials = fs::File::create(tables_dir.join(TABLE_TRIALS))?;
    let mut metrics_long = fs::File::create(tables_dir.join(TABLE_METRICS_LONG))?;
    let mut events_by_trial = fs::File::create(tables_dir.join(TABLE_EVENT_COUNTS_BY_TRIAL))?;
    let mut events_by_variant = fs::File::create(tables_dir.join(TABLE_EVENT_COUNTS_BY_VARIANT))?;
    let mut variant_summary = fs::File::create(tables_dir.join(TABLE_VARIANT_SUMMARY))?;
    let mut bindings_long = fs::File::create(tables_dir.join(TABLE_BINDINGS_LONG))?;

    let mut variant_bindings: BTreeMap<String, Value> = BTreeMap::new();
    for s in summaries {
        if let (Some(variant_id), Some(bindings)) = (
            s.get("variant_id").and_then(Value::as_str),
            s.get("bindings"),
        ) {
            variant_bindings
                .entry(variant_id.to_string())
                .or_insert_with(|| bindings.clone());
        }
    }

    for s in summaries {
        let trial_id = s
            .get("trial_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let hook_counts = trial_event_counts
            .get(&trial_id)
            .cloned()
            .unwrap_or_default();
        let hook_total: usize = hook_counts.values().sum();
        let mut trial_row = s.clone();
        if let Some(obj) = trial_row.as_object_mut() {
            obj.insert("baseline_id".to_string(), json!(baseline_id));
            obj.insert("hook_events_total".to_string(), json!(hook_total));
            obj.insert("has_hook_events".to_string(), json!(hook_total > 0));
        }
        serde_json::to_writer(&mut trials, &trial_row)?;
        writeln!(&mut trials)?;

        if let Some(metrics) = s.get("metrics").and_then(Value::as_object) {
            for (metric_name, metric_value) in metrics {
                let row = json!({
                    "run_id": s.get("run_id").cloned().unwrap_or(json!(null)),
                    "trial_id": s.get("trial_id").cloned().unwrap_or(json!(null)),
                    "variant_id": s.get("variant_id").cloned().unwrap_or(json!(null)),
                    "task_id": s.get("task_id").cloned().unwrap_or(json!(null)),
                    "repl_idx": s.get("repl_idx").cloned().unwrap_or(json!(null)),
                    "outcome": s.get("outcome").cloned().unwrap_or(json!(null)),
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                });
                serde_json::to_writer(&mut metrics_long, &row)?;
                writeln!(&mut metrics_long)?;
            }
        }
        if let (Some(name), Some(value)) = (
            s.get("primary_metric_name").and_then(Value::as_str),
            s.get("primary_metric_value"),
        ) {
            let row = json!({
                "run_id": s.get("run_id").cloned().unwrap_or(json!(null)),
                "trial_id": s.get("trial_id").cloned().unwrap_or(json!(null)),
                "variant_id": s.get("variant_id").cloned().unwrap_or(json!(null)),
                "task_id": s.get("task_id").cloned().unwrap_or(json!(null)),
                "repl_idx": s.get("repl_idx").cloned().unwrap_or(json!(null)),
                "outcome": s.get("outcome").cloned().unwrap_or(json!(null)),
                "metric_name": name,
                "metric_value": value,
                "metric_source": "primary"
            });
            serde_json::to_writer(&mut metrics_long, &row)?;
            writeln!(&mut metrics_long)?;
        }

        if let Some(bindings) = s.get("bindings").and_then(Value::as_object) {
            for (binding_name, binding_value) in bindings {
                let row = json!({
                    "run_id": s.get("run_id").cloned().unwrap_or(json!(null)),
                    "trial_id": s.get("trial_id").cloned().unwrap_or(json!(null)),
                    "variant_id": s.get("variant_id").cloned().unwrap_or(json!(null)),
                    "task_id": s.get("task_id").cloned().unwrap_or(json!(null)),
                    "repl_idx": s.get("repl_idx").cloned().unwrap_or(json!(null)),
                    "binding_name": binding_name,
                    "binding_value": binding_value,
                    "binding_value_text": binding_value_to_text(binding_value),
                });
                serde_json::to_writer(&mut bindings_long, &row)?;
                writeln!(&mut bindings_long)?;
            }
        }
    }

    for (trial_id, counts) in trial_event_counts {
        for (event_type, count) in counts {
            let row = json!({
                "trial_id": trial_id,
                "event_type": event_type,
                "count": count
            });
            serde_json::to_writer(&mut events_by_trial, &row)?;
            writeln!(&mut events_by_trial)?;
        }
    }

    for (variant_id, counts) in event_counts {
        for (event_type, count) in counts {
            let row = json!({
                "variant_id": variant_id,
                "event_type": event_type,
                "count": count
            });
            serde_json::to_writer(&mut events_by_variant, &row)?;
            writeln!(&mut events_by_variant)?;
        }
    }

    for (variant_id, data) in summary_map {
        let row = json!({
            "baseline_id": baseline_id,
            "variant_id": variant_id,
            "total": data.get("total").cloned().unwrap_or(json!(0)),
            "success_rate": data.get("success_rate").cloned().unwrap_or(json!(0.0)),
            "primary_metric_name": data.get("primary_metric_name").cloned().unwrap_or(json!("success")),
            "primary_metric_mean": data.get("primary_metric_mean").cloned().unwrap_or(json!(0.0)),
            "event_counts": data.get("event_counts").cloned().unwrap_or(json!({})),
            "bindings": variant_bindings.get(variant_id).cloned().unwrap_or(json!({})),
        });
        serde_json::to_writer(&mut variant_summary, &row)?;
        writeln!(&mut variant_summary)?;
    }

    let context =
        load_run_context_from_analysis_dir(analysis_dir).unwrap_or_else(|_| RunAnalysisContext {
            run_dir: analysis_dir.to_path_buf(),
            analysis_dir: analysis_dir.to_path_buf(),
            tables_dir: analysis_dir.join("tables"),
            comparison_policy: "unknown".to_string(),
            scheduling_policy: "unknown".to_string(),
            view_set: ViewSet::CoreOnly,
        });

    let bundle_sql = load_view_bundle_sql(context.view_set)?;
    let load_sql = build_load_sql_relative(&context, bundle_sql.as_deref());
    fs::write(tables_dir.join(LOAD_SQL_FILE), load_sql)?;
    fs::write(
        analysis_dir.join("duckdb_view_context.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": "duckdb_view_context_v1",
            "view_set": context.view_set.as_str(),
            "comparison_policy": context.comparison_policy,
            "scheduling_policy": context.scheduling_policy
        }))?,
    )?;

    if let Err(err) = materialize_run_duckdb(&context) {
        let warning = format!(
            "DuckDB materialization skipped for run analysis in {}: {}",
            analysis_dir.display(),
            err
        );
        fs::write(
            analysis_dir.join("duckdb_materialization_error.txt"),
            warning,
        )?;
    } else {
        let warning_path = analysis_dir.join("duckdb_materialization_error.txt");
        if warning_path.exists() {
            let _ = fs::remove_file(warning_path);
        }
    }

    Ok(())
}

fn load_run_context(run_dir: &Path) -> Result<RunAnalysisContext> {
    let canonical = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run directory not found: {}", run_dir.display()))?;
    let analysis_dir = canonical.join("analysis");
    let tables_dir = analysis_dir.join("tables");
    if !tables_dir.exists() {
        return Err(anyhow!(
            "analysis tables not found: {}",
            tables_dir.display()
        ));
    }
    let resolved = read_resolved_experiment(&canonical)?;
    let design = resolved
        .as_ref()
        .map(parse_experiment_design)
        .unwrap_or_else(default_experiment_design);
    let view_set = view_set_for_design(&design);
    Ok(RunAnalysisContext {
        run_dir: canonical,
        analysis_dir,
        tables_dir,
        comparison_policy: design.comparison,
        scheduling_policy: design.scheduling,
        view_set,
    })
}

fn load_run_context_from_analysis_dir(analysis_dir: &Path) -> Result<RunAnalysisContext> {
    let run_dir = analysis_dir.parent().ok_or_else(|| {
        anyhow!(
            "analysis directory has no parent: {}",
            analysis_dir.display()
        )
    })?;
    load_run_context(run_dir)
}

fn read_resolved_experiment(run_dir: &Path) -> Result<Option<Value>> {
    let path = run_dir.join("resolved_experiment.json");
    if !path.exists() {
        return Ok(None);
    }
    let raw =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let value = serde_json::from_str::<Value>(&raw)
        .with_context(|| format!("invalid JSON in {}", path.display()))?;
    Ok(Some(value))
}

fn default_experiment_design() -> ExperimentDesign {
    ExperimentDesign {
        comparison: "paired".to_string(),
        scheduling: "variant_sequential".to_string(),
        variant_count: 1,
    }
}

fn parse_experiment_design(resolved: &Value) -> ExperimentDesign {
    let comparison = resolved
        .pointer("/design/policies/comparison")
        .and_then(Value::as_str)
        .or_else(|| {
            resolved
                .pointer("/design/comparison")
                .and_then(Value::as_str)
        })
        .unwrap_or("paired")
        .trim()
        .to_ascii_lowercase();
    let scheduling = resolved
        .pointer("/design/policies/scheduling")
        .and_then(Value::as_str)
        .unwrap_or("variant_sequential")
        .trim()
        .to_ascii_lowercase();

    let mut variants = BTreeSet::new();
    if let Some(base) = resolved
        .pointer("/baseline/variant_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        variants.insert(base.to_string());
    }
    if let Some(plan) = resolved.pointer("/variant_plan").and_then(Value::as_array) {
        for item in plan {
            if let Some(id) = item
                .get("variant_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|v| !v.is_empty())
            {
                variants.insert(id.to_string());
            }
        }
    }
    ExperimentDesign {
        comparison,
        scheduling,
        variant_count: variants.len().max(1),
    }
}

fn view_set_for_design(design: &ExperimentDesign) -> ViewSet {
    if design.comparison == "none" {
        return ViewSet::Regression;
    }
    if design.scheduling == "paired_interleaved" && design.comparison == "paired" {
        return if design.variant_count <= 2 {
            ViewSet::AbTest
        } else {
            ViewSet::MultiVariant
        };
    }
    if design.scheduling == "variant_sequential" && design.comparison == "unpaired" {
        return ViewSet::ParameterSweep;
    }

    match design.comparison.as_str() {
        "paired" => {
            if design.variant_count <= 2 {
                ViewSet::AbTest
            } else {
                ViewSet::MultiVariant
            }
        }
        "unpaired" => ViewSet::ParameterSweep,
        _ => ViewSet::CoreOnly,
    }
}

fn load_view_bundle_sql(view_set: ViewSet) -> Result<Option<String>> {
    let Some(file_name) = view_set.bundle_file() else {
        return Ok(None);
    };
    let file = VIEW_BUNDLES
        .get_file(file_name)
        .ok_or_else(|| anyhow!("missing embedded view bundle: {}", file_name))?;
    let content = file
        .contents_utf8()
        .ok_or_else(|| anyhow!("view bundle is not valid UTF-8: {}", file_name))?;
    Ok(Some(content.to_string()))
}

fn build_load_sql_relative(context: &RunAnalysisContext, bundle_sql: Option<&str>) -> String {
    let metadata_sql = build_metadata_view_sql(context);
    let mut sql = String::from(
        "-- Run from analysis directory:
-- duckdb .lab/runs/<run_id>/analysis/agentlab.duckdb < tables/load_duckdb.sql
LOAD json;

CREATE OR REPLACE VIEW trials AS
SELECT * FROM read_json_auto('tables/trials.jsonl', format='newline_delimited', union_by_name=true);

CREATE OR REPLACE VIEW metrics_long AS
SELECT * FROM read_json_auto('tables/metrics_long.jsonl', format='newline_delimited', union_by_name=true);

CREATE OR REPLACE VIEW event_counts_by_trial AS
SELECT * FROM read_json_auto('tables/event_counts_by_trial.jsonl', format='newline_delimited', union_by_name=true);

CREATE OR REPLACE VIEW event_counts_by_variant AS
SELECT * FROM read_json_auto('tables/event_counts_by_variant.jsonl', format='newline_delimited', union_by_name=true);

CREATE OR REPLACE VIEW variant_summary AS
SELECT * FROM read_json_auto('tables/variant_summary.jsonl', format='newline_delimited', union_by_name=true);

CREATE OR REPLACE VIEW bindings_long AS
SELECT * FROM read_json_auto('tables/bindings_long.jsonl', format='newline_delimited', union_by_name=true);
",
    );
    sql.push_str(&metadata_sql);
    sql.push('\n');
    if let Some(bundle) = bundle_sql {
        sql.push_str("-- Opinionated view bundle\n");
        sql.push_str(bundle);
        if !bundle.ends_with('\n') {
            sql.push('\n');
        }
    }
    sql
}

fn build_load_sql_absolute(context: &RunAnalysisContext, bundle_sql: Option<&str>) -> String {
    let trials_path = context.tables_dir.join(TABLE_TRIALS);
    let metrics_path = context.tables_dir.join(TABLE_METRICS_LONG);
    let events_trial_path = context.tables_dir.join(TABLE_EVENT_COUNTS_BY_TRIAL);
    let events_variant_path = context.tables_dir.join(TABLE_EVENT_COUNTS_BY_VARIANT);
    let variant_summary_path = context.tables_dir.join(TABLE_VARIANT_SUMMARY);
    let bindings_long_path = context.tables_dir.join(TABLE_BINDINGS_LONG);
    let mut sql = format!(
        "LOAD json;
CREATE OR REPLACE VIEW trials AS
SELECT * FROM read_json_auto({}, format='newline_delimited', union_by_name=true);
CREATE OR REPLACE VIEW metrics_long AS
SELECT * FROM read_json_auto({}, format='newline_delimited', union_by_name=true);
CREATE OR REPLACE VIEW event_counts_by_trial AS
SELECT * FROM read_json_auto({}, format='newline_delimited', union_by_name=true);
CREATE OR REPLACE VIEW event_counts_by_variant AS
SELECT * FROM read_json_auto({}, format='newline_delimited', union_by_name=true);
CREATE OR REPLACE VIEW variant_summary AS
SELECT * FROM read_json_auto({}, format='newline_delimited', union_by_name=true);
CREATE OR REPLACE VIEW bindings_long AS
SELECT * FROM read_json_auto({}, format='newline_delimited', union_by_name=true);
",
        sql_literal_path(&trials_path),
        sql_literal_path(&metrics_path),
        sql_literal_path(&events_trial_path),
        sql_literal_path(&events_variant_path),
        sql_literal_path(&variant_summary_path),
        sql_literal_path(&bindings_long_path),
    );
    sql.push_str(&build_metadata_view_sql(context));
    sql.push('\n');
    if let Some(bundle) = bundle_sql {
        sql.push_str(bundle);
        if !bundle.ends_with('\n') {
            sql.push('\n');
        }
    }
    sql
}

fn build_metadata_view_sql(context: &RunAnalysisContext) -> String {
    format!(
        "CREATE OR REPLACE VIEW analysis_metadata AS
SELECT
    {} AS run_id,
    {} AS view_set,
    {} AS comparison_policy,
    {} AS scheduling_policy;
",
        sql_literal(
            context
                .run_dir
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or("run")
        ),
        sql_literal(context.view_set.as_str()),
        sql_literal(&context.comparison_policy),
        sql_literal(&context.scheduling_policy),
    )
}

#[cfg(feature = "duckdb_engine")]
fn materialize_run_duckdb(context: &RunAnalysisContext) -> Result<()> {
    ensure_dir(&context.analysis_dir)?;
    ensure_dir(&context.tables_dir)?;
    ensure_table_files(&context.tables_dir)?;
    let db_path = context.analysis_dir.join(ANALYSIS_DB_FILE);
    let conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open DuckDB {}", db_path.display()))?;
    load_json_extension(&conn)?;
    let bundle_sql = load_view_bundle_sql(context.view_set)?;
    let sql = build_load_sql_absolute(context, bundle_sql.as_deref());
    conn.execute_batch(&sql)
        .with_context(|| format!("failed to materialize run DuckDB for {}", db_path.display()))?;
    Ok(())
}

#[cfg(not(feature = "duckdb_engine"))]
fn materialize_run_duckdb(_context: &RunAnalysisContext) -> Result<()> {
    Err(duckdb_disabled_error("run materialization"))
}

#[cfg(feature = "duckdb_engine")]
fn open_run_connection(context: &RunAnalysisContext) -> Result<Connection> {
    let db_path = context.analysis_dir.join(ANALYSIS_DB_FILE);
    let conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open DuckDB {}", db_path.display()))?;
    load_json_extension(&conn)?;
    Ok(conn)
}

fn ensure_table_files(tables_dir: &Path) -> Result<()> {
    let files = [
        TABLE_TRIALS,
        TABLE_METRICS_LONG,
        TABLE_EVENT_COUNTS_BY_TRIAL,
        TABLE_EVENT_COUNTS_BY_VARIANT,
        TABLE_VARIANT_SUMMARY,
        TABLE_BINDINGS_LONG,
    ];
    for file in files {
        let path = tables_dir.join(file);
        if !path.exists() {
            fs::write(&path, b"")
                .with_context(|| format!("failed to initialize {}", path.display()))?;
        }
    }
    Ok(())
}

#[cfg(feature = "duckdb_engine")]
fn load_json_extension(conn: &Connection) -> Result<()> {
    match conn.execute_batch("LOAD json;") {
        Ok(_) => Ok(()),
        Err(_) => conn
            .execute_batch("INSTALL json; LOAD json;")
            .context("failed to load DuckDB json extension"),
    }
}

#[cfg(feature = "duckdb_engine")]
fn materialize_project_duckdb(project_root: &Path) -> Result<PathBuf> {
    let project_root = project_root
        .canonicalize()
        .map_err(|_| anyhow!("project root not found: {}", project_root.display()))?;
    let lab_dir = project_root.join(".lab");
    ensure_dir(&lab_dir)?;
    let db_path = lab_dir.join(ANALYSIS_DB_FILE);
    let conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open project DuckDB {}", db_path.display()))?;
    load_json_extension(&conn)?;

    let runs_dir = lab_dir.join("runs");
    let mut trial_sources: Vec<PathBuf> = Vec::new();
    let mut run_metadata: Vec<(String, PathBuf)> = Vec::new();

    if runs_dir.exists() {
        for entry in fs::read_dir(&runs_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let run_id = entry.file_name().to_string_lossy().to_string();
            let run_path = entry.path();
            let trials_path = run_path.join("analysis").join("tables").join(TABLE_TRIALS);
            if trials_path.exists() {
                trial_sources.push(trials_path);
            }
            let resolved_path = run_path.join("resolved_experiment.json");
            if resolved_path.exists() {
                run_metadata.push((run_id, resolved_path));
            }
        }
    }

    let all_trials_sql = if trial_sources.is_empty() {
        "CREATE OR REPLACE VIEW all_trials AS
SELECT
    CAST(NULL AS VARCHAR) AS run_id,
    CAST(NULL AS VARCHAR) AS variant_id,
    CAST(NULL AS VARCHAR) AS task_id,
    CAST(NULL AS VARCHAR) AS outcome,
    CAST(NULL AS DOUBLE) AS primary_metric_value
WHERE FALSE;"
            .to_string()
    } else {
        let unions = trial_sources
            .iter()
            .map(|path| {
                format!(
                    "SELECT * FROM read_json_auto({}, format='newline_delimited', union_by_name=true)",
                    sql_literal_path(path)
                )
            })
            .collect::<Vec<_>>()
            .join("\nUNION ALL\n");
        format!("CREATE OR REPLACE VIEW all_trials AS\n{};", unions)
    };

    let all_runs_sql = if run_metadata.is_empty() {
        "CREATE OR REPLACE VIEW all_runs AS
SELECT
    CAST(NULL AS VARCHAR) AS run_id,
    CAST(NULL AS VARCHAR) AS experiment_id
WHERE FALSE;"
            .to_string()
    } else {
        let unions = run_metadata
            .iter()
            .map(|(run_id, path)| {
                format!(
                    "SELECT
    {} AS run_id,
    coalesce(try_cast(experiment.id AS VARCHAR), '') AS experiment_id
FROM read_json_auto({}, union_by_name=true)",
                    sql_literal(run_id),
                    sql_literal_path(path),
                )
            })
            .collect::<Vec<_>>()
            .join("\nUNION ALL\n");
        format!("CREATE OR REPLACE VIEW all_runs AS\n{};", unions)
    };

    let sql = format!(
        "{}
{}
CREATE OR REPLACE VIEW pass_rate_trend AS
SELECT
    t.run_id,
    t.variant_id,
    round(avg(CASE WHEN t.outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate,
    count(*) AS n_trials
FROM all_trials t
GROUP BY t.run_id, t.variant_id
ORDER BY t.run_id, t.variant_id;

CREATE OR REPLACE VIEW task_pass_rate_trend AS
SELECT
    t.run_id,
    t.variant_id,
    t.task_id,
    round(avg(CASE WHEN t.outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate,
    count(*) AS n_trials
FROM all_trials t
GROUP BY t.run_id, t.variant_id, t.task_id
ORDER BY t.run_id, t.variant_id, t.task_id;
",
        all_trials_sql, all_runs_sql
    );
    conn.execute_batch(&sql).with_context(|| {
        format!(
            "failed to materialize project DuckDB views in {}",
            db_path.display()
        )
    })?;

    Ok(db_path)
}

#[cfg(feature = "duckdb_engine")]
fn execute_select_query(conn: &Connection, sql: &str) -> Result<QueryTable> {
    let normalized = normalize_sql(sql)?;
    let column_probe_sql = format!("SELECT * FROM ({}) AS __q LIMIT 0", normalized);
    let mut column_stmt = conn
        .prepare(&column_probe_sql)
        .with_context(|| format!("failed to inspect query columns: {}", normalized))?;
    let columns = column_stmt
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    drop(column_stmt);

    let row_json_sql = format!(
        "SELECT to_json(__q) AS row_json FROM ({}) AS __q",
        normalized
    );
    let mut stmt = conn
        .prepare(&row_json_sql)
        .with_context(|| format!("failed to prepare query: {}", normalized))?;
    let mut rows = stmt
        .query([])
        .with_context(|| format!("failed to execute query: {}", normalized))?;

    let mut out_rows: Vec<Vec<Value>> = Vec::new();
    while let Some(row) = rows.next()? {
        let raw: Option<String> = row.get(0)?;
        let parsed = match raw {
            Some(text) => serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text)),
            None => Value::Null,
        };
        if let Some(obj) = parsed.as_object() {
            let mut out = Vec::with_capacity(columns.len());
            for column in &columns {
                out.push(obj.get(column).cloned().unwrap_or(Value::Null));
            }
            out_rows.push(out);
        } else if columns.is_empty() {
            out_rows.push(vec![parsed]);
        } else {
            out_rows.push(vec![parsed; columns.len()]);
        }
    }

    Ok(QueryTable {
        columns,
        rows: out_rows,
    })
}

fn validate_read_only_sql(sql: &str) -> Result<String> {
    let normalized = normalize_sql(sql)?;
    let lower = normalized.to_ascii_lowercase();
    let starters = ["select", "with", "show", "describe", "pragma", "explain"];
    if !starters.iter().any(|prefix| lower.starts_with(prefix)) {
        return Err(anyhow!(
            "lab query only supports read-only SQL starting with SELECT/WITH/SHOW/DESCRIBE/PRAGMA/EXPLAIN"
        ));
    }

    let forbidden = [
        "insert", "update", "delete", "drop", "alter", "create", "attach", "detach", "copy",
        "vacuum", "install", "load",
    ];
    for token in lower
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|token| !token.is_empty())
    {
        if forbidden.contains(&token) {
            return Err(anyhow!(
                "lab query only supports read-only SQL (found forbidden keyword '{}')",
                token
            ));
        }
    }

    Ok(normalized)
}

fn normalize_sql(sql: &str) -> Result<String> {
    let mut normalized = sql.trim();
    while normalized.ends_with(';') {
        normalized = normalized[..normalized.len() - 1].trim_end();
    }
    if normalized.is_empty() {
        return Err(anyhow!("query cannot be empty"));
    }
    if normalized.contains(';') {
        return Err(anyhow!(
            "multiple SQL statements are not supported in a single query"
        ));
    }
    Ok(normalized.to_string())
}

fn is_safe_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn binding_value_to_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_literal_path(path: &Path) -> String {
    sql_literal(&path.to_string_lossy())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_ab_test_for_two_variant_paired_interleaved() {
        let design = ExperimentDesign {
            comparison: "paired".to_string(),
            scheduling: "paired_interleaved".to_string(),
            variant_count: 2,
        };
        assert_eq!(view_set_for_design(&design), ViewSet::AbTest);
    }

    #[test]
    fn picks_multi_variant_for_three_variant_paired_interleaved() {
        let design = ExperimentDesign {
            comparison: "paired".to_string(),
            scheduling: "paired_interleaved".to_string(),
            variant_count: 3,
        };
        assert_eq!(view_set_for_design(&design), ViewSet::MultiVariant);
    }

    #[test]
    fn picks_parameter_sweep_for_unpaired_variant_sequential() {
        let design = ExperimentDesign {
            comparison: "unpaired".to_string(),
            scheduling: "variant_sequential".to_string(),
            variant_count: 5,
        };
        assert_eq!(view_set_for_design(&design), ViewSet::ParameterSweep);
    }

    #[test]
    fn picks_regression_when_comparison_is_none() {
        let design = ExperimentDesign {
            comparison: "none".to_string(),
            scheduling: "variant_sequential".to_string(),
            variant_count: 1,
        };
        assert_eq!(view_set_for_design(&design), ViewSet::Regression);
    }

    #[test]
    fn parse_design_uses_policy_fields_and_variant_plan() {
        let resolved = json!({
            "design": {
                "comparison": "paired",
                "policies": {
                    "comparison": "unpaired",
                    "scheduling": "paired_interleaved"
                }
            },
            "baseline": { "variant_id": "base" },
            "variant_plan": [
                { "variant_id": "v1" },
                { "variant_id": "v2" }
            ]
        });
        let parsed = parse_experiment_design(&resolved);
        assert_eq!(parsed.comparison, "unpaired");
        assert_eq!(parsed.scheduling, "paired_interleaved");
        assert_eq!(parsed.variant_count, 3);
    }

    #[test]
    fn query_validation_rejects_writes() {
        let err = validate_read_only_sql("SELECT * FROM trials; DROP TABLE trials")
            .expect_err("should reject multi statement");
        assert!(err.to_string().contains("multiple SQL statements"));

        let err = validate_read_only_sql("DELETE FROM trials").expect_err("should reject delete");
        assert!(err.to_string().contains("read-only"));
    }
}
