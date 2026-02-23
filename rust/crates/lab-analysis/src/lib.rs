use anyhow::{anyhow, Context, Result};
#[cfg(feature = "duckdb_engine")]
use duckdb::Connection;
#[cfg(feature = "duckdb_engine")]
use include_dir::{include_dir, Dir};
#[cfg(feature = "duckdb_engine")]
use lab_core::ensure_dir;
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
#[cfg(feature = "duckdb_engine")]
use std::path::PathBuf;

#[cfg(feature = "duckdb_engine")]
static VIEW_BUNDLES: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/views");

const FACTS_DIR: &str = "facts";
#[cfg(feature = "duckdb_engine")]
const RUNTIME_DIR: &str = "runtime";
#[cfg(feature = "duckdb_engine")]
const FACTS_TRIALS_FILE: &str = "trials.jsonl";
#[cfg(feature = "duckdb_engine")]
const FACTS_METRICS_LONG_FILE: &str = "metrics_long.jsonl";
#[cfg(feature = "duckdb_engine")]
const FACTS_EVENTS_FILE: &str = "events.jsonl";
#[cfg(feature = "duckdb_engine")]
const FACTS_VARIANT_SNAPSHOTS_FILE: &str = "variant_snapshots.jsonl";
#[cfg(feature = "duckdb_engine")]
const SLOT_COMMIT_JOURNAL_FILE: &str = "slot_commit_journal.jsonl";
#[cfg(feature = "duckdb_engine")]
const SCHEDULE_PROGRESS_FILE: &str = "schedule_progress.json";

#[cfg(feature = "duckdb_engine")]
const ANALYSIS_DB_FILE: &str = "agentlab.duckdb";
#[cfg(feature = "duckdb_engine")]
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

    pub fn headline_view(self) -> Option<&'static str> {
        match self {
            Self::AbTest => Some("win_loss_tie"),
            Self::MultiVariant => Some("variant_ranking"),
            Self::ParameterSweep => Some("best_config"),
            Self::Regression => Some("pass_rate_trend"),
            Self::CoreOnly => None,
        }
    }

    #[cfg(feature = "duckdb_engine")]
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
    #[cfg(feature = "duckdb_engine")]
    run_dir: PathBuf,
    #[cfg(feature = "duckdb_engine")]
    analysis_dir: PathBuf,
    #[cfg(feature = "duckdb_engine")]
    facts_dir: PathBuf,
    #[cfg(feature = "duckdb_engine")]
    comparison_policy: String,
    #[cfg(feature = "duckdb_engine")]
    scheduling_policy: String,
    view_set: ViewSet,
}

#[derive(Debug, Clone)]
pub struct QueryTable {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub fn run_view_set(run_dir: &Path) -> Result<ViewSet> {
    let context = load_run_context(run_dir)?;
    Ok(context.view_set)
}

#[cfg(not(feature = "duckdb_engine"))]
fn duckdb_disabled_error(op: &str) -> anyhow::Error {
    anyhow!(
        "DuckDB support is disabled in this build; '{}' is unavailable (enable feature 'duckdb_engine' on lab-analysis)",
        op
    )
}

#[cfg(feature = "duckdb_engine")]
pub fn list_views(run_dir: &Path) -> Result<Vec<String>> {
    let context = load_run_context(run_dir)?;
    let list_sql = "SELECT view_name AS table_name
                    FROM duckdb_views()
                    WHERE schema_name = 'main'
                    ORDER BY view_name";
    let table = match query_run_with_materialized_db(&context, list_sql) {
        Ok(table) => table,
        Err(err) if is_duckdb_lock_contention_error(&err) => {
            query_run_with_ephemeral_db(&context, list_sql).with_context(|| {
                format!(
                    "DuckDB lock contention while listing views for {}. Falling back to in-memory view materialization.",
                    run_dir.display()
                )
            })?
        }
        Err(err) => return Err(err),
    };
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
    match query_run_with_materialized_db(&context, &normalized) {
        Ok(table) => Ok(table),
        Err(err) if is_duckdb_lock_contention_error(&err) => {
            query_run_with_ephemeral_db(&context, &normalized).with_context(|| {
                format!(
                    "DuckDB lock contention for {}. Falling back to in-memory query execution.",
                    run_dir.display()
                )
            })
        }
        Err(err) => Err(err),
    }
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

fn load_run_context(run_dir: &Path) -> Result<RunAnalysisContext> {
    let canonical = run_dir
        .canonicalize()
        .map_err(|_| anyhow!("run directory not found: {}", run_dir.display()))?;
    #[cfg(feature = "duckdb_engine")]
    let analysis_dir = canonical.join("analysis");
    let facts_dir = canonical.join(FACTS_DIR);
    if !facts_dir.exists() {
        return Err(anyhow!("run facts not found: {}", facts_dir.display()));
    }
    let resolved = read_resolved_experiment(&canonical)?;
    let design = resolved
        .as_ref()
        .map(parse_experiment_design)
        .unwrap_or_else(default_experiment_design);
    let view_set = view_set_for_design(&design);
    Ok(RunAnalysisContext {
        #[cfg(feature = "duckdb_engine")]
        run_dir: canonical,
        #[cfg(feature = "duckdb_engine")]
        analysis_dir,
        #[cfg(feature = "duckdb_engine")]
        facts_dir,
        #[cfg(feature = "duckdb_engine")]
        comparison_policy: design.comparison,
        #[cfg(feature = "duckdb_engine")]
        scheduling_policy: design.scheduling,
        view_set,
    })
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

#[cfg(feature = "duckdb_engine")]
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

#[cfg(feature = "duckdb_engine")]
fn build_load_sql_relative(context: &RunAnalysisContext, bundle_sql: Option<&str>) -> String {
    let mut sql = String::from(
        "-- Run from analysis directory:
-- duckdb .lab/runs/<run_id>/analysis/agentlab.duckdb < load_duckdb.sql

LOAD json;
",
    );
    sql.push_str(&build_fact_views_sql(
        &sql_literal("../facts/trials.jsonl"),
        &sql_literal("../facts/metrics_long.jsonl"),
        &sql_literal("../facts/events.jsonl"),
        &sql_literal("../facts/variant_snapshots.jsonl"),
        &sql_literal("../runtime/slot_commit_journal.jsonl"),
        &sql_literal("../runtime/schedule_progress.json"),
    ));
    let metadata_sql = build_metadata_view_sql(context);
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

#[cfg(feature = "duckdb_engine")]
fn build_load_sql_absolute(context: &RunAnalysisContext, bundle_sql: Option<&str>) -> String {
    let trials_path = context.facts_dir.join(FACTS_TRIALS_FILE);
    let metrics_path = context.facts_dir.join(FACTS_METRICS_LONG_FILE);
    let events_path = context.facts_dir.join(FACTS_EVENTS_FILE);
    let variant_snapshots_path = context.facts_dir.join(FACTS_VARIANT_SNAPSHOTS_FILE);
    let slot_commit_journal_path = context
        .run_dir
        .join("runtime")
        .join("slot_commit_journal.jsonl");
    let schedule_progress_path = context
        .run_dir
        .join("runtime")
        .join("schedule_progress.json");
    let mut sql = String::from("LOAD json;\n");
    sql.push_str(&build_fact_views_sql(
        &sql_literal_path(&trials_path),
        &sql_literal_path(&metrics_path),
        &sql_literal_path(&events_path),
        &sql_literal_path(&variant_snapshots_path),
        &sql_literal_path(&slot_commit_journal_path),
        &sql_literal_path(&schedule_progress_path),
    ));
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

#[cfg(feature = "duckdb_engine")]
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
fn build_fact_views_sql(
    trials_path: &str,
    metrics_long_path: &str,
    events_path: &str,
    variant_snapshots_path: &str,
    slot_commit_journal_path: &str,
    schedule_progress_path: &str,
) -> String {
    format!(
        "CREATE OR REPLACE VIEW slot_commit_journal_commits AS
WITH raw AS (
    SELECT to_json(r) AS row_json
    FROM read_json_auto({}, format='newline_delimited', union_by_name=true) AS r
)
SELECT
    try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT) AS schedule_idx,
    json_extract_string(row_json, '$.slot_commit_id') AS slot_commit_id
FROM raw
WHERE json_extract_string(row_json, '$.record_type') = 'commit';

CREATE OR REPLACE VIEW schedule_progress_runtime AS
SELECT
    coalesce(try_cast(next_schedule_index AS BIGINT), 9223372036854775807) AS next_schedule_index
FROM read_json_auto({}, union_by_name=true);

CREATE OR REPLACE VIEW committed_slot_publications AS
SELECT
    c.schedule_idx,
    c.slot_commit_id
FROM slot_commit_journal_commits c
CROSS JOIN schedule_progress_runtime p
WHERE c.schedule_idx < p.next_schedule_index;

CREATE OR REPLACE VIEW committed_slot_guard AS
SELECT count(*) AS committed_count
FROM committed_slot_publications;

CREATE OR REPLACE VIEW trials AS
WITH raw AS (
    SELECT to_json(r) AS row_json
    FROM read_json_auto({}, format='newline_delimited', union_by_name=true) AS r
),
filtered AS (
    SELECT row_json
    FROM raw
    WHERE (
        (
            json_extract_string(row_json, '$.slot_commit_id') IS NULL
            OR try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT) IS NULL
        )
        AND (SELECT committed_count FROM committed_slot_guard) = 0
    )
    OR EXISTS (
        SELECT 1
        FROM committed_slot_publications c
        WHERE c.slot_commit_id = json_extract_string(row_json, '$.slot_commit_id')
          AND c.schedule_idx = try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT)
    )
)
SELECT
    json_extract_string(row_json, '$.run_id') AS run_id,
    json_extract_string(row_json, '$.trial_id') AS trial_id,
    try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT) AS schedule_idx,
    json_extract_string(row_json, '$.slot_commit_id') AS slot_commit_id,
    try_cast(json_extract(row_json, '$.attempt') AS BIGINT) AS attempt,
    try_cast(json_extract(row_json, '$.row_seq') AS BIGINT) AS row_seq,
    json_extract_string(row_json, '$.variant_id') AS variant_id,
    json_extract_string(row_json, '$.baseline_id') AS baseline_id,
    json_extract_string(row_json, '$.task_id') AS task_id,
    try_cast(json_extract(row_json, '$.repl_idx') AS BIGINT) AS repl_idx,
    json_extract_string(row_json, '$.outcome') AS outcome,
    json_extract_string(row_json, '$.primary_metric_name') AS primary_metric_name,
    json_extract_string(row_json, '$.primary_metric_value') AS primary_metric_value,
    json_extract(row_json, '$.bindings') AS bindings
FROM filtered;

CREATE OR REPLACE VIEW metrics_long AS
WITH raw AS (
    SELECT to_json(r) AS row_json
    FROM read_json_auto({}, format='newline_delimited', union_by_name=true) AS r
),
filtered AS (
    SELECT row_json
    FROM raw
    WHERE (
        (
            json_extract_string(row_json, '$.slot_commit_id') IS NULL
            OR try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT) IS NULL
        )
        AND (SELECT committed_count FROM committed_slot_guard) = 0
    )
    OR EXISTS (
        SELECT 1
        FROM committed_slot_publications c
        WHERE c.slot_commit_id = json_extract_string(row_json, '$.slot_commit_id')
          AND c.schedule_idx = try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT)
    )
)
SELECT
    json_extract_string(row_json, '$.run_id') AS run_id,
    json_extract_string(row_json, '$.trial_id') AS trial_id,
    try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT) AS schedule_idx,
    json_extract_string(row_json, '$.slot_commit_id') AS slot_commit_id,
    try_cast(json_extract(row_json, '$.attempt') AS BIGINT) AS attempt,
    try_cast(json_extract(row_json, '$.row_seq') AS BIGINT) AS row_seq,
    json_extract_string(row_json, '$.variant_id') AS variant_id,
    json_extract_string(row_json, '$.task_id') AS task_id,
    json_extract_string(row_json, '$.metric_name') AS metric_name,
    json_extract_string(row_json, '$.metric_value') AS metric_value
FROM filtered;

CREATE OR REPLACE VIEW events AS
WITH raw AS (
    SELECT to_json(r) AS row_json
    FROM read_json_auto({}, format='newline_delimited', union_by_name=true) AS r
),
filtered AS (
    SELECT row_json
    FROM raw
    WHERE (
        (
            json_extract_string(row_json, '$.slot_commit_id') IS NULL
            OR try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT) IS NULL
        )
        AND (SELECT committed_count FROM committed_slot_guard) = 0
    )
    OR EXISTS (
        SELECT 1
        FROM committed_slot_publications c
        WHERE c.slot_commit_id = json_extract_string(row_json, '$.slot_commit_id')
          AND c.schedule_idx = try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT)
    )
)
SELECT
    json_extract_string(row_json, '$.run_id') AS run_id,
    json_extract_string(row_json, '$.trial_id') AS trial_id,
    try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT) AS schedule_idx,
    json_extract_string(row_json, '$.slot_commit_id') AS slot_commit_id,
    try_cast(json_extract(row_json, '$.attempt') AS BIGINT) AS attempt,
    try_cast(json_extract(row_json, '$.row_seq') AS BIGINT) AS row_seq,
    json_extract_string(row_json, '$.variant_id') AS variant_id,
    json_extract_string(row_json, '$.event_type') AS event_type
FROM filtered;

CREATE OR REPLACE VIEW variant_snapshots AS
WITH raw AS (
    SELECT to_json(r) AS row_json
    FROM read_json_auto({}, format='newline_delimited', union_by_name=true) AS r
),
filtered AS (
    SELECT row_json
    FROM raw
    WHERE (
        (
            json_extract_string(row_json, '$.slot_commit_id') IS NULL
            OR try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT) IS NULL
        )
        AND (SELECT committed_count FROM committed_slot_guard) = 0
    )
    OR EXISTS (
        SELECT 1
        FROM committed_slot_publications c
        WHERE c.slot_commit_id = json_extract_string(row_json, '$.slot_commit_id')
          AND c.schedule_idx = try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT)
    )
)
SELECT
    json_extract_string(row_json, '$.run_id') AS run_id,
    json_extract_string(row_json, '$.trial_id') AS trial_id,
    try_cast(json_extract(row_json, '$.schedule_idx') AS BIGINT) AS schedule_idx,
    json_extract_string(row_json, '$.slot_commit_id') AS slot_commit_id,
    try_cast(json_extract(row_json, '$.attempt') AS BIGINT) AS attempt,
    try_cast(json_extract(row_json, '$.row_seq') AS BIGINT) AS row_seq,
    json_extract_string(row_json, '$.variant_id') AS variant_id,
    json_extract_string(row_json, '$.task_id') AS task_id,
    try_cast(json_extract(row_json, '$.repl_idx') AS BIGINT) AS repl_idx,
    json_extract_string(row_json, '$.binding_name') AS binding_name,
    json_extract(row_json, '$.binding_value') AS binding_value,
    json_extract_string(row_json, '$.binding_value_text') AS binding_value_text
FROM filtered;

CREATE OR REPLACE VIEW bindings_long AS
SELECT
    run_id,
    trial_id,
    variant_id,
    task_id,
    repl_idx,
    binding_name,
    binding_value,
    binding_value_text
FROM variant_snapshots;

CREATE OR REPLACE VIEW event_counts_by_trial AS
SELECT
    run_id,
    trial_id,
    variant_id,
    event_type,
    count(*) AS count
FROM events
GROUP BY run_id, trial_id, variant_id, event_type;

CREATE OR REPLACE VIEW event_counts_by_variant AS
SELECT
    run_id,
    variant_id,
    event_type,
    count(*) AS count
FROM events
GROUP BY run_id, variant_id, event_type;

CREATE OR REPLACE VIEW variant_summary AS
SELECT
    min(baseline_id) AS baseline_id,
    variant_id,
    count(*)::DOUBLE AS total,
    avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END) AS success_rate,
    first(primary_metric_name) AS primary_metric_name,
    avg(try_cast(primary_metric_value AS DOUBLE)) AS primary_metric_mean,
    NULL AS event_counts,
    first(bindings) AS bindings
FROM trials
GROUP BY variant_id;

CREATE OR REPLACE VIEW task_variant_matrix AS
SELECT
    task_id,
    variant_id,
    round(avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate,
    count(*) AS n_trials
FROM trials
GROUP BY task_id, variant_id
ORDER BY task_id, variant_id;

CREATE OR REPLACE VIEW run_progress AS
SELECT
    run_id,
    count(*) AS completed_trials,
    count(DISTINCT variant_id) AS variants_seen,
    count(DISTINCT task_id) AS tasks_seen,
    round(avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate
FROM trials
GROUP BY run_id
ORDER BY run_id;
",
        slot_commit_journal_path,
        schedule_progress_path,
        trials_path,
        metrics_long_path,
        events_path,
        variant_snapshots_path
    )
}

#[cfg(feature = "duckdb_engine")]
fn materialize_run_duckdb(context: &RunAnalysisContext) -> Result<()> {
    ensure_dir(&context.analysis_dir)?;
    ensure_dir(&context.facts_dir)?;
    ensure_fact_files(&context.facts_dir)?;
    ensure_runtime_files(&context.run_dir)?;
    fs::write(
        context.analysis_dir.join("duckdb_view_context.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": "duckdb_view_context_v1",
            "view_set": context.view_set.as_str(),
            "comparison_policy": context.comparison_policy,
            "scheduling_policy": context.scheduling_policy
        }))?,
    )?;
    let db_path = context.analysis_dir.join(ANALYSIS_DB_FILE);
    let conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open DuckDB {}", db_path.display()))?;
    load_json_extension(&conn)?;
    let bundle_sql = load_view_bundle_sql(context.view_set)?;
    let relative_sql = build_load_sql_relative(context, bundle_sql.as_deref());
    fs::write(context.analysis_dir.join(LOAD_SQL_FILE), relative_sql)?;
    let sql = build_load_sql_absolute(context, bundle_sql.as_deref());
    conn.execute_batch(&sql)
        .with_context(|| format!("failed to materialize run DuckDB for {}", db_path.display()))?;
    Ok(())
}

#[cfg(feature = "duckdb_engine")]
fn open_run_connection(context: &RunAnalysisContext) -> Result<Connection> {
    let db_path = context.analysis_dir.join(ANALYSIS_DB_FILE);
    let conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open DuckDB {}", db_path.display()))?;
    load_json_extension(&conn)?;
    Ok(conn)
}

#[cfg(feature = "duckdb_engine")]
fn query_run_with_materialized_db(context: &RunAnalysisContext, sql: &str) -> Result<QueryTable> {
    materialize_run_duckdb(context)?;
    let conn = open_run_connection(context)?;
    execute_select_query(&conn, sql)
}

#[cfg(feature = "duckdb_engine")]
fn query_run_with_ephemeral_db(context: &RunAnalysisContext, sql: &str) -> Result<QueryTable> {
    ensure_dir(&context.facts_dir)?;
    ensure_fact_files(&context.facts_dir)?;
    ensure_runtime_files(&context.run_dir)?;
    let conn = Connection::open_in_memory()
        .context("failed to open in-memory DuckDB for fallback query")?;
    load_json_extension(&conn)?;
    let bundle_sql = load_view_bundle_sql(context.view_set)?;
    let load_sql = build_load_sql_absolute(context, bundle_sql.as_deref());
    conn.execute_batch(&load_sql)
        .context("failed to materialize in-memory DuckDB fallback views")?;
    execute_select_query(&conn, sql)
}

#[cfg(feature = "duckdb_engine")]
fn is_duckdb_lock_contention_error(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("lock")
        && (message.contains("duckdb")
            || message.contains("io error")
            || message.contains("conflicting lock")
            || message.contains("already open")
            || message.contains("resource temporarily unavailable"))
}

#[cfg(feature = "duckdb_engine")]
fn ensure_fact_files(facts_dir: &Path) -> Result<()> {
    let files = [
        FACTS_TRIALS_FILE,
        FACTS_METRICS_LONG_FILE,
        FACTS_EVENTS_FILE,
        FACTS_VARIANT_SNAPSHOTS_FILE,
    ];
    for file in files {
        let path = facts_dir.join(file);
        if !path.exists() {
            fs::write(&path, b"")
                .with_context(|| format!("failed to initialize {}", path.display()))?;
        }
    }
    Ok(())
}

#[cfg(feature = "duckdb_engine")]
fn ensure_runtime_files(run_dir: &Path) -> Result<()> {
    let runtime_dir = run_dir.join(RUNTIME_DIR);
    ensure_dir(&runtime_dir)?;
    let journal_path = runtime_dir.join(SLOT_COMMIT_JOURNAL_FILE);
    if !journal_path.exists() {
        fs::write(&journal_path, b"")
            .with_context(|| format!("failed to initialize {}", journal_path.display()))?;
    }
    let progress_path = runtime_dir.join(SCHEDULE_PROGRESS_FILE);
    if !progress_path.exists() {
        let default_progress = serde_json::json!({
            "schema_version": "schedule_progress_v2",
            "next_schedule_index": 9223372036854775807_i64
        });
        fs::write(&progress_path, serde_json::to_vec(&default_progress)?)
            .with_context(|| format!("failed to initialize {}", progress_path.display()))?;
    }
    Ok(())
}

#[cfg(feature = "duckdb_engine")]
fn load_json_extension(conn: &Connection) -> Result<()> {
    if conn.execute_batch("LOAD json;").is_ok() {
        return Ok(());
    }
    if conn.execute_batch("INSTALL json; LOAD json;").is_ok() {
        return Ok(());
    }
    Ok(())
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
            let trials_path = run_path.join(FACTS_DIR).join(FACTS_TRIALS_FILE);
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

    let mut columns: Vec<String> = Vec::new();
    let mut seen_columns: BTreeSet<String> = BTreeSet::new();
    let mut parsed_rows: Vec<Value> = Vec::new();

    while let Some(row) = rows.next()? {
        let raw: Option<String> = row.get(0)?;
        let parsed = match raw {
            Some(text) => serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text)),
            None => Value::Null,
        };
        if let Some(obj) = parsed.as_object() {
            for key in obj.keys() {
                if seen_columns.insert(key.clone()) {
                    columns.push(key.clone());
                }
            }
        }
        parsed_rows.push(parsed);
    }

    let mut out_rows: Vec<Vec<Value>> = Vec::new();
    for parsed in parsed_rows {
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

#[cfg(any(feature = "duckdb_engine", test))]
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

#[cfg(any(feature = "duckdb_engine", test))]
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

#[cfg(feature = "duckdb_engine")]
fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(feature = "duckdb_engine")]
fn sql_literal_path(path: &Path) -> String {
    sql_literal(&path.to_string_lossy())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
