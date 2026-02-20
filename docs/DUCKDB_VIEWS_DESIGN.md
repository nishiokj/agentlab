# DuckDB Views Design

## Overview

DuckDB serves as the **query layer** over AgentLab's existing JSONL output. JSONL remains the append-only, hashchained source of truth. DuckDB reads JSONL in place via `read_json_auto` — no import step, no data duplication.

### Integration depth

| Layer | Status |
|---|---|
| JSONL tables written by `lab-analysis` | Exists today |
| `load_duckdb.sql` generated per run | Exists today |
| Auto-execute SQL at end of run | **To build** |
| Embed `duckdb-rs` in `lab-cli` | **To build** |
| `lab query` / `lab views` subcommands | **To build** |
| Opinionated view bundles per experiment type | **To build** |
| Cross-run views for regression tracking | **To build** |

## Data flow

```
Runner executes trials
  → writes JSONL (trials.jsonl, metrics_long.jsonl, event_counts_*.jsonl, etc.)
  → hashchained events.jsonl stays as provenance source of truth
  → DuckDB reads JSONL in place (read_json_auto)
  → Opinionated views materialized based on experiment type
  → CLI renders views to terminal or exports
```

## Experiment type → View set mapping

The experiment type (from `design.policies.comparison` + `design.policies.scheduling`) determines which statistical views are valid. Each type ships a SQL view bundle, embedded in `lab-analysis` via `include_dir`.

```
resolved_experiment.json
        │
        ├─ read design.policies.comparison
        │   → "paired" | "unpaired" | "none"
        │
        ├─ read design.policies.scheduling
        │   → "paired_interleaved" | "variant_sequential"
        │
        └─ select ViewSet
            ├── AB_TEST         → paired_views.sql
            ├── MULTI_VARIANT   → multi_variant_views.sql
            ├── PARAMETER_SWEEP → sweep_views.sql
            └── REGRESSION      → regression_views.sql
```

---

## AB_TEST views

**Design:** `paired_interleaved` scheduling, `paired` comparison, 1 baseline vs 1 treatment.

**Core question:** Did the treatment cause a difference?

### Paired outcome table

Side-by-side comparison of baseline vs treatment on each task.

```sql
CREATE VIEW paired_outcomes AS
SELECT
    b.task_id,
    b.repl_idx,
    b.outcome AS baseline_outcome,
    t.outcome AS treatment_outcome,
    b.primary_metric_value AS baseline_metric,
    t.primary_metric_value AS treatment_metric,
    t.primary_metric_value - b.primary_metric_value AS metric_delta,
    CASE
        WHEN b.outcome = t.outcome THEN 'same'
        WHEN b.outcome = 'success' AND t.outcome != 'success' THEN 'regression'
        WHEN b.outcome != 'success' AND t.outcome = 'success' THEN 'improvement'
        ELSE 'changed'
    END AS delta_type
FROM trials b
JOIN trials t USING (task_id, repl_idx)
WHERE b.variant_id = b.baseline_id
  AND t.variant_id != t.baseline_id;
```

### Win/loss/tie summary

Aggregate how many tasks flipped in each direction.

```sql
CREATE VIEW win_loss_tie AS
SELECT
    delta_type,
    count(*) AS n,
    round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct
FROM paired_outcomes
GROUP BY delta_type
ORDER BY n DESC;
```

### McNemar's test inputs

Provides the 2x2 contingency table for McNemar's test (the correct significance test for paired binary outcomes — not a t-test).

```sql
CREATE VIEW mcnemar_contingency AS
SELECT
    count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome = 'success') AS both_pass,
    count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome != 'success') AS base_only,
    count(*) FILTER (WHERE baseline_outcome != 'success' AND treatment_outcome = 'success') AS treat_only,
    count(*) FILTER (WHERE baseline_outcome != 'success' AND treatment_outcome != 'success') AS both_fail,
    -- Discordant pairs (the cells McNemar's test uses)
    count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome != 'success') AS b,
    count(*) FILTER (WHERE baseline_outcome != 'success' AND treatment_outcome = 'success') AS c,
    -- McNemar chi-squared (with continuity correction)
    CASE WHEN (count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome != 'success')
             + count(*) FILTER (WHERE baseline_outcome != 'success' AND treatment_outcome = 'success')) > 0
        THEN power(
            abs(count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome != 'success')
              - count(*) FILTER (WHERE baseline_outcome != 'success' AND treatment_outcome = 'success')) - 1,
            2
        ) / (count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome != 'success')
           + count(*) FILTER (WHERE baseline_outcome != 'success' AND treatment_outcome = 'success'))
        ELSE NULL
    END AS mcnemar_chi2
FROM paired_outcomes;
```

### Effect size (Cohen's h)

Magnitude of difference in pass rates, independent of sample size.

```sql
CREATE VIEW effect_size AS
WITH rates AS (
    SELECT
        variant_id,
        avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END) AS pass_rate,
        count(*) AS n
    FROM trials
    GROUP BY variant_id
)
SELECT
    b.pass_rate AS baseline_rate,
    t.pass_rate AS treatment_rate,
    t.pass_rate - b.pass_rate AS absolute_diff,
    -- Cohen's h = 2 * arcsin(sqrt(p1)) - 2 * arcsin(sqrt(p2))
    2 * asin(sqrt(t.pass_rate)) - 2 * asin(sqrt(b.pass_rate)) AS cohens_h,
    CASE
        WHEN abs(2 * asin(sqrt(t.pass_rate)) - 2 * asin(sqrt(b.pass_rate))) < 0.2 THEN 'negligible'
        WHEN abs(2 * asin(sqrt(t.pass_rate)) - 2 * asin(sqrt(b.pass_rate))) < 0.5 THEN 'small'
        WHEN abs(2 * asin(sqrt(t.pass_rate)) - 2 * asin(sqrt(b.pass_rate))) < 0.8 THEN 'medium'
        ELSE 'large'
    END AS magnitude
FROM rates b, rates t
WHERE b.variant_id = (SELECT baseline_id FROM trials LIMIT 1)
  AND t.variant_id != b.variant_id;
```

### Task-level diffs

Tasks sorted by "most interesting" — flips first, then by metric delta magnitude.

```sql
CREATE VIEW task_diffs AS
SELECT *
FROM paired_outcomes
ORDER BY
    CASE delta_type WHEN 'regression' THEN 0 WHEN 'improvement' THEN 1 ELSE 2 END,
    abs(metric_delta) DESC;
```

---

## MULTI_VARIANT views

**Design:** `paired_interleaved` scheduling, `paired` comparison, 1 baseline vs N treatments.

**Core question:** Which variant performs best, and are the differences significant?

### Heatmap (task x variant grid)

```sql
CREATE VIEW heatmap AS
PIVOT (
    SELECT task_id, variant_id,
           CASE WHEN outcome = 'success' THEN 1 ELSE 0 END AS pass
    FROM trials
) ON variant_id USING first(pass)
ORDER BY task_id;
```

### Variant ranking

All variants ranked by effect size vs baseline.

```sql
CREATE VIEW variant_ranking AS
WITH rates AS (
    SELECT variant_id,
           avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END) AS pass_rate,
           count(*) AS n
    FROM trials
    GROUP BY variant_id
),
baseline AS (
    SELECT pass_rate FROM rates
    WHERE variant_id = (SELECT baseline_id FROM trials LIMIT 1)
)
SELECT
    r.variant_id,
    r.pass_rate,
    r.pass_rate - bl.pass_rate AS diff_vs_baseline,
    2 * asin(sqrt(r.pass_rate)) - 2 * asin(sqrt(bl.pass_rate)) AS cohens_h,
    r.n
FROM rates r, baseline bl
ORDER BY r.pass_rate DESC;
```

### Pairwise comparisons

Every variant pair with discordant pair counts.

```sql
CREATE VIEW pairwise_comparisons AS
SELECT
    a.variant_id AS variant_a,
    b.variant_id AS variant_b,
    count(*) AS n_tasks,
    count(*) FILTER (WHERE a.outcome = 'success' AND b.outcome != 'success') AS a_wins,
    count(*) FILTER (WHERE b.outcome = 'success' AND a.outcome != 'success') AS b_wins,
    count(*) FILTER (WHERE a.outcome = b.outcome) AS ties
FROM trials a
JOIN trials b USING (task_id, repl_idx)
WHERE a.variant_id < b.variant_id
GROUP BY a.variant_id, b.variant_id
ORDER BY a.variant_id, b.variant_id;
```

### Consensus tasks

Tasks all variants agree on — universally easy or universally hard.

```sql
CREATE VIEW consensus_tasks AS
SELECT
    task_id,
    count(DISTINCT outcome) AS outcome_diversity,
    first(outcome) AS unanimous_outcome,
    count(*) AS n_trials
FROM trials
GROUP BY task_id
HAVING count(DISTINCT outcome) = 1
ORDER BY task_id;
```

---

## PARAMETER_SWEEP views

**Design:** `variant_sequential` scheduling, `unpaired` comparison, exploring parameter space.

**Core question:** Which parameter values produce the best outcome?

### Parameter vs metric

Each swept knob value plotted against the primary metric. The `bindings` field in trials contains the knob values — these need to be extracted per the knob manifest.

```sql
-- Assumes bindings are flattened into the trials table or joined from trial_input.
-- Actual implementation will read bindings from the trial input JSONL.
CREATE VIEW parameter_metric AS
SELECT
    variant_id,
    primary_metric_name,
    avg(primary_metric_value) AS mean_metric,
    stddev(primary_metric_value) AS std_metric,
    count(*) AS n
FROM trials
GROUP BY variant_id, primary_metric_name
ORDER BY mean_metric DESC;
```

### Best configuration

Top parameter combinations by metric.

```sql
CREATE VIEW best_config AS
SELECT
    variant_id,
    round(avg(primary_metric_value), 4) AS mean_metric,
    round(avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate,
    count(*) AS n_trials
FROM trials
GROUP BY variant_id
ORDER BY mean_metric DESC
LIMIT 10;
```

### Sensitivity ranking

Which parameter had the largest effect on the metric? Uses variance of group means as a simple sensitivity proxy.

```sql
CREATE VIEW sensitivity AS
WITH variant_means AS (
    SELECT variant_id,
           avg(primary_metric_value) AS mean_val
    FROM trials
    GROUP BY variant_id
)
SELECT
    variance(mean_val) AS inter_variant_variance,
    max(mean_val) - min(mean_val) AS range,
    count(*) AS n_variants
FROM variant_means;
```

---

## REGRESSION views

**Design:** `variant_sequential` scheduling, `none` comparison, tracking over time.

**Core question:** Is it still working? What broke?

### Pass rate trend (cross-run)

Requires the cross-run view that unions all runs for a given experiment.

```sql
CREATE VIEW pass_rate_trend AS
SELECT
    run_id,
    variant_id,
    round(avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate,
    count(*) AS n_trials
FROM all_trials  -- cross-run union view
GROUP BY run_id, variant_id
ORDER BY run_id;
```

### Flaky task report

Tasks with high variance across replications within a single run.

```sql
CREATE VIEW flaky_tasks AS
SELECT
    task_id,
    count(*) AS n_replications,
    sum(CASE WHEN outcome = 'success' THEN 1 ELSE 0 END) AS passes,
    sum(CASE WHEN outcome != 'success' THEN 1 ELSE 0 END) AS failures,
    round(avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate
FROM trials
GROUP BY task_id
HAVING count(DISTINCT outcome) > 1  -- not unanimous
ORDER BY pass_rate ASC;
```

### Failure clustering

Are failures concentrated in one repo/category or spread evenly?

```sql
CREATE VIEW failure_clusters AS
SELECT
    -- Extract repo from task_id (convention: repo__project-NNNN)
    split_part(task_id, '__', 1) AS repo,
    count(*) AS total,
    sum(CASE WHEN outcome != 'success' THEN 1 ELSE 0 END) AS failures,
    round(1.0 - avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS failure_rate
FROM trials
GROUP BY repo
ORDER BY failure_rate DESC;
```

---

## CLI interface

### Per-run views

```
lab views <run_id>                      # list available views for this run's experiment type
lab views <run_id> paired-diffs         # render the paired outcome table
lab views <run_id> win-loss-tie         # win/loss/tie summary
lab views <run_id> effect-size          # effect size + significance
lab views <run_id> heatmap              # task x variant grid (terminal-rendered)
lab views <run_id> --all                # dump all views
```

### Ad-hoc query

```
lab query <run_id> "SELECT task_id, outcome FROM trials WHERE success = false"
```

### Cross-run trend

```
lab trend <experiment_id>                               # pass rate across all runs
lab trend <experiment_id> --task django__django-15814    # single task over time
```

---

## Implementation notes

### Rust integration

Use `duckdb-rs` crate. Core pattern:

```rust
use duckdb::Connection;

let db = Connection::open_in_memory()?;
db.execute_batch("INSTALL json; LOAD json;")?;

let query = format!(
    "SELECT variant_id, avg(success::int) as pass_rate
     FROM read_json_auto('{}', format='newline_delimited')
     GROUP BY 1",
    trials_jsonl_path.display()
);

let mut stmt = db.prepare(&query)?;
// iterate rows...
```

### View bundles

Ship as embedded SQL files in `lab-analysis` (same `include_dir` pattern used for schemas). Each experiment type maps to a `.sql` file containing the view definitions for that type.

### Cross-run views

A root-level `.lab/agentlab.duckdb` that unions all runs:

```sql
CREATE VIEW all_trials AS
SELECT * FROM read_json_auto('.lab/runs/*/analysis/tables/trials.jsonl');
```

DuckDB glob patterns allow querying across all runs without manual concatenation.

### JSONL stays as source of truth

DuckDB reads JSONL in place — no duplication. The hashchained `events.jsonl` and content-addressed `ArtifactStore` remain the provenance layer. DuckDB is a disposable, rebuildable query lens.
