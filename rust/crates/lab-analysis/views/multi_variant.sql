CREATE OR REPLACE VIEW heatmap AS
PIVOT (
    SELECT
        task_id,
        variant_id,
        CASE WHEN outcome = 'success' THEN 1 ELSE 0 END AS pass
    FROM trials
) ON variant_id USING first(pass)
ORDER BY task_id;

CREATE OR REPLACE VIEW variant_ranking AS
WITH rates AS (
    SELECT
        variant_id,
        avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END) AS pass_rate,
        avg(try_cast(primary_metric_value AS DOUBLE)) AS mean_primary_metric,
        count(*) AS n
    FROM trials
    GROUP BY variant_id
),
baseline AS (
    SELECT pass_rate
    FROM rates
    WHERE variant_id = (SELECT min(baseline_id) FROM trials)
)
SELECT
    r.variant_id,
    r.pass_rate,
    r.pass_rate - b.pass_rate AS diff_vs_baseline,
    2 * asin(sqrt(r.pass_rate)) - 2 * asin(sqrt(b.pass_rate)) AS cohens_h,
    r.mean_primary_metric,
    r.n
FROM rates r
CROSS JOIN baseline b
ORDER BY r.pass_rate DESC, r.mean_primary_metric DESC NULLS LAST, r.variant_id;

CREATE OR REPLACE VIEW pairwise_comparisons AS
SELECT
    a.variant_id AS variant_a,
    b.variant_id AS variant_b,
    count(*) AS n_tasks,
    count(*) FILTER (WHERE a.outcome = 'success' AND b.outcome <> 'success') AS a_wins,
    count(*) FILTER (WHERE b.outcome = 'success' AND a.outcome <> 'success') AS b_wins,
    count(*) FILTER (WHERE a.outcome = b.outcome) AS ties
FROM trials a
JOIN trials b
    ON a.task_id = b.task_id
   AND a.repl_idx = b.repl_idx
WHERE a.variant_id < b.variant_id
GROUP BY a.variant_id, b.variant_id
ORDER BY a.variant_id, b.variant_id;

CREATE OR REPLACE VIEW consensus_tasks AS
SELECT
    task_id,
    count(DISTINCT outcome) AS outcome_diversity,
    first(outcome) AS unanimous_outcome,
    count(*) AS n_trials
FROM trials
GROUP BY task_id
HAVING count(DISTINCT outcome) = 1
ORDER BY task_id;
