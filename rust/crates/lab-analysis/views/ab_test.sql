CREATE OR REPLACE VIEW ab_variant_roles AS
WITH baseline AS (
    SELECT min(baseline_id) AS baseline_id
    FROM trials
),
treatments AS (
    SELECT DISTINCT t.variant_id
    FROM trials t, baseline b
    WHERE t.variant_id <> b.baseline_id
    ORDER BY t.variant_id
)
SELECT
    b.baseline_id,
    (SELECT variant_id FROM treatments LIMIT 1) AS treatment_id,
    (SELECT count(*) FROM treatments) AS treatment_variant_count
FROM baseline b;

CREATE OR REPLACE VIEW paired_outcomes AS
SELECT
    b.run_id,
    b.task_id,
    b.repl_idx,
    b.outcome AS baseline_outcome,
    t.outcome AS treatment_outcome,
    try_cast(b.primary_metric_value AS DOUBLE) AS baseline_metric,
    try_cast(t.primary_metric_value AS DOUBLE) AS treatment_metric,
    try_cast(t.primary_metric_value AS DOUBLE) - try_cast(b.primary_metric_value AS DOUBLE) AS metric_delta,
    CASE
        WHEN b.outcome = t.outcome THEN 'same'
        WHEN b.outcome = 'success' AND t.outcome <> 'success' THEN 'regression'
        WHEN b.outcome <> 'success' AND t.outcome = 'success' THEN 'improvement'
        ELSE 'changed'
    END AS delta_type
FROM trials b
JOIN ab_variant_roles roles ON b.variant_id = roles.baseline_id
JOIN trials t
    ON t.task_id = b.task_id
   AND t.repl_idx = b.repl_idx
   AND t.variant_id = roles.treatment_id;

CREATE OR REPLACE VIEW win_loss_tie AS
SELECT
    delta_type,
    count(*) AS n,
    round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct
FROM paired_outcomes
GROUP BY delta_type
ORDER BY n DESC, delta_type;

CREATE OR REPLACE VIEW mcnemar_contingency AS
SELECT
    count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome = 'success') AS both_pass,
    count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome <> 'success') AS base_only,
    count(*) FILTER (WHERE baseline_outcome <> 'success' AND treatment_outcome = 'success') AS treat_only,
    count(*) FILTER (WHERE baseline_outcome <> 'success' AND treatment_outcome <> 'success') AS both_fail,
    count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome <> 'success') AS b,
    count(*) FILTER (WHERE baseline_outcome <> 'success' AND treatment_outcome = 'success') AS c,
    CASE
        WHEN (
            count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome <> 'success')
            + count(*) FILTER (WHERE baseline_outcome <> 'success' AND treatment_outcome = 'success')
        ) > 0
        THEN power(
            abs(
                count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome <> 'success')
                - count(*) FILTER (WHERE baseline_outcome <> 'success' AND treatment_outcome = 'success')
            ) - 1,
            2
        ) / (
            count(*) FILTER (WHERE baseline_outcome = 'success' AND treatment_outcome <> 'success')
            + count(*) FILTER (WHERE baseline_outcome <> 'success' AND treatment_outcome = 'success')
        )
        ELSE NULL
    END AS mcnemar_chi2
FROM paired_outcomes;

CREATE OR REPLACE VIEW effect_size AS
WITH rates AS (
    SELECT
        variant_id,
        avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END) AS pass_rate,
        count(*) AS n
    FROM trials
    GROUP BY variant_id
),
roles AS (
    SELECT baseline_id, treatment_id
    FROM ab_variant_roles
)
SELECT
    b.pass_rate AS baseline_rate,
    t.pass_rate AS treatment_rate,
    t.pass_rate - b.pass_rate AS absolute_diff,
    2 * asin(sqrt(t.pass_rate)) - 2 * asin(sqrt(b.pass_rate)) AS cohens_h,
    CASE
        WHEN abs(2 * asin(sqrt(t.pass_rate)) - 2 * asin(sqrt(b.pass_rate))) < 0.2 THEN 'negligible'
        WHEN abs(2 * asin(sqrt(t.pass_rate)) - 2 * asin(sqrt(b.pass_rate))) < 0.5 THEN 'small'
        WHEN abs(2 * asin(sqrt(t.pass_rate)) - 2 * asin(sqrt(b.pass_rate))) < 0.8 THEN 'medium'
        ELSE 'large'
    END AS magnitude
FROM roles
JOIN rates b ON b.variant_id = roles.baseline_id
JOIN rates t ON t.variant_id = roles.treatment_id;

CREATE OR REPLACE VIEW task_diffs AS
SELECT *
FROM paired_outcomes
ORDER BY
    CASE delta_type WHEN 'regression' THEN 0 WHEN 'improvement' THEN 1 ELSE 2 END,
    abs(metric_delta) DESC NULLS LAST;
