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
    (SELECT count(*) FROM treatments) AS treatment_variant_count,
    b.baseline_id AS variant_a_id,
    (SELECT variant_id FROM treatments LIMIT 1) AS variant_b_id
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

CREATE OR REPLACE VIEW ab_task_outcomes AS
SELECT
    b.task_id,
    b.outcome AS a_outcome,
    t.outcome AS b_outcome,
    try_cast(b.primary_metric_value AS DOUBLE) AS a_result_score,
    try_cast(t.primary_metric_value AS DOUBLE) AS b_result_score,
    b.trial_id AS a_trial_id,
    t.trial_id AS b_trial_id
FROM trials b
JOIN ab_variant_roles roles ON b.variant_id = roles.variant_a_id
JOIN trials t
    ON t.task_id = b.task_id
   AND t.repl_idx = b.repl_idx
   AND t.variant_id = roles.variant_b_id
ORDER BY b.task_id, b.repl_idx;

CREATE OR REPLACE VIEW trial_metrics AS
SELECT
    t.run_id,
    t.task_id,
    t.repl_idx,
    t.trial_id,
    t.variant_id,
    t.outcome,
    try_cast(t.primary_metric_value AS DOUBLE) AS result_score,
    max(CASE WHEN m.metric_name = 'resolved' THEN try_cast(m.metric_value AS DOUBLE) END) AS resolved_score,
    max(CASE WHEN m.metric_name = 'latency_ms' THEN try_cast(m.metric_value AS DOUBLE) END) AS latency_ms,
    max(CASE WHEN m.metric_name = 'tokens_in' THEN try_cast(m.metric_value AS DOUBLE) END) AS tokens_in,
    max(CASE WHEN m.metric_name = 'tokens_out' THEN try_cast(m.metric_value AS DOUBLE) END) AS tokens_out,
    max(CASE WHEN m.metric_name = 'total_tokens' THEN try_cast(m.metric_value AS DOUBLE) END) AS total_tokens,
    max(CASE WHEN m.metric_name = 'turn_count' THEN try_cast(m.metric_value AS DOUBLE) END) AS turn_count,
    max(CASE WHEN m.metric_name = 'tool_call_count' THEN try_cast(m.metric_value AS DOUBLE) END) AS tool_call_count,
    max(CASE WHEN m.metric_name = 'status_code' THEN try_cast(m.metric_value AS DOUBLE) END) AS status_code
FROM trials t
LEFT JOIN metrics_long m
    ON m.run_id = t.run_id
   AND m.trial_id = t.trial_id
GROUP BY
    t.run_id,
    t.task_id,
    t.repl_idx,
    t.trial_id,
    t.variant_id,
    t.outcome,
    t.primary_metric_value;

CREATE OR REPLACE VIEW ab_task_metrics_side_by_side AS
SELECT
    a.task_id,
    a.repl_idx,
    roles.variant_a_id AS a_variant_id,
    roles.variant_b_id AS b_variant_id,
    a.trial_id AS a_trial_id,
    b.trial_id AS b_trial_id,
    a.outcome AS a_outcome,
    b.outcome AS b_outcome,
    a.result_score AS a_result,
    b.result_score AS b_result,
    b.result_score - a.result_score AS d_result,
    a.resolved_score AS a_resolved,
    b.resolved_score AS b_resolved,
    b.resolved_score - a.resolved_score AS d_resolved,
    a.latency_ms AS a_latency_ms,
    b.latency_ms AS b_latency_ms,
    b.latency_ms - a.latency_ms AS d_latency_ms,
    a.tokens_in AS a_tokens_in,
    b.tokens_in AS b_tokens_in,
    b.tokens_in - a.tokens_in AS d_tokens_in,
    a.tokens_out AS a_tokens_out,
    b.tokens_out AS b_tokens_out,
    b.tokens_out - a.tokens_out AS d_tokens_out,
    a.total_tokens AS a_total_tokens,
    b.total_tokens AS b_total_tokens,
    b.total_tokens - a.total_tokens AS d_total_tokens,
    a.turn_count AS a_turns,
    b.turn_count AS b_turns,
    b.turn_count - a.turn_count AS d_turns,
    a.tool_call_count AS a_tool_calls,
    b.tool_call_count AS b_tool_calls,
    b.tool_call_count - a.tool_call_count AS d_tool_calls,
    CASE
        WHEN a.outcome = b.outcome THEN 'same'
        WHEN a.outcome = 'success' AND b.outcome <> 'success' THEN 'regression'
        WHEN a.outcome <> 'success' AND b.outcome = 'success' THEN 'improvement'
        ELSE 'changed'
    END AS outcome_change
FROM trial_metrics a
JOIN ab_variant_roles roles ON a.variant_id = roles.variant_a_id
JOIN trial_metrics b
    ON b.task_id = a.task_id
   AND b.repl_idx = a.repl_idx
   AND b.variant_id = roles.variant_b_id
ORDER BY a.task_id, a.repl_idx;

CREATE OR REPLACE VIEW ab_trace_row_side_by_side AS
WITH variant_a_events AS (
    SELECT
        t.task_id,
        t.repl_idx,
        e.run_id,
        e.trial_id,
        e.row_seq,
        e.event_type,
        e.turn_index,
        e.call_id,
        e.model_identity,
        e.tool_name,
        e.outcome_status
    FROM events e
    JOIN trials t
      ON t.run_id = e.run_id
     AND t.trial_id = e.trial_id
     AND t.variant_id = e.variant_id
    JOIN ab_variant_roles roles ON t.variant_id = roles.variant_a_id
),
variant_b_events AS (
    SELECT
        t.task_id,
        t.repl_idx,
        e.run_id,
        e.trial_id,
        e.row_seq,
        e.event_type,
        e.turn_index,
        e.call_id,
        e.model_identity,
        e.tool_name,
        e.outcome_status
    FROM events e
    JOIN trials t
      ON t.run_id = e.run_id
     AND t.trial_id = e.trial_id
     AND t.variant_id = e.variant_id
    JOIN ab_variant_roles roles ON t.variant_id = roles.variant_b_id
)
SELECT
    coalesce(a.task_id, b.task_id) AS task_id,
    coalesce(a.repl_idx, b.repl_idx) AS repl_idx,
    roles.variant_a_id,
    roles.variant_b_id,
    a.trial_id AS variant_a_trial_id,
    b.trial_id AS variant_b_trial_id,
    coalesce(a.row_seq, b.row_seq) AS row_seq,
    a.event_type AS variant_a_event_type,
    b.event_type AS variant_b_event_type,
    a.turn_index AS variant_a_turn_index,
    b.turn_index AS variant_b_turn_index,
    a.model_identity AS variant_a_model,
    b.model_identity AS variant_b_model,
    a.tool_name AS variant_a_tool,
    b.tool_name AS variant_b_tool,
    a.outcome_status AS variant_a_status,
    b.outcome_status AS variant_b_status,
    a.call_id AS variant_a_call_id,
    b.call_id AS variant_b_call_id
FROM variant_a_events a
FULL OUTER JOIN variant_b_events b
    ON b.task_id = a.task_id
   AND b.repl_idx = a.repl_idx
   AND b.row_seq = a.row_seq
CROSS JOIN ab_variant_roles roles
ORDER BY task_id, repl_idx, row_seq;

CREATE OR REPLACE VIEW ab_turn_side_by_side AS
WITH variant_a_turns AS (
    SELECT
        t.task_id,
        t.repl_idx,
        e.run_id,
        e.trial_id,
        e.turn_index,
        e.model_identity,
        e.outcome_status,
        e.usage_tokens_in,
        e.usage_tokens_out,
        e.row_seq
    FROM events e
    JOIN trials t
      ON t.run_id = e.run_id
     AND t.trial_id = e.trial_id
     AND t.variant_id = e.variant_id
    JOIN ab_variant_roles roles ON t.variant_id = roles.variant_a_id
    WHERE e.event_type = 'model_call_end' AND e.turn_index IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY t.task_id, t.repl_idx, e.turn_index
        ORDER BY e.row_seq DESC
    ) = 1
),
variant_b_turns AS (
    SELECT
        t.task_id,
        t.repl_idx,
        e.run_id,
        e.trial_id,
        e.turn_index,
        e.model_identity,
        e.outcome_status,
        e.usage_tokens_in,
        e.usage_tokens_out,
        e.row_seq
    FROM events e
    JOIN trials t
      ON t.run_id = e.run_id
     AND t.trial_id = e.trial_id
     AND t.variant_id = e.variant_id
    JOIN ab_variant_roles roles ON t.variant_id = roles.variant_b_id
    WHERE e.event_type = 'model_call_end' AND e.turn_index IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY t.task_id, t.repl_idx, e.turn_index
        ORDER BY e.row_seq DESC
    ) = 1
)
SELECT
    coalesce(a.task_id, b.task_id) AS task_id,
    coalesce(a.repl_idx, b.repl_idx) AS repl_idx,
    roles.variant_a_id,
    roles.variant_b_id,
    a.trial_id AS variant_a_trial_id,
    b.trial_id AS variant_b_trial_id,
    coalesce(a.turn_index, b.turn_index) AS turn_index,
    a.model_identity AS variant_a_model,
    b.model_identity AS variant_b_model,
    a.outcome_status AS variant_a_status,
    b.outcome_status AS variant_b_status,
    a.usage_tokens_in AS variant_a_tokens_in,
    b.usage_tokens_in AS variant_b_tokens_in,
    a.usage_tokens_out AS variant_a_tokens_out,
    b.usage_tokens_out AS variant_b_tokens_out,
    b.usage_tokens_in - a.usage_tokens_in AS delta_tokens_in,
    b.usage_tokens_out - a.usage_tokens_out AS delta_tokens_out
FROM variant_a_turns a
FULL OUTER JOIN variant_b_turns b
    ON b.task_id = a.task_id
   AND b.repl_idx = a.repl_idx
   AND b.turn_index = a.turn_index
CROSS JOIN ab_variant_roles roles
ORDER BY task_id, repl_idx, turn_index;

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
