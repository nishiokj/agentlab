CREATE OR REPLACE VIEW parameter_metric AS
SELECT
    b.binding_name AS parameter_name,
    b.binding_value_text AS parameter_value,
    t.primary_metric_name,
    avg(try_cast(t.primary_metric_value AS DOUBLE)) AS mean_metric,
    stddev_samp(try_cast(t.primary_metric_value AS DOUBLE)) AS std_metric,
    count(*) AS n
FROM trials t
JOIN bindings_long b USING (trial_id)
GROUP BY b.binding_name, b.binding_value_text, t.primary_metric_name
ORDER BY mean_metric DESC NULLS LAST, parameter_name, parameter_value;

CREATE OR REPLACE VIEW best_config AS
SELECT
    variant_id,
    round(avg(try_cast(primary_metric_value AS DOUBLE)), 4) AS mean_metric,
    round(avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate,
    count(*) AS n_trials
FROM trials
GROUP BY variant_id
ORDER BY mean_metric DESC NULLS LAST, pass_rate DESC, variant_id
LIMIT 10;

CREATE OR REPLACE VIEW sensitivity AS
WITH variant_means AS (
    SELECT
        variant_id,
        avg(try_cast(primary_metric_value AS DOUBLE)) AS mean_metric
    FROM trials
    GROUP BY variant_id
),
variant_bindings AS (
    SELECT DISTINCT
        variant_id,
        binding_name,
        binding_value_text
    FROM bindings_long
),
parameter_means AS (
    SELECT
        vb.binding_name,
        vb.binding_value_text,
        avg(vm.mean_metric) AS value_mean_metric
    FROM variant_bindings vb
    JOIN variant_means vm USING (variant_id)
    GROUP BY vb.binding_name, vb.binding_value_text
)
SELECT
    binding_name AS parameter_name,
    var_samp(value_mean_metric) AS inter_value_variance,
    max(value_mean_metric) - min(value_mean_metric) AS value_range,
    count(*) AS n_values
FROM parameter_means
GROUP BY binding_name
ORDER BY inter_value_variance DESC NULLS LAST, value_range DESC NULLS LAST, parameter_name;
