CREATE OR REPLACE VIEW pass_rate_trend AS
SELECT
    run_id,
    variant_id,
    round(avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate,
    count(*) AS n_trials
FROM trials
GROUP BY run_id, variant_id
ORDER BY run_id, variant_id;

CREATE OR REPLACE VIEW flaky_tasks AS
SELECT
    task_id,
    count(*) AS n_replications,
    sum(CASE WHEN outcome = 'success' THEN 1 ELSE 0 END) AS passes,
    sum(CASE WHEN outcome <> 'success' THEN 1 ELSE 0 END) AS failures,
    round(avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS pass_rate
FROM trials
GROUP BY task_id
HAVING count(DISTINCT outcome) > 1
ORDER BY pass_rate ASC, n_replications DESC, task_id;

CREATE OR REPLACE VIEW failure_clusters AS
SELECT
    split_part(task_id, '__', 1) AS task_group,
    count(*) AS total,
    sum(CASE WHEN outcome <> 'success' THEN 1 ELSE 0 END) AS failures,
    round(1.0 - avg(CASE WHEN outcome = 'success' THEN 1.0 ELSE 0.0 END), 4) AS failure_rate
FROM trials
GROUP BY task_group
ORDER BY failure_rate DESC, failures DESC, task_group;
