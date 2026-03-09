# CLI And Views

Use this file for the command map and the minimum view logic needed to run the loop safely.

## Package lifecycle

Build a sealed package from authoring YAML:

```bash
lab build experiments/<candidate>.yaml --out .lab/builds/<package_name> --json
```

Inspect the package and confirm task count, variants, and network mode:

```bash
lab describe .lab/builds/<package_name> --json
```

Preflight the sealed package:

```bash
lab preflight .lab/builds/<package_name> --json
```

Run the sealed package:

```bash
lab run-experiment .lab/builds/<package_name> --json
```

Get the durable package identity for the journal:

```bash
jq -r '.package_digest' .lab/builds/<package_name>/package.lock
```

## Journal helper

Validate:

```bash
python3 skills/agent-driven-experimentation/scripts/experiment_journal.py check --journal .lab/journal.jsonl --json
```

Repair a truncated tail:

```bash
python3 skills/agent-driven-experimentation/scripts/experiment_journal.py repair --journal .lab/journal.jsonl --json
```

Append one entry:

```bash
python3 skills/agent-driven-experimentation/scripts/experiment_journal.py append \
  --journal .lab/journal.jsonl \
  --entry-file /tmp/journal_entry.json \
  --json
```

## View discovery

Never assume a view exists. Discover the run's standardized view set first:

```bash
lab views <run_id> --json
```

Use `run_progress` as the authoritative completion surface:

```bash
lab views <run_id> run_progress --json
```

## View-set map

### AB test

Use:

- `comparison_summary`
- `task_outcomes`
- `task_metrics`
- `scoreboard`

Do not claim a p-value from current surfaces. `comparison_summary` exposes McNemar chi2, not a p-value.

### Parameter sweep

Use:

- `variant_summary`
- `config_ranking`
- `parameter_effects`
- `parameter_sensitivity`
- `scoreboard`

### Multi-variant

Use:

- `variant_summary`
- `variant_ranking`
- `pairwise_compare`
- `task_variant_matrix`
- `scoreboard`

### Regression

Use:

- `variant_summary`
- `run_trend`
- `flaky_tasks`
- `failure_clusters`
- `scoreboard`

## Cross-run trend

Only use cross-run trend inside one comparable lineage:

```bash
lab trend <experiment_id> --json
```

If dataset, split, replications, scoring logic, or baseline mapping change enough to break comparability, use a new `experiment_id`.
