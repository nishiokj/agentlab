---
name: agent-driven-experimentation
description: Run iterative benchmark experiment loops on top of AgentLab's existing build, preflight, run, and analysis commands. Use when Codex needs to improve an agent by editing experiment authoring YAML, building sealed packages, running controlled comparisons, analyzing standardized views, and maintaining a durable `.lab/journal.jsonl` experiment journal across sessions.
---

# Agent Driven Experimentation

## Overview

Use this skill to drive an experiment loop without inventing new runner surfaces. Edit authoring YAML, build a sealed package, preflight it, run it, analyze the actual `view_set`, and record one journal entry for every terminal package attempt.

For the full contract, read:

- `../../docs/AGENT_DRIVEN_EXPERIMENTATION_SPEC.md`
- `../../schemas/experiment_journal_entry_v1.jsonschema`

For project-specific experiment controls, also locate any knob manifest the benchmark provides. The schema is at `../../schemas/knob_manifest_v1.jsonschema`.

## Workflow

### 1. Load journal context first

Run the bundled helper before planning:

```bash
python3 skills/agent-driven-experimentation/scripts/experiment_journal.py check --journal .lab/journal.jsonl --json
python3 skills/agent-driven-experimentation/scripts/experiment_journal.py repair --journal .lab/journal.jsonl --json
```

Rules:

- Treat a missing journal as empty context.
- Repair only a malformed tail parse error. If validation fails on an earlier line or on schema shape, stop and inspect the file manually.
- Do not append to a journal with a damaged tail until it has been repaired.

### 2. Choose a comparable parent

Read recent journal entries and choose one parent attempt. Keep these control axes fixed unless the hypothesis explicitly targets them:

- dataset identity and split
- dataset limit or task subset
- scoring or grader logic
- replication policy
- comparison design and baseline mapping

If those axes change enough to break trend comparability, assign a new `experiment_id`.

Default mode:

- change one treatment axis at a time
- keep confounds fixed
- treat exploratory runs as informative but usually `inconclusive`

### 3. Edit authoring YAML, then build a package

Do not run raw YAML with `lab run-experiment`. The strict experiment path runs sealed packages.

```bash
lab build experiments/<candidate>.yaml --out .lab/builds/<package_name> --json
lab describe .lab/builds/<package_name> --json
lab preflight .lab/builds/<package_name> --json
```

Rules:

- Never edit files inside `.lab/builds/<package_name>`.
- If the YAML changes after build, discard the old package and build a new one.
- If preflight fails, do not run the package. Write a `preflight_failed` journal entry and move on.
- Read `package.lock` or `manifest.json` to get `package_digest` for the journal.

### 4. Run and monitor the package

```bash
lab run-experiment .lab/builds/<package_name> --json
lab views <run_id> --json
lab views <run_id> run_progress --json
```

Rules:

- Discover `view_set` before assuming which analysis views exist.
- Treat mid-run comparisons as diagnostic unless a stopping rule was declared before launch.
- Use `lab kill <run_id>` only for operational aborts, runaway regressions, or a predeclared stopping rule.

### 5. Analyze by actual `view_set`

Use the standardized views that the run exposes. Start with:

```bash
lab views <run_id> --json
```

Then use the relevant subset:

- AB test: `comparison_summary`, `task_outcomes`, `task_metrics`, `scoreboard`
- parameter sweep: `variant_summary`, `config_ranking`, `parameter_effects`, `parameter_sensitivity`
- multi-variant: `variant_summary`, `variant_ranking`, `pairwise_compare`, `task_variant_matrix`
- regression: `variant_summary`, `run_trend`, `flaky_tasks`, `failure_clusters`

Do not invent p-values or views that are not present. The current compact AB summary is `comparison_summary`; it exposes pass-rate delta, changed outcome counts, McNemar chi2, and Cohen's h.

Read `references/cli-and-views.md` for the command map.

### 6. Record one terminal journal entry

Use the helper script to append only after a package attempt reaches a terminal state:

```bash
python3 skills/agent-driven-experimentation/scripts/experiment_journal.py append \
  --journal .lab/journal.jsonl \
  --entry-file /tmp/journal_entry.json \
  --json
```

Rules:

- Record one entry per terminal package attempt: `preflight_failed`, `run_failed`, `run_killed`, or `run_completed`.
- Include `package_digest` in every entry.
- Deduplicate by `run_id` when present; otherwise by `package_digest` plus `attempt_status`.
- Keep `regressions` and `novel_passes` sampled and store total counts separately.
- Default failed and killed runs to `verdict: inconclusive` unless a stopping rule was declared before launch and satisfied.

## Journal Helper

The bundled script `scripts/experiment_journal.py` implements the journal rules from the spec for `experiment_journal_entry_v1` only.

Commands:

- `check`: validate the existing journal and report the first failure
- `repair`: back up a journal with a truncated final line and rewrite the longest valid prefix
- `append`: validate an entry file, deduplicate, and append one newline-terminated JSON object

Use `--json` whenever another agent step needs structured output.

## Read Next

- `references/cli-and-views.md` for package, run, and analysis commands
- `references/examples.md` for small and real experiment YAML examples in this repo
