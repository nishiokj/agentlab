# Agent-Driven Experimentation Spec

Status: Draft v0.2
Date: 2026-03-08

## 1. Objective

Enable an agent to autonomously drive the experiment lifecycle using AgentLab's existing authoring, build, run, and analysis surfaces, with one addition: a structured experiment journal that serves as persistent memory across sessions.

The agent should be able to:

1. Form a testable hypothesis.
2. Edit an `experiment_v1_0` authoring YAML.
3. Build an immutable sealed package from that YAML.
4. Preflight and run the package.
5. Monitor execution and analyze results.
6. Record what happened, including failed terminal attempts, in a compact journal.
7. Iterate.

No new Rust code. No new CLI commands. One new schema, one file convention, one skill definition.

## 2. Problem Statement

AgentLab already exposes the surfaces an agent needs:

- `experiment_v1_0` YAML for editable intent.
- `lab build` to create a sealed package.
- `lab describe` and `lab preflight` to inspect and validate that package.
- `lab run-experiment` to execute strict experiment runs.
- `lab views --json`, `lab runs --json`, and `lab trend --json` for structured analysis.
- `lab kill` for early termination.

What is missing is durable, information-dense memory across sessions. After 20 attempts, a fresh agent session should not need to re-read every run directory and every view to answer basic questions:

- What hypotheses were already tested?
- Which packages actually ran?
- Which attempts failed in preflight and should not be retried unchanged?
- Which regressions keep recurring?
- Which next move is scientifically valid rather than just different?

The journal solves the memory problem. This spec also hardens the loop around real implementation constraints: immutable packages, view-set-dependent analysis, duplicate retries, partial runs, and invalid comparisons.

## 3. Design Principles

1. **Use what exists.** The agent edits `experiment_v1_0` YAML, builds a sealed package with `lab build`, runs that package, and queries results via `lab views --json`.
2. **Executed reality beats editable intent.** The authoring YAML is what the agent meant to run. The sealed package digest is what actually ran. Any durable claim must point at the package digest, not just the YAML path.
3. **The agent owns the experiment logic.** Hypothesis formation, comparability judgment, stopping decisions, and journal maintenance stay agent-side. The harness does not read the journal.
4. **The journal is a scientific ledger, not a scratchpad.** It records terminal attempts and reusable conclusions. It should not contain raw logs or full task tables.
5. **Comparability is a first-class constraint.** The agent can explore any knob, but it can only confirm or refute a hypothesis when control axes remain comparable.
6. **Single-writer semantics are required in v1.** The journal is safe only when one agent writes it at a time.
7. **Token budget matters.** The journal must stay compact enough to reload at session start without re-reading all run artifacts.

### 3.1 Failure Modes and Controls

| Failure mode | Example | Consequence | Required control |
|---|---|---|---|
| Authoring/package drift | Agent edits YAML, then runs an older package | Journal records the wrong treatment | Every journal entry must record `package_digest`; rebuild after every YAML edit |
| Incomparable experiments | Dataset limit or replications change between parent and child | False causal claims | Only emit comparative metrics and `confirmed`/`refuted` when control axes match |
| Duplicate journal entries | Agent crashes after append, retries same run | Double-counted evidence | Deduplicate by `run_id` when present, else by `package_digest` + `attempt_status` |
| Corrupt journal tail | Crash leaves half-written JSON on the last line | Next session cannot parse memory | Use valid-prefix repair; never append another line on top of malformed tail bytes |
| Wrong view assumption | Agent asks for `comparison_summary` on a multi-variant run | Analysis step fails or hallucinates missing data | Discover `view_set` and `available_views` first with `lab views <run_id> --json` |
| Optional stopping | Agent kills a run as soon as an early delta looks good | Inflated false positives | Mid-run views are diagnostic by default; confirmatory early stop requires a predeclared rule |
| Forgotten dead ends | Preflight fails and no journal entry is written | Future sessions retry the same broken package | Journal terminal package attempts, including `preflight_failed` |
| Shared-writer journal | Two agents append simultaneously | Interleaved or conflicting history | v1 is single-writer only; concurrent experimentation needs external locking or sharded journals |
| Trend contamination | Reuse one `experiment_id` across different datasets/splits | `lab trend` becomes misleading | Change `experiment_id` whenever control axes change enough to break comparability |

## 4. The Experiment Journal

### 4.1 Location

```
.lab/journal.jsonl
```

Append-only JSONL. One line per terminal package attempt.

### 4.2 Scope

The journal starts once a sealed package exists. v1 records these terminal attempt states:

- `preflight_failed`
- `run_failed`
- `run_killed`
- `run_completed`

Pure authoring failures before a package exists, such as malformed YAML or a failed `lab build`, are fixed inline and are out of scope for the journal. The moment a package digest exists, retries and failures become durable knowledge and must be journaled.

### 4.3 Schema: `experiment_journal_entry_v1`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://agentlab.dev/schemas/experiment_journal_entry_v1.jsonschema",
  "title": "Experiment Journal Entry v1",
  "type": "object",
  "required": [
    "schema_version",
    "experiment_id",
    "package_digest",
    "timestamp",
    "hypothesis",
    "attempt_status",
    "verdict"
  ],
  "additionalProperties": false,
  "properties": {
    "schema_version": { "const": "experiment_journal_entry_v1" },
    "run_id": {
      "type": ["string", "null"],
      "description": "The run_id from lab run-experiment output. Null when the package never reached run creation, e.g. preflight_failed."
    },
    "experiment_id": {
      "type": "string",
      "description": "The experiment.id from the authoring YAML. This should define a comparable lineage; change it when control axes change enough to invalidate trend comparison."
    },
    "package_digest": {
      "type": "string",
      "description": "The sealed package digest from package.lock / manifest.json. This is the ground-truth identity of what was evaluated."
    },
    "parent_run_id": {
      "type": ["string", "null"],
      "description": "The run_id of the prior run this attempt was derived from. Null for the first comparable lineage or when no parent run exists."
    },
    "timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "hypothesis": {
      "type": "string",
      "description": "What the agent expected to happen and why. Stated as a testable claim before results are known."
    },
    "changes": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["knob", "from", "to"],
        "properties": {
          "knob": { "type": "string", "description": "The binding key or config path that was changed." },
          "from": { "description": "Previous value." },
          "to": { "description": "New value." }
        },
        "additionalProperties": false
      },
      "description": "What changed relative to parent_run_id."
    },
    "attempt_status": {
      "type": "string",
      "enum": ["preflight_failed", "run_failed", "run_killed", "run_completed"],
      "description": "Terminal state of the package attempt."
    },
    "blocking_checks": {
      "type": "array",
      "items": { "type": "string" },
      "description": "Blocking preflight checks or terminal failure reasons worth remembering."
    },
    "verdict": {
      "type": "string",
      "enum": ["confirmed", "refuted", "inconclusive"],
      "description": "Whether the hypothesis was supported by valid comparable evidence. Failed or killed attempts are usually inconclusive."
    },
    "pass_rate": {
      "type": "number",
      "minimum": 0,
      "maximum": 1,
      "description": "Overall pass rate for the treatment variant. Omit when no comparable run result exists."
    },
    "baseline_pass_rate": {
      "type": "number",
      "minimum": 0,
      "maximum": 1,
      "description": "Pass rate for the baseline or parent comparator when the comparison is valid."
    },
    "effect": {
      "type": "string",
      "description": "Compact result summary using metrics the views actually expose. Example: '+0.04 pass rate (0.43->0.47), McNemar chi2=4.7, Cohen\\'s h=0.18 (small)'"
    },
    "regression_count": {
      "type": "integer",
      "minimum": 0,
      "description": "Total number of regressed tasks when a comparable task-level delta exists."
    },
    "regressions": {
      "type": "array",
      "items": { "type": "string" },
      "description": "A capped sample of task IDs that got worse relative to baseline or parent."
    },
    "novel_pass_count": {
      "type": "integer",
      "minimum": 0,
      "description": "Total number of newly passing tasks when a comparable task-level delta exists."
    },
    "novel_passes": {
      "type": "array",
      "items": { "type": "string" },
      "description": "A capped sample of task IDs that newly passed."
    },
    "insight": {
      "type": "string",
      "description": "Reusable finding stated as a causal or mechanistic conclusion, or an operational lesson for failed attempts. Not a restatement of numbers."
    },
    "next_steps": {
      "type": "array",
      "items": { "type": "string" },
      "description": "Candidate hypotheses or directions suggested by this result."
    }
  }
}
```

### 4.4 Journal Invariants

- **Append-only semantics.** Prior valid entries are never changed or reordered. Repair is allowed only to drop an invalid tail and preserve the longest valid prefix byte-for-byte.
- **Single writer.** Exactly one agent may append to `.lab/journal.jsonl` at a time.
- **One entry per terminal package attempt.** Do not write partial entries while a run is still active.
- **Package identity is mandatory.** `package_digest` is the durable identity of the attempt.
- **Deduplicate before append.** If an entry already exists for the same `run_id`, do not append another. If `run_id` is null, deduplicate on `package_digest` + `attempt_status`.
- **Hypothesis before outcome.** The hypothesis is written before the agent inspects results. No post-hoc hypothesis rewriting.
- **Comparative claims require comparable controls.** Only populate `baseline_pass_rate`, `regression_count`, `novel_pass_count`, `regressions`, `novel_passes`, or a non-`inconclusive` verdict when the parent/baseline comparison keeps these control axes stable unless the axis itself is the hypothesis:
  - dataset identity and split
  - dataset limit / task subset
  - scoring or grader logic
  - replication policy
  - comparison design / baseline mapping
- **`experiment_id` defines a trend lineage.** If control axes change enough to make `lab trend <experiment_id>` scientifically misleading, assign a new `experiment_id`.
- **Killed and failed runs are usually not confirmatory.** `run_killed` and `run_failed` should default to `verdict: inconclusive` unless a stopping rule was declared before launch and its conditions were met.
- **Task lists are sampled, not exhaustive.** `regressions` and `novel_passes` should be capped, with full magnitude preserved in `regression_count` and `novel_pass_count`.
- **Failed preflight is remembered as an operational lesson, not a scientific result.** `preflight_failed` entries normally omit pass-rate fields and store the blocking reason in `blocking_checks` and `insight`.

### 4.5 Safe Append Protocol

The agent should follow this write protocol:

1. Read `.lab/journal.jsonl` line-by-line. Ignore empty lines.
2. If a non-empty line fails JSON parse, stop. Do not append to the damaged file.
3. Repair by copying the longest valid prefix into a new `.lab/journal.jsonl` and moving the damaged original aside, for example to `.lab/journal.corrupt.<timestamp>.jsonl`.
4. Construct the new entry fully in memory.
5. Check for duplicate `run_id` or duplicate `package_digest` + `attempt_status`.
6. Append exactly one newline-terminated JSON object in a single append operation.
7. On restart after a crash, always deduplicate before retrying the append.

### 4.6 Token Budget

A well-written run-complete entry is still roughly 100-150 tokens if task lists are sampled and counts are stored separately. Failed attempts are usually shorter. Sampling task IDs keeps the journal reload cost bounded without losing the shape of the evidence.

## 5. Agent Workflow

The loop uses only existing CLI commands plus the journal file.

### 5.1 Session Start: Load Context

```
1. Read .lab/journal.jsonl                           -> accumulated knowledge
2. Repair only if the tail is malformed              -> valid-prefix recovery
3. Identify the best comparable prior attempt        -> candidate parent
4. Read the relevant authoring YAML                  -> editable intent
5. If needed, inspect the chosen package manifest    -> executed reality
6. Optionally: lab runs --json                       -> list terminal runs
```

If the journal is missing, treat it as empty.

### 5.2 Plan: Form Hypothesis

Based on journal entries, the agent:

- Identifies a comparable parent attempt.
- Reviews `next_steps` from recent entries.
- Looks for repeated blockers in `blocking_checks`.
- Reviews regressions for recurring task or cluster patterns.
- Chooses one primary treatment variable by default.
- Declares a stopping rule up front if early termination might be used.

The default scientific mode is conservative: change one treatment axis at a time, hold controls constant, and treat everything else as exploratory.

### 5.3 Create Experiment

The agent edits authoring YAML, then builds an immutable package:

```
1. Read experiments/<parent>.yaml
2. Copy to experiments/<new_experiment>.yaml
3. Modify treatment knobs or other intended authoring fields
4. If changing dataset/split/limit/replications/scoring, mark the attempt exploratory
5. Build: lab build experiments/<new_experiment>.yaml --out .lab/builds/<new_package> --json
6. Optionally inspect: lab describe .lab/builds/<new_package> --json
7. Preflight: lab preflight .lab/builds/<new_package> --json
```

Rules:

- The built package is immutable. Do not edit files inside `.lab/builds/<new_package>`.
- If the YAML changes after build, discard the old package and build a new one.
- The `--out` directory must be new or empty. Use a unique package path or omit `--out`.
- Changing dataset size for speed is allowed for exploratory screening, but those results are not directly comparable to a full benchmark run unless the hypothesis is explicitly about dataset size or task selection.

If preflight fails, do not run the package. Write a `preflight_failed` journal entry and move on.

### 5.4 Run Experiment

```
lab run-experiment .lab/builds/<new_package> --json
```

The JSON output includes the `run_id`.

Important contract:

- `lab run-experiment` runs against a sealed package, not a raw YAML file.
- `lab run-experiment` is the strict offline experiment path and requires effective network mode `none`.
- If a treatment requires network access, that is outside this strict experiment loop and should use a different run contract.

### 5.5 Monitor / Early Kill

Start by discovering what the run can actually expose:

```
lab views <run_id> --json                 -> view_set + available_views
lab views <run_id> run_progress --json    -> completion status
```

For AB tests, `comparison_summary` can be used as an early diagnostic:

```
lab views <run_id> comparison_summary --json
lab kill <run_id>
```

Hard rules:

- Mid-run views are diagnostic by default, not confirmatory.
- Early kill is appropriate for operational reasons such as obvious misconfiguration, runaway regressions, or a predeclared stopping rule.
- Repeated peeking at `comparison_summary` without a stopping rule is optional stopping and weakens the validity of `confirmed` / `refuted`.
- In highly parallel runs, facts commit in deterministic order, so partial analysis may lag worker completion slightly. `run_progress` is the authoritative completion surface.

### 5.6 Analyze Results

First discover the run's standardized view set:

```
lab views <run_id> --json
```

Then branch on `view_set`.

For AB tests:

```
lab views <run_id> comparison_summary --json
lab views <run_id> task_outcomes --json
lab views <run_id> task_metrics --json
lab views <run_id> scoreboard --json
```

For parameter sweeps:

```
lab views <run_id> variant_summary --json
lab views <run_id> config_ranking --json
lab views <run_id> parameter_effects --json
lab views <run_id> parameter_sensitivity --json
lab views <run_id> scoreboard --json
```

For multi-variant comparisons:

```
lab views <run_id> variant_summary --json
lab views <run_id> variant_ranking --json
lab views <run_id> pairwise_compare --json
lab views <run_id> task_variant_matrix --json
lab views <run_id> scoreboard --json
```

For regression-style runs:

```
lab views <run_id> variant_summary --json
lab views <run_id> run_trend --json
lab views <run_id> flaky_tasks --json
lab views <run_id> failure_clusters --json
lab views <run_id> scoreboard --json
```

For cross-run trends inside one comparable lineage:

```
lab trend <experiment_id> --json
```

Interpretation rules:

- If a desired view is absent, treat that as a design mismatch, not as missing data.
- Use `comparison_summary` as the compact AB summary because it already joins pass-rate delta, changed outcome counts, McNemar chi2, and Cohen's h.
- Do not invent p-values that the current view does not expose.
- If comparability is broken, the run can still be informative, but the verdict should usually remain `inconclusive`.

### 5.7 Record Learnings

Append one entry to `.lab/journal.jsonl` after every terminal package attempt.

Example: completed comparable AB run

```jsonl
{"schema_version":"experiment_journal_entry_v1","run_id":"run_20260307_143200","experiment_id":"swebench_temp_sweep_03","package_digest":"sha256:6be4d8d7b8f9b9e0a5c3f9f1f4a97d4f3d5d4c0a2d5e9f4a1b2c3d4e5f607182","parent_run_id":"run_20260306_091500","timestamp":"2026-03-07T14:55:00Z","hypothesis":"Lowering temperature from 0.5 to 0.2 reduces false-positive patches on single-file tasks","changes":[{"knob":"agent.temperature","from":0.5,"to":0.2}],"attempt_status":"run_completed","verdict":"confirmed","pass_rate":0.47,"baseline_pass_rate":0.43,"effect":"+0.04 pass rate (0.43->0.47), McNemar chi2=4.7, Cohen's h=0.18 (small)","regression_count":1,"regressions":["django__django-13710"],"novel_pass_count":2,"novel_passes":["sympy__sympy-20442","flask__flask-4045"],"insight":"Lower temperature reduces false-positive patches on simpler single-file tasks, but the django regression suggests complex multi-file edits need more exploration room.","next_steps":["Test temperature 0.1 for diminishing returns","Test task-conditional temperature: 0.2 for single-file, 0.5 for multi-file","Inspect django__django-13710 failure traces before changing another knob"]}
```

Example: preflight failure remembered as operational knowledge

```jsonl
{"schema_version":"experiment_journal_entry_v1","run_id":null,"experiment_id":"swebench_temp_sweep_04","package_digest":"sha256:99b8a4e4d2479ab7f5d80d73594c4d410d47bf9849a7df526ba63d8bde4b1c55","parent_run_id":"run_20260307_143200","timestamp":"2026-03-08T09:12:00Z","hypothesis":"Raising max_turns from 40 to 60 improves hard multi-file repairs without harming single-file tasks","changes":[{"knob":"agent.max_turns","from":40,"to":60}],"attempt_status":"preflight_failed","blocking_checks":["variant 'treatment': provider env var missing for configured model binding"],"verdict":"inconclusive","insight":"This package was never evaluated. The blocker is environment wiring, not model behavior, so retrying unchanged after auth is fixed is valid.","next_steps":["Restore the missing provider env var and rerun the same package","If the environment cannot support this model, choose a mapped model before changing any scientific knob"]}
```

### 5.8 Repeat

Go back to 5.2.

## 6. Skill Definition

The agent receives its instructions via a skill. The skill provides:

1. The experiment workflow from section 5, condensed.
2. The authoring schema and the build/package boundary.
3. The knob manifest for the agent under test.
4. The journal schema or key fields inline.
5. The specific `lab` commands it needs.
6. The control axes that must usually stay fixed for a valid comparison.
7. The stopping-rule policy for early termination.

### 6.1 Skill Content Structure

```
## Your Role
You are an experiment driver. Your job is to iteratively improve an agent's
performance on a benchmark by forming hypotheses, building sealed packages,
running controlled experiments, analyzing results, and recording findings.

## Context
- Experiment journal: .lab/journal.jsonl
- Authoring files: experiments/*.yaml
- Built packages: .lab/builds/*
- Knob manifest: <path to knob_manifest.json>

## Workflow
1. Read .lab/journal.jsonl for context
2. Form a hypothesis and choose a comparable parent
3. Copy and edit an authoring YAML
4. Build: lab build <yaml> --json
5. Preflight: lab preflight <package_dir> --json
6. Run: lab run-experiment <package_dir> --json
7. Discover views: lab views <run_id> --json
8. Analyze with the appropriate standardized views
9. Append one journal entry for the terminal attempt
10. Repeat

## Scientific Guardrails
- Change one treatment axis at a time by default
- Keep control axes fixed unless the hypothesis explicitly targets them
- Treat mid-run comparisons as diagnostic unless a stopping rule was declared up front
- Use a new experiment_id when trend comparability would otherwise be broken

## Knobs You Can Change
<knob manifest contents or summary>

## Key CLI Commands
lab build <yaml> --json
lab describe <package_dir> --json
lab preflight <package_dir> --json
lab run-experiment <package_dir> --json
lab views <run_id> --json
lab trend <experiment_id> --json
lab runs --json
lab kill <run_id>
```

### 6.2 Knob Documentation

The skill should include the knob manifest contents or a filtered summary. For each knob, the agent needs:

- `id`: what to put in `bindings`
- `type` plus `minimum` / `maximum` / `options`: valid values
- `scientific_role`: treatment variable, control, confound, or invariant
- `description`: what it does

Knobs with `scientific_role: invariant` should be flagged as "do not change." Knobs with `scientific_role: confound` should be flagged as "hold constant unless explicitly investigating." Experiment-level control axes such as dataset split, limit, scoring logic, and replication policy should be documented the same way even if they are not normal binding knobs.

### 6.3 Examples

The skill should include 1-2 minimal authoring YAML examples relevant to the benchmark so the agent can reason from real structure instead of from the schema alone.

## 7. Boundaries

| Concern | Owner | Notes |
|---|---|---|
| Authoring YAML construction | Agent | Agent reads, copies, modifies `experiment_v1_0` YAML |
| Package build | Runner via `lab build` | Produces immutable sealed package and digest |
| Package immutability | Agent + runner contract | Package must not be edited after build |
| Preflight validation | Runner via `lab preflight` | Agent interprets checks and decides next step |
| Experiment execution | Runner via `lab run-experiment` | Strict package run, network mode `none` |
| Result analysis | Runner via `lab views --json` / `lab trend --json` | Agent must respect view-set availability |
| Hypothesis formation | Agent | Agent reasons over journal + results |
| Comparability judgment | Agent | Views do not know whether the scientific comparison is valid |
| Journal maintenance | Agent | Agent appends entries; harness never reads or writes the journal |
| Knob documentation | Skill author (human) | Provided once in the skill definition |
| Statistical calculations | Views layer | McNemar chi2, Cohen's h, and related aggregates are computed by analysis views |

## 8. What This Spec Does NOT Add

- No new Rust code.
- No new CLI commands.
- No new DuckDB views.
- No automated journal summarization.
- No built-in journal lock service.
- No built-in JSONL schema validator command for the journal.
- No multi-agent coordination protocol.
- No agent-source-code mutation contract.

The journal is a file. The package digest is the identity. The CLI is the interface. The agent is responsible for scientific discipline.

## 9. Validation and Repair

The schema file belongs in `schemas/experiment_journal_entry_v1.jsonschema`.

Current CLI reality matters here: `lab schema-validate` validates a single JSON document, not a JSONL file line-by-line. That means journal validation in v1 is a line-oriented procedure, not a one-command CLI feature.

Required validation behavior:

1. Parse each non-empty line independently as JSON.
2. Validate each parsed object against `experiment_journal_entry_v1`.
3. Report the first failing line with line number.
4. If the only failure is a truncated tail line, repair by keeping the longest valid prefix.

Adding first-class `lab schema-validate --jsonl` support would be useful, but it is explicitly out of scope for this spec.

## 10. Future Considerations (Out of Scope)

- **Journal summarization.** If the journal grows past roughly 100 entries, older entries may need compression.
- **Automated hypothesis suggestion.** The analysis layer could propose unexplored high-sensitivity knobs.
- **Multi-agent coordination.** Shared experimentation needs locking or sharded journals plus merge rules.
- **Pre-package failure journaling.** Build failures before `package_digest` exists are still out of band in v1.
- **Sequential-testing-aware stopping rules.** Stronger statistical support for early stopping would improve confirmatory use of mid-run peeks.
- **First-class JSONL schema validation.** A CLI mode for line-by-line schema validation would reduce tool friction.
