---
name: experiment
description: >
  Autonomous experiment driver. Maintains a lab notebook, forms hypotheses,
  authors experiments, runs trials, analyzes results, and advances or rejects
  deltas against a tracked champion.
user-invocable: true
---

# Experiment Skill

You drive a closed-loop experiment system. You maintain a lab notebook, form hypotheses, author experiment YAML, run trials via `lab-cli`, analyze results, and decide whether to accept or reject each delta. You do not guess. Every claim is grounded in run data.

## Champion Model

```
C0 --(delta A wins)--> C1 --(delta C wins)--> C2 --> ...
                         \-- delta B rejected
```

- There is one **champion** — the current best-known variant for a given benchmark. The champion must be beaten for any challenger to be adopted. The champion is the control.
- Each experiment compares one or more **challengers** against the champion. Each challenger differs from the champion by one **delta** — a specific, intentional change.
- **The 1-delta invariant**: Every challenger in an experiment MUST differ from the champion by exactly one delta. This is a hard constraint. If a challenger becomes invalid mid-run (harness bug, broken bundle), you cannot patch it arbitrarily — you must rebuild it from the champion with a clean single-delta difference. Violating this invariant makes the experiment uninterpretable.
- Accept: the winning variant becomes the new champion. Reject: champion holds, try a different delta.
- Inconclusive: increase N or accept ambiguity. Never advance or roll back from an inconclusive result.
- The harness is the product, not the model. Model comparisons are useful but the enduring value is harness improvement.

## Invocation

```
/experiment                        # Assess — where are we in the history?
/experiment analyze <run_id>       # Analyze a completed run, accept or reject
/experiment hypothesis             # Generate next delta to test
/experiment author <description>   # Author experiment YAML from a hypothesis
/experiment run <yaml>             # Build and run an experiment
```

---

## Notebook Protocol

### Location

`.lab/notebooks/<benchmark_id>.md` — one notebook per (project, benchmark) pair. Pass rates are not comparable across benchmarks, so each benchmark gets its own history chain.

### Schema

```markdown
# Lab Notebook: <benchmark_id>

## Champion
- Established by: <experiment_id> (run: <run_id>, variant: <variant_id>)
- Pass rate: <X> (N=<tasks>)
- Avg tokens: <in>/<out>
- Description: <1-2 line human description of this configuration>
- Key bindings: model=<model>, model_provider=<provider>
- Artifact: <artifact name or bundle path>
- Bundle digest: <bundle_digest from resolved_variants.json>

## History
### Experiment <N>: <experiment_id> — <what was tested>
- Delta type: binding | config | patch | code
- Delta: <what changed, in human terms>
- Result: accepted | rejected | inconclusive
- Winner: <variant_id> | none
- Variants:
  - <variant_a_id>: pass=<X>, N=<tasks>, avg_tokens_in=<X>, avg_tokens_out=<X>
  - <variant_b_id>: pass=<Y>, N=<tasks>, avg_tokens_in=<X>, avg_tokens_out=<X>
- Effect: h=<cohen_h>
- Run: <run_id>
- Notes: <observations, failure patterns, token anomalies>

### Experiment <N-1>: ...
(reverse chronological — most recent first)

## Rejected
- <delta description> — <reason> (run: <run_id>)

## Open Questions
- <candidate hypotheses, observations, things to investigate>
```

### Notebook Rules

1. The notebook is the durable narrative. Exact provenance lives in run artifacts — dereference `run_id` when precision matters.
2. One history entry per analyzed run. Entries are immutable once written (append-only).
3. Champion updates only when `/experiment analyze` concludes "accepted."
4. Open Questions is mutable — add and remove freely.
5. If the notebook does not exist, create it from the template above. Set Champion to "Not yet established" until the first run is analyzed and accepted.

---

## Procedures

### `/experiment` — Assess

Purpose: Orient. Where are we in the history?

**Steps:**

1. Determine the benchmark scope. Use the user's input, or infer from recent runs.
2. Read `.lab/notebooks/<benchmark>.md`. If absent, create from template with empty champion.
3. Run:
   ```
   ./lab-cli runs --json
   ```
   Filter to runs whose `experiment.id` matches this benchmark's experiments.
4. Cross-reference runs against notebook history entries. Identify:
   - Completed runs not yet in the notebook (need `/experiment analyze`)
   - Currently running experiments
   - The current champion and its pass rate
5. Output a status summary:
   ```
   Champion: <description> at <pass_rate> (N=<tasks>), established by <run_id>
   Last experiment: <experiment_id>, result: <accepted|rejected|inconclusive>
   Unanalyzed completed runs: <count>
   ```
6. Recommend next action:

   | State | Recommendation |
   |-------|---------------|
   | Unanalyzed completed runs exist | `/experiment analyze <run_id>` |
   | No hypothesis articulated | `/experiment hypothesis` |
   | Hypothesis exists, no YAML authored | `/experiment author` |
   | YAML authored, not yet run | `/experiment run` |

---

### `/experiment analyze <run_id>` — Analyze

Purpose: Analyze a completed run. Produce an accept/reject/inconclusive decision grounded in data.

**Step 1 — Load results:**

```bash
./lab-cli views <run_id> run_progress --json
./lab-cli views <run_id> variant_summary --json
./lab-cli views <run_id> comparison_summary --json
./lab-cli views <run_id> task_outcomes --json
./lab-cli views <run_id> task_metrics --json
```

If `comparison_summary` is empty or the run is single-variant, skip to Step 6 and record as a champion-establishing run.

**Step 2 — Describe what changed:**

Read `.lab/runs/<run_id>/resolved_variants.json`. Compare all variants across:

| Field | Location in resolved variant |
|-------|------------------------------|
| Bindings | `variants[].bindings` |
| Agent bundle | `variants[].runtime_overrides.agent.bundle` |
| Bundle digest | `variants[].runtime_overrides.agent.bundle_digest` |
| Command | `variants[].runtime_overrides.agent.command` |
| Env vars | `variants[].runtime_overrides.agent.env` |
| Env from host | `variants[].runtime_overrides.agent.env_from_host` |
| Arg map | `variants[].runtime_overrides.agent.arg_map` |
| Workspace patches | `variants[].runtime_overrides.agent.workspace_patches` |
| File staging | `variants[].runtime_overrides.dependencies.file_staging` |

Produce two outputs:
- **Exact delta**: which fields differ, with values
- **Human summary**: one sentence describing the delta (this goes in the notebook)

**Step 3 — Classify regressions:**

For each task where a challenger failed but the champion succeeded, read the challenger's trial log:
```
.lab/runs/<run_id>/trials/<trial_id>/harness_stderr.log
```

Classify each regression:

| Category | Indicators | Counts against delta? |
|----------|-----------|----------------------|
| Infra noise | API 500/401/403, DNS failure, Docker error, harness timeout unrelated to agent | No |
| Capability failure | Agent ran, made tool calls, produced wrong answer | Yes |
| Resource exhaustion | Token usage >10x champion for same task, followed by timeout | Yes (thrashing signal) |

**Step 4 — Check operational metrics:**

```bash
./lab-cli query <run_id> "SELECT variant_id, AVG(CAST(primary_metric_value AS DOUBLE)) as avg_metric, COUNT(*) as n FROM trials GROUP BY variant_id" --json
```

Note: `primary_metric_value` is VARCHAR in the `trials` table — always CAST to DOUBLE before aggregating.

Token metrics live in `trial_metrics` (not `trials`):
```bash
./lab-cli query <run_id> "SELECT variant_id, AVG(tokens_in) as avg_tokens_in, AVG(tokens_out) as avg_tokens_out, AVG(total_tokens) as avg_total_tokens FROM trial_metrics GROUP BY variant_id" --json
```

Token analysis is mandatory, not optional:
- Challenger uses fewer tokens at equal pass rate = efficiency gain (accept signal)
- Challenger uses more tokens at equal pass rate = thrashing risk (caution signal)
- Challenger uses >5x tokens of champion = strong reject signal regardless of pass rate

**Step 5 — Decision:**

Compare all variants (champion + challengers). The winner is the variant with the best combination of pass rate and token efficiency.

| Condition | Decision |
|-----------|----------|
| A challenger beats champion on pass_rate, regressions are infra noise | **Accept** — challenger becomes new champion |
| A challenger beats champion on pass_rate, some genuine regressions | **Accept with note** — record which tasks regressed |
| Challenger ~ champion pass_rate, challenger uses fewer tokens | **Accept** (efficiency gain) |
| All challengers < champion on pass_rate, genuine capability regressions | **Reject** |
| All variants ~ same pass_rate, no efficiency difference | **Inconclusive** — increase N or accept ambiguity |
| N < 10 completed tasks per variant | **Inconclusive** — re-run with higher limit |
| >50% of regressions are infra noise | **Inconclusive** — fix infra, re-run |

When multiple challengers compete: if one challenger beats both the champion and the other challenger, it wins outright. If challengers split (one better pass rate, the other better efficiency), note the tradeoff and pick the one with higher pass rate unless the token difference is extreme (>3x).

**Step 6 — Update notebook:**

- Add a history entry with: delta type, delta description, result, winner, per-variant stats (pass rate + token metrics), effect size, run_id, notes.
- If **accepted**: update the Champion section with the winning variant's identity and token stats.
- If **rejected**: add one line to the Rejected section.
- Add observations to Open Questions if they suggest follow-up hypotheses.

---

### `/experiment hypothesis` — Hypothesize

Purpose: Generate the next delta to test, informed by the history so far.

**Steps:**

1. Read the notebook. Load: current champion, history chain, rejected list, open questions.
2. Review the rejected list. Do not repeat a rejected delta unless you state what changed and why retrying is justified.
3. Sources of hypotheses:
   - Open Questions (may already have candidates)
   - Observations from recent runs (e.g., "challenger used 170x more tokens — is compaction broken?")
   - Task-level analysis: query `task_outcomes` from recent runs to find tasks that consistently fail — these may indicate a harness limitation
   - Operational metrics: high `tool_call_count` or `turn_count` may indicate thrashing, suggesting structural changes
   - Token efficiency: if the champion solves tasks but burns excessive tokens, a delta targeting efficiency is worth testing
4. Write the hypothesis in this format:
   ```
   Delta: <what to change, specifically>
   Delta type: binding | config | patch | code
   Expected effect: <direction + magnitude reasoning>
   Mechanism: <why this should help>
   Sample size: <limit value, based on expected effect size>
   ```
5. Write the hypothesis to the notebook's Open Questions section.
6. Recommend `/experiment author`.

### When to pivot vs. continue

- **Pivot** if 3+ deltas in the same category were rejected (e.g., tried 3 compaction strategies, none helped). Switch categories.
- **Continue** if the effect was directional but underpowered (N too small). Re-run with higher limit.
- **Re-validate** if 3+ deltas were accepted in sequence without checking champion stability. Run the current champion against an older known-good state.

---

### `/experiment author <description>` — Author

Purpose: Create the experiment YAML that tests the hypothesis.

**Steps:**

1. Read the notebook for the current hypothesis (from Open Questions or user input).
2. Determine the delta type. This dictates the YAML format:

   | Delta type | YAML format | What changes |
   |------------|-------------|-------------|
   | binding | Single `agent:` block, `baseline:` + `variants:` | `bindings` values |
   | config | Single `agent:` block, different `config_files` per variant | Config file content |
   | patch | Single `agent:` block, different `workspace_patches` per variant | Source file overlays |
   | code | `agent_builds:` block, `variants:` with `agent_ref:` | Different artifacts |

   > **YAML ↔ concept mapping**: The YAML schema uses `baseline:` to denote the champion variant. This is a lab-cli schema convention, not a conceptual term. The champion is whichever variant currently holds the best-known result.

3. For **binding deltas**, fork from a working experiment YAML:

   ```yaml
   experiment:
     id: <benchmark>_<delta_name>_v0
     name: "<Human Readable Name>"
     tags: [<benchmark>, ab-test, <delta-tag>]

   benchmark: <benchmark_id>
   limit: <from hypothesis sample size>
   concurrency: <2-4>

   agent:
     artifact: <current champion artifact>
     command: [...]
     default_config: <config file>
     env_from_host: [...]
     arg_map: [...]
     config_files: [...]

   # The `baseline:` key designates the champion in lab-cli YAML schema
   baseline:
     id: <champion_variant_name>
     bindings: { <current champion bindings> }

   variants:
     - id: <challenger_a_name>
       bindings: { <delta A bindings> }
     - id: <challenger_b_name>
       bindings: { <delta B bindings> }

   overrides:
     network: full
     root_read_only: false
   ```

4. For **code deltas**, use the `agent_builds` format:

   ```yaml
   agent_builds:
     - id: champion_build
       artifact: <champion artifact>
       command: [...]
       default_config: <config>
       env_from_host: [...]
       arg_map: [...]
       config_files: [...]
     - id: challenger_build
       artifact: <challenger artifact>
       command: [...]
       default_config: <config>
       env_from_host: [...]
       arg_map: [...]
       config_files: [...]

   variants:
     - id: <champion_name>
       baseline: true    # lab-cli flag for the champion
       agent_ref: champion_build
       config: { <identical bindings> }
     - id: <challenger_name>
       agent_ref: challenger_build
       config: { <identical bindings> }
   ```

   Both variants use the same bindings. The only difference is `agent_ref`. This isolates the code change.

   Building the challenger artifact requires a worktree. See `docs/experiment-worktree-builds-spec.md` for the full workflow.

   **Artifact build flow:**
   ```bash
   # 1. Create worktree (from main tree)
   git worktree add .lab/implementations/<name> -b exp/<name> <base_ref>

   # 2. Make code changes in the worktree, commit
   cd .lab/implementations/<name>
   # ... edit, test, validate ...
   git add -A && git commit -m "exp: <description>"
   cd -

   # 3. Build artifact (ALWAYS from the main tree, --source points to worktree)
   bun scripts/build-agentlab-rex-artifact.ts --source .lab/implementations/<name> --build --target host
   # Output: .lab/agents/rex-<name>.<target>.tar.gz
   ```

   The build script must always be invoked from the main tree. The `--source` flag tells it where the code lives, but the artifact is written to `.lab/agents/` in the main tree. This is how cross-worktree referencing works — all artifacts land in a shared location, and experiment YAML references them by basename (e.g., `artifact: rex-greedy-compaction.linux-aarch64.tar.gz`). Lab-cli resolves basenames by looking in `.lab/agents/`.

   **Validating the artifact before experimenting:** After building, run the agent locally or against a single smoke task to confirm the bundle works. A broken bundle invalidates all variants that use it and wastes an entire experiment run.

5. Write the YAML to `.lab/experiments/<experiment_id>.yaml`.
6. Validate:
   ```bash
   ./lab-cli build .lab/experiments/<experiment_id>.yaml --json
   ./lab-cli preflight <package_path> --json
   ```
7. If errors, fix and retry. If preflight warnings (dirty build, stale artifact), assess and decide whether to proceed.
8. Recommend `/experiment run`.

---

### `/experiment run <yaml>` — Run

Purpose: Execute the experiment.

**Steps:**

1. Resolve the env file. Read `.lab/build.json` and extract the `env_file` field. If present, pass it to every `lab-cli` run command.

2. Run:
   ```bash
   ./lab-cli build-run <yaml> --env-file <env_file>
   ```
   Capture the `run_id` from stdout. The `--env-file` flag is **mandatory** — provider API keys live in the env file, not in the shell environment. Omitting it causes silent auth failures inside containers.

3. If it fails, read the error output. Common failures and fixes:

   | Error | Fix |
   |-------|-----|
   | YAML validation error | Fix the YAML syntax or field names |
   | Missing artifact | Check `.lab/agents/`, rebuild if needed |
   | Missing Docker image | `docker pull <image>` |
   | Missing env var / auth error | Check `.lab/build.json` `env_file` path, verify key exists in the file |
   | Port conflict | Kill stale containers or change concurrency |

3. **Monitor the run.** Use a backoff polling schedule to avoid wasting tokens on idle checks:

   ```
   Poll schedule: 30s, 1m, 2m, 5m, 5m, 5m, ...
   ```

   At each check:
   ```bash
   ./lab-cli views <run_id> run_progress --json
   ```

   Read `completed` vs `total`. Once the run is stable (tasks completing at a steady rate), extend the sleep interval. If `completed == total`, the run is done — proceed to analysis.

   If the run appears stuck (no progress across 2+ checks), investigate:
   ```bash
   ./lab-cli views <run_id> variant_summary --json
   ```
   Check if one variant is completing while another is hung. A hung variant may indicate a harness/agent error — see **Failure Classification** below.

4. On completion, proceed to `/experiment analyze <run_id>`.

5. Failed or aborted runs are non-decisive. They do not change the champion. Classify the failure before deciding next steps — see **Failure Classification** below.

---

## Failure Classification

When a run fails or produces errors, classify the failure before taking action. The classification determines severity, scope, and recovery path.

### Category 1: Experiment Design Error

**What it is:** The experiment definition is wrong — bad YAML, incorrect bindings, missing config file reference, wrong artifact name, misconfigured env vars. The experiment itself is malformed, not the agent or harness.

**Indicators:**
- Run fails immediately or on the very first trial
- Error message references YAML fields, missing files, or config validation
- All variants fail identically with the same error
- `lab-cli build` or `preflight` would have caught it

**Recovery:**
1. Fix the experiment YAML or config
2. Re-run. No notebook entry needed — this was not a real experiment, it was a typo.
3. If the same design error recurs, note the pattern in Open Questions to avoid repeating it.

### Category 2: Harness/Agent Error

**What it is:** The packaged agent bundle has a bug. The code itself is broken — not the experiment design, not infra. This invalidates the bundle.

**Indicators:**
- Agent crashes with a stack trace (not an API error)
- Agent produces degenerate behavior: infinite loops, empty tool calls, context explosion
- Error is reproducible across tasks (not a one-off fluke)
- Error appears in `harness_stderr.log` as an application-level failure

**Severity assessment:**

| Severity | Criteria | Action |
|----------|----------|--------|
| **Large** | Core agent loop broken, affects all tasks, root cause unclear | **Exit.** Write the issue to Open Questions with the stack trace and affected variant. Do not attempt to fix and re-run in the same session — the issue needs investigation. |
| **Small** | Edge case crash, affects subset of tasks, root cause identifiable | Assess variant scope (see below), then decide whether to continue or rebuild. |

**Variant scope assessment:**

Determine which variants the error affects:

| Scope | Meaning | Action |
|-------|---------|--------|
| All variants affected | Shared code bug (in champion bundle too) | **Exit.** The champion itself is broken. Write the issue, investigate separately. |
| Champion only | Champion bundle has a bug the challengers don't | Unusual — verify this isn't infra noise. If confirmed, the champion may need to be re-established. |
| One challenger only | That challenger's delta introduced a bug | Mark that challenger's results as **invalid**. If other challengers are clean, their data is still usable. |
| Multiple challengers, not champion | Shared bug in challenger deltas | All affected challengers are invalid. Champion data is still usable. |

**Rebuilding an invalidated challenger:**

If a challenger is invalidated but the experiment is otherwise sound, you may rebuild it — but you MUST uphold the 1-delta invariant. The new challenger must differ from the current champion by exactly one delta. Do not patch the broken challenger; rebuild it from the champion with a clean delta.

```
Champion (control) ──[1 delta]──> Challenger A (valid)
                   ──[1 delta]──> Challenger B (invalid, bug in bundle)
                   ──[1 delta]──> Challenger B' (rebuilt from champion, same intended delta, clean bundle)
```

### Category 3: Infra Error

**What it is:** External infrastructure failure — API rate limits, Docker daemon issues, network timeouts, disk space, auth token expiry. Not the agent's fault, not the experiment's fault.

**Indicators:**
- API 500/401/403 errors
- Docker container failed to start
- DNS resolution failure
- Harness timeout unrelated to agent behavior

**Recovery:** Fix the infra issue, re-run. Non-decisive — does not count for or against any delta.

---

## Decision Heuristics

### Statistical sufficiency

| Sample size (N per variant) | Detectable effect size |
|----------------------------|----------------------|
| N < 10 | No conclusions. Directional signal only. |
| 10 <= N < 25 | Large effects (Cohen's h >= 0.8) |
| 25 <= N < 65 | Medium effects (h >= 0.5) |
| N >= 65 | Small effects (h >= 0.2) |

### Cohen's h interpretation

| h | Magnitude | Meaning |
|---|-----------|---------|
| < 0.2 | Negligible | No practical difference |
| 0.2-0.5 | Small | Detectable, may not be practically meaningful |
| 0.5-0.8 | Medium | Likely practically meaningful |
| > 0.8 | Large | Clear practical significance |

### Token efficiency as signal

Token metrics are first-class data, not optional. Every analysis must include them.

- A delta that achieves the same pass rate with fewer tokens is an improvement worth accepting.
- A delta that achieves slightly higher pass rate but vastly more tokens is suspicious — it may be brute-forcing through thrashing.
- Token anomalies (>5x champion) should be investigated even if pass rate improved — check for retry loops, context explosion, or degenerate tool-call patterns.

---

## Error Recovery

| Situation | Action |
|-----------|--------|
| Run crashed mid-execution | `./lab-cli recover --run-dir .lab/runs/<run_id>` then `./lab-cli continue --run-dir .lab/runs/<run_id>` |
| Run paused | `./lab-cli resume --run-dir .lab/runs/<run_id>` |
| Need to kill a running experiment | `./lab-cli kill <run_id>` |
| lab-cli rebuild needed | Delete the binary and re-run any `./lab-cli` command — the wrapper auto-rebuilds |
| Notebook is missing or corrupt | Reconstruct from `./lab-cli runs --json` and reading run artifacts |

---

## Constraints

1. **Never advance the champion without data.** No aspirational champions. No "we think this would work."
2. **Never edit a history entry.** If a prior conclusion was wrong, add a new entry documenting the reversal.
3. **Never conclude from N < 10.** Report directional signal, but mark as inconclusive.
4. **Always use `--json` for programmatic consumption.** Parse `result.rows`. Check `ok` before reading `result`.
5. **Never guess run IDs.** Use `./lab-cli runs --json` to list them.
6. **Read logs for failures.** The answer is in `trials/<trial_id>/harness_stderr.log`.
7. **The experiment YAML is the source of truth.** Resolved JSON in the run directory is derived. Iterate on the YAML.
8. **Infra failures are non-decisive.** API errors, Docker timeouts, auth issues — these do not count for or against a delta unless the delta is specifically about fixing that failure mode.
9. **Token metrics are mandatory.** Every analysis must record per-variant token usage. Pass rate without token context is incomplete data.

---

## Lab-CLI Terminology Notes

The lab-cli YAML schema uses `baseline:` to designate the champion variant. This is a YAML schema convention, not a conceptual term.

| Notebook concept | Lab-CLI term |
|-----------------|-------------|
| Champion | `baseline:` (YAML key), `baseline: true` (variant flag) |
| Challenger | Entry in `variants:` list |

The CLI views already use neutral `variant_a`/`variant_b` terminology — no mapping needed when reading view output.

---

## Appendix A: CLI Reference

| Command | Purpose |
|---------|---------|
| `./lab-cli build <yaml>` | Compile YAML into sealed package |
| `./lab-cli build-run <yaml>` | Build + run in one step |
| `./lab-cli run <package>` | Run a sealed package |
| `./lab-cli run-experiment <package>` | Durable run (pause/resume/recover) |
| `./lab-cli describe <package>` | Show resolved experiment config |
| `./lab-cli preflight <package>` | Validate before running |
| `./lab-cli views <run_id> [view]` | Query views for a run |
| `./lab-cli views-live <run_id> [view]` | Live-refresh view during run |
| `./lab-cli query <run_id> <sql>` | SQL against the run's DuckDB |
| `./lab-cli runs` | List all runs |
| `./lab-cli trend <experiment_id>` | Metric trends across runs |
| `./lab-cli kill <run_id>` | Kill a running experiment |
| `./lab-cli recover --run-dir <dir>` | Recover after crash |
| `./lab-cli resume --run-dir <dir>` | Resume a paused trial |
| `./lab-cli continue --run-dir <dir>` | Continue from next schedule slot |

All commands accept `--json` for machine-readable output. JSON structure:
```json
{
  "command": "<name>",
  "ok": true,
  "result": { "columns": [...], "row_count": N, "rows": [...] }
}
```
If `ok` is false, read `error.message` before retrying.

## Appendix B: Views

Available per-run views (use `./lab-cli views <run_id> <view> --json`):

| View | Key columns | Use for |
|------|-------------|---------|
| `run_progress` | completed, total, pass_rate | Overall status |
| `variant_summary` | variant_id, n_trials, success_rate, primary_metric_mean | Per-variant breakdown |
| `comparison_summary` | variant_a_rate, variant_b_rate, cohens_h, magnitude, mcnemar_chi2, variant_a_better_n, variant_b_better_n | Statistical comparison |
| `task_outcomes` | task_id, variant_a_outcome, variant_b_outcome, variant_a_result_score, variant_b_result_score | Which tasks differ |
| `task_metrics` | task_id, variant_a_tokens_in, variant_b_tokens_in, variant_a_tokens_out, variant_b_tokens_out, variant_a_total_tokens, variant_b_total_tokens, delta_total_tokens, variant_a_turns, variant_b_turns, variant_a_tool_calls, variant_b_tool_calls | Per-task operational metrics with computed deltas |

## Appendix C: Run Artifacts

```
.lab/runs/<run_id>/
  manifest.json                    # run metadata
  resolved_experiment.json         # fully resolved experiment config
  resolved_variants.json           # resolved variant configs with provenance
  resolved_schedule.json           # trial execution schedule
  run.sqlite                       # results database
  trials/
    <trial_id>/
      trial_metadata.json          # IDs, runtime config
      trial_state.json             # outcome, timing
      harness_stdout.log           # agent stdout
      harness_stderr.log           # agent stderr
      artifacts/                   # patches, output files
```
