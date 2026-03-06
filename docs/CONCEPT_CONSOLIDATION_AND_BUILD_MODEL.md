# Concept Consolidation, Build Model & Observability Surface

**Date:** 2026-03-03
**Status:** Proposal

---

## 1. Problem Statement

AgentLab has accumulated concept bloat across its schemas, terminology, and developer surface. 44 JSON schemas. Three versions of `resolved_experiment`. Two active experiment formats. Terminology drift across layers (Rust calls it `exp_id`, CLI JSON calls it `"experiment"`, SDK calls it `experimentId`). Concepts like `knob_manifest`, `scientific_role`, and `bindings` serve internal machinery but leak into the developer experience where they add cognitive load without clarity.

Separately, the system lacks meaningful cross-experiment analysis and has no stable external surface for monitoring or APIs. Those are secondary concerns for this proposal.

This document captures findings from a design review and proposes an ordered implementation plan that prioritizes concept consolidation and build-time compilation first, then analysis and monitoring surfaces after the schema and runtime boundary are cleaned up.

---

## 2. The Developer's Mental Model

The developer thinks in five steps:

1. **Experiment** — has variants and tasks
2. **Variant** — a configuration that differs by some parameters
3. **Task** — what the agent has to do
4. **Run** — agent gets a variant + task in a container, produces a result, grade it
5. **Analyze** — look at what happened

That is five concepts. The current system exposes: experiment, variant, variant_plan, baseline, bindings, knob, knob_manifest, parameter, scientific_role, design comparison, task, trial, replication, runtime, policy, facts, evidence, run_sink, ViewSet, and more. Most of these are implementation internals that should not be part of the developer's vocabulary.

### Concepts That Earn Their Place

| Concept | Why it stays |
|---------|-------------|
| **Experiment** | The whole thing. Top-level container. |
| **Variant** | A configuration you're testing. Plain English, self-explanatory. |
| **Task** | A problem instance from the dataset. |
| **Trial** | One variant x one task execution. Internal but useful for debugging. |
| **Result / Grade** | What came back, how it scored. |

### Concepts That Don't

| Concept | Problem | Resolution |
|---------|---------|------------|
| `bindings` | Jargon from PL theory. Nobody says "bindings" when they mean "the model is opus." | Rename to `config` on variants. |
| `variant_plan` | Just "the variants." The word "plan" adds nothing. | Merge with `baseline` into a single `variants` list. |
| `baseline` (as separate section) | It's a variant. Separate section implies it's a different kind of thing. | A variant with `baseline: true`. |
| `knob_manifest` | Developer-authored declaration of tunable parameters. Developers don't think about this — they just set values on their variants. | Derived internally by diffing variant configs. Not authored. |
| `scientific_role` | `treatment`, `control`, `confound` — experimental design jargon forced onto parameter declarations. | Removed entirely. The system infers which parameters are swept (differ across variants) vs. fixed (same in all variants). |
| `design.comparison` | `paired`, `unpaired`, `none` — forces the developer to think in scheduling terms. | Inferred: no variants → single run. Variants present → paired. |
| `experiment_overrides_v1` | Separate schema for overrides that already live inline in the minimal format. | Delete schema. Overrides are inline. |
| `resolved_experiment_v0_3`, `v0_4` | Dead versions of the internal resolved format. | Delete. One resolved format. |
| `v0.5` experiment format | Verbose legacy format. The minimal format replaces it. | Delete. One authoring format. |

---

## 3. Terminology Fixes

### 3.1 "Bindings" Is Doing Three Jobs

In the current codebase, "bindings" refers to three unrelated operations:

1. **Variant config values**: `baseline.bindings: { model: opus, temperature: 0.7 }` — the per-variant parameter values.
2. **Argument projection**: `bindings_to_args` — a mechanism that maps config keys to CLI flags.
3. **Build-time value resolution** (proposed): template variables in the YAML resolved against the author's environment.

These must be separated:

| Current term | Proposed term | What it actually is |
|---|---|---|
| `bindings` (on variant) | `config` | Per-variant parameter values |
| `bindings_to_args` | `arg_map` | Map from config keys to CLI flags |
| Build-time value bindings | `variables` / `values` | Template substitution, gone after build |

### 3.2 Other Renames

| Current | Proposed | Rationale |
|---------|----------|-----------|
| `baseline` + `variant_plan` | `variants` (list, one has `baseline: true`) | They're the same concept |
| `replications` | `repeats` | Already identified in DOMAIN_MODEL D4 |
| `knob` | `parameter` (internal only) | Already identified in D1; developers never see this term |
| `call_id` | `turn_id` | Already identified in D5 |

### 3.3 Parameter Space Is Derived, Not Declared

The `knob_manifest_v1` schema asks the developer to declare every tunable parameter with its type, range, and `scientific_role`. This is unnecessary. Given a set of variants:

```yaml
variants:
  - id: sonnet_v2
    baseline: true
    config: { model: claude-sonnet-4, temperature: 0.7, prompt: prompts/v2.md }
  - id: opus_v2
    config: { model: claude-opus-4, temperature: 0.7, prompt: prompts/v2.md }
  - id: opus_v3
    config: { model: claude-opus-4, temperature: 0.7, prompt: prompts/v3.md }
```

The system can derive:
- `model` is **swept** (takes values: claude-sonnet-4, claude-opus-4)
- `prompt` is **swept** (takes values: prompts/v2.md, prompts/v3.md)
- `temperature` is **fixed** (0.7 in all variants)

No manifest needed. No `scientific_role: treatment` declaration. The analysis layer groups results by swept parameters automatically.

The manifest survives only as an **optional internal representation** for validation or auto-generating variant combinations (power-user escape hatch). It is never part of the default developer workflow.

---

## 4. The Build Model

### 4.1 Current State

Today the runner does double duty:
1. **Resolves references** — local paths, benchmark lookup, config file staging, artifact caching
2. **Executes trials** — dispatches work, collects results, writes facts

These are separate concerns conflated into one phase. The runner must know about `.lab/agents/`, the author's filesystem, `overrides/` directories, and benchmark registry internals. This means:
- Local paths can leak into the runner
- The runner is not portable — it's coupled to the authoring environment
- Deployment flexibility is limited

### 4.2 Proposed: Author → Build → Run → Analyze

Introduce an explicit **build phase** that resolves all references against the author's environment and produces a self-contained, portable package.

```
Author                    Build                         Run
──────                    ─────                         ───
experiment.yaml    →    lab build experiment.yaml   →   lab run <package>
  references local          resolves variables            runner receives
  paths, uses               bundles artifacts             fully-resolved,
  template variables,       materializes benchmark        portable artifact
  names benchmarks          internals
                            outputs: package
```

**What gets resolved at build time:**
- `artifact: rex-minimal-linux-dir` → bundled into package with SHA256 pin
- `config_files: [overrides/defaults.json]` → bundled
- `workspace_patches: { src: dst }` → source files bundled
- `benchmark: bench_v0` → all benchmark internals materialized (dataset, adapter, policy, metrics)
- Template variables (`${AGENT_DIR}`, etc.) → concrete values

**What survives into runtime:**
- Variants (list of configs to test)
- Tasks (from the dataset, bundled in package)
- Variant runtime wiring (`agent_ref`, `config`, `env`)
- Policy (timeout, network, sandbox)

**What this kills:**
- `config_files` and `workspace_patches` become **build concerns**, not runtime concerns. After build, they're files in the package. The runner doesn't need special staging logic.
- `resolve_dx_artifact_path()` at run time — artifact resolution moves to build.
- Benchmark registry lookup at run time — happens at build.
- Local path leakage into the runner — impossible, paths are resolved before the runner sees anything.

**What the package looks like:**

```
<experiment_package>/
├── manifest.json              # Resolved experiment definition (fully materialized)
├── agent_builds/              # One or more bundled agent artifacts
│   └── rex_default/
│       └── bin/rex
├── tasks/                     # Bundled dataset
│   └── tasks.jsonl
├── files/                     # Bundled config files and workspace patches
│   ├── defaults.json
│   └── providers.ts
└── checksums.json             # SHA256 of all bundled content
```

### 4.3 Backward Compatibility

`lab run experiment.yaml` continues to work — it internally runs build → run. The build step is implicit for local development. Explicit `lab build` is for producing portable artifacts for remote execution, CI, or sharing.

### 4.4 What This Means for the Experiment YAML

The authoring format after consolidation:

```yaml
experiment:
  id: bench_v0_model_comparison
  name: "Model comparison on Bench v0"
  tags: [bench-v0, ab-test]

benchmark: bench_v0
limit: 20

agent_builds:
  - id: rex_default
    artifact: rex-minimal-linux-dir
    command: [rex, run, --dangerous]
    io: { input: --input-file, output: --output }
    config_files:
      - overrides/defaults.bench-lmstudio-headless.json
    workspace_patches:
      overrides/providers.lmstudio-docker.ts: packages/core/types/src/providers.ts
    arg_map:
      - key: model_provider
        flag: --provider
      - key: model
        flag: --model

variants:
  - id: qwen_35b
    baseline: true
    agent_ref: rex_default
    env:
      MEMORY_DAEMON_URL: ""
    config:
      model_provider: lmstudio
      model: qwen3.5-35b-a3b
  - id: sonnet
    agent_ref: rex_default
    env:
      ANTHROPIC_REGION: us
    config:
      model_provider: anthropic
      model: claude-sonnet-4

overrides:
  network: full
  root_read_only: false
```

Changes from current minimal format:
- `baseline:` section → entry in `variants:` list with `baseline: true`
- `bindings:` → `config:`
- `bindings_to_args:` → `arg_map:`, `binding:` key → `key:`
- `agent:` split into reusable `agent_builds:` + per-variant `agent_ref` and `env`
- No `version:` field (the build step handles schema evolution)

---

## 5. Cross-Experiment Analysis

### 5.1 The Gap

Today, each run lives in `.lab/runs/<run_id>/` with its own facts. There is no way to query across runs. "What was the pass rate for opus across all experiments?" requires manually aggregating JSONL files from multiple run directories.

### 5.2 Cross-Experiment Catalog

A DuckDB database (`.lab/catalog.duckdb`) that indexes all runs:

| Column | Source |
|--------|--------|
| `run_id` | Run manifest |
| `experiment_id` | Resolved experiment |
| `benchmark` | Resolved experiment |
| `variant_id` | Variant config |
| `config_json` | Variant config (full JSON for arbitrary queries) |
| `pass_rate` | Aggregated from trials |
| `mean_duration_ms` | Aggregated from trials |
| `token_cost` | Aggregated from metrics |
| `run_date` | Run manifest |

### 5.3 Querying by Variant Config JSON (No Wide Parameter Table in v1)

When a run completes, the catalog:
1. Extracts all variant configs
2. Stores full `config_json` with run-level summary metrics
3. Optionally stores inferred parameter metadata separately for analysis, not as a wide catalog table

Cross-experiment queries then work naturally:

```sql
-- All runs where model=opus, regardless of experiment
SELECT experiment_id, pass_rate, mean_duration_ms
FROM catalog
WHERE config_json->>'model' = 'claude-opus-4'
ORDER BY run_date DESC;

-- Compare models across all bench_v0 experiments
SELECT config_json->>'model' AS model,
       AVG(pass_rate) AS avg_pass_rate,
       COUNT(*) AS n_runs
FROM catalog
WHERE benchmark = 'bench_v0'
GROUP BY 1;
```

### 5.4 Statistical Analysis

The analysis layer should provide primitives beyond `AVG(metric)`:

- **Confidence intervals** on pass rates (Wilson score for proportions)
- **Paired hypothesis tests** (McNemar's test for pass/fail, paired t-test for continuous metrics)
- **Effect sizes** (Cohen's d, odds ratios)
- **Power analysis** ("do I have enough trials to detect a 5% difference?")
- **Multi-factor decomposition** — for experiments with multiple swept parameters, decompose into main effects and interactions (ANOVA-style, using the inferred parameter structure)

These are computed by the analysis layer, not the runner. They consume facts and produce summary tables.

---

## 6. Live Monitoring

### 6.1 The Gap

The runner emits rich hook events (`model_call_start/end`, `tool_call_start/end`, `agent_step_start/end`) into JSONL, but there is no live aggregation surface. During a run, the developer has no way to see progress and variant-level health without manually tailing JSONL files.

### 6.2 Live Aggregation Layer

A component that sits between the RunSink and consumers. As trials complete:

- **Rolling stats per variant**: pass rate, mean duration, token cost, error rate
- **Progress and throughput**: completed trials, in-flight trials, ETA
- **Failure/timeout counters**: continuous visibility into unstable variants
- **Cost projection**: estimated remaining compute/token cost at current rate

### 6.3 Two Consumer Surfaces

**For human developers** — a structured feed, not a dashboard with 50 charts:

```
[12:03:04] Trial t_0032 (opus/prompt_v2) completed: pass (14.2s, 3.2k tokens)
[12:03:07] Variant sonnet/prompt_v1: 0/8 passing, 2 timeouts
[12:03:11] Trial t_0035 (opus/prompt_v1) timeout after 600s — 3rd timeout for this variant
[12:03:15] Cost projection: $47.20 remaining (~2.1h at current rate)
```

**For agent consumers** — a structured API:

```
GET /runs/{run_id}/status     → { completed: 45/120, error_rate: 0.02, variants: [...] }
GET /runs/{run_id}/events     → stream of normalized trial + aggregate updates
```

This phase is observability-only. It intentionally excludes automated early stopping, pruning, or decision recommendations.

---

## 7. Implementation Plan

Build/run separation comes first, then schema/interface cleanup, then secondary surfaces.

### Phase 1: Build Step (Internal)

**Goal:** Extract reference resolution from the runner into an explicit build phase. The runner accepts fully-resolved packages.

**What changes:**
- New `lab-build` crate (or module within `lab-cli`) that:
  - Resolves `artifact` references → bundles agent artifact with SHA256 pin
  - Resolves `config_files` → bundles into package
  - Resolves `workspace_patches` → bundles source files
  - Resolves `benchmark: X` → materializes benchmark internals
  - Resolves template variables → concrete values
  - Outputs a self-contained package directory
- Runner modified to accept packages instead of raw YAML
- `lab run experiment.yaml` implicitly builds first (backward-compatible)
- `lab build experiment.yaml -o <package_dir>` for explicit builds

**What doesn't change:** The experiment YAML format. The CLI surface. The runner's trial execution. This is purely internal restructuring.

**Why first:** Every subsequent phase benefits from the build/run separation. The terminology cleanup and variant model are simpler once the runner consumes only fully-resolved packages.

### Phase 2: Schema & Terminology Cleanup (First Interface Change)

**Goal:** Consolidate to one experiment format with clean terminology and fewer top-level concepts.

**What changes:**

Schema deletions:
- `resolved_experiment_v0_3.jsonschema` — dead version
- `resolved_experiment_v0_4.jsonschema` — dead version
- `knob_manifest_v1.jsonschema` — replaced by parameter inference
- `experiment_overrides_v1.jsonschema` — overrides are inline
- `experiment_v1_0.jsonschema` — replaced by minimal format

Schema modifications:
- One authoring format (evolved from current minimal)
- One resolved format (output of build step)
- `bindings` → `config` on variants
- `bindings_to_args` → `arg_map`, `binding:` → `key:`
- `baseline` + `variant_plan` → `variants` list, one entry has `baseline: true`
- `replications` → `repeats`

Rust/SDK renames:
- Struct field renames to match schema changes
- `knob` → `parameter` in internal code
- `call_id` → `turn_id` in event schemas

Hard cutover:
- Legacy fields rejected in preflight with actionable error messages
- No silent fallback to old parsing

### Phase 3: Variant Runtime Model (First Interface Change)

**Goal:** Keep `variants` top-level while allowing variant-specific runtime differences without duplicating agent builds.

**What changes:**
- Introduce reusable top-level `agent_builds` entries
- `variants[].agent_ref` points to one `agent_builds[].id`
- Variants carry their own `config` and `env` deltas declaratively
- Build output materializes the per-variant runtime tuple: `(agent_ref, command/io/arg_map, config, env, policy)`
- Build deduplicates shared agent artifacts when multiple variants reference the same build

Validation rules:
- Every `agent_ref` must resolve to exactly one build
- If more than one variant is declared, exactly one must be `baseline: true`

### Phase 4: Parameter Inference Engine (Internal)

**Goal:** Given a set of resolved variants, automatically determine which parameters are swept vs. fixed.

**What changes:**
- New internal module that:
  - Extracts all config keys across variants
  - Identifies swept parameters (differ across variants) and their distinct values
  - Identifies fixed parameters (same in all variants) and their constant values
  - Infers types from values
- Wired into the build output as analysis metadata
- Consumed by analysis views, not by a wide catalog-table schema

**What this replaces:** `knob_manifest_v1` as a developer-authored artifact. `scientific_role` as a declared field.

### Phase 5: Cross-Experiment Catalog (Internal)

**Goal:** A queryable index of all runs with their variant configs and summary results.

**What changes:**
- `.lab/catalog.duckdb` created and maintained
- On run completion, index: run_id, experiment_id, benchmark, variant_id, full `config_json`, summary metrics
- Query interface via `lab query` CLI command or DuckDB SQL
- No one-column-per-parameter materialization in catalog v1

**Depends on:** Phase 1 (required). Phase 4 is optional for richer derived analysis views.

### Phase 6: Analysis Improvements (Internal)

**Goal:** Cross-experiment aggregation and statistical primitives over cataloged results.

**What changes:**
- Per-experiment views generated from inferred swept parameters (no manual ViewSet selection)
- Cross-experiment views in the catalog DB
- Statistical functions: confidence intervals, hypothesis tests, effect sizes
- Multi-factor decomposition for experiments with multiple swept parameters

**Depends on:** Phase 4 (parameter inference), Phase 5 (catalog for cross-experiment).

### Phase 7: Live Monitoring (Deferred, Status-Only)

**Goal:** Real-time run visibility for humans and programmatic consumers, without automated decisioning.

**What changes:**
- Aggregation layer between RunSink and consumers
- Rolling statistics per variant
- Progress/throughput/ETA summaries
- Event stream (SSE/WebSocket) for status updates

**Explicitly out of scope in this phase:**
- Early stopping signals
- Automatic variant pruning endpoints

**Depends on:** Phase 1. Can ship independently of Phases 4-6.

### Phase 8: UI & API Surface (Last)

**Goal:** Human and agent interfaces for authoring, status monitoring, and analysis.

**Authoring surface:**
- `lab build` CLI command for explicit package creation
- Preflight validation with clear error messages referencing the simplified schema

**Monitoring surface:**
- `lab watch <run_id>` — structured feed in terminal
- WebSocket/SSE status and event endpoints
- No `signal` / `prune` APIs in the consolidation milestone

**Analysis surface:**
- `lab analyze <run_id>` — per-experiment summary with swept parameter decomposition
- `lab compare <run_id_1> <run_id_2> ...` — cross-experiment comparison
- `lab query <sql>` — direct catalog queries
- Visual dashboard (web UI) consuming the catalog and analysis APIs

---

## 8. Dependency Graph

```
Phase 1: Build Step
    │
    ├──→ Phase 2: Schema & Terminology Cleanup
    │        │
    │        └──→ Phase 3: Variant Runtime Model
    │                 │
    │                 └──→ Phase 4: Parameter Inference
    │                          │
    │                          └──→ Phase 5: Cross-Experiment Catalog
    │                                   │
    │                                   └──→ Phase 6: Analysis Improvements
    │
    ├──→ Phase 7: Live Monitoring (Deferred, Status-Only)
    │
    └──→ Phase 8: UI & API Surface (Last)
```

Phases 1-3 are the consolidation milestone (schema slimming + build/run boundary).
Phases 4-6 are analysis depth.
Phase 7 is explicitly deferred and can ship later.
Phase 8 waits for stabilization of the prior shipped phases.

---

## 9. What Gets Deleted

An explicit inventory of concepts, schemas, and code that this plan removes:

### Schemas deleted
- `resolved_experiment_v0_3.jsonschema`
- `resolved_experiment_v0_4.jsonschema`
- `knob_manifest_v1.jsonschema`
- `experiment_overrides_v1.jsonschema`
- `experiment_v1_0.jsonschema` (legacy verbose format)

### Concepts deleted
- `scientific_role` (treatment/control/confound/derived/invariant) — derived, not declared
- `knob_manifest` as developer-authored artifact — derived from variants
- `variant_plan` as a named concept — just `variants`
- `baseline` as a separate section — a variant with a flag
- `bindings` as terminology — `config` on variants
- `design.comparison` as required field — inferred from variant structure
- `role` on parameters (core/harness/benchmark/safety/analysis/infra) — serves no runtime purpose

### Code paths simplified
- `resolve_dx_artifact_path()` — moves to build step, removed from runner hot path
- Config file staging at trial time — files bundled at build, not staged per trial
- Workspace patch staging at trial time — patches bundled at build
- Benchmark registry lookup at run time — materialized at build
- ViewSet manual selection — analysis auto-selects based on inferred parameter structure
