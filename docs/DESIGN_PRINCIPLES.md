# Design Principles — Experiments Library

Derived from analysis of [Prime Intellect's Verifiers](https://github.com/PrimeIntellect-ai/verifiers) library and how its patterns apply to our Rust-based experiment framework.

---

## 1. Trait-Based Argument Injection

Verifiers' best idea: reward functions declare dependencies via parameter names (`async def check(completion, answer, state)`). The framework inspects the signature and provides only what's requested.

In Rust, the equivalent is **extractor-based handlers** (the Axum pattern). Instead of every rubric function receiving a monolithic `TrialContext`, define extractor traits:

```rust
trait FromTrialContext {
    fn from_context(ctx: &TrialContext) -> Self;
}

// Rubric functions declare what they need
async fn exact_match(completion: Completion, answer: Answer) -> f64 { ... }
async fn judge_quality(completion: Completion, judge: JudgeClient) -> f64 { ... }
async fn diversity_bonus(group: CompletionGroup) -> Vec<f64> { ... }
```

The harness boundary stays schema-driven (trial_input/trial_output JSON) — that's correct for a process boundary. But the **rubric/analysis boundary** should be function-ergonomic. Researchers writing scoring functions shouldn't think about JSON schemas; they declare what they need and get it.

---

## 2. Rubric as a Composable Pipeline

The benchmark adapter already separates predict from score. The scoring side should be further decomposed into weighted, composable reward functions:

```rust
let rubric = RubricBuilder::new()
    .reward(exact_match, 1.0)           // hard correctness
    .reward(partial_credit, 0.3)        // soft credit
    .reward(judge_quality, 0.5)         // LLM-as-judge
    .metric(token_count)                // logged, weight=0
    .metric(latency_ms)                 // logged, weight=0
    .build();

// Compose heterogeneous rubrics
let group = RubricGroup::new(vec![
    math_rubric(),      // symbolic verification
    style_rubric(),     // judge-based
    custom_rubric(),    // domain-specific
]);
```

Key insight: **metrics and rewards are the same thing with different weights**. A metric is a reward with weight 0. Analysis tables get populated as a side effect of scoring — no separate metrics collection step.

---

## 3. The Four-Stage Pipeline

Verifiers has three stages (Dataset → Harness → Rubric). We add a fourth — Analysis — because our use cases (AB testing, forking, replay comparison) require cross-trial reasoning that doesn't belong in per-trial scoring.

```
Dataset       → what to evaluate (task inventory)
Harness       → how the agent runs (interaction protocol, tools, sandbox)
Rubric        → per-trial scoring (reward functions, metrics)
Analysis      → cross-trial reasoning (comparisons, significance tests, reports)
```

Analysis is a trait:

```rust
trait Analysis: Send + Sync {
    async fn analyze(&self, scored_trials: &[ScoredTrial]) -> AnalysisOutput;
}

// Built-in implementations
struct PairwiseComparison { baseline: VariantId }
struct PassAtK { k: usize }
struct BootstrapCI { confidence: f64, n_resamples: usize }
```

This formalizes what the DuckDB-ready JSONL tables already do implicitly. Making it a trait means researchers can plug in custom analysis (e.g., "show me trials where variant A succeeded and baseline failed, grouped by task difficulty").

---

## 4. "Everything is a Trial"

Verifiers unifies SingleTurnEnv as MultiTurnEnv(max_turns=1). Same principle here: **every execution is a trial, even a single-shot evaluation**. The type system should enforce this.

The implication for forking/replaying: a fork is a new trial with a `parent_trial_id` and a `fork_point` (step selector). No special infrastructure — it's a trial whose `trial_input` was derived from another trial's state at a checkpoint. The artifact store stays uniform.

---

## 5. Dual Entry Points — Builder API + YAML

Verifiers uses `load_environment()` as a single factory function. Our YAML + resolver pipeline is more rigorous but less ergonomic for quick iteration. Support both:

```rust
// Programmatic (research ergonomics, REPL-friendly iteration)
fn load_experiment() -> Experiment {
    Experiment::builder()
        .dataset(gsm8k_dataset())
        .harness(claude_harness().with_tools(calculator))
        .rubric(math_rubric().weighted(1.0) + style_rubric().weighted(0.1))
        .analysis(pairwise_comparison("baseline"))
        .variant_plan([
            variant("baseline").bind("model", "gpt-4"),
            variant("treatment").bind("model", "claude"),
        ])
        .replications(3)
        .build()
}

// Declarative (CI/production reproducibility)
// experiment.yaml → resolves to the same Experiment struct
```

Both paths produce the same `Experiment` struct, the same digest, the same artifact layout.

---

## 6. Variant Planning vs. Adaptive Selection

Default terminology should match the common workflow (fixed A/B/N runs):

- `Variant`: one runnable configuration (model/prompt/tool flags/params).
- `VariantPlan`: explicit set of variants to evaluate.
- `SelectionStrategy` (optional): adaptive allocation policy that changes which variant to run next based on observed outcomes.

This keeps the base runtime model simple and predictable while still enabling advanced adaptive experimentation.

```rust
struct Experiment {
    dataset: Dataset,
    harness: Harness,
    rubric: Rubric,
    analysis: Vec<Box<dyn Analysis>>,
    variant_plan: VariantPlan,
    selection_strategy: Option<Box<dyn SelectionStrategy>>, // advanced
}
```

Important runtime constraint: `SelectionStrategy` mutates scheduling behavior during execution, so it should be modeled as an explicit extension point with clear provenance in artifacts.

---

## 7. Content-Addressable Everything

Every intermediate state should be content-addressable via canonical JSON + SHA256:

```
experiment.yaml  →  resolved_experiment.json  →  digest
trial_input.json →  digest
checkpoint       →  artifact://sha256/...
fork_point       →  (parent_digest, step_selector)
```

A "frozen experiment" is its digest. A "replay" is loading by digest and re-executing. A "fork" is a new experiment whose provenance references the parent digest + mutation.

---

## 8. Integration Level as a Type-Level Constraint

The integration level hierarchy (cli_basic → sdk_full) should be a **compile-time guarantee**, not just a runtime grade:

```rust
trait ReplayCapability {}
struct StrictReplay;    // sdk_full
struct BestEffort;      // cli_events/otel
struct ReExecOnly;      // cli_basic

impl Trial<StrictReplay> {
    fn fork_at(&self, step: StepSelector) -> TrialInput { ... }
}

impl Trial<BestEffort> {
    fn fork_at(&self, step: StepSelector) -> Result<TrialInput, ReplayDegraded> { ... }
}

// cli_basic trials don't have fork_at — it's absent from the type
```

Researchers can't accidentally call `fork_at` on a trial that doesn't support it.

---

## Summary — Steal vs. Extend

| Principle | From Verifiers | Action |
|-----------|---------------|--------|
| Argument injection (extractor traits) | Function signature inspection | **Adopt** for rubric functions |
| Rubric composition + weights | RubricGroup | **Adopt** as weighted reward pipeline |
| Metrics = rewards(weight=0) | Built-in | **Adopt** to unify metrics into rubric |
| Content addressing | (absent) | **Already ahead** — double down |
| Integration levels | (absent) | **Already ahead** — push to type level |
| Replay/fork model | (absent) | **Already ahead** — unify under trial model |
| Analysis stage | (absent) | **Formalize** as a trait |
| Factory pattern | `load_environment()` | **Add builder API** alongside YAML |
| Variant planning terminology | (absent) | **Use `VariantPlan`; reserve `SelectionStrategy` for adaptive mode** |

**Highest leverage changes:** extractor-based rubric functions and the builder API. Those two transform this from "infrastructure for running experiments" into "a library researchers want to use."
