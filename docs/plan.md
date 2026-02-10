# Agent Lab v0.3 — Plan

## Summary (What we are building)
Agent Lab is an experiment harness for agentic systems that prioritizes auditability, replayability, and valid inference. v0.3 is standards‑first: the framework does not assume it owns the internal agent runtime boundaries. Instead, it standardizes external seams that BYO harnesses can realistically expose: CLI I/O, hook/event streams, and tracing. The framework still enforces sandboxing, trial lifecycle, evaluation, analysis, provenance, and reporting.

## Goals (v0.3)
- Interoperable, pluginable runtime boundaries and tool metadata enforcement.
- Fast iteration via checkpoints, fork/replay, and predictable restore.
- Traceable runs with correlation across LLM/tools/evaluators.
- Benchmark hygiene: paired design integrity, missingness handling, retries labeling, multiple comparisons.
- Provenance: resolved spec registration + digest, hashchain, attestation, optional SBOM.
- Interpretability: per-task diffs, suspects, exemplars.
- CLI surface: run, validate, replay, fork, compare, doctor.

## Non‑goals (v0.3)
- Perfect determinism across closed providers/hardware/OS scheduling.
- Universal bypass I/O detection without explicit instrumentation.
- Secure sandbox claims beyond container isolation + explicit controls.

## Critical Invariants
- Only mounted state surfaces are writable; root FS read‑only.
- Profiles enforce invariants (replay_strict_v2, hermetic_functional_v2, perf_benchmark_v2).
- Every run/trial emits grades; results without grades are “debug only.”
- Events are hash‑chained and stored in a stable envelope; payloads are content‑addressed artifacts with redaction applied.
- Registration (resolved spec + digest) occurs before run start.

## Primary Seams / Integration Boundaries
- **Harness boundary (new)**: CLI I/O contract, hook events JSONL, tracing (OTLP), optional SDK control plane.
- **Framework boundary**: sandbox + trial lifecycle + evaluation + analysis + provenance + reporting.
- **Runner boundary**: spec resolution, variant expansion, scheduling, container orchestration, profile enforcement.
- **Analysis boundary**: analysis_plan execution, missingness policy, multiple comparisons, effect sizes, interpretability bundles.
- **Report boundary**: static HTML/report bundle + trace viewer (optional but recommended).

## Data & Artifact Layout
- Run directory: `.lab/runs/<run_id>/...` with manifest, resolved spec + digest, trials, analysis, report, debug bundles.
- Events: framework `events.jsonl` with stable envelope + hashchain; harness hook stream at `/out/harness_events.jsonl` when enabled.
- Traces: OTLP export or trace manifest + artifacts when provided.
- Checkpoints: workspace + state + memory + RNG + budgets snapshot.
- Interpretability: paired diffs tables, suspects, exemplars (trace/tool/memory/workspace diffs).

## Risks / Footguns (to guard explicitly)
- **False replay/interpretability claims**: must be derived from integration_level and evidence actually observed.
- **Replay claims without instrumentation**: do not imply per‑file replayability or boundary capture unless hooks/traces/sdk_full are present.
- **Missingness handling**: paired_drop vs impute vs treat_as_failure must be enforced consistently.
- **Retry influence**: must label results and record all attempts as events.
- **Non‑hermetic caches**: shared caches only allowed in perf profile; must label.
- **Network allowlist claims**: only valid if enforced at the network layer (netns+iptables or sidecar proxy with bypass blocked).
- **Evaluator drift**: LLM evaluator versions + params must be captured.
- **Concurrency confounds**: queue times and throttling differences need warnings.
- **Redaction**: must be applied before payload is stored; avoid secret leakage in inventory.

## Milestones (initial pass)
1. **Foundation (Phase 0)**
   - Core data models, event envelope, artifact store, hashchain, grades schema, harness integration levels.
2. **Harness + Runner (Phase 1)**
   - CLI I/O contract, hook collector + schema validator, control‑plane file protocol, container runner, profiles.
3. **Recording + Checkpoints (Phase 2)**
   - Framework event capture, checkpointing, fork/replay semantics derived from integration level.
4. **Analysis + Interpretability (Phase 3)**
   - analysis_plan execution using hooks/traces/framework events; suspects/exemplars that report evidence sources.
5. **CLI + Report (Phase 4)**
   - run/validate/replay/fork/compare/doctor, report output, debug bundles, provenance summaries.

## Open Questions (to resolve before locking Phase 0)
- Language/runtime (Rust/Go/Python/TS) and interoperability needs.
- Container engine support beyond Docker (podman?) and overlayfs availability.
- Storage choices for artifacts (filesystem-only vs OCI layout vs object storage).
- Analysis stack (Python/pandas/pyarrow) vs in‑language analytics.
- Expected scale (tasks/trials), concurrency targets, and performance constraints.
- How “tools” are registered/loaded (plugin system vs static registry).
 - Harness integration level targets for MVP (cli_basic vs cli_events vs otel).
 - Network enforcement mechanism availability (netns+iptables vs sidecar proxy).
