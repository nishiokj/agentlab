# Harbor First-Class Integration Spec

Status: Implemented through Phase 3 scaffolding v0.2  
Date: 2026-02-23  
Owner: AgentLab Runtime/Benchmark Integrations  
Scope: `/Users/jevinnishioka/Desktop/Experiments` as integration source, `/Users/jevinnishioka/Desktop/jesus` as experiment consumer

## 1) Objective

Make Harbor a first-class integration path for Terminal Bench 2.0 (and future Harbor-backed benchmarks) while preserving:

1. AgentLab as source of truth for orchestration, provenance, and analytics.
2. Strict dependency isolation so Harbor dependencies do not leak into runner core or non-Harbor experiments.
3. A stable boundary that can absorb Harbor API drift without breaking unrelated workloads.

## 2) Non-Goals

1. No Harbor dependency added to Rust runner crates.
2. No native runner ingestion of Harbor task directories in this phase.
3. No benchmark-specific logic embedded in runner core.
4. No requirement that every experiment image include Harbor tooling.

## 3) Boundary Model

Use two explicit seams that already exist in AgentLab:

1. Dataset seam: `dataset.path` JSONL ingestion.
2. Scoring seam: `benchmark.adapter.command` producing benchmark artifacts.

Runner remains unchanged and Harbor stays in adapter/build tooling.

## 4) Architecture

### 4.1 Build-time Bridge (Harbor -> AgentLab Dataset)

Add a Harbor dataset exporter that maps Harbor task sources into AgentLab task JSONL.

Output format:

1. `task_boundary_v2` rows when per-task image/workspace is present.
2. Plain task rows when task boundary metadata is not needed.

Primary output fields:

1. `schema_version`
2. `task.id`
3. `task.image` (optional)
4. `task.workspace` (optional)
5. benchmark identity fields (`adapter_id`, `name`, `split`)
6. benchmark/task payload needed by harness + evaluator

### 4.2 Runtime Adapter (AgentLab -> Harbor Evaluator)

Add a Harbor benchmark adapter command that:

1. Reads `AGENTLAB_TASK_PATH` and `AGENTLAB_RESULT_PATH`.
2. Invokes Harbor evaluation/scoring flow.
3. Writes:
   1. `AGENTLAB_BENCHMARK_PREDICTION_PATH`
   2. `AGENTLAB_BENCHMARK_SCORE_PATH`
4. Emits records conforming to:
   1. `benchmark_prediction_record_v1`
   2. `benchmark_score_record_v1`

## 5) Repo Ownership Split

### 5.1 `/Experiments` (reusable integration layer)

Place here:

1. `scripts/harbor/export_harbor_to_agentlab_jsonl.py`
2. `scripts/harbor/harbor_benchmark_adapter.py`
3. `scripts/harbor/tests/*`
4. `scripts/harbor/requirements-harbor.txt` (or equivalent optional dependency manifest)
5. `docs/HARBOR_FIRST_CLASS_INTEGRATION_SPEC.md`
6. generic runbook docs for Harbor integration

Rule: code here must be repo-agnostic and reusable across consumer repos.

### 5.2 `/jesus` (consumer/runtime layer)

Place here:

1. experiment YAML files consuming the Harbor bridge/adapter
2. wrapper run scripts with local paths/env
3. generated mapped datasets
4. local env and deployment-specific knobs

Rule: only runtime configuration and generated artifacts live here.

## 6) Dependency Isolation Strategy

Harbor dependencies are optional and scoped:

1. Keep Harbor packages out of Rust workspace and SDK dependencies.
2. Harbor adapter runs as an external command, not an in-process library.
3. Harbor install only required in:
   1. Harbor-specific container images, or
   2. Harbor setup virtualenv used by Harbor wrapper scripts.
4. Non-Harbor experiments remain unaffected and require no Harbor install.

Operational stance:

1. Pin Harbor versions for production reproducibility.
2. Test against latest minor in a separate compatibility lane.

## 7) Terminal Bench 2.0 Support Model

Support TB2 via a profile, not hard-coding TB2 into core integration:

1. Generic Harbor bridge logic handles common Harbor task model.
2. TB2 mapper profile handles TB2-specific field mapping/defaults.
3. New Harbor-backed benchmarks reuse the same bridge and adapter with new profile mappings.

## 8) Implementation Plan

## Phase 1: Minimal First-Class Path

1. Add Harbor exporter script.
2. Add Harbor benchmark adapter script.
3. Add Harbor-backed experiment template.
4. Add wrapper script for build+run flow.
5. Add unit tests for mapper + adapter output schema shape.

## Phase 2: Hardening

1. Add per-task image mode coverage (`image_source: per_task`).
2. Add smoke run fixture (single task) for end-to-end validation.
3. Add strict error taxonomy in adapter (mapping/eval failures).

## Phase 3: Compatibility Monitoring

1. Add CI lane against pinned Harbor.
2. Add CI lane against latest Harbor minor (canary).
3. Alert on Harbor API changes with actionable failure messages.

## 9) CI and Typecheck Strategy

Use two classes of checks:

### 9.1 Always-on (no Harbor runtime required)

1. Static checks for Harbor scripts (format/lint/type where applicable).
2. Unit tests using fixtures and mocked evaluator outputs.
3. JSON schema validation for generated prediction/score records.

### 9.2 Optional/Canary (Harbor installed)

1. Harbor integration tests:
   1. pinned Harbor version
   2. latest Harbor minor
2. One-task end-to-end run exercising:
   1. dataset export
   2. trial execution
   3. benchmark artifacts
   4. analysis ingestion

Failure policy:

1. Pinned lane is blocking.
2. Latest-minor lane is warning or nightly-blocking based on team preference.

## 10) Experiment Contract Example

Minimal Harbor-backed experiment requirements:

1. `dataset.provider: local_jsonl`
2. `dataset.path: <generated_harbor_mapped.jsonl>`
3. `benchmark.adapter.command: [ ... harbor_benchmark_adapter.py ... ]`
4. container image containing runtime + Harbor adapter dependencies when container mode is used

Optional for Harbor task images:

1. `runtime.agent.image_source: per_task`
2. `runtime.agent.artifact: <agent artifact tar path>`
3. dataset rows include `task.image`

## 11) Acceptance Criteria

1. `lab-cli describe` succeeds on Harbor-backed experiment config.
2. `lab-cli run` with one Harbor-backed task produces:
   1. valid `benchmark_prediction.json`
   2. valid `benchmark_score.json`
3. run-level benchmark outputs are present:
   1. `benchmark/predictions.jsonl`
   2. `benchmark/scores.jsonl`
4. analysis pipeline ingests Harbor-backed run without schema errors.
5. non-Harbor experiments run unchanged on same branch.

## 12) Risks and Mitigations

1. Risk: Harbor API changes break adapter.
   1. Mitigation: pinned + canary matrix; clear adapter compatibility layer.
2. Risk: repo bloat from benchmark-specific logic.
   1. Mitigation: keep generic Harbor core + profile mappings; keep generated datasets out of source when possible.
3. Risk: dependency leakage into core runner.
   1. Mitigation: command boundary only; no Rust/SDK Harbor deps.
4. Risk: brittle local path assumptions across repos.
   1. Mitigation: strict `/Experiments` reusable code vs `/jesus` runtime config split.

## 13) Open Decisions

1. Canary policy currently defaults to non-blocking with optional blocking on manual dispatch.
2. Whether Harbor integration scripts should be Python-only or have a Node wrapper.
3. Whether to add a thin `lab-cli` helper for dataset export orchestration in a later phase.

## 14) Recommendation

Proceed with first-class Harbor integration as a boundary plugin in `/Experiments` now.

This gives:

1. fast TB2 support,
2. low runner-core risk,
3. future Harbor benchmark reuse,
4. controlled dependency surface.
