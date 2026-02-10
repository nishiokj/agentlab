# AgentLab GUI MVP Plan

## Objective

Ship a minimum viable GUI that gets a user from repo onboarding to scientifically credible experiment comparison quickly, while keeping execution grounded in existing CLI + schema contracts.

## Product Constraints

1. GUI must not fork execution semantics from CLI.
2. Validity/grades must be visible before performance conclusions.
3. Knob editing must be schema-driven (no per-harness UI code).
4. Outputs must remain reproducible via file artifacts + CLI commands.
5. `allowlist_enforced` claims require real network-layer enforcement (proxy + bypass blocking), not tool-level conventions.

## Primary User Journeys

1. Onboard
- Connect harness repo.
- Detect likely runtime/harness entrypoints.
- Scaffold config + run smoke validation.

2. Integrate
- Configure runtime surfaces (command, container/image, network, hooks/traces).
- Validate integration quality and show missing pieces.

3. Design
- Choose workload type (`agent_harness` or `trainer`).
- Select treatment/control knobs.
- Build a variant plan and analysis plan.

4. Run
- Launch `run-dev` or `run-experiment`.
- Monitor trial progress, errors, warnings, and artifacts.

5. Analyze
- Compare baseline vs variant-plan entries with grades, effect sizes, CIs, and confounds.
- Drill into per-task/per-trial evidence.

6. Publish
- Build debug/report bundle and provenance summary for sharing.

## MVP Screens

1. Home
- Recent runs, statuses, grades, quick actions.

2. Onboarding Wizard
- Repo scan, entrypoint pick, scaffold files, smoke test.

3. Experiment + Knob Editor
- Form + raw YAML editor.
- Knob registry table + override editor + variant diff.

4. Run Console
- Live run timeline, trial statuses, warning stream.

5. Results
- Primary metric comparison + uncertainty + validity warnings.
- Event/metric drilldowns.

6. Trial Inspector
- Trial input/output, hook/tracing evidence, artifacts, error details.

7. Publish
- Bundle generation, provenance/attestation snapshot.

## MVP Data/Execution Architecture

1. Execution Engine
- Keep existing `lab-cli` as source of truth.
- GUI invokes CLI commands and surfaces exact command equivalents.

2. Storage Model
- Source of truth remains `.lab/runs/<run_id>/...`.
- GUI reads existing JSON/JSONL artifacts and schema-validates where needed.

3. Analytics Backend
- Read normalized run tables from `analysis/tables/*.jsonl`.
- Optional DuckDB acceleration for aggregate queries/charts.

4. Config Surfaces
- Experiment spec: `.lab/experiment.yaml`
- Knobs: `.lab/knobs/manifest.json`
- Overrides: `.lab/knobs/overrides.json`

## Proxy-Egress Baseline (MVP)

Purpose:
- Provide universal network enforcement and coarse observability even when hooks are absent.

Requirements:
1. Egress enforcement property
- If `network.mode = allowlist_enforced`, all outbound traffic must be forced through proxy and direct egress must be blocked.

2. Runner architecture
- Per-trial proxy process/container.
- Firewall rules that prevent bypass.
- `HTTP_PROXY`/`HTTPS_PROXY` env hints are optional convenience, not enforcement.

3. Evidence and UI surfacing
- Show evidence source badges:
  - `hooks`
  - `traces`
  - `network_proxy`
- Mark proxy-only runs as coarse-causal evidence.

4. Self-test contract
- Before trial starts in `allowlist_enforced`, run egress self-test:
  - disallowed target must fail
  - allowed target must succeed
- Persist test artifact and status in state inventory.

5. Telemetry artifact
- `network_events.jsonl` with:
  - timestamp
  - trial_id
  - destination host/ip
  - protocol/method (when available)
  - allow/block decision
  - bytes and latency
  - rule id/reason

## Must-Have Knob UX

1. Knob Browser
- Show `id`, `type`, `options`, bounds, role, scientific_role.

2. Override Safety
- Validate knob ID existence and type/range/options before save/run.
- Show unresolved/invalid pointer updates as hard errors.

3. Variant Builder
- Start from baseline.
- Apply isolated override diffs per variant.
- Render “what changed” in plain language + raw JSON.

## Must-Have Analytics UX

1. Validity Header
- `replay/isolation/comparability/provenance/privacy` grades at top.

2. Primary Metric Panel
- Baseline vs variant effect + CI + practical significance threshold.

3. Confound Panel
- Missingness, retries, queue skew, network/profile caveats.

4. Evidence Drilldown
- Trial/task exemplars.
- Event count deltas by variant/trial.
- Hook/tracing availability badges.

## Delivery Plan (Phased)

### Phase G1: Onboarding + Integration Health
Deliver:
- Repo detection wizard.
- `init`, `describe`, `knobs-init`, `knobs-validate` integration.
- Smoke run path with actionable error hints.
- Proxy capability detection and policy preview (`none`, `full`, `allowlist_enforced`).
- Explicit warning/fallback when bypass-block enforcement is unavailable on host runtime.
Exit criteria:
- New user gets to a successful smoke run end-to-end from GUI.

### Phase G2: Knobs + Experiment Design
Deliver:
- Schema-driven knob editor.
- Overrides/variant builder.
- Analysis plan editor with guardrails.
Exit criteria:
- User can define baseline + >=1 variant with validated overrides.

### Phase G3: Run Orchestration + Monitoring
Deliver:
- Launch controls for `run-dev` and `run-experiment`.
- Live status dashboard and error surfacing.
Exit criteria:
- User can monitor active runs and inspect failed trials.

### Phase G4: Results + Scientific Inference
Deliver:
- Comparison dashboard (effect sizes, CIs, warnings).
- Trial/task drilldowns and evidence linking.
Exit criteria:
- User can make/decline a conclusion with visible validity assumptions.

### Phase G5: Publish + Share
Deliver:
- Publish flow with bundle and attestation summary.
Exit criteria:
- User can export an auditable package from GUI.

## Hard Parts (Do Not Hand-Wave)

1. Path/Environment Resolution
- Repo root vs binary location vs experiment path.
- Local vs container path translation.

2. Process Reliability
- Long-running CLI subprocess management, cancellation, retries, log streaming.

3. Schema Drift
- GUI/CLI schema version mismatch and migration behavior.

4. Scientific Guardrails
- Prevent invalid runs from appearing “green.”
- Enforce warning semantics before conclusions.

5. Performance at Scale
- Large run directories and high trial counts in UI.
- Query latency for per-trial drilldowns.

6. Proxy Correctness
- Distinguish "proxy configured" from "proxy enforced."
- Ensure bypass blocking is active before granting strict isolation claims.

7. Privacy + Inspection Boundaries
- Avoid accidental secret capture in proxy logs.
- Default to metadata logging unless explicit debug mode is enabled.

## Expansion Template (For Iterative Deepening)

For each phase and each feature we will fill:

1. Functional spec
- User action, expected behavior, success/failure states.

2. Data contract
- Input/output schemas, persisted files, migration/versioning.

3. Integration seams
- CLI calls, artifact paths, parsing/validation steps.

4. Error handling
- User-visible errors, retry logic, diagnostics.

5. Observability
- Internal telemetry/logs to debug GUI failures.

6. Test plan
- Unit, integration, and end-to-end acceptance criteria.

7. Non-goals
- Explicitly what this step will not solve yet.

## Next Iteration Order

1. Deep-spec Phase G1 (onboarding + integration health).
2. Deep-spec Phase G2 (knob editing and variant construction).
3. Deep-spec Phase G3 (run monitoring and lifecycle state machine).
