# Agent Lab v0.3 — Multi‑Phase Implementation Spec (Draft)

> This is the initial scaffold. Each phase will be expanded with concrete interfaces, data structures, and acceptance tests. Open questions are listed at the end of each phase.

## Assumptions (to confirm)
- Single‑repo implementation with a core library + CLI binary.
- Filesystem‑backed artifact store (content‑addressed) in `.lab/runs/<run_id>`.
- Container engine: Docker (primary) with overlayfs available.
- Analysis implemented in Python (pandas/pyarrow) unless constrained otherwise.
- Harness integration is externalized: CLI I/O + optional hook events/traces; no requirement to own in‑process ModelClient boundaries.
- For `allowlist_enforced`, reference implementation uses a proxy-based egress path with network-level bypass blocking (where host/runtime supports it).

## Harness Integration Levels (v0.3)
**Purpose:** Define what the framework can guarantee based on observable harness evidence.

Levels:
- `cli_basic`: CLI I/O only.
- `cli_events`: CLI + canonical hook events JSONL.
- `otel`: CLI + OTel export (OTLP).
- `sdk_control`: in‑process SDK with control‑plane callbacks (pause/stop/checkpoint).
- `sdk_full`: SDK with framework‑wrapped causal boundaries (highest replay fidelity).

Derived guarantees (grades) must be computed from the integration level actually observed, not aspirational config.

---

## Phase 0 — Foundation: Core Schemas, IDs, and Storage

### Goals
- Define canonical data formats for framework events, harness I/O, hook events, grades, manifests, resolved specs, artifacts, and checkpoints.
- Provide minimal storage and hashing primitives used everywhere else.

### Deliverables
- **Schemas**
  - `event_envelope_v1.jsonschema`
  - `hook_events_v1.jsonschema` (harness events JSONL)
  - `trial_input_v1.jsonschema` (harness CLI input)
  - `trial_output_v1.jsonschema` (harness CLI output)
  - `trace_manifest_v1.jsonschema` (when traces are file‑based)
  - `state_inventory_v1.jsonschema` (effective network + storage surfaces)
  - `harness_manifest_v1.jsonschema` (authoritative harness metadata)
  - `grades_v1.jsonschema`
  - `manifest_v1.jsonschema`
  - `attestation_v1.jsonschema`
  - `resolved_experiment_v0_3.jsonschema` (canonical JSON form)
- **ID generation**
  - `run_id`, `trial_id`, `variant_id`, `task_id` deterministic formats.
- **Artifact store**
  - Content‑addressed storage at `artifacts/sha256/<hash>`.
  - `artifact://sha256/<hash>` URI scheme.
- **Hashchain**
  - Canonical JSON line hashing for events; `events.head` output.

### Interfaces
- `ArtifactStore.put(bytes, content_type, redaction_meta) -> artifact_ref`
- `ArtifactStore.get(artifact_ref) -> bytes`
- `EventHasher.hash_prev(prev_hash, event_line) -> self_hash`
- `Grades.emit(grades_struct) -> grades.json`

### Data Decisions
- Canonical JSON serialization (sorted keys, no whitespace) for digesting.
- Event `payload_ref` always refers to already‑redacted payload.
- `grades.json` must include `integration_level` and evidence sources (hooks/traces/sdk).
- `trial_input.json` and `trial_output.json` are required for CLI harness mode.
- `network.mode`: `none | full | allowlist_enforced` and `network.enforcement` must be recorded in resolved spec and state inventory.
- Hook schema requires a `step_semantics` string in `harness_manifest.json` and `step_index` on causal boundary events; `turn_count` is derived from `model_call_end` count.
 - `harness_manifest.json` minimal fields: `hooks_schema_version`, `step_semantics`, `integration_level`, `entry_command`, `exec_digest`, `trace_export` (mode/endpoint or manifest), optional `source_digest`.

### Acceptance Criteria
- Given a JSON event line, `hashchain.self` is stable across runs on the same platform.
- `resolved_experiment.digest` equals SHA256 of canonical JSON.
- Artifacts are retrievable by hash and match their original bytes.

### Open Questions
- Canonical JSON implementation & library choice.
- Whether to store per‑artifact metadata (content‑type, redaction) in sidecar JSON.

---

## Phase 1 — Harness + Runner: CLI Contract, Hooks, Control‑Plane

### Goals
- Establish the harness execution contract (CLI I/O) as the baseline integration.
- Collect and validate optional harness hooks/events and traces.
- Provide a harness‑agnostic control‑plane for pause/stop/checkpoint at step boundaries.

### Deliverables
- **CLI Harness Executor**
  - `runtime.harness` schema: mode `cli`, command, input/output paths.
  - Writes `trial_input.json` and validates `trial_output.json`.
  - `trial_input.json` minimal fields: run/trial/task ids, variant bindings, budgets/timeouts, paths, sanitization profile, expected integration_level.
  - `trial_output.json` minimal fields: outcome (success/failure/missing), evaluator inputs or artifact refs, optional metrics, produced artifacts.
- **Hook Collector + Validator**
  - Reads `/out/harness_events.jsonl` when integration_level >= `cli_events`.
  - Requires `harness_manifest.json` for `cli_events`, `otel`, and `sdk_*` modes (even if hooks absent).
  - Validates hook schema version and `step_semantics` from `harness_manifest.json`.
  - Optional stream header event (e.g., `hooks.header`) is allowed but must exactly match the manifest.
  - Enforces minimal step contract: monotonic `step_index`, step start/end events, and `step_index` on causal boundary events.
  - Derives `turn_count` from `model_call_end` events (universal baseline).
  - Requires `control_ack` event at each `agent_step_end`.
  - Validates `seq` field is present and monotonically increasing per trial.
- **Control‑Plane File Protocol**
  - `runtime.harness.control_plane`: file path `/state/lab_control.json`.
  - Actions: `continue`, `stop`, `checkpoint` (label).
  - Timing: harness MUST check control‑plane at `agent_step_end(step_index)` before `agent_step_start(step_index+1)`.
  - `control_ack` event includes `step_index`, `control_version` (sha256 of file contents), and `action_observed`.
- **Tracing Ingestion (optional)**
  - Runner may start local OTLP receiver; injects env vars for OTEL export.
  - File‑based trace ingestion via `/out/trace_manifest.json`.
- **Spec Resolution**
  - Default filling, path resolution, dataset hashing, registration + digest.
- **Variant Expansion + Scheduling**
  - Baseline + `variant_plan` -> resolved variants stored in `variants/` (artifact path kept stable).
  - Task shuffling, variant order randomization, block by task.
- **Container Runner + Profiles**
  - Read‑only root FS, surface mounts, per‑trial workspace isolation.
  - Network policy enforcement: `none | full | allowlist_enforced`.
  - `allowlist_enforced` requires netns+iptables or sidecar proxy with bypass blocked.
  - Runner SHOULD support proxy-first enforcement:
    - start per-trial egress proxy
    - force outbound path through proxy
    - block direct egress at network layer
  - Proxy headers/env (`HTTP_PROXY`, `HTTPS_PROXY`) are convenience only; claims rely on blocking bypass.
  - Record `network.mode_effective`, `network.enforcement_effective`, `allowed_hosts`, `bypass_risk` in `state_inventory.json`.
  - Require a preflight egress self‑test artifact for `allowlist_enforced` (expected allow/deny).
  - Emit optional `network_events.jsonl` with allow/block decisions and transport metadata for coarse observability.
  - Profile invariant checks; fail or grade as bounded/leaky.

### Interfaces
- `HarnessExecutor.run(trial_input) -> trial_output`
- `HookCollector.collect(path) -> validated_hook_stream`
- `ControlPlane.write(action) -> void`
- `TraceIngestor.ingest(otlp|manifest) -> trace_artifacts`
- `HarnessManifest.load(path) -> harness_manifest`
- `ExperimentResolver.resolve(yaml_path) -> resolved_experiment.json + digest`
- `TrialPlanner.plan(resolved_experiment) -> trial list`
- `ContainerRunner.run(trial_spec) -> exit_status`
- `ProfileEnforcer.check(runtime_state) -> invariants_ok|violations`
- `NetworkProxy.start(policy) -> proxy_handle`
- `NetworkEnforcer.apply(mode, policy, proxy_handle) -> enforcement_state`
- `EgressSelfTest.run(policy, enforcement_state) -> artifact_ref + pass/fail`

### Acceptance Criteria
- A CLI harness can run end‑to‑end with only trial_input/output.
- If hooks are enabled, invalid hook schema fails validation.
- At `agent_step_end`, harness can observe a `checkpoint` action via control‑plane file.
- If `allowlist_enforced` is requested without enforcement, the run fails or isolation_grade = leaky (per `fail_on_profile_invariant_violation`).
- Missing `control_ack` at a step end downgrades replay/interpretability grades or fails in strict profiles.
- `allowlist_enforced` requires a recorded egress self‑test artifact; ambiguous results fail or downgrade per policy.
- Reports must label evidence sources explicitly (`hooks`, `traces`, `network_proxy`, `framework_events`) and avoid causal over-claims when only proxy evidence exists.
- Missing `harness_manifest.json` for `cli_events` / `otel` / `sdk_*` is a hard fail by default.
- Optional runner flag `--allow-missing-harness-manifest` can downgrade effective integration to `cli_basic` and label grades.

### Open Questions
- None for Phase 1 (resolved: manifest required, control_ack required, step-end timing rule).

---

## Phase 2 — Recording + Checkpoints: Evidence, Replay, and Fork

### Goals
- Record framework events and ingest harness evidence (hooks/traces).
- Support checkpoints and fork/replay semantics derived from integration level.
- Provide optional SDK paths for `sdk_control` / `sdk_full` integrations.

### Deliverables
- **Framework Event Capture**
  - Framework `events.jsonl` with hashchain and artifact payloads.
  - Evidence sources labeled: framework events, hooks, traces.
- **Checkpointing**
  - Every N steps + end; snapshots of workspace/state/memory/RNG/budgets.
- **Fork / Replay Semantics**
  - `cli_basic`: re‑exec only, no step‑accurate replay.
  - `cli_events` / `otel`: re‑exec prefix + step boundary replay (best effort).
  - `sdk_full`: strict replay with fail‑closed on missing boundaries.
- **Optional SDK Integration**
  - `sdk_control`: pause/stop/checkpoint callbacks.
  - `sdk_full`: framework‑wrapped causal boundaries for highest fidelity.

### Interfaces
- `Recorder.record(event, payload_bytes) -> event_line`
- `Replayer.replay(event_selector) -> payload_bytes`
- `CheckpointManager.save(label, surfaces, runtime_state) -> checkpoint_ref`
- `CheckpointManager.restore(checkpoint_ref) -> runtime_state`
- `IntegrationLevelResolver.derive(evidence) -> integration_level`

### Acceptance Criteria
- Run cannot start without resolved spec digest.
- Replay grade is derived from observed integration level and evidence.
- Fork at step uses nearest checkpoint + re‑exec prefix when full replay is not possible.

### Open Questions
- Memory snapshot format (raw serialization vs event‑reconstruction).
- Workspace diff capture mechanism (rsync/patch vs fs‑level diff).

### Precision Substeps (Phase 2 Implementation)
1. Implement an append‑only event recorder that writes canonical JSONL with `hashchain.prev/self` computed deterministically.
2. Store payloads via the content‑addressed artifact store; record `payload_ref` in each event.
3. Write an `events.head` file with the final hashchain head.
4. Implement an event indexer that loads `events.jsonl` and supports lookup by `seq`.
5. Implement a replayer that can return event payload bytes from artifact refs.
6. Implement checkpoint save: tar/gzip each surface to a temp file, store via artifact store, write `checkpoint_<label>.json`.
7. Implement checkpoint restore: fetch tar via artifact ref, safe‑extract into surface path, guard against path traversal.
8. Implement integration‑level derivation from evidence (hooks/traces/sdk) and cap by manifest level.
9. Map effective integration level to replay grades (`strict`, `checkpointed`, `best_effort`, `none`).
10. Add validation that hook `seq` is monotonic and `control_ack` sequencing is respected.
11. Add tests that cover hashchain linking, checkpoint round‑trip, and integration‑level mapping.

## Runner Validation Rules (Cross‑Event, Non‑Schema)
- `seq` must be monotonically increasing per trial in `harness_events.jsonl`; violations fail (strict) or invalidate hook stream (non‑strict).
- `control_ack` must follow each `agent_step_end` (same `step_index`), and must appear before the next `agent_step_start`.
- If control action is `stop`, there must be no subsequent `agent_step_start`.
- If step events exist, all causal events (`model_call_end`, `tool_call_end`, `error`) must include `step_index`; missing values degrade interpretability grades.
- Optional header event (if present) must exactly match `harness_manifest.json`.

---

## Phase 3 — Analysis + Interpretability

### Goals
- Execute analysis_plan: missingness, effect sizes, CI, multiple comparisons.
- Generate interpretability bundles (diffs, exemplars, suspects) using hooks/traces/framework events.

### Deliverables
- **Analysis Engine**
  - Paired/unpaired handling; missingness policy enforcement.
  - Bootstrap CI for primary/secondary metrics.
  - Multiple comparisons correction (Holm/BH).
- **Interpretability**
  - Task‑level paired diffs table (Parquet/JSONL).
  - Exemplars: worst/best/uncertain tasks.
  - Suspects report (confounds + behavior correlations) with evidence sources.
  - Step‑level divergence when step events are present; otherwise fall back to turn‑based diagnostics.

### Interfaces
- `AnalysisRunner.run(resolved_experiment, trials, metrics) -> analysis/`
- `DiffBuilder.build(task_id, trial_a, trial_b) -> evidence‑based diffs (hooks/traces/framework events; optional tool/memory/workspace)`

### Acceptance Criteria
- Missingness policy applied consistently and recorded.
- Comparability grade computed from design + missingness + retries.
- Suspects report includes confound + behavior suspects and declares evidence source coverage.

### Open Questions
- Metric definitions source (per task vs per evaluator).
- Storage format for diffs (JSON vs parquet for large traces).

### Precision Substeps (Phase 3 Implementation)
1. Load `resolved_experiment.json` and parse `analysis_plan` into a normalized internal model.
2. Load per‑trial results from `trials/<trial_id>/metrics.json` (fallback: `trial_output.json`).
3. Build paired sets by `(task_id, repl_idx)` across baseline and each variant.
4. For each metric, apply missingness policy (`paired_drop`, `paired_impute`, `treat_as_failure`).
5. Compute effect sizes (risk_diff, median_diff, mean_diff) and paired bootstrap CIs.
6. Compute p‑values from bootstrap distributions and apply multiple‑comparison correction (Holm or BH).
7. Emit run‑level summaries: `analysis/summary.json`, `analysis/comparisons.json`.
8. Emit task‑level paired diffs (JSONL, plus Parquet when available).
9. Select exemplars (worst, best, highest uncertainty) from primary metric deltas.
10. Emit suspects report with evidence sources and conservative placeholders when evidence is missing.

---

## Phase 4 — CLI + Reporting

### Goals
- Provide DX surface: run, validate, replay, fork, compare, doctor.
- Emit human‑readable report with grades and interpretability links.

### Deliverables
- **CLI**
  - `lab run <experiment.yaml>`
  - `lab validate <experiment.yaml>`
  - `lab replay <trial_id> [--strict]`
  - `lab fork --from <trial_id> --at <step_or_event_selector> --set <binding>=<value>`
  - `lab compare <run_id> --baseline base --variant drop_10p`
  - `lab doctor`
- **Report**
  - Minimal static HTML (grades, integration level, primary metric CI/effect size, missingness, suspects, exemplars).
  - Evidence source summary (hooks/traces/framework events).

### Acceptance Criteria
- CLI prints run_id, grades, report location, invalidating warnings.
- `compare` outputs paired diffs and suspects bundle.
- `doctor` validates container runtime + schema.

### Open Questions
- Whether to embed a trace viewer or link to JSON/Parquet artifacts.

### Precision Substeps (Phase 4 Implementation)
1. Implement `lab` CLI entrypoint with subcommands.
2. Add `schema-validate` to validate JSON against registry schemas.
3. Add `hooks-validate` to validate harness events against `harness_manifest.json`.
4. Add `analyze` and `compare` commands that call analysis and report builders.
5. Implement minimal static HTML report generator reading `analysis/*.json`.
6. Implement `doctor` command to validate environment and schema availability.
7. Stub `run`, `validate`, `replay`, `fork` until runner/container orchestration exists.

---

## Phase 5 — Provenance + SBOM + Publishing (Optional v0.3 extras)

### Goals
- Attestation completeness; optional SBOM capture; debug bundles.

### Deliverables
- **Attestation**
  - Resolved spec digest, image digest, events hashchain heads, SBOM refs.
  - Harness identity:
    - `harness.source_digest` (git commit + dirty + diff_digest or directory hash).
    - `harness.exec_digest` (image digest or dist/binary hash).
    - `harness.entry_command` (array).
    - `harness.runtime_fingerprint` (e.g., node/python version).
    - `harness.lockfile_digest` when present.
  - Hook schema version and trace ingestion mode.
  - Redaction rules + summary digest.
- **SBOM**
  - SPDX capture; non‑blocking if tool missing unless required.
- **Debug Bundle**
  - Zip of key artifacts for audit.

### Acceptance Criteria
- Attestation fully references run artifacts.
- SBOM artifacts referenced when enabled.

### Open Questions
- Preferred SBOM tool and invocation.

### Precision Substeps (Phase 5 Implementation)
1. Read `resolved_experiment.digest` and include it in `attestation.json`.
2. Collect hashchain heads from `trials/*/events.head`.
3. Compute a best‑effort artifact store root digest.
4. Include grades summary and harness identity in attestation when present.
5. Record hooks schema version and trace ingestion mode.
6. If `sbom/image.spdx.json` exists, store it via artifact store and reference it.
7. Emit `attestation.json` at run root.
8. Build a minimal debug bundle zip with key artifacts.

---

## Cross‑Cutting Test Plan (initial)
- Unit tests for hashing, redaction, event envelope validation.
- Integration tests for hook schema validation, control‑plane actions, trace ingestion.
- Integration tests for record/replay, checkpoint restore, strict replay failure paths.
- End‑to‑end test: small paired experiment with two variant-plan entries; verify grades + analysis outputs.
- Negative tests: allowlist_enforced requested without enforcement -> fail or leaky grade.
- Negative tests: hooks header mismatch with manifest -> fail; missing control_ack -> fail or downgrade.

## Hand‑Off Notes (Implementation Status)
- Phase 0 implemented:
  - Core utilities: canonical JSON, SHA256 helpers, hash chain, artifact store.
  - Schema files materialized under `schemas/` (manifest, attestation, grades, event envelope, harness manifest, hook events, trial input/output, trace manifest, state inventory, resolved experiment).
  - Basic tests: `tests/test_core.py`.
- Phase 1 implemented:
  - Schema registry + Draft 2020‑12 validation: `src/agentlab_runner/schemas.py`.
  - Harness manifest loader/validator: `src/agentlab_runner/harness_manifest.py`.
  - Harness CLI executor: `src/agentlab_runner/harness_executor.py` (writes `trial_input.json`, validates `trial_output.json`).
  - Hook collector + validator: `src/agentlab_runner/hook_collector.py` (seq monotonicity, step contract, control_ack, optional header match).
  - Control‑plane writer (sha256 of file contents): `src/agentlab_runner/control_plane.py`.
  - Trace manifest ingestion stub: `src/agentlab_runner/trace_ingest.py` (OTLP not implemented yet).
  - Tests: `tests/test_hook_collector.py`, `tests/test_control_plane.py`.
- Phase 2 implemented:
  - Event recorder with hashchain + payload refs: `src/agentlab_runner/event_recorder.py`.
  - Event replayer and seq index: `src/agentlab_runner/replay_engine.py`.
  - Checkpoint save/restore (tar surfaces, safe extract): `src/agentlab_runner/checkpoints.py`.
  - Integration‑level derivation + replay grade mapping: `src/agentlab_runner/integration.py`.
  - Tests: `tests/test_event_recorder.py`, `tests/test_checkpoints.py`, `tests/test_integration.py`.
- Phase 3 implemented:
  - Analysis plan parsing + defaulting: `src/agentlab_analysis/analysis_plan.py`.
  - Missingness policies (paired_drop / paired_impute / treat_as_failure): `src/agentlab_analysis/missingness.py`.
  - Effect sizes + paired bootstrap CI/p-values: `src/agentlab_analysis/effects.py`.
  - Multiple comparisons correction (Holm/BH): `src/agentlab_analysis/multiple_comparisons.py`.
  - Interpretability outputs (paired diffs JSONL/parquet when available, exemplars, suspects): `src/agentlab_analysis/interpretability.py`.
  - End‑to‑end analysis runner: `src/agentlab_analysis/analysis_runner.py`.
  - Tests: `tests/test_analysis.py`.
- Phase 4 implemented:
  - CLI entrypoint with subcommands: `src/agentlab_cli/cli.py` and `src/agentlab_cli/__main__.py`.
  - Schema validation (`schema-validate`) and hooks validation (`hooks-validate`).
  - `analyze` and `compare` commands wired to analysis + report builder.
  - `run`, `validate`, `replay`, `fork` implemented with a local CLI harness runner (best effort).
  - Minimal experiment resolver and dataset loader powering CLI `run` / `validate`:
    - `src/agentlab_runner/experiment_resolver.py`
    - `src/agentlab_runner/dataset_loader.py`
  - Local run engine: `src/agentlab_runner/run_engine.py`.
  - Minimal HTML report builder: `src/agentlab_report/report_builder.py`.
  - Tests: `tests/test_report.py`.
- Phase 5 implemented (best effort):
  - Attestation writer: `src/agentlab_runner/provenance.py`.
  - SBOM capture stub (stores existing `sbom/image.spdx.json` if present).
  - Debug bundle builder: `src/agentlab_runner/debug_bundle.py`.
- Not yet implemented:
  - OTLP receiver / tracing ingest path (only manifest ingestion stub exists).
  - Container runner, network enforcement, preflight egress self‑test, and profile enforcement.
  - Full trial planner / scheduler / profile‑aware spec resolver (only minimal YAML resolver exists).
  - Full containerized `run` / `replay` / `fork` semantics (current implementation is local best‑effort).

## Appendix A — JSON Schemas (Draft 2020‑12)

**harness_manifest_v1.jsonschema**
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://agent-lab.local/schemas/harness_manifest_v1.json",
  "title": "Harness Manifest v1",
  "type": "object",
  "additionalProperties": false,
  "required": ["schema_version", "created_at", "integration_level", "step"],
  "properties": {
    "schema_version": { "const": "harness_manifest_v1" },
    "created_at": { "type": "string", "format": "date-time" },
    "integration_level": {
      "type": "string",
      "enum": ["cli_basic", "cli_events", "otel", "sdk_control", "sdk_full"]
    },
    "harness": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name"],
      "properties": {
        "name": { "type": "string", "minLength": 1 },
        "version": { "type": "string", "minLength": 1 },
        "source_digest": { "$ref": "#/$defs/sha256_digest" },
        "exec_digest": { "$ref": "#/$defs/sha256_digest" },
        "entry_command": {
          "type": "array",
          "items": { "type": "string", "minLength": 1 },
          "minItems": 1
        },
        "runtime": {
          "type": "object",
          "additionalProperties": true,
          "properties": {
            "node": { "type": "string" }
          }
        },
        "lockfile_digest": { "$ref": "#/$defs/sha256_digest" }
      }
    },
    "step": {
      "type": "object",
      "additionalProperties": false,
      "required": ["semantics"],
      "properties": {
        "semantics": {
          "type": "string",
          "minLength": 1,
          "description": "Human-readable description of what an 'agent step' means."
        }
      }
    },
    "hooks": {
      "type": "object",
      "additionalProperties": false,
      "required": ["schema_version", "events_path"],
      "properties": {
        "schema_version": { "const": "hook_events_v1" },
        "events_path": { "type": "string", "minLength": 1 },
        "header_event_emitted": { "type": "boolean", "default": false }
      }
    },
    "control_plane": {
      "type": "object",
      "additionalProperties": false,
      "required": ["mode", "path"],
      "properties": {
        "mode": { "type": "string", "enum": ["file", "sdk"] },
        "path": { "type": "string", "minLength": 1 },
        "format_version": { "type": "string", "default": "control_plane_v1" }
      }
    },
    "tracing": {
      "type": "object",
      "additionalProperties": false,
      "required": ["mode"],
      "properties": {
        "mode": { "type": "string", "enum": ["none", "otlp", "manifest"] },
        "otlp_endpoint": { "type": "string", "minLength": 1 },
        "trace_manifest_path": { "type": "string", "minLength": 1 }
      }
    },
    "redaction": {
      "type": "object",
      "additionalProperties": false,
      "required": ["mode"],
      "properties": {
        "mode": { "type": "string", "enum": ["on", "off"] },
        "applied_by_harness": { "type": "boolean", "default": false },
        "content_policy": { "type": "string", "enum": ["store", "hash", "drop"] }
      }
    },
    "ext": {
      "type": "object",
      "additionalProperties": true
    }
  },
  "allOf": [
    {
      "if": {
        "properties": {
          "integration_level": { "enum": ["cli_events", "sdk_control", "sdk_full"] }
        },
        "required": ["integration_level"]
      },
      "then": { "required": ["hooks"] }
    },
    {
      "if": {
        "properties": { "integration_level": { "const": "otel" } },
        "required": ["integration_level"]
      },
      "then": { "required": ["tracing"] }
    }
  ],
  "$defs": {
    "sha256_digest": {
      "type": "string",
      "pattern": "^sha256:[0-9a-f]{64}$"
    }
  }
}
```

**hook_events_v1.jsonschema** (per JSONL line)
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://agent-lab.local/schemas/hook_events_v1.json",
  "title": "Hook Events v1 (per-line schema)",
  "type": "object",
  "oneOf": [
    { "$ref": "#/$defs/agent_step_start" },
    { "$ref": "#/$defs/agent_step_end" },
    { "$ref": "#/$defs/control_ack" },
    { "$ref": "#/$defs/model_call_end" },
    { "$ref": "#/$defs/tool_call_end" },
    { "$ref": "#/$defs/error_event" }
  ],
  "$defs": {
    "base_event": {
      "type": "object",
      "additionalProperties": false,
      "required": ["event_type", "ts", "seq", "ids"],
      "properties": {
        "hooks_schema_version": { "const": "hook_events_v1" },
        "event_type": { "type": "string", "minLength": 1 },
        "ts": { "type": "string", "format": "date-time" },
        "seq": { "type": "integer", "minimum": 0 },
        "ids": {
          "type": "object",
          "additionalProperties": false,
          "required": ["run_id", "trial_id", "variant_id", "task_id", "repl_idx"],
          "properties": {
            "run_id": { "type": "string", "minLength": 1 },
            "trial_id": { "type": "string", "minLength": 1 },
            "variant_id": { "type": "string", "minLength": 1 },
            "task_id": { "type": "string", "minLength": 1 },
            "repl_idx": { "type": "integer", "minimum": 0 }
          }
        },
        "step_index": {
          "type": ["integer", "null"],
          "minimum": 0,
          "description": "If steps are supported, associate this event to a step."
        },
        "payload_ref": {
          "type": "string",
          "pattern": "^artifact://sha256/[0-9a-f]{64}$"
        },
        "redaction": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "applied": { "type": "boolean" },
            "mode": { "type": "string", "enum": ["store", "hash", "drop"] }
          }
        },
        "ext": {
          "type": "object",
          "additionalProperties": true
        }
      }
    },
    "agent_step_start": {
      "allOf": [
        { "$ref": "#/$defs/base_event" },
        {
          "type": "object",
          "required": ["event_type", "step_index"],
          "properties": {
            "event_type": { "const": "agent_step_start" },
            "step_index": { "type": "integer", "minimum": 0 }
          }
        }
      ]
    },
    "agent_step_end": {
      "allOf": [
        { "$ref": "#/$defs/base_event" },
        {
          "type": "object",
          "required": ["event_type", "step_index"],
          "properties": {
            "event_type": { "const": "agent_step_end" },
            "step_index": { "type": "integer", "minimum": 0 },
            "budgets": {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "steps": { "type": "integer", "minimum": 0 },
                "tokens_in": { "type": "integer", "minimum": 0 },
                "tokens_out": { "type": "integer", "minimum": 0 },
                "tool_calls": { "type": "integer", "minimum": 0 }
              }
            }
          }
        }
      ]
    },
    "control_ack": {
      "allOf": [
        { "$ref": "#/$defs/base_event" },
        {
          "type": "object",
          "required": ["event_type", "step_index", "control_version", "action_observed"],
          "properties": {
            "event_type": { "const": "control_ack" },
            "step_index": { "type": "integer", "minimum": 0 },
            "control_version": {
              "type": "string",
              "pattern": "^sha256:[0-9a-f]{64}$"
            },
            "control_seq": { "type": "integer", "minimum": 0 },
            "action_observed": {
              "type": "string",
              "enum": ["continue", "stop", "checkpoint"]
            },
            "action_taken": {
              "type": "string",
              "enum": ["continue", "stop", "checkpoint"]
            },
            "reason": { "type": "string" }
          }
        }
      ]
    },
    "model_call_end": {
      "allOf": [
        { "$ref": "#/$defs/base_event" },
        {
          "type": "object",
          "required": ["event_type", "call_id", "outcome"],
          "properties": {
            "event_type": { "const": "model_call_end" },
            "call_id": { "type": "string", "minLength": 1 },
            "turn_index": { "type": "integer", "minimum": 0 },
            "model": {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "identity": { "type": "string", "minLength": 1 },
                "params_digest": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}$" }
              }
            },
            "usage": {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "tokens_in": { "type": "integer", "minimum": 0 },
                "tokens_out": { "type": "integer", "minimum": 0 }
              }
            },
            "timing": {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "queue_wait_ms": { "type": "integer", "minimum": 0 },
                "duration_ms": { "type": "integer", "minimum": 0 }
              }
            },
            "attempt_index": { "type": "integer", "minimum": 0 },
            "outcome": {
              "type": "object",
              "additionalProperties": false,
              "required": ["status"],
              "properties": {
                "status": { "type": "string", "enum": ["ok", "error"] },
                "error_type": { "type": "string" },
                "message": { "type": "string" }
              }
            }
          }
        }
      ]
    },
    "tool_call_end": {
      "allOf": [
        { "$ref": "#/$defs/base_event" },
        {
          "type": "object",
          "required": ["event_type", "call_id", "tool", "outcome"],
          "properties": {
            "event_type": { "const": "tool_call_end" },
            "call_id": { "type": "string", "minLength": 1 },
            "tool": {
              "type": "object",
              "additionalProperties": false,
              "required": ["name"],
              "properties": {
                "name": { "type": "string", "minLength": 1 },
                "version": { "type": "string" }
              }
            },
            "timing": {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "queue_wait_ms": { "type": "integer", "minimum": 0 },
                "duration_ms": { "type": "integer", "minimum": 0 }
              }
            },
            "attempt_index": { "type": "integer", "minimum": 0 },
            "outcome": {
              "type": "object",
              "additionalProperties": false,
              "required": ["status"],
              "properties": {
                "status": { "type": "string", "enum": ["ok", "error"] },
                "error_type": { "type": "string" },
                "message": { "type": "string" }
              }
            }
          }
        }
      ]
    },
    "error_event": {
      "allOf": [
        { "$ref": "#/$defs/base_event" },
        {
          "type": "object",
          "required": ["event_type", "message"],
          "properties": {
            "event_type": { "const": "error" },
            "error_type": { "type": "string" },
            "message": { "type": "string", "minLength": 1 },
            "stack": { "type": "string" }
          }
        }
      ]
    }
  }
}
```

**trial_input_v1.jsonschema**
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://agent-lab.local/schemas/trial_input_v1.json",
  "title": "Trial Input v1",
  "type": "object",
  "additionalProperties": false,
  "required": ["schema_version", "ids", "task", "bindings", "runtime", "design"],
  "properties": {
    "schema_version": { "const": "trial_input_v1" },
    "ids": {
      "type": "object",
      "additionalProperties": false,
      "required": ["run_id", "trial_id", "variant_id", "task_id", "repl_idx"],
      "properties": {
        "run_id": { "type": "string", "minLength": 1 },
        "trial_id": { "type": "string", "minLength": 1 },
        "variant_id": { "type": "string", "minLength": 1 },
        "task_id": { "type": "string", "minLength": 1 },
        "repl_idx": { "type": "integer", "minimum": 0 }
      }
    },
    "task": {
      "type": "object",
      "additionalProperties": true,
      "description": "Benchmark task payload. Schema is benchmark-specific."
    },
    "task_schema": { "type": "string" },
    "bindings": {
      "type": "object",
      "additionalProperties": true,
      "description": "Variant bindings/primitives; interpreted by the harness."
    },
    "design": {
      "type": "object",
      "additionalProperties": false,
      "required": ["sanitization_profile", "integration_level"],
      "properties": {
        "sanitization_profile": {
          "type": "string",
          "enum": ["replay_strict_v2", "hermetic_functional_v2", "perf_benchmark_v2"]
        },
        "integration_level": {
          "type": "string",
          "enum": ["cli_basic", "cli_events", "otel", "sdk_control", "sdk_full"]
        }
      }
    },
    "runtime": {
      "type": "object",
      "additionalProperties": false,
      "required": ["paths", "network", "control_plane"],
      "properties": {
        "paths": {
          "type": "object",
          "additionalProperties": false,
          "required": ["workspace", "state", "dataset", "out", "tmp"],
          "properties": {
            "workspace": { "type": "string" },
            "state": { "type": "string" },
            "cache": { "type": "string" },
            "dataset": { "type": "string" },
            "out": { "type": "string" },
            "tmp": { "type": "string" }
          }
        },
        "network": {
          "type": "object",
          "additionalProperties": false,
          "required": ["mode_requested"],
          "properties": {
            "mode_requested": {
              "type": "string",
              "enum": ["none", "full", "allowlist_enforced"]
            },
            "allowed_hosts": {
              "type": "array",
              "items": { "type": "string", "minLength": 1 }
            }
          }
        },
        "control_plane": {
          "type": "object",
          "additionalProperties": false,
          "required": ["mode", "path"],
          "properties": {
            "mode": { "type": "string", "enum": ["file", "sdk"] },
            "path": { "type": "string", "minLength": 1 }
          }
        },
        "budgets": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "max_steps": { "type": "integer", "minimum": 0 },
            "max_total_tokens": { "type": "integer", "minimum": 0 },
            "max_tool_calls": { "type": "integer", "minimum": 0 }
          }
        },
        "timeouts": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "trial_seconds": { "type": "integer", "minimum": 1 },
            "tool_seconds": { "type": "integer", "minimum": 1 }
          }
        }
      }
    },
    "ext": { "type": "object", "additionalProperties": true }
  }
}
```

**trial_output_v1.jsonschema**
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://agent-lab.local/schemas/trial_output_v1.json",
  "title": "Trial Output v1",
  "type": "object",
  "additionalProperties": false,
  "required": ["schema_version", "ids", "outcome"],
  "properties": {
    "schema_version": { "const": "trial_output_v1" },
    "ids": {
      "type": "object",
      "additionalProperties": false,
      "required": ["run_id", "trial_id", "variant_id", "task_id", "repl_idx"],
      "properties": {
        "run_id": { "type": "string", "minLength": 1 },
        "trial_id": { "type": "string", "minLength": 1 },
        "variant_id": { "type": "string", "minLength": 1 },
        "task_id": { "type": "string", "minLength": 1 },
        "repl_idx": { "type": "integer", "minimum": 0 }
      }
    },
    "outcome": { "type": "string", "enum": ["success", "failure", "missing", "error"] },
    "answer": {
      "description": "Optional raw harness answer; evaluator-specific.",
      "oneOf": [
        { "type": "string" },
        { "type": "object", "additionalProperties": true },
        { "type": "array" }
      ]
    },
    "metrics": {
      "type": "object",
      "additionalProperties": {
        "oneOf": [
          { "type": "number" },
          { "type": "integer" },
          { "type": "string" },
          { "type": "boolean" },
          { "type": "null" }
        ]
      }
    },
    "artifacts": {
      "type": "array",
      "items": { "$ref": "#/$defs/artifact_decl" }
    },
    "error": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "error_type": { "type": "string" },
        "message": { "type": "string" },
        "stack": { "type": "string" }
      }
    },
    "ext": { "type": "object", "additionalProperties": true }
  },
  "$defs": {
    "artifact_decl": {
      "type": "object",
      "additionalProperties": false,
      "required": ["path"],
      "properties": {
        "path": { "type": "string", "minLength": 1 },
        "logical_name": { "type": "string" },
        "mime_type": { "type": "string" }
      }
    }
  }
}
```

**state_inventory_v1.jsonschema**
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://agent-lab.local/schemas/state_inventory_v1.json",
  "title": "State Inventory v1",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "schema_version",
    "sanitization_profile",
    "integration_level",
    "mounts",
    "network",
    "harness_identity",
    "violations"
  ],
  "properties": {
    "schema_version": { "const": "state_inventory_v1" },
    "sanitization_profile": {
      "type": "string",
      "enum": ["replay_strict_v2", "hermetic_functional_v2", "perf_benchmark_v2"]
    },
    "integration_level": {
      "type": "string",
      "enum": ["cli_basic", "cli_events", "otel", "sdk_control", "sdk_full"]
    },
    "mounts": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["name", "path", "writable"],
        "properties": {
          "name": { "type": "string" },
          "path": { "type": "string" },
          "writable": { "type": "boolean" }
        }
      }
    },
    "network": {
      "type": "object",
      "additionalProperties": false,
      "required": ["mode_requested", "mode_effective", "enforcement_effective", "egress_self_test"],
      "properties": {
        "mode_requested": { "type": "string", "enum": ["none", "full", "allowlist_enforced"] },
        "mode_effective": { "type": "string", "enum": ["none", "full", "allowlist_enforced"] },
        "allowed_hosts": {
          "type": "array",
          "items": { "type": "string", "minLength": 1 }
        },
        "enforcement_effective": {
          "type": "string",
          "enum": ["docker_none", "netns_iptables", "sidecar_proxy", "unknown"]
        },
        "egress_self_test": {
          "type": "object",
          "additionalProperties": false,
          "required": ["performed", "cases"],
          "properties": {
            "performed": { "type": "boolean" },
            "cases": {
              "type": "array",
              "items": {
                "type": "object",
                "additionalProperties": false,
                "required": ["target", "expected", "observed", "ok"],
                "properties": {
                  "target": { "type": "string", "minLength": 1 },
                  "expected": { "type": "string", "enum": ["allow", "block"] },
                  "observed": { "type": "string", "enum": ["allow", "block", "error", "unknown"] },
                  "ok": { "type": "boolean" },
                  "artifact_ref": {
                    "type": "string",
                    "pattern": "^artifact://sha256/[0-9a-f]{64}$"
                  }
                }
              }
            }
          }
        }
      }
    },
    "harness_identity": {
      "type": "object",
      "additionalProperties": false,
      "required": ["exec_digest"],
      "properties": {
        "name": { "type": "string" },
        "source_digest": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}$" },
        "exec_digest": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}$" },
        "entry_command": {
          "type": "array",
          "items": { "type": "string", "minLength": 1 },
          "minItems": 1
        }
      }
    },
    "violations": {
      "type": "object",
      "additionalProperties": false,
      "required": ["state_leak", "profile_invariant_violation"],
      "properties": {
        "state_leak": { "type": "boolean" },
        "profile_invariant_violation": { "type": "boolean" },
        "notes": { "type": "array", "items": { "type": "string" } }
      }
    },
    "ext": { "type": "object", "additionalProperties": true }
  }
}
```
