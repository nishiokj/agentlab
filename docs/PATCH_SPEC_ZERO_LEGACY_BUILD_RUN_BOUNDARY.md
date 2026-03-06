# Patch Spec: Zero-Legacy Build/Run Boundary Hard Cutover

Status: Draft (Hard Cut Required)  
Date: 2026-03-04  
Owner: `lab-runner`, `lab-cli`, `schemas`  
Priority: P0 Blocker  

## 1. Intent

This patch hard-cuts AgentLab into two typed, non-overlapping stages:

1. Build stage: consumes user authoring inputs and emits a sealed run package.
2. Run stage: consumes only a sealed package and executes it.

No runtime authoring normalization, no legacy fallback parsing, no compatibility aliases, and no user-controlled internal mount topology are permitted in run execution paths.

## 2. Non-Negotiable Invariants

1. `run`, `preflight`, and `describe` never parse authoring YAML/JSON experiment specs.
2. Runner internals are runner-owned; user/task/agent cannot set internal paths or mount layout.
3. Agent-visible filesystem contains only runner workspace plus explicitly staged contract paths.
4. Task image internals and agent artifact internals are never visible as readable source trees to the agent.
5. All contract boundaries are typed and schema-validated at build time; run consumes typed outputs only.
6. Any removed field or legacy alias is a hard error, never warning, never fallback.

## 3. North Star Horizon State

### 3.1 User Experience Contract

User flow is always:

1. `lab-cli build <authoring_spec> --out <package_dir>`
2. `lab-cli preflight <package_dir>`
3. `lab-cli describe <package_dir>`
4. `lab-cli run <package_dir>`

Optional convenience command:

1. `lab-cli build-run <authoring_spec>` is implemented as compose(`build`, `run`) and still enforces package boundary internally.

No command in the run plane accepts an authoring spec path.

### 3.2 Data Model Contract

There are exactly two top-level schemas:

1. `authoring_spec_v3` (build input only)
2. `sealed_run_package_v2` (run/preflight/describe input only)

Authoring schema never appears in run directories.  
Sealed package schema is immutable and content-addressed.

### 3.3 Build Output Contract

Build emits:

1. `manifest.json` with `schema_version=sealed_run_package_v2`
2. `resolved_experiment.json` (fully normalized runtime form)
3. `checksums.json` with per-file digests
4. `package.lock` containing package digest
5. `tasks/tasks.jsonl` in strict task boundary shape only
6. `files/*` and `agent_builds/*` staged artifacts

No unresolved host-relative paths remain in `resolved_experiment.json`.

### 3.4 Run Input Contract

Run plane accepts only:

1. package directory containing `manifest.json`
2. explicit path to package `manifest.json`

Anything else fails immediately with a typed error:

1. `run_input_invalid_kind`
2. `expected sealed package dir or manifest`

### 3.5 Isolation and Visibility Contract

Inside container execution:

1. Writable workspace: `/agentlab/workspace`
2. Contract IO/state: `/agentlab/in`, `/agentlab/out`, `/agentlab/state`
3. Staged dependencies: `/agentlab/deps`

Hard masked:

1. `/workspace` as tmpfs
2. `/opt/bench` as tmpfs

Hard rule:

1. benchmark grader script path must resolve under `/agentlab/deps` or `/agentlab/state`
2. all other absolute script paths are rejected

Agent sees only:

1. workspace contents materialized by runner
2. files written during its own execution
3. runner-staged dependency files required by contract

Agent does not see:

1. benchmark source trees
2. dataset authoring files
3. task generator internals
4. package source provenance paths

### 3.6 Path Ownership Contract

Build stage is the only stage that may resolve host paths.  
Run stage may only resolve package-internal paths or contract-root-relative container paths.

Run stage never interprets host paths from user/task data.

## 4. Hard Deletions (No Compatibility Layer)

The following behaviors are removed, not deprecated:

1. `load_experiment_input` YAML parse branch in run-plane call sites.
2. DX auto-normalization in run/preflight/describe.
3. Legacy non-DX experiment acceptance in run/preflight/describe.
4. Variant aliases (`variant_plan` vs `variants`, `variant_id` vs `id`) in run-plane.
5. Baseline aliases (`bindings` vs `config`) in run-plane.
6. Arg-map alias (`binding` key) acceptance.
7. Any adapter script path acceptance outside `/agentlab/deps` or `/agentlab/state`.
8. Any dataset path that escapes package root.

## 5. Build Stage Specification

### 5.1 Inputs

Build consumes:

1. authoring spec file
2. optional build overrides schema (`experiment_overrides_v2`)
3. project root

### 5.2 Build Validation

Build fails hard if any condition is violated:

1. unknown fields in `authoring_spec_v3`
2. removed fields from prior schemas
3. unresolved artifact references
4. host path outside project root
5. non-deterministic path references
6. task boundary rows not matching `task_boundary_v2_strict`
7. benchmark adapter command missing or invalid
8. adapter script path not under runner contract deps/state

### 5.3 Build Normalization

Build computes a canonical resolved form:

1. runtime command tokens
2. fully staged dependency table
3. sealed dataset path `tasks/tasks.jsonl`
4. sealed artifact paths under package root
5. sealed benchmark adapter path under `files/*` mapped to `/agentlab/deps/...`

### 5.4 Build Sealing

Build seals package by:

1. content digest over all package files except checksum/lock being produced
2. recording digests in `checksums.json`
3. recording top-level package digest in `package.lock`

Run stage verifies package seal before any execution work.

## 6. Run Stage Specification

### 6.1 Input Acceptance

Run, preflight, and describe accept only sealed package.

No authoring normalization function is linked into run-plane entrypoints.

### 6.2 Runtime Validation

Before task execution, runner validates:

1. manifest schema = `sealed_run_package_v2`
2. checksum integrity
3. package digest matches lock
4. dataset rows are strict `task_boundary_v2_strict`
5. adapter script path under `/agentlab/deps` or `/agentlab/state`

If any validation fails, run aborts with `preflight_failed` and zero trial dispatch.

### 6.3 Mount and Topology Control

Runtime mount map is generated by runner constants only.

No input field can override:

1. mount roots
2. contract root names
3. workspace mount target
4. masking strategy for `/workspace` and `/opt/bench`

## 7. Typed Boundary Schemas

### 7.1 `task_boundary_v2_strict`

Allowed keys exactly:

1. `schema_version`
2. `task`
3. `workspace_seed`
4. `workspace_files`
5. `mount_references`
6. `limits`

All unknown keys are hard errors.  
`task.workspace` is invalid and rejected.

### 7.2 `sealed_run_package_v2`

Top-level allowed keys exactly:

1. `schema_version`
2. `created_at`
3. `resolved_experiment`
4. `checksums_ref`
5. `package_digest`

### 7.3 `resolved_experiment_v2`

Run-plane fields are closed-world typed:

1. no alias fields
2. no null-as-presence ambiguity
3. no deprecated path keys

## 8. Threat Model and Anti-Gameability Controls

### 8.1 Threats Addressed

1. task attempts to expose image internals through path tricks
2. authoring tries to inject runtime mount topology
3. adapter path points at image internals or arbitrary absolute paths
4. stale build artifacts accidentally executed as if fresh
5. runtime resolves host-relative paths dynamically

### 8.2 Controls

1. strict schemas with unknown-field rejection
2. package-only run-plane input
3. package integrity check (checksums + digest)
4. explicit allowlist for adapter script contract roots
5. hard mask of image internals (`/workspace`, `/opt/bench`)
6. project-root boundary checks for build-time host sources

## 9. Code-Level Cutover Plan

### 9.1 CLI Changes

1. `lab-cli run` requires package dir or manifest only
2. `lab-cli preflight` requires package dir or manifest only
3. `lab-cli describe` requires package dir or manifest only
4. add `lab-cli build-run` convenience command to preserve one-shot UX without breaking boundary

### 9.2 Runner Changes

1. split loaders by stage and remove mixed-path loader usage
2. add `load_authoring_input_for_build(...)`
3. add `load_sealed_package_for_run(...)`
4. remove run-plane calls to authoring normalization
5. enforce adapter script path root allowlist (`/agentlab/deps`, `/agentlab/state`) in both preflight and runtime launch
6. enforce package-root-bounded dataset path resolution
7. remove alias parsing in run-plane structures

### 9.3 Schema Changes

1. add `sealed_run_package_v2.jsonschema`
2. add `resolved_experiment_v2.jsonschema`
3. tighten `task_boundary_v2` to strict closed-world schema
4. remove deprecated aliases from schemas

## 10. Deletion Checklist

Each item must be removed, not merely hidden:

1. YAML parse path from run-plane loaders
2. `normalize_experiment_authoring(...)` call in run/preflight/describe paths
3. legacy alias parsing (`binding`, `config`, mixed variant key aliases) in run-plane
4. permissive adapter script absolute-path acceptance except contract roots
5. any fallback branch that continues after schema mismatch

## 11. Acceptance Test Matrix (Must Pass)

### 11.1 Positive Tests

1. package build emits sealed package with valid digest
2. run from sealed package succeeds
3. preflight from sealed package succeeds
4. describe from sealed package succeeds

### 11.2 Negative Tests (Hard Errors)

1. `run <authoring.yaml>` fails
2. `preflight <authoring.yaml>` fails
3. `describe <authoring.yaml>` fails
4. task row with unknown key fails
5. task row with `task.workspace` fails
6. adapter script `/opt/bench/...` fails
7. adapter script `/abs/other/path.py` fails
8. dataset path escaping package root fails
9. package digest mismatch fails
10. checksum mismatch fails
11. alias field presence in run package fails

### 11.3 Mutation Tests

1. flip one byte in `tasks/tasks.jsonl` after build; run must fail before dispatch
2. flip one byte in staged adapter; run must fail before dispatch
3. alter `resolved_experiment.json`; run must fail before dispatch

## 12. Operational Gates

Cutover cannot ship unless:

1. all negative tests above are green
2. no code path in run/preflight/describe imports authoring normalization
3. static scan finds zero usage of removed alias fields in run-plane parser code
4. release notes list explicit hard-cut breaking changes

## 13. Rollout Strategy

This is a flagless hard cut:

1. merge behind one release boundary
2. remove old CLI UX paths in same release
3. require rebuild of experiment packages post-upgrade

No dual-mode operation is allowed.

## 14. Explicit Non-Guarantees

These are outside runner boundary guarantees:

1. kernel/container runtime zero-day escapes
2. host compromise by privileged Docker daemon outside runner controls
3. malicious model output content

These non-guarantees do not weaken build/run boundary guarantees within the runner contract.

## 15. Definition of Done

Done means all are true:

1. run-plane cannot parse or normalize authoring specs
2. build-plane is the sole path resolver
3. sealed package integrity is mandatory for execution
4. adapter/task path contracts are strict allowlist-based
5. legacy and fallback code paths are deleted
6. CI enforces negative tests as blocking gates

## 16. Immediate Next Implementation Batch

1. Introduce `load_sealed_package_for_run` and reroute run/preflight/describe.
2. Add CLI guardrails rejecting authoring spec paths in run-plane commands.
3. Tighten adapter script path validator from blacklist to allowlist.
4. Add package-root dataset path boundary check.
5. Remove alias parsers in run-plane (`binding`, `config`, mixed variant keys).
6. Add mutation/negative tests to CI.

## 17. Hole-Closure Matrix (No Gameability)

Each historical hole has a mandatory closure mechanism:

1. Hole: run-plane can parse YAML and normalize authoring on the fly.
   Closure: run-plane loader accepts package dir/manifest only; all other input kinds hard-fail.
2. Hole: mixed DX/legacy parsing allows accidental legacy execution.
   Closure: run-plane schema is closed-world `sealed_run_package_v2` only.
3. Hole: adapter script path blocked only by `/opt/bench` blacklist.
   Closure: adapter script path is allowlist-only under `/agentlab/deps` or `/agentlab/state`.
4. Hole: dataset path can be dynamically host-resolved in run.
   Closure: run-plane resolves dataset path only inside package root and rejects escapes.
5. Hole: alias fields (`binding`, `config`, `variant_id/id`) keep compatibility behavior alive.
   Closure: alias fields removed from run-plane schema and parser; any appearance is hard error.
6. Hole: stale package mutation after build can still execute.
   Closure: run verifies package digest and checksums before preflight/run dispatch.
7. Hole: users can force internal topology through spec fields.
   Closure: mount topology is generated from runner constants only; no user override fields exist.
8. Hole: task can leak image internals by relying on image filesystem layout.
   Closure: hard tmpfs masking of `/workspace` and `/opt/bench`, independent of user config.

## 18. Kill List Mapped to Current Code (Must Be Removed or Replaced)

The following current paths are identified and must be cut over:

1. YAML parse in mixed loader path: `rust/crates/lab-runner/src/lib.rs` `load_experiment_input(...)`.
2. Run-plane authoring normalization call from loader.
3. `run` command path using raw experiment authoring file without package boundary.
4. `preflight` command path using raw experiment authoring file without package boundary.
5. `describe` command path using raw experiment authoring file without package boundary.
6. Adapter script path validation that only rejects `/opt/bench/*`.
7. Run-plane alias parser acceptance in `arg_map` (`binding` key).
8. Run-plane variant/baseline alias acceptance paths.
9. Run-plane dataset resolver that does not enforce package-root containment.

Acceptance requires a code search that confirms no run-plane call graph reaches removed behavior.

## 19. Enforcement-by-Construction

Hard cut relies on both schema and code-level construction constraints:

1. CLI type split is explicit: build commands accept `AuthoringInputPath`, run-plane commands accept `SealedPackagePath`.
2. Rust type split is explicit: `AuthoringSpecV3` is not referenced by run modules; `SealedRunPackageV2` is required by run modules.
3. Module split is explicit: authoring normalization code is build-only; run modules import sealed package decoder only.
4. CI static checks are mandatory: fail build if run modules reference authoring parser symbols.
5. CI static checks are mandatory: fail build if run modules reference removed alias field names.

## 20. CI Release Gates (Blocking)

Release is blocked unless all gates pass:

1. Unit tests for sealed package loader reject YAML/legacy paths.
2. Integration tests verify run/preflight/describe fail on authoring input.
3. Mutation tests verify checksum/digest tampering blocks dispatch.
4. Contract tests verify adapter script allowlist semantics.
5. Static symbol scan verifies run modules do not reference authoring normalization.
6. Static string scan verifies removed alias keys are absent from run-plane parser logic.
7. Golden-path end-to-end test runs package-only workflow in CI.
