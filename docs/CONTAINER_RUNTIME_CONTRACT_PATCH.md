# Container Runtime Contract Patch

Status: Draft follow-on patch
Date: 2026-03-18

## Goal

Extract the runner/container runtime contract out of scattered implementation literals and turn it into one explicit internal contract.

This patch is intentionally not user-facing. It does not make container-internal paths configurable by experiment authors. It makes the fixed runtime ABI explicit, validated, and reusable.

## Why This Patch Exists

The current runtime mixes three different kinds of data:

1. Stable runner/container ABI that is probably meant to be fixed.
2. Image capability assumptions that are currently implicit.
3. Plan fields that look configurable but are ignored at execution time.

Today that creates two problems:

1. `TaskSandboxPlan.artifact_mount.container_artifact_dir` implies the artifact root is plan-driven, while execution hardcodes `/opt/agent`.
2. The runtime shells into containers with `/bin/sh`, `cp`, `mv`, `rm`, `find`, and sometimes `tar`, but those are not modeled as a capability contract or preflighted as such.

## Current Problems To Fix

### 1. Fixed paths are real ABI but are not declared as such

The runtime currently assumes fixed internal locations such as:

1. `/agentlab/in`
2. `/agentlab/out`
3. `/opt/agent`
4. `/opt/bench`
5. `/tmp`

These do not need to be user-facing. They do need to be centralized and treated as an explicit internal contract.

### 2. `TaskSandboxPlan` overstates configurability

`TaskSandboxPlan` currently carries artifact mount metadata, but the runtime does not consume it as the source of truth.

That creates drift risk:

1. build/persistence can serialize one contract
2. execution can silently use another
3. tests can validate the serialized object without validating the actual runtime behavior

### 3. POSIX tooling is an implicit requirement

The runtime currently depends on shell-based container mutation for:

1. workspace copy-in
2. hidden-asset stash/reveal
3. injected grader bundle materialization

That means the image must currently provide:

1. `/bin/sh`
2. `cp`
3. `mv`
4. `rm`
5. `find`
6. `tar` for injected archive extraction

This is a real contract today, but it is undocumented and only discovered at runtime by failure.

## Proposed Contract

Add one internal contract object that represents runner-owned container ABI and image capability requirements.

Suggested shape:

```rust
pub(crate) enum ContainerOsFamily {
    Linux,
}

pub(crate) struct ShellToolingContract {
    pub(crate) shell_path: &'static str,
    pub(crate) required_tools: &'static [&'static str],
    pub(crate) archive_tools: &'static [&'static str],
}

pub(crate) struct FixedContainerPaths {
    pub(crate) contract_in_dir: &'static str,
    pub(crate) contract_out_dir: &'static str,
    pub(crate) artifact_root_dir: &'static str,
    pub(crate) benchmark_support_dir: &'static str,
    pub(crate) temp_dir: &'static str,
    pub(crate) workspace_materialize_src_dir: &'static str,
    pub(crate) injected_bundle_src_dir: &'static str,
}

pub(crate) struct ContainerRuntimeContract {
    pub(crate) os_family: ContainerOsFamily,
    pub(crate) fixed_paths: FixedContainerPaths,
    pub(crate) shell_tooling: Option<ShellToolingContract>,
}
```

Initial default:

1. OS family is `Linux`
2. artifact root is fixed at `/opt/agent`
3. benchmark support dir is fixed at `/opt/bench`
4. shell tooling is required on the current path

## Design Rules

### Fixed internal paths stay fixed

This patch does not expose `/opt/agent` or `/opt/bench` to users.

Those paths should become explicit internal ABI, not experiment authoring input.

### Plans must stop pretending fixed ABI is variable

If the artifact root is always `/opt/agent`, then `TaskSandboxPlan.artifact_mount.container_artifact_dir` should not exist as mutable-looking plan data.

Choose one:

1. Remove the field and derive the path from the runtime contract.
2. Keep the field, but execution must consume it directly and it must equal the contract constant in preflight.

The preferred option is to remove the fake configurability.

### Shell requirements must be modeled as capabilities

If the runtime continues to shell into containers, shell tooling must be explicit in the contract and validated before the run starts.

If a later patch removes shell-based mutation, the tooling contract can become optional or strategy-specific.

## Proposed Code Changes

### 1. Add a shared runtime-contract module

Create one module that owns:

1. fixed internal container paths
2. OS family
3. shell tooling requirements
4. any strategy-specific image capability requirements

### 2. Replace production hardcoded path literals with contract constants

Move these into the runtime contract:

1. `/opt/agent`
2. `/opt/bench`
3. `/tmp`
4. `/agentlab/_materialize/workspace_src`
5. `/agentlab/_materialize/injected_bundle_src`

Contract paths already defined in `lab-core` such as `/agentlab/in` and `/agentlab/out` should remain centralized and be referenced through the same contract object rather than repeated literals.

### 3. Remove or rewire misleading `TaskSandboxPlan` fields

Required patch:

1. stop storing `container_artifact_dir` as mutable plan state if it is fixed ABI
2. make `build_container_spec(...)` consume the contract object instead of open-coded literals
3. ensure prepared manifests and runtime execution agree on one source of truth

### 4. Add runtime-contract preflight

Add one preflight check that validates image compatibility with the active runtime contract.

Minimum checks for the current Linux shell-based path:

1. shell path exists
2. required tools resolve on `PATH`
3. optional archive tools resolve when the grading strategy needs them

Suggested execution model:

1. create/start the target image
2. exec a contract probe
3. emit machine-readable failures with explicit missing capability names

Suggested failure examples:

1. `runtime_contract_missing_shell: /bin/sh`
2. `runtime_contract_missing_tool: find`
3. `runtime_contract_missing_archive_tool: tar`
4. `runtime_contract_os_family_unsupported: windows`

### 5. Make the shell-tooling contract strategy-aware

The contract should allow the runtime to say:

1. task execution requires no shell tooling
2. shell tooling is required only for workspace sync
3. archive tooling is required only for injected grading with archive bundles

That allows future work to shrink the contract rather than expanding it permanently.

### 6. Prepare for future non-Linux or non-POSIX support

This patch does not need to implement other OS families now.

It should avoid baking Linux assumptions deeper into public types by:

1. explicitly modeling `ContainerOsFamily`
2. isolating Linux shell-tooling requirements in one contract implementation
3. keeping preflight messages structured enough to branch by OS family later

## Acceptance Criteria

### Contract extraction

1. Non-test production code contains zero raw `/opt/agent` literals outside the runtime-contract owner.
2. Non-test production code contains zero raw `/opt/bench` literals outside the runtime-contract owner.
3. Non-test production code contains zero raw `/agentlab/_materialize/...` literals outside the runtime-contract owner.
4. Runtime code builds container specs and staging behavior from the contract owner instead of repeated open-coded literals.

### `TaskSandboxPlan` cleanup

1. `TaskSandboxPlan` no longer carries mutable-looking artifact-path data if the artifact root is fixed ABI.
2. Any remaining path-bearing plan fields are directly consumed by execution, not duplicated by separate literals.
3. `GradingSandboxPlan` is either persisted/consumed as a real runtime contract or removed as a placeholder contract type.

### Preflight

1. Preflight fails before trial execution when the image does not satisfy the active shell-tooling contract.
2. Failures are machine-readable and name the missing capability.
3. Preflight covers task images and any separate grader image used by the active grading strategy.

### Extensibility

1. The runtime contract explicitly models OS family or platform flavor.
2. Linux shell-tooling requirements are isolated to a Linux-specific contract implementation.
3. Adding a future non-POSIX or non-Linux contract does not require changing user-facing experiment authoring.

## Non-Goals

This patch does not:

1. expose internal container paths to experiment YAML
2. add Windows container support now
3. remove shell-based staging in the same patch

## Follow-On Patch After This One

Once the contract is extracted and preflighted, the next cleanup patch should remove shell-driven staging where possible:

1. replace workspace copy-in with Docker archive/copy primitives
2. replace injected bundle materialization with archive APIs or host-side preparation
3. reduce the shell-tooling contract to only the pieces that remain genuinely required
