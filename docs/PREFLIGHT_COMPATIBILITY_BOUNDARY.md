# Preflight Compatibility Boundary

Status: active policy note  
Owner: `lab-runner` / `lab-cli`  
Date: 2026-03-09

## Purpose

Define a practical boundary between:

1. build-time validation,
2. preflight hard gates,
3. runtime failures we still accept.

The goal is not to prove all agent behavior before execution. The goal is to prevent deterministic
compatibility failures from reaching live trials.

## Boundary

### Build stage

`lab build` owns host-independent validation and sealing.

It must reject:

1. malformed authoring inputs,
2. missing or forbidden host paths,
3. sealed-package integrity violations,
4. static contract mismatches knowable without the executor.

It does not own host/image realization. A package built on one machine may run on another machine
against a different set of task images.

### Preflight stage

`lab preflight` owns deterministic executor + image compatibility.

It must hard-fail when a fixed tuple of:

1. agent artifact digest,
2. runtime profile,
3. task image digest/tag,
4. executor host/runtime

cannot satisfy the minimal runner contract.

This includes:

1. Docker daemon/image availability,
2. launch reachability for the agent entrypoint in every required image,
3. known fatal bootstrap incompatibility diagnostics emitted during a successful probe,
4. benchmark wrapper/adapter reachability,
5. machine-state gates such as disk headroom and provider wiring.

### Runtime stage

Runtime is still allowed to fail for dynamic or semantic reasons:

1. provider outages,
2. auth or quota failures,
3. task-specific logic bugs,
4. long-running execution timeouts,
5. nondeterministic external service behavior.

These are not compatibility failures. They are execution failures.

## Compatibility Policy

If a failure is reproducible for a fixed artifact/image/runtime tuple, it should be caught before
trial dispatch.

In practice, this means preflight should reject:

1. ABI/runtime incompatibilities between the supplied agent binary and the task image,
2. missing runtime components that the packaged agent depends on,
3. deterministic bootstrap diagnostics that indicate the agent cannot run correctly even if the
   probe command exits zero.

Examples:

1. `CPU lacks AVX support`
2. `Agent 'coding' references tool 'Skill' which is not available`

Those are not acceptable live-trial surprises. They are hard preflight failures.

## What Preflight Does Not Promise

Preflight does not prove:

1. the agent will solve the task,
2. the full long-running task path is bug-free,
3. external providers will remain healthy during execution.

It only promises that the package can satisfy the minimal execution contract in the target runtime.

## Current Runner Enforcement

The runner now treats the following as blocking in `agent_runtime_reachable`:

1. the real adapter command failing to complete a bounded contract smoke run,
2. missing or malformed `result.json` during that smoke run,
3. known deterministic compatibility blockers emitted in harness stdout/stderr.

The benchmark preflight check now runs the real benchmark wrapper contract too. It must produce the
runner-owned prediction and score artifacts during smoke, not merely launch the grader command.

The benchmark wrapper also treats missing `result.json` as a contract failure owned by the runner,
not as a grader traceback.

## Test Intent

The reliability target is:

1. deterministic compatibility failures fail in preflight,
2. benchmark contract breakage is surfaced as runner-owned grade errors,
3. runtime remains reserved for genuinely dynamic failures.
