# Phase 2 SDK Hard Cutover

## Goal

Hard cutover to the new SDK surface with zero legacy bloat.

## Primitives (Locked)

1. `Experiment`: authoring object only.
2. `BenchmarkRef`: `builtin("bench_v0")` now, extensible later.
3. `Agent`: artifact + command + env/config injection + binding projection.
4. `Variant`: `{ id, bindings }` for baseline and treatments.
5. `Overrides`: policy overrides only (`network`, `root_read_only`, `timeout_ms`).
6. `RuntimeMode` (Phase 2 optional): lifecycle/protocol extensions, opt-in.

## Authoring Shape (Single Source of Truth)

```ts
type Experiment = {
  experiment: { id: string; name?: string; description?: string; owner?: string; tags?: string[] };
  benchmark: "bench_v0";
  limit?: number;
  concurrency?: number;
  replications?: number;
  random_seed?: number;
  agent: {
    artifact: string;
    command: string | string[];
    io?: { input?: string; output?: string };
    env?: Record<string, string>;
    env_from_host?: string[];
    config_files?: string[];
    workspace_patches?: Record<string, string> | { source: string; target: string }[];
    bindings_to_args?: { binding: string; flag: string }[];
    mode?: {
      lifecycle?: "ephemeral" | "persistent";
      protocol?: "file" | "http";
      setup?: string | string[];
      port?: number;
      health_path?: string;
    };
  };
  baseline: { id: string; bindings: Record<string, unknown> };
  variants?: { id: string; bindings: Record<string, unknown> }[];
  overrides?: {
    network?: "none" | "full" | "allowlist_enforced";
    root_read_only?: boolean;
    timeout_ms?: number;
  };
};
```

## Public SDK Surface (Minimal)

1. `Experiment.create(input)` -> validated immutable model.
2. `Experiment.toYaml(exp)` / `Experiment.toJson(exp)` -> deterministic.
3. `Benchmark.builtin(name)` -> typed ref.
4. `Agent.fromArtifact(name, opts)` -> typed agent block.
5. `LabClient.prepare(...)` and `LabClient.run(...)` (explicit lifecycle boundary).
6. `LabClient.describe/preflight/...` stays.

## Delete Entirely (Hard Cutover)

1. `ExperimentBuilder` and all builder-fluent methods.
2. Legacy types exposing `dataset/design/runtime/variant_plan/benchmark.adapter/policy`.
3. Legacy README/examples/tests tied to `version: "0.5"` shape.

## Boundary Rules

1. SDK must never require or emit internal wiring fields (`dataset`, `design`, `runtime`, `metrics`, `variant_plan`, benchmark adapter internals).
2. Runner owns normalization/materialization from DX shape to runtime schema.
3. `prepare` side effects are explicit; `run` behavior must declare whether it requires prior prepare or performs it.
4. Advanced runtime modes are opt-in only; default remains Phase 1 file-IO ephemeral semantics.

## Invariants to Enforce

1. No-minimal-surface-regression: DX minimal fixture unchanged and valid.
2. Forbidden-field fence: SDK output cannot contain internal keys.
3. Deterministic serialization: stable canonical output for same input.
4. Side-effect clarity: `prepare` and `run` contracts verified separately.
5. Optionality parity: with no advanced mode flags, behavior matches Phase 1.
6. Advanced-mode isolation: enabling persistent/server changes only declared mode behavior.

## Testing Contract

1. Golden tests for canonical DX fixture output.
2. Property tests for command tokenization + `bindings_to_args` projection.
3. Contract tests for `prepare`/`run` command semantics.
4. Integration tests against real runner preflight/describe.
5. Evidence gate: Phase 2 strict validator must pass with all artifacts.
