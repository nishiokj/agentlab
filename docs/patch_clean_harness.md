# Patch Spec: Clean Harness Boundary

North-star reference: `examples/clean_harness/`

This document maps every gap between the current runner implementation and the clean contract, organized by the change required.

---

## 1. Variant Model: `bindings` → `args` + `env`

### Current
`Variant` struct holds `bindings: Value` (arbitrary JSON object). Parsed from `/baseline/bindings` and `/variant_plan[*].bindings`. At trial time, bindings are serialized to `/agentlab/in/bindings.json` and the harness is expected to read and interpret them.

**Files:**
- `lab-runner/src/lib.rs:4118-4122` — `Variant { id, bindings, runtime_overrides }`
- `lab-runner/src/lib.rs:4130-4135` — parse `/baseline/bindings`
- `lab-runner/src/lib.rs:4182-4184` — parse `/variant_plan[*].bindings`
- `lab-runner/src/lib.rs:6437-6440` — extract bindings from trial input
- `lab-runner/src/lib.rs:6451` — write `bindings.json` to host

### Target
`Variant` holds `args: Vec<String>` and `env: BTreeMap<String, String>`. No `bindings` field. Variants declare what CLI args and env vars differ — the runner appends args to the command and sets env on the container. The harness never sees a "bindings" concept.

### Patch

**a) `Variant` struct** (`lib.rs:4118`)
```rust
struct Variant {
    id: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    image: Option<String>,        // replaces runtime_overrides for image swap
}
```

**b) `resolve_variant_plan()`** (`lib.rs:4124-4211`)
- Parse `args` as string array from `/baseline/args` and `/variant_plan[*].args` (default: empty)
- Parse `env` as string map from `/baseline/env` and `/variant_plan[*].env` (default: empty)
- Parse `image` as optional string from `/variant_plan[*].image` (already partially there at line 4196)
- Stop parsing `bindings` and `runtime_overrides`

**c) Command construction** (`lib.rs:6116-6134` — `resolve_runtime_agent_command`)
- After rendering the base command, append `variant.args` before the positional IO args
- Current: `{command} {io_input_arg} {io_output_arg}`
- Target: `{command} {variant_args...} {input_path} {output_path}`

**d) Container env injection** (`lib.rs:6080-6085`)
- Inject `variant.env` into container environment (alongside any user-specified `runtime.agent.env`)

**e) Image override**
- If `variant.image` is set, use it instead of `runtime.agent.image` — currently done via `runtime_overrides` deep merge, should become a direct field check

**f) Kill bindings.json materialization** (`lib.rs:6450-6453`)
- Stop writing `bindings.json`, `dependencies.json`, `policy.json` as separate files
- Only write `task.json`

---

## 2. IO Contract: `input_arg`/`output_arg` → Positional Args

### Current
`AgentRuntimeIoConfig` has `input_arg` and `output_arg` (default `--input`/`--output`). The runner uses `append_runtime_io_arg()` to expand templates like `--input={path}` or `--input` + path as separate token.

**Files:**
- `lab-runner/src/lib.rs:5041-5044` — `AgentRuntimeIoConfig { input_arg, output_arg }`
- `lab-runner/src/lib.rs:5284-5301` — parse from `/runtime/agent/io/input_arg` and `output_arg`
- `lab-runner/src/lib.rs:6100-6134` — `append_runtime_io_arg` + `resolve_runtime_agent_command`

### Target
Runner always appends input and output as the last two positional args. No `io` config in the experiment file.

### Patch

**a) Delete `AgentRuntimeIoConfig`** and the `io` field from `AgentRuntimeConfig` (`lib.rs:5041-5044`, `lib.rs:5051`)

**b) Simplify `resolve_runtime_agent_command()`** (`lib.rs:6116-6134`)
```rust
fn resolve_runtime_agent_command(request: &AdapterRunRequest) -> Result<Vec<String>> {
    let mut command = apply_agentlab_template_to_command(
        &request.runtime.command_raw, request.runtime_env
    );
    // Append variant args
    command.extend(request.variant_args.iter().cloned());
    // Positional: input then output
    command.push(request.io_paths.task_path.clone());
    command.push(request.io_paths.result_path.clone());
    Ok(command)
}
```

**c) Delete `append_runtime_io_arg()`** (`lib.rs:6100-6114`) — no longer needed

**d) Drop parsing of `/runtime/agent/io/*`** (`lib.rs:5284-5301`)

---

## 3. Mount Points: 7 → 2

### Current
Container gets 7+ mounts:

| Mount | Source |
|---|---|
| `/agentlab/in` (ro) | `trial_dir/in/` |
| `/agentlab/out` | `trial_dir/out/` |
| `/agentlab/state` | `trial_dir/state/` |
| `/agentlab/deps` | `trial_dir/deps/` |
| `/agentlab/workspace` | `trial_dir/workspace/` |
| `/dataset` (ro) | `trial_dir/dataset/` |
| `/tmp` (tmpfs) | — |
| dynamic mounts | dataset packs |

**Files:**
- `lab-runner/src/lib.rs:6027-6078` — mount construction in `run_builtin_adapter_container`
- `lab-core/src/lib.rs:8-14` — `AGENTLAB_CONTRACT_*` path constants

### Target
Two paths visible to the harness:
- `/in/task.json` (ro) — the task
- `/out/` (rw) — harness writes `result.json`

Runner may still use internal mounts for its own bookkeeping, but the harness contract is just these two.

### Patch

**a) New constants** (`lab-core/src/lib.rs`)
```rust
pub const HARNESS_IN_DIR: &str = "/in";
pub const HARNESS_OUT_DIR: &str = "/out";
pub const HARNESS_TASK_PATH: &str = "/in/task.json";
pub const HARNESS_RESULT_PATH: &str = "/out/result.json";
```

**b) Container mount construction** (`lib.rs:6027-6078`)
For clean-contract experiments, mount only:
```rust
cmd.args(["-v", &format!("{}:/in/task.json:ro", task_host_path.display())]);
cmd.args(["-v", &format!("{}:/out", out_dir.display())]);
```

The runner still creates internal directories on the host for evidence collection — it just doesn't mount them into the container.

**c) Trial path preparation** — the runner still writes `task.json` to `trial_dir/in/` on the host, then mounts that single file. Stop writing `bindings.json`, `dependencies.json`, `policy.json` into `trial_dir/in/`.

**d) Working directory** — currently `-w /agentlab/workspace`. For clean contract: either `/app` (from Dockerfile WORKDIR) or omit to let the image decide.

---

## 4. Environment Variables: 12+ `AGENTLAB_*` → 0

### Current
`build_runtime_contract_env()` injects 12+ env vars into every container:

```
AGENTLAB_TASK_PATH, AGENTLAB_BINDINGS_PATH, AGENTLAB_DEPENDENCIES_PATH,
AGENTLAB_POLICY_PATH, AGENTLAB_RESULT_PATH, AGENTLAB_TRAJECTORY_PATH,
AGENTLAB_RUN_ID, AGENTLAB_TRIAL_ID, AGENTLAB_VARIANT_ID,
AGENTLAB_TASK_ID, AGENTLAB_REPL_IDX, AGENTLAB_TIMEOUT_MS
```

**Files:**
- `lab-core/src/lib.rs:33-50` — constant definitions
- `lab-runner/src/lib.rs:5726-5774` — `build_runtime_contract_env()`
- `lab-runner/src/lib.rs:6083-6084` — injected into docker command

### Target
Zero `AGENTLAB_*` env vars. Only variant-specified `env` is set. The runner tracks trial IDs, paths, etc. internally.

### Patch

**a) Stop calling `build_runtime_contract_env()`** for clean-contract experiments, or make it return an empty map.

**b) Only inject `variant.env`** — the user-specified env overrides from the experiment config.

**c) Constants in `lab-core`** can remain (other runner internals may use them), but they stop being projected into the container.

---

## 5. Experiment Config: Required Fields

### Current — `validate_required_fields()` (`lib.rs:2243-2348`)
Requires:
- `/experiment/workload_type`
- `/design/sanitization_profile`
- `/design/replications`
- `/runtime/policy/timeout_ms`
- `/runtime/policy/network/mode`
- `/baseline/variant_id`
- `/runtime/agent` (object)
- `/runtime/agent/command` (non-empty)
- `/runtime/agent/image` (if sandbox mode = container)

### Target (from `experiment.yaml`)
Required:
- `/experiment/id`
- `/experiment/name`
- `/dataset/path`
- `/design/replications`
- `/baseline/variant_id`
- `/runtime/image`
- `/runtime/command`

Not required (runner defaults):
- `workload_type` — runner infers or uses a single default
- `sanitization_profile` — runner default
- `timeout_ms` — lives in `runtime.timeout_ms`, not nested under `policy`
- `network/mode` — lives in `runtime.network`, not nested

### Patch

**a) New validation** for version `"1.0"` experiments:
```rust
let required_v1: &[&str] = &[
    "/experiment/id",
    "/experiment/name",
    "/dataset/path",
    "/design/replications",
    "/baseline/variant_id",
    "/runtime/image",
    "/runtime/command",
];
```

**b) Schema migration path**: detect `version: "1.0"` and route to the new parser. Existing `0.3`–`0.5` configs continue through the current path until deprecated.

---

## 6. Experiment Config: Schema Flattening

### Current nested structure
```yaml
runtime:
  agent:
    command: [...]
    image: my-harness:latest
    integration_level: cli_events
    io:
      input_arg: --input
      output_arg: --output
  policy:
    timeout_ms: 300000
    network:
      mode: none
    sandbox:
      mode: container
      resources:
        cpu_count: 2
        memory_mb: 2048
  telemetry:
    trajectory_path: /agentlab/out/trajectory.jsonl
  dependencies: {}
```

### Target flat structure
```yaml
runtime:
  image: my-harness:latest
  command: [python, harness.py]
  timeout_ms: 300000
  network: none
  resources:
    cpus: 2
    memory_mb: 2048
```

### Patch

**a) Parse v1 runtime** — new parsing path for `version: "1.0"`:
- `/runtime/image` → `container_image`
- `/runtime/command` → `command_raw`
- `/runtime/timeout_ms` → `default_timeout_ms`
- `/runtime/network` → network mode string
- `/runtime/resources/cpus` → cpu resource limit
- `/runtime/resources/memory_mb` → memory limit

**b) Dropped fields** (not parsed, not accepted):
- `integration_level` — runner decides internally
- `io.input_arg` / `io.output_arg` — positional convention
- `telemetry.*` — runner internal
- `dependencies` — runner handles file staging without experiment config
- `policy.sandbox.*` (hardening, root_read_only) — runner defaults

---

## 7. Input Files: 5 → 1

### Current
`prepare_io_paths()` (`lib.rs:6342-6483`) writes 5 files into `trial_dir/in/`:
- `trial_input.json` — the full composite input
- `task.json` — extracted `/task`
- `bindings.json` — extracted `/bindings`
- `dependencies.json` — extracted `/dependencies`
- `policy.json` — extracted `/policy`

### Target
One file: `task.json` — the raw task payload from the dataset, written to `trial_dir/in/task.json`.

### Patch

**a) Simplify `prepare_io_paths()`** for v1 experiments:
- Write only `task.json` (the task payload from the dataset row)
- Skip bindings/dependencies/policy extraction
- `PreparedTrialIo` simplifies to `{ task_path, result_path }`

**b) `build_agent_task()`** — for v1, the "trial input" *is* the task payload. No wrapping in `{ ids, task, bindings, dependencies, policy }` envelope.

---

## 8. JSON Schema

### Current
`schemas/resolved_experiment_v0_5.jsonschema` requires:
- `experiment.workload_type` — required
- `dataset.provider`, `suite_id`, `schema_version`, `split_id` — all required
- `design.sanitization_profile`, `random_seed`, `shuffle_tasks`, `max_concurrency` — all required
- `baseline.bindings` — required
- `variant_plan[*].bindings` — required
- `runtime.agent.mode` — required enum
- `runtime.dependencies` — required
- `runtime.policy` with nested `timeout_ms`, `network`, `sandbox` — all required

### Target (v1 schema)
New schema file: `schemas/experiment_v1_0.jsonschema`
```json
{
  "required": ["version", "experiment", "dataset", "design", "baseline", "runtime"],
  "properties": {
    "version": { "const": "1.0" },
    "experiment": {
      "required": ["id", "name"],
      "properties": {
        "id": { "type": "string" },
        "name": { "type": "string" }
      }
    },
    "dataset": {
      "required": ["path"],
      "properties": {
        "path": { "type": "string" },
        "limit": { "type": "integer", "minimum": 1 }
      }
    },
    "design": {
      "required": ["replications"],
      "properties": {
        "comparison": { "enum": ["paired", "unpaired", "none"] },
        "replications": { "type": "integer", "minimum": 1 },
        "seed": { "type": "integer" }
      }
    },
    "baseline": {
      "required": ["variant_id"],
      "properties": {
        "variant_id": { "type": "string" }
      }
    },
    "variant_plan": {
      "type": "array",
      "items": {
        "required": ["variant_id"],
        "properties": {
          "variant_id": { "type": "string" },
          "args": { "type": "array", "items": { "type": "string" } },
          "image": { "type": "string" },
          "env": { "type": "object", "additionalProperties": { "type": "string" } }
        }
      }
    },
    "runtime": {
      "required": ["image", "command"],
      "properties": {
        "image": { "type": "string" },
        "command": { "type": "array", "items": { "type": "string" } },
        "timeout_ms": { "type": "integer" },
        "network": { "type": "string", "enum": ["none", "full"] },
        "resources": {
          "properties": {
            "cpus": { "type": "integer" },
            "memory_mb": { "type": "integer" }
          }
        }
      }
    }
  }
}
```

---

## 9. `lab-core` Constants

### Current (`lab-core/src/lib.rs:8-50`)
18 path constants (`AGENTLAB_*_PATH`) + 18 env var constants (`AGENTLAB_ENV_*`).

### Target
Constants remain for backward compat with v0.x code path. Add new constants for v1:
```rust
pub const CLEAN_IN_TASK: &str = "/in/task.json";
pub const CLEAN_OUT_DIR: &str = "/out";
pub const CLEAN_OUT_RESULT: &str = "/out/result.json";
```

`RunnerRuntimeHostPaths` keeps its fields (used by the runner's own bookkeeping), but v1 experiments use a simpler subset.

---

## 10. `TrialPaths` and Host Directory Structure

### Current (`lab-runner/src/lib.rs:5414-5425`)
```rust
struct TrialPaths {
    in_dir, workspace, state, deps, dataset, out, tmp, runtime, dataset_src, exp_dir
}
```
Creates 7 directories per trial.

### Target
For v1 experiments, the runner still needs `in_dir` and `out` on the host. Internal bookkeeping dirs (`state`, `deps`, `workspace`, `dataset`, `tmp`) are runner concerns — they can exist on the host but don't affect the contract.

### Patch
No structural change to `TrialPaths` required. The change is in *what gets mounted* (patch #3) and *what gets written* (patch #7), not in how the runner organizes its own host directories.

---

## Execution Order

| Phase | Patches | Rationale |
|---|---|---|
| **1: Version routing** | 5b | Detect `version: "1.0"`, branch parsing. No behavior change for v0.x. |
| **2: Schema** | 8 | Add `experiment_v1_0.jsonschema`, validate v1 configs. |
| **3: Variant model** | 1a–1e | New `Variant` struct with `args`/`env`/`image` for v1 path. |
| **4: IO simplification** | 2, 7 | Positional args, single input file. |
| **5: Mount reduction** | 3 | Two mounts for v1 containers. |
| **6: Env cleanup** | 4 | No `AGENTLAB_*` env projection for v1. |
| **7: Config flattening** | 5a, 6 | New required-fields set and flat runtime parsing for v1. |
| **8: Constants** | 9 | Add clean-contract constants. |
| **9: Validation + demo** | 5a | Wire up `examples/clean_harness/experiment.yaml` as integration test. |

Each phase is independently testable. The v0.x code path is untouched throughout — all changes are additive behind the `version: "1.0"` gate.
