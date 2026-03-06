# AgentLab Experiment DX Audit: Boundary Leaks & Minimal Surface

## Problem Statement

A single-variant bench_v0 experiment (`bench_v0_qwen35b_a3b_only.yaml`) takes **143 lines** of YAML. The actual user decisions being made are:

1. Which benchmark (bench_v0)
2. Which agent binary (rex)
3. Which model (qwen3.5-35b-a3b via LM Studio)
4. Two policy overrides (network: full, root_read_only: false)
5. Some agent config files

Everything else is the runner's job. The user is currently transcribing internal protocol versions, schema names, container mount paths, adapter capabilities, and evaluator wiring — all for a benchmark the runner owns and ships.

Target: **~30 lines**.

---

## Part 1: The Artifact & Command Problem

### What `artifact` actually is

`artifact` is a tar.gz or directory containing the agent's code and binaries. The runner:

1. Computes a SHA256 digest of the archive
2. Checks a host-side cache at `{artifact_parent}/.agentlab_artifact_cache/{digest}/`
3. If miss: unpacks to staging dir, atomically renames to cache, writes ready marker
4. Mounts the unpacked directory into the container at the **hardcoded path `/opt/agent/`** (read-only)
5. Or if direct mount fails: `docker cp` + `tar -xzf /opt/agent.tar.gz -C /opt/agent` inside the container

So `artifact: .lab/agents/rex-minimal-linux-dir` means "mount this directory tree at `/opt/agent/` in the container."

### Why the command has ugly `/opt/agent/bin/rex` paths

The container mount point `/opt/agent/` is a runner internal constant:

```rust
// lab-runner/src/lib.rs
const AGENT_ARTIFACT_UNPACK_COMMAND: &str =
    "tar ... -xzf /opt/agent.tar.gz -C /opt/agent && rm -f /opt/agent.tar.gz";
```

The user has to know this mount point to write their command. So they write:

```yaml
command: [/opt/agent/bin/rex, run, --bindings-file, ${AGENTLAB_BINDINGS_PATH}, ...]
```

This is a **boundary leak**. The user is addressing a path chosen by the runner's container layout. If the runner changed the mount point, every experiment YAML would break.

### Why the command has `${AGENTLAB_BINDINGS_PATH}` etc.

The runner sets environment variables inside the container:

```
AGENTLAB_TASK_PATH       = /agentlab/in/task.json
AGENTLAB_BINDINGS_PATH   = /agentlab/in/bindings.json
AGENTLAB_RESULT_PATH     = /agentlab/out/result.json
AGENTLAB_TRAJECTORY_PATH = /agentlab/out/trajectory.jsonl
AGENTLAB_TIMEOUT_MS      = 600000
AGENTLAB_TRIAL_ID        = trial_1
```

The user references these in the command template. But the user also declares `io: { input_arg: --input-file, output_arg: --output }`, which tells the runner to *automatically append* `--input-file {task_path} --output {result_path}`. So there's a **redundancy**: the command manually references `${AGENTLAB_BINDINGS_PATH}` but the io block handles input/output automatically.

### The `cp` hack

```yaml
command:
  - /bin/sh
  - -lc
  - >-
    cp /agentlab/deps/providers.lmstudio-docker.ts /workspace/packages/core/types/src/providers.ts &&
    cp /agentlab/deps/providers.lmstudio-docker.js /workspace/packages/core/types/dist/providers.js &&
    exec /opt/agent/bin/rex run --bindings-file "${AGENTLAB_BINDINGS_PATH}" ...
```

The user stages files to `/agentlab/deps/` via `file_staging`, then manually copies them into the workspace in the command preamble. This is because `file_staging` can only target `/agentlab/deps/` — there's no way to inject files directly into the workspace at specific paths.

### What the ideal looks like

```yaml
agent:
  artifact: rex-minimal-linux-dir
  command: rex run --model={bindings.model} --provider={bindings.model_provider}
  io: { input: --input-file, output: --output }
```

What this requires from the runner:

1. **PATH resolution**: Add `/opt/agent/bin` to `$PATH` so the user writes `rex` not `/opt/agent/bin/rex`
2. **Binding interpolation**: `{bindings.model}` resolves from the variant's bindings, not from raw env vars
3. **Implicit io wiring**: The runner already appends io args. The env var references in the command are redundant
4. **Artifact name resolution**: `rex-minimal-linux-dir` resolves from `.lab/agents/` without an absolute path
5. **Workspace injection**: A first-class mechanism to inject files into the workspace (not just `/agentlab/deps/`)

---

## Part 2: Full Field-by-Field Audit

### Category A: Benchmark internals the runner should resolve (~65 lines)

| Field | Why it's a boundary leak |
|---|---|
| `dataset.provider: local_jsonl` | Only one provider exists |
| `dataset.path` | Runner owns bench_v0, knows where the data lives |
| `dataset.schema_version` | Coupled to the benchmark definition |
| `dataset.suite_id` | Redundant with benchmark name |
| `dataset.split_id: test` | Default for bench_v0 |
| `workload_type: agent_loop` | Inferrable from runtime config |
| `image_source: per_task` | bench_v0 always uses per-task images |
| `design.sanitization_profile` | Always `hermetic_functional` |
| `benchmark.policy.*` (all 4 fields) | bench_v0 defines its own task_model, evaluator_mode, scoring_lifecycle, chain_failure_policy |
| `benchmark.adapter.command` | bench_v0 owns its adapter |
| `benchmark.adapter.manifest` (entire block) | 15 lines of internal protocol — schema versions, record schemas, evaluator details, capabilities |
| `metrics` for bench-specific metrics | `resolved`, `hidden_cases_passed`, `hidden_cases_total` are defined by bench_v0 |
| `metrics` for runner auto-metrics | `duration_ms`, `turn_count` are always available |
| File staging for `bench_benchmark_adapter.py` | Runner should stage its own grader |
| `destination_path` on all file_staging | User should not know container-internal paths |
| `runtime.agent.env.HOME` | Container internal — sandbox implementation detail |

**Resolution**: A benchmark registry. `benchmark: bench_v0` resolves:
- Dataset (path, schema_version, suite_id, split_id, provider)
- Adapter (command, manifest, capabilities)
- Policy (task_model, evaluator_mode, scoring_lifecycle, chain_failure_policy)
- Default metrics
- Image source strategy
- Any benchmark-owned file staging (grader, hidden test bundles)
- Sanitization profile

### Category B: Defaults restated for no reason (~25 lines)

| Field | Default value in ExperimentBuilder |
|---|---|
| `comparison: none` | Should be inferred: no `variant_plan` → `none` |
| `replications: 1` | Builder default |
| `random_seed: 42` | Builder default (close — builder uses `1`, but point stands) |
| `shuffle_tasks: true` | Builder default |
| `max_concurrency: 1` | Builder default |
| `policies.retry.max_attempts: 1` | Builder default |
| `timeout_ms: 600000` | Builder default |
| `validity.fail_on_state_leak: true` | Builder default |
| `validity.fail_on_profile_invariant_violation: true` | Builder default |
| `services: []` | Empty, omittable |
| `allowed_hosts: []` | Empty (meaningless when mode=full) |

**Resolution**: Omit everything that matches the default. Only declare overrides.

### Category C: Legitimate user concerns (~30 lines → ~25 with better ergonomics)

| Field | User intent |
|---|---|
| `experiment.id` | Identity |
| `experiment.name` | Display name |
| `experiment.tags` | Categorization |
| `experiment.description` | Optional |
| `benchmark: bench_v0` | Which benchmark |
| `limit: 20` | How many tasks to run |
| `baseline.variant_id` | Variant name |
| `baseline.bindings.*` | Model, provider, agent_type — the experimental variable |
| `agent.artifact` | Which agent binary |
| `agent.command` | How to invoke it |
| `agent.io` | CLI arg mapping |
| `agent.env.MEMORY_DAEMON_URL` | Agent-specific config |
| User config files | defaults.json, provider overrides — agent-specific |
| Workspace patches | Files that need to land in the workspace |
| `network: full` | Override (LM Studio needs network) |
| `root_read_only: false` | Override |

---

## Part 3: What the File Should Look Like

### Minimal experiment (single variant, runner-owned benchmark):

```yaml
experiment:
  id: bench_v0_qwen35b_a3b_only
  name: "Bench v0: Qwen3.5 35B A3B (LM Studio)"
  tags: [bench-v0, single-variant, lmstudio, qwen3.5-35b-a3b]

benchmark: bench_v0
limit: 20

agent:
  artifact: rex-minimal-linux-dir
  command: rex run --dangerous
  io: { input: --input-file, output: --output }
  env:
    MEMORY_DAEMON_URL: ""
  config_files:
    - overrides/defaults.bench-lmstudio-headless.json
    - overrides/providers.lmstudio-docker.ts
    - overrides/providers.lmstudio-docker.js
  workspace_patches:
    overrides/providers.lmstudio-docker.ts: packages/core/types/src/providers.ts
    overrides/providers.lmstudio-docker.js: packages/core/types/dist/providers.js

baseline:
  id: qwen_35b_a3b
  bindings:
    model_provider: lmstudio
    model: qwen3.5-35b-a3b

overrides:
  network: full
  root_read_only: false
```

**~30 lines. Same experiment. Same semantics.**

### A/B test (two variants):

```yaml
experiment:
  id: bench_v0_glm5_vs_codex_spark
  name: "Bench v0: GLM-5 vs Codex Spark"
  tags: [bench-v0, ab-test]

benchmark: bench_v0
limit: 20
concurrency: 2

agent:
  artifact: rex-minimal-linux.tar.gz
  command: rex run --dangerous --provider-env z.ai-coder=ZAI_CODER_API_KEY
  io: { input: --input-file, output: --output }
  env_from_host: [ZAI_CODER_API_KEY]
  config_files:
    - overrides/defaults.json

baseline:
  id: glm_5
  bindings: { model_provider: z.ai-coder, model: glm-5 }

variants:
  - id: codex_spark
    bindings: { model_provider: codex, model: gpt-5.3-codex-spark }

overrides:
  network: full
```

**~25 lines** vs the current 164 lines.

---

## Part 4: What the Runner Needs

### A. Benchmark Registry

A built-in registry mapping benchmark names to their full configuration:

```
bench_v0 → {
  dataset: { path: "data/bench_v0.task_boundary_v2.jsonl", schema: "task_boundary_v2", suite: "bench_v0", split: "test" },
  image_source: per_task,
  adapter: { command: ["python3", "/opt/bench/bench_benchmark_adapter.py"], ... },
  policy: { task_model: independent, evaluator_mode: custom, scoring_lifecycle: predict_then_score, ... },
  metrics: [resolved, hidden_cases_passed, hidden_cases_total, duration_ms, turn_count],
  sanitization_profile: hermetic_functional,
  file_staging: [{ benchmark_adapter → /opt/bench/ }],
}
```

External benchmarks (harbor, swe-bench) would register via manifest files. Runner-owned benchmarks are built in.

### B. Agent PATH + Structured Binding Projection

1. Set `PATH=/opt/agent/bin:$PATH` in the container environment
2. Replace string interpolation with structured binding projection:

```yaml
agent:
  command: [rex, run, --dangerous]
  bindings_to_args:
    - binding: model_provider
      flag: --provider
    - binding: model
      flag: --model
```

This avoids quoting/template ambiguity. Runner appends args deterministically and fails fast if a required binding key is missing.
3. Auto-wire io args (already done, just make sure the command doesn't also need to reference them)

### C. Workspace Injection

Add `workspace_patches` (or `workspace_inject`) to file_staging:

```yaml
workspace_patches:
  staged_file_name: path/relative/to/workspace
```

The runner copies from `/agentlab/deps/{staged_file_name}` to `$WORKSPACE/{target_path}` before executing the agent command. Eliminates the `cp` shell hack.

### D. Config File Conventions

`config_files` resolves from `.lab/experiments/overrides/` (or a configured base). The user writes the filename, not an absolute host path + a container destination path.

### E. Inference Rules

- No `variant_plan` → `comparison: none`, `scheduling: variant_sequential`
- Has `variant_plan` → `comparison: paired`, `scheduling: paired_interleaved`
- `benchmark: X` → resolve dataset, adapter, metrics, policy, image_source, sanitization
- `artifact` without absolute path → resolve from `.lab/agents/`
- `owner` → from `git config user.name` or `$USER`

---

## Part 5: SDK / Effect Opportunity

### Current SDK: 1:1 YAML Serializer

`ExperimentBuilder` doesn't abstract anything. It generates the same 143-line schema. No benchmark awareness, no path resolution, no presets.

### What the SDK should provide

```typescript
import { Experiment, Benchmark, Agent } from "@agentlab/sdk"

const experiment = Experiment.create({
  id: "bench_v0_qwen35b_a3b_only",
  name: "Bench v0: Qwen3.5 35B A3B",
  benchmark: Benchmark.builtin("bench_v0"),
  limit: 20,
  agent: Agent.fromArtifact("rex-minimal-linux-dir", {
    command: "rex run --dangerous",
    io: { input: "--input-file", output: "--output" },
    env: { MEMORY_DAEMON_URL: "" },
    configFiles: ["overrides/defaults.bench-lmstudio-headless.json"],
    workspacePatches: {
      "overrides/providers.lmstudio-docker.ts": "packages/core/types/src/providers.ts",
    },
  }),
  baseline: {
    id: "qwen_35b_a3b",
    bindings: { model_provider: "lmstudio", model: "qwen3.5-35b-a3b" },
  },
  overrides: { network: "full", rootReadOnly: false },
})
```

### Effect-based alternative

Effect's `Layer` system maps naturally to experiment resources:

```typescript
import { Effect, Layer } from "effect"
import { Benchmark, Agent, Experiment, LabRuntime } from "@agentlab/effect-sdk"

// Benchmark is a Layer that provides dataset, adapter, metrics, policy
const bench = Benchmark.builtin("bench_v0")

// Agent is a Layer that provides command, artifact, env, config
const agent = Agent.fromArtifact("rex-minimal-linux-dir", {
  command: "rex run --dangerous",
  io: { input: "--input-file", output: "--output" },
})

// Experiment composes Benchmark + Agent layers
// Missing layers are TYPE ERRORS, not runtime crashes
const run = Experiment.single({
  id: "bench_v0_qwen35b_a3b",
  baseline: { id: "qwen_35b_a3b", bindings: { model: "qwen3.5-35b-a3b" } },
}).pipe(
  Effect.provide(bench),
  Effect.provide(agent),
  Effect.provide(LabRuntime.localDocker),
)

// Type-safe: if you forget to provide the Benchmark layer, it won't compile
await Effect.runPromise(run)
```

Advantages of Effect:
- **Composable resources**: Benchmark, Agent, Runtime are independent layers
- **Type-safe requirements**: Missing benchmark or agent is a compile error
- **Declarative overrides**: `Layer.merge` to override specific fields
- **Resource lifecycle**: Effect manages setup/teardown (container spin-up, artifact caching)
- **Schema validation**: Effect/Schema for runtime validation of bindings, task payloads

---

## Part 6: Shell Script Elimination

The 600-line `run-bench-experiment.sh` should mostly not exist:

| Current script command | Should be |
|---|---|
| `build-image` | `lab-cli benchmark prepare bench_v0` (or automatic on first run) |
| `build-task-images` | Part of benchmark prepare |
| `repair-artifact` | Automatic in preflight (detect platform mismatch) |
| `export` | Automatic: `benchmark: bench_v0` triggers dataset resolution |
| `preflight` | `lab-cli preflight experiment.yaml` (already exists) |
| `describe` | `lab-cli describe experiment.yaml` (already exists) |
| `run` | `lab-cli run experiment.yaml` (already exists) |
| `scoreboard` | `lab-cli scoreboard` (already exists or should) |
| Grade audit (inline Python) | `lab-cli audit --strict <run_dir>` |

The script exists because the runner doesn't handle benchmark lifecycle. With a benchmark registry, `lab-cli run` can:
1. Resolve `benchmark: bench_v0`
2. Check if task images exist → build if needed
3. Check if dataset JSONL is current → export if needed
4. Check artifact platform → repair if needed
5. Run preflight
6. Execute

One command: `lab-cli run .lab/experiments/bench_v0_qwen35b_a3b_only.yaml`

---

## Part 7: IO Contract Design — File, Persistent, Server

### The Two Orthogonal Axes

Container lifecycle and IO method are independent concerns, but the current design conflates them:

| Axis | Current | Design space |
|---|---|---|
| **IO method** | File only (task.json → command → result.json) | File, stdin/stdout, HTTP/gRPC, UDS |
| **Container lifecycle** | Ephemeral only (fresh per task) | Ephemeral, persistent, external |

### Current Architecture (File + Ephemeral)

Every task, regardless of `task_model`, gets a fresh container:

```
docker create → docker start → docker exec → docker rm
```

Even `task_model: dependent` chains don't keep the container alive. Instead:
1. Task N completes → runner snapshots workspace filesystem
2. Task N+1 → fresh container → runner restores snapshot into new workspace
3. Agent runs in the pre-populated workspace

State transfer is via filesystem snapshot-restore, never via container persistence.

The agent IO contract is:
1. Runner writes `task.json` to `/agentlab/in/task.json`
2. Runner writes `bindings.json` to `/agentlab/in/bindings.json`
3. Runner sets env vars: `AGENTLAB_TASK_PATH`, `AGENTLAB_RESULT_PATH`, etc.
4. Runner execs the agent command
5. Agent reads input from well-known path (env var)
6. Agent writes result to well-known path (env var)
7. Runner reads result

### Should We Be Opinionated About File IO?

**Yes. File-based IO should be the opinionated, zero-config default.**

Reasons:
- **Simplest agent contract**: Read a file, write a file. Works in any language, any runtime.
- **Debuggable**: `cat /agentlab/in/task.json` to see what the agent received. `cat /agentlab/out/result.json` to see what it produced.
- **Auditable**: Evidence records just reference file paths. No transient in-memory state.
- **Reproducible**: Replay is trivial — feed the same input file, compare output files.
- **Sufficient for 95%+ of benchmarks**: Most benchmarks are "run agent on task, check result."

### The IO Tier System

**Tier 0: Well-known paths (default, no declaration needed)**

Agent reads `$AGENTLAB_TASK_PATH`, writes `$AGENTLAB_RESULT_PATH`. The runner sets these env vars. No `io:` block in the experiment YAML.

This is the simplest possible contract. An agent that implements this needs zero configuration:

```yaml
agent:
  command: rex run --dangerous
  # No io: block. Agent reads $AGENTLAB_TASK_PATH, writes $AGENTLAB_RESULT_PATH.
```

The agent code:
```python
task = json.load(open(os.environ["AGENTLAB_TASK_PATH"]))
# ... do work ...
json.dump(result, open(os.environ["AGENTLAB_RESULT_PATH"], "w"))
```

**Tier 1: CLI arg mapping (override for agents with their own CLI)**

The agent has its own CLI and expects specific flag names. The `io:` block tells the runner how to map well-known paths to the agent's flags:

```yaml
agent:
  command: rex run --dangerous
  io: { input: --input-file, output: --output }
  # Runner appends: --input-file /agentlab/in/task.json --output /agentlab/out/result.json
```

This is an escape hatch, not the default. If you control the agent, design it to read env vars and skip this entirely.

**Tier 2: Persistent container (for expensive startup or dependent chains)**

```yaml
agent:
  command: rex run --dangerous
  lifecycle: persistent
```

Same file IO contract. What changes is the container lifecycle:

```
docker create + start  (once per chain or run)
  for each task:
    update bind-mounted /agentlab/in/task.json
    docker exec <container> <agent-command>
    read /agentlab/out/result.json
docker rm  (end of chain or run)
```

This is strictly better than snapshot-restore for dependent task chains:
- No snapshot/restore overhead
- Workspace state naturally persists (same filesystem)
- In-memory state persists too (loaded models, warm caches)
- Agent startup cost paid once

The runner's `docker exec` per task means the agent command is re-invoked each time, but the container (and any background processes, loaded models, etc.) stays alive. If the agent binary is fast to re-invoke (just reads new input, does work, writes output), this is transparent. If the agent loads models on startup, it could run a daemon and the command just signals "process next task."

**When persistent makes sense:**
- Agent has expensive initialization (model loading, environment setup)
- Dependent task chains where workspace continuity matters
- Benchmarks with many small tasks where container spin-up dominates

**Tier 3: Server protocol (future, for conversational/multi-turn)**

```yaml
agent:
  protocol: http
  port: 8080
  command: rex serve --port 8080
```

Agent starts an HTTP server. Runner POSTs task JSON, receives result JSON. For:
- Multi-turn conversational benchmarks (back-and-forth within a single task)
- Agent-as-a-service (external API, no container)
- High-throughput streaming benchmarks

This is a genuine protocol change, not just a lifecycle change. The input/output format may differ (streaming chunks vs. single JSON). Probably not needed for v1 — file + persistent covers the vast majority of cases.

### Long-Running Benchmarks: How Would It Actually Work?

> "What if we have a long-running benchmark where we don't reset per task?"

Two sub-questions here:

**Q1: Can the runner feed tasks sequentially to a persistent container via file IO?**

Yes. This is `lifecycle: persistent` with file IO:

```
Container created (once)
│
├── Task 1:
│   ├── Runner writes /agentlab/in/task.json (task 1 payload)
│   ├── Runner: docker exec container rex run
│   ├── Agent reads task.json, does work, writes result.json
│   └── Runner reads /agentlab/out/result.json
│
├── Task 2:
│   ├── Runner overwrites /agentlab/in/task.json (task 2 payload)
│   ├── Runner: docker exec container rex run
│   ├── Agent reads task.json, does work in SAME workspace (state from task 1 persists)
│   ├── Agent writes result.json
│   └── Runner reads result.json
│
└── Container destroyed (end of chain)
```

The input file is just overwritten. The command is re-invoked via `docker exec`. The workspace state (files, databases, caches) persists between tasks because it's the same container. This is clean, simple, and doesn't require any new IO mechanism.

**Q2: What about an agent that's a long-running daemon (not re-invoked per task)?**

If the agent binary itself is stateful and shouldn't be re-invoked (e.g., it loaded a 70B model into GPU memory), the command model doesn't fit. You'd need:

Option A: **Thin invoker + daemon**
```yaml
agent:
  setup: rex daemon start --port 9999    # Starts daemon once
  command: rex invoke --port 9999        # Sends task per invocation
  lifecycle: persistent
```

`setup` runs once when the container starts. `command` runs per task via `docker exec`. The daemon stays alive between invocations.

Option B: **Server protocol (Tier 3)**
```yaml
agent:
  protocol: http
  port: 8080
  command: rex serve --port 8080
  lifecycle: persistent
```

Runner starts the container, waits for the server to be ready (health check), then POSTs tasks.

Option A is implementable today with minor runner changes (add `setup` as a one-time container init command). Option B requires the server protocol.

### What About Agents That Are Just a Binary?

> "What if the user just has a binary or some ready-to-go installation?"

Three scenarios:

**1. Binary on host, run in container**
```yaml
agent:
  artifact: ./my-agent          # directory or tar.gz containing the binary
  command: my-agent solve       # runner adds /opt/agent/bin to PATH
```

Runner mounts the artifact at `/opt/agent/`, adds to PATH. User writes the binary name, not a container path.

**2. Binary already in the Docker image**
```yaml
agent:
  image: my-agent:latest        # Custom image with agent pre-installed
  command: my-agent solve       # Binary already on PATH in the image
```

No artifact needed. The agent is baked into the image.

**3. Binary on host, run locally (no container)**
```yaml
agent:
  command: ./my-agent solve
  sandbox: local                # No container, run directly on host
```

For local development / testing. Runner invokes the command directly, sets env vars, manages input/output files on the host filesystem.

### Summary: IO Design Principles

1. **File IO is the opinionated default.** No declaration needed. Agent reads `$AGENTLAB_TASK_PATH`, writes `$AGENTLAB_RESULT_PATH`.

2. **`io:` is an escape hatch** for mapping well-known paths to agent CLI flags. Not the primary contract.

3. **Container lifecycle is a separate axis.** `lifecycle: ephemeral` (default) or `lifecycle: persistent`. Both use the same file IO contract.

4. **Persistent containers eliminate snapshot-restore** for dependent task chains. Same `docker exec` model, just don't tear down between tasks.

5. **Server protocol is future work** for multi-turn conversational benchmarks. Not needed for v1.

6. **The agent should never need to know container-internal paths.** Env vars abstract the mount points. The runner owns the filesystem layout.

---

## Execution Plan: Hard Cutover

No compatibility mode. No versioning detour. We intentionally break the old surface and move to a slim, opinionated contract.

## Phase Gate Rules (Anti-Gaming)

1. **The invariant checklist is add-only.** We never delete or weaken an invariant after adding it.
2. **Every invariant needs two proofs:** an automated test and a concrete evidence artifact path.
3. **No manual QA sign-off as primary proof.** Manual checks are supplemental only.
4. **No feature flags for core contract behavior.** If the new contract can be bypassed via fallback toggles, the phase is not done.
5. **Phase completion is all-or-nothing.** Missing evidence for one invariant blocks the phase.
6. **No "implemented but untested" claims.** If there is no CI-enforced test for an invariant, it is not implemented.
7. **No "doc-only" completion.** Canonical docs must be backed by executable fixtures that run in CI.
8. **No hidden fallback behavior.** Deprecated/removed paths must fail loudly, not silently route to legacy behavior.

## Known Gaming Vectors + Counter-Invariants

1. **Gaming vector:** Keep legacy parsing as a silent fallback and claim hard cutover.
   Counter-invariant: legacy-field fixtures must fail preflight with explicit "removed field" errors.
2. **Gaming vector:** Claim PATH ergonomics, but keep canonical examples using `/opt/agent/bin/...`.
   Counter-invariant: doc lint forbids `/opt/agent/bin/` in canonical author-facing examples.
3. **Gaming vector:** Compute artifact digest but do not enforce it at execution time.
   Counter-invariant: mutation test changes artifact bytes after preflight; run must fail with digest mismatch.
4. **Gaming vector:** Implement `bindings_to_args` via shell string concatenation that reintroduces quoting bugs.
   Counter-invariant: argv integrity tests with spaces/special characters prove one-token-per-value behavior.
5. **Gaming vector:** Accept `workspace_patches` but allow path traversal via `..` or symlink escape.
   Counter-invariant: traversal and symlink breakout tests must fail before execution.
6. **Gaming vector:** Keep benchmark internals user-overridable and call it "registry-owned."
   Counter-invariant: benchmark-owned fields are either rejected in authoring or ignored with explicit warning + audit event.
7. **Gaming vector:** Declare phase done from local machine runs only.
   Counter-invariant: CI artifacts are required and linked per invariant ID.

## Required Evidence Bundle (Per Phase)

Each phase sign-off must include an evidence bundle committed in-repo (for example `docs/evidence/dx/phase0/`), with:

1. `invariants.md` table: invariant ID, status, test name, evidence path, reviewer.
2. CI test report artifact link(s) covering all invariant IDs.
3. Canonical minimal authoring fixture(s) used for verification.
4. Resolved manifest snapshot(s) showing benchmark resolution/default materialization.
5. Negative preflight snapshot(s) showing hard-cut failures for removed/invalid fields.
6. Runtime evidence for key UX claims (for example direct `rex` invocation without absolute path).
7. Artifact pinning evidence (digest in manifest + mismatch failure proof).
8. Doc-lint report proving canonical docs/examples match the contract.

Phase is blocked unless every invariant row in `invariants.md` has both a passing test and an evidence artifact path.

Validation command contract:
- `scripts/ci/validate-dx-evidence.sh --phase phase0 --mode strict`
- `scripts/ci/validate-dx-evidence.sh --phase phase1 --mode strict`
- `scripts/ci/validate-dx-evidence.sh --phase phase2 --mode strict`

### Phase 0 (Do Immediately): Contract Reset + Registry + Schema/Data Migration

**North Star UX/DX State (Phase 0)**
1. A user can declare `benchmark: bench_v0` without wiring dataset/adapter/policy internals.
2. A container command can invoke `rex` by name, without `/opt/agent/bin/...`.
3. Authoring uses short artifact/config references, with runner-resolved canonical paths.
4. Resolved manifests are deterministic and include pinned artifact digests.

**Implementation Scope**
1. First-class benchmark registry (P0).
2. Hard-cut schema and authoring surface.
3. Boundary leak fixes in runtime (`PATH`, artifact/config resolution).
4. Opinionated defaults.
5. Artifact pinning.

**Invariant Checklist (Living, Add-Only)**
1. `P0-I01 Registry ownership`: `benchmark: bench_v0` authoring requires no dataset/adapter/policy internals.
   Test case/evidence required: integration test with minimal bench_v0 yaml + resolved manifest showing fully materialized benchmark fields.
2. `P0-I02 Hard-cut enforcement`: legacy removed fields fail preflight with actionable errors.
   Test case/evidence required: negative preflight fixtures + exact error snapshots.
3. `P0-I03 PATH leak removed`: command `[rex, --version]` succeeds in container mode without absolute binary path.
   Test case/evidence required: container integration run log proving direct `rex` invocation.
4. `P0-I04 Artifact pinning`: resolved manifest records canonical artifact path + SHA256 digest.
   Test case/evidence required: manifest assertion test + mismatch test that fails when artifact bytes change.
5. `P0-I05 Deterministic defaults`: omitted benchmark-owned/default fields are resolved deterministically.
   Test case/evidence required: golden resolved manifest fixture with stable values.
6. `P0-I06 Doc contract`: canonical docs/examples do not require `/opt/agent/...` or benchmark internals.
   Test case/evidence required: doc lint/grep test over canonical example set.

### Phase 1 (Do Immediately After Phase 0): Ergonomics + Safety + Docs

**North Star UX/DX State (Phase 1)**
1. Agent commands express intent only; bindings are projected structurally, not templated.
2. Workspace file injection is declarative (`workspace_patches`), no shell `cp` preambles.
3. Preflight catches missing bindings/invalid patch paths before any run starts.
4. Canonical docs show only the minimal opinionated path.

**Implementation Scope**
1. Structured bindings projection (`bindings_to_args`, optional `bindings_to_env`).
2. First-class workspace injection (`workspace_patches`).
3. Documentation hardening.
4. Preflight hardening.

**Invariant Checklist (Living, Add-Only)**
1. `P1-I01 Binding completeness`: missing required binding keys are hard preflight errors.
   Test case/evidence required: negative fixture matrix for missing keys + error snapshots.
2. `P1-I02 Binding determinism`: `bindings_to_args` emits args in declaration order and is stable across runs.
   Test case/evidence required: unit test asserting exact argv output.
3. `P1-I03 Binding safety`: projected values are argv-safe (no string-template splitting behavior).
   Test case/evidence required: values-with-spaces test proving single-token integrity.
4. `P1-I04 Workspace patch path safety`: reject absolute paths and `..` traversal.
   Test case/evidence required: negative preflight fixtures + error snapshots.
5. `P1-I05 Workspace patch determinism`: copy order and overwrite semantics are explicit and tested.
   Test case/evidence required: integration test with conflicting patch entries + deterministic outcome assertion.
6. `P1-I06 No cp hacks in canon`: canonical examples/templates contain no shell `cp` glue.
   Test case/evidence required: doc/template lint check + canonical fixture review artifact.

### Phase 2 (Later): Advanced SDK and Runtime Modes

**North Star UX/DX State (Phase 2)**
1. Power users get higher-level composition APIs without widening the base contract.
2. Runtime lifecycle automation is explicit and predictable.
3. Extended lifecycle/protocol modes remain optional and benchmark-driven.

**Implementation Scope**
1. Effect-based SDK track (optional).
2. Runner lifecycle automation policy (`prepare`/`run` boundaries).
3. Extended IO/lifecycle capabilities (persistent/server modes where justified).

**Invariant Checklist (Living, Add-Only)**
1. `P2-I01 No regression of minimal surface`: advanced APIs do not reintroduce required low-level wiring in basic authoring.
   Test case/evidence required: minimal authoring smoke test remains unchanged.
2. `P2-I02 Side-effect clarity`: lifecycle automation behavior is explicit and testable.
   Test case/evidence required: command-level contract tests for prepare/run side effects.
3. `P2-I03 Optionality`: advanced runtime modes are opt-in and do not alter default file-IO behavior.
   Test case/evidence required: baseline mode parity tests against Phase 1 behavior.
