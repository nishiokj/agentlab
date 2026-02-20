# @agentlab/sdk

TypeScript SDK for authoring AgentLab `version: '0.5'` experiments and invoking the Rust runner.

The SDK is runtime-agent-first:

1. You declare `runtime.agent`, `runtime.dependencies`, and `runtime.policy`.
2. Runner owns container/process execution, isolation, and causal artifact extraction.
3. Your runtime command is invoked once per trial.

## Install

```bash
npm install @agentlab/sdk
```

Local development:

```bash
cd sdk
npm install
npm run build
npm test
```

## Quick Start

```ts
import { ExperimentBuilder, LabClient, Metric } from '@agentlab/sdk';
import { mkdirSync, writeFileSync } from 'node:fs';

const builder = ExperimentBuilder.create('rex_ab', 'Rex Prompt A/B')
  .description('Compare prompt variants on curated coding tasks')
  .datasetJsonl('./data/tasks.boundary.jsonl', {
    suiteId: 'swebench_lite_curated',
    splitId: 'test',
    limit: 10,
  })

  // Runtime boundary: runner executes this in an isolated trial container.
  .customAgentImage(
    'ghcr.io/acme/rex-agent@sha256:0123456789abcdef...',
    ['python', '-m', 'rex.run_trial'],
  )
  .usePrebuiltRexJesusAdapter()
  .agentEnvFromHost(['OPENAI_API_KEY'])

  // Dependency boundary: stage host files into trial paths.
  .dependencyFileStaging([
    {
      source_from_host: './deps/sqlite/main.db',
      destination_path: '/agentlab/deps/sqlite/main.db',
      required: true,
    },
    {
      source_from_host: './deps/ast/index.tar.zst',
      destination_path: '/agentlab/deps/ast/index.tar.zst',
      required: false,
    },
  ])

  .baseline('control', { model: 'gpt-4o-mini', prompt: 'v1' })
  .addVariant('treatment', { model: 'gpt-4o-mini', prompt: 'treatment_prompt' })

  .metric(Metric.DURATION_MS)
  .metric(Metric.fromOutput('solved', '/metrics/solved', {
    primary: true,
    weight: 1,
    direction: 'maximize',
  }))

  .timeoutMs(600_000)
  .networkMode('none');

mkdirSync('.lab', { recursive: true });
writeFileSync('.lab/experiment.yaml', builder.toYaml());

const client = new LabClient({ cwd: process.cwd() });
const run = await client.run({
  experiment: '.lab/experiment.yaml',
  executor: 'local_docker',
  materialize: 'outputs_only',
});

console.log(run.run.run_id);
```

## What You Need To Bring

1. A dataset JSONL (`dataset.path`) with one JSON object per line.
2. An agent runtime declaration:
   - `runtime.agent.mode: known_agent_ref`, or
   - `runtime.agent.mode: custom_image`.
   - optional `runtime.agent.adapter` for explicit adapter identity.
3. At least one baseline variant (`.baseline(...)`).
4. Optional treatment variants (`.addVariant(...)`) and metrics (`.metric(...)`).
5. Optional staged dependency files (`.dependencyFileStaging(...)`).

You do not provide a control-plane protocol, runner socket wiring, or runner state handling.

## Runtime Agent Modes

### `known_agent_ref`

Use this when the runner should resolve a pre-registered runtime by id/version.

```ts
builder.agentRef('rex_daemon', '1.2.0', { registry: 'internal' });
```

Runner resolves manifests from:

1. `.lab/agents/<registry>/<id>/<version>.json` (if `registry` provided)
2. `.lab/agents/<id>/<version>.json`

Manifest shape:

```json
{
  "image": "ghcr.io/acme/rex-agent@sha256:...",
  "entrypoint": ["python", "-m", "rex.run_trial"],
  "default_env": {
    "PYTHONUNBUFFERED": "1"
  }
}
```

### `custom_image`

Use this when you provide image + command directly.

```ts
builder.customAgentImage(
  'ghcr.io/acme/rex-agent@sha256:...',
  ['python', '-m', 'rex.run_trial'],
);
```

### `adapter` (optional)

Use this when you want explicit runner adapter identity in metadata/control paths.

```ts
builder.usePrebuiltCodexAdapter();      // runtime.agent.adapter = prebuilt.codex_cli@v1
builder.usePrebuiltRexJesusAdapter();   // runtime.agent.adapter = prebuilt.rex_jesus@v1
builder.useBuiltinAdapter();            // runtime.agent.adapter = builtin.command_contract@v1
```

Or set custom adapter id/version directly:

```ts
builder.agentAdapter('my.custom.adapter', 'v7');
```

You can also set command/env through:

1. `.agentLoop(command)` (sets `custom_image.entrypoint`)
2. `.agentArgs(args)`
3. `.agentEnv(env)`
4. `.agentEnvFromHost(keys)`
5. `.agentAdapter(id, version?)`
6. `.useBuiltinAdapter(version?)`
7. `.usePrebuiltCodexAdapter(version?)`
8. `.usePrebuiltRexJesusAdapter(version?)`

## Command Semantics

1. Runner executes exactly one command per trial.
2. In container mode, command runs inside the selected image with working dir `/agentlab/workspace`.
3. In local mode, command runs in the per-trial workspace directory on host.
4. `runtime.agent` commands are treated as literal tokens; runner does not rewrite them to host paths.
5. `${AGENTLAB_*}` placeholders in command tokens are expanded by runner before launch.

Practical implication: the command must be valid in the runtime environment you selected (image or local process).

## Dependencies: Intuitive Staging

Preferred API:

1. `.dependencyFileStaging(entries)`
2. `.stageDependencyFile(sourceFromHost, destinationPath, options?)`

Entry fields:

1. `source_from_host`: host file path (supports `~`; relative paths resolve from project root, parent of `.lab`).
2. `destination_path`: absolute path exposed in trial filesystem (usually under `/agentlab/deps/...`).
3. `required` (optional, default true): if false, missing source is tolerated.

## What Goes Into The Trial Container

For each trial, runner prepares and mounts:

1. `/agentlab/in` (read-only): `task.json`, `bindings.json`, `dependencies.json`, `policy.json`
2. `/agentlab/workspace` (read-write): workspace copy seeded from project root
3. `/agentlab/deps` (read-write): staged dependency files
4. `/agentlab/out` (read-write): `result.json`, optional `trajectory.jsonl`
5. `/agentlab/state` (read-write): runner internal state and metadata
6. `/dataset` (read-only): dataset copy for the trial

If task boundaries include `mount_references`, dataset packs are additionally mounted read-only to their declared paths.

## Agent Env Contract

Runner sets these env vars for your command:

1. `AGENTLAB_TASK_PATH`
2. `AGENTLAB_BINDINGS_PATH`
3. `AGENTLAB_DEPENDENCIES_PATH`
4. `AGENTLAB_POLICY_PATH`
5. `AGENTLAB_RESULT_PATH`
6. `AGENTLAB_TRAJECTORY_PATH`
7. `AGENTLAB_TIMEOUT_MS`
8. `AGENTLAB_RUN_ID`
9. `AGENTLAB_TRIAL_ID`
10. `AGENTLAB_VARIANT_ID`
11. `AGENTLAB_TASK_ID`
12. `AGENTLAB_REPL_IDX`
13. `AGENTLAB_PREBUILT_ADAPTER` (only for prebuilt adapters)
14. `AGENTLAB_PREBUILT_ADAPTER_ID` (only for prebuilt adapters)

Your loop should write `agent_result_v1` JSON to `AGENTLAB_RESULT_PATH`.

## Running From Another Directory

You can build and run experiments from any folder as long as you pass the right `cwd` and experiment path.

```ts
const client = new LabClient({ cwd: '/absolute/path/to/project' });
await client.run({ experiment: '.lab/experiment.yaml' });
```

Resolution rules:

1. `dataset.path` is resolved relative to the experiment file directory.
2. `runtime.dependencies.*.source_from_host` is resolved relative to project root (parent of `.lab`) when relative.
3. `known_agent_ref` manifests are resolved under that same project root (`.lab/agents/...`).

## ExperimentBuilder API (Primary)

Required before `build()`:

1. `.datasetJsonl(path, opts)`
2. One runtime agent mode:
   - `.agentRef(...)`, or
   - `.agentLoop(...)` / `.customAgentImage(...)`

Common optional setters:

1. `.baseline(id, bindings)`
2. `.addVariant(id, bindings)`
3. `.metric(def)`
4. `.guardrail(def)`
5. `.artifacts({ collect, diff, baseDir? })`
6. `.networkMode('none' | 'full' | 'allowlist_enforced', hosts?)`
7. `.sandboxImage(image)`
8. `.localSandbox()`
9. `.timeoutMs(ms)`

## LabClient API (Primary)

Runner commands:

1. `describe(args)`
2. `run(args)`
3. `runDev(args)`
4. `replay(args)`
5. `fork(args)`
6. `pause(args)`
7. `resume(args)`
8. `publish(args)`

Run artifact readers:

1. `readAnalysis(args)`
2. `readEvidence(args)`
3. `readBenchmark(args)`

Validation helpers:

1. `validateKnobs(args)`
2. `validateHooks(args)`
3. `validateSchema(args)`

## Output Files

Canonical per-trial result is:

1. `.lab/runs/<run_id>/trials/<trial_id>/result.json`

## Current Runtime Notes

1. `networkMode('allowlist_enforced', ...)` is not yet implemented by the Rust container executor.
2. Container reproducibility is strongest when image references are pinned by digest (`image@sha256:...`).
3. `runtime.agent` is the runtime source of truth.
