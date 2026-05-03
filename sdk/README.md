# @agentlab/sdk

TypeScript SDK for authoring AgentLab experiments and invoking the Rust runner.

The SDK is runtime-agent-first:

1. You declare `runtime.agent_runtime` and `policy.task_sandbox`.
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

  // Agent runtime artifact plus agent-runtime image.
  .agentArtifact('./agents/rex-bundle.tar.gz')
  .agentRuntime(
    'ghcr.io/acme/task-sandbox@sha256:0123456789abcdef...',
    ['python', '-m', 'rex.run_trial'],
  )
  .runtimeEnvFromHost(['OPENAI_API_KEY'])

  .baseline('control', { model: 'gpt-4o-mini', prompt: 'v1' })
  .addVariant('treatment', { model: 'gpt-4o-mini', prompt: 'treatment_prompt' })

  .metric(Metric.DURATION_MS)
  .metric(Metric.fromOutput('solved', '/metrics/solved', {
    primary: true,
    weight: 1,
    direction: 'maximize',
  }))

  .timeoutMs(600_000)
  .networkPolicy('none');

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
2. An agent bundle (`runtime.agent_runtime.artifact`) plus a runtime command (`runtime.agent_runtime.command`).
3. An agent-runtime image (`runtime.agent_runtime.image`).
4. At least one baseline variant (`.baseline(...)`).
5. Optional treatment variants (`.addVariant(...)`) and metrics (`.metric(...)`).

You do not provide a control-plane protocol, runner socket wiring, or runner state handling.

## Runtime Command

```ts
builder
  .agentArtifact('./agents/rex-bundle.tar.gz')
  .agentRuntime(
    'ghcr.io/acme/task-sandbox@sha256:...',
    ['python', '-m', 'rex.run_trial'],
  );
```

You can set runtime command/env through:

1. `.agentArtifact(path)` (sets `runtime.agent_runtime.artifact`)
2. `.agentCommand(command)` (sets `runtime.agent_runtime.command`)
3. `.agentRuntime(image, command)` (sets `runtime.agent_runtime.image` and `runtime.agent_runtime.command`)
4. `.appendAgentCommandArgs(args)`
5. `.agentEnv(env)`
6. `.runtimeEnvFromHost(keys)`

## Command Semantics

1. Runner executes exactly one command per trial.
2. `runtime.agent_runtime.command` launches the external agent runtime.
3. `runtime.agent_runtime.image` selects the runtime container image.
4. In local agent-runtime mode, runner rewrites `/agentlab/...` and `/opt/agent/...` command paths to host paths before launch.
5. `${AGENTLAB_*}` placeholders in command tokens are expanded by runner before launch.

Practical implication: the command must be valid in the runtime environment you selected (image or local process).

## Runtime Files

If your runtime needs config or support files, package them inside the agent artifact. The SDK no longer exposes authored host-path staging fields.

## What Goes Into The Trial Container

For each trial, runner prepares:

1. `/agentlab/in/trial_input.json`
2. `/agentlab/in/grader_input.json`
3. `/agentlab/out/result.json`
4. `/agentlab/out/raw_grader_output.json`
5. `/agentlab/out/mapped_grader_output.json`
6. optional `/agentlab/out/trajectory.jsonl`
7. the declared task `workdir`, mounted as the runnable workspace root

Task datasets must compile into `task_row_v1` rows with:

1. `id`
2. `image`
3. `workdir`
4. `task`
5. `materialization`
6. optional `time_limit_ms`

## Agent Env Contract

Runner sets these env vars for your command:

1. `AGENTLAB_TRIAL_INPUT_PATH`
2. `AGENTLAB_RESULT_PATH`
3. `AGENTLAB_TRAJECTORY_PATH`
4. `AGENTLAB_TIMEOUT_MS`
5. `AGENTLAB_RUN_ID`
6. `AGENTLAB_TRIAL_ID`
7. `AGENTLAB_VARIANT_ID`
8. `AGENTLAB_TASK_ID`
9. `AGENTLAB_REPL_IDX`

Your loop should write `artifact_envelope_v1` JSON to `AGENTLAB_RESULT_PATH`.

## Running From Another Directory

You can build and run experiments from any folder as long as you pass the right `cwd` and experiment path.

```ts
const client = new LabClient({ cwd: '/absolute/path/to/project' });
await client.run({ experiment: '.lab/experiment.yaml' });
```

Resolution rules:

1. `dataset.path` is resolved relative to the experiment file directory.

## ExperimentBuilder API (Primary)

Required before `build()`:

1. `.datasetJsonl(path, opts)`
2. `.agentArtifact(path)`
3. `.agentCommand(...)` or `.agentRuntime(...)`

Common optional setters:

1. `.baseline(id, bindings)`
2. `.addVariant(id, bindings)`
3. `.metric(def)`
4. `.guardrail(def)`
5. `.artifacts({ collect, diff, baseDir? })`
6. `.networkPolicy('none' | 'full' | 'allowlist_enforced', hosts?)`
7. `.timeoutMs(ms)`

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

1. `networkPolicy('allowlist_enforced', ...)` is not yet implemented by the Rust container executor.
2. Container reproducibility is strongest when image references are pinned by digest (`image@sha256:...`).
3. `runtime.agent_runtime` is the runtime source of truth.
