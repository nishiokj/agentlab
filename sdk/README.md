# @agentlab/sdk

TypeScript SDK for defining experiments and driving the AgentLab Rust runner. Build experiment configs with a fluent API, execute them through a typed client, get structured JSON results.

## Install

```bash
npm install @agentlab/sdk
```

Local development:

```bash
cd sdk && npm install && npm run build && npm test
```

## Quick Start

```ts
import { ExperimentBuilder, LabClient } from '@agentlab/sdk';
import { writeFileSync, mkdirSync } from 'node:fs';

// 1. Define the experiment
const builder = ExperimentBuilder.create('prompt_ab', 'Prompt A/B Test')
  .description('Compare prompt v1 vs v2 on coding tasks')

  // Your dataset — path relative to the directory containing experiment.yaml.
  .datasetJsonl('./data/tasks.jsonl', {
    suiteId: 'coding_tasks',
    splitId: 'dev',
    limit: 50,
  })

  // Your harness — the command the runner invokes for EACH trial.
  // Path is relative to the project root (parent of .lab/).
  //
  //   Node:   ['node', './src/harness/run-trial.js']
  //   Python: ['python', '-m', 'my_agent.harness']
  //   Binary: ['./bin/evaluate']
  //
  // If this path is wrong, every trial fails.
  .harnessCli(
    ['node', './src/harness/run-trial.js'],
    { integrationLevel: 'cli_events' }
  )

  .sanitizationProfile('hermetic_functional_v2')
  .replications(3)
  .randomSeed(42)

  .baseline('control', { model: 'gpt-4o', temperature: 0.0 })
  .addVariant('treatment', { model: 'gpt-4o', temperature: 0.7 })

  .primaryMetrics(['success', 'accuracy'])
  .secondaryMetrics(['latency_ms', 'cost_usd'])
  .networkMode('allowlist_enforced', ['api.openai.com']);

// 2. Write config to disk
mkdirSync('.lab', { recursive: true });
writeFileSync('.lab/experiment.yaml', builder.toYaml());

// 3. Validate and run
const client = new LabClient();

const summary = await client.describe({ experiment: '.lab/experiment.yaml' });
console.log(`Planned: ${summary.summary.total_trials} trials`);

const run = await client.runExperiment({ experiment: '.lab/experiment.yaml' });
console.log(`Done: ${run.run.run_id}`);
```

## ExperimentBuilder

Fluent API for building `ExperimentSpec` objects. All required fields must be explicitly set — `build()` validates completeness and throws listing any missing fields.

```ts
const builder = ExperimentBuilder.create('id', 'Name')
```

### Required methods

These must be called before `build()` or `toYaml()`:

| Method | What it sets |
|---|---|
| `.datasetJsonl(path, opts)` | Dataset source. `opts` requires `suiteId`, `splitId`, `limit`. |
| `.harnessCli(command, opts)` | Harness command array. `opts` requires `integrationLevel`. |
| `.sanitizationProfile(value)` | Sanitization profile name (e.g. `'hermetic_functional_v2'`). |
| `.replications(n)` | How many times each (task, variant) pair runs. |
| `.randomSeed(n)` | Seed for trial ordering reproducibility. |

### Optional methods

| Method | What it sets |
|---|---|
| `.description(text)` | Experiment description. |
| `.owner(name)` | Experiment owner. |
| `.tags(list)` | Tag array. |
| `.baseline(id, bindings)` | Baseline variant with parameter bindings. Default: `{ variant_id: 'base', bindings: {} }`. |
| `.addVariant(id, bindings)` | Additional variant. Call multiple times for multiple variants. |
| `.maxConcurrency(n)` | Parallel trial limit. Default: `1`. |
| `.primaryMetrics(names)` | Primary success metrics for analysis. |
| `.secondaryMetrics(names)` | Secondary metrics for analysis. |
| `.networkMode(mode, hosts?)` | `'none'` (default), `'full'`, or `'allowlist_enforced'` with allowed hosts. |
| `.sandboxImage(image)` | Docker image name. Sets sandbox mode to `container`. |
| `.localSandbox()` | Run without container isolation (default). |

### Terminal methods

| Method | What it returns |
|---|---|
| `.build()` | Deep-copied `ExperimentSpec`. Throws if required fields are missing. |
| `.toYaml()` | YAML string of the spec. Validates completeness first. |

All setters return `this` for chaining.

## LabClient

Spawns the Rust `lab` binary and parses structured JSON responses.

```ts
const client = new LabClient({
  runnerBin: '/path/to/lab',   // or set AGENTLAB_RUNNER_BIN env var
  cwd: '/project/root',
  env: { OPENAI_API_KEY: '...' },
});
```

### Runner discovery

Resolves the binary in order:

1. `runnerBin` constructor option
2. `AGENTLAB_RUNNER_BIN` environment variable
3. `lab` (assumes on `PATH`)

### Commands

| Method | Returns | Description |
|---|---|---|
| `describe(args)` | `DescribeResponse` | Dry-run: planned trials and resolved config |
| `run(args)` | `RunResponse` | Execute trials (optional `container` flag) |
| `runDev(args)` | `RunResponse` | Dev run: full network, optional `setup` command |
| `runExperiment(args)` | `RunResponse` | Strict run: network mode must be `none` |
| `replay(args)` | `ReplayResponse` | Re-execute a trial from run artifacts |
| `fork(args)` | `ForkResponse` | Fork a trial at a checkpoint with binding overrides |
| `pause(args)` | `PauseResponse` | Cooperative pause via checkpoint+stop handshake |
| `resume(args)` | `ResumeResponse` | Resume a paused trial |
| `publish(args)` | `PublishResponse` | Create debug bundle from a run |
| `validateKnobs(args)` | `ValidateResponse` | Validate parameter overrides against manifest |
| `validateHooks(args)` | `ValidateResponse` | Validate event stream against harness manifest |
| `validateSchema(args)` | `ValidateResponse` | Validate JSON file against schema |

All commands accept per-call `cwd` and `env` overrides.

### Control lifecycle

```ts
const client = new LabClient();
const runDir = '.lab/runs/run_20260211_120000';

// Pause at next safe boundary
const paused = await client.pause({
  runDir,
  trialId: 'trial_001',
  label: 'before_tool_call',
  timeoutSeconds: 90,
});

// Fork from checkpoint with modified bindings
const forked = await client.fork({
  runDir,
  fromTrial: paused.pause.trial_id,
  at: 'checkpoint:before_tool_call',
  set: { model: 'gpt-4.1-mini', temperature: 0.2 },
  strict: true,
});

// Resume the original trial
const resumed = await client.resume({
  runDir,
  trialId: paused.pause.trial_id,
  label: 'before_tool_call',
  set: { max_steps: 50 },
});

// Replay for validation
const replayed = await client.replay({
  runDir,
  trialId: paused.pause.trial_id,
  strict: true,
});
```

### Error handling

All runner failures throw `LabRunnerError`:

```ts
import { LabRunnerError } from '@agentlab/sdk';

try {
  await client.run({ experiment: 'experiment.yaml' });
} catch (err) {
  if (err instanceof LabRunnerError) {
    err.code;      // 'bad_config', 'spawn_failed', 'invalid_json', etc.
    err.message;   // Human-readable description
    err.command;   // Full command array that was spawned
    err.stderr;    // Runner stderr output
    err.exitCode;  // Process exit code (if available)
    err.details;   // Structured error details (if available)
  }
}
```

## Exports

```ts
// Classes
export { LabClient, LabRunnerError } from '@agentlab/sdk';
export { ExperimentBuilder } from '@agentlab/sdk';

// Types
export type {
  ExperimentSpec,
  DatasetJsonlOptions,
  HarnessCliOptions,
  LabClientOptions,
  DescribeArgs,
  DescribeResponse,
  ExperimentSummary,
  RunArgs,
  RunDevArgs,
  RunExperimentArgs,
  RunResponse,
  ReplayArgs,
  ReplayResponse,
  ForkArgs,
  ForkResponse,
  PauseArgs,
  PauseResponse,
  ResumeArgs,
  ResumeResponse,
  PublishArgs,
  PublishResponse,
  KnobsValidateArgs,
  HooksValidateArgs,
  SchemaValidateArgs,
  ValidateResponse,
  LabErrorEnvelope,
  LabErrorPayload,
} from '@agentlab/sdk';
```
