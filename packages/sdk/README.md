# @agentlab/sdk

TypeScript SDK for driving the AgentLab Rust runner via stable JSON CLI responses.

## Install

```bash
npm install @agentlab/sdk
```

For local development in this repository:

```bash
cd packages/sdk
npm install
npm run build
```

## Runner Discovery

`LabClient` resolves the runner binary in this order:

1. `runnerBin` option
2. `AGENTLAB_RUNNER_BIN` env var
3. default `lab`

## Usage

```ts
import { ExperimentBuilder, LabClient } from '@agentlab/sdk';

const client = new LabClient({ runnerBin: '/path/to/lab' });

const exp = ExperimentBuilder.create('exp_1', 'Prompt A/B')
  .datasetJsonl('tasks.jsonl', { limit: 50 })
  .harnessCli(['node', './harness.js', 'run'], { integrationLevel: 'cli_events' })
  .baseline('base', { prompt: 'prompt:v1' })
  .addVariant('prompt_v2', { prompt: 'prompt:v2' })
  .build();

// Write exp.toYaml() to .lab/experiment.yaml, then:
const summary = await client.describe({ experiment: '.lab/experiment.yaml' });
const run = await client.runExperiment({ experiment: '.lab/experiment.yaml' });
console.log(summary.summary.total_trials, run.run.run_id);
```
