import assert from 'node:assert/strict';
import test, { describe } from 'node:test';

import {
  ExperimentBuilder,
  ExperimentType,
  Metric,
} from '../src/experiment-builder.js';
import type { DesignPolicies, MetricDef } from '../src/experiment-builder.js';

function validBuilder(): ExperimentBuilder {
  return ExperimentBuilder.create('exp-1', 'My Experiment')
    .datasetJsonl('tasks.jsonl', { suiteId: 'suite', splitId: 'dev', limit: 50 })
    .agentArtifact('./agents/agent-runtime.tar.gz')
    .agentRuntime('ghcr.io/acme/agent-runtime:latest', ['node', './agent_loop.js', 'run']);
}

function validFromBuilder(policies: DesignPolicies): ExperimentBuilder {
  return ExperimentBuilder.from(policies)
    .id('exp-from')
    .name('From Preset')
    .datasetJsonl('tasks.jsonl', { suiteId: 'suite', splitId: 'dev', limit: 50 })
    .agentArtifact('./agents/agent-runtime.tar.gz')
    .agentRuntime('ghcr.io/acme/agent-runtime:latest', ['node', './agent_loop.js', 'run']);
}

describe('ExperimentBuilder defaults', () => {
  const spec = validBuilder().build();

  test('top-level version field is omitted', () => {
    assert.equal('version' in spec, false);
  });

  test('design defaults are set', () => {
    assert.equal(spec.design.sanitization_profile, 'hermetic_functional');
    assert.equal(spec.design.replications, 1);
    assert.equal(spec.design.random_seed, 1);
    assert.equal(spec.design.comparison, 'paired');
    assert.equal(spec.design.shuffle_tasks, true);
    assert.equal(spec.design.max_concurrency, 1);
    assert.equal(spec.dataset.schema_version, 'task_spec_v1');
  });

  test('runtime defaults are set', () => {
    assert.deepEqual(spec.runtime.agent_runtime.command, ['node', './agent_loop.js', 'run']);
    assert.equal(spec.runtime.agent_runtime.artifact, './agents/agent-runtime.tar.gz');
    assert.equal(spec.runtime.agent_runtime.image, 'ghcr.io/acme/agent-runtime:latest');
    assert.equal(spec.runtime.agent_runtime.network, 'none');
    assert.equal(spec.policy.timeout_ms, 600_000);
    assert.equal(spec.policy.task_sandbox.profile, 'default');
    assert.equal(spec.policy.task_sandbox.network, 'none');
    assert.deepEqual(spec.policy.task_sandbox.allowed_hosts, []);
  });

  test('baseline and variants defaults', () => {
    assert.equal(spec.baseline.variant_id, 'base');
    assert.deepEqual(spec.baseline.bindings, {});
    assert.deepEqual(spec.variant_plan, []);
  });
});

describe('ExperimentBuilder validation', () => {
  test('build throws when required fields are missing', () => {
    assert.throws(
      () => ExperimentBuilder.create('e', 'n').build(),
      (err: Error) => {
        assert.ok(err.message.includes('required fields not set'));
        assert.ok(err.message.includes('dataset path'));
        assert.ok(err.message.includes('runtime.agent_runtime.command'));
        assert.ok(err.message.includes('runtime.agent_runtime.artifact'));
        return true;
      },
    );
  });

  test('build throws when id/name are missing on from()', () => {
    assert.throws(
      () => ExperimentBuilder.from(ExperimentType.AB_TEST)
        .datasetJsonl('tasks.jsonl', { suiteId: 'suite', splitId: 'dev', limit: 1 })
        .agentCommand(['node', './agent_loop.js'])
        .build(),
      (err: Error) => {
        assert.ok(err.message.includes('experiment id'));
        assert.ok(err.message.includes('experiment name'));
        return true;
      },
    );
  });

  test('paired policies require a treatment variant', () => {
    assert.throws(
      () => validBuilder().policies(ExperimentType.AB_TEST).build(),
      (err: Error) => {
        assert.ok(err.message.includes('policy coherence errors'));
        assert.ok(err.message.includes('paired comparison requires at least one treatment variant'));
        assert.ok(err.message.includes('paired_interleaved scheduling requires at least 2 variants'));
        return true;
      },
    );
  });

  test('paired policies pass when at least one treatment exists', () => {
    const spec = validBuilder()
      .policies(ExperimentType.AB_TEST)
      .addVariant('treatment-a', { model: 'x' })
      .build();
    assert.equal(spec.variant_plan.length, 1);
  });

});

describe('ExperimentBuilder runtime APIs', () => {
  test('agentCommand sets entrypoint', () => {
    const spec = validBuilder().agentCommand(['python', 'agent_loop.py']).build();
    assert.deepEqual(spec.runtime.agent_runtime.command, ['python', 'agent_loop.py']);
  });

  test('agentRuntime sets runtime image and command', () => {
    const spec = validBuilder().agentRuntime('ghcr.io/acme/agent:latest', ['python', 'run.py']).build();
    assert.deepEqual(spec.runtime.agent_runtime.command, ['python', 'run.py']);
    assert.equal(spec.runtime.agent_runtime.image, 'ghcr.io/acme/agent:latest');
  });

  test('networkPolicy and timeoutMs are applied', () => {
    const spec = validBuilder()
      .networkPolicy('allowlist_enforced', ['api.openai.com'])
      .timeoutMs(42_000)
      .build();
    assert.equal(spec.runtime.agent_runtime.network, 'allowlist_enforced');
    assert.equal(spec.policy.task_sandbox.network, 'allowlist_enforced');
    assert.deepEqual(spec.policy.task_sandbox.allowed_hosts, ['api.openai.com']);
    assert.equal(spec.policy.timeout_ms, 42_000);
  });

  test('networkPolicy llm_egress keeps task sandbox network isolated', () => {
    const spec = validBuilder()
      .networkPolicy('llm_egress', ['api.openai.com'])
      .build();
    assert.equal(spec.runtime.agent_runtime.network, 'llm_egress');
    assert.equal(spec.policy.task_sandbox.network, 'none');
    assert.deepEqual(spec.policy.task_sandbox.allowed_hosts, ['api.openai.com']);
  });

  test('localSandbox is rejected after hard cutover', () => {
    assert.throws(
      () => validBuilder().localSandbox(),
      /localSandbox\(\) was removed/,
    );
  });
});

describe('ExperimentBuilder fluent setters', () => {
  test('description/owner/tags', () => {
    const inputTags = ['a', 'b'];
    const spec = validBuilder()
      .description('desc')
      .owner('owner')
      .tags(inputTags)
      .build();

    inputTags.push('mutated');
    assert.equal(spec.experiment.description, 'desc');
    assert.equal(spec.experiment.owner, 'owner');
    assert.deepEqual(spec.experiment.tags, ['a', 'b']);
  });

  test('baseline and variants copy bindings', () => {
    const baselineBindings = { model: 'gpt-4o-mini' };
    const variantBindings = { model: 'gpt-4.1' };

    const spec = validBuilder()
      .baseline('control', baselineBindings)
      .addVariant('treatment', variantBindings)
      .build();

    baselineBindings.model = 'mutated';
    variantBindings.model = 'mutated';

    assert.equal(spec.baseline.variant_id, 'control');
    assert.deepEqual(spec.baseline.bindings, { model: 'gpt-4o-mini' });
    assert.equal(spec.variant_plan[0].variant_id, 'treatment');
    assert.deepEqual(spec.variant_plan[0].bindings, { model: 'gpt-4.1' });
  });

  test('datasetJsonl accepts custom schema version', () => {
    const spec = validBuilder()
      .datasetJsonl('tasks2.jsonl', {
        suiteId: 'suite-2',
        splitId: 'test',
        limit: 10,
        schemaVersion: 'dataset_custom',
      })
      .build();

    assert.equal(spec.dataset.path, 'tasks2.jsonl');
    assert.equal(spec.dataset.suite_id, 'suite-2');
    assert.equal(spec.dataset.split_id, 'test');
    assert.equal(spec.dataset.limit, 10);
    assert.equal(spec.dataset.schema_version, 'dataset_custom');
  });
});

describe('Metrics, guardrails, and artifacts', () => {
  test('metric() replaces by id', () => {
    const builder = validBuilder();

    const first = Metric.fromOutput('solved', '/solved', {
      weight: 1,
      primary: true,
      direction: 'maximize',
    });
    const replacement = Metric.fromOutput('solved', '/score', {
      weight: 2,
      primary: false,
      direction: 'maximize',
    });

    const spec = builder.metric(first).metric(replacement).build();
    assert.equal(spec.metrics.length, 1);
    assert.equal(spec.metrics[0].json_pointer, '/score');
    assert.equal(spec.metrics[0].weight, 2);
  });

  test('guardrail() replaces by metric_id', () => {
    const spec = validBuilder()
      .guardrail({ metric_id: 'tokens_in', max: 10_000 })
      .guardrail({ metric_id: 'tokens_in', max: 5_000 })
      .build();

    assert.equal(spec.guardrails?.length, 1);
    assert.equal(spec.guardrails?.[0].max, 5_000);
  });

  test('artifacts() copies collect and sets defaults', () => {
    const collect = ['workspace/**', 'logs/**'];
    const spec = validBuilder().artifacts({ collect }).build();
    collect.push('mutated/**');

    assert.deepEqual(spec.artifacts?.collect, ['workspace/**', 'logs/**']);
    assert.equal(spec.artifacts?.diff, false);
  });

  test('predefined metrics have expected defaults', () => {
    const predefined: MetricDef[] = [
      Metric.DURATION_MS,
      Metric.EXIT_CODE,
      Metric.TOKENS_IN,
      Metric.TOKENS_OUT,
      Metric.STEP_COUNT,
      Metric.TURN_COUNT,
      Metric.TOOL_CALL_COUNT,
      Metric.FILES_CREATED,
      Metric.FILES_MODIFIED,
      Metric.DIFF_BYTES,
      Metric.DIFF_LINES,
    ];

    for (const metric of predefined) {
      assert.equal(metric.weight, 0, `${metric.id} weight`);
      assert.equal(metric.primary, false, `${metric.id} primary`);
    }
  });
});

describe('ExperimentBuilder.from()', () => {
  test('from preset applies design policies', () => {
    const spec = validFromBuilder(ExperimentType.PARAMETER_SWEEP).build();
    assert.equal(spec.design.policies?.scheduling, 'variant_sequential');
    assert.equal(spec.design.policies?.comparison, 'unpaired');
    assert.equal(spec.design.comparison, 'unpaired');
  });

  test('policies() overrides from preset', () => {
    const custom: DesignPolicies = {
      scheduling: 'randomized',
      comparison: 'none',
      retry: { max_attempts: 2, retry_on: ['timeout'] },
      pruning: { max_consecutive_failures: 3 },
    };

    const spec = validFromBuilder(ExperimentType.AB_TEST)
      .policies(custom)
      .build();

    assert.equal(spec.design.policies?.scheduling, 'randomized');
    assert.equal(spec.design.policies?.comparison, 'none');
    assert.equal(spec.design.comparison, 'none');
    assert.equal(spec.design.policies?.retry.max_attempts, 2);
    assert.deepEqual(spec.design.policies?.retry.retry_on, ['timeout']);
    assert.equal(spec.design.policies?.pruning?.max_consecutive_failures, 3);
  });
});

describe('YAML serialization', () => {
  test('toYaml() renders key sections', () => {
    const yaml = validBuilder()
      .baseline('control', { model: 'gpt-4o-mini' })
      .addVariant('treatment', { model: 'gpt-4.1' })
      .metric(Metric.DURATION_MS)
      .toYaml();

    assert.ok(!yaml.includes('version: "0.5"'));
    assert.ok(yaml.includes('runtime:'));
    assert.ok(yaml.includes('agent_runtime:'));
    assert.ok(yaml.includes('baseline:'));
    assert.ok(yaml.includes('variant_plan:'));
    assert.ok(yaml.includes('metrics:'));
  });
});
