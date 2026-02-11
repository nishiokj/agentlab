import assert from 'node:assert/strict';
import test, { describe } from 'node:test';

import { ExperimentBuilder } from '../src/experiment-builder.js';
import type { ExperimentSpec } from '../src/experiment-builder.js';

// Helper: create a fully configured builder that passes build() validation
function validBuilder(): ExperimentBuilder {
  return ExperimentBuilder.create('exp-1', 'My Experiment')
    .datasetJsonl('tasks.jsonl', { suiteId: 'suite', splitId: 'dev', limit: 50 })
    .sanitizationProfile('hermetic_functional_v2')
    .replications(1)
    .randomSeed(1337)
    .harnessCli(['node', './harness.js', 'run'], { integrationLevel: 'cli_basic' });
}

// ---------------------------------------------------------------------------
// Structural defaults (fields that have reasonable defaults without setters)
// ---------------------------------------------------------------------------
describe('ExperimentBuilder structural defaults', () => {
  const spec = validBuilder().build();

  test('version is 0.3', () => {
    assert.equal(spec.version, '0.3');
  });

  test('experiment id and name are set', () => {
    assert.equal(spec.experiment.id, 'exp-1');
    assert.equal(spec.experiment.name, 'My Experiment');
  });

  test('description and owner are undefined by default', () => {
    assert.equal(spec.experiment.description, undefined);
    assert.equal(spec.experiment.owner, undefined);
  });

  test('default comparison is paired', () => {
    assert.equal(spec.design.comparison, 'paired');
  });

  test('default shuffle_tasks is true', () => {
    assert.equal(spec.design.shuffle_tasks, true);
  });

  test('default max_concurrency is 1', () => {
    assert.equal(spec.design.max_concurrency, 1);
  });

  test('default analysis plan has primary and secondary metrics', () => {
    assert.deepEqual(spec.analysis_plan.primary_metrics, ['success']);
    assert.deepEqual(spec.analysis_plan.secondary_metrics, ['latency_ms']);
  });

  test('default tests are generated for all metrics', () => {
    assert.ok('success' in spec.analysis_plan.tests);
    assert.ok('latency_ms' in spec.analysis_plan.tests);
  });

  test('default baseline', () => {
    assert.equal(spec.baseline.variant_id, 'base');
    assert.deepEqual(spec.baseline.bindings, {});
  });

  test('default variant_plan is empty', () => {
    assert.deepEqual(spec.variant_plan, []);
  });

  test('default harness mode is cli', () => {
    assert.equal(spec.runtime.harness.mode, 'cli');
  });

  test('default input/output paths', () => {
    assert.equal(spec.runtime.harness.input_path, '/out/trial_input.json');
    assert.equal(spec.runtime.harness.output_path, '/out/trial_output.json');
  });

  test('default control plane', () => {
    assert.equal(spec.runtime.harness.control_plane.mode, 'file');
    assert.equal(spec.runtime.harness.control_plane.path, '/state/lab_control.json');
  });

  test('default sandbox is local mode', () => {
    assert.equal(spec.runtime.sandbox.mode, 'local');
  });

  test('default network is none', () => {
    assert.equal(spec.runtime.network.mode, 'none');
    assert.deepEqual(spec.runtime.network.allowed_hosts, []);
  });

  test('default validity flags', () => {
    assert.equal(spec.validity.fail_on_state_leak, true);
    assert.equal(spec.validity.fail_on_profile_invariant_violation, true);
  });
});

// ---------------------------------------------------------------------------
// build() validation
// ---------------------------------------------------------------------------
describe('ExperimentBuilder build() validation', () => {
  test('build() throws when no required fields are set', () => {
    assert.throws(
      () => ExperimentBuilder.create('e', 'n').build(),
      (err: Error) => {
        assert.ok(err.message.includes('ExperimentBuilder: required fields not set'));
        assert.ok(err.message.includes('dataset path'));
        assert.ok(err.message.includes('dataset suite_id'));
        assert.ok(err.message.includes('dataset split_id'));
        assert.ok(err.message.includes('dataset limit'));
        assert.ok(err.message.includes('sanitization_profile'));
        assert.ok(err.message.includes('replications'));
        assert.ok(err.message.includes('random_seed'));
        assert.ok(err.message.includes('harness command'));
        assert.ok(err.message.includes('harness integration_level'));
        return true;
      },
    );
  });

  test('build() throws listing only missing fields', () => {
    assert.throws(
      () =>
        ExperimentBuilder.create('e', 'n')
          .datasetJsonl('tasks.jsonl', { suiteId: 's', splitId: 'dev', limit: 10 })
          .sanitizationProfile('hermetic_functional_v2')
          .replications(1)
          .randomSeed(42)
          .build(),
      (err: Error) => {
        // harness command and integration_level still missing
        assert.ok(err.message.includes('harness command'));
        assert.ok(err.message.includes('harness integration_level'));
        // these should NOT be listed
        assert.ok(!err.message.includes('dataset path'));
        assert.ok(!err.message.includes('replications'));
        return true;
      },
    );
  });

  test('build() succeeds when all required fields are set', () => {
    const spec = validBuilder().build();
    assert.equal(spec.version, '0.3');
    assert.equal(spec.dataset.path, 'tasks.jsonl');
    assert.equal(spec.design.replications, 1);
    assert.equal(spec.runtime.harness.integration_level, 'cli_basic');
  });

  test('toYaml() also validates', () => {
    assert.throws(
      () => ExperimentBuilder.create('e', 'n').toYaml(),
      (err: Error) => err.message.includes('required fields not set'),
    );
  });
});

// ---------------------------------------------------------------------------
// Fluent setters
// ---------------------------------------------------------------------------
describe('ExperimentBuilder fluent API', () => {
  test('description()', () => {
    const spec = validBuilder().description('custom desc').build();
    assert.equal(spec.experiment.description, 'custom desc');
  });

  test('owner()', () => {
    const spec = validBuilder().owner('alice').build();
    assert.equal(spec.experiment.owner, 'alice');
  });

  test('tags()', () => {
    const spec = validBuilder().tags(['a', 'b']).build();
    assert.deepEqual(spec.experiment.tags, ['a', 'b']);
  });

  test('tags() copies the input array', () => {
    const input = ['x'];
    const spec = validBuilder().tags(input).build();
    input.push('y');
    assert.deepEqual(spec.experiment.tags, ['x']);
  });

  test('datasetJsonl() sets all required fields', () => {
    const spec = validBuilder()
      .datasetJsonl('my.jsonl', {
        suiteId: 'suite-2',
        splitId: 'test',
        limit: 10,
        schemaVersion: 'v2',
      })
      .build();
    assert.equal(spec.dataset.path, 'my.jsonl');
    assert.equal(spec.dataset.suite_id, 'suite-2');
    assert.equal(spec.dataset.split_id, 'test');
    assert.equal(spec.dataset.limit, 10);
    assert.equal(spec.dataset.schema_version, 'v2');
  });

  test('harnessCli() sets command and integrationLevel', () => {
    const spec = validBuilder()
      .harnessCli(['python', 'harness.py'], { integrationLevel: 'cli_events' })
      .build();
    assert.deepEqual(spec.runtime.harness.command, ['python', 'harness.py']);
    assert.equal(spec.runtime.harness.integration_level, 'cli_events');
  });

  test('harnessCli() copies the command array', () => {
    const cmd = ['python', 'h.py'];
    const spec = validBuilder()
      .harnessCli(cmd, { integrationLevel: 'cli_basic' })
      .build();
    cmd.push('--extra');
    assert.deepEqual(spec.runtime.harness.command, ['python', 'h.py']);
  });

  test('harnessCli() with custom paths', () => {
    const spec = validBuilder()
      .harnessCli(['node', 'run.js'], {
        integrationLevel: 'cli_events',
        inputPath: '/custom/in.json',
        outputPath: '/custom/out.json',
      })
      .build();
    assert.equal(spec.runtime.harness.integration_level, 'cli_events');
    assert.equal(spec.runtime.harness.input_path, '/custom/in.json');
    assert.equal(spec.runtime.harness.output_path, '/custom/out.json');
  });

  test('baseline()', () => {
    const spec = validBuilder()
      .baseline('control', { model: 'gpt-4' })
      .build();
    assert.equal(spec.baseline.variant_id, 'control');
    assert.deepEqual(spec.baseline.bindings, { model: 'gpt-4' });
  });

  test('baseline() copies bindings', () => {
    const bindings = { k: 'v' };
    const spec = validBuilder().baseline('b', bindings).build();
    bindings.k = 'mutated';
    assert.equal(spec.baseline.bindings.k, 'v');
  });

  test('addVariant() appends to variant_plan', () => {
    const spec = validBuilder()
      .addVariant('v1', { temp: 0.5 })
      .addVariant('v2', { temp: 1.0 })
      .build();
    assert.equal(spec.variant_plan.length, 2);
    assert.equal(spec.variant_plan[0].variant_id, 'v1');
    assert.deepEqual(spec.variant_plan[0].bindings, { temp: 0.5 });
    assert.equal(spec.variant_plan[1].variant_id, 'v2');
    assert.deepEqual(spec.variant_plan[1].bindings, { temp: 1.0 });
  });

  test('addVariant() copies bindings', () => {
    const bindings = { k: 1 };
    const spec = validBuilder().addVariant('v', bindings).build();
    bindings.k = 999;
    assert.equal(spec.variant_plan[0].bindings.k, 1);
  });

  test('replications()', () => {
    const spec = validBuilder().replications(5).build();
    assert.equal(spec.design.replications, 5);
  });

  test('sanitizationProfile()', () => {
    const spec = validBuilder().sanitizationProfile('custom_profile').build();
    assert.equal(spec.design.sanitization_profile, 'custom_profile');
  });

  test('randomSeed()', () => {
    const spec = validBuilder().randomSeed(42).build();
    assert.equal(spec.design.random_seed, 42);
  });

  test('maxConcurrency()', () => {
    const spec = validBuilder().maxConcurrency(4).build();
    assert.equal(spec.design.max_concurrency, 4);
  });

  test('primaryMetrics() regenerates tests', () => {
    const spec = validBuilder()
      .primaryMetrics(['accuracy', 'f1'])
      .build();
    assert.deepEqual(spec.analysis_plan.primary_metrics, ['accuracy', 'f1']);
    assert.ok('accuracy' in spec.analysis_plan.tests);
    assert.ok('f1' in spec.analysis_plan.tests);
    // secondary still present in tests
    assert.ok('latency_ms' in spec.analysis_plan.tests);
  });

  test('secondaryMetrics() regenerates tests', () => {
    const spec = validBuilder()
      .secondaryMetrics(['cost', 'tokens'])
      .build();
    assert.deepEqual(spec.analysis_plan.secondary_metrics, ['cost', 'tokens']);
    assert.ok('cost' in spec.analysis_plan.tests);
    assert.ok('tokens' in spec.analysis_plan.tests);
    // primary still present
    assert.ok('success' in spec.analysis_plan.tests);
  });

  test('duplicate metrics are deduplicated in tests', () => {
    const spec = validBuilder()
      .primaryMetrics(['shared'])
      .secondaryMetrics(['shared'])
      .build();
    assert.ok('shared' in spec.analysis_plan.tests);
    assert.equal(Object.keys(spec.analysis_plan.tests).length, 1);
  });

  test('networkMode() with allowlist', () => {
    const spec = validBuilder()
      .networkMode('allowlist_enforced', ['api.openai.com'])
      .build();
    assert.equal(spec.runtime.network.mode, 'allowlist_enforced');
    assert.deepEqual(spec.runtime.network.allowed_hosts, ['api.openai.com']);
  });

  test('networkMode() full', () => {
    const spec = validBuilder().networkMode('full').build();
    assert.equal(spec.runtime.network.mode, 'full');
    assert.deepEqual(spec.runtime.network.allowed_hosts, []);
  });

  test('sandboxImage()', () => {
    const spec = validBuilder()
      .sandboxImage('python:3.12')
      .build();
    assert.equal(spec.runtime.sandbox.mode, 'container');
    assert.equal(spec.runtime.sandbox.image, 'python:3.12');
  });

  test('localSandbox() strips container fields', () => {
    const spec = validBuilder().sandboxImage('x').localSandbox().build();
    assert.equal(spec.runtime.sandbox.mode, 'local');
    assert.equal(spec.runtime.sandbox.image, undefined);
    assert.equal(spec.runtime.sandbox.engine, undefined);
    assert.equal(spec.runtime.sandbox.hardening, undefined);
  });

  test('sandboxImage() after localSandbox() restores container mode', () => {
    const spec = validBuilder()
      .localSandbox()
      .sandboxImage('ubuntu:22.04')
      .build();
    assert.equal(spec.runtime.sandbox.mode, 'container');
    assert.equal(spec.runtime.sandbox.image, 'ubuntu:22.04');
  });
});

// ---------------------------------------------------------------------------
// Chaining
// ---------------------------------------------------------------------------
describe('ExperimentBuilder chaining', () => {
  test('all fluent methods return the same builder (this)', () => {
    const builder = validBuilder();
    assert.equal(builder.description('d'), builder);
    assert.equal(builder.owner('o'), builder);
    assert.equal(builder.tags([]), builder);
    assert.equal(builder.datasetJsonl('p', { suiteId: 's', splitId: 'd', limit: 1 }), builder);
    assert.equal(builder.harnessCli(['x'], { integrationLevel: 'cli_basic' }), builder);
    assert.equal(builder.baseline('b', {}), builder);
    assert.equal(builder.addVariant('v', {}), builder);
    assert.equal(builder.replications(1), builder);
    assert.equal(builder.sanitizationProfile('p'), builder);
    assert.equal(builder.randomSeed(1), builder);
    assert.equal(builder.maxConcurrency(1), builder);
    assert.equal(builder.primaryMetrics([]), builder);
    assert.equal(builder.secondaryMetrics([]), builder);
    assert.equal(builder.networkMode('none'), builder);
    assert.equal(builder.sandboxImage('x'), builder);
    assert.equal(builder.localSandbox(), builder);
  });
});

// ---------------------------------------------------------------------------
// build() immutability
// ---------------------------------------------------------------------------
describe('ExperimentBuilder build() immutability', () => {
  test('build() returns a deep copy', () => {
    const builder = validBuilder()
      .addVariant('v1', { k: 'original' });
    const spec1 = builder.build();
    const spec2 = builder.build();

    // Different object references
    assert.notEqual(spec1, spec2);
    assert.notEqual(spec1.variant_plan, spec2.variant_plan);

    // Mutating one does not affect the other
    spec1.variant_plan[0].bindings.k = 'mutated';
    assert.equal(spec2.variant_plan[0].bindings.k, 'original');
  });

  test('mutating build output does not affect builder', () => {
    const builder = validBuilder();
    const spec = builder.build();
    spec.experiment.name = 'MUTATED';
    const fresh = builder.build();
    assert.equal(fresh.experiment.name, 'My Experiment');
  });
});

// ---------------------------------------------------------------------------
// toYaml()
// ---------------------------------------------------------------------------
describe('ExperimentBuilder toYaml()', () => {
  test('produces valid YAML string', () => {
    const yaml = validBuilder().toYaml();
    assert.equal(typeof yaml, 'string');
    assert.ok(yaml.includes('version:'));
    assert.ok(yaml.includes('experiment:'));
  });

  test('YAML contains experiment id', () => {
    const yaml = validBuilder().toYaml();
    assert.ok(yaml.includes('exp-1'));
    assert.ok(yaml.includes('My Experiment'));
  });

  test('YAML contains variant plan entries', () => {
    const yaml = validBuilder()
      .addVariant('v1', { temp: 0.7 })
      .toYaml();
    assert.ok(yaml.includes('v1'));
    assert.ok(yaml.includes('0.7'));
  });
});

// ---------------------------------------------------------------------------
// Complex composition
// ---------------------------------------------------------------------------
describe('ExperimentBuilder complex composition', () => {
  test('full experiment build', () => {
    const spec = ExperimentBuilder.create('swe-bench-eval', 'SWE-Bench Evaluation')
      .description('Compare models on SWE-Bench Lite')
      .owner('team-eval')
      .tags(['swe-bench', 'comparison'])
      .datasetJsonl('./swe_bench_lite.jsonl', {
        suiteId: 'swe-bench',
        splitId: 'test',
        limit: 100,
      })
      .harnessCli(['python', '-m', 'harness', 'run'], {
        integrationLevel: 'cli_events',
      })
      .baseline('gpt-4', { model: 'gpt-4', temperature: 0.0 })
      .addVariant('claude-3-opus', { model: 'claude-3-opus', temperature: 0.0 })
      .addVariant('claude-3-sonnet', { model: 'claude-3-sonnet', temperature: 0.0 })
      .replications(3)
      .sanitizationProfile('hermetic_functional_v2')
      .randomSeed(1337)
      .maxConcurrency(8)
      .primaryMetrics(['resolved', 'applied'])
      .secondaryMetrics(['cost_usd', 'duration_s'])
      .networkMode('allowlist_enforced', ['api.openai.com', 'api.anthropic.com'])
      .sandboxImage('python:3.11-slim')
      .build();

    assert.equal(spec.experiment.id, 'swe-bench-eval');
    assert.equal(spec.experiment.description, 'Compare models on SWE-Bench Lite');
    assert.equal(spec.experiment.owner, 'team-eval');
    assert.deepEqual(spec.experiment.tags, ['swe-bench', 'comparison']);
    assert.equal(spec.dataset.path, './swe_bench_lite.jsonl');
    assert.equal(spec.dataset.suite_id, 'swe-bench');
    assert.equal(spec.dataset.limit, 100);
    assert.equal(spec.baseline.variant_id, 'gpt-4');
    assert.equal(spec.variant_plan.length, 2);
    assert.equal(spec.design.replications, 3);
    assert.equal(spec.design.max_concurrency, 8);
    assert.equal(spec.design.sanitization_profile, 'hermetic_functional_v2');
    assert.equal(spec.design.random_seed, 1337);
    assert.deepEqual(spec.analysis_plan.primary_metrics, ['resolved', 'applied']);
    assert.equal(spec.runtime.network.mode, 'allowlist_enforced');
    assert.equal(spec.runtime.sandbox.image, 'python:3.11-slim');
    // 4 unique metrics in tests
    assert.equal(Object.keys(spec.analysis_plan.tests).length, 4);
  });
});
