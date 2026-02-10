import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { LabClient, LabRunnerError } from '../src/client.js';

function makeFakeRunner(dir: string): string {
  const binPath = join(dir, 'fake-lab.js');
  const script = `#!/usr/bin/env node
const args = process.argv.slice(2);
const cmd = args[0] || '';
if (cmd === 'describe') {
  console.log(JSON.stringify({ ok: true, command: 'describe', summary: { experiment: 'exp1', workload_type: 'agent_harness', dataset: 'tasks.jsonl', tasks: 2, replications: 1, variant_plan_entries: 1, total_trials: 2, harness: ['node','./harness.js','run'], integration_level: 'cli_basic', container_mode: false, network: 'none', control_path: '/state/lab_control.json', harness_script_exists: true } }));
  process.exit(0);
}
if (cmd === 'run') {
  console.log(JSON.stringify({ ok: false, error: { code: 'bad_config', message: 'invalid config', details: { path: 'x' } } }));
  process.exit(1);
}
console.log(JSON.stringify({ ok: true, command: cmd, valid: true }));
`;
  writeFileSync(binPath, script, { encoding: 'utf8', mode: 0o755 });
  return binPath;
}

test('LabClient describe parses JSON success payload', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'agentlab-sdk-test-'));
  try {
    const fakeRunner = makeFakeRunner(dir);
    const client = new LabClient({ runnerBin: fakeRunner, cwd: dir });
    const res = await client.describe({ experiment: 'experiment.yaml' });

    assert.equal(res.ok, true);
    assert.equal(res.command, 'describe');
    assert.equal(res.summary.experiment, 'exp1');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('LabClient throws typed LabRunnerError for error envelope', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'agentlab-sdk-test-'));
  try {
    const fakeRunner = makeFakeRunner(dir);
    const client = new LabClient({ runnerBin: fakeRunner, cwd: dir });

    await assert.rejects(
      () => client.run({ experiment: 'experiment.yaml' }),
      (error: unknown) => {
        assert.ok(error instanceof LabRunnerError);
        assert.equal(error.code, 'bad_config');
        assert.equal(error.message, 'invalid config');
        return true;
      },
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
