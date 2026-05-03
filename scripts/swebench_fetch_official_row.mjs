#!/usr/bin/env node

import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { parseArgs } from 'node:util';

const DEFAULT_DATASET = 'princeton-nlp/SWE-bench_Lite';
const DEFAULT_SPLIT = 'test';
const PAGE_SIZE = 100;

async function fetchPage(dataset, split, offset) {
  const url = new URL('https://datasets-server.huggingface.co/rows');
  url.searchParams.set('dataset', dataset);
  url.searchParams.set('config', 'default');
  url.searchParams.set('split', split);
  url.searchParams.set('offset', String(offset));
  url.searchParams.set('length', String(PAGE_SIZE));
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`fetch failed (${res.status}): ${body.slice(0, 300)}`);
  }
  const payload = await res.json();
  return Array.isArray(payload.rows)
    ? payload.rows.map((entry) => entry?.row ?? entry).filter(Boolean)
    : [];
}

async function findRow(dataset, split, instanceId) {
  for (let offset = 0; ; offset += PAGE_SIZE) {
    const rows = await fetchPage(dataset, split, offset);
    if (rows.length === 0) break;
    const found = rows.find((row) => row && row.instance_id === instanceId);
    if (found) return found;
    if (rows.length < PAGE_SIZE) break;
  }
  return null;
}

async function main() {
  const { values } = parseArgs({
    options: {
      dataset: { type: 'string', default: DEFAULT_DATASET },
      split: { type: 'string', default: DEFAULT_SPLIT },
      instance: { type: 'string' },
      output: { type: 'string' },
    },
  });

  if (!values.instance) {
    throw new Error('--instance is required');
  }
  if (!values.output) {
    throw new Error('--output is required');
  }

  const row = await findRow(values.dataset, values.split, values.instance);
  if (!row) {
    throw new Error(`instance not found: ${values.instance}`);
  }

  const output = resolve(values.output);
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, JSON.stringify(row, null, 2) + '\n');
  console.log(JSON.stringify({
    output,
    instance_id: row.instance_id,
    has_patch: typeof row.patch === 'string' && row.patch.length > 0,
    has_test_patch: typeof row.test_patch === 'string' && row.test_patch.length > 0,
    fail_to_pass: row.FAIL_TO_PASS,
    pass_to_pass_count: Array.isArray(row.PASS_TO_PASS) ? row.PASS_TO_PASS.length : null,
  }, null, 2));
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
