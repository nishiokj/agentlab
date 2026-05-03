#!/usr/bin/env node
const fs = require('fs');

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJson(filePath, obj) {
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2));
}

const graderInputPath = process.env.AGENTLAB_GRADER_INPUT_PATH;
const mappedOutputPath = process.env.AGENTLAB_MAPPED_GRADER_OUTPUT_PATH;

if (!graderInputPath) {
  throw new Error('AGENTLAB_GRADER_INPUT_PATH is required');
}
if (!mappedOutputPath) {
  throw new Error('AGENTLAB_MAPPED_GRADER_OUTPUT_PATH is required');
}

const graderInput = readJson(graderInputPath);
const result = readJson(graderInput.paths.result_path);
const value = Number(result.metrics?.difficulty_match ?? 0);

writeJson(mappedOutputPath, {
  schema_version: 'trial_conclusion_v1',
  reported_outcome: value === 1 ? 'success' : 'failure',
  primary_metric: {
    name: 'difficulty_match',
    value,
  },
  payload: {
    task_id: graderInput.ids.task_id,
    resolved: value,
    agent_outcome: result.outcome || null,
  },
  grader: {
    name: 'agentlab_demo_grader',
    strategy: 'in_task_image',
    version: '0.1.0',
  },
});
