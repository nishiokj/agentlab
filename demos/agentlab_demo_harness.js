#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

function sha256Bytes(buf) {
  return 'sha256:' + crypto.createHash('sha256').update(buf).digest('hex');
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJson(filePath, obj) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2));
}

function writeText(filePath, text) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, text);
}

function appendJsonl(filePath, obj) {
  ensureDir(path.dirname(filePath));
  fs.appendFileSync(filePath, JSON.stringify(obj) + '\n');
}

function nowIso() {
  return new Date().toISOString();
}

function tokenizeEstimate(text) {
  const source = String(text || '');
  return Math.max(1, Math.ceil(source.length / 4));
}

function classifyDifficulty(prompt, bindings) {
  const text = String(prompt || '');

  const hardSignals = [
    /transform/i,
    /topocentric/i,
    /geocentric/i,
    /aberration/i,
    /refraction/i,
    /ephemeris/i,
    /coordinate/i,
  ];

  const mediumSignals = [
    /support/i,
    /header/i,
    /format/i,
    /attribute/i,
    /error message/i,
  ];

  let score = 0;
  for (const pattern of hardSignals) {
    if (pattern.test(text)) score += 2;
  }
  for (const pattern of mediumSignals) {
    if (pattern.test(text)) score += 1;
  }

  const threshold = Number.isFinite(Number(bindings?.difficulty_threshold))
    ? Number(bindings.difficulty_threshold)
    : 4;
  const bias = Number.isFinite(Number(bindings?.difficulty_bias))
    ? Number(bindings.difficulty_bias)
    : 0;

  const finalScore = score + bias;
  const predictedBin = finalScore >= threshold ? 'hard' : 'easy';

  return {
    predictedBin,
    predictedDifficulty: predictedBin === 'hard' ? '1-4 hours' : '15 min - 1 hour',
    keywordHits: score,
    threshold,
    bias,
    finalScore,
  };
}

function writeHarnessManifest(manifestDir, integration) {
  if (integration === 'cli_basic') {
    return;
  }

  const manifest = {
    schema_version: 'harness_manifest_v1',
    created_at: nowIso(),
    integration_level: integration,
    harness: {
      name: 'agentlab_demo_node',
      version: '0.2.0',
      entry_command: ['node', './demos/agentlab_demo_harness.js', 'run'],
    },
    step: { semantics: 'decision_cycle' },
    control_plane: { mode: 'file', path: '/state/lab_control.json' },
  };

  if (integration === 'cli_events') {
    manifest.hooks = {
      schema_version: 'hook_events_v1',
      events_path: '/out/harness_events.jsonl',
      header_event_emitted: false,
    };
  }

  writeJson(path.join(manifestDir, 'harness_manifest.json'), manifest);
}

function emitEvents(params) {
  const {
    integration,
    eventsPath,
    ids,
    difficulty,
    response,
    prompt,
    controlPath,
  } = params;

  if (integration !== 'cli_events') {
    return;
  }

  const baseEvent = (event_type, seq, step_index) => ({
    hooks_schema_version: 'hook_events_v1',
    event_type,
    ts: nowIso(),
    seq,
    ids: {
      run_id: ids.run_id,
      trial_id: ids.trial_id,
      variant_id: ids.variant_id,
      task_id: ids.task_id,
      repl_idx: ids.repl_idx,
    },
    step_index,
  });

  const responseText = `${response.predicted_difficulty} (${response.predicted_bin})`;
  const tokensIn = tokenizeEstimate(prompt);
  const tokensOut = tokenizeEstimate(responseText);
  const modelLatencyMs = Math.max(1, 2 + difficulty.keywordHits);
  const toolLatencyMs = 1;

  appendJsonl(eventsPath, baseEvent('agent_step_start', 1, 0));

  appendJsonl(eventsPath, {
    ...baseEvent('model_call_end', 2, 0),
    call_id: 'difficulty_classifier_1',
    turn_index: 0,
    outcome: { status: 'ok' },
    usage: { tokens_in: tokensIn, tokens_out: tokensOut },
    timing: { duration_ms: modelLatencyMs },
    ext: {
      response_preview: responseText,
      expected_bin: response.expected_bin,
    },
  });

  appendJsonl(eventsPath, {
    ...baseEvent('tool_call_end', 3, 0),
    call_id: 'keyword_scan_1',
    tool: { name: 'keyword-scan', version: '0.1.0' },
    timing: { duration_ms: toolLatencyMs },
    outcome: { status: 'ok' },
    ext: {
      keyword_hits: difficulty.keywordHits,
    },
  });

  appendJsonl(eventsPath, {
    ...baseEvent('agent_step_end', 4, 0),
    budgets: {
      steps: 1,
      tokens_in: tokensIn,
      tokens_out: tokensOut,
      tool_calls: 1,
    },
  });

  let cpBytes = Buffer.from('{"action":"continue"}');
  if (controlPath && fs.existsSync(controlPath)) {
    cpBytes = fs.readFileSync(controlPath);
  }
  const controlVersion = sha256Bytes(cpBytes);

  appendJsonl(eventsPath, {
    ...baseEvent('control_ack', 5, 0),
    control_version: controlVersion,
    action_observed: 'continue',
    action_taken: 'continue',
  });
}

function main() {
  const inputPath = process.env.AGENTLAB_TRIAL_INPUT || 'trial_input.json';
  const outputPath = process.env.AGENTLAB_TRIAL_OUTPUT || 'trial_output.json';

  const ti = readJson(inputPath);
  const ids = ti.ids;
  const integration = (ti.design && ti.design.integration_level) || 'cli_basic';

  const outDir = path.dirname(outputPath);
  const runtimeOutDir = ti.runtime?.paths?.out || outDir;
  ensureDir(outDir);
  ensureDir(runtimeOutDir);

  writeHarnessManifest(runtimeOutDir, integration);

  const prompt =
    ti.task?.input?.prompt ||
    ti.task?.problem_statement ||
    ti.task?.prompt ||
    '';

  const difficulty = classifyDifficulty(prompt, ti.bindings || {});
  const expectedBin = ti.task?.gold?.difficulty_bin;
  const expectedDifficulty = ti.task?.gold?.difficulty;

  const match = expectedBin
    ? Number(difficulty.predictedBin === expectedBin)
    : expectedDifficulty
      ? Number(difficulty.predictedDifficulty === expectedDifficulty)
      : Number(Boolean(prompt));

  const outcome = match === 1 ? 'success' : 'failure';

  const response = {
    predicted_difficulty: difficulty.predictedDifficulty,
    predicted_bin: difficulty.predictedBin,
    expected_bin: expectedBin || null,
    keyword_hits: difficulty.keywordHits,
    threshold: difficulty.threshold,
    score: difficulty.finalScore,
  };

  const workspaceDir = ti.runtime?.paths?.workspace || process.cwd();
  const trialArtifactDir = path.join(workspaceDir, 'artifacts', ids.task_id);
  ensureDir(trialArtifactDir);

  const responseArtifactPath = path.join(trialArtifactDir, 'response.json');
  const gradingArtifactPath = path.join(trialArtifactDir, 'grading.json');
  const noteArtifactPath = path.join(trialArtifactDir, 'notes.txt');

  writeJson(responseArtifactPath, {
    schema_version: 'demo_response_v1',
    ids,
    task_source: ti.task?.source || 'unknown',
    response,
  });

  writeJson(gradingArtifactPath, {
    schema_version: 'demo_grading_v1',
    ids,
    outcome,
    match,
    expected: {
      difficulty: expectedDifficulty || null,
      difficulty_bin: expectedBin || null,
    },
    predicted: {
      difficulty: difficulty.predictedDifficulty,
      difficulty_bin: difficulty.predictedBin,
    },
  });

  writeText(
    noteArtifactPath,
    `task_id=${ids.task_id}\npredicted=${difficulty.predictedDifficulty}\noutcome=${outcome}\n`,
  );

  const eventsPath = path.join(runtimeOutDir, 'harness_events.jsonl');
  emitEvents({
    integration,
    eventsPath,
    ids,
    difficulty,
    response,
    prompt,
    controlPath: ti.runtime?.control_plane?.path,
  });

  const out = {
    schema_version: 'trial_output_v1',
    ids,
    outcome,
    answer: response,
    objective: {
      name: 'difficulty_match',
      value: match,
      direction: 'maximize',
    },
    metrics: {
      difficulty_match: match,
      keyword_hits: difficulty.keywordHits,
      predicted_bin: difficulty.predictedBin,
      expected_bin: expectedBin || null,
      latency_ms: Math.max(1, 2 + difficulty.keywordHits),
      response_chars: JSON.stringify(response).length,
    },
    artifacts: [
      {
        path: `/workspace/artifacts/${ids.task_id}/response.json`,
        logical_name: 'response_json',
        mime_type: 'application/json',
      },
      {
        path: `/workspace/artifacts/${ids.task_id}/grading.json`,
        logical_name: 'grading_json',
        mime_type: 'application/json',
      },
      {
        path: `/workspace/artifacts/${ids.task_id}/notes.txt`,
        logical_name: 'notes_txt',
        mime_type: 'text/plain',
      },
    ],
  };

  writeJson(outputPath, out);
}

main();
