DEMO_NODE_HARNESS = r"""#!/usr/bin/env node
const fs = require('fs');
const crypto = require('crypto');

function sha256Bytes(buf) {
  return 'sha256:' + crypto.createHash('sha256').update(buf).digest('hex');
}

function readJson(path) {
  return JSON.parse(fs.readFileSync(path, 'utf8'));
}

function writeJson(path, obj) {
  fs.writeFileSync(path, JSON.stringify(obj, null, 2));
}

function appendJsonl(path, obj) {
  fs.appendFileSync(path, JSON.stringify(obj) + '\n');
}

function nowIso() {
  return new Date().toISOString();
}

function main() {
  const inputPath = process.env.AGENTLAB_TRIAL_INPUT || 'trial_input.json';
  const outputPath = process.env.AGENTLAB_TRIAL_OUTPUT || 'trial_output.json';

  const ti = readJson(inputPath);
  const ids = ti.ids;
  const integration = (ti.design && ti.design.integration_level) || 'cli_basic';

  // Minimal behavior: success if prompt is present.
  const prompt = ti.task && ti.task.input && ti.task.input.prompt;
  const outcome = prompt ? 'success' : 'failure';

  // Emit manifest + hooks if requested.
  if (integration !== 'cli_basic') {
    const manifest = {
      schema_version: 'harness_manifest_v1',
      created_at: nowIso(),
      integration_level: integration,
      harness: {
        name: 'agentlab_demo_node',
        version: '0.1.0',
        entry_command: ['node', './agentlab_demo_harness.js', 'run'],
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
    writeJson('harness_manifest.json', manifest);
  }

  if (integration === 'cli_events') {
    const eventsPath = 'harness_events.jsonl';
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

    appendJsonl(eventsPath, baseEvent('agent_step_start', 1, 0));

    // model_call_end as a universal "turn" signal
    appendJsonl(eventsPath, {
      ...baseEvent('model_call_end', 2, 0),
      call_id: 'call_1',
      outcome: { status: 'ok' },
      usage: { tokens_in: 0, tokens_out: 0 },
      timing: { duration_ms: 1 },
    });

    appendJsonl(eventsPath, baseEvent('agent_step_end', 3, 0));

    // Control plane ack.
    const cpPath = ti.runtime && ti.runtime.control_plane && ti.runtime.control_plane.path;
    let cpBytes = Buffer.from('{"action":"continue"}');
    if (cpPath && fs.existsSync(cpPath)) {
      cpBytes = fs.readFileSync(cpPath);
    }
    const controlVersion = sha256Bytes(cpBytes);

    appendJsonl(eventsPath, {
      ...baseEvent('control_ack', 4, 0),
      control_version: controlVersion,
      action_observed: 'continue',
      action_taken: 'continue',
    });
  }

  const out = {
    schema_version: 'trial_output_v1',
    ids,
    outcome,
    metrics: { latency_ms: 1 },
  };
  writeJson(outputPath, out);
}

main();
"""


DEMO_PY_HARNESS = r"""#!/usr/bin/env python3
import json
import os
import hashlib
from datetime import datetime, timezone


def sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def append_jsonl(path: str, obj):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")


def main():
    input_path = os.environ.get("AGENTLAB_TRIAL_INPUT", "trial_input.json")
    output_path = os.environ.get("AGENTLAB_TRIAL_OUTPUT", "trial_output.json")

    ti = read_json(input_path)
    ids = ti["ids"]
    integration = (ti.get("design") or {}).get("integration_level", "cli_basic")

    prompt = ((ti.get("task") or {}).get("input") or {}).get("prompt")
    outcome = "success" if prompt else "failure"

    if integration != "cli_basic":
        manifest = {
            "schema_version": "harness_manifest_v1",
            "created_at": now_iso(),
            "integration_level": integration,
            "harness": {
                "name": "agentlab_demo_py",
                "version": "0.1.0",
                "entry_command": ["python3", "./agentlab_demo_harness.py", "run"],
            },
            "step": {"semantics": "decision_cycle"},
            "control_plane": {"mode": "file", "path": "/state/lab_control.json"},
        }
        if integration == "cli_events":
            manifest["hooks"] = {
                "schema_version": "hook_events_v1",
                "events_path": "/out/harness_events.jsonl",
                "header_event_emitted": False,
            }
        write_json("harness_manifest.json", manifest)

    if integration == "cli_events":
        def base(event_type, seq, step_index):
            return {
                "hooks_schema_version": "hook_events_v1",
                "event_type": event_type,
                "ts": now_iso(),
                "seq": seq,
                "ids": {
                    "run_id": ids["run_id"],
                    "trial_id": ids["trial_id"],
                    "variant_id": ids["variant_id"],
                    "task_id": ids["task_id"],
                    "repl_idx": ids["repl_idx"],
                },
                "step_index": step_index,
            }

        append_jsonl("harness_events.jsonl", base("agent_step_start", 1, 0))
        append_jsonl(
            "harness_events.jsonl",
            {
                **base("model_call_end", 2, 0),
                "call_id": "call_1",
                "outcome": {"status": "ok"},
                "usage": {"tokens_in": 0, "tokens_out": 0},
                "timing": {"duration_ms": 1},
            },
        )
        append_jsonl("harness_events.jsonl", base("agent_step_end", 3, 0))

        cp_path = ((ti.get("runtime") or {}).get("control_plane") or {}).get("path")
        cp_bytes = b'{"action":"continue"}'
        if cp_path and os.path.exists(cp_path):
            with open(cp_path, "rb") as f:
                cp_bytes = f.read()

        append_jsonl(
            "harness_events.jsonl",
            {
                **base("control_ack", 4, 0),
                "control_version": sha256_bytes(cp_bytes),
                "action_observed": "continue",
                "action_taken": "continue",
            },
        )

    out = {
        "schema_version": "trial_output_v1",
        "ids": ids,
        "outcome": outcome,
        "metrics": {"latency_ms": 1},
    }
    write_json(output_path, out)


if __name__ == "__main__":
    main()
"""
