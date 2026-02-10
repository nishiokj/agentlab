import json
import os
import tempfile

from agentlab_runner.run_engine import fork_trial, run_experiment


def _write_experiment(
    path: str,
    tasks_path: str,
    harness_py: str,
    *,
    integration_level: str,
    limit: int,
    variant_field: str = "variant_plan",
) -> None:
    with open(path, "w", encoding="utf-8") as f:
        variant_block = f"""{variant_field}:
  - variant_id: var
    bindings: {{}}
"""
        f.write(
            f"""
version: '0.3'
experiment:
  id: 'exp_test'
  name: 'test'
dataset:
  provider: local_jsonl
  path: '{tasks_path}'
  schema_version: task_jsonl_v1
  split_id: dev
  limit: {limit}
design:
  sanitization_profile: hermetic_functional_v2
  comparison: paired
  replications: 1
analysis_plan:
  primary_metrics: ['success']
  secondary_metrics: ['latency_ms']
  missingness: {{ policy: paired_drop, record_reasons: true }}
  tests:
    success: {{ method: paired_bootstrap, ci: 0.9, resamples: 50 }}
    latency_ms: {{ method: paired_bootstrap, ci: 0.9, resamples: 50 }}
  multiple_comparisons: {{ method: none }}
baseline:
  variant_id: base
  bindings: {{}}
{variant_block}runtime:
  harness:
    mode: cli
    command: ['python3', '{harness_py}', 'run']
    integration_level: {integration_level}
  network:
    mode: none
"""
        )


def test_run_experiment_supports_legacy_variants_field():
    with tempfile.TemporaryDirectory() as tmp:
        harness_py = os.path.join(tmp, "harness.py")
        with open(harness_py, "w", encoding="utf-8") as f:
            f.write(
                """
import json
import os

in_path = os.environ.get('AGENTLAB_TRIAL_INPUT', 'trial_input.json')
out_path = os.environ.get('AGENTLAB_TRIAL_OUTPUT', 'trial_output.json')

with open(in_path, 'r', encoding='utf-8') as f:
    ti = json.load(f)

out = {
  'schema_version': 'trial_output_v1',
  'ids': ti['ids'],
  'outcome': 'success',
  'metrics': {'latency_ms': 1}
}

with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(out, f)
"""
            )

        tasks_path = os.path.join(tmp, "tasks.jsonl")
        with open(tasks_path, "w", encoding="utf-8") as f:
            f.write('{"task_id":"t1","input":{"prompt":"hi"}}\n')

        exp_path = os.path.join(tmp, "experiment.yaml")
        _write_experiment(
            exp_path,
            tasks_path,
            harness_py,
            integration_level="cli_basic",
            limit=1,
            variant_field="variants",
        )

        cwd = os.getcwd()
        try:
            os.chdir(tmp)
            run_id, report_dir = run_experiment(exp_path)
        finally:
            os.chdir(cwd)

        assert os.path.exists(os.path.join(report_dir, "index.html"))


def test_run_experiment_local_harness_smoke():
    with tempfile.TemporaryDirectory() as tmp:
        # Create minimal harness script that reads trial_input.json from CWD and writes trial_output.json.
        harness_py = os.path.join(tmp, "harness.py")
        with open(harness_py, "w", encoding="utf-8") as f:
            f.write(
                """
import json
import os
import time

in_path = os.environ.get('AGENTLAB_TRIAL_INPUT', 'trial_input.json')
out_path = os.environ.get('AGENTLAB_TRIAL_OUTPUT', 'trial_output.json')

with open(in_path, 'r', encoding='utf-8') as f:
    ti = json.load(f)

# trivial behavior: succeed if prompt exists
prompt = (ti.get('task') or {}).get('input', {}).get('prompt')
outcome = 'success' if prompt else 'failure'

out = {
  'schema_version': 'trial_output_v1',
  'ids': ti['ids'],
  'outcome': outcome,
  'metrics': {'latency_ms': 1}
}

with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(out, f)
"""
            )

        # Create dataset
        tasks_path = os.path.join(tmp, "tasks.jsonl")
        with open(tasks_path, "w", encoding="utf-8") as f:
            f.write('{"task_id":"t1","input":{"prompt":"hi"}}\n')

        exp_path = os.path.join(tmp, "experiment.yaml")
        _write_experiment(exp_path, tasks_path, harness_py, integration_level="cli_basic", limit=1)

        # Run inside tmp so .lab goes there.
        cwd = os.getcwd()
        try:
            os.chdir(tmp)
            run_id, report_dir = run_experiment(exp_path)
        finally:
            os.chdir(cwd)

        assert os.path.exists(os.path.join(report_dir, "index.html"))


def test_run_experiment_allow_missing_manifest_does_not_downgrade_later_trials():
    with tempfile.TemporaryDirectory() as tmp:
        harness_py = os.path.join(tmp, "harness.py")
        with open(harness_py, "w", encoding="utf-8") as f:
            f.write(
                """
import json
import os

in_path = os.environ['AGENTLAB_TRIAL_INPUT']
out_path = os.environ['AGENTLAB_TRIAL_OUTPUT']
with open(in_path, 'r', encoding='utf-8') as fp:
    ti = json.load(fp)

task_id = ti['ids']['task_id']
integration = (ti.get('design') or {}).get('integration_level')
if task_id == 't2' and integration == 'cli_events':
    manifest = {
      'schema_version': 'harness_manifest_v1',
      'created_at': '2026-02-01T00:00:00Z',
      'integration_level': 'cli_events',
      'step': {'semantics': 'none'},
      'hooks': {'schema_version': 'hook_events_v1', 'events_path': '/out/harness_events.jsonl'}
    }
    with open('harness_manifest.json', 'w', encoding='utf-8') as fp:
        json.dump(manifest, fp)
    event = {
      'hooks_schema_version': 'hook_events_v1',
      'event_type': 'model_call_end',
      'ts': '2026-02-01T00:00:00Z',
      'seq': 1,
      'ids': ti['ids'],
      'call_id': 'call_1',
      'outcome': {'status': 'ok'},
      'usage': {'tokens_in': 0, 'tokens_out': 0},
      'timing': {'duration_ms': 1}
    }
    with open('harness_events.jsonl', 'w', encoding='utf-8') as fp:
        fp.write(json.dumps(event) + '\\n')

out = {'schema_version': 'trial_output_v1', 'ids': ti['ids'], 'outcome': 'success', 'metrics': {'latency_ms': 1}}
with open(out_path, 'w', encoding='utf-8') as fp:
    json.dump(out, fp)
"""
            )

        tasks_path = os.path.join(tmp, "tasks.jsonl")
        with open(tasks_path, "w", encoding="utf-8") as f:
            f.write('{"task_id":"t1","input":{"prompt":"hi"}}\n')
            f.write('{"task_id":"t2","input":{"prompt":"there"}}\n')

        exp_path = os.path.join(tmp, "experiment.yaml")
        _write_experiment(exp_path, tasks_path, harness_py, integration_level="cli_events", limit=2)

        cwd = os.getcwd()
        try:
            os.chdir(tmp)
            run_id, _ = run_experiment(exp_path, allow_missing_manifest=True)
            run_dir = os.path.join(tmp, ".lab", "runs", run_id)
            trials_dir = os.path.join(run_dir, "trials")
            trial_dirs = [
                os.path.join(trials_dir, trial_id)
                for trial_id in os.listdir(trials_dir)
                if os.path.isdir(os.path.join(trials_dir, trial_id))
            ]
            levels = []
            manifest_count = 0
            for trial_dir in trial_dirs:
                with open(os.path.join(trial_dir, "trial_input.json"), "r", encoding="utf-8") as fp:
                    levels.append((json.load(fp).get("design") or {}).get("integration_level"))
                if os.path.exists(os.path.join(trial_dir, "harness_manifest.json")):
                    manifest_count += 1
        finally:
            os.chdir(cwd)

        assert levels and all(level == "cli_events" for level in levels)
        assert manifest_count >= 1


def test_fork_trial_rebases_runtime_paths_and_writes_output():
    with tempfile.TemporaryDirectory() as tmp:
        harness_py = os.path.join(tmp, "harness.py")
        with open(harness_py, "w", encoding="utf-8") as f:
            f.write(
                """
import json
import os

in_path = os.environ['AGENTLAB_TRIAL_INPUT']
out_path = os.environ['AGENTLAB_TRIAL_OUTPUT']
with open(in_path, 'r', encoding='utf-8') as fp:
    ti = json.load(fp)

out = {'schema_version': 'trial_output_v1', 'ids': ti['ids'], 'outcome': 'success', 'metrics': {'latency_ms': 1}}
with open(out_path, 'w', encoding='utf-8') as fp:
    json.dump(out, fp)
"""
            )

        tasks_path = os.path.join(tmp, "tasks.jsonl")
        with open(tasks_path, "w", encoding="utf-8") as f:
            f.write('{"task_id":"t1","input":{"prompt":"hi"}}\n')

        exp_path = os.path.join(tmp, "experiment.yaml")
        _write_experiment(exp_path, tasks_path, harness_py, integration_level="cli_basic", limit=1)

        cwd = os.getcwd()
        try:
            os.chdir(tmp)
            run_id, _ = run_experiment(exp_path)
            run_dir = os.path.join(tmp, ".lab", "runs", run_id)
            trials_dir = os.path.join(run_dir, "trials")
            parent_trial_id = next(iter(os.listdir(trials_dir)))
            forked_id = fork_trial(parent_trial_id, "step:0", {"mode": "forked"})
            forked_dir = os.path.join(trials_dir, forked_id)

            with open(os.path.join(forked_dir, "trial_input.json"), "r", encoding="utf-8") as fp:
                forked_input = json.load(fp)
        finally:
            os.chdir(cwd)

        runtime_paths = ((forked_input.get("runtime") or {}).get("paths") or {})
        control_plane = ((forked_input.get("runtime") or {}).get("control_plane") or {})
        assert forked_input["ids"]["trial_id"] == forked_id
        assert os.path.realpath(runtime_paths.get("workspace")) == os.path.realpath(
            os.path.join(forked_dir, "workspace")
        )
        assert os.path.realpath(runtime_paths.get("state")) == os.path.realpath(
            os.path.join(forked_dir, "state")
        )
        assert os.path.realpath(runtime_paths.get("cache")) == os.path.realpath(
            os.path.join(forked_dir, "cache")
        )
        assert os.path.realpath(runtime_paths.get("out")) == os.path.realpath(forked_dir)
        assert os.path.realpath(runtime_paths.get("tmp")) == os.path.realpath(
            os.path.join(forked_dir, "tmp")
        )
        assert os.path.realpath(control_plane.get("path")) == os.path.realpath(
            os.path.join(forked_dir, "state", "lab_control.json")
        )
        assert os.path.exists(os.path.join(forked_dir, "trial_output.json"))
