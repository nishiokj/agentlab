import json
import os
import tempfile

from agentlab_analysis import run_analysis


def _write_trial(tmp, trial_id, variant_id, task_id, repl_idx, outcome, metrics):
    tdir = os.path.join(tmp, "trials", trial_id)
    os.makedirs(tdir, exist_ok=True)
    data = {
        "ids": {
            "run_id": "run_1",
            "trial_id": trial_id,
            "variant_id": variant_id,
            "task_id": task_id,
            "repl_idx": repl_idx,
        },
        "outcome": outcome,
        "metrics": metrics,
    }
    with open(os.path.join(tdir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(data, f)


def test_run_analysis_paired():
    with tempfile.TemporaryDirectory() as tmp:
        _write_trial(tmp, "t1", "base", "task1", 0, "success", {"latency_ms": 10})
        _write_trial(tmp, "t2", "var", "task1", 0, "failure", {"latency_ms": 20})

        resolved = {
            "version": "0.3",
            "analysis_plan": {
                "primary_metrics": ["success"],
                "secondary_metrics": ["latency_ms"],
                "missingness": {"policy": "paired_drop"},
                "tests": {
                    "success": {"method": "paired_bootstrap", "ci": 0.9, "resamples": 100},
                    "latency_ms": {"method": "paired_bootstrap", "ci": 0.9, "resamples": 100},
                },
                "multiple_comparisons": {"method": "none"},
            },
        }
        with open(os.path.join(tmp, "resolved_experiment.json"), "w", encoding="utf-8") as f:
            json.dump(resolved, f)

        result = run_analysis(tmp, "base", ["var"], {"hooks": True, "traces": False, "framework_events": False})
        assert "comparisons" in result
        assert result["comparisons"][0]["baseline"] == "base"
