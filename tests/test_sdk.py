import json
import os
import tempfile

import yaml

from agentlab_sdk import AgentLabClient, AnalysisPlan, Experiment, VariantPlan


def test_experiment_serialization_is_deterministic():
    variant_plan = (
        VariantPlan()
        .set_baseline("baseline", model="model-a")
        .add_variant("treatment", model="model-b")
    )
    analysis_plan = AnalysisPlan(
        primary_metrics=["success"],
        secondary_metrics=["latency_ms"],
    )
    experiment = (
        Experiment.builder("exp_sdk", "SDK Experiment")
        .metadata(description="SDK smoke", tags=["sdk"])
        .dataset_jsonl("tasks.jsonl", limit=10)
        .harness_cli(["python3", "./harness.py", "run"], integration_level="sdk_control")
        .analysis_plan(analysis_plan)
        .variant_plan(variant_plan)
        .build()
    )

    json_1 = experiment.to_json()
    json_2 = experiment.to_json()
    yaml_1 = experiment.to_yaml()
    yaml_2 = experiment.to_yaml()

    assert json_1 == json_2
    assert yaml_1 == yaml_2

    parsed = yaml.safe_load(yaml_1)
    assert parsed["baseline"]["variant_id"] == "baseline"
    assert parsed["variant_plan"][0]["variant_id"] == "treatment"


def test_sdk_client_run_supports_in_memory_experiment():
    with tempfile.TemporaryDirectory() as tmp:
        tasks_path = os.path.join(tmp, "tasks.jsonl")
        with open(tasks_path, "w", encoding="utf-8") as f:
            f.write('{"task_id":"t1","input":{"prompt":"hi"}}\n')

        harness_path = os.path.join(tmp, "harness.py")
        with open(harness_path, "w", encoding="utf-8") as f:
            f.write(
                """
import json
import os

with open(os.environ["AGENTLAB_TRIAL_INPUT"], "r", encoding="utf-8") as fp:
    trial_input = json.load(fp)

out = {
  "schema_version": "trial_output_v1",
  "ids": trial_input["ids"],
  "outcome": "success",
  "metrics": {"latency_ms": 1}
}

with open(os.environ["AGENTLAB_TRIAL_OUTPUT"], "w", encoding="utf-8") as fp:
    json.dump(out, fp)
"""
            )

        experiment = (
            Experiment.builder("exp_sdk_run", "SDK Run")
            .dataset_jsonl("tasks.jsonl", limit=1)
            .harness_cli(["python3", "./harness.py", "run"], integration_level="sdk_control")
            .add_variant("treatment", model="model-b")
            .build()
        )

        client = AgentLabClient(base_dir=tmp)
        result = client.run(experiment, allow_missing_harness_manifest=True)

        assert os.path.exists(os.path.join(result.report_dir, "index.html"))
        assert os.path.isdir(result.run_dir)

        with open(os.path.join(result.run_dir, "grades.json"), "r", encoding="utf-8") as f:
            grades = json.load(f)
        assert grades["evidence"]["sdk"] is True


def test_sdk_client_validate_path_resolves_relative_dataset_path():
    with tempfile.TemporaryDirectory() as tmp:
        tasks_path = os.path.join(tmp, "tasks.jsonl")
        with open(tasks_path, "w", encoding="utf-8") as f:
            f.write('{"task_id":"t1","input":{"prompt":"hi"}}\n')

        experiment = (
            Experiment.builder("exp_sdk_validate", "SDK Validate")
            .dataset_jsonl("tasks.jsonl", limit=1)
            .harness_cli(["python3", "-c", "print('ok')", "run"])
            .build()
        )
        experiment_path = os.path.join(tmp, "experiment.yaml")
        experiment.write_yaml(experiment_path)

        client = AgentLabClient(base_dir=tmp)
        resolved = client.validate("experiment.yaml")

        assert os.path.isabs(resolved["dataset"]["path"])
