import os
import tempfile

import yaml

from agentlab_cli.init_scaffold import scaffold


def test_scaffold_creates_files():
    with tempfile.TemporaryDirectory() as tmp:
        exp = os.path.join(tmp, "experiment.yaml")
        tasks = os.path.join(tmp, "tasks.jsonl")
        manifest = os.path.join(tmp, "harness_manifest.json")
        created, warnings = scaffold(
            repo_dir=tmp,
            experiment_path=exp,
            tasks_path=tasks,
            manifest_path=manifest,
            force=False,
        )
        assert os.path.exists(exp)
        assert os.path.exists(tasks)
        assert os.path.exists(manifest)
        assert os.path.exists(os.path.join(tmp, "AGENTLAB_ONBOARDING.md"))
        assert len(created) == 4
        with open(exp, "r", encoding="utf-8") as f:
            experiment = yaml.safe_load(f)
        assert "variant_plan" in experiment
        assert "variants" not in experiment


def test_scaffold_typescript_wrapper_creates_files():
    with tempfile.TemporaryDirectory() as tmp:
        exp = os.path.join(tmp, "experiment.yaml")
        tasks = os.path.join(tmp, "tasks.jsonl")
        manifest = os.path.join(tmp, "harness_manifest.json")
        created, _warnings = scaffold(
            repo_dir=tmp,
            experiment_path=exp,
            tasks_path=tasks,
            manifest_path=manifest,
            typescript_wrapper=True,
            force=False,
        )
        assert os.path.exists(os.path.join(tmp, "agentlab", "harness.ts"))
        assert os.path.exists(os.path.join(tmp, "agentlab", "harness.js"))
        assert os.path.exists(os.path.join(tmp, "agentlab", "package.json"))
        assert os.path.exists(os.path.join(tmp, "agentlab", "agentlab.config.json"))
        assert len(created) == 8
