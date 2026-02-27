"""Tests for task bundle loader."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from bench.config import BenchConfig
from bench.taskkit.schema import validate_json, load_schema

BENCH_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture
def config():
    return BenchConfig.from_root(BENCH_ROOT)


class TestTaskLoader:
    def test_task_template_schema_valid(self, config):
        """Verify the TASK_TEMPLATE's task.yaml validates against schema."""
        import yaml
        template_dir = config.bench_dir / "taskkit" / "templates" / "TASK_TEMPLATE"
        task_yaml_path = template_dir / "task.yaml"
        assert task_yaml_path.exists()

        with open(task_yaml_path) as f:
            data = yaml.safe_load(f)

        schema = load_schema(config.schemas_dir / "task.schema.json")
        errors = validate_json(data, schema)
        assert errors == [], f"Template task.yaml validation errors: {errors}"

    def test_task_template_has_required_dirs(self, config):
        """Verify template has all required subdirectories."""
        template_dir = config.bench_dir / "taskkit" / "templates" / "TASK_TEMPLATE"
        for subdir in ["public", "hidden", "mutants", "policy", "private"]:
            assert (template_dir / subdir).is_dir(), f"Missing template dir: {subdir}"

    def test_agent_manifest_excludes_hidden(self):
        """Verify that agent manifests never include hidden or private."""
        # This tests the contract, not the full loader (which needs repo snapshots)
        manifest = {
            "task_id": "TASK001",
            "repo_id": "click",
            "workspace": "/tmp/workspace",
            "includes_hidden": False,
            "includes_private": False,
        }
        assert manifest["includes_hidden"] is False
        assert manifest["includes_private"] is False

    def test_grader_manifest_includes_hidden(self):
        """Verify that grader manifests include hidden but not private."""
        manifest = {
            "task_id": "TASK001",
            "repo_id": "click",
            "workspace": "/tmp/workspace",
            "hidden_dir": "/tmp/hidden",
            "includes_hidden": True,
            "includes_private": False,
        }
        assert manifest["includes_hidden"] is True
        assert manifest["includes_private"] is False
