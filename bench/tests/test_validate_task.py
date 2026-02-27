"""Tests for task validation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from bench.config import BenchConfig
from bench.taskkit.validate_task import run_validate_task, _check_structure, _check_prompt_leaks

BENCH_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture
def config():
    return BenchConfig.from_root(BENCH_ROOT)


@pytest.fixture
def valid_task_dir(tmp_path):
    """Create a minimal valid task directory."""
    task_dir = tmp_path / "TASK001"
    task_dir.mkdir()

    # task.yaml
    task_data = {
        "task_id": "TASK001",
        "repo_id": "click",
        "repo_snapshot": "click/src.tar.zst",
        "baseline_injection_patch": "injection.patch",
        "public_command": "bash .bench_public/run_public.sh",
        "hidden_command": "python hidden/runner.py /workspace hidden/cases.jsonl",
        "time_limits": {
            "agent_timeout": 1200,
            "grade_timeout": 300,
            "hidden_timeout": 60,
            "public_timeout": 30,
        },
        "determinism_env": {
            "PYTHONHASHSEED": "0",
            "TZ": "UTC",
            "LC_ALL": "C.UTF-8",
            "LANG": "C.UTF-8",
        },
        "patch_policy": {
            "allow_edit_globs": ["src/**/*.py"],
            "deny_edit_globs": [],
        },
    }
    (task_dir / "task.yaml").write_text(yaml.dump(task_data))
    (task_dir / "issue.md").write_text("# Bug\n\nSomething is broken.\n")

    # Public
    public = task_dir / "public"
    public.mkdir()
    (public / "repro.md").write_text("# Repro\n")
    (public / "run_public.sh").write_text("#!/bin/bash\nexit 0\n")

    # Hidden
    hidden = task_dir / "hidden"
    hidden.mkdir()
    (hidden / "runner.py").write_text("# runner\n")
    cases = []
    for i in range(50):
        cases.append(json.dumps({
            "case_id": f"case_{i:03d}",
            "case_type": "api_call",
            "input_data": {"x": i},
            "tags": ["basic"],
            "timeout_s": 5,
        }, sort_keys=True))
    (hidden / "cases.jsonl").write_text("\n".join(cases) + "\n")

    # Mutants
    mutants = task_dir / "mutants"
    mutants.mkdir()
    (mutants / "README.md").write_text("# Mutants\n")
    for i in range(10):
        (mutants / f"M{i+1:02d}.patch").write_text(f"# mutant {i+1}\n")

    # Policy
    policy = task_dir / "policy"
    policy.mkdir()
    (policy / "allow_edit_globs.txt").write_text("src/**/*.py\n")
    (policy / "deny_edit_globs.txt").write_text("conftest.py\n")

    # Private
    private = task_dir / "private"
    private.mkdir()
    (private / "solution.patch").write_text("# solution\n")

    return task_dir


class TestCheckStructure:
    def test_valid_structure(self, valid_task_dir):
        errors = _check_structure(valid_task_dir)
        assert errors == []

    def test_missing_files(self, tmp_path):
        task_dir = tmp_path / "TASK002"
        task_dir.mkdir()
        errors = _check_structure(task_dir)
        assert len(errors) > 0
        assert any("task.yaml" in e for e in errors)


class TestPromptLeaks:
    def test_no_leaks(self, tmp_path):
        (tmp_path / "issue.md").write_text(
            "# Bug\nThe function returns wrong values for negative inputs.\n"
        )
        errors = _check_prompt_leaks(tmp_path)
        assert errors == []

    def test_detects_file_path(self, tmp_path):
        (tmp_path / "issue.md").write_text(
            "# Bug\nEdit file src/parser.py to fix the issue.\n"
        )
        errors = _check_prompt_leaks(tmp_path)
        assert len(errors) > 0


class TestValidateTask:
    def test_valid_task_passes(self, valid_task_dir, config):
        result = run_validate_task(valid_task_dir, config)
        assert result["valid"] is True
        assert result["checks"]["structure"]["passed"] is True
        assert result["checks"]["schema"]["passed"] is True
        assert result["checks"]["hidden_cases"]["passed"] is True
        assert result["checks"]["mutant_count"]["passed"] is True

    def test_invalid_task_fails(self, tmp_path, config):
        task_dir = tmp_path / "TASK_BAD"
        task_dir.mkdir()
        result = run_validate_task(task_dir, config)
        assert result["valid"] is False

    def test_too_few_cases(self, valid_task_dir, config):
        # Overwrite cases with only 5
        cases_path = valid_task_dir / "hidden" / "cases.jsonl"
        cases = [
            json.dumps({"case_id": f"c{i}", "case_type": "api_call",
                        "input_data": {}, "tags": [], "timeout_s": 5}, sort_keys=True)
            for i in range(5)
        ]
        cases_path.write_text("\n".join(cases) + "\n")
        result = run_validate_task(valid_task_dir, config)
        assert result["checks"]["hidden_cases"]["passed"] is False

    def test_too_few_mutants(self, valid_task_dir, config):
        # Remove most mutant patches
        mutants_dir = valid_task_dir / "mutants"
        for p in mutants_dir.glob("M*.patch"):
            if int(p.stem[1:]) > 3:
                p.unlink()
        result = run_validate_task(valid_task_dir, config)
        assert result["checks"]["mutant_count"]["passed"] is False
