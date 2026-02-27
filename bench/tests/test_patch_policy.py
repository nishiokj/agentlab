"""Tests for patch and command policy enforcement."""

from __future__ import annotations

import pytest

from bench.runner.patch_policy import PatchPolicy, PatchClassifier


class TestPatchPolicy:
    def test_default_deny_harness_paths(self):
        policy = PatchPolicy()
        violations = policy.check_files(["bench/cli.py"])
        assert len(violations) > 0
        assert "deny" in violations[0].lower() or "POLICY_VIOLATION" in violations[0]

    def test_default_deny_repo_config(self):
        policy = PatchPolicy()
        violations = policy.check_files(["pytest.ini"])
        assert len(violations) > 0

    def test_default_deny_conftest(self):
        policy = PatchPolicy()
        violations = policy.check_files(["conftest.py"])
        assert len(violations) > 0

    def test_allow_python_files(self):
        policy = PatchPolicy(allow_edit_globs=["**/*.py"])
        violations = policy.check_files(["src/main.py"])
        assert len(violations) == 0

    def test_deny_overrides_allow(self):
        policy = PatchPolicy(
            allow_edit_globs=["**/*.py"],
            deny_edit_globs=["secret.py"],
        )
        violations = policy.check_files(["secret.py"])
        assert len(violations) > 0

    def test_custom_allow_list(self):
        policy = PatchPolicy(allow_edit_globs=["src/**/*.py", "lib/**/*.py"])
        assert len(policy.check_files(["src/module.py"])) == 0
        assert len(policy.check_files(["lib/util.py"])) == 0
        # Not in allow list
        assert len(policy.check_files(["other/file.py"])) > 0

    def test_from_task(self):
        task_data = {
            "patch_policy": {
                "allow_edit_globs": ["src/**/*.py"],
                "deny_edit_globs": ["src/secret.py"],
                "allow_run_globs": ["python", "pytest"],
            }
        }
        policy = PatchPolicy.from_task(task_data)
        assert len(policy.check_files(["src/main.py"])) == 0
        assert len(policy.check_files(["src/secret.py"])) > 0


class TestCommandPolicy:
    def test_allowed_command(self):
        policy = PatchPolicy()
        violations = policy.check_command(["python", "-c", "print(1)"])
        assert len(violations) == 0

    def test_disallowed_command(self):
        policy = PatchPolicy()
        violations = policy.check_command(["rm", "-rf", "/"])
        assert len(violations) > 0

    def test_empty_command(self):
        policy = PatchPolicy()
        violations = policy.check_command([])
        assert len(violations) > 0


class TestPatchClassifier:
    def test_classify_harness_edit(self):
        label = PatchClassifier.classify("POLICY_VIOLATION: 'bench/cli.py' matches deny pattern")
        assert label == "harness_edit"

    def test_classify_config_edit(self):
        label = PatchClassifier.classify("POLICY_VIOLATION: 'pytest.ini' matches deny pattern")
        assert label == "config_edit"

    def test_classify_conftest(self):
        label = PatchClassifier.classify("POLICY_VIOLATION: 'conftest.py' matches deny pattern")
        assert label == "test_runner_edit"

    def test_classify_escape(self):
        label = PatchClassifier.classify("Path escapes workspace: ../../../etc/passwd")
        assert label == "workspace_escape"
