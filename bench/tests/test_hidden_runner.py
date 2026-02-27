"""Tests for the hidden suite runner."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from bench.taskkit.hidden_runner import run_hidden_suite, HiddenSuiteResult


@pytest.fixture
def workspace_with_runner(tmp_path):
    """Create a workspace with a simple hidden runner."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    # Create a simple Python module to test
    (workspace / "module.py").write_text(
        "def add(a, b): return a + b\n"
    )

    hidden_dir = tmp_path / "hidden"
    hidden_dir.mkdir()

    # Create runner
    (hidden_dir / "runner.py").write_text(textwrap.dedent("""\
        import json
        import sys
        import time
        from pathlib import Path

        workspace = Path(sys.argv[1])
        cases_path = Path(sys.argv[2])

        sys.path.insert(0, str(workspace))
        from module import add

        for line in cases_path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            case = json.loads(line)
            start = time.monotonic()
            try:
                result = add(case["input_data"]["a"], case["input_data"]["b"])
                expected = case["expected"]["result"]
                passed = result == expected
                err = None
                msg = None
            except Exception as e:
                passed = False
                err = type(e).__name__
                msg = str(e)
            dur = (time.monotonic() - start) * 1000
            print(json.dumps({
                "case_id": case["case_id"],
                "passed": passed,
                "error_type": err,
                "error_message": msg,
                "duration_ms": round(dur, 2),
                "output_summary": str(result) if passed else "",
            }, sort_keys=True))
    """))

    # Create cases
    cases = []
    for i in range(50):
        cases.append(json.dumps({
            "case_id": f"case_{i:03d}",
            "case_type": "api_call",
            "input_data": {"a": i, "b": i * 2},
            "expected": {"result": i + i * 2},
            "tags": ["basic"],
            "timeout_s": 5,
        }, sort_keys=True))
    (hidden_dir / "cases.jsonl").write_text("\n".join(cases) + "\n")

    return workspace, hidden_dir


class TestHiddenRunner:
    def test_run_passing_suite(self, workspace_with_runner):
        workspace, hidden_dir = workspace_with_runner
        result = run_hidden_suite(
            workspace=workspace,
            hidden_dir=hidden_dir,
            task_data={"task_id": "TASK001"},
            timeout=60,
        )
        assert result.total == 50
        assert result.passed == 50
        assert result.all_passed
        assert not result.timed_out

    def test_stable_results(self, workspace_with_runner):
        workspace, hidden_dir = workspace_with_runner
        r1 = run_hidden_suite(workspace, hidden_dir, {"task_id": "TASK001"})
        r2 = run_hidden_suite(workspace, hidden_dir, {"task_id": "TASK001"})
        assert r1.total == r2.total
        assert r1.passed == r2.passed
        assert len(r1.case_results) == len(r2.case_results)
        for cr1, cr2 in zip(r1.case_results, r2.case_results):
            assert cr1.case_id == cr2.case_id
            assert cr1.passed == cr2.passed

    def test_missing_runner(self, tmp_path):
        result = run_hidden_suite(
            workspace=tmp_path,
            hidden_dir=tmp_path / "nonexistent",
            task_data={"task_id": "TASK001"},
        )
        assert result.error_message is not None
        assert "not found" in result.error_message.lower()

    def test_timeout_handling(self, tmp_path):
        workspace = tmp_path / "ws"
        workspace.mkdir()
        hidden_dir = tmp_path / "hidden"
        hidden_dir.mkdir()

        # Slow runner
        (hidden_dir / "runner.py").write_text(
            "import time; time.sleep(10)\n"
        )
        (hidden_dir / "cases.jsonl").write_text(
            '{"case_id": "c1", "case_type": "api_call", "input_data": {}, "tags": [], "timeout_s": 1}\n'
        )

        result = run_hidden_suite(
            workspace=workspace,
            hidden_dir=hidden_dir,
            task_data={"task_id": "TASK001"},
            timeout=2,
        )
        assert result.timed_out
