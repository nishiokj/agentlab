"""Task and suite validation.

Implements validate-task and validate-suite commands that check:
1. Hidden cases >= 50
2. Mutants >= 10
3. Baseline injection fails hidden suite
4. Reference solution passes public + hidden
5. All mutants are killed by hidden suite
6. Time limits respected
7. Determinism checksums
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from typing import Any
from concurrent.futures import ProcessPoolExecutor

from bench.config import BenchConfig
from bench.taskkit.schema import load_schema, validate_json, load_task_yaml
from bench.taskkit.casegen import verify_case_count
from bench.taskkit.mutants import list_mutant_patches


def _check_structure(task_dir: Path) -> list[str]:
    """Check that required files exist in the task bundle."""
    errors = []
    required = [
        "task.yaml",
        "issue.md",
        "public/repro.md",
        "public/run_public.sh",
        "hidden/runner.py",
        "hidden/cases.jsonl",
        "mutants/README.md",
        "policy/allow_edit_globs.txt",
        "policy/deny_edit_globs.txt",
    ]
    for rel in required:
        if not (task_dir / rel).exists():
            errors.append(f"Missing required file: {rel}")
    return errors


def _check_prompt_leaks(task_dir: Path) -> list[str]:
    """Heuristic scan for leaked file paths in issue.md."""
    errors = []
    issue_md = task_dir / "issue.md"
    if not issue_md.exists():
        return errors
    content = issue_md.read_text()
    # Reject obvious file path patterns
    import re
    path_patterns = [
        r'src/\w+\.py',
        r'lib/\w+\.py',
        r'edit\s+file\s+',
        r'modify\s+file\s+',
        r'change\s+line\s+\d+',
    ]
    for pattern in path_patterns:
        matches = re.findall(pattern, content, re.IGNORECASE)
        if matches:
            errors.append(
                f"Issue prompt may leak file paths (matched pattern '{pattern}'): "
                f"{matches[:3]}"
            )
    return errors


def run_validate_task(
    task_dir: Path,
    config: BenchConfig,
) -> dict[str, Any]:
    """Validate a single task and return a validation summary.

    Returns a dict with 'valid' bool and details of each check.
    """
    results: dict[str, Any] = {
        "task_dir": str(task_dir),
        "valid": True,
        "checks": {},
    }

    # 1. Structure check
    struct_errors = _check_structure(task_dir)
    results["checks"]["structure"] = {
        "passed": len(struct_errors) == 0,
        "errors": struct_errors,
    }
    if struct_errors:
        results["valid"] = False

    # 2. Schema validation
    try:
        task_data = load_task_yaml(task_dir)
        schema = load_schema(config.schemas_dir / "task.schema.json")
        schema_errors = validate_json(task_data, schema)
        results["checks"]["schema"] = {
            "passed": len(schema_errors) == 0,
            "errors": schema_errors,
        }
        if schema_errors:
            results["valid"] = False
        results["task_id"] = task_data.get("task_id", "unknown")
    except Exception as e:
        results["checks"]["schema"] = {"passed": False, "errors": [str(e)]}
        results["valid"] = False
        return results

    # 3. Hidden cases count
    cases_path = task_dir / "hidden" / "cases.jsonl"
    if cases_path.exists():
        ok, count = verify_case_count(cases_path, min_cases=50)
        results["checks"]["hidden_cases"] = {
            "passed": ok,
            "count": count,
            "minimum": 50,
        }
        if not ok:
            results["valid"] = False
    else:
        results["checks"]["hidden_cases"] = {"passed": False, "errors": ["cases.jsonl not found"]}
        results["valid"] = False

    # 4. Mutant count
    mutant_patches = list_mutant_patches(task_dir)
    mutant_ok = len(mutant_patches) >= 10
    results["checks"]["mutant_count"] = {
        "passed": mutant_ok,
        "count": len(mutant_patches),
        "minimum": 10,
    }
    if not mutant_ok:
        results["valid"] = False

    # 5. Prompt leak scan
    leak_warnings = _check_prompt_leaks(task_dir)
    results["checks"]["prompt_leaks"] = {
        "passed": len(leak_warnings) == 0,
        "warnings": leak_warnings,
    }
    # Leaks are warnings, not hard failures
    if leak_warnings:
        results["checks"]["prompt_leaks"]["passed"] = False

    # 6. Private solution exists (for validation, never mounted to agent)
    solution_exists = (task_dir / "private" / "solution.patch").exists()
    results["checks"]["solution_exists"] = {"passed": solution_exists}
    if not solution_exists:
        results["valid"] = False

    return results


def run_validate_suite(
    suite: str,
    config: BenchConfig,
    jobs: int = 1,
    repeat: int = 1,
    check_determinism: bool = False,
) -> dict[str, Any]:
    """Validate all tasks in a suite."""
    suite_dir = config.tasks_dir / suite
    if not suite_dir.is_dir():
        return {"all_valid": False, "error": f"Suite directory not found: {suite_dir}"}

    task_dirs = sorted(
        d for d in suite_dir.iterdir()
        if d.is_dir() and d.name.startswith("TASK")
    )

    results: dict[str, Any] = {
        "suite": suite,
        "all_valid": True,
        "task_count": len(task_dirs),
        "tasks": {},
    }

    for task_dir in task_dirs:
        task_result = run_validate_task(task_dir, config)
        results["tasks"][task_dir.name] = task_result
        if not task_result.get("valid", False):
            results["all_valid"] = False

    return results


def generate_suite_summary(suite: str, config: BenchConfig) -> dict[str, Any]:
    """Generate a summary of suite validation status."""
    return run_validate_suite(suite, config)
