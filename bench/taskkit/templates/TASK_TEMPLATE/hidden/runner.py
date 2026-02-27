"""Hidden test runner template.

This script is invoked by the benchmark grader:
    python runner.py <workspace_path> <cases_jsonl_path>

It must output JSONL to stdout with one record per test case.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path


def run_case(workspace: Path, case: dict) -> dict:
    """Run a single test case and return the result record."""
    case_id = case["case_id"]
    start = time.monotonic()

    try:
        # TODO: Import code under test from workspace
        # sys.path.insert(0, str(workspace / "src"))
        # from module import function

        # TODO: Execute the test case
        # result = function(**case["input_data"])
        # expected = case.get("expected", {})
        # passed = result == expected.get("output")

        passed = False  # Placeholder
        error_type = None
        error_message = "Not implemented"

    except Exception as e:
        passed = False
        error_type = type(e).__name__
        error_message = str(e)[:512]

    duration_ms = (time.monotonic() - start) * 1000

    return {
        "case_id": case_id,
        "passed": passed,
        "error_type": error_type if not passed else None,
        "error_message": error_message if not passed else None,
        "duration_ms": round(duration_ms, 2),
        "output_summary": "",
    }


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python runner.py <workspace> <cases.jsonl>", file=sys.stderr)
        sys.exit(1)

    workspace = Path(sys.argv[1])
    cases_path = Path(sys.argv[2])

    for line in cases_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        case = json.loads(line)
        result = run_case(workspace, case)
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
