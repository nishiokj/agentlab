"""Deterministic case generation framework.

Provides utilities for generating >=50 hidden test cases per task
with seeded randomness and stable serialization.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable, Iterator

from bench.taskkit.determinism import get_seeded_rng, stable_json, stable_hash


@dataclass
class TestCase:
    """A single test case for the hidden suite."""
    case_id: str
    case_type: str  # "api_call", "cli_invocation", "assertion"
    input_data: dict[str, Any] = field(default_factory=dict)
    expected: dict[str, Any] | None = None
    tags: list[str] = field(default_factory=list)
    timeout_s: int = 5


CaseGenerator = Callable[[int], Iterator[TestCase]]


def generate_cases(
    generator: CaseGenerator,
    seed: int = 42,
    min_cases: int = 50,
) -> list[TestCase]:
    """Run a case generator and validate minimum count.

    The generator receives the seed and yields TestCase objects.
    """
    cases = list(generator(seed))
    if len(cases) < min_cases:
        raise ValueError(
            f"Generator produced {len(cases)} cases, minimum is {min_cases}"
        )
    return cases


def write_cases_jsonl(cases: list[TestCase], output_path: Path) -> str:
    """Write cases to JSONL with stable serialization. Returns the file hash."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for case in cases:
        d = asdict(case)
        lines.append(stable_json(d))
    content = "\n".join(lines) + "\n"
    output_path.write_text(content)
    return stable_hash(content)


def read_cases_jsonl(path: Path) -> list[TestCase]:
    """Read cases from a JSONL file."""
    cases = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        cases.append(TestCase(**d))
    return cases


def verify_case_count(cases_path: Path, min_cases: int = 50) -> tuple[bool, int]:
    """Check that a cases.jsonl has at least min_cases. Returns (ok, count)."""
    cases = read_cases_jsonl(cases_path)
    return len(cases) >= min_cases, len(cases)


def verify_cases_determinism(
    generator: CaseGenerator,
    seed: int = 42,
) -> tuple[bool, str, str]:
    """Run generator twice and compare hashes. Returns (match, hash1, hash2)."""
    import tempfile
    cases1 = list(generator(seed))
    cases2 = list(generator(seed))

    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f1:
        p1 = Path(f1.name)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f2:
        p2 = Path(f2.name)

    h1 = write_cases_jsonl(cases1, p1)
    h2 = write_cases_jsonl(cases2, p2)

    p1.unlink(missing_ok=True)
    p2.unlink(missing_ok=True)

    return h1 == h2, h1, h2
