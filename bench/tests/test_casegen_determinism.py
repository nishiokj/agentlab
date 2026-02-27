"""Tests for deterministic case generation."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from bench.taskkit.casegen import (
    TestCase,
    generate_cases,
    write_cases_jsonl,
    read_cases_jsonl,
    verify_case_count,
    verify_cases_determinism,
)
from bench.taskkit.determinism import (
    get_seeded_rng,
    stable_json,
    stable_hash,
    hash_jsonl_file,
)


def _sample_generator(seed: int):
    """Sample case generator for testing."""
    rng = get_seeded_rng(seed)
    for i in range(60):
        yield TestCase(
            case_id=f"case_{i:03d}",
            case_type="api_call",
            input_data={"x": rng.randint(0, 1000), "y": rng.randint(0, 1000)},
            expected={"sum": None},  # Placeholder
            tags=["basic"],
        )


class TestDeterminism:
    def test_seeded_rng_reproducible(self):
        r1 = get_seeded_rng(42)
        r2 = get_seeded_rng(42)
        values1 = [r1.randint(0, 100) for _ in range(20)]
        values2 = [r2.randint(0, 100) for _ in range(20)]
        assert values1 == values2

    def test_stable_json_sorted_keys(self):
        a = stable_json({"z": 1, "a": 2, "m": 3})
        b = stable_json({"a": 2, "m": 3, "z": 1})
        assert a == b

    def test_stable_hash_consistent(self):
        h1 = stable_hash("hello world")
        h2 = stable_hash("hello world")
        assert h1 == h2

    def test_stable_hash_different_inputs(self):
        h1 = stable_hash("hello")
        h2 = stable_hash("world")
        assert h1 != h2


class TestCaseGeneration:
    def test_generate_cases_meets_minimum(self):
        cases = generate_cases(_sample_generator, seed=42, min_cases=50)
        assert len(cases) >= 50

    def test_generate_cases_rejects_too_few(self):
        def small_gen(seed):
            for i in range(5):
                yield TestCase(case_id=f"c{i}", case_type="api_call")
        with pytest.raises(ValueError, match="minimum"):
            generate_cases(small_gen, seed=42, min_cases=50)

    def test_write_and_read_cases(self, tmp_path):
        cases = generate_cases(_sample_generator, seed=42)
        path = tmp_path / "cases.jsonl"
        write_cases_jsonl(cases, path)

        loaded = read_cases_jsonl(path)
        assert len(loaded) == len(cases)
        assert loaded[0].case_id == cases[0].case_id

    def test_write_cases_deterministic_hash(self, tmp_path):
        cases1 = generate_cases(_sample_generator, seed=42)
        cases2 = generate_cases(_sample_generator, seed=42)

        p1 = tmp_path / "c1.jsonl"
        p2 = tmp_path / "c2.jsonl"
        h1 = write_cases_jsonl(cases1, p1)
        h2 = write_cases_jsonl(cases2, p2)
        assert h1 == h2

    def test_hash_jsonl_file_deterministic(self, tmp_path):
        cases = generate_cases(_sample_generator, seed=42)
        p1 = tmp_path / "c1.jsonl"
        p2 = tmp_path / "c2.jsonl"
        write_cases_jsonl(cases, p1)
        write_cases_jsonl(cases, p2)
        assert hash_jsonl_file(p1) == hash_jsonl_file(p2)

    def test_verify_case_count(self, tmp_path):
        cases = generate_cases(_sample_generator, seed=42)
        path = tmp_path / "cases.jsonl"
        write_cases_jsonl(cases, path)
        ok, count = verify_case_count(path, min_cases=50)
        assert ok
        assert count >= 50

    def test_verify_cases_determinism(self):
        match, h1, h2 = verify_cases_determinism(_sample_generator, seed=42)
        assert match
        assert h1 == h2
