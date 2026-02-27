"""Tests for mutant gate automation."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.taskkit.mutants import list_mutant_patches, MutantGateResult, MUTANT_STRATEGIES


class TestMutantGate:
    def test_list_empty_dir(self, tmp_path):
        patches = list_mutant_patches(tmp_path)
        assert patches == []

    def test_list_mutant_patches(self, tmp_path):
        mutants_dir = tmp_path / "mutants"
        mutants_dir.mkdir()
        for i in range(10):
            (mutants_dir / f"M{i+1:02d}.patch").write_text(f"mutant {i+1}")
        patches = list_mutant_patches(tmp_path)
        assert len(patches) == 10
        assert patches[0].name == "M01.patch"

    def test_mutant_strategies_cover_minimum(self):
        """Verify we have at least 10 documented mutant strategies."""
        assert len(MUTANT_STRATEGIES) >= 10

    def test_gate_result_insufficient_mutants(self):
        result = MutantGateResult(mutants_total=5, passed=False)
        assert not result.passed

    def test_gate_result_all_killed(self):
        result = MutantGateResult(
            mutants_total=10,
            mutants_killed=10,
            unkilled_list=[],
            passed=True,
        )
        assert result.passed
        assert len(result.unkilled_list) == 0

    def test_gate_result_some_unkilled(self):
        result = MutantGateResult(
            mutants_total=10,
            mutants_killed=8,
            unkilled_list=["M03", "M07"],
            passed=False,
        )
        assert not result.passed
        assert "M03" in result.unkilled_list
