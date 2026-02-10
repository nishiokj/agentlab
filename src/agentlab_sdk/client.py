from __future__ import annotations

import copy
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Union

import yaml

from agentlab_analysis import run_analysis
from agentlab_report import build_report
from agentlab_runner.publish import publish_run
from agentlab_runner.run_engine import (
    fork_trial,
    replay_trial,
    run_experiment_spec,
    validate_experiment_spec,
)

from .models import Experiment

ExperimentInput = Union[Experiment, Mapping[str, Any], str, os.PathLike[str]]


@dataclass(frozen=True)
class RunResult:
    run_id: str
    run_dir: str
    report_dir: str


class AgentLabClient:
    def __init__(self, base_dir: Optional[str] = None) -> None:
        self.base_dir = os.path.abspath(base_dir or os.getcwd())

    def validate(self, experiment: ExperimentInput) -> Dict[str, Any]:
        payload, resolution_base = self._coerce_experiment(experiment)
        return validate_experiment_spec(payload, base_dir=resolution_base)

    def run(
        self,
        experiment: ExperimentInput,
        *,
        allow_missing_harness_manifest: bool = False,
    ) -> RunResult:
        payload, resolution_base = self._coerce_experiment(experiment)
        run_id, report_dir = run_experiment_spec(
            payload,
            allow_missing_manifest=allow_missing_harness_manifest,
            resolution_base_dir=resolution_base,
            run_base_dir=self.base_dir,
        )
        run_dir = os.path.join(self.base_dir, ".lab", "runs", run_id)
        return RunResult(run_id=run_id, run_dir=run_dir, report_dir=report_dir)

    def replay(self, trial_id: str, *, strict: bool = False) -> str:
        with self._in_base_dir():
            return replay_trial(trial_id, strict=strict)

    def fork(self, from_trial: str, at: str, bindings: Mapping[str, str]) -> str:
        with self._in_base_dir():
            return fork_trial(from_trial, at, dict(bindings))

    def publish(self, run_dir: str, out_path: Optional[str] = None) -> str:
        resolved_out = self._abs_path(out_path) if out_path else None
        return publish_run(self._abs_path(run_dir), resolved_out)

    def analyze(
        self,
        *,
        run_dir: str,
        baseline_id: str,
        variant_ids: Sequence[str],
        evidence_sources: Mapping[str, bool],
        random_seed: int = 1337,
    ) -> Dict[str, Any]:
        return run_analysis(
            run_dir=self._abs_path(run_dir),
            baseline_id=baseline_id,
            variant_ids=list(variant_ids),
            evidence_sources=dict(evidence_sources),
            random_seed=random_seed,
        )

    def report(self, *, run_dir: str, out_dir: Optional[str] = None) -> str:
        resolved_run_dir = self._abs_path(run_dir)
        resolved_out_dir = self._abs_path(out_dir) if out_dir else os.path.join(resolved_run_dir, "report")
        return build_report(resolved_run_dir, resolved_out_dir)

    def compare(
        self,
        *,
        run_dir: str,
        baseline_id: str,
        variant_ids: Sequence[str],
        evidence_sources: Mapping[str, bool],
        random_seed: int = 1337,
        out_dir: Optional[str] = None,
    ) -> str:
        self.analyze(
            run_dir=run_dir,
            baseline_id=baseline_id,
            variant_ids=variant_ids,
            evidence_sources=evidence_sources,
            random_seed=random_seed,
        )
        return self.report(run_dir=run_dir, out_dir=out_dir)

    def _coerce_experiment(self, experiment: ExperimentInput) -> tuple[Dict[str, Any], str]:
        if isinstance(experiment, Experiment):
            return experiment.to_dict(), self.base_dir
        if isinstance(experiment, Mapping):
            return copy.deepcopy(dict(experiment)), self.base_dir
        if isinstance(experiment, (str, os.PathLike)):
            path = os.path.abspath(os.path.join(self.base_dir, os.fspath(experiment)))
            with open(path, "r", encoding="utf-8") as f:
                payload = yaml.safe_load(f)
            if not isinstance(payload, dict):
                raise ValueError("Experiment YAML must be a mapping")
            return payload, os.path.dirname(path)
        raise TypeError("experiment must be an Experiment, mapping, or path")

    def _abs_path(self, path: str) -> str:
        if os.path.isabs(path):
            return path
        return os.path.abspath(os.path.join(self.base_dir, path))

    @contextmanager
    def _in_base_dir(self):
        prev = os.getcwd()
        try:
            os.chdir(self.base_dir)
            yield
        finally:
            os.chdir(prev)
