from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence

import yaml

from agentlab_core.canonical_json import canonical_dumps
from agentlab_core.hashing import sha256_bytes


def _stable(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _stable(value[k]) for k in sorted(value.keys())}
    if isinstance(value, list):
        return [_stable(v) for v in value]
    return value


def _default_analysis_tests() -> Dict[str, Dict[str, Any]]:
    return {
        "success": {"method": "paired_bootstrap", "ci": 0.95, "resamples": 1000},
        "latency_ms": {"method": "paired_bootstrap", "ci": 0.95, "resamples": 1000},
    }


@dataclass
class Variant:
    variant_id: str
    bindings: Dict[str, Any] = field(default_factory=dict)

    def bind(self, key: str, value: Any) -> "Variant":
        self.bindings[key] = value
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            "variant_id": self.variant_id,
            "bindings": copy.deepcopy(self.bindings),
        }


@dataclass
class VariantPlan:
    baseline: Variant = field(default_factory=lambda: Variant("base"))
    variants: List[Variant] = field(default_factory=list)

    def set_baseline(
        self,
        variant_id: str = "base",
        bindings: Optional[Mapping[str, Any]] = None,
        **binding_kwargs: Any,
    ) -> "VariantPlan":
        merged = dict(bindings or {})
        merged.update(binding_kwargs)
        self.baseline = Variant(variant_id=variant_id, bindings=merged)
        return self

    def add_variant(
        self,
        variant_id: str,
        bindings: Optional[Mapping[str, Any]] = None,
        **binding_kwargs: Any,
    ) -> "VariantPlan":
        merged = dict(bindings or {})
        merged.update(binding_kwargs)
        self.variants.append(Variant(variant_id=variant_id, bindings=merged))
        return self

    def to_config(self) -> Dict[str, Any]:
        return {
            "baseline": self.baseline.to_dict(),
            "variant_plan": [v.to_dict() for v in self.variants],
        }


@dataclass
class AnalysisPlan:
    primary_metrics: List[str] = field(default_factory=lambda: ["success"])
    secondary_metrics: List[str] = field(default_factory=lambda: ["latency_ms"])
    missingness_policy: str = "paired_drop"
    record_missingness_reasons: bool = True
    tests: Dict[str, Dict[str, Any]] = field(default_factory=_default_analysis_tests)
    multiple_comparisons_method: str = "none"
    effect_sizes: List[str] = field(default_factory=lambda: ["risk_diff", "median_diff"])
    show_task_level_table: bool = True

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AnalysisPlan":
        missingness = data.get("missingness") if isinstance(data.get("missingness"), Mapping) else {}
        reporting = data.get("reporting") if isinstance(data.get("reporting"), Mapping) else {}
        multiple = (
            data.get("multiple_comparisons")
            if isinstance(data.get("multiple_comparisons"), Mapping)
            else {}
        )
        tests = data.get("tests") if isinstance(data.get("tests"), Mapping) else _default_analysis_tests()
        return cls(
            primary_metrics=list(data.get("primary_metrics") or ["success"]),
            secondary_metrics=list(data.get("secondary_metrics") or ["latency_ms"]),
            missingness_policy=str(missingness.get("policy", "paired_drop")),
            record_missingness_reasons=bool(missingness.get("record_reasons", True)),
            tests=copy.deepcopy(dict(tests)),
            multiple_comparisons_method=str(multiple.get("method", "none")),
            effect_sizes=list(reporting.get("effect_sizes") or ["risk_diff", "median_diff"]),
            show_task_level_table=bool(reporting.get("show_task_level_table", True)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "primary_metrics": list(self.primary_metrics),
            "secondary_metrics": list(self.secondary_metrics),
            "missingness": {
                "policy": self.missingness_policy,
                "record_reasons": self.record_missingness_reasons,
            },
            "tests": copy.deepcopy(self.tests),
            "multiple_comparisons": {"method": self.multiple_comparisons_method},
            "reporting": {
                "effect_sizes": list(self.effect_sizes),
                "show_task_level_table": self.show_task_level_table,
            },
        }


class Experiment:
    _TOP_LEVEL_ORDER = [
        "version",
        "experiment",
        "dataset",
        "design",
        "analysis_plan",
        "baseline",
        "variant_plan",
        "runtime",
        "validity",
    ]

    def __init__(self, data: Mapping[str, Any]) -> None:
        self._data = copy.deepcopy(dict(data))

    @classmethod
    def builder(cls, experiment_id: str, name: str) -> "ExperimentBuilder":
        return ExperimentBuilder(experiment_id=experiment_id, name=name)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Experiment":
        return cls(data)

    @classmethod
    def from_yaml_file(cls, path: str) -> "Experiment":
        with open(path, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f)
        if not isinstance(loaded, dict):
            raise ValueError("Experiment YAML must be a mapping")
        return cls(loaded)

    @classmethod
    def from_json_file(cls, path: str) -> "Experiment":
        with open(path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        if not isinstance(loaded, dict):
            raise ValueError("Experiment JSON must be an object")
        return cls(loaded)

    def to_dict(self) -> Dict[str, Any]:
        return copy.deepcopy(self._data)

    def _ordered_dict(self) -> Dict[str, Any]:
        data = self.to_dict()
        out: Dict[str, Any] = {}
        for key in self._TOP_LEVEL_ORDER:
            if key in data:
                out[key] = _stable(data[key])
        for key in sorted(data.keys()):
            if key not in out:
                out[key] = _stable(data[key])
        return out

    def to_json(self, indent: Optional[int] = 2) -> str:
        kwargs: Dict[str, Any] = {"ensure_ascii": True}
        if indent is None:
            kwargs["separators"] = (",", ":")
        else:
            kwargs["indent"] = indent
        return json.dumps(self._ordered_dict(), **kwargs)

    def to_yaml(self) -> str:
        return yaml.safe_dump(self._ordered_dict(), sort_keys=False)

    def write_json(self, path: str, indent: Optional[int] = 2) -> str:
        abs_path = os.path.abspath(path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "w", encoding="utf-8") as f:
            f.write(self.to_json(indent=indent))
            f.write("\n")
        return abs_path

    def write_yaml(self, path: str) -> str:
        abs_path = os.path.abspath(path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "w", encoding="utf-8") as f:
            f.write(self.to_yaml())
        return abs_path

    def digest(self) -> str:
        return sha256_bytes(canonical_dumps(self._ordered_dict()).encode("utf-8"))


class ExperimentBuilder:
    def __init__(self, experiment_id: str, name: str) -> None:
        self._analysis_plan = AnalysisPlan()
        self._variant_plan = VariantPlan()
        self._data: Dict[str, Any] = {
            "version": "0.3",
            "experiment": {
                "id": experiment_id,
                "name": name,
                "description": "Generated by AgentLab SDK",
                "owner": "you",
                "tags": [],
            },
            "dataset": {
                "suite_id": "local_suite",
                "provider": "local_jsonl",
                "path": "tasks.jsonl",
                "schema_version": "task_jsonl_v1",
                "split_id": "dev",
                "limit": 50,
            },
            "design": {
                "sanitization_profile": "hermetic_functional_v2",
                "comparison": "paired",
                "replications": 1,
                "random_seed": 1337,
                "shuffle_tasks": True,
                "max_concurrency": 1,
            },
            "runtime": {
                "harness": {
                    "mode": "cli",
                    "command": ["<set-your-harness-command>"],
                    "integration_level": "cli_basic",
                    "input_path": "/out/trial_input.json",
                    "output_path": "/out/trial_output.json",
                    "control_plane": {"mode": "file", "path": "/state/lab_control.json"},
                },
                "network": {
                    "mode": "none",
                    "allowed_hosts": [],
                },
            },
            "validity": {
                "fail_on_state_leak": True,
                "fail_on_profile_invariant_violation": True,
            },
        }

    def metadata(
        self,
        *,
        description: Optional[str] = None,
        owner: Optional[str] = None,
        tags: Optional[Sequence[str]] = None,
    ) -> "ExperimentBuilder":
        meta = self._data.setdefault("experiment", {})
        if description is not None:
            meta["description"] = description
        if owner is not None:
            meta["owner"] = owner
        if tags is not None:
            meta["tags"] = list(tags)
        return self

    def dataset_jsonl(
        self,
        path: str,
        *,
        suite_id: str = "local_suite",
        split_id: str = "dev",
        limit: Optional[int] = 50,
        schema_version: str = "task_jsonl_v1",
    ) -> "ExperimentBuilder":
        self._data["dataset"] = {
            "suite_id": suite_id,
            "provider": "local_jsonl",
            "path": path,
            "schema_version": schema_version,
            "split_id": split_id,
            "limit": limit,
        }
        return self

    def design(
        self,
        *,
        replications: Optional[int] = None,
        random_seed: Optional[int] = None,
        shuffle_tasks: Optional[bool] = None,
        max_concurrency: Optional[int] = None,
        sanitization_profile: Optional[str] = None,
        comparison: Optional[str] = None,
    ) -> "ExperimentBuilder":
        design = self._data.setdefault("design", {})
        if replications is not None:
            design["replications"] = replications
        if random_seed is not None:
            design["random_seed"] = random_seed
        if shuffle_tasks is not None:
            design["shuffle_tasks"] = shuffle_tasks
        if max_concurrency is not None:
            design["max_concurrency"] = max_concurrency
        if sanitization_profile is not None:
            design["sanitization_profile"] = sanitization_profile
        if comparison is not None:
            design["comparison"] = comparison
        return self

    def analysis_plan(self, plan: AnalysisPlan | Mapping[str, Any]) -> "ExperimentBuilder":
        if isinstance(plan, AnalysisPlan):
            self._analysis_plan = plan
        else:
            self._analysis_plan = AnalysisPlan.from_dict(plan)
        return self

    def variant_plan(self, plan: VariantPlan) -> "ExperimentBuilder":
        self._variant_plan = copy.deepcopy(plan)
        return self

    def baseline(
        self,
        variant_id: str = "base",
        bindings: Optional[Mapping[str, Any]] = None,
        **binding_kwargs: Any,
    ) -> "ExperimentBuilder":
        self._variant_plan.set_baseline(variant_id=variant_id, bindings=bindings, **binding_kwargs)
        return self

    def add_variant(
        self,
        variant_id: str,
        bindings: Optional[Mapping[str, Any]] = None,
        **binding_kwargs: Any,
    ) -> "ExperimentBuilder":
        self._variant_plan.add_variant(variant_id=variant_id, bindings=bindings, **binding_kwargs)
        return self

    def harness_cli(
        self,
        command: Sequence[str],
        *,
        integration_level: str = "cli_basic",
        input_path: str = "/out/trial_input.json",
        output_path: str = "/out/trial_output.json",
        control_plane_mode: str = "file",
        control_plane_path: str = "/state/lab_control.json",
    ) -> "ExperimentBuilder":
        harness = self._data.setdefault("runtime", {}).setdefault("harness", {})
        harness["mode"] = "cli"
        harness["command"] = list(command)
        harness["integration_level"] = integration_level
        harness["input_path"] = input_path
        harness["output_path"] = output_path
        harness["control_plane"] = {
            "mode": control_plane_mode,
            "path": control_plane_path,
        }
        if integration_level in ("cli_events", "sdk_control", "sdk_full"):
            harness["events"] = {
                "mode": "jsonl",
                "path": "/out/harness_events.jsonl",
                "schema_version": "hook_events_v1",
            }
            harness.pop("tracing", None)
        elif integration_level == "otel":
            harness["tracing"] = {"mode": "otlp", "otlp_endpoint": "http://127.0.0.1:4318"}
            harness.pop("events", None)
        else:
            harness.pop("events", None)
            harness.pop("tracing", None)
        return self

    def network(self, *, mode: str = "none", allowed_hosts: Optional[Sequence[str]] = None) -> "ExperimentBuilder":
        self._data.setdefault("runtime", {})["network"] = {
            "mode": mode,
            "allowed_hosts": list(allowed_hosts or []),
        }
        return self

    def validity(
        self,
        *,
        fail_on_state_leak: Optional[bool] = None,
        fail_on_profile_invariant_violation: Optional[bool] = None,
    ) -> "ExperimentBuilder":
        validity = self._data.setdefault("validity", {})
        if fail_on_state_leak is not None:
            validity["fail_on_state_leak"] = fail_on_state_leak
        if fail_on_profile_invariant_violation is not None:
            validity["fail_on_profile_invariant_violation"] = fail_on_profile_invariant_violation
        return self

    def build(self) -> Experiment:
        payload = copy.deepcopy(self._data)
        payload["analysis_plan"] = self._analysis_plan.to_dict()
        payload.update(self._variant_plan.to_config())
        return Experiment(payload)
