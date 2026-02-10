from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class Parameter:
    key: str
    name: str
    description: str
    knob_id: str
    options: List[Any]
    role: str = "core"
    scientific_role: str = "control"
    recommended_variable: bool = False


FALLBACK_PARAMETER_CATALOG: List[Parameter] = [
    Parameter(
        key="replications",
        name="Replication Count",
        description="How many repeated runs per task to improve estimate stability.",
        knob_id="design.replications",
        options=[1, 3, 5, 10],
        role="core",
        scientific_role="control",
        recommended_variable=True,
    ),
    Parameter(
        key="dataset_limit",
        name="Task Count",
        description="How many tasks are included in each arm.",
        knob_id="dataset.limit",
        options=[10, 25, 50, 100, 200],
        role="core",
        scientific_role="control",
        recommended_variable=True,
    ),
    Parameter(
        key="network_mode",
        name="Network Policy",
        description="Internet access policy during trials. Keep fixed for clean causal comparisons.",
        knob_id="runtime.network.mode",
        options=["none", "full", "allowlist_enforced"],
        role="infra",
        scientific_role="invariant",
        recommended_variable=False,
    ),
    Parameter(
        key="integration_level",
        name="Evidence Depth",
        description="Telemetry depth from hooks/tracing integration. Usually a control parameter.",
        knob_id="runtime.harness.integration_level",
        options=["cli_basic", "cli_events", "otel", "sdk_control", "sdk_full"],
        role="harness",
        scientific_role="confound",
        recommended_variable=False,
    ),
]


def _slug(value: str) -> str:
    out = []
    for ch in value.lower():
        if ch.isalnum():
            out.append(ch)
        elif out and out[-1] != "_":
            out.append("_")
    slug = "".join(out).strip("_")
    return slug or "parameter"


def _fallback_description(knob_id: str) -> str:
    if knob_id == "runtime.harness.command":
        return "Harness entrypoint command. Usually fixed across comparison arms."
    if knob_id.startswith("runtime.network"):
        return "Network configuration. Keep fixed unless you are explicitly studying network effects."
    if knob_id.startswith("design."):
        return "Experiment design parameter."
    return "Experiment parameter."


def _options_from_numeric(
    value_type: str,
    minimum: Optional[float],
    maximum: Optional[float],
    current: Optional[Any],
    step: Optional[float],
) -> List[Any]:
    if value_type == "integer":
        default_min = int(minimum) if minimum is not None else 1
        default_step = int(step) if step not in (None, 0) else 1
        cur = int(current) if isinstance(current, (int, float)) else default_min
        candidate_hi = cur + default_step
        if maximum is not None:
            candidate_hi = min(int(maximum), candidate_hi)
        if candidate_hi == cur:
            candidate_hi = max(default_min, cur - default_step)
        values = [cur]
        if candidate_hi != cur:
            values.append(candidate_hi)
        if minimum is not None and int(minimum) not in values:
            values.append(int(minimum))
        if maximum is not None and int(maximum) not in values:
            values.append(int(maximum))
        return values[:4]
    if value_type == "number":
        default_min = float(minimum) if minimum is not None else 0.0
        default_step = float(step) if step not in (None, 0) else 0.1
        cur = float(current) if isinstance(current, (int, float)) else default_min
        candidate_hi = cur + default_step
        if maximum is not None:
            candidate_hi = min(float(maximum), candidate_hi)
        values: List[float] = [cur]
        if candidate_hi != cur:
            values.append(candidate_hi)
        if minimum is not None and float(minimum) not in values:
            values.append(float(minimum))
        if maximum is not None and float(maximum) not in values:
            values.append(float(maximum))
        return values[:4]
    return []


def _coerce_options(knob: Dict[str, Any], base_values: Dict[str, Any]) -> List[Any]:
    explicit = knob.get("options")
    if isinstance(explicit, list) and explicit:
        return explicit
    knob_id = str(knob.get("id", ""))
    value_type = str(knob.get("type", ""))
    current = base_values.get(knob_id)
    if value_type == "boolean":
        return [False, True]
    if value_type in {"integer", "number"}:
        return _options_from_numeric(
            value_type=value_type,
            minimum=knob.get("minimum") if isinstance(knob.get("minimum"), (int, float)) else None,
            maximum=knob.get("maximum") if isinstance(knob.get("maximum"), (int, float)) else None,
            current=current,
            step=knob.get("step") if isinstance(knob.get("step"), (int, float)) else None,
        )
    if current is not None and isinstance(current, (str, int, float, bool)):
        return [current]
    return []


def _recommended_variable(role: str, scientific_role: str, options: List[Any]) -> bool:
    if len(options) < 2:
        return False
    if scientific_role in {"invariant", "confound", "derived"}:
        return False
    if scientific_role == "treatment":
        return True
    return role in {"core", "benchmark", "harness"}


def load_parameters_from_manifest(
    manifest: Optional[Dict[str, Any]],
    base_values: Optional[Dict[str, Any]] = None,
) -> List[Parameter]:
    base = base_values or {}
    knobs = []
    if isinstance(manifest, dict):
        raw = manifest.get("knobs")
        if isinstance(raw, list):
            knobs = [k for k in raw if isinstance(k, dict)]
    if not knobs:
        return FALLBACK_PARAMETER_CATALOG

    params: List[Parameter] = []
    for knob in knobs:
        knob_id = str(knob.get("id", "")).strip()
        if not knob_id:
            continue
        name = str(knob.get("label", "")).strip() or knob_id
        desc = str(knob.get("description", "")).strip() or _fallback_description(knob_id)
        role = str(knob.get("role", "core"))
        scientific_role = str(knob.get("scientific_role", "control"))
        options = _coerce_options(knob, base)
        params.append(
            Parameter(
                key=_slug(knob_id),
                name=name,
                description=desc,
                knob_id=knob_id,
                options=options,
                role=role,
                scientific_role=scientific_role,
                recommended_variable=_recommended_variable(role, scientific_role, options),
            )
        )

    if not params:
        return FALLBACK_PARAMETER_CATALOG
    return sorted(params, key=lambda p: p.name.lower())


def parameter_by_name(name: str, parameters: List[Parameter]) -> Parameter:
    for p in parameters:
        if p.name == name:
            return p
    raise KeyError(name)


def default_parameter_names(parameters: List[Parameter]) -> List[str]:
    recommended = [p.name for p in parameters if p.recommended_variable]
    if recommended:
        return recommended
    return [p.name for p in parameters if len(p.options) >= 2]


def control_parameter_names(primary_name: str, parameters: List[Parameter]) -> List[str]:
    return [p.name for p in parameters if p.name != primary_name and p.options]


def merge_values(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    out.update(updates)
    return out


def build_arm_overrides(
    manifest_path: str,
    base_values: Dict[str, Any],
    variable: Parameter,
    arm_a_value: Any,
    arm_b_value: Any,
    secondary_updates: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    arm_a_values = merge_values(base_values, {variable.knob_id: arm_a_value})
    arm_b_values = merge_values(base_values, {variable.knob_id: arm_b_value})
    if secondary_updates:
        arm_a_values = merge_values(arm_a_values, secondary_updates)
        arm_b_values = merge_values(arm_b_values, secondary_updates)

    arm_a = {
        "schema_version": "experiment_overrides_v1",
        "manifest_path": manifest_path,
        "values": arm_a_values,
    }
    arm_b = {
        "schema_version": "experiment_overrides_v1",
        "manifest_path": manifest_path,
        "values": arm_b_values,
    }
    summary = [
        f"Variable under test: {variable.name}",
        f"Arm A (baseline) value: {arm_a_value}",
        f"Arm B (comparison) value: {arm_b_value}",
    ]
    if secondary_updates:
        for k, v in secondary_updates.items():
            summary.append(f"Controlled parameter: {k} = {v}")
    return arm_a, arm_b, summary


def summarize_trials(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    trials = len(rows)
    successes = sum(1 for r in rows if bool(r.get("success")))
    success_rate = (successes / trials) if trials else 0.0
    pm_vals: List[float] = []
    pm_name = "primary_metric"
    for r in rows:
        if isinstance(r.get("primary_metric_name"), str):
            pm_name = r["primary_metric_name"]
        v = r.get("primary_metric_value")
        if isinstance(v, (int, float)):
            pm_vals.append(float(v))
    pm_mean = (sum(pm_vals) / len(pm_vals)) if pm_vals else 0.0
    return {
        "trials": trials,
        "successes": successes,
        "success_rate": success_rate,
        "primary_metric_name": pm_name,
        "primary_metric_mean": pm_mean,
    }


def compare_summaries(baseline: Dict[str, Any], treatment: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "success_rate_delta": treatment.get("success_rate", 0.0) - baseline.get("success_rate", 0.0),
        "primary_metric_delta": treatment.get("primary_metric_mean", 0.0)
        - baseline.get("primary_metric_mean", 0.0),
        "primary_metric_name": treatment.get("primary_metric_name", baseline.get("primary_metric_name", "primary_metric")),
    }


# Backward-compatible wrappers for older imports.
Factor = Parameter
FACTOR_CATALOG = FALLBACK_PARAMETER_CATALOG


def factor_by_name(name: str) -> Parameter:
    return parameter_by_name(name, FALLBACK_PARAMETER_CATALOG)


def default_factor_names() -> List[str]:
    return default_parameter_names(FALLBACK_PARAMETER_CATALOG)


def control_factor_names(primary_name: str) -> List[str]:
    return control_parameter_names(primary_name, FALLBACK_PARAMETER_CATALOG)
