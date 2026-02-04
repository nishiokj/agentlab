import json
from typing import Any, Dict

from .models import AnalysisPlan


def plan_from_resolved_experiment(resolved: Dict[str, Any]) -> AnalysisPlan:
    plan_data = resolved.get("analysis_plan") or {}
    missingness = plan_data.get("missingness", {})

    return AnalysisPlan(
        primary_metrics=plan_data.get("primary_metrics", ["success"]),
        secondary_metrics=plan_data.get("secondary_metrics", []),
        missingness_policy=missingness.get("policy", "paired_drop"),
        multiple_comparisons=(plan_data.get("multiple_comparisons") or {}).get(
            "method", "none"
        ),
        tests=plan_data.get("tests", {}),
        effect_sizes=(plan_data.get("reporting") or {}).get(
            "effect_sizes", ["risk_diff", "median_diff"]
        ),
        show_task_level_table=(plan_data.get("reporting") or {}).get(
            "show_task_level_table", True
        ),
    )
