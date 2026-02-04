from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class TrialRecord:
    run_id: str
    trial_id: str
    variant_id: str
    task_id: str
    repl_idx: int
    outcome: str
    metrics: Dict[str, Any]
    missing_reason: Optional[str] = None


@dataclass
class PairedRecord:
    task_id: str
    repl_idx: int
    baseline: TrialRecord
    variant: TrialRecord


@dataclass
class AnalysisPlan:
    primary_metrics: List[str] = field(default_factory=lambda: ["success"])
    secondary_metrics: List[str] = field(default_factory=list)
    missingness_policy: str = "paired_drop"
    multiple_comparisons: str = "none"
    tests: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    effect_sizes: List[str] = field(default_factory=lambda: ["risk_diff", "median_diff"])
    show_task_level_table: bool = True


@dataclass
class MetricResult:
    metric: str
    effect_sizes: Dict[str, float]
    ci_low: float
    ci_high: float
    p_value: float
    n_pairs: int
    n_missing: int
