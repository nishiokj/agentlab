import math
import random
from statistics import mean, median
from typing import List, Tuple


def effect_sizes(base: List[float], var: List[float]) -> dict:
    return {
        "risk_diff": mean(var) - mean(base),
        "median_diff": median(var) - median(base),
        "mean_diff": mean(var) - mean(base),
    }


def paired_bootstrap(
    base: List[float],
    var: List[float],
    resamples: int,
    seed: int,
) -> List[float]:
    if len(base) != len(var):
        raise ValueError("Base/variant lengths must match")
    rng = random.Random(seed)
    n = len(base)
    if n == 0:
        return []
    diffs = []
    for _ in range(resamples):
        idx = [rng.randrange(n) for _ in range(n)]
        b = [base[i] for i in idx]
        v = [var[i] for i in idx]
        diffs.append(mean(v) - mean(b))
    return diffs


def ci_from_bootstrap(diffs: List[float], ci: float) -> Tuple[float, float]:
    if not diffs:
        return (float("nan"), float("nan"))
    lo = (1.0 - ci) / 2.0
    hi = 1.0 - lo
    diffs_sorted = sorted(diffs)
    lo_idx = int(math.floor(lo * (len(diffs_sorted) - 1)))
    hi_idx = int(math.ceil(hi * (len(diffs_sorted) - 1)))
    return diffs_sorted[lo_idx], diffs_sorted[hi_idx]


def p_value_from_bootstrap(diffs: List[float]) -> float:
    if not diffs:
        return float("nan")
    pos = sum(1 for d in diffs if d >= 0)
    neg = sum(1 for d in diffs if d <= 0)
    p = 2 * min(pos / len(diffs), neg / len(diffs))
    return min(1.0, p)
