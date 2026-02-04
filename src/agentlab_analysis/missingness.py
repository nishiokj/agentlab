from statistics import median
from typing import List, Tuple, Optional


def _median(values: List[float]) -> float:
    if not values:
        return 0.0
    return float(median(values))


def prepare_pairs(
    base_vals: List[Optional[float]],
    var_vals: List[Optional[float]],
    policy: str,
    metric: str,
) -> Tuple[List[float], List[float], int]:
    """Apply missingness policy and return filtered/imputed pairs.

    Returns (base_filtered, var_filtered, missing_count)
    """
    missing_count = 0
    base_out: List[float] = []
    var_out: List[float] = []

    base_obs = [v for v in base_vals if v is not None]
    var_obs = [v for v in var_vals if v is not None]
    base_impute = _median(base_obs)
    var_impute = _median(var_obs)

    for b, v in zip(base_vals, var_vals):
        b_missing = b is None
        v_missing = v is None
        if b_missing or v_missing:
            missing_count += 1

        if policy == "paired_drop":
            if b_missing or v_missing:
                continue
            base_out.append(float(b))
            var_out.append(float(v))
            continue

        if policy == "paired_impute":
            if b_missing:
                b = base_impute
            if v_missing:
                v = var_impute
            base_out.append(float(b))
            var_out.append(float(v))
            continue

        if policy == "treat_as_failure":
            if metric == "success":
                if b_missing:
                    b = 0.0
                if v_missing:
                    v = 0.0
                base_out.append(float(b))
                var_out.append(float(v))
            else:
                if b_missing or v_missing:
                    continue
                base_out.append(float(b))
                var_out.append(float(v))
            continue

        # default fallback
        if b_missing or v_missing:
            continue
        base_out.append(float(b))
        var_out.append(float(v))

    return base_out, var_out, missing_count
