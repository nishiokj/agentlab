from typing import List, Tuple


def holm(p_values: List[float]) -> List[float]:
    indexed = list(enumerate(p_values))
    indexed.sort(key=lambda x: x[1])
    m = len(p_values)
    adjusted = [0.0] * m
    for i, (idx, p) in enumerate(indexed):
        adj = min(1.0, p * (m - i))
        adjusted[idx] = adj
    # enforce monotonicity
    for i in range(1, m):
        adjusted[indexed[i][0]] = max(adjusted[indexed[i][0]], adjusted[indexed[i-1][0]])
    return adjusted


def benjamini_hochberg(p_values: List[float]) -> List[float]:
    indexed = list(enumerate(p_values))
    indexed.sort(key=lambda x: x[1])
    m = len(p_values)
    adjusted = [0.0] * m
    for i, (idx, p) in enumerate(indexed, start=1):
        adjusted[idx] = min(1.0, p * m / i)
    # enforce monotonicity
    for i in range(m - 2, -1, -1):
        idx_curr = indexed[i][0]
        idx_next = indexed[i + 1][0]
        adjusted[idx_curr] = min(adjusted[idx_curr], adjusted[idx_next])
    return adjusted
