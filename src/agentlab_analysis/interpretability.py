import json
import os
from typing import Any, Dict, List


def write_jsonl(rows: List[Dict[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def write_json(data: Dict[str, Any], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def write_parquet_if_available(rows: List[Dict[str, Any]], path: str) -> bool:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception:
        return False

    table = pa.Table.from_pylist(rows)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path)
    return True


def build_exemplars(rows: List[Dict[str, Any]], metric: str, k: int = 5) -> Dict[str, Any]:
    # assumes rows contain metric delta as f"delta_{metric}"
    key = f"delta_{metric}"
    rows_with_delta = [r for r in rows if key in r and r[key] is not None]
    rows_sorted = sorted(rows_with_delta, key=lambda r: r[key])
    worst = rows_sorted[:k]
    best = rows_sorted[-k:][::-1]
    # uncertainty proxy: closest to zero
    uncertain = sorted(rows_with_delta, key=lambda r: abs(r[key]))[:k]
    return {
        "worst_regressions": worst,
        "best_improvements": best,
        "highest_uncertainty": uncertain,
    }


def build_suspects(evidence_sources: Dict[str, bool]) -> Dict[str, Any]:
    return {
        "evidence_sources": evidence_sources,
        "confound_suspects": ["insufficient_evidence"],
        "behavior_suspects": ["insufficient_evidence"],
        "notes": ["Suspects require hooks/traces or framework events."]
    }
