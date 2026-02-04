import json
import os
from typing import Any, Dict, List, Tuple

from .analysis_plan import plan_from_resolved_experiment
from .effects import effect_sizes, paired_bootstrap, ci_from_bootstrap, p_value_from_bootstrap
from .missingness import prepare_pairs
from .models import AnalysisPlan, MetricResult, PairedRecord, TrialRecord
from .multiple_comparisons import holm, benjamini_hochberg
from .interpretability import (
    write_json,
    write_jsonl,
    write_parquet_if_available,
    build_exemplars,
    build_suspects,
)


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_trial_records(run_dir: str) -> List[TrialRecord]:
    trials_root = os.path.join(run_dir, "trials")
    records: List[TrialRecord] = []
    if not os.path.isdir(trials_root):
        return records

    for trial_id in os.listdir(trials_root):
        tdir = os.path.join(trials_root, trial_id)
        if not os.path.isdir(tdir):
            continue

        metrics_path = os.path.join(tdir, "metrics.json")
        output_path = os.path.join(tdir, "trial_output.json")

        data = None
        if os.path.exists(metrics_path):
            data = _load_json(metrics_path)
        elif os.path.exists(output_path):
            data = _load_json(output_path)
        else:
            continue

        ids = data.get("ids") or {}
        outcome = data.get("outcome", "missing")
        metrics = data.get("metrics") or {}
        missing_reason = data.get("missingness_reason")

        record = TrialRecord(
            run_id=ids.get("run_id", ""),
            trial_id=ids.get("trial_id", trial_id),
            variant_id=ids.get("variant_id", ""),
            task_id=ids.get("task_id", ""),
            repl_idx=int(ids.get("repl_idx", 0)),
            outcome=outcome,
            metrics=metrics,
            missing_reason=missing_reason,
        )
        records.append(record)

    return records


def _metric_value(record: TrialRecord, metric: str) -> Any:
    if metric == "success":
        if record.outcome in ("missing", "error"):
            return None
        return 1.0 if record.outcome == "success" else 0.0
    return record.metrics.get(metric)


def _pair_records(
    records: List[TrialRecord],
    baseline_id: str,
    variant_id: str,
) -> List[PairedRecord]:
    by_key: Dict[Tuple[str, int, str], TrialRecord] = {}
    base_keys = set()
    var_keys = set()
    for r in records:
        key = (r.task_id, r.repl_idx, r.variant_id)
        by_key[key] = r
        if r.variant_id == baseline_id:
            base_keys.add((r.task_id, r.repl_idx))
        if r.variant_id == variant_id:
            var_keys.add((r.task_id, r.repl_idx))

    all_keys = sorted(base_keys.union(var_keys))
    pairs: List[PairedRecord] = []
    for task_id, repl_idx in all_keys:
        base = by_key.get((task_id, repl_idx, baseline_id))
        var = by_key.get((task_id, repl_idx, variant_id))
        if base is None:
            base = TrialRecord(
                run_id="",
                trial_id=f"missing:{baseline_id}:{task_id}:{repl_idx}",
                variant_id=baseline_id,
                task_id=task_id,
                repl_idx=repl_idx,
                outcome="missing",
                metrics={},
                missing_reason="missing_pair",
            )
        if var is None:
            var = TrialRecord(
                run_id="",
                trial_id=f"missing:{variant_id}:{task_id}:{repl_idx}",
                variant_id=variant_id,
                task_id=task_id,
                repl_idx=repl_idx,
                outcome="missing",
                metrics={},
                missing_reason="missing_pair",
            )
        pairs.append(PairedRecord(task_id=task_id, repl_idx=repl_idx, baseline=base, variant=var))

    return pairs


def _analyze_metric(
    pairs: List[PairedRecord],
    metric: str,
    plan: AnalysisPlan,
    seed: int,
) -> MetricResult:
    base_vals = [_metric_value(p.baseline, metric) for p in pairs]
    var_vals = [_metric_value(p.variant, metric) for p in pairs]

    base_vals_f, var_vals_f, missing_count = prepare_pairs(
        base_vals, var_vals, plan.missingness_policy, metric
    )

    if not base_vals_f:
        return MetricResult(metric, {}, float("nan"), float("nan"), float("nan"), 0, missing_count)

    effects = effect_sizes(base_vals_f, var_vals_f)

    test_cfg = plan.tests.get(metric, {})
    method = test_cfg.get("method", "paired_bootstrap")
    resamples = int(test_cfg.get("resamples", 1000))
    ci = float(test_cfg.get("ci", 0.95))

    if method != "paired_bootstrap":
        raise ValueError(f"Unsupported test method: {method}")

    diffs = paired_bootstrap(base_vals_f, var_vals_f, resamples=resamples, seed=seed)
    ci_low, ci_high = ci_from_bootstrap(diffs, ci)
    p_value = p_value_from_bootstrap(diffs)

    return MetricResult(
        metric=metric,
        effect_sizes={k: effects[k] for k in plan.effect_sizes if k in effects},
        ci_low=ci_low,
        ci_high=ci_high,
        p_value=p_value,
        n_pairs=len(base_vals_f),
        n_missing=missing_count,
    )


def run_analysis(
    run_dir: str,
    baseline_id: str,
    variant_ids: List[str],
    evidence_sources: Dict[str, bool],
    random_seed: int = 1337,
) -> Dict[str, Any]:
    resolved_path = os.path.join(run_dir, "resolved_experiment.json")
    resolved = _load_json(resolved_path) if os.path.exists(resolved_path) else {}
    plan = plan_from_resolved_experiment(resolved)

    records = load_trial_records(run_dir)
    results = {}
    comparisons = []

    for variant_id in variant_ids:
        pairs = _pair_records(records, baseline_id, variant_id)
        metric_results = []
        p_values = []
        metrics = plan.primary_metrics + plan.secondary_metrics
        for metric in metrics:
            r = _analyze_metric(pairs, metric, plan, seed=random_seed)
            metric_results.append(r)
            p_values.append(r.p_value)

        if plan.multiple_comparisons == "holm":
            adj = holm(p_values)
        elif plan.multiple_comparisons == "benjamini_hochberg":
            adj = benjamini_hochberg(p_values)
        else:
            adj = p_values

        summary_metrics = []
        for r, adj_p in zip(metric_results, adj):
            summary_metrics.append(
                {
                    "metric": r.metric,
                    "effect_sizes": r.effect_sizes,
                    "ci": [r.ci_low, r.ci_high],
                    "p_value": r.p_value,
                    "p_adjusted": adj_p,
                    "n_pairs": r.n_pairs,
                    "n_missing": r.n_missing,
                }
            )

        comparison = {
            "baseline": baseline_id,
            "variant": variant_id,
            "metrics": summary_metrics,
        }
        comparisons.append(comparison)

        # interpretability outputs
        out_dir = os.path.join(run_dir, "analysis", "interpretability", f"{baseline_id}__{variant_id}")
        rows = []
        for p in pairs:
            row = {
                "task_id": p.task_id,
                "repl_idx": p.repl_idx,
                "outcome_baseline": p.baseline.outcome,
                "outcome_variant": p.variant.outcome,
            }
            for metric in metrics:
                b = _metric_value(p.baseline, metric)
                v = _metric_value(p.variant, metric)
                row[f"baseline_{metric}"] = b
                row[f"variant_{metric}"] = v
                if b is not None and v is not None:
                    row[f"delta_{metric}"] = v - b
                else:
                    row[f"delta_{metric}"] = None
            rows.append(row)

        jsonl_path = os.path.join(out_dir, "paired_diffs.jsonl")
        write_jsonl(rows, jsonl_path)
        parquet_path = os.path.join(out_dir, "paired_diffs.parquet")
        write_parquet_if_available(rows, parquet_path)

        primary = plan.primary_metrics[0] if plan.primary_metrics else "success"
        exemplars = build_exemplars(rows, primary)
        write_json(exemplars, os.path.join(out_dir, "exemplars.json"))

        suspects = build_suspects(evidence_sources)
        write_json(suspects, os.path.join(out_dir, "suspects.json"))

    results["comparisons"] = comparisons
    summary_path = os.path.join(run_dir, "analysis", "summary.json")
    comparisons_path = os.path.join(run_dir, "analysis", "comparisons.json")
    os.makedirs(os.path.dirname(summary_path), exist_ok=True)
    write_json({"comparisons": comparisons}, summary_path)
    write_json({"comparisons": comparisons}, comparisons_path)

    # Flatten summary table for run-level tables output
    table_rows = []
    for comp in comparisons:
        for m in comp["metrics"]:
            row = {
                "baseline": comp["baseline"],
                "variant": comp["variant"],
                "metric": m["metric"],
                "ci_low": m["ci"][0],
                "ci_high": m["ci"][1],
                "p_value": m["p_value"],
                "p_adjusted": m["p_adjusted"],
                "n_pairs": m["n_pairs"],
                "n_missing": m["n_missing"],
            }
            for k, v in m["effect_sizes"].items():
                row[f"effect_{k}"] = v
            table_rows.append(row)

    tables_jsonl = os.path.join(run_dir, "analysis", "tables.jsonl")
    write_jsonl(table_rows, tables_jsonl)
    tables_parquet = os.path.join(run_dir, "analysis", "tables.parquet")
    write_parquet_if_available(table_rows, tables_parquet)

    return results
