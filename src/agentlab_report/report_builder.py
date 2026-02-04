import json
import os
from typing import Any, Dict


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _safe_get(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        return _load_json(path)
    return {}


def build_report(run_dir: str, out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)

    resolved = _safe_get(os.path.join(run_dir, "resolved_experiment.json"))
    summary = _safe_get(os.path.join(run_dir, "analysis", "summary.json"))
    comparisons = _safe_get(os.path.join(run_dir, "analysis", "comparisons.json"))

    title = resolved.get("experiment", {}).get("name", "AgentLab Report")

    html = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        f"<title>{title}</title>",
        "<style>body{font-family:system-ui, sans-serif; margin:40px;}",
        "h1{margin-bottom:0.2em} .section{margin-top:24px}",
        "table{border-collapse:collapse;width:100%} th,td{border:1px solid #ddd;padding:8px}",
        "</style></head><body>",
        f"<h1>{title}</h1>",
        f"<div>Run Dir: {run_dir}</div>",
    ]

    comps = comparisons.get("comparisons", []) or summary.get("comparisons", [])
    if comps:
        html.append("<div class='section'><h2>Comparisons</h2>")
        for comp in comps:
            html.append(f"<h3>{comp['baseline']} vs {comp['variant']}</h3>")
            html.append("<table><tr><th>Metric</th><th>Effect Sizes</th><th>CI</th><th>p</th><th>p_adj</th></tr>")
            for m in comp.get("metrics", []):
                eff = ", ".join([f"{k}={v:.4f}" for k, v in m.get("effect_sizes", {}).items()])
                ci = m.get("ci", [None, None])
                html.append(
                    f"<tr><td>{m['metric']}</td><td>{eff}</td><td>[{ci[0]}, {ci[1]}]</td><td>{m.get('p_value')}</td><td>{m.get('p_adjusted')}</td></tr>"
                )
            html.append("</table>")
        html.append("</div>")

    html.append("</body></html>")

    out_path = os.path.join(out_dir, "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html))

    return out_path
