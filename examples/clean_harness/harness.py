"""
Harness — a plain program that reads a task and writes a result.

Contract:
  - Reads task JSON from the input path (last two positional args, or /in/task.json)
  - Writes result JSON to the output path
  - Accepts arbitrary CLI flags for tuning knobs (--temperature, --model, etc.)
  - Knows nothing about experiments, variants, or the runner
"""

import argparse
import json
import sys
import time


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Example evaluation harness")
    p.add_argument("--temperature", type=float, default=0.7)
    p.add_argument("--model", type=str, default="claude-3.5-sonnet")
    p.add_argument("input_path", type=str, nargs="?", default="/in/task.json")
    p.add_argument("output_path", type=str, nargs="?", default="/out/result.json")
    return p


def solve(task: dict, temperature: float, model: str) -> dict:
    """Placeholder solver — swap this with your actual evaluation logic."""
    prompt = task.get("prompt", "")
    start = time.monotonic()

    # Stub: echo back the prompt with config for demonstration purposes
    answer = f"[{model} @ t={temperature}] Response to: {prompt}"

    elapsed_ms = round((time.monotonic() - start) * 1000)
    return {
        "outcome": "success",
        "objective": {"name": "accuracy", "value": 1.0},
        "metrics": {"latency_ms": elapsed_ms, "tokens": len(prompt.split())},
        "answer": answer,
    }


def main() -> None:
    args = build_parser().parse_args()

    with open(args.input_path) as f:
        task = json.load(f)

    result = solve(task, temperature=args.temperature, model=args.model)

    with open(args.output_path, "w") as f:
        json.dump(result, f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()
