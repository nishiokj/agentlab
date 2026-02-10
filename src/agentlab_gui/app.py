from __future__ import annotations

import json
import shlex
from pathlib import Path
from typing import Any, Dict, List, Tuple

import streamlit as st
import yaml

from agentlab_gui.experiment_model import (
    build_arm_overrides,
    compare_summaries,
    control_parameter_names,
    default_parameter_names,
    load_parameters_from_manifest,
    parameter_by_name,
    summarize_trials,
)
from agentlab_gui.utils import (
    list_run_dirs,
    load_jsonl,
    parse_key_value_lines,
    parse_run_identity,
    read_json,
    read_text,
    resolve_lab_binary,
    run_grade_summary,
    write_json,
    write_text,
)


STAGES: List[Tuple[str, str]] = [
    ("Setup", "Connect your harness and verify run surfaces end-to-end."),
    ("Experiment", "Define one variable and generate clean Arm A vs Arm B overrides."),
    ("Run", "Execute Arm A and Arm B with explicit safety profile."),
    ("Results", "Interpret outcomes, validity grades, and causal evidence."),
]


def run_lab(repo_root: Path, lab_bin: str, args: List[str]) -> Dict[str, Any]:
    import subprocess

    cmd = [lab_bin] + args
    proc = subprocess.run(cmd, cwd=str(repo_root), capture_output=True, text=True)
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    output = stdout + ("\n" + stderr if stderr else "")
    return {
        "cmd": " ".join(cmd),
        "returncode": proc.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "output": output.strip(),
    }


def quoted_cmd(parts: List[str]) -> str:
    return " ".join(shlex.quote(p) for p in parts)


def status_block(res: Dict[str, Any]) -> None:
    st.code(res["cmd"], language="bash")
    if res["returncode"] == 0:
        st.success("Command succeeded")
    else:
        st.error(f"Command failed (exit {res['returncode']})")
    if res["output"]:
        st.text(res["output"])


def ensure_session_defaults() -> None:
    if "repo_root" not in st.session_state:
        st.session_state["repo_root"] = str(Path.cwd())
    if "lab_bin" not in st.session_state:
        st.session_state["lab_bin"] = resolve_lab_binary(Path(st.session_state["repo_root"]))
    if "wizard_stage" not in st.session_state:
        st.session_state["wizard_stage"] = 0
    if "safety_profile" not in st.session_state:
        st.session_state["safety_profile"] = "experiment"
    if "setup_passed" not in st.session_state:
        st.session_state["setup_passed"] = False
    if "last_probe" not in st.session_state:
        st.session_state["last_probe"] = {}
    if "last_run_dir" not in st.session_state:
        st.session_state["last_run_dir"] = ""


def experiment_path(repo_root: Path) -> Path:
    return repo_root / ".lab" / "experiment.yaml"


def knobs_manifest_path(repo_root: Path) -> Path:
    return repo_root / ".lab" / "knobs" / "manifest.json"


def knobs_overrides_path(repo_root: Path) -> Path:
    return repo_root / ".lab" / "knobs" / "overrides.json"


def arm_path(repo_root: Path, arm: str) -> Path:
    return repo_root / ".lab" / "arms" / f"{arm}.overrides.json"


def hypothesis_path(repo_root: Path) -> Path:
    return repo_root / ".lab" / "ux" / "hypothesis.json"


def run_labels_path(repo_root: Path) -> Path:
    return repo_root / ".lab" / "ux" / "run_labels.json"


def load_experiment_yaml(repo_root: Path) -> Dict[str, Any]:
    p = experiment_path(repo_root)
    if not p.exists():
        return {}
    txt = read_text(p)
    if not txt.strip():
        return {}
    value = yaml.safe_load(txt)
    return value if isinstance(value, dict) else {}


def load_knob_manifest(repo_root: Path) -> Dict[str, Any]:
    raw = read_json(knobs_manifest_path(repo_root))
    return raw if isinstance(raw, dict) else {}


def save_experiment_yaml(repo_root: Path, data: Dict[str, Any]) -> None:
    write_text(experiment_path(repo_root), yaml.safe_dump(data, sort_keys=False))


def set_harness_command(repo_root: Path, command_string: str) -> None:
    parts = shlex.split(command_string)
    if not parts:
        return
    exp = load_experiment_yaml(repo_root)
    runtime = exp.setdefault("runtime", {})
    harness = runtime.setdefault("harness", {})
    harness["command"] = parts
    save_experiment_yaml(repo_root, exp)


def apply_observability_upgrades(repo_root: Path, hooks: bool, tracing: bool, proxy_allowlist: bool) -> None:
    exp = load_experiment_yaml(repo_root)
    runtime = exp.setdefault("runtime", {})
    harness = runtime.setdefault("harness", {})
    network = runtime.setdefault("network", {})

    if tracing:
        harness["integration_level"] = "otel"
        tracing_cfg = harness.setdefault("tracing", {})
        tracing_cfg["mode"] = "otlp"
    elif hooks:
        harness["integration_level"] = "cli_events"
        events = harness.setdefault("events", {})
        events["mode"] = "jsonl"
        events["path"] = "/out/harness_events.jsonl"
        events["schema_version"] = "hooks_v1"
    else:
        harness["integration_level"] = "cli_basic"

    if proxy_allowlist:
        network["mode"] = "allowlist_enforced"
    save_experiment_yaml(repo_root, exp)


def default_arm_override(repo_root: Path) -> str:
    baseline = arm_path(repo_root, "baseline")
    if baseline.exists():
        return str(baseline.relative_to(repo_root))
    return ".lab/knobs/overrides.json"


def parse_probe(res: Dict[str, Any], repo_root: Path) -> Dict[str, Any]:
    kv = parse_key_value_lines(res["output"])
    checks = [
        {
            "check": "Experiment file exists",
            "ok": experiment_path(repo_root).exists(),
            "why": "Runner needs a concrete experiment definition.",
        },
        {
            "check": "Knob manifest exists",
            "ok": knobs_manifest_path(repo_root).exists(),
            "why": "Experiment factors map to safe overrides.",
        },
        {
            "check": "Describe command works",
            "ok": res["returncode"] == 0,
            "why": "Confirms runner can resolve your setup.",
        },
    ]
    if kv:
        checks.append(
            {
                "check": "Harness script path resolves",
                "ok": str(kv.get("harness_script_exists", "")).lower() == "true",
                "why": "Prevents pathing failures at run time.",
            }
        )
    setup_passed = all(bool(c["ok"]) for c in checks)
    return {"kv": kv, "checks": checks, "setup_passed": setup_passed}


def setup_complete(repo_root: Path) -> bool:
    if st.session_state.get("setup_passed"):
        return True
    # fallback for session reset: look for successful probe state persisted in files
    return experiment_path(repo_root).exists() and knobs_manifest_path(repo_root).exists()


def design_complete(repo_root: Path) -> bool:
    return arm_path(repo_root, "baseline").exists() and arm_path(repo_root, "treatment").exists()


def run_complete(repo_root: Path) -> bool:
    return len(list_run_dirs(repo_root)) > 0


def unlocked_stage(repo_root: Path) -> int:
    if not setup_complete(repo_root):
        return 0
    if not design_complete(repo_root):
        return 1
    if not run_complete(repo_root):
        return 2
    return 3


def load_run_labels(repo_root: Path) -> Dict[str, str]:
    raw = read_json(run_labels_path(repo_root)) or {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out


def save_run_label(repo_root: Path, run_id: str, arm: str) -> None:
    labels = load_run_labels(repo_root)
    labels[run_id] = arm
    write_json(run_labels_path(repo_root), labels)


def integration_contract_rows(kv: Dict[str, str]) -> List[Dict[str, str]]:
    fields = [
        ("harness", "Harness command"),
        ("harness_script_resolved", "Resolved harness script"),
        ("container_mode", "Runtime mode"),
        ("network", "Network mode"),
        ("integration_level", "Integration level"),
        ("control_path", "Control-plane path"),
        ("events_path", "Hooks path"),
        ("tracing", "Tracing mode"),
        ("image", "Container image"),
    ]
    out: List[Dict[str, str]] = []
    for key, label in fields:
        val = kv.get(key)
        if val is None:
            continue
        out.append({"surface": label, "value": val})
    return out


def setup_stage(repo_root: Path, lab_bin: str) -> None:
    st.subheader("Step 1: Setup")
    st.markdown(
        "Goal: prove that AgentLab can execute your harness in this repository with predictable paths.\n\n"
        "Minimal walkthrough:\n"
        "1. Initialize `.lab/` files.\n"
        "2. Enter your harness command explicitly.\n"
        "3. Run `describe` to verify path resolution.\n"
        "4. Run one smoke trial."
    )
    st.info(
        "Only `.lab/` files are managed by AgentLab. Your harness source tree stays untouched unless you explicitly edit it."
    )

    st.markdown("### 1A) Initialize `.lab/` defaults")
    c1, c2 = st.columns(2)
    workload = c1.selectbox(
        "Workload type",
        ["agent_harness", "trainer"],
        help="`agent_harness` for agent execution loops. `trainer` for deep-learning/backprop jobs.",
    )
    container = c2.checkbox("Use container runtime", value=True, help="Recommended for reproducibility.")
    init_args = ["init", "--workload-type", workload, "--force"]
    if container:
        init_args.append("--container")
    st.caption("Command preview")
    st.code(quoted_cmd([lab_bin] + init_args), language="bash")
    if st.button("Initialize", key="setup_init"):
        res = run_lab(repo_root, lab_bin, init_args)
        status_block(res)

    st.markdown("### 1B) Set harness command (required)")
    st.caption("This exact command is executed per trial. No auto-magic here.")
    st.markdown(
        "Examples:\n"
        "- Node/Bun: `bun ./scripts/agentlab/run_cli.ts`\n"
        "- Python: `python3 ./harness.py run`\n"
        "- Rust binary: `./target/release/harness run`"
    )
    exp = load_experiment_yaml(repo_root)
    current_cmd = " ".join((exp.get("runtime", {}).get("harness", {}).get("command") or []))
    manual = st.text_input(
        "Manual harness command",
        value=current_cmd,
        help="Command is parsed exactly as shell words.",
    )
    if st.button("Apply manual command"):
        set_harness_command(repo_root, manual)
        st.success("Updated runtime.harness.command")

    st.markdown("### 1C) Contract probe")
    st.caption("Probe validates command resolution, script path existence, and effective runtime settings.")
    ov = default_arm_override(repo_root)
    probe_args = ["describe", "--overrides", ov, ".lab/experiment.yaml"]
    st.caption("Command preview")
    st.code(quoted_cmd([lab_bin] + probe_args), language="bash")
    if st.button("Run contract probe"):
        res = run_lab(repo_root, lab_bin, probe_args)
        parsed = parse_probe(res, repo_root)
        st.session_state["last_probe"] = {"res": res, **parsed}
        st.session_state["setup_passed"] = parsed["setup_passed"]
        status_block(res)

    probe = st.session_state.get("last_probe", {})
    if probe:
        checks = probe.get("checks", [])
        if checks:
            st.dataframe(checks, use_container_width=True)
        kv = probe.get("kv", {})
        rows = integration_contract_rows(kv)
        if rows:
            st.markdown("**Integration contract (resolved surfaces)**")
            st.dataframe(rows, use_container_width=True)
        if probe.get("setup_passed"):
            st.success("Setup probe passed. You can move to Step 2: Experiment.")

    st.markdown("### 1D) Safety profile")
    profile = st.radio(
        "Choose profile",
        ["dev", "experiment"],
        index=1 if st.session_state.get("safety_profile") == "experiment" else 0,
        horizontal=True,
        help="`dev` = fast iteration. `experiment` = stronger isolation and validity defaults.",
    )
    st.session_state["safety_profile"] = profile
    if profile == "dev":
        st.info("Dev profile: fast feedback, weaker guarantees, best for local debugging.")
    else:
        st.info("Experiment profile: stronger scientific posture, stricter isolation expectations.")

    with st.expander("Advanced: observability/network upgrades", expanded=False):
        hooks = st.checkbox("Enable hooks (`cli_events`)")
        tracing = st.checkbox("Enable tracing (`otel`)")
        proxy = st.checkbox("Request proxy-enforced allowlist")
        st.caption("Strict allowlist claims require real bypass blocking plus egress self-test pass.")
        if st.button("Apply observability settings"):
            apply_observability_upgrades(repo_root, hooks, tracing, proxy)
            st.success("Applied observability settings to experiment config.")

    st.markdown("### 1E) Smoke run")
    setup_cmd = st.text_input("Dev setup command (only used in dev profile)", value="")
    if profile == "dev":
        smoke_args = ["run-dev", "--overrides", ov, ".lab/experiment.yaml"]
        if setup_cmd.strip():
            smoke_args.extend(["--setup", setup_cmd.strip()])
    else:
        smoke_args = ["run-experiment", "--overrides", ov, ".lab/experiment.yaml"]
    st.caption("Command preview")
    st.code(quoted_cmd([lab_bin] + smoke_args), language="bash")
    if st.button("Run smoke test"):
        if profile == "dev":
            args = ["run-dev", "--overrides", ov, ".lab/experiment.yaml"]
            if setup_cmd.strip():
                args.extend(["--setup", setup_cmd.strip()])
        else:
            args = ["run-experiment", "--overrides", ov, ".lab/experiment.yaml"]
        res = run_lab(repo_root, lab_bin, args)
        status_block(res)
        run_id, run_dir = parse_run_identity(res["output"])
        if run_id:
            save_run_label(repo_root, run_id, "smoke")
        if run_dir:
            st.session_state["last_run_dir"] = run_dir

    st.markdown("### 1F) Copyable runbook")
    runbook_lines = [
        quoted_cmd([lab_bin] + init_args),
    ]
    if manual.strip():
        runbook_lines.append(
            "# set runtime.harness.command in .lab/experiment.yaml to: " + manual.strip()
        )
    runbook_lines.append(quoted_cmd([lab_bin] + probe_args))
    runbook_lines.append(quoted_cmd([lab_bin] + smoke_args))
    st.code("\n".join(runbook_lines), language="bash")


def experiment_stage(repo_root: Path) -> None:
    st.subheader("Step 2: Experiment")
    if not setup_complete(repo_root):
        st.warning("Complete Step 1 (Setup) first.")
        return

    st.markdown(
        "Design one clean A/B experiment:\n"
        "1. Choose exactly one **variable/parameter** to change.\n"
        "2. Set its value for **Arm A (baseline)** and **Arm B (comparison)**.\n"
        "3. Keep other relevant parameters fixed as **controls**."
    )

    st.markdown("**Example**")
    st.info(
        "Variable: `Prompt Template`\n"
        "Arm A: `prompt:v1`\n"
        "Arm B: `prompt:v2`\n"
        "Controls: `design.replications=5`, `runtime.network.mode=none`"
    )

    hyp = read_json(hypothesis_path(repo_root)) or {}
    c1, c2 = st.columns(2)
    hypothesis = c1.text_area(
        "Hypothesis statement",
        value=hyp.get("hypothesis", "Changing [variable] from Arm A to Arm B will change [metric] because [reason]."),
        height=90,
    )
    primary_metric = c1.text_input("Primary decision metric", value=hyp.get("primary_metric", "success_rate"))
    practical_effect = c2.text_input("Minimum practical effect", value=hyp.get("practical_effect", ">= +2%"))
    rationale = c2.text_area("Rationale / context", value=hyp.get("notes", ""), height=90)

    if st.button("Save hypothesis"):
        write_json(
            hypothesis_path(repo_root),
            {
                "hypothesis": hypothesis,
                "primary_metric": primary_metric,
                "practical_effect": practical_effect,
                "notes": rationale,
            },
        )
        st.success("Saved hypothesis.")

    base_ov = read_json(knobs_overrides_path(repo_root)) or {
        "schema_version": "experiment_overrides_v1",
        "manifest_path": ".lab/knobs/manifest.json",
        "values": {},
    }
    base_values = base_ov.get("values", {}) if isinstance(base_ov.get("values"), dict) else {}
    manifest = load_knob_manifest(repo_root)
    parameters = load_parameters_from_manifest(manifest, base_values)

    variable_names = default_parameter_names(parameters)
    if not variable_names:
        variable_names = [p.name for p in parameters if len(p.options) >= 2]
    show_all = st.checkbox("Show all parameters (including confounds/invariants)", value=False)
    if show_all:
        variable_names = [p.name for p in parameters if len(p.options) >= 2]
    if not variable_names:
        st.error("No selectable variable found. Check `.lab/knobs/manifest.json` and ensure at least one knob has 2+ values.")
        return

    variable_name = st.selectbox("Variable/parameter under test", variable_names)
    variable = parameter_by_name(variable_name, parameters)
    st.caption(f"`{variable.knob_id}` • role={variable.role} • scientific_role={variable.scientific_role}")
    st.caption(variable.description)

    arm_a_value = st.selectbox("Arm A value (baseline)", variable.options)
    arm_b_candidates = [v for v in variable.options if v != arm_a_value] or variable.options
    arm_b_value = st.selectbox("Arm B value (comparison)", arm_b_candidates)

    control_names = control_parameter_names(variable_name, parameters)
    selected_controls = st.multiselect(
        "Controlled parameters (kept fixed in Arm A and Arm B)",
        control_names,
        max_selections=4,
    )
    control_updates: Dict[str, Any] = {}
    for name in selected_controls:
        p = parameter_by_name(name, parameters)
        control_updates[p.knob_id] = st.selectbox(f"Control value: {name}", p.options, key=f"control_{p.key}")

    if st.button("Generate Arm A and Arm B override files"):
        baseline, treatment, summary = build_arm_overrides(
            manifest_path=".lab/knobs/manifest.json",
            base_values=base_values,
            variable=variable,
            arm_a_value=arm_a_value,
            arm_b_value=arm_b_value,
            secondary_updates=control_updates,
        )
        write_json(arm_path(repo_root, "baseline"), baseline)
        write_json(arm_path(repo_root, "treatment"), treatment)
        st.success("Created Arm A and Arm B override files.")
        st.markdown("**Plan summary**")
        for line in summary:
            st.write(f"- {line}")
        st.caption("Copy/paste run commands")
        st.code(
            "\n".join(
                [
                    quoted_cmd(
                        [
                            st.session_state.get("lab_bin", "lab"),
                            "run-experiment",
                            "--overrides",
                            ".lab/arms/baseline.overrides.json",
                            ".lab/experiment.yaml",
                        ]
                    ),
                    quoted_cmd(
                        [
                            st.session_state.get("lab_bin", "lab"),
                            "run-experiment",
                            "--overrides",
                            ".lab/arms/treatment.overrides.json",
                            ".lab/experiment.yaml",
                        ]
                    ),
                ]
            ),
            language="bash",
        )

    b = read_json(arm_path(repo_root, "baseline"))
    t = read_json(arm_path(repo_root, "treatment"))
    if b and t:
        st.markdown("**Current arm preview**")
        st.json({"baseline": b, "treatment": t}, expanded=False)


def run_stage(repo_root: Path, lab_bin: str) -> None:
    st.subheader("Step 3: Run")
    if not design_complete(repo_root):
        st.warning("Create Arm A/Arm B overrides in Step 2 first.")
        return

    profile = st.radio(
        "Safety profile",
        ["dev", "experiment"],
        index=1 if st.session_state.get("safety_profile") == "experiment" else 0,
        horizontal=True,
    )
    st.session_state["safety_profile"] = profile
    arm = st.radio(
        "Arm to run",
        ["baseline", "treatment"],
        format_func=lambda x: "Arm A (baseline)" if x == "baseline" else "Arm B (comparison)",
        horizontal=True,
    )
    ov = str(arm_path(repo_root, arm).relative_to(repo_root))

    setup_cmd = st.text_input("Dev setup command (optional)", value="")
    if profile == "dev":
        cmd_preview = f"{lab_bin} run-dev --overrides {ov} .lab/experiment.yaml"
        if setup_cmd.strip():
            cmd_preview += f" --setup '{setup_cmd.strip()}'"
    else:
        cmd_preview = f"{lab_bin} run-experiment --overrides {ov} .lab/experiment.yaml"
    st.code(cmd_preview, language="bash")

    if st.button("Launch run"):
        if profile == "dev":
            args = ["run-dev", "--overrides", ov, ".lab/experiment.yaml"]
            if setup_cmd.strip():
                args.extend(["--setup", setup_cmd.strip()])
        else:
            args = ["run-experiment", "--overrides", ov, ".lab/experiment.yaml"]
        res = run_lab(repo_root, lab_bin, args)
        status_block(res)
        run_id, run_dir = parse_run_identity(res["output"])
        if run_id:
            save_run_label(repo_root, run_id, arm)
        if run_dir:
            st.session_state["last_run_dir"] = run_dir

    runs = list_run_dirs(repo_root)
    labels = load_run_labels(repo_root)
    if runs:
        rows = [{"run_id": r.name, "arm": labels.get(r.name, ""), "path": str(r)} for r in runs[:20]]
        st.dataframe(rows, use_container_width=True)


def load_run_artifacts(run_dir: Path) -> Dict[str, Any]:
    return {
        "attestation": read_json(run_dir / "attestation.json") or {},
        "trials": load_jsonl(run_dir / "analysis" / "tables" / "trials.jsonl"),
        "events_by_trial": load_jsonl(run_dir / "analysis" / "tables" / "event_counts_by_trial.jsonl"),
    }


def results_stage(repo_root: Path) -> None:
    st.subheader("Step 4: Results")
    if not run_complete(repo_root):
        st.warning("Run at least one arm in Step 3 first.")
        return

    labels = load_run_labels(repo_root)
    runs = [r.name for r in list_run_dirs(repo_root)]

    baseline_candidates = [r for r in runs if labels.get(r) == "baseline"] or runs
    treatment_candidates = [r for r in runs if labels.get(r) == "treatment"] or runs
    baseline_run = st.selectbox("Arm A run (baseline)", baseline_candidates, index=0)
    treatment_run = st.selectbox("Arm B run (comparison)", treatment_candidates, index=0)

    base = load_run_artifacts(repo_root / ".lab" / "runs" / baseline_run)
    treat = load_run_artifacts(repo_root / ".lab" / "runs" / treatment_run)
    base_summary = summarize_trials(base["trials"])
    treat_summary = summarize_trials(treat["trials"])
    delta = compare_summaries(base_summary, treat_summary)

    st.markdown("### 4A) Can we trust this?")
    base_grades = run_grade_summary(base["attestation"])
    treat_grades = run_grade_summary(treat["attestation"])
    st.dataframe(
        [
            {"run_id": baseline_run, "arm": labels.get(baseline_run, "baseline"), **base_grades},
            {"run_id": treatment_run, "arm": labels.get(treatment_run, "treatment"), **treat_grades},
        ],
        use_container_width=True,
    )
    warnings = []
    for run_id, g in ((baseline_run, base_grades), (treatment_run, treat_grades)):
        if g.get("isolation_grade") == "leaky":
            warnings.append(f"{run_id}: isolation is leaky")
        if g.get("comparability_grade") in {"unknown", "invalid"}:
            warnings.append(f"{run_id}: comparability is {g.get('comparability_grade')}")
    if warnings:
        for w in warnings:
            st.warning(w)
    else:
        st.success("No blocking validity warnings from recorded grades.")

    st.markdown("### 4B) Did Arm B improve over Arm A?")
    st.dataframe(
        [
            {"arm": "Arm A (baseline)", **base_summary},
            {"arm": "Arm B (comparison)", **treat_summary},
            {"arm": "delta", **delta},
        ],
        use_container_width=True,
    )
    st.caption(
        "Interpret delta with metric direction in mind. "
        "For loss metrics lower may be better; for success metrics higher is better."
    )

    st.markdown("### 4C) Why might it have changed?")
    st.write(f"Baseline event evidence rows: {len(base['events_by_trial'])}")
    st.write(f"Treatment event evidence rows: {len(treat['events_by_trial'])}")
    st.caption("If hooks/traces are disabled, this evidence is coarse.")

    st.markdown("### 4D) Suggested next action")
    if delta.get("success_rate_delta", 0.0) > 0:
        st.info("Promising Arm B. Run a confirmatory experiment with more replications.")
    else:
        st.info("No clear gain. Narrow the factor change and rerun.")


def advanced_panel(repo_root: Path, lab_bin: str) -> None:
    with st.expander("Advanced (raw files and manual commands)", expanded=False):
        exp_txt = st.text_area("Raw experiment.yaml", value=read_text(experiment_path(repo_root)), height=240)
        if st.button("Save experiment.yaml"):
            write_text(experiment_path(repo_root), exp_txt)
            st.success("Saved experiment.yaml")

        ov_txt = st.text_area("Raw overrides.json", value=read_text(knobs_overrides_path(repo_root)), height=200)
        if st.button("Save overrides.json"):
            try:
                write_json(knobs_overrides_path(repo_root), json.loads(ov_txt))
                st.success("Saved overrides.json")
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON: {e}")

        if st.button("Run knobs-validate + describe"):
            status_block(run_lab(repo_root, lab_bin, ["knobs-validate"]))
            status_block(
                run_lab(
                    repo_root,
                    lab_bin,
                    ["describe", "--overrides", ".lab/knobs/overrides.json", ".lab/experiment.yaml"],
                )
            )


def main() -> None:
    st.set_page_config(page_title="AgentLab GUI v2", layout="wide")
    st.title("AgentLab GUI v2")
    st.caption("Guided, scientific UX for harness and training experiments.")

    ensure_session_defaults()
    repo_root = Path(st.sidebar.text_input("Repo Root", st.session_state["repo_root"])).expanduser().resolve()
    st.session_state["repo_root"] = str(repo_root)
    lab_bin = st.sidebar.text_input("Lab Binary", st.session_state.get("lab_bin", resolve_lab_binary(repo_root)))
    st.session_state["lab_bin"] = lab_bin

    unlock = unlocked_stage(repo_root)
    cur = int(st.session_state.get("wizard_stage", 0))
    if cur > unlock:
        cur = unlock
        st.session_state["wizard_stage"] = cur

    st.sidebar.markdown("### Workflow")
    for i, (name, desc) in enumerate(STAGES):
        if i > unlock + 1:
            continue
        state = "locked"
        if i < cur:
            state = "done"
        elif i == cur:
            state = "current"
        elif i <= unlock:
            state = "available"
        icon = {"done": "✅", "current": "➡️", "available": "🟢", "locked": "🔒"}[state]
        st.sidebar.write(f"{icon} {i+1}. {name}")
        st.sidebar.caption(desc)

    c1, c2 = st.sidebar.columns(2)
    if c1.button("Back", disabled=cur <= 0):
        st.session_state["wizard_stage"] = max(0, cur - 1)
        st.rerun()
    if c2.button("Next", disabled=cur >= unlock):
        st.session_state["wizard_stage"] = min(len(STAGES) - 1, cur + 1)
        st.rerun()

    name, desc = STAGES[cur]
    st.markdown(f"## {name}")
    st.caption(desc)
    if cur == 0:
        setup_stage(repo_root, lab_bin)
    elif cur == 1:
        experiment_stage(repo_root)
    elif cur == 2:
        run_stage(repo_root, lab_bin)
    else:
        results_stage(repo_root)

    advanced_panel(repo_root, lab_bin)


if __name__ == "__main__":
    main()
