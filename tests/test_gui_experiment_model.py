from agentlab_gui.experiment_model import (
    build_arm_overrides,
    compare_summaries,
    load_parameters_from_manifest,
    parameter_by_name,
    summarize_trials,
)


def test_build_arm_overrides() -> None:
    manifest = {
        "schema_version": "knob_manifest_v1",
        "knobs": [
            {
                "id": "design.replications",
                "label": "Replications",
                "type": "integer",
                "minimum": 1,
                "maximum": 10,
                "scientific_role": "treatment",
                "role": "core",
            }
        ],
    }
    params = load_parameters_from_manifest(manifest, {"design.replications": 1})
    primary = parameter_by_name("Replications", params)
    baseline, treatment, summary = build_arm_overrides(
        manifest_path=".lab/knobs/manifest.json",
        base_values={"dataset.limit": 50},
        variable=primary,
        arm_a_value=1,
        arm_b_value=5,
        secondary_updates={"runtime.network.mode": "none"},
    )
    assert baseline["values"]["design.replications"] == 1
    assert treatment["values"]["design.replications"] == 5
    assert baseline["values"]["runtime.network.mode"] == "none"
    assert any("Arm A" in s for s in summary)


def test_load_parameters_from_manifest_filters_roles() -> None:
    manifest = {
        "schema_version": "knob_manifest_v1",
        "knobs": [
            {
                "id": "runtime.harness.integration_level",
                "label": "Integration Level",
                "type": "string",
                "options": ["cli_basic", "cli_events"],
                "scientific_role": "confound",
                "role": "harness",
            },
            {
                "id": "design.replications",
                "label": "Replications",
                "type": "integer",
                "minimum": 1,
                "maximum": 10,
                "scientific_role": "treatment",
                "role": "core",
            },
        ],
    }
    params = load_parameters_from_manifest(manifest, {"design.replications": 1})
    by_name = {p.name: p for p in params}
    assert by_name["Replications"].recommended_variable is True
    assert by_name["Integration Level"].recommended_variable is False


def test_summarize_and_compare_trials() -> None:
    baseline_rows = [
        {"success": True, "primary_metric_name": "val_loss", "primary_metric_value": 0.5},
        {"success": False, "primary_metric_name": "val_loss", "primary_metric_value": 0.7},
    ]
    treatment_rows = [
        {"success": True, "primary_metric_name": "val_loss", "primary_metric_value": 0.4},
        {"success": True, "primary_metric_name": "val_loss", "primary_metric_value": 0.45},
    ]
    b = summarize_trials(baseline_rows)
    t = summarize_trials(treatment_rows)
    d = compare_summaries(b, t)
    assert b["trials"] == 2
    assert t["success_rate"] > b["success_rate"]
    assert d["primary_metric_name"] == "val_loss"
