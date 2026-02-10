# Knob System Protocol v1 (Draft)

## Goal

Provide a general mechanism for UI, agents, or scripts to tweak experiments without custom runner code per harness.

## Artifacts

- `knob_manifest_v1.jsonschema`
- `experiment_overrides_v1.jsonschema`

Typical locations:

- `.lab/knobs/manifest.json`
- `.lab/knobs/overrides.json`

## Data Model

1. `knob_manifest`
- Declares knobs with stable `id`.
- Maps each `id` to experiment `json_pointer`.
- Declares type, allowed options, and numeric bounds.
- Classifies knobs by role (`core`, `harness`, `benchmark`, `infra`, etc.) and scientific role (`treatment`, `control`, `confound`, `invariant`).

2. `experiment_overrides`
- Contains `values` map of `knob_id -> value`.
- References `manifest_path`.

## Runner Contract

Before `describe/run/build`, runner must:

1. Validate overrides against `experiment_overrides_v1`.
2. Validate manifest against `knob_manifest_v1`.
3. Validate each override:
- knob id exists,
- type matches,
- options/bounds are satisfied.
4. Apply overrides into experiment JSON via knob `json_pointer`.

If any check fails, command fails.

## CLI Contract

- `lab knobs-init`
- `lab knobs-validate --manifest <path> --overrides <path>`
- `lab describe <experiment> --overrides <path>`
- `lab run <experiment> --overrides <path>`
- `lab run-dev <experiment> --overrides <path>`
- `lab run-experiment <experiment> --overrides <path>`
- `lab image-build <experiment> --overrides <path>`

## UI Contract

A UI can be generic by:

1. Rendering controls from `manifest.json`.
2. Writing chosen values into `overrides.json`.
3. Calling CLI with `--overrides`.

No harness-specific frontend code is required beyond knob metadata.
