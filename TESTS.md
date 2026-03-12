# Testing Standards

This repository should be hostile to fake confidence.

Tests exist to catch behavioral regressions, contract regressions, and integration breakage with the least ambiguity possible. If a test can be made green by tweaking a private literal that the product does not expose, that test is asserting the wrong thing.

## Priorities

In order:

1. Catch real product regressions.
2. Fail at the boundary that actually changed.
3. Make failures easy to interpret.
4. Keep one canonical way to construct current inputs.
5. Delete dead paths instead of preserving them in tests.

## Hard Boundary Rule

For happy-path behavioral and E2E tests, the test owns only boundary inputs and public assertions.

Boundary inputs are the files, CLI args, env files, images, and artifacts a real operator can provide
before invoking the product.

Once the public command or API under test is invoked, the test must not help the product in any way.

This is a hard rule with no exceptions:

- no mocking
- no stubbing
- no monkeypatching
- no manual support-file staging to rescue the product
- no manual dataset/export/materialization work that the product is supposed to perform
- no hand-authoring resolved or semi-resolved internals when the workflow starts from public authoring
- no fake runtime/image/artifact substitution when the real path exists
- no post-boundary file edits, env injection, or package mutation to make a happy-path test pass

If the real path cannot be exercised, stop and report a blocker. Do not silently substitute an easier
fixture or a partially pre-solved setup.

A test is not end to end if it can still pass while the real operator path is broken.

Do not report an end-to-end test as completed unless it exercises the actual public boundary from
boundary input to public output. If the real path is blocked, report the blocker immediately.

## Non-Negotiable Rules

1. Behavioral tests must exercise public product surfaces, not handwritten private representations.
2. Compatibility tests may pin old contracts on purpose. Behavioral tests may not.
3. E2E CLI happy-path tests must use the real public workflow under test. If the workflow includes scaffold generation, use `lab-cli init`. If no init profile exists, author only the minimal public input file. Do not hand-author resolved or semi-resolved experiment payloads.
4. Current contract literals must come from one canonical helper or template, not be duplicated across tests.
5. If a hard cut changes the contract, update the one canonical helper and keep a small explicit set of legacy-rejection tests.
6. If a test suite fails broadly because a shared helper hardcoded an obsolete schema version, that is a test architecture bug.
7. Do not reintroduce compatibility shims into the product just to keep stale tests green.
8. Do not assert full error strings unless the precise wording is itself part of the contract. Prefer asserting the error class and the key semantic message.
9. A test should fail for the behavior it is named after. If it fails earlier for unrelated setup drift, fix the test architecture.
10. Delete obsolete tests and fixtures when their product path is deleted. Do not keep undead coverage.

## What E2E CLI Tests Must Cover

The E2E CLI layer is for user-visible workflow coverage:

- project initialization
- package build
- package preflight
- scientific run
- build-and-run workflow
- materialization and artifact persistence
- run querying and views
- benchmark grading record flow

It is not the place to hand-author internal runner payloads. The only exception is an explicit
rejection test that proves those payloads are rejected.

## Current Runtime Model

The current runtime model has two execution planes and one shared contract:

- `agent_runtime`: the external agent executable, launched from `runtime.agent_runtime.{artifact,image,command,...}`
- `task_sandbox`: the task/grader plane, driven by `task.environment.image` plus `policy.task_sandbox`
- `TrialContract`: the stable filesystem ABI shared across both planes

Stable contract paths:

- `/agentlab/in`
- `/agentlab/out`
- `/agentlab/state`
- `/agentlab/workspace`
- `/agentlab/deps`
- `/agentlab/in/trial_input.json`
- `/agentlab/in/task.json`
- `/agentlab/in/bindings.json`
- `/agentlab/in/policy.json`
- `/agentlab/out/result.json`
- `/agentlab/out/trajectory.jsonl`

Stable grading outputs:

- `result.json`
- `benchmark_prediction_record_v1`
- `benchmark_score_record_v1`

Behavioral E2E assertions should target those stable surfaces, plus stable operator outputs such as `run.sqlite`, `attestation.json`, `trial_state.json`, `state_inventory.json`, `query`, and `views`.

## Current Input Contract

Current task rows are unversioned.

Current experiment/runtime config must use:

- `runtime.agent_runtime`
- `policy.timeout_ms`
- `policy.task_sandbox`

Removed inputs are hard errors and must only appear in tests that explicitly verify rejection:

- `version`
- `dataset.schema_version`
- `task.schema_version`
- `runtime.agent`
- `runtime.sandbox`
- `runtime.dependencies.file_staging`
- `benchmark.adapter`
- `--executor`

## Good vs Bad

Bad behavioral test pattern:

```python
experiment = {
    "runtime": {
        "agent": {"bundle": "./agent.tar.gz", "io": {"input_arg": "--input"}},
        "sandbox": {"image_source": "global", "image": "task-image"},
        "dependencies": {"file_staging": []},
    },
}

payload = _run_lab(
    lab_cli_bin,
    "run",
    package_dir,
    "--executor",
    "local_docker",
    "--json",
    cwd=project.root,
)
```

Why this is bad:

- it hardcodes a private contract that is expected to change
- it bypasses the public authoring and build surfaces
- it causes unrelated tests to fail at setup time
- it muddies whether the product behavior regressed or the fixture drifted

Good behavioral test pattern:

```python
_run([str(lab_cli_bin), "init", "--profile", "agent-eval", "--in-place"], cwd=project.root)
experiment_path = project.root / ".lab" / "experiments" / "my_eval.yaml"

task_row = {
    "task": {"id": "TASK001"},
    "environment": {"image": fixture_image},
    "workspace": {
        "mode": "scratch",
        "base": {"kind": "empty"},
        "overlays": [],
        "aux_mounts": [],
    },
    "dependencies": {"files": []},
    "limits": {},
}

_write_jsonl(project.root / "tasks.jsonl", [task_row])
package = _run_lab(lab_cli_bin, "build", experiment_path, "--out", package_dir, "--json", cwd=project.root)
preflight = _run_lab(lab_cli_bin, "preflight", package_dir, "--json", cwd=project.root)
run = _run_lab(
    lab_cli_bin,
    "run",
    package_dir,
    "--materialize",
    "full",
    "--json",
    cwd=project.root,
)
```

Why this is good:

- it uses the public CLI workflow
- it exercises the current contract
- it fails at the boundary that changed
- when the contract changes, one helper/template should absorb the update

## Specific Bad Tendencies

These are repository-level anti-patterns:

- Hand-authoring resolved experiments inside E2E tests.
- Hardcoding schema versions in shared behavioral helpers.
- Encoding removed CLI flags into broad helpers.
- Making all tests depend on one stale fixture shape.
- Asserting internal representation details when a stable operator surface exists.
- Preserving tests for deleted product paths after a hard cut.

## Specific Good Tendencies

- One canonical helper for current task row generation.
- One canonical helper for current experiment initialization.
- Small explicit tests for legacy rejection.
- Broad E2E coverage built on current public surfaces.
- Assertions against stable contract files, sqlite records, and CLI outputs.
- Fast unit tests for parser/validator edge cases.
- Focused integration tests for materialization, contract paths, grading flow, and persistence.

## Test Layer Guidance

Use the smallest layer that can prove the behavior:

- Unit tests: parsing, normalization, validation, path handling, record validation
- Integration tests: runner lifecycle, materialization, state inventory, grading wrappers, persistence
- E2E CLI tests: user workflows and end-to-end operator-visible behavior

Do not push parser and schema minutiae into E2E unless the CLI contract itself is what you are testing.

## Hard Cutover Rule

Hard cuts are allowed to break compatibility. Tests must reflect that cleanly.

When a hard cut lands:

1. Update the canonical current-contract helpers.
2. Delete tests that covered removed paths.
3. Add or update a small number of explicit rejection tests for the removed contract.
4. Verify that broad behavioral E2E tests still reach the runtime/grading layers instead of dying in shared setup.

If a hard cut requires touching dozens of behavioral tests individually, the helper boundary is wrong.

## E2E CLI Usage Model

The required happy-path E2E CLI workflow is:

1. Create a temp project root.
2. Initialize a current experiment scaffold with `lab-cli init --profile ... --in-place`.
3. Write current boundary input files only.
4. Point the experiment at a real agent runtime artifact and image.
5. `lab-cli build`
6. `lab-cli preflight`
7. `lab-cli run` or `lab-cli build-run`
8. Assert against stable run outputs and query surfaces.

That is the required path for happy-path CLI E2E coverage. Anything else is not end to end and must
be labeled as a compatibility or rejection test.
