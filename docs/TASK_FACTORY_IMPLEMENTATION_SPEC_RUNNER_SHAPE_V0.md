# Task Factory Implementation Spec (Runner-Shape Locked, V0)

Status: Proposed  
Date: 2026-02-27  
Owner: Benchmark Core

## 1. Decision

All generated tasks MUST conform exactly to the benchmark runner's task contract.
No alternate task schema, folder shape, or artifact naming is allowed in V0.
No manual grader interpretation is allowed: admission is machine-evaluated and fail-closed.

Rationale:
1. Prevent adapter glue and drift between authoring and runtime.
2. Keep validation and grading fail-closed.
3. Preserve determinism and auditability.

## 2. Canonical Task Contract (Non-Negotiable)

Each generated task must be emitted as:

```text
tasks/v0/TASK###/
  task.yaml
  issue.md
  injection.patch
  public/repro.md
  public/run_public.sh
  hidden/runner.py
  hidden/cases.jsonl
  mutants/M01.patch ... M10.patch (or more)
  policy/allow_edit_globs.txt
  policy/deny_edit_globs.txt
  private/solution.patch
```

Generated strict artifacts (post-validation/admission):
1. `task_validation_report.json`
2. `mutant_gate_report.json`
3. `injection_manifest.json`
4. `admission_record.json`

## 3. Task Semantics (What a Task Is)

A task is a bug-fix benchmark unit with four distinct roles:
1. Public prompt: natural-language symptom in `issue.md` (no leaked paths/fix steps).
2. Fault injection: `injection.patch` introduces the target bug into pinned snapshot.
3. Hidden verification: `hidden/runner.py` + `hidden/cases.jsonl` objectively evaluate fixes.
4. Robustness guard: mutant patches prove hidden suite resists shallow/hardcoded fixes.

V0 non-triviality requirement (mandatory):
1. Each task must require real program reasoning, not a cosmetic/string-only tweak.
2. A passing fix must involve at least one of:
   - control-flow correction across >=2 branches, or
   - data-shape/invariant handling across >=2 distinct input classes, or
   - interaction across >=2 functions/modules in the target behavior path.
3. Tasks that are satisfiable by a one-line constant flip, message string edit, or path rename alone are rejected.

## 4. Generator Responsibilities

The task generator must:
1. Materialize canonical task folder shape exactly.
2. Build `task.yaml` aligned with runner/loader requirements.
3. Produce deterministic hidden case sets (`>=50`).
4. Produce mutant set (`>=10`) with strategy diversity (`>=8` categories).
5. Write strict policy files and task patch policy fields.
6. Write `private/solution.patch` that passes hidden suite.
7. Produce leak-sanitized `issue.md` (symptom-focused, no path hints).
8. Encode objective pass/fail grading only (no rubric text requiring human judgment).
9. Prove task isolation constraints during validation/admission (network/file/env).

The task generator must NOT:
1. Emit non-canonical file names or directories.
2. Depend on network for task execution/validation.
3. Embed hidden/private hints in public artifacts.
4. Emit tasks whose intended fix is trivial under the non-triviality rule above.

## 5. Repo Qualification Gate (Before Task Generation)

A candidate repo is admissible only if all pass:
1. Snapshot gate: pinned commit + reproducible `src.tar.zst`.
2. Offline gate: selected task slice runs with network disabled.
3. Determinism gate: repeated runs produce identical normalized outcomes.
4. Runtime gate: hidden/public checks fit benchmark timeouts.
5. Surface gate: enough bug-bearing code paths for non-trivial tasks.
6. Isolation gate:
   - network-off execution is enforced for validation/grading.
   - no required secrets/online credentials for the scoped task slice.
   - file writes remain inside task-approved working paths.

## 6. Jesus Repo Qualification Summary (Current)

Observed candidate: `/Users/jevinnishioka/Desktop/jesus`

Initial scan summary:
1. Total test files: `96`
2. Likely offline-safe files: `63`
3. High-signal bug/invariant/edge-case files: `15`

Recommended initial derivation domains:
1. `tests/tools/**` (apply_patch/read/write/grep/bash/types)
2. `tests/context/**` + `tests/context-compact.test.ts`
3. `tests/llm/retry.test.ts`
4. `tests/entity-graph/pr-review.mutation.test.ts`
5. `tests/tui/parsing.test.ts`, `tests/tui/normalization.test.ts`
6. `tests/workflow-handling.test.ts`

Exclude in first wave:
1. DB/integration tests requiring `TEST_DATABASE_URL`.
2. Explicit web-search/network-dependent surfaces.
3. Any test path requiring live external API keys/services.

## 7. Expected Task Yield

From `jesus` (scoped, offline-first):
1. Conservative shippable V0 set: `20-24` tasks.
2. Stretch target after stabilization: `24-34` tasks.

Recommended V0 target:
1. Generate and admit first `20` tasks from scoped domains above.
2. Hold additional tasks as expansion backlog pending determinism soak.

## 8. Task Factory Pipeline

1. Select source slice (module + tests).
2. Define bug hypothesis and target behavior delta.
3. Create injection patch and private solution patch.
4. Implement hidden runner and cases (`>=50`).
5. Generate mutants (`>=10`, diverse categories).
6. Author leak-safe issue and public repro script.
7. Run `validate-task --strict`.
8. Run `admit-task`.
9. Repeat until suite reaches 20 admitted tasks.

## 9. Hard Acceptance Gates Per Task

A task is admissible only if all are true:
1. `TASK.HIDDEN.COUNT` passed.
2. `TASK.MUTANT.COUNT` passed.
3. `TASK.BASELINE.FAILS` passed.
4. `TASK.SOLUTION.PASSES` passed.
5. `TASK.MUTANT.KILL` passed with threshold `100%` (all admitted mutants killed).
6. `TASK.CASE.DETERMINISM` passed.
7. `TASK.PROMPT.NO_LEAK` passed.
8. `TASK.INJECTION.PROVENANCE` passed.
9. `TASK.MUTANT.DIVERSITY` passed with `>=8` categories represented.
10. `TASK.GRADING.OBJECTIVE` passed (binary machine-evaluable verdict only).
11. `TASK.ISOLATION.NETWORK_OFF` passed.
12. `TASK.ISOLATION.FS_BOUNDARY` passed.
13. `TASK.ISOLATION.NO_SECRETS_DEP` passed.
14. `TASK.DIFFICULTY.FLOOR` passed (non-triviality rule in Section 3).

Enforcement rule:
1. Validation/admission must fail closed if any required gate is missing, skipped, or reported as non-binary/unknown.

## 10. Commands of Record

Qualification and generation flow:

```bash
python3 -m bench.cli validate-schemas
python3 -m bench.cli validate-task tasks/v0/TASK001 --strict
python3 -m bench.cli admit-task tasks/v0/TASK001
python3 -m bench.cli validate-suite v0 --strict --repeat 5 --check-determinism
```

## 11. Definition of Done (V0 Task Factory)

1. 20 tasks are present under `tasks/v0/TASK001..TASK020`.
2. All 20 pass strict validation and are admitted.
3. Every admitted task passes all hard gates in Section 9 with no waivers/exceptions.
4. Suite determinism replay (`repeat=5`) passes with zero mismatches.
5. Isolation proof is clean: no network access, no secret dependency, no FS boundary violations.
6. Validation/admission artifacts are complete and compliant with no missing hard checks.

## 12. Immediate Next Actions

1. Freeze `jesus` commit and produce `repos/jesus/src.tar.zst` + `baseline_commit.txt`.
2. Wire validator/admission to emit and enforce every Section 9 gate (including difficulty/isolation/objective-grading gates) with fail-closed behavior.
3. Scaffold `TASK001..TASK020` canonical folders.
4. Implement first 5 tasks from tooling domain (`tests/tools/**`) and strict-admit them.
5. Expand to context/llm/entity-graph/tui/workflow domains until 20 admitted tasks.
