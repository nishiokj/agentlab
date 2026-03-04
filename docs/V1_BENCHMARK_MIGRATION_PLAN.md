# V1 Benchmark Task Migration Plan (Single Driver)

Date: 2026-03-03  
Status: Active implementation plan (single source of truth for this migration)

## North Star
Deliver 30 robust benchmark tasks (TASK021-TASK050) with graders/validation that are hard to game by:
1. task authors,
2. model submissions,
3. runtime infra failures.

## Scope
1. Task authoring and taxonomy for V1.
2. Task-level grading robustness and anti-gaming gates.
3. Runner/adapters/CI reliability hard gates required for trustworthy execution.
4. Suite admission and final quality bar for 30 tasks.

## Authoritative Decisions
1. Task types: `bugfix`, `feature`, `refactor`, `agentic_search`, `code_review`, `greenfield`.
2. `multi_file_debug` is folded into `bugfix` via `task_profile.bugfix_profile=deep_diagnosis`.
3. `feature` remains one type (no `delta_feature` split).
4. Performance is optional secondary continuous data (`performance_continuous_v1`) on eligible types (`bugfix`, `feature`, `refactor`).
5. Correctness remains binary pass/fail; performance never overrides correctness.

## Migration Workstreams

### WS1: Reliability Hard Gates (Execution Trust)
Implement and land INV-01..INV-08 for runner/adapters/schema/CI reliability.

Blocking outcomes:
1. Adapter outputs are schema-valid with required identity fields.
2. Timeout/disk/provider preflight gates are behaviorally enforced.
3. Artifact staging, workspace exclusion, and recover->continue are regression-tested.
4. CI blocks merges on all reliability invariants.

Primary source: `docs/EXPERIMENTS_RELIABILITY_HARD_GATES_PLAN.md`.

### WS2: Task Authoring Contract + Taxonomy
Use the V1 prompt contract with updated taxonomy/profile model.

Blocking outcomes:
1. All new tasks use updated task types.
2. Deep diagnosis tasks are encoded as `bugfix` + profile.
3. Optional performance secondary metric rules are followed exactly.

Primary source: `docs/TASK_GENERATION_PROMPT_V1.md`.

### WS3: Task Backlog and Production
Build and admit TASK021-TASK050 under strict gates.

Blocking outcomes:
1. Every task has required artifacts and strict validation pass.
2. Catalog entries match actual admitted task type/profile.
3. Suite-level determinism and admission records are present.

Primary source: `docs/V1_TASK_CATALOG.md`.

## Delivery Sequence (Mandatory)
1. Land WS1 reliability hard gates first.
2. Freeze WS2 authoring contract and templates.
3. Produce tasks in batches of 5 with probe-first workflow.
4. Run strict admission per task.
5. Run strict suite validation and determinism replay for full V1.

## Task Production Pipeline (Per Task)

### Stage A: Idea Gate (fast, low commitment)
Required artifacts:
1. `task_pitch.yaml`

Must pass:
1. Task type/profile assigned.
2. Non-trivial reasoning path documented.
3. Issue framing does not leak implementation paths.

### Stage B: Probe Gate (still cheap)
Required artifacts:
1. Draft `issue.md`
2. 8-12 probe hidden cases
3. 6-10 probe mutants

Must pass:
1. Probe suite catches obvious shortcut attacks.
2. Probe mutants are mostly killed.
3. No obvious ambiguity in acceptance criteria.

### Stage C: Full Build + Admission
Required artifacts:
1. Full task bundle with `>=50` hidden cases
2. `>=10` mutants with diversity
3. `injection.patch`, `private/solution.patch`, policy files, public repro

Must pass strict gates:
1. baseline fails
2. solution passes
3. all admitted mutants killed
4. determinism check passes
5. prompt leak and isolation checks pass
6. admission record generated

## Definition of Done for 30 Tasks
1. 30/30 tasks admitted under strict validation.
2. No task uses deprecated `multi_file_debug` type.
3. Each deep diagnosis case is represented as `bugfix` with profile.
4. Optional performance metric is attached only where eligible and correctly reported.
5. Full suite strict validation passes with determinism checks.

## Tracking Board (Update In Place)

| Task | Type | Profile | Stage | Strict Valid | Admitted | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| TASK021 | feature | - | pending | no | no | |
| TASK022 | bugfix | deep_diagnosis | pending | no | no | |
| TASK023 | refactor | - | pending | no | no | |
| TASK024 | agentic_search | - | pending | no | no | |
| TASK025 | code_review | - | pending | no | no | |
| TASK026 | greenfield | - | pending | no | no | |
| TASK027 | bugfix | standard | pending | no | no | |
| TASK028 | feature | - | pending | no | no | |
| TASK029 | bugfix | deep_diagnosis | pending | no | no | |
| TASK030 | refactor | - | pending | no | no | |
| TASK031 | bugfix | standard | pending | no | no | |
| TASK032 | feature | - | pending | no | no | |
| TASK033 | bugfix | deep_diagnosis | pending | no | no | |
| TASK034 | greenfield | - | pending | no | no | |
| TASK035 | bugfix | standard | pending | no | no | |
| TASK036 | feature | - | pending | no | no | |
| TASK037 | refactor | - | pending | no | no | |
| TASK038 | bugfix | standard | pending | no | no | |
| TASK039 | bugfix | deep_diagnosis | pending | no | no | |
| TASK040 | greenfield | - | pending | no | no | |
| TASK041 | bugfix | standard | pending | no | no | |
| TASK042 | feature | - | pending | no | no | |
| TASK043 | code_review | - | pending | no | no | |
| TASK044 | bugfix | standard | pending | no | no | |
| TASK045 | agentic_search | - | pending | no | no | |
| TASK046 | feature | - | pending | no | no | |
| TASK047 | bugfix | deep_diagnosis | pending | no | no | |
| TASK048 | bugfix | standard | pending | no | no | |
| TASK049 | refactor | - | pending | no | no | |
| TASK050 | bugfix | standard | pending | no | no | |

## Commands (Operational)
1. `python -m bench.cli validate-task bench/benchmark/tasks/v1/TASK### --strict`
2. `python -m bench.cli admit-task bench/benchmark/tasks/v1/TASK###`
3. `python -m bench.cli validate-suite v1 --strict --repeat 2 --check-determinism`
