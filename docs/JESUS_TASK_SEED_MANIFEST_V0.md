# Jesus Task Dataset Manifest (V0)

Date: 2026-02-27

Source repo: `/Users/jevinnishioka/Desktop/jesus`
Pinned commit: see `bench/benchmark/repos/jesus/baseline_commit.txt`
Snapshot: `bench/benchmark/repos/jesus/src.tar.zst`

This manifest tracks the V0 dataset under `bench/benchmark/tasks/v0/TASK001..TASK020`, sourced from
`/Users/jevinnishioka/Desktop/jesus`.

Current status (2026-02-27):
1. `TASK001..TASK020` are strict-validated and admitted.
2. Suite determinism check passed with `repeat=5` and identical normalized hashes.
3. Task validation/admission artifacts are present per task bundle.

Evidence files:
1. `bench/benchmark/reports/v0_validate_suite_strict_repeat5.json`
2. `bench/benchmark/tasks/v0/TASK001/task_validation_report.json`
3. `bench/benchmark/tasks/v0/TASK001/admission_record.json`

| Task | Domain | Source Test |
|---|---|---|
| TASK001 | tools | `tests/tools/builtins/apply_patch.test.ts` |
| TASK002 | tools | `tests/tools/builtins/bash.test.ts` |
| TASK003 | tools | `tests/tools/builtins/grep.test.ts` |
| TASK004 | tools | `tests/tools/builtins/read.test.ts` |
| TASK005 | tools | `tests/tools/builtins/read.test.ts` |
| TASK006 | tools | `tests/tools/builtins/write.test.ts` |
| TASK007 | tools | `tests/tools/builtins/write.test.ts` |
| TASK008 | tools | `tests/tools/types.test.ts` |
| TASK009 | tools | `tests/tools/types.test.ts` |
| TASK010 | context | `tests/context/context-window.test.ts` |
| TASK011 | context | `tests/context/context-window.test.ts` |
| TASK012 | context | `tests/context-compact.test.ts` |
| TASK013 | llm | `tests/llm/retry.test.ts` |
| TASK014 | llm | `tests/llm/retry.test.ts` |
| TASK015 | llm | `tests/llm/retry.test.ts` |
| TASK016 | llm | `tests/llm/retry.test.ts` |
| TASK017 | entity-graph | `tests/entity-graph/pr-review.mutation.test.ts` |
| TASK018 | entity-graph | `tests/entity-graph/pr-review.mutation.test.ts` |
| TASK019 | tui | `tests/tui/parsing.test.ts` |
| TASK020 | workflow | `tests/workflow-handling.test.ts` |
