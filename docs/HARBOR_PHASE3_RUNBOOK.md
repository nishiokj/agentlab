# Harbor Phase 3 Runbook

This runbook covers compatibility monitoring additions for Harbor integration.

## What Phase 3 Added

1. CI lanes for Harbor compatibility:
   1. `.github/workflows/harbor-phase3-compat.yml`
   2. blocking `pinned` lane
   3. non-blocking `canary` lane (can be made blocking in manual dispatch)
2. Lane runner:
   1. `scripts/harbor/run_harbor_phase3_lane.sh`
3. Adapter/evaluator compatibility probe:
   1. `adapters/harbor/check_harbor_adapter_compat.py`
4. Split dependency manifests:
   1. `scripts/harbor/requirements-harbor-pinned.txt`
   2. `scripts/harbor/requirements-harbor-canary.txt`

## CI Variable Configuration

Set repository variables:

1. `HARBOR_PIP_SPEC_PINNED`
2. `HARBOR_EVALUATOR_CMD_JSON_PINNED`
3. `HARBOR_PIP_SPEC_CANARY`
4. `HARBOR_EVALUATOR_CMD_JSON_CANARY`

Examples:

```text
HARBOR_PIP_SPEC_PINNED=harbor-framework==1.4.2,terminal-bench-harbor==0.7.1
HARBOR_PIP_SPEC_CANARY=harbor-framework>=1.4,<1.5,terminal-bench-harbor>=0.7,<0.8
HARBOR_EVALUATOR_CMD_JSON_PINNED=["python","-m","terminal_bench_harbor.eval"]
HARBOR_EVALUATOR_CMD_JSON_CANARY=["python","-m","terminal_bench_harbor.eval"]
```

The workflow warns and skips a lane when required vars are missing.

## Local Lane Runs

Pinned lane:

```bash
HARBOR_PIP_SPECS='harbor-framework==1.4.2,terminal-bench-harbor==0.7.1' \
HARBOR_EVALUATOR_CMD_JSON='["python","-m","terminal_bench_harbor.eval"]' \
bash scripts/harbor/run_harbor_phase3_lane.sh pinned
```

Canary lane:

```bash
HARBOR_PIP_SPECS='harbor-framework>=1.4,<1.5,terminal-bench-harbor>=0.7,<0.8' \
HARBOR_EVALUATOR_CMD_JSON='["python","-m","terminal_bench_harbor.eval"]' \
bash scripts/harbor/run_harbor_phase3_lane.sh canary
```

If you want to dry-run lane logic without Harbor dependency install:

```bash
HARBOR_ENFORCE_DEP_SPECS=0 \
HARBOR_REQUIRE_EVALUATOR_CMD=0 \
HARBOR_SKIP_PIP_BOOTSTRAP=1 \
bash scripts/harbor/run_harbor_phase3_lane.sh pinned
```

## Actionable Failure Modes

`check_harbor_adapter_compat.py` maps failures to actionable guidance:

1. `config.*`: evaluator/env wiring issue.
2. `evaluator.command_failed`: likely Harbor API drift in evaluator glue.
3. `evaluator.invalid_json|missing_output|invalid_payload`: evaluator output contract changed.
4. `io.*`: adapter input/output path issue.

The probe prints `error_code=<...>` plus next-step guidance on stderr.
