# Benchmark Usage Guide

## Scope

This guide covers task-set generation and validation only.
Trial execution and run orchestration are handled by the experiment runner.

## Prerequisites

- Python 3.11.x

## Quick Start

```bash
# 1. Bootstrap development environment
make bootstrap

# 2. Validate schemas
python -m bench.cli validate-schemas

# 3. Validate a single task
python -m bench.cli validate-task bench/benchmark/tasks/v0/TASK001 --strict

# 4. Validate entire suite
python -m bench.cli validate-suite v0 --strict --repeat 5 --check-determinism

# 5. Admit a task
python -m bench.cli admit-task bench/benchmark/tasks/v0/TASK001
```

## Commands

- `validate-schemas`: validate JSON schemas in `schemas/`
- `validate-task`: run task-level checks and strict gates
- `validate-suite`: run suite-level validation and determinism replay
- `import-suite`: import external tasks into canonical benchmark layout
- `admit-task`: fail-closed admission gate for strict tasks
- `new-task`: scaffold a new task from template
- `suite-summary`: write suite summary JSON

## Runner Contract After Hard Cutover

Task datasets consumed by the runner must now use `task_spec_v1`. The accepted top-level shape is:

```json
{
  "schema_version": "task_spec_v1",
  "task": { "id": "TASK001" },
  "environment": { "image": "ghcr.io/example/task-image:latest" },
  "workspace": {
    "mode": "patch",
    "base": {
      "kind": "dataset_pack",
      "dataset_pack_ref": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    },
    "overlays": [
      { "path": "ISSUE.md", "content": "Reproduce and fix the failure.\n" }
    ],
    "aux_mounts": []
  },
  "dependencies": { "files": [] },
  "limits": {}
}
```

Removed boundary fields are hard errors:

- `task.image`
- `task.workspace`
- `workspace_seed`
- `workspace_files`
- `mount_references`

Patch tasks must declare a real workspace base. `workspace.mode = "patch"` with `workspace.base.kind = "empty"` is rejected during validation and preflight.

## Runtime Contract After Hard Cutover

Runtime configuration is split between the external agent runtime and runner-owned task sandbox policy:

```yaml
runtime:
  agent_runtime:
    artifact: .lab/agents/rex-current.tar.gz
    command: [rex, run]
    image: ghcr.io/example/agent-runtime:sha256-...
    network: none
    root_read_only: true
policy:
  timeout_ms: 600000
  task_sandbox:
    profile: default
    network: none
```

Hard-cut runtime rules:

- `runtime.agent_runtime.artifact` and `runtime.agent_runtime.image` are required for scientific `run` and `build-run`
- commands are literal argv; there is no `runtime.agent.io` command synthesis
- task sandbox images come only from `task_spec_v1.environment.image`
- `policy.task_sandbox.profile` is runner-owned topology selection
- scientific `run` and `build-run` launch the agent inside the hermetic `agent_runtime` container, never as a host process
- `--dangerous` and similar bypass flags are rejected in scientific runs
- the task sandbox owns task-image execution, graders, and bash-plane work

The logical writable workspace root inside both the `agent_runtime` and `task_sandbox` planes is always `/agentlab/workspace`. Profiles may add compatibility aliases such as `/testbed`, but tasks must not author those paths directly.
