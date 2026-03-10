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

Task datasets consumed by the runner must now use `task_boundary_v3`. The accepted top-level shape is:

```json
{
  "schema_version": "task_boundary_v3",
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

Runtime configuration is split between the external agent runtime and the task sandbox:

```yaml
runtime:
  agent:
    bundle: .lab/agents/rex-current.tar.gz
    command: [rex, run]
    io:
      input_arg: --input
      output_arg: --output
  sandbox:
    executor: docker
    image_source: per_task
    image: null
    profile: default
    network: none
  policy:
    timeout_ms: 600000
```

Hard-cut runtime rules:

- `runtime.agent.bundle` replaces `runtime.agent.artifact`
- `runtime.sandbox.image_source` replaces `runtime.agent.image_source`
- `runtime.sandbox.image` replaces `runtime.agent.image`
- `runtime.sandbox.profile` is runner-owned topology selection
- the agent runs outside the sandbox; the sandbox owns task-image execution

The logical writable workspace root inside the sandbox is always `/agentlab/workspace`. Profiles may add compatibility aliases such as `/testbed`, but tasks must not author those paths directly.
