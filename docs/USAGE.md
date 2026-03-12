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

Task datasets consumed by the runner use a single-head unversioned task contract. The accepted top-level shape is:

```json
{
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
    command: [rex, run, --config, configs/rex.yaml, --model, $MODEL]
    env:
      OPENAI_API_KEY: $OPENAI_API_KEY
      PROMPT_FILE: prompts/system.txt
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

- `runtime.agent_runtime.artifact`, `runtime.agent_runtime.image`, and `runtime.agent_runtime.command` are required for scientific `run` and `build-run`
- put public argv directly in `runtime.agent_runtime.command`; DX authoring uses `agent.command`
- put public env directly in `runtime.agent_runtime.env`; DX authoring uses `agent.env`
- plain relative paths in argv/env are build-time file or data refs
- use `$NAME` for runtime bindings from variant bindings or launch-time env; do not use removed `${...}` templating
- do not use `env_from_host`, `binding_args`, `support_files`, `provider_env`, `default_config`, or `config_files`
- task sandbox images come only from `task.environment.image`
- scientific `run` and `build-run` launch the agent inside the hermetic `agent_runtime` container
- `--dangerous` and similar bypass flags are rejected in scientific runs
- runner topology is private; do not author `/agentlab/...` paths or rely on `AGENTLAB_*` names
- `policy.task_sandbox.profile` is runner-owned topology selection
