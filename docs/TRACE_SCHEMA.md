# Trace Schema Documentation

## Overview

Traces record every tool call and lifecycle event during agent and grader phases.
Format: JSONL (one JSON object per line).

## Trace Record Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Unique run identifier |
| `task_id` | string | yes | Task identifier (TASK###) |
| `phase` | enum | yes | "agent", "grader", or "validation" |
| `event_type` | enum | yes | Event classification |
| `ts_start` | datetime | yes | ISO 8601 start timestamp |
| `ts_end` | datetime | yes | ISO 8601 end timestamp |
| `duration_ms` | number | yes | Duration in milliseconds |
| `tool_name` | string? | no | Tool name for tool_call events |
| `input` | object? | no | Tool input (redacted if needed) |
| `output_summary` | string? | no | Truncated output (max 4096 chars) |
| `exit_code` | int? | no | Exit code for run/subprocess |
| `error_type` | string? | no | Error classification |
| `error_message` | string? | no | Error details (max 2048 chars) |
| `workspace_relpaths_touched` | array? | no | Files affected |

## Event Types

- `tool_call`: Agent invoked a tool
- `phase_start`: Phase began
- `phase_end`: Phase completed
- `timeout`: Operation timed out
- `error`: Unrecoverable error
- `patch_applied`: Patch was applied to workspace
- `policy_check`: Policy enforcement result

## Redaction Policy

- Output summaries are truncated to 4096 characters
- Large binary content is replaced with `[BINARY: N bytes]`
- Secrets (if detected) are replaced with `[REDACTED]`

## Score Fields

| Field | Type | Description |
|-------|------|-------------|
| `run_id` | string | Run identifier |
| `task_id` | string | Task identifier |
| `public_pass` | bool | Public repro test passed |
| `hidden_pass` | bool | Hidden suite passed |
| `policy_pass` | bool | Patch policy respected |
| `overall_pass` | bool | All three passed |
| `failure_label` | string? | Primary failure classification |
| `metrics.tool_calls` | int | Total tool invocations |
| `metrics.wall_clock_s` | float | Agent phase time |
| `metrics.patch.*` | object | Patch diff statistics |
| `metrics.token_usage` | object? | Optional token counts |
| `metrics.coverage` | object? | Optional coverage data |

## Failure Labels

| Label | Description |
|-------|-------------|
| `AGENT_TIMEOUT` | Agent exceeded wall-clock limit |
| `AGENT_ERROR` | Agent crashed or errored |
| `NO_PATCH` | No patch produced |
| `PATCH_APPLY_FAIL` | Patch could not be applied |
| `POLICY_VIOLATION` | Patch edited forbidden files |
| `PUBLIC_FAIL` | Public repro test failed |
| `HIDDEN_FAIL` | Hidden test suite failed |
| `HIDDEN_TIMEOUT` | Hidden suite timed out |
| `HIDDEN_ERROR` | Hidden suite errored |
| `GRADER_ERROR` | Grader infrastructure error |
