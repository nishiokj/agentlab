# Engine / Preflight / Runtime — Function Triage

Classification key:

| Code | Meaning |
|------|---------|
| **DELETE** | Obsolete. Workspace slop, dead code, or no-op stubs. |
| **experiment/x** | Experiment-level concern. Pre-trial prep, scheduling, variant resolution, policy. |
| **trial/x** | Trial-level concern. Execution, state, grading, container lifecycle. |
| **package/x** | Build-time concern. Sealed package authoring, staging manifests, asset compilation. |
| **persistence/x** | Storage concern. Artifact store, JSONL, SQLite, evidence records. |
| **SPLIT** | Function body crosses boundaries. Must be decomposed. |
| **NEW: x** | Needs a new focused module. |

---

## engine.rs

### Utility / Logging

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 54 | `parse_bool_env` | **NEW: config/env** | `config.rs` or `config/env.rs` | Pure env parsing. Shared utility. |
| 62 | `progress_logs_enabled` | **NEW: config/env** | Same | Cached env check. |
| 74 | `emit_progress_log` | **experiment/logging** | `experiment/runner.rs` or inline | Only used during experiment execution. |
| 82 | `emit_preflight_log` | **experiment/logging** | Same | Wrapper. |
| 86 | `emit_run_log` | **experiment/logging** | Same | Wrapper. |
| 90 | `should_emit_image_probe_progress` | **experiment/preflight** | `preflight.rs` | Only used by preflight image probes. |
| 97 | `parse_parallelism` | **experiment/preflight** | `preflight.rs` | Only used by preflight. |
| 107 | `preflight_image_probe_parallelism` | **experiment/preflight** | `preflight.rs` | Only used by preflight. |
| 114 | `run_bounded_image_probes` | **experiment/preflight** | `preflight.rs` | Generic parallel probe runner. Only caller is preflight. |
| 168 | `emit_slot_commit_progress` | **experiment/runner** | `experiment/runner.rs` or `experiment/commit.rs` | Scheduling progress emission. |

### Config / Capacity Parsing

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 210 | `parse_local_worker_capacity_ceiling_from_env` | **experiment/runner** | `experiment/runner.rs` | Local worker config. Only used by schedule engine. |
| 241 | `parse_max_run_bytes_from_env` | **experiment/runner** | `experiment/runner.rs` | Runtime budget. Only used by schedule engine. |
| 272 | `parse_max_workspace_bundle_bytes_from_env` | **DELETE** | — | Workspace concept. |
| 303 | `resolve_local_worker_max_in_flight` | **experiment/runner** | `experiment/runner.rs` | Worker capacity logic. |
| 323 | `make_slot_commit_id` | **experiment/commit** | `experiment/commit.rs` | Slot commit ID generation. |

### Trial State

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 334 | `write_trial_state` | **trial/state** | `trial/state.rs` | Writes trial_state.json. Currently called from both experiment and trial code — trial should own this. |
| 354 | `TrialStateGuard` (struct + impl + Drop) | **trial/state** | `trial/state.rs` | RAII guard. Belongs with the state it guards. |

### Public API Entry Points

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 432 | `run_experiment` | **experiment/runner** | `lib.rs` facade → `experiment/runner.rs` | Thin wrapper. |
| 436 | `run_experiment_with_options` | **experiment/runner** | Same | Thin wrapper. |
| 440 | `run_experiment_strict` | **experiment/runner** | Same | Thin wrapper. |
| 444 | `run_experiment_strict_with_options` | **experiment/runner** | Same | Thin wrapper. |
| 457 | `find_project_root_from_run_dir` | **experiment/runner** | `experiment/runner.rs` | Path navigation used only by experiment. |
| 478 | `continue_run` | **experiment/runner** | `lib.rs` facade → `experiment/runner.rs` | Thin wrapper. |

### Structs

| Line | Item | Classification | Target | Notes |
|------|------|---------------|--------|-------|
| 190 | `AdapterRunRequest<'a>` | **SPLIT** | Becomes owned `TrialSpec` in trial, constructed by experiment | This is the boundary type. Currently all-borrowed. Needs to become an owned value that experiment builds and hands off. Fields referencing workspace/dynamic_mounts reviewed — `trial_paths` and `dynamic_mounts` are workspace-adjacent. |

### engine.rs summary

engine.rs is a junk drawer. Nothing should remain here. Every function moves to its natural home. The file itself gets deleted.

---

## preflight.rs

### Entry Points

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 25 | `preflight_experiment` | **experiment/preflight** | `experiment/preflight.rs` | Top-level entry. |
| 29 | `preflight_experiment_with_options` | **experiment/preflight** | `experiment/preflight.rs` | Main orchestrator. Loads package, resolves variants, runs checks. |

### Check Functions — Variant Validation

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 123 | `check_agent_runtime_hermetic_for_variants` | **experiment/preflight** | Same | Wrapper iterating variants. |
| 147 | `check_agent_runtime_hermetic` | **experiment/preflight** | Same | Image pin validation. |
| 167 | `check_dangerous_mode_forbidden_for_variants` | **experiment/preflight** | Same | Wrapper. |
| 191 | `check_dangerous_mode_forbidden` | **experiment/preflight** | Same | Scans argv for bypass tokens. |
| 215 | `check_workspace_contract_not_host_path_for_variants` | **DELETE** | — | Workspace concept. Checks host scratch path leakage into workspace mounts. |
| 239 | `check_workspace_contract_not_host_path` | **DELETE** | — | Same. |
| 275 | `check_agent_bundle_container_compatible_for_variants` | **experiment/preflight** | Same | Wrapper. |
| 299 | `check_agent_bundle_container_compatible` | **experiment/preflight** | Same | Checks artifact filename. |
| 329 | `check_task_sandbox_bash_plane_for_variants` | **DELETE** | — | No-op stub. Always passes. |
| 353 | `check_task_sandbox_bash_plane` | **DELETE** | — | No-op stub. |
| 1445 | `check_dependency_files_exist` | **DELETE** | — | No-op stub. Returns always-passing check. |
| 1460 | `check_workspace_patch_sources_exist` | **DELETE** | — | No-op stub. Returns always-passing check. |

### Check Functions — Container & Runtime Reachability

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 365 | `check_benchmark_grader_reachable_for_variants` | **experiment/preflight** | Same | Wrapper. |
| 399 | `check_container_ready_for_variants` | **experiment/preflight** | Same | Wrapper. |
| 962 | `check_benchmark_grader_reachable` | **experiment/preflight** | Same | Convenience wrapper. |
| 979 | `check_agent_runtime_reachable_for_variants` | **experiment/preflight** | Same | Wrapper. |
| 1010 | `check_agent_runtime_reachable_with_scan` | **experiment/preflight** | Same | Core agent reachability check. Probes images. |
| 1119 | `check_benchmark_grader_reachable_with_scan` | **experiment/preflight** | Same | Core grader reachability check. |
| 1239 | `check_container_ready` | **experiment/preflight** | Same | Multi-stage: daemon, images, shell. |

### Check Functions — Data Validation

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 861 | `check_dataset_task_ids` | **experiment/preflight** | Same | Task ID uniqueness + grading validation. |

### Check Orchestration

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 682 | `collect_preflight_checks` | **experiment/preflight** | Same | Master collector. Runs all checks in sequence. References to workspace checks get deleted along with those checks. |

### Disk & Budget Enforcement

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 504 | `has_blocking_preflight_error` | **experiment/preflight** | Same | Check result inspection. |
| 510 | `resolve_min_free_bytes` | **experiment/preflight** | Same | Env config. |
| 544 | `free_bytes_for_path` | **experiment/preflight** | Same | Runs `df -Pk`. |
| 581 | `dir_size_bytes` | **experiment/preflight** | Same | Walks dir tree. |
| 597 | `enforce_runtime_disk_headroom` | **experiment/runner** | `experiment/runner.rs` | Called from schedule engine loop, not just preflight. |
| 610 | `enforce_runtime_run_size_budget` | **experiment/runner** | `experiment/runner.rs` | Same. |
| 623 | `check_disk_headroom_with_threshold` | **experiment/preflight** | Same | Preflight check. |
| 669 | `check_disk_headroom` | **experiment/preflight** | Same | Preflight check. |

### Helpers

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 104 | `PerTaskImageScanResult` (struct) | **experiment/preflight** | Same | Scan result type. |
| 111 | `format_preview` | **experiment/preflight** | Same | Formatting utility. |
| 421 | `collect_per_task_images_for_preflight` | **experiment/preflight** | Same | Task image scanning. |
| 453 | `resolve_preflight_images` | **experiment/preflight** | Same | Image list resolution. |
| 1223 | `is_runner_staged_script_path` | **experiment/preflight** | Same | Path classification. |
| 1475 | `resolve_dataset_path` | **experiment/preflight** | Same | Dataset path extraction. |
| 1483 | `count_tasks` | **experiment/preflight** | Same | Task counting. |

### trial/preflight.rs

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 12 | `stage_benchmark_trial_preflight` | **experiment/runner** | `experiment/schedule.rs` or inline in trial prep | This is pre-trial prep work (step 37 in boundary spec). Stages benchmark artifacts before trial launch. Not trial-owned. |

### preflight.rs summary

Straightforward. The whole file moves to `experiment/preflight.rs` minus the DELETE items (4 no-op stubs + 2 workspace checks). `trial/preflight.rs` single function moves to experiment.

---

## runtime.rs

This is the big one. ~3200 lines, dozens of functions across at least 8 different concerns.

### Workspace Materialization — DELETE

| Line | Function | Classification | Notes |
|------|----------|---------------|-------|
| 288 | `materialize_workspace_git_checkout` | **DELETE** | Workspace concept. |
| 297 | `materialize_workspace_git_checkout_to_dir` | **DELETE** | Workspace concept. |
| 315 | `materialize_workspace_base_to_dir` | **DELETE** | Workspace concept. Routes Empty/DatasetPack/GitCheckout. |
| 355 | `materialize_workspace_base` | **DELETE** | Workspace concept. Wrapper. |
| 363 | `materialize_workspace_overlays_to_dir` | **DELETE** | Workspace concept. Writes inline overlay files. |
| 401 | `materialize_workspace_overlays` | **DELETE** | Workspace concept. Wrapper. |
| 583 | `materialize_workspace_aux_mounts_to_dir` | **DELETE** | Workspace concept. Resolves dataset pack mounts. |

### Git Checkout Management — DELETE

| Line | Function | Classification | Notes |
|------|----------|---------------|-------|
| 132 | `git_repo_cache_dir` | **DELETE** | Only used by workspace git checkout. |
| 139 | `git_checkout_clone_url` | **DELETE** | Same. |
| 147 | `git_commit_available` | **DELETE** | Same. |
| 160 | `ensure_git_checkout_cache` | **DELETE** | Same. |
| 206 | `git_checkout_staging_dir` | **DELETE** | Same. |
| 218 | `prepare_git_checkout_worktree` | **DELETE** | Same. |
| 255 | `cleanup_git_checkout_worktree` | **DELETE** | Same. |
| 278 | `hydrate_git_checkout_cache` | **DELETE** | Same. |

### Path Validation & Resolution

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 48 | `validate_workspace_relative_path` | **DELETE** | — | Workspace concept. Only validates workspace-relative paths. |
| 75 | `validate_container_workspace_path` | **trial/execution** | `trial/execution.rs` | Validates absolute container paths. Rename to `validate_container_path`. |
| 88 | `parse_dataset_pack_ref_digest` | **DELETE** | — | Only used by workspace materialization + aux mounts. |
| 98 | `resolve_dataset_pack_host_path` | **DELETE** | — | Same. |
| 572 | `container_workspace_rel_path` | **DELETE** | — | Workspace concept. |

### Task Bundle & Dependencies

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 491 | `TaskDependencyFileSpec` (struct) | **package/staging** | `package/staging.rs` | Build-time dependency spec. |
| 505 | `parse_task_dependency_files_value` | **package/staging** | Same | Parses dependency JSON. |
| 515 | `materialize_task_dependencies_to_dir` | **DELETE** | — | Workspace materialization variant. Dependencies delivered via IO contract. |
| 562 | `materialize_task_dependencies_for_trial` | **DELETE** | — | Same. |
| 632 | `stage_dependencies_for_trial` | **DELETE** | — | Workspace adjacent. Stages files from runtime config to host workspace. |
| 708 | `DependencyFileStagingSpec` (struct) | **package/staging** | `package/staging.rs` | Staging spec type. Used in package compilation. |
| 1877 | `resolve_task_bundle_host_path` | **DELETE** | — | Task bundle materialization (workspace concept). |
| 1906 | `materialize_task_bundle_for_trial` | **DELETE** | — | Same. Copies/extracts bundle to workspace. |

### Agent Runtime Config Resolution

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 727 | `AgentRuntimeConfig` (struct) | **experiment/runtime** | `experiment/runtime.rs` | Core config type. Defines how the agent runs. Experiment resolves this. |
| 764 | `parse_command_field` | **experiment/runtime** | Same | JSON parsing for command arrays. |
| 788 | `reject_packaged_public_path_references` | **package/validate** | `package/validate.rs` | Sealed package validation. |
| 821 | `load_staging_specs_from_package` | **package/staging** | `package/staging.rs` | Reads staging manifest from sealed package. |
| 888 | `derive_public_command_path_staging_specs` | **package/staging** | Same | Extracts public path refs from commands. |
| 931 | `derive_public_path_staging_specs` | **package/staging** | Same | Combines command + env path refs. |
| 979 | `normalize_staged_support_source_path` | **package/staging** | Same | Path normalization for staged assets. |
| 1014 | `parse_build_runtime_asset_specs` | **package/staging** | Same | Parses runtime_assets arrays. |
| 1062 | `merge_dependency_file_staging` | **package/staging** | Same | Dedup merge of staging specs. |
| 1078 | `binding_lookup` | **experiment/runtime** | Same | JSON pointer binding lookup. |
| 1086 | `binding_lookup_string` | **experiment/runtime** | Same | String extraction from binding. |
| 1110 | `resolve_runtime_binding_value` | **experiment/runtime** | Same | Layered binding resolution. |
| 1135 | `render_runtime_template` | **experiment/runtime** | Same | $NAME template rendering. |
| 1189 | `resolve_command_templates` | **experiment/runtime** | Same | Renders all command tokens. |
| 1206 | `resolve_env_templates` | **experiment/runtime** | Same | Renders all env values. |
| 1227 | `resolve_agent_runtime` | **experiment/runtime** | Same | Build-time resolution. |
| 1241 | `resolve_packaged_agent_runtime` | **experiment/runtime** | Same | Sealed package resolution. |
| 1255 | `resolve_agent_artifact_path_for_context` | **SPLIT** | Build-time → `package/`, Run-time → `experiment/runtime` | Routes by context. |
| 1283 | `resolve_runtime_source_path_for_context` | **SPLIT** | Same split. | Routes by context. |
| 1305 | `resolve_agent_runtime_with_context` | **SPLIT** | Core in `experiment/runtime`, package-specific bits in `package/` | ~200-line function. The core parsing is experiment config; the sealed package path resolution is package concern. |
| 716 | `PathResolutionContext` (enum) | **SPLIT** | Build variant → `package/`, Run variant → `experiment/runtime` | The enum itself encodes the split. |

### Runtime Environment Resolution

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 1516 | `parse_runtime_env_file` | **experiment/runtime** | `experiment/runtime.rs` | Parses KEY=VALUE files. |
| 1552 | `resolve_runtime_env_inputs` | **experiment/runtime** | Same | Merges env file + CLI env. |
| 1575 | `resolve_agent_runtime_env` | **experiment/runtime** | Same | Renders env templates. |
| 1588 | `ensure_required_runtime_env_present` | **experiment/runtime** | Same | Validates env_from_host. |
| 1603 | `validate_agent_artifact_pin` | **experiment/runtime** | Same | Digest validation. |

### Benchmark Runtime Assets

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 1637 | `resolve_benchmark_runtime_assets` | **experiment/runtime** | `experiment/runtime.rs` | Collects grader/mapper staging specs. |

### Variant Runtime Profile

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 1714 | `VariantRuntimeProfile` (struct) | **experiment/runtime** | `experiment/runtime.rs` | Per-variant resolved profile. |
| 1725 | `command_contains_scientific_bypass` | **experiment/preflight** | `experiment/preflight.rs` | Bypass detection. Used only by preflight + profile_is_hermetic. |
| 1740 | `preview_agent_command` | **experiment/runtime** | Same | Combines command + args. |
| 1746 | `value_contains_host_scratch_path` | **DELETE** | — | Workspace concept. Checks for .scratch paths. |
| 1751 | `profile_is_hermetic` | **experiment/runtime** | Same | Hermetic profile check. Remove host scratch path check. |
| 1764 | `resolve_run_isolation_grade` | **experiment/runtime** | Same | Run-level grade. |
| 1776 | `resolve_variant_runtime_profile_with_context` | **SPLIT** | Core → `experiment/runtime`, package parts → `package/` | Massive function. Merges build/run context, resolves everything. |
| 1856 | `resolve_variant_runtime_profile` | **experiment/runtime** | Same | Context detection wrapper. |

### Preflight Probe Functions

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 2241 | `PreflightProbeRoot` (struct) | **experiment/preflight** | `experiment/preflight.rs` | Temp dir with Drop. |
| 2251 | `PreflightProbeContext` (struct) | **experiment/preflight** | Same | Probe environment. |
| 2262 | `create_preflight_probe_root` | **experiment/preflight** | Same | Creates temp probe dir. |
| 2273 | `select_preflight_probe_task` | **experiment/preflight** | Same | Finds matching task for image. |
| 2302 | `build_preflight_probe_context` | **experiment/preflight** | Same | Builds probe context. Currently calls `prepare_task_environment` — that dependency needs to change when prepare is refactored. |
| 2400 | `build_preflight_probe_request` | **experiment/preflight** | Same | Constructs AdapterRunRequest for probe. |
| 2430 | `PreflightContractSmokeExecution` (struct) | **experiment/preflight** | Same | Smoke test result. |
| 2436 | `read_optional_text_file` | **experiment/preflight** | Same or shared utility | Trivial file read. |
| 2443 | `run_preflight_contract_smoke` | **experiment/preflight** | Same | Runs trial for probe. |
| 2472 | `detect_known_probe_output_blockers` | **experiment/preflight** | Same | Error pattern matching. |
| 2497 | `summarize_preflight_failure_logs` | **experiment/preflight** | Same | Log excerpting. |
| 2512 | `validate_preflight_result_payload` | **experiment/preflight** | Same | Result schema validation. |
| 2584 | `validate_preflight_benchmark_smoke_outputs` | **experiment/preflight** | Same | Grader output validation. |
| 2630 | `collect_preflight_contract_smoke_failures` | **experiment/preflight** | Same | Aggregates failures. |

### Agent Runtime Command Helpers

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 2664 | `resolve_agent_runtime_command` | **experiment/runtime** | `experiment/runtime.rs` | Template rendering. |
| 2672 | `validate_agent_runtime_command` | **experiment/runtime** | Same | Non-empty check. |
| 2682 | `shell_join` | **NEW: util** | Shared utility | Generic. |
| 2690 | `shell_quote` | **NEW: util** | Shared utility | Generic. |

### Container & Docker Helpers

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 2038 | `resolve_task_sandbox_image` | **trial/execution** | `trial/execution.rs` | Validates image from request. |
| 2046 | `resolve_container_workspace` | **DELETE** | — | Validates task_workdir — rename opportunity to `validate_task_workdir` if needed. But "workspace" name is misleading. Trivial validation, inline where used. |
| 2085 | `resolve_container_platform` | **trial/execution** | `trial/execution.rs` | Image → platform mapping. Used during container creation. |
| 2096 | `resolve_container_image_digest` | **trial/execution** | `trial/execution.rs` | Docker image digest lookup. Used for state inventory. |
| 2056 | `run_checked_command` | **NEW: util** | Shared utility | Generic command runner. |
| 2065 | `output_error_detail` | **NEW: util** | Shared utility | Generic error formatting. |

### Agent Artifact Handling

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 2106 | `agent_artifact_cache_lock` | **trial/execution** | `trial/execution.rs` | Process-wide lock for artifact unpacking. |
| 2111 | `repair_agent_artifact_layout` | **trial/execution** | Same | Symlink fixup for nested package layouts. |
| 2142 | `resolve_agent_artifact_mount_dir` | **trial/execution** | Same | Unpack tar to cache dir. Used when building container mounts. |

### Trial Materialization & Layout

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 2703 | `materialize_trial_result` | **experiment/runner** | `experiment/schedule.rs` (finalization) | Copies output to canonical location. Post-trial, experiment-owned. |
| 2719 | `copy_file_if_exists` | **NEW: util** | Shared utility | Safe file copy. |
| 2733 | `copy_dir_preserve_contents` | **NEW: util** | Shared utility | Deep dir copy. |
| 2772 | `materialize_trial_runtime_layout` | **experiment/runner** | `experiment/schedule.rs` (finalization) | Applies materialization mode to trial dir. Post-trial, experiment-owned. |
| 2808 | `write_adapter_continue_control` | **DELETE** | — | Test-only. |
| 2814 | `write_adapter_control_action` | **DELETE** | — | Control plane action. Appears unused outside tests. Verify before delete. |

### State & Inventory

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 2835 | `resolve_agent_runtime_manifest_path` | **experiment/runner** | `experiment/schedule.rs` (finalization) | Maps contract path to host. Used in finalize_scheduled_trial. |
| 2857 | `write_state_inventory` | **experiment/runner** | `experiment/schedule.rs` (finalization) | Writes state_inventory.json. Post-trial aggregation. |
| 2842 | `resolve_exec_digest` | **experiment/runtime** | `experiment/runtime.rs` | Command digest for state inventory. |

### Path Cleanup & Filesystem Utilities

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 1958 | `sanitize_for_fs` | **NEW: util** | Shared utility | Generic string sanitizer. |
| 2979 | `remove_path_if_exists` | **NEW: util** | Shared utility | Safe removal. |
| 3000 | `make_path_tree_writable` | **NEW: util** | Shared utility | Permission fix. |
| 3051 | `preserve_symlink` | **NEW: util** | Shared utility | Symlink recreation. |
| 3061 | `apply_materialization_policy` | **experiment/runner** | `experiment/schedule.rs` | Post-trial cleanup. |
| 3096 | `copy_dir_with_policy` | **SPLIT** | Core copy → `util`, workspace evidence exclusion → **DELETE** | The `respect_workspace_evidence_exclusions` param is workspace slop. Without it, this is a generic filtered dir copy. |
| 3147 | `copy_dir_filtered` | **SPLIT** | Rename to just use `copy_dir_with_policy` after removing workspace exclusion. | Currently enables workspace evidence exclusion by default. |
| 3151 | `copy_dir_preserve_all` | **NEW: util** | Shared utility | The clean variant (no workspace exclusion). |
| 3155 | `command_part_looks_like_path` | **experiment/runtime** | `experiment/runtime.rs` | Heuristic for digest target. |
| 3167 | `resolve_command_digest_target` | **experiment/runtime** | Same | Picks command token to hash. |

### Persistence (JSONL / SQLite)

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| 1974 | `append_jsonl_file` | **persistence/journal** | `persistence/journal.rs` | Raw JSONL file append. |
| 1987 | `append_jsonl` | **persistence/journal** | Same | Routes JSONL vs SQLite. This is persistence routing logic. |
| 2030 | `output_peer_path` | **NEW: util** | Shared utility | Sibling file path computation. |

### Map / Path Resolution (Container ↔ Host)

| Line | Function | Classification | Target | Notes |
|------|----------|---------------|--------|-------|
| — | `map_container_path_to_host` | **trial/execution** | `trial/execution.rs` | Maps container contract paths to host paths. Core to container IO contract. (Referenced but defined elsewhere or in imports — verify location.) |

---

## Summary Counts

| Classification | engine.rs | preflight.rs | runtime.rs | Total |
|---------------|-----------|-------------|------------|-------|
| **DELETE** | 1 | 6 | ~22 | ~29 |
| **experiment/preflight** | 4 | ~22 | ~14 | ~40 |
| **experiment/runtime** | — | — | ~28 | ~28 |
| **experiment/runner** | 7 | 2 | ~5 | ~14 |
| **trial/state** | 2 | — | — | 2 |
| **trial/execution** | — | — | ~7 | ~7 |
| **package/staging** | — | — | ~8 | ~8 |
| **package/validate** | — | — | 1 | 1 |
| **persistence/journal** | — | — | 2 | 2 |
| **NEW: util** | — | — | ~12 | ~12 |
| **NEW: config/env** | 2 | — | — | 2 |
| **SPLIT** | 1 | — | ~5 | ~6 |

### New modules needed

- **`experiment/preflight.rs`** — All preflight checks. Currently exists as `preflight.rs` at crate root, just needs to move under `experiment/`.
- **`experiment/runtime.rs`** — Agent runtime config resolution, variant profile resolution, template rendering, env resolution. The heaviest chunk from `runtime.rs`.
- **`util.rs`** (or `util/fs.rs` + `util/shell.rs`) — Filesystem ops, shell quoting, sanitization, command execution. Small, no business logic.
- **`config/env.rs`** (optional) — `parse_bool_env`, env var utilities. Could also just fold into `config.rs`.

### Files to delete entirely after extraction

- **`engine.rs`** — Everything moves out. File deleted.
- **`runtime.rs`** — Everything moves out. File deleted.
- **`preflight.rs`** (at crate root) — Moves to `experiment/preflight.rs`.
- **`trial/preflight.rs`** — Single function moves to experiment.
- **`trial/schedule.rs`** — Moves to `experiment/schedule.rs` (per boundary spec).
- **`trial/spec.rs`** — Task row parsing stays (moves to experiment or model), workspace default invention deleted.
- **`trial/prepare.rs`** — Workspace materialization deleted, IO/env/plan building moves to experiment, TrialPaths gutted.
- **`trial/workspace.rs`** — Snapshot/diff/sync machinery deleted.
