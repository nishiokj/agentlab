# ISSUE: Agent Artifact Integrity Failure — Hollow Shims Pass Build and Run Unchallenged

**Severity:** Critical — Correctness
**Affects:** `lab-cli build`, `lab-cli build-run`, `lab-cli run`, all variant comparisons
**Date:** 2026-03-04

---

## Summary

The build and run pipeline accepts agent artifacts that contain no agent code. A 90-byte shell shim that `exec`s into code baked into the Docker image passes every validation gate — content digest, sealed package checksums, integrity verification, and artifact pin checks — while contributing zero executable substance to the experiment.

This means:

1. **Variant comparisons are fraudulent.** Two variants specifying different artifacts will run the same agent code from the image. The A/B test infrastructure is producing results that do not reflect the declared experiment design.
2. **The sealed package guarantee is void.** The package claims to be a self-contained, tamper-evident bundle. It is not. The actual agent behavior is determined by an untracked Docker image, not the checksummed artifact.
3. **Artifact digests protect nothing.** `compute_artifact_content_digest` faithfully hashes the shim. The digest is stable and verifiable. It is also meaningless — it certifies that the 90-byte redirect has not been tampered with while the actual agent code has no integrity tracking whatsoever.

---

## The Concrete Failure

Production artifact `rex-minimal-linux-dir-dfe3634c6e34`:

```
rex-minimal-linux-dir-dfe3634c6e34/
├── bin/rex          (90 bytes)
└── manifest.json
```

`bin/rex`:
```sh
#!/usr/bin/env sh
exec /usr/local/bin/bun /workspace/packages/apps/launcher/index.ts "$@"
```

This artifact is a trampoline. The actual agent is:
- `/usr/local/bin/bun` — runtime, from the Docker image
- `/workspace/packages/apps/launcher/index.ts` — agent code, from the Docker image

Neither is inside the artifact. Neither is tracked by the sealed package.

### What this breaks

The YAML declares variants with different bindings:
```yaml
variants:
  - id: codex_spark
    bindings:
      model_provider: codex
      model: gpt-5.3-codex-spark
```

If a user specifies `runtime_overrides` with a different artifact per variant — which is the entire point of the variant system — both variants would mount their respective hollow shims at `/opt/agent`, and both would `exec` into the same `/workspace/packages/apps/launcher/index.ts` from the image. Different artifact, identical execution.

---

## Build-Side Failures

### 1. `normalize_experiment_authoring` (lib.rs:4850)

Resolves `agent.artifact`, confirms the path exists, computes a content digest. **Does not inspect what the artifact contains.** A directory with a single empty file passes.

### 2. `compute_artifact_content_digest` (lib.rs:4362)

Walks the artifact directory, hashes every file, produces a stable digest. **Does not evaluate whether the artifact is self-contained.** It treats `#!/usr/bin/env sh\nexec /usr/local/bin/bun ...` the same as a real bundled binary.

### 3. `rewrite_runtime_paths_for_package` (lib.rs:8153)

Copies the artifact into `agent_builds/`, rewrites the JSON pointer. **Does not validate that the staged artifact is functional.** It copies a shim and calls it done.

### 4. `validate_required_fields` (lib.rs:5276)

Checks that required JSON pointers exist. Does not validate artifact content at all.

### 5. `build_experiment_package` (lib.rs:8241)

Orchestrates the above, produces sealed checksums. **The checksums are correct** — they accurately hash the hollow artifact. The integrity system works perfectly on worthless data.

---

## Run-Side Failures

### 6. `validate_agent_artifact_pin` (lib.rs:13383)

At run time, verifies the artifact path matches the expected resolved path and recomputes the content digest against the pinned value. **This confirms the shim hasn't been modified, not that it does anything.**

### 7. `run_injected_container` (lib.rs:14886)

Bind-mounts the artifact at `/opt/agent:ro` and sets `PATH=/opt/agent/bin:...`. The container executes `/opt/agent/bin/rex`, which immediately `exec`s out of `/opt/agent` into image-resident code. **The mount is cosmetic.**

### 8. `resolve_agent_artifact_mount_dir` (lib.rs:14787)

For directory artifacts, returns the path directly. For tarballs, unpacks and caches. **No validation that the unpacked content constitutes a runnable agent.**

---

## Why Tests Did Not Catch This

### The test fixture is itself a hollow shim

`create_dx_authoring_fixture` (lib.rs:22008) creates the test artifact:

```rust
fs::write(artifact_bin.join("rex"), "#!/bin/sh\necho rex\n").expect("artifact binary");
```

This is a 20-byte shell script that prints "rex". Every test that uses `create_dx_authoring_fixture` or `minimal_new_dx_spec` is testing against a shim. The fixture enshrines the defect as the expected behavior.

### `build_experiment_package_rewrites_runtime_sources` (lib.rs:22414)

Verifies:
- Manifest exists and has `sealed_run_package_v2` schema
- Dataset path is rewritten to `tasks/tasks.jsonl`
- Artifact path starts with `agent_builds/`

**Does not verify:** That the artifact under `agent_builds/` contains agent code, a real binary, or anything beyond a non-empty directory.

### `p0_i04_artifact_digest_pin_rejects_mutation` (lib.rs:22864)

Verifies that mutating the artifact content changes the digest and triggers rejection. **This test works correctly** — it catches tampering. But tampering with a shim that `exec`s elsewhere is not the threat. The threat is that the artifact was never the agent in the first place.

### Digest tests in normalization (lib.rs:22211)

Verify `artifact_digest` starts with `sha256:`. **Existence check only.** The digest could be of an empty file.

### Zero tests exist for:
- Artifact contains a real executable or entry point that doesn't delegate outside `/opt/agent`
- Artifact is self-contained (no references to paths outside the mount)
- Variant-level artifact isolation (two variants with different artifacts produce different behavior)
- The `command` field in the YAML is consistent with what the artifact provides

---

## Required Fixes

### Build-Side (Blocking)

1. **Artifact content validation in `build_experiment_package`.**
   After staging the artifact into `agent_builds/`, validate:
   - The artifact contains at least one executable file, or a manifest declaring an entrypoint
   - If an `entrypoint` is declared (via `manifest.json` or convention), it exists in the artifact
   - Shell scripts at the entrypoint do not `exec` or reference absolute paths outside `/opt/agent` (heuristic, not perfect, but catches the blatant case)

2. **Warn or reject shim-only artifacts.**
   If the total non-manifest content of the artifact is under a configurable threshold (e.g., 1KB), emit a build warning. If every executable in `bin/` is a shell trampoline to an external path, reject the build.

3. **Self-containment check for the `command` field.**
   The YAML command `exec /opt/agent/bin/rex run ...` is fine — it references the mount. But the shim inside then does `exec /usr/local/bin/bun /workspace/...`. The build can't see inside the container, but it can flag when the artifact entrypoint's content references paths outside the artifact tree.

### Run-Side (Blocking)

4. **Runtime artifact self-containment assertion.**
   Before mounting, scan the entrypoint script. If it `exec`s or sources a path outside `/opt/agent`, fail with an explicit error: "artifact entrypoint delegates to image-resident code at {path}; the artifact must be self-contained."

5. **Variant artifact isolation test.**
   At the start of a multi-variant run, verify that distinct variant artifacts have distinct content digests. If two variants declare different artifacts but resolve to the same digest, warn. If they resolve to the same digest and the artifact is a shim, reject.

### Test Coverage (Blocking)

6. **Replace the test fixture.**
   `create_dx_authoring_fixture` must produce a real artifact: a compiled binary or a self-contained script that doesn't delegate outside the artifact tree. The current `echo rex` shim must die.

7. **New tests — build side:**
   - `build_rejects_shim_only_artifact` — artifact whose entrypoint `exec`s to `/usr/local/bin/X`. Build must fail or warn.
   - `build_rejects_empty_artifact` — artifact directory with no executable files.
   - `build_accepts_self_contained_artifact` — artifact with a real bundled binary/script. Build succeeds.
   - `build_artifact_entrypoint_references_only_artifact_paths` — parse the entrypoint, verify all `exec`/source targets are relative or under `/opt/agent`.

8. **New tests — run side:**
   - `run_rejects_artifact_with_external_exec` — mount a shim artifact, attempt `run_injected_container`, expect failure.
   - `variant_isolation_detects_identical_artifacts` — two variants declare different artifact paths that resolve to identical content. Expect warning/error.
   - `variant_isolation_distinct_artifacts_produce_distinct_mounts` — two variants with genuinely different artifacts mount different content.

9. **New tests — end-to-end:**
   - `build_run_with_bundled_artifact_executes_artifact_code` — build an experiment with a real self-contained artifact. Run it. Verify the output came from the artifact's code, not image-resident code.

---

## Root Cause

The pipeline was built to track **identity and integrity** of artifacts (path pinning, content digests, sealed checksums) without ever validating **substance**. It answers "is this the same artifact?" without asking "is this artifact anything?" The test suite reinforced this by using fixtures that are themselves hollow, making the defect invisible at every layer.
