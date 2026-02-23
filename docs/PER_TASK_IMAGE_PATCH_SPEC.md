# Per-Task Image Patch Spec

## Problem

Agent containers have no Python environment. The Dockerfile installs bare `python3`
(no pip, no conda), and the setup script only clones the repo. Agents thrash trying
to run `python`, `pytest`, `pip install` — all fail with exit 127 or "no module."

SWE-bench tasks require per-instance environments: specific Python versions, pre-installed
dependencies (numpy, pytest, astropy, django, etc.), and the repo installed in editable mode.
These environments vary by (repo, version) pair.

## Solution

Use the per-task Docker images that SWE-bench publishes. Each task's JSONL record
specifies its container image. The runner creates a container from that image (which
has all deps), injects the frozen agent artifact, and executes. No setup script needed.

## What is `docker create`

```
# Current: one shot, one image, agent baked in
docker run --rm {rex_image} {setup} && {agent_command}

# After: task image + agent injected at runtime
cid=$(docker create [flags] [mounts] {task_image} tail -f /dev/null)
docker start $cid
docker cp agent.tar.gz $cid:/tmp/agent.tar.gz
docker exec $cid tar xzf /tmp/agent.tar.gz -C /opt/agent
docker exec $cid [env vars] /opt/agent/bin/rex run ...
docker exec $cid [env vars] /opt/agent/grade/run.sh     # if grading
docker cp $cid:/out/. {trial_dir}/out/
docker rm -f $cid
```

`docker create` = `docker run` without starting. It allocates the container
(filesystem layers, mounts, network) and returns a container ID. You then
`docker start` it, `docker cp` files in, and `docker exec` commands inside it.

Key property: the container's filesystem is the task image (which has Python deps
pre-installed at `/testbed`). The agent is a separate artifact layered on top at
runtime. Environment and agent are decoupled.

## Effort Estimate

| Component | Lines | Difficulty |
|-----------|-------|------------|
| Runner: container lifecycle rewrite | ~150 | Medium — replacing `run_builtin_adapter_container` |
| Runner: per-task image threading | ~50 | Easy — read `task.image`, pass through |
| Runner: experiment config additions | ~20 | Easy — `image_source`, `agent_artifact` fields |
| Agent freeze script | ~60 | Easy — shell script, tar + manifest |
| Dataset builder: v2 fields | ~30 | Easy — add `image` + `workspace` to JSONL |
| **Total** | **~310** | **1-2 days** |

No new crates. No new CLI subcommands in phase 1 (freeze is a shell script).
The change is concentrated in one function (`run_builtin_adapter_container`) plus
plumbing to get `task.image` to it.

---

## Changes

### 1. Dataset JSONL: `task_boundary_v2`

Current (`task_boundary_v1`):
```json
{
  "schema_version": "task_boundary_v1",
  "task": {
    "id": "swebench_astropy_astropy_12907",
    "input": { "prompt": "..." },
    "swebench": { "input": { "repo": "astropy/astropy", "instance_id": "astropy__astropy-12907", "base_commit": "d16bfe05..." } }
  }
}
```

After (`task_boundary_v2`):
```json
{
  "schema_version": "task_boundary_v2",
  "task": {
    "id": "swebench_astropy_astropy_12907",
    "image": "swebench/sweb.eval.x86_64.astropy__astropy-12907:latest",
    "workspace": "/testbed",
    "input": { "prompt": "..." },
    "swebench": { "input": { "repo": "astropy/astropy", "instance_id": "astropy__astropy-12907", "base_commit": "d16bfe05..." } }
  }
}
```

New fields:
- `task.image` — Docker image for this task. Runner reads it, doesn't interpret it.
- `task.workspace` — working directory inside the container. Replaces hardcoded `/agentlab/workspace/repo`.

### 2. Experiment YAML: new fields

```yaml
runtime:
  agent:
    artifact: .lab/agents/rex-current.tar.gz    # NEW: frozen agent artifact
    image_source: per_task                       # NEW: "per_task" | "global" (default)
    command:
      - /opt/agent/bin/rex                       # Changed: path inside container after injection
      - run
      - --bindings-file
      - ${AGENTLAB_BINDINGS_PATH}
      - --events
      - ${AGENTLAB_TRAJECTORY_PATH}
      - --session-key
      - ${AGENTLAB_TRIAL_ID}
      - --working-dir
      - ${WORKSPACE}                             # Changed: uses task.workspace
      - --dangerous
    image: rex-harness:swebench-lite             # Kept: fallback when image_source=global
```

When `image_source: per_task`:
- Runner reads `task.image` from the JSONL row for each trial
- `runtime.agent.image` is ignored (but kept for backwards compat)
- `runtime.agent.artifact` is required (agent must be injected)

When `image_source: global` (default, current behavior):
- Runner uses `runtime.agent.image` for all trials
- `runtime.agent.artifact` is optional (agent baked into image)
- Existing experiments unchanged

### 3. Runner: `AgentRuntimeConfig` additions

**File:** `lab-runner/src/lib.rs`

```rust
// Add to AgentRuntimeConfig struct (~line 8642)
struct AgentRuntimeConfig {
    // ... existing fields ...
    agent_artifact: Option<PathBuf>,     // NEW: path to frozen agent tar.gz
    image_source: ImageSource,           // NEW: PerTask or Global
}

enum ImageSource {
    Global,     // Use container_image for all trials (current behavior)
    PerTask,    // Read task.image from JSONL per trial
}
```

**Parse in `resolve_agent_runtime()` (~line 8897):**
```rust
let image_source = agent
    .pointer("/image_source")
    .and_then(|v| v.as_str())
    .unwrap_or("global");
let image_source = match image_source {
    "per_task" => ImageSource::PerTask,
    _ => ImageSource::Global,
};

let agent_artifact = agent
    .pointer("/artifact")
    .and_then(|v| v.as_str())
    .map(|s| exp_dir.join(s));
```

### 4. Runner: per-task image threading

**File:** `lab-runner/src/lib.rs`

**`TaskBoundaryMaterialization` (~line 8178):**
```rust
struct TaskBoundaryMaterialization {
    task_payload: Value,
    workspace_files: Vec<WorkspaceFileSpec>,
    mount_references: Vec<MountReferenceSpec>,
    limits: TaskBoundaryLimits,
    task_image: Option<String>,       // NEW
    task_workspace: Option<String>,   // NEW
}
```

**`parse_task_boundary_from_dataset_task` (~line 8200):**
```rust
fn parse_task_boundary_from_dataset_task(task: &Value) -> Result<TaskBoundaryMaterialization> {
    let schema = task.get("schema_version").and_then(|v| v.as_str());
    // Accept both v1 and v2
    if schema != Some("task_boundary_v1") && schema != Some("task_boundary_v2") {
        return Ok(default_task_boundary(task.clone()));
    }

    // ... existing parsing ...

    let task_image = task_payload
        .pointer("/image")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let task_workspace = task_payload
        .pointer("/workspace")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Ok(TaskBoundaryMaterialization {
        task_payload,
        workspace_files: parse_workspace_files(obj.get("workspace_files"))?,
        mount_references: parse_mount_references(obj.get("mount_references"))?,
        limits: parse_task_limits(obj.get("limits"))?,
        task_image,
        task_workspace,
    })
}
```

**`AdapterRunRequest` (~line 163):**
```rust
struct AdapterRunRequest<'a> {
    // ... existing fields ...
    task_image: Option<&'a str>,        // NEW: per-task image override
    task_workspace: Option<&'a str>,    // NEW: per-task workspace override
    agent_artifact: Option<&'a Path>,   // NEW: frozen agent path
}
```

Thread through `execute_slot()` at ~line 4608: when building the `AdapterRunRequest`,
pass `task_boundary.task_image.as_deref()`, `task_boundary.task_workspace.as_deref()`,
and `agent_runtime.agent_artifact.as_deref()`.

### 5. Runner: container lifecycle rewrite

**File:** `lab-runner/src/lib.rs`, function `run_builtin_adapter_container` (~line 9690)

Replace the current `docker run --rm` pattern with a multi-step lifecycle when
`agent_artifact` is set (per-task mode). Keep the existing `docker run` path as
fallback for `image_source: global` with no artifact.

```rust
fn run_builtin_adapter_container(request: &AdapterRunRequest<'_>) -> Result<ProcessRunResult> {
    // Resolve image: per-task override or global
    let image = match request.task_image {
        Some(ti) => ti.to_string(),
        None => request.runtime.container_image
            .as_deref()
            .ok_or_else(|| anyhow!("no container image: set task.image or runtime.agent.image"))?
            .to_string(),
    };

    if let Some(artifact) = request.agent_artifact {
        run_injected_container(request, &image, artifact)
    } else {
        run_baked_container(request, &image)  // existing docker run logic
    }
}
```

**New function `run_injected_container`:**

```rust
fn run_injected_container(
    request: &AdapterRunRequest<'_>,
    image: &str,
    artifact: &Path,
) -> Result<ProcessRunResult> {
    // Phase 1: Create container
    let mut create = Command::new("docker");
    create.arg("create");

    // Apply same flags as current: --read-only, -u, --network, --security-opt, etc.
    // Apply same volume mounts as current: -v for in_dir, out, state, deps, workspace, dataset
    apply_sandbox_flags(&mut create, request);
    apply_volume_mounts(&mut create, request);
    apply_env_vars(&mut create, request);

    // Override workspace dir if task specifies it
    if let Some(ws) = request.task_workspace {
        create.args(["-w", ws]);
        create.arg("-e").arg(format!("WORKSPACE={}", ws));
    }

    create.arg(&image);
    create.args(["tail", "-f", "/dev/null"]);

    let create_out = create.output().context("docker create failed")?;
    if !create_out.status.success() {
        let stderr = String::from_utf8_lossy(&create_out.stderr);
        return Err(anyhow!("docker create failed: {}", stderr));
    }
    let cid = String::from_utf8_lossy(&create_out.stdout).trim().to_string();

    // Guard: ensure cleanup on any exit path
    let _cleanup = ContainerCleanup(&cid);

    // Phase 2: Start container
    run_docker(&["start", &cid])?;

    // Phase 3: Inject agent artifact
    run_docker(&["cp", &artifact.display().to_string(), &format!("{}:/tmp/agent.tar.gz", cid)])?;
    run_docker_exec(&cid, &["tar", "xzf", "/tmp/agent.tar.gz", "-C", "/opt/agent"])?;
    run_docker_exec(&cid, &["rm", "/tmp/agent.tar.gz"])?;

    // Phase 4: Execute agent
    let command = resolve_runtime_agent_command(request)?;
    let agent_result = run_docker_exec_with_timeout(&cid, &command, request.timeout)?;

    // Phase 5: Execute grader (if configured)
    if let Some(grader_cmd) = resolve_benchmark_grader_command(request) {
        // Set AGENTLAB_AGENT_EXIT_STATUS for grader
        run_docker_exec_with_env(&cid, &grader_cmd, &[
            (AGENTLAB_ENV_AGENT_EXIT_STATUS, &agent_result.exit_code.to_string()),
        ])?;
    }

    // Phase 6: Collect results (volumes already mounted, no docker cp needed for /out)

    // _cleanup drops here → docker rm -f $cid
    Ok(agent_result)
}

struct ContainerCleanup(String);
impl Drop for ContainerCleanup {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.0])
            .output();
    }
}
```

**Extract shared logic into helpers:**

Factor the sandbox flags, volume mounts, and env var logic out of the current
monolithic function into reusable helpers:

```rust
fn apply_sandbox_flags(cmd: &mut Command, request: &AdapterRunRequest<'_>) { ... }
fn apply_volume_mounts(cmd: &mut Command, request: &AdapterRunRequest<'_>) { ... }
fn apply_env_vars(cmd: &mut Command, request: &AdapterRunRequest<'_>) { ... }
```

These are pure extractions from the existing `run_builtin_adapter_container` body
(lines 9706-9862). No new logic, just moving code into named functions.

**Keep existing path as `run_baked_container`:**

Rename the existing `docker run --rm` logic (unchanged) for `image_source: global`:

```rust
fn run_baked_container(request: &AdapterRunRequest<'_>, image: &str) -> Result<ProcessRunResult> {
    // Exact current implementation: docker run --rm {image} ...
}
```

### 6. Agent freeze script

**New file:** `scripts/agentlab/freeze_agent.sh`

Packages the current build into a portable tar.gz that can be injected into any
Linux container via `docker cp`.

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${1:-.lab/agents/rex-current.tar.gz}"
OUT_ABS="$(cd "$(dirname "$OUT")" && pwd)/$(basename "$OUT")"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

# Copy bun binary
BUN="$(command -v bun)"
mkdir -p "$STAGE/bin"
cp "$BUN" "$STAGE/bin/bun"

# Copy built application
cp -R "$ROOT_DIR/package.json" "$STAGE/"
cp -R "$ROOT_DIR/bun.lock" "$STAGE/"
cp -R "$ROOT_DIR/node_modules" "$STAGE/"
cp -R "$ROOT_DIR/packages" "$STAGE/"
cp -R "$ROOT_DIR/config" "$STAGE/"
cp -R "$ROOT_DIR/scripts" "$STAGE/"

# Create entrypoint
cat > "$STAGE/bin/rex" <<'ENTRY'
#!/usr/bin/env sh
exec /opt/agent/bin/bun /opt/agent/packages/apps/launcher/dist/index.js "$@"
ENTRY
chmod +x "$STAGE/bin/rex"

# Write manifest
cat > "$STAGE/manifest.json" <<JSON
{
  "schema_version": "agent_artifact_v1",
  "id": "rex",
  "platform": "linux-x64",
  "entrypoint": "bin/rex",
  "frozen_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "source_commit": "$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
}
JSON

mkdir -p "$(dirname "$OUT_ABS")"
tar czf "$OUT_ABS" -C "$STAGE" .
echo "wrote $OUT_ABS ($(du -h "$OUT_ABS" | cut -f1))"
```

### 7. Dataset builder: v2 fields

**File:** `scripts/agentlab/build_swebench_curated_ab_experiment.mjs`

The dataset JSONL builder needs to emit `task.image` and `task.workspace`.
This requires knowing the SWE-bench Docker image naming convention.

SWE-bench image names:
```
swebench/sweb.eval.x86_64.{instance_id}:latest
```

Where `instance_id` is the SWE-bench instance ID with `__` intact (e.g., `astropy__astropy-12907`).

**New file:** `scripts/agentlab/enrich_dataset_v2.mjs`

Reads a v1 JSONL, adds `task.image` and `task.workspace`, writes v2 JSONL.

```javascript
import { readFileSync, writeFileSync } from 'node:fs';

const input = process.argv[2];
const output = process.argv[3] || input.replace('.jsonl', '.v2.jsonl');

const lines = readFileSync(input, 'utf8').split('\n').filter(l => l.trim());
const enriched = lines.map(line => {
  const row = JSON.parse(line);
  const instanceId = row.task?.swebench?.input?.instance_id;
  if (instanceId) {
    row.schema_version = 'task_boundary_v2';
    row.task.image = `swebench/sweb.eval.x86_64.${instanceId}:latest`;
    row.task.workspace = '/testbed';
  }
  return JSON.stringify(row);
});

writeFileSync(output, enriched.join('\n') + '\n');
console.log(`wrote ${enriched.length} rows to ${output}`);
```

### 8. Delete setup script dependency

The setup script (`setup_swebench_trial_workspace.sh`) is no longer needed when
using per-task images — the SWE-bench image already has the repo checked out at
the right commit with all deps installed at `/testbed`.

- Remove `--setup` from `run_curated_experiment.sh`
- Keep the script for backwards compat but it becomes a no-op for v2 tasks

### 9. Experiment builder update

**File:** `scripts/agentlab/build_swebench_curated_ab_experiment.mjs`

```javascript
builder
  .customAgentImage(image, [
    '/opt/agent/bin/rex',           // Changed: injected agent path
    'run',
    '--bindings-file', '${AGENTLAB_BINDINGS_PATH}',
    '--events', '${AGENTLAB_TRAJECTORY_PATH}',
    '--session-key', '${AGENTLAB_TRIAL_ID}',
    '--working-dir', '${WORKSPACE}',  // Changed: from task.workspace
    '--dangerous',
  ])
  // NEW:
  .agentArtifact('.lab/agents/rex-current.tar.gz')
  .imageSource('per_task')
```

### 10. Run script update

**File:** `scripts/agentlab/run_curated_experiment.sh`

Remove the `--setup` flag from `run-dev` mode when using per-task images:
```bash
if [[ "$RUN_MODE" == "run_dev" ]]; then
  "$RUNNER_BIN" run-dev "$EXPERIMENT_REL"
  # No --setup needed: task images have the environment
fi
```

---

## Sandbox policy: `--read-only` root filesystem

Current: the runner sets `--read-only` on the container root filesystem by default.

SWE-bench task images expect writable `/testbed` and system paths. Two options:

**Option A:** Disable `--read-only` for per-task image mode.
Set `runtime.policy.sandbox.root_read_only: false` in experiment YAML.

**Option B:** Add `--tmpfs` mounts for writable paths.
More secure but requires knowing which paths the agent writes to.

Recommendation: **Option A** for now. The volume mounts (`/out`, `/state`, etc.)
are already writable. The task image's `/testbed` needs to be writable for the
agent to edit source files. Security hardens later via per-path tmpfs.

---

## Image pre-pull

SWE-bench Lite has ~300 instances but only ~60 unique environment images (many
instances share the same repo+version image). Pre-pulling avoids per-trial pull
latency.

Add to `run_curated_experiment.sh`:
```bash
# Pre-pull all unique task images
jq -r '.task.image' "$DATASET_PATH" | sort -u | while read -r img; do
  docker pull "$img" &
done
wait
```

Or defer to `lab preflight` (future work).

---

## What Does NOT Change

| Component | Why |
|-----------|-----|
| Scheduling engine | Doesn't care about image source |
| Variant management | Variants are bindings, not container config |
| Benchmark adapter / grader | Runs inside same container, path contract unchanged |
| Evidence collection | Already collects from trial dirs |
| Trial directory structure | Same paths: in/, out/, state/, workspace/, deps/ |
| `benchmark_score_record_v1` | Schema unchanged |
| Result collection | Same `result.json` path |
| Existing `image_source: global` experiments | Zero changes, default behavior preserved |

---

## Verification

After implementation, run one trial:

```bash
# 1. Freeze agent
bash scripts/agentlab/freeze_agent.sh .lab/agents/rex-current.tar.gz

# 2. Enrich dataset
node scripts/agentlab/enrich_dataset_v2.mjs \
  .lab/experiments/data/swebench_lite_curated.task_boundary_v1.jsonl \
  .lab/experiments/data/swebench_lite_curated.task_boundary_v2.jsonl

# 3. Pull one task image
docker pull swebench/sweb.eval.x86_64.astropy__astropy-12907:latest

# 4. Manual smoke test (no runner)
cid=$(docker create \
  -v /tmp/test-out:/out \
  -w /testbed \
  swebench/sweb.eval.x86_64.astropy__astropy-12907:latest \
  tail -f /dev/null)
docker start $cid
docker cp .lab/agents/rex-current.tar.gz $cid:/tmp/agent.tar.gz
docker exec $cid tar xzf /tmp/agent.tar.gz -C /opt/agent
docker exec $cid /opt/agent/bin/rex --version
docker exec $cid python -c "import astropy; print(astropy.__version__)"  # Should work!
docker rm -f $cid

# 5. Run single trial through runner
AGENTLAB_LIMIT=1 bash scripts/agentlab/run_curated_experiment.sh
```

Step 4 is the critical validation: `python -c "import astropy"` succeeds because
the task image has the full conda environment pre-built.
