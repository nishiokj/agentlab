# DX Ergonomics: Prioritized Plan

## Current State Assessment

**Isolation is already strong.** Per-trial `docker run --rm` with `--read-only`, `--cap-drop ALL`, `--no-new-privileges`, `--network=none`, resource limits. Each trial is a fresh container. Cross-trial contamination through Docker is structurally prevented. `fail_on_state_leak` catches host-side state issues post-hoc.

**The real gaps are in the data model and onboarding flow:**

- `lab init` emits one blank YAML with `# REQUIRED` scattered everywhere — no guidance on experiment type
- `Variant` is an ephemeral in-memory struct (`{ id, bindings, runtime_overrides }` at `lib.rs:4118`), re-parsed from experiment YAML on every operation. `trial_metadata.json` persists only `ids.variant_id` (the string). The full variant definition — bindings, resolved image, runtime overrides — is not independently persisted or queryable.
- No image provenance — `lab runs` shows run_id/experiment/created_at/variants(count)/pass_rate but zero information about which container ran. Attestation has a placeholder `{"name": "unknown"}`.
- No structured path from code change to experiment image.

---

## Priority 1: Experiment Profiles (`lab init --profile <name>`)

**Leverage: highest | Invasiveness: lowest | Foundation: excellent**

### Why first

- Reduces time-to-first-run from "read the schema, figure out what combination of settings makes sense" to `lab init --profile agent-eval`
- Profile names map directly to existing `ViewSet` analysis types (`ab_test`, `multi_variant`, `parameter_sweep`, `regression`, `core_only`), creating a coherent vocabulary from init -> run -> analysis
- Purely additive — only touches the `Init` command path in `lab-cli/src/main.rs`
- Subsequent features benefit because the profile constrains the experiment shape, which constrains what metadata matters

### Proposed profiles

| Profile | `workload_type` | `comparison` | `design` defaults | `sandbox.mode` | Use case |
|---------|-----------------|--------------|-------------------|----------------|----------|
| `agent-eval` | `agent_runtime` | `paired` | replications: 3, shuffle: true | `container` | Single-variant agent evaluation (pass/fail) |
| `ab-test` | `agent_runtime` | `paired` | replications: 5, shuffle: true | `container` | Baseline vs treatment comparison |
| `sweep` | `agent_runtime` | `independent` | replications: 1, shuffle: false | `container` | Parameter sweep across variant_plan |
| `regression` | `agent_runtime` | `paired` | replications: 3, shuffle: false | `container` | Track pass_rate over time on fixed suite |
| `local-dev` | `agent_runtime` | `paired` | replications: 1, max_concurrency: 1 | `local` | Rapid local iteration, network full |

### Implementation

- Add `--profile` arg to `Commands::Init`
- Each profile is a const `&str` YAML template in `main.rs` (same pattern as today's single template, just five of them)
- `lab init` with no `--profile` lists available profiles with one-line descriptions
- `lab init --profile agent-eval` emits the pre-filled YAML
- Profile name persisted in YAML as `experiment.profile` (informational, not enforced) so downstream tooling can key off it

### What this does NOT do

No "registry" data structure. Profiles are compile-time constants. This is intentional — keeps scope to a CLI change. If user-defined profiles are needed later, a `.lab/profiles/` directory can overlay the builtins.

---

## Priority 2: Variant as First-Class Persisted Object

**Leverage: high | Invasiveness: medium | Foundation: critical — this is the missing data model**

### The problem

Today `Variant` is a private struct with no serialization:

```rust
// lib.rs:4118
struct Variant {
    id: String,
    bindings: Value,
    runtime_overrides: Option<Value>,
}
```

`resolve_variant_plan()` re-parses it from experiment YAML on every `run`, `continue`, `replay`, `fork`, and `describe` call. The resolved variant — what the runner actually used — is never written down. Consequences:

1. **Replay drift**: If experiment YAML changes between a run and a replay, the variant definition silently differs.
2. **Analysis opacity**: `summarize_trial()` receives bindings from the in-memory struct, but analysis queries can't join on variant properties without re-parsing the experiment blob.
3. **Image-per-variant is invisible**: Variants can override the image (`variant_plan[].image`), but trial_metadata only records the variant ID string — not which image it resolved to.
4. **Cross-run comparison is fragile**: "How did variant X perform across runs?" assumes bindings are stable across YAML edits.

### What to persist

At run start, after `resolve_variant_plan()` + `resolve_variant_runtime_profile()`, write a `variants.json` in the run directory:

```json
{
  "schema_version": "variant_manifest_v1",
  "variants": [
    {
      "variant_id": "baseline",
      "is_baseline": true,
      "bindings": { "temperature": 0.7 },
      "runtime_overrides_applied": null,
      "resolved_image": "myagent:latest",
      "effective_network_mode": "none",
      "container_mode": true
    },
    {
      "variant_id": "high-temp",
      "is_baseline": false,
      "bindings": { "temperature": 1.2 },
      "runtime_overrides_applied": { "agent": { "image": "myagent:experimental" } },
      "resolved_image": "myagent:experimental",
      "effective_network_mode": "none",
      "container_mode": true
    }
  ]
}
```

This is the **resolved** variant — what the runner computed, not what the YAML declared.

### Implementation

1. Make `Variant` pub with `Serialize`/`Deserialize`
2. Add a `ResolvedVariant` struct that includes the resolved runtime fields (image, network mode, container mode) — computed from `VariantRuntimeProfile`
3. Write `variants.json` to run directory alongside `manifest.json` and `resolved_experiment.json`
4. `trial_metadata.json` gains a `variant_digest` field (hash of the resolved variant JSON) for integrity
5. `replay`/`fork`/`continue` load from `variants.json` instead of re-parsing experiment YAML — eliminates drift
6. `lab runs` can show variant IDs (not just count)
7. `lab describe` prints the resolved variant table

### What this enables

- Image provenance (Priority 3) attaches naturally to the variant record — `resolved_image` is already there, provenance enriches it with the digest
- Analysis can query variant properties directly from `variants.json` without navigating the experiment blob
- Cross-run variant comparison becomes a join on variant_id + bindings hash
- `lab views` can surface per-variant image info

---

## Priority 3: Image Provenance Capture

**Leverage: medium-high | Invasiveness: medium | Foundation: critical for container re-use visibility**

### Why third (not second)

Image provenance per-variant requires knowing which image each variant resolved to. With Priority 2 in place, each resolved variant already carries `resolved_image`. Provenance enriches that with the runtime-captured digest and OCI labels.

### What to capture

Before `docker run` in `run_builtin_adapter_container()`, call:
```
docker image inspect <image> --format json
```

Extract and store as `image_provenance` on the trial metadata:

```json
{
  "image_provenance": {
    "image_ref": "myagent:latest",
    "digest": "sha256:abc123...",
    "oci_labels": {
      "org.opencontainers.image.revision": "a1b2c3d",
      "org.opencontainers.image.source": "https://github.com/user/repo",
      "org.opencontainers.image.created": "2026-02-20T..."
    },
    "inspected_at": "2026-02-21T..."
  }
}
```

Uses [OCI image spec label conventions](https://github.com/opencontainers/image-spec/blob/main/annotations.md) — standard `LABEL` directives in the user's Dockerfile. No custom convention.

### Also persist per-run

Deduplicate across variants: `run_image_provenance.json` at the run level maps `image_ref -> provenance`. This avoids re-inspecting the same image for every trial.

### Surface in CLI

- `lab runs` gains an `image` column (shows `image_ref@sha256:short`, or multiple if variants use different images)
- `lab describe` prints image provenance when available
- Wire `image_digest` into attestation payload (replacing current `json!({"name": "unknown"})`)

### What this enables

- `lab images` (Priority 4) becomes a query over this data
- Git-branch tracking becomes: "show runs where `oci_labels.revision` matches commits on branch X"
- Container re-use: "these 3 runs used the same digest"

---

## Priority 4: `lab images` — Retrospective Image Catalog

**Leverage: medium | Invasiveness: low (given Priority 3) | Foundation: good**

### Why fourth

Once provenance is captured per-run, this is a query over existing data. No new data structures.

### Implementation

- New `Commands::Images` in lab-cli
- Scans `.lab/runs/*/run_image_provenance.json`
- Deduplicates by digest
- Displays: image_ref, digest (short), git_revision (from OCI label), last_used_at, run_count, best_pass_rate

```
$ lab images
IMAGE               DIGEST       REVISION   LAST USED    RUNS  BEST PASS
myagent:latest      sha256:abc1  a1b2c3d    2h ago       3     0.85
myagent:experiment  sha256:def4  e5f6a7b    1d ago       1     0.72
baseline:v2         sha256:789a  --         5d ago       7     0.91
```

- `lab images --json` for machine-readable output
- `lab images <digest-prefix>` shows all runs that used that image

### No persistent registry file

Catalog is computed from run history. Stateless. If pinned/blessed images are needed later, that's a separate feature referencing digests from this catalog.

---

## Priority 5: Git-Branch Image Convention (defer)

**Leverage: high long-term | Invasiveness: high | Foundation: depends on 2+3**

### Why defer

- Many design decisions: Dockerfile location, tag convention, multi-stage, caching, remote vs local
- User already brings their own image — this adds a second path
- Profiles + Variant persistence + Provenance give 80% of the value at 20% of the complexity

### When it becomes worth it

When you're repeatedly doing:
```sh
git checkout feature/foo
docker build -t myagent:foo-$(git rev-parse --short HEAD) .
# edit experiment.yaml to update image field
lab run ...
```

### Sketch (for future reference)

```yaml
# experiment.yaml (future extension)
runtime:
  agent:
    image:
      source: git
      context: .
      dockerfile: Dockerfile
      branch: feature/foo     # optional: defaults to current branch
      # resolved tag: lab/<experiment-id>:<branch>-<short-sha>
```

- `lab build` reads this, runs `docker build`, tags with convention
- `lab run` auto-builds if `image.source: git` and image doesn't exist locally
- OCI labels set automatically during build -> flows into provenance capture for free

---

## Priority 6: Isolation Tightening (already strong, marginal gains)

**Isolation model is sound.** Per-trial `docker run --rm`, per-trial host directories for writable mounts, `--read-only` root fs, `--cap-drop ALL` + `--no-new-privileges`.

### The "user must ensure internal purity" concern

Real but narrow: within a single trial, the agent could write to `/agentlab/workspace` non-deterministically. The runner can't police intra-trial behavior without becoming a sandbox OS. This is an inherent boundary of the BYOC (bring-your-own-container) model.

### Low-cost options (sprinkle as needed)

| Option | Effort | Effect |
|--------|--------|--------|
| Mount `/agentlab/workspace` as tmpfs | 1 line | Ephemeral RAM workspace, no host-fs side effects |
| `--pids-limit` | 1 line | Prevent fork bombs |
| `--ulimit nofile=1024:1024` | 1 line | Prevent fd exhaustion |
| Checksum workspace before/after trial | Small | Detect unexpected mutations in validity check |

Single-line additions to `run_builtin_adapter_container()`.

---

## Build Order Summary

```
1. Experiment Profiles          <- CLI-only, immediate DX win
2. Variant Persistence          <- Data model fix, eliminates drift, enables clean provenance
3. Image Provenance Capture     <- Enriches variant records with runtime digest/labels
4. `lab images` catalog         <- Query over provenance data
5. Git-branch builds            <- Defer until manual workflow is painful
6. Isolation tightening         <- Sprinkle individual flags as needed
```

**Phase 1** is a single session. ~5 YAML template constants + a CLI argument.

**Phase 2** is focused but important. `Variant` gets `Serialize`/`Deserialize`, new `ResolvedVariant` struct, `variants.json` per run, replay/fork/continue load from it instead of re-parsing YAML.

**Phase 3** composes with Phase 2. One `docker inspect` call per unique image per run, stored alongside variant data.

**Phase 4** falls out of Phase 3 almost for free.

**Phases 5-6** are future work that builds cleanly on 1-4.
