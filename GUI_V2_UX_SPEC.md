# AgentLab GUI v2 UX Spec (Harsh Reset)

## 0. Brutal Postmortem (v1)

This is what is wrong and must be fixed:

1. Harness integration is opaque.
- Users cannot tell how their harness is discovered, invoked, and validated.
- Pathing and runtime assumptions are hidden until failure.

2. Choice overload at the wrong time.
- Too many controls before first successful run.
- Users are asked to decide things they do not understand yet.

3. Abstraction mismatch.
- "Knobs" were exposed as raw JSON mechanics, not experiment variables with meaning.
- UI reflects internal implementation details, not researcher intent.

4. No opinionated happy path.
- There is no guided lane from "new harness repo" to "first credible comparison."
- Too easy to end in dead states.

5. Scientific UX is weak.
- Users are not guided into hypothesis-driven design.
- Validity, confounds, and evidence strength are not the central narrative.

v2 exists to fix these failures directly.

## 1. Product Thesis (v2)

AgentLab GUI v2 is a guided experiment cockpit:

1. First run in minutes with minimal choices.
2. Hypothesis-driven experiment design (not raw config editing).
3. Transparent integration contract for any harness.
4. Scientific conclusions before dashboards.

## 2. Primary Personas

1. Harness engineer
- Wants to compare agent strategies quickly without breaking infra.

2. AI researcher
- Wants reliable claims, not vanity metrics.

3. Applied scientist / PM
- Wants a simple "did it improve and can I trust it?" answer.

## 3. Core User Journeys (v2)

1. Connect Harness
- "Can AgentLab run my harness at all?"

2. Define Experiment
- "What variable am I testing and why?"

3. Run Safely
- "Execute in dev or strict mode with clear permissions."

4. Interpret Results
- "What changed, why, and how confident are we?"

5. Decide/Publish
- "Ship, iterate, or reject with audit trail."

## 4. UX Principles

1. Guided by default.
- Progressive disclosure, zero raw JSON for new users.

2. Explain every integration surface.
- Command, paths, container, network, hooks/traces shown with plain-language meaning.

3. Human-first variables.
- "Factors" and "treatments," not "knobs/json pointers."

4. Scientific integrity first.
- Validity gates precede performance claims.

5. One-click fallback to expert mode.
- Raw files and advanced controls remain available, but not default.

## 5. Information Architecture

Only 4 primary views in v2:

1. Setup
2. Experiment
3. Run
4. Results

Everything else is secondary panels.

## 6. Setup View (Guided Wizard)

### Step S1: Repo + Runtime Detection

System auto-detects:
- repo root
- language ecosystem
- likely harness entry commands
- container/runtime availability

User chooses one recommended command from ranked options.

No manual path editing in default mode.

### Step S2: Contract Check (Live Probe)

AgentLab executes a contract probe:
- can invoke harness command
- can write/read `trial_input` and `trial_output`
- validates trial output schema

Output is a pass/fail checklist with concrete fixes.

### Step S3: Safety Profile

Two explicit lanes:

1. Dev Lane
- fast iteration
- network full
- lower guarantees

2. Experiment Lane
- strict posture
- network none or enforced allowlist
- stronger guarantees

### Step S4: Optional Observability Upgrades

System asks only after base contract succeeds:
- enable hooks?
- enable tracing?
- enable proxy-based network evidence?

Each option displays:
- what it adds
- what it cannot prove
- setup effort

### Step S5: Smoke Run

Single-task automatic smoke run.

Result page shows:
- readiness score
- detected limitations
- exact command used

## 7. Experiment View (Replace "Knobs")

v2 replaces "Knobs" with three human concepts:

1. Hypothesis
2. Factors
3. Metrics

### 7.1 Hypothesis Builder

Required fields:
- "We believe changing ___ will improve ___ because ___."
- primary decision metric
- minimum practical effect size

### 7.2 Factor Catalog

Factor categories:
- Prompt strategy
- Model/provider
- Tool policy
- Memory policy
- Planning policy
- Runtime/resources
- Evaluator policy
- Training hyperparams (for trainer workload)

Each factor card includes:
- what it controls
- confound risks
- recommended value range

### 7.3 Variant Builder

User creates variant arms by selecting factors (not editing raw JSON).

Guardrails:
- novice mode caps to 1-3 factors
- warns on confounded combinations
- auto-generates variant names and summaries

### 7.4 Analysis Plan Assistant

Wizard asks:
- paired or unpaired?
- retries policy?
- missingness policy?
- multiple comparisons method?

Then generates valid plan defaults with explanations.

## 8. Run View

### 8.1 Launch Panel

Simple inputs:
- mode (Dev / Experiment)
- budget cap
- concurrency
- start

### 8.2 Live Monitor

Tracks:
- trial progress
- failure classes
- queue/rate-limit pressure
- effective safety profile

### 8.3 Integrity Banner

Always visible:
- integration level
- isolation/comparability status
- blocking warnings

## 9. Results View (Scientific Narrative)

Order of presentation:

1. Can we trust this result?
- grades, confounds, missingness

2. Did the treatment help?
- effect size + CI + practical significance

3. Why did it help/hurt?
- exemplar tasks/trials
- divergence evidence
- behavior deltas

4. What should we do next?
- suggested follow-up experiments with rationale

Avoid raw event-count jargon in default view.
Advanced table remains available.

## 10. Harness Integration Contract UX

v2 must make this explicit:

1. What command is run.
2. Which file paths are exchanged.
3. Which environment variables are injected.
4. Whether this run was local or containerized.
5. Which evidence sources were present.

A dedicated "Integration Contract" panel must show all of the above.

## 11. Proxy + Network UX

Default messaging:

1. "Configured proxy" is not "enforced allowlist."
2. Strict allowlist claims require bypass blocking and self-test pass.
3. Proxy evidence is network-level only (not full causal behavior).

UI must show:
- mode requested vs effective
- enforcement method
- self-test result
- bypass risk

## 12. Minimal v2 Scope (What ships first)

1. Guided setup wizard with live contract probe.
2. Hypothesis/factor/variant builder (no raw JSON default).
3. Run launcher + live monitor.
4. Results page with validity-first narrative.
5. Advanced mode panel for direct file editing.

## 13. Non-Goals (v2)

1. Full custom dashboard builder.
2. Full distributed training orchestration UI.
3. Rich notebook replacement.
4. Generic BI platform features.

## 14. Acceptance Criteria

1. New user can complete setup and smoke run without touching JSON.
2. 90%+ of setup failures return actionable fixes tied to a specific integration surface.
3. Default experiment builder creates a valid paired A/B design in under 3 minutes.
4. Results page answers:
- "Can we trust this?"
- "Did it improve?"
- "Why?"
5. Every GUI-run action is reproducible via displayed CLI command.

## 15. Implementation Plan (v2 UX-first)

### V2-P1 Setup Rebuild
- Replace current onboarding with strict guided wizard and contract probe.

### V2-P2 Experiment Builder Rebuild
- Replace raw knob UX with hypothesis + factors + variant plan.

### V2-P3 Results Reframe
- Validity-first narrative and actionable next experiments.

### V2-P4 Advanced Mode
- Keep expert/raw controls behind explicit toggle.

## 16. Open Questions

1. Should factor definitions be centrally versioned per harness adapter?
2. How aggressively should novice mode restrict multi-factor experiments?
3. What recommendation engine should generate follow-up experiments?
