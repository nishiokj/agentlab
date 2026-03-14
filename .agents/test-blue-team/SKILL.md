---
name: test-blue-team
description: >
  AST-driven behavioral test writer. Uses the metarepo service to pick ready
  boundaries, wire real dependencies, and write behavioral tests from the
  largest observable surface inward. Invoke with /test-blue-team <target>.
user-invocable: true
---

# Behavioral Test Writer

You write behavioral tests for real boundaries. The red team will attack your most recent additions. Weak tests lose.

For the smoke run, optimize for one well-defended boundary at a time. Do not spread effort thinly across many cheap helpers.

## Core Rules

1. Start from exported or stateful boundaries, not helpers.
2. Use the entity graph first. Do not guess coverage from source alone.
3. Keep the longest real internal call chain practical.
4. Mock only at true system boundaries.
5. Assert observable behavior: return values, errors, persisted effects.
6. If a boundary cannot be wired safely, mark it `blocked`. Do not mock around the blocker.

## Test Placement

- Use Vitest
- New behavioral tests live under `tests/behavioral/<subsystem>/...`
- New behavioral test files use the `.behavior.test.ts` suffix
- Shared behavioral-test setup lives in `tests/_infra/`
- Reusable payloads and fixtures live in `tests/_fixtures/`
- Existing behavioral tests outside `tests/behavioral/` are legacy. Extend them only when the file already owns the exact boundary and splitting would add churn.

Before writing a new file, inspect in this order:
1. `tests/behavioral/<subsystem>/`
2. `tests/_infra/`
3. `tests/_fixtures/`
4. legacy tests that already exercise the same boundary family or resource

## Boundary Selection

When invoked with `/test-blue-team <target>`:

1. Run `boundaries <target>` or `gaps <target>`.
2. Pick the highest fan-in boundary with `readiness=ready`.
3. Skip lower-value helpers while a higher-risk ready boundary exists.
4. If the target names a specific entity, test that entity directly.

Boundary fields:
- `entity_id`
- `fan_in`
- `readiness` = `ready | blocked | unknown`

## Metarepo

Use `./metarepo` as the query and persistence backend. Do not query `entity-graph` directly and do not manage the graph lifecycle yourself.
Do not `curl` the metarepo server directly. Do not call HTTP or RPC endpoints yourself.
The CLI wrapper is the contract. If `./metarepo` cannot do what you need, stop and report the gap instead of bypassing it.

Required env:
- `METAREPO_BASE_URL`

Cold-start bootstrap:

```bash
./metarepo add
./metarepo secrets add --file .env
```

Core queries:

```bash
./metarepo blue latest
./metarepo graph gaps src/orders
./metarepo graph boundaries src/orders
./metarepo graph deps function:src/orders/process.ts:processOrder
./metarepo graph tree function:src/orders/process.ts:processOrder --max-depth 5
./metarepo graph env function:src/orders/process.ts:processOrder
./metarepo graph readiness function:src/orders/process.ts:processOrder
./metarepo graph index src/orders --max-depth 5
```

Important:
- every metarepo workflow rebuilds the graph from the repo filesystem at run start
- if `metarepo` is unavailable, the workflow is `blocked`
- `metarepo` does not write tests for you; it only returns structural context and persists artifacts/bugs/secrets

Required per boundary:
1. `deps`
2. `tree`
3. `env`
4. read the source

From those, determine:
- valid outputs
- invalid outputs
- thrown errors
- side effects through injected dependencies

## Persistence Contract

You are responsible for the actual blue-team work.

Persist by:
- writing behavioral test files under `tests/behavioral/...`
- writing shared fixtures under `tests/_fixtures/` or `tests/_infra/` when needed
- recording a blue handoff artifact with `./metarepo blue record --file payload.json`
- creating a metarepo bug record when you confirm a real product defect or a safe local setup blocker

For each boundary, leave behind:
- the exact test file changes
- a blue handoff artifact naming the defended boundary and changed files
- the exact command a reviewer should run to exercise those tests
- a durable bug only when the boundary is truly blocked or reveals a real defect

Blue handoff payload:

```json
{
  "scope": "src/orders",
  "boundaryId": "function:src/orders/process.ts:processOrder",
  "testFiles": ["tests/behavioral/orders/process.behavior.test.ts"],
  "changedFiles": [
    "tests/behavioral/orders/process.behavior.test.ts",
    "tests/_fixtures/orders.ts"
  ],
  "testCommand": ["bun", "test", "tests/behavioral/orders/process.behavior.test.ts"],
  "summary": "Covers happy path, invalid sku, and duplicate order id",
  "notes": "Uses real local postgres test db",
  "bugIds": []
}
```

Use this syntax to persist a bug:

```bash
./metarepo bug create --title "orders processor requires local postgres" --description "Blocked until db:setup succeeds in disposable test DB"
```

Do not:
- treat `metarepo` query output as the completed task
- invent `blue-team` report artifacts instead of writing tests
- leave a blocker only in chat when it should be a durable bug record
- claim broad coverage when you only defended a tiny helper

## Dependency Policy

Default wiring rules:
- value/config objects: pass real values directly
- internal collaborators you own: use real implementations
- database connections: use a real test database
- filesystem: use temp directories and real IO
- clocks/timers: use fake timers
- third-party integrations: mock or stub at the owned integration boundary
- env vars: set explicitly and restore after the test

Do not:
- mock internal collaborators
- test private functions directly
- replace a real boundary with a helper-only unit test

## Resource Policy

### Test Databases

DB-backed behavioral tests must use a test-scoped database target.

Rules:
- prefer `TEST_DATABASE_URL`
- a disposable local DB is acceptable
- if no test DB exists yet, the default setup path is `bun run db:setup`
- prefer schema-per-file or database-per-suite isolation
- never use production, staging, or ambiguous shared developer data
- never run destructive cleanup against an unclear target
- do not silently fall back from `TEST_DATABASE_URL` to `DATABASE_URL` unless you verified it is disposable and local
- if DB safety or isolation is unclear, mark the boundary `blocked`

When using a real test DB:
- create only test data
- clean up deterministically
- scope cleanup to the test schema or test DB
- use unique schema names or unique IDs when files may run in parallel

### Env Vars And Secrets

Use `env <entity-id>` to enumerate env dependencies.

Rules:
- set only obviously test-safe defaults
- restore modified env in teardown
- never invent or guess credentials
- never print secrets in assertions, snapshots, or logs
- never write guessed credentials into `.env`
- if a boundary needs credentials, tokens, keys, or account IDs that are not already present in a clearly test-safe form, stop and ask the user

Use a secret from `.env` only when all are true:
- it already exists locally
- it belongs to a sandbox, dev, or otherwise test-safe account
- the boundary actually requires real provider behavior
- the test remains isolated and non-destructive

### Third-Party APIs

Default stance:
- do not hit live production third-party APIs from behavioral tests

Preferred order:
1. mock or stub at the actual third-party integration point you own
2. use the provider sandbox when provider behavior itself matters
3. use replay or fake-server tooling only for protocol-level behavior that boundary stubbing cannot cover
4. use a live account only with explicit user direction and only when the operation is safe and reversible

Rules:
- keep the rest of your internal call chain real
- do not mock deeper inside your own business logic
- if only live production credentials exist, mark the boundary `blocked` unless the user explicitly approves a safe path
- never spend money, send real user data, or create externally visible side effects just to get coverage

### Long-Running Processes

You may start local daemons, containers, databases, and background services when needed.

Rules:
- own lifecycle explicitly: start, wait for readiness, stop
- use timeouts and readiness checks
- prefer ephemeral ports and temp directories
- do not leave orphaned processes behind
- if startup requires manual login, interactive auth, or persistent operator intervention, mark the boundary `blocked`

### Non-Idempotent Operations

Allowed:
- disposable local DB writes
- temp directory writes
- sandbox/test-account operations designed for repeated execution
- operations with deterministic cleanup or rollback

Blocked by default:
- charging money
- sending real notifications
- mutating shared external state without cleanup
- operations that cannot be repeated safely
- operations whose effects cannot be observed and cleaned up by the test

If the real behavior cannot be isolated to a disposable environment, mark the boundary `blocked`.

## Blockers

Blue team should solve setup problems itself when the resource is local and safe:
- start local DBs or services
- run migrations
- seed test data
- create temp dirs
- set obvious test-safe env defaults
- install dev dependencies needed for the test

Blue team must ask the user when it needs:
- missing credentials, secrets, or API keys
- access to an external account or provider
- confirmation that a DB or external account is test-safe
- permission for a real non-idempotent external operation
- clarification on unclear business logic

When blocked, say so directly:

```text
BLOCKED: <entity-id>
NEED: <resource or decision>
WHY: <why the boundary cannot be tested safely as-is>
ACTION: <specific thing the user can provide or approve>
```

Do not continue with placeholders, guessed values, or silently downgraded coverage.

## Writing The Test

For each behavioral contract:
1. Arrange: wire deps and seed state
2. Act: make one boundary call
3. Assert: verify a specific output, error, or side effect

Coverage order:
1. happy path
2. error paths
3. edge cases

Bias toward failure modes when risk is high.

After asserting the boundary return value, inspect `tree` for `injected=true` nodes and verify the resulting side effects against the real resource.

Recurse only when all are true:
- the node is `injected=true`
- the node is itself a boundary
- that node has its own side-effecting subtree

## Assertion Standard

Every test must contain at least one data-dependent assertion.

Good assertions fail when the behavior changes.

Required:
- assert exact or semantically specific outputs
- assert error type and error content
- assert side effects by reading the real resource

Forbidden:
- existence-only assertions like `toBeDefined()` or `toBeTruthy()` as the main check
- `expect(fn).not.toThrow()` as the only assertion
- conditional assertions that can silently skip
- `describe.skip` or similar infra gating for missing resources
- identity/reference assertions tied to storage details
- exact key-count or shape trivia
- snapshots as the primary contract
- comments that justify expectations with `current implementation` or similar language

## Isolation

Tests must be independent.

Choose one isolation strategy per DB-backed file:
1. schema-per-file
2. serial execution
3. transaction rollback per test

Prefer schema-per-file when practical.

## Final Check

Before you stop, ask:
- what minimal wrong-value mutation would survive?
- what missing side effect would survive?
- did I stop at a helper while a ready boundary remained?
- does the test specify behavior, or only today’s implementation?

In your handoff, include a boundary ledger:
- ready boundaries tested
- ready boundaries deferred
- blocked boundaries
- reason for each defer/block
