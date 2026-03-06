# V1 Task Catalog (TASK021–TASK050)

> Status: Backlog and task content reference. Execution tracking lives in `docs/V1_BENCHMARK_MIGRATION_PLAN.md`.


30 new tasks. V1 suite = these 30 + 20 existing v0 tasks = 50 total.

All tasks use synthetic codebases (Python 3.10+ stdlib only). No external repos.

## Distribution

| Type | Count | Task IDs |
|---|---|---|
| `bugfix` | 13 | 022, 027, 029, 031, 033, 035, 038, 039, 041, 044, 047, 048, 050 |
| `feature` | 6 | 021, 028, 032, 036, 042, 046 |
| `refactor` | 4 | 023, 030, 037, 049 |
| `greenfield` | 3 | 026, 034, 040 |
| `agentic_search` | 2 | 024, 045 |
| `code_review` | 2 | 025, 043 |

`multi_file_debug` has been merged into `bugfix` via the `deep_diagnosis` profile.

## Difficulty Distribution

| Difficulty | Count |
|---|---|
| medium | 7 |
| hard | 23 |

---

## Type And Metric Policy

- `bugfix` now includes deep-diagnosis tasks (formerly `multi_file_debug`) via `task_profile.bugfix_profile=deep_diagnosis`.
- `feature` remains a single first-class type for repo state transitions (no separate delta-feature type).
- Performance is an optional secondary metric (`performance_continuous_v1`) for eligible tasks (`bugfix`, `feature`, `refactor`), reported only for correctness-passing solutions.
- Performance metrics are continuous (throughput, p95 latency, memory, scaling), not a standalone pass/fail replacement.

---

## Task Specifications

### TASK021 — `feature` / medium

**Codebase:** CLI argument parser library (4 modules: lexer, parser, registry, formatter).

**Feature to implement:** Add `--format json` output mode. Currently the library only supports `--format text` (human-readable table output). The agent must add JSON serialization to the formatter module, wire it through the registry, and handle the new format flag in the parser.

**Injection:** Remove the JSON formatter class and its registration. Stub the format dispatch to raise NotImplementedError for "json".

**Key test scenarios:** JSON output for simple args, nested subcommands, mixed types, empty args, error formatting in JSON mode.

**Mutant strategies:** partial_implementation (only format simple types), wrong_interface (wrong JSON structure), missing_edge_case (no nested support), default_return, skip_step, hardcode_value, wrong_type, swallow_error, off_by_one, special_case.

---

### TASK022 — `bugfix` / hard (`deep_diagnosis`)

**Codebase:** HTTP request builder library (5 modules: url_encoder, query_builder, header_manager, request_dispatcher, response_parser).

**Bug:** url_encoder incorrectly handles percent-encoding of reserved characters in query parameters. The symptom appears in request_dispatcher as malformed requests that the response_parser can't decode. The agent sees "response parsing failed" errors but the root cause is in url_encoder (3 hops away).

**Injection:** Break the percent-encoding logic in url_encoder (encode `+` as `%20` instead of `%2B`, mishandle `&` in values).

**Key test scenarios:** URLs with special chars, query params with reserved chars, nested encoding, empty params, unicode in values, already-encoded values.

**Issue.md angle:** "Response parser throws DecodeError on certain query patterns" — no mention of URL encoding.

---

### TASK023 — `refactor` / hard

**Codebase:** Data pipeline library (4 modules: source, transforms, filters, sink). Three handler classes (`CSVHandler`, `JSONHandler`, `XMLHandler`) each duplicate 80% of the same transform-and-filter logic.

**Refactoring:** Extract shared transform/filter logic into a base `PipelineHandler` class. Each format handler only implements `parse()` and `serialize()`. The pipeline orchestration is in the base class.

**Injection:** Inline the base class logic into each handler, creating the duplicated version.

**Key test scenarios:** All three formats with same data, filter combinations, empty input, large datasets, chained transforms. Tests validate behavior (same output), not structure.

**Mutant strategies:** revert_refactor, partial_refactor, wrong_abstraction, missing_edge_case, skip_step, default_return, wrong_type, swallow_error, off_by_one, special_case.

---

### TASK024 — `agentic_search` / medium

**Codebase:** Configuration management library (15 files across 4 packages: loaders, validators, mergers, resolvers). A specific config key (`retry.backoff_multiplier`) causes a validation failure when its value is a float between 0 and 1, but only when the config is loaded from YAML format and merged with defaults.

**Task:** Find which config key, in which loader, triggers the validation failure, and create `findings.json` documenting the root cause.

**Injection:** Remove `findings.json`.

**Key test scenarios:** Validate findings.json structure, check that correct key is identified, check that correct loader is identified, check that the interaction (YAML + merge + validation) is described.

**Red herrings:** Other validators that check similar numeric ranges, a different loader that handles the same key correctly.

---

### TASK025 — `code_review` / hard

**Codebase:** Caching layer library (4 modules: cache_store, eviction, serializer, cache_decorator). A PR diff introduces 3 planted issues.

**Planted issues:**
1. Race condition: `get()` and `set()` don't hold the lock atomically during check-and-update
2. Memory leak: evicted entries are removed from the index but not from the backing store
3. Type confusion: serializer silently coerces `None` to `"null"` string on read-back

**Task:** Create `review.json` with structured findings: `[{"issue": str, "severity": str, "location": str, "explanation": str}]`.

**Injection:** Remove `review.json`.

**Key test scenarios:** Check 3 issues identified, severity ratings reasonable, explanations reference the actual problem, no false positives for non-issues.

---

### TASK026 — `greenfield` / medium

**Codebase:** Markdown table formatter. Agent builds from scratch given a spec and public test suite.

**Spec (in issue.md):** Parse a list of dicts into a formatted Markdown table with:
- Auto-column-width based on content
- Left/right/center alignment per column
- Header separator row
- Handle missing keys (empty cell)
- Handle multiline cell content (escape newlines)

**Public tests:** 5 basic tests covering simple table, alignment, missing keys.

**Hidden tests:** 50+ cases including empty input, single row, single column, unicode, very long values, all alignment combos, special characters in cells.

**Injection:** Delete all src/ implementation files, leave only README and tests.

---

### TASK027 — `bugfix` / hard

**Codebase:** Expression evaluator library (4 modules: tokenizer, parser, evaluator, builtins). Supports arithmetic with operator precedence, parentheses, and built-in functions (abs, min, max).

**Bug:** Operator precedence is wrong for nested parentheses combined with unary minus. `(-3 + 4) * 2` evaluates to `14` instead of `2` because the unary minus binds too loosely after an open paren.

**Injection:** Break the precedence handling in the parser's `parse_unary()` method so unary minus after `(` doesn't bind correctly.

**Key test scenarios:** Unary minus in various positions, nested parens, combined with multiplication/division, double negation, unary minus with function calls.

---

### TASK028 — `feature` / hard

**Codebase:** File watcher library (4 modules: scanner, differ, event_emitter, watcher). Currently watches a single directory non-recursively.

**Feature:** Add recursive directory watching. The scanner must traverse subdirectories, the differ must track files across directory levels, the event emitter must include relative paths from the watch root, and the watcher must handle new subdirectories created after watching starts.

**Injection:** Remove recursive scanning logic, revert to flat directory listing.

**Key test scenarios:** Nested dirs 3 levels deep, new subdir creation, file changes in subdirs, symlinks (should not follow), empty subdirs, rapid changes across levels.

---

### TASK029 — `bugfix` / hard (`deep_diagnosis`)

**Codebase:** Template engine (5 modules: lexer, parser, ast_nodes, resolver, renderer). Supports variable interpolation, conditionals, and loops.

**Bug:** Variable resolution fails for variables defined in a parent loop scope when accessed inside a nested conditional. The resolver's scope chain lookup skips the loop scope when inside an `{% if %}` block. The symptom is "undefined variable" errors in the renderer, but the root cause is in the resolver's scope chain construction (called from the parser, manifesting in the renderer — 4 module chain).

**Injection:** Break the scope chain push in the resolver when entering conditional blocks inside loops.

**Issue.md angle:** "Template rendering fails with 'undefined variable' for variables that are clearly defined in the template" — no mention of scope, resolver, or conditionals-in-loops.

---

### TASK030 — `refactor` / hard

**Codebase:** State machine library (3 modules: machine, states, transitions). Currently a god-class `StateMachine` with a giant `handle_event()` method containing a switch statement for every state.

**Refactoring:** Extract into strategy pattern: each state becomes a `State` subclass with its own `handle()` method. The `StateMachine` delegates to the current state object. Transition rules move from the switch statement to state-specific transition tables.

**Injection:** Inline all state handlers back into the god-class switch statement.

**Key test scenarios:** All state transitions, invalid transitions, re-entry to same state, full lifecycle traversal, error states.

---

### TASK031 — `bugfix` / hard

**Codebase:** JSON Schema validator library (4 modules: parser, resolver, validators, reporter). Supports `$ref` resolution, `allOf`/`anyOf`/`oneOf`, and nested schemas.

**Bug:** Recursive `$ref` resolution has an off-by-one in the depth counter, causing it to stop resolving one level too early. Schemas with `$ref` chains of depth 3+ silently pass validation instead of applying the referenced constraints.

**Injection:** Decrement the depth counter before the recursive call instead of after.

**Key test scenarios:** $ref depth 1-5, circular $ref detection, $ref combined with allOf, nested $ref inside array items, $ref to $ref.

---

### TASK032 — `feature` / hard

**Codebase:** Task queue library (4 modules: queue, scheduler, worker, task_registry). Currently FIFO only.

**Feature:** Add priority scheduling with preemption. Tasks have priority levels (0=low, 9=critical). Higher priority tasks preempt lower ones. Running tasks can be paused and resumed. Priority inversion must be handled (if high-priority task depends on low-priority task's result, boost the low-priority task).

**Injection:** Remove priority sorting, preemption hooks, and inversion detection.

**Key test scenarios:** Basic priority ordering, preemption mid-execution, priority inversion scenario, equal-priority FIFO fallback, dynamic priority changes, empty queue edge cases.

---

### TASK033 — `bugfix` / hard (`deep_diagnosis`)

**Codebase:** ORM layer (5 modules: model, fields, query_builder, join_resolver, executor). Supports defining models with foreign key relationships and generating SQL queries.

**Bug:** The join_resolver generates `LEFT JOIN` instead of `INNER JOIN` for required foreign keys, but only when the query has more than one join. Single-join queries work correctly. The executor runs the wrong SQL, producing extra null rows in results.

**Injection:** Break the join type selection logic in join_resolver for multi-join queries.

**Issue.md angle:** "Query results contain unexpected null rows when filtering across related models" — no mention of JOIN types or the join resolver.

---

### TASK034 — `greenfield` / medium

**Codebase:** Cron expression parser. Agent builds from scratch.

**Spec:** Parse standard 5-field cron expressions (`minute hour day-of-month month day-of-week`) and compute the next N occurrence times given a reference datetime.

Support: `*`, ranges (`1-5`), steps (`*/15`), lists (`1,3,5`), and combinations (`1-5/2`).

**Public tests:** 5 tests covering simple expressions (`* * * * *`, `0 12 * * *`, `*/15 * * * *`).

**Hidden tests:** 50+ cases including all field types, combined expressions, month/day boundary crossings, leap year handling, far-future computation.

**Injection:** Delete all src/ files.

---

### TASK035 — `bugfix` / hard

**Codebase:** Graph traversal library (4 modules: graph, traversal, cycle_detector, path_finder). Supports directed and undirected graphs with DFS, BFS, topological sort, and cycle detection.

**Bug:** Cycle detection fails on diamond patterns in directed graphs. When node A has edges to B and C, and both B and C have edges to D, the detector incorrectly reports a cycle. The DFS "visited" set doesn't distinguish between "currently in recursion stack" and "already fully explored."

**Injection:** Replace the two-set tracking (visited + recursion_stack) with a single visited set.

**Key test scenarios:** Diamond DAG (no cycle), actual cycle, self-loop, diamond with added back-edge (real cycle), deeply nested diamonds, disconnected components with and without cycles.

---

### TASK036 — `feature` / medium

**Codebase:** Logging framework (3 modules: logger, formatters, handlers). Currently supports plain text output to stdout.

**Feature:** Add structured JSON log formatter. Each log entry outputs as a single JSON line with: timestamp (ISO 8601), level, message, logger name, and arbitrary extra fields passed via `log.info("msg", extra={"key": "val"})`. Must handle non-serializable extras gracefully (repr fallback).

**Injection:** Remove the JSON formatter class and its registration.

**Key test scenarios:** Basic JSON output, extra fields, nested extras, non-serializable extras, all log levels, unicode messages, multiline messages, empty extras.

---

### TASK037 — `refactor` / hard

**Codebase:** Event system (4 modules: bus, handlers, events, dispatcher). Currently uses string-based event names with `bus.emit("user.created", data)`. Handlers are registered by string matching with wildcards.

**Refactoring:** Convert to typed event bus. Events are dataclass instances. Handlers are registered by event class. Wildcard matching becomes class hierarchy matching (handler for `UserEvent` catches `UserCreatedEvent(UserEvent)`).

**Injection:** Revert typed events to string-based dispatch.

**Key test scenarios:** Direct event match, hierarchical matching, multiple handlers, handler ordering, no matching handler, event with data, async-style handlers.

---

### TASK038 — `bugfix` / hard

**Codebase:** Diff algorithm library (3 modules: diff_core, sequence_matcher, output_formatter). Implements Myers diff for computing edit scripts between sequences.

**Bug:** The diff produces incorrect edit operations when the input sequences contain repeated elements. Specifically, when a line appears multiple times, the LCS computation picks the wrong anchor points, producing edits that are valid but not minimal (extra delete+insert pairs where a "keep" would suffice). For certain repeated patterns, it even produces incorrect diffs (applying the edit script doesn't produce the target).

**Injection:** Break the LCS backtracking logic when encountering duplicate elements in the edit graph.

**Key test scenarios:** Sequences with no repeats (baseline), sequences with adjacent repeats, sequences where repeats span the diff boundary, all-same-element sequences, interleaved repeats.

---

### TASK039 — `bugfix` / hard (`deep_diagnosis`)

**Codebase:** Plugin system (5 modules: loader, registry, config_parser, initializer, plugin_base). Plugins are Python modules discovered by the loader, registered in the registry with parsed config, and initialized.

**Bug:** The config_parser returns config dicts with references to a shared mutable default dict. When the initializer modifies config for plugin A, it silently mutates the config for plugin B (registered later but sharing the same default dict object). The symptom is "plugin B has wrong configuration" but the cause is in config_parser's default handling.

**Injection:** Use a mutable default dict in config_parser's merge function instead of creating a fresh copy.

**Issue.md angle:** "Second plugin loads with incorrect configuration values that don't match its config file" — no mention of shared state, defaults, or config_parser.

---

### TASK040 — `greenfield` / hard

**Codebase:** Semver constraint resolver. Agent builds from scratch.

**Spec:** Parse semantic version strings (`1.2.3`, `1.2.3-beta.1`, `1.2.3+build.456`) and version constraint expressions. Resolve a set of constraints to determine if a version satisfies all of them.

Constraint syntax: `^1.2.3` (compatible), `~1.2.3` (patch-level), `>=1.0.0 <2.0.0` (range), `1.2.x` (wildcard), `1.2.3 || 2.0.0` (union).

**Public tests:** 5 tests for basic parsing and simple constraint matching.

**Hidden tests:** 60+ cases covering all constraint types, pre-release ordering, build metadata (ignored for precedence), complex compound constraints, edge cases (0.x versions, pre-release vs release ordering).

**Injection:** Delete all src/ files.

---

### TASK041 — `bugfix` / hard

**Codebase:** Rate limiter library (3 modules: limiter, window, store). Implements sliding window rate limiting with configurable window size and request limits.

**Bug:** The sliding window calculation wraps incorrectly at the window boundary. When the current timestamp modulo the window size is near zero, the window count includes requests from the PREVIOUS window period. This causes intermittent rate limit violations for legitimate requests near window boundaries.

**Injection:** Break the window boundary calculation (use `//` integer division where `%` modulo is needed, or vice versa).

**Key test scenarios:** Requests at window start, requests at window end, requests spanning boundary, rapid requests within window, requests exactly at boundary, large time gaps between requests, concurrent window checks.

---

### TASK042 — `feature` / hard

**Codebase:** Serialization library (4 modules: registry, serializers, deserializers, type_coercion). Currently supports JSON output only.

**Feature:** Add YAML-style output with custom tag support. Tags allow marking values with type hints (e.g., `!!timestamp 2024-01-01` → `datetime` object). The serializer must handle: tagged scalars, tagged sequences, tagged mappings, nested tagged values, and custom user-defined tags registered via the registry.

**Injection:** Remove YAML serializer, tag registry integration, and tagged value handling.

**Key test scenarios:** Basic YAML output, tagged scalars, tagged sequences, nested tags, custom tags, round-trip (serialize → deserialize preserves types), invalid tag handling, unregistered tag behavior.

---

### TASK043 — `code_review` / hard

**Codebase:** Authentication middleware (4 modules: auth_handler, session_store, token_validator, permission_checker). A PR diff introduces the middleware.

**Planted issues:**
1. TOCTOU race: `token_validator` checks token expiry, then `session_store` reads the session — token could expire between check and use
2. Missing input sanitization: `auth_handler` passes user-supplied `redirect_url` to response headers without validating it's a relative path (open redirect)
3. Timing side-channel: `permission_checker` uses string equality (`==`) to compare permission tokens, which is vulnerable to timing attacks (should use `hmac.compare_digest`)

**Task:** Create `review.json` listing the issues.

**Injection:** Remove `review.json`.

---

### TASK044 — `bugfix` / hard

**Codebase:** AST transformer library (4 modules: parser, ast_nodes, visitor, transformer). Implements the visitor pattern for transforming abstract syntax trees.

**Bug:** The visitor's `visit()` method uses `isinstance` dispatch but doesn't handle nodes that are subclasses of other node types correctly. When a `ForLoopNode(LoopNode(StatementNode))` is visited, the visitor matches `StatementNode` handler instead of `ForLoopNode` handler because it iterates the MRO in the wrong order. This causes nested `ForLoopNode`s inside `IfNode`s to be skipped.

**Injection:** Reverse the MRO iteration order in the visitor dispatch.

**Key test scenarios:** Flat AST, nested same-type nodes, mixed node types, deeply nested (4+ levels), subclass dispatch correctness, transform that adds nodes, transform that removes nodes.

---

### TASK045 — `agentic_search` / medium

**Codebase:** Microservice framework (20 files across 5 packages: handlers, middleware, connections, config, utils). The service handles HTTP-like requests with a connection pool.

**Task:** Find which request handler leaks database connections under error conditions. Create `findings.json` documenting: which handler, which error path, why the connection isn't returned to the pool, and which middleware should have caught it.

**Injection:** Remove `findings.json`.

**Clues scattered across:** A handler that catches exceptions but doesn't call `conn.release()` in the except branch. A middleware that's supposed to wrap handlers in try/finally for connection cleanup but has a config flag that disables it for certain routes. A connection pool that logs warnings at threshold but doesn't fail.

**Red herrings:** Other handlers that correctly release connections. A different pool implementation that has its own cleanup. A retry middleware that re-acquires connections (correctly).

---

### TASK046 — `feature` / medium

**Codebase:** CLI framework library (4 modules: parser, commands, registry, help_formatter). Supports defining commands with flags and positional args.

**Feature:** Add subcommand aliasing with conflict detection. Users can define aliases (`git co` → `git checkout`). The registry must detect and reject conflicting aliases (two commands aliased to the same name). The help formatter must show aliases. Aliases must work with tab-completion candidates.

**Injection:** Remove alias registration, conflict detection, help integration, and completion support.

**Key test scenarios:** Basic alias, multiple aliases for one command, conflicting alias (error), alias of alias (error or chain?—spec says error), help output includes aliases, completion includes aliases, removing an alias.

---

### TASK047 — `bugfix` / hard (`deep_diagnosis`)

**Codebase:** Mini compiler pipeline (5 modules: lexer, parser, symbol_table, type_checker, code_generator). Compiles a simple expression language to stack-based bytecode.

**Bug:** The parser builds the symbol table correctly during parsing, but the type_checker reads from a stale copy of the symbol table when checking function call expressions. The parser updates the table after parsing each function definition, but the type_checker received a snapshot from before the second-pass function resolution. Functions defined after their first call site appear as "undefined" to the type checker even though the parser accepted them.

**Injection:** Make the type_checker receive a copy of the symbol table instead of a reference, so it doesn't see late-bound entries.

**Issue.md angle:** "Type checker reports 'undefined function' for functions that are clearly defined and parse correctly" — no mention of symbol table copies or binding order.

---

### TASK048 — `bugfix` / hard

**Codebase:** LRU cache library (3 modules: cache, linked_list, hasher). Implements a thread-safe LRU cache using a doubly-linked list + hash map.

**Bug:** Under concurrent access patterns, the eviction logic and the get logic can race. When a `get()` promotes an entry to the head of the LRU list while `put()` is evicting the tail, the linked list's prev/next pointers can become inconsistent if both operations modify adjacent nodes. This doesn't crash immediately but causes the LRU order to be wrong, leading to premature eviction of recently-accessed entries.

**Injection:** Remove the lock acquisition around the linked list pointer updates in `get()`'s promote-to-head logic.

**Key test scenarios:** Sequential access (baseline), interleaved get/put, access pattern that triggers eviction during promotion, capacity-1 cache, repeated access to same key during eviction.

Note: Since we need determinism, model concurrency via explicit interleaving sequences in test cases rather than actual threads.

---

### TASK049 — `refactor` / hard

**Codebase:** Request handler (4 modules: handler, middleware, validators, responders). A single monolithic `handle_request()` function that's 200+ lines with nested conditionals for auth, validation, rate limiting, logging, and response formatting.

**Refactoring:** Extract into middleware chain pattern. Each concern (auth, validation, rate limit, logging, response format) becomes a middleware class with a `process(request, next)` method. The handler composes them into a pipeline.

**Injection:** Inline all middleware back into the monolithic handler.

**Key test scenarios:** Full request lifecycle, auth failure (short-circuit), validation failure, rate limit hit, successful request, middleware ordering matters (auth before validation), custom middleware insertion.

---

### TASK050 — `bugfix` / hard

**Codebase:** Path resolver library (3 modules: resolver, normalizer, fs_adapter). Resolves file paths with support for symlinks, `..`, `.`, and `~` expansion.

**Bug:** Symlink resolution enters an infinite loop on circular symlinks (`a → b → c → a`). The resolver tracks visited paths to detect cycles, but it normalizes paths before adding them to the visited set. When a symlink chain includes `..` segments, the normalized path differs from the raw path, so the cycle detection misses it.

**Injection:** Apply normalization before the cycle check insertion point instead of after.

**Key test scenarios:** No symlinks (baseline), single symlink, chain of 3 symlinks, circular symlink (2-cycle), circular with `..` in path (the actual bug), deeply nested non-circular symlinks, symlink to self, symlink to parent directory.
