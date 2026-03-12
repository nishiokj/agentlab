---
name: audit
description: >
  Audit a completed session's context window for pathological behavior: thrashing, repeated tool calls, failed calls, non-salient work, and context window inefficiencies.
user-invocable: true
---

# Context Window Audit

Post-mortem analysis of a completed session's context window. Reads `.haiku/sessions/` to find the target session, then incrementally scans every context file (main + work-contexts) looking for behavioral anti-patterns.

## Invocation

```
/audit                       # audit most recent session
/audit <session-dir-name>    # audit a specific session (e.g. tui_1772837780896_snhzlv)
/audit <date>                # audit most recent session on that date (e.g. 2026-03-06)
```

## Methodology

### Phase 0 — Locate Target Session

1. Base path: `<project-root>/.haiku/sessions/`
2. If no argument: list date folders, pick the most recent, then pick the most recent session directory inside it.
3. If argument is a date (YYYY-MM-DD): use that date folder, pick the most recent session.
4. If argument matches a session directory name: locate it directly.
5. Enumerate all context files: `context.md` + everything in `work-contexts/`.

### Phase 1 — Create Scratchpad

Create a temporary scratchpad file at `/tmp/audit-<session-id>.md` to accumulate findings. Structure:

```markdown
# Audit Scratchpad: <session-id>

## Session Metadata
- Created: <timestamp>
- Max tokens: <value>
- Context files: <list>

## Tool Call Ledger
| Tool | Count | Failed | Avg Gap (calls) |
|------|-------|--------|-----------------|

## Findings
### [category] description
- Location: lines X-Y
- Evidence: <brief quote or pattern>
- Severity: low | medium | high | critical
```

### Phase 2 — Incremental Scan

Context files can be 4000+ lines. Read each file in chunks of **500 lines**. For each chunk:

1. **Parse items** — identify each `### <type>` block. Track:
   - Tool call sequences: `(callId, tool name, arguments summary, timestamp)`
   - Tool outputs: `(callId, success/error, output length)`
   - Message content: user/assistant exchanges

2. **Update the scratchpad** — append findings and update the tool call ledger.

3. **Carry forward state** — remember the last few items from the previous chunk to detect cross-boundary patterns (e.g., a repeated call that spans a chunk boundary).

### Phase 3 — Pattern Detection

After scanning all chunks, analyze the accumulated data for these anti-patterns:

#### 1. Thrashing (severity: high)
The agent oscillates between approaches without converging.
- **Signal**: Alternating tool calls to the same files/directories, or repeated Read→Edit→Read cycles on the same file with reverts.
- **Metric**: 3+ back-and-forth oscillations on the same target within 20 calls.

#### 2. Repeated Tool Calls (severity: high)
The same tool is called with identical or near-identical arguments multiple times.
- **Signal**: Same `(tool, arguments-hash)` pair appearing 3+ times.
- **Metric**: Track argument fingerprints. Flag exact duplicates and near-duplicates (same tool + same first argument).

#### 3. Failed Tool Call Storms (severity: critical)
Tool calls that fail repeatedly without the agent changing approach.
- **Signal**: `isError` or error-indicating output followed by a retry of the same or similar call.
- **Metric**: 2+ consecutive failures on the same tool, or 5+ total failures on the same tool across the session.

#### 4. Non-Salient Work (severity: medium)
The agent performs work that doesn't contribute to the stated goal.
- **Signal**: Tool calls to files/directories unrelated to the user's request. Long tangents that don't reference the original task.
- **Metric**: Requires understanding the user's initial message and checking whether tool calls relate to it. Flag sequences of 5+ calls that appear unrelated.

#### 5. Runaway Exploration (severity: medium)
Excessive Glob/Grep/Read calls without producing any edits or conclusions.
- **Signal**: Long sequences of read-only tool calls (Glob, Grep, Read) with no Write/Edit/Bash following.
- **Metric**: 15+ consecutive read-only calls with no action taken.

#### 6. Context Window Bloat (severity: medium)
Loading excessive file content that isn't subsequently referenced.
- **Signal**: `file_content` items whose paths never appear in later function_call arguments.
- **Metric**: Files loaded but never referenced in any subsequent tool call or assistant message.

#### 7. Abandoned Branches (severity: low)
The agent starts a line of investigation, abandons it without explanation, and pivots.
- **Signal**: A sequence of related tool calls followed by an abrupt topic/file switch with no assistant message explaining why.
- **Metric**: 3+ related calls followed by an unrelated call with no intervening assistant message.

#### 8. Excessive Self-Correction (severity: medium)
The agent writes code, then immediately edits it multiple times.
- **Signal**: Edit→Edit→Edit on the same file within a short span.
- **Metric**: 3+ sequential edits to the same file without any other tool call in between.

### Phase 4 — Report

After completing the scan, produce a structured report. Write it to `/tmp/audit-<session-id>-report.md` and also output a summary to the conversation.

```markdown
# Context Window Audit Report

## Session
- ID: <session-id>
- Date: <date>
- Duration: <first-ts to last-ts>
- Total tool calls: <N>
- Failed tool calls: <N> (<percentage>)
- Unique tools used: <list>

## Severity Summary
- Critical: <N>
- High: <N>
- Medium: <N>
- Low: <N>

## Findings (ordered by severity)

### [CRITICAL] <title>
**Lines**: X-Y in <context-file>
**Pattern**: <description>
**Evidence**: <concrete examples from the context>
**Impact**: <what this cost in terms of tokens/time/correctness>

...

## Tool Usage Breakdown
| Tool | Calls | Failed | Repeated | Notes |
|------|-------|--------|----------|-------|

## Recommendations
- <actionable suggestion based on findings>
```

## Rules of Engagement

1. **Read incrementally** — never try to read an entire large context file at once. Use 500-line chunks.
2. **Scratchpad is mandatory** — always create and maintain the scratchpad. It's your working memory across chunks.
3. **Be specific** — every finding must reference concrete line numbers, tool names, and argument snippets. No vague hand-waving.
4. **Severity must be justified** — don't cry wolf. Critical means the agent wasted significant resources or produced incorrect output. Low means a minor inefficiency.
5. **Count, don't guess** — tool call counts, failure rates, and repetition counts must be computed from the actual data, not estimated.
6. **Preserve context for the user** — when quoting evidence, include enough surrounding context that the finding is understandable without re-reading the full context window.
7. **Skip `file_content` bodies** — when scanning, note that file_content items loaded are present (record path, id, approximate size) but don't analyze their content line-by-line. Focus on the *behavioral* items: messages, function_calls, and function_call_outputs.
