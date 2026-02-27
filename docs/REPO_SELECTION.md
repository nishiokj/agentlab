# Repository Selection Guide

## Selection Criteria

### Size Bounds
- Source code: 5K-100K lines of Python
- Test suite: runs in < 5 minutes on a single core
- Dependencies: < 20 direct runtime deps

### Dependency Constraints
- Prefer pure-Python dependencies (no native compilation)
- All deps must be pip-installable with pinned versions
- No dependencies on external services or databases

### Test Runtime Budget
- Full test suite: < 5 minutes
- Individual test files: < 30 seconds
- No flaky tests (must pass 100% on two consecutive runs)

### Disqualifiers
- Requires network access at test time
- Heavy native dependencies (C extensions with complex build)
- Flaky test suite (non-deterministic failures)
- Insufficient test coverage for task creation
- License incompatible with redistribution

## v0 Selected Repositories

### click (Pallets)
- **License**: BSD-3-Clause
- **Rationale**: Command-line interface toolkit. Rich surface for CLI parsing, validation,
  parameter types, help formatting, and shell completion tasks.
- **Size**: ~15K lines
- **Deps**: Pure Python

### rich (Textualize)
- **License**: MIT
- **Rationale**: Terminal formatting library. Diverse surface: markup parsing, color
  handling, table rendering, progress bars, console output, tree display.
- **Size**: ~30K lines
- **Deps**: Pure Python (+ optional pygments)

### jinja2 (Pallets)
- **License**: BSD-3-Clause
- **Rationale**: Template engine. Good surface for parser/lexer tasks, template
  compilation, filter/test implementation, sandboxing, and autoescaping.
- **Size**: ~15K lines
- **Deps**: markupsafe (pure Python wheel)

## Target Distribution (v0, 20 tasks)
- click: ~7 tasks
- rich: ~7 tasks
- jinja2: ~6 tasks

## Evaluation Script

Use `scripts/eval_repo.sh` to evaluate a candidate repository:

```bash
bash scripts/eval_repo.sh --help
bash scripts/eval_repo.sh --repo click --checkout v8.1.7
```
