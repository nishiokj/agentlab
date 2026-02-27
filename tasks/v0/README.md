# v0 Task Suite

This suite contains 20 tasks across 3 repositories (click, rich, jinja2).

## Task Distribution

| Repo | Tasks | IDs |
|------|-------|-----|
| click | ~7 | TASK001-TASK007 |
| rich | ~7 | TASK008-TASK014 |
| jinja2 | ~6 | TASK015-TASK020 |

## Validation

```bash
# Validate entire suite
python -m bench.cli validate-suite v0 --jobs 4

# Validate with determinism check
python -m bench.cli validate-suite v0 --repeat 2 --check-determinism
```

## Requirements per Task

- >= 50 deterministic hidden test cases
- >= 10 mutant patches (all must be killed by hidden suite)
- Baseline (injected) state fails hidden suite
- Reference solution passes hidden suite
- Issue prompt does not leak file paths
