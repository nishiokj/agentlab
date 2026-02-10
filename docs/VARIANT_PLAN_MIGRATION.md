# Variant Plan Migration

This project now prefers `variant_plan` over `variants` in experiment configs.

## Why

- `variant_plan` is explicit about intent: this is a planned set of variant entries.
- `variants` remains valid as a legacy alias.

## Config Migration

Old:

```yaml
baseline:
  variant_id: base
  bindings: {}
variants:
  - variant_id: treatment
    bindings: {}
```

New:

```yaml
baseline:
  variant_id: base
  bindings: {}
variant_plan:
  - variant_id: treatment
    bindings: {}
```

## Runtime Compatibility

- Python runner: prefers `variant_plan`, falls back to `variants`.
- Rust runner: prefers `variant_plan`, falls back to `variants`.
- Run artifact directory remains `variants/` for compatibility.

## CLI/SDK Naming Guidance

- Keep `variant_id` unchanged.
- Keep baseline terminology unchanged.
- Use `variant_plan` for config fields and docs.
- Reserve `SelectionStrategy` for adaptive scheduling extensions.
