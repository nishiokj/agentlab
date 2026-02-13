# Variant Plan Migration

This project uses `variant_plan` in experiment configs.

## Why

`variant_plan` is explicit about intent: this is a planned set of variant entries.

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

- Rust runner requires `variant_plan`. `variants` is not supported.

## CLI/SDK Naming Guidance

- Keep `variant_id` unchanged.
- Keep baseline terminology unchanged.
- Use `variant_plan` for config fields and docs.
- Reserve `SelectionStrategy` for adaptive scheduling extensions.
