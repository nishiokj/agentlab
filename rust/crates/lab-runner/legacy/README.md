This directory archives legacy and transitional `lab-runner` source files that no
longer belong on the production compile path.

Layout:
- `legacy/src/*.rs` mirrors the former `src/` locations for explicit legacy
  files such as `core.rs`, `io.rs`, `runner.rs`, `lifecycle.rs`, `types.rs`,
  `validations.rs`, and the old `sink.rs` owner.
- `legacy/src/run/` and `legacy/src/persistence/` preserve the transitional
  module shapes that still existed outside the accepted target ownership model.

These files are retained for audit and comparison only. They are not target
owners and should not be reintroduced through shims, re-exports, or forwarding
modules.
