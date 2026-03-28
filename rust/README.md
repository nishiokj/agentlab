# AgentLab Rust Workspace

This workspace contains the Rust implementation for the AgentLab runner, CLI, and analysis stack.

## Docs

- [CLI Reference](./docs/cli.md)

## Quick Start

```bash
cargo build -p lab-cli --release
./target/release/lab-cli --help
```

For command-specific help:

```bash
./target/release/lab-cli views-live --help
./target/release/lab-cli views --help
./target/release/lab-cli query --help
./target/release/lab-cli scoreboard --help
```
