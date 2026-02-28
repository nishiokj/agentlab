# Benchmark Adapter Layout

Canonical external benchmark adapters now live under top-level `adapters/`:

- `adapters/swebench/`
- `adapters/harbor/`

The in-house benchmark bridge to AgentLab is separate and lives at:

- `bench/integration/agentlab/`

If you are adding or modifying Harbor/SWE-bench adapter logic, edit
`adapters/*` first.
