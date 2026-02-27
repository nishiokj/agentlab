# Mutant Patches

## Strategy Documentation

Each mutant patch introduces a specific defect. Document the strategy for each:

| Mutant | Strategy | Description |
|--------|----------|-------------|
| M01 | swallow_error | Catch and ignore the critical exception |
| M02 | default_return | Return a default value instead of computing |
| M03 | special_case | Hardcode result for specific input |
| M04 | weaken_validation | Skip input validation |
| M05 | incorrect_boundary | Off-by-one in loop bounds |
| M06 | skip_step | Remove a necessary processing step |
| M07 | wrong_type | Return wrong type |
| M08 | off_by_one | Index off by one |
| M09 | missing_edge_case | Don't handle empty input |
| M10 | hardcode_value | Return constant instead of computed value |

## Requirements

- At least 10 mutant patches required
- Each must cause at least 1 hidden case to fail
- At least 80% should fail via assertion mismatch (not crash)
