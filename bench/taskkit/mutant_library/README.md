# Mutant Pattern Library

## Standard Mutant Strategies

### 1. swallow_error
Catch and silently ignore an exception that should propagate.
```python
# Before (correct)
def validate(x):
    if x < 0:
        raise ValueError("negative")

# Mutant
def validate(x):
    try:
        if x < 0:
            raise ValueError("negative")
    except ValueError:
        pass  # swallowed
```

### 2. default_return
Return a default value instead of computing the real result.
```python
# Mutant: return empty string instead of processing
def format_output(data):
    return ""  # skip all processing
```

### 3. special_case
Hardcode a result for specific test-visible inputs.
```python
# Mutant: special-case a known input
def compute(x):
    if x == 42:
        return 1764  # hardcoded for one case
    return x * x  # wrong for other cases
```

### 4. weaken_validation
Skip or reduce a validation step.
```python
# Mutant: skip length check
def parse(input_str):
    # removed: if len(input_str) > MAX_LEN: raise TooLong
    return _parse_internal(input_str)
```

### 5. incorrect_boundary
Change comparison operators or boundary values.
```python
# Mutant: off-by-one
def in_range(x, lo, hi):
    return lo < x < hi  # should be lo <= x <= hi
```

### 6. skip_step
Remove a necessary processing step from a pipeline.

### 7. wrong_type
Return the wrong type that might pass shallow checks.

### 8. off_by_one
Classic index or loop bound off-by-one errors.

### 9. missing_edge_case
Skip handling of empty, null, or boundary inputs.

### 10. hardcode_value
Return a constant instead of a computed value.
