# Remove `open()` Function - Design

## Overview

This design document outlines the approach for removing the redundant `open()` function from Jetliner's Python API, consolidating on direct `AvroReader` instantiation.

## Architecture

### Current State

```
Python API Layer:
  jetliner.open(path, **kwargs)  ──────┐
                                       ├──> AvroReader::new(path, **kwargs)
  jetliner.AvroReader(path, **kwargs) ─┘
```

The `open()` function in `src/python/reader.rs` is a PyO3 `#[pyfunction]` that simply forwards all arguments to `AvroReader::new()`:

```rust
#[pyfunction]
pub fn open(path: PyPathLike, ...) -> PyResult<AvroReader> {
    let path_str = path.into_path();
    AvroReader::new(path_str, ...)
}
```

### Target State

```
Python API Layer:
  jetliner.AvroReader(path, **kwargs) ──> AvroReader::new(path, **kwargs)
```

Direct instantiation with no wrapper function.

## Implementation Strategy

### Phase 1: Remove Rust Function

**File: `src/python/reader.rs`**
- Delete the `open()` function (lines ~1215-1365)
- Keep all `AvroReader` implementation unchanged

**File: `src/lib.rs`**
- Remove `open` from `pub use python::{...}` statement
- Remove `m.add_function(wrap_pyfunction!(open, m)?)?;` from module initialization

### Phase 2: Remove Python Wrapper

**File: `python/jetliner/__init__.py`**
- Remove `open as _open` from imports
- Remove the `open()` wrapper function (lines ~431-600)
- Remove `"open"` from `__all__` list
- Update module docstring to remove references to `open()`

### Phase 3: Update Tests

**Strategy:** Review and update test files, removing redundancy

**Approach:**
1. Identify tests that use `jetliner.open()`
2. Check if equivalent tests already exist using `AvroReader` directly
3. If redundant, remove the test
4. If unique, update to use `jetliner.AvroReader(`

**Pattern:** `jetliner.open(` → `jetliner.AvroReader(` (for non-redundant tests)

**Files to review:**
- `python/tests/performance/test_performance.py`
- `python/tests/performance/test_memory_efficiency.py`
- `python/tests/performance/test_streaming.py`
- `python/tests/performance/test_large_file_stress.py`
- `python/tests/integration/test_exception_attributes.py`
- `python/tests/integration/test_edge_cases.py`
- `python/tests/integration/test_exception_hierarchy.py`
- `python/tests/integration/test_multi_block.py`
- `python/tests/interop/test_parametrized.py`
- `python/tests/types/test_recursive_types.py`
- `python/tests/types/test_non_record_schemas.py`
- `python/tests/types/test_nested_complex_types.py`
- All other test files using `jetliner.open()`

**Verification:** Run `poe test-python` to ensure all tests pass

### Phase 4: Update Benchmarks

**File: `benches/python/compare.py`**

Remove functions:
- `read_jetliner_open()` (lines ~93-110)
- `read_jetliner_open_s3()` (lines ~200-210)

Remove from benchmark scenarios:
- All references to `"jetliner_open"` and `"jetliner_open_local"` and `"jetliner_open_s3"`
- Update scenario definitions to only include `"jetliner_scan"`

Remove from output tables:
- "open (local)" column
- "open (S3)" column
- "open() overhead" calculations

**File: `benches/python/plot.py`**

Remove from `READER_LABELS`:
```python
"jetliner_open": "jetliner (open)",  # DELETE THIS LINE
```

**Verification:** Run benchmarks to ensure they execute without errors

### Phase 5: Update Documentation

**File: `README.md`**

Current section:
```markdown
### Streaming with open()

Use `open()` for fine-grained control...
```

Replace with:
```markdown
### Streaming with AvroReader

Use `AvroReader` for fine-grained control over batch processing:

```python
import jetliner

# Basic iteration
with jetliner.AvroReader("data.avro") as reader:
    for df in reader:
        process(df)
```

Update comparison table:
- Change `open()` column header to `AvroReader`
- Update all examples to use `AvroReader`

**File: `python/jetliner/__init__.py`** (module docstring)

Current:
```python
3. **open()** - Iterator for Streaming Control
   - Returns an AvroReader iterator yielding DataFrame batches
   ...
```

Replace with:
```python
3. **AvroReader** - Iterator for Streaming Control
   - Direct class instantiation for streaming iteration
   - Yields DataFrame batches with full control over processing
   ...

Example usage:
    >>> # Using AvroReader for streaming control with error handling
    >>> with jetliner.AvroReader("data.avro", ignore_errors=True) as reader:
    ...     for df in reader:
    ...         process(df)
```

**File: `docs/user-guide/*.md`**
- Update all code examples
- Replace `jetliner.open(` with `jetliner.AvroReader(`

**File: `docs/api/*.md`**
- Remove `open()` function documentation
- Ensure `AvroReader` class documentation is complete

### Phase 6: Update Scripts

**File: `scripts/generate_large_avro.py`**

Line 177:
```python
# OLD
with jetliner.open(str(output_path)) as reader:

# NEW
with jetliner.AvroReader(str(output_path)) as reader:
```

**Verification:** Run the script to ensure it works

## Testing Strategy

### Unit Tests
- No new unit tests needed (functionality unchanged)
- Existing tests verify `AvroReader` works correctly

### Integration Tests
- Review all tests using `open()` for redundancy
- Remove tests that duplicate existing `AvroReader` tests
- Update unique tests to use `AvroReader`
- Tests verify same behavior as before

### Regression Tests
- Run full test suite: `poe test-python`
- Run benchmarks: `poe bench-python`
- Verify all pass
- Verify test coverage remains the same or improves (no unique coverage lost)

## Rollout Plan

1. Implement changes in order (Phase 1-6)
2. Run full test suite after each phase
3. Verify all tests and benchmarks pass

## Risks and Mitigations

### Risk: Missing test updates
**Mitigation:**
- Comprehensive grep search for all usages
- Run full test suite to catch any missed updates
- Check for import errors

### Risk: Documentation inconsistencies
**Mitigation:**
- Systematic review of all documentation files
- Search for "open(" in all markdown files
- Build documentation to verify no broken links

## Correctness Properties

### Property 1: API Equivalence
**Statement:** For all valid inputs, `AvroReader(path, **kwargs)` produces the same behavior as the old `open(path, **kwargs)`.

**Rationale:** Since `open()` was just a wrapper, removing it cannot change behavior.

**Validation:** All non-redundant tests pass with the updated API.

### Property 2: Complete Removal
**Statement:** No references to `jetliner.open()` remain in the codebase after removal.

**Rationale:** Incomplete removal would cause import errors or confusion.

**Validation:**
```bash
# Should return no results
rg "jetliner\.open\(" --type py
rg "from.*import.*open" python/jetliner/
```

### Property 3: Documentation Consistency
**Statement:** All documentation examples use `AvroReader` and none reference `open()`.

**Rationale:** Inconsistent documentation confuses users.

**Validation:**
```bash
# Should return no results in docs
rg "jetliner\.open\(" docs/
rg "\.open\(" README.md
```

## Implementation Checklist

- [ ] Phase 1: Remove Rust function
  - [ ] Delete `open()` from `src/python/reader.rs`
  - [ ] Remove exports from `src/lib.rs`
  - [ ] Verify Rust builds: `cargo build`

- [ ] Phase 2: Remove Python wrapper
  - [ ] Remove from `__init__.py`
  - [ ] Update module docstring
  - [ ] Verify Python imports: `python -c "import jetliner"`

- [ ] Phase 3: Update tests
  - [ ] Find all test files using `open()`
  - [ ] Replace with `AvroReader()`
  - [ ] Run test suite: `poe test-python`

- [ ] Phase 4: Update benchmarks
  - [ ] Remove functions from `compare.py`
  - [ ] Remove labels from `plot.py`
  - [ ] Run benchmarks: `poe bench-python`

- [ ] Phase 5: Update documentation
  - [ ] Update README.md
  - [ ] Update module docstring
  - [ ] Update user guide
  - [ ] Update API reference
  - [ ] Build docs: `mkdocs build`

- [ ] Phase 6: Update scripts
  - [ ] Update `generate_large_avro.py`
  - [ ] Run script to verify

- [ ] Final verification
  - [ ] Run full test suite
  - [ ] Run benchmarks
  - [ ] Build documentation
  - [ ] Search for remaining references
  - [ ] Update CHANGELOG.md
