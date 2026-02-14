# Remove `open()` Function - Tasks

## 1. Remove Rust Implementation
**Validates: Requirements 4.1, 4.2**

- [x] 1.1 Remove `open()` function from `src/python/reader.rs`
  - Delete the `#[pyfunction]` `open()` function (approximately lines 1215-1365)
  - Keep all `AvroReader` implementation unchanged

- [x] 1.2 Remove `open` exports from `src/lib.rs`
  - Remove `open` from `pub use python::{...}` statement
  - Remove `m.add_function(wrap_pyfunction!(open, m)?)?;` from module init

- [x] 1.3 Verify Rust builds successfully
  - Run: `cargo build`
  - Ensure no compilation errors

## 2. Remove Python Wrapper
**Validates: Requirements 4.3, 4.4**

- [x] 2.1 Remove `open()` from `python/jetliner/__init__.py`
  - Remove `open as _open` from imports (line ~74)
  - Remove the `open()` wrapper function (lines ~431-600)
  - Remove `"open"` from `__all__` list

- [x] 2.2 Update module docstring in `__init__.py`
  - Change section "3. **open()** - Iterator for Streaming Control" to "3. **AvroReader** - Iterator for Streaming Control"
  - Update example code to use `AvroReader` instead of `open()`
  - Update "Classes:" section to clarify `AvroReader` is directly instantiated

- [x] 2.3 Verify Python imports work
  - Run: `python -c "import jetliner; print(dir(jetliner))"`
  - Verify `open` is not in the output
  - Verify `AvroReader` is in the output

## 3. Update Test Files
**Validates: Requirements 5.1, 5.2, 5.3, 5.4**

- [x] 3.1 Review and update performance tests
  - `python/tests/performance/test_performance.py`
  - `python/tests/performance/test_memory_efficiency.py`
  - `python/tests/performance/test_streaming.py`
  - `python/tests/performance/test_large_file_stress.py`
  - For each test using `jetliner.open()`:
    - Check if an equivalent test using `AvroReader` already exists
    - If redundant, remove the test
    - If unique, replace `jetliner.open(` with `jetliner.AvroReader(`

- [x] 3.2 Review and update integration tests
  - `python/tests/integration/test_exception_attributes.py`
  - `python/tests/integration/test_edge_cases.py`
  - `python/tests/integration/test_exception_hierarchy.py`
  - `python/tests/integration/test_multi_block.py`
  - For each test using `jetliner.open()`:
    - Check if an equivalent test using `AvroReader` already exists
    - If redundant, remove the test
    - If unique, replace `jetliner.open(` with `jetliner.AvroReader(`

- [x] 3.3 Review and update type tests
  - `python/tests/types/test_recursive_types.py`
  - `python/tests/types/test_non_record_schemas.py`
  - `python/tests/types/test_nested_complex_types.py`
  - For each test using `jetliner.open()`:
    - Check if an equivalent test using `AvroReader` already exists
    - If redundant, remove the test
    - If unique, replace `jetliner.open(` with `jetliner.AvroReader(`

- [x] 3.4 Review and update interop tests
  - `python/tests/interop/test_parametrized.py`
  - Check if the `api == "open"` test case is redundant with other API tests
  - If redundant, remove the test case
  - If unique, update to use `AvroReader` and replace `jetliner.open(` with `jetliner.AvroReader(`

- [x] 3.5 Search for any remaining test usages
  - Run: `rg "jetliner\.open\(" python/tests/`
  - Review and update any files found (remove if redundant, update if unique)

- [x] 3.6 Run full Python test suite
  - Run: `poe test-python`
  - Verify all tests pass

## 4. Update Benchmarks
**Validates: Requirements 7.1, 7.2, 7.3, 7.4, 7.5, 7.6**

- [x] 4.1 Remove functions from `benches/python/compare.py`
  - Delete `read_jetliner_open()` function
  - Delete `read_jetliner_open_s3()` function
  - Remove all `"jetliner_open"`, `"jetliner_open_local"`, `"jetliner_open_s3"` from scenario definitions
  - Remove "open" related columns from output tables
  - Remove "open() overhead" calculations

- [x] 4.2 Update `benches/python/plot.py`
  - Remove `"jetliner_open": "jetliner (open)"` from `READER_LABELS` dict

- [x] 4.3 Verify benchmarks run
  - Run: `python benches/python/compare.py --help`
  - Verify no errors about missing readers

## 5. Update Documentation
**Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5**

- [x] 5.1 Update `README.md`
  - Change "### Streaming with open()" section to "### Streaming with AvroReader"
  - Update all code examples to use `jetliner.AvroReader(` instead of `jetliner.open(`
  - Update comparison table: change `open()` column to `AvroReader`

- [x] 5.2 Update user guide documentation
  - Search for `open(` in `docs/user-guide/*.md`
  - Replace with `AvroReader(` in all examples

- [x] 5.3 Update API reference documentation
  - Search for `open(` in `docs/api/*.md`
  - Remove `open()` function documentation if it exists
  - Ensure `AvroReader` class documentation is complete

- [x] 5.4 Build documentation
  - Run: `mkdocs build`
  - Verify no errors or warnings

- [x] 5.5 Search for remaining documentation references
  - Run: `rg "jetliner\.open\(" docs/`
  - Run: `rg "\.open\(" README.md`
  - Update any remaining references

## 6. Update Scripts
**Validates: Requirements 8.1, 8.2**

- [x] 6.1 Update `scripts/generate_large_avro.py`
  - Replace `jetliner.open(` with `jetliner.AvroReader(`

- [x] 6.2 Search for other script usages
  - Run: `rg "jetliner\.open\(" scripts/`
  - Update any files found

- [x] 6.3 Verify scripts run
  - Run: `python scripts/generate_large_avro.py --help`
  - Verify no import errors

## 7. Final Verification
**Validates: Requirements 9.1, 9.2, 9.3, 9.4**

- [x] 7.1 Search for any remaining references
  - Run: `rg "jetliner\.open\(" --type py`
  - Verify no results (except in NOTES.md which we skip)
  - Run: `rg "from jetliner import.*open" --type py`
  - Verify no results

- [x] 7.2 Run full test suite
  - Run: `poe test-python`
  - Verify all tests pass

- [x] 7.3 Run linting
  - Run: `poe lint-python`
  - Verify no errors

- [x] 7.4 Run type checking
  - Run: `poe typecheck`
  - Verify no errors

- [x] 7.5 Build Rust project
  - Run: `cargo build --release`
  - Verify successful build

- [x] 7.6 Test Python package import
  - Run: `python -c "import jetliner; reader = jetliner.AvroReader('test.avro')"`
  - Verify `AvroReader` is accessible
  - Verify no `open` function exists

## Task Dependencies

```
1.1, 1.2 → 1.3 → 2.1, 2.2 → 2.3 → 3.1, 3.2, 3.3, 3.4, 3.5 → 3.6
                                  → 4.1, 4.2 → 4.3
                                  → 5.1, 5.2, 5.3 → 5.4, 5.5
                                  → 6.1, 6.2 → 6.3

3.6, 4.3, 5.5, 6.3 → 7.1, 7.2, 7.3, 7.4, 7.5, 7.6 → 8.1
```
