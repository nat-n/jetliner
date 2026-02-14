# Remove `open()` Function - Requirements

## Overview

Simplify the Jetliner Python API by removing the redundant `open()` function. Users will directly instantiate `AvroReader` instead of calling a wrapper function.

## Background

Currently, Jetliner provides two ways to create an `AvroReader`:
1. `jetliner.open("file.avro")` - A module-level function
2. `jetliner.AvroReader("file.avro")` - Direct class instantiation

The `open()` function is a thin wrapper that simply calls `AvroReader::new()` with the same parameters. This redundancy adds no value and creates API confusion.

## User Stories

### 1. As a library user, I want a clear, simple API
**Acceptance Criteria:**
- 1.1: There is only one way to create an `AvroReader` instance
- 1.2: The API follows Python conventions (direct class instantiation)
- 1.3: All functionality previously available through `open()` is available through `AvroReader`

### 2. As a library maintainer, I want less code to maintain
**Acceptance Criteria:**
- 2.1: The `open()` function is removed from Rust bindings
- 2.2: The `open()` wrapper is removed from Python package
- 2.3: No duplicate code paths for creating readers

### 3. As a developer, I want clear documentation
**Acceptance Criteria:**
- 3.1: Documentation clearly shows the API
- 3.2: Examples demonstrate direct `AvroReader` instantiation

## Functional Requirements

### 4. API Changes
- 4.1: Remove `open()` function from `src/python/reader.rs`
- 4.2: Remove `open()` function from `src/lib.rs` exports
- 4.3: Remove `open()` wrapper from `python/jetliner/__init__.py`
- 4.4: Remove `"open"` from `__all__` in `python/jetliner/__init__.py`
- 4.5: `AvroReader` class remains unchanged and fully functional

### 5. Test Updates
- 5.1: All tests using `jetliner.open()` are updated to use `jetliner.AvroReader()`
- 5.2: Redundant tests that duplicate existing `AvroReader` tests are removed
- 5.3: All tests continue to pass with the same coverage
- 5.4: No unique test functionality is lost

### 6. Documentation Updates
- 6.1: README.md is updated to show `AvroReader` usage
- 6.2: Module docstring in `__init__.py` is updated
- 6.3: All code examples use `AvroReader` instead of `open()`
- 6.4: API reference documentation is updated
- 6.5: User guide documentation is updated

### 7. Benchmark Updates
- 7.1: Remove `read_jetliner_open()` function from `benches/python/compare.py`
- 7.2: Remove `read_jetliner_open_s3()` function from `benches/python/compare.py`
- 7.3: Remove "jetliner_open" from `READER_LABELS` in `benches/python/plot.py`
- 7.4: Remove all "open" related benchmark logic and comparisons
- 7.5: Keep only `scan_avro` benchmarks for Jetliner
- 7.6: Update benchmark output tables to remove "open" columns

### 8. Script Updates
- 8.1: Update `scripts/generate_large_avro.py` to use `AvroReader` if it uses `open()`
- 8.2: Verify all scripts in `scripts/` directory are updated

## Non-Functional Requirements

### 9. Code Quality
- 9.1: No dead code remains after removal
- 9.2: All imports are cleaned up
- 9.3: Type hints remain accurate
- 9.4: Linting passes without warnings

## Out of Scope

- Renaming `AvroReader` class (it keeps its current name)
- Changes to `MultiAvroReader` (it remains as-is)
- Changes to `scan_avro()` or `read_avro()` functions

## Success Metrics

- All tests pass
- Documentation builds without errors
- Benchmarks run successfully
- Code is simpler and easier to understand
- API surface is reduced by one function
