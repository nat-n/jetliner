# Implementation Plan: Polars API Alignment

## Overview

This plan implements Polars API alignment for Jetliner, making the library feel native to Polars users.

**Python is the primary use case**, with core reading logic in Rust for performance:
- Python `scan_avro()` uses `register_io_source` with Rust batch reading under the hood
- Rust-native `scan_avro()` returning `LazyFrame` is out of scope (due to PyLazyFrame deserialization issues)

Key changes from current API:
- `scan()` → `scan_avro()` (Python only, using `register_io_source`)
- ✅ `parse_avro_schema()` → `read_avro_schema()`
- `strict` → `ignore_errors` (inverted semantics)
- `buffer_blocks`, `buffer_bytes`, etc. → flattened top-level kwargs (not nested dict)
- `retries` → `storage_options["max_retries"]`
- `endpoint_url` → `endpoint` (aligned with Polars)
- Added `max_block_size` for decompression bomb protection

## Testing Philosophy

Each implementation task includes its own test coverage. Unit tests should be written alongside implementation, not deferred to later phases. More advanced tests such as integration may be deferred to a later task. This ensures:
- Immediate validation of functionality
- Faster feedback loops
- No accumulation of untested code

## Tasks

- [x] 1. Rust API Foundation
  - [x] 1.1 Create `AvroOptions` struct with tests
    - Define struct with `buffer_blocks`, `buffer_bytes`, `read_chunk_size`, `batch_size`, `max_decompressed_block_size`
    - Use `usize` for all size/count fields
    - Implement `Default` trait
    - Write unit tests for default values and builder pattern
    - Location: `src/api/options.rs`
    - _Requirements: 9.2, 9.3, 9.4, 9.5_

  - [x] 1.2 Update `CloudOptions` struct with tests
    - Add `max_retries: usize` field (default: 2)
    - Use `endpoint` key (not `endpoint_url`)
    - Implement `from_dict()` to parse from HashMap
    - Write unit tests for `from_dict()` with various key combinations
    - Location: `src/api/cloud.rs`
    - _Requirements: 8.2, 8.3, 8.4_

  - [x] 1.3 Create `ScanArgsAvro` struct with tests
    - Include scan configuration fields (n_rows, row_index, glob, include_file_paths, ignore_errors)
    - Use Polars types: `PlSmallStr`, `RowIndex` from `polars::prelude`
    - Implement `Default` trait and builder methods
    - Write unit tests for defaults and builder pattern
    - Location: `src/api/args.rs`
    - _Requirements: 13.1, 13.7_

  - [x] 1.4 Implement `From<ReaderError> for PolarsError` with tests
    - Map `ReaderError::Schema` → `PolarsError::SchemaMismatch`
    - Map `ReaderError::Source(NotFound)` → `PolarsError::IO` with `NotFound` kind
    - Map other `ReaderError::Source` → `PolarsError::IO`
    - Map other errors → `PolarsError::ComputeError`
    - Write unit tests for each error mapping
    - Location: `src/error.rs`
    - _Requirements: 14.2, 14.3, 14.4_

  - [x] 1.5 Implement column resolution with tests
    - Create `ColumnSelection` enum (Names/Indices variants)
    - Implement `resolve_columns()` function
    - Write unit tests for: valid names, valid indices, invalid names (error), out-of-range indices (error)
    - Location: `src/api/columns.rs`
    - _Requirements: 3.5, 3.6, 3.7, 3.8, 3.10_

  - [x] 1.6 Implement glob pattern handling with tests
    - Implement `is_glob_pattern()` helper
    - Implement `expand_local_glob()` for filesystem patterns
    - Sort results for deterministic ordering
    - Write unit tests for pattern detection and expansion
    - Location: `src/api/glob.rs`
    - _Requirements: 2.4, 2.5, 2.6, 2.9_

- [x] 2. Multi-Source Infrastructure
  - [x] 2.1 Implement `ResolvedSources` struct with tests
    - Store expanded paths and validated schema
    - Implement `resolve()` async method
    - Write unit tests for single file, multiple files, glob patterns
    - Location: `src/api/sources.rs`
    - _Requirements: 2.7, 2.8_

  - [x] 2.2 Implement schema validation with tests
    - Read schema from first file as canonical
    - Validate all subsequent files have identical schemas
    - Raise `SchemaError` on any mismatch (names, types, order, nullability)
    - Write unit tests for matching and mismatching schemas
    - Location: `src/api/sources.rs`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [x] 2.3 Implement `RowIndexTracker` with tests
    - Track current offset across batches
    - Use `IdxSize` from `polars::prelude`
    - Implement `add_to_dataframe()` method
    - Write unit tests for continuity across multiple batches
    - Location: `src/api/row_index.rs`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_

  - [x] 2.4 Implement `FilePathInjector` with tests
    - Store column name for file paths
    - Implement `add_to_dataframe()` method
    - Write unit tests for column addition
    - Location: `src/api/file_path.rs`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7_

  - [x] 2.5 Implement `MultiSourceReader` with tests
    - Orchestrate reading from multiple sources
    - Handle source exhaustion and transitions
    - Apply row index and file path injection
    - Respect `n_rows` limit across all sources
    - Track accumulated errors when `ignore_errors=True`
    - **CRITICAL: Support BOTH local files AND S3 sources** - currently only LocalSource is implemented
    - Make `open_source()` generic over source type or use trait objects to handle both `LocalSource` and `S3Source`
    - Ensure `read_avro()` with S3 glob patterns works (parity with `scan_avro()`)
    - Write unit tests for state transitions, n_rows limit, error accumulation
    - **Write integration tests for S3 multi-source reading** (glob patterns, multiple S3 URIs)
    - Location: `src/api/multi_source.rs`
    - _Requirements: 2.4, 2.7, 4.3, 4.5, 4.6, 5.6, 7.4, 12.2, 12.3, 12.4_

- [x] 3. Public Rust API
  - [x] 3.1 Create `src/api/mod.rs` module structure
    - Re-export public types and functions
    - Organize submodules (args, options, columns, glob, sources, etc.)
    - _Requirements: 13.1, 13.2_

  - [x] 3.2 Implement `read_avro()` and `read_avro_sources()` with tests
    - Accept `columns` parameter for eager projection
    - Use `MultiSourceReader` for multi-file support
    - Return `PolarsResult<DataFrame>`
    - Write Rust integration tests for: single file, multiple files, with columns, with n_rows, with row_index
    - Location: `src/api/read.rs`
    - _Requirements: 13.3, 11.1, 11.2, 11.4_

  - [x] 3.3 Implement `read_avro_schema()` with tests
    - Read only file header
    - Convert Avro schema to Polars schema
    - Return `PolarsResult<Schema>`
    - Write unit tests for schema extraction
    - Location: `src/api/schema.rs`
    - _Requirements: 13.4_

  - [x] 3.4 Implement `ScanConfig` and `resolve_scan_config()` with tests
    - Bundle resolved sources, args, and options
    - Provide `create_reader()` method for Python's `scan_avro()` generator
    - Provide `polars_schema()` method
    - Write unit tests for config creation and reader instantiation
    - Location: `src/api/scan.rs`
    - _Requirements: 13.5_

- [x] 4. Python Bindings Update (Path support for open() pending)
  - [x] 4.1 Implement `PyFileSource` type with tests
    - Accept `str`, `Path`, `Sequence[str]`, `Sequence[Path]`
    - Implement `to_strings()` conversion method
    - Write Python tests for various input types
    - Location: `src/python/types.rs`
    - _Requirements: 2.1, 2.2, 2.3_

  - [x] 4.2 Implement `PyColumnSelection` type with tests
    - Accept `Sequence[str]` or `Sequence[int]`
    - Implement conversion to `ColumnSelection`
    - Write Python tests for name and index selection
    - Location: `src/python/types.rs`
    - _Requirements: 3.2, 3.3_

  - [x] 4.3 Implement Python exception classes with structured metadata
    - `DecodeError` with attributes: `block_index`, `record_index`, `offset`, `message`
    - `ParseError` with attributes: `offset`, `message`
    - `SourceError` with attributes: `path`, `message`
    - `SchemaError` with attributes: `message`
    - Use `#[pyo3(get)]` for attribute access
    - Location: `src/python/errors.rs`
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6_

  - [x] 4.4 Implement `scan_avro` Python function with tests
    - Use `register_io_source` to create LazyFrame with Rust `ScanConfig` for batch reading
    - Parameters: source, n_rows, row_index_name, row_index_offset, glob, include_file_paths, ignore_errors, storage_options
    - Flattened Avro options: buffer_blocks, buffer_bytes, read_chunk_size, batch_size, max_block_size
    - NO `columns` parameter (projection via LazyFrame)
    - Generator must iterate through ALL paths (not just first)
    - Write Python tests for: basic scan, multi-file, with options
    - Location: `src/python/api.rs`
    - _Requirements: 1.1, 4.1, 4.2, 4.4, 9.1, 9.4, 10.1, 12.1, 12.2_

  - [x] 4.5 Implement `read_avro` Python function with tests
    - Same parameters as `scan_avro` PLUS `columns`
    - Include `max_block_size` parameter
    - Call Rust `read_avro_sources()`
    - Write Python tests for: basic read, with columns, with n_rows
    - Location: `src/python/api.rs`
    - _Requirements: 1.2, 3.1, 3.4, 9.1, 9.4, 11.1, 11.3, 11.4, 12.1, 12.2_

  - [x] 4.6 Implement `read_avro_schema` Python function with tests
    - Accept `PyFileSource` and `storage_options`
    - Call Rust `read_avro_schema()`
    - Write Python tests for schema extraction
    - Location: `src/python/api.rs`
    - _Requirements: 1.3_

  - [x] 4.7 Update PyO3 module registration
    - Register new functions: `scan_avro`, `read_avro`, `read_avro_schema`
    - Register exception classes
    - ✅ Removed old functions: `scan`, `parse_avro_schema`
    - Location: `src/lib.rs`
    - _Requirements: 1.4, 1.5_

  - [x] 4.8 Update Python `__init__.py`
    - Import new functions from Rust module
    - Update `__all__` exports
    - Add `FileSource` and `MissingColumnsPolicy` type aliases
    - Location: `python/jetliner/__init__.py`
    - _Requirements: 1.6, 16.7_

  - [x] 4.9 Update `open()` to accept Path objects
    - Update `path` parameter to accept `str | Path`
    - Convert Path to str before passing to Rust
    - Update type annotations
    - Location: `src/python/reader.rs`, `python/jetliner/__init__.py`
    - _Requirements: 1.7_

- [x] 5. S3 Glob Support
  - [x] 5.1 Implement `expand_s3_glob()` with tests
    - Parse S3 URI to extract bucket and prefix
    - Use S3 `list_objects_v2` to enumerate keys
    - Filter keys matching glob pattern
    - Handle pagination for large buckets
    - Write unit tests for URI parsing and pattern matching
    - Location: `src/api/glob.rs`
    - _Requirements: 2.4_

  - [x] 5.2 Implement retry logic with exponential backoff and tests
    - Use `max_retries` from `CloudOptions`
    - Implement exponential backoff (1s, 2s, 4s, 8s, up to 30s max)
    - Define retryable error types (timeouts, 5xx, 429, SlowDown)
    - Write unit tests with mocked failures
    - Location: `src/source/s3.rs`
    - _Requirements: 8.4, 8.5, 8.6, 8.7, 8.8, 8.9_

- [x] 6. Codebase Cleanup
  - [x] 6.1 Update Python tests to use new API
    - Replace all `scan()` calls with `scan_avro()`
    - ✅ Replaced all `parse_avro_schema()` calls with `read_avro_schema()`
    - Replace `strict=True` with `ignore_errors=False` (inverted)
    - Replace `endpoint_url` with `endpoint` in storage_options
    - Update buffer config to use flattened kwargs
    - Location: `python/tests/**/*.py`
    - _Requirements: 16.1, 16.2, 16.3, 16.4, 16.5, 16.6_

  - [x] 6.2 Update documentation
    - Update README.md examples
    - Update mkdocs user guide
    - Update API reference
    - Document Python API (`scan_avro`, `read_avro`, `read_avro_schema`, `open`)
    - Location: `README.md`, `docs/**/*.md`
    - _Requirements: 16.5_

  - [x] 6.3 Add type annotation support
    - Create `python/jetliner/py.typed` marker file
    - Ensure all public functions have complete type annotations
    - Verify `ty` passes
    - Location: `python/jetliner/py.typed`, `python/jetliner/__init__.py`
    - _Requirements: 17.1, 17.2, 17.3, 17.4, 17.5, 17.6_

  - [x] 6.4 Remove deprecated code
    - Remove `scan()` function from Rust
    - ✅ Removed `parse_avro_schema()` function from Rust
    - Remove old `strict` parameter handling
    - Remove old `endpoint_url` handling
    - Remove `engine_options` dict parsing
    - _Requirements: 1.4, 1.5_

- [x] 7. Integration Tests (Python)
  - [x] 7.1 Test scan_avro and read_avro parity
    - Verify `scan_avro(paths).collect()` returns same rows as `read_avro(paths)`
    - Test with explicit list of files
    - Test with glob patterns
    - Location: `python/tests/integration/test_multi_source.py`
    - _Requirements: 2.7_

  - [x] 7.2 Test multi-file behavior
    - Verify total row count equals sum of individual file row counts
    - Test `n_rows` limit across files (stops after N total, not N per file)
    - Test row_index continuity across file boundaries
    - Test include_file_paths with multiple files
    - _Requirements: 2.7, 4.3, 4.5, 5.6, 6.3_

  - [x] 7.3 Test schema validation across files
    - Test schema mismatch raises `SchemaError` with descriptive message
    - Test identical schemas work correctly
    - _Requirements: 7.1, 7.2, 7.3_

  - [x] 7.4 Test LazyFrame operations
    - Test projection pushdown via `.select()`
    - Test slice pushdown via `head()` and `limit()`
    - Test `collect()`, `.head(n).collect()`, `explain()`
    - Test `collect(engine="streaming")`
    - Location: `python/tests/integration/test_lazyframe.py`
    - _Requirements: 10.2, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9_

  - [x] 7.5 Test edge cases
    - Test `glob=False` treats path literally
    - Test column selection by index
    - Test `scan_avro` rejects `columns` parameter (TypeError)
    - Test empty glob result raises descriptive error
    - Test empty file handling
    - Test mixed codec files
    - _Requirements: 2.5, 2.6, 3.1, 3.3, 3.8, 3.9_

  - [x] 7.6 Test exception attributes
    - Test `DecodeError` attributes: `block_index`, `record_index`, `offset`, `message`
    - Test `ParseError` attributes: `offset`, `message`
    - Test `SourceError` attributes: `path`, `message`
    - Test attributes contain meaningful values
    - _Requirements: 15.2, 15.3, 15.4, 15.5, 15.6_

  - [x] 7.7 Test unknown kwargs rejection
    - Verify passing unknown kwargs raises `TypeError`
    - _Requirements: 9.5_

## Checkpoint Tasks

- [x] 8. Checkpoint - All Complete
  - All Rust unit tests pass
  - All Python tests pass with new API
  - Python `scan_avro()` uses `register_io_source` with Rust batch reading
  - Documentation updated
  - `ty` passes
  - `py.typed` marker present
  - ✅ No references to old function names (`scan`, `parse_avro_schema`)
  - No references to old `strict` parameter
  - No references to old `endpoint_url`
  - `scan_avro().collect()` and `read_avro()` produce consistent results for same input
  - _Requirements: All_

## File Structure

After implementation, new/modified files:

```
src/
├── api/
│   ├── mod.rs           # Public API module
│   ├── args.rs          # ScanArgsAvro (uses Polars types)
│   ├── options.rs       # AvroOptions (separate from scan args)
│   ├── policy.rs        # MissingColumnsPolicy (local definition)
│   ├── columns.rs       # ColumnSelection, resolve_columns
│   ├── glob.rs          # Glob expansion (local + S3)
│   ├── sources.rs       # ResolvedSources, unify_schemas
│   ├── row_index.rs     # RowIndexTracker (uses IdxSize)
│   ├── file_path.rs     # FilePathInjector
│   ├── multi_source.rs  # MultiSourceReader
│   ├── scan.rs          # ScanConfig (building block for Python's scan_avro)
│   ├── read.rs          # read_avro, read_avro_sources
│   └── schema.rs        # read_avro_schema
├── python/
│   ├── mod.rs           # Updated module
│   ├── api.rs           # scan_avro (register_io_source), read_avro, read_avro_schema, _resolve_avro_sources
│   ├── types.rs         # PyFileSource, PyColumnSelection
│   ├── errors.rs        # Exception classes with structured metadata
│   └── reader.rs        # AvroReader, MultiAvroReader, open (AvroReaderCore eliminated)
├── error.rs             # Updated: From<ReaderError> for PolarsError
├── source/
│   └── s3.rs            # Updated: CloudOptions with endpoint, max_retries, retry logic
└── lib.rs               # Updated exports

python/jetliner/
├── __init__.py          # Updated imports and exports
└── py.typed             # Type annotation marker
```

## Rust vs Python API Note

| Function             | Python | Rust | Notes                                                          |
| -------------------- | ------ | ---- | -------------------------------------------------------------- |
| `scan_avro()`        | ✓      | -    | Python only via `register_io_source`; Rust provides ScanConfig |
| `read_avro()`        | ✓      | ✓    | Full support in both                                           |
| `read_avro_schema()` | ✓      | ✓    | Full support in both                                           |
| `open()`             | ✓      | -    | Python wrapper; Rust users use `AvroStreamReader` directly     |

Python `scan_avro()` returns `LazyFrame` with projection and slice pushdown support. Rust-native `scan_avro()` returning `LazyFrame` is out of scope.

For streaming in Rust, use `AvroStreamReader` directly - `open()` is just a Pythonic wrapper around this.

## Feature Support Matrix

| Feature        | `scan_avro()` | `read_avro()` | `open()` |
| -------------- | ------------- | ------------- | -------- |
| S3             | ✓             | ✓             | ✓        |
| Glob patterns  | ✓             | ✓             | -        |
| Multiple files | ✓             | ✓             | -        |
| Path objects   | ✓             | ✓             | ✓        |
| Mixed sources  | ✓             | ✓             | -        |

**Note:** Both `scan_avro()` and `read_avro()` fully support S3 sources, including glob patterns, multiple S3 URIs, and mixed local + S3 sources in the same call.

`open()` is designed for simple single-file streaming. For glob patterns or multiple files, use `scan_avro()` or `read_avro()`.
