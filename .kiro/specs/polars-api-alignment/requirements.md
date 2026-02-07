# Requirements Document: Polars API Alignment

## Introduction

This document specifies requirements for aligning Jetliner's API with standard Polars conventions. The goal is to make Jetliner feel native to Polars users by adopting familiar function names, parameter signatures, and behaviors from Polars' built-in readers (`read_parquet`, `scan_parquet`, etc.).

**Python is the primary use case.** Core reading logic is implemented in Rust for performance, with Python bindings as the main interface. This architecture means:
- Python gets the full API (`scan_avro()`, `read_avro()`, `read_avro_schema()`, `open()`)
- Rust provides the implementation infrastructure that Python wraps
- Rust-native `scan_avro()` returning `LazyFrame` is explicitly out of scope (see Requirement 13)

## Motivation

Currently, Jetliner uses non-standard naming (`scan`, `open`, `parse_avro_schema`) and limited parameter support compared to Polars conventions. Users familiar with Polars expect:
- `scan_avro()` / `read_avro()` naming pattern
- Standard parameters like `n_rows`, `row_index_name`
- Support for `Path` objects, glob patterns, and multiple files
- Consistent behavior with other Polars readers

Additionally, the current architecture has too much logic in Python. Moving logic to Rust:
- Improves performance (no Python overhead in hot paths)
- Provides a clean separation of concerns
- Enables future Rust-native usage if needed

**Note on Polars' `read_avro`:** Polars has a built-in `pl.read_avro()` with limited functionality (only `source`, `columns`, `n_rows` parameters - no streaming, no S3, no glob, no `scan_avro` equivalent). **Polars is deprecating this function**, and the community has created plugins like `polars-avro` to fill the gap. Jetliner's API is a superior alternative with full streaming support, S3 access, glob patterns, and LazyFrame integration via `scan_avro()`.

## Glossary

- **FileSource**: Union type representing valid file inputs (str, Path, Sequence of paths, glob patterns)
- **LazyFrame**: Polars lazy computation holder returned by scan functions
- **DataFrame**: Polars eager DataFrame returned by read functions
- **Projection**: Selecting a subset of columns to read
- **Row Index**: Synthetic column containing row numbers
- **ScanArgsAvro**: Rust struct containing scan configuration (mirrors Polars' `ScanArgsParquet`)
- **AvroOptions**: Rust struct containing Avro-specific reader options (mirrors Polars' `ParquetOptions`)

## Requirements

### Requirement 1: Function Naming Alignment

**User Story:** As a Polars user, I want Jetliner functions to follow Polars naming conventions, so that the API feels familiar and discoverable.

#### Acceptance Criteria

1. THE library SHALL expose a `scan_avro()` function that returns a `pl.LazyFrame`
2. THE library SHALL expose a `read_avro()` function that returns a `pl.DataFrame`
3. THE library SHALL expose a `read_avro_schema()` function that returns a `pl.Schema`
4. THE existing `scan()` function SHALL be removed (replaced by `scan_avro()`)
5. THE existing `parse_avro_schema()` function SHALL be removed (replaced by `read_avro_schema()`)
6. THE `open()` function SHALL remain available for streaming iterator access (no Polars equivalent exists)
7. THE `open()` function SHALL accept `str` or `Path` for the source parameter (single file only, no glob or multi-file support)

### Requirement 2: FileSource Support

**User Story:** As a data engineer, I want to specify file sources in multiple ways, so that I can use the most convenient format for my use case.

#### Acceptance Criteria

1. THE `source` parameter SHALL accept `str` (file path or S3 URI)
2. THE `source` parameter SHALL accept `pathlib.Path` objects
3. THE `source` parameter SHALL accept a `Sequence` of `str` or `Path` for multiple files
4. WHEN a glob pattern is provided (e.g., `"data/*.avro"`), THE reader SHALL expand it to matching files
5. THE `glob` parameter (default `True`) SHALL control whether glob expansion is performed
6. WHEN `glob=False`, THE reader SHALL treat the path literally without expansion
7. WHEN multiple files are provided, THE reader SHALL read them in sequence
8. WHEN multiple files have incompatible schemas, the reader SHALL raise a `SchemaError`
9. PATH normalization and glob expansion SHALL be implemented in Rust

### Requirement 3: Column Selection (Projection)

**User Story:** As a data analyst, I want to select specific columns at read time, so that I can reduce memory usage and improve performance.

#### Acceptance Criteria

1. THE `columns` parameter SHALL be available on `read_avro()` only (not `scan_avro()`)
2. THE `columns` parameter SHALL accept `Sequence[str]` for column names
3. THE `columns` parameter SHALL accept `Sequence[int]` for column indices (0-based)
4. THE `columns` parameter SHALL accept `None` to read all columns (default)
5. WHEN column names are provided, THE reader SHALL only decode and return those columns
6. WHEN column indices are provided, THE reader SHALL map them to column names using the schema
7. WHEN an invalid column name is provided, THE reader SHALL raise a descriptive error
8. WHEN an invalid column index is provided, THE reader SHALL raise an `IndexError`
9. FOR `scan_avro()`, projection SHALL be done via LazyFrame operations (Polars handles pushdown)
10. COLUMN resolution SHALL be implemented in Rust

### Requirement 4: Row Limiting

**User Story:** As a data analyst, I want to limit the number of rows read, so that I can quickly preview data or test queries.

#### Acceptance Criteria

1. THE `n_rows` parameter SHALL accept `int` to limit rows read
2. THE `n_rows` parameter SHALL accept `None` to read all rows (default)
3. WHEN `n_rows` is specified, THE reader SHALL stop reading after that many rows
4. FOR `scan_avro()`, THE `n_rows` parameter SHALL serve as a hint; Polars may override via slice pushdown
5. WHEN reading multiple files, THE `n_rows` limit SHALL apply to the total across all files
6. ROW limiting logic SHALL be implemented in Rust

### Requirement 5: Row Index Column

**User Story:** As a data analyst, I want to add a row index column to track original row positions, so that I can reference rows after transformations.

#### Acceptance Criteria

1. THE `row_index_name` parameter SHALL accept `str` to specify the index column name
2. THE `row_index_name` parameter SHALL accept `None` to disable row indexing (default)
3. WHEN `row_index_name` is specified, THE reader SHALL insert an index column as the first column
4. THE `row_index_offset` parameter SHALL accept `int` to specify the starting index (default 0)
5. THE row index SHALL be of type `pl.UInt32` (using Polars' `IdxSize` type)
6. WHEN reading multiple files, THE row index SHALL be continuous across files
7. ROW index generation SHALL be implemented in Rust

### Requirement 6: File Path Inclusion

**User Story:** As a data engineer working with partitioned data, I want to know which file each row came from, so that I can trace data lineage.

#### Acceptance Criteria

1. THE `include_file_paths` parameter SHALL accept `str` to specify the column name for file paths
2. THE `include_file_paths` parameter SHALL accept `None` to disable (default)
3. WHEN `include_file_paths` is specified, THE reader SHALL add a column containing the source file path for each row
4. THE file path column SHALL be of type `pl.String`
5. FOR S3 sources, THE file path SHALL be the full S3 URI
6. FILE path injection SHALL be implemented in Rust
7. THE `include_file_paths` parameter SHALL work with single files (not just multi-file)

### Requirement 7: Schema Compatibility

**User Story:** As a data engineer reading multiple files, I want clear error messages when schemas don't match, so that I can identify and fix data quality issues.

#### Acceptance Criteria

1. WHEN multiple files are provided, THE reader SHALL validate that all files have identical schemas
2. SCHEMA equality SHALL require exact match of: field names, field types, field order, and nullability
3. WHEN schemas differ, THE reader SHALL raise a `SchemaError` with details about the mismatch
4. THE first file's schema SHALL be used as the canonical schema for validation
5. SCHEMA validation SHALL occur during source resolution (before reading data)

### Requirement 8: Storage Options

**User Story:** As a data engineer reading from S3, I want to configure cloud access and retry behavior.

#### Acceptance Criteria

1. THE `storage_options` parameter SHALL accept `dict[str, str] | None`
2. SUPPORTED keys SHALL include: `endpoint`, `aws_access_key_id`, `aws_secret_access_key`, `region`
3. THE `endpoint` key SHALL specify custom S3 endpoint (for MinIO, LocalStack, R2, etc.), aligned with Polars' `AmazonS3ConfigKey::Endpoint`
4. THE `max_retries` key SHALL control retry count for transient S3 failures (default: "2")
5. RETRYABLE errors SHALL include: connection timeouts, 5xx responses, throttling (429, SlowDown)
6. THE reader SHALL use exponential backoff between retries (1s, 2s, 4s, 8s, up to 30s max)
7. STORAGE options SHALL take precedence over environment variables
8. RETRY logic SHALL be implemented in Rust
9. S3 glob patterns SHALL use prefix optimization to minimize API calls

### Requirement 9: Avro Reader Options (Flattened)

**User Story:** As a power user, I want fine-grained control over Jetliner's internal buffering and performance tuning.

#### Acceptance Criteria

1. AVRO-specific options SHALL be exposed as top-level kwargs (not nested in a dict), following Polars' Python API style
2. SUPPORTED parameters SHALL include:
   - `buffer_blocks`: Number of blocks to prefetch (default: 4)
   - `buffer_bytes`: Maximum bytes to buffer (default: 64MB)
   - `read_chunk_size`: I/O read chunk size in bytes (default: auto-detect)
   - `batch_size`: Target rows per DataFrame batch (default: 100,000)
   - `max_block_size`: Maximum decompressed block size in bytes (default: 512MB)
3. IN Rust, these SHALL be organized in a separate `AvroOptions` struct (analogous to Polars' `ParquetOptions`)
4. THE `max_block_size` parameter SHALL protect against decompression bombs by rejecting blocks that exceed the limit
5. WHEN `max_block_size=None`, THE size limit SHALL be disabled
6. FUTURE parameters MAY include concurrency options (e.g., `decode_workers`, `decompress_workers`)
7. UNKNOWN parameters SHALL raise an error (not silently ignored)

### Requirement 10: LazyFrame Integration (Python)

**User Story:** As a Polars user, I want `scan_avro()` to fully integrate with Polars query optimization, so that I get the best performance.

#### Acceptance Criteria

1. THE `scan_avro()` function SHALL return a valid `pl.LazyFrame`
2. THE LazyFrame SHALL support projection pushdown (only read needed columns)
3. THE LazyFrame SHALL support predicate pushdown (filter during read) - future enhancement
4. THE LazyFrame SHALL support slice pushdown (early stopping for `head()`/`limit()`)
5. THE LazyFrame SHALL support `collect()` to materialize results
6. THE LazyFrame SHALL support `collect_async()` for async materialization
7. THE LazyFrame SHALL support `.head(n).collect()` for partial materialization (note: `fetch()` is deprecated)
8. THE LazyFrame SHALL support `explain()` to show query plan
9. THE LazyFrame SHALL be compatible with streaming execution via `collect(engine="streaming")` (note: `streaming=True` is deprecated since Polars 1.31). This is a compatibility requirement - Jetliner's batch-yielding generator naturally supports Polars' streaming engine.
10. THE Python implementation SHALL use `register_io_source` approach (Polars' documented API for Python IO plugins)

### Requirement 11: Read Function Behavior

**User Story:** As a data analyst, I want `read_avro()` to be a convenient shorthand for `scan_avro().collect()`, so that I can quickly load data.

#### Acceptance Criteria

1. THE `read_avro()` function SHALL return a `pl.DataFrame`
2. THE `read_avro()` function SHALL be equivalent to `scan_avro(...).collect()` with eager column selection
3. THE `read_avro()` function SHALL accept all parameters that `scan_avro()` accepts
4. THE `read_avro()` function SHALL additionally accept `columns` parameter for eager projection

### Requirement 12: Error Handling Mode

**User Story:** As a data engineer, I want control over how data corruption errors are handled.

#### Acceptance Criteria

1. THE `ignore_errors` parameter SHALL accept `bool` (default: `False`)
2. WHEN `ignore_errors=False`, THE reader SHALL fail immediately on any decode/corruption error
3. WHEN `ignore_errors=True`, THE reader SHALL skip bad records/blocks and continue
4. FOR `open()` iterator, accumulated errors MAY be accessible via the reader (future enhancement)
5. THIS is orthogonal to schema validation (schema mismatches raise `SchemaError`)
6. THE parameter name `ignore_errors` aligns with Polars CSV reader conventions (inverted from previous `strict`)

### Requirement 13: Rust API

**User Story:** As a Rust developer, I want Jetliner's core logic implemented in Rust, so that Python bindings are thin wrappers over high-performance Rust code.

#### Acceptance Criteria

1. THE library SHALL expose a `ScanArgsAvro` struct with scan configuration options
2. THE library SHALL expose an `AvroOptions` struct with Avro-specific reader options (separate from scan args)
3. THE library SHALL expose a `read_avro()` function returning `PolarsResult<DataFrame>`
4. THE library SHALL expose a `read_avro_schema()` function returning `PolarsResult<Schema>`
5. THE library SHALL expose a `ScanConfig` struct that Python's `scan_avro()` uses via `register_io_source`
6. THE Rust API SHALL be the source of truth for data reading; Python SHALL use Rust for batch reading
7. THE library SHALL use Polars types directly: `PlSmallStr`, `IdxSize`, `RowIndex` from `polars::prelude`

**Out of Scope:** Rust-native `scan_avro()` returning `LazyFrame` via `AnonymousScan` trait. Python is the primary use case; Rust provides the implementation that Python wraps.

### Requirement 14: Error Integration

**User Story:** As a developer, I want Jetliner errors to integrate well with Polars while preserving rich debugging context.

#### Acceptance Criteria

1. THE library SHALL maintain its rich hierarchical error types (`ReaderError`, `DecodeError`, `SourceError`, etc.)
2. THE library SHALL implement `From<ReaderError> for PolarsError` for API compatibility
3. PUBLIC API functions SHALL return `PolarsResult` for Polars ecosystem compatibility
4. ERROR conversion SHALL map to appropriate Polars error variants:
   - Schema errors → `PolarsError::SchemaMismatch`
   - Not found errors → `PolarsError::IO` with `NotFound` kind
   - Other source errors → `PolarsError::IO`
   - Decode/other errors → `PolarsError::ComputeError`

### Requirement 15: Python Exception Metadata

**User Story:** As a Python developer debugging data issues, I want structured access to error context, so that I can programmatically handle and report errors.

#### Acceptance Criteria

1. PYTHON exception classes SHALL expose error context as attributes (not just in message string)
2. `DecodeError` SHALL expose: `block_index`, `record_index`, `offset`, `message`
3. `ParseError` SHALL expose: `offset`, `message`
4. `SourceError` SHALL expose: `path`, `message`
5. `SchemaError` SHALL expose: `message` and relevant schema context
6. ATTRIBUTES SHALL be accessible via standard Python attribute access (e.g., `e.block_index`)

### Requirement 16: Code Cleanup

**User Story:** As a maintainer, I want the codebase to be clean and consistent, so that it's easy to maintain.

#### Acceptance Criteria

1. ALL references to `scan()` SHALL be updated to `scan_avro()`
2. ALL references to `parse_avro_schema()` SHALL be updated to `read_avro_schema()`
3. ALL references to `strict` parameter SHALL be updated to `ignore_errors` (with inverted semantics)
4. ALL references to `endpoint_url` in storage_options SHALL be updated to `endpoint`
5. ALL documentation SHALL use the new function names and parameter names
6. ALL tests SHALL use the new function names and parameter names
7. THE `__all__` export list SHALL be updated

### Requirement 17: Type Annotations

**User Story:** As a Python developer, I want complete type annotations, so that my IDE provides accurate completions and type checking works.

#### Acceptance Criteria

1. ALL public functions SHALL have complete type annotations
2. THE `source` parameter SHALL be typed as `str | Path | Sequence[str] | Sequence[Path]`
3. THE `columns` parameter SHALL be typed as `Sequence[int] | Sequence[str] | None`
4. THE return types SHALL be accurately annotated (`pl.LazyFrame`, `pl.DataFrame`, `pl.Schema`)
5. THE library SHALL pass `ty` type checking
6. THE library SHALL include a `py.typed` marker file

## Non-Requirements (Out of Scope)

1. **File-like objects**: Supporting `IO[bytes]` or `BytesIO` as sources (requires architectural changes)
2. **Credential provider callback**: Supporting dynamic credential refresh (future enhancement)
3. **Hive partitioning**: Inferring partitions from directory structure (Avro doesn't use this pattern)
4. **Write support**: Writing DataFrames to Avro format (explicitly out of scope for Jetliner)
5. **Schema override**: Providing a different schema than the file contains (complex, low priority)
6. **rechunk parameter**: Our batching model already produces reasonably-sized chunks
7. **cache parameter**: Handled by Polars' query engine, not by IO sources
8. **low_memory parameter**: Redundant with explicit buffer options (buffer_blocks, buffer_bytes)
9. **parallel parameter**: Polars' `scan_parquet` has this for parallel reading strategy; Avro's block structure handles this differently via `buffer_blocks`
10. **use_statistics parameter**: Polars uses Parquet statistics for query optimization; Avro format doesn't have equivalent statistics
11. **Rust-native `scan_avro()`**: Returning `LazyFrame` via `AnonymousScan` trait is out of scope; Python is the primary use case
12. **Glob/multi-file for `open()`**: The `open()` function only supports single files; use `scan_avro()` or `read_avro()` for glob patterns and multiple files
13. **Schema flexibility across files**: All files in a multi-file read must have identical schemas (exact match of names, types, order, nullability). Any schema mismatch raises a `SchemaError`.

## Dependencies

- Polars >= 1.0.0 (for `register_io_source` and modern API)
- Python >= 3.9 (for modern type annotation syntax)

## API Examples

### Python API

### Python API

```python
import jetliner
import polars as pl

# Scan - returns LazyFrame (projection via LazyFrame operations)
lf = jetliner.scan_avro("file.avro")
lf = jetliner.scan_avro("s3://bucket/file.avro")
result = lf.select(["a", "b"]).filter(pl.col("a") > 0).collect()

# Read - returns DataFrame (supports columns parameter)
df = jetliner.read_avro("file.avro", columns=["a", "b"], n_rows=1000)

# Schema inspection
schema = jetliner.read_avro_schema("file.avro")

# Multiple files (Sequence of paths)
df = jetliner.read_avro(["file1.avro", "file2.avro"])
df = jetliner.read_avro(("file1.avro", "file2.avro"))  # Tuples work too
df = jetliner.read_avro("data/*.avro")  # Glob pattern

# With row index
df = jetliner.read_avro("file.avro", row_index_name="idx")

# With file paths
df = jetliner.read_avro("data/*.avro", include_file_paths="source_file")

# Error handling mode (ignore_errors replaces strict with inverted semantics)
df = jetliner.read_avro("file.avro", ignore_errors=True)  # Skip bad records

# S3 with custom options (note: 'endpoint' not 'endpoint_url')
df = jetliner.read_avro(
    "s3://bucket/file.avro",
    storage_options={
        "endpoint": "http://localhost:9000",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        "max_retries": "5",
    }
)

# Performance tuning (flattened kwargs, not nested dict)
df = jetliner.read_avro(
    "large_file.avro",
    buffer_blocks=8,
    buffer_bytes=128 * 1024 * 1024,
    batch_size=200_000,
)

# Decompression bomb protection
df = jetliner.read_avro(
    "untrusted.avro",
    max_block_size=512 * 1024 * 1024,  # Reject blocks > 512MB
)

# Streaming iterator (unchanged)
with jetliner.open("file.avro") as reader:
    for batch in reader:
        process(batch)

# Structured exception handling
try:
    df = jetliner.read_avro("corrupted.avro")
except jetliner.DecodeError as e:
    print(f"Error at block {e.block_index}, record {e.record_index}")
    print(f"Offset: {e.offset}")
```

### Rust API

```rust
use jetliner::{read_avro, read_avro_schema, ScanArgsAvro, AvroOptions, ColumnSelection};
use polars::prelude::{RowIndex, PlSmallStr};

// Read - returns DataFrame
let df = read_avro("file.avro", None, ScanArgsAvro::default(), AvroOptions::default())?;

// With options
let args = ScanArgsAvro {
    n_rows: Some(1000),
    row_index: Some(RowIndex {
        name: PlSmallStr::from("idx"),
        offset: 0,
    }),
    ignore_errors: true,
    ..Default::default()
};
let avro_opts = AvroOptions {
    buffer_blocks: 8,
    batch_size: 200_000,
    max_decompressed_block_size: Some(512 * 1024 * 1024),
    ..Default::default()
};
let df = read_avro("file.avro", None, args, avro_opts)?;

// Read with column selection
let columns = ColumnSelection::Names(vec![Arc::from("a"), Arc::from("b")]);
let df = read_avro("file.avro", Some(columns), ScanArgsAvro::default(), AvroOptions::default())?;

// Schema inspection
let schema = read_avro_schema("file.avro", None)?;
```

**Note:** Rust-native `scan_avro()` returning `LazyFrame` is out of scope. For lazy evaluation, use Python's `scan_avro()` which uses `register_io_source` with Rust batch reading under the hood.
