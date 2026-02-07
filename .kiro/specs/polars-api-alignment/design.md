# Design Document: Polars API Alignment

## Overview

This document describes the technical design for aligning Jetliner's API with Polars conventions. **Python is the primary use case**, with core reading logic implemented in Rust for performance. Python bindings provide the full API; Rust provides the implementation infrastructure.

## Key Design Decisions

The following decisions were made during spec review to ensure alignment with Polars conventions:

1. **Struct organization**: Separate `AvroOptions` (format-specific) from `ScanArgsAvro` (user-facing scan config), following Polars' `ParquetOptions` / `ScanArgsParquet` pattern
2. **Python kwargs**: Flatten all options as top-level parameters (no nested `engine_options` dict)
3. **Use `usize`** for counts/sizes - idiomatic Rust, matches Polars
4. **Use Polars types directly**: `PlSmallStr`, `IdxSize`, `RowIndex` from `polars::prelude`
5. **Rename `strict` to `ignore_errors`** (inverted semantics) for data reading - aligns with Polars CSV
6. **Schema validation**: Multi-file reads require identical schemas (exact match of names, types, order, nullability); any mismatch raises `SchemaError`
7. **Python `scan_avro()` uses `register_io_source`** - creates Python-native LazyFrame with Rust batch reading under the hood
8. **Rust-native `scan_avro()` is out of scope** - Python is the primary use case; Rust provides infrastructure that Python wraps
9. **Skip `rechunk` and `cache` parameters** - not applicable for streaming Avro reader
10. **Use `endpoint`** in storage_options - aligns with Polars' `AmazonS3ConfigKey::Endpoint`
11. **Error handling**: Keep Jetliner's rich errors, add `impl From<ReaderError> for PolarsError`
12. **Python exceptions with structured metadata**: Expose error context as attributes
13. **Decompression bomb protection**: Add `max_block_size` parameter to reject oversized decompressed blocks
14. **S3 implementation**: Use `BoxedSource` trait objects for dynamic dispatch, enabling mixed local/S3 sources

## Architecture

### Implemented State (Post-Simplification)

The architecture has been simplified to eliminate duplication and expose Rust components directly to Python:

```
Rust Layer (src/api/):
  ScanArgsAvro { n_rows, row_index, glob, include_file_paths, ignore_errors, s3_config }
  AvroOptions { buffer_blocks, buffer_bytes, read_chunk_size, batch_size }
  ResolvedSources { paths, schema } ──► Glob expansion + schema validation
  MultiAvroReader { multi-file streaming, row_index, file_paths, n_rows }
  ScanConfig { resolved sources, args, opts } ──► Building block for scan_avro

Rust Layer (src/python/):
  AvroReader ─────────────────► Single-file streaming iterator
  MultiAvroReader ────────────► Multi-file streaming iterator (exposes Rust MultiAvroReader)
  _resolve_avro_sources() ────► Internal: glob expansion + schema (for scan_avro)

Python Layer (python/jetliner/__init__.py):
  scan_avro(source, ...) ─────► _resolve_avro_sources + MultiAvroReader + register_io_source
  read_avro(source, ...) ─────► Rust read_avro() via MultiAvroReader
  read_avro_schema(source) ───► Rust schema reading
  open(source, **kwargs) ─────► AvroReader (single file)
```

**Key Simplifications:**

1. **Eliminated AvroReaderCore** - `AvroReader` handles all single-file streaming (~300 lines of duplication removed)
2. **Exposed MultiAvroReader to Python** - Multi-file reading logic in Rust, directly usable from Python
3. **Simplified scan_avro generator** - ~15 lines of Python (down from ~150) wrapping MultiAvroReader
4. **Internal _resolve_avro_sources** - Handles glob expansion and schema validation in Rust, used by scan_avro

**Class Hierarchy:**

```
Python Classes (public):
  AvroReader        - single file streaming with context manager
  MultiAvroReader   - multi-file streaming with row_index/file_paths support

Python Functions (public):
  scan_avro()       → LazyFrame (uses MultiAvroReader internally)
  read_avro()       → DataFrame (uses MultiAvroReader internally)
  read_avro_schema() → Schema
  open()            → AvroReader

Internal Functions (not in __all__):
  _resolve_avro_sources() - glob expansion + schema validation
```

**Note on scan_avro:** Python's `scan_avro()` uses `register_io_source` (Polars' documented approach for Python IO plugins - see [IO Plugins Guide](https://docs.pola.rs/user-guide/plugins/io_plugins/)). The generator is now a thin wrapper around `MultiAvroReader`.

**Out of Scope:** Rust-native `scan_avro()` returning `LazyFrame` via `AnonymousScan` trait. This would require `polars-lazy` dependency and has known issues with PyLazyFrame deserialization (see [pyo3-polars#67](https://github.com/pola-rs/pyo3-polars/issues/67)). Python is the primary use case.

## Detailed Design

### 1. Configuration Structs (Rust)

Following Polars' pattern of separating format-specific options from scan args:

```rust
use polars::prelude::{PlSmallStr, IdxSize, RowIndex};

/// Avro-specific reader options (analogous to ParquetOptions).
/// Controls how Avro data is read and decoded.
#[derive(Clone, Debug)]
pub struct AvroOptions {
    /// Number of blocks to prefetch (default: 4)
    pub buffer_blocks: usize,
    /// Maximum bytes to buffer during prefetching (default: 64MB)
    pub buffer_bytes: usize,
    /// I/O read chunk size in bytes (None = auto-detect based on source)
    pub read_chunk_size: Option<usize>,
    /// Target number of rows per DataFrame batch (default: 100,000)
    pub batch_size: usize,
    /// Maximum decompressed block size in bytes (default: 512MB, None = unlimited)
    /// Protects against decompression bombs
    pub max_decompressed_block_size: Option<usize>,
}

impl Default for AvroOptions {
    fn default() -> Self {
        Self {
            buffer_blocks: 4,
            buffer_bytes: 64 * 1024 * 1024,
            read_chunk_size: None,
            batch_size: 100_000,
            max_decompressed_block_size: Some(512 * 1024 * 1024),
        }
    }
}

/// Configuration for scanning Avro files.
/// Mirrors Polars' ScanArgsParquet for API consistency.
/// Note: Does NOT include `columns` - projection is done via LazyFrame operations for scan,
/// or passed separately to read_avro() for eager reading.
#[derive(Clone, Default)]
pub struct ScanArgsAvro {
    /// Maximum number of rows to read
    pub n_rows: Option<usize>,
    /// Row index configuration (uses Polars' RowIndex type)
    pub row_index: Option<RowIndex>,
    /// S3 configuration options
    pub s3_config: Option<S3Config>,
    /// Whether to expand glob patterns (default: true)
    pub glob: bool,
    /// Column name for source file paths
    pub include_file_paths: Option<PlSmallStr>,
    /// If true, skip bad records; if false, fail on first error (default: false)
    pub ignore_errors: bool,
}

impl Default for ScanArgsAvro {
    fn default() -> Self {
        Self {
            n_rows: None,
            row_index: None,
            s3_config: None,
            glob: true,
            include_file_paths: None,
            ignore_errors: false,
        }
    }
}
```

### 2. Cloud Options

```rust
/// Cloud storage configuration.
/// Parsed from storage_options dict in Python.
#[derive(Clone, Default, Debug)]
pub struct CloudOptions {
    /// Custom S3 endpoint (for MinIO, LocalStack, R2, etc.)
    /// Key: "endpoint" (aligned with Polars AmazonS3ConfigKey::Endpoint)
    pub endpoint: Option<String>,
    /// AWS access key ID
    pub aws_access_key_id: Option<String>,
    /// AWS secret access key
    pub aws_secret_access_key: Option<String>,
    /// AWS region
    pub region: Option<String>,
    /// Maximum retry attempts for transient failures (default: 2)
    pub max_retries: usize,
}

impl CloudOptions {
    pub fn from_dict(opts: &HashMap<String, String>) -> Self {
        Self {
            endpoint: opts.get("endpoint").cloned(),
            aws_access_key_id: opts.get("aws_access_key_id").cloned(),
            aws_secret_access_key: opts.get("aws_secret_access_key").cloned(),
            region: opts.get("region").cloned(),
            max_retries: opts.get("max_retries")
                .and_then(|s| s.parse().ok())
                .unwrap_or(2),
        }
    }
}
```

### S3 Implementation Details

**Multi-source architecture**: `MultiSourceReader` uses `BoxedSource` trait objects to handle both `LocalSource` and `S3Source` dynamically. This enables:
- Mixed local and S3 files in the same read operation
- Simpler API without complex generic bounds
- Negligible performance overhead (dynamic dispatch is trivial compared to I/O)

**Retry logic**: Exponential backoff with configurable `max_retries`:
- Backoff delays: 1s, 2s, 4s, 8s, up to 30s maximum
- Retryable errors: connection timeouts, 5xx responses, throttling (429, SlowDown)
- Non-retryable errors: 404, 403, invalid credentials (fail immediately)

**S3 glob optimization**: For patterns like `s3://bucket/path/to/*.avro`:
- Extract prefix up to first glob character (`path/to/`)
- Use prefix in `list_objects_v2` for server-side filtering
- Apply full glob pattern client-side for final matching
- Minimizes S3 API calls and data transfer

### 3. Error Handling

Jetliner maintains its rich hierarchical error types for detailed debugging context, with conversion to `PolarsError` for API compatibility:

```rust
use polars::prelude::{PolarsError, PolarsResult};

impl From<ReaderError> for PolarsError {
    fn from(err: ReaderError) -> Self {
        match err {
            ReaderError::Schema(_) => PolarsError::SchemaMismatch(err.to_string().into()),
            ReaderError::Source(SourceError::NotFound(_)) => PolarsError::IO {
                error: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    err.to_string(),
                )),
                msg: None,
            },
            ReaderError::Source(_) => PolarsError::IO {
                error: Arc::new(std::io::Error::other(err.to_string())),
                msg: None,
            },
            _ => PolarsError::ComputeError(err.to_string().into()),
        }
    }
}

// Public Rust API functions return PolarsResult for Polars ecosystem compatibility
pub fn read_avro(source: &str, columns: Option<ColumnSelection>, args: ScanArgsAvro, opts: AvroOptions) -> PolarsResult<DataFrame> { ... }
pub fn read_avro_schema(source: &str, cloud_options: Option<CloudOptions>) -> PolarsResult<Schema> { ... }

// Building blocks for Python's scan_avro (uses register_io_source)
pub fn resolve_scan_config(sources: &[String], args: ScanArgsAvro, opts: AvroOptions) -> PolarsResult<ScanConfig> { ... }
pub fn collect_scan(config: &ScanConfig, projected_columns: Option<Vec<String>>, n_rows: Option<usize>) -> PolarsResult<DataFrame> { ... }
```

### 4. Python Exception Classes with Structured Metadata

Python exceptions expose error context as attributes for programmatic access:

```rust
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Decode error with structured metadata
#[pyclass(extends=JetlinerError)]
pub struct DecodeError {
    #[pyo3(get)]
    pub block_index: usize,
    #[pyo3(get)]
    pub record_index: usize,
    #[pyo3(get)]
    pub offset: u64,
    #[pyo3(get)]
    pub message: String,
}

/// Parse error with structured metadata
#[pyclass(extends=JetlinerError)]
pub struct ParseError {
    #[pyo3(get)]
    pub offset: u64,
    #[pyo3(get)]
    pub message: String,
}

/// Source error with structured metadata
#[pyclass(extends=JetlinerError)]
pub struct SourceError {
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub message: String,
}
```

Usage in Python:
```python
try:
    df = jetliner.read_avro("corrupted.avro")
except jetliner.DecodeError as e:
    print(f"Error at block {e.block_index}, record {e.record_index}")
    print(f"Offset: {e.offset}")
    print(f"Message: {e.message}")
```

### 5. Python Bindings (Flattened kwargs)

All options are top-level parameters, matching Polars' Python API style:

```rust
#[pyfunction]
#[pyo3(signature = (
    source,
    *,
    n_rows = None,
    row_index_name = None,
    row_index_offset = 0,
    glob = true,
    include_file_paths = None,
    ignore_errors = false,
    storage_options = None,
    // Avro-specific options (flattened, not nested)
    buffer_blocks = 4,
    buffer_bytes = 67108864,  // 64MB
    read_chunk_size = None,
    batch_size = 100000,
    max_block_size = 536870912,  // 512MB
))]
fn scan_avro(
    py: Python<'_>,
    source: PyFileSource,
    n_rows: Option<usize>,
    row_index_name: Option<String>,
    row_index_offset: IdxSize,
    glob: bool,
    include_file_paths: Option<String>,
    ignore_errors: bool,
    storage_options: Option<HashMap<String, String>>,
    buffer_blocks: usize,
    buffer_bytes: usize,
    read_chunk_size: Option<usize>,
    batch_size: usize,
    max_block_size: Option<usize>,
) -> PyResult<PyLazyFrame> {
    // Implementation uses _resolve_avro_sources() + MultiAvroReader + register_io_source
    // See src/python/api.rs for the actual implementation
}

#[pyfunction]
#[pyo3(signature = (
    source,
    *,
    columns = None,
    n_rows = None,
    row_index_name = None,
    row_index_offset = 0,
    glob = true,
    include_file_paths = None,
    ignore_errors = false,
    storage_options = None,
    buffer_blocks = 4,
    buffer_bytes = 67108864,
    read_chunk_size = None,
    batch_size = 100000,
    max_block_size = 536870912,
))]
fn read_avro(
    py: Python<'_>,
    source: PyFileSource,
    columns: Option<PyColumnSelection>,  // Only read_avro has columns
    // ... same params as scan_avro
) -> PyResult<PyDataFrame> {
    // Implementation uses MultiAvroReader internally
}

#[pyfunction]
#[pyo3(signature = (source, *, storage_options = None))]
fn read_avro_schema(
    source: PyFileSource,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<PySchema> {
    // Only reads first file if multiple provided
    // ... implementation
}
```

Note: Unknown kwargs are not silently ignored. PyO3's signature macro will raise `TypeError` for any unrecognized keyword arguments, which aligns with Requirement 9.5.

### 6. Column Selection and Resolution

```rust
/// Column selection by name or index.
///
/// Polars alignment: Matches `columns` parameter behavior in pl.read_parquet()
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnSelection {
    /// Select columns by name.
    Names(Vec<Arc<str>>),
    /// Select columns by 0-based index.
    Indices(Vec<usize>),
}

/// Result of resolving column selection against a schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedColumns {
    /// The resolved column names in order.
    pub names: Vec<Arc<str>>,
    /// The resolved column indices in order.
    pub indices: Vec<usize>,
}

/// Resolve column selection against a record schema.
///
/// Returns descriptive errors for:
/// - Invalid column names (lists available columns)
/// - Out-of-range indices (shows valid range)
pub fn resolve_columns(
    selection: &ColumnSelection,
    schema: &RecordSchema,
) -> Result<ResolvedColumns, SchemaError>;
```

Location: `src/api/columns.rs`

### 7. File Path Injection

```rust
/// Injects file path column into DataFrames.
///
/// Polars alignment: Matches `include_file_paths` parameter in pl.scan_parquet()
#[derive(Debug, Clone)]
pub struct FilePathInjector {
    /// Name of the file path column.
    column_name: Arc<str>,
    /// Current file path to inject.
    current_path: Arc<str>,
}

impl FilePathInjector {
    /// Create a new injector with the given column name.
    pub fn new(column_name: impl Into<Arc<str>>) -> Self;

    /// Set the current file path (call when switching files in multi-file reading).
    pub fn set_path(&mut self, path: impl Into<Arc<str>>);

    /// Add the file path column to a DataFrame.
    /// Creates a String column with the path repeated for each row.
    pub fn add_to_dataframe(&self, df: DataFrame) -> PolarsResult<DataFrame>;
}
```

Location: `src/api/file_path.rs`

### 8. Error Mode Mapping

The `ignore_errors` Python parameter maps to the existing `ErrorMode` enum:

```rust
/// Error handling mode (existing implementation in src/convert/dataframe.rs)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ErrorMode {
    /// Fail immediately on any error (ignore_errors=False)
    #[default]
    Strict,
    /// Skip bad records/blocks and continue (ignore_errors=True)
    Skip,
}
```

The mapping is:
- `ignore_errors=False` (default) → `ErrorMode::Strict`
- `ignore_errors=True` → `ErrorMode::Skip`

In Skip mode:
- Bad blocks are skipped at the `PrefetchBuffer` level
- Bad records are skipped at the `DataFrameBuilder` level
- Errors are accumulated (not silently dropped) for inspection

Location: `src/convert/dataframe.rs` (ErrorMode), `src/reader/buffer.rs` (block-level), `src/reader/stream.rs` (integration)

### 9. Source Resolution

```rust
/// Resolved sources after glob expansion and validation
pub struct ResolvedSources {
    pub paths: Vec<String>,
    pub schema: AvroSchema,
}

impl ResolvedSources {
    pub async fn resolve(
        sources: &[String],
        glob: bool,
        s3_config: Option<&S3Config>,
    ) -> Result<Self, ReaderError> {
        let mut paths = Vec::new();

        for source in sources {
            if glob && is_glob_pattern(source) {
                let expanded = expand_glob(source, s3_config).await?;
                if expanded.is_empty() {
                    return Err(ReaderError::Source(
                        SourceError::NotFound(format!("No files match pattern: {}", source))
                    ));
                }
                paths.extend(expanded);
            } else {
                paths.push(source.clone());
            }
        }

        if paths.is_empty() {
            return Err(ReaderError::Configuration("No source files provided".into()));
        }

        // Validate all files have identical schemas (per Requirement 7)
        let schema = validate_unified_schema(&paths, s3_config).await?;
        Ok(Self { paths, schema })
    }
}
```

### 10. Row Index Tracking

```rust
use polars::prelude::{IdxSize, Series, DataFrame, PolarsResult};

/// Tracks row indices across batches and files
pub struct RowIndexTracker {
    name: PlSmallStr,
    current_offset: IdxSize,
}

impl RowIndexTracker {
    pub fn new(name: PlSmallStr, initial_offset: IdxSize) -> Self {
        Self { name, current_offset: initial_offset }
    }

    pub fn add_to_dataframe(&mut self, df: DataFrame) -> PolarsResult<DataFrame> {
        let height = df.height() as IdxSize;
        let indices: Vec<IdxSize> = (self.current_offset..self.current_offset + height).collect();
        self.current_offset += height;

        let index_series = Series::new(self.name.clone(), indices);

        let mut columns = vec![index_series.into_column()];
        columns.extend(df.get_columns().iter().cloned());
        DataFrame::new(columns)
    }
}
```

### 11. Python Module

```python
# python/jetliner/__init__.py

"""
Jetliner - High-performance Avro streaming reader for Polars DataFrames.

Provides scan_avro() and read_avro() as superior alternatives to pl.read_avro(),
with support for streaming, S3, glob patterns, and multi-file reading.
"""

from __future__ import annotations
from pathlib import Path
from typing import Sequence

import polars as pl

from .jetliner import (
    # API functions
    scan_avro as _scan_avro,
    read_avro as _read_avro,
    read_avro_schema as _read_avro_schema,
    open as _open,
    # Internal function (not in __all__)
    _resolve_avro_sources,
    # Classes
    AvroReader,
    MultiAvroReader,
)

# Exception types (defined in Python for proper inheritance hierarchy)
from .exceptions import (
    JetlinerError,
    DecodeError,
    ParseError,
    SourceError,
    SchemaError,
    CodecError,
    AuthenticationError,
    FileNotFoundError,
    PermissionError,
    ConfigurationError,
)

# Type aliases
FileSource = str | Path | Sequence[str] | Sequence[Path]

__all__ = [
    # API functions
    "scan_avro",
    "read_avro",
    "read_avro_schema",
    "open",
    # Classes
    "AvroReader",
    "MultiAvroReader",
    # Exception types (hierarchy: JetlinerError -> specific errors)
    "JetlinerError",
    "DecodeError",
    "ParseError",
    "SourceError",
    "SchemaError",
    "CodecError",
    "AuthenticationError",
    "FileNotFoundError",
    "PermissionError",
    "ConfigurationError",
    # Type aliases
    "FileSource",
]
```

**Note:** `AvroReaderCore` has been eliminated - all single-file streaming functionality is now in `AvroReader`. `MultiAvroReader` is newly exposed for multi-file streaming.

## Implementation Phases

### Phase 1: Rust API Foundation (1-2 days)

1. Create `AvroOptions` struct (separate from scan args)
2. Create `ScanArgsAvro` struct using Polars types (`PlSmallStr`, `RowIndex`)
3. Create `MissingColumnsPolicy` enum
4. Update `CloudOptions` with `endpoint` key and `max_retries`
5. Add `impl From<ReaderError> for PolarsError`
6. Implement column resolution (`ColumnSelection` enum)

### Phase 2: Multi-Source Infrastructure (2-3 days)

1. Implement `ResolvedSources` with schema validation
2. Implement `unify_schemas()` with `MissingColumnsPolicy` support
3. Implement `RowIndexTracker` using `IdxSize`
4. Implement `FilePathInjector`
5. Implement `MultiSourceReader` orchestration

### Phase 3: Public Rust API (1-2 days)

1. Create `src/api.rs` with public functions returning `PolarsResult`
2. Implement `read_avro()` with columns support
3. Implement `read_avro_schema()`
4. Implement `ScanConfig` and `resolve_scan_config()` as building blocks for Python's `scan_avro()`

### Phase 4: Python Bindings Update (2 days)

1. Update Python exception classes with structured metadata attributes
2. Implement `scan_avro` with flattened kwargs
3. Implement `read_avro` with flattened kwargs + columns
4. Implement `read_avro_schema`
5. Rename `strict` → `ignore_errors` (inverted)
6. Update `storage_options` to use `endpoint` key
7. Remove old `scan()` and `parse_avro_schema()` functions

### Phase 5: S3 Glob Support (1-2 days)

1. Implement `expand_s3_glob()` using S3 list_objects_v2
2. Handle S3 pagination
3. Implement retry logic with `max_retries`

### Phase 6: Cleanup (1-2 days)

1. Update all tests
2. Update documentation
3. Add `py.typed` marker

## Testing Strategy

### Rust Unit Tests

- `ScanArgsAvro` and `AvroOptions` default values
- `CloudOptions::from_dict()` with `endpoint` key
- `From<ReaderError> for PolarsError` conversion
- Column resolution (names and indices)
- Row index tracking with `IdxSize`
- `ScanConfig` creation and batch reading

### Python Tests

- `scan_avro()` with flattened kwargs
- `read_avro()` with columns parameter
- `ignore_errors` parameter (inverted from `strict`)
- `storage_options` with `endpoint` key
- Exception attributes accessible (`e.block_index`, `e.offset`, etc.)

## Non-Requirements

- **Rust-native `scan_avro()`** - `AnonymousScan` trait implementation is out of scope; Python is the primary use case
- `rechunk` parameter - not applicable for streaming
- `cache` parameter - handled by Polars query engine
- `low_memory` parameter - redundant with explicit buffer options
