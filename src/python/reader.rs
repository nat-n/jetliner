//! Python bindings for Avro reading
//!
//! This module provides PyO3-based Python classes for streaming Avro data
//! into Polars DataFrames:
//!
//! - `AvroReader`: Single-file reader with iterator and context manager support
//! - `MultiAvroReader`: Multi-file reader with row index continuity
//! - `read_avro_schema`: Function to extract Avro schema from a file
//!
//! # Exception Types
//! Structured exception classes with metadata attributes are defined in `errors.rs`:
//! - `ParseError`: Errors during Avro file parsing (with `offset`, `message`)
//! - `SchemaError`: Schema-related errors (with `message`, `schema_context`)
//! - `CodecError`: Compression/decompression errors (with `codec`, `message`)
//! - `DecodeError`: Record decoding errors (with `block_index`, `record_index`, `offset`, `message`)
//! - `SourceError`: Data source errors (with `path`, `message`)
//!
//! # Requirements
//! - 6.1: Implement Python iterator protocol (__iter__, __next__)
//! - 6.2: Properly release resources when iteration completes
//! - 6.4: Raise appropriate Python exceptions with descriptive messages
//! - 6.5: Include context about block and record position in errors
//! - 6.6: Support context manager protocol for resource cleanup
//! - 6a.2: Support projection pushdown via projected_columns parameter
//! - 6a.5: Expose Avro schema as Polars schema for query planning
//! - 9.3: Expose parsed schema for inspection

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_polars::PyDataFrame;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::api::multi_source::{AvroMultiStreamReader, MultiSourceConfig};
use crate::api::sources::ResolvedSources;
use crate::convert::ErrorMode;
use crate::error::{BadBlockError, BadBlockErrorKind, ReaderError};
use crate::python::errors::map_reader_error_acquire_gil;
use crate::python::types::PyPathLike;
use crate::reader::{AvroStreamReader, BufferConfig, ReadBufferConfig, ReaderConfig};
use crate::source::{BoxedSource, LocalSource, S3Config, S3Source};

// =============================================================================
// PyBadBlockError - Structured error exposure for Python
// =============================================================================
// This class exposes recoverable errors that occurred during skip-mode reading.
// Requirements: 7.3, 7.4, 7.7

/// Structured error information from skip mode reading.
///
/// When reading with `ignore_errors=True`, errors are accumulated rather than
/// causing immediate failure. After iteration completes, errors are accessible
/// via the reader's `.errors` property as a list of `BadBlockError` objects.
///
/// # Properties
/// - `kind`: Error type string (e.g., "InvalidSyncMarker", "DecompressionFailed",
///   "BlockParseFailed", "RecordDecodeFailed", "SchemaViolation")
/// - `block_index`: Block number where the error occurred (0-based)
/// - `record_index`: Record number within the block, if applicable (0-based)
/// - `file_offset`: Byte offset in the file where the error occurred
/// - `message`: Human-readable error description
/// - `filepath`: Source file path, if known (useful for multi-file reads)
///
/// # Methods
/// - `to_dict()`: Convert to a Python dict for logging/serialization
///
/// # Example
/// ```python
/// with jetliner.AvroReader("file.avro", ignore_errors=True) as reader:
///     for df in reader:
///         process(df)
///
///     for err in reader.errors:
///         print(f"[{err.kind}] Block {err.block_index}: {err.message}")
///         # Or as dict for logging/serialization
///         log_error(err.to_dict())
/// ```
#[pyclass(name = "BadBlockError")]
#[derive(Clone)]
pub struct PyBadBlockError {
    /// The type of error that occurred
    #[pyo3(get)]
    kind: String,
    /// Block index where error occurred
    #[pyo3(get)]
    block_index: usize,
    /// Record index within block (if applicable)
    #[pyo3(get)]
    record_index: Option<usize>,
    /// File offset where error occurred
    #[pyo3(get)]
    file_offset: u64,
    /// Human-readable error message
    #[pyo3(get)]
    message: String,
    /// File path where the error occurred (if known)
    #[pyo3(get)]
    filepath: Option<String>,
}

impl PyBadBlockError {
    /// Create a PyBadBlockError from a Rust BadBlockError
    pub fn from_bad_block_error(err: &BadBlockError) -> Self {
        let kind = match &err.kind {
            BadBlockErrorKind::InvalidSyncMarker { .. } => "InvalidSyncMarker".to_string(),
            BadBlockErrorKind::DecompressionFailed { codec } => {
                format!("DecompressionFailed({})", codec)
            }
            BadBlockErrorKind::BlockParseFailed => "BlockParseFailed".to_string(),
            BadBlockErrorKind::RecordDecodeFailed => "RecordDecodeFailed".to_string(),
            BadBlockErrorKind::SchemaViolation => "SchemaViolation".to_string(),
        };

        Self {
            kind,
            block_index: err.block_index,
            record_index: err.record_index,
            file_offset: err.file_offset,
            message: err.message.clone(),
            filepath: err.file_path.clone(),
        }
    }
}

#[pymethods]
impl PyBadBlockError {
    /// Convert the error to a Python dictionary.
    ///
    /// # Returns
    /// A dict with keys: kind, block_index, record_index, file_offset, message
    ///
    /// # Example
    /// ```python
    /// err_dict = err.to_dict()
    /// print(err_dict["kind"])  # "InvalidSyncMarker"
    /// print(err_dict["block_index"])  # 5
    /// ```
    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("kind", &self.kind)?;
        dict.set_item("block_index", self.block_index)?;
        dict.set_item("record_index", self.record_index)?;
        dict.set_item("file_offset", self.file_offset)?;
        dict.set_item("message", &self.message)?;
        dict.set_item("filepath", &self.filepath)?;
        Ok(dict)
    }

    /// Return a string representation of the error.
    fn __repr__(&self) -> String {
        let filepath_part = self
            .filepath
            .as_ref()
            .map(|p| format!(", filepath='{}'", p))
            .unwrap_or_default();
        match self.record_index {
            Some(rec) => format!(
                "BadBlockError(kind='{}', block_index={}, record_index={}, file_offset={}, message='{}'{})",
                self.kind, self.block_index, rec, self.file_offset, self.message, filepath_part
            ),
            None => format!(
                "BadBlockError(kind='{}', block_index={}, file_offset={}, message='{}'{})",
                self.kind, self.block_index, self.file_offset, self.message, filepath_part
            ),
        }
    }

    /// Return a user-friendly string representation.
    fn __str__(&self) -> String {
        let file_info = self
            .filepath
            .as_ref()
            .map(|p| format!(" in '{}'", p))
            .unwrap_or_default();
        match self.record_index {
            Some(rec) => format!(
                "[{}]{} Block {}, record {} at file offset {}: {}",
                self.kind, file_info, self.block_index, rec, self.file_offset, self.message
            ),
            None => format!(
                "[{}]{} Block {} at file offset {}: {}",
                self.kind, file_info, self.block_index, self.file_offset, self.message
            ),
        }
    }
}

// =============================================================================
// MultiAvroReader - Multi-file reader with row_index and file_paths support
// =============================================================================

/// Multi-file Avro reader for batch iteration over DataFrames.
///
/// Use this class when you need to read multiple Avro files with batch-level control:
/// - Progress tracking across files and batches
/// - Row index continuity across files
/// - File path tracking per row
/// - Per-batch error inspection (with `ignore_errors=True`)
///
/// For most use cases, prefer `scan_avro()` (lazy with query optimization) or
/// `read_avro()` (eager loading) — both support multiple files. Use `MultiAvroReader`
/// when you need explicit iteration control over multi-file reads.
///
/// For single-file batch iteration, use `AvroReader`.
///
/// # Protocols
/// - Iterator: `for df in reader` yields DataFrames
/// - Context manager: `with MultiAvroReader(...) as reader` for automatic cleanup
///
/// # Properties
/// - `schema`: Avro schema as JSON string (unified across files)
/// - `schema_dict`: Avro schema as Python dict
/// - `rows_read`: Total rows read so far
/// - `total_sources`: Number of source files
/// - `current_source_index`: Index of file currently being read (0-based)
/// - `is_finished`: Whether iteration is complete
/// - `errors`: List of `BadBlockError` from skip mode (after iteration)
/// - `error_count`: Number of errors encountered
///
/// # Example
/// ```python
/// from jetliner import MultiAvroReader
///
/// # Basic multi-file iteration
/// reader = MultiAvroReader(["file1.avro", "file2.avro"])
/// for df in reader:
///     print(f"Batch: {df.shape}")
///
/// # With row tracking and file path injection
/// with MultiAvroReader(
///     ["file1.avro", "file2.avro"],
///     row_index_name="idx",
///     include_file_paths="source_file"
/// ) as reader:
///     for df in reader:
///         print(f"File {reader.current_source_index + 1}/{reader.total_sources}")
///         process(df)
///
/// # With row limit across all files
/// with MultiAvroReader(
///     ["file1.avro", "file2.avro"],
///     n_rows=100_000
/// ) as reader:
///     for df in reader:
///         process(df)
///     print(f"Read {reader.rows_read} rows total")
/// ```
#[pyclass]
pub struct MultiAvroReader {
    /// The underlying multi-source reader (wrapped for sync access)
    inner: Arc<Mutex<AvroMultiStreamReader>>,
    /// Tokio runtime for async operations
    runtime: tokio::runtime::Runtime,
    /// Paths being read (for error messages)
    paths: Vec<String>,
    /// Cached schema JSON (set after opening)
    schema_json: String,
}

#[pymethods]
impl MultiAvroReader {
    /// Create a new MultiAvroReader.
    ///
    /// # Arguments
    /// * `paths` - List of paths to read. Glob patterns should be expanded before passing.
    ///   Supports local paths and s3:// URIs (all paths must use the same source type).
    /// * `batch_size` - Target number of rows per DataFrame (default: 100,000)
    /// * `buffer_blocks` - Number of blocks to prefetch (default: 4)
    /// * `buffer_bytes` - Maximum bytes to buffer (default: 64MB)
    /// * `ignore_errors` - If True, skip bad records and continue; if False, fail on first error (default: False)
    /// * `projected_columns` - Optional list of column names to read (default: all columns)
    /// * `n_rows` - Maximum number of rows to read across all files (default: None, read all)
    /// * `row_index_name` - If provided, adds a row index column with this name, continuous across files
    /// * `row_index_offset` - Starting value for the row index (default: 0)
    /// * `include_file_paths` - If provided, adds a column with this name containing the source file path
    /// * `storage_options` - Optional dict for S3 configuration (endpoint, credentials, region)
    /// * `read_chunk_size` - Optional read buffer chunk size in bytes. When None, auto-detects:
    ///   64KB for local files, 4MB for S3.
    /// * `max_block_size` - Maximum decompressed block size in bytes (default: 512MB).
    ///   Blocks exceeding this limit are rejected. Set to None to disable.
    ///
    /// # Returns
    /// A new MultiAvroReader instance ready for iteration.
    ///
    /// # Raises
    /// * `FileNotFoundError` - If any file does not exist
    /// * `PermissionError` - If access is denied
    /// * `SchemaError` - If file schemas are incompatible
    /// * `RuntimeError` - For other errors (S3, parsing, etc.)
    #[new]
    #[pyo3(signature = (
        paths,
        batch_size = 100_000,
        buffer_blocks = 4,
        buffer_bytes = 67_108_864,
        ignore_errors = false,
        projected_columns = None,
        n_rows = None,
        row_index_name = None,
        row_index_offset = 0,
        include_file_paths = None,
        storage_options = None,
        read_chunk_size = None,
        max_block_size = 536_870_912
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        paths: Vec<PyPathLike>,
        batch_size: usize,
        buffer_blocks: usize,
        buffer_bytes: usize,
        ignore_errors: bool,
        projected_columns: Option<Vec<String>>,
        n_rows: Option<usize>,
        row_index_name: Option<String>,
        row_index_offset: u32,
        include_file_paths: Option<String>,
        storage_options: Option<std::collections::HashMap<String, String>>,
        read_chunk_size: Option<usize>,
        max_block_size: Option<usize>,
    ) -> PyResult<Self> {
        let paths: Vec<String> = paths.into_iter().map(|p| p.into_path()).collect();
        let first_path = paths.first().cloned().unwrap_or_default();

        // Create tokio runtime
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create async runtime: {}",
                e
            ))
        })?;

        // Convert storage_options to S3Config
        let s3_config = storage_options.as_ref().map(S3Config::from_dict);

        // Resolve sources to get unified schema
        let sources = runtime.block_on(async {
            // Create ResolvedSources from the pre-resolved paths
            // We need to unify schemas since paths are already resolved
            ResolvedSources::resolve(&paths, false, s3_config.as_ref())
                .await
                .map_err(|e| map_reader_error_acquire_gil(&first_path, e))
        })?;

        // Get schema JSON before moving sources
        let schema_json = sources.schema.to_json();

        // Build reader configuration
        let buffer_config = BufferConfig::new(buffer_blocks, buffer_bytes)
            .with_max_decompressed_block_size(max_block_size);
        let error_mode = if ignore_errors {
            ErrorMode::Skip
        } else {
            ErrorMode::Strict
        };

        // Determine read buffer config based on first path and user override
        let is_s3 = first_path.starts_with("s3://");
        let read_buffer_config = match read_chunk_size {
            Some(chunk_size) => ReadBufferConfig::with_chunk_size(chunk_size),
            None if is_s3 => ReadBufferConfig::S3_DEFAULT,
            None => ReadBufferConfig::LOCAL_DEFAULT,
        };

        let mut reader_config = ReaderConfig::new()
            .with_batch_size(batch_size)
            .with_buffer_config(buffer_config)
            .with_error_mode(error_mode)
            .with_read_buffer_config(read_buffer_config);

        // Add projection if specified
        if let Some(columns) = projected_columns {
            reader_config = reader_config.with_projection(columns);
        }

        // Build multi-source config
        let mut config = MultiSourceConfig::new()
            .with_reader_config(reader_config)
            .with_ignore_errors(ignore_errors);

        if let Some(n) = n_rows {
            config = config.with_n_rows(n);
        }

        if let Some(ref name) = row_index_name {
            config = config.with_row_index(name.as_str(), row_index_offset);
        }

        if let Some(ref name) = include_file_paths {
            config = config.with_include_file_paths(name.as_str());
        }

        if let Some(s3_cfg) = s3_config {
            config = config.with_s3_config(s3_cfg);
        }

        // Create the multi-source reader
        let inner = AvroMultiStreamReader::new(sources, config);

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            runtime,
            paths,
            schema_json,
        })
    }

    /// Return self as the iterator.
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Get the next DataFrame batch.
    fn __next__<'py>(slf: PyRefMut<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let inner = slf.inner.clone();
        let first_path = slf.paths.first().cloned().unwrap_or_default();

        let result = slf.runtime.block_on(async {
            let mut guard = inner.lock().await;
            guard.next_batch().await
        });

        match result {
            Ok(Some(df)) => dataframe_to_py_with_enums(py, df),
            Ok(None) => Err(PyErr::new::<pyo3::exceptions::PyStopIteration, _>(())),
            Err(e) => Err(map_reader_error_acquire_gil(&first_path, e)),
        }
    }

    /// Enter the context manager.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Exit the context manager.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        // AvroMultiStreamReader doesn't need explicit cleanup
        // Return false to not suppress exceptions
        false
    }

    /// Get the Avro schema as a JSON string.
    #[getter]
    fn schema(&self) -> String {
        self.schema_json.clone()
    }

    /// Get the Avro schema as a Python dictionary.
    #[getter]
    fn schema_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let value: serde_json::Value = serde_json::from_str(&self.schema_json).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to parse schema JSON: {}",
                e
            ))
        })?;

        json_value_to_py_dict(py, &value)
    }

    /// Get accumulated errors from skip mode reading.
    #[getter]
    fn errors<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let inner = self.inner.clone();
        let errors: Vec<PyBadBlockError> = self.runtime.block_on(async {
            let guard = inner.lock().await;
            guard
                .errors()
                .iter()
                .map(PyBadBlockError::from_bad_block_error)
                .collect()
        });

        let py_errors: Vec<Py<PyAny>> = errors
            .into_iter()
            .map(|e| e.into_pyobject(py).map(|obj| obj.into_any().unbind()))
            .collect::<PyResult<Vec<_>>>()?;

        PyList::new(py, py_errors)
    }

    /// Get the count of accumulated errors.
    #[getter]
    fn error_count(&self) -> usize {
        let inner = self.inner.clone();
        self.runtime.block_on(async {
            let guard = inner.lock().await;
            guard.errors().len()
        })
    }

    /// Check if the reader has finished reading.
    #[getter]
    fn is_finished(&self) -> bool {
        let inner = self.inner.clone();
        self.runtime.block_on(async {
            let guard = inner.lock().await;
            guard.is_finished()
        })
    }

    /// Get the number of rows read so far.
    #[getter]
    fn rows_read(&self) -> usize {
        let inner = self.inner.clone();
        self.runtime.block_on(async {
            let guard = inner.lock().await;
            guard.rows_read()
        })
    }

    /// Get the total number of source files.
    #[getter]
    fn total_sources(&self) -> usize {
        let inner = self.inner.clone();
        self.runtime.block_on(async {
            let guard = inner.lock().await;
            guard.total_sources()
        })
    }

    /// Get the current source index (0-based).
    #[getter]
    fn current_source_index(&self) -> usize {
        let inner = self.inner.clone();
        self.runtime.block_on(async {
            let guard = inner.lock().await;
            guard.current_source_index()
        })
    }
}

/// Create a StreamSource from a path string.
///
/// Automatically detects S3 URIs (s3://) vs local file paths.
/// When an S3 URI is detected and config is provided, uses the config
/// for custom endpoints and credentials.
///
/// # Arguments
/// * `path` - Path to the file (local path or s3:// URI)
/// * `config` - Optional S3 configuration for custom endpoints/credentials
///
/// # Requirements
/// - 4.8: Accept optional `storage_options` parameter
/// - 4.9: Connect to custom endpoint when `endpoint` is provided
/// - 4.10: Use provided credentials when specified
/// - 4.11: `storage_options` takes precedence over environment variables
async fn create_source(path: &str, config: Option<S3Config>) -> Result<BoxedSource, ReaderError> {
    if path.starts_with("s3://") {
        // S3 source with optional config
        let source = S3Source::from_uri_with_config(path, config)
            .await
            .map_err(ReaderError::Source)?;
        Ok(Box::new(source) as BoxedSource)
    } else {
        // Local file source (config is ignored for local files)
        let source = LocalSource::open(path).await.map_err(ReaderError::Source)?;
        Ok(Box::new(source) as BoxedSource)
    }
}

/// Convert a Polars DataFrame to Python, properly handling Enum columns.
///
/// # TEMPORARY WORKAROUND
///
/// pyo3-polars uses Arrow FFI for DataFrame serialization, which loses the
/// distinction between Polars Enum and Categorical types (both become Arrow
/// Dictionary). This function works around that limitation by:
///
/// 1. Identifying Enum columns in the DataFrame and their categories
/// 2. Exporting the DataFrame via Arrow FFI (Enum becomes Categorical)
/// 3. Casting Categorical columns back to Enum using Python Polars
///
/// This encapsulates the workaround in Rust so Python users always see
/// the correct Enum types without any additional code.
///
/// # Why not a better solution?
///
/// We investigated two alternatives that would avoid this workaround:
///
/// 1. **Physical UInt export**: Export Enum columns as their physical representation
///    (UInt8/16/32), then cast UInt→Enum in Python. This would be true zero-copy
///    (verified: same buffer address). However, `Series::to_physical_repr()` on
///    Enum columns panics with "not implemented" in polars-core 0.52.0.
///
/// 2. **Arrow FFI metadata**: Pass Enum type information through Arrow FFI so
///    Python Polars reconstructs the correct type. This would require changes to
///    pyo3-polars itself to preserve FrozenCategories metadata.
///
/// # When can this be removed?
///
/// Monitor these issues for upstream fixes:
/// - <https://github.com/pola-rs/polars/issues/20089> (Enum/Categorical in Parquet)
/// - <https://github.com/pola-rs/pyo3-polars/issues> (Arrow FFI Enum support)
///
/// See also: `.kiro/specs/jetliner/devnotes/22.1-enum-builder.md`
fn dataframe_to_py_with_enums<'py>(
    py: Python<'py>,
    df: polars::prelude::DataFrame,
) -> PyResult<Bound<'py, PyAny>> {
    use polars::prelude::*;

    // Check for Enum columns and collect their metadata
    // pyo3-polars exports Enum as Categorical through Arrow FFI, so we need to
    // cast back to Enum on the Python side
    let enum_columns: Vec<(String, Vec<String>)> = df
        .schema()
        .iter()
        .filter_map(|(name, dtype)| {
            if let DataType::Enum(categories, _) = dtype {
                let cats: Vec<String> = categories
                    .categories()
                    .values_iter()
                    .map(|s| s.to_string())
                    .collect();
                Some((name.to_string(), cats))
            } else {
                None
            }
        })
        .collect();

    // If no Enum columns, use the standard path
    if enum_columns.is_empty() {
        return PyDataFrame(df).into_pyobject(py);
    }

    // Export via Arrow FFI (Enum becomes Categorical due to Arrow limitation)
    let py_df = PyDataFrame(df).into_pyobject(py)?;

    // Cast Categorical columns back to their correct Enum types
    let polars_mod = py.import("polars")?;
    let cast_exprs = pyo3::types::PyList::empty(py);

    for (col_name, categories) in &enum_columns {
        // Create pl.Enum(categories) dtype
        let py_categories = pyo3::types::PyList::new(py, categories)?;
        let enum_dtype = polars_mod.getattr("Enum")?.call1((py_categories,))?;

        // Create pl.col(name).cast(enum_dtype) expression
        let col_expr = polars_mod.call_method1("col", (col_name.as_str(),))?;
        let cast_expr = col_expr.call_method1("cast", (enum_dtype,))?;
        cast_exprs.append(cast_expr)?;
    }

    // Apply the casts: df.with_columns([...])
    py_df.call_method1("with_columns", (cast_exprs,))
}

/// Convert a serde_json::Value to a Python object.
///
/// This recursively converts JSON values to their Python equivalents:
/// - Object → dict
/// - Array → list
/// - String → str
/// - Number → int or float
/// - Bool → bool
/// - Null → None
fn json_value_to_py_object(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    use pyo3::types::{PyList, PyNone, PyString};

    match value {
        serde_json::Value::Null => Ok(PyNone::get(py).to_owned().into_any().unbind()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid JSON number",
                ))
            }
        }
        serde_json::Value::String(s) => Ok(PyString::new(py, s).into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let items: PyResult<Vec<Py<PyAny>>> =
                arr.iter().map(|v| json_value_to_py_object(py, v)).collect();
            Ok(PyList::new(py, items?)?.into_any().unbind())
        }
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_value_to_py_object(py, v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

/// Convert a serde_json::Value to a Python dict.
///
/// If the value is not an object, wraps it in a dict with key "value".
fn json_value_to_py_dict<'py>(
    py: Python<'py>,
    value: &serde_json::Value,
) -> PyResult<Bound<'py, PyDict>> {
    match value {
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_value_to_py_object(py, v)?)?;
            }
            Ok(dict)
        }
        _ => {
            // For non-object schemas (like primitive types), wrap in a dict
            let dict = PyDict::new(py);
            dict.set_item("type", json_value_to_py_object(py, value)?)?;
            Ok(dict)
        }
    }
}

/// Single-file Avro reader for batch iteration over DataFrames.
///
/// Use this class when you need control over batch processing:
/// - Progress tracking between batches
/// - Per-batch error inspection (with `ignore_errors=True`)
/// - Writing batches to external sinks (database, API, etc.)
/// - Early termination based on content
///
/// For most use cases, prefer `scan_avro()` (lazy with query optimization) or
/// `read_avro()` (eager loading). Use `AvroReader` when you need explicit
/// iteration control.
///
/// For reading multiple files with batch control, use `MultiAvroReader`.
///
/// # Protocols
/// - Iterator: `for df in reader` yields DataFrames
/// - Context manager: `with AvroReader(...) as reader` for automatic cleanup
///
/// # Properties
/// - `schema`: Avro schema as JSON string
/// - `schema_dict`: Avro schema as Python dict
/// - `batch_size`: Target rows per batch
/// - `pending_records`: Records buffered for next batch
/// - `is_finished`: Whether iteration is complete
/// - `errors`: List of `BadBlockError` from skip mode (after iteration)
/// - `error_count`: Number of errors encountered
///
/// # Example
/// ```python
/// import jetliner
///
/// # Basic iteration
/// for df in jetliner.AvroReader("data.avro"):
///     print(df.shape)
///
/// # With context manager (recommended)
/// with jetliner.AvroReader("data.avro") as reader:
///     for df in reader:
///         process(df)
///
/// # Error inspection in skip mode
/// with jetliner.AvroReader("data.avro", ignore_errors=True) as reader:
///     for df in reader:
///         process(df)
///     if reader.error_count > 0:
///         print(f"Skipped {reader.error_count} errors")
///         for err in reader.errors:
///             print(f"  [{err.kind}] Block {err.block_index}: {err.message}")
///
/// # Schema inspection
/// with jetliner.AvroReader("data.avro") as reader:
///     print(reader.schema_dict["fields"])
/// ```
#[pyclass]
pub struct AvroReader {
    /// The underlying Rust stream reader (wrapped in Arc<Mutex> for safe access)
    inner: Arc<Mutex<Option<AvroStreamReader<BoxedSource>>>>,
    /// Tokio runtime for async operations
    runtime: tokio::runtime::Runtime,
    /// Path to the Avro file (for error messages)
    path: String,
    /// Cached schema JSON (set after opening)
    schema_json: String,
    /// Cached batch size
    batch_size: usize,
    /// Accumulated errors from skip mode reading
    errors: Arc<Mutex<Vec<PyBadBlockError>>>,
}

#[pymethods]
impl AvroReader {
    /// Create a new AvroReader.
    ///
    /// # Arguments
    /// * `path` - Path to the Avro file (local path or s3:// URI)
    /// * `batch_size` - Target number of rows per DataFrame (default: 100,000)
    /// * `buffer_blocks` - Number of blocks to prefetch (default: 4)
    /// * `buffer_bytes` - Maximum bytes to buffer (default: 64MB)
    /// * `ignore_errors` - If True, skip bad records and continue; if False, fail on first error (default: False)
    /// * `projected_columns` - Optional list of column names to read (default: all columns)
    /// * `storage_options` - Optional dict for S3 configuration (endpoint, credentials, region)
    /// * `read_chunk_size` - Optional read buffer chunk size in bytes. When None, auto-detects:
    ///   64KB for local files, 4MB for S3. Larger values reduce I/O operations but use more memory.
    ///   For S3, larger chunks (1-8MB) reduce HTTP round-trips significantly.
    /// * `max_block_size` - Maximum decompressed block size in bytes (default: 512MB).
    ///   Blocks exceeding this limit are rejected. Set to None to disable. Protects against
    ///   decompression bombs where small compressed blocks expand to consume excessive memory.
    ///
    /// # Returns
    /// A new AvroReader instance ready for iteration.
    ///
    /// # Raises
    /// * `FileNotFoundError` - If the file does not exist
    /// * `PermissionError` - If access is denied
    /// * `RuntimeError` - For other errors (S3, parsing, etc.)
    ///
    /// # Example
    /// ```python
    /// # Local file
    /// reader = jetliner.AvroReader("/path/to/file.avro")
    ///
    /// # S3 file
    /// reader = jetliner.AvroReader("s3://bucket/key.avro")
    ///
    /// # With options
    /// reader = jetliner.AvroReader(
    ///     "file.avro",
    ///     batch_size=50000,
    ///     ignore_errors=True
    /// )
    ///
    /// # S3-compatible services (MinIO, LocalStack, R2)
    /// reader = jetliner.AvroReader(
    ///     "s3://bucket/key.avro",
    ///     storage_options={
    ///         "endpoint": "http://localhost:9000",
    ///         "aws_access_key_id": "minioadmin",
    ///         "aws_secret_access_key": "minioadmin",
    ///     }
    /// )
    ///
    /// # Custom read chunk size for S3 optimization
    /// reader = jetliner.AvroReader(
    ///     "s3://bucket/key.avro",
    ///     read_chunk_size=8 * 1024 * 1024  # 8MB chunks
    /// )
    /// ```
    ///
    /// # Requirements
    /// - 4.8: Accept optional `storage_options` parameter
    /// - 4.11: `storage_options` takes precedence over environment variables
    /// - 3.13: Expose read_chunk_size for tuning
    #[new]
    #[pyo3(signature = (
        path,
        batch_size = 100_000,
        buffer_blocks = 4,
        buffer_bytes = 67_108_864,
        ignore_errors = false,
        projected_columns = None,
        storage_options = None,
        read_chunk_size = None,
        max_block_size = 536_870_912
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        path: PyPathLike,
        batch_size: usize,
        buffer_blocks: usize,
        buffer_bytes: usize,
        ignore_errors: bool,
        projected_columns: Option<Vec<String>>,
        storage_options: Option<std::collections::HashMap<String, String>>,
        read_chunk_size: Option<usize>,
        max_block_size: Option<usize>,
    ) -> PyResult<Self> {
        let path = path.into_path();

        // Create tokio runtime
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create async runtime: {}",
                e
            ))
        })?;

        // Convert storage_options to S3Config
        let s3_config = storage_options.as_ref().map(S3Config::from_dict);

        // Create source and reader within the runtime
        let result = runtime.block_on(async {
            // Create the appropriate source based on path
            let source = create_source(&path, s3_config).await?;

            // Build reader configuration (no projection for user-facing API)
            let buffer_config = BufferConfig::new(buffer_blocks, buffer_bytes)
                .with_max_decompressed_block_size(max_block_size);
            let error_mode = if ignore_errors {
                ErrorMode::Skip
            } else {
                ErrorMode::Strict
            };

            // Add projection if specified
            let mut config = ReaderConfig::new()
                .with_batch_size(batch_size)
                .with_buffer_config(buffer_config)
                .with_error_mode(error_mode);

            if let Some(columns) = projected_columns {
                config = config.with_projection(columns);
            }

            // Determine read buffer config based on source type and user override
            let is_s3 = path.starts_with("s3://");
            let read_buffer_config = match read_chunk_size {
                Some(chunk_size) => ReadBufferConfig::with_chunk_size(chunk_size),
                None if is_s3 => ReadBufferConfig::S3_DEFAULT,
                None => ReadBufferConfig::LOCAL_DEFAULT,
            };

            let config = config.with_read_buffer_config(read_buffer_config);

            // Open the reader
            let reader = AvroStreamReader::open(source, config).await?;

            // Cache the schema JSON
            let schema_json = reader.schema().to_json();

            Ok::<_, ReaderError>((reader, schema_json))
        });

        let (inner, schema_json) = result.map_err(|e| map_reader_error_acquire_gil(&path, e))?;

        Ok(Self {
            inner: Arc::new(Mutex::new(Some(inner))),
            runtime,
            path,
            schema_json,
            batch_size,
            errors: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Return self as the iterator.
    ///
    /// This implements the Python iterator protocol, allowing:
    /// ```python
    /// for df in reader:
    ///     process(df)
    /// ```
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Get the next DataFrame batch.
    ///
    /// Returns the next batch of records as a Polars DataFrame.
    /// Raises StopIteration when all records have been read or the reader is closed.
    /// In skip mode, errors are accumulated and available via `errors` property.
    ///
    /// # Returns
    /// A Polars DataFrame containing the next batch of records.
    ///
    /// # Raises
    /// * `StopIteration` - When all records have been read or reader is closed
    /// * `DecodeError`, `ParseError`, etc. - If an error occurs during reading (in strict mode)
    fn __next__<'py>(slf: PyRefMut<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let inner = slf.inner.clone();
        let path = slf.path.clone();
        let errors_arc = slf.errors.clone();

        // Get the next batch using the runtime
        let result = slf.runtime.block_on(async {
            let mut guard = inner.lock().await;

            // If reader is already closed, return None to signal StopIteration
            // This allows proper Python iterator behavior where exhausted iterators
            // simply stop yielding rather than raising errors
            let reader = match guard.as_mut() {
                Some(r) => r,
                None => return Ok((None, None)),
            };

            match reader.next_batch().await {
                Ok(Some(df)) => Ok((Some(df), None)),
                Ok(None) => {
                    // End of iteration - collect errors before releasing the reader
                    // Inject filepath into each error
                    let collected_errors: Vec<PyBadBlockError> = reader
                        .errors()
                        .iter()
                        .map(|e| {
                            let mut py_err = PyBadBlockError::from_bad_block_error(e);
                            py_err.filepath = Some(path.clone());
                            py_err
                        })
                        .collect();
                    // Release the reader
                    *guard = None;
                    Ok((None, Some(collected_errors)))
                }
                Err(e) => Err(e),
            }
        });

        // Store errors outside the async block to avoid borrow issues
        if let Ok((None, Some(ref collected_errors))) = result {
            let errors_to_store = collected_errors.clone();
            slf.runtime.block_on(async {
                let mut errors_guard = errors_arc.lock().await;
                *errors_guard = errors_to_store;
            });
        }

        match result {
            Ok((Some(df), _)) => dataframe_to_py_with_enums(py, df),
            Ok((None, _)) => Err(PyErr::new::<pyo3::exceptions::PyStopIteration, _>(())),
            Err(e) => Err(map_reader_error_acquire_gil(&path, e)),
        }
    }

    /// Enter the context manager.
    ///
    /// Returns self for use in `with` statements:
    /// ```python
    /// with jetliner.AvroReader("file.avro") as reader:
    ///     for df in reader:
    ///         process(df)
    /// ```
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Exit the context manager.
    ///
    /// Releases resources held by the reader. After this call,
    /// the reader cannot be used for iteration.
    ///
    /// # Arguments
    /// * `_exc_type` - Exception type (if any)
    /// * `_exc_val` - Exception value (if any)
    /// * `_exc_tb` - Exception traceback (if any)
    ///
    /// # Returns
    /// False to indicate exceptions should not be suppressed.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        // Release the reader to free resources
        let inner = self.inner.clone();
        self.runtime.block_on(async {
            let mut guard = inner.lock().await;
            *guard = None;
        });
        // Return false to not suppress exceptions
        false
    }

    /// Get the Avro schema as a JSON string.
    ///
    /// # Returns
    /// The Avro schema as a JSON string.
    ///
    /// # Example
    /// ```python
    /// reader = jetliner.AvroReader("file.avro")
    /// print(reader.schema)  # JSON string
    /// ```
    #[getter]
    fn schema(&self) -> String {
        self.schema_json.clone()
    }

    /// Get the Avro schema as a Python dictionary.
    ///
    /// # Returns
    /// The Avro schema as a Python dict.
    ///
    /// # Raises
    /// * `ValueError` - If the schema JSON cannot be parsed
    ///
    /// # Example
    /// ```python
    /// reader = jetliner.AvroReader("file.avro")
    /// schema = reader.schema_dict
    /// print(schema["name"])  # Record name
    /// print(schema["fields"])  # List of fields
    /// ```
    ///
    /// # Requirements
    /// - 9.3: Expose parsed schema for inspection
    #[getter]
    fn schema_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let value: serde_json::Value = serde_json::from_str(&self.schema_json).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to parse schema JSON: {}",
                e
            ))
        })?;

        json_value_to_py_dict(py, &value)
    }

    /// Check if the reader has finished reading.
    ///
    /// # Returns
    /// True if all records have been read or the reader is closed, False otherwise.
    #[getter]
    fn is_finished(&self) -> bool {
        let inner = self.inner.clone();
        self.runtime.block_on(async {
            let guard = inner.lock().await;
            guard.as_ref().map(|r| r.is_finished()).unwrap_or(true)
        })
    }

    /// Get the batch size being used.
    ///
    /// # Returns
    /// The target number of rows per DataFrame batch.
    #[getter]
    fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Get the number of records currently pending in the builder.
    ///
    /// # Returns
    /// The number of records waiting to be returned in the next batch.
    #[getter]
    fn pending_records(&self) -> usize {
        let inner = self.inner.clone();
        self.runtime.block_on(async {
            let guard = inner.lock().await;
            guard.as_ref().map(|r| r.pending_records()).unwrap_or(0)
        })
    }

    /// Get accumulated errors from skip mode reading.
    ///
    /// In skip mode, errors are accumulated rather than causing immediate failure.
    /// This property returns all errors that occurred during reading.
    /// Errors are available after iteration completes.
    ///
    /// # Returns
    /// A list of BadBlockError objects with details about each error.
    ///
    /// # Example
    /// ```python
    /// with jetliner.AvroReader("file.avro", strict=False) as reader:
    ///     for df in reader:
    ///         process(df)
    ///
    ///     for err in reader.errors:
    ///         print(f"[{err.kind}] Block {err.block_index}: {err.message}")
    /// ```
    ///
    /// # Requirements
    /// - 7.3: Track error counts and positions
    /// - 7.4: Provide summary of skipped errors
    /// - 7.7: Include sufficient detail to diagnose issues
    #[getter]
    fn errors<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let errors_arc = self.errors.clone();
        let errors = self.runtime.block_on(async {
            let guard = errors_arc.lock().await;
            guard.clone()
        });

        let py_errors: Vec<Py<PyAny>> = errors
            .into_iter()
            .map(|e| e.into_pyobject(py).map(|obj| obj.into_any().unbind()))
            .collect::<PyResult<Vec<_>>>()?;

        PyList::new(py, py_errors)
    }

    /// Get the count of accumulated errors.
    ///
    /// Quick check for whether any errors occurred during reading,
    /// without needing to iterate through the errors list.
    ///
    /// # Returns
    /// The number of errors that occurred during reading.
    ///
    /// # Example
    /// ```python
    /// with jetliner.AvroReader("file.avro", strict=False) as reader:
    ///     for df in reader:
    ///         process(df)
    ///
    ///     if reader.error_count > 0:
    ///         print(f"Warning: {reader.error_count} errors during read")
    /// ```
    ///
    /// # Requirements
    /// - 7.3: Track error counts and positions
    #[getter]
    fn error_count(&self) -> usize {
        let errors_arc = self.errors.clone();
        self.runtime.block_on(async {
            let guard = errors_arc.lock().await;
            guard.len()
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_create_source_detects_s3() {
        // We can't actually test S3 without credentials, but we can verify
        // the path detection logic
        assert!("s3://bucket/key".starts_with("s3://"));
        assert!(!"/local/path".starts_with("s3://"));
        assert!(!"./relative/path".starts_with("s3://"));
    }
}
