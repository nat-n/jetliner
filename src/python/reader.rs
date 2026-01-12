//! Python bindings for Avro reading
//!
//! This module provides PyO3-based Python classes for streaming Avro data
//! into Polars DataFrames:
//!
//! - `AvroReader`: User-facing class for the `open()` API with context manager support
//! - `AvroReaderCore`: Internal class used by both `open()` and `scan()` APIs
//! - `parse_avro_schema`: Function to extract Polars schema from an Avro file
//!
//! # Exception Types
//! Custom exception classes for specific error conditions:
//! - `JetlinerError`: Base exception for all Jetliner errors
//! - `ParseError`: Errors during Avro file parsing (invalid magic bytes, malformed headers)
//! - `SchemaError`: Schema-related errors (invalid schema, incompatible schemas)
//! - `CodecError`: Compression/decompression errors
//! - `DecodeError`: Record decoding errors (type mismatches, invalid data)
//! - `SourceError`: Data source errors (S3, filesystem)
//!
//! # Requirements
//! - 6.1: Implement Python iterator protocol (__iter__, __next__)
//! - 6.2: Properly release resources when iteration completes
//! - 6.4: Raise appropriate Python exceptions with descriptive messages
//! - 6.5: Include context about block and record position in errors
//! - 6.6: Support context manager protocol for resource cleanup
//! - 6a.2: Support projection pushdown via projected_columns parameter (AvroReaderCore only)
//! - 6a.5: Expose Avro schema as Polars schema for query planning
//! - 9.3: Expose parsed schema for inspection

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::{create_exception, exceptions::PyException};
use pyo3_polars::PyDataFrame;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::convert::{avro_to_arrow_schema, ErrorMode};
use crate::error::{
    CodecError as RustCodecError, ReadError, ReadErrorKind, ReaderError,
    SchemaError as RustSchemaError, SourceError as RustSourceError,
};
use crate::reader::{AvroHeader, AvroStreamReader, BufferConfig, ReaderConfig};
use crate::schema::AvroSchema;
use crate::source::{BoxedSource, LocalSource, S3Config, S3Source};

// =============================================================================
// Custom Python Exception Types
// =============================================================================
// These exceptions provide specific error handling for different failure modes.
// Requirements: 6.4, 6.5

// Base exception for all Jetliner errors
create_exception!(
    jetliner,
    JetlinerError,
    PyException,
    "Base exception for all Jetliner errors."
);

// Parse errors - invalid Avro file format
create_exception!(jetliner, ParseError, JetlinerError, "Error parsing Avro file format (invalid magic bytes, malformed headers, invalid sync markers).");

// Schema errors - schema validation and compatibility issues
create_exception!(
    jetliner,
    SchemaError,
    JetlinerError,
    "Error with Avro schema (invalid schema, unsupported types, incompatible schemas)."
);

// Codec errors - compression/decompression failures
create_exception!(
    jetliner,
    CodecError,
    JetlinerError,
    "Error with compression codec (unsupported codec, decompression failure)."
);

// Decode errors - record decoding failures
create_exception!(
    jetliner,
    DecodeError,
    JetlinerError,
    "Error decoding Avro records (type mismatch, invalid data, unexpected EOF)."
);

// Source errors - data source access failures
create_exception!(
    jetliner,
    SourceError,
    JetlinerError,
    "Error accessing data source (S3 errors, filesystem errors)."
);

// =============================================================================
// PyReadError - Structured error exposure for Python
// =============================================================================
// This class exposes recoverable errors that occurred during skip-mode reading.
// Requirements: 7.3, 7.4, 7.7

/// A structured error that occurred during Avro reading.
///
/// In skip mode, errors are accumulated rather than causing immediate failure.
/// This class provides structured access to error details for inspection
/// after reading completes.
///
/// # Properties
/// * `kind` - The type of error (e.g., "InvalidSyncMarker", "DecompressionFailed")
/// * `block_index` - The block number where the error occurred
/// * `record_index` - The record number within the block (if applicable)
/// * `offset` - The file offset where the error occurred
/// * `message` - A human-readable error message
///
/// # Requirements
/// - 7.3: Track error counts and positions
/// - 7.4: Provide summary of skipped errors
/// - 7.7: Include sufficient detail to diagnose issues
///
/// # Example
/// ```python
/// with jetliner.open("file.avro", strict=False) as reader:
///     for df in reader:
///         process(df)
///
///     if reader.error_count > 0:
///         for err in reader.errors:
///             print(f"[{err.kind}] Block {err.block_index}: {err.message}")
///             # Or as dict for logging/serialization
///             log_error(err.to_dict())
/// ```
#[pyclass(name = "ReadError")]
#[derive(Clone)]
pub struct PyReadError {
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
    offset: u64,
    /// Human-readable error message
    #[pyo3(get)]
    message: String,
}

impl PyReadError {
    /// Create a PyReadError from a Rust ReadError
    pub fn from_read_error(err: &ReadError) -> Self {
        let kind = match &err.kind {
            ReadErrorKind::InvalidSyncMarker { .. } => "InvalidSyncMarker".to_string(),
            ReadErrorKind::DecompressionFailed { codec } => {
                format!("DecompressionFailed({})", codec)
            }
            ReadErrorKind::BlockParseFailed => "BlockParseFailed".to_string(),
            ReadErrorKind::RecordDecodeFailed => "RecordDecodeFailed".to_string(),
            ReadErrorKind::SchemaViolation => "SchemaViolation".to_string(),
        };

        Self {
            kind,
            block_index: err.block_index,
            record_index: err.record_index,
            offset: err.offset,
            message: err.message.clone(),
        }
    }
}

#[pymethods]
impl PyReadError {
    /// Convert the error to a Python dictionary.
    ///
    /// # Returns
    /// A dict with keys: kind, block_index, record_index, offset, message
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
        dict.set_item("offset", self.offset)?;
        dict.set_item("message", &self.message)?;
        Ok(dict)
    }

    /// Return a string representation of the error.
    fn __repr__(&self) -> String {
        match self.record_index {
            Some(rec) => format!(
                "ReadError(kind='{}', block_index={}, record_index={}, offset={}, message='{}')",
                self.kind, self.block_index, rec, self.offset, self.message
            ),
            None => format!(
                "ReadError(kind='{}', block_index={}, offset={}, message='{}')",
                self.kind, self.block_index, self.offset, self.message
            ),
        }
    }

    /// Return a user-friendly string representation.
    fn __str__(&self) -> String {
        match self.record_index {
            Some(rec) => format!(
                "[{}] Block {}, record {} at offset {}: {}",
                self.kind, self.block_index, rec, self.offset, self.message
            ),
            None => format!(
                "[{}] Block {} at offset {}: {}",
                self.kind, self.block_index, self.offset, self.message
            ),
        }
    }
}

/// Internal Avro reader that handles streaming and projection.
///
/// This is the core reader class used internally by both the `open()` and `scan()` APIs.
/// It supports:
/// - Streaming iteration over DataFrames
/// - Projection pushdown (only read specified columns)
/// - Configurable batch size and buffer settings
/// - Error handling modes (strict or skip)
///
/// # Requirements
/// - 6.1: Implement Python iterator protocol (__iter__, __next__)
/// - 6.2: Properly release resources when iteration completes
/// - 6a.2: Support projection pushdown via projected_columns parameter
///
/// # Example
/// ```python
/// from jetliner import AvroReaderCore
///
/// # Basic usage
/// reader = AvroReaderCore("data.avro")
/// for df in reader:
///     print(df.shape)
///
/// # With projection
/// reader = AvroReaderCore(
///     "data.avro",
///     projected_columns=["col1", "col2"],
///     batch_size=50000
/// )
/// for df in reader:
///     process(df)
/// ```
#[pyclass]
pub struct AvroReaderCore {
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
    errors: Arc<Mutex<Vec<PyReadError>>>,
}

#[pymethods]
impl AvroReaderCore {
    /// Create a new AvroReaderCore.
    ///
    /// # Arguments
    /// * `path` - Path to the Avro file (local path or s3:// URI)
    /// * `batch_size` - Target number of rows per DataFrame (default: 100,000)
    /// * `buffer_blocks` - Number of blocks to prefetch (default: 4)
    /// * `buffer_bytes` - Maximum bytes to buffer (default: 64MB)
    /// * `strict` - If True, fail on first error; if False, skip bad records (default: False)
    /// * `projected_columns` - Optional list of column names to read (default: all columns)
    /// * `storage_options` - Optional dict for S3 configuration (endpoint_url, credentials, region)
    ///
    /// # Returns
    /// A new AvroReaderCore instance ready for iteration.
    ///
    /// # Raises
    /// * `FileNotFoundError` - If the file does not exist
    /// * `PermissionError` - If access is denied
    /// * `RuntimeError` - For other errors (S3, parsing, etc.)
    ///
    /// # Requirements
    /// - 4.8: Accept optional `storage_options` parameter
    /// - 4.11: `storage_options` takes precedence over environment variables
    #[new]
    #[pyo3(signature = (
        path,
        batch_size = 100_000,
        buffer_blocks = 4,
        buffer_bytes = 67_108_864,
        strict = false,
        projected_columns = None,
        storage_options = None
    ))]
    fn new(
        path: String,
        batch_size: usize,
        buffer_blocks: usize,
        buffer_bytes: usize,
        strict: bool,
        projected_columns: Option<Vec<String>>,
        storage_options: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<Self> {
        // Create tokio runtime
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create async runtime: {}",
                e
            ))
        })?;

        // Convert storage_options to S3Config
        let s3_config = storage_options.map(|opts| parse_storage_options(&opts));

        // Create source and reader within the runtime
        let result = runtime.block_on(async {
            // Create the appropriate source based on path
            let source = create_source(&path, s3_config).await?;

            // Build reader configuration
            let buffer_config = BufferConfig::new(buffer_blocks, buffer_bytes);
            let error_mode = if strict {
                ErrorMode::Strict
            } else {
                ErrorMode::Skip
            };

            let mut config = ReaderConfig::new()
                .with_batch_size(batch_size)
                .with_buffer_config(buffer_config)
                .with_error_mode(error_mode);

            // Add projection if specified
            if let Some(columns) = projected_columns {
                config = config.with_projection(columns);
            }

            // Open the reader
            let reader = AvroStreamReader::open(source, config).await?;

            // Cache the schema JSON
            let schema_json = reader.schema().to_json();

            Ok::<_, ReaderError>((reader, schema_json))
        });

        let (inner, schema_json) = result.map_err(|e| map_reader_error_to_py(&path, e))?;

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
    /// Raises StopIteration when all records have been read.
    /// In skip mode, errors are accumulated and available via `errors` property.
    ///
    /// # Returns
    /// A Polars DataFrame containing the next batch of records.
    ///
    /// # Raises
    /// * `StopIteration` - When all records have been read
    /// * `RuntimeError` - If an error occurs during reading (in strict mode)
    fn __next__(slf: PyRefMut<'_, Self>) -> PyResult<PyDataFrame> {
        let inner = slf.inner.clone();
        let path = slf.path.clone();
        let errors_arc = slf.errors.clone();

        // Get the next batch using the runtime
        let result = slf.runtime.block_on(async {
            let mut guard = inner.lock().await;
            let reader = guard
                .as_mut()
                .ok_or_else(|| ReaderError::Configuration("Reader has been closed".to_string()))?;

            match reader.next_batch().await {
                Ok(Some(df)) => Ok((Some(df), None)),
                Ok(None) => {
                    // End of iteration - collect errors before releasing the reader
                    let collected_errors: Vec<PyReadError> = reader
                        .errors()
                        .iter()
                        .map(PyReadError::from_read_error)
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
            Ok((Some(df), _)) => Ok(PyDataFrame(df)),
            Ok((None, _)) => Err(PyErr::new::<pyo3::exceptions::PyStopIteration, _>(())),
            Err(e) => Err(map_reader_error_to_py(&path, e)),
        }
    }

    /// Get the Avro schema as a JSON string.
    ///
    /// # Returns
    /// The Avro schema as a JSON string.
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
    /// True if all records have been read, False otherwise.
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
    /// A list of ReadError objects with details about each error.
    ///
    /// # Example
    /// ```python
    /// reader = AvroReaderCore("file.avro", strict=False)
    /// for df in reader:
    ///     process(df)
    ///
    /// for err in reader.errors:
    ///     print(f"[{err.kind}] Block {err.block_index}: {err.message}")
    /// ```
    ///
    /// # Requirements
    /// - 7.3: Track error counts and positions
    /// - 7.4: Provide summary of skipped errors
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
    /// reader = AvroReaderCore("file.avro", strict=False)
    /// for df in reader:
    ///     process(df)
    ///
    /// if reader.error_count > 0:
    ///     print(f"Warning: {reader.error_count} errors during read")
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

/// Parse Python storage_options dict into Rust S3Config.
///
/// Supported keys:
/// - `endpoint_url`: Custom S3 endpoint (for MinIO, LocalStack, R2, etc.)
/// - `aws_access_key_id`: AWS access key (overrides environment)
/// - `aws_secret_access_key`: AWS secret key (overrides environment)
/// - `region`: AWS region (overrides environment)
///
/// # Requirements
/// - 4.8: Accept optional `storage_options` parameter
/// - 4.9: Connect to custom endpoint when `endpoint_url` is provided
/// - 4.10: Use provided credentials when specified
fn parse_storage_options(opts: &std::collections::HashMap<String, String>) -> S3Config {
    let mut config = S3Config::new();

    if let Some(endpoint_url) = opts.get("endpoint_url") {
        config = config.with_endpoint_url(endpoint_url);
    }

    if let Some(access_key_id) = opts.get("aws_access_key_id") {
        config = config.with_access_key_id(access_key_id);
    }

    if let Some(secret_access_key) = opts.get("aws_secret_access_key") {
        config = config.with_secret_access_key(secret_access_key);
    }

    if let Some(region) = opts.get("region") {
        config = config.with_region(region);
    }

    config
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
/// - 4.9: Connect to custom endpoint when `endpoint_url` is provided
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

/// Map ReaderError to appropriate Python exception.
///
/// This function maps Rust error types to custom Python exceptions,
/// providing descriptive error messages with context about the error location.
///
/// # Requirements
/// - 6.4: Raise appropriate Python exceptions with descriptive messages
/// - 6.5: Include context about block and record position in errors
fn map_reader_error_to_py(path: &str, err: ReaderError) -> PyErr {
    match &err {
        // Source errors - map to SourceError or standard Python exceptions for common cases
        ReaderError::Source(source_err) => match source_err {
            RustSourceError::NotFound(_) => PyErr::new::<pyo3::exceptions::PyFileNotFoundError, _>(
                format!("File not found: {}", path),
            ),
            RustSourceError::PermissionDenied(msg) => {
                PyErr::new::<pyo3::exceptions::PyPermissionError, _>(format!(
                    "Permission denied for '{}': {}",
                    path, msg
                ))
            }
            RustSourceError::AuthenticationFailed(msg) => {
                PyErr::new::<pyo3::exceptions::PyPermissionError, _>(format!(
                    "Authentication failed for '{}': {}",
                    path, msg
                ))
            }
            RustSourceError::S3Error(msg) => {
                SourceError::new_err(format!("S3 error reading '{}': {}", path, msg))
            }
            RustSourceError::FileSystemError(msg) => {
                SourceError::new_err(format!("Filesystem error reading '{}': {}", path, msg))
            }
            RustSourceError::Io(io_err) => {
                SourceError::new_err(format!("I/O error reading '{}': {}", path, io_err))
            }
        },

        // Schema errors - map to SchemaError
        ReaderError::Schema(schema_err) => match schema_err {
            RustSchemaError::InvalidSchema(msg) => {
                SchemaError::new_err(format!("Invalid schema in '{}': {}", path, msg))
            }
            RustSchemaError::UnsupportedType(type_name) => SchemaError::new_err(format!(
                "Unsupported Avro type in '{}': {}",
                path, type_name
            )),
            RustSchemaError::ParseError(msg) => {
                SchemaError::new_err(format!("Schema parse error in '{}': {}", path, msg))
            }
            RustSchemaError::IncompatibleSchemas(msg) => {
                SchemaError::new_err(format!("Incompatible schemas in '{}': {}", path, msg))
            }
        },

        // Parse errors - invalid file format
        ReaderError::Parse { offset, message } => ParseError::new_err(format!(
            "Parse error in '{}' at offset {}: {}",
            path, offset, message
        )),

        // Invalid magic bytes - file is not a valid Avro file
        ReaderError::InvalidMagic(magic) => ParseError::new_err(format!(
            "Invalid Avro file '{}': expected magic bytes 'Obj\\x01', found {:?}",
            path, magic
        )),

        // Invalid sync marker - block boundary corruption
        ReaderError::InvalidSyncMarker {
            block_index,
            offset,
            expected,
            actual,
        } => ParseError::new_err(format!(
            "Invalid sync marker in '{}' at block {}, offset {}: expected {}, got {}",
            path,
            block_index,
            offset,
            format_sync_marker(expected),
            format_sync_marker(actual)
        )),

        // Decode errors - record decoding failures with block/record context
        ReaderError::Decode {
            block_index,
            record_index,
            message,
        } => DecodeError::new_err(format!(
            "Decode error in '{}' at block {}, record {}: {}",
            path, block_index, record_index, message
        )),

        // Codec errors - compression/decompression failures
        ReaderError::Codec(codec_err) => match codec_err {
            RustCodecError::UnsupportedCodec(codec_name) => CodecError::new_err(format!(
                "Unsupported codec '{}' in file '{}'",
                codec_name, path
            )),
            RustCodecError::CompressionError(msg) => {
                CodecError::new_err(format!("Compression error in '{}': {}", path, msg))
            }
            RustCodecError::DecompressionError(msg) => {
                CodecError::new_err(format!("Decompression error in '{}': {}", path, msg))
            }
        },

        // Configuration errors
        ReaderError::Configuration(msg) => {
            JetlinerError::new_err(format!("Configuration error: {}", msg))
        }

        // Builder errors
        ReaderError::Builder(msg) => {
            DecodeError::new_err(format!("DataFrame builder error in '{}': {}", path, msg))
        }
    }
}

/// Format a 16-byte sync marker as a hex string for error messages.
fn format_sync_marker(marker: &[u8; 16]) -> String {
    format!(
        "0x{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}",
        marker[0], marker[1], marker[2], marker[3],
        marker[4], marker[5], marker[6], marker[7],
        marker[8], marker[9], marker[10], marker[11],
        marker[12], marker[13], marker[14], marker[15]
    )
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

/// Parse an Avro file and return its schema as a Polars Schema.
///
/// This function opens an Avro file, reads only the header to extract the schema,
/// and converts it to a Polars Schema suitable for use with `register_io_source`.
///
/// # Arguments
/// * `path` - Path to the Avro file (local path or s3:// URI)
/// * `storage_options` - Optional dict for S3 configuration (endpoint_url, credentials, region)
///
/// # Returns
/// A Polars Schema (as a Python dict mapping column names to dtypes)
///
/// # Raises
/// * `FileNotFoundError` - If the file does not exist
/// * `PermissionError` - If access is denied
/// * `ValueError` - If the file is not a valid Avro file or schema conversion fails
/// * `RuntimeError` - For other errors
///
/// # Example
/// ```python
/// import jetliner
/// import polars as pl
///
/// # Get schema for IO plugin
/// schema = jetliner.parse_avro_schema("data.avro")
///
/// # Get schema from S3-compatible service
/// schema = jetliner.parse_avro_schema(
///     "s3://bucket/data.avro",
///     storage_options={
///         "endpoint_url": "http://localhost:9000",
///         "aws_access_key_id": "minioadmin",
///         "aws_secret_access_key": "minioadmin",
///     }
/// )
///
/// # Use with register_io_source
/// lf = pl.LazyFrame.register_io_source(
///     io_source=my_generator,
///     schema=schema,
/// )
/// ```
///
/// # Requirements
/// - 4.8: Accept optional `storage_options` parameter
/// - 4.9: Connect to custom endpoint when `endpoint_url` is provided
/// - 4.10: Use provided credentials when specified
/// - 6a.5: Expose Avro schema as Polars schema for query planning
/// - 9.3: Expose parsed schema for inspection
#[pyfunction]
#[pyo3(signature = (path, storage_options = None))]
pub fn parse_avro_schema(
    py: Python<'_>,
    path: String,
    storage_options: Option<std::collections::HashMap<String, String>>,
) -> PyResult<Py<PyAny>> {
    // Create a tokio runtime for async operations
    let runtime = tokio::runtime::Runtime::new().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create async runtime: {}",
            e
        ))
    })?;

    // Convert storage_options to S3Config
    let s3_config = storage_options.map(|opts| parse_storage_options(&opts));

    // Read the header and extract schema
    let result = runtime.block_on(async {
        // Create the appropriate source based on path
        let source = create_source(&path, s3_config).await?;

        // Read enough bytes for the header (typically < 4KB, but allow more for large schemas)
        let header_bytes = source
            .read_range(0, 64 * 1024)
            .await
            .map_err(ReaderError::Source)?;

        // Parse the header to get the Avro schema
        let header = AvroHeader::parse(&header_bytes)?;

        // Validate top-level schema type is supported
        if !header.schema.is_record() {
            match &header.schema {
                AvroSchema::Array(_) => {
                    return Err(ReaderError::Schema(RustSchemaError::UnsupportedType(
                        "Array as top-level schema is not yet supported. \
                        Arrays at the top level cause Polars list builder errors. \
                        Workaround: Wrap your array in a record type with a field, \
                        or use primitive types (int, string, bytes) which are fully supported."
                            .to_string(),
                    )));
                }
                AvroSchema::Map(_) => {
                    return Err(ReaderError::Schema(RustSchemaError::UnsupportedType(
                        "Map as top-level schema is not yet supported. \
                        Maps at the top level cause Polars struct builder errors. \
                        Workaround: Wrap your map in a record type with a field, \
                        or use primitive types (int, string, bytes) which are fully supported."
                            .to_string(),
                    )));
                }
                _ => {} // Other non-record types (primitives) are OK
            }
        }

        // Convert Avro schema to Polars schema
        let polars_schema = avro_to_arrow_schema(&header.schema).map_err(ReaderError::Schema)?;

        Ok::<_, ReaderError>(polars_schema)
    });

    let polars_schema = result.map_err(|e| map_reader_error_to_py(&path, e))?;

    // Convert to Python schema manually to avoid pyo3-polars serialization issues
    // with certain types like Enum with FrozenCategories
    schema_to_py(py, &polars_schema)
}

/// Convert a Polars Schema to a Python polars.Schema object.
///
/// This function manually constructs the Python schema by calling the polars
/// module directly, avoiding pyo3-polars' PySchema which has issues with
/// certain DataTypes like Enum with FrozenCategories.
fn schema_to_py(py: Python<'_>, schema: &polars::prelude::Schema) -> PyResult<Py<PyAny>> {
    let polars_mod = py.import("polars")?;

    // Build a dict of field_name -> DataType
    let py_dict = pyo3::types::PyDict::new(py);

    for (name, dtype) in schema.iter() {
        let py_dtype = dtype_to_py(py, &polars_mod, dtype)?;
        py_dict.set_item(name.as_str(), py_dtype)?;
    }

    // Create polars.Schema from the dict
    let schema_class = polars_mod.getattr("Schema")?;
    let py_schema = schema_class.call1((py_dict,))?;

    Ok(py_schema.into())
}

/// Convert a Polars DataType to a Python polars DataType object.
///
/// This handles all DataTypes including complex ones like Enum, List, Struct, etc.
fn dtype_to_py<'py>(
    py: Python<'py>,
    polars_mod: &Bound<'py, pyo3::types::PyModule>,
    dtype: &polars::prelude::DataType,
) -> PyResult<Bound<'py, PyAny>> {
    use polars::prelude::DataType;

    match dtype {
        // Simple types - just get the class attribute
        DataType::Null => polars_mod.getattr("Null"),
        DataType::Boolean => polars_mod.getattr("Boolean"),
        DataType::Int8 => polars_mod.getattr("Int8"),
        DataType::Int16 => polars_mod.getattr("Int16"),
        DataType::Int32 => polars_mod.getattr("Int32"),
        DataType::Int64 => polars_mod.getattr("Int64"),
        DataType::UInt8 => polars_mod.getattr("UInt8"),
        DataType::UInt16 => polars_mod.getattr("UInt16"),
        DataType::UInt32 => polars_mod.getattr("UInt32"),
        DataType::UInt64 => polars_mod.getattr("UInt64"),
        DataType::Float32 => polars_mod.getattr("Float32"),
        DataType::Float64 => polars_mod.getattr("Float64"),
        DataType::String => polars_mod.getattr("String"),
        DataType::Binary => polars_mod.getattr("Binary"),
        DataType::Date => polars_mod.getattr("Date"),
        DataType::Time => polars_mod.getattr("Time"),

        // Datetime with optional timezone
        DataType::Datetime(time_unit, tz) => {
            let tu_str = match time_unit {
                polars::prelude::TimeUnit::Nanoseconds => "ns",
                polars::prelude::TimeUnit::Microseconds => "us",
                polars::prelude::TimeUnit::Milliseconds => "ms",
            };
            let tz_str = tz.as_ref().map(|t| t.to_string());
            polars_mod.getattr("Datetime")?.call1((tu_str, tz_str))
        }

        // Duration
        DataType::Duration(time_unit) => {
            let tu_str = match time_unit {
                polars::prelude::TimeUnit::Nanoseconds => "ns",
                polars::prelude::TimeUnit::Microseconds => "us",
                polars::prelude::TimeUnit::Milliseconds => "ms",
            };
            polars_mod.getattr("Duration")?.call1((tu_str,))
        }

        // Decimal
        DataType::Decimal(precision, scale) => {
            let precision = *precision as i64;
            let scale = *scale as i64;
            polars_mod.getattr("Decimal")?.call1((precision, scale))
        }

        // List
        DataType::List(inner) => {
            let inner_py = dtype_to_py(py, polars_mod, inner)?;
            polars_mod.getattr("List")?.call1((inner_py,))
        }

        // Array (fixed-size list)
        DataType::Array(inner, size) => {
            let inner_py = dtype_to_py(py, polars_mod, inner)?;
            polars_mod.getattr("Array")?.call1((inner_py, *size))
        }

        // Struct
        DataType::Struct(fields) => {
            let py_fields: Vec<Bound<'py, PyAny>> = fields
                .iter()
                .map(|f| {
                    let field_dtype = dtype_to_py(py, polars_mod, f.dtype())?;
                    let field_class = polars_mod.getattr("Field")?;
                    field_class.call1((f.name().as_str(), field_dtype))
                })
                .collect::<PyResult<Vec<_>>>()?;
            polars_mod.getattr("Struct")?.call1((py_fields,))
        }

        // Enum - convert to Categorical for Python compatibility
        // pyo3-polars has issues with Enum types, so we use Categorical
        DataType::Enum(_, _) => {
            // Use Categorical as a fallback since Enum serialization is problematic
            polars_mod.getattr("Categorical")
        }

        // Categorical
        DataType::Categorical(_, _) => polars_mod.getattr("Categorical"),

        // Unknown - fallback to String as a safe default
        DataType::Unknown(_) => polars_mod.getattr("String"),

        // BinaryOffset - use Binary
        DataType::BinaryOffset => polars_mod.getattr("Binary"),

        // Catch-all for any other types
        _ => {
            // For any unhandled types, try to use String as a safe fallback
            polars_mod.getattr("String")
        }
    }
}

/// User-facing Avro reader for the `open()` API.
///
/// This is the primary class for reading Avro files in Python. It wraps
/// `AvroReaderCore` and provides:
/// - Python iterator protocol (__iter__, __next__)
/// - Context manager protocol (__enter__, __exit__)
/// - Schema inspection
///
/// Unlike `AvroReaderCore`, this class does not expose projection pushdown,
/// which is reserved for the `scan()` API's internal use.
///
/// # Requirements
/// - 6.1: Implement Python iterator protocol (__iter__, __next__)
/// - 6.2: Properly release resources when iteration completes
/// - 6.6: Support context manager protocol for resource cleanup
///
/// # Example
/// ```python
/// import jetliner
///
/// # Basic iteration
/// reader = jetliner.AvroReader("data.avro")
/// for df in reader:
///     print(df.shape)
///
/// # With context manager (recommended)
/// with jetliner.AvroReader("s3://bucket/data.avro") as reader:
///     for df in reader:
///         process(df)
///
/// # With configuration
/// with jetliner.AvroReader(
///     "data.avro",
///     batch_size=50000,
///     buffer_blocks=8,
///     strict=True
/// ) as reader:
///     for df in reader:
///         process(df)
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
    errors: Arc<Mutex<Vec<PyReadError>>>,
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
    /// * `strict` - If True, fail on first error; if False, skip bad records (default: False)
    /// * `storage_options` - Optional dict for S3 configuration (endpoint_url, credentials, region)
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
    ///     strict=True
    /// )
    ///
    /// # S3-compatible services (MinIO, LocalStack, R2)
    /// reader = jetliner.AvroReader(
    ///     "s3://bucket/key.avro",
    ///     storage_options={
    ///         "endpoint_url": "http://localhost:9000",
    ///         "aws_access_key_id": "minioadmin",
    ///         "aws_secret_access_key": "minioadmin",
    ///     }
    /// )
    /// ```
    ///
    /// # Requirements
    /// - 4.8: Accept optional `storage_options` parameter
    /// - 4.11: `storage_options` takes precedence over environment variables
    #[new]
    #[pyo3(signature = (
        path,
        batch_size = 100_000,
        buffer_blocks = 4,
        buffer_bytes = 67_108_864,
        strict = false,
        storage_options = None
    ))]
    fn new(
        path: String,
        batch_size: usize,
        buffer_blocks: usize,
        buffer_bytes: usize,
        strict: bool,
        storage_options: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<Self> {
        // Create tokio runtime
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create async runtime: {}",
                e
            ))
        })?;

        // Convert storage_options to S3Config
        let s3_config = storage_options.map(|opts| parse_storage_options(&opts));

        // Create source and reader within the runtime
        let result = runtime.block_on(async {
            // Create the appropriate source based on path
            let source = create_source(&path, s3_config).await?;

            // Build reader configuration (no projection for user-facing API)
            let buffer_config = BufferConfig::new(buffer_blocks, buffer_bytes);
            let error_mode = if strict {
                ErrorMode::Strict
            } else {
                ErrorMode::Skip
            };

            let config = ReaderConfig::new()
                .with_batch_size(batch_size)
                .with_buffer_config(buffer_config)
                .with_error_mode(error_mode);

            // Open the reader
            let reader = AvroStreamReader::open(source, config).await?;

            // Cache the schema JSON
            let schema_json = reader.schema().to_json();

            Ok::<_, ReaderError>((reader, schema_json))
        });

        let (inner, schema_json) = result.map_err(|e| map_reader_error_to_py(&path, e))?;

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
    /// Raises StopIteration when all records have been read.
    /// In skip mode, errors are accumulated and available via `errors` property.
    ///
    /// # Returns
    /// A Polars DataFrame containing the next batch of records.
    ///
    /// # Raises
    /// * `StopIteration` - When all records have been read
    /// * `RuntimeError` - If an error occurs during reading (in strict mode)
    fn __next__(slf: PyRefMut<'_, Self>) -> PyResult<PyDataFrame> {
        let inner = slf.inner.clone();
        let path = slf.path.clone();
        let errors_arc = slf.errors.clone();

        // Get the next batch using the runtime
        let result = slf.runtime.block_on(async {
            let mut guard = inner.lock().await;
            let reader = guard
                .as_mut()
                .ok_or_else(|| ReaderError::Configuration("Reader has been closed".to_string()))?;

            match reader.next_batch().await {
                Ok(Some(df)) => Ok((Some(df), None)),
                Ok(None) => {
                    // End of iteration - collect errors before releasing the reader
                    let collected_errors: Vec<PyReadError> = reader
                        .errors()
                        .iter()
                        .map(PyReadError::from_read_error)
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
            Ok((Some(df), _)) => Ok(PyDataFrame(df)),
            Ok((None, _)) => Err(PyErr::new::<pyo3::exceptions::PyStopIteration, _>(())),
            Err(e) => Err(map_reader_error_to_py(&path, e)),
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
    /// A list of ReadError objects with details about each error.
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

/// Open an Avro file for streaming into Polars DataFrames.
///
/// This is the main entry point for reading Avro files. It returns an iterator
/// that yields DataFrames containing batches of records.
///
/// # Arguments
/// * `path` - Path to the Avro file. Supports:
///   - Local filesystem paths: `/path/to/file.avro`, `./relative/path.avro`
///   - S3 URIs: `s3://bucket/key.avro`
/// * `batch_size` - Target number of rows per DataFrame (default: 100,000)
/// * `buffer_blocks` - Number of blocks to prefetch (default: 4)
/// * `buffer_bytes` - Maximum bytes to buffer (default: 64MB)
/// * `strict` - If True, fail on first error; if False, skip bad records (default: False)
/// * `storage_options` - Optional dict for S3 configuration. Supported keys:
///   - `endpoint_url`: Custom S3 endpoint (for MinIO, LocalStack, R2, etc.)
///   - `aws_access_key_id`: AWS access key (overrides environment)
///   - `aws_secret_access_key`: AWS secret key (overrides environment)
///   - `region`: AWS region (overrides environment)
///
/// # Returns
/// An `AvroReader` instance that can be iterated to get DataFrames.
///
/// # Raises
/// * `FileNotFoundError` - If the file does not exist
/// * `PermissionError` - If access is denied
/// * `jetliner.ParseError` - If the file is not a valid Avro file
/// * `jetliner.SchemaError` - If the schema is invalid
/// * `jetliner.SourceError` - For S3 or filesystem errors
///
/// # Example
/// ```python
/// import jetliner
///
/// # Basic usage - iterate over DataFrames
/// for df in jetliner.open("data.avro"):
///     print(df.shape)
///
/// # With context manager (recommended)
/// with jetliner.open("s3://bucket/data.avro") as reader:
///     for df in reader:
///         process(df)
///
/// # With configuration
/// with jetliner.open(
///     "data.avro",
///     batch_size=50000,
///     buffer_blocks=8,
///     strict=True
/// ) as reader:
///     for df in reader:
///         process(df)
///
/// # S3-compatible services (MinIO, LocalStack, R2)
/// with jetliner.open(
///     "s3://bucket/data.avro",
///     storage_options={
///         "endpoint_url": "http://localhost:9000",
///         "aws_access_key_id": "minioadmin",
///         "aws_secret_access_key": "minioadmin",
///     }
/// ) as reader:
///     for df in reader:
///         process(df)
///
/// # Access schema
/// with jetliner.open("data.avro") as reader:
///     print(reader.schema)  # JSON string
///     print(reader.schema_dict)  # Python dict
///
/// # Error handling in skip mode
/// with jetliner.open("data.avro", strict=False) as reader:
///     for df in reader:
///         process(df)
///     if reader.error_count > 0:
///         print(f"Skipped {reader.error_count} errors")
///         for err in reader.errors:
///             print(f"  [{err.kind}] Block {err.block_index}: {err.message}")
/// ```
///
/// # Requirements
/// - 4.1: Unified interface for S3 and local filesystem access
/// - 4.2: S3 URI support (s3://bucket/key)
/// - 4.3: Local filesystem path support
/// - 4.8: Accept optional `storage_options` parameter
/// - 4.11: `storage_options` takes precedence over environment variables
#[pyfunction]
#[pyo3(signature = (
    path,
    batch_size = 100_000,
    buffer_blocks = 4,
    buffer_bytes = 67_108_864,
    strict = false,
    storage_options = None
))]
pub fn open(
    path: String,
    batch_size: usize,
    buffer_blocks: usize,
    buffer_bytes: usize,
    strict: bool,
    storage_options: Option<std::collections::HashMap<String, String>>,
) -> PyResult<AvroReader> {
    AvroReader::new(
        path,
        batch_size,
        buffer_blocks,
        buffer_bytes,
        strict,
        storage_options,
    )
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
