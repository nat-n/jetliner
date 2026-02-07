//! High-performance Avro streaming reader with Python bindings
//!
//! This library provides streaming Avro data into Polars DataFrames,
//! supporting both S3 and local filesystem sources.

pub mod api;
pub mod codec;
pub mod convert;
pub mod error;
pub mod python;
pub mod reader;
pub mod schema;
pub mod source;

// Re-export main types
pub use codec::Codec;
pub use convert::{
    avro_to_arrow, avro_to_arrow_field, avro_to_arrow_schema, BuilderConfig, BuilderError,
    DataFrameBuilder, ErrorMode,
};
pub use error::{
    BadBlockError, BadBlockErrorKind, CodecError, DecodeError, ReaderError, SchemaError,
    SourceError,
};
pub use reader::{
    decode_boolean, decode_bytes, decode_bytes_ref, decode_double, decode_float, decode_int,
    decode_long, decode_null, decode_string, decode_string_ref, decode_value_with_context,
    decode_varint, skip_value_with_context,
};
pub use reader::{
    AvroBlock, AvroHeader, AvroStreamReader, BlockReader, DecompressedBlock, FullRecordDecoder,
    ProjectedRecordDecoder, ReadBufferConfig, ReaderConfig, RecordDecode, RecordDecoder,
};
pub use schema::{
    apply_promotion, check_compatibility, decode_record_with_resolution, json_to_avro_value,
    parse_schema, resolve_schema, resolve_schema_with_context, validate_schema_compatibility,
    AvroSchema, CompatibilityResult, EnumSchema, FieldSchema, FixedSchema, IncompatibilityReason,
    LogicalType, LogicalTypeName, ReaderWriterResolution, RecordSchema, ResolvedField,
    SchemaIncompatibility, SchemaParser, SchemaResolutionContext, TypePromotion,
};
pub use source::{BoxedSource, LocalSource, S3Source, StreamSource};

// Re-export Python bindings
pub use python::{open, AvroReader, MultiAvroReader, PyBadBlockError};
// Re-export new Python API functions
pub use python::api::_resolve_avro_sources;
pub use python::{read_avro, read_avro_schema, scan_avro};
// Note: Exception types are now defined in Python (jetliner.exceptions)
// and imported by Rust when raising errors. No Rust re-exports needed.

// PyO3 module registration
use pyo3::prelude::*;
use tracing_subscriber::EnvFilter;

/// Python module for Jetliner
///
/// This is the main entry point for the Python bindings.
///
/// # Functions
/// - `scan_avro`: Scan Avro files returning a LazyFrame
/// - `read_avro`: Read Avro files returning a DataFrame
/// - `read_avro_schema`: Extract Polars schema from an Avro file
/// - `open`: Open an Avro file for streaming iteration
///
/// # Exception Types
/// The module provides custom exception types for specific error conditions:
/// - `JetlinerError`: Base exception for all Jetliner errors
/// - `ParseError`: Errors during Avro file parsing (invalid magic bytes, malformed headers)
/// - `SchemaError`: Schema-related errors (invalid schema, incompatible schemas)
/// - `CodecError`: Compression/decompression errors
/// - `DecodeError`: Record decoding errors (type mismatches, invalid data)
/// - `SourceError`: Data source errors (S3, filesystem)
///
/// # Requirements
/// - 1.1: Expose `scan_avro()` function that returns `pl.LazyFrame`
/// - 1.2: Expose `read_avro()` function that returns `pl.DataFrame`
/// - 1.3: Expose `read_avro_schema()` function that returns `pl.Schema`
/// - 1.4: Remove old `scan()` function (replaced by `scan_avro()`)
/// - 1.5: Remove old `parse_avro_schema()` function (replaced by `read_avro_schema()`)
/// - 6.4: Raise appropriate Python exceptions with descriptive messages
/// - 6.5: Include context about block and record position in errors
#[pymodule]
fn jetliner(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize tracing subscriber for debug logging
    // Controlled via JETLINER_LOG env var (e.g., JETLINER_LOG=debug)
    // Logs go to stderr to avoid interfering with Python stdout
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("JETLINER_LOG").unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    // Register classes
    m.add_class::<AvroReader>()?;
    m.add_class::<MultiAvroReader>()?;
    m.add_class::<PyBadBlockError>()?;

    // Note: Exception classes (JetlinerError, DecodeError, ParseError, etc.)
    // are defined in Python (jetliner.exceptions) and re-exported via __init__.py.
    // Rust code imports them when raising errors.

    // Register new API functions
    m.add_function(wrap_pyfunction!(python::api::scan_avro, m)?)?;
    m.add_function(wrap_pyfunction!(python::api::read_avro, m)?)?;
    m.add_function(wrap_pyfunction!(python::api::read_avro_schema, m)?)?;
    m.add_function(wrap_pyfunction!(python::api::_resolve_avro_sources, m)?)?;

    // Register open function for streaming iteration
    m.add_function(wrap_pyfunction!(open, m)?)?;

    Ok(())
}

// Initialize tracing for Rust tests
// Controlled via RUST_LOG env var (e.g., RUST_LOG=debug cargo test)
#[cfg(test)]
#[ctor::ctor]
fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}
