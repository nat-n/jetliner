//! High-performance Avro streaming reader with Python bindings
//!
//! This library provides streaming Avro data into Polars DataFrames,
//! supporting both S3 and local filesystem sources.

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
    CodecError, DecodeError, ReadError, ReadErrorKind, ReaderError, SchemaError, SourceError,
};
pub use reader::{
    decode_boolean, decode_bytes, decode_bytes_ref, decode_double, decode_float, decode_int,
    decode_long, decode_null, decode_string, decode_string_ref, decode_value_with_context,
    decode_varint, skip_value_with_context,
};
pub use reader::{
    AvroBlock, AvroHeader, AvroStreamReader, BlockReader, DecompressedBlock, FullRecordDecoder,
    ProjectedRecordDecoder, ReaderConfig, RecordDecode, RecordDecoder,
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
pub use python::{open, parse_avro_schema, AvroReader, AvroReaderCore, PyReadError};
// Re-export Python exception types
pub use python::{
    CodecError as PyCodecError, DecodeError as PyDecodeError, JetlinerError as PyJetlinerError,
    ParseError as PyParseError, SchemaError as PySchemaError, SourceError as PySourceError,
};

// PyO3 module registration
use pyo3::prelude::*;

/// Python module for Jetliner
///
/// This is the main entry point for the Python bindings.
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
/// - 6.4: Raise appropriate Python exceptions with descriptive messages
/// - 6.5: Include context about block and record position in errors
#[pymodule]
fn jetliner(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register classes
    m.add_class::<AvroReader>()?;
    m.add_class::<AvroReaderCore>()?;
    m.add_class::<PyReadError>()?;

    // Register functions
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(parse_avro_schema, m)?)?;

    // Register exception types
    // These allow Python code to catch specific error types:
    //   try:
    //       reader = jetliner.open("file.avro")
    //   except jetliner.ParseError as e:
    //       print(f"Invalid Avro file: {e}")
    //   except jetliner.SchemaError as e:
    //       print(f"Schema problem: {e}")
    m.add("JetlinerError", m.py().get_type::<PyJetlinerError>())?;
    m.add("ParseError", m.py().get_type::<PyParseError>())?;
    m.add("SchemaError", m.py().get_type::<PySchemaError>())?;
    m.add("CodecError", m.py().get_type::<PyCodecError>())?;
    m.add("DecodeError", m.py().get_type::<PyDecodeError>())?;
    m.add("SourceError", m.py().get_type::<PySourceError>())?;

    Ok(())
}
