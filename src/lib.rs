//! High-performance Avro streaming reader with Python bindings
//!
//! This library provides streaming Avro data into Polars DataFrames,
//! supporting both S3 and local filesystem sources.

pub mod codec;
pub mod convert;
pub mod error;
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
