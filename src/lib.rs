//! High-performance Avro streaming reader with Python bindings
//!
//! This library provides streaming Avro data into Polars DataFrames,
//! supporting both S3 and local filesystem sources.

pub mod error;
pub mod schema;

// Re-export main types
pub use error::{CodecError, DecodeError, ReadError, ReadErrorKind, ReaderError, SchemaError, SourceError};
pub use schema::{AvroSchema, EnumSchema, FieldSchema, FixedSchema, LogicalType, LogicalTypeName, RecordSchema};
