//! Avro schema types and parsing.
//!
//! This module defines the complete Avro schema type system including
//! primitives, complex types, logical types, JSON parsing, named type resolution,
//! schema compatibility checking, and reader/writer schema resolution.

mod compatibility;
mod parser;
mod reader_writer_resolution;
mod resolution;
mod types;

pub use compatibility::{
    check_compatibility, validate_schema_compatibility, CompatibilityResult, IncompatibilityReason,
    SchemaIncompatibility,
};
pub use parser::{parse_schema, parse_schema_with_options, SchemaParser};
pub use reader_writer_resolution::{
    apply_promotion, decode_record_with_resolution, json_to_avro_value, ReaderWriterResolution,
    ResolvedField, TypePromotion,
};
pub use resolution::{resolve_schema, resolve_schema_with_context, SchemaResolutionContext};
pub use types::*;
