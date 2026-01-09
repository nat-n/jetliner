//! Python bindings for Jetliner
//!
//! This module provides PyO3-based Python bindings for the Jetliner library,
//! enabling Python users to stream Avro data into Polars DataFrames.
//!
//! # Classes
//! - `AvroReader`: User-facing class for the `open()` API with context manager support
//! - `AvroReaderCore`: Internal class used by both `open()` and `scan()` APIs
//! - `ReadError`: Structured error information for skip mode reading
//!
//! # Exception Types
//! Custom exception classes for specific error conditions:
//! - `JetlinerError`: Base exception for all Jetliner errors
//! - `ParseError`: Errors during Avro file parsing
//! - `SchemaError`: Schema-related errors
//! - `CodecError`: Compression/decompression errors
//! - `DecodeError`: Record decoding errors
//! - `SourceError`: Data source errors
//!
//! # Functions
//! - `parse_avro_schema`: Extract Polars schema from an Avro file for IO plugin integration
//!
//! # Requirements
//! - 6.1: Implement Python iterator protocol (__iter__, __next__)
//! - 6.2: Properly release resources when iteration completes
//! - 6.4: Raise appropriate Python exceptions with descriptive messages
//! - 6.5: Include context about block and record position in errors
//! - 6.6: Support context manager protocol for resource cleanup
//! - 6a.2: Support projection pushdown via projected_columns parameter
//! - 6a.5: Expose Avro schema as Polars schema for query planning
//! - 7.3: Track error counts and positions
//! - 7.4: Provide summary of skipped errors
//! - 7.7: Include sufficient detail to diagnose issues
//! - 9.3: Expose parsed schema for inspection

mod reader;

pub use reader::{open, parse_avro_schema, AvroReader, AvroReaderCore, PyReadError};

// Re-export exception types for module registration
pub use reader::{CodecError, DecodeError, JetlinerError, ParseError, SchemaError, SourceError};
