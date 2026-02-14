//! Python bindings for Jetliner
//!
//! This module provides PyO3-based Python bindings for the Jetliner library,
//! enabling Python users to stream Avro data into Polars DataFrames.
//!
//! # Classes
//! - `AvroReader`: User-facing class for streaming iteration with context manager support
//! - `BadBlockError`: Structured error information for skip mode reading
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
//! - `scan_avro`: Scan Avro files returning a LazyFrame
//! - `read_avro`: Read Avro files returning a DataFrame
//! - `read_avro_schema`: Extract Polars schema from an Avro file
//!
//! # Requirements
//! - 1.1: Expose `scan_avro()` function that returns `pl.LazyFrame`
//! - 1.2: Expose `read_avro()` function that returns `pl.DataFrame`
//! - 1.3: Expose `read_avro_schema()` function that returns `pl.Schema`
//! - 2.1-2.3: FileSource support (str, Path, Sequence)
//! - 3.2-3.3: Column selection support (names, indices)
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
//! - 15.1-15.6: Python exception metadata

pub mod api;
pub mod errors;
mod reader;
pub mod types;

pub use reader::{AvroReader, MultiAvroReader, PyBadBlockError};

// Note: Exception types (JetlinerError, DecodeError, etc.) are now defined
// in Python (jetliner.exceptions). The errors module provides mapping functions
// to create these exceptions from Rust errors.

// Re-export new API functions
pub use api::{_resolve_avro_sources, read_avro, read_avro_schema, scan_avro};

// Re-export types
pub use types::{PyColumnSelection, PyFileSource};
