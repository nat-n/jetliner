//! Error types for Avro streaming

use std::io;
use thiserror::Error;

/// Errors that can occur during schema operations
#[derive(Debug, Error)]
pub enum SchemaError {
    /// Invalid schema format
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    /// Unsupported schema type
    #[error("Unsupported type: {0}")]
    UnsupportedType(String),
    /// Schema parsing error
    #[error("Parse error: {0}")]
    ParseError(String),
    /// Incompatible schema evolution
    #[error("Incompatible schemas: {0}")]
    IncompatibleSchemas(String),
}

/// Errors that can occur during codec operations
#[derive(Debug, Error)]
pub enum CodecError {
    /// Unsupported codec
    #[error("Unsupported codec: {0}")]
    UnsupportedCodec(String),
    /// Compression error
    #[error("Compression error: {0}")]
    CompressionError(String),
    /// Decompression error
    #[error("Decompression error: {0}")]
    DecompressionError(String),
}

/// Errors that can occur during decoding
#[derive(Debug, Error)]
pub enum DecodeError {
    /// Invalid Avro data
    #[error("Invalid data: {0}")]
    InvalidData(String),
    /// Unexpected end of data
    #[error("Unexpected end of file")]
    UnexpectedEof,
    /// Type mismatch
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    /// Invalid varint encoding
    #[error("Invalid varint encoding")]
    InvalidVarint,
    /// String is not valid UTF-8
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
}

/// Errors that can occur with data sources
#[derive(Debug, Error)]
pub enum SourceError {
    /// S3 error
    #[error("S3 error: {0}")]
    S3Error(String),
    /// File system error
    #[error("File system error: {0}")]
    FileSystemError(String),
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    /// Path not found
    #[error("Not found: {0}")]
    NotFound(String),
    /// Permission denied
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    /// Authentication failed
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),
}

/// Top-level reader error type
#[derive(Debug, Error)]
pub enum ReaderError {
    /// Source error
    #[error("Source error: {0}")]
    Source(#[from] SourceError),

    /// Parse error at specific offset
    #[error("Parse error at offset {offset}: {message}")]
    Parse { offset: u64, message: String },

    /// Schema error
    #[error("Schema error: {0}")]
    Schema(#[from] SchemaError),

    /// Decode error in block/record
    #[error("Decode error in block {block_index}, record {record_index}: {message}")]
    Decode {
        block_index: usize,
        record_index: usize,
        message: String,
    },

    /// Codec error
    #[error("Codec error: {0}")]
    Codec(#[from] CodecError),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Invalid magic bytes
    #[error("Invalid magic bytes: expected 'Obj\\x01', found {0:?}")]
    InvalidMagic([u8; 4]),

    /// Invalid sync marker
    #[error("Invalid sync marker at block {block_index}, offset {offset}")]
    InvalidSyncMarker { block_index: usize, offset: u64 },
}

impl From<DecodeError> for ReaderError {
    fn from(err: DecodeError) -> Self {
        ReaderError::Decode {
            block_index: 0,
            record_index: 0,
            message: err.to_string(),
        }
    }
}

/// Recoverable error that occurred during reading (for skip mode)
#[derive(Debug, Clone)]
pub struct ReadError {
    /// The kind of error that occurred
    pub kind: ReadErrorKind,
    /// Block index where error occurred
    pub block_index: usize,
    /// Record index within block (if applicable)
    pub record_index: Option<usize>,
    /// File offset where error occurred
    pub offset: u64,
    /// Human-readable error message
    pub message: String,
}

impl ReadError {
    /// Create a new ReadError
    pub fn new(
        kind: ReadErrorKind,
        block_index: usize,
        record_index: Option<usize>,
        offset: u64,
        message: String,
    ) -> Self {
        Self {
            kind,
            block_index,
            record_index,
            offset,
            message,
        }
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.record_index {
            Some(record_idx) => write!(
                f,
                "{:?} at block {}, record {}, offset {}: {}",
                self.kind, self.block_index, record_idx, self.offset, self.message
            ),
            None => write!(
                f,
                "{:?} at block {}, offset {}: {}",
                self.kind, self.block_index, self.offset, self.message
            ),
        }
    }
}

/// Types of recoverable errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadErrorKind {
    /// Sync marker doesn't match expected value
    InvalidSyncMarker,
    /// Block decompression failed
    DecompressionFailed,
    /// Record decoding failed
    RecordDecodeFailed,
    /// Data violates schema constraints
    SchemaViolation,
}
