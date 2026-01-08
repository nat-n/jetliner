//! Error types for Avro streaming

use std::io;
use thiserror::Error;

/// Format a 16-byte sync marker as a hex string for error messages.
///
/// Returns a string like "0xDEADBEEF..." showing the first 8 bytes
/// followed by "..." if there are more bytes.
fn format_sync_marker(marker: &[u8; 16]) -> String {
    format!(
        "0x{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}",
        marker[0], marker[1], marker[2], marker[3],
        marker[4], marker[5], marker[6], marker[7],
        marker[8], marker[9], marker[10], marker[11],
        marker[12], marker[13], marker[14], marker[15]
    )
}

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
    #[error(
        "Invalid sync marker at block {block_index}, offset {offset}: expected {}, got {}",
        format_sync_marker(expected),
        format_sync_marker(actual)
    )]
    InvalidSyncMarker {
        block_index: usize,
        offset: u64,
        /// Expected sync marker from file header
        expected: [u8; 16],
        /// Actual sync marker found in block
        actual: [u8; 16],
    },

    /// Builder error (from DataFrameBuilder)
    #[error("Builder error: {0}")]
    Builder(String),
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

// Forward declaration to avoid circular dependency
// BuilderError is defined in convert::dataframe
impl From<crate::convert::BuilderError> for ReaderError {
    fn from(err: crate::convert::BuilderError) -> Self {
        ReaderError::Builder(err.to_string())
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
        // Format the error kind with details
        let kind_str = match &self.kind {
            ReadErrorKind::InvalidSyncMarker { expected, actual } => {
                format!(
                    "InvalidSyncMarker (expected {}, got {})",
                    format_sync_marker(expected),
                    format_sync_marker(actual)
                )
            }
            ReadErrorKind::DecompressionFailed { codec } => {
                format!("DecompressionFailed (codec: {})", codec)
            }
            ReadErrorKind::BlockParseFailed => "BlockParseFailed".to_string(),
            ReadErrorKind::RecordDecodeFailed => "RecordDecodeFailed".to_string(),
            ReadErrorKind::SchemaViolation => "SchemaViolation".to_string(),
        };

        match self.record_index {
            Some(record_idx) => write!(
                f,
                "{} at block {}, record {}, offset {}: {}",
                kind_str, self.block_index, record_idx, self.offset, self.message
            ),
            None => write!(
                f,
                "{} at block {}, offset {}: {}",
                kind_str, self.block_index, self.offset, self.message
            ),
        }
    }
}

/// Types of recoverable errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadErrorKind {
    /// Sync marker doesn't match expected value
    InvalidSyncMarker {
        /// Expected sync marker from file header
        expected: [u8; 16],
        /// Actual sync marker found in block
        actual: [u8; 16],
    },
    /// Block decompression failed
    DecompressionFailed {
        /// Name of the codec that failed
        codec: String,
    },
    /// Block parsing failed (truncated data, invalid varints, etc.)
    BlockParseFailed,
    /// Record decoding failed
    RecordDecodeFailed,
    /// Data violates schema constraints
    SchemaViolation,
}
