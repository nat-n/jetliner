//! Error types for Avro streaming
//!
//! This module defines the error types used throughout Jetliner.
//! It also provides conversion to `PolarsError` for API compatibility.
//!
//! # Requirements
//! - 14.2: The library implements `From<ReaderError> for PolarsError` for API compatibility
//! - 14.3: Public API functions return `PolarsResult` for Polars ecosystem compatibility
//! - 14.4: Error conversion maps to appropriate Polars error variants

use std::io;
use std::sync::Arc;
use thiserror::Error;

use polars::prelude::PolarsError;

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
    /// Decompressed size exceeds configured limit (protection against decompression bombs)
    ///
    /// For snappy, the exact decompressed size is known from the header.
    /// For streaming codecs, we only know it exceeded the limit.
    #[error("{}", match .size {
        Some(s) => format!("Decompressed size ({} bytes) exceeds limit of {} bytes", s, .limit),
        None => format!("Decompressed output exceeds limit of {} bytes", .limit),
    })]
    OversizedOutput {
        /// Decompressed size if known (snappy), None for streaming codecs
        size: Option<usize>,
        /// Configured limit
        limit: usize,
    },
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

    /// Parse error at specific file offset
    #[error("Parse error at offset {file_offset}: {message}")]
    Parse { file_offset: u64, message: String },

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
        "Invalid sync marker at block {block_index}, offset {file_offset}: expected {}, got {}",
        format_sync_marker(expected),
        format_sync_marker(actual)
    )]
    InvalidSyncMarker {
        block_index: usize,
        file_offset: u64,
        /// Expected sync marker from file header
        expected: [u8; 16],
        /// Actual sync marker found in block
        actual: [u8; 16],
    },

    /// Builder error (from DataFrameBuilder)
    #[error("Builder error: {0}")]
    Builder(crate::convert::BuilderError),

    /// Error with file path context
    ///
    /// This variant wraps another error with the path of the file that caused it.
    /// Used during multi-file operations (e.g., schema validation) to identify
    /// which file caused the error.
    #[error("In file '{path}': {source}")]
    InFile {
        /// The path of the file that caused the error
        path: String,
        /// The underlying error
        #[source]
        source: Box<ReaderError>,
    },
}

impl ReaderError {
    /// Wrap this error with file path context.
    ///
    /// Returns `InFile { path, source: Box::new(self) }`.
    pub fn in_file(self, path: impl Into<String>) -> Self {
        ReaderError::InFile {
            path: path.into(),
            source: Box::new(self),
        }
    }

    /// Extract the file path from this error, if available.
    ///
    /// Returns the path from `InFile` variant, or `None` for other variants.
    pub fn file_path(&self) -> Option<&str> {
        match self {
            ReaderError::InFile { path, .. } => Some(path.as_str()),
            _ => None,
        }
    }

    /// Unwrap `InFile` variant to get the inner error, or return self.
    ///
    /// This is useful when you want to pattern-match on the underlying error
    /// while still having access to the path separately.
    pub fn into_inner(self) -> Self {
        match self {
            ReaderError::InFile { source, .. } => *source,
            other => other,
        }
    }
}

// Forward declaration to avoid circular dependency
// BuilderError is defined in convert::dataframe
impl From<crate::convert::BuilderError> for ReaderError {
    fn from(err: crate::convert::BuilderError) -> Self {
        ReaderError::Builder(err)
    }
}

/// Convert `PolarsError` to `ReaderError`.
///
/// This wraps Polars errors in the Builder variant since they typically
/// occur during DataFrame construction.
impl From<PolarsError> for ReaderError {
    fn from(err: PolarsError) -> Self {
        ReaderError::Builder(crate::convert::BuilderError::Polars(err))
    }
}

/// Recoverable error that occurred during reading (for skip mode)
#[derive(Debug, Clone)]
pub struct BadBlockError {
    /// The kind of error that occurred
    pub kind: BadBlockErrorKind,
    /// Block index where error occurred
    pub block_index: usize,
    /// Record index within block (if applicable)
    pub record_index: Option<usize>,
    /// File offset where error occurred
    pub file_offset: u64,
    /// Human-readable error message
    pub message: String,
    /// Optional file path where the error occurred
    pub file_path: Option<String>,
}

impl BadBlockError {
    /// Create a new BadBlockError
    pub fn new(
        kind: BadBlockErrorKind,
        block_index: usize,
        record_index: Option<usize>,
        file_offset: u64,
        message: String,
    ) -> Self {
        Self {
            kind,
            block_index,
            record_index,
            file_offset,
            message,
            file_path: None,
        }
    }

    /// Add a file path to this error (builder pattern)
    pub fn with_file_path(mut self, path: impl Into<String>) -> Self {
        self.file_path = Some(path.into());
        self
    }
}

impl std::fmt::Display for BadBlockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Format the error kind with details
        let kind_str = match &self.kind {
            BadBlockErrorKind::InvalidSyncMarker { expected, actual } => {
                format!(
                    "InvalidSyncMarker (expected {}, got {})",
                    format_sync_marker(expected),
                    format_sync_marker(actual)
                )
            }
            BadBlockErrorKind::DecompressionFailed { codec } => {
                format!("DecompressionFailed (codec: {})", codec)
            }
            BadBlockErrorKind::BlockParseFailed => "BlockParseFailed".to_string(),
            BadBlockErrorKind::RecordDecodeFailed => "RecordDecodeFailed".to_string(),
            BadBlockErrorKind::SchemaViolation => "SchemaViolation".to_string(),
        };

        // Format file path if present
        let file_info = self
            .file_path
            .as_ref()
            .map(|p| format!(" in '{}'", p))
            .unwrap_or_default();

        match self.record_index {
            Some(record_idx) => write!(
                f,
                "{}{} at block {}, record {}, offset {}: {}",
                kind_str, file_info, self.block_index, record_idx, self.file_offset, self.message
            ),
            None => write!(
                f,
                "{}{} at block {}, offset {}: {}",
                kind_str, file_info, self.block_index, self.file_offset, self.message
            ),
        }
    }
}

/// Types of recoverable errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BadBlockErrorKind {
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

// =============================================================================
// Polars Error Conversion
// =============================================================================

/// Convert `SourceError` to `PolarsError` for API compatibility.
impl From<SourceError> for PolarsError {
    fn from(err: SourceError) -> Self {
        match err {
            SourceError::NotFound(ref _path) => PolarsError::IO {
                error: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    err.to_string(),
                )),
                msg: None,
            },
            _ => PolarsError::IO {
                error: Arc::new(std::io::Error::other(err.to_string())),
                msg: None,
            },
        }
    }
}

/// Convert `SchemaError` to `PolarsError` for API compatibility.
impl From<SchemaError> for PolarsError {
    fn from(err: SchemaError) -> Self {
        PolarsError::SchemaMismatch(err.to_string().into())
    }
}

/// Convert `ReaderError` to `PolarsError` for API compatibility.
///
/// This implementation maps Jetliner's rich error types to appropriate
/// Polars error variants:
///
/// - `ReaderError::Schema` → `PolarsError::SchemaMismatch`
/// - `ReaderError::Source(NotFound)` → `PolarsError::IO` with `NotFound` kind
/// - `ReaderError::Source(_)` → `PolarsError::IO`
/// - Other errors → `PolarsError::ComputeError`
///
/// # Requirements
/// - 14.2: Implement `From<ReaderError> for PolarsError`
/// - 14.4: Map to appropriate Polars error variants
impl From<ReaderError> for PolarsError {
    fn from(err: ReaderError) -> Self {
        match err {
            // InFile wraps another error with path context - unwrap and convert
            ReaderError::InFile { source, .. } => (*source).into(),

            // Schema errors map to SchemaMismatch
            ReaderError::Schema(ref _schema_err) => {
                PolarsError::SchemaMismatch(err.to_string().into())
            }

            // NotFound source errors map to IO with NotFound kind
            ReaderError::Source(SourceError::NotFound(ref _path)) => PolarsError::IO {
                error: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    err.to_string(),
                )),
                msg: None,
            },

            // Other source errors map to IO
            ReaderError::Source(ref _source_err) => PolarsError::IO {
                error: Arc::new(std::io::Error::other(err.to_string())),
                msg: None,
            },

            // Builder errors: map based on inner variant
            ReaderError::Builder(ref builder_err) => match builder_err {
                crate::convert::BuilderError::Schema(_) => {
                    PolarsError::SchemaMismatch(err.to_string().into())
                }
                crate::convert::BuilderError::Decode { .. }
                | crate::convert::BuilderError::Polars(_) => {
                    PolarsError::ComputeError(err.to_string().into())
                }
            },

            // All other errors map to ComputeError
            _ => PolarsError::ComputeError(err.to_string().into()),
        }
    }
}

#[cfg(test)]
mod polars_error_tests {
    use super::*;

    #[test]
    fn test_schema_error_to_polars() {
        let err = ReaderError::Schema(SchemaError::InvalidSchema("test".to_string()));
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::SchemaMismatch(msg) => {
                assert!(msg.contains("Schema error"));
            }
            _ => panic!("Expected SchemaMismatch"),
        }
    }

    #[test]
    fn test_not_found_error_to_polars() {
        let err = ReaderError::Source(SourceError::NotFound("file.avro".to_string()));
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::IO { error, .. } => {
                assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
            }
            _ => panic!("Expected IO error"),
        }
    }

    #[test]
    fn test_source_error_to_polars() {
        let err = ReaderError::Source(SourceError::S3Error("connection failed".to_string()));
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::IO { error, .. } => {
                assert!(error.to_string().contains("S3 error"));
            }
            _ => panic!("Expected IO error"),
        }
    }

    #[test]
    fn test_decode_error_to_polars() {
        let err = ReaderError::Decode {
            block_index: 1,
            record_index: 5,
            message: "invalid data".to_string(),
        };
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::ComputeError(msg) => {
                assert!(msg.contains("Decode error"));
            }
            _ => panic!("Expected ComputeError"),
        }
    }

    #[test]
    fn test_parse_error_to_polars() {
        let err = ReaderError::Parse {
            file_offset: 100,
            message: "invalid header".to_string(),
        };
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::ComputeError(msg) => {
                assert!(msg.contains("Parse error"));
            }
            _ => panic!("Expected ComputeError"),
        }
    }

    #[test]
    fn test_codec_error_to_polars() {
        let err = ReaderError::Codec(CodecError::UnsupportedCodec("unknown".to_string()));
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::ComputeError(msg) => {
                assert!(msg.contains("Codec error"));
            }
            _ => panic!("Expected ComputeError"),
        }
    }

    #[test]
    fn test_invalid_magic_to_polars() {
        let err = ReaderError::InvalidMagic([0, 0, 0, 0]);
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::ComputeError(msg) => {
                assert!(msg.contains("Invalid magic bytes"));
            }
            _ => panic!("Expected ComputeError"),
        }
    }

    #[test]
    fn test_configuration_error_to_polars() {
        let err = ReaderError::Configuration("invalid config".to_string());
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::ComputeError(msg) => {
                assert!(msg.contains("Configuration error"));
            }
            _ => panic!("Expected ComputeError"),
        }
    }

    // =========================================================================
    // BuilderError to ReaderError conversion tests
    // =========================================================================

    #[test]
    fn test_builder_error_schema_to_reader_error() {
        let schema_err = SchemaError::InvalidSchema("bad schema".to_string());
        let builder_err = crate::convert::BuilderError::Schema(schema_err);
        let reader_err: ReaderError = builder_err.into();

        match reader_err {
            ReaderError::Builder(inner) => match inner {
                crate::convert::BuilderError::Schema(s) => {
                    assert!(s.to_string().contains("bad schema"));
                }
                _ => panic!("Expected Schema variant"),
            },
            _ => panic!("Expected Builder variant"),
        }
    }

    #[test]
    fn test_builder_error_decode_to_reader_error() {
        let decode_err = DecodeError::InvalidData("invalid bytes".to_string());
        let builder_err = crate::convert::BuilderError::Decode {
            error: decode_err,
            block_index: 5,
            record_index: Some(10),
        };
        let reader_err: ReaderError = builder_err.into();

        match reader_err {
            ReaderError::Builder(inner) => match inner {
                crate::convert::BuilderError::Decode {
                    error,
                    block_index,
                    record_index,
                } => {
                    assert!(error.to_string().contains("invalid"));
                    assert_eq!(block_index, 5);
                    assert_eq!(record_index, Some(10));
                }
                _ => panic!("Expected Decode variant with position"),
            },
            _ => panic!("Expected Builder variant"),
        }
    }

    #[test]
    fn test_builder_error_decode_display_includes_position() {
        let decode_err = DecodeError::InvalidVarint;
        let builder_err = crate::convert::BuilderError::Decode {
            error: decode_err,
            block_index: 42,
            record_index: Some(7),
        };

        let msg = builder_err.to_string();
        assert!(
            msg.contains("42") && msg.contains("7"),
            "Error message should include block 42 and record 7, got: {}",
            msg
        );
    }

    #[test]
    fn test_builder_error_decode_display_without_record_index() {
        let decode_err = DecodeError::InvalidVarint;
        let builder_err = crate::convert::BuilderError::Decode {
            error: decode_err,
            block_index: 42,
            record_index: None,
        };

        let msg = builder_err.to_string();
        assert!(
            msg.contains("block 42"),
            "Error message should include block 42, got: {}",
            msg
        );
        // Should NOT say "record" when record_index is None
        assert!(
            !msg.contains("record"),
            "Error message should NOT include 'record' when record_index is None, got: {}",
            msg
        );
    }

    #[test]
    fn test_builder_error_polars_to_reader_error() {
        let polars_err = PolarsError::ComputeError("polars error".into());
        let builder_err: crate::convert::BuilderError = polars_err.into();
        let reader_err: ReaderError = builder_err.into();

        match reader_err {
            ReaderError::Builder(inner) => match inner {
                crate::convert::BuilderError::Polars(err) => {
                    assert!(err.to_string().contains("polars error"));
                }
                _ => panic!("Expected Polars variant"),
            },
            _ => panic!("Expected Builder variant"),
        }
    }

    // =========================================================================
    // ReaderError::Builder to PolarsError conversion tests
    // =========================================================================

    #[test]
    fn test_builder_schema_error_to_polars() {
        let schema_err = SchemaError::UnsupportedType("complex union".to_string());
        let builder_err = crate::convert::BuilderError::Schema(schema_err);
        let err = ReaderError::Builder(builder_err);
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::SchemaMismatch(msg) => {
                assert!(msg.contains("complex union"));
            }
            _ => panic!("Expected SchemaMismatch"),
        }
    }

    #[test]
    fn test_builder_decode_error_to_polars() {
        let decode_err = DecodeError::InvalidVarint;
        let builder_err = crate::convert::BuilderError::Decode {
            error: decode_err,
            block_index: 1,
            record_index: Some(0),
        };
        let err = ReaderError::Builder(builder_err);
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::ComputeError(msg) => {
                assert!(msg.contains("varint") || msg.contains("Decode") || msg.contains("decode"));
            }
            _ => panic!("Expected ComputeError"),
        }
    }

    #[test]
    fn test_builder_polars_error_to_polars() {
        let inner_polars_err = PolarsError::ComputeError("DataFrame construction failed".into());
        let builder_err: crate::convert::BuilderError = inner_polars_err.into();
        let err = ReaderError::Builder(builder_err);
        let polars_err: PolarsError = err.into();

        match polars_err {
            PolarsError::ComputeError(msg) => {
                assert!(msg.contains("DataFrame construction failed"));
            }
            _ => panic!("Expected ComputeError"),
        }
    }

    // =========================================================================
    // Error display/debug preservation tests
    // =========================================================================

    #[test]
    fn test_builder_error_display_preserves_message() {
        let builder_err = crate::convert::BuilderError::Schema(SchemaError::InvalidSchema(
            "field 'x' has invalid type".to_string(),
        ));
        let reader_err = ReaderError::Builder(builder_err);

        let display = reader_err.to_string();
        assert!(display.contains("field 'x' has invalid type"));
    }

    #[test]
    fn test_reader_error_builder_variant_is_not_flattened() {
        // This test ensures we didn't accidentally flatten BuilderError to String
        let builder_err = crate::convert::BuilderError::Decode {
            error: DecodeError::UnexpectedEof,
            block_index: 0,
            record_index: Some(0),
        };
        let reader_err = ReaderError::Builder(builder_err);

        // We should be able to pattern match on the inner variant
        if let ReaderError::Builder(inner) = reader_err {
            assert!(matches!(inner, crate::convert::BuilderError::Decode { .. }));
        } else {
            panic!("Expected Builder variant");
        }
    }

    // =========================================================================
    // BadBlockError file_path tests
    // =========================================================================

    #[test]
    fn test_bad_block_error_with_file_path() {
        let err = BadBlockError::new(
            BadBlockErrorKind::BlockParseFailed,
            5,
            Some(10),
            1024,
            "test error".to_string(),
        )
        .with_file_path("test.avro");

        assert_eq!(err.file_path, Some("test.avro".to_string()));
        assert_eq!(err.block_index, 5);
        assert_eq!(err.record_index, Some(10));
        assert_eq!(err.file_offset, 1024);
        assert_eq!(err.message, "test error");
    }

    #[test]
    fn test_bad_block_error_default_no_file_path() {
        let err = BadBlockError::new(
            BadBlockErrorKind::RecordDecodeFailed,
            3,
            None,
            512,
            "decode failed".to_string(),
        );

        assert_eq!(err.file_path, None);
    }

    #[test]
    fn test_bad_block_error_display_with_file_path() {
        let err = BadBlockError::new(
            BadBlockErrorKind::BlockParseFailed,
            5,
            None,
            1024,
            "test error".to_string(),
        )
        .with_file_path("data/file.avro");

        let display = err.to_string();
        assert!(display.contains("'data/file.avro'"));
        assert!(display.contains("block 5"));
    }

    #[test]
    fn test_bad_block_error_display_without_file_path() {
        let err = BadBlockError::new(
            BadBlockErrorKind::BlockParseFailed,
            5,
            None,
            1024,
            "test error".to_string(),
        );

        let display = err.to_string();
        // Should not contain file path formatting
        assert!(!display.contains(" in '"));
        assert!(display.contains("block 5"));
    }

    #[test]
    fn test_bad_block_error_display_with_record_index_and_file_path() {
        let err = BadBlockError::new(
            BadBlockErrorKind::RecordDecodeFailed,
            3,
            Some(42),
            2048,
            "record decode failed".to_string(),
        )
        .with_file_path("s3://bucket/key.avro");

        let display = err.to_string();
        assert!(display.contains("'s3://bucket/key.avro'"));
        assert!(display.contains("block 3"));
        assert!(display.contains("record 42"));
    }
}
