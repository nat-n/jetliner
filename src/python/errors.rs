//! Python exception mapping for Jetliner errors.
//!
//! This module maps Rust errors to Python exceptions defined in
//! `jetliner.exceptions`. The exception classes are defined in Python
//! to enable proper inheritance hierarchy with structured attributes.
//!
//! # Exception Hierarchy
//! ```text
//! Exception (Python built-in)
//! └── JetlinerError (base class for all Jetliner errors)
//!     ├── DecodeError   - message, variant, path, block_index, record_index, file_offset, block_offset
//!     ├── ParseError    - message, variant, path, file_offset
//!     ├── SourceError   - message, variant, path
//!     ├── SchemaError   - message, variant, path, schema_context
//!     ├── CodecError    - message, variant, path, codec, block_index, file_offset
//!     ├── AuthenticationError - message, variant, path
//!     ├── FileNotFoundError - message, variant, path (also inherits from builtins.FileNotFoundError)
//!     ├── PermissionError - message, variant, path (also inherits from builtins.PermissionError)
//!     └── ConfigurationError - message, variant (also inherits from builtins.ValueError)
//! ```

use pyo3::prelude::*;
use pyo3::types::PyModule;

use polars::prelude::PolarsError;

use crate::convert::BuilderError;
use crate::error::{
    CodecError as RustCodecError, DecodeError as RustDecodeError, ReaderError,
    SchemaError as RustSchemaError, SourceError as RustSourceError,
};

// =============================================================================
// Exception class accessors
// =============================================================================

/// Get the jetliner.exceptions module.
fn get_exceptions_module(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    py.import("jetliner.exceptions")
}

/// Create a DecodeError with structured attributes.
#[allow(clippy::too_many_arguments)]
fn create_decode_error(
    py: Python<'_>,
    message: String,
    variant: &str,
    path: Option<&str>,
    block_index: Option<usize>,
    record_index: Option<usize>,
    file_offset: Option<u64>,
    block_offset: Option<u64>,
) -> PyErr {
    match get_exceptions_module(py) {
        Ok(module) => match module.getattr("DecodeError") {
            Ok(cls) => {
                match cls.call1((
                    message.clone(),
                    variant,
                    path,
                    block_index,
                    record_index,
                    file_offset,
                    block_offset,
                )) {
                    Ok(instance) => PyErr::from_value(instance.into_any()),
                    Err(e) => e,
                }
            }
            Err(e) => e,
        },
        Err(_) => {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("DecodeError: {}", message))
        }
    }
}

/// Create a ParseError with structured attributes.
fn create_parse_error(
    py: Python<'_>,
    message: String,
    variant: &str,
    path: Option<&str>,
    file_offset: Option<u64>,
) -> PyErr {
    match get_exceptions_module(py) {
        Ok(module) => match module.getattr("ParseError") {
            Ok(cls) => match cls.call1((message.clone(), variant, path, file_offset)) {
                Ok(instance) => PyErr::from_value(instance.into_any()),
                Err(e) => e,
            },
            Err(e) => e,
        },
        Err(_) => {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("ParseError: {}", message))
        }
    }
}

/// Create a SourceError with structured attributes.
fn create_source_error(
    py: Python<'_>,
    message: String,
    variant: &str,
    path: Option<&str>,
) -> PyErr {
    match get_exceptions_module(py) {
        Ok(module) => match module.getattr("SourceError") {
            Ok(cls) => match cls.call1((message.clone(), variant, path)) {
                Ok(instance) => PyErr::from_value(instance.into_any()),
                Err(e) => e,
            },
            Err(e) => e,
        },
        Err(_) => {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("SourceError: {}", message))
        }
    }
}

/// Create a SchemaError with structured attributes.
fn create_schema_error(
    py: Python<'_>,
    message: String,
    variant: &str,
    path: Option<&str>,
    schema_context: Option<String>,
) -> PyErr {
    match get_exceptions_module(py) {
        Ok(module) => match module.getattr("SchemaError") {
            Ok(cls) => match cls.call1((message.clone(), variant, path, schema_context)) {
                Ok(instance) => PyErr::from_value(instance.into_any()),
                Err(e) => e,
            },
            Err(e) => e,
        },
        Err(_) => {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("SchemaError: {}", message))
        }
    }
}

/// Create a CodecError with structured attributes.
fn create_codec_error(
    py: Python<'_>,
    message: String,
    variant: &str,
    path: Option<&str>,
    codec: String,
    block_index: Option<usize>,
    file_offset: Option<u64>,
) -> PyErr {
    match get_exceptions_module(py) {
        Ok(module) => match module.getattr("CodecError") {
            Ok(cls) => {
                match cls.call1((
                    message.clone(),
                    variant,
                    path,
                    codec,
                    block_index,
                    file_offset,
                )) {
                    Ok(instance) => PyErr::from_value(instance.into_any()),
                    Err(e) => e,
                }
            }
            Err(e) => e,
        },
        Err(_) => {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("CodecError: {}", message))
        }
    }
}

/// Create an AuthenticationError with structured attributes.
fn create_authentication_error(
    py: Python<'_>,
    message: String,
    variant: &str,
    path: Option<&str>,
) -> PyErr {
    match get_exceptions_module(py) {
        Ok(module) => match module.getattr("AuthenticationError") {
            Ok(cls) => match cls.call1((message.clone(), variant, path)) {
                Ok(instance) => PyErr::from_value(instance.into_any()),
                Err(e) => e,
            },
            Err(e) => e,
        },
        Err(_) => PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "AuthenticationError: {}",
            message
        )),
    }
}

/// Create a FileNotFoundError with structured attributes.
fn create_file_not_found_error(
    py: Python<'_>,
    message: String,
    variant: &str,
    path: Option<&str>,
) -> PyErr {
    match get_exceptions_module(py) {
        Ok(module) => match module.getattr("FileNotFoundError") {
            Ok(cls) => match cls.call1((message.clone(), variant, path)) {
                Ok(instance) => PyErr::from_value(instance.into_any()),
                Err(e) => e,
            },
            Err(e) => e,
        },
        Err(_) => PyErr::new::<pyo3::exceptions::PyFileNotFoundError, _>(message),
    }
}

/// Create a PermissionError with structured attributes.
fn create_permission_error(
    py: Python<'_>,
    message: String,
    variant: &str,
    path: Option<&str>,
) -> PyErr {
    match get_exceptions_module(py) {
        Ok(module) => match module.getattr("PermissionError") {
            Ok(cls) => match cls.call1((message.clone(), variant, path)) {
                Ok(instance) => PyErr::from_value(instance.into_any()),
                Err(e) => e,
            },
            Err(e) => e,
        },
        Err(_) => PyErr::new::<pyo3::exceptions::PyPermissionError, _>(message),
    }
}

/// Create a ConfigurationError with structured attributes.
fn create_configuration_error(py: Python<'_>, message: String, variant: &str) -> PyErr {
    match get_exceptions_module(py) {
        Ok(module) => match module.getattr("ConfigurationError") {
            Ok(cls) => match cls.call1((message.clone(), variant)) {
                Ok(instance) => PyErr::from_value(instance.into_any()),
                Err(e) => e,
            },
            Err(e) => e,
        },
        Err(_) => PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Configuration error: {}",
            message
        )),
    }
}

// =============================================================================
// Variant name extraction
// =============================================================================

/// Extract variant name from DecodeError.
fn decode_error_variant(err: &RustDecodeError) -> &'static str {
    match err {
        RustDecodeError::InvalidData(_) => "InvalidData",
        RustDecodeError::UnexpectedEof => "UnexpectedEof",
        RustDecodeError::TypeMismatch(_) => "TypeMismatch",
        RustDecodeError::Io(_) => "Io",
        RustDecodeError::InvalidVarint => "InvalidVarint",
        RustDecodeError::InvalidUtf8(_) => "InvalidUtf8",
    }
}

/// Extract variant name from SchemaError.
fn schema_error_variant(err: &RustSchemaError) -> &'static str {
    match err {
        RustSchemaError::InvalidSchema(_) => "InvalidSchema",
        RustSchemaError::UnsupportedType(_) => "UnsupportedType",
        RustSchemaError::ParseError(_) => "ParseError",
        RustSchemaError::IncompatibleSchemas(_) => "IncompatibleSchemas",
    }
}

/// Map a RustSchemaError to a Python SchemaError exception.
///
/// This helper extracts variant info and context from the error and creates
/// the appropriate Python exception.
fn map_schema_error_to_py(
    py: Python<'_>,
    schema_err: &RustSchemaError,
    path: Option<&str>,
) -> PyErr {
    let variant = schema_error_variant(schema_err);
    let (message, context) = match schema_err {
        RustSchemaError::InvalidSchema(msg) => (format!("Invalid schema: {}", msg), None),
        RustSchemaError::UnsupportedType(type_name) => (
            format!("Unsupported type: {}", type_name),
            Some(type_name.clone()),
        ),
        RustSchemaError::ParseError(msg) => (format!("Schema parse error: {}", msg), None),
        RustSchemaError::IncompatibleSchemas(msg) => {
            (format!("Incompatible schemas: {}", msg), None)
        }
    };

    create_schema_error(py, message, variant, path, context)
}

/// Extract variant name from CodecError.
fn codec_error_variant(err: &RustCodecError) -> &'static str {
    match err {
        RustCodecError::UnsupportedCodec(_) => "UnsupportedCodec",
        RustCodecError::CompressionError(_) => "CompressionError",
        RustCodecError::DecompressionError(_) => "DecompressionError",
        RustCodecError::OversizedOutput { .. } => "OversizedOutput",
    }
}

/// Extract variant name from SourceError.
fn source_error_variant(err: &RustSourceError) -> &'static str {
    match err {
        RustSourceError::S3Error(_) => "S3Error",
        RustSourceError::FileSystemError(_) => "FileSystemError",
        RustSourceError::Io(_) => "Io",
        RustSourceError::NotFound(_) => "NotFound",
        RustSourceError::PermissionDenied(_) => "PermissionDenied",
        RustSourceError::AuthenticationFailed(_) => "AuthenticationFailed",
    }
}

// =============================================================================
// Error mapping functions
// =============================================================================

/// Map a ReaderError to an appropriate Python exception with structured metadata.
///
/// Use this version when you have a `Python<'_>` token available.
///
/// If the error is a [`ReaderError::InFile`] variant, the path from the error
/// takes precedence over the `path` parameter, ensuring that errors during
/// multi-file operations (like schema validation) correctly identify which
/// file caused the error.
pub fn map_reader_error(py: Python<'_>, path: &str, err: ReaderError) -> PyErr {
    match err {
        // Handle InFile variant: use embedded path and map inner error
        ReaderError::InFile {
            path: error_path,
            source,
        } => map_reader_error(py, &error_path, *source),

        ReaderError::Decode {
            block_index,
            record_index,
            message,
        } => create_decode_error(
            py,
            message,
            "Decode", // Generic variant when we don't have the original error
            Some(path),
            Some(block_index),
            Some(record_index),
            None, // file_offset not available in this error variant
            None, // block_offset not available in this error variant
        ),

        ReaderError::Parse {
            file_offset,
            message,
        } => create_parse_error(py, message, "Parse", Some(path), Some(file_offset)),

        ReaderError::InvalidMagic(magic) => {
            let message = format!(
                "Invalid Avro file: expected magic bytes 'Obj\\x01', found {:?}",
                magic
            );
            create_parse_error(py, message, "InvalidMagic", Some(path), Some(0))
            // At file start
        }

        ReaderError::InvalidSyncMarker {
            block_index,
            file_offset,
            expected,
            actual,
        } => {
            let message = format!(
                "Invalid sync marker at block {}: expected {:?}, got {:?}",
                block_index, expected, actual
            );
            create_parse_error(
                py,
                message,
                "InvalidSyncMarker",
                Some(path),
                Some(file_offset),
            )
        }

        ReaderError::Source(source_err) => {
            let variant = source_error_variant(&source_err);
            match &source_err {
                RustSourceError::NotFound(p) => {
                    let message = format!("File not found: {}", p);
                    create_file_not_found_error(py, message, variant, Some(path))
                }
                RustSourceError::PermissionDenied(msg) => {
                    let message = format!("Permission denied: {}", msg);
                    create_permission_error(py, message, variant, Some(path))
                }
                RustSourceError::AuthenticationFailed(msg) => {
                    let message = format!("Authentication failed: {}", msg);
                    create_authentication_error(py, message, variant, Some(path))
                }
                RustSourceError::S3Error(msg) => {
                    let message = format!("S3 error: {}", msg);
                    create_source_error(py, message, variant, Some(path))
                }
                RustSourceError::FileSystemError(msg) => {
                    let message = format!("Filesystem error: {}", msg);
                    create_source_error(py, message, variant, Some(path))
                }
                RustSourceError::Io(io_err) => {
                    let message = format!("I/O error: {}", io_err);
                    create_source_error(py, message, variant, Some(path))
                }
            }
        }

        ReaderError::Schema(schema_err) => map_schema_error_to_py(py, &schema_err, Some(path)),

        ReaderError::Codec(codec_err) => {
            let variant = codec_error_variant(&codec_err);
            let (codec, message) = match &codec_err {
                RustCodecError::UnsupportedCodec(name) => {
                    (name.clone(), format!("Unsupported codec: {}", name))
                }
                RustCodecError::CompressionError(msg) => {
                    (String::new(), format!("Compression error: {}", msg))
                }
                RustCodecError::DecompressionError(msg) => {
                    (String::new(), format!("Decompression error: {}", msg))
                }
                RustCodecError::OversizedOutput { size, limit } => (
                    String::new(),
                    match size {
                        Some(s) => format!(
                            "Decompressed size ({} bytes) exceeds limit of {} bytes",
                            s, limit
                        ),
                        None => format!("Decompressed output exceeds limit of {} bytes", limit),
                    },
                ),
            };

            create_codec_error(py, message, variant, Some(path), codec, None, None)
        }

        ReaderError::Configuration(msg) => {
            create_configuration_error(py, format!("Configuration error: {}", msg), "Configuration")
        }

        ReaderError::Builder(builder_err) => {
            // Map BuilderError to appropriate Python exception using structured matching
            match builder_err {
                BuilderError::Schema(schema_err) => {
                    map_schema_error_to_py(py, &schema_err, Some(path))
                }
                BuilderError::Decode {
                    error: decode_err,
                    block_index,
                    record_index,
                } => {
                    let variant = decode_error_variant(&decode_err);
                    let message = decode_err.to_string();
                    create_decode_error(
                        py,
                        message,
                        variant,
                        Some(path),
                        Some(block_index),
                        record_index, // Already Option<usize>
                        None,
                        None,
                    )
                }
                BuilderError::Polars(polars_err) => {
                    // Polars errors during DataFrame construction
                    create_decode_error(
                        py,
                        polars_err.to_string(),
                        "Polars",
                        Some(path),
                        None,
                        None,
                        None,
                        None,
                    )
                }
            }
        }
    }
}

/// Map a ReaderError to a Python exception, acquiring the GIL internally.
///
/// Use this version when you don't have a `Python<'_>` token available.
pub fn map_reader_error_acquire_gil(path: &str, err: ReaderError) -> PyErr {
    Python::attach(|py| map_reader_error(py, path, err))
}

/// Map a PolarsError to an appropriate Python exception with structured metadata.
///
/// Uses enum variant matching instead of string parsing for reliable classification.
pub fn map_polars_error(py: Python<'_>, path: &str, err: PolarsError) -> PyErr {
    let error_msg = err.to_string();

    match err {
        // IO errors - check the underlying error kind
        PolarsError::IO { ref error, .. } => match error.kind() {
            std::io::ErrorKind::NotFound => create_file_not_found_error(
                py,
                format!("File not found: {}", path),
                "NotFound",
                Some(path),
            ),
            std::io::ErrorKind::PermissionDenied => {
                create_permission_error(py, error_msg, "PermissionDenied", Some(path))
            }
            _ => create_source_error(py, error_msg, "Io", Some(path)),
        },

        // Schema errors
        PolarsError::SchemaMismatch(ref _msg) => {
            create_schema_error(py, error_msg, "SchemaMismatch", Some(path), None)
        }

        // Compute errors - try to infer specific error type from message
        // This is necessary because ReaderError types get flattened to ComputeError
        PolarsError::ComputeError(ref msg) => {
            let msg_lower = msg.to_string().to_lowercase();

            // Invalid magic bytes -> ParseError
            if msg_lower.contains("invalid magic") || msg_lower.contains("magic bytes") {
                create_parse_error(py, error_msg, "InvalidMagic", Some(path), Some(0))
            }
            // Invalid sync marker -> ParseError
            else if msg_lower.contains("sync marker") {
                create_parse_error(py, error_msg, "InvalidSyncMarker", Some(path), None)
            }
            // Parse errors -> ParseError
            else if msg_lower.contains("parse error") {
                create_parse_error(py, error_msg, "Parse", Some(path), None)
            }
            // Codec/compression errors -> CodecError
            else if msg_lower.contains("codec")
                || msg_lower.contains("decompression")
                || msg_lower.contains("compression")
            {
                create_codec_error(
                    py,
                    error_msg,
                    "Codec",
                    Some(path),
                    String::new(),
                    None,
                    None,
                )
            }
            // Default to DecodeError for other compute errors
            else {
                create_decode_error(
                    py,
                    error_msg,
                    "ComputeError",
                    Some(path),
                    None,
                    None,
                    None,
                    None,
                )
            }
        }

        // All other Polars error variants - map to appropriate Python exceptions
        PolarsError::InvalidOperation(ref _msg) => {
            create_configuration_error(py, error_msg, "InvalidOperation")
        }

        PolarsError::OutOfBounds(ref _msg) => create_decode_error(
            py,
            error_msg,
            "OutOfBounds",
            Some(path),
            None,
            None,
            None,
            None,
        ),

        PolarsError::NoData(ref _msg) => {
            create_decode_error(py, error_msg, "NoData", Some(path), None, None, None, None)
        }

        PolarsError::ShapeMismatch(ref _msg) => {
            create_schema_error(py, error_msg, "ShapeMismatch", Some(path), None)
        }

        PolarsError::Duplicate(ref _msg) => {
            create_schema_error(py, error_msg, "Duplicate", Some(path), None)
        }

        // Catch-all for any other/new Polars error variants
        _ => create_decode_error(py, error_msg, "Polars", Some(path), None, None, None, None),
    }
}

/// Map a PolarsError to a Python exception, acquiring the GIL internally.
pub fn map_polars_error_acquire_gil(path: &str, err: PolarsError) -> PyErr {
    Python::attach(|py| map_polars_error(py, path, err))
}
