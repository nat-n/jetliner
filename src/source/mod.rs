//! Data source abstractions for streaming Avro data
//!
//! This module provides a unified interface for reading Avro data from
//! different sources (S3, local filesystem) with async range-request support.

mod local;
mod s3;
mod traits;

pub use local::LocalSource;
pub use s3::{S3Config, S3Source, DEFAULT_MAX_RETRIES};
pub use traits::{BoxedSource, StreamSource};
