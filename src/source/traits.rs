//! StreamSource trait definition
//!
//! Provides a unified async interface for reading data from various sources.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::SourceError;

/// Abstraction over data sources with async range-request support.
///
/// This trait provides a unified interface for S3 and local filesystem access,
/// enabling efficient partial file reads through range requests.
///
/// # Requirements
/// - 4.1: Unified interface for S3 and local filesystem access
/// - 4.4: Support range requests for efficient partial file reads
#[async_trait]
pub trait StreamSource: Send + Sync {
    /// Read bytes from a specific offset with a given length.
    ///
    /// # Arguments
    /// * `offset` - The byte offset to start reading from
    /// * `length` - The number of bytes to read
    ///
    /// # Returns
    /// The bytes read from the specified range, or an error if the read fails.
    ///
    /// # Errors
    /// Returns `SourceError` if:
    /// - The offset is beyond the file size
    /// - The source is not accessible
    /// - An I/O error occurs
    async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, SourceError>;

    /// Get the total size of the data source in bytes.
    ///
    /// # Returns
    /// The total size in bytes, or an error if the size cannot be determined.
    async fn size(&self) -> Result<u64, SourceError>;

    /// Read all bytes from a specific offset to the end of the source.
    ///
    /// # Arguments
    /// * `offset` - The byte offset to start reading from
    ///
    /// # Returns
    /// All bytes from the offset to the end of the source.
    ///
    /// # Errors
    /// Returns `SourceError` if:
    /// - The offset is beyond the file size
    /// - The source is not accessible
    /// - An I/O error occurs
    async fn read_from(&self, offset: u64) -> Result<Bytes, SourceError>;
}

/// A boxed StreamSource for dynamic dispatch
pub type BoxedSource = Box<dyn StreamSource>;

/// Implement StreamSource for BoxedSource to allow using it with generic code
#[async_trait]
impl StreamSource for BoxedSource {
    async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, SourceError> {
        (**self).read_range(offset, length).await
    }

    async fn size(&self) -> Result<u64, SourceError> {
        (**self).size().await
    }

    async fn read_from(&self, offset: u64) -> Result<Bytes, SourceError> {
        (**self).read_from(offset).await
    }
}
