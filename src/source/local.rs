//! Local filesystem source implementation
//!
//! Provides async file I/O for reading Avro files from the local filesystem.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::Mutex;

use super::traits::StreamSource;
use crate::error::SourceError;

/// A data source for reading from local filesystem.
///
/// Uses tokio's async file I/O for non-blocking reads with support
/// for range requests via seek + read operations.
///
/// # Requirements
/// - 4.3: Use filesystem APIs for local file access
pub struct LocalSource {
    /// The file handle wrapped in a mutex for safe concurrent access
    file: Mutex<File>,
    /// Path to the file (for error reporting)
    path: PathBuf,
    /// Cached file size
    file_size: u64,
}

impl LocalSource {
    /// Open a local file for reading.
    ///
    /// # Arguments
    /// * `path` - Path to the file to open
    ///
    /// # Returns
    /// A new `LocalSource` instance, or an error if the file cannot be opened.
    ///
    /// # Errors
    /// Returns `SourceError::NotFound` if the file doesn't exist.
    /// Returns `SourceError::PermissionDenied` if access is denied.
    /// Returns `SourceError::Io` for other I/O errors.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, SourceError> {
        let path = path.as_ref().to_path_buf();

        let file = File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                SourceError::NotFound(path.display().to_string())
            } else if e.kind() == std::io::ErrorKind::PermissionDenied {
                SourceError::PermissionDenied(path.display().to_string())
            } else {
                SourceError::FileSystemError(format!("{}: {}", path.display(), e))
            }
        })?;

        let metadata = file.metadata().await.map_err(|e| {
            SourceError::FileSystemError(format!(
                "Failed to get metadata for {}: {}",
                path.display(),
                e
            ))
        })?;

        let file_size = metadata.len();

        Ok(Self {
            file: Mutex::new(file),
            path,
            file_size,
        })
    }

    /// Get the path to the file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl StreamSource for LocalSource {
    async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, SourceError> {
        if offset >= self.file_size {
            return Err(SourceError::FileSystemError(format!(
                "Offset {} is beyond file size {} for {}",
                offset,
                self.file_size,
                self.path.display()
            )));
        }

        // Clamp length to not exceed file bounds
        let available = (self.file_size - offset) as usize;
        let actual_length = length.min(available);

        let mut file = self.file.lock().await;

        // Seek to the requested offset
        file.seek(SeekFrom::Start(offset)).await.map_err(|e| {
            SourceError::FileSystemError(format!(
                "Failed to seek to offset {} in {}: {}",
                offset,
                self.path.display(),
                e
            ))
        })?;

        // Read the requested bytes
        let mut buffer = vec![0u8; actual_length];
        file.read_exact(&mut buffer).await.map_err(|e| {
            SourceError::FileSystemError(format!(
                "Failed to read {} bytes at offset {} from {}: {}",
                actual_length,
                offset,
                self.path.display(),
                e
            ))
        })?;

        Ok(Bytes::from(buffer))
    }

    async fn size(&self) -> Result<u64, SourceError> {
        Ok(self.file_size)
    }

    async fn read_from(&self, offset: u64) -> Result<Bytes, SourceError> {
        if offset >= self.file_size {
            return Ok(Bytes::new());
        }

        let length = (self.file_size - offset) as usize;
        self.read_range(offset, length).await
    }
}

impl std::fmt::Debug for LocalSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalSource")
            .field("path", &self.path)
            .field("file_size", &self.file_size)
            .finish()
    }
}
