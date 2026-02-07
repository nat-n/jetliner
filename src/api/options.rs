//! Avro-specific reader options.
//!
//! This module defines `AvroOptions`, which controls how Avro data is read and decoded.
//! It is analogous to Polars' `ParquetOptions` - format-specific settings that are
//! separate from scan configuration (`ScanArgsAvro`).
//!
//! # Requirements
//! - 9.2: Supported parameters include buffer_blocks, buffer_bytes, read_chunk_size, batch_size
//! - 9.3: In Rust, these are organized in a separate `AvroOptions` struct

/// Avro-specific reader options.
///
/// Controls how Avro data is read and decoded. This struct is analogous to
/// Polars' `ParquetOptions` - it contains format-specific settings that are
/// separate from scan configuration.
///
/// # Future Extensibility
/// This struct may be extended with concurrency options in the future:
/// - `decode_workers`: Number of parallel decode workers
/// - `decompress_workers`: Number of parallel decompression workers
///
/// # Example
/// ```
/// use jetliner::api::AvroOptions;
///
/// let opts = AvroOptions {
///     buffer_blocks: 8,
///     buffer_bytes: 128 * 1024 * 1024, // 128MB
///     batch_size: 200_000,
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AvroOptions {
    /// Number of blocks to prefetch (default: 4).
    ///
    /// Higher values improve throughput for sequential reads but increase memory usage.
    pub buffer_blocks: usize,

    /// Maximum bytes to buffer during prefetching (default: 64MB).
    ///
    /// This limits memory usage when blocks are large.
    pub buffer_bytes: usize,

    /// I/O read chunk size in bytes (None = auto-detect based on source).
    ///
    /// For local files, this is typically the filesystem block size.
    /// For S3, this affects the size of range requests.
    pub read_chunk_size: Option<usize>,

    /// Target number of rows per DataFrame batch (default: 100,000).
    ///
    /// Larger batches reduce overhead but increase memory usage per batch.
    pub batch_size: usize,

    /// Maximum decompressed block size in bytes (default: 512MB).
    ///
    /// Blocks that would decompress to more than this limit are rejected.
    /// This protects against decompression bombs - maliciously crafted files
    /// where a small compressed block expands to consume excessive memory.
    ///
    /// Set to `None` to disable the limit (not recommended for untrusted data).
    pub max_decompressed_block_size: Option<usize>,
}

impl Default for AvroOptions {
    fn default() -> Self {
        Self {
            buffer_blocks: 4,
            buffer_bytes: 64 * 1024 * 1024, // 64MB
            read_chunk_size: None,
            batch_size: 100_000,
            max_decompressed_block_size: Some(512 * 1024 * 1024), // 512MB
        }
    }
}

impl AvroOptions {
    /// Create a new `AvroOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of blocks to prefetch.
    pub fn with_buffer_blocks(mut self, buffer_blocks: usize) -> Self {
        self.buffer_blocks = buffer_blocks;
        self
    }

    /// Set the maximum bytes to buffer.
    pub fn with_buffer_bytes(mut self, buffer_bytes: usize) -> Self {
        self.buffer_bytes = buffer_bytes;
        self
    }

    /// Set the I/O read chunk size.
    pub fn with_read_chunk_size(mut self, read_chunk_size: usize) -> Self {
        self.read_chunk_size = Some(read_chunk_size);
        self
    }

    /// Set the target batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the maximum decompressed block size.
    ///
    /// Blocks that would decompress to more than this limit are rejected.
    /// Set to `None` to disable the limit.
    pub fn with_max_decompressed_block_size(mut self, limit: Option<usize>) -> Self {
        self.max_decompressed_block_size = limit;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avro_options_default() {
        let opts = AvroOptions::default();
        assert_eq!(opts.buffer_blocks, 4);
        assert_eq!(opts.buffer_bytes, 64 * 1024 * 1024);
        assert_eq!(opts.read_chunk_size, None);
        assert_eq!(opts.batch_size, 100_000);
    }

    #[test]
    fn test_avro_options_new() {
        let opts = AvroOptions::new();
        assert_eq!(opts, AvroOptions::default());
    }

    #[test]
    fn test_avro_options_builder() {
        let opts = AvroOptions::new()
            .with_buffer_blocks(8)
            .with_buffer_bytes(128 * 1024 * 1024)
            .with_read_chunk_size(1024 * 1024)
            .with_batch_size(200_000);

        assert_eq!(opts.buffer_blocks, 8);
        assert_eq!(opts.buffer_bytes, 128 * 1024 * 1024);
        assert_eq!(opts.read_chunk_size, Some(1024 * 1024));
        assert_eq!(opts.batch_size, 200_000);
    }

    #[test]
    fn test_avro_options_clone() {
        let opts = AvroOptions::new().with_buffer_blocks(8);
        let cloned = opts.clone();
        assert_eq!(opts, cloned);
    }

    #[test]
    fn test_avro_options_debug() {
        let opts = AvroOptions::default();
        let debug_str = format!("{:?}", opts);
        assert!(debug_str.contains("AvroOptions"));
        assert!(debug_str.contains("buffer_blocks"));
    }
}
