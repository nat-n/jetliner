//! Avro block parsing and reading
//!
//! Parses Avro data blocks which contain:
//! - Record count (varint)
//! - Compressed data size (varint)
//! - Compressed data bytes
//! - 16-byte sync marker
//!
//! Also provides the `BlockReader` for orchestrating header parsing
//! and block iteration from a `StreamSource`.

use bytes::Bytes;

use crate::codec::Codec;
use crate::error::{DecodeError, ReaderError};
use crate::source::StreamSource;

use super::AvroHeader;

// =============================================================================
// ReadBufferConfig - Configuration for BlockReader's internal read buffer
// =============================================================================

/// Configuration for BlockReader's internal read buffer.
///
/// Controls how data is fetched from the source to minimize I/O operations.
/// Different sources have different optimal configurations:
///
/// - **Local filesystem**: Low latency (~0.1ms), OS page cache handles prefetching,
///   small reads are cheap. Use smaller chunks with no eager prefetch.
///
/// - **S3**: High latency (~50-100ms), each request costs money, larger reads
///   amortize overhead. Use larger chunks with eager prefetch to hide latency.
///
/// # Requirements
/// - 3.8: Retain unused bytes from previous I/O operations
/// - 3.9: Parse multiple small blocks without additional I/O
/// - 3.10: Minimize total I/O operations
/// - 3.11: Use 4MB default chunk size for S3
/// - 3.12: Use 64KB default chunk size for local filesystem
/// - 3.13: Expose read_chunk_size parameter for user tuning
///
/// # Example
/// ```ignore
/// use jetliner::reader::ReadBufferConfig;
///
/// // Use defaults for local files
/// let config = ReadBufferConfig::LOCAL_DEFAULT;
///
/// // Use defaults for S3
/// let config = ReadBufferConfig::S3_DEFAULT;
///
/// // Custom configuration
/// let config = ReadBufferConfig::new(1024 * 1024, 0.25); // 1MB chunks, 25% threshold
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReadBufferConfig {
    /// Size of each read chunk from the source in bytes.
    ///
    /// - Local default: 64KB (OS page cache handles the rest)
    /// - S3 default: 4MB (amortize HTTP request overhead)
    pub chunk_size: usize,

    /// Threshold (0.0-1.0) at which to trigger a prefetch.
    ///
    /// When buffer falls below this fraction of chunk_size, fetch more data.
    /// - Local default: 0.0 (fetch only when empty - OS handles prefetch)
    /// - S3 default: 0.5 (fetch when 50% consumed - hide latency)
    pub prefetch_threshold: f32,
}

impl ReadBufferConfig {
    /// Default config for local filesystem.
    ///
    /// Uses 64KB chunks with no eager prefetch since the OS page cache
    /// handles prefetching efficiently.
    pub const LOCAL_DEFAULT: Self = Self {
        chunk_size: 64 * 1024,   // 64KB
        prefetch_threshold: 0.0, // Fetch only when empty
    };

    /// Default config for S3 (optimized for high-latency, pay-per-request).
    ///
    /// Uses 4MB chunks with 50% prefetch threshold to amortize HTTP request
    /// overhead and hide latency by fetching ahead.
    ///
    /// Industry reference points:
    /// - Spark: 64-128MB chunks for S3
    /// - Polars cloud: 4MB chunks
    /// - DuckDB: 1-10MB chunks
    pub const S3_DEFAULT: Self = Self {
        chunk_size: 4 * 1024 * 1024, // 4MB
        prefetch_threshold: 0.5,     // Fetch at 50% consumed
    };

    /// Create config with custom chunk size (threshold defaults to 0.0).
    ///
    /// # Arguments
    /// * `chunk_size` - Size of each read chunk in bytes
    ///
    /// # Example
    /// ```ignore
    /// let config = ReadBufferConfig::with_chunk_size(1024 * 1024); // 1MB chunks
    /// ```
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        Self {
            chunk_size,
            prefetch_threshold: 0.0,
        }
    }

    /// Create config with both custom chunk size and prefetch threshold.
    ///
    /// # Arguments
    /// * `chunk_size` - Size of each read chunk in bytes
    /// * `prefetch_threshold` - Threshold (0.0-1.0) at which to trigger prefetch
    ///
    /// # Example
    /// ```ignore
    /// let config = ReadBufferConfig::new(2 * 1024 * 1024, 0.25); // 2MB, 25% threshold
    /// ```
    pub fn new(chunk_size: usize, prefetch_threshold: f32) -> Self {
        Self {
            chunk_size,
            prefetch_threshold: prefetch_threshold.clamp(0.0, 1.0),
        }
    }

    /// Get the prefetch trigger point in bytes.
    ///
    /// When the buffer has fewer bytes than this, a prefetch should be triggered.
    #[inline]
    pub fn prefetch_trigger_bytes(&self) -> usize {
        (self.chunk_size as f32 * self.prefetch_threshold) as usize
    }
}

impl Default for ReadBufferConfig {
    fn default() -> Self {
        Self::LOCAL_DEFAULT
    }
}

// =============================================================================
// AvroBlock and DecompressedBlock
// =============================================================================

/// A single data block from an Avro file.
///
/// Each block contains a batch of records that have been serialized
/// and optionally compressed together.
///
/// # Requirements
/// - 1.3: Validate sync markers match the file header's sync marker
/// - 3.1: Read and process one block at a time
#[derive(Debug, Clone)]
pub struct AvroBlock {
    /// Number of records in this block
    pub record_count: i64,
    /// Size of the compressed data in bytes
    pub compressed_size: i64,
    /// The compressed block data
    pub data: Bytes,
    /// The sync marker following this block
    pub sync_marker: [u8; 16],
    /// Position of this block in the file (for error reporting)
    pub file_offset: u64,
    /// Sequential block number (0-indexed)
    pub block_index: usize,
}

/// A decompressed block ready for record decoding.
#[derive(Debug, Clone)]
pub struct DecompressedBlock {
    /// Number of records in this block
    pub record_count: i64,
    /// The decompressed data containing serialized records
    pub data: Bytes,
    /// Sequential block number (for error reporting)
    pub block_index: usize,
}

impl AvroBlock {
    /// Parse an Avro block from raw bytes.
    ///
    /// # Arguments
    /// * `bytes` - The raw bytes starting at the block
    /// * `expected_sync` - The expected sync marker from the file header
    /// * `file_offset` - The offset in the file where this block starts
    /// * `block_index` - The sequential block number
    ///
    /// # Returns
    /// A tuple of (parsed block, bytes consumed) or an error.
    ///
    /// # Errors
    /// - `ReaderError::Parse` if the block structure is invalid
    /// - `ReaderError::InvalidSyncMarker` if sync marker doesn't match
    ///
    /// # Requirements
    /// - 1.3: Validate sync markers match the file header's sync marker
    /// - 3.1: Read and process one block at a time
    pub fn parse(
        bytes: &[u8],
        expected_sync: &[u8; 16],
        file_offset: u64,
        block_index: usize,
    ) -> Result<(Self, usize), ReaderError> {
        let mut cursor = bytes;
        let mut offset = 0u64;

        // Parse record count (signed varint, zigzag encoded)
        let record_count =
            decode_varint_signed(&mut cursor, &mut offset).map_err(|e| ReaderError::Parse {
                offset: file_offset + offset,
                message: format!("Failed to decode block record count: {}", e),
            })?;

        // Parse compressed size (signed varint, zigzag encoded)
        let compressed_size =
            decode_varint_signed(&mut cursor, &mut offset).map_err(|e| ReaderError::Parse {
                offset: file_offset + offset,
                message: format!("Failed to decode block compressed size: {}", e),
            })?;

        // Validate sizes
        if record_count < 0 {
            return Err(ReaderError::Parse {
                offset: file_offset,
                message: format!("Invalid negative record count: {}", record_count),
            });
        }

        if compressed_size < 0 {
            return Err(ReaderError::Parse {
                offset: file_offset + offset,
                message: format!("Invalid negative compressed size: {}", compressed_size),
            });
        }

        let compressed_size_usize = compressed_size as usize;

        // Check we have enough bytes for data + sync marker
        if cursor.len() < compressed_size_usize + 16 {
            return Err(ReaderError::Parse {
                offset: file_offset + offset,
                message: format!(
                    "Not enough bytes for block data: need {} + 16, have {}",
                    compressed_size_usize,
                    cursor.len()
                ),
            });
        }

        // Read compressed data
        let data = Bytes::copy_from_slice(&cursor[..compressed_size_usize]);
        cursor = &cursor[compressed_size_usize..];
        offset += compressed_size_usize as u64;

        // Read and validate sync marker
        let mut sync_marker = [0u8; 16];
        sync_marker.copy_from_slice(&cursor[..16]);
        offset += 16;

        if &sync_marker != expected_sync {
            return Err(ReaderError::InvalidSyncMarker {
                block_index,
                offset: file_offset + offset - 16,
                expected: *expected_sync,
                actual: sync_marker,
            });
        }

        let block = AvroBlock {
            record_count,
            compressed_size,
            data,
            sync_marker,
            file_offset,
            block_index,
        };

        Ok((block, offset as usize))
    }

    /// Check if this block is empty (contains no records).
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Get the total size of this block in bytes (including headers and sync marker).
    pub fn total_size(&self) -> usize {
        // This is an approximation since varint sizes vary
        // The actual consumed size is returned by parse()
        self.data.len() + 16 + 10 + 10 // data + sync + max varint sizes
    }
}

impl DecompressedBlock {
    /// Create a new decompressed block.
    pub fn new(record_count: i64, data: Bytes, block_index: usize) -> Self {
        Self {
            record_count,
            data,
            block_index,
        }
    }

    /// Check if this block is empty.
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }
}

// Use shared varint functions from the varint module
use super::varint::decode_zigzag_with_offset;

/// Decode a variable-length signed integer (zigzag encoded).
///
/// Avro uses zigzag encoding for signed integers, where the sign bit
/// is moved to the least significant position.
#[inline]
fn decode_varint_signed(cursor: &mut &[u8], offset: &mut u64) -> Result<i64, DecodeError> {
    decode_zigzag_with_offset(cursor, offset)
}

/// Size of the sync marker in bytes
const SYNC_MARKER_SIZE: usize = 16;

/// Reads and parses Avro blocks from a StreamSource.
///
/// `BlockReader` orchestrates header parsing and block iteration,
/// tracking the current file offset and block index. It supports
/// seeking to sync markers for resumable reads.
///
/// # Read Buffering
///
/// The BlockReader maintains an internal read buffer to minimize I/O operations.
/// When reading from the source, it fetches data in configurable chunks and
/// retains unused bytes for subsequent block parsing. This is critical for S3
/// performance where each `read_range` call is an HTTP request with ~50-100ms
/// latency and per-request costs.
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────────────────┐
/// │                        READ BUFFER LIFECYCLE                               │
/// ├────────────────────────────────────────────────────────────────────────────┤
/// │                                                                            │
/// │  Initial State:     read_buffer = []                                       │
/// │                                                                            │
/// │  After 1st read:    read_buffer = [████████████████████████] (chunk_size)  │
/// │                                   ^block1^                                 │
/// │                                                                            │
/// │  After parse:       read_buffer = [        ████████████████] (retained)    │
/// │                                           ^block2^                         │
/// │                                                                            │
/// │  S3 eager refill:   When buffer < 50% of chunk_size, trigger async fetch   │
/// │                     read_buffer = [████████████████████████████████████]   │
/// │                                                                            │
/// │  Local lazy refill: When buffer empty, fetch next chunk                    │
/// │                     read_buffer = [████████████████████████]               │
/// │                                                                            │
/// └────────────────────────────────────────────────────────────────────────────┘
/// ```
///
/// # Requirements
/// - 3.1: Read and process one block at a time
/// - 3.7: Support seeking to a specific block by sync marker
/// - 3.8: Retain unused bytes from previous I/O operations
/// - 3.9: Parse multiple small blocks without additional I/O
/// - 3.10: Minimize total I/O operations
///
/// # Example
/// ```ignore
/// use jetliner::reader::{BlockReader, ReadBufferConfig};
/// use jetliner::source::LocalSource;
///
/// // Use default config (64KB chunks for local files)
/// let source = LocalSource::open("data.avro").await?;
/// let mut reader = BlockReader::new(source).await?;
///
/// // Use S3-optimized config (4MB chunks)
/// let source = S3Source::from_uri("s3://bucket/key").await?;
/// let mut reader = BlockReader::with_config(source, ReadBufferConfig::S3_DEFAULT).await?;
///
/// while let Some(block) = reader.next_block().await? {
///     println!("Block {} has {} records", block.block_index, block.record_count);
/// }
/// ```
pub struct BlockReader<S: StreamSource> {
    /// The data source to read from
    source: S,
    /// Parsed file header
    header: AvroHeader,
    /// Current read offset in the file (where next read would start if buffer is empty)
    current_offset: u64,
    /// Current block index (0-indexed)
    block_index: usize,
    /// Total file size (cached)
    file_size: u64,
    /// Internal read buffer - retains unused bytes between block reads
    read_buffer: Bytes,
    /// Offset in file where read_buffer starts
    buffer_file_offset: u64,
    /// Read buffer configuration
    buffer_config: ReadBufferConfig,
}

impl<S: StreamSource> BlockReader<S> {
    /// Create a new BlockReader from a StreamSource with default configuration.
    ///
    /// Uses `ReadBufferConfig::LOCAL_DEFAULT` (64KB chunks, no eager prefetch).
    /// For S3 sources, consider using `with_config()` with `ReadBufferConfig::S3_DEFAULT`.
    ///
    /// # Arguments
    /// * `source` - The data source to read from
    ///
    /// # Returns
    /// A new BlockReader positioned at the first block.
    ///
    /// # Errors
    /// - `ReaderError::Source` if reading from the source fails
    /// - `ReaderError::InvalidMagic` if magic bytes don't match
    /// - `ReaderError::Parse` if header parsing fails
    /// - `ReaderError::Schema` if schema is invalid
    /// - `ReaderError::Codec` if codec is unknown
    pub async fn new(source: S) -> Result<Self, ReaderError> {
        Self::with_config(source, ReadBufferConfig::LOCAL_DEFAULT).await
    }

    /// Create a new BlockReader from a StreamSource with custom buffer configuration.
    ///
    /// # Arguments
    /// * `source` - The data source to read from
    /// * `config` - Buffer configuration (chunk size and prefetch threshold)
    ///
    /// # Returns
    /// A new BlockReader positioned at the first block.
    ///
    /// # Errors
    /// - `ReaderError::Source` if reading from the source fails
    /// - `ReaderError::InvalidMagic` if magic bytes don't match
    /// - `ReaderError::Parse` if header parsing fails
    /// - `ReaderError::Schema` if schema is invalid
    /// - `ReaderError::Codec` if codec is unknown
    ///
    /// # Example
    /// ```ignore
    /// // For S3 sources, use S3_DEFAULT for better performance
    /// let reader = BlockReader::with_config(s3_source, ReadBufferConfig::S3_DEFAULT).await?;
    ///
    /// // For local files with custom chunk size
    /// let config = ReadBufferConfig::with_chunk_size(128 * 1024); // 128KB
    /// let reader = BlockReader::with_config(local_source, config).await?;
    /// ```
    pub async fn with_config(source: S, config: ReadBufferConfig) -> Result<Self, ReaderError> {
        // Get file size
        let file_size = source.size().await?;

        // Read enough bytes for the header (we'll read more if needed)
        // Most headers are small, but we need to handle large schemas
        let initial_read_size = std::cmp::min(file_size as usize, config.chunk_size);
        let header_bytes = source.read_range(0, initial_read_size).await?;

        // Parse the header
        let header = AvroHeader::parse(&header_bytes)?;
        let header_size = header.header_size;

        // Calculate how much of the initial read is left after the header
        let header_size_usize = header_size as usize;
        let remaining_buffer = if header_size_usize < header_bytes.len() {
            header_bytes.slice(header_size_usize..)
        } else {
            Bytes::new()
        };

        Ok(Self {
            source,
            current_offset: header_size,
            block_index: 0,
            file_size,
            header,
            read_buffer: remaining_buffer,
            buffer_file_offset: header_size,
            buffer_config: config,
        })
    }

    /// Read the next block from the file using the internal read buffer.
    ///
    /// This method uses buffered reading to minimize I/O operations:
    /// 1. Checks if read_buffer contains enough data for the next block
    /// 2. If buffer is below prefetch_threshold, fetches more data from source
    /// 3. Parses the block from the buffer
    /// 4. Advances buffer position, retaining unused bytes
    ///
    /// Returns `None` when all blocks have been read (EOF).
    ///
    /// # Returns
    /// The next block, or `None` if at end of file.
    ///
    /// # Errors
    /// - `ReaderError::Source` if reading fails
    /// - `ReaderError::Parse` if block parsing fails
    /// - `ReaderError::InvalidSyncMarker` if sync marker doesn't match
    ///
    /// # Requirements
    /// - 3.1: Read and process one block at a time
    /// - 3.8: Retain unused bytes from previous I/O operations
    /// - 3.9: Parse multiple small blocks without additional I/O
    /// - 3.10: Minimize total I/O operations
    pub async fn next_block(&mut self) -> Result<Option<AvroBlock>, ReaderError> {
        // Check if we've reached the end of the file
        if self.current_offset >= self.file_size {
            return Ok(None);
        }

        // Ensure we have data in the buffer
        self.ensure_buffer_filled().await?;

        // If buffer is still empty after trying to fill, we're at EOF
        if self.read_buffer.is_empty() {
            return Ok(None);
        }

        // Try to parse the block from the buffer
        match AvroBlock::parse(
            &self.read_buffer,
            &self.header.sync_marker,
            self.current_offset,
            self.block_index,
        ) {
            Ok((block, consumed)) => {
                // Advance buffer position, retaining unused bytes
                self.advance_buffer(consumed);
                self.block_index += 1;
                Ok(Some(block))
            }
            Err(ReaderError::Parse { message, .. })
                if message.starts_with("Not enough bytes for block data") =>
            {
                // Block is larger than current buffer - need to read more
                // This handles blocks larger than chunk_size
                self.read_large_block_buffered().await
            }
            Err(e) => Err(e),
        }
    }

    /// Ensure the read buffer has data, fetching from source if needed.
    ///
    /// This method implements the prefetch threshold logic:
    /// - If buffer is below prefetch_threshold * chunk_size, fetch more data
    /// - For local files (threshold=0.0), only fetch when buffer is empty
    /// - For S3 (threshold=0.5), fetch when buffer is 50% consumed
    async fn ensure_buffer_filled(&mut self) -> Result<(), ReaderError> {
        let trigger_bytes = self.buffer_config.prefetch_trigger_bytes();

        // Check if we need to fetch more data
        if self.read_buffer.len() > trigger_bytes {
            return Ok(());
        }

        // Calculate how much more data is available in the file
        let buffer_end_offset = self.buffer_file_offset + self.read_buffer.len() as u64;
        if buffer_end_offset >= self.file_size {
            // No more data to fetch
            return Ok(());
        }

        // Calculate how much to read
        let remaining_in_file = self.file_size - buffer_end_offset;
        let read_size = std::cmp::min(remaining_in_file as usize, self.buffer_config.chunk_size);

        // Fetch more data from source
        let new_data = self.source.read_range(buffer_end_offset, read_size).await?;

        if new_data.is_empty() {
            return Ok(());
        }

        // Append new data to existing buffer
        if self.read_buffer.is_empty() {
            self.read_buffer = new_data;
        } else {
            // Combine existing buffer with new data
            let mut combined = Vec::with_capacity(self.read_buffer.len() + new_data.len());
            combined.extend_from_slice(&self.read_buffer);
            combined.extend_from_slice(&new_data);
            self.read_buffer = Bytes::from(combined);
        }

        Ok(())
    }

    /// Advance the buffer by consuming `bytes` from the front.
    ///
    /// Updates current_offset and buffer_file_offset to reflect the new position.
    fn advance_buffer(&mut self, bytes: usize) {
        self.current_offset += bytes as u64;
        if bytes >= self.read_buffer.len() {
            self.read_buffer = Bytes::new();
            self.buffer_file_offset = self.current_offset;
        } else {
            self.read_buffer = self.read_buffer.slice(bytes..);
            self.buffer_file_offset += bytes as u64;
        }
    }

    /// Handle reading a block that's larger than the current buffer.
    ///
    /// This is the slow path - only called when a block exceeds the buffer size.
    /// It reads exactly the bytes needed for the block.
    async fn read_large_block_buffered(&mut self) -> Result<Option<AvroBlock>, ReaderError> {
        // Parse just the header to get the required size
        let mut cursor = &self.read_buffer[..];
        let mut offset = 0u64;

        let _record_count =
            decode_varint_signed(&mut cursor, &mut offset).map_err(|e| ReaderError::Parse {
                offset: self.current_offset,
                message: format!("Failed to decode block record count: {}", e),
            })?;

        let compressed_size =
            decode_varint_signed(&mut cursor, &mut offset).map_err(|e| ReaderError::Parse {
                offset: self.current_offset + offset,
                message: format!("Failed to decode block compressed size: {}", e),
            })?;

        if compressed_size < 0 {
            return Err(ReaderError::Parse {
                offset: self.current_offset + offset,
                message: format!("Invalid negative compressed size: {}", compressed_size),
            });
        }

        // Calculate total bytes needed: header varints + compressed data + sync marker
        let header_size = offset as usize;
        let total_needed = header_size + compressed_size as usize + SYNC_MARKER_SIZE;

        let remaining_in_file = (self.file_size - self.current_offset) as usize;
        if total_needed > remaining_in_file {
            return Err(ReaderError::Parse {
                offset: self.current_offset,
                message: format!(
                    "Block size {} exceeds remaining file size {}",
                    total_needed, remaining_in_file
                ),
            });
        }

        // Calculate how much more we need to read
        let already_have = self.read_buffer.len();
        if total_needed <= already_have {
            // We actually have enough data - this shouldn't happen but handle it
            let (block, consumed) = AvroBlock::parse(
                &self.read_buffer,
                &self.header.sync_marker,
                self.current_offset,
                self.block_index,
            )?;
            self.advance_buffer(consumed);
            self.block_index += 1;
            return Ok(Some(block));
        }

        // Need to read more data
        let need_more = total_needed - already_have;
        let read_offset = self.buffer_file_offset + already_have as u64;
        let additional_data = self.source.read_range(read_offset, need_more).await?;

        // Combine existing buffer with additional data
        let mut combined = Vec::with_capacity(total_needed);
        combined.extend_from_slice(&self.read_buffer);
        combined.extend_from_slice(&additional_data);

        // Parse the complete block
        let (block, consumed) = AvroBlock::parse(
            &combined,
            &self.header.sync_marker,
            self.current_offset,
            self.block_index,
        )?;

        // Update state - we consumed the entire combined buffer for this block
        self.current_offset += consumed as u64;
        self.block_index += 1;

        // Clear the buffer and set offset to after the block
        self.read_buffer = Bytes::new();
        self.buffer_file_offset = self.current_offset;

        Ok(Some(block))
    }

    /// Handle reading a block that's larger than the default buffer size.
    ///
    /// This is the slow path - only called when a block exceeds 64KB.
    /// DEPRECATED: Use read_large_block_buffered instead.
    #[allow(dead_code)]
    async fn read_large_block(
        &mut self,
        initial_data: &[u8],
        remaining_in_file: usize,
    ) -> Result<Option<AvroBlock>, ReaderError> {
        // Parse just the header to get the required size
        let mut cursor = initial_data;
        let mut offset = 0u64;

        let _record_count =
            decode_varint_signed(&mut cursor, &mut offset).map_err(|e| ReaderError::Parse {
                offset: self.current_offset,
                message: format!("Failed to decode block record count: {}", e),
            })?;

        let compressed_size =
            decode_varint_signed(&mut cursor, &mut offset).map_err(|e| ReaderError::Parse {
                offset: self.current_offset + offset,
                message: format!("Failed to decode block compressed size: {}", e),
            })?;

        if compressed_size < 0 {
            return Err(ReaderError::Parse {
                offset: self.current_offset + offset,
                message: format!("Invalid negative compressed size: {}", compressed_size),
            });
        }

        // Calculate total bytes needed: header varints + compressed data + sync marker
        let header_size = offset as usize;
        let total_needed = header_size + compressed_size as usize + SYNC_MARKER_SIZE;

        if total_needed > remaining_in_file {
            return Err(ReaderError::Parse {
                offset: self.current_offset,
                message: format!(
                    "Block size {} exceeds remaining file size {}",
                    total_needed, remaining_in_file
                ),
            });
        }

        // Read exactly what we need
        let data = self
            .source
            .read_range(self.current_offset, total_needed)
            .await?;

        // Parse the complete block
        let (block, consumed) = AvroBlock::parse(
            &data,
            &self.header.sync_marker,
            self.current_offset,
            self.block_index,
        )?;

        self.current_offset += consumed as u64;
        self.block_index += 1;
        Ok(Some(block))
    }

    /// Get a reference to the parsed header.
    pub fn header(&self) -> &AvroHeader {
        &self.header
    }

    /// Get the codec used for block compression.
    pub fn codec(&self) -> Codec {
        self.header.codec
    }

    /// Get the current file offset.
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// Get the current block index.
    pub fn block_index(&self) -> usize {
        self.block_index
    }

    /// Get the total file size.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Check if we've reached the end of the file.
    pub fn is_finished(&self) -> bool {
        self.current_offset >= self.file_size
    }

    /// Seek to a position in the file and scan for the next sync marker.
    ///
    /// This method is useful for resumable reads or recovering from errors.
    /// It scans forward from the given position looking for the file's
    /// sync marker, then positions the reader to start reading blocks
    /// from that point.
    ///
    /// # Arguments
    /// * `position` - The file offset to start scanning from
    ///
    /// # Returns
    /// `true` if a sync marker was found and the reader is positioned
    /// to read the next block, `false` if no sync marker was found
    /// (reached end of file).
    ///
    /// # Errors
    /// - `ReaderError::Source` if reading from the source fails
    ///
    /// # Requirements
    /// - 3.7: Support seeking to a specific block by sync marker
    ///
    /// # Note
    /// After a successful seek, the block_index is reset to 0 since
    /// we don't know which block we're at. The caller should track
    /// block positions if needed.
    pub async fn seek_to_sync(&mut self, position: u64) -> Result<bool, ReaderError> {
        // Can't seek past end of file
        if position >= self.file_size {
            return Ok(false);
        }

        // Clear the read buffer since we're seeking to a new position
        self.read_buffer = Bytes::new();

        let sync_marker = self.header.sync_marker;
        let mut scan_offset = position;

        // Scan for sync marker
        while scan_offset + SYNC_MARKER_SIZE as u64 <= self.file_size {
            // Read a chunk of data to scan
            let remaining = self.file_size - scan_offset;
            let read_size = std::cmp::min(remaining as usize, self.buffer_config.chunk_size);
            let data = self.source.read_range(scan_offset, read_size).await?;

            if data.is_empty() {
                return Ok(false);
            }

            // Scan for sync marker in this chunk
            if let Some(marker_pos) = find_sync_marker(&data, &sync_marker) {
                // Found the sync marker!
                // Position after the sync marker to read the next block
                let new_offset = scan_offset + marker_pos as u64 + SYNC_MARKER_SIZE as u64;

                if new_offset <= self.file_size {
                    self.current_offset = new_offset;
                    self.buffer_file_offset = new_offset;
                    // Reset block index since we don't know which block we're at
                    self.block_index = 0;
                    return Ok(true);
                }
            }

            // Move forward, but overlap by sync_marker_size - 1 to catch markers
            // that span chunk boundaries
            let advance = if data.len() > SYNC_MARKER_SIZE {
                data.len() - SYNC_MARKER_SIZE + 1
            } else {
                data.len()
            };
            scan_offset += advance as u64;
        }

        Ok(false)
    }

    /// Reset the reader to the beginning of the blocks.
    ///
    /// This positions the reader right after the header, ready to
    /// read the first block again. Also clears the read buffer.
    pub fn reset(&mut self) {
        self.current_offset = self.header.header_size;
        self.block_index = 0;
        self.read_buffer = Bytes::new();
        self.buffer_file_offset = self.header.header_size;
    }

    /// Advance the reader past a known invalid sync marker.
    ///
    /// This method is used for error recovery when we encounter an `InvalidSyncMarker`
    /// error. The error contains the offset where the invalid sync marker was found.
    /// We advance past it to try reading the next block.
    ///
    /// # Arguments
    /// * `invalid_sync_offset` - The file offset where the invalid sync marker was found
    ///
    /// # Requirements
    /// - 7.1: Skip bad blocks and continue to next sync marker
    pub fn advance_past_invalid_sync(&mut self, invalid_sync_offset: u64) {
        // Position after the invalid sync marker (16 bytes)
        self.current_offset = invalid_sync_offset + SYNC_MARKER_SIZE as u64;
        self.buffer_file_offset = self.current_offset;
        self.block_index += 1;
        // Clear the read buffer since we're jumping to a new position
        self.read_buffer = Bytes::new();
    }

    /// Skip past corrupted data and find the next valid sync marker, starting from a given position.
    ///
    /// This method is specifically designed for error recovery in skip mode.
    /// It scans forward from the given position looking for the file's sync marker,
    /// then positions the reader immediately after the found sync marker.
    ///
    /// # Arguments
    /// * `start_from` - The file offset to start scanning from
    ///
    /// # Returns
    /// A tuple of (found, bytes_skipped) where:
    /// - `found` is true if a sync marker was found and the reader is positioned
    ///   to read the next block, false if no more sync markers exist (EOF)
    /// - `bytes_skipped` is the number of bytes skipped from `start_from` to find the sync marker
    ///
    /// # Errors
    /// - `ReaderError::Source` if reading from the source fails
    ///
    /// # Requirements
    /// - 7.1: Skip bad blocks and continue to next sync marker
    /// - 7.2: Skip bad records within blocks
    /// - 7.7: Log descriptive errors with recovery information
    pub async fn skip_to_next_sync_from(
        &mut self,
        start_from: u64,
    ) -> Result<(bool, u64), ReaderError> {
        // Can't skip past end of file
        if start_from >= self.file_size {
            return Ok((false, 0));
        }

        // Clear the read buffer since we're scanning for sync markers
        self.read_buffer = Bytes::new();

        let sync_marker = self.header.sync_marker;
        let mut scan_offset = start_from;

        // Scan for sync marker
        while scan_offset + SYNC_MARKER_SIZE as u64 <= self.file_size {
            // Read a chunk of data to scan
            let remaining = self.file_size - scan_offset;
            let read_size = std::cmp::min(remaining as usize, self.buffer_config.chunk_size);
            let data = self.source.read_range(scan_offset, read_size).await?;

            if data.is_empty() {
                let bytes_skipped = self.file_size - start_from;
                return Ok((false, bytes_skipped));
            }

            // Scan for sync marker in this chunk
            if let Some(marker_pos) = find_sync_marker(&data, &sync_marker) {
                // Found the sync marker!
                // Position after the sync marker to read the next block
                let marker_offset = scan_offset + marker_pos as u64;
                let new_offset = marker_offset + SYNC_MARKER_SIZE as u64;

                if new_offset <= self.file_size {
                    let bytes_skipped = marker_offset - start_from;
                    self.current_offset = new_offset;
                    self.buffer_file_offset = new_offset;
                    // Increment block index since we're moving to the next block
                    self.block_index += 1;
                    return Ok((true, bytes_skipped));
                }
            }

            // Move forward, but overlap by sync_marker_size - 1 to catch markers
            // that span chunk boundaries
            let advance = if data.len() > SYNC_MARKER_SIZE {
                data.len() - SYNC_MARKER_SIZE + 1
            } else {
                data.len()
            };
            scan_offset += advance as u64;
        }

        let bytes_skipped = self.file_size - start_from;
        Ok((false, bytes_skipped))
    }

    /// Skip past corrupted data and find the next valid sync marker.
    ///
    /// This method is specifically designed for error recovery in skip mode.
    /// Unlike `seek_to_sync`, which searches from a given position, this method:
    /// 1. Starts searching from current_offset + 1 (to skip past the current bad data)
    /// 2. Scans forward looking for the file's sync marker
    /// 3. Positions the reader immediately after the found sync marker
    ///
    /// This is necessary because when we encounter an invalid sync marker:
    /// - The block data has already been read
    /// - The "sync marker" we read doesn't match the expected one
    /// - We need to find the NEXT occurrence of the correct sync marker
    ///
    /// # Returns
    /// A tuple of (found, bytes_skipped) where:
    /// - `found` is true if a sync marker was found and the reader is positioned
    ///   to read the next block, false if no more sync markers exist (EOF)
    /// - `bytes_skipped` is the number of bytes skipped to find the sync marker
    ///
    /// # Errors
    /// - `ReaderError::Source` if reading from the source fails
    ///
    /// # Requirements
    /// - 7.1: Skip bad blocks and continue to next sync marker
    /// - 7.2: Skip bad records within blocks
    /// - 7.7: Log descriptive errors with recovery information
    pub async fn skip_to_next_sync(&mut self) -> Result<(bool, u64), ReaderError> {
        let start_offset = self.current_offset;

        // Can't skip past end of file
        if start_offset >= self.file_size {
            return Ok((false, 0));
        }

        // Clear the read buffer since we're scanning for sync markers
        self.read_buffer = Bytes::new();

        let sync_marker = self.header.sync_marker;
        // Start scanning from current_offset + 1 to skip past any partial/wrong sync marker
        let mut scan_offset = start_offset.saturating_add(1);

        // Scan for sync marker
        while scan_offset + SYNC_MARKER_SIZE as u64 <= self.file_size {
            // Read a chunk of data to scan
            let remaining = self.file_size - scan_offset;
            let read_size = std::cmp::min(remaining as usize, self.buffer_config.chunk_size);
            let data = self.source.read_range(scan_offset, read_size).await?;

            if data.is_empty() {
                let bytes_skipped = self.file_size - start_offset;
                return Ok((false, bytes_skipped));
            }

            // Scan for sync marker in this chunk
            if let Some(marker_pos) = find_sync_marker(&data, &sync_marker) {
                // Found the sync marker!
                // Position after the sync marker to read the next block
                let marker_offset = scan_offset + marker_pos as u64;
                let new_offset = marker_offset + SYNC_MARKER_SIZE as u64;

                if new_offset <= self.file_size {
                    let bytes_skipped = marker_offset - start_offset;
                    self.current_offset = new_offset;
                    self.buffer_file_offset = new_offset;
                    // Increment block index since we're moving to the next block
                    // (we don't reset to 0 because we want to track position for error reporting)
                    self.block_index += 1;
                    return Ok((true, bytes_skipped));
                }
            }

            // Move forward, but overlap by sync_marker_size - 1 to catch markers
            // that span chunk boundaries
            let advance = if data.len() > SYNC_MARKER_SIZE {
                data.len() - SYNC_MARKER_SIZE + 1
            } else {
                data.len()
            };
            scan_offset += advance as u64;
        }

        let bytes_skipped = self.file_size - start_offset;
        Ok((false, bytes_skipped))
    }
}

/// Find the position of a sync marker in a byte slice.
///
/// Returns the offset of the first byte of the sync marker if found,
/// or `None` if not found.
fn find_sync_marker(data: &[u8], sync_marker: &[u8; 16]) -> Option<usize> {
    if data.len() < SYNC_MARKER_SIZE {
        return None;
    }

    // Simple linear scan - could be optimized with Boyer-Moore or similar
    (0..=(data.len() - SYNC_MARKER_SIZE)).find(|&i| &data[i..i + SYNC_MARKER_SIZE] == sync_marker)
}

#[cfg(test)]
mod tests {
    use super::super::varint::encode_zigzag;
    use super::*;

    /// Create a test block with given parameters
    fn create_test_block(record_count: i64, data: &[u8], sync_marker: &[u8; 16]) -> Vec<u8> {
        let mut block = Vec::new();
        block.extend_from_slice(&encode_zigzag(record_count));
        block.extend_from_slice(&encode_zigzag(data.len() as i64));
        block.extend_from_slice(data);
        block.extend_from_slice(sync_marker);
        block
    }

    #[test]
    fn test_parse_block_simple() {
        let sync = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let data = b"hello world";
        let block_bytes = create_test_block(5, data, &sync);

        let (block, consumed) = AvroBlock::parse(&block_bytes, &sync, 100, 0).unwrap();

        assert_eq!(block.record_count, 5);
        assert_eq!(block.compressed_size, data.len() as i64);
        assert_eq!(&block.data[..], data);
        assert_eq!(block.sync_marker, sync);
        assert_eq!(block.file_offset, 100);
        assert_eq!(block.block_index, 0);
        assert_eq!(consumed, block_bytes.len());
    }

    #[test]
    fn test_parse_block_empty() {
        let sync = [0u8; 16];
        let block_bytes = create_test_block(0, &[], &sync);

        let (block, _) = AvroBlock::parse(&block_bytes, &sync, 0, 0).unwrap();

        assert_eq!(block.record_count, 0);
        assert!(block.is_empty());
        assert_eq!(block.data.len(), 0);
    }

    #[test]
    fn test_parse_block_large_record_count() {
        let sync = [0xFFu8; 16];
        let data = vec![0u8; 1000];
        let block_bytes = create_test_block(100_000, &data, &sync);

        let (block, _) = AvroBlock::parse(&block_bytes, &sync, 0, 5).unwrap();

        assert_eq!(block.record_count, 100_000);
        assert_eq!(block.block_index, 5);
    }

    #[test]
    fn test_parse_block_invalid_sync_marker() {
        let expected_sync = [1u8; 16];
        let actual_sync = [2u8; 16];
        let block_bytes = create_test_block(1, b"data", &actual_sync);

        let result = AvroBlock::parse(&block_bytes, &expected_sync, 0, 3);

        assert!(matches!(
            result,
            Err(ReaderError::InvalidSyncMarker { block_index: 3, .. })
        ));
    }

    #[test]
    fn test_parse_block_truncated_data() {
        let sync = [0u8; 16];
        let mut block_bytes = Vec::new();
        block_bytes.extend_from_slice(&encode_zigzag(1)); // record count
        block_bytes.extend_from_slice(&encode_zigzag(100)); // claims 100 bytes
        block_bytes.extend_from_slice(b"short"); // only 5 bytes

        let result = AvroBlock::parse(&block_bytes, &sync, 0, 0);

        assert!(matches!(result, Err(ReaderError::Parse { .. })));
    }

    #[test]
    fn test_parse_block_negative_record_count() {
        let sync = [0u8; 16];
        let mut block_bytes = Vec::new();
        block_bytes.extend_from_slice(&encode_zigzag(-5)); // negative count
        block_bytes.extend_from_slice(&encode_zigzag(0));
        block_bytes.extend_from_slice(&sync);

        let result = AvroBlock::parse(&block_bytes, &sync, 0, 0);

        assert!(matches!(result, Err(ReaderError::Parse { .. })));
    }

    #[test]
    fn test_parse_block_negative_size() {
        let sync = [0u8; 16];
        let mut block_bytes = Vec::new();
        block_bytes.extend_from_slice(&encode_zigzag(1));
        block_bytes.extend_from_slice(&encode_zigzag(-10)); // negative size
        block_bytes.extend_from_slice(&sync);

        let result = AvroBlock::parse(&block_bytes, &sync, 0, 0);

        assert!(matches!(result, Err(ReaderError::Parse { .. })));
    }

    #[test]
    fn test_parse_multiple_blocks() {
        let sync = [0xABu8; 16];
        let data1 = b"first block";
        let data2 = b"second block data";

        let mut bytes = create_test_block(10, data1, &sync);
        let block2_offset = bytes.len();
        bytes.extend_from_slice(&create_test_block(20, data2, &sync));

        // Parse first block
        let (block1, consumed1) = AvroBlock::parse(&bytes, &sync, 0, 0).unwrap();
        assert_eq!(block1.record_count, 10);
        assert_eq!(&block1.data[..], data1);

        // Parse second block
        let (block2, _) =
            AvroBlock::parse(&bytes[consumed1..], &sync, consumed1 as u64, 1).unwrap();
        assert_eq!(block2.record_count, 20);
        assert_eq!(&block2.data[..], data2);
        assert_eq!(block2.file_offset, block2_offset as u64);
    }

    #[test]
    fn test_decompressed_block_new() {
        let data = Bytes::from_static(b"test data");
        let block = DecompressedBlock::new(42, data.clone(), 7);

        assert_eq!(block.record_count, 42);
        assert_eq!(block.data, data);
        assert_eq!(block.block_index, 7);
        assert!(!block.is_empty());
    }

    #[test]
    fn test_decompressed_block_empty() {
        let block = DecompressedBlock::new(0, Bytes::new(), 0);
        assert!(block.is_empty());
    }

    #[test]
    fn test_find_sync_marker_at_start() {
        let sync = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ];
        let mut data = sync.to_vec();
        data.extend_from_slice(b"extra data");

        let result = find_sync_marker(&data, &sync);
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_find_sync_marker_in_middle() {
        let sync = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ];
        let mut data = b"prefix data ".to_vec();
        let expected_pos = data.len();
        data.extend_from_slice(&sync);
        data.extend_from_slice(b" suffix");

        let result = find_sync_marker(&data, &sync);
        assert_eq!(result, Some(expected_pos));
    }

    #[test]
    fn test_find_sync_marker_at_end() {
        let sync = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ];
        let mut data = b"prefix data".to_vec();
        let expected_pos = data.len();
        data.extend_from_slice(&sync);

        let result = find_sync_marker(&data, &sync);
        assert_eq!(result, Some(expected_pos));
    }

    #[test]
    fn test_find_sync_marker_not_found() {
        let sync = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ];
        let data = b"no sync marker here at all";

        let result = find_sync_marker(data, &sync);
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_sync_marker_partial_match() {
        let sync = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ];
        // Only first 8 bytes match
        let data = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ];

        let result = find_sync_marker(&data, &sync);
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_sync_marker_too_short() {
        let sync = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ];
        let data = [0xDE, 0xAD, 0xBE, 0xEF]; // Only 4 bytes

        let result = find_sync_marker(&data, &sync);
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_sync_marker_empty() {
        let sync = [0u8; 16];
        let data: &[u8] = &[];

        let result = find_sync_marker(data, &sync);
        assert_eq!(result, None);
    }

    // BlockReader tests using a mock source
    mod block_reader_tests {
        use super::*;
        use crate::error::SourceError;
        use crate::reader::header::AVRO_MAGIC;

        /// A simple in-memory source for testing
        struct MockSource {
            data: Vec<u8>,
        }

        impl MockSource {
            fn new(data: Vec<u8>) -> Self {
                Self { data }
            }
        }

        #[async_trait::async_trait]
        impl StreamSource for MockSource {
            async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, SourceError> {
                let start = offset as usize;
                if start >= self.data.len() {
                    return Ok(Bytes::new());
                }
                let end = std::cmp::min(start + length, self.data.len());
                Ok(Bytes::copy_from_slice(&self.data[start..end]))
            }

            async fn size(&self) -> Result<u64, SourceError> {
                Ok(self.data.len() as u64)
            }

            async fn read_from(&self, offset: u64) -> Result<Bytes, SourceError> {
                let start = offset as usize;
                if start >= self.data.len() {
                    return Ok(Bytes::new());
                }
                Ok(Bytes::copy_from_slice(&self.data[start..]))
            }
        }

        /// Create a minimal valid Avro file with header and blocks
        fn create_test_avro_file(
            schema_json: &str,
            codec: Option<&str>,
            blocks: &[(i64, &[u8])], // (record_count, data)
        ) -> (Vec<u8>, [u8; 16]) {
            let mut file = Vec::new();

            // Magic bytes
            file.extend_from_slice(&AVRO_MAGIC);

            // Count the number of metadata entries
            let entry_count: i64 = if codec.is_some() { 2 } else { 1 };

            // Metadata map: block count (zigzag encoded)
            file.extend_from_slice(&encode_zigzag(entry_count));

            // Schema entry
            let schema_key = b"avro.schema";
            file.extend_from_slice(&encode_zigzag(schema_key.len() as i64));
            file.extend_from_slice(schema_key);
            file.extend_from_slice(&encode_zigzag(schema_json.len() as i64));
            file.extend_from_slice(schema_json.as_bytes());

            // Codec entry (if provided)
            if let Some(codec_name) = codec {
                let codec_key = b"avro.codec";
                file.extend_from_slice(&encode_zigzag(codec_key.len() as i64));
                file.extend_from_slice(codec_key);
                file.extend_from_slice(&encode_zigzag(codec_name.len() as i64));
                file.extend_from_slice(codec_name.as_bytes());
            }

            // End of map
            file.push(0x00);

            // Sync marker (16 bytes)
            let sync_marker: [u8; 16] = [
                0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
                0xDE, 0xF0,
            ];
            file.extend_from_slice(&sync_marker);

            // Add blocks
            for (record_count, data) in blocks {
                file.extend_from_slice(&create_test_block(*record_count, data, &sync_marker));
            }

            (file, sync_marker)
        }

        /// Helper to run async tests
        fn run_async<F: std::future::Future>(f: F) -> F::Output {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f)
        }

        #[test]
        fn test_block_reader_new() {
            run_async(async {
                let (file_data, sync_marker) = create_test_avro_file(r#""string""#, None, &[]);
                let source = MockSource::new(file_data.clone());

                let reader = BlockReader::new(source).await.unwrap();

                assert_eq!(reader.header().sync_marker, sync_marker);
                assert_eq!(reader.header().codec, Codec::Null);
                assert_eq!(reader.block_index(), 0);
                assert_eq!(reader.file_size(), file_data.len() as u64);
            });
        }

        #[test]
        fn test_block_reader_read_single_block() {
            run_async(async {
                let block_data = b"test record data";
                let (file_data, _) = create_test_avro_file(r#""string""#, None, &[(5, block_data)]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 5);
                assert_eq!(&block.data[..], block_data);
                assert_eq!(block.block_index, 0);

                // Should return None at EOF
                let next = reader.next_block().await.unwrap();
                assert!(next.is_none());
                assert!(reader.is_finished());
            });
        }

        #[test]
        fn test_block_reader_read_multiple_blocks() {
            run_async(async {
                let block1_data = b"first block";
                let block2_data = b"second block";
                let block3_data = b"third block";
                let (file_data, _) = create_test_avro_file(
                    r#""string""#,
                    None,
                    &[(10, block1_data), (20, block2_data), (30, block3_data)],
                );
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                // Read first block
                let block1 = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block1.record_count, 10);
                assert_eq!(&block1.data[..], block1_data);
                assert_eq!(block1.block_index, 0);

                // Read second block
                let block2 = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block2.record_count, 20);
                assert_eq!(&block2.data[..], block2_data);
                assert_eq!(block2.block_index, 1);

                // Read third block
                let block3 = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block3.record_count, 30);
                assert_eq!(&block3.data[..], block3_data);
                assert_eq!(block3.block_index, 2);

                // Should return None at EOF
                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_reset() {
            run_async(async {
                let block_data = b"test data";
                let (file_data, _) = create_test_avro_file(r#""string""#, None, &[(5, block_data)]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                // Read the block
                let block1 = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block1.record_count, 5);

                // Verify EOF
                assert!(reader.next_block().await.unwrap().is_none());

                // Reset and read again
                reader.reset();
                assert_eq!(reader.block_index(), 0);
                assert!(!reader.is_finished());

                let block2 = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block2.record_count, 5);
                assert_eq!(&block2.data[..], block_data);
            });
        }

        #[test]
        fn test_block_reader_seek_to_sync() {
            run_async(async {
                let block1_data = b"first";
                let block2_data = b"second";
                let (file_data, _sync_marker) = create_test_avro_file(
                    r#""string""#,
                    None,
                    &[(10, block1_data), (20, block2_data)],
                );
                let source = MockSource::new(file_data.clone());

                let mut reader = BlockReader::new(source).await.unwrap();
                let header_size = reader.header().header_size;

                // Seek from the start of blocks - should find the first sync marker
                let found = reader.seek_to_sync(header_size).await.unwrap();
                assert!(found);

                // After seeking past a sync marker, we should be able to read the next block
                let block = reader.next_block().await.unwrap().unwrap();
                // This should be the second block since we seeked past the first sync marker
                assert_eq!(block.record_count, 20);
            });
        }

        #[test]
        fn test_block_reader_seek_to_sync_not_found() {
            run_async(async {
                let (file_data, _) = create_test_avro_file(r#""string""#, None, &[]);
                let source = MockSource::new(file_data.clone());

                let mut reader = BlockReader::new(source).await.unwrap();

                // Seek past end of file
                let found = reader
                    .seek_to_sync(file_data.len() as u64 + 100)
                    .await
                    .unwrap();
                assert!(!found);
            });
        }

        #[test]
        fn test_block_reader_empty_file() {
            run_async(async {
                let (file_data, _) = create_test_avro_file(r#""null""#, None, &[]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                // Should return None immediately (no blocks)
                let block = reader.next_block().await.unwrap();
                assert!(block.is_none());
                assert!(reader.is_finished());
            });
        }

        #[test]
        fn test_block_reader_with_codec() {
            run_async(async {
                let block_data = b"compressed data";
                let (file_data, _) =
                    create_test_avro_file(r#""string""#, Some("deflate"), &[(5, block_data)]);
                let source = MockSource::new(file_data);

                let reader = BlockReader::new(source).await.unwrap();

                assert_eq!(reader.codec(), Codec::Deflate);
            });
        }

        #[test]
        fn test_block_reader_accessors() {
            run_async(async {
                let (file_data, _) = create_test_avro_file(r#""int""#, None, &[(1, b"x")]);
                let file_size = file_data.len() as u64;
                let source = MockSource::new(file_data);

                let reader = BlockReader::new(source).await.unwrap();

                assert_eq!(reader.file_size(), file_size);
                assert_eq!(reader.block_index(), 0);
                assert!(!reader.is_finished());
                assert!(reader.current_offset() > 0); // After header
            });
        }

        // ====================================================================
        // Block Size Edge Case Tests
        // ====================================================================
        // These tests verify correct handling of blocks at various sizes
        // relative to the 64KB default read buffer.

        #[test]
        fn test_block_reader_very_small_blocks() {
            // Test multiple very small blocks (< 100 bytes each)
            // All should fit within a single 64KB read
            run_async(async {
                let blocks: Vec<(i64, &[u8])> = vec![
                    (1, b"a"),
                    (2, b"bb"),
                    (3, b"ccc"),
                    (4, b"dddd"),
                    (5, b"eeeee"),
                ];
                let (file_data, _) = create_test_avro_file(r#""string""#, None, &blocks);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                for (i, (expected_count, expected_data)) in blocks.iter().enumerate() {
                    let block = reader.next_block().await.unwrap().unwrap();
                    assert_eq!(
                        block.record_count, *expected_count,
                        "Block {} record count",
                        i
                    );
                    assert_eq!(&block.data[..], *expected_data, "Block {} data", i);
                    assert_eq!(block.block_index, i, "Block {} index", i);
                }

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_block_exactly_64kb() {
            // Test a block that is exactly 64KB (the buffer size boundary)
            run_async(async {
                let block_data = vec![0xABu8; 64 * 1024]; // Exactly 64KB
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &[(1, &block_data)]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(block.data.len(), 64 * 1024);
                assert_eq!(block.block_index, 0);

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_block_just_over_64kb() {
            // Test a block that is just over 64KB (triggers slow path)
            run_async(async {
                let block_data = vec![0xCDu8; 64 * 1024 + 1]; // 64KB + 1 byte
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &[(1, &block_data)]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(block.data.len(), 64 * 1024 + 1);
                assert_eq!(block.block_index, 0);

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_large_block_100kb() {
            // Test a 100KB block (well over the 64KB buffer)
            run_async(async {
                let block_data = vec![0xEFu8; 100 * 1024]; // 100KB
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &[(1, &block_data)]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(block.data.len(), 100 * 1024);

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_large_block_1mb() {
            // Test a 1MB block (much larger than buffer)
            run_async(async {
                let block_data = vec![0x12u8; 1024 * 1024]; // 1MB
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &[(1, &block_data)]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(block.data.len(), 1024 * 1024);

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_mixed_small_and_large_blocks() {
            // Test a mix of small blocks, then a large block, then small again
            run_async(async {
                let small1 = b"small block 1";
                let small2 = b"small block 2";
                let large = vec![0x99u8; 100 * 1024]; // 100KB
                let small3 = b"small block 3";

                let blocks: Vec<(i64, &[u8])> =
                    vec![(1, small1), (2, small2), (3, &large), (4, small3)];
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &blocks);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                // First small block
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(&block.data[..], small1);
                assert_eq!(block.block_index, 0);

                // Second small block
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 2);
                assert_eq!(&block.data[..], small2);
                assert_eq!(block.block_index, 1);

                // Large block (triggers slow path)
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 3);
                assert_eq!(block.data.len(), 100 * 1024);
                assert_eq!(block.block_index, 2);

                // Third small block (back to fast path)
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 4);
                assert_eq!(&block.data[..], small3);
                assert_eq!(block.block_index, 3);

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_multiple_large_blocks() {
            // Test multiple consecutive large blocks
            run_async(async {
                let large1 = vec![0xAAu8; 80 * 1024]; // 80KB
                let large2 = vec![0xBBu8; 90 * 1024]; // 90KB
                let large3 = vec![0xCCu8; 70 * 1024]; // 70KB

                let blocks: Vec<(i64, &[u8])> = vec![(10, &large1), (20, &large2), (30, &large3)];
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &blocks);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 10);
                assert_eq!(block.data.len(), 80 * 1024);
                assert!(block.data.iter().all(|&b| b == 0xAA));

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 20);
                assert_eq!(block.data.len(), 90 * 1024);
                assert!(block.data.iter().all(|&b| b == 0xBB));

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 30);
                assert_eq!(block.data.len(), 70 * 1024);
                assert!(block.data.iter().all(|&b| b == 0xCC));

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_block_just_under_64kb() {
            // Test a block that is just under 64KB (should use fast path)
            run_async(async {
                // Account for header varints (~4 bytes) and sync marker (16 bytes)
                // So data should be 64KB - 20 = ~65516 bytes to stay under buffer
                let block_data = vec![0xDDu8; 64 * 1024 - 100]; // Safe margin under 64KB
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &[(1, &block_data)]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(block.data.len(), 64 * 1024 - 100);

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_many_tiny_blocks() {
            // Test many tiny blocks (stress test offset tracking)
            run_async(async {
                let blocks: Vec<(i64, Vec<u8>)> = (0..100)
                    .map(|i| (i as i64, vec![i as u8; (i % 10 + 1) as usize]))
                    .collect();
                let block_refs: Vec<(i64, &[u8])> =
                    blocks.iter().map(|(c, d)| (*c, d.as_slice())).collect();

                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &block_refs);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                for (i, (expected_count, expected_data)) in blocks.iter().enumerate() {
                    let block = reader.next_block().await.unwrap().unwrap();
                    assert_eq!(
                        block.record_count, *expected_count,
                        "Block {} record count",
                        i
                    );
                    assert_eq!(
                        &block.data[..],
                        expected_data.as_slice(),
                        "Block {} data",
                        i
                    );
                    assert_eq!(block.block_index, i, "Block {} index", i);
                }

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_alternating_small_large() {
            // Test alternating between small and large blocks
            run_async(async {
                let small = b"tiny";
                let large = vec![0xFFu8; 100 * 1024];

                let blocks: Vec<(i64, &[u8])> =
                    vec![(1, small), (2, &large), (3, small), (4, &large), (5, small)];
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &blocks);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                for i in 0..5 {
                    let block = reader.next_block().await.unwrap().unwrap();
                    assert_eq!(block.record_count, (i + 1) as i64);
                    assert_eq!(block.block_index, i);

                    if i % 2 == 0 {
                        assert_eq!(&block.data[..], small, "Block {} should be small", i);
                    } else {
                        assert_eq!(block.data.len(), 100 * 1024, "Block {} should be large", i);
                    }
                }

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_empty_block_between_large() {
            // Test empty blocks interspersed with large blocks
            run_async(async {
                let large = vec![0x77u8; 80 * 1024];
                let empty: &[u8] = &[];

                let blocks: Vec<(i64, &[u8])> = vec![
                    (0, empty),
                    (10, &large),
                    (0, empty),
                    (20, &large),
                    (0, empty),
                ];
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &blocks);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                // Empty block
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 0);
                assert!(block.is_empty());

                // Large block
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 10);
                assert_eq!(block.data.len(), 80 * 1024);

                // Empty block
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 0);
                assert!(block.is_empty());

                // Large block
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 20);
                assert_eq!(block.data.len(), 80 * 1024);

                // Empty block
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 0);
                assert!(block.is_empty());

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_reset_after_large_block() {
            // Test that reset works correctly after reading a large block
            run_async(async {
                let large = vec![0x88u8; 100 * 1024];
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &[(5, &large)]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                // Read the large block
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 5);
                assert_eq!(block.data.len(), 100 * 1024);

                // Verify EOF
                assert!(reader.next_block().await.unwrap().is_none());

                // Reset and read again
                reader.reset();
                assert_eq!(reader.block_index(), 0);

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 5);
                assert_eq!(block.data.len(), 100 * 1024);
                assert_eq!(block.block_index, 0);
            });
        }

        #[test]
        fn test_block_reader_large_block_512kb() {
            // Test a 512KB block (8x the 64KB buffer)
            run_async(async {
                let block_data = vec![0x34u8; 512 * 1024]; // 512KB
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &[(1, &block_data)]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(block.data.len(), 512 * 1024);
                // Verify data integrity
                assert!(block.data.iter().all(|&b| b == 0x34));

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_large_block_5mb() {
            // Test a 5MB block (extreme case, ~80x the buffer)
            run_async(async {
                let block_data = vec![0x56u8; 5 * 1024 * 1024]; // 5MB
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &[(1, &block_data)]);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(block.data.len(), 5 * 1024 * 1024);
                // Verify data integrity
                assert!(block.data.iter().all(|&b| b == 0x56));

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_multiple_512kb_blocks() {
            // Test multiple 512KB blocks in sequence
            run_async(async {
                let block1 = vec![0xAAu8; 512 * 1024];
                let block2 = vec![0xBBu8; 512 * 1024];
                let block3 = vec![0xCCu8; 512 * 1024];

                let blocks: Vec<(i64, &[u8])> = vec![(10, &block1), (20, &block2), (30, &block3)];
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &blocks);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 10);
                assert_eq!(block.data.len(), 512 * 1024);
                assert!(block.data.iter().all(|&b| b == 0xAA));

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 20);
                assert_eq!(block.data.len(), 512 * 1024);
                assert!(block.data.iter().all(|&b| b == 0xBB));

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 30);
                assert_eq!(block.data.len(), 512 * 1024);
                assert!(block.data.iter().all(|&b| b == 0xCC));

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_tiny_then_huge_then_tiny() {
            // Test extreme size variation: tiny -> 5MB -> tiny
            run_async(async {
                let tiny1 = b"x";
                let huge = vec![0x78u8; 5 * 1024 * 1024]; // 5MB
                let tiny2 = b"y";

                let blocks: Vec<(i64, &[u8])> = vec![(1, tiny1), (2, &huge), (3, tiny2)];
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &blocks);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                // Tiny block (fast path)
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(&block.data[..], b"x");

                // Huge block (slow path)
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 2);
                assert_eq!(block.data.len(), 5 * 1024 * 1024);

                // Tiny block again (back to fast path)
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 3);
                assert_eq!(&block.data[..], b"y");

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_reader_progressively_larger_blocks() {
            // Test blocks that progressively get larger
            run_async(async {
                let sizes = [
                    1 * 1024,    // 1KB
                    10 * 1024,   // 10KB
                    50 * 1024,   // 50KB
                    64 * 1024,   // 64KB (boundary)
                    100 * 1024,  // 100KB
                    256 * 1024,  // 256KB
                    512 * 1024,  // 512KB
                    1024 * 1024, // 1MB
                ];

                let block_data: Vec<Vec<u8>> = sizes
                    .iter()
                    .enumerate()
                    .map(|(i, &size)| vec![(i + 1) as u8; size])
                    .collect();

                let blocks: Vec<(i64, &[u8])> = block_data
                    .iter()
                    .enumerate()
                    .map(|(i, data)| ((i + 1) as i64, data.as_slice()))
                    .collect();

                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &blocks);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                for (i, &expected_size) in sizes.iter().enumerate() {
                    let block = reader.next_block().await.unwrap().unwrap();
                    assert_eq!(
                        block.record_count,
                        (i + 1) as i64,
                        "Block {} record count",
                        i
                    );
                    assert_eq!(block.data.len(), expected_size, "Block {} size", i);
                    assert_eq!(block.block_index, i, "Block {} index", i);
                    // Verify data integrity
                    assert!(
                        block.data.iter().all(|&b| b == (i + 1) as u8),
                        "Block {} data integrity",
                        i
                    );
                }

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        // ====================================================================
        // ReadBufferConfig Tests
        // ====================================================================
        // These tests verify the ReadBufferConfig struct and its integration
        // with BlockReader for I/O optimization.

        #[test]
        fn test_read_buffer_config_local_default() {
            let config = ReadBufferConfig::LOCAL_DEFAULT;
            assert_eq!(config.chunk_size, 64 * 1024); // 64KB
            assert_eq!(config.prefetch_threshold, 0.0);
            assert_eq!(config.prefetch_trigger_bytes(), 0);
        }

        #[test]
        fn test_read_buffer_config_s3_default() {
            let config = ReadBufferConfig::S3_DEFAULT;
            assert_eq!(config.chunk_size, 4 * 1024 * 1024); // 4MB
            assert_eq!(config.prefetch_threshold, 0.5);
            assert_eq!(config.prefetch_trigger_bytes(), 2 * 1024 * 1024); // 2MB
        }

        #[test]
        fn test_read_buffer_config_with_chunk_size() {
            let config = ReadBufferConfig::with_chunk_size(128 * 1024);
            assert_eq!(config.chunk_size, 128 * 1024);
            assert_eq!(config.prefetch_threshold, 0.0);
        }

        #[test]
        fn test_read_buffer_config_new() {
            let config = ReadBufferConfig::new(256 * 1024, 0.25);
            assert_eq!(config.chunk_size, 256 * 1024);
            assert_eq!(config.prefetch_threshold, 0.25);
            assert_eq!(config.prefetch_trigger_bytes(), 64 * 1024); // 25% of 256KB
        }

        #[test]
        fn test_block_reader_with_custom_config() {
            run_async(async {
                let block_data = b"test data";
                let (file_data, _) = create_test_avro_file(r#""string""#, None, &[(5, block_data)]);
                let source = MockSource::new(file_data);

                // Use a custom config with 128KB chunk size
                let config = ReadBufferConfig::with_chunk_size(128 * 1024);
                let mut reader = BlockReader::with_config(source, config).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 5);
                assert_eq!(&block.data[..], block_data);
            });
        }

        #[test]
        fn test_block_reader_with_s3_config() {
            run_async(async {
                let block_data = b"s3 test data";
                let (file_data, _) = create_test_avro_file(r#""string""#, None, &[(3, block_data)]);
                let source = MockSource::new(file_data);

                // Use S3 default config (4MB chunks)
                let mut reader = BlockReader::with_config(source, ReadBufferConfig::S3_DEFAULT)
                    .await
                    .unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 3);
                assert_eq!(&block.data[..], block_data);
            });
        }

        #[test]
        fn test_block_reader_reset_clears_buffer() {
            // Verify that reset() clears the internal buffer
            run_async(async {
                let block1_data = b"first block data";
                let block2_data = b"second block data";
                let (file_data, _) = create_test_avro_file(
                    r#""string""#,
                    None,
                    &[(5, block1_data), (10, block2_data)],
                );
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                // Read first block
                let block1 = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block1.record_count, 5);

                // Reset should clear buffer and allow re-reading from start
                reader.reset();
                assert_eq!(reader.block_index(), 0);
                assert!(!reader.is_finished());

                // Should read first block again (not second)
                let block1_again = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block1_again.record_count, 5);
                assert_eq!(&block1_again.data[..], block1_data);
            });
        }

        #[test]
        fn test_block_reader_seek_clears_buffer() {
            // Verify that seek_to_sync() clears the internal buffer
            run_async(async {
                let block1_data = b"first";
                let block2_data = b"second";
                let block3_data = b"third";
                let (file_data, _) = create_test_avro_file(
                    r#""string""#,
                    None,
                    &[(1, block1_data), (2, block2_data), (3, block3_data)],
                );
                let source = MockSource::new(file_data.clone());

                let mut reader = BlockReader::new(source).await.unwrap();
                let header_size = reader.header().header_size;

                // Read first block to populate buffer
                let block1 = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block1.record_count, 1);

                // Seek should clear buffer
                let found = reader.seek_to_sync(header_size).await.unwrap();
                assert!(found);

                // After seek, we should read from the new position
                let block = reader.next_block().await.unwrap().unwrap();
                // Should be block 2 since we seeked past block 1's sync marker
                assert_eq!(block.record_count, 2);
            });
        }

        #[test]
        fn test_multiple_small_blocks_single_read() {
            // Test that multiple small blocks can be parsed from a single buffer fill
            // This verifies the buffering optimization works correctly
            run_async(async {
                // Create 20 small blocks, each ~50 bytes
                // Total ~1KB, well within a single 64KB read
                let blocks: Vec<(i64, Vec<u8>)> = (0..20)
                    .map(|i| (i as i64 + 1, format!("block_{:02}_data", i).into_bytes()))
                    .collect();
                let block_refs: Vec<(i64, &[u8])> =
                    blocks.iter().map(|(c, d)| (*c, d.as_slice())).collect();

                let (file_data, _) = create_test_avro_file(r#""string""#, None, &block_refs);
                let source = MockSource::new(file_data);

                let mut reader = BlockReader::new(source).await.unwrap();

                // All 20 blocks should be readable
                for (i, (expected_count, expected_data)) in blocks.iter().enumerate() {
                    let block = reader.next_block().await.unwrap().unwrap();
                    assert_eq!(block.record_count, *expected_count, "Block {} count", i);
                    assert_eq!(
                        &block.data[..],
                        expected_data.as_slice(),
                        "Block {} data",
                        i
                    );
                }

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }

        #[test]
        fn test_block_spanning_buffer_boundary() {
            // Test a block that starts in one buffer fill and ends in another
            // Use a small chunk size to force this scenario
            run_async(async {
                // Create a block that's larger than our small chunk size
                let block_data = vec![0xABu8; 2000]; // 2KB block
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &[(1, &block_data)]);
                let source = MockSource::new(file_data);

                // Use a 1KB chunk size - block will span multiple reads
                let config = ReadBufferConfig::with_chunk_size(1024);
                let mut reader = BlockReader::with_config(source, config).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(block.data.len(), 2000);
                assert!(block.data.iter().all(|&b| b == 0xAB));
            });
        }

        #[test]
        fn test_large_block_exceeds_chunk_size() {
            // Test a block much larger than the chunk size
            run_async(async {
                let block_data = vec![0xCDu8; 100 * 1024]; // 100KB
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &[(1, &block_data)]);
                let source = MockSource::new(file_data);

                // Use 32KB chunk size - block is 3x larger
                let config = ReadBufferConfig::with_chunk_size(32 * 1024);
                let mut reader = BlockReader::with_config(source, config).await.unwrap();

                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(block.data.len(), 100 * 1024);
                assert!(block.data.iter().all(|&b| b == 0xCD));
            });
        }

        #[test]
        fn test_mixed_blocks_with_small_chunk_size() {
            // Test mixed small and large blocks with a small chunk size
            run_async(async {
                let small1 = b"tiny";
                let large = vec![0xEFu8; 50 * 1024]; // 50KB
                let small2 = b"also tiny";

                let blocks: Vec<(i64, &[u8])> = vec![(1, small1), (2, &large), (3, small2)];
                let (file_data, _) = create_test_avro_file(r#""bytes""#, None, &blocks);
                let source = MockSource::new(file_data);

                // Use 16KB chunk size
                let config = ReadBufferConfig::with_chunk_size(16 * 1024);
                let mut reader = BlockReader::with_config(source, config).await.unwrap();

                // Small block (fits in buffer)
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 1);
                assert_eq!(&block.data[..], small1);

                // Large block (exceeds buffer, uses slow path)
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 2);
                assert_eq!(block.data.len(), 50 * 1024);

                // Small block again (back to fast path)
                let block = reader.next_block().await.unwrap().unwrap();
                assert_eq!(block.record_count, 3);
                assert_eq!(&block.data[..], small2);

                assert!(reader.next_block().await.unwrap().is_none());
            });
        }
    }
}
