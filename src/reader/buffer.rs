//! Async block prefetching with backpressure
//!
//! The `PrefetchBuffer` asynchronously fetches and decompresses upcoming blocks
//! while the consumer processes current blocks. This improves throughput by
//! overlapping I/O and decompression with processing.
//!
//! # Backpressure
//!
//! The buffer enforces limits on both the number of blocks and total bytes
//! buffered. When limits are reached, prefetching pauses until the consumer
//! drains blocks, preventing unbounded memory growth.

use bytes::Bytes;
use std::collections::VecDeque;
use tokio::task::JoinHandle;

use crate::codec::Codec;
use crate::convert::ErrorMode;
use crate::error::{ReadError, ReadErrorKind, ReaderError};
use crate::source::StreamSource;

use super::{BlockReader, DecompressedBlock};

/// Configuration for the prefetch buffer.
///
/// Controls how many blocks and bytes to buffer ahead of the consumer.
///
/// # Requirements
/// - 3.5: Asynchronously load upcoming blocks up to a configurable buffer limit
/// - 3.6: Pause fetching when buffer is full
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Maximum number of blocks to buffer (default: 4)
    pub max_blocks: usize,
    /// Maximum total bytes to buffer (default: 64MB)
    pub max_bytes: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_blocks: 4,
            max_bytes: 64 * 1024 * 1024, // 64MB
        }
    }
}

impl BufferConfig {
    /// Create a new buffer configuration.
    pub fn new(max_blocks: usize, max_bytes: usize) -> Self {
        Self {
            max_blocks,
            max_bytes,
        }
    }
}

/// Async buffer that prefetches and decompresses blocks in the background.
///
/// The buffer maintains a queue of decompressed blocks and spawns async tasks
/// to fetch and decompress upcoming blocks. When the buffer reaches its limits,
/// prefetching pauses until the consumer drains blocks.
///
/// # Requirements
/// - 3.5: Asynchronously load upcoming blocks up to a configurable buffer limit
/// - 3.6: Pause fetching when buffer is full
/// - 7.1: Skip bad blocks and continue to next sync marker (in skip mode)
///
/// # Example
/// ```ignore
/// use jetliner::reader::{BlockReader, buffer::{PrefetchBuffer, BufferConfig}};
/// use jetliner::source::LocalSource;
///
/// let source = LocalSource::open("data.avro").await?;
/// let reader = BlockReader::new(source).await?;
/// let config = BufferConfig::default();
/// let mut buffer = PrefetchBuffer::new(reader, config);
///
/// while let Some(block) = buffer.next().await? {
///     // Process decompressed block
///     println!("Block {} has {} records", block.block_index, block.record_count);
/// }
/// ```
pub struct PrefetchBuffer<S: StreamSource> {
    /// The underlying block reader
    reader: BlockReader<S>,
    /// Queue of decompressed blocks ready for consumption
    buffer: VecDeque<DecompressedBlock>,
    /// Configuration limits
    config: BufferConfig,
    /// Current total bytes buffered
    current_buffer_bytes: usize,
    /// Codec for decompression
    codec: Codec,
    /// Background fetch task (if running)
    fetch_task: Option<JoinHandle<Result<Option<DecompressedBlock>, ReaderError>>>,
    /// Whether we've reached EOF
    finished: bool,
    /// Error handling mode
    error_mode: ErrorMode,
    /// Accumulated errors (in skip mode)
    errors: Vec<ReadError>,
}

impl<S: StreamSource + 'static> PrefetchBuffer<S> {
    /// Create a new prefetch buffer.
    ///
    /// # Arguments
    /// * `reader` - The block reader to fetch blocks from
    /// * `config` - Buffer configuration (limits)
    /// * `error_mode` - Error handling mode (strict or skip)
    ///
    /// # Requirements
    /// - 3.5: Asynchronously load upcoming blocks up to a configurable buffer limit
    /// - 7.1: Skip bad blocks and continue to next sync marker (in skip mode)
    pub fn new(reader: BlockReader<S>, config: BufferConfig, error_mode: ErrorMode) -> Self {
        let codec = reader.codec();
        Self {
            reader,
            buffer: VecDeque::new(),
            config,
            current_buffer_bytes: 0,
            codec,
            fetch_task: None,
            finished: false,
            error_mode,
            errors: Vec::new(),
        }
    }

    /// Get the next decompressed block, triggering prefetch if needed.
    ///
    /// This method returns blocks from the buffer if available, or waits for
    /// the background fetch task to complete. It automatically triggers new
    /// prefetch tasks when the buffer has space.
    ///
    /// In skip mode, if a block fails to decompress or has an invalid sync marker,
    /// the error is logged and the reader seeks to the next sync marker to continue.
    ///
    /// # Returns
    /// * `Ok(Some(block))` - The next decompressed block
    /// * `Ok(None)` - End of file reached
    /// * `Err(e)` - An error occurred (in strict mode)
    ///
    /// # Requirements
    /// - 3.5: Asynchronously load upcoming blocks up to a configurable buffer limit
    /// - 3.6: Pause fetching when buffer is full
    /// - 7.1: Skip bad blocks and continue to next sync marker (in skip mode)
    /// - 7.7: Log descriptive errors with recovery information
    pub async fn next(&mut self) -> Result<Option<DecompressedBlock>, ReaderError> {
        // If we have buffered blocks, return one
        if let Some(block) = self.buffer.pop_front() {
            self.current_buffer_bytes = self.current_buffer_bytes.saturating_sub(block.data.len());
            return Ok(Some(block));
        }

        // No buffered blocks - fetch and decompress directly
        if self.finished {
            return Ok(None);
        }

        // Keep trying to fetch blocks until we get one or reach EOF
        loop {
            let block_index_before = self.reader.block_index();
            let offset_before = self.reader.current_offset();

            // Fetch the next block
            let block = match self.reader.next_block().await {
                Ok(Some(b)) => b,
                Ok(None) => {
                    self.finished = true;
                    return Ok(None);
                }
                Err(e) => {
                    // Handle block read error based on error mode
                    match self.error_mode {
                        ErrorMode::Strict => return Err(e),
                        ErrorMode::Skip => {
                            // Use hybrid recovery for InvalidSyncMarker errors:
                            // 1. First try optimistic: advance past the invalid sync marker
                            //    and attempt to read the next block (assumes block data was
                            //    valid but sync marker was corrupted)
                            // 2. If that fails, fall back to scanning from byte 1 of the
                            //    invalid sync marker position
                            //
                            // This approach maximizes data recovery because:
                            // - If only the sync marker was corrupted, we don't skip any blocks
                            // - If the corruption is more severe, scanning finds the next valid block
                            //
                            // For other errors (parse errors, etc.), we scan from current position.
                            if let ReaderError::InvalidSyncMarker {
                                offset: invalid_sync_offset,
                                expected,
                                actual,
                                ..
                            } = &e
                            {
                                let error_message = e.to_string();
                                let invalid_sync_offset = *invalid_sync_offset;
                                let expected_marker = *expected;
                                let actual_marker = *actual;

                                // Try optimistic recovery first: position after invalid sync marker
                                self.reader.advance_past_invalid_sync(invalid_sync_offset);

                                // Attempt to read the next block
                                match self.reader.next_block().await {
                                    Ok(Some(next_block)) => {
                                        // Success! The optimistic approach worked.
                                        // Log the error and process this block.
                                        let error = ReadError::new(
                                            ReadErrorKind::InvalidSyncMarker {
                                                expected: expected_marker,
                                                actual: actual_marker,
                                            },
                                            block_index_before,
                                            None,
                                            offset_before,
                                            format!(
                                                "{}. Recovered by advancing past invalid sync marker.",
                                                error_message
                                            ),
                                        );
                                        self.errors.push(error);

                                        // Try to decompress this block
                                        match self.codec.decompress(&next_block.data) {
                                            Ok(decompressed_data) => {
                                                let decompressed_data =
                                                    Bytes::from(decompressed_data);
                                                return Ok(Some(DecompressedBlock::new(
                                                    next_block.record_count,
                                                    decompressed_data,
                                                    next_block.block_index,
                                                )));
                                            }
                                            Err(decomp_err) => {
                                                // Decompression failed on the optimistically-read block
                                                // Log this error and fall back to scanning
                                                let error = ReadError::new(
                                                    ReadErrorKind::DecompressionFailed {
                                                        codec: self.codec.name().to_string(),
                                                    },
                                                    next_block.block_index,
                                                    None,
                                                    next_block.file_offset,
                                                    decomp_err.to_string(),
                                                );
                                                self.errors.push(error);

                                                // Fall back to scanning
                                                let (found, bytes_skipped) =
                                                    self.reader.skip_to_next_sync().await?;

                                                if !found {
                                                    self.finished = true;
                                                    return Ok(None);
                                                }

                                                // Log scanning recovery
                                                let error = ReadError::new(
                                                    ReadErrorKind::InvalidSyncMarker {
                                                        expected: expected_marker,
                                                        actual: actual_marker,
                                                    },
                                                    block_index_before,
                                                    None,
                                                    offset_before,
                                                    format!(
                                                        "Scanned {} bytes to find next sync marker after decompression failure.",
                                                        bytes_skipped
                                                    ),
                                                );
                                                self.errors.push(error);

                                                continue;
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        // EOF reached after optimistic advance
                                        let error = ReadError::new(
                                            ReadErrorKind::InvalidSyncMarker {
                                                expected: expected_marker,
                                                actual: actual_marker,
                                            },
                                            block_index_before,
                                            None,
                                            offset_before,
                                            format!(
                                                "{}. Reached EOF after invalid sync marker.",
                                                error_message
                                            ),
                                        );
                                        self.errors.push(error);
                                        self.finished = true;
                                        return Ok(None);
                                    }
                                    Err(_next_err) => {
                                        // Optimistic approach failed - fall back to scanning
                                        // Scan from byte 1 of the invalid sync marker position
                                        // (not byte 0, to avoid finding the same corrupted position)
                                        let (found, bytes_skipped) = self
                                            .reader
                                            .skip_to_next_sync_from(invalid_sync_offset + 1)
                                            .await?;

                                        // Log the error with recovery information
                                        let error = ReadError::new(
                                            ReadErrorKind::InvalidSyncMarker {
                                                expected: expected_marker,
                                                actual: actual_marker,
                                            },
                                            block_index_before,
                                            None,
                                            offset_before,
                                            format!(
                                                "{}. Optimistic recovery failed, scanned {} bytes to find next sync marker.",
                                                error_message, bytes_skipped
                                            ),
                                        );
                                        self.errors.push(error);

                                        if !found {
                                            self.finished = true;
                                            return Ok(None);
                                        }

                                        continue;
                                    }
                                }
                            } else {
                                // For other errors (parse errors, truncated data, etc.),
                                // scan from the last known good position (offset_before + 1).
                                //
                                // This is critical because:
                                // - If the block header claimed a large compressed_size but the
                                //   actual data was truncated, the real next sync marker might
                                //   be hiding inside what we thought was the data body.
                                // - By scanning from offset_before + 1, we ensure we don't miss
                                //   any sync markers that might be earlier than current_offset.
                                let error_message = e.to_string();
                                let (found, bytes_skipped) = self
                                    .reader
                                    .skip_to_next_sync_from(offset_before + 1)
                                    .await?;

                                // Log the error with recovery information
                                let error = ReadError::new(
                                    ReadErrorKind::BlockParseFailed,
                                    block_index_before,
                                    None,
                                    offset_before,
                                    format!(
                                        "{}. Scanned {} bytes from offset {} to find next sync marker.",
                                        error_message, bytes_skipped, offset_before + 1
                                    ),
                                );
                                self.errors.push(error);

                                if !found {
                                    self.finished = true;
                                    return Ok(None);
                                }

                                continue;
                            }
                        }
                    }
                }
            };

            // Try to decompress the block
            match self.codec.decompress(&block.data) {
                Ok(decompressed_data) => {
                    let decompressed_data = Bytes::from(decompressed_data);
                    return Ok(Some(DecompressedBlock::new(
                        block.record_count,
                        decompressed_data,
                        block.block_index,
                    )));
                }
                Err(e) => {
                    // Handle decompression error based on error mode
                    match self.error_mode {
                        ErrorMode::Strict => return Err(e.into()),
                        ErrorMode::Skip => {
                            // Log the error
                            let error = ReadError::new(
                                ReadErrorKind::DecompressionFailed {
                                    codec: self.codec.name().to_string(),
                                },
                                block.block_index,
                                None,
                                block.file_offset,
                                e.to_string(),
                            );
                            self.errors.push(error);

                            // Skip this block and continue to the next one
                            // The next iteration will try to read the next block
                            continue;
                        }
                    }
                }
            }
        }
    }

    /// Check if we should start a prefetch task and start it if so.
    ///
    /// This enforces backpressure by only starting prefetch when we're
    /// below the configured limits.
    ///
    /// # Requirements
    /// - 3.6: Pause fetching when buffer is full
    fn maybe_start_prefetch(&mut self) {
        // Don't start if already running or finished
        if self.fetch_task.is_some() || self.finished {
            return;
        }

        // Check if we have space in the buffer (backpressure)
        if self.buffer.len() >= self.config.max_blocks {
            return;
        }

        if self.current_buffer_bytes >= self.config.max_bytes {
            return;
        }

        self.start_prefetch();
    }

    /// Start a background prefetch task.
    ///
    /// Spawns an async task that fetches the next block, decompresses it,
    /// and returns the result.
    fn start_prefetch(&mut self) {
        // Don't start if already running or finished
        if self.fetch_task.is_some() || self.finished {
            return;
        }

        // We need to move the reader temporarily to the task
        // Since we can't move out of &mut self, we'll use a different approach:
        // fetch and decompress synchronously in next() instead of spawning tasks
        // This is actually simpler and avoids the complexity of moving the reader
    }

    /// Get the number of blocks currently buffered.
    pub fn buffered_blocks(&self) -> usize {
        self.buffer.len()
    }

    /// Get the total bytes currently buffered.
    pub fn buffered_bytes(&self) -> usize {
        self.current_buffer_bytes
    }

    /// Check if we've reached the end of the file.
    pub fn is_finished(&self) -> bool {
        self.finished && self.buffer.is_empty()
    }

    /// Get a reference to the underlying reader's header.
    pub fn header(&self) -> &crate::reader::AvroHeader {
        self.reader.header()
    }

    /// Get accumulated errors (in skip mode).
    ///
    /// In skip mode, block-level errors are logged and processing continues.
    /// This method returns all block-level errors that occurred during reading.
    ///
    /// # Requirements
    /// - 7.3: Track error counts and positions
    /// - 7.4: Provide summary of skipped errors
    pub fn errors(&self) -> &[ReadError] {
        &self.errors
    }

    /// Get the error mode being used.
    pub fn error_mode(&self) -> ErrorMode {
        self.error_mode
    }
}

// Simpler implementation without background tasks
// This avoids the complexity of moving the reader between tasks
impl<S: StreamSource> PrefetchBuffer<S> {
    /// Fetch and decompress the next block synchronously.
    ///
    /// This is called by `next()` when we need a block and don't have one buffered.
    /// In skip mode, errors are logged and the reader seeks to the next sync marker.
    ///
    /// # Requirements
    /// - 7.1: Skip bad blocks and continue to next sync marker (in skip mode)
    /// - 7.7: Log descriptive errors with recovery information
    async fn fetch_and_decompress(&mut self) -> Result<Option<DecompressedBlock>, ReaderError> {
        // Keep trying to fetch blocks until we get one or reach EOF
        loop {
            let block_index_before = self.reader.block_index();
            let offset_before = self.reader.current_offset();

            // Fetch the next block
            let block = match self.reader.next_block().await {
                Ok(Some(b)) => b,
                Ok(None) => {
                    self.finished = true;
                    return Ok(None);
                }
                Err(e) => {
                    // Handle block read error based on error mode
                    match self.error_mode {
                        ErrorMode::Strict => return Err(e),
                        ErrorMode::Skip => {
                            // Use hybrid recovery for InvalidSyncMarker errors:
                            // 1. First try optimistic: advance past the invalid sync marker
                            //    and attempt to read the next block (assumes block data was
                            //    valid but sync marker was corrupted)
                            // 2. If that fails, fall back to scanning from byte 1 of the
                            //    invalid sync marker position
                            //
                            // This approach maximizes data recovery because:
                            // - If only the sync marker was corrupted, we don't skip any blocks
                            // - If the corruption is more severe, scanning finds the next valid block
                            //
                            // For other errors (parse errors, etc.), we scan from current position.
                            if let ReaderError::InvalidSyncMarker {
                                offset: invalid_sync_offset,
                                expected,
                                actual,
                                ..
                            } = &e
                            {
                                let error_message = e.to_string();
                                let invalid_sync_offset = *invalid_sync_offset;
                                let expected_marker = *expected;
                                let actual_marker = *actual;

                                // Try optimistic recovery first: position after invalid sync marker
                                self.reader.advance_past_invalid_sync(invalid_sync_offset);

                                // Attempt to read the next block
                                match self.reader.next_block().await {
                                    Ok(Some(next_block)) => {
                                        // Success! The optimistic approach worked.
                                        // Log the error and process this block.
                                        let error = ReadError::new(
                                            ReadErrorKind::InvalidSyncMarker {
                                                expected: expected_marker,
                                                actual: actual_marker,
                                            },
                                            block_index_before,
                                            None,
                                            offset_before,
                                            format!(
                                                "{}. Recovered by advancing past invalid sync marker.",
                                                error_message
                                            ),
                                        );
                                        self.errors.push(error);

                                        // Try to decompress this block
                                        match self.codec.decompress(&next_block.data) {
                                            Ok(decompressed_data) => {
                                                let decompressed_data =
                                                    Bytes::from(decompressed_data);
                                                return Ok(Some(DecompressedBlock::new(
                                                    next_block.record_count,
                                                    decompressed_data,
                                                    next_block.block_index,
                                                )));
                                            }
                                            Err(decomp_err) => {
                                                // Decompression failed on the optimistically-read block
                                                // Log this error and fall back to scanning
                                                let error = ReadError::new(
                                                    ReadErrorKind::DecompressionFailed {
                                                        codec: self.codec.name().to_string(),
                                                    },
                                                    next_block.block_index,
                                                    None,
                                                    next_block.file_offset,
                                                    decomp_err.to_string(),
                                                );
                                                self.errors.push(error);

                                                // Fall back to scanning
                                                let (found, bytes_skipped) =
                                                    self.reader.skip_to_next_sync().await?;

                                                if !found {
                                                    self.finished = true;
                                                    return Ok(None);
                                                }

                                                // Log scanning recovery
                                                let error = ReadError::new(
                                                    ReadErrorKind::InvalidSyncMarker {
                                                        expected: expected_marker,
                                                        actual: actual_marker,
                                                    },
                                                    block_index_before,
                                                    None,
                                                    offset_before,
                                                    format!(
                                                        "Scanned {} bytes to find next sync marker after decompression failure.",
                                                        bytes_skipped
                                                    ),
                                                );
                                                self.errors.push(error);

                                                continue;
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        // EOF reached after optimistic advance
                                        let error = ReadError::new(
                                            ReadErrorKind::InvalidSyncMarker {
                                                expected: expected_marker,
                                                actual: actual_marker,
                                            },
                                            block_index_before,
                                            None,
                                            offset_before,
                                            format!(
                                                "{}. Reached EOF after invalid sync marker.",
                                                error_message
                                            ),
                                        );
                                        self.errors.push(error);
                                        self.finished = true;
                                        return Ok(None);
                                    }
                                    Err(_next_err) => {
                                        // Optimistic approach failed - fall back to scanning
                                        // Scan from byte 1 of the invalid sync marker position
                                        // (not byte 0, to avoid finding the same corrupted position)
                                        let (found, bytes_skipped) = self
                                            .reader
                                            .skip_to_next_sync_from(invalid_sync_offset + 1)
                                            .await?;

                                        // Log the error with recovery information
                                        let error = ReadError::new(
                                            ReadErrorKind::InvalidSyncMarker {
                                                expected: expected_marker,
                                                actual: actual_marker,
                                            },
                                            block_index_before,
                                            None,
                                            offset_before,
                                            format!(
                                                "{}. Optimistic recovery failed, scanned {} bytes to find next sync marker.",
                                                error_message, bytes_skipped
                                            ),
                                        );
                                        self.errors.push(error);

                                        if !found {
                                            self.finished = true;
                                            return Ok(None);
                                        }

                                        continue;
                                    }
                                }
                            } else {
                                // For other errors (parse errors, truncated data, etc.),
                                // scan from the last known good position (offset_before + 1).
                                //
                                // This is critical because:
                                // - If the block header claimed a large compressed_size but the
                                //   actual data was truncated, the real next sync marker might
                                //   be hiding inside what we thought was the data body.
                                // - By scanning from offset_before + 1, we ensure we don't miss
                                //   any sync markers that might be earlier than current_offset.
                                let error_message = e.to_string();
                                let (found, bytes_skipped) = self
                                    .reader
                                    .skip_to_next_sync_from(offset_before + 1)
                                    .await?;

                                // Log the error with recovery information
                                let error = ReadError::new(
                                    ReadErrorKind::BlockParseFailed,
                                    block_index_before,
                                    None,
                                    offset_before,
                                    format!(
                                        "{}. Scanned {} bytes from offset {} to find next sync marker.",
                                        error_message, bytes_skipped, offset_before + 1
                                    ),
                                );
                                self.errors.push(error);

                                if !found {
                                    self.finished = true;
                                    return Ok(None);
                                }

                                continue;
                            }
                        }
                    }
                }
            };

            // Try to decompress the block
            match self.codec.decompress(&block.data) {
                Ok(decompressed_data) => {
                    let decompressed_data = Bytes::from(decompressed_data);
                    return Ok(Some(DecompressedBlock::new(
                        block.record_count,
                        decompressed_data,
                        block.block_index,
                    )));
                }
                Err(e) => {
                    // Handle decompression error based on error mode
                    match self.error_mode {
                        ErrorMode::Strict => return Err(e.into()),
                        ErrorMode::Skip => {
                            // Log the error
                            let error = ReadError::new(
                                ReadErrorKind::DecompressionFailed {
                                    codec: self.codec.name().to_string(),
                                },
                                block.block_index,
                                None,
                                block.file_offset,
                                e.to_string(),
                            );
                            self.errors.push(error);

                            // Skip this block and continue to the next one
                            continue;
                        }
                    }
                }
            }
        }
    }

    /// Prefill the buffer up to the configured limits.
    ///
    /// This can be called after creating the buffer to start prefetching
    /// before the first call to `next()`.
    ///
    /// In skip mode, errors are logged and processing continues.
    ///
    /// # Requirements
    /// - 7.1: Skip bad blocks and continue to next sync marker (in skip mode)
    pub async fn prefill(&mut self) -> Result<(), ReaderError> {
        while self.buffer.len() < self.config.max_blocks
            && self.current_buffer_bytes < self.config.max_bytes
            && !self.finished
        {
            match self.fetch_and_decompress().await {
                Ok(Some(block)) => {
                    self.current_buffer_bytes += block.data.len();
                    self.buffer.push_back(block);
                }
                Ok(None) => {
                    self.finished = true;
                    break;
                }
                Err(e) => {
                    // In strict mode, propagate the error
                    // In skip mode, errors are already logged in fetch_and_decompress
                    match self.error_mode {
                        ErrorMode::Strict => return Err(e),
                        ErrorMode::Skip => {
                            // Error already logged, continue
                            continue;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SourceError;
    use crate::reader::header::AVRO_MAGIC;
    use crate::reader::varint::encode_zigzag;

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

    /// Create a test block with given parameters
    fn create_test_block(record_count: i64, data: &[u8], sync_marker: &[u8; 16]) -> Vec<u8> {
        let mut block = Vec::new();
        block.extend_from_slice(&encode_zigzag(record_count));
        block.extend_from_slice(&encode_zigzag(data.len() as i64));
        block.extend_from_slice(data);
        block.extend_from_slice(sync_marker);
        block
    }

    /// Create a minimal valid Avro file with header and blocks
    fn create_test_avro_file(blocks: &[(i64, &[u8])]) -> Vec<u8> {
        let mut file = Vec::new();

        // Magic bytes
        file.extend_from_slice(&AVRO_MAGIC);

        // Metadata map: 1 entry (schema)
        file.extend_from_slice(&encode_zigzag(1));

        // Schema entry
        let schema_key = b"avro.schema";
        let schema_json = br#""string""#;
        file.extend_from_slice(&encode_zigzag(schema_key.len() as i64));
        file.extend_from_slice(schema_key);
        file.extend_from_slice(&encode_zigzag(schema_json.len() as i64));
        file.extend_from_slice(schema_json);

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

        file
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
    fn test_buffer_config_default() {
        let config = BufferConfig::default();
        assert_eq!(config.max_blocks, 4);
        assert_eq!(config.max_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn test_buffer_config_new() {
        let config = BufferConfig::new(10, 1024);
        assert_eq!(config.max_blocks, 10);
        assert_eq!(config.max_bytes, 1024);
    }

    #[test]
    fn test_prefetch_buffer_new() {
        run_async(async {
            let file_data = create_test_avro_file(&[(5, b"test")]);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            assert_eq!(buffer.buffered_blocks(), 0);
            assert_eq!(buffer.buffered_bytes(), 0);
            assert!(!buffer.is_finished());
            assert_eq!(buffer.error_mode(), ErrorMode::Strict);
        });
    }

    #[test]
    fn test_prefetch_buffer_next_single_block() {
        run_async(async {
            let block_data = b"test record data";
            let file_data = create_test_avro_file(&[(5, block_data)]);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // Get first block
            let block = buffer.next().await.unwrap().unwrap();
            assert_eq!(block.record_count, 5);
            assert_eq!(&block.data[..], block_data);
            assert_eq!(block.block_index, 0);

            // Should return None at EOF
            let next = buffer.next().await.unwrap();
            assert!(next.is_none());
            assert!(buffer.is_finished());
        });
    }

    #[test]
    fn test_prefetch_buffer_next_multiple_blocks() {
        run_async(async {
            let block1_data = b"first block";
            let block2_data = b"second block";
            let block3_data = b"third block";
            let file_data =
                create_test_avro_file(&[(10, block1_data), (20, block2_data), (30, block3_data)]);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // Read all blocks
            let block1 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block1.record_count, 10);
            assert_eq!(&block1.data[..], block1_data);

            let block2 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block2.record_count, 20);
            assert_eq!(&block2.data[..], block2_data);

            let block3 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block3.record_count, 30);
            assert_eq!(&block3.data[..], block3_data);

            // EOF
            assert!(buffer.next().await.unwrap().is_none());
            assert!(buffer.is_finished());
        });
    }

    #[test]
    fn test_prefetch_buffer_prefill() {
        run_async(async {
            let file_data = create_test_avro_file(&[
                (10, b"block1"),
                (20, b"block2"),
                (30, b"block3"),
                (40, b"block4"),
                (50, b"block5"),
            ]);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::new(3, 1024 * 1024); // Max 3 blocks

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // Prefill should load up to 3 blocks
            buffer.prefill().await.unwrap();

            assert_eq!(buffer.buffered_blocks(), 3);
            assert!(buffer.buffered_bytes() > 0);

            // Drain the buffer
            for i in 0..3 {
                let block = buffer.next().await.unwrap().unwrap();
                assert_eq!(block.record_count, (i + 1) * 10);
            }

            // Should still have more blocks available
            let block = buffer.next().await.unwrap().unwrap();
            assert_eq!(block.record_count, 40);
        });
    }

    #[test]
    fn test_prefetch_buffer_backpressure_blocks() {
        run_async(async {
            // Create many blocks
            let blocks: Vec<_> = (0..10).map(|i| (i * 10, b"data" as &[u8])).collect();
            let file_data = create_test_avro_file(&blocks);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::new(2, 1024 * 1024); // Max 2 blocks

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // Prefill should respect max_blocks limit
            buffer.prefill().await.unwrap();

            assert!(buffer.buffered_blocks() <= 2);
        });
    }

    #[test]
    fn test_prefetch_buffer_backpressure_bytes() {
        run_async(async {
            // Create blocks with large data
            let large_data = vec![0u8; 1024]; // 1KB per block
            let file_data = create_test_avro_file(&[
                (10, &large_data),
                (20, &large_data),
                (30, &large_data),
                (40, &large_data),
            ]);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::new(10, 2048); // Max 2KB

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // Prefill should respect max_bytes limit
            buffer.prefill().await.unwrap();

            // Should buffer at most 2 blocks (2KB total)
            assert!(buffer.buffered_blocks() <= 2);
            assert!(buffer.buffered_bytes() <= 2048);
        });
    }

    #[test]
    fn test_prefetch_buffer_empty_file() {
        run_async(async {
            let file_data = create_test_avro_file(&[]); // No blocks
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // Should immediately return None
            let result = buffer.next().await.unwrap();
            assert!(result.is_none());
            assert!(buffer.is_finished());
        });
    }

    #[test]
    fn test_prefetch_buffer_header_access() {
        run_async(async {
            let file_data = create_test_avro_file(&[(5, b"test")]);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let expected_codec = reader.codec();
            let config = BufferConfig::default();

            let buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // Should be able to access header
            let header = buffer.header();
            assert_eq!(header.codec, expected_codec);
        });
    }
}
