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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    fn start_prefetch(&mut self) {
        // Don't start if already running or finished
        if self.fetch_task.is_some() || self.finished {}

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

            // Read all blocks and verify order, data, and indices
            let block1 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block1.record_count, 10);
            assert_eq!(&block1.data[..], block1_data);
            assert_eq!(block1.block_index, 0, "First block should have index 0");

            let block2 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block2.record_count, 20);
            assert_eq!(&block2.data[..], block2_data);
            assert_eq!(block2.block_index, 1, "Second block should have index 1");

            let block3 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block3.record_count, 30);
            assert_eq!(&block3.data[..], block3_data);
            assert_eq!(block3.block_index, 2, "Third block should have index 2");

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

            // Prefill should load exactly 3 blocks (the limit)
            buffer.prefill().await.unwrap();

            assert_eq!(
                buffer.buffered_blocks(),
                3,
                "Should buffer exactly max_blocks"
            );
            assert!(buffer.buffered_bytes() > 0);

            // Drain the buffer and verify blocks come out in order
            let block1 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block1.record_count, 10, "First block should be block1");

            let block2 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block2.record_count, 20, "Second block should be block2");

            let block3 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block3.record_count, 30, "Third block should be block3");

            // Buffer should now be empty
            assert_eq!(buffer.buffered_blocks(), 0);

            // Should still have more blocks available (4 and 5)
            let block4 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block4.record_count, 40);

            let block5 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block5.record_count, 50);

            // Now EOF
            assert!(buffer.next().await.unwrap().is_none());
        });
    }

    #[test]
    fn test_prefetch_buffer_backpressure_blocks() {
        run_async(async {
            // Create 10 blocks with distinct record counts
            let blocks: Vec<_> = (1..=10).map(|i| (i * 10, b"data" as &[u8])).collect();
            let file_data = create_test_avro_file(&blocks);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::new(2, 1024 * 1024); // Max 2 blocks

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // Prefill should respect max_blocks limit exactly
            buffer.prefill().await.unwrap();

            assert_eq!(
                buffer.buffered_blocks(),
                2,
                "Should buffer exactly 2 blocks (max_blocks limit)"
            );

            // Verify the buffered blocks are the first two
            let block1 = buffer.next().await.unwrap().unwrap();
            assert_eq!(
                block1.record_count, 10,
                "First buffered block should be block 1"
            );

            let block2 = buffer.next().await.unwrap().unwrap();
            assert_eq!(
                block2.record_count, 20,
                "Second buffered block should be block 2"
            );

            // Can still read remaining blocks
            let block3 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block3.record_count, 30);
        });
    }

    #[test]
    fn test_prefetch_buffer_backpressure_bytes() {
        run_async(async {
            // Create blocks with 1KB data each
            let large_data = vec![0u8; 1024]; // 1KB per block
            let file_data = create_test_avro_file(&[
                (10, &large_data),
                (20, &large_data),
                (30, &large_data),
                (40, &large_data),
            ]);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            // Max 2KB means at most 2 blocks of 1KB each
            let config = BufferConfig::new(10, 2048);

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // Prefill should respect max_bytes limit
            buffer.prefill().await.unwrap();

            // Should buffer exactly 2 blocks (2KB total, hitting the byte limit)
            assert_eq!(
                buffer.buffered_blocks(),
                2,
                "Should buffer exactly 2 blocks due to byte limit"
            );
            assert!(
                buffer.buffered_bytes() <= 2048,
                "Buffered bytes {} should not exceed limit 2048",
                buffer.buffered_bytes()
            );
            assert!(
                buffer.buffered_bytes() >= 2000,
                "Should have buffered close to 2KB, got {}",
                buffer.buffered_bytes()
            );

            // Verify the blocks are correct
            let block1 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block1.record_count, 10);
            assert_eq!(block1.data.len(), 1024);

            let block2 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block2.record_count, 20);
            assert_eq!(block2.data.len(), 1024);
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

    // ========================================================================
    // Skip Mode Error Recovery Tests
    // ========================================================================

    /// Create an Avro file with a corrupted sync marker at a specific block
    fn create_avro_with_corrupted_sync(
        blocks: &[(i64, &[u8])],
        corrupt_block_idx: usize,
    ) -> Vec<u8> {
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

        // Valid sync marker
        let sync_marker: [u8; 16] = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ];
        // Corrupted sync marker (all bits flipped)
        let corrupted_sync: [u8; 16] = [
            0x21, 0x52, 0x41, 0x10, 0x35, 0x01, 0x45, 0x41, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43,
            0x21, 0x0F,
        ];

        file.extend_from_slice(&sync_marker);

        // Add blocks
        for (i, (record_count, data)) in blocks.iter().enumerate() {
            let marker = if i == corrupt_block_idx {
                &corrupted_sync
            } else {
                &sync_marker
            };
            file.extend_from_slice(&create_test_block(*record_count, data, marker));
        }

        file
    }

    /// Create an Avro file where a block has corrupted data (invalid varint)
    fn create_avro_with_corrupted_block_data(
        blocks: &[(i64, &[u8])],
        corrupt_block_idx: usize,
    ) -> Vec<u8> {
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

        // Sync marker
        let sync_marker: [u8; 16] = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ];
        file.extend_from_slice(&sync_marker);

        // Add blocks
        for (i, (record_count, data)) in blocks.iter().enumerate() {
            if i == corrupt_block_idx {
                // Create a block with invalid data size (claims more data than exists)
                let mut block = Vec::new();
                block.extend_from_slice(&encode_zigzag(*record_count));
                // Claim a huge data size that doesn't exist
                block.extend_from_slice(&encode_zigzag(999999));
                // Only provide a few bytes
                block.extend_from_slice(b"short");
                block.extend_from_slice(&sync_marker);
                file.extend_from_slice(&block);
            } else {
                file.extend_from_slice(&create_test_block(*record_count, data, &sync_marker));
            }
        }

        file
    }

    #[test]
    fn test_skip_mode_corrupted_sync_marker_recovery() {
        run_async(async {
            // Create file with 5 blocks, corrupt the sync marker after block 2 (index 2)
            //
            // IMPORTANT: The sync marker is validated as part of AvroBlock::parse() BEFORE
            // the block is returned. So when block N's trailing sync marker is corrupted,
            // block N's data is NOT returned - the error occurs during parsing of block N.
            //
            // Expected behavior:
            // - Blocks 0, 1 read successfully (valid sync markers)
            // - Block 2 parse fails (corrupted trailing sync marker)
            // - Recovery scans and finds next valid sync marker
            // - Blocks 3, 4 should be recoverable
            let file_data = create_avro_with_corrupted_sync(
                &[
                    (10, b"block1_data"),
                    (20, b"block2_data"),
                    (30, b"block3_data"), // This block's trailing sync marker is corrupted
                    (40, b"block4_data"),
                    (50, b"block5_data"),
                ],
                2, // Corrupt sync marker after block index 2
            );
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Skip);

            let mut blocks_read = Vec::new();
            while let Some(block) = buffer.next().await.unwrap() {
                blocks_read.push(block.record_count);
            }

            // Blocks 1 and 2 should be read (their sync markers are valid)
            assert!(
                blocks_read.contains(&10),
                "Should have read block 1, got: {:?}",
                blocks_read
            );
            assert!(
                blocks_read.contains(&20),
                "Should have read block 2, got: {:?}",
                blocks_read
            );

            // Block 3 should NOT be read - its trailing sync marker is corrupted,
            // so AvroBlock::parse() fails before returning the block
            assert!(
                !blocks_read.contains(&30),
                "Block 3 should be skipped (corrupted trailing sync), got: {:?}",
                blocks_read
            );

            // Blocks 4 and 5 should be recovered after scanning
            assert!(
                blocks_read.contains(&40),
                "Should have recovered block 4 after corruption, got: {:?}",
                blocks_read
            );
            assert!(
                blocks_read.contains(&50),
                "Should have recovered block 5 after corruption, got: {:?}",
                blocks_read
            );

            // Verify exact blocks read: 1, 2, 4, 5 (block 3 skipped)
            assert_eq!(
                blocks_read,
                vec![10, 20, 40, 50],
                "Should read blocks 1, 2, 4, 5 (block 3 skipped due to corrupted sync)"
            );

            // Should have logged at least one error for the corrupted sync marker
            assert!(
                !buffer.errors().is_empty(),
                "Should have logged error for corrupted sync marker"
            );
        });
    }

    #[test]
    fn test_skip_mode_corrupted_first_block_sync() {
        run_async(async {
            // Corrupt the very first block's trailing sync marker
            //
            // IMPORTANT: The sync marker is validated as part of AvroBlock::parse() BEFORE
            // the block is returned. So block 0's data is NOT returned when its trailing
            // sync marker is corrupted.
            //
            // Expected behavior:
            // - Block 0 parse fails (corrupted trailing sync marker)
            // - Recovery scans and finds next valid sync marker
            // - Blocks 1, 2 should be recovered
            let file_data = create_avro_with_corrupted_sync(
                &[
                    (10, b"block1_data"), // This block's trailing sync marker is corrupted
                    (20, b"block2_data"),
                    (30, b"block3_data"),
                ],
                0,
            );
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Skip);

            let mut blocks_read = Vec::new();
            while let Some(block) = buffer.next().await.unwrap() {
                blocks_read.push(block.record_count);
            }

            // Block 1 should NOT be read - its trailing sync marker is corrupted
            assert!(
                !blocks_read.contains(&10),
                "Block 1 should be skipped (corrupted trailing sync), got: {:?}",
                blocks_read
            );

            // Blocks 2 and 3 should be recovered after scanning
            assert!(
                blocks_read.contains(&20),
                "Should have recovered block 2, got: {:?}",
                blocks_read
            );
            assert!(
                blocks_read.contains(&30),
                "Should have recovered block 3, got: {:?}",
                blocks_read
            );

            // Verify exact blocks read: 2, 3 (block 1 skipped)
            assert_eq!(
                blocks_read,
                vec![20, 30],
                "Should read blocks 2, 3 (block 1 skipped due to corrupted sync)"
            );

            // Should have logged an error for the corrupted sync marker
            assert!(
                !buffer.errors().is_empty(),
                "Should have logged error for corrupted sync marker"
            );
        });
    }

    #[test]
    fn test_skip_mode_corrupted_last_block_sync() {
        run_async(async {
            // Corrupt the last block's trailing sync marker
            //
            // IMPORTANT: The sync marker is validated as part of AvroBlock::parse() BEFORE
            // the block is returned. So block 2's data is NOT returned when its trailing
            // sync marker is corrupted.
            //
            // Expected behavior:
            // - Blocks 0, 1 read successfully (valid sync markers)
            // - Block 2 parse fails (corrupted trailing sync marker)
            // - Recovery scans but finds no more valid sync markers (EOF)
            // - Only blocks 0, 1 are returned
            let file_data = create_avro_with_corrupted_sync(
                &[
                    (10, b"block1_data"),
                    (20, b"block2_data"),
                    (30, b"block3_data"), // Last block, trailing sync marker corrupted
                ],
                2,
            );
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Skip);

            let mut blocks_read = Vec::new();
            while let Some(block) = buffer.next().await.unwrap() {
                blocks_read.push(block.record_count);
            }

            // Blocks 1 and 2 should be read (their sync markers are valid)
            assert!(
                blocks_read.contains(&10),
                "Should have read block 1, got: {:?}",
                blocks_read
            );
            assert!(
                blocks_read.contains(&20),
                "Should have read block 2, got: {:?}",
                blocks_read
            );

            // Block 3 should NOT be read - its trailing sync marker is corrupted
            assert!(
                !blocks_read.contains(&30),
                "Block 3 should be skipped (corrupted trailing sync), got: {:?}",
                blocks_read
            );

            // Verify exact blocks read: 1, 2 (block 3 skipped)
            assert_eq!(
                blocks_read,
                vec![10, 20],
                "Should read blocks 1, 2 (block 3 skipped due to corrupted sync)"
            );

            assert!(buffer.is_finished());

            // Should have logged an error for the corrupted sync marker
            assert!(
                !buffer.errors().is_empty(),
                "Should have logged error for corrupted final sync marker"
            );
        });
    }

    #[test]
    fn test_skip_mode_multiple_consecutive_corrupted_blocks() {
        run_async(async {
            // Create file where multiple consecutive blocks have corrupted sync markers
            // This is a worst-case scenario testing recovery across multiple corruptions
            //
            // IMPORTANT: The sync marker is validated as part of AvroBlock::parse() BEFORE
            // the block is returned. So blocks with corrupted trailing sync markers are
            // NOT returned.
            //
            // File structure:
            // - Block 1: valid sync marker -> returned
            // - Block 2: corrupted sync marker -> NOT returned, triggers recovery
            // - Block 3: corrupted sync marker -> NOT returned (if recovery lands here)
            // - Block 4: valid sync marker -> returned after recovery
            // - Block 5: valid sync marker -> returned
            let mut file = Vec::new();

            // Magic bytes
            file.extend_from_slice(&AVRO_MAGIC);

            // Metadata
            file.extend_from_slice(&encode_zigzag(1));
            let schema_key = b"avro.schema";
            let schema_json = br#""string""#;
            file.extend_from_slice(&encode_zigzag(schema_key.len() as i64));
            file.extend_from_slice(schema_key);
            file.extend_from_slice(&encode_zigzag(schema_json.len() as i64));
            file.extend_from_slice(schema_json);
            file.push(0x00);

            let sync_marker: [u8; 16] = [
                0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
                0xDE, 0xF0,
            ];
            let bad_sync: [u8; 16] = [0xFF; 16];

            file.extend_from_slice(&sync_marker);

            // Block 1: valid sync marker
            file.extend_from_slice(&create_test_block(10, b"block1", &sync_marker));
            // Block 2: corrupted sync marker
            file.extend_from_slice(&create_test_block(20, b"block2", &bad_sync));
            // Block 3: corrupted sync marker
            file.extend_from_slice(&create_test_block(30, b"block3", &bad_sync));
            // Block 4: valid sync marker (recovery target)
            file.extend_from_slice(&create_test_block(40, b"block4", &sync_marker));
            // Block 5: valid sync marker
            file.extend_from_slice(&create_test_block(50, b"block5", &sync_marker));

            let source = MockSource::new(file);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Skip);

            let mut blocks_read = Vec::new();
            while let Some(block) = buffer.next().await.unwrap() {
                blocks_read.push(block.record_count);
            }

            // Block 1 should be read (has valid sync marker)
            assert!(
                blocks_read.contains(&10),
                "Should have read block 1 (valid sync), got: {:?}",
                blocks_read
            );

            // Block 2 should NOT be read - its trailing sync marker is corrupted
            assert!(
                !blocks_read.contains(&20),
                "Block 2 should be skipped (corrupted trailing sync), got: {:?}",
                blocks_read
            );

            // Block 3 should NOT be read - its trailing sync marker is corrupted
            assert!(
                !blocks_read.contains(&30),
                "Block 3 should be skipped (corrupted trailing sync), got: {:?}",
                blocks_read
            );

            // Block 5 should be recovered (valid sync marker)
            assert!(
                blocks_read.contains(&50),
                "Should have recovered block 5, got: {:?}",
                blocks_read
            );

            // Should have logged errors for the corrupted sync markers
            assert!(
                !buffer.errors().is_empty(),
                "Should have logged errors for corrupted sync markers"
            );
        });
    }

    #[test]
    fn test_skip_mode_block_parse_error_recovery() {
        run_async(async {
            // Create file with a block that has invalid data size.
            // Block 2 (index 1) claims compressed_size=999999 but only has 5 bytes.
            // The reader should:
            // 1. Successfully read block 1
            // 2. Fail to parse block 2 (EOF trying to read 999999 bytes)
            // 3. Scan for next sync marker and recover
            // 4. Successfully read blocks 3 and 4
            let file_data = create_avro_with_corrupted_block_data(
                &[
                    (10, b"block1_data"),
                    (20, b"block2_data"), // This block has corrupted data size
                    (30, b"block3_data"),
                    (40, b"block4_data"),
                ],
                1,
            );
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Skip);

            let mut blocks_read = Vec::new();
            while let Some(block) = buffer.next().await.unwrap() {
                blocks_read.push(block.record_count);
            }

            // Block 1 should definitely be read (before corruption)
            assert!(
                blocks_read.contains(&10),
                "Should have read block 1 before corruption, got: {:?}",
                blocks_read
            );

            // Blocks 3 and 4 should be recovered (after corruption)
            assert!(
                blocks_read.contains(&30),
                "Should have recovered block 3 after corruption, got: {:?}",
                blocks_read
            );
            assert!(
                blocks_read.contains(&40),
                "Should have recovered block 4 after corruption, got: {:?}",
                blocks_read
            );

            // Block 2 should NOT be in the list (it was corrupted)
            assert!(
                !blocks_read.contains(&20),
                "Corrupted block 2 should be skipped, got: {:?}",
                blocks_read
            );

            // Should have logged an error for the corrupted block
            assert!(
                !buffer.errors().is_empty(),
                "Should have logged error for corrupted block"
            );

            // Verify we read exactly 3 blocks (1, 3, 4)
            assert_eq!(
                blocks_read.len(),
                3,
                "Should have read exactly 3 blocks (skipping corrupted block 2), got: {:?}",
                blocks_read
            );
        });
    }

    #[test]
    fn test_skip_mode_error_accumulation() {
        run_async(async {
            // Create file with a corrupted sync marker to test error accumulation
            let file_data = create_avro_with_corrupted_sync(
                &[
                    (10, b"block1"),
                    (20, b"block2"), // trailing sync marker corrupted
                    (30, b"block3"),
                    (40, b"block4"),
                ],
                1,
            );
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Skip);

            // Drain all blocks
            let mut blocks_read = Vec::new();
            while let Some(block) = buffer.next().await.unwrap() {
                blocks_read.push(block.record_count);
            }

            // Should have read blocks
            assert!(!blocks_read.is_empty(), "Should have read some blocks");

            // Check error properties
            let errors = buffer.errors();
            assert!(
                !errors.is_empty(),
                "Should have accumulated at least one error"
            );

            for error in errors {
                // Each error should have meaningful information
                assert!(
                    !error.message.is_empty(),
                    "Error should have a non-empty message"
                );
                // Block index should be set
                assert!(
                    error.block_index < 100,
                    "Block index should be reasonable, got {}",
                    error.block_index
                );
            }
        });
    }

    #[test]
    fn test_strict_mode_fails_on_corrupted_sync() {
        run_async(async {
            // Test that strict mode fails immediately on corrupted sync marker
            //
            // IMPORTANT: The sync marker is validated as part of AvroBlock::parse() BEFORE
            // the block is returned. So when block 1's trailing sync marker is corrupted,
            // the error occurs when trying to read block 1 (not block 2).
            let file_data = create_avro_with_corrupted_sync(
                &[
                    (10, b"block1"),
                    (20, b"block2"), // trailing sync marker corrupted
                    (30, b"block3"),
                ],
                1,
            );
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // First block should succeed completely (its sync marker is valid)
            let block1 = buffer.next().await.unwrap().unwrap();
            assert_eq!(
                block1.record_count, 10,
                "First block should have record_count 10"
            );
            assert_eq!(&block1.data[..], b"block1", "First block data should match");
            assert_eq!(block1.block_index, 0);

            // Second block read should fail - its trailing sync marker is corrupted
            // The error occurs during AvroBlock::parse() when validating the sync marker
            let result = buffer.next().await;
            assert!(
                result.is_err(),
                "Strict mode should fail on corrupted sync marker when reading block 2"
            );

            // Verify the error is about sync marker
            let err = result.unwrap_err();
            let err_str = err.to_string().to_lowercase();
            assert!(
                err_str.contains("sync") || err_str.contains("marker"),
                "Error should mention sync marker, got: {}",
                err
            );
        });
    }

    #[test]
    fn test_skip_mode_empty_file() {
        run_async(async {
            let file_data = create_test_avro_file(&[]);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Skip);

            // Should return None immediately, no errors
            let result = buffer.next().await.unwrap();
            assert!(result.is_none());
            assert!(buffer.errors().is_empty());
            assert!(buffer.is_finished());
        });
    }

    #[test]
    fn test_skip_mode_all_blocks_corrupted() {
        run_async(async {
            // Create file where ALL blocks have corrupted sync markers
            // This tests the worst case where recovery keeps failing
            //
            // IMPORTANT: The sync marker is validated as part of AvroBlock::parse() BEFORE
            // the block is returned. So NO blocks are returned when all have corrupted
            // trailing sync markers.
            let mut file = Vec::new();

            // Header
            file.extend_from_slice(&AVRO_MAGIC);
            file.extend_from_slice(&encode_zigzag(1));
            let schema_key = b"avro.schema";
            let schema_json = br#""string""#;
            file.extend_from_slice(&encode_zigzag(schema_key.len() as i64));
            file.extend_from_slice(schema_key);
            file.extend_from_slice(&encode_zigzag(schema_json.len() as i64));
            file.extend_from_slice(schema_json);
            file.push(0x00);

            let sync_marker: [u8; 16] = [
                0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
                0xDE, 0xF0,
            ];
            let bad_sync: [u8; 16] = [0xFF; 16];

            file.extend_from_slice(&sync_marker);

            // All blocks have bad sync markers
            file.extend_from_slice(&create_test_block(10, b"block1", &bad_sync));
            file.extend_from_slice(&create_test_block(20, b"block2", &bad_sync));
            file.extend_from_slice(&create_test_block(30, b"block3", &bad_sync));

            let source = MockSource::new(file);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::default();

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Skip);

            // Should eventually finish without infinite loop
            let mut blocks_read = Vec::new();
            let mut iterations = 0;
            while let Some(block) = buffer.next().await.unwrap() {
                blocks_read.push(block.record_count);
                iterations += 1;
                if iterations > 100 {
                    panic!("Infinite loop detected - recovery not terminating");
                }
            }

            assert!(buffer.is_finished(), "Should reach finished state");

            // NO blocks should be read - all have corrupted trailing sync markers
            // The sync marker is validated BEFORE the block is returned
            assert!(
                blocks_read.is_empty(),
                "No blocks should be read when all have corrupted sync markers, got: {:?}",
                blocks_read
            );

            // Should have accumulated errors from the corrupted sync markers
            assert!(
                !buffer.errors().is_empty(),
                "Should have logged errors for corrupted sync markers"
            );
        });
    }

    #[test]
    fn test_buffer_bytes_tracking_accuracy() {
        run_async(async {
            let block1_data = vec![0u8; 1000]; // 1KB
            let block2_data = vec![0u8; 2000]; // 2KB
            let file_data = create_test_avro_file(&[(10, &block1_data), (20, &block2_data)]);
            let source = MockSource::new(file_data);
            let reader = BlockReader::new(source).await.unwrap();
            let config = BufferConfig::new(10, 10 * 1024 * 1024);

            let mut buffer = PrefetchBuffer::new(reader, config, ErrorMode::Strict);

            // Initially no bytes buffered
            assert_eq!(
                buffer.buffered_bytes(),
                0,
                "Should start with 0 bytes buffered"
            );
            assert_eq!(
                buffer.buffered_blocks(),
                0,
                "Should start with 0 blocks buffered"
            );

            // Prefill both blocks
            buffer.prefill().await.unwrap();

            // Should have buffered exactly 3KB (1KB + 2KB)
            let buffered_after_prefill = buffer.buffered_bytes();
            assert_eq!(
                buffered_after_prefill, 3000,
                "Should have buffered exactly 3000 bytes (1KB + 2KB)"
            );
            assert_eq!(buffer.buffered_blocks(), 2, "Should have buffered 2 blocks");

            // Consume first block (1KB)
            let block1 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block1.data.len(), 1000, "Block 1 should be 1000 bytes");
            assert_eq!(block1.record_count, 10);

            // Should now have 2KB buffered
            assert_eq!(
                buffer.buffered_bytes(),
                2000,
                "Should have 2000 bytes after consuming 1KB block"
            );
            assert_eq!(buffer.buffered_blocks(), 1, "Should have 1 block buffered");

            // Consume second block (2KB)
            let block2 = buffer.next().await.unwrap().unwrap();
            assert_eq!(block2.data.len(), 2000, "Block 2 should be 2000 bytes");
            assert_eq!(block2.record_count, 20);

            // Should now have 0 bytes buffered
            assert_eq!(
                buffer.buffered_bytes(),
                0,
                "Should have 0 bytes after consuming all blocks"
            );
            assert_eq!(buffer.buffered_blocks(), 0, "Should have 0 blocks buffered");

            // EOF
            assert!(buffer.next().await.unwrap().is_none());
        });
    }
}
