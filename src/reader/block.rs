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

/// Default read buffer size for fetching data from source
const DEFAULT_READ_BUFFER_SIZE: usize = 64 * 1024; // 64KB

/// Reads and parses Avro blocks from a StreamSource.
///
/// `BlockReader` orchestrates header parsing and block iteration,
/// tracking the current file offset and block index. It supports
/// seeking to sync markers for resumable reads.
///
/// # Requirements
/// - 3.1: Read and process one block at a time
/// - 3.7: Support seeking to a specific block by sync marker
///
/// # Example
/// ```ignore
/// use avro_stream::reader::BlockReader;
/// use avro_stream::source::LocalSource;
///
/// let source = LocalSource::open("data.avro").await?;
/// let mut reader = BlockReader::new(source).await?;
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
    /// Current read offset in the file
    current_offset: u64,
    /// Current block index (0-indexed)
    block_index: usize,
    /// Total file size (cached)
    file_size: u64,
}

impl<S: StreamSource> BlockReader<S> {
    /// Create a new BlockReader from a StreamSource.
    ///
    /// This will read and parse the Avro file header, validating
    /// the magic bytes and extracting the schema and codec.
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
        // Get file size
        let file_size = source.size().await?;

        // Read enough bytes for the header (we'll read more if needed)
        // Most headers are small, but we need to handle large schemas
        let initial_read_size = std::cmp::min(file_size as usize, DEFAULT_READ_BUFFER_SIZE);
        let header_bytes = source.read_range(0, initial_read_size).await?;

        // Parse the header
        let header = AvroHeader::parse(&header_bytes)?;

        Ok(Self {
            source,
            current_offset: header.header_size,
            block_index: 0,
            file_size,
            header,
        })
    }

    /// Read the next block from the file.
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
    pub async fn next_block(&mut self) -> Result<Option<AvroBlock>, ReaderError> {
        // Check if we've reached the end of the file
        if self.current_offset >= self.file_size {
            return Ok(None);
        }

        // Calculate how much to read
        let remaining = self.file_size - self.current_offset;
        let read_size = std::cmp::min(remaining as usize, DEFAULT_READ_BUFFER_SIZE);

        // Read data from source
        let data = self
            .source
            .read_range(self.current_offset, read_size)
            .await?;

        if data.is_empty() {
            return Ok(None);
        }

        // Try to parse the block
        match AvroBlock::parse(
            &data,
            &self.header.sync_marker,
            self.current_offset,
            self.block_index,
        ) {
            Ok((block, consumed)) => {
                self.current_offset += consumed as u64;
                self.block_index += 1;
                Ok(Some(block))
            }
            Err(ReaderError::Parse { message, offset })
                if message.starts_with("Not enough bytes for block data") =>
            {
                // Block is larger than our initial read - need to read more
                // Parse the required size from the block header we already have
                self.read_large_block(&data, remaining as usize).await
            }
            Err(e) => Err(e),
        }
    }

    /// Handle reading a block that's larger than the default buffer size.
    ///
    /// This is the slow path - only called when a block exceeds 64KB.
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

        let sync_marker = self.header.sync_marker;
        let mut scan_offset = position;

        // Scan for sync marker
        while scan_offset + SYNC_MARKER_SIZE as u64 <= self.file_size {
            // Read a chunk of data to scan
            let remaining = self.file_size - scan_offset;
            let read_size = std::cmp::min(remaining as usize, DEFAULT_READ_BUFFER_SIZE);
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
    /// read the first block again.
    pub fn reset(&mut self) {
        self.current_offset = self.header.header_size;
        self.block_index = 0;
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
        self.block_index += 1;
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

        let sync_marker = self.header.sync_marker;
        let mut scan_offset = start_from;

        // Scan for sync marker
        while scan_offset + SYNC_MARKER_SIZE as u64 <= self.file_size {
            // Read a chunk of data to scan
            let remaining = self.file_size - scan_offset;
            let read_size = std::cmp::min(remaining as usize, DEFAULT_READ_BUFFER_SIZE);
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

        let sync_marker = self.header.sync_marker;
        // Start scanning from current_offset + 1 to skip past any partial/wrong sync marker
        let mut scan_offset = start_offset.saturating_add(1);

        // Scan for sync marker
        while scan_offset + SYNC_MARKER_SIZE as u64 <= self.file_size {
            // Read a chunk of data to scan
            let remaining = self.file_size - scan_offset;
            let read_size = std::cmp::min(remaining as usize, DEFAULT_READ_BUFFER_SIZE);
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
    }
}
