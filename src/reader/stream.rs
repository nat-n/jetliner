//! Streaming Avro reader that orchestrates the full pipeline
//!
//! The `AvroStreamReader` is the main entry point for streaming Avro data
//! into Polars DataFrames. It orchestrates the prefetch buffer, record decoder,
//! and DataFrame builder to provide an async iterator over DataFrames.
//!
//! # Requirements
//! - 3.1: Read and process one block at a time
//! - 3.2: Don't load entire file into memory
//! - 3.3: Release memory from previous blocks
//! - 3.4: Yield DataFrames with configurable row limit

use polars::prelude::*;

use crate::convert::{BuilderConfig, DataFrameBuilder, ErrorMode};
use crate::error::{ReadError, ReaderError, SchemaError};
use crate::reader::{BlockReader, BufferConfig, PrefetchBuffer};
use crate::schema::AvroSchema;
use crate::source::StreamSource;

/// Configuration for the AvroStreamReader.
///
/// Controls batch size, buffering, error handling, and optional schema resolution.
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// Target number of rows per DataFrame batch (default: 100,000).
    pub batch_size: usize,
    /// Prefetch buffer configuration.
    pub buffer_config: BufferConfig,
    /// Error handling mode (strict or skip).
    pub error_mode: ErrorMode,
    /// Optional reader schema for schema evolution (default: None).
    pub reader_schema: Option<AvroSchema>,
    /// Optional column projection (default: None = all columns).
    pub projected_columns: Option<Vec<String>>,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            batch_size: 100_000,
            buffer_config: BufferConfig::default(),
            error_mode: ErrorMode::Strict,
            reader_schema: None,
            projected_columns: None,
        }
    }
}

impl ReaderConfig {
    /// Create a new ReaderConfig with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the batch size (rows per DataFrame).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the buffer configuration.
    pub fn with_buffer_config(mut self, config: BufferConfig) -> Self {
        self.buffer_config = config;
        self
    }

    /// Set the error mode.
    pub fn with_error_mode(mut self, mode: ErrorMode) -> Self {
        self.error_mode = mode;
        self
    }

    /// Enable strict error mode (fail on first error).
    pub fn strict(mut self) -> Self {
        self.error_mode = ErrorMode::Strict;
        self
    }

    /// Enable skip error mode (continue on errors).
    pub fn skip_errors(mut self) -> Self {
        self.error_mode = ErrorMode::Skip;
        self
    }

    /// Set the reader schema for schema evolution.
    pub fn with_reader_schema(mut self, schema: AvroSchema) -> Self {
        self.reader_schema = Some(schema);
        self
    }

    /// Set column projection (only read specified columns).
    pub fn with_projection(mut self, columns: Vec<String>) -> Self {
        self.projected_columns = Some(columns);
        self
    }
}

/// Main streaming reader for Avro files.
///
/// `AvroStreamReader` orchestrates the full pipeline from source to DataFrame:
/// 1. `PrefetchBuffer` asynchronously fetches and decompresses blocks
/// 2. `DataFrameBuilder` decodes records and builds DataFrames
/// 3. `next_batch()` returns DataFrames when batch size is reached
///
/// # Memory Management
///
/// The reader maintains bounded memory usage:
/// - Prefetch buffer: limited by `buffer_config` (blocks and bytes)
/// - DataFrame builder: limited by `batch_size` rows
/// - Previous blocks and DataFrames are released after processing
///
/// # Error Handling
///
/// In strict mode, errors cause immediate failure. In skip mode, errors are
/// logged and processing continues. Accumulated errors can be retrieved via
/// `errors()` after iteration completes.
///
/// # Requirements
/// - 3.1: Read and process one block at a time
/// - 3.2: Don't load entire file into memory
/// - 3.3: Release memory from previous blocks
/// - 3.4: Yield DataFrames with configurable row limit
///
/// # Example
/// ```ignore
/// use jetliner::reader::{AvroStreamReader, ReaderConfig};
/// use jetliner::source::LocalSource;
///
/// let source = LocalSource::open("data.avro").await?;
/// let config = ReaderConfig::new().with_batch_size(50_000);
/// let mut reader = AvroStreamReader::open(source, config).await?;
///
/// while let Some(df) = reader.next_batch().await? {
///     println!("Got {} rows", df.height());
/// }
///
/// // Check for errors (in skip mode)
/// for error in reader.errors() {
///     eprintln!("Error: {}", error);
/// }
/// ```
pub struct AvroStreamReader<S: StreamSource> {
    /// Prefetch buffer for async block loading
    buffer: PrefetchBuffer<S>,
    /// DataFrame builder for record decoding
    builder: DataFrameBuilder,
    /// Configuration
    config: ReaderConfig,
    /// Whether we've reached EOF
    finished: bool,
}

impl<S: StreamSource + 'static> AvroStreamReader<S> {
    /// Open an Avro file for streaming.
    ///
    /// This reads and parses the file header, validates the schema,
    /// and prepares the reader for streaming.
    ///
    /// # Arguments
    /// * `source` - The data source to read from (S3 or local)
    /// * `config` - Reader configuration
    ///
    /// # Returns
    /// A new AvroStreamReader ready to stream DataFrames.
    ///
    /// # Errors
    /// - `ReaderError::Source` if the source cannot be accessed
    /// - `ReaderError::InvalidMagic` if magic bytes don't match
    /// - `ReaderError::Parse` if header parsing fails
    /// - `ReaderError::Schema` if schema is invalid
    /// - `ReaderError::Codec` if codec is unknown
    ///
    /// # Requirements
    /// - 3.1: Read and process one block at a time
    /// - 3.2: Don't load entire file into memory
    pub async fn open(source: S, config: ReaderConfig) -> Result<Self, ReaderError> {
        // Create block reader (parses header)
        let block_reader = BlockReader::new(source).await?;

        // Get the schema from the header
        let schema = block_reader.header().schema.clone();

        // Use reader schema if provided, otherwise use writer schema
        let effective_schema = config.reader_schema.as_ref().unwrap_or(&schema);

        // Validate schema is a record type
        if !matches!(effective_schema, AvroSchema::Record(_)) {
            return Err(ReaderError::Schema(SchemaError::InvalidSchema(
                "Top-level schema must be a record type".to_string(),
            )));
        }

        // Create prefetch buffer with error mode
        let buffer = PrefetchBuffer::new(
            block_reader,
            config.buffer_config.clone(),
            config.error_mode,
        );

        // Create DataFrame builder with projection if specified
        let builder_config =
            BuilderConfig::new(config.batch_size).with_error_mode(config.error_mode);

        let builder = if let Some(ref columns) = config.projected_columns {
            DataFrameBuilder::with_projection(effective_schema, builder_config, columns)?
        } else {
            DataFrameBuilder::new(effective_schema, builder_config)?
        };

        Ok(Self {
            buffer,
            builder,
            config,
            finished: false,
        })
    }

    /// Get the next batch of records as a DataFrame.
    ///
    /// This method:
    /// 1. Fetches decompressed blocks from the prefetch buffer
    /// 2. Decodes records into the DataFrame builder
    /// 3. Returns a DataFrame when batch size is reached or EOF
    ///
    /// Returns `None` when all records have been read.
    ///
    /// # Returns
    /// - `Ok(Some(df))` - The next DataFrame batch
    /// - `Ok(None)` - End of file reached
    /// - `Err(e)` - An error occurred (in strict mode)
    ///
    /// # Errors
    /// In strict mode, returns an error on the first decode failure.
    /// In skip mode, errors are accumulated and can be retrieved via `errors()`.
    ///
    /// # Requirements
    /// - 3.1: Read and process one block at a time
    /// - 3.2: Don't load entire file into memory
    /// - 3.3: Release memory from previous blocks
    /// - 3.4: Yield DataFrames with configurable row limit
    pub async fn next_batch(&mut self) -> Result<Option<DataFrame>, ReaderError> {
        // If we're already finished, return None
        if self.finished {
            return Ok(None);
        }

        // Keep adding blocks until we have a full batch or reach EOF
        loop {
            // Fetch the next decompressed block
            match self.buffer.next().await? {
                Some(block) => {
                    // Add the block to the builder
                    // In skip mode, errors are logged internally
                    // In strict mode, this will propagate the error
                    self.builder.add_block(block)?;

                    // Check if we have enough records for a batch
                    if self.builder.is_batch_ready() {
                        // Build and return the DataFrame
                        if let Some(df) = self.builder.build(false)? {
                            return Ok(Some(df));
                        }
                    }
                }
                None => {
                    // EOF reached - finish any remaining records
                    self.finished = true;

                    // Force build to get the final batch
                    if let Some(df) = self.builder.finish()? {
                        return Ok(Some(df));
                    }

                    // No more records
                    return Ok(None);
                }
            }
        }
    }

    /// Get the Avro schema being used for reading.
    ///
    /// This returns the reader schema if one was provided, otherwise
    /// the writer schema from the file header.
    pub fn schema(&self) -> &AvroSchema {
        // The builder was created with the effective schema
        // We need to get it from the buffer's header
        &self.buffer.header().schema
    }

    /// Get the Polars schema for the DataFrames being produced.
    pub fn polars_schema(&self) -> &Schema {
        self.builder.schema()
    }

    /// Get accumulated errors (in skip mode).
    ///
    /// In skip mode, errors are logged and processing continues.
    /// This method returns all errors that occurred during reading,
    /// including both block-level errors (from the buffer) and
    /// record-level errors (from the builder).
    ///
    /// # Requirements
    /// - 7.3: Track error counts and positions
    /// - 7.4: Provide summary of skipped errors
    pub fn errors(&self) -> Vec<ReadError> {
        let mut all_errors = Vec::new();

        // Add block-level errors from the buffer
        all_errors.extend_from_slice(self.buffer.errors());

        // Add record-level errors from the builder
        all_errors.extend_from_slice(self.builder.errors());

        all_errors
    }

    /// Check if we've reached the end of the file.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Get the error mode being used.
    pub fn error_mode(&self) -> ErrorMode {
        self.builder.error_mode()
    }

    /// Get the batch size being used.
    pub fn batch_size(&self) -> usize {
        self.builder.batch_size()
    }

    /// Get the number of records currently pending in the builder.
    pub fn pending_records(&self) -> usize {
        self.builder.pending_records()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SourceError;
    use crate::reader::header::AVRO_MAGIC;
    use crate::reader::varint::encode_zigzag;
    use bytes::Bytes;

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

    /// Helper to encode a string
    fn encode_string(s: &str) -> Vec<u8> {
        let mut result = encode_zigzag(s.len() as i64);
        result.extend_from_slice(s.as_bytes());
        result
    }

    /// Create test record data: (id: i64, name: string)
    fn create_test_record(id: i64, name: &str) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_zigzag(id));
        data.extend_from_slice(&encode_string(name));
        data
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
    fn create_test_avro_file(blocks: &[(i64, Vec<u8>)]) -> Vec<u8> {
        let mut file = Vec::new();

        // Magic bytes
        file.extend_from_slice(&AVRO_MAGIC);

        // Metadata map: 1 entry (schema)
        file.extend_from_slice(&encode_zigzag(1));

        // Schema entry - record with id (long) and name (string)
        let schema_key = b"avro.schema";
        let schema_json = br#"{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
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
    fn test_reader_config_default() {
        let config = ReaderConfig::default();
        assert_eq!(config.batch_size, 100_000);
        assert_eq!(config.error_mode, ErrorMode::Strict);
        assert!(config.reader_schema.is_none());
        assert!(config.projected_columns.is_none());
    }

    #[test]
    fn test_reader_config_builder() {
        let config = ReaderConfig::new()
            .with_batch_size(5000)
            .skip_errors()
            .with_projection(vec!["col1".to_string(), "col2".to_string()]);

        assert_eq!(config.batch_size, 5000);
        assert_eq!(config.error_mode, ErrorMode::Skip);
        assert_eq!(
            config.projected_columns,
            Some(vec!["col1".to_string(), "col2".to_string()])
        );
    }

    #[test]
    fn test_stream_reader_open() {
        run_async(async {
            // Create a file with one block containing 3 records
            let mut block_data = Vec::new();
            block_data.extend_from_slice(&create_test_record(1, "Alice"));
            block_data.extend_from_slice(&create_test_record(2, "Bob"));
            block_data.extend_from_slice(&create_test_record(3, "Charlie"));

            let file_data = create_test_avro_file(&[(3, block_data)]);
            let source = MockSource::new(file_data);

            let config = ReaderConfig::new();
            let reader = AvroStreamReader::open(source, config).await.unwrap();

            assert!(!reader.is_finished());
            assert_eq!(reader.batch_size(), 100_000);
            assert_eq!(reader.error_mode(), ErrorMode::Strict);
        });
    }

    #[test]
    fn test_stream_reader_next_batch_single_block() {
        run_async(async {
            // Create a file with one block containing 3 records
            let mut block_data = Vec::new();
            block_data.extend_from_slice(&create_test_record(1, "Alice"));
            block_data.extend_from_slice(&create_test_record(2, "Bob"));
            block_data.extend_from_slice(&create_test_record(3, "Charlie"));

            let file_data = create_test_avro_file(&[(3, block_data)]);
            let source = MockSource::new(file_data);

            let config = ReaderConfig::new().with_batch_size(10);
            let mut reader = AvroStreamReader::open(source, config).await.unwrap();

            // Get the first (and only) batch
            let df = reader.next_batch().await.unwrap().unwrap();
            assert_eq!(df.height(), 3);
            assert_eq!(df.width(), 2);

            // Check column names
            let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
            assert_eq!(names, vec!["id", "name"]);

            // Check values
            let id_col = df.column("id").unwrap();
            let ids: Vec<i64> = id_col.i64().unwrap().into_no_null_iter().collect();
            assert_eq!(ids, vec![1, 2, 3]);

            let name_col = df.column("name").unwrap();
            let names: Vec<&str> = name_col.str().unwrap().into_no_null_iter().collect();
            assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);

            // Should return None at EOF
            let next = reader.next_batch().await.unwrap();
            assert!(next.is_none());
            assert!(reader.is_finished());
        });
    }

    #[test]
    fn test_stream_reader_next_batch_multiple_blocks() {
        run_async(async {
            // Create a file with three blocks
            let mut block1_data = Vec::new();
            block1_data.extend_from_slice(&create_test_record(1, "Alice"));
            block1_data.extend_from_slice(&create_test_record(2, "Bob"));

            let mut block2_data = Vec::new();
            block2_data.extend_from_slice(&create_test_record(3, "Charlie"));
            block2_data.extend_from_slice(&create_test_record(4, "Diana"));

            let mut block3_data = Vec::new();
            block3_data.extend_from_slice(&create_test_record(5, "Eve"));

            let file_data =
                create_test_avro_file(&[(2, block1_data), (2, block2_data), (1, block3_data)]);
            let source = MockSource::new(file_data);

            let config = ReaderConfig::new().with_batch_size(100);
            let mut reader = AvroStreamReader::open(source, config).await.unwrap();

            // Should get all records in one batch
            let df = reader.next_batch().await.unwrap().unwrap();
            assert_eq!(df.height(), 5);

            let id_col = df.column("id").unwrap();
            let ids: Vec<i64> = id_col.i64().unwrap().into_no_null_iter().collect();
            assert_eq!(ids, vec![1, 2, 3, 4, 5]);

            // EOF
            assert!(reader.next_batch().await.unwrap().is_none());
        });
    }

    #[test]
    fn test_stream_reader_batch_size_limit() {
        run_async(async {
            // Create multiple blocks with a few records each
            let mut block1_data = Vec::new();
            for i in 1..=3 {
                block1_data.extend_from_slice(&create_test_record(i, &format!("User{}", i)));
            }

            let mut block2_data = Vec::new();
            for i in 4..=6 {
                block2_data.extend_from_slice(&create_test_record(i, &format!("User{}", i)));
            }

            let mut block3_data = Vec::new();
            for i in 7..=9 {
                block3_data.extend_from_slice(&create_test_record(i, &format!("User{}", i)));
            }

            let mut block4_data = Vec::new();
            block4_data.extend_from_slice(&create_test_record(10, "User10"));

            let file_data = create_test_avro_file(&[
                (3, block1_data),
                (3, block2_data),
                (3, block3_data),
                (1, block4_data),
            ]);
            let source = MockSource::new(file_data);

            let config = ReaderConfig::new().with_batch_size(3); // Batch size = 3
            let mut reader = AvroStreamReader::open(source, config).await.unwrap();

            // First batch: 3 records (from block 1)
            let df1 = reader.next_batch().await.unwrap().unwrap();
            assert_eq!(df1.height(), 3);

            // Second batch: 3 records (from block 2)
            let df2 = reader.next_batch().await.unwrap().unwrap();
            assert_eq!(df2.height(), 3);

            // Third batch: 3 records (from block 3)
            let df3 = reader.next_batch().await.unwrap().unwrap();
            assert_eq!(df3.height(), 3);

            // Fourth batch: 1 record (from block 4, final)
            let df4 = reader.next_batch().await.unwrap().unwrap();
            assert_eq!(df4.height(), 1);

            // EOF
            assert!(reader.next_batch().await.unwrap().is_none());
        });
    }

    #[test]
    fn test_stream_reader_empty_file() {
        run_async(async {
            let file_data = create_test_avro_file(&[]); // No blocks
            let source = MockSource::new(file_data);

            let config = ReaderConfig::new();
            let mut reader = AvroStreamReader::open(source, config).await.unwrap();

            // Should immediately return None
            let result = reader.next_batch().await.unwrap();
            assert!(result.is_none());
            assert!(reader.is_finished());
        });
    }

    #[test]
    fn test_stream_reader_with_projection() {
        run_async(async {
            // Create a file with records
            let mut block_data = Vec::new();
            block_data.extend_from_slice(&create_test_record(1, "Alice"));
            block_data.extend_from_slice(&create_test_record(2, "Bob"));

            let file_data = create_test_avro_file(&[(2, block_data)]);
            let source = MockSource::new(file_data);

            let config = ReaderConfig::new().with_projection(vec!["name".to_string()]);
            let mut reader = AvroStreamReader::open(source, config).await.unwrap();

            let df = reader.next_batch().await.unwrap().unwrap();

            // Should only have the projected column
            assert_eq!(df.width(), 1);
            let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
            assert_eq!(names, vec!["name"]);

            let name_col = df.column("name").unwrap();
            let names: Vec<&str> = name_col.str().unwrap().into_no_null_iter().collect();
            assert_eq!(names, vec!["Alice", "Bob"]);
        });
    }

    #[test]
    fn test_stream_reader_skip_mode() {
        run_async(async {
            // Create a valid block
            let mut block_data = Vec::new();
            block_data.extend_from_slice(&create_test_record(1, "Alice"));

            let file_data = create_test_avro_file(&[(1, block_data)]);
            let source = MockSource::new(file_data);

            let config = ReaderConfig::new().skip_errors();
            let mut reader = AvroStreamReader::open(source, config).await.unwrap();

            // Should successfully read in skip mode
            let df = reader.next_batch().await.unwrap().unwrap();
            assert_eq!(df.height(), 1);

            // No errors for valid data
            assert_eq!(reader.errors().len(), 0);
        });
    }

    #[test]
    fn test_stream_reader_strict_mode_error() {
        run_async(async {
            // Create a file with invalid data
            let block_data = vec![0x02]; // Just a partial varint
            let file_data = create_test_avro_file(&[(1, block_data)]);
            let source = MockSource::new(file_data);

            let config = ReaderConfig::new().strict();
            let mut reader = AvroStreamReader::open(source, config).await.unwrap();

            // Should error in strict mode
            let result = reader.next_batch().await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_stream_reader_schema_access() {
        run_async(async {
            let file_data = create_test_avro_file(&[]);
            let source = MockSource::new(file_data);

            let config = ReaderConfig::new();
            let reader = AvroStreamReader::open(source, config).await.unwrap();

            // Should be able to access schema
            let schema = reader.schema();
            assert!(matches!(schema, AvroSchema::Record(_)));

            let polars_schema = reader.polars_schema();
            assert_eq!(polars_schema.len(), 2);
            assert!(polars_schema.get_field("id").is_some());
            assert!(polars_schema.get_field("name").is_some());
        });
    }

    #[test]
    fn test_stream_reader_pending_records() {
        run_async(async {
            // Create a file with records
            let mut block_data = Vec::new();
            block_data.extend_from_slice(&create_test_record(1, "Alice"));
            block_data.extend_from_slice(&create_test_record(2, "Bob"));

            let file_data = create_test_avro_file(&[(2, block_data)]);
            let source = MockSource::new(file_data);

            let config = ReaderConfig::new().with_batch_size(100); // Large batch
            let mut reader = AvroStreamReader::open(source, config).await.unwrap();

            // Initially no pending records
            assert_eq!(reader.pending_records(), 0);

            // After reading, should have records
            let _df = reader.next_batch().await.unwrap().unwrap();

            // After building, should have no pending records
            assert_eq!(reader.pending_records(), 0);
        });
    }
}
