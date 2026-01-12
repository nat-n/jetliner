//! DataFrameBuilder for converting decoded Avro records to Polars DataFrames.
//!
//! This module provides the `DataFrameBuilder` which orchestrates the conversion
//! of Avro records into Polars DataFrames, handling batch size limits and error
//! tracking in skip mode.
//!
//! # Requirements
//! - 5.6: Convert Avro arrays to Polars List columns
//! - 5.7: Convert Avro maps to Polars List columns containing Structs
//! - 5.8: Convert nested Avro records to Polars Struct columns

use crate::error::{DecodeError, ReadError, ReadErrorKind, SchemaError};
use crate::reader::{DecompressedBlock, RecordDecode, RecordDecoder};
use crate::schema::AvroSchema;
use polars::prelude::*;

/// Error handling mode for the DataFrameBuilder.
///
/// Determines how the builder responds to errors during decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ErrorMode {
    /// Fail immediately on any error.
    ///
    /// This is the safest mode and ensures data integrity.
    #[default]
    Strict,
    /// Skip bad records/blocks and continue processing.
    ///
    /// Errors are logged and can be retrieved after processing.
    /// This mode is useful for recovering data from partially corrupted files.
    Skip,
}

/// Configuration for the DataFrameBuilder.
#[derive(Debug, Clone)]
pub struct BuilderConfig {
    /// Target number of rows per DataFrame batch.
    ///
    /// The builder will accumulate records until this limit is reached,
    /// then return a DataFrame. The final batch may contain fewer rows.
    pub batch_size: usize,
    /// Error handling mode.
    pub error_mode: ErrorMode,
}

impl Default for BuilderConfig {
    fn default() -> Self {
        Self {
            batch_size: 100_000,
            error_mode: ErrorMode::Strict,
        }
    }
}

impl BuilderConfig {
    /// Create a new BuilderConfig with the specified batch size.
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            ..Default::default()
        }
    }

    /// Set the error mode.
    pub fn with_error_mode(mut self, mode: ErrorMode) -> Self {
        self.error_mode = mode;
        self
    }

    /// Set strict error mode (fail on first error).
    pub fn strict(mut self) -> Self {
        self.error_mode = ErrorMode::Strict;
        self
    }

    /// Set skip error mode (continue on errors).
    pub fn skip_errors(mut self) -> Self {
        self.error_mode = ErrorMode::Skip;
        self
    }
}

/// Error type for DataFrameBuilder operations.
#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    /// Schema error during builder creation.
    #[error("Schema error: {0}")]
    Schema(#[from] SchemaError),
    /// Decode error during record processing.
    #[error("Decode error: {0}")]
    Decode(#[from] DecodeError),
    /// Polars error during DataFrame creation.
    #[error("Polars error: {0}")]
    Polars(String),
}

impl From<PolarsError> for BuilderError {
    fn from(err: PolarsError) -> Self {
        BuilderError::Polars(err.to_string())
    }
}

/// Builds Polars DataFrames from decoded Avro records.
///
/// The `DataFrameBuilder` orchestrates the conversion of Avro records into
/// Polars DataFrames. It uses a `RecordDecoder` to decode records directly
/// into column builders, then converts those to a DataFrame when the batch
/// size limit is reached.
///
/// # Error Handling
///
/// The builder supports two error modes:
/// - `Strict`: Fails immediately on any error
/// - `Skip`: Logs errors and continues processing
///
/// In skip mode, errors are accumulated and can be retrieved after processing.
///
/// # Requirements
/// - 5.6: Convert Avro arrays to Polars List columns
/// - 5.7: Convert Avro maps to Polars List columns containing Structs
/// - 5.8: Convert nested Avro records to Polars Struct columns
///
/// # Example
/// ```ignore
/// use jetliner::convert::{DataFrameBuilder, BuilderConfig, ErrorMode};
/// use jetliner::schema::AvroSchema;
///
/// let config = BuilderConfig::new(10_000).with_error_mode(ErrorMode::Skip);
/// let mut builder = DataFrameBuilder::new(&schema, config)?;
///
/// // Add blocks
/// builder.add_block(block)?;
///
/// // Build DataFrame when ready
/// if let Some(df) = builder.build(false)? {
///     println!("Got {} rows", df.height());
/// }
///
/// // Check for errors
/// for error in builder.errors() {
///     eprintln!("Error: {}", error);
/// }
/// ```
pub struct DataFrameBuilder {
    /// The record decoder that handles Avro-to-Arrow conversion.
    decoder: RecordDecoder,
    /// Target batch size (rows per DataFrame).
    batch_size: usize,
    /// Error handling mode.
    error_mode: ErrorMode,
    /// Accumulated errors (in skip mode).
    errors: Vec<ReadError>,
    /// Current block index being processed.
    current_block_index: usize,
}

impl DataFrameBuilder {
    /// Create a new DataFrameBuilder for the given schema.
    ///
    /// # Arguments
    /// * `schema` - The Avro schema (must be a Record type)
    /// * `config` - Builder configuration
    ///
    /// # Returns
    /// A new DataFrameBuilder, or an error if the schema is invalid.
    pub fn new(schema: &AvroSchema, config: BuilderConfig) -> Result<Self, BuilderError> {
        let decoder = RecordDecoder::new(schema, None)?;
        Ok(Self {
            decoder,
            batch_size: config.batch_size,
            error_mode: config.error_mode,
            errors: Vec::new(),
            current_block_index: 0,
        })
    }

    /// Create a new DataFrameBuilder with column projection.
    ///
    /// Only the specified columns will be decoded and included in the
    /// resulting DataFrames. This can significantly reduce memory usage
    /// and improve performance when only a subset of columns is needed.
    ///
    /// # Arguments
    /// * `schema` - The Avro schema (must be a Record type)
    /// * `config` - Builder configuration
    /// * `columns` - List of column names to project
    ///
    /// # Returns
    /// A new DataFrameBuilder with projection, or an error if the schema is invalid.
    pub fn with_projection(
        schema: &AvroSchema,
        config: BuilderConfig,
        columns: &[String],
    ) -> Result<Self, BuilderError> {
        let decoder = RecordDecoder::new(schema, Some(columns))?;
        Ok(Self {
            decoder,
            batch_size: config.batch_size,
            error_mode: config.error_mode,
            errors: Vec::new(),
            current_block_index: 0,
        })
    }

    /// Add a decompressed block's records to the builder.
    ///
    /// This method decodes all records in the block and adds them to the
    /// internal column builders. In skip mode, decode errors are logged
    /// and processing continues with the next record.
    ///
    /// # Arguments
    /// * `block` - The decompressed block to process
    ///
    /// # Returns
    /// `Ok(())` on success, or an error in strict mode if decoding fails.
    ///
    /// # Errors
    /// In strict mode, returns an error on the first decode failure.
    /// In skip mode, errors are accumulated and can be retrieved via `errors()`.
    pub fn add_block(&mut self, block: DecompressedBlock) -> Result<(), BuilderError> {
        self.current_block_index = block.block_index;
        let mut data = block.data.as_ref();
        let record_count = block.record_count as usize;

        // Pre-allocate capacity for this block's records
        // This reduces reallocations during the decode loop
        self.decoder.reserve_for_batch(record_count);

        for record_index in 0..record_count {
            match self.decoder.decode_record(&mut data) {
                Ok(()) => {}
                Err(e) => {
                    match self.error_mode {
                        ErrorMode::Strict => {
                            return Err(BuilderError::Decode(e));
                        }
                        ErrorMode::Skip => {
                            // Log the error and continue
                            self.errors.push(ReadError::new(
                                ReadErrorKind::RecordDecodeFailed,
                                block.block_index,
                                Some(record_index),
                                0, // We don't track exact byte offset here
                                e.to_string(),
                            ));
                            // In skip mode, we can't easily skip to the next record
                            // without knowing the record boundaries, so we stop
                            // processing this block
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Build a DataFrame from accumulated records.
    ///
    /// If `force` is false, only builds a DataFrame if the batch size limit
    /// has been reached. If `force` is true, builds a DataFrame with whatever
    /// records are currently accumulated (useful for the final batch).
    ///
    /// # Arguments
    /// * `force` - If true, build even if batch size hasn't been reached
    ///
    /// # Returns
    /// `Some(DataFrame)` if records were available, `None` if no records.
    pub fn build(&mut self, force: bool) -> Result<Option<DataFrame>, BuilderError> {
        let pending = self.decoder.pending_records();

        // Check if we should build
        if pending == 0 {
            return Ok(None);
        }

        if !force && pending < self.batch_size {
            return Ok(None);
        }

        // Finish the batch and get Series
        let series = self.decoder.finish_batch()?;

        if series.is_empty() {
            return Ok(None);
        }

        // Convert Series to Columns for DataFrame creation
        let columns: Vec<Column> = series.into_iter().map(Column::from).collect();

        // Create DataFrame from Columns
        let df = DataFrame::new(columns)?;

        Ok(Some(df))
    }

    /// Force build a DataFrame with all accumulated records.
    ///
    /// This is equivalent to `build(true)` and is useful for getting
    /// the final batch of records.
    pub fn finish(&mut self) -> Result<Option<DataFrame>, BuilderError> {
        self.build(true)
    }

    /// Get the number of records currently pending in the builder.
    pub fn pending_records(&self) -> usize {
        self.decoder.pending_records()
    }

    /// Check if the batch size limit has been reached.
    pub fn is_batch_ready(&self) -> bool {
        self.decoder.pending_records() >= self.batch_size
    }

    /// Get the accumulated errors (in skip mode).
    pub fn errors(&self) -> &[ReadError] {
        &self.errors
    }

    /// Clear accumulated errors.
    pub fn clear_errors(&mut self) {
        self.errors.clear();
    }

    /// Get the error mode.
    pub fn error_mode(&self) -> ErrorMode {
        self.error_mode
    }

    /// Get the batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Get the schema for the DataFrames being built.
    pub fn schema(&self) -> &Schema {
        self.decoder.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldSchema, RecordSchema};
    use bytes::Bytes;

    /// Helper to create a simple test schema
    fn create_test_schema() -> AvroSchema {
        AvroSchema::Record(RecordSchema::new(
            "TestRecord",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        ))
    }

    /// Helper to encode a zigzag varint
    fn encode_zigzag(value: i64) -> Vec<u8> {
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;
        encode_varint(zigzag)
    }

    /// Helper to encode a varint
    fn encode_varint(mut value: u64) -> Vec<u8> {
        let mut result = Vec::new();
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            result.push(byte);
            if value == 0 {
                break;
            }
        }
        result
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

    #[test]
    fn test_builder_config_default() {
        let config = BuilderConfig::default();
        assert_eq!(config.batch_size, 100_000);
        assert_eq!(config.error_mode, ErrorMode::Strict);
    }

    #[test]
    fn test_builder_config_custom() {
        let config = BuilderConfig::new(5000).skip_errors();
        assert_eq!(config.batch_size, 5000);
        assert_eq!(config.error_mode, ErrorMode::Skip);
    }

    #[test]
    fn test_builder_new() {
        let schema = create_test_schema();
        let config = BuilderConfig::new(100);
        let builder = DataFrameBuilder::new(&schema, config).unwrap();

        assert_eq!(builder.batch_size(), 100);
        assert_eq!(builder.error_mode(), ErrorMode::Strict);
        assert_eq!(builder.pending_records(), 0);
        assert!(builder.errors().is_empty());
    }

    #[test]
    fn test_builder_add_block_and_build() {
        let schema = create_test_schema();
        let config = BuilderConfig::new(10);
        let mut builder = DataFrameBuilder::new(&schema, config).unwrap();

        // Create block data with 3 records
        let mut block_data = Vec::new();
        block_data.extend_from_slice(&create_test_record(1, "Alice"));
        block_data.extend_from_slice(&create_test_record(2, "Bob"));
        block_data.extend_from_slice(&create_test_record(3, "Charlie"));

        let block = DecompressedBlock::new(3, Bytes::from(block_data), 0);

        builder.add_block(block).unwrap();
        assert_eq!(builder.pending_records(), 3);

        // Force build since we haven't reached batch size
        let df = builder.finish().unwrap().unwrap();

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
    }

    #[test]
    fn test_builder_batch_size_limit() {
        let schema = create_test_schema();
        let config = BuilderConfig::new(2); // Small batch size
        let mut builder = DataFrameBuilder::new(&schema, config).unwrap();

        // Create block data with 3 records
        let mut block_data = Vec::new();
        block_data.extend_from_slice(&create_test_record(1, "Alice"));
        block_data.extend_from_slice(&create_test_record(2, "Bob"));
        block_data.extend_from_slice(&create_test_record(3, "Charlie"));

        let block = DecompressedBlock::new(3, Bytes::from(block_data), 0);

        builder.add_block(block).unwrap();
        assert_eq!(builder.pending_records(), 3);
        assert!(builder.is_batch_ready()); // 3 >= 2

        // Build without force - should work since we exceeded batch size
        let df = builder.build(false).unwrap().unwrap();
        assert_eq!(df.height(), 3);
    }

    #[test]
    fn test_builder_build_not_ready() {
        let schema = create_test_schema();
        let config = BuilderConfig::new(100); // Large batch size
        let mut builder = DataFrameBuilder::new(&schema, config).unwrap();

        // Create block data with 3 records
        let mut block_data = Vec::new();
        block_data.extend_from_slice(&create_test_record(1, "Alice"));
        block_data.extend_from_slice(&create_test_record(2, "Bob"));
        block_data.extend_from_slice(&create_test_record(3, "Charlie"));

        let block = DecompressedBlock::new(3, Bytes::from(block_data), 0);

        builder.add_block(block).unwrap();
        assert!(!builder.is_batch_ready()); // 3 < 100

        // Build without force - should return None
        let result = builder.build(false).unwrap();
        assert!(result.is_none());

        // Records should still be pending
        assert_eq!(builder.pending_records(), 3);

        // Force build should work
        let df = builder.finish().unwrap().unwrap();
        assert_eq!(df.height(), 3);
    }

    #[test]
    fn test_builder_empty_build() {
        let schema = create_test_schema();
        let config = BuilderConfig::new(100);
        let mut builder = DataFrameBuilder::new(&schema, config).unwrap();

        // Build with no records
        let result = builder.build(true).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_builder_multiple_blocks() {
        let schema = create_test_schema();
        let config = BuilderConfig::new(100);
        let mut builder = DataFrameBuilder::new(&schema, config).unwrap();

        // Add first block
        let mut block1_data = Vec::new();
        block1_data.extend_from_slice(&create_test_record(1, "Alice"));
        block1_data.extend_from_slice(&create_test_record(2, "Bob"));
        let block1 = DecompressedBlock::new(2, Bytes::from(block1_data), 0);
        builder.add_block(block1).unwrap();

        // Add second block
        let mut block2_data = Vec::new();
        block2_data.extend_from_slice(&create_test_record(3, "Charlie"));
        block2_data.extend_from_slice(&create_test_record(4, "Diana"));
        let block2 = DecompressedBlock::new(2, Bytes::from(block2_data), 1);
        builder.add_block(block2).unwrap();

        assert_eq!(builder.pending_records(), 4);

        let df = builder.finish().unwrap().unwrap();
        assert_eq!(df.height(), 4);

        let id_col = df.column("id").unwrap();
        let ids: Vec<i64> = id_col.i64().unwrap().into_no_null_iter().collect();
        assert_eq!(ids, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_builder_with_projection() {
        let schema = create_test_schema();
        let config = BuilderConfig::new(100);
        let columns = vec!["name".to_string()]; // Only project 'name' column
        let mut builder = DataFrameBuilder::with_projection(&schema, config, &columns).unwrap();

        // Create block data with 2 records
        let mut block_data = Vec::new();
        block_data.extend_from_slice(&create_test_record(1, "Alice"));
        block_data.extend_from_slice(&create_test_record(2, "Bob"));

        let block = DecompressedBlock::new(2, Bytes::from(block_data), 0);

        builder.add_block(block).unwrap();
        let df = builder.finish().unwrap().unwrap();

        // Should only have the projected column
        assert_eq!(df.width(), 1);
        let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        assert_eq!(names, vec!["name"]);

        let name_col = df.column("name").unwrap();
        let names: Vec<&str> = name_col.str().unwrap().into_no_null_iter().collect();
        assert_eq!(names, vec!["Alice", "Bob"]);
    }

    #[test]
    fn test_builder_skip_mode_error_tracking() {
        let schema = create_test_schema();
        let config = BuilderConfig::new(100).skip_errors();
        let mut builder = DataFrameBuilder::new(&schema, config).unwrap();

        // Create invalid block data (truncated)
        let block_data = vec![0x02]; // Just a partial varint
        let block = DecompressedBlock::new(1, Bytes::from(block_data), 5);

        // Should not error in skip mode
        builder.add_block(block).unwrap();

        // Should have recorded an error
        assert_eq!(builder.errors().len(), 1);
        let error = &builder.errors()[0];
        assert_eq!(error.kind, ReadErrorKind::RecordDecodeFailed);
        assert_eq!(error.block_index, 5);
        assert_eq!(error.record_index, Some(0));

        // Clear errors
        builder.clear_errors();
        assert!(builder.errors().is_empty());
    }

    #[test]
    fn test_builder_strict_mode_error() {
        let schema = create_test_schema();
        let config = BuilderConfig::new(100).strict();
        let mut builder = DataFrameBuilder::new(&schema, config).unwrap();

        // Create invalid block data (truncated)
        let block_data = vec![0x02]; // Just a partial varint
        let block = DecompressedBlock::new(1, Bytes::from(block_data), 0);

        // Should error in strict mode
        let result = builder.add_block(block);
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_schema_access() {
        let schema = create_test_schema();
        let config = BuilderConfig::new(100);
        let builder = DataFrameBuilder::new(&schema, config).unwrap();

        let polars_schema = builder.schema();
        assert_eq!(polars_schema.len(), 2);
        assert!(polars_schema.get_field("id").is_some());
        assert!(polars_schema.get_field("name").is_some());
    }
}
