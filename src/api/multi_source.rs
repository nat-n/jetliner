//! Multi-source reader for reading from multiple Avro files.
//!
//! This module provides `AvroMultiStreamReader` for orchestrating reading from
//! multiple Avro files with support for row indexing, file path injection,
//! and error accumulation.
//!
//! # Requirements
//! - 2.4: Support glob patterns for both local and S3 sources
//! - 2.7: When multiple files are provided, read them in sequence
//! - 4.3: When `n_rows` is specified, stop reading after that many rows
//! - 4.5: When reading multiple files, the `n_rows` limit applies to the total across all files
//! - 4.6: Row limiting logic is implemented in Rust
//! - 5.6: When reading multiple files, the row index is continuous across files
//! - 12.2: When `ignore_errors=False`, fail immediately on any decode/corruption error
//! - 12.3: When `ignore_errors=True`, skip bad records/blocks and continue
//! - 12.4: Accumulated errors are accessible via the reader's `errors` property

use std::sync::Arc;

use polars::prelude::DataFrame;

use crate::convert::avro_to_arrow_schema;
use crate::error::{BadBlockError, ReaderError};
use crate::reader::{AvroStreamReader, ReaderConfig};
use crate::schema::AvroSchema;
use crate::source::{BoxedSource, LocalSource, S3Config, S3Source};

use super::args::IdxSize;
use super::file_path::FilePathInjector;
use super::glob::is_s3_uri;
use super::row_index::RowIndexTracker;
use super::sources::ResolvedSources;

/// Configuration for multi-source reading.
#[derive(Debug, Clone, Default)]
pub struct MultiSourceConfig {
    /// Reader configuration for individual files.
    pub reader_config: ReaderConfig,
    /// Maximum number of rows to read across all files.
    pub n_rows: Option<usize>,
    /// Row index configuration.
    pub row_index: Option<(Arc<str>, IdxSize)>,
    /// File path column name.
    pub include_file_paths: Option<Arc<str>>,
    /// Whether to ignore errors.
    pub ignore_errors: bool,
    /// S3 configuration for cloud access.
    pub s3_config: Option<S3Config>,
}

impl MultiSourceConfig {
    /// Create a new `MultiSourceConfig` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the reader configuration.
    pub fn with_reader_config(mut self, config: ReaderConfig) -> Self {
        self.reader_config = config;
        self
    }

    /// Set the maximum number of rows to read.
    pub fn with_n_rows(mut self, n_rows: usize) -> Self {
        self.n_rows = Some(n_rows);
        self
    }

    /// Set the row index configuration.
    pub fn with_row_index(mut self, name: impl Into<Arc<str>>, offset: IdxSize) -> Self {
        self.row_index = Some((name.into(), offset));
        self
    }

    /// Set the file path column name.
    pub fn with_include_file_paths(mut self, column_name: impl Into<Arc<str>>) -> Self {
        self.include_file_paths = Some(column_name.into());
        self
    }

    /// Set whether to ignore errors.
    pub fn with_ignore_errors(mut self, ignore_errors: bool) -> Self {
        self.ignore_errors = ignore_errors;
        self
    }

    /// Set S3 configuration for cloud access.
    pub fn with_s3_config(mut self, s3_config: S3Config) -> Self {
        self.s3_config = Some(s3_config);
        self
    }
}

/// Multi-source reader for reading from multiple Avro files.
///
/// This reader orchestrates reading from multiple files, handling:
/// - Sequential file reading
/// - Row index continuity across files
/// - File path injection
/// - Error accumulation in ignore_errors mode
/// - n_rows limit across all files
/// - Both local files and S3 sources
///
/// # Requirements
/// - 2.4: Support glob patterns for both local and S3 sources
/// - 2.7: Read files in sequence
/// - 4.5: n_rows limit applies to total across all files
/// - 5.6: Row index is continuous across files
/// - 12.4: Accumulate errors in ignore_errors mode
pub struct AvroMultiStreamReader {
    /// Resolved sources (paths and unified schema).
    sources: ResolvedSources,
    /// Configuration.
    config: MultiSourceConfig,
    /// Current source index.
    current_source_index: usize,
    /// Current reader (if any) - uses BoxedSource for both local and S3.
    current_reader: Option<AvroStreamReader<BoxedSource>>,
    /// Row index tracker (if enabled).
    row_index_tracker: Option<RowIndexTracker>,
    /// File path injector (if enabled).
    file_path_injector: Option<FilePathInjector>,
    /// Total rows read so far.
    rows_read: usize,
    /// Whether we've finished reading all sources.
    finished: bool,
    /// Accumulated errors (in ignore_errors mode).
    accumulated_errors: Vec<BadBlockError>,
    /// Whether we've returned any DataFrame (for empty file handling).
    returned_any_df: bool,
}

impl AvroMultiStreamReader {
    /// Create a new `AvroMultiStreamReader` from resolved sources.
    ///
    /// # Arguments
    /// * `sources` - Resolved sources (paths and unified schema)
    /// * `config` - Multi-source configuration
    ///
    /// # Returns
    /// A new `AvroMultiStreamReader` ready to read from the sources.
    pub fn new(sources: ResolvedSources, config: MultiSourceConfig) -> Self {
        // Initialize row index tracker if configured
        let row_index_tracker = config
            .row_index
            .as_ref()
            .map(|(name, offset)| RowIndexTracker::new(name.clone(), *offset));

        // Initialize file path injector if configured
        let file_path_injector = config
            .include_file_paths
            .as_ref()
            .map(|name| FilePathInjector::new(name.clone()));

        Self {
            sources,
            config,
            current_source_index: 0,
            current_reader: None,
            row_index_tracker,
            file_path_injector,
            rows_read: 0,
            finished: false,
            accumulated_errors: Vec::new(),
            returned_any_df: false,
        }
    }

    /// Get the next batch of records as a DataFrame.
    ///
    /// This method:
    /// 1. Opens the next source file if needed
    /// 2. Reads batches from the current file
    /// 3. Applies row index and file path injection
    /// 4. Respects n_rows limit
    ///
    /// Returns `None` when all records have been read or n_rows limit is reached.
    ///
    /// # Requirements
    /// - 2.7: Read files in sequence
    /// - 4.5: n_rows limit applies to total
    /// - 5.6: Row index is continuous
    pub async fn next_batch(&mut self) -> Result<Option<DataFrame>, ReaderError> {
        // Check if we've finished
        if self.finished {
            return Ok(None);
        }

        // Check if we've reached n_rows limit
        if let Some(n_rows) = self.config.n_rows {
            if self.rows_read >= n_rows {
                self.finished = true;
                return Ok(None);
            }
        }

        loop {
            // Ensure we have a reader
            if self.current_reader.is_none() && !self.open_next_source().await? {
                // No more sources
                self.finished = true;

                // If we never returned any DataFrame, return one empty DataFrame
                // with the correct schema and transformations applied.
                // This ensures row_index and include_file_paths columns are present
                // even for empty files, matching Polars behavior.
                if !self.returned_any_df {
                    self.returned_any_df = true;
                    let empty_df = self.create_empty_dataframe_with_schema()?;
                    let df = self.apply_transformations(empty_df)?;
                    return Ok(Some(df));
                }
                return Ok(None);
            }

            // Get the next batch from the current reader
            let reader = self.current_reader.as_mut().unwrap();
            match reader.next_batch().await {
                Ok(Some(mut df)) => {
                    // Apply n_rows limit if needed
                    if let Some(n_rows) = self.config.n_rows {
                        let remaining = n_rows.saturating_sub(self.rows_read);
                        if df.height() > remaining {
                            df = df.head(Some(remaining));
                        }
                    }

                    // Update rows read
                    self.rows_read += df.height();

                    // Apply transformations
                    df = self.apply_transformations(df)?;

                    // Track that we've returned a DataFrame
                    self.returned_any_df = true;

                    return Ok(Some(df));
                }
                Ok(None) => {
                    // Current source exhausted, collect errors and move to next
                    self.collect_reader_errors();
                    self.current_reader = None;
                    // Continue loop to open next source
                }
                Err(e) => {
                    if self.config.ignore_errors {
                        // In ignore_errors mode, log the error and try next source
                        // current_source_index was incremented after opening, so -1 to get current
                        let current_path = self
                            .sources
                            .paths
                            .get(self.current_source_index.saturating_sub(1))
                            .cloned()
                            .unwrap_or_default();
                        self.accumulated_errors.push(
                            BadBlockError::new(
                                crate::error::BadBlockErrorKind::BlockParseFailed,
                                0,
                                None,
                                0,
                                e.to_string(),
                            )
                            .with_file_path(current_path),
                        );
                        self.current_reader = None;
                        // Continue loop to open next source
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Open the next source file.
    ///
    /// Returns `true` if a source was opened, `false` if no more sources.
    async fn open_next_source(&mut self) -> Result<bool, ReaderError> {
        while self.current_source_index < self.sources.paths.len() {
            let path = &self.sources.paths[self.current_source_index];
            self.current_source_index += 1;

            // Update file path injector
            if let Some(ref mut injector) = self.file_path_injector {
                injector.set_path(path.as_str());
            }

            // Open the source
            match self.open_source(path).await {
                Ok(reader) => {
                    self.current_reader = Some(reader);
                    return Ok(true);
                }
                Err(e) => {
                    if self.config.ignore_errors {
                        // In ignore_errors mode, log the error and try next source
                        self.accumulated_errors.push(
                            BadBlockError::new(
                                crate::error::BadBlockErrorKind::BlockParseFailed,
                                0,
                                None,
                                0,
                                format!("Failed to open '{}': {}", path, e),
                            )
                            .with_file_path(path),
                        );
                        // Continue to next source
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Open a single source file.
    ///
    /// Supports both local files and S3 URIs. For S3 sources, uses the
    /// S3 config from the configuration.
    ///
    /// # Requirements
    /// - 2.4: Support both local files and S3 sources
    async fn open_source(&self, path: &str) -> Result<AvroStreamReader<BoxedSource>, ReaderError> {
        let source: BoxedSource = if is_s3_uri(path) {
            // Open S3 source
            let s3_source =
                S3Source::from_uri_with_config(path, self.config.s3_config.clone()).await?;
            Box::new(s3_source)
        } else {
            // Open local source
            let local_source = LocalSource::open(path).await?;
            Box::new(local_source)
        };

        let reader = AvroStreamReader::open(source, self.config.reader_config.clone()).await?;
        Ok(reader)
    }

    /// Get the column mapping for the current file (if any).
    ///
    /// Returns None if no reordering is needed (identical schema or first file).
    fn get_current_column_mapping(&self) -> Option<&Vec<String>> {
        // current_source_index was incremented after opening, so -1 to get current
        let idx = self.current_source_index.saturating_sub(1);
        self.sources
            .column_mappings
            .get(idx)
            .and_then(|m| m.as_ref())
    }

    /// Apply transformations to a DataFrame.
    ///
    /// This applies:
    /// 1. Column reordering (if schema field order differs)
    /// 2. Row index injection
    /// 3. File path injection
    fn apply_transformations(&mut self, mut df: DataFrame) -> Result<DataFrame, ReaderError> {
        // Step 0: Reorder columns if needed (zero-copy, just reorders Arc<Series> pointers)
        if let Some(column_order) = self.get_current_column_mapping() {
            df = df
                .select(column_order.iter().map(|s| s.as_str()))
                .map_err(|e| ReaderError::Builder(e.into()))?;
        }

        // Apply row index
        if let Some(ref mut tracker) = self.row_index_tracker {
            df = tracker
                .add_to_dataframe(df)
                .map_err(|e| ReaderError::Builder(e.into()))?;
        }

        // Apply file path
        if let Some(ref injector) = self.file_path_injector {
            df = injector
                .add_to_dataframe(df)
                .map_err(|e| ReaderError::Builder(e.into()))?;
        }

        Ok(df)
    }

    /// Create an empty DataFrame with the correct schema.
    ///
    /// This is used when all sources are empty to ensure we return a DataFrame
    /// with the correct column names and types.
    fn create_empty_dataframe_with_schema(&self) -> Result<DataFrame, ReaderError> {
        let polars_schema = avro_to_arrow_schema(&self.sources.schema)?;
        Ok(DataFrame::empty_with_schema(&polars_schema))
    }

    /// Collect errors from the current reader.
    fn collect_reader_errors(&mut self) {
        if let Some(ref reader) = self.current_reader {
            // Get the current file path to inject into errors
            // current_source_index was incremented after opening, so -1 to get current file
            let current_path = self
                .sources
                .paths
                .get(self.current_source_index.saturating_sub(1))
                .cloned()
                .unwrap_or_default();

            // Clone errors and inject file path
            self.accumulated_errors.extend(
                reader
                    .errors()
                    .iter()
                    .map(|e| e.clone().with_file_path(&current_path)),
            );
        }
    }

    /// Get accumulated errors.
    ///
    /// In ignore_errors mode, this returns all errors that occurred during reading.
    ///
    /// # Requirements
    /// - 12.4: Accumulated errors are accessible via the reader's `errors` property
    pub fn errors(&self) -> &[BadBlockError] {
        &self.accumulated_errors
    }

    /// Get the unified schema.
    pub fn schema(&self) -> &AvroSchema {
        &self.sources.schema
    }

    /// Get the number of rows read so far.
    pub fn rows_read(&self) -> usize {
        self.rows_read
    }

    /// Check if reading is finished.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Get the current source index.
    pub fn current_source_index(&self) -> usize {
        self.current_source_index
    }

    /// Get the total number of sources.
    pub fn total_sources(&self) -> usize {
        self.sources.paths.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldSchema, RecordSchema};

    fn create_test_record_schema(name: &str, fields: &[&str]) -> AvroSchema {
        let field_schemas: Vec<FieldSchema> = fields
            .iter()
            .map(|f| FieldSchema::new(*f, AvroSchema::String))
            .collect();
        AvroSchema::Record(RecordSchema::new(name, field_schemas))
    }

    #[test]
    fn test_multi_source_config_default() {
        let config = MultiSourceConfig::default();
        assert!(config.n_rows.is_none());
        assert!(config.row_index.is_none());
        assert!(config.include_file_paths.is_none());
        assert!(!config.ignore_errors);
    }

    #[test]
    fn test_multi_source_config_builder() {
        let config = MultiSourceConfig::new()
            .with_n_rows(1000)
            .with_row_index("idx", 0)
            .with_include_file_paths("source_file")
            .with_ignore_errors(true);

        assert_eq!(config.n_rows, Some(1000));
        assert!(config.row_index.is_some());
        let (name, offset) = config.row_index.unwrap();
        assert_eq!(&*name, "idx");
        assert_eq!(offset, 0);
        assert_eq!(&*config.include_file_paths.unwrap(), "source_file");
        assert!(config.ignore_errors);
    }

    #[test]
    fn test_multi_source_reader_new() {
        let schema = create_test_record_schema("Test", &["id", "name"]);
        let sources = ResolvedSources::new(
            vec!["file1.avro".to_string(), "file2.avro".to_string()],
            schema,
        );
        let config = MultiSourceConfig::new()
            .with_row_index("idx", 0)
            .with_include_file_paths("source");

        let reader = AvroMultiStreamReader::new(sources, config);

        assert_eq!(reader.current_source_index(), 0);
        assert_eq!(reader.total_sources(), 2);
        assert_eq!(reader.rows_read(), 0);
        assert!(!reader.is_finished());
        assert!(reader.row_index_tracker.is_some());
        assert!(reader.file_path_injector.is_some());
    }

    #[test]
    fn test_multi_source_reader_without_options() {
        let schema = create_test_record_schema("Test", &["id"]);
        let sources = ResolvedSources::new(vec!["file.avro".to_string()], schema);
        let config = MultiSourceConfig::new();

        let reader = AvroMultiStreamReader::new(sources, config);

        assert!(reader.row_index_tracker.is_none());
        assert!(reader.file_path_injector.is_none());
    }

    #[test]
    fn test_multi_source_reader_errors_empty() {
        let schema = create_test_record_schema("Test", &["id"]);
        let sources = ResolvedSources::new(vec!["file.avro".to_string()], schema);
        let config = MultiSourceConfig::new();

        let reader = AvroMultiStreamReader::new(sources, config);

        assert!(reader.errors().is_empty());
    }

    #[test]
    fn test_multi_source_config_with_reader_config() {
        let reader_config = ReaderConfig::new().with_batch_size(5000);
        let config = MultiSourceConfig::new().with_reader_config(reader_config);

        assert_eq!(config.reader_config.batch_size, 5000);
    }

    #[test]
    fn test_multi_source_reader_state_initial() {
        let schema = create_test_record_schema("Test", &["id"]);
        let sources = ResolvedSources::new(
            vec![
                "file1.avro".to_string(),
                "file2.avro".to_string(),
                "file3.avro".to_string(),
            ],
            schema,
        );
        let config = MultiSourceConfig::new();

        let reader = AvroMultiStreamReader::new(sources, config);

        // Initial state checks
        assert_eq!(reader.current_source_index(), 0);
        assert_eq!(reader.total_sources(), 3);
        assert_eq!(reader.rows_read(), 0);
        assert!(!reader.is_finished());
        assert!(reader.errors().is_empty());
    }

    #[test]
    fn test_multi_source_reader_with_n_rows_config() {
        let schema = create_test_record_schema("Test", &["id"]);
        let sources = ResolvedSources::new(vec!["file.avro".to_string()], schema);
        let config = MultiSourceConfig::new().with_n_rows(100);

        let reader = AvroMultiStreamReader::new(sources, config);

        assert_eq!(reader.config.n_rows, Some(100));
    }

    #[test]
    fn test_multi_source_reader_schema_access() {
        let schema = create_test_record_schema("TestRecord", &["id", "name", "value"]);
        let sources = ResolvedSources::new(vec!["file.avro".to_string()], schema.clone());
        let config = MultiSourceConfig::new();

        let reader = AvroMultiStreamReader::new(sources, config);

        // Should be able to access the unified schema
        let reader_schema = reader.schema();
        match reader_schema {
            AvroSchema::Record(record) => {
                assert_eq!(record.name, "TestRecord");
                assert_eq!(record.fields.len(), 3);
            }
            _ => panic!("Expected Record schema"),
        }
    }

    #[test]
    fn test_multi_source_reader_with_all_options() {
        let schema = create_test_record_schema("Test", &["id"]);
        let sources = ResolvedSources::new(vec!["file.avro".to_string()], schema);
        let config = MultiSourceConfig::new()
            .with_n_rows(500)
            .with_row_index("row_nr", 10)
            .with_include_file_paths("source_file")
            .with_ignore_errors(true);

        let reader = AvroMultiStreamReader::new(sources, config);

        // Verify all options are set
        assert!(reader.row_index_tracker.is_some());
        assert!(reader.file_path_injector.is_some());
        assert_eq!(reader.config.n_rows, Some(500));
        assert!(reader.config.ignore_errors);

        // Check row index tracker initial state
        let tracker = reader.row_index_tracker.as_ref().unwrap();
        assert_eq!(tracker.name(), "row_nr");
        assert_eq!(tracker.current_offset(), 10);

        // Check file path injector
        let injector = reader.file_path_injector.as_ref().unwrap();
        assert_eq!(injector.column_name(), "source_file");
    }

    #[tokio::test]
    async fn test_multi_source_reader_next_batch_no_files() {
        let schema = create_test_record_schema("Test", &["id"]);
        let sources = ResolvedSources::new(vec![], schema);
        let config = MultiSourceConfig::new();

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Should return one empty DataFrame with correct schema
        let result = reader.next_batch().await.unwrap();
        assert!(result.is_some());
        let df = result.unwrap();
        assert_eq!(df.height(), 0);
        assert!(df.column("id").is_ok());
        assert!(reader.is_finished());

        // Second call should return None
        let result2 = reader.next_batch().await.unwrap();
        assert!(result2.is_none());
    }

    #[tokio::test]
    async fn test_multi_source_reader_with_nonexistent_file() {
        let schema = create_test_record_schema("Test", &["id"]);
        let sources = ResolvedSources::new(vec!["nonexistent_file_12345.avro".to_string()], schema);
        let config = MultiSourceConfig::new();

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Should error for nonexistent file in strict mode
        let result = reader.next_batch().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multi_source_reader_ignore_errors_nonexistent_file() {
        let schema = create_test_record_schema("Test", &["id"]);
        let sources = ResolvedSources::new(vec!["nonexistent_file_12345.avro".to_string()], schema);
        let config = MultiSourceConfig::new().with_ignore_errors(true);

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Should return one empty DataFrame with correct schema (no data) and accumulate error
        let result = reader.next_batch().await.unwrap();
        assert!(result.is_some());
        let df = result.unwrap();
        assert_eq!(df.height(), 0);
        assert!(df.column("id").is_ok());
        assert!(reader.is_finished());
        // Should have accumulated an error
        assert!(!reader.errors().is_empty());

        // Second call should return None
        let result2 = reader.next_batch().await.unwrap();
        assert!(result2.is_none());
    }

    // =========================================================================
    // Integration tests with real Avro files
    // =========================================================================

    /// Helper to read schema from a real file for testing
    async fn read_schema_from_test_file(path: &str) -> AvroSchema {
        use crate::reader::AvroHeader;
        use crate::source::StreamSource;

        let source = LocalSource::open(path).await.unwrap();
        let header_bytes = source.read_range(0, 64 * 1024).await.unwrap();
        let header = AvroHeader::parse(&header_bytes).unwrap();
        header.schema
    }

    #[tokio::test]
    async fn test_multi_source_reader_single_file_read() {
        // Read schema from the actual file
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec!["tests/data/apache-avro/weather.avro".to_string()],
            schema,
        );
        let config = MultiSourceConfig::new();

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Read all batches
        let mut total_rows = 0;
        while let Some(df) = reader.next_batch().await.unwrap() {
            total_rows += df.height();
        }

        assert!(total_rows > 0, "Should have read some rows");
        assert!(reader.is_finished());
        assert_eq!(reader.rows_read(), total_rows);
    }

    #[tokio::test]
    async fn test_multi_source_reader_state_transitions() {
        // Test reading from multiple files and verify state transitions
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec![
                "tests/data/apache-avro/weather.avro".to_string(),
                "tests/data/apache-avro/weather-deflate.avro".to_string(),
            ],
            schema,
        );
        let config = MultiSourceConfig::new();

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Initial state
        assert_eq!(reader.current_source_index(), 0);
        assert!(!reader.is_finished());

        // Read first batch - should open first file
        let df1 = reader.next_batch().await.unwrap();
        assert!(df1.is_some());
        // After opening first file, index should be 1
        assert!(reader.current_source_index() >= 1);

        // Read all remaining batches
        while let Some(_df) = reader.next_batch().await.unwrap() {
            // Continue reading
        }

        // Final state - should have processed both files
        assert!(reader.is_finished());
        assert_eq!(reader.current_source_index(), 2);
    }

    #[tokio::test]
    async fn test_multi_source_reader_n_rows_limit_single_file() {
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec!["tests/data/apache-avro/weather.avro".to_string()],
            schema,
        );
        let config = MultiSourceConfig::new().with_n_rows(3);

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Read all batches
        let mut total_rows = 0;
        while let Some(df) = reader.next_batch().await.unwrap() {
            total_rows += df.height();
        }

        // Should have read at most 3 rows
        assert!(total_rows <= 3, "Should have read at most 3 rows");
        assert!(reader.is_finished());
        assert_eq!(reader.rows_read(), total_rows);
    }

    #[tokio::test]
    async fn test_multi_source_reader_n_rows_limit_across_files() {
        // Test that n_rows limit applies to total across all files
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec![
                "tests/data/apache-avro/weather.avro".to_string(),
                "tests/data/apache-avro/weather-deflate.avro".to_string(),
            ],
            schema,
        );
        // Set n_rows to a small number that should be reached within first file
        let config = MultiSourceConfig::new().with_n_rows(2);

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Read all batches
        let mut total_rows = 0;
        while let Some(df) = reader.next_batch().await.unwrap() {
            total_rows += df.height();
        }

        // Should have read at most 2 rows total
        assert!(total_rows <= 2, "Should have read at most 2 rows total");
        assert!(reader.is_finished());
    }

    #[tokio::test]
    async fn test_multi_source_reader_row_index_continuity() {
        // Test that row index is continuous across files
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec![
                "tests/data/apache-avro/weather.avro".to_string(),
                "tests/data/apache-avro/weather-deflate.avro".to_string(),
            ],
            schema,
        );
        let config = MultiSourceConfig::new().with_row_index("idx", 0);

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Read all batches and collect row indices
        let mut all_indices: Vec<u32> = Vec::new();
        while let Some(df) = reader.next_batch().await.unwrap() {
            let idx_col = df.column("idx").expect("Should have idx column");
            let indices: Vec<u32> = idx_col.u32().unwrap().into_no_null_iter().collect();
            all_indices.extend(indices);
        }

        // Verify indices are continuous (0, 1, 2, 3, ...)
        for (i, idx) in all_indices.iter().enumerate() {
            assert_eq!(
                *idx, i as u32,
                "Row index should be continuous: expected {}, got {}",
                i, idx
            );
        }
    }

    #[tokio::test]
    async fn test_multi_source_reader_row_index_with_offset() {
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec!["tests/data/apache-avro/weather.avro".to_string()],
            schema,
        );
        let config = MultiSourceConfig::new()
            .with_row_index("idx", 100)
            .with_n_rows(5);

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Read first batch
        let df = reader.next_batch().await.unwrap().unwrap();
        let idx_col = df.column("idx").expect("Should have idx column");
        let first_idx = idx_col.u32().unwrap().get(0).unwrap();

        // First index should be 100 (the offset)
        assert_eq!(first_idx, 100, "First index should be 100");
    }

    #[tokio::test]
    async fn test_multi_source_reader_file_path_injection() {
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec!["tests/data/apache-avro/weather.avro".to_string()],
            schema,
        );
        let config = MultiSourceConfig::new().with_include_file_paths("source_file");

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Read first batch
        let df = reader.next_batch().await.unwrap().unwrap();

        // Should have source_file column
        let path_col = df
            .column("source_file")
            .expect("Should have source_file column");
        let first_path: &str = path_col.str().unwrap().get(0).unwrap();

        assert!(
            first_path.contains("weather.avro"),
            "Path should contain weather.avro"
        );
    }

    #[tokio::test]
    async fn test_multi_source_reader_file_path_changes_across_files() {
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec![
                "tests/data/apache-avro/weather.avro".to_string(),
                "tests/data/apache-avro/weather-deflate.avro".to_string(),
            ],
            schema,
        );
        let config = MultiSourceConfig::new().with_include_file_paths("source");

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Collect all file paths
        let mut all_paths: Vec<String> = Vec::new();
        while let Some(df) = reader.next_batch().await.unwrap() {
            let path_col = df.column("source").expect("Should have source column");
            for i in 0..df.height() {
                let path: &str = path_col.str().unwrap().get(i).unwrap();
                all_paths.push(path.to_string());
            }
        }

        // Should have paths from both files
        let has_weather = all_paths.iter().any(|p| p.contains("weather.avro"));
        let has_deflate = all_paths.iter().any(|p| p.contains("weather-deflate.avro"));

        assert!(has_weather, "Should have paths from weather.avro");
        assert!(has_deflate, "Should have paths from weather-deflate.avro");
    }

    #[tokio::test]
    async fn test_multi_source_reader_error_accumulation_mixed_files() {
        // Test error accumulation with a mix of valid and invalid files
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec![
                "tests/data/apache-avro/weather.avro".to_string(),
                "nonexistent_file_xyz.avro".to_string(), // This will fail
                "tests/data/apache-avro/weather-deflate.avro".to_string(),
            ],
            schema,
        );
        let config = MultiSourceConfig::new().with_ignore_errors(true);

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Read all batches
        let mut total_rows = 0;
        while let Some(df) = reader.next_batch().await.unwrap() {
            total_rows += df.height();
        }

        // Should have read rows from valid files
        assert!(total_rows > 0, "Should have read rows from valid files");

        // Should have accumulated an error for the nonexistent file
        assert!(
            !reader.errors().is_empty(),
            "Should have accumulated errors"
        );

        // Verify the error message mentions the nonexistent file
        let error_messages: Vec<String> =
            reader.errors().iter().map(|e| e.message.clone()).collect();
        let has_nonexistent_error = error_messages
            .iter()
            .any(|msg| msg.contains("nonexistent_file_xyz"));
        assert!(
            has_nonexistent_error,
            "Should have error for nonexistent file"
        );
    }

    #[tokio::test]
    async fn test_multi_source_reader_strict_mode_fails_on_error() {
        // Test that strict mode fails immediately on error
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec![
                "nonexistent_file_xyz.avro".to_string(), // This will fail first
                "tests/data/apache-avro/weather.avro".to_string(),
            ],
            schema,
        );
        let config = MultiSourceConfig::new().with_ignore_errors(false); // Strict mode

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Should fail on first batch attempt
        let result = reader.next_batch().await;
        assert!(result.is_err(), "Should fail in strict mode");
    }

    #[tokio::test]
    async fn test_multi_source_reader_empty_after_finish() {
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec!["tests/data/apache-avro/weather.avro".to_string()],
            schema,
        );
        let config = MultiSourceConfig::new().with_n_rows(1);

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Read until finished
        while let Some(_df) = reader.next_batch().await.unwrap() {}

        assert!(reader.is_finished());

        // Subsequent calls should return None
        let result = reader.next_batch().await.unwrap();
        assert!(result.is_none(), "Should return None after finished");

        // And again
        let result2 = reader.next_batch().await.unwrap();
        assert!(result2.is_none(), "Should still return None");
    }

    #[tokio::test]
    async fn test_multi_source_reader_rows_read_tracking() {
        let schema = read_schema_from_test_file("tests/data/apache-avro/weather.avro").await;
        let sources = ResolvedSources::new(
            vec!["tests/data/apache-avro/weather.avro".to_string()],
            schema,
        );
        let config = MultiSourceConfig::new();

        let mut reader = AvroMultiStreamReader::new(sources, config);

        // Initial rows_read should be 0
        assert_eq!(reader.rows_read(), 0);

        // Read batches and verify rows_read is updated
        let mut expected_rows = 0;
        while let Some(df) = reader.next_batch().await.unwrap() {
            expected_rows += df.height();
            assert_eq!(
                reader.rows_read(),
                expected_rows,
                "rows_read should match total rows read"
            );
        }
    }

    // =========================================================================
    // Cloud options configuration tests
    // =========================================================================

    #[test]
    fn test_multi_source_config_with_s3_config() {
        let s3_cfg = S3Config::new()
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_max_retries(5);

        let config = MultiSourceConfig::new().with_s3_config(s3_cfg.clone());

        assert!(config.s3_config.is_some());
        let cfg = config.s3_config.unwrap();
        assert_eq!(cfg.endpoint, Some("http://localhost:9000".to_string()));
        assert_eq!(cfg.aws_access_key_id, Some("minioadmin".to_string()));
        assert_eq!(cfg.aws_secret_access_key, Some("minioadmin".to_string()));
        assert_eq!(cfg.region, Some("us-east-1".to_string()));
        assert_eq!(cfg.max_retries, 5);
    }

    #[test]
    fn test_multi_source_config_default_no_s3_config() {
        let config = MultiSourceConfig::default();
        assert!(config.s3_config.is_none());
    }

    #[test]
    fn test_multi_source_config_builder_chain_with_s3_config() {
        let s3_cfg = S3Config::new().with_endpoint("http://localhost:9000");

        let config = MultiSourceConfig::new()
            .with_n_rows(1000)
            .with_row_index("idx", 0)
            .with_include_file_paths("source_file")
            .with_ignore_errors(true)
            .with_s3_config(s3_cfg);

        assert_eq!(config.n_rows, Some(1000));
        assert!(config.row_index.is_some());
        assert!(config.include_file_paths.is_some());
        assert!(config.ignore_errors);
        assert!(config.s3_config.is_some());
    }

    #[test]
    fn test_is_s3_uri_detection() {
        // S3 URIs
        assert!(is_s3_uri("s3://bucket/key"));
        assert!(is_s3_uri("s3://my-bucket/path/to/file.avro"));
        assert!(is_s3_uri("s3://bucket/"));

        // Not S3 URIs
        assert!(!is_s3_uri("/path/to/file.avro"));
        assert!(!is_s3_uri("./relative/path.avro"));
        assert!(!is_s3_uri("file.avro"));
        assert!(!is_s3_uri("http://example.com/file.avro"));
        assert!(!is_s3_uri("S3://bucket/key")); // Case sensitive
    }

    #[test]
    fn test_multi_source_reader_with_s3_paths_requires_s3_config() {
        // This test verifies that S3 paths are recognized
        // Actual S3 reading is tested in integration tests
        let schema = create_test_record_schema("Test", &["id"]);
        let sources = ResolvedSources::new(vec!["s3://bucket/file.avro".to_string()], schema);

        // Without S3 config, S3 reading will fail at runtime
        // but the reader should be created successfully
        let config = MultiSourceConfig::new();
        let reader = AvroMultiStreamReader::new(sources, config);

        assert_eq!(reader.total_sources(), 1);
        assert!(!reader.is_finished());
    }

    #[test]
    fn test_multi_source_reader_mixed_local_and_s3_paths() {
        // Test that reader can be configured with mixed paths
        // Actual reading is tested in integration tests
        let schema = create_test_record_schema("Test", &["id"]);
        let sources = ResolvedSources::new(
            vec![
                "tests/data/file.avro".to_string(),
                "s3://bucket/file.avro".to_string(),
            ],
            schema,
        );

        let s3_cfg = S3Config::new()
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("test")
            .with_secret_access_key("test");

        let config = MultiSourceConfig::new().with_s3_config(s3_cfg);
        let reader = AvroMultiStreamReader::new(sources, config);

        assert_eq!(reader.total_sources(), 2);
    }
}
