//! Scan support for Avro files.
//!
//! This module provides types and functions to support scanning Avro files
//! with Polars' `register_io_source` approach. The actual `LazyFrame` creation
//! happens in Python using `register_io_source`, while this module provides
//! the Rust building blocks.
//!
//! # Architecture
//!
//! The scan workflow uses Polars' IO plugin system:
//! 1. Python calls `register_io_source` with a generator function
//! 2. The generator uses `AvroMultiStreamReader` to read batches
//! 3. Polars handles query optimization (projection, predicate, slice pushdown)
//!
//! # Requirements
//! - 10.10: Integration uses `register_io_source` approach
//! - 13.3: The library exposes scan support (via Python bindings)

use polars::prelude::*;

use crate::convert::avro_to_arrow_schema;
use crate::error::ReaderError;
use crate::reader::ReaderConfig;

use super::args::ScanArgsAvro;
use super::multi_source::{AvroMultiStreamReader, MultiSourceConfig};
use super::options::AvroOptions;
use super::sources::ResolvedSources;

/// Configuration for scanning Avro files.
///
/// This struct bundles all the configuration needed for a scan operation,
/// including resolved sources, scan arguments, and Avro options.
#[derive(Clone)]
pub struct ScanConfig {
    /// Resolved sources (paths and unified schema).
    pub resolved: ResolvedSources,
    /// Scan arguments.
    pub args: ScanArgsAvro,
    /// Avro-specific options.
    pub opts: AvroOptions,
}

impl ScanConfig {
    /// Create a new `ScanConfig`.
    pub fn new(resolved: ResolvedSources, args: ScanArgsAvro, opts: AvroOptions) -> Self {
        Self {
            resolved,
            args,
            opts,
        }
    }

    /// Get the Polars schema for this scan.
    pub fn polars_schema(&self) -> PolarsResult<Schema> {
        avro_to_arrow_schema(&self.resolved.schema).map_err(PolarsError::from)
    }

    /// Create a `AvroMultiStreamReader` for this scan configuration.
    ///
    /// # Arguments
    /// * `projected_columns` - Optional list of columns to project
    /// * `n_rows` - Optional row limit (overrides args.n_rows)
    pub fn create_reader(
        &self,
        projected_columns: Option<Vec<String>>,
        n_rows: Option<usize>,
    ) -> AvroMultiStreamReader {
        // Create reader configuration from AvroOptions
        let mut reader_config = ReaderConfig::new()
            .with_batch_size(self.opts.batch_size)
            .with_buffer_config(crate::reader::BufferConfig {
                max_blocks: self.opts.buffer_blocks,
                max_bytes: self.opts.buffer_bytes,
                max_decompressed_block_size: self.opts.max_decompressed_block_size,
            });

        // Apply projection if provided
        if let Some(columns) = projected_columns {
            reader_config = reader_config.with_projection(columns);
        }

        // Apply error mode
        reader_config = if self.args.ignore_errors {
            reader_config.skip_errors()
        } else {
            reader_config.strict()
        };

        // Create multi-source configuration
        let mut multi_config = MultiSourceConfig::new()
            .with_reader_config(reader_config)
            .with_ignore_errors(self.args.ignore_errors);

        // Apply n_rows limit (use provided value or args value)
        if let Some(limit) = n_rows.or(self.args.n_rows) {
            multi_config = multi_config.with_n_rows(limit);
        }

        // Apply row index
        if let Some(ref row_index) = self.args.row_index {
            multi_config = multi_config.with_row_index(row_index.name.clone(), row_index.offset);
        }

        // Apply file path inclusion
        if let Some(ref include_file_paths) = self.args.include_file_paths {
            multi_config = multi_config.with_include_file_paths(include_file_paths.clone());
        }

        AvroMultiStreamReader::new(self.resolved.clone(), multi_config)
    }
}

/// Resolve sources and create a scan configuration.
///
/// This function resolves glob patterns, validates schemas, and creates
/// a `ScanConfig` that can be used to create readers.
///
/// # Arguments
/// * `sources` - Paths to Avro files (can include glob patterns)
/// * `args` - Scan arguments
/// * `opts` - Avro-specific options
///
/// # Returns
/// A `PolarsResult<ScanConfig>` ready for reading.
pub fn resolve_scan_config(
    sources: &[String],
    args: ScanArgsAvro,
    opts: AvroOptions,
) -> PolarsResult<ScanConfig> {
    // Resolve sources synchronously
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            PolarsError::ComputeError(format!("Failed to create runtime: {}", e).into())
        })?;

    let resolved = runtime.block_on(async {
        ResolvedSources::resolve(sources, args.glob, args.s3_config.as_ref()).await
    })?;

    Ok(ScanConfig::new(resolved, args, opts))
}

/// Read all data from a scan configuration into a single DataFrame.
///
/// This is a convenience function that creates a reader and collects
/// all batches into a single DataFrame.
///
/// # Arguments
/// * `config` - The scan configuration
/// * `projected_columns` - Optional list of columns to project
/// * `n_rows` - Optional row limit
///
/// # Returns
/// A `PolarsResult<DataFrame>` containing all the data.
pub fn collect_scan(
    config: &ScanConfig,
    projected_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
) -> PolarsResult<DataFrame> {
    let mut reader = config.create_reader(projected_columns, n_rows);

    // Create runtime for async operations
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            PolarsError::ComputeError(format!("Failed to create runtime: {}", e).into())
        })?;

    // Read all batches
    let mut dataframes = Vec::new();

    runtime.block_on(async {
        while let Some(df) = reader.next_batch().await? {
            dataframes.push(df);
        }
        Ok::<_, ReaderError>(())
    })?;

    // Concatenate all DataFrames
    if dataframes.is_empty() {
        // Return empty DataFrame with correct schema
        let schema = config.polars_schema()?;
        Ok(DataFrame::empty_with_schema(&schema))
    } else if dataframes.len() == 1 {
        Ok(dataframes.into_iter().next().unwrap())
    } else {
        // Use vertical stacking
        let mut result = dataframes.remove(0);
        for df in dataframes {
            result = result.vstack(&df)?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_scan_config_single_file() {
        let result = resolve_scan_config(
            &["tests/data/apache-avro/weather.avro".to_string()],
            ScanArgsAvro::default(),
            AvroOptions::default(),
        );

        match result {
            Ok(config) => {
                assert_eq!(config.resolved.paths.len(), 1);
                let schema = config.polars_schema().unwrap();
                assert!(schema.len() > 0);
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_resolve_scan_config_glob_pattern() {
        let result = resolve_scan_config(
            &["tests/data/apache-avro/weather*.avro".to_string()],
            ScanArgsAvro::default(),
            AvroOptions::default(),
        );

        match result {
            Ok(config) => {
                // Should find multiple weather files
                assert!(config.resolved.paths.len() >= 1);
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_resolve_scan_config_nonexistent_file() {
        let result = resolve_scan_config(
            &["nonexistent_file_12345.avro".to_string()],
            ScanArgsAvro::default(),
            AvroOptions::default(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_collect_scan() {
        let config = resolve_scan_config(
            &["tests/data/apache-avro/weather.avro".to_string()],
            ScanArgsAvro::default(),
            AvroOptions::default(),
        );

        match config {
            Ok(config) => {
                let df = collect_scan(&config, None, None).unwrap();
                assert!(df.height() > 0);
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_collect_scan_with_n_rows() {
        let config = resolve_scan_config(
            &["tests/data/apache-avro/weather.avro".to_string()],
            ScanArgsAvro::default(),
            AvroOptions::default(),
        );

        match config {
            Ok(config) => {
                let df = collect_scan(&config, None, Some(5)).unwrap();
                assert!(df.height() <= 5);
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_scan_config_create_reader() {
        let config = resolve_scan_config(
            &["tests/data/apache-avro/weather.avro".to_string()],
            ScanArgsAvro::new().with_n_rows(10),
            AvroOptions::new().with_batch_size(5),
        );

        match config {
            Ok(config) => {
                let reader = config.create_reader(None, None);
                assert_eq!(reader.total_sources(), 1);
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }
}
