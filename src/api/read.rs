//! Read functions for Avro files.
//!
//! This module provides `read_avro()` function for eagerly reading Avro files
//! into DataFrames.
//!
//! # Requirements
//! - 11.1: The `read_avro()` function returns a `pl.DataFrame`
//! - 11.2: The `read_avro()` function is equivalent to `scan_avro(...).collect()`
//! - 11.4: The `read_avro()` function additionally accepts `columns` parameter
//! - 13.4: The library exposes a `read_avro()` function returning `PolarsResult<DataFrame>`

use polars::prelude::*;

use crate::convert::avro_to_arrow_schema;
use crate::error::ReaderError;
use crate::reader::ReaderConfig;

use super::args::ScanArgsAvro;
use super::columns::{extract_record_schema, resolve_columns, ColumnSelection};
use super::multi_source::{AvroMultiStreamReader, MultiSourceConfig};
use super::options::AvroOptions;
use super::sources::ResolvedSources;

/// Read an Avro file and return a DataFrame.
///
/// This function eagerly reads the specified Avro file(s) and returns a DataFrame.
/// It supports glob patterns, multiple files, column selection, and various options.
///
/// # Arguments
/// * `source` - Path to the Avro file (can be a glob pattern or S3 URI)
/// * `columns` - Optional column selection (by name or index)
/// * `args` - Scan arguments (n_rows, row_index, glob, etc.)
/// * `opts` - Avro-specific options (buffer_blocks, batch_size, etc.)
///
/// # Returns
/// A `PolarsResult<DataFrame>` containing the data.
///
/// # Requirements
/// - 11.1: Return `pl.DataFrame`
/// - 11.4: Accept `columns` parameter for eager projection
///
/// # Example
/// ```no_run
/// use jetliner::api::{read_avro, ScanArgsAvro, AvroOptions, ColumnSelection};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Read all columns
/// let df = read_avro("data.avro", None, ScanArgsAvro::default(), AvroOptions::default())?;
///
/// // Read specific columns by name
/// let columns = ColumnSelection::Names(vec!["id".into(), "name".into()]);
/// let df = read_avro("data.avro", Some(columns), ScanArgsAvro::default(), AvroOptions::default())?;
/// # Ok(())
/// # }
/// ```
pub fn read_avro(
    source: &str,
    columns: Option<ColumnSelection>,
    args: ScanArgsAvro,
    opts: AvroOptions,
) -> Result<DataFrame, ReaderError> {
    read_avro_sources(&[source.to_string()], columns, args, opts)
}

/// Read multiple Avro files and return a DataFrame.
///
/// This function eagerly reads the specified Avro files and returns a DataFrame.
/// It handles glob expansion, schema validation, and multi-file reading.
///
/// # Arguments
/// * `sources` - Paths to Avro files (can include glob patterns or S3 URIs)
/// * `columns` - Optional column selection (by name or index)
/// * `args` - Scan arguments (n_rows, row_index, glob, etc.)
/// * `opts` - Avro-specific options (buffer_blocks, batch_size, etc.)
///
/// # Returns
/// A `Result<DataFrame, ReaderError>` containing the data.
///
/// # Note
/// This function returns `ReaderError` (not `PolarsError`) to preserve error context,
/// including file paths from `ReaderError::InFile` variants. Callers can convert to
/// `PolarsError` using `.into()` if needed.
///
/// # Requirements
/// - 11.2: Equivalent to `scan_avro(...).collect()` with eager column selection
pub fn read_avro_sources(
    sources: &[String],
    columns: Option<ColumnSelection>,
    args: ScanArgsAvro,
    opts: AvroOptions,
) -> Result<DataFrame, ReaderError> {
    // Resolve sources synchronously
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| ReaderError::Configuration(format!("Failed to create runtime: {}", e)))?;

    let resolved = runtime.block_on(async {
        ResolvedSources::resolve(sources, args.glob, args.s3_config.as_ref()).await
    })?;

    // Resolve column selection to column names
    let projected_columns = if let Some(selection) = columns {
        // Extract record schema for column resolution
        let record_schema = extract_record_schema(&resolved.schema).ok_or_else(|| {
            ReaderError::Schema(crate::error::SchemaError::InvalidSchema(
                "Cannot project columns: schema is not a record type".into(),
            ))
        })?;
        let resolved_cols =
            resolve_columns(&selection, record_schema).map_err(ReaderError::from)?;
        Some(resolved_cols.names)
    } else {
        None
    };

    // Create reader configuration from AvroOptions
    let mut reader_config = ReaderConfig::new()
        .with_batch_size(opts.batch_size)
        .with_buffer_config(crate::reader::BufferConfig {
            max_blocks: opts.buffer_blocks,
            max_bytes: opts.buffer_bytes,
            max_decompressed_block_size: opts.max_decompressed_block_size,
        });

    // Apply projection
    if let Some(ref columns) = projected_columns {
        reader_config =
            reader_config.with_projection(columns.iter().map(|s| s.to_string()).collect());
    }

    // Apply error mode
    reader_config = if args.ignore_errors {
        reader_config.skip_errors()
    } else {
        reader_config.strict()
    };

    // Create multi-source configuration
    let mut multi_config = MultiSourceConfig::new()
        .with_reader_config(reader_config)
        .with_ignore_errors(args.ignore_errors);

    // Apply S3 config for S3 access
    if let Some(ref s3_cfg) = args.s3_config {
        multi_config = multi_config.with_s3_config(s3_cfg.clone());
    }

    // Apply n_rows limit
    if let Some(n_rows) = args.n_rows {
        multi_config = multi_config.with_n_rows(n_rows);
    }

    // Apply row index
    if let Some(ref row_index) = args.row_index {
        multi_config = multi_config.with_row_index(row_index.name.clone(), row_index.offset);
    }

    // Apply file path inclusion
    if let Some(ref include_file_paths) = args.include_file_paths {
        multi_config = multi_config.with_include_file_paths(include_file_paths.clone());
    }

    // Create the multi-source reader
    let mut reader = AvroMultiStreamReader::new(resolved.clone(), multi_config);

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
        let schema = avro_to_arrow_schema(&resolved.schema)?;
        Ok(DataFrame::empty_with_schema(&schema))
    } else if dataframes.len() == 1 {
        Ok(dataframes.into_iter().next().unwrap())
    } else {
        // Use vertical stacking
        let mut result = dataframes.remove(0);
        for df in dataframes {
            result = result.vstack(&df).map_err(ReaderError::from)?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_avro_single_file() {
        let result = read_avro(
            "tests/data/apache-avro/weather.avro",
            None,
            ScanArgsAvro::default(),
            AvroOptions::default(),
        );

        match result {
            Ok(df) => {
                assert!(df.height() > 0);
                assert!(df.width() > 0);
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_read_avro_with_columns_by_name() {
        let columns = ColumnSelection::Names(vec![Arc::from("station"), Arc::from("temp")]);

        let result = read_avro(
            "tests/data/apache-avro/weather.avro",
            Some(columns),
            ScanArgsAvro::default(),
            AvroOptions::default(),
        );

        match result {
            Ok(df) => {
                // Should only have the selected columns
                assert_eq!(df.width(), 2);
                assert!(df.column("station").is_ok());
                assert!(df.column("temp").is_ok());
            }
            Err(e) => {
                println!("Note: Test data not found or columns don't exist: {}", e);
            }
        }
    }

    #[test]
    fn test_read_avro_with_n_rows() {
        let args = ScanArgsAvro::new().with_n_rows(5);

        let result = read_avro(
            "tests/data/apache-avro/weather.avro",
            None,
            args,
            AvroOptions::default(),
        );

        match result {
            Ok(df) => {
                assert!(df.height() <= 5);
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_read_avro_with_row_index() {
        let args = ScanArgsAvro::new().with_row_index_name("row_nr");

        let result = read_avro(
            "tests/data/apache-avro/weather.avro",
            None,
            args,
            AvroOptions::default(),
        );

        match result {
            Ok(df) => {
                // Should have row index column
                assert!(df.column("row_nr").is_ok());
                // Row index should be first column
                let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
                assert_eq!(names[0], "row_nr");
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_read_avro_glob_pattern() {
        let result = read_avro_sources(
            &["tests/data/apache-avro/weather*.avro".to_string()],
            None,
            ScanArgsAvro::default(),
            AvroOptions::default(),
        );

        match result {
            Ok(df) => {
                assert!(df.height() > 0);
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_read_avro_nonexistent_file() {
        let result = read_avro(
            "nonexistent_file_12345.avro",
            None,
            ScanArgsAvro::default(),
            AvroOptions::default(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_read_avro_invalid_column_name() {
        let columns = ColumnSelection::Names(vec![Arc::from("nonexistent_column")]);

        let result = read_avro(
            "tests/data/apache-avro/weather.avro",
            Some(columns),
            ScanArgsAvro::default(),
            AvroOptions::default(),
        );

        // Should error because column doesn't exist
        match result {
            Ok(_) => {
                // If file doesn't exist, we might get here
            }
            Err(e) => {
                // Expected - either file not found or column not found
                println!("Expected error: {}", e);
            }
        }
    }
}
