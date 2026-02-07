//! Scan arguments for Avro files.
//!
//! This module defines `ScanArgsAvro`, which contains configuration for scanning
//! Avro files. It mirrors Polars' `ScanArgsParquet` for API consistency.
//!
//! # Requirements
//! - 13.1: The library exposes a `ScanArgsAvro` struct with scan configuration options
//! - 13.8: The library uses Polars types directly: `PlSmallStr`, `IdxSize`, `RowIndex`

use std::sync::Arc;

use crate::source::S3Config;

/// Type alias for row index size, matching Polars' `IdxSize` (u32).
pub type IdxSize = u32;

/// Row index configuration.
///
/// This struct mirrors Polars' `RowIndex` type. It specifies the name and
/// starting offset for a synthetic row index column.
///
/// # Example
/// ```
/// use jetliner::api::RowIndex;
///
/// let row_index = RowIndex {
///     name: "idx".into(),
///     offset: 0,
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RowIndex {
    /// Name of the row index column.
    pub name: Arc<str>,
    /// Starting offset for the row index (default: 0).
    pub offset: IdxSize,
}

impl RowIndex {
    /// Create a new `RowIndex` with the given name and offset 0.
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            offset: 0,
        }
    }

    /// Create a new `RowIndex` with the given name and offset.
    pub fn with_offset(name: impl Into<Arc<str>>, offset: IdxSize) -> Self {
        Self {
            name: name.into(),
            offset,
        }
    }
}

/// Configuration for scanning Avro files.
///
/// This struct mirrors Polars' `ScanArgsParquet` for API consistency. It contains
/// all the configuration options for scanning Avro files, separate from the
/// format-specific `AvroOptions`.
///
/// # Note
/// `AvroOptions` (buffer settings, batch size) is passed separately to scan functions,
/// not nested within this struct. This follows Polars' pattern of separating
/// `ScanArgsParquet` from `ParquetOptions`.
///
/// # Example
/// ```
/// use jetliner::api::{ScanArgsAvro, RowIndex};
///
/// let args = ScanArgsAvro {
///     n_rows: Some(1000),
///     row_index: Some(RowIndex::new("idx")),
///     glob: true,
///     ignore_errors: false,
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug)]
pub struct ScanArgsAvro {
    /// Maximum number of rows to read.
    ///
    /// For `scan_avro()`, this serves as a hint; Polars may override via slice pushdown.
    /// When reading multiple files, the limit applies to the total across all files.
    pub n_rows: Option<usize>,

    /// Row index configuration.
    ///
    /// When set, a synthetic row index column is added as the first column.
    /// The index is continuous across files when reading multiple files.
    pub row_index: Option<RowIndex>,

    /// S3 configuration.
    ///
    /// Configuration for S3 and S3-compatible services.
    pub s3_config: Option<S3Config>,

    /// Whether to expand glob patterns (default: true).
    ///
    /// When true, patterns like `"data/*.avro"` are expanded to matching files.
    /// When false, the path is treated literally.
    pub glob: bool,

    /// Column name for source file paths.
    ///
    /// When set, a column is added containing the source file path for each row.
    /// For S3 sources, this is the full S3 URI.
    pub include_file_paths: Option<Arc<str>>,

    /// If true, skip bad records; if false, fail on first error (default: false).
    ///
    /// The parameter name aligns with Polars CSV reader conventions.
    pub ignore_errors: bool,
}

impl Default for ScanArgsAvro {
    fn default() -> Self {
        Self {
            n_rows: None,
            row_index: None,
            s3_config: None,
            glob: true, // Default is true - expand glob patterns
            include_file_paths: None,
            ignore_errors: false,
        }
    }
}

impl ScanArgsAvro {
    /// Create a new `ScanArgsAvro` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of rows to read.
    pub fn with_n_rows(mut self, n_rows: usize) -> Self {
        self.n_rows = Some(n_rows);
        self
    }

    /// Set the row index configuration.
    pub fn with_row_index(mut self, row_index: RowIndex) -> Self {
        self.row_index = Some(row_index);
        self
    }

    /// Set the row index by name with offset 0.
    pub fn with_row_index_name(mut self, name: impl Into<Arc<str>>) -> Self {
        self.row_index = Some(RowIndex::new(name));
        self
    }

    /// Set the S3 configuration.
    pub fn with_s3_config(mut self, s3_config: S3Config) -> Self {
        self.s3_config = Some(s3_config);
        self
    }

    /// Set whether to expand glob patterns.
    pub fn with_glob(mut self, glob: bool) -> Self {
        self.glob = glob;
        self
    }

    /// Set the column name for source file paths.
    pub fn with_include_file_paths(mut self, column_name: impl Into<Arc<str>>) -> Self {
        self.include_file_paths = Some(column_name.into());
        self
    }

    /// Set whether to ignore errors.
    pub fn with_ignore_errors(mut self, ignore_errors: bool) -> Self {
        self.ignore_errors = ignore_errors;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_index_new() {
        let ri = RowIndex::new("idx");
        assert_eq!(&*ri.name, "idx");
        assert_eq!(ri.offset, 0);
    }

    #[test]
    fn test_row_index_with_offset() {
        let ri = RowIndex::with_offset("idx", 100);
        assert_eq!(&*ri.name, "idx");
        assert_eq!(ri.offset, 100);
    }

    #[test]
    fn test_scan_args_avro_default() {
        let args = ScanArgsAvro::default();
        assert_eq!(args.n_rows, None);
        assert_eq!(args.row_index, None);
        assert_eq!(args.s3_config, None);
        assert!(args.glob); // default is true
        assert_eq!(args.include_file_paths, None);
        assert!(!args.ignore_errors);
    }

    #[test]
    fn test_scan_args_avro_builder() {
        let args = ScanArgsAvro::new()
            .with_n_rows(1000)
            .with_row_index_name("idx")
            .with_glob(false)
            .with_include_file_paths("source_file")
            .with_ignore_errors(true);

        assert_eq!(args.n_rows, Some(1000));
        assert!(args.row_index.is_some());
        assert_eq!(&*args.row_index.as_ref().unwrap().name, "idx");
        assert!(!args.glob);
        assert_eq!(&*args.include_file_paths.unwrap(), "source_file");
        assert!(args.ignore_errors);
    }

    #[test]
    fn test_scan_args_avro_with_row_index() {
        let ri = RowIndex::with_offset("row_nr", 50);
        let args = ScanArgsAvro::new().with_row_index(ri);

        let row_index = args.row_index.unwrap();
        assert_eq!(&*row_index.name, "row_nr");
        assert_eq!(row_index.offset, 50);
    }

    #[test]
    fn test_scan_args_avro_with_s3_config() {
        let s3_cfg = S3Config::new()
            .with_endpoint("http://localhost:9000")
            .with_max_retries(5);

        let args = ScanArgsAvro::new().with_s3_config(s3_cfg);

        let cfg = args.s3_config.unwrap();
        assert_eq!(cfg.endpoint, Some("http://localhost:9000".to_string()));
        assert_eq!(cfg.max_retries, 5);
    }

    #[test]
    fn test_scan_args_avro_clone() {
        let args = ScanArgsAvro::new()
            .with_n_rows(100)
            .with_row_index_name("idx");

        let cloned = args.clone();
        assert_eq!(args.n_rows, cloned.n_rows);
        assert_eq!(args.row_index, cloned.row_index);
    }

    #[test]
    fn test_scan_args_avro_debug() {
        let args = ScanArgsAvro::new().with_n_rows(100);
        let debug_str = format!("{:?}", args);
        assert!(debug_str.contains("ScanArgsAvro"));
        assert!(debug_str.contains("n_rows"));
    }
}
