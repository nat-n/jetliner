//! File path injection for multi-file reading.
//!
//! This module provides `FilePathInjector` for adding a column containing
//! the source file path to each row in a DataFrame.
//!
//! # Requirements
//! - 6.1: The `include_file_paths` parameter accepts `str` to specify the column name for file paths
//! - 6.2: The `include_file_paths` parameter accepts `None` to disable (default)
//! - 6.3: When `include_file_paths` is specified, add a column containing the source file path for each row
//! - 6.4: The file path column is of type `pl.String`
//! - 6.5: For S3 sources, the file path is the full S3 URI
//! - 6.6: File path injection is implemented in Rust
//! - 6.7: The `include_file_paths` parameter works with single files (not just multi-file)

use std::sync::Arc;

use polars::prelude::{Column, DataFrame, IntoColumn, NamedFrom, PlSmallStr, PolarsResult, Series};

/// Injects file path column into DataFrames.
///
/// This struct stores the column name and current file path, and provides
/// a method to add the file path as a new column to DataFrames.
///
/// # Requirements
/// - 6.4: Uses `pl.String` type for the file path column
/// - 6.7: Works with single files as well as multi-file reading
#[derive(Debug, Clone)]
pub struct FilePathInjector {
    /// Name of the file path column.
    column_name: Arc<str>,
    /// Current file path to inject.
    current_path: Arc<str>,
}

impl FilePathInjector {
    /// Create a new `FilePathInjector` with the given column name.
    ///
    /// # Arguments
    /// * `column_name` - The name of the column to add for file paths
    ///
    /// # Example
    /// ```
    /// use jetliner::api::file_path::FilePathInjector;
    ///
    /// let injector = FilePathInjector::new("source_file");
    /// ```
    pub fn new(column_name: impl Into<Arc<str>>) -> Self {
        Self {
            column_name: column_name.into(),
            current_path: Arc::from(""),
        }
    }

    /// Create a new `FilePathInjector` with the given column name and initial path.
    ///
    /// # Arguments
    /// * `column_name` - The name of the column to add for file paths
    /// * `path` - The initial file path
    pub fn with_path(column_name: impl Into<Arc<str>>, path: impl Into<Arc<str>>) -> Self {
        Self {
            column_name: column_name.into(),
            current_path: path.into(),
        }
    }

    /// Get the column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Get the current file path.
    pub fn current_path(&self) -> &str {
        &self.current_path
    }

    /// Set the current file path.
    ///
    /// Call this when switching to a new file in multi-file reading.
    pub fn set_path(&mut self, path: impl Into<Arc<str>>) {
        self.current_path = path.into();
    }

    /// Add the file path column to a DataFrame.
    ///
    /// This method:
    /// 1. Creates a new String column with the current file path repeated for each row
    /// 2. Appends the column to the end of the DataFrame
    ///
    /// # Arguments
    /// * `df` - The DataFrame to add the file path column to
    ///
    /// # Returns
    /// A new DataFrame with the file path column appended.
    ///
    /// # Requirements
    /// - 6.3: Add column containing source file path for each row
    /// - 6.4: Column is of type `pl.String`
    pub fn add_to_dataframe(&self, df: DataFrame) -> PolarsResult<DataFrame> {
        let height = df.height();

        // Create a vector of the file path repeated for each row
        let paths: Vec<&str> = vec![self.current_path.as_ref(); height];

        // Create the file path series with String type
        let path_series = Series::new(PlSmallStr::from(self.column_name.as_ref()), paths);

        // Build new DataFrame with file path column appended
        let mut columns: Vec<Column> = df.get_columns().to_vec();
        columns.push(path_series.into_column());

        DataFrame::new(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    fn create_test_dataframe(height: usize) -> DataFrame {
        let col_a: Vec<i32> = (0..height as i32).collect();
        let col_b: Vec<&str> = (0..height).map(|_| "test").collect();

        df! {
            "a" => col_a,
            "b" => col_b,
        }
        .unwrap()
    }

    #[test]
    fn test_file_path_injector_new() {
        let injector = FilePathInjector::new("source_file");
        assert_eq!(injector.column_name(), "source_file");
        assert_eq!(injector.current_path(), "");
    }

    #[test]
    fn test_file_path_injector_with_path() {
        let injector = FilePathInjector::with_path("source", "/path/to/file.avro");
        assert_eq!(injector.column_name(), "source");
        assert_eq!(injector.current_path(), "/path/to/file.avro");
    }

    #[test]
    fn test_set_path() {
        let mut injector = FilePathInjector::new("source_file");
        assert_eq!(injector.current_path(), "");

        injector.set_path("/path/to/file1.avro");
        assert_eq!(injector.current_path(), "/path/to/file1.avro");

        injector.set_path("/path/to/file2.avro");
        assert_eq!(injector.current_path(), "/path/to/file2.avro");
    }

    #[test]
    fn test_add_to_dataframe_basic() {
        let injector = FilePathInjector::with_path("source_file", "/data/test.avro");
        let df = create_test_dataframe(3);

        let result = injector.add_to_dataframe(df).unwrap();

        // Check that file path column is last
        assert_eq!(result.width(), 3); // a + b + source_file
        assert_eq!(result.get_column_names()[2], "source_file");

        // Check file path values
        let path_col = result.column("source_file").unwrap();
        assert_eq!(path_col.dtype(), &DataType::String);

        let path_values: Vec<&str> = path_col.str().unwrap().into_no_null_iter().collect();
        assert_eq!(path_values, vec!["/data/test.avro"; 3]);
    }

    #[test]
    fn test_add_to_dataframe_s3_uri() {
        let injector = FilePathInjector::with_path("source", "s3://bucket/path/to/file.avro");
        let df = create_test_dataframe(2);

        let result = injector.add_to_dataframe(df).unwrap();

        let path_col = result.column("source").unwrap();
        let path_values: Vec<&str> = path_col.str().unwrap().into_no_null_iter().collect();
        assert_eq!(path_values, vec!["s3://bucket/path/to/file.avro"; 2]);
    }

    #[test]
    fn test_add_to_dataframe_empty() {
        let injector = FilePathInjector::with_path("source_file", "/data/test.avro");
        let df = DataFrame::empty();

        let result = injector.add_to_dataframe(df).unwrap();

        assert_eq!(result.height(), 0);
        // Column should still be added even for empty DataFrame
        assert!(result.column("source_file").is_ok());
    }

    #[test]
    fn test_add_to_dataframe_preserves_columns() {
        let injector = FilePathInjector::with_path("source", "/test.avro");

        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30i32, 25],
        }
        .unwrap();

        let result = injector.add_to_dataframe(df).unwrap();

        // Check original columns are preserved
        assert_eq!(result.get_column_names(), vec!["name", "age", "source"]);

        let names: Vec<&str> = result
            .column("name")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(names, vec!["Alice", "Bob"]);

        let ages: Vec<i32> = result
            .column("age")
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(ages, vec![30, 25]);
    }

    #[test]
    fn test_multiple_files_different_paths() {
        let mut injector = FilePathInjector::new("source_file");

        // First file
        injector.set_path("/data/file1.avro");
        let df1 = create_test_dataframe(2);
        let result1 = injector.add_to_dataframe(df1).unwrap();

        let paths1: Vec<&str> = result1
            .column("source_file")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(paths1, vec!["/data/file1.avro"; 2]);

        // Second file
        injector.set_path("/data/file2.avro");
        let df2 = create_test_dataframe(3);
        let result2 = injector.add_to_dataframe(df2).unwrap();

        let paths2: Vec<&str> = result2
            .column("source_file")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(paths2, vec!["/data/file2.avro"; 3]);
    }

    #[test]
    fn test_clone() {
        let injector = FilePathInjector::with_path("source", "/test.avro");
        let cloned = injector.clone();

        assert_eq!(injector.column_name(), cloned.column_name());
        assert_eq!(injector.current_path(), cloned.current_path());
    }

    #[test]
    fn test_debug() {
        let injector = FilePathInjector::with_path("source_file", "/data/test.avro");
        let debug_str = format!("{:?}", injector);

        assert!(debug_str.contains("FilePathInjector"));
        assert!(debug_str.contains("source_file"));
        assert!(debug_str.contains("/data/test.avro"));
    }

    #[test]
    fn test_single_file_works() {
        // Requirement 6.7: Works with single files
        let injector = FilePathInjector::with_path("file", "single_file.avro");
        let df = create_test_dataframe(5);

        let result = injector.add_to_dataframe(df).unwrap();

        assert_eq!(result.height(), 5);
        let paths: Vec<&str> = result
            .column("file")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(paths, vec!["single_file.avro"; 5]);
    }
}
