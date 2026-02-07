//! Row index tracking for multi-file reading.
//!
//! This module provides `RowIndexTracker` for maintaining continuous row indices
//! across multiple batches and files.
//!
//! # Requirements
//! - 5.1: The `row_index_name` parameter accepts `str` to specify the index column name
//! - 5.2: The `row_index_name` parameter accepts `None` to disable row indexing (default)
//! - 5.3: When `row_index_name` is specified, insert an index column as the first column
//! - 5.4: The `row_index_offset` parameter accepts `int` to specify the starting index (default 0)
//! - 5.5: The row index is of type `pl.UInt32` (using Polars' `IdxSize` type)
//! - 5.6: When reading multiple files, the row index is continuous across files
//! - 5.7: Row index generation is implemented in Rust

use std::sync::Arc;

use polars::prelude::{Column, DataFrame, IntoColumn, NamedFrom, PlSmallStr, PolarsResult, Series};

use super::args::IdxSize;

/// Tracks row indices across batches and files.
///
/// This struct maintains a running offset that is incremented as batches are
/// processed, ensuring continuous row indices across multiple files.
///
/// # Requirements
/// - 5.5: Uses `IdxSize` (u32) for index values, matching Polars' `pl.UInt32`
/// - 5.6: Maintains continuity across files by tracking current offset
#[derive(Debug, Clone)]
pub struct RowIndexTracker {
    /// Name of the row index column.
    name: Arc<str>,
    /// Current offset (next index to assign).
    current_offset: IdxSize,
}

impl RowIndexTracker {
    /// Create a new `RowIndexTracker` with the given name and initial offset.
    ///
    /// # Arguments
    /// * `name` - The name of the row index column
    /// * `initial_offset` - The starting index value (default: 0)
    ///
    /// # Example
    /// ```
    /// use jetliner::api::row_index::RowIndexTracker;
    ///
    /// let tracker = RowIndexTracker::new("idx", 0);
    /// ```
    pub fn new(name: impl Into<Arc<str>>, initial_offset: IdxSize) -> Self {
        Self {
            name: name.into(),
            current_offset: initial_offset,
        }
    }

    /// Get the name of the row index column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the current offset (next index to assign).
    pub fn current_offset(&self) -> IdxSize {
        self.current_offset
    }

    /// Add a row index column to a DataFrame.
    ///
    /// This method:
    /// 1. Creates a new column with sequential indices starting from `current_offset`
    /// 2. Inserts the column as the first column in the DataFrame
    /// 3. Updates `current_offset` for the next batch
    ///
    /// # Arguments
    /// * `df` - The DataFrame to add the row index to
    ///
    /// # Returns
    /// A new DataFrame with the row index column as the first column.
    ///
    /// # Requirements
    /// - 5.3: Insert index as first column
    /// - 5.5: Index is of type `pl.UInt32` (IdxSize)
    pub fn add_to_dataframe(&mut self, df: DataFrame) -> PolarsResult<DataFrame> {
        let height = df.height();

        // Generate sequential indices
        let indices: Vec<IdxSize> = (0..height as IdxSize)
            .map(|i| self.current_offset.saturating_add(i))
            .collect();

        // Update offset for next batch
        self.current_offset = self.current_offset.saturating_add(height as IdxSize);

        // Create the index series with UInt32 type
        let index_series = Series::new(PlSmallStr::from(self.name.as_ref()), indices);

        // Build new DataFrame with index as first column
        let mut columns: Vec<Column> = Vec::with_capacity(df.width() + 1);
        columns.push(index_series.into_column());
        columns.extend(df.get_columns().iter().cloned());

        DataFrame::new(columns)
    }

    /// Reset the tracker to a new offset.
    ///
    /// This is useful when starting a new read operation.
    pub fn reset(&mut self, offset: IdxSize) {
        self.current_offset = offset;
    }

    /// Get the number of rows that have been indexed so far.
    ///
    /// This is calculated as `current_offset - initial_offset`, but since we
    /// don't store the initial offset, this returns the current offset.
    pub fn rows_indexed(&self) -> IdxSize {
        self.current_offset
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
    fn test_row_index_tracker_new() {
        let tracker = RowIndexTracker::new("idx", 0);
        assert_eq!(tracker.name(), "idx");
        assert_eq!(tracker.current_offset(), 0);
    }

    #[test]
    fn test_row_index_tracker_with_offset() {
        let tracker = RowIndexTracker::new("row_nr", 100);
        assert_eq!(tracker.name(), "row_nr");
        assert_eq!(tracker.current_offset(), 100);
    }

    #[test]
    fn test_add_to_dataframe_basic() {
        let mut tracker = RowIndexTracker::new("idx", 0);
        let df = create_test_dataframe(5);

        let result = tracker.add_to_dataframe(df).unwrap();

        // Check that index column is first
        assert_eq!(result.get_column_names()[0], "idx");
        assert_eq!(result.width(), 3); // idx + a + b

        // Check index values
        let idx_col = result.column("idx").unwrap();
        assert_eq!(idx_col.dtype(), &DataType::UInt32);

        let idx_values: Vec<u32> = idx_col.u32().unwrap().into_no_null_iter().collect();
        assert_eq!(idx_values, vec![0, 1, 2, 3, 4]);

        // Check offset was updated
        assert_eq!(tracker.current_offset(), 5);
    }

    #[test]
    fn test_add_to_dataframe_with_initial_offset() {
        let mut tracker = RowIndexTracker::new("idx", 100);
        let df = create_test_dataframe(3);

        let result = tracker.add_to_dataframe(df).unwrap();

        let idx_col = result.column("idx").unwrap();
        let idx_values: Vec<u32> = idx_col.u32().unwrap().into_no_null_iter().collect();
        assert_eq!(idx_values, vec![100, 101, 102]);

        assert_eq!(tracker.current_offset(), 103);
    }

    #[test]
    fn test_add_to_dataframe_continuity_across_batches() {
        let mut tracker = RowIndexTracker::new("idx", 0);

        // First batch
        let df1 = create_test_dataframe(3);
        let result1 = tracker.add_to_dataframe(df1).unwrap();

        let idx1: Vec<u32> = result1
            .column("idx")
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(idx1, vec![0, 1, 2]);

        // Second batch - should continue from 3
        let df2 = create_test_dataframe(4);
        let result2 = tracker.add_to_dataframe(df2).unwrap();

        let idx2: Vec<u32> = result2
            .column("idx")
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(idx2, vec![3, 4, 5, 6]);

        // Third batch - should continue from 7
        let df3 = create_test_dataframe(2);
        let result3 = tracker.add_to_dataframe(df3).unwrap();

        let idx3: Vec<u32> = result3
            .column("idx")
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(idx3, vec![7, 8]);

        assert_eq!(tracker.current_offset(), 9);
    }

    #[test]
    fn test_add_to_dataframe_empty() {
        let mut tracker = RowIndexTracker::new("idx", 0);
        let df = DataFrame::empty();

        let result = tracker.add_to_dataframe(df).unwrap();

        assert_eq!(result.height(), 0);
        assert_eq!(tracker.current_offset(), 0); // No change for empty df
    }

    #[test]
    fn test_reset() {
        let mut tracker = RowIndexTracker::new("idx", 0);

        // Add some rows
        let df = create_test_dataframe(5);
        let _ = tracker.add_to_dataframe(df).unwrap();
        assert_eq!(tracker.current_offset(), 5);

        // Reset
        tracker.reset(100);
        assert_eq!(tracker.current_offset(), 100);
    }

    #[test]
    fn test_rows_indexed() {
        let mut tracker = RowIndexTracker::new("idx", 10);

        let df = create_test_dataframe(5);
        let _ = tracker.add_to_dataframe(df).unwrap();

        // rows_indexed returns current_offset
        assert_eq!(tracker.rows_indexed(), 15);
    }

    #[test]
    fn test_preserves_original_columns() {
        let mut tracker = RowIndexTracker::new("idx", 0);

        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30i32, 25],
        }
        .unwrap();

        let result = tracker.add_to_dataframe(df).unwrap();

        // Check original columns are preserved
        assert_eq!(result.get_column_names(), vec!["idx", "name", "age"]);

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
    fn test_clone() {
        let tracker = RowIndexTracker::new("idx", 50);
        let cloned = tracker.clone();

        assert_eq!(tracker.name(), cloned.name());
        assert_eq!(tracker.current_offset(), cloned.current_offset());
    }

    #[test]
    fn test_debug() {
        let tracker = RowIndexTracker::new("idx", 100);
        let debug_str = format!("{:?}", tracker);

        assert!(debug_str.contains("RowIndexTracker"));
        assert!(debug_str.contains("idx"));
        assert!(debug_str.contains("100"));
    }

    #[test]
    fn test_saturating_add_near_max() {
        // Test that indices saturate at u32::MAX instead of overflowing
        let mut tracker = RowIndexTracker::new("idx", u32::MAX - 2);
        let df = create_test_dataframe(5);

        let result = tracker.add_to_dataframe(df).unwrap();

        // Indices should saturate at u32::MAX
        let idx_values: Vec<u32> = result
            .column("idx")
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();

        assert_eq!(
            idx_values,
            vec![
                u32::MAX - 2, // 4294967293
                u32::MAX - 1, // 4294967294
                u32::MAX,     // 4294967295 (saturates)
                u32::MAX,     // 4294967295 (saturates)
                u32::MAX,     // 4294967295 (saturates)
            ]
        );

        // Tracker offset should also saturate
        assert_eq!(tracker.current_offset(), u32::MAX);
    }
}
