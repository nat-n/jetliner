//! Integration tests for the public Rust API.
//!
//! These tests verify the `read_avro()`, `read_avro_schema()`, and related
//! functions work correctly with real Avro files.
//!
//! # Requirements
//! - 13.3: Test scan support (via ScanConfig)
//! - 13.4: Test `read_avro()` returns DataFrame
//! - 13.5: Test `read_avro_schema()` returns Schema
//! - 13.6: Test with various configurations

use std::sync::Arc;

use jetliner::api::{
    read_avro, read_avro_schema, read_avro_sources, AvroOptions, ColumnSelection, RowIndex,
    ScanArgsAvro,
};

// =============================================================================
// read_avro() Tests
// =============================================================================

#[test]
fn test_read_avro_returns_dataframe() {
    let result = read_avro(
        "tests/data/apache-avro/weather.avro",
        None,
        ScanArgsAvro::default(),
        AvroOptions::default(),
    );

    let df = result.expect("Should read weather.avro successfully");
    assert!(df.height() > 0, "DataFrame should have rows");
    assert!(df.width() > 0, "DataFrame should have columns");
}

#[test]
fn test_read_avro_with_columns_parameter() {
    // Read with specific columns by name
    let columns = ColumnSelection::Names(vec![Arc::from("station"), Arc::from("time")]);

    let result = read_avro(
        "tests/data/apache-avro/weather.avro",
        Some(columns),
        ScanArgsAvro::default(),
        AvroOptions::default(),
    );

    let df = result.expect("Should read with column projection");
    assert_eq!(df.width(), 2, "Should only have 2 columns");
    assert!(df.column("station").is_ok(), "Should have station column");
    assert!(df.column("time").is_ok(), "Should have time column");
}

#[test]
fn test_read_avro_with_columns_by_index() {
    // Read with specific columns by index
    let columns = ColumnSelection::Indices(vec![0, 1]);

    let result = read_avro(
        "tests/data/apache-avro/weather.avro",
        Some(columns),
        ScanArgsAvro::default(),
        AvroOptions::default(),
    );

    let df = result.expect("Should read with index projection");
    assert_eq!(df.width(), 2, "Should only have 2 columns");
}

// =============================================================================
// read_avro_schema() Tests
// =============================================================================

#[test]
fn test_read_avro_schema_returns_schema() {
    let result = read_avro_schema("tests/data/apache-avro/weather.avro", None);

    let schema = result.expect("Should read schema successfully");
    assert!(schema.len() > 0, "Schema should have fields");
}

#[test]
fn test_read_avro_schema_contains_expected_fields() {
    let schema =
        read_avro_schema("tests/data/apache-avro/weather.avro", None).expect("Should read schema");

    // Weather schema should have station and time fields
    // Use iter_names() to check field existence
    let field_names: Vec<_> = schema.iter_names().map(|s| s.as_str()).collect();
    assert!(
        field_names.contains(&"station"),
        "Schema should have station field"
    );
    assert!(
        field_names.contains(&"time"),
        "Schema should have time field"
    );
}

// =============================================================================
// ScanArgsAvro Configuration Tests
// =============================================================================

#[test]
fn test_read_avro_with_n_rows_limit() {
    let args = ScanArgsAvro::new().with_n_rows(5);

    let df = read_avro(
        "tests/data/apache-avro/weather.avro",
        None,
        args,
        AvroOptions::default(),
    )
    .expect("Should read with n_rows limit");

    assert!(df.height() <= 5, "Should have at most 5 rows");
}

#[test]
fn test_read_avro_with_row_index() {
    let args = ScanArgsAvro::new().with_row_index(RowIndex::new("idx"));

    let df = read_avro(
        "tests/data/apache-avro/weather.avro",
        None,
        args,
        AvroOptions::default(),
    )
    .expect("Should read with row index");

    // Row index should be present as first column
    assert!(df.column("idx").is_ok(), "Should have idx column");
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert_eq!(names[0], "idx", "idx should be first column");
}

#[test]
fn test_read_avro_with_row_index_offset() {
    let args = ScanArgsAvro::new().with_row_index(RowIndex::with_offset("idx", 100));

    let df = read_avro(
        "tests/data/apache-avro/weather.avro",
        None,
        args,
        AvroOptions::default(),
    )
    .expect("Should read with row index offset");

    // First row index should be 100
    let idx_col = df.column("idx").expect("Should have idx column");
    let first_idx = idx_col.u32().expect("Should be u32").get(0).unwrap();
    assert_eq!(first_idx, 100, "First index should be 100");
}

#[test]
fn test_read_avro_with_include_file_paths() {
    let args = ScanArgsAvro::new().with_include_file_paths("source_file");

    let df = read_avro(
        "tests/data/apache-avro/weather.avro",
        None,
        args,
        AvroOptions::default(),
    )
    .expect("Should read with file paths");

    // Should have source_file column
    assert!(
        df.column("source_file").is_ok(),
        "Should have source_file column"
    );
}

// =============================================================================
// AvroOptions Configuration Tests
// =============================================================================

#[test]
fn test_read_avro_with_custom_batch_size() {
    let opts = AvroOptions::new().with_batch_size(10);

    let df = read_avro(
        "tests/data/apache-avro/weather.avro",
        None,
        ScanArgsAvro::default(),
        opts,
    )
    .expect("Should read with custom batch size");

    assert!(df.height() > 0, "Should have rows");
}

#[test]
fn test_read_avro_with_custom_buffer_config() {
    let opts = AvroOptions::new()
        .with_buffer_blocks(2)
        .with_buffer_bytes(1024 * 1024);

    let df = read_avro(
        "tests/data/apache-avro/weather.avro",
        None,
        ScanArgsAvro::default(),
        opts,
    )
    .expect("Should read with custom buffer config");

    assert!(df.height() > 0, "Should have rows");
}

// =============================================================================
// Multi-file Reading Tests
// =============================================================================

#[test]
fn test_read_avro_sources_multiple_files() {
    // Read multiple weather files with identical schemas
    let result = read_avro_sources(
        &[
            "tests/data/apache-avro/weather.avro".to_string(),
            "tests/data/apache-avro/weather-deflate.avro".to_string(),
        ],
        None,
        ScanArgsAvro::default(),
        AvroOptions::default(),
    );

    let df = result.expect("Should read multiple files");
    assert!(df.height() > 0, "Should have rows from multiple files");
}

#[test]
fn test_read_avro_with_glob_disabled() {
    // With glob disabled, the pattern should be treated literally
    let args = ScanArgsAvro::new().with_glob(false);

    let result = read_avro(
        "tests/data/apache-avro/weather.avro",
        None,
        args,
        AvroOptions::default(),
    );

    // Should still work for exact path
    let df = result.expect("Should read exact path");
    assert!(df.height() > 0, "Should have rows");
}

// =============================================================================
// ignore_errors Mode Tests
// =============================================================================

#[test]
fn test_read_avro_ignore_errors_false() {
    // Default is ignore_errors=false (strict mode)
    let args = ScanArgsAvro::new().with_ignore_errors(false);

    let result = read_avro(
        "tests/data/apache-avro/weather.avro",
        None,
        args,
        AvroOptions::default(),
    );

    assert!(
        result.is_ok(),
        "Should succeed in strict mode for valid file"
    );
}

#[test]
fn test_read_avro_ignore_errors_true() {
    let args = ScanArgsAvro::new().with_ignore_errors(true);

    let result = read_avro(
        "tests/data/apache-avro/weather.avro",
        None,
        args,
        AvroOptions::default(),
    );

    assert!(result.is_ok(), "Should succeed in skip mode");
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[test]
fn test_read_avro_nonexistent_file() {
    let result = read_avro(
        "nonexistent_file_12345.avro",
        None,
        ScanArgsAvro::default(),
        AvroOptions::default(),
    );

    assert!(result.is_err(), "Should fail for nonexistent file");
}

#[test]
fn test_read_avro_schema_nonexistent_file() {
    let result = read_avro_schema("nonexistent_file_12345.avro", None);

    assert!(result.is_err(), "Should fail for nonexistent file");
}

#[test]
fn test_read_avro_invalid_column_name() {
    let columns = ColumnSelection::Names(vec![Arc::from("nonexistent_column_xyz")]);

    let result = read_avro(
        "tests/data/apache-avro/weather.avro",
        Some(columns),
        ScanArgsAvro::default(),
        AvroOptions::default(),
    );

    assert!(result.is_err(), "Should fail for invalid column name");
}

#[test]
fn test_read_avro_invalid_column_index() {
    let columns = ColumnSelection::Indices(vec![999]); // Out of range

    let result = read_avro(
        "tests/data/apache-avro/weather.avro",
        Some(columns),
        ScanArgsAvro::default(),
        AvroOptions::default(),
    );

    assert!(result.is_err(), "Should fail for invalid column index");
}

// =============================================================================
// Different Codec Tests
// =============================================================================

#[test]
fn test_read_avro_deflate_codec() {
    let result = read_avro(
        "tests/data/apache-avro/weather-deflate.avro",
        None,
        ScanArgsAvro::default(),
        AvroOptions::default(),
    );

    let df = result.expect("Should read deflate-compressed file");
    assert!(df.height() > 0, "Should have rows");
}

#[test]
fn test_read_avro_snappy_codec() {
    let result = read_avro(
        "tests/data/apache-avro/weather-snappy.avro",
        None,
        ScanArgsAvro::default(),
        AvroOptions::default(),
    );

    let df = result.expect("Should read snappy-compressed file");
    assert!(df.height() > 0, "Should have rows");
}

#[test]
fn test_read_avro_zstd_codec() {
    let result = read_avro(
        "tests/data/apache-avro/weather-zstd.avro",
        None,
        ScanArgsAvro::default(),
        AvroOptions::default(),
    );

    let df = result.expect("Should read zstd-compressed file");
    assert!(df.height() > 0, "Should have rows");
}
