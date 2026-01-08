//! Unit tests for spec compliance fixes (Task 10.6)
//!
//! This test module validates the following spec compliance fixes:
//! - Task 10.1: Snappy CRC32C validation
//! - Task 10.2: "zstandard" codec name alias
//! - Task 10.3: Strict schema validation mode
//! - Task 10.4: Local timestamp Arrow type mapping
//!
//! Requirements: 10.2, 10.3, 10.4, 10.5

use jetliner::codec::Codec;
use jetliner::convert::avro_to_arrow;
use jetliner::schema::{parse_schema_with_options, AvroSchema, LogicalType, LogicalTypeName};
use polars::prelude::*;

// ============================================================================
// Task 10.1 & 10.2: Snappy CRC32C Validation Tests
// ============================================================================

#[cfg(feature = "snappy")]
mod snappy_crc_tests {
    use super::*;

    /// Helper to create Avro-framed snappy data (compressed + 4-byte CRC)
    fn create_avro_snappy_data(uncompressed: &[u8]) -> Vec<u8> {
        use snap::raw::Encoder;

        let mut encoder = Encoder::new();
        let compressed = encoder.compress_vec(uncompressed).unwrap();

        // Compute CRC32C of uncompressed data (big-endian)
        let crc = crc32c::crc32c(uncompressed);

        let mut result = compressed;
        result.extend_from_slice(&crc.to_be_bytes());
        result
    }

    #[test]
    fn test_snappy_crc32c_validation_with_valid_data() {
        // Test that valid snappy data with correct CRC passes validation
        let original = b"The quick brown fox jumps over the lazy dog";
        let avro_snappy_data = create_avro_snappy_data(original);

        let result = Codec::Snappy.decompress(&avro_snappy_data);
        assert!(
            result.is_ok(),
            "Valid snappy data with correct CRC should decompress successfully"
        );
        assert_eq!(result.unwrap(), original);
    }

    #[test]
    fn test_snappy_crc32c_validation_with_corrupted_crc() {
        // Test that corrupted CRC is detected
        let original = b"Test data for CRC validation";
        let mut avro_snappy_data = create_avro_snappy_data(original);

        // Corrupt the CRC by flipping bits in the last byte
        let len = avro_snappy_data.len();
        avro_snappy_data[len - 1] ^= 0xFF;

        let result = Codec::Snappy.decompress(&avro_snappy_data);
        assert!(
            result.is_err(),
            "Corrupted CRC should cause decompression to fail"
        );

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("CRC32C checksum mismatch"),
            "Error should mention CRC32C checksum mismatch, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_snappy_crc32c_validation_with_corrupted_data() {
        // Test that data corruption is detected via CRC
        let original = b"Important data that must not be corrupted";
        let mut avro_snappy_data = create_avro_snappy_data(original);

        // Corrupt the compressed data (not the CRC)
        // Flip a bit in the middle of the compressed data
        if avro_snappy_data.len() > 10 {
            let mid_index = avro_snappy_data.len() / 2;
            avro_snappy_data[mid_index] ^= 0x01;
        }

        let result = Codec::Snappy.decompress(&avro_snappy_data);
        // This should either fail during decompression or CRC validation
        assert!(
            result.is_err(),
            "Corrupted compressed data should cause decompression to fail"
        );
    }

    #[test]
    fn test_snappy_crc32c_validation_with_wrong_crc_value() {
        use snap::raw::Encoder;

        let original = b"Test data";
        let mut encoder = Encoder::new();
        let compressed = encoder.compress_vec(original).unwrap();

        // Create data with intentionally wrong CRC (all zeros)
        let mut bad_data = compressed;
        bad_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

        let result = Codec::Snappy.decompress(&bad_data);
        assert!(
            result.is_err(),
            "Wrong CRC value should cause validation to fail"
        );

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("CRC32C checksum mismatch"),
            "Error should mention CRC32C checksum mismatch"
        );
        assert!(
            err_msg.contains("expected 0x00000000"),
            "Error should show expected CRC value"
        );
    }

    #[test]
    fn test_snappy_crc32c_validation_empty_data() {
        // Test that empty data with correct CRC passes
        let empty_crc = crc32c::crc32c(&[]);
        let mut data = Vec::new();
        data.extend_from_slice(&empty_crc.to_be_bytes());

        let result = Codec::Snappy.decompress(&data);
        assert!(
            result.is_ok(),
            "Empty data with correct CRC should pass validation"
        );
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_snappy_crc32c_validation_large_data() {
        // Test CRC validation with larger data
        let original: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let avro_snappy_data = create_avro_snappy_data(&original);

        let result = Codec::Snappy.decompress(&avro_snappy_data);
        assert!(
            result.is_ok(),
            "Large data with correct CRC should decompress successfully"
        );
        assert_eq!(result.unwrap(), original);
    }
}

// ============================================================================
// Task 10.2: "zstandard" Codec Name Alias Tests
// ============================================================================

#[test]
fn test_zstandard_codec_name_acceptance() {
    // Test that both "zstd" and "zstandard" are accepted
    let zstd_result = Codec::from_name("zstd");
    assert!(zstd_result.is_ok(), "Codec name 'zstd' should be accepted");
    assert_eq!(zstd_result.unwrap(), Codec::Zstd);

    let zstandard_result = Codec::from_name("zstandard");
    assert!(
        zstandard_result.is_ok(),
        "Codec name 'zstandard' should be accepted (Avro spec canonical name)"
    );
    assert_eq!(zstandard_result.unwrap(), Codec::Zstd);
}

#[test]
fn test_zstandard_and_zstd_are_equivalent() {
    // Verify that both names map to the same codec
    let zstd = Codec::from_name("zstd").unwrap();
    let zstandard = Codec::from_name("zstandard").unwrap();

    assert_eq!(
        zstd, zstandard,
        "Both 'zstd' and 'zstandard' should map to the same codec"
    );
}

#[test]
fn test_codec_name_case_sensitivity() {
    // Verify that codec names are case-sensitive (per Avro spec)
    let result = Codec::from_name("ZSTD");
    assert!(
        result.is_err(),
        "Codec names should be case-sensitive; 'ZSTD' should not be accepted"
    );

    let result = Codec::from_name("Zstandard");
    assert!(
        result.is_err(),
        "Codec names should be case-sensitive; 'Zstandard' should not be accepted"
    );
}

// ============================================================================
// Task 10.3: Strict Schema Validation Mode Tests
// ============================================================================

#[test]
fn test_strict_mode_enabled_rejects_duplicate_union_types() {
    // Strict mode should reject unions with duplicate types
    let json = r#"["int", "int"]"#;

    let result = parse_schema_with_options(json, true);
    assert!(
        result.is_err(),
        "Strict mode should reject duplicate types in union"
    );

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("duplicate type"),
        "Error should mention duplicate type, got: {}",
        err_msg
    );
}

#[test]
fn test_strict_mode_disabled_allows_duplicate_union_types() {
    // Permissive mode (strict=false) should allow duplicates with warning
    let json = r#"["int", "int"]"#;

    let result = parse_schema_with_options(json, false);
    assert!(
        result.is_ok(),
        "Permissive mode should allow duplicate types in union (with warning)"
    );
}

#[test]
fn test_strict_mode_enabled_rejects_nested_unions() {
    // Strict mode should reject nested unions
    let json = r#"["int", ["string", "null"]]"#;

    let result = parse_schema_with_options(json, true);
    assert!(result.is_err(), "Strict mode should reject nested unions");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("nested union"),
        "Error should mention nested union, got: {}",
        err_msg
    );
}

#[test]
fn test_strict_mode_disabled_allows_nested_unions() {
    // Permissive mode should allow nested unions with warning
    let json = r#"["int", ["string", "null"]]"#;

    let result = parse_schema_with_options(json, false);
    assert!(
        result.is_ok(),
        "Permissive mode should allow nested unions (with warning)"
    );
}

#[test]
fn test_strict_mode_enabled_rejects_invalid_names() {
    // Strict mode should reject names that don't follow Avro naming rules

    // Name starting with number
    let json = r#"{
        "type": "record",
        "name": "123Invalid",
        "fields": [{"name": "value", "type": "int"}]
    }"#;

    let result = parse_schema_with_options(json, true);
    assert!(
        result.is_err(),
        "Strict mode should reject names starting with numbers"
    );

    // Name with special characters
    let json2 = r#"{
        "type": "record",
        "name": "Invalid-Name",
        "fields": [{"name": "value", "type": "int"}]
    }"#;

    let result2 = parse_schema_with_options(json2, true);
    assert!(
        result2.is_err(),
        "Strict mode should reject names with special characters"
    );
}

#[test]
fn test_strict_mode_disabled_allows_invalid_names() {
    // Permissive mode should allow invalid names with warning
    let json = r#"{
        "type": "record",
        "name": "123Invalid",
        "fields": [{"name": "value", "type": "int"}]
    }"#;

    let result = parse_schema_with_options(json, false);
    assert!(
        result.is_ok(),
        "Permissive mode should allow invalid names (with warning)"
    );
}

#[test]
fn test_strict_mode_enabled_accepts_valid_schemas() {
    // Strict mode should accept valid schemas
    let json = r#"{
        "type": "record",
        "name": "ValidRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "optional", "type": ["null", "string"]}
        ]
    }"#;

    let result = parse_schema_with_options(json, true);
    assert!(result.is_ok(), "Strict mode should accept valid schemas");
}

#[test]
fn test_strict_mode_validates_field_names() {
    // Strict mode should validate field names
    let json = r#"{
        "type": "record",
        "name": "Test",
        "fields": [{"name": "field-with-dash", "type": "int"}]
    }"#;

    let result = parse_schema_with_options(json, true);
    assert!(
        result.is_err(),
        "Strict mode should reject field names with invalid characters"
    );

    let err = result.unwrap_err();
    assert!(err.to_string().contains("invalid character"));
}

#[test]
fn test_strict_mode_validates_enum_symbols() {
    // Strict mode should validate enum symbol names
    let json = r#"{
        "type": "enum",
        "name": "Status",
        "symbols": ["OK", "NOT-OK"]
    }"#;

    let result = parse_schema_with_options(json, true);
    assert!(
        result.is_err(),
        "Strict mode should reject enum symbols with invalid characters"
    );

    let err = result.unwrap_err();
    assert!(err.to_string().contains("invalid character"));
}

// ============================================================================
// Task 10.4: Local Timestamp Arrow Type Mapping Tests
// ============================================================================

#[test]
fn test_timestamp_millis_maps_to_utc() {
    // timestamp-millis should map to Datetime with UTC timezone
    let logical = LogicalType::new(AvroSchema::Long, LogicalTypeName::TimestampMillis);
    let schema = AvroSchema::Logical(logical);

    let result = avro_to_arrow(&schema);
    assert!(result.is_ok(), "timestamp-millis should map successfully");

    let dtype = result.unwrap();
    assert_eq!(
        dtype,
        DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::UTC)),
        "timestamp-millis should map to Datetime with UTC timezone"
    );
}

#[test]
fn test_timestamp_micros_maps_to_utc() {
    // timestamp-micros should map to Datetime with UTC timezone
    let logical = LogicalType::new(AvroSchema::Long, LogicalTypeName::TimestampMicros);
    let schema = AvroSchema::Logical(logical);

    let result = avro_to_arrow(&schema);
    assert!(result.is_ok(), "timestamp-micros should map successfully");

    let dtype = result.unwrap();
    assert_eq!(
        dtype,
        DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
        "timestamp-micros should map to Datetime with UTC timezone"
    );
}

#[test]
fn test_local_timestamp_millis_maps_without_timezone() {
    // local-timestamp-millis should map to Datetime WITHOUT timezone
    let logical = LogicalType::new(AvroSchema::Long, LogicalTypeName::LocalTimestampMillis);
    let schema = AvroSchema::Logical(logical);

    let result = avro_to_arrow(&schema);
    assert!(
        result.is_ok(),
        "local-timestamp-millis should map successfully"
    );

    let dtype = result.unwrap();
    assert_eq!(
        dtype,
        DataType::Datetime(TimeUnit::Milliseconds, None),
        "local-timestamp-millis should map to Datetime WITHOUT timezone"
    );
}

#[test]
fn test_local_timestamp_micros_maps_without_timezone() {
    // local-timestamp-micros should map to Datetime WITHOUT timezone
    let logical = LogicalType::new(AvroSchema::Long, LogicalTypeName::LocalTimestampMicros);
    let schema = AvroSchema::Logical(logical);

    let result = avro_to_arrow(&schema);
    assert!(
        result.is_ok(),
        "local-timestamp-micros should map successfully"
    );

    let dtype = result.unwrap();
    assert_eq!(
        dtype,
        DataType::Datetime(TimeUnit::Microseconds, None),
        "local-timestamp-micros should map to Datetime WITHOUT timezone"
    );
}

#[test]
fn test_timestamp_types_distinction() {
    // Verify that UTC and local timestamps are distinct
    let utc_millis = LogicalType::new(AvroSchema::Long, LogicalTypeName::TimestampMillis);
    let local_millis = LogicalType::new(AvroSchema::Long, LogicalTypeName::LocalTimestampMillis);

    let utc_dtype = avro_to_arrow(&AvroSchema::Logical(utc_millis)).unwrap();
    let local_dtype = avro_to_arrow(&AvroSchema::Logical(local_millis)).unwrap();

    assert_ne!(
        utc_dtype, local_dtype,
        "UTC and local timestamps should map to different types"
    );

    // Verify UTC has timezone
    match &utc_dtype {
        DataType::Datetime(_, Some(tz)) if tz.as_str() == "UTC" => {} // Expected
        _ => panic!(
            "UTC timestamp should have UTC timezone, got: {:?}",
            utc_dtype
        ),
    }

    // Verify local has no timezone
    match &local_dtype {
        DataType::Datetime(_, None) => {} // Expected
        _ => panic!(
            "Local timestamp should have no timezone, got: {:?}",
            local_dtype
        ),
    }
}

#[test]
fn test_local_timestamp_parsing_from_json() {
    // Test parsing local-timestamp logical types from JSON
    let json_millis = r#"{
        "type": "long",
        "logicalType": "local-timestamp-millis"
    }"#;

    let schema = jetliner::schema::parse_schema(json_millis);
    assert!(
        schema.is_ok(),
        "Should parse local-timestamp-millis from JSON"
    );

    let dtype = avro_to_arrow(&schema.unwrap()).unwrap();
    assert_eq!(
        dtype,
        DataType::Datetime(TimeUnit::Milliseconds, None),
        "Parsed local-timestamp-millis should map correctly"
    );

    let json_micros = r#"{
        "type": "long",
        "logicalType": "local-timestamp-micros"
    }"#;

    let schema = jetliner::schema::parse_schema(json_micros);
    assert!(
        schema.is_ok(),
        "Should parse local-timestamp-micros from JSON"
    );

    let dtype = avro_to_arrow(&schema.unwrap()).unwrap();
    assert_eq!(
        dtype,
        DataType::Datetime(TimeUnit::Microseconds, None),
        "Parsed local-timestamp-micros should map correctly"
    );
}

// ============================================================================
// Integration Tests: Multiple Fixes Together
// ============================================================================

#[test]
fn test_strict_mode_with_complex_valid_schema() {
    // Test that strict mode accepts a complex but valid schema
    let json = r#"{
        "type": "record",
        "name": "Event",
        "namespace": "com.example",
        "fields": [
            {"name": "id", "type": "string"},
            {
                "name": "timestamp",
                "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                }
            },
            {
                "name": "local_time",
                "type": {
                    "type": "long",
                    "logicalType": "local-timestamp-millis"
                }
            },
            {"name": "data", "type": ["null", "bytes"]},
            {"name": "tags", "type": {"type": "array", "items": "string"}}
        ]
    }"#;

    let result = parse_schema_with_options(json, true);
    assert!(
        result.is_ok(),
        "Strict mode should accept complex valid schema with local timestamps"
    );

    // Verify the schema can be converted to Arrow
    let schema = result.unwrap();
    let arrow_result = avro_to_arrow(&schema);
    assert!(
        arrow_result.is_ok(),
        "Valid schema should convert to Arrow successfully"
    );
}

#[test]
fn test_permissive_mode_maximizes_compatibility() {
    // Test that permissive mode (default) allows schemas that might exist in real files
    // even if they violate strict Avro rules
    let json = r#"{
        "type": "record",
        "name": "LegacyData",
        "fields": [
            {"name": "field-with-dash", "type": "int"},
            {"name": "123numeric", "type": "string"},
            {"name": "valid_field", "type": ["int", "int"]}
        ]
    }"#;

    let result = parse_schema_with_options(json, false);
    assert!(
        result.is_ok(),
        "Permissive mode should allow non-compliant schemas for read compatibility"
    );
}
