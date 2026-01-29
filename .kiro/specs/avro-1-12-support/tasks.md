# Implementation Plan: Avro 1.12.0 Support

## Overview

This implementation plan covers adding Avro 1.12.0 nanosecond timestamp support and filling test coverage gaps. The implementation follows the existing patterns for logical types and extends them to support nanosecond precision.

## Tasks

- [x] 1. Add nanosecond timestamp logical types to schema
  - [x] 1.1 Add TimestampNanos and LocalTimestampNanos variants to LogicalTypeName enum in `src/schema/types.rs`
    - Add doc comments explaining these are Avro 1.12.0+ features
    - Add name() match arms returning "timestamp-nanos" and "local-timestamp-nanos"
    - _Requirements: 1.1, 1.2_

  - [x] 1.2 Add parsing for nanosecond timestamps in `src/schema/parser.rs`
    - Add match arms in parse_logical_type for "timestamp-nanos" and "local-timestamp-nanos"
    - _Requirements: 1.1, 1.2_

  - [x] 1.3 Add Arrow type mapping in `src/convert/arrow.rs`
    - Map TimestampNanos to Datetime(Nanoseconds, UTC)
    - Map LocalTimestampNanos to Datetime(Nanoseconds, None)
    - Add unit tests for the new mappings
    - _Requirements: 1.3, 1.4_

  - [x] 1.4 Add record decoder support in `src/reader/record_decoder.rs`
    - Add match arms in FieldBuilder::create_builder for TimestampNanos and LocalTimestampNanos
    - Use existing DatetimeBuilder with TimeUnit::Nanoseconds
    - _Requirements: 1.5, 1.6_

  - [x] 1.5 Add schema compatibility rules in `src/schema/compatibility.rs`
    - Add match arms for TimestampNanos and LocalTimestampNanos compatibility
    - _Requirements: 1.1, 1.2_

- [x] 2. Checkpoint - Verify nanosecond timestamp implementation
  - Ensure all Rust tests pass with `poe test-rust`
  - Ask the user if questions arise

- [x] 3. Add property tests for nanosecond timestamps
  - [x] 3.1 Extend arb_logical_type() generator in `tests/property_tests.rs`
    - Add TimestampNanos and LocalTimestampNanos to the generator
    - **Property 1: Nanosecond Timestamp Schema Round-Trip**
    - **Validates: Requirements 1.1, 1.2, 1.7**

  - [x] 3.2 Add property test for Arrow type mapping
    - Generate nanosecond timestamp schemas and verify correct DataType
    - **Property 2: Nanosecond Timestamp Arrow Type Mapping**
    - **Validates: Requirements 1.3, 1.4**

- [x] 4. Add Python integration tests for nanosecond timestamps
  - [x] 4.1 Add test_timestamp_nanos_type in `python/tests/types/test_logical_types.py`
    - Create Avro file with timestamp-nanos field
    - Verify DataFrame has Datetime(ns, UTC) column
    - Verify values are correctly decoded
    - _Requirements: 1.3, 1.5_

  - [x] 4.2 Add test_local_timestamp_nanos_type in `python/tests/types/test_logical_types.py`
    - Create Avro file with local-timestamp-nanos field
    - Verify DataFrame has Datetime(ns, None) column
    - Verify values are correctly decoded
    - _Requirements: 1.4, 1.6_

- [x] 5. Checkpoint - Verify nanosecond timestamp tests pass
  - Run `poe test` to verify all tests pass
  - Ask the user if questions arise

- [x] 6. Add missing test coverage for existing features
  - [x] 6.1 Add test_duration_type in `python/tests/types/test_logical_types.py`
    - Create Avro file with duration field (fixed[12])
    - Verify DataFrame has Duration column
    - Verify months/days/milliseconds are correctly interpreted
    - _Requirements: 2.1, 2.2, 2.3_

  - [x] 6.2 Add test_uuid_fixed16_type in `python/tests/types/test_logical_types.py`
    - Create Avro file with UUID stored as fixed[16]
    - Verify DataFrame has String column with valid UUID format
    - _Requirements: 3.1, 3.2_

  - [x] 6.3 Add test_unknown_codec_error in `python/tests/integration/test_error_handling.py`
    - Create Avro file with unknown codec in metadata
    - Verify error message contains codec name
    - _Requirements: 5.1, 5.2_

  - [x] 6.4 Add test_complex_union_error in `python/tests/integration/test_error_handling.py`
    - Create Avro file with complex union (e.g., ["string", "int", "long"])
    - Verify error message indicates complex unions not supported
    - _Requirements: 7.1, 7.2_

- [x] 7. Add schema validation edge case tests
  - [x] 7.1 Add test_empty_namespace_component_rejected in `tests/schema_tests.rs`
    - Test that namespace "a..b" is rejected in strict mode
    - _Requirements: 6.1_

  - [x] 7.2 Add test_invalid_enum_symbol_rejected in `tests/schema_tests.rs`
    - Test that enum symbol "1invalid" is rejected in strict mode
    - _Requirements: 6.2_

  - [x] 7.3 Add test_valid_names_accepted in `tests/schema_tests.rs`
    - Test that valid names work in both strict and permissive modes
    - _Requirements: 6.3_

- [x] 8. Add property test for negative block counts
  - [x] 8.1 Add property test for negative block count decoding in `tests/property_tests.rs`
    - Generate array/map data
    - Encode with negative block counts (byte size prefix)
    - Verify decoding produces same result as positive block count encoding
    - **Property 3: Negative Block Count Decoding Equivalence**
    - **Validates: Requirements 4.1, 4.2, 4.3**

- [x] 9. Add additional encoding edge case tests
  - [x] 9.1 Add test_array_multiple_blocks in `python/tests/integration/test_data_types.py`
    - Create Avro file with array split across multiple blocks
    - Verify all items are correctly decoded
    - _Requirements: 4.3_

  - [x] 9.2 Add test_sync_marker_in_data in `python/tests/integration/test_robustness.py`
    - Create Avro file where data bytes happen to match sync marker pattern
    - Verify file is read correctly without confusion
    - _Requirements: Robustness_

- [x] 10. Final checkpoint - Ensure all tests pass
  - Run `poe test-rust` for Rust tests
  - Run `poe test` for Python tests
  - Ask the user if questions arise

- [x] 11. Add big-decimal logical type support
  - [x] 11.1 Add BigDecimal variant to LogicalTypeName enum in `src/schema/types.rs`
    - Add doc comment explaining this is an Avro 1.12.0+ feature with variable scale
    - Add name() match arm returning "big-decimal"
    - _Requirements: 8.1_

  - [x] 11.2 Add parsing for big-decimal in `src/schema/parser.rs`
    - Add match arm in parse_logical_type for "big-decimal"
    - Validate base type is bytes (big-decimal only supports bytes, not fixed)
    - _Requirements: 8.1_

  - [x] 11.3 Add Arrow type mapping in `src/convert/arrow.rs`
    - Map BigDecimal to String (preserves exact representation)
    - Add unit test for the mapping
    - _Requirements: 8.2_

  - [x] 11.4 Add BigDecimal variant to AvroValue enum in `src/reader/decode.rs`
    - Add BigDecimal(String) variant
    - Add decode_big_decimal function that:
      - Reads bytes from input
      - Extracts scale as varint from start of bytes
      - Converts remaining bytes (unscaled value) to decimal string
    - Add to_json() support for BigDecimal variant
    - Add unit tests for decoding
    - _Requirements: 8.3, 8.4_

  - [x] 11.5 Add BigDecimalBuilder in `src/reader/record_decoder.rs`
    - Create BigDecimalBuilder struct with name and values: Vec<String>
    - Implement decode() that reads bytes, extracts scale, formats decimal string
    - Implement finish() that creates String Series
    - Add FieldBuilder::BigDecimal variant
    - Wire up in create_builder for LogicalTypeName::BigDecimal
    - _Requirements: 8.3, 8.4_

  - [x] 11.6 Add decode_logical_value support in `src/reader/decode.rs`
    - Add match arm for LogicalTypeName::BigDecimal in decode_logical_value
    - Validate base type is bytes
    - _Requirements: 8.3_

- [x] 12. Checkpoint - Verify big-decimal Rust implementation
  - Run `poe test-rust` to verify all Rust tests pass
  - Ask the user if questions arise

- [x] 13. Add big-decimal tests
  - [x] 13.1 Add property test for big-decimal string representation in `tests/property_tests.rs`
    - Generate random scale (0-38) and unscaled integer values
    - Encode as big-decimal bytes (scale varint + unscaled bytes)
    - Decode to string
    - Verify string parses back to original value
    - **Property 4: Big-Decimal String Representation Preserves Value**
    - **Validates: Requirements 8.3, 8.4, 8.5**

  - [x] 13.2 Add test_big_decimal_type in `python/tests/types/test_logical_types.py`
    - Create Avro file with big-decimal field using raw bytes encoding
    - Test various scales: 0, 2, 10, 18
    - Test positive and negative values
    - Verify DataFrame has String column
    - Verify string values match expected decimal representation
    - _Requirements: 8.2, 8.3, 8.4_

  - [x] 13.3 Add big-decimal edge case tests
    - Test scale=0 (integer values)
    - Test empty bytes (should decode to "0")
    - Test large scale values
    - Test negative unscaled values
    - _Requirements: 8.3, 8.4_

- [x] 14. Final checkpoint - Ensure all big-decimal tests pass
  - Run `poe test-rust` for Rust tests
  - Run `poe test` for Python tests
  - Ask the user if questions arise

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases
- The implementation follows existing patterns in the codebase for consistency
- Big-decimal maps to String (not Decimal) because Polars Decimal requires fixed scale at type level, but big-decimal has variable scale per value
