# Requirements Document

## Introduction

This document specifies the requirements for implementing Avro 1.12.0 nanosecond timestamp support and filling test coverage gaps in the Jetliner project. The Avro 1.12.0 specification (released August 2024) introduced nanosecond precision timestamps that are currently not supported. Additionally, several implemented features lack Python integration tests, reducing confidence in end-to-end correctness.

## Glossary

- **Jetliner**: A high-performance Polars plugin for streaming Avro files into DataFrames
- **Avro_Reader**: The component responsible for reading and decoding Avro binary data
- **Schema_Parser**: The component responsible for parsing Avro JSON schemas
- **Arrow_Converter**: The component responsible for mapping Avro types to Arrow/Polars types
- **Record_Decoder**: The component responsible for decoding Avro records into Arrow column builders
- **Logical_Type**: An Avro type annotation that provides semantic meaning to a base type
- **timestamp-nanos**: Avro 1.12.0 logical type representing nanoseconds since Unix epoch (long base)
- **local-timestamp-nanos**: Avro 1.12.0 logical type representing local nanoseconds without timezone (long base)
- **Duration_Type**: Avro logical type stored as fixed[12] bytes representing months, days, milliseconds
- **UUID_Fixed**: UUID logical type stored as fixed[16] bytes instead of string
- **Negative_Block_Count**: Avro encoding where array/map blocks use negative counts with byte size prefix
- **Big_Decimal**: Avro 1.12.0 logical type for variable-scale decimals where scale is stored in the value itself

## Requirements

### Requirement 1: Nanosecond Timestamp Logical Type Support

**User Story:** As a data engineer, I want to read Avro files containing nanosecond precision timestamps, so that I can process high-precision temporal data from modern Avro writers.

#### Acceptance Criteria

1. WHEN the Schema_Parser encounters a schema with `{"type": "long", "logicalType": "timestamp-nanos"}`, THE Schema_Parser SHALL parse it as a TimestampNanos logical type
2. WHEN the Schema_Parser encounters a schema with `{"type": "long", "logicalType": "local-timestamp-nanos"}`, THE Schema_Parser SHALL parse it as a LocalTimestampNanos logical type
3. WHEN the Arrow_Converter maps a TimestampNanos logical type, THE Arrow_Converter SHALL produce a Datetime type with Nanoseconds precision and UTC timezone
4. WHEN the Arrow_Converter maps a LocalTimestampNanos logical type, THE Arrow_Converter SHALL produce a Datetime type with Nanoseconds precision and no timezone
5. WHEN the Record_Decoder decodes a timestamp-nanos field, THE Record_Decoder SHALL decode the long value as nanoseconds since Unix epoch
6. WHEN the Record_Decoder decodes a local-timestamp-nanos field, THE Record_Decoder SHALL decode the long value as nanoseconds without timezone conversion
7. FOR ALL valid nanosecond timestamp values, serializing to JSON and parsing back SHALL produce equivalent schema objects (round-trip property)

### Requirement 2: Duration Logical Type Python Integration Test

**User Story:** As a developer, I want Python integration tests for the duration logical type, so that I can verify end-to-end correctness of duration handling.

#### Acceptance Criteria

1. WHEN a Python test reads an Avro file containing duration fields, THE Jetliner SHALL return a DataFrame with Duration columns
2. WHEN duration values are read, THE Jetliner SHALL correctly interpret the fixed[12] bytes as months, days, and milliseconds
3. WHEN duration values are converted to Polars Duration, THE Jetliner SHALL approximate months as 30 days for the conversion

### Requirement 3: UUID on Fixed[16] Python Integration Test

**User Story:** As a developer, I want Python integration tests for UUID stored as fixed[16], so that I can verify end-to-end correctness of binary UUID handling.

#### Acceptance Criteria

1. WHEN a Python test reads an Avro file containing UUID fields stored as fixed[16], THE Jetliner SHALL return a DataFrame with String columns containing valid UUID strings
2. WHEN UUID bytes are decoded, THE Jetliner SHALL correctly format the 16 bytes as a standard UUID string (8-4-4-4-12 format)

### Requirement 4: Negative Block Count Handling Tests

**User Story:** As a developer, I want explicit tests for negative block counts in arrays and maps, so that I can verify robustness against all valid Avro encodings.

#### Acceptance Criteria

1. WHEN an array block has a negative count, THE Avro_Reader SHALL interpret the absolute value as the item count and read the following long as byte size
2. WHEN a map block has a negative count, THE Avro_Reader SHALL interpret the absolute value as the entry count and read the following long as byte size
3. WHEN multiple blocks with mixed positive and negative counts are present, THE Avro_Reader SHALL correctly decode all blocks

### Requirement 5: Unknown Codec Error Message Test

**User Story:** As a developer, I want a test verifying helpful error messages for unknown codecs, so that users get actionable feedback when encountering unsupported compression.

#### Acceptance Criteria

1. WHEN an Avro file specifies an unknown codec, THE Avro_Reader SHALL return an error containing the codec name
2. WHEN an unknown codec error occurs, THE error message SHALL suggest checking if the codec feature is enabled

### Requirement 6: Schema Validation Edge Cases

**User Story:** As a developer, I want explicit tests for schema validation edge cases, so that I can verify the parser correctly rejects invalid schemas.

#### Acceptance Criteria

1. WHEN a namespace contains empty components (e.g., `a..b`), THE Schema_Parser in strict mode SHALL reject the schema with a clear error
2. WHEN an enum symbol starts with a digit (e.g., `1invalid`), THE Schema_Parser in strict mode SHALL reject the schema
3. WHEN a schema contains valid names and namespaces, THE Schema_Parser SHALL accept the schema in both strict and permissive modes

### Requirement 7: Complex Union Error Message

**User Story:** As a developer, I want a clear error message when encountering unsupported complex unions, so that users understand why their file cannot be read.

#### Acceptance Criteria

1. WHEN an Avro file contains a union with multiple non-null types (e.g., `["string", "int", "long"]`), THE Avro_Reader SHALL return an error indicating complex unions are not supported
2. WHEN a complex union error occurs, THE error message SHALL indicate the number of non-null variants in the union

### Requirement 8: Big-Decimal Logical Type Support

**User Story:** As a data engineer, I want to read Avro files containing big-decimal logical types, so that I can process variable-scale decimal data from Avro 1.12.0+ writers.

#### Acceptance Criteria

1. WHEN the Schema_Parser encounters a schema with `{"type": "bytes", "logicalType": "big-decimal"}`, THE Schema_Parser SHALL parse it as a BigDecimal logical type
2. WHEN the Arrow_Converter maps a BigDecimal logical type, THE Arrow_Converter SHALL produce a String type (to preserve exact decimal representation without precision loss)
3. WHEN the Record_Decoder decodes a big-decimal field, THE Record_Decoder SHALL:
   - Read the scale as a varint from the value bytes
   - Read the remaining bytes as the unscaled big-endian two's complement integer
   - Format the result as a decimal string with the correct scale
4. WHEN a big-decimal value is decoded, THE resulting string SHALL preserve the exact decimal representation (e.g., "123.45" for unscaled=12345, scale=2)
5. FOR ALL valid big-decimal values, the decoded string representation SHALL be parseable back to the original numeric value

