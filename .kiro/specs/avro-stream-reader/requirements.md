# Requirements Document

## Introduction

This document specifies requirements for a high-performance Rust library with Python bindings that streams Avro data from S3 or local filesystem into Polars DataFrames. The library enables processing of large Avro files without loading entire files into memory, targeting production use cases including AWS Lambda deployments. This is a read-only library - write support is explicitly out of scope.

## Glossary

- **Avro_Reader**: The core Rust component responsible for parsing Avro file format and deserializing records
- **Block**: An Avro data block containing a count, size, compressed data, and sync marker
- **Codec**: Compression algorithm used within Avro blocks (null, snappy, deflate, zstd, bzip2, xz)
- **DataFrame**: A Polars DataFrame containing deserialized Avro records
- **Stream_Source**: An abstraction over data sources (S3 or local filesystem)
- **Sync_Marker**: 16-byte marker used to identify block boundaries in Avro files
- **Schema**: Avro schema embedded in file header or provided externally
- **Batch_Iterator**: Python generator that yields DataFrames from streamed Avro blocks
- **Prefetch_Buffer**: Internal buffer that asynchronously loads upcoming blocks

## Requirements

### Requirement 1: Avro File Format Parsing

**User Story:** As a data engineer, I want to read any valid Avro file, so that I can process data regardless of how it was produced.

#### Acceptance Criteria

1. WHEN an Avro file is opened, THE Avro_Reader SHALL parse the file header and extract the embedded schema
2. WHEN an Avro file is opened, THE Avro_Reader SHALL validate the magic bytes match the Avro specification
3. WHEN reading blocks, THE Avro_Reader SHALL validate sync markers match the file header's sync marker
4. THE Avro_Reader SHALL support all Avro primitive types: null, boolean, int, long, float, double, bytes, string
5. THE Avro_Reader SHALL support all Avro complex types: records, enums, arrays, maps, unions, fixed
6. THE Avro_Reader SHALL support logical types: decimal, uuid, date, time-millis, time-micros, timestamp-millis, timestamp-micros, duration
7. WHEN a schema contains named types, THE Avro_Reader SHALL resolve type references correctly
8. THE Pretty_Printer SHALL format Avro schemas as human-readable JSON
9. FOR ALL valid Avro schemas, parsing then printing then parsing SHALL produce an equivalent schema object (round-trip property)

### Requirement 2: Codec Support

**User Story:** As a data engineer, I want to read Avro files regardless of compression codec, so that I can work with data from various sources.

#### Acceptance Criteria

1. THE Avro_Reader SHALL support the null (uncompressed) codec
2. THE Avro_Reader SHALL support the snappy codec for block decompression
3. THE Avro_Reader SHALL support the deflate codec for block decompression
4. THE Avro_Reader SHALL support the zstd codec for block decompression
5. THE Avro_Reader SHALL support the bzip2 codec for block decompression
6. THE Avro_Reader SHALL support the xz codec for block decompression
7. IF an unknown codec is encountered, THEN THE Avro_Reader SHALL return a descriptive error identifying the codec

### Requirement 3: Streaming Block-by-Block Reading

**User Story:** As a data engineer, I want to process large Avro files without loading them entirely into memory, so that I can handle files larger than available RAM.

#### Acceptance Criteria

1. WHEN streaming is initiated, THE Avro_Reader SHALL read and process one block at a time
2. THE Avro_Reader SHALL NOT load the entire file into memory before processing begins
3. WHEN a block is processed, THE Avro_Reader SHALL release memory from previous blocks
4. THE Batch_Iterator SHALL yield DataFrames containing records from one or more blocks up to a configurable row limit
5. WHILE streaming, THE Prefetch_Buffer SHALL asynchronously load upcoming blocks up to a configurable buffer limit
6. WHEN the buffer is full, THE Prefetch_Buffer SHALL pause fetching until space is available
7. THE Avro_Reader SHALL support seeking to a specific block by sync marker for resumable reads

### Requirement 4: Data Source Abstraction

**User Story:** As a data engineer, I want to read Avro files from both S3 and local filesystem using the same API, so that I can write portable code.

#### Acceptance Criteria

1. THE Stream_Source SHALL provide a unified interface for S3 and local filesystem access
2. WHEN an S3 URI is provided, THE Stream_Source SHALL use AWS SDK for data access
3. WHEN a local file path is provided, THE Stream_Source SHALL use filesystem APIs for data access
4. THE Stream_Source SHALL support range requests for efficient partial file reads
5. WHEN reading from S3, THE Stream_Source SHALL authenticate using environment credentials (access key/secret or web identity tokens)
6. IF authentication fails, THEN THE Stream_Source SHALL return a descriptive error
7. IF the file does not exist, THEN THE Stream_Source SHALL return a descriptive error

### Requirement 5: Polars DataFrame Integration

**User Story:** As a Python developer, I want Avro data loaded directly into Polars DataFrames, so that I can use familiar tools for data analysis.

#### Acceptance Criteria

1. THE Avro_Reader SHALL construct Polars DataFrames entirely in Rust to minimize cross-language overhead
2. THE Avro_Reader SHALL deserialize Avro data directly into Polars Arrow-backed column builders
3. THE Avro_Reader SHALL avoid intermediate data representations between Avro bytes and Polars columns
4. WHEN converting types, THE Avro_Reader SHALL map Avro types to appropriate Polars types
5. THE Avro_Reader SHALL preserve null values from Avro unions containing null
6. WHEN an Avro array is encountered, THE Avro_Reader SHALL convert it to a Polars List column
7. WHEN an Avro map is encountered, THE Avro_Reader SHALL convert it to a Polars List column containing Structs with "key" and "value" fields
8. WHEN an Avro record is nested, THE Avro_Reader SHALL convert it to a Polars Struct column
9. FOR ALL Avro records, converting to DataFrame and back SHALL preserve data values (round-trip property where applicable)

### Requirement 6: Python API Ergonomics

**User Story:** As a Python developer, I want a clean, Pythonic API, so that the library feels natural to use.

#### Acceptance Criteria

1. THE Batch_Iterator SHALL implement Python's iterator protocol (__iter__, __next__)
2. WHEN iteration completes, THE Batch_Iterator SHALL properly release resources
3. THE Python API SHALL accept configuration via keyword arguments with sensible defaults
4. THE Python API SHALL raise appropriate Python exceptions with descriptive messages
5. WHEN an error occurs during iteration, THE Batch_Iterator SHALL include context about the block and record position
6. THE Python API SHALL support context manager protocol for resource cleanup

### Requirement 7: Error Handling and Resilience

**User Story:** As a data engineer, I want to recover as much data as possible from partially corrupted files, so that I minimize data loss.

#### Acceptance Criteria

1. IF a block fails validation, THEN THE Avro_Reader SHALL log the error with block position and continue to the next block
2. IF a record within a block fails deserialization, THEN THE Avro_Reader SHALL log the error and skip to the next record
3. WHEN errors are skipped, THE Avro_Reader SHALL track error counts and positions
4. THE Avro_Reader SHALL provide a summary of skipped errors after iteration completes
5. WHERE strict mode is enabled, THE Avro_Reader SHALL fail immediately on any error
6. THE Avro_Reader SHALL distinguish between recoverable errors (bad data) and fatal errors (I/O failure)
7. WHEN logging errors, THE Avro_Reader SHALL include sufficient detail to diagnose the issue

### Requirement 8: Performance Optimization

**User Story:** As a data engineer, I want the fastest possible data loading with minimal memory usage, so that I can minimize processing time and Lambda costs.

#### Acceptance Criteria

1. THE Avro_Reader SHALL minimize memory allocations during deserialization
2. THE Avro_Reader SHALL maintain bounded memory usage proportional to batch size and buffer configuration, not file size
3. THE Avro_Reader SHALL release block memory promptly after DataFrame conversion
4. THE Avro_Reader SHALL use SIMD operations where beneficial for decompression
5. WHILE reading from S3, THE Stream_Source SHALL use concurrent range requests for prefetching
6. THE Avro_Reader SHALL support configurable parallelism for block decompression
7. THE Avro_Reader SHALL avoid unnecessary data copies between Rust and Python
8. WHEN creating DataFrames, THE Avro_Reader SHALL use zero-copy transfers where possible

### Requirement 9: Schema Handling

**User Story:** As a data engineer, I want flexibility in how schemas are handled, so that I can work with various data evolution scenarios.

#### Acceptance Criteria

1. THE Avro_Reader SHALL extract and use the embedded schema by default
2. WHERE an external reader schema is provided, THE Avro_Reader SHALL use schema resolution rules per Avro spec
3. THE Avro_Reader SHALL expose the parsed schema for inspection
4. WHEN schema resolution fails, THE Avro_Reader SHALL return a descriptive error explaining the incompatibility
5. THE Avro_Reader SHALL cache parsed schemas to avoid repeated parsing

### Requirement 10: Testing and Verification

**User Story:** As a maintainer, I want comprehensive test coverage, so that I can confidently make changes without breaking functionality.

#### Acceptance Criteria

1. THE test suite SHALL include Apache Avro's official interoperability test files
2. THE test suite SHALL verify correct handling of all primitive types
3. THE test suite SHALL verify correct handling of all complex types
4. THE test suite SHALL verify correct handling of all logical types
5. THE test suite SHALL verify correct handling of all supported codecs
6. THE test suite SHALL include property-based tests for schema parsing round-trips
7. THE test suite SHALL include property-based tests for data serialization round-trips
8. THE test suite SHALL include performance benchmarks with reproducible results
