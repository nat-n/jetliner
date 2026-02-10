# Implementation Plan: Jetliner

## Overview

This plan implements Jetliner, a high-performance Rust library with Python bindings for streaming Avro data into Polars DataFrames. Named after the Avro Jetliner, the library emphasizes speed and streaming. The implementation follows a bottom-up approach: core types → parsing → codecs → sources → streaming → Python bindings.

## Tasks

- [x] 1. Project setup and core types
  - [x] 1.1 Initialize Rust project with Cargo.toml and pyproject.toml
    - Configure workspace with maturin build system
    - Add all dependencies (tokio, pyo3, polars, compression crates, aws-sdk-s3)
    - Set up feature flags for optional codecs
    - _Requirements: 10.1_

  - [x] 1.2 Define core error types
    - Implement ReaderError, SourceError, CodecError, SchemaError, DecodeError
    - Implement ReadError struct for recoverable errors
    - Use thiserror for error derivation
    - _Requirements: 7.5, 7.6_

  - [x] 1.3 Define Avro schema types
    - Implement AvroSchema enum with all primitive types
    - Implement RecordSchema, FieldSchema, EnumSchema, FixedSchema
    - Implement LogicalType wrapper and LogicalTypeName enum
    - _Requirements: 1.4, 1.5, 1.6_

- [x] 2. Schema parsing and pretty printing
  - [x] 2.1 Implement JSON schema parser
    - Parse primitive type strings
    - Parse complex type objects (record, enum, array, map, union, fixed)
    - Parse logical type annotations
    - Handle named type references with resolution context
    - _Requirements: 1.1, 1.7_

  - [x] 2.2 Implement schema pretty printer (to_json)
    - Serialize AvroSchema back to canonical JSON
    - Handle named type references correctly
    - _Requirements: 1.8_

  - [x] 2.3 Write property test for schema round-trip
    - **Property 1: Schema Round-Trip**
    - **Validates: Requirements 1.9**

- [x] 3. Codec implementations
  - [x] 3.1 Implement Codec enum and null codec
    - Implement Codec::from_name parser
    - Implement null codec (passthrough)
    - _Requirements: 2.1, 2.7_

  - [x] 3.2 Implement snappy codec
    - Use snap crate for decompression
    - Handle Avro's snappy framing (4-byte CRC suffix)
    - _Requirements: 2.2_

  - [x] 3.3 Implement deflate codec
    - Use flate2 crate for decompression
    - _Requirements: 2.3_

  - [x] 3.4 Implement zstd codec
    - Use zstd crate for decompression
    - _Requirements: 2.4_

  - [x] 3.5 Implement bzip2 codec
    - Use bzip2 crate for decompression
    - _Requirements: 2.5_

  - [x] 3.6 Implement xz codec
    - Use xz2 crate for decompression
    - _Requirements: 2.6_

  - [x] 3.7 Write property test for codec round-trip
    - **Property 4: All Codecs Decompress Correctly**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6**

- [x] 4. Checkpoint - Core types and codecs
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Stream source abstraction
  - [x] 5.1 Define StreamSource trait
    - Define async read_range, size, read_from methods
    - _Requirements: 4.1, 4.4_

  - [x] 5.2 Implement LocalSource
    - Use tokio::fs::File for async file I/O
    - Implement range reads via seek + read
    - _Requirements: 4.3_

  - [x] 5.3 Implement S3Source
    - Use aws-sdk-s3 for S3 access
    - Implement range requests via GetObject with Range header
    - Handle authentication via environment credentials
    - _Requirements: 4.2, 4.5, 4.6, 4.7_

  - [x] 5.4 Write property test for range requests
    - **Property 8: Range Requests Return Correct Data**
    - **Validates: Requirements 4.4**

- [x] 6. Avro header and block parsing
  - [x] 6.1 Implement AvroHeader parsing
    - Parse magic bytes and validate
    - Parse metadata map (schema, codec)
    - Extract sync marker
    - _Requirements: 1.1, 1.2_

  - [x] 6.2 Implement AvroBlock parsing
    - Parse block record count (varint)
    - Parse block compressed size (varint)
    - Read compressed data
    - Validate sync marker
    - _Requirements: 1.3, 3.1_

  - [x] 6.3 Implement BlockReader
    - Orchestrate header parsing and block iteration
    - Track current offset and block index
    - Implement seek_to_sync for resumable reads
    - _Requirements: 3.1, 3.7_

  - [x] 6.4 Write property test for sync marker validation
    - **Property 5: Sync Marker Validation**
    - **Validates: Requirements 1.3**

- [x] 7. Checkpoint - Parsing layer
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Record decoding and Arrow conversion
  - [x] 8.1 Implement Avro binary decoder for primitives
    - Decode null, boolean, int (varint), long (varint)
    - Decode float, double (little-endian)
    - Decode bytes, string (length-prefixed)
    - _Requirements: 1.4_

  - [x] 8.2 Implement Avro binary decoder for complex types
    - Decode records (field sequence)
    - Decode enums (varint index)
    - Decode arrays (block encoding with count)
    - Decode maps (block encoding with key-value pairs)
    - Decode unions (varint index + value)
    - Decode fixed (raw bytes)
    - _Requirements: 1.5_

  - [x] 8.3 Implement logical type decoding
    - Decode decimal (bytes with precision/scale)
    - Decode uuid (fixed or string)
    - Decode date, time-millis, time-micros
    - Decode timestamp-millis, timestamp-micros
    - Decode duration
    - _Requirements: 1.6_

  - [x] 8.4 Implement schema type resolution for named types
    - Build name resolution context during parsing
    - Resolve Named references during decoding
    - _Requirements: 1.7_

  - [x] 8.5 Write property test for named type resolution
    - **Property 6: Named Type Resolution**
    - **Validates: Requirements 1.7**

  - [x] 8.6 Write property test for all types deserialize
    - **Property 3: All Avro Types Deserialize Correctly**
    - **Validates: Requirements 1.4, 1.5, 1.6**

- [x] 9. Arrow/Polars integration
  - [x] 9.1 Implement Avro to Arrow type mapping
    - Map primitives to Arrow types
    - Map complex types (record→Struct, enum→Dictionary, array→LargeList, map→LargeList<Struct>)
    - Map logical types to Arrow types
    - _Requirements: 5.4_

  - [x] 9.2 Implement RecordDecode trait and FullRecordDecoder ✓
    - Define RecordDecode trait with decode_record, finish_batch, pending_records
    - Implement FullRecordDecoder for non-projected reads (zero overhead)
    - Create appropriate ArrayBuilder for each field
    - Decode records directly into builders (no intermediate Value)
    - _Requirements: 5.1, 5.2, 5.3_

  - [x] 9.3 Implement ProjectedRecordDecoder
    - Implement ProjectedRecordDecoder for projected reads
    - Only create builders for projected columns
    - Implement skip_field() for non-projected columns
    - Handle nullable fields from unions
    - _Requirements: 5.5, 6a.2_

  - [x] 9.4 Implement RecordDecoder enum factory
    - Create RecordDecoder enum wrapping Full/Projected variants
    - Implement RecordDecoder::new() factory that chooses variant
    - Delegate trait methods to inner decoder
    - _Requirements: 6a.2_

  - [x] 9.5 Implement DataFrameBuilder
    - Convert Arrow arrays to Polars DataFrame
    - Handle batch size limits
    - Track and report errors in skip mode
    - _Requirements: 5.6, 5.7, 5.8_

  - [x] 9.6 Write property test for null preservation
    - **Property 9: Null Preservation in Unions**
    - **Validates: Requirements 5.5**

  - [x] 9.7 Write property test for data round-trip
    - **Property 2: Data Round-Trip with Type Preservation**
    - **Validates: Requirements 5.4, 5.5, 5.6, 5.7, 5.8, 5.9**

- [x] 10. Checkpoint - Decoding and conversion
  - Ensure all tests pass, ask the user if questions arise.

  - [x] 10.1 Implement snappy CRC32C validation
    - Add crc32c crate dependency
    - Validate CRC32C checksum after snappy decompression
    - Return CodecError on checksum mismatch with descriptive message
    - _Requirements: 2.2, 7.6_

  - [x] 10.2 Add "zstandard" codec name alias
    - Accept both "zstd" and "zstandard" in Codec::from_name
    - The Avro spec uses "zstandard" as the canonical name
    - _Requirements: 2.4, 2.7_

  - [x] 10.3 Implement optional strict schema validation mode
    - Add `strict_schema: bool` option to parser (default: false)
    - When enabled, validate union rules (no duplicate types, no nested unions)
    - When enabled, validate names follow Avro naming rules
    - Log warnings for violations when strict mode is disabled
    - For read-only use, permissive parsing maximizes compatibility with existing files
    - _Requirements: 1.4, 1.5 (optional strictness)_

  - [x] 10.4 Distinguish local timestamps from UTC timestamps in Arrow mapping
    - Map timestamp-millis/micros to Arrow Timestamp with UTC timezone
    - Map local-timestamp-millis/micros to Arrow Timestamp without timezone
    - Ensure Polars Datetime reflects the timezone distinction
    - _Requirements: 1.6, 5.4_

  - [x] 10.5 Consolidate duplicate varint decoding code
    - Create shared varint module in src/reader/varint.rs
    - Move decode_varint, decode_zigzag, encode_varint functions to shared module
    - Update header.rs, block.rs, decode.rs to use shared module
    - Remove duplicate implementations
    - _Requirements: 8.1 (code quality)_

  - [x] 10.6 Write unit tests for spec compliance fixes
    - Test snappy CRC32C validation with valid and corrupted data
    - Test "zstandard" codec name acceptance
    - Test strict schema validation mode (when enabled)
    - Test local timestamp Arrow type mapping
    - _Requirements: 10.2, 10.3, 10.4, 10.5_

- [x] 11. Streaming infrastructure
  - [x] 11.1 Implement PrefetchBuffer
    - Async block prefetching with configurable limits
    - Backpressure when buffer is full
    - Decompress blocks in background
    - _Requirements: 3.5, 3.6_

  - [x] 11.2 Implement AvroStreamReader
    - Orchestrate buffer, decoder, and builder
    - Implement next_batch async method
    - Track finished state and accumulated errors
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

  - [x] 11.3 Write property test for batch size limit
    - **Property 7: Batch Size Limit Respected**
    - **Validates: Requirements 3.4**

- [x] 12. Error handling modes
  - [x] 12.1 Implement strict mode
    - Fail immediately on any error
    - Propagate errors up the call stack
    - _Requirements: 7.5_

  - [x] 12.2 Implement skip mode (resilient reading)
    - Skip bad blocks, continue to next sync marker
    - Skip bad records within blocks
    - Track error counts and positions
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [x] 12.3 Write property test for resilient reading
    - **Property 10: Resilient Reading Skips Bad Data**
    - **Validates: Requirements 7.1, 7.2, 7.3**

  - [x] 12.4 Write property test for strict mode
    - **Property 11: Strict Mode Fails on First Error**
    - **Validates: Requirements 7.5**

  - [x] 12.5 Fix sync marker recovery in skip mode
    - Implement `BlockReader::skip_to_next_sync()` method for error recovery
    - Method should scan forward from current position to find next valid sync marker
    - Update `PrefetchBuffer` to use hybrid recovery approach for InvalidSyncMarker errors:
      1. First try optimistic: advance past invalid sync marker and read next block
      2. If that fails, fall back to scanning from byte 1 of invalid sync marker position
    - Log descriptive error with block index, file offset, expected vs actual sync marker bytes
    - Include bytes skipped count when recovering to next sync marker
    - Ensure property tests `prop_resilient_reading_skips_bad_blocks` and `prop_resilient_reading_multiple_corruptions` pass
    - _Requirements: 7.1, 7.2, 7.7_

  - [x] 12.6 Enhance error details for diagnostics
    - Add expected vs actual sync marker bytes to InvalidSyncMarker error
    - Include hex representation of both markers in error message for easy comparison
    - Add codec name to DecompressionFailed errors
    - Update ReadError message formatting to include these details
    - _Requirements: 7.7_

- [x] 13. Schema resolution (reader schema support)
  - [x] 13.1 Implement schema compatibility checking
    - Check reader/writer schema compatibility per Avro spec
    - Return descriptive errors for incompatibilities
    - _Requirements: 9.4_

  - [x] 13.2 Implement schema resolution during decoding
    - Handle field defaults for missing fields
    - Handle field reordering
    - Handle type promotions (int→long, float→double)
    - _Requirements: 9.2_

  - [x] 13.3 Write property test for schema resolution ✓
    - **Property 12: Schema Resolution with Reader Schema**
    - **Validates: Requirements 9.2**

- [x] 14. Checkpoint - Streaming and error handling
  - Ensure all tests pass, ask the user if questions arise.

- [x] 15. Python bindings
  - [x] 15.1 Implement AvroReaderCore PyClass (internal)
    - Constructor with path, configuration kwargs, and projected_columns
    - Implement __iter__ and __next__ for sync iteration
    - Support projection pushdown via projected_columns parameter
    - _Requirements: 6.1, 6.2, 6a.2_

  - [x] 15.2 Implement AvroReader PyClass (user-facing open() API)
    - Wrapper around AvroReaderCore without projection
    - Implement __enter__ and __exit__ for context manager
    - _Requirements: 6.1, 6.2, 6.6_

  - [x] 15.3 Implement schema and error accessors
    - Expose schema as JSON string and dict
    - Implement read_avro_schema() for IO plugin schema extraction
    - _Requirements: 9.3, 6a.5_

  - [x] 15.3a Implement structured error exposure for Python
    - Create ReadError PyClass with properties: kind, block_index, record_index, offset, message
    - Add `to_dict()` method returning error as Python dict
    - Expose `reader.errors` property returning list of ReadError objects (accumulated during read)
    - Expose `reader.error_count` property for quick check without iterating
    - Errors accumulate during iteration and are available after reading completes
    - Keep normal iteration API simple - no callbacks or exceptions for skipped errors
    - Example usage:
      ```python
      with jetliner.AvroReader("file.avro", ignore_errors=True) as reader:
          for df in reader:
              process(df)
          if reader.error_count > 0:
              for err in reader.errors:
                  print(f"Block {err.block_index}: {err.message}")
      ```
    - _Requirements: 7.3, 7.4, 7.7_

  - [x] 15.4 Implement Python exception types
    - Define custom exception classes (ParseError, SchemaError, etc.)
    - Map Rust errors to appropriate Python exceptions
    - Include context in error messages
    - _Requirements: 6.4, 6.5_

  - [x] 15.5 Implement AvroReader class
    - Parse S3 URIs vs local paths
    - Create appropriate source and reader
    - _Requirements: 4.1, 4.2, 4.3_

  - [x] 15.6 Implement jetliner.scan() with IO plugin
    - Implement scan() function returning LazyFrame via register_io_source
    - Create source_generator that accepts with_columns, predicate, n_rows, batch_size
    - Pass projected_columns to AvroReaderCore for builder-level filtering
    - Apply predicate filter to each yielded DataFrame
    - Implement early stopping when n_rows limit reached
    - _Requirements: 6a.1, 6a.2, 6a.3, 6a.4, 6a.5, 6a.6_

  - [x] 15.7 Write property test for projection correctness
    - **Property 14: Projection Preserves Selected Columns**
    - **Validates: Requirements 6a.2**

  - [x] 15.8 Write property test for early stopping
    - **Property 15: Early Stopping Respects Row Limit**
    - **Validates: Requirements 6a.4**

  - [x] 15.9 Write unit tests for Python API
    - Test iterator protocol (open API)
    - Test context manager
    - Test error handling
    - Test scan() returns LazyFrame
    - Test projection pushdown reduces memory
    - Test predicate pushdown filters correctly
    - Test early stopping with head()
    - _Requirements: 6.1, 6.2, 6.4, 6.5, 6.6, 6a.1, 6a.2, 6a.3, 6a.4_

- [x] 16. Seek functionality
  - [x] 16.1 Implement seek_to_sync in BlockReader
    - Scan for sync marker from given position
    - Resume reading from found position
    - _Requirements: 3.7_

  - [x] 16.2 Write property test for seek to sync marker
    - **Property 13: Seek to Sync Marker**
    - **Validates: Requirements 3.7**

- [x] 17. Integration tests with Apache Avro test files
  - [x] 17.1 Add Apache Avro interoperability test files ✓
    - See Appendix: A_avro_java_test_research.md
    - See Appendix: B_e2e-test-plan.md
    - Downloaded official test files from Apache Avro and fastavro
    - Created license attribution files (Apache 2.0, MIT)
    - Verify all primitive types (weather files)
    - Verify all complex types (recursive, arrays, maps)
    - Verify all logical types (UUID via java-generated-uuid.avro)
    - Verify all codecs (null, deflate, snappy, zstd)
    - 166 tests passing, 16 xfailed for known Polars limitations
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

  - [x] 17.2 Fix interoperability gaps discovered in E2E testing
    - [x] 17.2.1 Fix snappy codec returning 0 records
      - Root cause: Avro uses CRC32 (ISO polynomial), not CRC32C (Castagnoli)
      - Fixed by replacing crc32c crate with crc32fast crate
      - Updated all tests to use CRC32 instead of CRC32C
      - Snappy-compressed files now produce same data as uncompressed
      - Removed xfail markers from snappy tests
      - _Requirements: 2.2, 10.5_

    - [x] 17.2.2 Support non-record top-level schemas
      - Currently fails with "Top-level schema must be a record type"
      - Add support for reading files where top-level schema is null, array, or other types
      - Test with fastavro/null.avro
      - Remove xfail marker once fixed
      - Make sure it works when loading a file with non-record top level schema into a polars dataframe, the non-record value should be read into a single column dataframe with a column called 'value'
      - _Requirements: 1.4, 1.5_

    - [x] 17.2.3 Fix recursive type resolution ✓
      - Implemented JSON serialization approach for recursive types (Arrow/Polars doesn't support recursive structures)
      - Modified `src/convert/arrow.rs` to return `DataType::String` for `Named` types
      - Added `RecursiveBuilder` in `src/reader/record_decoder.rs` that uses `decode_value_with_context` to decode recursive structures and serialize to JSON
      - Added `to_json()` method to `AvroValue` in `src/reader/decode.rs` for JSON serialization
      - Test with fastavro/recursive.avro passes - linked list pattern correctly serialized to JSON strings
      - Removed xfail markers from tests
      - _Requirements: 1.7_

  - [x] 17.3 Multi-block and large file testing
    - See Appendix: C_functional_coverage_gap_analysis.md (Gap 1)
    - [x] 17.3.1 Generate large test file (10K+ records)
      - Create Python script using fastavro to generate file spanning multiple blocks
      - Use weather schema for consistency with existing tests
      - Include variety of record sizes to ensure multiple blocks
      - _Requirements: 3.1, 3.4_

    - [x] 17.3.2 Add multi-block E2E tests
      - Verify correct total record count across all blocks
      - Verify batch size respected across block boundaries
      - Verify sync markers validated between blocks
      - _Requirements: 3.1, 3.4, 1.3_

    - [x] 17.3.3 Add memory efficiency tests
      - Verify memory doesn't grow unbounded with file size
      - Verify memory bounded by batch_size and buffer config
      - _Requirements: 8.2, 3.5, 3.6_

    - [x] 17.3.4 Add large file stress test with RSS memory verification
      - Add psutil as dev dependency for process memory tracking
      - Generate 1GB test file on-demand (or parameterized: 100MB, 500MB, 1GB)
      - Track peak RSS during streaming read to verify memory stays bounded
      - Assert peak RSS < threshold (e.g., 500MB for 1GB file)
      - Verify data integrity (record count matches expected)
      - Measure and report throughput (MB/s, records/s)
      - Mark as @pytest.mark.slow for CI exclusion
      - _Requirements: 8.2, 3.5, 3.6_

  - [x] 17.4 Implement missing property tests
    - See Appendix: C_functional_coverage_gap_analysis.md (Gap 2)
    - [x] 17.4.1 Implement Property 14: Projection Preserves Selected Columns
      - Generate random schemas and column subsets
      - Verify projected read equals full read + select
      - **Property 14: Projection Preserves Selected Columns**
      - **Validates: Requirements 6a.2**

    - [x] 17.4.2 Implement Property 15: Early Stopping Respects Row Limit
      - Generate random files and row limits
      - Verify at most N rows returned
      - Verify rows are first N rows of file
      - **Property 15: Early Stopping Respects Row Limit**
      - **Validates: Requirements 6a.4**

  - [x] 17.5 Error recovery E2E tests with corrupted files
    - See Appendix: C_functional_coverage_gap_analysis.md (Gap 3)
    - [x] 17.5.1 Generate corrupted test files
      - Create Python script to generate files with specific corruption patterns
      - File with invalid magic bytes
      - Truncated file (EOF mid-block)
      - File with corrupted sync marker
      - File with corrupted compressed data
      - File with invalid record data
      - _Requirements: 7.1, 7.2_

    - [x] 17.5.2 Add skip mode recovery tests
      - Test recovery from each corruption type
      - Verify valid data before/after corruption is read
      - Verify error tracking (error_count, errors list)
      - _Requirements: 7.1, 7.2, 7.3, 7.4_

    - [x] 17.5.3 Add strict mode failure tests
      - Verify immediate failure on each corruption type
      - Verify descriptive error messages
      - _Requirements: 7.5, 7.7_

  - [x] 17.6 Edge case value testing
    - See Appendix: C_functional_coverage_gap_analysis.md (Gap 5)
    - [x] 17.6.1 Generate edge case test file
      - Create Python script using fastavro to generate file with boundary values
      - Max/min int32, int64 values
      - NaN, Infinity, -Infinity for floats
      - Empty strings, empty bytes, empty arrays, empty maps
      - Very long strings (> 64KB)
      - Unicode edge cases (emoji, RTL, combining characters)
      - _Requirements: 1.4, 1.5_

    - [x] 17.6.2 Add edge case E2E tests
      - Verify all boundary values read correctly
      - Verify empty collections handled properly
      - Verify Unicode preserved correctly
      - _Requirements: 1.4, 1.5_

- [ ] 18. Performance benchmarks
  - [x] 18.1 Create benchmark suite with criterion
    - Benchmark read throughput (records/sec)
    - Benchmark memory usage
    - Benchmark with different codecs
    - Benchmark with different batch sizes
    - _Requirements: 10.8_

  - [x] 18.2 Create comparative benchmark suite
    - Compare jetliner against polars read_avro, fastavro, fastavro+pandas, apache-avro
    - Use pytest-benchmark for timing with statistical rigor
    - Track peak memory (RSS delta via psutil) alongside timing
    - All test files use snappy codec
    - _Requirements: 10.8_

    - [x] 18.2.1 Add benchmark dependencies
      - Add apache-avro as dev dependencies
      - Ensure psutil is available (already a dev dependency)
      - _Requirements: 10.8_

    - [x] 18.2.2 Create benchmark data generator
      - Generate files to `benches/data/` (gitignored)
      - Skip generation if files already exist
      - Use fastavro with snappy codec for all files
      - Schemas:
        - small: 100 records, simple schema (id, name, value)
        - large_simple: 1M records, 1 string + 4 numeric columns
        - large_wide: 1M records, 100 columns (mixed types)
        - large_complex: 1M records, nested structs/arrays/maps (no recursion)
      - _Requirements: 10.8_

    - [x] 18.2.3 Implement benchmark fixtures
      - Create fixtures for each library: jetliner, polars, fastavro, fastavro+pandas, apache-avro, polars-avro
      - Each fixture reads file and returns DataFrame/records
      - Add memory tracking fixture using psutil RSS delta
      - _Requirements: 10.8_

    - [x] 18.2.4 Implement benchmark scenarios
      - Scenario: read_small (100 records) - measures overhead/startup
      - Scenario: read_large_simple (1M records, 5 cols) - raw throughput
      - Scenario: read_large_wide (1M records, 100 cols) - column handling
      - Scenario: read_large_complex (1M records, nested) - complex type handling
      - Scenario: read_with_projection (1M records, select 5 of 100 cols) - projection pushdown
      - _Requirements: 10.8_

    - [x] 18.2.5 Create benchmark runner and reporting
      - Add poe task for running comparative benchmarks
      - Generate comparison table (time + memory per library per scenario)
      - Output results to console and optionally JSON for CI tracking
      - _Requirements: 10.8_

  - [x] 18.3 Performance Optimization: Pre-allocate Builders (Optimization 1)
    - [x] 18.3.1 Add reserve() to all FieldBuilder variants
      - Add reserve() to primitive builders (Boolean, Int32, Int64, Float32, Float64, etc.)
      - Add reserve() to Binary and String builders
      - Add no-op reserve() to NullBuilder
      - Add recursive reserve() to NullableBuilder, StructBuilder
      - Add reserve() to List, Map, Enum, Fixed, Recursive builders
      - Add reserve() dispatch to FieldBuilder enum
      - _Requirement: Optimization 1 from D_optimization-analysis.md_

    - [x] 18.3.2 Add reserve_for_batch() to decoders
      - Add reserve_for_batch() to FullRecordDecoder
      - Add reserve_for_batch() to RecordDecoder wrapper
      - Call from DataFrameBuilder::add_block() before decode loop
      - _Requirement: Optimization 1 from D_optimization-analysis.md_

    - [x] 18.3.3 Add reserve calls in array/map decode
      - Call reserve() in ListBuilder::decode() after reading item_count
      - Call reserve() in MapBuilder::decode() after reading entry_count
      - _Requirement: Optimization 1 from D_optimization-analysis.md_

    - [x] 18.3.4 Testing and verification
      - All Rust tests pass (363 tests)
      - All Python tests pass (283 tests, 11 xpassed)
      - Benchmark results: 2.2% improvement on large_complex (1.338s → 1.308s)
      - Still 15% slower than polars-avro (1.308s vs 1.139s = 169ms gap)
      - _Requirement: Optimization 1 verification_

  - [ ] 18.4 Performance Optimization: Direct Arrow Array Construction for Variable-Length Types (Optimization 4)
    - See Appendix: D1_varlen-builder-optimization.md
    - [x] 18.4.1 Implement optimized BinaryBuilder
      - Replace `Vec<Vec<u8>>` with contiguous data buffer + offsets array
      - Use `polars_arrow::array::BinaryArray` for direct construction
      - Pattern: append bytes to single buffer, track offsets, build array at finish()
      - Reference: ListBuilder implementation in same file (commit 34e9288)
      - _Requirements: 8.1, 5.3_

    - [x] 18.4.2 Implement optimized StringBuilder
      - Replace `Vec<String>` with contiguous data buffer + offsets array
      - Use `polars_arrow::array::Utf8Array` for direct construction
      - Validate UTF-8 during decode or at finish (choose based on performance)
      - _Requirements: 8.1, 5.3_

    - [ ] 18.4.3 Testing and verification
      - Unit tests: empty values, long values (>64KB), Unicode edge cases
      - Property tests: verify optimized output == original implementation
      - Run `poe bench-compare` to verify ~40-50% improvement on variable-length columns
      - Verify `large_wide` benchmark shows improvement (currently 4.4s)
      - _Requirements: 10.7, 10.8_

- [x] 19. storage_options support for S3-compatible services
  - [x] 19.1 Add S3Config struct to Rust S3Source
    - Define S3Config with endpoint_url, aws_access_key_id, aws_secret_access_key, region
    - Update S3Source::new() to accept optional S3Config
    - Configure AWS SDK client with custom endpoint when provided
    - _Requirements: 4.8, 4.9, 4.10_

  - [x] 19.2 Add storage_options parameter to Python API
    - Add storage_options: dict[str, str] | None to scan() and open()
    - Parse storage_options dict and pass to Rust S3Config
    - Ensure storage_options takes precedence over environment variables
    - _Requirements: 4.8, 4.11_

  - [x] 19.3 Write tests for S3-compatible service support
    - Test with MinIO endpoint_url
    - Test credential override via storage_options
    - Test that storage_options takes precedence over environment
    - _Requirements: 4.9, 4.10, 4.11, 4.12_

- [x] 20. Extended type coverage tests
  - [x] 20.1 Add tests for logical types
    - Test date, time-millis, time-micros, timestamp-millis, timestamp-micros
    - Test uuid (string logical type)
    - Test decimal (bytes with precision/scale)
    - Some logical types have value interpretation issues (xfail)
    - _Requirements: 1.6_

  - [x] 20.2 Add tests for enum and fixed types
    - Test enum as Categorical type
    - Test fixed as Binary type
    - Currently not implemented (xfail)
    - _Requirements: 1.4, 1.5_

  - [x] 20.3 Add tests for nullable complex types
    - Test nullable arrays and records
    - Currently fails with null mask error (xfail)
    - _Requirements: 5.5_

- [ ] 21. Fix nullable complex types
  - [x] 21.1 Investigate null mask error for nullable arrays/records
    - Debug the "Failed to apply null mask: failed to determine supertype" error
    - Identify root cause in `src/reader/record_decoder.rs` union handling
    - Document findings in devnotes
    - _Requirements: 5.5_

  - [x] 21.2 Fix ListBuilder null handling for nullable arrays
    - Ensure ListBuilder correctly handles null values in unions
    - Fix null mask application for List types
    - Test with `["null", {"type": "array", "items": "int"}]` schema
    - Input: 21.1-nullable-complex-types-investigation.md
    - _Requirements: 5.5_

  - [x] 21.3 Fix StructBuilder null handling for nullable records
    - Ensure StructBuilder correctly handles null values in unions
    - Fix null mask application for Struct types
    - Test with `["null", {"type": "record", ...}]` schema
    - Input: 21.1-nullable-complex-types-investigation.md
    - _Requirements: 5.5_

  - [x] 21.4 Remove xfail markers from nullable type tests
    - Update `python/tests/types/test_nullable_types.py`
    - Verify all nullable primitive and complex type tests pass
    - _Requirements: 5.5_

  - [x] 21.5 Fix NullableBuilder for enum types (nullable enum)
    - Currently causes polars-core panic: "not implemented" when applying null mask to Categorical
    - Test with `["null", {"type": "enum", "name": "Status", "symbols": ["A", "B"]}]` schema
    - Options to investigate:
      1. Check if Polars Enum type (vs Categorical) handles nulls differently
      2. Convert nullable enum to String type (loses categorical efficiency)
      3. Use different null representation in EnumBuilder
    - Remove xfail from `python/tests/types/test_enum_fixed.py::test_enum_nullable`
    - _Requirements: 5.5, 1.5_

- [x] 22. Implement enum type support
  - [x] 22.1 Add EnumBuilder for Avro enum decoding
    - Create builder that maps enum index to symbol string
    - Uses Polars Enum type with FrozenCategories (not Dictionary/Categorical)
    - Store enum symbols from schema for lookup
    - Note: pyo3-polars converts Enum→Categorical during FFI; Python workaround applied
    - See devnotes/22.1-enum-builder.md for details
    - _Requirements: 1.5_

  - [x] 22.2 Implement enum decoding in record_decoder.rs
    - Decode varint index from Avro binary
    - Validate index is within symbol range
    - Append to indices vector, build Enum Series at finish()
    - _Requirements: 1.5_

  - [x] 22.3 Add Arrow type mapping for enum
    - Map Avro enum to Polars Enum type with FrozenCategories
    - Categories defined from schema symbols (not runtime-inferred)
    - Update `src/convert/arrow.rs`
    - _Requirements: 5.4_

  - [x] 22.4 Remove xfail markers from enum tests
    - Updated `python/tests/types/test_enum_fixed.py`
    - All non-nullable enum tests pass
    - Nullable enum (union with null) marked xfail - polars-core limitation
    - _Requirements: 1.5_

- [x] 23. Implement fixed type support
  - [x] 23.1 Add FixedBuilder for Avro fixed decoding
    - Create builder for fixed-size binary data
    - Use Arrow FixedSizeBinary or Binary type
    - Store size from schema for validation
    - _Requirements: 1.5_

  - [x] 23.2 Implement fixed decoding in record_decoder.rs
    - Read exactly N bytes from Avro binary (N from schema)
    - Append to BinaryBuilder
    - _Requirements: 1.5_

  - [x] 23.3 Add Arrow type mapping for fixed
    - Map Avro fixed to Arrow FixedSizeBinary(N) or Binary
    - Ensure Polars reads as Binary type
    - Update `src/convert/arrow.rs`
    - _Requirements: 5.4_

  - [x] 23.4 Remove xfail markers from fixed tests
    - Update `python/tests/types/test_enum_fixed.py`
    - Verify fixed values have correct size and content
    - _Requirements: 1.5_

- [x] 24. Fix logical type value interpretation
  - [x] 24.1 Fix date logical type (off by one day)
    - Investigate date calculation in `src/reader/decode.rs`
    - Ensure days-since-epoch is correctly converted
    - May be timezone or epoch definition issue
    - _Requirements: 1.6_

  - [x] 24.2 Fix time-millis/time-micros interpretation
    - Investigate time value decoding
    - Ensure milliseconds/microseconds since midnight is correct
    - Check Arrow Time32/Time64 unit handling
    - Write thorough tests
    - _Requirements: 1.6_

  - [x] 24.3 Fix timestamp-millis timezone handling
    - Investigate 1-hour offset issue
    - Ensure UTC timezone is correctly applied
    - Check Arrow Timestamp timezone handling
    - _Requirements: 1.6_

  - [x] 24.4 Remove xfail markers from logical type tests
    - Update `python/tests/types/test_logical_types.py`
    - Verify all date/time values match expected
    - _Requirements: 1.6_

- [x] 25. BlockReader read buffering optimization
  - [x] 25.1 Add ReadBufferConfig struct
    - Create `ReadBufferConfig` struct with `chunk_size` and `prefetch_threshold` fields
    - Add `LOCAL_DEFAULT` constant (64KB chunk, 0.0 threshold)
    - Add `S3_DEFAULT` constant (4MB chunk, 0.5 threshold)
    - Add constructors: `with_chunk_size()`, `new()`
    - Place in `src/reader/block.rs` or new `src/reader/buffer_config.rs`
    - _Requirements: 3.11, 3.12, 3.13_

  - [x] 25.2 Add read buffer to BlockReader struct
    - Add `read_buffer: Bytes` field to retain unused bytes
    - Add `buffer_file_offset: u64` to track buffer position
    - Replace `read_chunk_size: usize` with `buffer_config: ReadBufferConfig`
    - Update `new()` to use `ReadBufferConfig::LOCAL_DEFAULT`
    - Add `with_config()` constructor accepting `ReadBufferConfig`
    - _Requirements: 3.8, 3.9, 3.10_

  - [x] 25.3 Implement buffered next_block()
    - Check if read_buffer contains enough data for next block header
    - If buffer below `prefetch_threshold * chunk_size`, fetch more data
    - Parse block from buffer
    - Advance buffer position, retaining unused bytes
    - Handle large blocks (> chunk size) by reading exact amount needed
    - _Requirements: 3.8, 3.9, 3.10_

  - [x] 25.4 Update seek and reset methods
    - Clear read_buffer on reset()
    - Clear read_buffer on seek_to_sync()
    - Ensure buffer state is consistent after error recovery
    - _Requirements: 3.7_

  - [x] 25.5 Add ReadBufferConfig to ReaderConfig
    - Add `read_buffer_config: ReadBufferConfig` field to `ReaderConfig`
    - Update `ReaderConfig::default()` to use `LOCAL_DEFAULT`
    - Add `ReaderConfig::for_s3()` constructor using `S3_DEFAULT`
    - Pass config through `AvroStreamReader` → `PrefetchBuffer` → `BlockReader`
    - _Requirements: 3.11, 3.12_

  - [x] 25.6 Expose read_chunk_size in Python API
    - Add `read_chunk_size: int | None` parameter to `scan()` and `open()`
    - When `None`, auto-detect source type and use appropriate default
    - When set, override the default with user-specified value
    - Update docstrings with guidance on tuning
    - _Requirements: 3.13_

  - [x] 25.7 Write unit tests for buffered reading
    - Test multiple small blocks parsed from single read
    - Test block spanning buffer boundary
    - Test large block (> chunk size)
    - Test mixed small and large blocks
    - Test reset clears buffer
    - Test seek clears buffer
    - Test S3 default (4MB) vs local default (64KB)
    - _Requirements: 3.8, 3.9, 3.10, 3.11, 3.12_

  - [x] 25.8 Write property test for I/O efficiency
    - **Property 16: BlockReader I/O Efficiency**
    - For N blocks totaling B bytes, verify at most ceil(B / chunk_size) I/O operations
    - Use mock source that counts read_range calls
    - **Validates: Requirements 3.10**

  - [x] 25.9 Benchmark S3 read reduction
    - Create test with many small blocks (e.g., 1000 blocks of 1KB each)
    - Compare I/O operation count: before (1000 calls) vs after (~1 call with 4MB buffer)
    - Verify significant reduction for small-block files
    - Document expected latency improvement for S3 (fewer HTTP round-trips)
    - _Requirements: 8.5_

- [ ] 26. *Remove Enum→Categorical FFI workaround
  - Blocked until upstream supports either:
    - polars-core `to_physical_repr()` on Enum columns (panics in 0.52.0)
    - pyo3-polars Arrow FFI Enum metadata preservation
  - See: `devnotes/22.1-enum-builder.md`
  - [ ] 26.1 Remove `dataframe_to_py_with_enums()` from `src/python/reader.rs`
  - [ ] 26.2 Update AvroReaderCore and AvroReader `__next__()` to use direct FFI
  - [ ] 26.3 Verify all enum tests pass

- [x] 27. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases
- The implementation uses Rust's proptest crate for property-based testing
