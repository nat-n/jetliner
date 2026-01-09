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

- [ ] 15. Python bindings
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
    - Implement parse_avro_schema() for IO plugin schema extraction
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
      with jetliner.open("file.avro", on_error="skip") as reader:
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

  - [x] 15.5 Implement jetliner.open() entry point
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
  - [x] 17.1 Add Apache Avro interoperability test files
    - See Appendix: A_avro_java_test_research.md
    - See Appendix: B_e2e-test-plan.md
    - Verify all primitive types
    - Verify all complex types
    - Verify all logical types
    - Verify all codecs
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

- [ ] 18. Performance benchmarks
  - [ ] 18.1 Create benchmark suite with criterion
    - Benchmark read throughput (records/sec)
    - Benchmark memory usage
    - Benchmark with different codecs
    - Benchmark with different batch sizes
    - _Requirements: 10.8_

- [ ] 19. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases
- The implementation uses Rust's proptest crate for property-based testing
