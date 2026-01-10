# Implementation Plan: S3 Mock Testing

## Overview

This plan implements S3 mock testing for Jetliner using MinIO via testcontainers. The implementation focuses on validating S3Source functionality and ensuring parity between local and S3 file reads.

Note: moto (in-process Python S3 mock) cannot be used because Jetliner's S3 implementation is in Rust using the AWS SDK, which makes actual HTTP calls rather than boto3 calls that moto intercepts. MinIO provides a real HTTP endpoint that works with the Rust SDK.

## Tasks

- [x] 1. Add S3 testing dependencies
  - Add testcontainers[minio] and hypothesis to dev dependencies via `uv add --dev`
  - _Requirements: 1.1_

- [x] 2. Create S3 test infrastructure
  - [x] 2.1 Create python/tests/s3/ directory structure with __init__.py
    - _Requirements: 1.2, 1.3_
  - [x] 2.2 Implement MockS3Context dataclass in conftest.py
    - Include upload_file() and upload_bytes() helper methods
    - _Requirements: 1.5_
  - [x] 2.3 Implement MinIO fixture with testcontainers (mock_s3_minio)
    - Session-scoped container, per-test bucket isolation
    - Skip gracefully if Docker unavailable
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.6, 1.7_

- [x] 3. Checkpoint - Verify fixtures work
  - Ensure fixtures start/stop correctly, ask user if questions arise

- [x] 4. Implement S3 read operation tests
  - [x] 4.1 Implement basic scan() test with MinIO
    - Upload test Avro file, read via s3:// URI, verify DataFrame
    - _Requirements: 2.1_
  - [x] 4.2 Implement basic open() test with MinIO
    - Upload test Avro file, iterate batches via s3:// URI
    - _Requirements: 2.2_
  - [x] 4.3 Write property test for local/S3 read equivalence
    - **Property 1: Local/S3 Read Equivalence**
    - **Validates: Requirements 2.1, 2.2, 2.5**

- [x] 5. Implement S3 error handling tests
  - [x] 5.1 Test non-existent key error
    - Verify SourceError raised with descriptive message
    - _Requirements: 3.1_
  - [x] 5.2 Test non-existent bucket error
    - Verify SourceError raised with descriptive message
    - _Requirements: 3.2_
  - [x] 5.3 Test invalid S3 URI error
    - Verify appropriate error for malformed URIs
    - _Requirements: 3.4_

- [x] 6. Implement S3 query operation tests
  - [x] 6.1 Test projection pushdown with S3
    - Select subset of columns, verify only those columns returned
    - _Requirements: 4.1_
  - [x] 6.2 Test predicate pushdown with S3
    - Apply filter, verify filtered results
    - _Requirements: 4.2_
  - [x] 6.3 Test early stopping with S3
    - Use head(N), verify at most N rows returned
    - _Requirements: 4.3_
  - [x] 6.4 Write property test for query operation equivalence
    - **Property 4: Query Operation Equivalence**
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4**

- [x] 7. Final checkpoint - Ensure all tests pass
  - Run full test suite including container tests
  - Ensure all tests pass, ask user if questions arise

- [x] 8. Implement S3 edge case tests
  - [x] 8.1 Implement large/multi-block file tests
    - Test multi-block Avro files from S3 (range requests across block boundaries)
    - Test batch iteration with multi-block files
    - Test different compression codecs (deflate, snappy, zstd) from S3
    - _Requirements: 2.3, 2.5_
  - [x] 8.2 Implement special characters in S3 keys tests
    - Test keys with spaces, safe special chars, deeply nested paths
    - Test keys with unicode characters
    - Property test for random safe key combinations
    - _Requirements: 2.1, 2.2_
  - [x] 8.3 Implement empty/edge-case file tests
    - Test single-record Avro file from S3
    - Test file with null values from S3
    - Test no-fields record schema from S3
    - _Requirements: 2.1, 2.2, 2.5_
  - [x] 8.4 Implement range request edge case tests
    - Test with very small batch size (batch_size=1)
    - Test head() at various row boundaries
    - _Requirements: 2.3, 4.3_
  - [x] 8.5 Implement S3 URI edge case tests
    - Test keys that look like filesystem paths
    - Test keys with dots, plus signs, equals signs
    - Test very long (but valid) S3 keys
    - _Requirements: 2.1, 2.2_

- [x] 9. Final checkpoint - Verify all edge case tests pass
  - Run full S3 test suite including new edge cases
  - Ensure all tests pass, ask user if questions arise

## Notes

- All S3 tests require Docker (MinIO container)
- Tests are marked with @pytest.mark.container for CI configuration
- Property tests use hypothesis with minimum 100 iterations
- All tasks are required for comprehensive coverage
