# Requirements Document

## Introduction

This document specifies requirements for adding S3 mock testing to Jetliner. Currently, the S3Source implementation in Rust has only unit tests for URI parsing, with no integration tests against an actual S3-compatible service. This gap means the core S3 functionality (range requests, authentication, error handling) is untested. Adding mock S3 testing will validate the S3 integration works correctly and provide infrastructure for future performance testing.

The test infrastructure uses MinIO (via testcontainers) as the S3-compatible mock backend. Note that moto (in-process Python S3 mock) cannot be used because Jetliner's S3 implementation is in Rust using the AWS SDK, which makes actual HTTP calls rather than boto3 calls that moto intercepts.

## Glossary

- **Mock_S3**: A local S3-compatible service that simulates AWS S3 behavior for testing
- **S3Source**: The Rust component that reads Avro files from S3 using the AWS SDK
- **Stream_Source**: The trait abstraction over data sources (S3 or local filesystem)
- **Range_Request**: HTTP range header used to read partial file content from S3
- **MinIO**: Container-based S3-compatible object storage that provides a real HTTP endpoint
- **testcontainers**: Python library for managing Docker containers in tests

## Requirements

### Requirement 1: Mock S3 Test Infrastructure

**User Story:** As a developer, I want reusable mock S3 fixtures using MinIO, so that I can write S3 integration tests without requiring real AWS credentials.

#### Acceptance Criteria

1. WHEN a test requires S3 mocking, THE MinIO fixture SHALL provide realistic S3 behavior via testcontainers
2. THE Mock_S3 fixtures SHALL create a test bucket with a known name
3. WHEN a test completes, THE Mock_S3 fixtures SHALL clean up all resources
4. THE Mock_S3 fixtures SHALL configure the AWS SDK to use the mock endpoint via storage_options
5. THE Mock_S3 fixtures SHALL provide helper functions to upload test files to the mock bucket
6. THE test suite SHALL mark container-based tests appropriately for CI configuration
7. WHEN Docker is unavailable, THE test suite SHALL skip S3 tests gracefully

### Requirement 2: S3 Read Operations Testing

**User Story:** As a developer, I want to verify S3 read operations work correctly using MinIO, so that I can trust the S3Source implementation.

#### Acceptance Criteria

1. WHEN an Avro file is uploaded to mock S3, THE scan function SHALL read it successfully via s3:// URI
2. WHEN an Avro file is uploaded to mock S3, THE open function SHALL read it successfully via s3:// URI
3. WHEN reading from S3, THE S3Source SHALL correctly handle range requests for partial reads
4. WHEN reading from S3, THE S3Source SHALL return the correct file size
5. FOR ALL Avro files readable from local filesystem, reading the same file from mock S3 SHALL produce identical DataFrames

### Requirement 3: S3 Error Handling Testing

**User Story:** As a developer, I want to verify S3 error handling works correctly, so that users get meaningful error messages.

#### Acceptance Criteria

1. WHEN a non-existent S3 key is requested, THE S3Source SHALL raise SourceError with descriptive message
2. WHEN a non-existent S3 bucket is requested, THE S3Source SHALL raise SourceError with descriptive message
3. WHEN authentication fails, THE S3Source SHALL raise SourceError indicating authentication failure
4. WHEN an invalid S3 URI is provided, THE scan function SHALL raise an appropriate error

### Requirement 4: S3 Query Operations Testing

**User Story:** As a data analyst, I want S3-backed queries to work identically to local file queries, so that I can use the same code regardless of data location.

#### Acceptance Criteria

1. WHEN using scan() with S3 URI, projection pushdown SHALL work correctly
2. WHEN using scan() with S3 URI, predicate pushdown SHALL work correctly
3. WHEN using scan() with S3 URI, early stopping (head/limit) SHALL work correctly
4. WHEN using open() with S3 URI, batch iteration SHALL work correctly
