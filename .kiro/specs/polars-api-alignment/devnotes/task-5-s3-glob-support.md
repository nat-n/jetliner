# Task 5: S3 Glob Support - Implementation Notes

## Overview

Implemented S3 glob pattern expansion and retry logic with exponential backoff for transient S3 failures.

## Key Decisions

### 1. Glob Pattern Parsing Strategy

For S3 glob URIs like `s3://bucket/path/to/*.avro`, we extract:
- **bucket**: The S3 bucket name
- **prefix**: The key prefix up to the first glob character (used for efficient `list_objects_v2` filtering)
- **pattern**: The full key pattern (used for glob matching)

This approach minimizes S3 API calls by using the prefix to filter objects server-side before applying glob matching client-side.

### 2. Retry Logic Design

Implemented a generic `with_retry` function that:
- Accepts any async operation returning `Result<T, E>`
- Checks if errors are retryable based on error message patterns
- Uses exponential backoff: 1s, 2s, 4s, 8s, up to 30s max delay
- Respects `max_retries` from `CloudOptions`

Retryable errors include:
- Connection timeouts and dispatch failures
- 5xx server errors (500, 502, 503, 504)
- Throttling (429, SlowDown, RequestLimitExceeded)

Non-retryable errors (fail immediately):
- 404 Not Found
- 403 Access Denied
- Invalid credentials

### 3. S3Config Extension

Added `max_retries` field to `S3Config` with default value of 2, matching `CloudOptions`. The retry count is propagated through `S3Source` and applied to all S3 operations (head_object, get_object, list_objects_v2).

### 4. Async Glob Expansion

Created async versions of glob expansion functions:
- `expand_path_async`: Handles both local and S3 paths
- `expand_paths_async`: Expands multiple paths with S3 support
- `expand_s3_glob`: Core S3 glob expansion with pagination

The `ResolvedSources::resolve` method uses async glob expansion to support S3 patterns.

## Files Modified

- `src/api/glob.rs`: Added S3 glob parsing and expansion
- `src/source/s3.rs`: Added retry logic and `max_retries` to `S3Config`
- `src/source/mod.rs`: Exported retry functions
- `src/api/sources.rs`: Updated to use async glob expansion and S3 schema reading
- `src/api/mod.rs`: Exported new glob functions
- `python/tests/s3/test_s3_glob.py`: New test file for S3 glob functionality

## Testing

- 22 unit tests for glob parsing (including S3 URI parsing)
- 25 unit tests for S3 source including retry logic
- Python integration tests for S3 glob (requires MinIO container)
