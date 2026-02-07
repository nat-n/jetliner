# Task 2.5: MultiSourceReader S3 Support

## Decision: Use BoxedSource for Dynamic Dispatch

The `MultiSourceReader` now uses `BoxedSource` (trait object) instead of being generic over the source type. This allows handling both `LocalSource` and `S3Source` within the same reader instance.

### Rationale

1. **Mixed source types**: Multi-file reading may involve both local and S3 files in the same operation (though uncommon, it's architecturally cleaner to support)

2. **Simpler API**: Using trait objects avoids complex generic bounds and makes the API easier to use

3. **Minimal performance impact**: The dynamic dispatch overhead is negligible compared to I/O operations

### Implementation Details

1. **CloudOptions propagation**: Added `cloud_options` field to `MultiSourceConfig` to pass S3 credentials and configuration to the reader

2. **Source opening**: The `open_source()` method now:
   - Checks if the path is an S3 URI using `is_s3_uri()`
   - Builds `S3Config` from `CloudOptions` if S3
   - Opens appropriate source type and boxes it

3. **S3Config conversion**: CloudOptions fields map to S3Config:
   - `endpoint` → `endpoint_url`
   - `aws_access_key_id` → `access_key_id`
   - `aws_secret_access_key` → `secret_access_key`
   - `region` → `region`
   - `max_retries` → `max_retries`

### Testing

- Unit tests verify configuration builder patterns
- Integration tests (Python) verify S3 glob patterns work with `read_avro()`
- Parity tests confirm `scan_avro().collect()` equals `read_avro()` for S3 sources

### Related Changes

- `read.rs`: Now passes `cloud_options` from `ScanArgsAvro` to `MultiSourceConfig`
