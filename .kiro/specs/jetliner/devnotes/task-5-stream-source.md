# Task 5: Stream Source Abstraction

## Context

Implementing the `StreamSource` trait and its implementations for local filesystem and S3.

## Decisions

### 1. Mutex-Wrapped File Handle in LocalSource

**Problem:** The design showed `file: tokio::fs::File`, but `seek()` requires `&mut self` while `StreamSource` trait methods take `&self` to allow concurrent reads.

**Decision:** Wrap the file handle in `Mutex<File>`. This serializes access to the file handle while allowing the source to be shared across async tasks. The mutex is held only during the seek+read operation.

**Location:** `src/source/local.rs`, `LocalSource::file` field

**Trade-off:** Slight overhead from mutex acquisition, but necessary for correctness. For high-concurrency scenarios, consider opening multiple file handles instead.

### 2. Range Clamping vs Error on Overflow

**Problem:** What should `read_range(offset, length)` do when `offset + length > file_size`?

**Decision:** Silently clamp the read length to available bytes rather than returning an error. This matches typical HTTP range request semantics and simplifies caller code.

**Location:** `src/source/local.rs` and `src/source/s3.rs`, `read_range()` implementations

### 3. Property Tests Use LocalSource Only

**Problem:** S3Source requires AWS credentials and network access, making it unsuitable for property tests.

**Decision:** Property tests for "Range Requests Return Correct Data" (Property 8) test only LocalSource using temp files. S3Source shares the same trait contract, so LocalSource tests provide confidence in the interface semantics. S3-specific behavior (auth, network errors) should be covered by integration tests with localstack/minio.

**Location:** `tests/property_tests.rs`, `prop_range_requests_return_correct_data`

### 4. Cached File Size at Construction

**Problem:** Calling `size()` repeatedly could be expensive, especially for S3 (HEAD request each time).

**Decision:** Both LocalSource and S3Source cache the file/object size at construction time. This assumes the underlying data doesn't change during reading, which is a reasonable assumption for Avro file processing.

**Location:** `src/source/local.rs` and `src/source/s3.rs`, `file_size`/`object_size` fields

