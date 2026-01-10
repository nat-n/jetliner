# Design Document: S3 Mock Testing

## Overview

This design adds comprehensive S3 integration testing to Jetliner using MinIO (via testcontainers) as the S3-compatible mock backend.

**Important**: moto (in-process Python S3 mock) cannot be used for this project because Jetliner's S3 implementation is written in Rust using the AWS SDK, which makes actual HTTP calls. moto works by intercepting boto3 calls in-process, but since we're not using boto3, moto's interception doesn't apply. MinIO provides a real HTTP endpoint that the Rust AWS SDK can connect to.

The key insight is that S3 mock testing validates two distinct layers:
1. **Low-level S3Source**: Range requests, file size, error handling
2. **High-level API**: scan() and open() produce correct DataFrames from S3

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Test Layer                               │
├─────────────────────────────────────────────────────────────┤
│  test_s3_read.py            test_s3_errors.py               │
│  - Read operations          - Not found errors              │
│  - Query operations         - Auth errors                   │
│  - Local/S3 equivalence     - Invalid URI errors            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Fixture Layer                              │
├─────────────────────────────────────────────────────────────┤
│  conftest.py                                                 │
│  - mock_s3_minio (container, realistic HTTP endpoint)       │
│  - s3_weather_file_minio (uploads test Avro to mock)        │
│  - MockS3Context (helper for uploads)                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Mock Backend                               │
├─────────────────────────────────────────────────────────────┤
│              MinIO (Docker container)                        │
│              - Real S3 implementation                        │
│              - HTTP endpoint for Rust AWS SDK                │
│              - Range request fidelity                        │
│              - Realistic production behavior                 │
└─────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### Mock S3 Fixtures

#### MinIO Fixture (testcontainers)

MinIO provides a real HTTP endpoint that the Rust AWS SDK can connect to. We use testcontainers to manage the MinIO lifecycle automatically.

```python
from testcontainers.minio import MinioContainer

@pytest.fixture(scope="session")
def minio_container():
    """Session-scoped MinIO container - starts once, reused across tests."""
    # Skip if Docker not available
    try:
        container = MinioContainer("minio/minio:latest")
        container.start()
    except Exception as e:
        pytest.skip(f"Docker/MinIO not available: {e}")

    yield container
    container.stop()

@pytest.fixture
def mock_s3_minio(minio_container):
    """MinIO-backed S3 mock with per-test bucket isolation."""
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{minio_container.get_container_host_ip()}:{minio_container.get_exposed_port(9000)}",
        aws_access_key_id=minio_container.access_key,
        aws_secret_access_key=minio_container.secret_key,
    )

    bucket_name = f"jetliner-test-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket_name)

    yield MockS3Context(
        client=client,
        bucket=bucket_name,
        endpoint_url=f"http://{minio_container.get_container_host_ip()}:{minio_container.get_exposed_port(9000)}",
    )

    # Cleanup bucket after test
    _delete_bucket_contents(client, bucket_name)
    client.delete_bucket(Bucket=bucket_name)
```

The session-scoped container means MinIO starts once per test session, avoiding container startup overhead for each test.

#### MockS3Context Dataclass

```python
@dataclass
class MockS3Context:
    """Context for S3 mock operations."""
    client: Any  # boto3 S3 client
    bucket: str
    endpoint_url: str | None

    def upload_file(self, local_path: str, key: str) -> str:
        """Upload file and return s3:// URI."""
        self.client.upload_file(local_path, self.bucket, key)
        return f"s3://{self.bucket}/{key}"

    def upload_bytes(self, data: bytes, key: str) -> str:
        """Upload bytes and return s3:// URI."""
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return f"s3://{self.bucket}/{key}"
```

### Environment Configuration

For the AWS SDK in Rust to connect to MinIO, tests pass credentials via `storage_options`:

```python
storage_options = {
    "endpoint_url": mock_s3_minio.endpoint_url,
    "aws_access_key_id": minio_container.access_key,
    "aws_secret_access_key": minio_container.secret_key,
}

# Use with jetliner
df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
```

### Test File Organization

```
python/tests/
├── s3/
│   ├── __init__.py
│   ├── conftest.py          # S3-specific fixtures (MinIO)
│   ├── test_s3_read.py      # Read operation tests
│   ├── test_s3_errors.py    # Error handling tests
│   └── test_s3_queries.py   # Query operation tests
```

## Data Models

### Test Data Strategy

Tests will use existing test Avro files from `tests/data/` uploaded to mock S3:

```python
@pytest.fixture
def s3_weather_file(mock_s3_context, get_test_data_path):
    """Upload weather.avro to mock S3."""
    local_path = get_test_data_path("apache-avro/weather.avro")
    return mock_s3_context.upload_file(local_path, "weather.avro")
```

For property tests, we'll generate random Avro files:

```python
def generate_test_avro(records: list[dict]) -> bytes:
    """Generate Avro file bytes from records."""
    # Use fastavro to create valid Avro files
    ...
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Local/S3 Read Equivalence

*For any* valid Avro file, reading it from local filesystem and reading the same file from mock S3 SHALL produce identical DataFrames (same schema, same data, same row count).

**Validates: Requirements 2.1, 2.2, 2.5**

This property subsumes individual API tests because if local and S3 produce identical results, both scan() and open() must be working correctly.

### Property 2: Range Request Correctness

*For any* file uploaded to S3 and *for any* valid byte range (offset, length), the bytes returned by a range request SHALL exactly match the corresponding bytes from the original file.

**Validates: Requirements 2.3**

### Property 3: File Size Correctness

*For any* file uploaded to S3, the size reported by S3Source SHALL equal the actual file size in bytes.

**Validates: Requirements 2.4**

### Property 4: Query Operation Equivalence

*For any* valid Avro file, *for any* column subset (projection), *for any* filter predicate, and *for any* row limit, executing the query against S3 SHALL produce the same result as executing against local filesystem.

**Validates: Requirements 4.1, 4.2, 4.3, 4.4**

This combines projection, predicate, and limit pushdown into a single comprehensive property.

## Error Handling

### Error Categories

| Scenario            | Expected Error | Message Pattern                     |
| ------------------- | -------------- | ----------------------------------- |
| Non-existent key    | `SourceError`  | "s3://bucket/key" not found         |
| Non-existent bucket | `SourceError`  | "Bucket not found: bucket"          |
| Auth failure        | `SourceError`  | "Access denied" or "authentication" |
| Invalid URI         | `SourceError`  | "Invalid S3 URI"                    |

### Test Approach

Error tests use example-based testing since they verify specific error conditions:

```python
def test_not_found_error(mock_s3_context, s3_env_for_mock):
    """Non-existent key raises SourceError."""
    uri = f"s3://{mock_s3_context.bucket}/does-not-exist.avro"

    with pytest.raises(jetliner.SourceError) as exc_info:
        jetliner.scan(uri).collect()

    assert "not found" in str(exc_info.value).lower()
```

## Testing Strategy

### Testing Approach

All S3 tests use MinIO via testcontainers since the Rust AWS SDK requires a real HTTP endpoint. Tests are marked with `@pytest.mark.container` to indicate they require Docker.

- **Unit tests**: Verify specific examples, edge cases, and error conditions
- **Property tests**: Verify universal properties across all inputs

### Test Categories

1. **Container tests (MinIO)**: All S3 tests require Docker
   - Marked with `@pytest.mark.container`
   - Session-scoped container minimizes startup overhead
   - Real S3 behavior with HTTP endpoint
   - Range request fidelity

### Property-Based Testing Configuration

- Library: `hypothesis` (already in Python ecosystem)
- Minimum iterations: 100 per property
- Tag format: `Feature: s3-mock-testing, Property N: description`
- Health check suppression for function-scoped fixtures

### Test Markers

```python
# All S3 tests require containers
@pytest.mark.container
def test_s3_read(mock_s3_minio): ...

@pytest.mark.container
class TestMinioScanOperations:
    def test_scan_reads_avro_from_s3(self, mock_s3_minio, ...): ...
```

## Dependencies

### Python Dependencies (dev)

```toml
[dependency-groups]
dev = [
    "testcontainers[minio]>=4.0",
    "hypothesis>=6.0",
]
```

### Container Requirements

- Docker must be available for all S3 tests
- Tests gracefully skip if Docker unavailable
- Session-scoped container minimizes startup overhead

### Why Not moto?

moto is a popular Python library for mocking AWS services, but it cannot be used for Jetliner's S3 testing because:

1. **moto intercepts boto3 calls**: moto works by patching boto3's HTTP layer in-process
2. **Jetliner uses Rust AWS SDK**: The S3 implementation is in Rust, making actual HTTP calls
3. **No interception possible**: Since we're not using boto3, moto's interception doesn't apply

MinIO provides a real HTTP endpoint that any AWS SDK (including Rust's) can connect to.
