# S3 Access

Jetliner provides first-class support for reading Avro files directly from Amazon S3 and S3-compatible services.

## Basic S3 Usage

### Using Default Credentials

When AWS credentials are configured via environment variables, IAM roles, or AWS config files:

```python
import jetliner

# Read from S3 using default credentials
df = jetliner.scan_avro("s3://my-bucket/data/records.avro").collect()

# With the iterator API
with jetliner.open("s3://my-bucket/data/records.avro") as reader:
    for batch in reader:
        process(batch)
```

### Using Explicit Credentials

Pass credentials directly via `storage_options`:

```python
import jetliner

df = jetliner.scan_avro(
    "s3://my-bucket/data/records.avro",
    storage_options={
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "region": "us-east-1",
    }
).collect()
```

!!! warning "Security"
    Avoid hardcoding credentials in source code. Use environment variables or IAM roles in production.

## Authentication Methods

### Environment Variables

Set AWS credentials as environment variables:

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_REGION=us-east-1
```

Then read without explicit credentials:

```python
df = jetliner.scan_avro("s3://bucket/file.avro").collect()
```

### IAM Roles (EC2, Lambda, ECS)

When running on AWS infrastructure with an IAM role attached, credentials are obtained automatically:

```python
# No credentials needed - uses IAM role
df = jetliner.scan_avro("s3://bucket/file.avro").collect()
```

### AWS Profiles

Use a specific AWS profile:

```bash
export AWS_PROFILE=my-profile
```

```python
df = jetliner.scan_avro("s3://bucket/file.avro").collect()
```

### Explicit Credentials (storage_options)

The `storage_options` parameter accepts:

| Key                     | Description                                   |
| ----------------------- | --------------------------------------------- |
| `aws_access_key_id`     | AWS access key ID                             |
| `aws_secret_access_key` | AWS secret access key                         |
| `region`                | AWS region (e.g., `us-east-1`)                |
| `endpoint`              | Custom S3 endpoint URL                        |
| `max_retries`           | Maximum retry attempts for transient failures |

Explicit credentials take precedence over environment variables.

## S3-Compatible Services

Jetliner works with any S3-compatible service by specifying a custom endpoint.

### MinIO

```python
import jetliner

df = jetliner.scan_avro(
    "s3://my-bucket/data.avro",
    storage_options={
        "endpoint": "http://localhost:9000",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
    }
).collect()
```

### LocalStack

```python
import jetliner

df = jetliner.scan_avro(
    "s3://my-bucket/data.avro",
    storage_options={
        "endpoint": "http://localhost:4566",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
).collect()
```

### Cloudflare R2

```python
import jetliner

df = jetliner.scan_avro(
    "s3://my-bucket/data.avro",
    storage_options={
        "endpoint": "https://ACCOUNT_ID.r2.cloudflarestorage.com",
        "aws_access_key_id": "your-r2-access-key",
        "aws_secret_access_key": "your-r2-secret-key",
    }
).collect()
```

### DigitalOcean Spaces

```python
import jetliner

df = jetliner.scan_avro(
    "s3://my-space/data.avro",
    storage_options={
        "endpoint": "https://nyc3.digitaloceanspaces.com",
        "aws_access_key_id": "your-spaces-key",
        "aws_secret_access_key": "your-spaces-secret",
    }
).collect()
```

## Query Optimization with S3

All query optimizations work with S3 sources:

```python
import jetliner
import polars as pl

# Projection pushdown - only downloads needed columns
result = (
    jetliner.scan_avro("s3://bucket/large-file.avro")
    .select(["user_id", "amount"])
    .collect()
)

# Predicate pushdown - filters during download
result = (
    jetliner.scan_avro("s3://bucket/large-file.avro")
    .filter(pl.col("status") == "active")
    .collect()
)

# Early stopping - stops downloading after enough rows
result = (
    jetliner.scan_avro("s3://bucket/large-file.avro")
    .head(1000)
    .collect()
)

# Combined optimizations
result = (
    jetliner.scan_avro("s3://bucket/large-file.avro")
    .select(["user_id", "amount", "status"])
    .filter(pl.col("amount") > 100)
    .head(10000)
    .collect()
)
```

## S3 Glob Patterns

Read multiple files from S3 using glob patterns:

```python
import jetliner

# Read all Avro files in a prefix
df = jetliner.read_avro("s3://bucket/data/*.avro")

# With row index continuity
df = jetliner.read_avro("s3://bucket/data/*.avro", row_index_name="idx")

# Track source files
df = jetliner.read_avro("s3://bucket/data/*.avro", include_file_paths="source")
```

## Streaming from S3

Use the iterator API for memory-efficient S3 streaming:

```python
import jetliner

with jetliner.open(
    "s3://bucket/large-file.avro",
    storage_options={"region": "us-east-1"},
    buffer_blocks=2,  # Reduce prefetch for memory savings
) as reader:
    for batch in reader:
        # Process each batch as it's downloaded
        process(batch)
```

## Buffer Configuration for S3

Tune buffer settings for S3 performance:

```python
import jetliner

# High-throughput settings (more memory, faster)
df = jetliner.scan_avro(
    "s3://bucket/file.avro",
    buffer_blocks=8,              # More prefetching
    buffer_bytes=128*1024*1024,   # 128MB buffer
).collect()

# Low-memory settings (less memory, slower)
df = jetliner.scan_avro(
    "s3://bucket/file.avro",
    buffer_blocks=2,              # Less prefetching
    buffer_bytes=16*1024*1024,    # 16MB buffer
).collect()
```

## Retry Configuration

Configure retry behavior for transient S3 failures:

```python
import jetliner

# Increase retries for unreliable connections
df = jetliner.scan_avro(
    "s3://bucket/file.avro",
    storage_options={
        "max_retries": "5",  # Default is 2
    }
).collect()
```

Retryable errors include:
- Connection timeouts
- 5xx server errors
- 429 throttling responses

Retries use exponential backoff (1s, 2s, 4s, ...).

## Error Handling

Handle S3-specific errors:

```python
import jetliner

try:
    df = jetliner.scan("s3://bucket/file.avro").collect()
except jetliner.SourceError as e:
    # S3 connection, authentication, or access errors
    print(f"S3 error: {e}")
except FileNotFoundError:
    # Object doesn't exist
    print("File not found in S3")
```

## Best Practices

1. **Use IAM roles** in production instead of hardcoded credentials
2. **Enable projection pushdown** to minimize data transfer
3. **Use early stopping** when you only need a subset of rows
4. **Tune buffer settings** based on your network and memory constraints
5. **Use regional endpoints** to minimize latency

## Troubleshooting

### "Access Denied" errors

- Verify IAM permissions include `s3:GetObject` for the bucket/prefix
- Check bucket policy allows access from your identity
- Ensure credentials are valid and not expired

### Slow performance

- Use a regional endpoint close to your data
- Increase `buffer_blocks` for more prefetching
- Enable projection pushdown to reduce data transfer

### Connection timeouts

- Check network connectivity to S3
- Verify endpoint URL is correct for S3-compatible services
- Consider using VPC endpoints for private access

## Next Steps

- [Query Optimization](query-optimization.md) - Maximize S3 performance
- [Streaming Large Files](streaming.md) - Memory-efficient processing
- [Error Handling](error-handling.md) - Handling failures gracefully
