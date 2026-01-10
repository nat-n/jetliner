# User Guide

This guide covers common workflows and best practices for using Jetliner in your data pipelines.

## Overview

Jetliner provides two APIs for reading Avro files:

| API      | Returns   | Best For                             |
| -------- | --------- | ------------------------------------ |
| `scan()` | LazyFrame | Query optimization, most use cases   |
| `open()` | Iterator  | Streaming control, progress tracking |

Both APIs share the same high-performance Rust core and support local files and S3.

## Topics

### [Local Files](local-files.md)
Reading Avro files from local filesystem with examples for common patterns.

### [S3 Access](s3-access.md)
Reading from Amazon S3 and S3-compatible services (MinIO, LocalStack, Cloudflare R2).

### [Query Optimization](query-optimization.md)
Using projection pushdown, predicate pushdown, and early stopping to minimize I/O and memory usage.

### [Streaming Large Files](streaming.md)
Memory-efficient processing of large files using the iterator API and buffer configuration.

### [Error Handling](error-handling.md)
Understanding strict vs skip modes and handling corrupted data gracefully.

### [Schema Inspection](schemas.md)
Accessing Avro schemas and understanding type mapping to Polars.

### [Codec Support](codecs.md)
Supported compression codecs and their trade-offs.

## Quick Reference

### Basic Reading

```python
import jetliner

# LazyFrame API (recommended)
df = jetliner.scan("data.avro").collect()

# Iterator API
with jetliner.open("data.avro") as reader:
    for batch in reader:
        process(batch)
```

### S3 Reading

```python
# With default credentials
df = jetliner.scan("s3://bucket/file.avro").collect()

# With explicit credentials
df = jetliner.scan(
    "s3://bucket/file.avro",
    storage_options={"endpoint_url": "http://localhost:9000"}
).collect()
```

### Query Optimization

```python
import polars as pl

# Only reads needed columns, filters during read, stops early
result = (
    jetliner.scan("data.avro")
    .select(["col1", "col2"])
    .filter(pl.col("col1") > 100)
    .head(1000)
    .collect()
)
```

### Error Handling

```python
# Skip bad records (default)
df = jetliner.scan("data.avro", strict=False).collect()

# Fail on first error
df = jetliner.scan("data.avro", strict=True).collect()
```
