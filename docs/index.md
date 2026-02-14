# Getting Started

<p align="center">
  <img src="assets/jetliner_logo.png" alt="Jetliner Logo" width="200">
</p>

Jetliner is a high-performance Polars plugin for streaming Avro files into DataFrames with minimal memory overhead. Built in Rust with Python bindings, it's designed for data pipelines where Avro files live on S3 or local disk and need to land in Polars fast.

## Why Jetliner?

- **Streaming architecture**: Reads data block-by-block rather than loading entire files into memory
- **Query optimization**: Projection pushdown, predicate pushdown, and early stopping via Polars' IO plugin system
- **S3-native**: First-class support for reading directly from S3 with configurable authentication
- **Zero-copy techniques**: Uses `bytes::Bytes` for efficient memory handling
- **Full codec support**: Handles null, snappy, deflate, zstd, bzip2, and xz compression

### Performance benchmarks

Jetliner is built for speed. Benchmarks against other Python Avro readers show significant performance gains, especially on complex schemas and wide tables.

<iframe src="assets/benchmark_performance.html" width="100%" height="500" frameborder="0"></iframe>

Yes, that's a log scale. The chart compares read times across four scenarios using 1M-row Avro files. Note that Polars' built-in Avro reader is missing from the "Complex" scenario entirely—it doesn't support maps. Jetliner handles complex nested schemas with arrays, maps, and nullable fields without breaking a sweat.

For detailed methodology and additional comparisons, see [Performance Benchmarks](performance/benchmarks.md).

## Installation

=== "pip"

    ```bash
    pip install jetliner
    ```

=== "uv"

    ```bash
    uv add jetliner
    ```

=== "From source"

    ```bash
    git clone https://github.com/jetliner/jetliner.git
    cd jetliner
    pip install maturin
    maturin develop
    ```

## Quick Start

Here's a minimal example to verify your installation:

```python
import jetliner

# Read an Avro file into a DataFrame
df = jetliner.scan_avro("data.avro").collect()
print(df)

# Or use read_avro() for eager loading with column selection
df = jetliner.read_avro("data.avro", columns=["col1", "col2"])
```

## Three APIs: scan_avro() vs read_avro() vs AvroReader

Jetliner provides three complementary APIs for reading Avro files:

### scan_avro() - LazyFrame with Query Optimization

The recommended API for most use cases. Returns a Polars LazyFrame that enables query optimizations:

```python
import jetliner
import polars as pl

# Query with automatic optimization
result = (
    jetliner.scan_avro("data.avro")
    .select(["user_id", "amount"])      # Projection pushdown
    .filter(pl.col("amount") > 100)     # Predicate pushdown
    .head(1000)                         # Early stopping
    .collect()
)
```

**Benefits:**

- Only reads columns you actually use (projection pushdown)
- Filters data during reading, not after (predicate pushdown)
- Stops reading once you have enough rows (early stopping)
- Integrates with Polars streaming engine

### read_avro() - Eager DataFrame Loading

Use when you want to load data directly into a DataFrame with column selection:

```python
import jetliner

# Load specific columns eagerly
df = jetliner.read_avro("data.avro", columns=["user_id", "amount"], n_rows=1000)

# Load from multiple files
df = jetliner.read_avro(["file1.avro", "file2.avro"])

# Load with glob pattern
df = jetliner.read_avro("data/*.avro")
```

**Use cases:**

- Quick data loading with column selection
- When you need a DataFrame immediately
- Multi-file reading with schema validation

### AvroReader - Iterator for Streaming Control

Use when you need fine-grained control over batch processing:

```python
import jetliner

# Process batches with full control
with jetliner.AvroReader("data.avro") as reader:
    print(f"Schema: {reader.schema}")

    for batch in reader:
        # Process each batch individually
        process(batch)
```

**Use cases:**

- Progress tracking during iteration
- Custom memory management
- Streaming pipelines with backpressure
- Accessing schema before reading data

## Reading from S3

All APIs support reading directly from S3:

```python
import jetliner

# Using default AWS credentials (environment variables, IAM role, etc.)
df = jetliner.scan_avro("s3://bucket/path/to/file.avro").collect()

# With explicit credentials
df = jetliner.scan_avro(
    "s3://bucket/path/to/file.avro",
    storage_options={
        "aws_access_key_id": "your-key",
        "aws_secret_access_key": "your-secret",
        "region": "us-east-1",
    }
).collect()

# S3-compatible services (MinIO, LocalStack, R2)
df = jetliner.scan_avro(
    "s3://bucket/file.avro",
    storage_options={
        "endpoint": "http://localhost:9000",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
    }
).collect()
```

## Verification

To verify your installation is working correctly:

```python
import jetliner

# Check that the module loads
print(f"Jetliner version: {jetliner.__name__}")

# List available exports
print(f"Available: {jetliner.__all__}")
```

Expected output:
```
Jetliner version: jetliner
Available: ['scan_avro', 'read_avro', 'read_avro_schema', 'AvroReader', 'MultiAvroReader', 'BadBlockError', 'JetlinerError', 'DecodeError', 'ParseError', 'SourceError', 'SchemaError', 'CodecError', 'AuthenticationError', 'FileNotFoundError', 'PermissionError', 'ConfigurationError', 'FileSource']
```

## System Requirements

- **Python**: 3.11 or later
- **Polars**: 0.52 or later
- **Operating Systems**: Linux, macOS, Windows

## Next Steps

- [Installation](installation.md) - Detailed installation options and troubleshooting
- [User Guide](user-guide/index.md) - In-depth guides for common workflows
- [API Reference](api/index.md) - Complete function and class documentation
