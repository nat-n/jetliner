<p align="center">
  <img src="docs/assets/jetliner_logo.png" alt="Jetliner" width="400">
</p>

<p align="center">
  <a href="https://pypi.org/project/jetliner/"><img src="https://img.shields.io/pypi/v/jetliner.svg" alt="PyPI version"></a>
  <a href="https://pypi.org/project/jetliner/"><img src="https://img.shields.io/pypi/pyversions/jetliner.svg" alt="Python versions"></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://github.com/jetliner/jetliner/actions/workflows/ci.yml"><img src="https://github.com/jetliner/jetliner/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://jetliner.github.io/jetliner/"><img src="https://img.shields.io/badge/docs-mkdocs-blue.svg" alt="Documentation"></a>
</p>

A high-performance Polars plugin written in Rust for streaming Avro files into DataFrames with minimal memory overhead and maximal throughput.

Jetliner is designed for data pipelines where Avro files live on S3 or local disk and need to land in Polars fast. It streams data block-by-block rather than loading entire files into memory, uses zero-copy techniques, and handles all standard Avro codecs (null, snappy, deflate, zstd, bzip2, xz). The Rust core does the heavy lifting while Python bindings via PyO3 make it accessible from your existing Polars workflows.

## Features

- **High-performance streaming** — Supports block-by-block processing with minimal memory footprint, ideal for large files
- **Query optimization** — Projection pushdown (select columns) and predicate pushdown (filter rows) at the source via Polars LazyFrames
- **S3 and local file support** — Read Avro files from Amazon S3 or local disk with the same API
- **All standard codecs** — null, snappy, deflate, zstd, bzip2, and xz compression out of the box
- **(Almost) complete avro schema support** — reads almost any valid avro (see limitations)
- **Flexible error handling** — Optionally skip bad blocks for resilience to data corruption
- **Ridiculously fast reads** — Check the benchmarks!

This library was created to serve performance critical scenarios around processing large avro files from python. It's fast but limited to read use cases. If you also need to write avro files from Polars then you should check [polars-avro](https://github.com/hafaio/polars-avro).

## Benchmarks

Did I mention it's fast?

TODO: insert benchmarks plot

## Installation

Install from PyPI using pip:

```bash
pip install jetliner
```

Or with uv:

```bash
uv add jetliner
```

## Quick Start

### Basic File Reading

Use `scan()` to read an Avro file into a Polars LazyFrame:

```python
import jetliner

# Read a local file
df = jetliner.scan("data.avro").collect()

# Read from S3
df = jetliner.scan("s3://my-bucket/data.avro").collect()
```

### Streaming with open()

Use `open()` for fine-grained control over batch processing — useful for progress tracking, or memory-constrained environments:

```python
import jetliner

# Process batches one at a time
with jetliner.open("large_file.avro") as reader:
    for batch in reader:
        print(f"Processing batch with {batch.height} rows")
        process(batch)

# Configure batch size and buffer settings
with jetliner.open(
    "large_file.avro",
    batch_size=50_000,
    buffer_blocks=2,
) as reader:
    for batch in reader:
        process(batch)
```

### S3 Access

Jetliner reads from S3 using default AWS credentials (environment variables, IAM roles, or AWS config):

```python
import jetliner

# Uses AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY from environment
df = jetliner.scan("s3://my-bucket/data.avro").collect()

# Or pass credentials explicitly
df = jetliner.scan(
    "s3://my-bucket/data.avro",
    storage_options={
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "region": "us-east-1",
    }
).collect()

# S3-compatible services (MinIO, LocalStack, R2)
df = jetliner.scan(
    "s3://my-bucket/data.avro",
    storage_options={
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
    }
).collect()
```

### Query Optimization

The `scan()` API enables Polars query optimizations — projection pushdown, predicate pushdown, and early stopping:

```python
import jetliner
import polars as pl

# Only reads the columns you select (projection pushdown)
# Filters during read, not after (predicate pushdown)
# Stops reading after 1000 rows (early stopping)
result = (
    jetliner.scan("s3://bucket/large_file.avro")
    .select(["user_id", "amount", "status"])
    .filter(pl.col("status") == "active")
    .filter(pl.col("amount") > 100)
    .head(1000)
    .collect()
)
```

### scan() vs open()

| Feature            | `scan()`                                | `open()`                            |
| ------------------ | --------------------------------------- | ----------------------------------- |
| Returns            | LazyFrame                               | Iterator of DataFrames              |
| Query optimization | ✅ Projection, predicate, early stopping | ❌ Manual                            |
| Batch control      | Automatic                               | Full control                        |
| Best for           | Most queries                            | Custom streaming, progress tracking |

## Development

The project uses spec driven development via [kiro](https://kiro.dev/). See `./.kiro` for the specs and related documentation.

### Project tasks

This project uses [poethepoet](https://poethepoet.natn.io/index.html) for task management.

```sh
# Install poe globally with homebrew
brew tap nat-n/poethepoet
brew install nat-n/poethepoet/poethepoet
# Or with uv/pip/pipx
uv tool install poethepoet
# run poe without arguments to list available tasks, defined in pyproject.toml
poe
```

There are tasks available for formatting, linting, building, and testing. The `check` task orchestrated all tasks that must complete successfully for a change to be accepted.

### Running tests

```
poe test-rust # run rust unit tests
poe test-property # run rust property tests
poe test-schema # run rust schema tests
```

Feature flags control codec support: `snappy`, `deflate`, `zstd`, `bzip2`, `xz`. Disable what you don't need with `--no-default-features --features "snappy,zstd"` to optimize build times.

## Known Limitations

### Read-Only

Jetliner is a read-only library for Avro Object Container Files (`.avro`). It does not support:
- Writing Avro files
- Reading standalone schema files (`.avsc`) — schemas are extracted from the embedded header in `.avro` files

### Recursive Types

Avro supports recursive types (e.g., linked lists, trees) where a record can contain references to itself. Since Arrow and Polars don't natively support recursive data structures, Jetliner serializes recursive fields to JSON strings. This preserves data integrity while maintaining compatibility with the Polars DataFrame model.

Example: A binary tree node with `left` and `right` children will have those fields serialized as JSON strings that can be parsed if needed after reading.

### Nullable Enums

Avro enums wrapped in a union with null (e.g., `["null", {"type": "enum", ...}]`) are not yet supported. This triggers a "not implemented" panic in polars-core. Non-nullable enums work correctly and are mapped to Polars `Enum` type with proper categories.

### Fixed Type

Avro `fixed` type (fixed-size binary) causes a pyo3-polars panic during schema conversion. This is a known upstream limitation.

### Complex Top-Level Schemas

Avro is usually used as a table format, with a Record as the top level type. However it may also be used with any other type at the top level.

Jetliner support primitive top level schemas (int, long, string, bytes) which are treated in the resulting polars Dataframe as a Record with a single 'value' key. However complex types have the following limitations:

- **Arrays as top-level schema**: Not yet supported (Polars list builder constraints)
- **Maps as top-level schema**: Not yet supported (struct handling in list builder)

## Trivia

- The [Avro Canada C102 Jetliner](https://en.wikipedia.org/wiki/Avro_Canada_C102_Jetliner) was the worlds second purpose built jet powered airliner.

## Contributing

If you encounter an issue or have an idea for how to make jetliner more awesome, do come say hi in the issues 👋

If you discover an avro file that other libraries can read but jetliner fails (for reasons other than Known Limitation) then please share it.

## License

Licensed under the Apache License, Version 2.0.
See LICENSE for details.
