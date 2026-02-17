<p align="center">
  <img src="https://raw.githubusercontent.com/nat-n/jetliner/main/docs/assets/jetliner_logo.png" alt="Jetliner" width="400">
</p>

<p align="center">
  <a href="https://pypi.org/project/jetliner/"><img src="https://img.shields.io/pypi/v/jetliner.svg" alt="PyPI version"></a>
  <a href="https://pypi.org/project/jetliner/"><img src="https://img.shields.io/pypi/pyversions/jetliner.svg" alt="Python versions"></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="http://jetliner.natn.io/"><img src="https://img.shields.io/badge/docs-mkdocs-blue.svg" alt="Documentation"></a>
</p>

A high-performance a [Polars](https://pola.rs/) plugin written in Rust with python bindings for fast and memory efficient reading of Avro files into DataFrames.

Jetliner is designed for data pipelines where Avro files live on S3 or local disk and need to land in Polars fast. It streams data block-by-block rather than loading entire files into memory, uses zero-copy techniques, and has (almost) complete support for the Avro spec (see [Known Limitations](#known-limitations)).

[Read the docs 📖](http://jetliner.natn.io/)

## Features
- **High-performance streaming** — Supports block-by-block processing with minimal memory footprint, ideal for large files
- **Idiomatic polars integration** — Feels like a Polars native API
- **Query optimization** — Projection pushdown (select columns) and predicate pushdown (filter rows) at the source via Polars LazyFrames
- **S3 and local file support** — Read Avro files from Amazon S3 or local disk with the same API, including glob patterns
- **All standard codecs** — null, snappy, deflate, zstd, bzip2, and xz compression out of the box
- **(Almost) complete avro schema support** — reads almost any valid avro (see [limitations](#known-limitations))
- **Flexible error handling** — Optionally skip bad blocks for resilience to data corruption
- **Ridiculously fast reads** — Check the benchmarks!

This library was created to serve performance critical scenarios around processing large avro files from python. It's fast but limited to read use cases. If you also need to write avro files from Polars then you should check [polars-avro](https://github.com/hafaio/polars-avro).

## Performance benchmarks

Jetliner is built for speed, and significantly outperforms the alternatives. Yes, that's a log scale.

<p align="center">
  <img src="https://raw.githubusercontent.com/nat-n/jetliner/main/docs/assets/benchmark_performance.png" alt="Benchmark comparison" width="800">
</p>

The chart compares read times across four scenarios using 1M-row Avro files. Note that Polars' built-in Avro reader is missing from the "Complex" all the complex field types.

## Installation

Install from PyPI using pip or your favorite python dependency manager:

```bash
pip install jetliner
```

## Quick Start

### Lazy Reading with Query Optimization

Use `scan_avro()` for the best performance — Polars pushes projections and predicates down to the reader:

```python
import jetliner
import polars as pl

df = (
    jetliner.scan_avro("s3://bucket/events/*.avro")
    .select("user_id", "event_type", "timestamp") # Only these columns are loaded
    .filter(pl.col("event_type") == "purchase")   # Filter rows as they're loaded
    .head(10_000)                                 # Stops reading after 10k matches
    .collect()
)
```

### Eager Reading with Column Selection

Use `read_avro()` when you want a DataFrame immediately:

```python
df = jetliner.read_avro("data.avro", columns=["id", "name"], n_rows=1000)
```

### Streaming Iteration

Use `AvroReader` or `MultiAvroReader` for batch-by-batch control — useful for progress tracking, memory management, or custom pipelines:

```python
for batch in jetliner.AvroReader("huge_file.avro", batch_size=50_000):
    process(batch)  # each batch is a DataFrame

# MultiAvroReader handles multiple files with continuous row indexing
reader = jetliner.MultiAvroReader(
    ["file1.avro", "file2.avro"],
    row_index_name="idx",           # Continuous index across files
    include_file_paths="source",    # Track which file each row came from
)
for batch in reader:
    process(batch)
```

### Schema Inspection

Inspect the schema without reading data:

```python
schema = jetliner.read_avro_schema("data.avro")
print(schema)  # Polars Schema showing column names and types
```

### Reading from S3 and Local Files

All APIs work with local paths, S3 URIs, and glob patterns:

```python
# Local files
df = jetliner.read_avro("./data/events.avro")
df = jetliner.read_avro("./data/**/*.avro")  # Recursive glob

# S3 (credentials from environment or explicit)
df = jetliner.read_avro("s3://bucket/path/to/file.avro")
df = jetliner.read_avro(
    "s3://bucket/data/*.avro",
    storage_options={
        "endpoint": "http://localhost:9000",  # MinIO, LocalStack, R2
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
    }
)
```

### Error Recovery

Skip corrupted blocks instead of failing — errors are collected for inspection:

```python
reader = jetliner.AvroReader("suspect_file.avro", ignore_errors=True)
for batch in reader:
    process(batch)

if reader.error_count > 0:
    print(f"Skipped {reader.error_count} bad blocks")
    for err in reader.errors:
        print(f"  Block {err.block_index}: {err.message}")
```

### Performance Tuning

Fine-tune for your workload:

```python
df = jetliner.scan_avro(
    "s3://bucket/data/*.avro",
    batch_size=100_000,        # Rows per batch (default: 100k)
    buffer_blocks=8,           # Prefetch buffer depth (default: 4)
    buffer_bytes=128*1024*1024,# Prefetch buffer size (default: 64MB)
    read_chunk_size=8*1024*1024,# S3 read chunks (default: 4MB for S3)
).collect()
```

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
poe test-python # run python e2e tests
```

## Known Limitations

### Read-Only

Jetliner is a read-only library. It does not support writing Avro files.

### Avro object container files only

Jetliner reads **Avro Object Container Files** (`.avro`) — self-contained files where the schema is embedded in the file header. It does not support:
- **Single-object encoding** — Used with schema registries (e.g., Confluent Schema Registry, Kafka). These encode objects with a schema fingerprint that requires external lookup.
- **Bare Avro encoding** — Raw Avro binary without any schema information.
- **Standalone schema files** (`.avsc`) — Schema JSON files are not read directly; schemas are extracted from `.avro` file headers.

### Recursive types

Avro supports recursive types (e.g., linked lists, trees) where a record can contain references to itself. Since Arrow and Polars don't natively support recursive data structures, Jetliner serializes recursive fields to JSON strings. This preserves data integrity while maintaining compatibility with the Polars DataFrame model.

Example: A binary tree node with `left` and `right` children will have those fields serialized as JSON strings that can be parsed if needed after reading.

### Complex top-level schemas

Avro is usually used as a table format, with a Record as the top level type. However it may also be used with any other type at the top level.

Jetliner support primitive top level schemas (int, long, string, bytes) which are treated in the resulting polars Dataframe as a Record with a single 'value' key. However complex types have the following limitations:

- **Arrays as top-level schema**: Not yet supported (Polars list builder constraints)
- **Maps as top-level schema**: Not yet supported (struct handling in list builder)

### Empty schemas

An avro schema may consist of a Record with zero fields. Since Polars cannot represent a DataFrame with zero columns, such avro files are no compatible with Jetliner.

## Trivia

- The [Avro Canada C102 Jetliner](https://en.wikipedia.org/wiki/Avro_Canada_C102_Jetliner) was the worlds second purpose built jet powered airliner.

## Contributing

If you encounter an issue or have an idea for how to make jetliner more awesome, do come say hi in the issues 👋

If you discover an avro file that other libraries can read but jetliner fails (for reasons other than Known Limitation) then please share it.

## License

Licensed under the Apache License, Version 2.0.
See LICENSE for details.
