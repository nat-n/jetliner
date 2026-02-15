# Overview

<p align="center">
  <img src="assets/jetliner_logo.png" alt="Jetliner Logo" width="200">
</p>

Jetliner is a high-performance [Polars](https://pola.rs/) plugin for reading Avro files into DataFrames. Built in Rust with Python bindings, it streams data block-by-block with minimal memory overhead.

## Features

- **High-performance streaming** — Block-by-block processing with minimal memory footprint
- **(Almost) complete Avro spec support** — Reads almost any valid Avro (see [limitations](#known-limitations))
- **Query optimization** — Projection pushdown, predicate pushdown, and early stopping via Polars LazyFrames
- **S3 and local file support** — Same API for Amazon S3 and local disk, with glob resolution
- **All standard codecs** — null, snappy, deflate, zstd, bzip2, and xz compression
- **Flexible error handling** — Optionally skip bad blocks for resilience to data corruption

## Benchmarks

Jetliner significantly outperforms the alternatives. Yes, that's a log scale.

<iframe src="assets/benchmark_performance.html" width="100%" height="500" frameborder="0"></iframe>

The chart compares read times across four scenarios using 1M-row Avro files. Polars' built-in Avro reader is missing from "Complex" because it doesn't support maps.

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

    Requires [Rust](https://rustup.rs/).

    ```bash
    git clone https://github.com/nat-n/jetliner.git
    cd jetliner
    pip install maturin
    maturin develop --release
    ```

## Quick start

### Lazy reading with query optimization

Use `scan_avro()` for best performance—Polars pushes projections and predicates down to the reader:

```python
import jetliner
import polars as pl

df = (
    jetliner.scan_avro("s3://bucket/events/*.avro")
    .select("user_id", "event_type", "timestamp")  # Only these columns are read
    .filter(pl.col("event_type") == "purchase")    # Filters during reading
    .head(10_000)                                  # Stops after 10k matches
    .collect()
)
```

### Eager reading

Use `read_avro()` when you want a DataFrame immediately:

```python
df = jetliner.read_avro("data.avro", columns=["id", "name"], n_rows=1000)
```

### Streaming iteration

Use `AvroReader` for batch-by-batch control:

```python
for batch in jetliner.AvroReader("huge_file.avro", batch_size=50_000):
    process(batch)  # each batch is a DataFrame
```

### Error recovery

Skip corrupted blocks instead of failing:

```python
reader = jetliner.AvroReader("suspect_file.avro", ignore_errors=True)
for batch in reader:
    process(batch)

if reader.error_count > 0:
    print(f"Skipped {reader.error_count} bad blocks")
    for err in reader.errors:
        print(f"  Block {err.block_index}: {err.message}")
```

## Known limitations

- **Read-only** — Jetliner does not write Avro files
- **Object container files only** — Reads `.avro` files with embedded schemas; does not support single-object encoding (schema registries) or bare Avro encoding
- **Recursive types** — Serialized to JSON strings (Arrow/Polars don't support recursive structures)
- **Complex top-level schemas** — Arrays and maps as top-level types are not yet supported

## Next steps

- [User Guide](user-guide/index.md) — Data sources, query optimization, streaming, error handling
- [API Reference](api/scan_avro.md) — Complete function and class documentation
