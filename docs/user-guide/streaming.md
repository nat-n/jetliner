# Streaming large files

Jetliner is designed for streaming large Avro files with minimal memory overhead. This guide covers memory-efficient processing techniques.

## Architecture

Jetliner reads Avro files block-by-block rather than loading entire files into memory:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Avro File  │ ──► │   Buffer    │ ──► │  DataFrame  │
│  (blocks)   │     │  (prefetch) │     │  (batches)  │
└─────────────┘     └─────────────┘     └─────────────┘
```

This streaming architecture enables processing files larger than available RAM.

## Using AvroReader for streaming control

The `AvroReader` API gives you direct control over batch processing:

```python
import jetliner

with jetliner.AvroReader("large_file.avro") as reader:
    for batch in reader:
        # Process each batch individually
        # Memory is released after each iteration
        process(batch)
```

### Processing without accumulation

For true streaming (constant memory usage):

```python
import jetliner

total_amount = 0
row_count = 0

with jetliner.AvroReader("huge_file.avro") as reader:
    for batch in reader:
        # Aggregate without keeping data in memory
        total_amount += batch["amount"].sum()
        row_count += batch.height

print(f"Total: {total_amount}, Rows: {row_count}")
```

### Writing results incrementally

Stream results to disk without accumulating in memory:

```python
import jetliner

with jetliner.AvroReader("input.avro") as reader:
    for i, batch in enumerate(reader):
        # Process and write each batch
        processed = batch.filter(batch["amount"] > 0)
        processed.write_parquet(f"output/part_{i:04d}.parquet")
```

## Buffer configuration

Jetliner uses prefetching to overlap I/O with processing. Configure buffers based on your environment:

### Parameters

| Parameter       | Default | Description                       |
| --------------- | ------- | --------------------------------- |
| `buffer_blocks` | 4       | Number of Avro blocks to prefetch |
| `buffer_bytes`  | 64MB    | Maximum bytes to buffer           |

### High-throughput settings

For maximum speed when memory is available:

```python
import jetliner

# More prefetching, larger buffer
df = jetliner.scan_avro(
    "data.avro",
    buffer_blocks=8,
    buffer_bytes=128 * 1024 * 1024,  # 128MB
).collect()
```

### Memory-constrained settings

For environments with limited memory (Lambda, containers):

```python
import jetliner

# Less prefetching, smaller buffer
with jetliner.AvroReader(
    "data.avro",
    buffer_blocks=2,
    buffer_bytes=16 * 1024 * 1024,  # 16MB
) as reader:
    for batch in reader:
        process(batch)
```

### Batch size control

Control the number of records per batch:

```python
import jetliner

# Smaller batches for fine-grained control
with jetliner.AvroReader("data.avro", batch_size=10_000) as reader:
    for batch in reader:
        assert batch.height <= 10_000
        process(batch)

# Larger batches for better throughput
with jetliner.AvroReader("data.avro", batch_size=500_000) as reader:
    for batch in reader:
        process(batch)
```

## Progress tracking

Track progress during streaming:

```python
import jetliner

with jetliner.AvroReader("large_file.avro") as reader:
    total_rows = 0
    batch_count = 0

    for batch in reader:
        batch_count += 1
        total_rows += batch.height

        if batch_count % 10 == 0:
            print(f"Processed {batch_count} batches, {total_rows:,} rows")

        process(batch)

    print(f"Complete: {batch_count} batches, {total_rows:,} rows")
```

### With tqdm

```python
import jetliner
from tqdm import tqdm

with jetliner.AvroReader("large_file.avro") as reader:
    for batch in tqdm(reader, desc="Processing"):
        process(batch)
```

## Streaming with scan_avro()

The `scan_avro()` API streams internally and collects results at the end. When combined with a selective filter, [predicate pushdown](query-optimization.md) keeps memory usage low by discarding filtered rows during reading:

```python
import jetliner
import polars as pl

# Memory-efficient: only matching rows are accumulated
df = (
    jetliner.scan_avro("large_file.avro")
    .filter(pl.col("status") == "active")  # Selective filter
    .collect()
)
```

To write large results without accumulating in memory, use Polars' streaming sink:

```python
jetliner.scan_avro("large_file.avro").sink_parquet("output.parquet")
```

For batch-by-batch processing with query optimization, Polars provides two streaming methods on LazyFrame. Both apply projection pushdown, predicate pushdown, and early stopping before delivering data in batches:

`sink_batches()` pushes batches to a callback:

```python
import jetliner
import polars as pl

(
    jetliner.scan_avro("large_file.avro")
    .select(["user_id", "amount"])
    .filter(pl.col("amount") > 100)
    .sink_batches(lambda batch: process(batch))
)
```

`collect_batches()` returns an iterator you pull from:

```python
import jetliner
import polars as pl

lf = (
    jetliner.scan_avro("large_file.avro")
    .select(["user_id", "amount"])
    .filter(pl.col("amount") > 100)
)

for batch in lf.collect_batches():
    process(batch)
```

!!! note "When to use AvroReader instead"
    `sink_batches` and `collect_batches` give you Polars query composition before batches are delivered, but the reader is managed internally. `AvroReader` and `MultiAvroReader` expose the reading process directly: error inspection (`.errors`, `.error_count`), schema access, progress tracking (`.rows_read`, `.is_finished`), and early termination with resource cleanup. Choose the Polars streaming methods when you need query composition; choose the iterator APIs when you need control over the reader itself.

## Next steps

- [Query Optimization](query-optimization.md) - Reduce data read
- [Data Sources](data-sources.md) - Paths, S3, codecs
- [Error Handling](error-handling.md) - Handle failures gracefully
