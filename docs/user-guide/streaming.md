# Streaming Large Files

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

## Using open() for Streaming Control

The `open()` API gives you direct control over batch processing:

```python
import jetliner

with jetliner.AvroReader("large_file.avro") as reader:
    for batch in reader:
        # Process each batch individually
        # Memory is released after each iteration
        process(batch)
```

### Processing Without Accumulation

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

### Writing Results Incrementally

Stream results to disk without accumulating in memory:

```python
import jetliner

with jetliner.AvroReader("input.avro") as reader:
    for i, batch in enumerate(reader):
        # Process and write each batch
        processed = batch.filter(batch["amount"] > 0)
        processed.write_parquet(f"output/part_{i:04d}.parquet")
```

## Buffer Configuration

Jetliner uses prefetching to overlap I/O with processing. Configure buffers based on your environment:

### Parameters

| Parameter       | Default | Description                       |
| --------------- | ------- | --------------------------------- |
| `buffer_blocks` | 4       | Number of Avro blocks to prefetch |
| `buffer_bytes`  | 64MB    | Maximum bytes to buffer           |

### High-Throughput Settings

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

### Memory-Constrained Settings

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

### Batch Size Control

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

## Progress Tracking

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

## Memory Estimation

Estimate memory requirements for your data:

```python
import jetliner

# Check schema to estimate row size
with jetliner.AvroReader("data.avro") as reader:
    schema = reader.schema_dict

    # Get first batch to estimate memory per row
    first_batch = next(iter(reader))
    bytes_per_row = first_batch.estimated_size() / first_batch.height

    print(f"Estimated bytes per row: {bytes_per_row:.0f}")
    print(f"For 1M rows: {bytes_per_row * 1_000_000 / 1024**2:.0f} MB")
```

## Streaming with scan_avro()

The `scan_avro()` API also streams internally, but collects results at the end:

```python
import jetliner
import polars as pl

# Streaming happens internally, but collect() accumulates results
df = jetliner.scan_avro("large_file.avro").collect()
```

For truly large results, use `open()` or write results incrementally:

```python
import jetliner

# Stream and write without full accumulation
lf = jetliner.scan_avro("large_file.avro")
lf.sink_parquet("output.parquet")  # Polars streaming sink
```

## AWS Lambda Considerations

Lambda has limited memory (128MB - 10GB). Optimize for Lambda:

```python
import jetliner

def lambda_handler(event, context):
    # Conservative settings for Lambda
    with jetliner.AvroReader(
        event["s3_uri"],
        storage_options={"region": "us-east-1"},
        buffer_blocks=2,
        buffer_bytes=32 * 1024 * 1024,  # 32MB
        batch_size=50_000,
    ) as reader:
        results = []
        for batch in reader:
            # Process and aggregate, don't accumulate raw data
            summary = batch.group_by("category").agg(
                pl.col("amount").sum()
            )
            results.append(summary)

        return pl.concat(results).to_dicts()
```

## Parallel Processing

Process batches in parallel (when order doesn't matter):

```python
import jetliner
from concurrent.futures import ThreadPoolExecutor

def process_batch(batch):
    # CPU-bound processing
    return batch.filter(batch["amount"] > 0).height

with jetliner.AvroReader("data.avro") as reader:
    batches = list(reader)

with ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(process_batch, batches))

print(f"Total matching rows: {sum(results)}")
```

!!! warning "Memory"
    Collecting all batches into a list defeats streaming benefits. Use this pattern only when batches fit in memory.

## Best Practices

1. **Use open() for large files**: When you can't fit results in memory
2. **Process incrementally**: Aggregate or write results as you go
3. **Tune buffer settings**: Match your memory constraints
4. **Monitor memory**: Use tools like `psutil` to track usage
5. **Combine with projection**: Select only needed columns to reduce memory

## Troubleshooting

### Out of Memory

- Reduce `buffer_blocks` and `buffer_bytes`
- Use smaller `batch_size`
- Process batches without accumulating
- Select fewer columns with projection pushdown

### Slow Performance

- Increase `buffer_blocks` for more prefetching
- Increase `batch_size` for fewer Python iterations
- Use projection pushdown to read fewer columns

## Next Steps

- [Query Optimization](query-optimization.md) - Reduce data read
- [S3 Access](s3-access.md) - Stream from cloud storage
- [Error Handling](error-handling.md) - Handle failures gracefully
