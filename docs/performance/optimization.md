# Optimization Tips

Strategies for maximizing Jetliner performance in different scenarios.

## General Optimization

### Use Query Optimization

Always use `scan()` with query operations to enable pushdown:

```python
import jetliner
import polars as pl

# Good: pushdown enabled
result = (
    jetliner.scan("data.avro")
    .select(["col1", "col2"])
    .filter(pl.col("col1") > 100)
    .collect()
)

# Less efficient: no pushdown
df = jetliner.scan("data.avro").collect()
result = df.select(["col1", "col2"]).filter(pl.col("col1") > 100)
```

### Select Columns Early

Put `.select()` as early as possible:

```python
# Good: projection happens during read
result = (
    jetliner.scan("data.avro")
    .select(["id", "value"])  # Early
    .filter(pl.col("value") > 0)
    .collect()
)
```

### Use Early Stopping

When you only need a subset of rows:

```python
# Stops reading after 1000 rows
result = jetliner.scan("data.avro").head(1000).collect()
```

## Buffer Configuration

### High-Throughput Settings

For maximum speed when memory is available:

```python
df = jetliner.scan(
    "data.avro",
    buffer_blocks=8,
    buffer_bytes=128 * 1024 * 1024,  # 128MB
).collect()
```

### Memory-Constrained Settings

For Lambda, containers, or limited memory:

```python
df = jetliner.scan(
    "data.avro",
    buffer_blocks=2,
    buffer_bytes=16 * 1024 * 1024,  # 16MB
).collect()
```

## S3 Optimization

### Minimize Round Trips

Use projection to reduce data transfer:

```python
# Only downloads needed columns
result = (
    jetliner.scan("s3://bucket/large.avro")
    .select(["id", "timestamp"])
    .collect()
)
```

### Regional Endpoints

Use endpoints close to your data:

```python
df = jetliner.scan(
    "s3://bucket/data.avro",
    storage_options={"region": "us-east-1"}
).collect()
```

## Lambda Optimization

For AWS Lambda with limited memory and time:

```python
import jetliner
import polars as pl

def handler(event, context):
    # Conservative settings
    result = (
        jetliner.scan(
            event["s3_uri"],
            buffer_blocks=2,
            buffer_bytes=32 * 1024 * 1024,
        )
        .select(["id", "value"])  # Only needed columns
        .filter(pl.col("value") > 0)
        .head(10000)  # Limit rows
        .collect()
    )
    return result.to_dicts()
```

## Codec Considerations

### For Read-Heavy Workloads

Prefer faster decompression:

- `snappy`: Very fast decompression
- `zstd`: Fast decompression, better compression

### For Storage-Constrained Environments

Accept slower reads for smaller files:

- `zstd`: Good balance
- `bzip2` / `xz`: Maximum compression

## Batch Size Tuning

For `AvroReader` API, adjust batch size:

```python
# Smaller batches: lower memory, more Python overhead
with jetliner.AvroReader("data.avro", batch_size=10_000) as reader:
    for batch in reader:
        process(batch)

# Larger batches: higher memory, less overhead
with jetliner.AvroReader("data.avro", batch_size=500_000) as reader:
    for batch in reader:
        process(batch)
```

## Profiling

Use Python profiling to identify bottlenecks:

```python
import cProfile
import jetliner

cProfile.run('jetliner.scan("data.avro").collect()')
```

For memory profiling:

```python
import tracemalloc
import jetliner

tracemalloc.start()
df = jetliner.scan("data.avro").collect()
current, peak = tracemalloc.get_traced_memory()
print(f"Current: {current / 1024**2:.1f}MB, Peak: {peak / 1024**2:.1f}MB")
tracemalloc.stop()
```
