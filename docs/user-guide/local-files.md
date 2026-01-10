# Reading Local Files

This guide covers reading Avro files from the local filesystem.

## Basic Usage

### Using scan() (Recommended)

The `scan()` function returns a Polars LazyFrame, enabling query optimizations:

```python
import jetliner

# Read entire file
df = jetliner.scan("data.avro").collect()

# Relative paths work too
df = jetliner.scan("./data/records.avro").collect()

# Absolute paths
df = jetliner.scan("/home/user/data/records.avro").collect()
```

### Using open() for Streaming

The `open()` function returns an iterator for batch-by-batch processing:

```python
import jetliner

with jetliner.open("data.avro") as reader:
    for batch in reader:
        print(f"Batch with {batch.height} rows")
```

## Path Formats

Jetliner accepts various path formats:

```python
# Relative path
jetliner.scan("data.avro")

# Relative path with directory
jetliner.scan("./data/records.avro")

# Absolute path
jetliner.scan("/var/data/records.avro")

# Home directory expansion (Python handles this)
from pathlib import Path
jetliner.scan(str(Path.home() / "data" / "records.avro"))
```

## Working with Multiple Files

To read multiple Avro files, process them individually and concatenate:

```python
import jetliner
import polars as pl
from pathlib import Path

# Find all Avro files in a directory
avro_files = list(Path("data").glob("*.avro"))

# Read and concatenate
dfs = [jetliner.scan(str(f)).collect() for f in avro_files]
combined = pl.concat(dfs)
```

For large numbers of files, use lazy concatenation:

```python
import jetliner
import polars as pl
from pathlib import Path

avro_files = list(Path("data").glob("*.avro"))

# Create lazy frames
lazy_frames = [jetliner.scan(str(f)) for f in avro_files]

# Concatenate lazily and collect once
combined = pl.concat(lazy_frames).collect()
```

## Batch Processing with open()

The `open()` API gives you control over batch processing:

```python
import jetliner
import polars as pl

# Process batches with progress tracking
with jetliner.open("large_file.avro") as reader:
    total_rows = 0
    batches = []

    for batch in reader:
        total_rows += batch.height
        batches.append(batch)
        print(f"Processed {total_rows} rows...")

    # Combine all batches
    df = pl.concat(batches)
```

### Configuring Batch Size

Control memory usage by adjusting batch size:

```python
import jetliner

# Smaller batches for memory-constrained environments
with jetliner.open("data.avro", batch_size=10_000) as reader:
    for batch in reader:
        process(batch)

# Larger batches for better throughput
with jetliner.open("data.avro", batch_size=500_000) as reader:
    for batch in reader:
        process(batch)
```

## Accessing Schema Information

Get schema information before reading data:

```python
import jetliner

with jetliner.open("data.avro") as reader:
    # JSON string representation
    print(reader.schema)

    # Python dictionary
    schema_dict = reader.schema_dict
    print(f"Record type: {schema_dict['name']}")
    print(f"Fields: {[f['name'] for f in schema_dict['fields']]}")
```

Or use the standalone function:

```python
import jetliner

# Get Polars schema without reading data
polars_schema = jetliner.parse_avro_schema("data.avro")
print(polars_schema)
```

## Error Handling

Handle file errors gracefully:

```python
import jetliner

try:
    df = jetliner.scan("data.avro").collect()
except FileNotFoundError:
    print("File not found")
except jetliner.ParseError as e:
    print(f"Invalid Avro file: {e}")
except jetliner.SchemaError as e:
    print(f"Schema error: {e}")
```

## Performance Tips

1. **Use scan() for queries**: Enables projection and predicate pushdown
2. **Adjust buffer settings**: For very large files, tune `buffer_blocks` and `buffer_bytes`
3. **Process in batches**: Use `open()` when memory is constrained
4. **Select only needed columns**: Reduces I/O and memory usage

```python
import jetliner
import polars as pl

# Efficient: only reads two columns
df = (
    jetliner.scan("large_file.avro")
    .select(["id", "value"])
    .collect()
)

# Less efficient: reads all columns, then selects
df = jetliner.scan("large_file.avro").collect()
df = df.select(["id", "value"])
```

## Next Steps

- [S3 Access](s3-access.md) - Reading from cloud storage
- [Query Optimization](query-optimization.md) - Maximizing performance
- [Streaming Large Files](streaming.md) - Memory-efficient processing
