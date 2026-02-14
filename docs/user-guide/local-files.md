# Reading Local Files

This guide covers reading Avro files from the local filesystem.

## Basic Usage

### Using scan_avro() (Recommended)

The `scan_avro()` function returns a Polars LazyFrame, enabling query optimizations:

```python
import jetliner

# Read entire file
df = jetliner.scan_avro("data.avro").collect()

# Relative paths work too
df = jetliner.scan_avro("./data/records.avro").collect()

# Absolute paths
df = jetliner.scan_avro("/home/user/data/records.avro").collect()
```

### Using read_avro() for Eager Loading

The `read_avro()` function returns a DataFrame directly with optional column selection:

```python
import jetliner

# Read entire file
df = jetliner.read_avro("data.avro")

# Read specific columns
df = jetliner.read_avro("data.avro", columns=["user_id", "amount"])

# Read specific columns by index
df = jetliner.read_avro("data.avro", columns=[0, 2, 5])

# Limit rows
df = jetliner.read_avro("data.avro", n_rows=1000)
```

### Using AvroReader for Streaming

The `AvroReader` class returns an iterator for batch-by-batch processing:

```python
import jetliner

with jetliner.AvroReader("data.avro") as reader:
    for batch in reader:
        print(f"Batch with {batch.height} rows")
```
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

Jetliner supports reading multiple Avro files with glob patterns or explicit lists:

```python
import jetliner

# Glob pattern
df = jetliner.read_avro("data/*.avro")

# Explicit list
df = jetliner.read_avro(["file1.avro", "file2.avro"])

# With row index continuity across files
df = jetliner.read_avro("data/*.avro", row_index_name="idx")

# Track which file each row came from
df = jetliner.read_avro("data/*.avro", include_file_paths="source_file")
```

For lazy evaluation with multiple files:

```python
import jetliner
import polars as pl

# Lazy concatenation with glob
lf = jetliner.scan_avro("data/*.avro")

# Apply transformations before collecting
result = (
    lf.select(["user_id", "amount"])
    .filter(pl.col("amount") > 100)
    .collect()
)
```

### Schema Validation

When reading multiple files, all files must have identical schemas. If schemas differ, an error is raised:

```python
import jetliner

# Reads successfully - identical schemas
df = jetliner.read_avro(["file1.avro", "file2.avro"])

# Raises SchemaError if schemas differ
# SchemaError: Schema mismatch between 'file1.avro' and 'file2.avro'. All files must have identical schemas.
```

## Batch Processing with AvroReader

The `AvroReader` API gives you control over batch processing:

```python
import jetliner
import polars as pl

# Process batches with progress tracking
with jetliner.AvroReader("large_file.avro") as reader:
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
with jetliner.AvroReader("data.avro", batch_size=10_000) as reader:
    for batch in reader:
        process(batch)

# Larger batches for better throughput
with jetliner.AvroReader("data.avro", batch_size=500_000) as reader:
    for batch in reader:
        process(batch)
```

## Accessing Schema Information

Get schema information before reading data:

```python
import jetliner

with jetliner.AvroReader("data.avro") as reader:
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
polars_schema = jetliner.read_avro_schema("data.avro")
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

1. **Use scan_avro() for queries**: Enables projection and predicate pushdown
2. **Use read_avro() with columns**: For eager loading with column selection
3. **Adjust buffer settings**: For very large files, tune `buffer_blocks` and `buffer_bytes`
4. **Process in batches**: Use `AvroReader` when memory is constrained
5. **Select only needed columns**: Reduces I/O and memory usage

```python
import jetliner
import polars as pl

# Efficient: only reads two columns (via LazyFrame)
df = (
    jetliner.scan_avro("large_file.avro")
    .select(["id", "value"])
    .collect()
)

# Efficient: only reads two columns (via read_avro)
df = jetliner.read_avro("large_file.avro", columns=["id", "value"])

# Less efficient: reads all columns, then selects
df = jetliner.scan_avro("large_file.avro").collect()
df = df.select(["id", "value"])
```

## Next Steps

- [S3 Access](s3-access.md) - Reading from cloud storage
- [Query Optimization](query-optimization.md) - Maximizing performance
- [Streaming Large Files](streaming.md) - Memory-efficient processing
