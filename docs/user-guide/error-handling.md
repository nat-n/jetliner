# Error Handling

Jetliner provides flexible error handling modes for dealing with corrupted or malformed Avro data.

## Error Modes

Jetliner supports two error handling modes:

| Mode           | Parameter      | Behavior                           |
| -------------- | -------------- | ---------------------------------- |
| Skip (default) | `strict=False` | Skip bad records, continue reading |
| Strict         | `strict=True`  | Fail immediately on first error    |

## Skip Mode (Default)

Skip mode is the default behavior. When a corrupted record is encountered, it's skipped and reading continues:

```python
import jetliner

# Skip mode (default)
df = jetliner.scan("data.avro", strict=False).collect()

# Or explicitly
with jetliner.open("data.avro", strict=False) as reader:
    for batch in reader:
        process(batch)
```

### Checking for Errors

With `open()`, you can check if any records were skipped:

```python
import jetliner

with jetliner.open("data.avro", strict=False) as reader:
    batches = list(reader)

    # Check error count
    if reader.error_count > 0:
        print(f"Skipped {reader.error_count} bad records")

        # Get error details
        for error in reader.errors:
            print(f"  - {error}")
```

### When to Use Skip Mode

- Processing data from unreliable sources
- When partial results are acceptable
- Batch processing where some failures are expected
- Data exploration and debugging

## Strict Mode

Strict mode fails immediately when any error is encountered:

```python
import jetliner

# Strict mode - fail on first error
try:
    df = jetliner.scan("data.avro", strict=True).collect()
except jetliner.DecodeError as e:
    print(f"Failed to decode record: {e}")
```

### When to Use Strict Mode

- Data validation pipelines
- When data integrity is critical
- Testing and development
- When you need to know about every error

## Exception Types

Jetliner defines specific exception types for different error conditions:

| Exception       | Cause                                              |
| --------------- | -------------------------------------------------- |
| `JetlinerError` | Base class for all Jetliner errors                 |
| `ParseError`    | Invalid Avro file format (bad magic bytes, header) |
| `SchemaError`   | Invalid or unsupported schema                      |
| `CodecError`    | Decompression failure                              |
| `DecodeError`   | Record decoding failure                            |
| `SourceError`   | File/S3 access errors                              |

### Handling Specific Errors

```python
import jetliner

try:
    df = jetliner.scan("data.avro").collect()
except FileNotFoundError:
    print("File not found")
except jetliner.ParseError as e:
    print(f"Not a valid Avro file: {e}")
except jetliner.SchemaError as e:
    print(f"Schema problem: {e}")
except jetliner.CodecError as e:
    print(f"Decompression failed: {e}")
except jetliner.DecodeError as e:
    print(f"Record decode failed: {e}")
except jetliner.SourceError as e:
    print(f"Source access error: {e}")
except jetliner.JetlinerError as e:
    print(f"Other Jetliner error: {e}")
```

### Simplified Error Handling

For most cases, catching the base class is sufficient:

```python
import jetliner

try:
    df = jetliner.scan("data.avro").collect()
except FileNotFoundError:
    print("File not found")
except jetliner.JetlinerError as e:
    print(f"Error reading Avro file: {e}")
```

## Error Recovery Patterns

### Retry with Fallback

```python
import jetliner

def read_with_fallback(path):
    """Try strict mode first, fall back to skip mode."""
    try:
        return jetliner.scan(path, strict=True).collect()
    except jetliner.DecodeError:
        print("Strict mode failed, retrying with skip mode")
        return jetliner.scan(path, strict=False).collect()
```

### Validate Before Processing

```python
import jetliner

def validate_and_read(path):
    """Validate file before full read."""
    # Quick validation - read schema only
    try:
        schema = jetliner.parse_avro_schema(path)
    except jetliner.JetlinerError as e:
        raise ValueError(f"Invalid Avro file: {e}")

    # Full read
    return jetliner.scan(path).collect()
```

### Batch Error Handling

```python
import jetliner
from pathlib import Path

def process_directory(dir_path):
    """Process all Avro files, collecting errors."""
    results = []
    errors = []

    for path in Path(dir_path).glob("*.avro"):
        try:
            df = jetliner.scan(str(path)).collect()
            results.append((path, df))
        except jetliner.JetlinerError as e:
            errors.append((path, e))

    if errors:
        print(f"Failed to read {len(errors)} files:")
        for path, error in errors:
            print(f"  {path}: {error}")

    return results
```

## Debugging Corrupted Files

### Inspect File Header

```python
import jetliner

try:
    with jetliner.open("suspect.avro") as reader:
        print(f"Schema: {reader.schema}")
except jetliner.ParseError as e:
    print(f"Header is corrupted: {e}")
```

### Read with Error Details

```python
import jetliner

with jetliner.open("suspect.avro", strict=False) as reader:
    for i, batch in enumerate(reader):
        print(f"Batch {i}: {batch.height} rows")

    if reader.error_count > 0:
        print(f"\nErrors encountered:")
        for error in reader.errors:
            print(f"  {error}")
```

### Compare with Reference Implementation

```python
import jetliner
import fastavro

def compare_readers(path):
    """Compare Jetliner with fastavro for debugging."""
    # Read with fastavro
    with open(path, "rb") as f:
        fastavro_records = list(fastavro.reader(f))

    # Read with Jetliner
    with jetliner.open(path, strict=False) as reader:
        jetliner_batches = list(reader)
        jetliner_rows = sum(b.height for b in jetliner_batches)
        errors = reader.error_count

    print(f"fastavro: {len(fastavro_records)} records")
    print(f"Jetliner: {jetliner_rows} rows, {errors} errors")
```

## Best Practices

1. **Use skip mode for production**: More resilient to data issues
2. **Use strict mode for testing**: Catch problems early
3. **Log errors**: Always check `error_count` and `errors` in skip mode
4. **Validate critical data**: Use strict mode for data that must be complete
5. **Handle exceptions gracefully**: Catch specific exceptions when possible

## Common Error Scenarios

### Truncated Files

Files that were incompletely written:

```python
# Skip mode will read what's available
df = jetliner.scan("truncated.avro", strict=False).collect()
```

### Corrupted Compression

Blocks with invalid compressed data:

```python
# Skip mode skips corrupted blocks
df = jetliner.scan("bad_compression.avro", strict=False).collect()
```

### Invalid Records

Individual records with encoding errors:

```python
# Skip mode skips bad records within blocks
df = jetliner.scan("bad_records.avro", strict=False).collect()
```

## Next Steps

- [Schema Inspection](schemas.md) - Understand your data structure
- [Streaming Large Files](streaming.md) - Handle large files gracefully
- [Local Files](local-files.md) - Basic file reading
