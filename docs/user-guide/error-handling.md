# Error handling

Jetliner supports error recovery and reporting, to enable optimistic recovery of data from corrupted files, with detailed reporting of data corruption.

## Error modes

Jetliner supports two error handling modes:

| Mode             | Parameter             | Behavior                          |
| ---------------- | --------------------- | --------------------------------- |
| Strict (default) | `ignore_errors=False` | Fail immediately on first error   |
| Recovery         | `ignore_errors=True`  | Skip bad blocks, continue reading |

### Strict mode

Strict mode is the default behavior. When a corrupted record is encountered, reading fails immediately:

```python
import jetliner

# Strict mode (default)
try:
    df = jetliner.scan_avro("data.avro", ignore_errors=False).collect()
except jetliner.DecodeError as e:
    print(f"Failed to decode record: {e}")
```

### Recovery mode

Recovery mode continues reading when errors are encountered. Unreadable blocks are skipped:

```python
import jetliner

# Skip mode - skip bad blocks
df = jetliner.scan_avro("data.avro", ignore_errors=True).collect()

# Or with AvroReader
with jetliner.AvroReader("data.avro", ignore_errors=True) as reader:
    for batch in reader:
        process(batch)
```

#### Checking for errors

With `AvroReader` or `MultiAvroReader`, you can check if any blocks were skipped:

```python
import jetliner

with jetliner.AvroReader("data.avro", ignore_errors=True) as reader:
    batches = list(reader)

    # Check error count
    if reader.error_count > 0:
        print(f"Skipped {reader.error_count} bad records")

        # Get error details, including block details
        for error in reader.errors:
            print(f"  - {error}")
```

## Exception types

Jetliner defines specific exception types for different error conditions:

| Exception       | Cause                                              |
| --------------- | -------------------------------------------------- |
| `JetlinerError` | Base class for all Jetliner errors                 |
| `ParseError`    | Invalid Avro file format (bad magic bytes, header) |
| `SchemaError`   | Invalid or unsupported schema                      |
| `CodecError`    | Decompression failure                              |
| `DecodeError`   | Record decoding failure                            |
| `SourceError`   | File/S3 access errors                              |

### Structured exception types

For programmatic error handling, use the structured exception types with metadata attributes:

```python
import jetliner

try:
    df = jetliner.read_avro("corrupted.avro")
except jetliner.PyDecodeError as e:
    print(f"Error at block {e.block_index}, record {e.record_index}")
    print(f"Offset: {e.offset}")
    print(f"Message: {e.message}")
except jetliner.PySourceError as e:
    print(f"Source error for {e.path}: {e.message}")
except jetliner.PyParseError as e:
    print(f"Parse error at offset {e.offset}: {e.message}")
```

### Handling specific errors

```python
import jetliner

try:
    df = jetliner.scan_avro("data.avro").collect()
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

### Simplified error handling

For most cases, catching the base class is sufficient:

```python
import jetliner

try:
    df = jetliner.scan_avro("data.avro").collect()
except FileNotFoundError:
    print("File not found")
except jetliner.JetlinerError as e:
    print(f"Error reading Avro file: {e}")
```

## Error recovery patterns

### Retry with fallback

```python
import jetliner

def read_with_fallback(path):
    """Try strict mode first, fall back to skip mode."""
    try:
        return jetliner.scan_avro(path, ignore_errors=False).collect()
    except jetliner.DecodeError:
        print("Strict mode failed, retrying with skip mode")
        return jetliner.scan_avro(path, ignore_errors=True).collect()
```

### Validate before processing

```python
import jetliner

def validate_and_read(path):
    """Validate file before full read."""
    # Quick validation - read schema only
    try:
        schema = jetliner.read_avro_schema(path)
    except jetliner.JetlinerError as e:
        raise ValueError(f"Invalid Avro file: {e}")

    # Full read
    return jetliner.scan_avro(path).collect()
```

### Batch error handling

```python
import jetliner
from pathlib import Path

def process_directory(dir_path):
    """Process all Avro files, collecting errors."""
    results = []
    errors = []

    for path in Path(dir_path).glob("*.avro"):
        try:
            df = jetliner.scan_avro(str(path)).collect()
            results.append((path, df))
        except jetliner.JetlinerError as e:
            errors.append((path, e))

    if errors:
        print(f"Failed to read {len(errors)} files:")
        for path, error in errors:
            print(f"  {path}: {error}")

    return results
```

## Debugging corrupted files

### Inspect file header

```python
import jetliner

try:
    with jetliner.AvroReader("suspect.avro") as reader:
        print(f"Schema: {reader.schema}")
except jetliner.ParseError as e:
    print(f"Header is corrupted: {e}")
```

### Read with error details

```python
import jetliner

with jetliner.AvroReader("suspect.avro", ignore_errors=True) as reader:
    for i, batch in enumerate(reader):
        print(f"Batch {i}: {batch.height} rows")

    if reader.error_count > 0:
        print(f"\nErrors encountered:")
        for error in reader.errors:
            print(f"  {error}")
```

### Compare with reference implementation

```python
import jetliner
import fastavro

def compare_readers(path):
    """Compare Jetliner with fastavro for debugging."""
    # Read with fastavro
    with open(path, "rb") as f:
        fastavro_records = list(fastavro.reader(f))

    # Read with Jetliner
    with jetliner.AvroReader(path, ignore_errors=True) as reader:
        jetliner_batches = list(reader)
        jetliner_rows = sum(b.height for b in jetliner_batches)
        errors = reader.error_count

    print(f"fastavro: {len(fastavro_records)} records")
    print(f"Jetliner: {jetliner_rows} rows, {errors} errors")
```

## Best practices

1. **Use strict mode for production**: Ensures data integrity
2. **Use skip mode for exploration**: More resilient to data issues
3. **Log errors**: Always check `error_count` and `errors` in skip mode
4. **Validate critical data**: Use strict mode for data that must be complete
5. **Handle exceptions gracefully**: Catch specific exceptions when possible
6. **Use structured exceptions**: Access error metadata for detailed debugging

## Common error scenarios

### Truncated files

Files that were incompletely written:

```python
# Skip mode will read what's available
df = jetliner.scan_avro("truncated.avro", ignore_errors=True).collect()
```

### Corrupted compression

Blocks with invalid compressed data:

```python
# Skip mode skips corrupted blocks
df = jetliner.scan_avro("bad_compression.avro", ignore_errors=True).collect()
```

### Invalid records

Individual records with encoding errors:

```python
# Skip mode skips bad records within blocks
df = jetliner.scan_avro("bad_records.avro", ignore_errors=True).collect()
```

## Next steps

- [Schemas](schemas.md) - Understand your data structure
- [Streaming](streaming.md) - Handle large files gracefully
- [Data Sources](data-sources.md) - Paths, S3, codecs
