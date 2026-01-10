# API Overview

Jetliner provides a focused API for reading Avro files into Polars DataFrames.

## Core Functions

### scan()

The primary API for reading Avro files. Returns a Polars LazyFrame with query optimization support.

```python
import jetliner

lf = jetliner.scan("data.avro")
df = lf.collect()
```

**Key features:**

- Projection pushdown (only read needed columns)
- Predicate pushdown (filter during read)
- Early stopping (stop after row limit)
- S3 support via `storage_options`

[Full documentation →](reference.md#scan)

### open()

Iterator API for streaming control. Returns a context manager yielding DataFrame batches.

```python
import jetliner

with jetliner.open("data.avro") as reader:
    for batch in reader:
        process(batch)
```

**Key features:**

- Batch-by-batch processing
- Schema access before reading
- Error tracking in skip mode
- Memory-efficient streaming

[Full documentation →](reference.md#open)

### parse_avro_schema()

Extract Polars schema from an Avro file without reading data.

```python
import jetliner

schema = jetliner.parse_avro_schema("data.avro")
# {'user_id': Int64, 'name': String, ...}
```

[Full documentation →](reference.md#parse_avro_schema)

## Classes

### AvroReader

Context manager returned by `open()`. Provides iteration and schema access.

```python
with jetliner.open("data.avro") as reader:
    print(reader.schema)        # JSON string
    print(reader.schema_dict)   # Python dict
    print(reader.error_count)   # Errors in skip mode
```

[Full documentation →](reference.md#avroreader)

### AvroReaderCore

Low-level reader used internally. Most users should use `open()` instead.

[Full documentation →](reference.md#avroreadercore)

## Exception Types

All exceptions inherit from `JetlinerError`:

| Exception       | When Raised                   |
| --------------- | ----------------------------- |
| `JetlinerError` | Base class for all errors     |
| `ParseError`    | Invalid Avro file format      |
| `SchemaError`   | Invalid or unsupported schema |
| `CodecError`    | Decompression failure         |
| `DecodeError`   | Record decoding failure       |
| `SourceError`   | File/S3 access errors         |

```python
import jetliner

try:
    df = jetliner.scan("data.avro").collect()
except jetliner.ParseError as e:
    print(f"Invalid file: {e}")
except jetliner.JetlinerError as e:
    print(f"Error: {e}")
```

[Full documentation →](reference.md#exception-hierarchy)

## Quick Reference

### Common Parameters

| Parameter         | Type   | Default  | Description         |
| ----------------- | ------ | -------- | ------------------- |
| `path`            | `str`  | required | File path or S3 URI |
| `batch_size`      | `int`  | 100,000  | Records per batch   |
| `buffer_blocks`   | `int`  | 4        | Blocks to prefetch  |
| `buffer_bytes`    | `int`  | 64MB     | Max buffer size     |
| `strict`          | `bool` | `False`  | Fail on first error |
| `storage_options` | `dict` | `None`   | S3 configuration    |

### Storage Options Keys

| Key                     | Description        |
| ----------------------- | ------------------ |
| `endpoint_url`          | Custom S3 endpoint |
| `aws_access_key_id`     | AWS access key     |
| `aws_secret_access_key` | AWS secret key     |
| `region`                | AWS region         |

## Module Exports

All public symbols are available from the `jetliner` module:

```python
import jetliner

# Functions
jetliner.scan
jetliner.open
jetliner.parse_avro_schema

# Classes
jetliner.AvroReader
jetliner.AvroReaderCore

# Exceptions
jetliner.JetlinerError
jetliner.ParseError
jetliner.SchemaError
jetliner.CodecError
jetliner.DecodeError
jetliner.SourceError
```

## Next Steps

- [Full API Reference](reference.md) - Complete function signatures and details
- [User Guide](../user-guide/index.md) - Practical usage examples
