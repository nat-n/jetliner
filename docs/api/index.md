# API Overview

Jetliner provides a focused API for reading Avro files into Polars DataFrames.

## Core Functions

### scan_avro()

The primary API for reading Avro files. Returns a Polars LazyFrame with query optimization support.

```python
import jetliner

lf = jetliner.scan_avro("data.avro")
df = lf.collect()
```

**Key features:**

- Projection pushdown (only read needed columns)
- Predicate pushdown (filter during read)
- Early stopping (stop after row limit)
- S3 support via `storage_options`
- Multi-file support with glob patterns

[Full documentation →](reference.md#scan_avro)

### read_avro()

Eager API for loading Avro files directly into a DataFrame with column selection.

```python
import jetliner

df = jetliner.read_avro("data.avro", columns=["col1", "col2"])
```

**Key features:**

- Column selection via `columns` parameter
- Row limiting via `n_rows` parameter
- Multi-file support with glob patterns
- Equivalent to `scan_avro(...).collect()` with eager projection

[Full documentation →](reference.md#read_avro)

### AvroReader

Iterator API for streaming control. Returns a context manager yielding DataFrame batches.

```python
import jetliner

with jetliner.AvroReader("data.avro") as reader:
    for batch in reader:
        process(batch)
```

**Key features:**

- Batch-by-batch processing
- Schema access before reading
- Error tracking in skip mode
- Memory-efficient streaming

[Full documentation →](reference.md#avroreader)

### read_avro_schema()

Extract Polars schema from an Avro file without reading data.

```python
import jetliner

schema = jetliner.read_avro_schema("data.avro")
# {'user_id': Int64, 'name': String, ...}
```

[Full documentation →](reference.md#read_avro_schema)

## Classes

### AvroReader

Context manager for iteration and schema access.

```python
with jetliner.AvroReader("data.avro") as reader:
    print(reader.schema)        # JSON string
    print(reader.schema_dict)   # Python dict
    print(reader.error_count)   # Errors in skip mode
```

[Full documentation →](reference.md#avroreader)

### AvroReaderCore

Low-level reader used internally. Most users should use `AvroReader` instead.

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

### Structured Exception Types

For programmatic error handling, use the structured exception types with metadata attributes:

| Exception       | Attributes                                         |
| --------------- | -------------------------------------------------- |
| `PyDecodeError` | `block_index`, `record_index`, `offset`, `message` |
| `PyParseError`  | `offset`, `message`                                |
| `PySourceError` | `path`, `message`                                  |
| `PySchemaError` | `message`                                          |
| `PyCodecError`  | `message`                                          |

```python
import jetliner

try:
    df = jetliner.scan_avro("data.avro").collect()
except jetliner.PyDecodeError as e:
    print(f"Error at block {e.block_index}, record {e.record_index}")
    print(f"Offset: {e.offset}")
except jetliner.JetlinerError as e:
    print(f"Error: {e}")
```

[Full documentation →](reference.md#exception-hierarchy)

## Quick Reference

### Common Parameters

| Parameter            | Type   | Default   | Description                                        |
| -------------------- | ------ | --------- | -------------------------------------------------- |
| `source`             | `str`  | required  | File path, S3 URI, or glob pattern                 |
| `columns`            | `list` | `None`    | Columns to read (read_avro only)                   |
| `n_rows`             | `int`  | `None`    | Maximum rows to read                               |
| `row_index_name`     | `str`  | `None`    | Name for row index column                          |
| `row_index_offset`   | `int`  | `0`       | Starting value for row index                       |
| `glob`               | `bool` | `True`    | Whether to expand glob patterns                    |
| `include_file_paths` | `str`  | `None`    | Column name for source file paths                  |
| `ignore_errors`      | `bool` | `False`   | Skip bad records instead of failing                |
| `batch_size`         | `int`  | 100,000   | Records per batch                                  |
| `buffer_blocks`      | `int`  | 4         | Blocks to prefetch                                 |
| `buffer_bytes`       | `int`  | 64MB      | Max buffer size                                    |
| `read_chunk_size`    | `int`  | `None`    | I/O read chunk size (auto-detect if None)          |
| `storage_options`    | `dict` | `None`    | S3 configuration                                   |

### Storage Options Keys

| Key                     | Description                                   |
| ----------------------- | --------------------------------------------- |
| `endpoint`              | Custom S3 endpoint (MinIO, LocalStack, R2)    |
| `aws_access_key_id`     | AWS access key                                |
| `aws_secret_access_key` | AWS secret key                                |
| `region`                | AWS region                                    |
| `max_retries`           | Maximum retry attempts for transient failures |

## Module Exports

All public symbols are available from the `jetliner` module:

```python
import jetliner

# Functions
jetliner.scan_avro
jetliner.read_avro
jetliner.read_avro_schema
jetliner.AvroReader

# Classes
jetliner.AvroReader
jetliner.AvroReaderCore

# Exceptions (legacy)
jetliner.JetlinerError
jetliner.ParseError
jetliner.SchemaError
jetliner.CodecError
jetliner.DecodeError
jetliner.SourceError

# Structured exceptions with metadata
jetliner.PyDecodeError
jetliner.PyParseError
jetliner.PySourceError
jetliner.PySchemaError
jetliner.PyCodecError

# Type aliases
jetliner.FileSource
```

## Next Steps

- [Full API Reference](reference.md) - Complete function signatures and details
- [User Guide](../user-guide/index.md) - Practical usage examples
