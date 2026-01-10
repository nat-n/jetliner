# API Reference

Complete API documentation for Jetliner.

## Functions

### scan

```python
def scan(
    path: str,
    *,
    buffer_blocks: int = 4,
    buffer_bytes: int = 64 * 1024 * 1024,
    strict: bool = False,
    storage_options: dict[str, str] | None = None,
) -> pl.LazyFrame
```

Scan an Avro file, returning a LazyFrame with query optimization support.

This function uses Polars' IO plugin system to enable query optimizations:

- **Projection pushdown**: Only read columns that are actually used in the query
- **Predicate pushdown**: Apply filters during reading, not after
- **Early stopping**: Stop reading after the requested number of rows

**Parameters:**

| Parameter         | Type   | Default  | Description                                                                  |
| ----------------- | ------ | -------- | ---------------------------------------------------------------------------- |
| `path`            | `str`  | required | Path to Avro file. Supports local paths and S3 URIs (`s3://bucket/key.avro`) |
| `buffer_blocks`   | `int`  | `4`      | Number of blocks to prefetch for better I/O performance                      |
| `buffer_bytes`    | `int`  | `64MB`   | Maximum bytes to buffer during prefetching                                   |
| `strict`          | `bool` | `False`  | If `True`, fail on first error. If `False`, skip bad records                 |
| `storage_options` | `dict` | `None`   | Configuration for S3 connections                                             |

**Returns:**

`pl.LazyFrame` - A LazyFrame that can be used with Polars query operations.

**Raises:**

- `FileNotFoundError` - If the file does not exist
- `PermissionError` - If access is denied
- `ParseError` - If the file is not a valid Avro file
- `SchemaError` - If the schema is invalid or cannot be converted
- `SourceError` - For S3 or filesystem errors

**Examples:**

```python
import jetliner
import polars as pl

# Basic scan
lf = jetliner.scan("data.avro")
df = lf.collect()

# S3 with credentials
df = jetliner.scan(
    "s3://bucket/file.avro",
    storage_options={
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
    }
).collect()

# Query with optimization
result = (
    jetliner.scan("file.avro")
    .select(["col1", "col2"])
    .filter(pl.col("amount") > 100)
    .head(1000)
    .collect()
)
```

---

### open

```python
def open(
    path: str,
    *,
    batch_size: int = 100_000,
    buffer_blocks: int = 4,
    buffer_bytes: int = 64 * 1024 * 1024,
    strict: bool = False,
    storage_options: dict[str, str] | None = None,
) -> AvroReader
```

Open an Avro file for streaming iteration.

Returns a context manager that yields DataFrame batches. Use this API when you need fine-grained control over batch processing, progress tracking, or memory management.

**Parameters:**

| Parameter         | Type   | Default   | Description                                         |
| ----------------- | ------ | --------- | --------------------------------------------------- |
| `path`            | `str`  | required  | Path to Avro file. Supports local paths and S3 URIs |
| `batch_size`      | `int`  | `100_000` | Maximum records per batch                           |
| `buffer_blocks`   | `int`  | `4`       | Number of blocks to prefetch                        |
| `buffer_bytes`    | `int`  | `64MB`    | Maximum bytes to buffer                             |
| `strict`          | `bool` | `False`   | If `True`, fail on first error                      |
| `storage_options` | `dict` | `None`    | Configuration for S3 connections                    |

**Returns:**

`AvroReader` - A context manager that iterates over DataFrame batches.

**Examples:**

```python
import jetliner

# Basic iteration
with jetliner.open("data.avro") as reader:
    for batch in reader:
        print(f"Batch: {batch.height} rows")

# Access schema
with jetliner.open("data.avro") as reader:
    print(reader.schema)
    print(reader.schema_dict)

# Check errors in skip mode
with jetliner.open("data.avro", strict=False) as reader:
    batches = list(reader)
    if reader.error_count > 0:
        print(f"Skipped {reader.error_count} records")
```

---

### parse_avro_schema

```python
def parse_avro_schema(
    path: str,
    *,
    storage_options: dict[str, str] | None = None,
) -> dict[str, pl.DataType]
```

Parse an Avro file's schema and return the equivalent Polars schema.

Only reads the file header, making it fast even for large files.

**Parameters:**

| Parameter         | Type   | Default  | Description                      |
| ----------------- | ------ | -------- | -------------------------------- |
| `path`            | `str`  | required | Path to Avro file                |
| `storage_options` | `dict` | `None`   | Configuration for S3 connections |

**Returns:**

`dict[str, pl.DataType]` - Dictionary mapping column names to Polars data types.

**Examples:**

```python
import jetliner

schema = jetliner.parse_avro_schema("data.avro")
print(schema)
# {'user_id': Int64, 'name': String, 'amount': Float64}

# From S3
schema = jetliner.parse_avro_schema(
    "s3://bucket/data.avro",
    storage_options={"region": "us-east-1"}
)
```

---

## Classes

### AvroReader

Context manager returned by `open()`. Provides iteration over DataFrame batches and schema access.

**Properties:**

| Property      | Type        | Description                                |
| ------------- | ----------- | ------------------------------------------ |
| `schema`      | `str`       | Avro schema as JSON string                 |
| `schema_dict` | `dict`      | Avro schema as Python dictionary           |
| `error_count` | `int`       | Number of records skipped (skip mode only) |
| `errors`      | `list[str]` | List of error messages (skip mode only)    |

**Usage:**

```python
import jetliner

with jetliner.open("data.avro") as reader:
    # Access schema before reading
    print(f"Schema: {reader.schema}")

    # Iterate over batches
    for batch in reader:
        process(batch)

    # Check for errors (skip mode)
    if reader.error_count > 0:
        for error in reader.errors:
            print(f"Error: {error}")
```

---

### AvroReaderCore

Low-level reader class used internally by `scan()` and `open()`. Most users should use those functions instead.

**Parameters:**

| Parameter           | Type        | Default   | Description                  |
| ------------------- | ----------- | --------- | ---------------------------- |
| `path`              | `str`       | required  | Path to Avro file            |
| `batch_size`        | `int`       | `100_000` | Records per batch            |
| `buffer_blocks`     | `int`       | `4`       | Blocks to prefetch           |
| `buffer_bytes`      | `int`       | `64MB`    | Max buffer size              |
| `strict`            | `bool`      | `False`   | Fail on first error          |
| `projected_columns` | `list[str]` | `None`    | Columns to read (projection) |
| `storage_options`   | `dict`      | `None`    | S3 configuration             |

---

## Exceptions

All Jetliner exceptions inherit from `JetlinerError`:

```python
import jetliner

try:
    df = jetliner.scan("data.avro").collect()
except jetliner.JetlinerError as e:
    print(f"Jetliner error: {e}")
```

### Exception Hierarchy

```
JetlinerError (base class)
├── ParseError      # Invalid Avro file format
├── SchemaError     # Invalid or unsupported schema
├── CodecError      # Decompression failure
├── DecodeError     # Record decoding failure
└── SourceError     # File/S3 access errors
```

### JetlinerError

Base exception class for all Jetliner errors. Catch this to handle any library-specific error.

### ParseError

Raised when the file is not a valid Avro file.

**Common causes:**

- Invalid magic bytes (file is not Avro format)
- Corrupted file header
- Truncated file

### SchemaError

Raised when the Avro schema is invalid or cannot be converted to Polars types.

**Common causes:**

- Malformed schema JSON
- Unsupported schema features
- Invalid type definitions

### CodecError

Raised when decompression fails.

**Common causes:**

- Corrupted compressed data
- Unsupported codec
- Invalid compression block

### DecodeError

Raised when a record cannot be decoded. In strict mode, this stops reading. In skip mode, the record is skipped.

**Common causes:**

- Corrupted record data
- Schema mismatch
- Invalid field encoding

### SourceError

Raised for file or S3 access errors.

**Common causes:**

- File not found
- Permission denied
- S3 authentication failure
- Network errors

---

## Storage Options

The `storage_options` parameter configures S3 access:

| Key                     | Description        | Example                 |
| ----------------------- | ------------------ | ----------------------- |
| `endpoint_url`          | Custom S3 endpoint | `http://localhost:9000` |
| `aws_access_key_id`     | AWS access key     | `AKIAIOSFODNN7EXAMPLE`  |
| `aws_secret_access_key` | AWS secret key     | `wJalrXUtnFEMI/...`     |
| `region`                | AWS region         | `us-east-1`             |

Explicit credentials take precedence over environment variables.

---

## Type Mapping

### Primitive Types

| Avro Type | Polars Type |
| --------- | ----------- |
| `null`    | `Null`      |
| `boolean` | `Boolean`   |
| `int`     | `Int32`     |
| `long`    | `Int64`     |
| `float`   | `Float32`   |
| `double`  | `Float64`   |
| `bytes`   | `Binary`    |
| `string`  | `String`    |

### Logical Types

| Avro Logical Type  | Polars Type    |
| ------------------ | -------------- |
| `date`             | `Date`         |
| `time-millis`      | `Time`         |
| `time-micros`      | `Time`         |
| `timestamp-millis` | `Datetime(ms)` |
| `timestamp-micros` | `Datetime(μs)` |
| `uuid`             | `String`       |
| `decimal`          | `Decimal`      |

### Complex Types

| Avro Type | Polars Type   |
| --------- | ------------- |
| `array`   | `List`        |
| `map`     | `Struct`      |
| `record`  | `Struct`      |
| `enum`    | `Categorical` |
| `fixed`   | `Binary`      |

### Union Types

- `["null", T]` → `T` (nullable)
- `["null", T1, T2, ...]` → `Struct` with type indicator
- `[T1, T2, ...]` (no null) → `Struct` with type indicator

### Recursive Types

Recursive types are serialized to JSON strings since Polars doesn't support recursive structures.
