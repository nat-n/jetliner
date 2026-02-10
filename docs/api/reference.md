# API Reference

Complete API documentation for Jetliner.

## Functions

### scan_avro

```python
def scan_avro(
    source: str | Path | Sequence[str] | Sequence[Path],
    *,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
    glob: bool = True,
    include_file_paths: str | None = None,
    ignore_errors: bool = False,
    storage_options: dict[str, str] | None = None,
    buffer_blocks: int = 4,
    buffer_bytes: int = 64 * 1024 * 1024,
    read_chunk_size: int | None = None,
    batch_size: int = 100_000,
) -> pl.LazyFrame
```

Scan Avro file(s), returning a LazyFrame with query optimization support.

This function uses Polars' IO plugin system to enable query optimizations:

- **Projection pushdown**: Only read columns that are actually used in the query
- **Predicate pushdown**: Apply filters during reading, not after
- **Early stopping**: Stop reading after the requested number of rows

**Parameters:**

| Parameter            | Type   | Default   | Description                                                                  |
| -------------------- | ------ | --------- | ---------------------------------------------------------------------------- |
| `source`             | `str`  | required  | Path to Avro file(s). Supports local paths, S3 URIs, glob patterns, or lists |
| `n_rows`             | `int`  | `None`    | Maximum number of rows to read                                               |
| `row_index_name`     | `str`  | `None`    | Name for row index column (inserted as first column)                         |
| `row_index_offset`   | `int`  | `0`       | Starting value for row index                                                 |
| `glob`               | `bool` | `True`    | Whether to expand glob patterns in paths                                     |
| `include_file_paths` | `str`  | `None`    | Column name for source file paths                                            |
| `ignore_errors`      | `bool` | `False`   | If `True`, skip bad records. If `False`, fail on first error                 |
| `storage_options`    | `dict` | `None`    | Configuration for S3 connections                                             |
| `buffer_blocks`      | `int`  | `4`       | Number of blocks to prefetch for better I/O performance                      |
| `buffer_bytes`       | `int`  | `64MB`    | Maximum bytes to buffer during prefetching                                   |
| `read_chunk_size`    | `int`  | `None`    | I/O read chunk size in bytes (auto-detect if None)                           |
| `batch_size`         | `int`  | `100_000` | Target number of rows per DataFrame batch                                    |

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
lf = jetliner.scan_avro("data.avro")
df = lf.collect()

# S3 with credentials
df = jetliner.scan_avro(
    "s3://bucket/file.avro",
    storage_options={
        "endpoint": "http://localhost:9000",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
    }
).collect()

# Query with optimization
result = (
    jetliner.scan_avro("file.avro")
    .select(["col1", "col2"])
    .filter(pl.col("amount") > 100)
    .head(1000)
    .collect()
)

# Multiple files with glob pattern
df = jetliner.scan_avro("data/*.avro").collect()

# With row index
df = jetliner.scan_avro("file.avro", row_index_name="idx").collect()

# With file path tracking
df = jetliner.scan_avro("data/*.avro", include_file_paths="source_file").collect()
```

---

### read_avro

```python
def read_avro(
    source: str | Path | Sequence[str] | Sequence[Path],
    *,
    columns: Sequence[str] | Sequence[int] | None = None,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
    glob: bool = True,
    include_file_paths: str | None = None,
    ignore_errors: bool = False,
    storage_options: dict[str, str] | None = None,
    buffer_blocks: int = 4,
    buffer_bytes: int = 64 * 1024 * 1024,
    read_chunk_size: int | None = None,
    batch_size: int = 100_000,
) -> pl.DataFrame
```

Read Avro file(s) into a DataFrame with optional column selection.

This function is equivalent to `scan_avro(...).collect()` with eager column selection.

**Parameters:**

| Parameter            | Type   | Default   | Description                                                                  |
| -------------------- | ------ | --------- | ---------------------------------------------------------------------------- |
| `source`             | `str`  | required  | Path to Avro file(s). Supports local paths, S3 URIs, glob patterns, or lists |
| `columns`            | `list` | `None`    | Columns to read (by name or index). None reads all columns                   |
| `n_rows`             | `int`  | `None`    | Maximum number of rows to read                                               |
| `row_index_name`     | `str`  | `None`    | Name for row index column (inserted as first column)                         |
| `row_index_offset`   | `int`  | `0`       | Starting value for row index                                                 |
| `glob`               | `bool` | `True`    | Whether to expand glob patterns in paths                                     |
| `include_file_paths` | `str`  | `None`    | Column name for source file paths                                            |
| `ignore_errors`      | `bool` | `False`   | If `True`, skip bad records. If `False`, fail on first error                 |
| `storage_options`    | `dict` | `None`    | Configuration for S3 connections                                             |
| `buffer_blocks`      | `int`  | `4`       | Number of blocks to prefetch                                                 |
| `buffer_bytes`       | `int`  | `64MB`    | Maximum bytes to buffer                                                      |
| `read_chunk_size`    | `int`  | `None`    | I/O read chunk size in bytes (auto-detect if None)                           |
| `batch_size`         | `int`  | `100_000` | Target number of rows per batch                                              |

**Returns:**

`pl.DataFrame` - A DataFrame containing the Avro data.

**Examples:**

```python
import jetliner

# Read entire file
df = jetliner.read_avro("data.avro")

# Read specific columns by name
df = jetliner.read_avro("data.avro", columns=["user_id", "amount"])

# Read specific columns by index
df = jetliner.read_avro("data.avro", columns=[0, 2, 5])

# Limit rows
df = jetliner.read_avro("data.avro", n_rows=1000)

# Multiple files
df = jetliner.read_avro(["file1.avro", "file2.avro"])

# Glob pattern
df = jetliner.read_avro("data/*.avro")
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
with jetliner.AvroReader("data.avro") as reader:
    for batch in reader:
        print(f"Batch: {batch.height} rows")

# Access schema
with jetliner.AvroReader("data.avro") as reader:
    print(reader.schema)
    print(reader.schema_dict)

# Check errors in skip mode
with jetliner.AvroReader("data.avro", ignore_errors=True) as reader:
    batches = list(reader)
    if reader.error_count > 0:
        print(f"Skipped {reader.error_count} records")
```

---

### read_avro_schema

```python
def read_avro_schema(
    source: str | Path | Sequence[str] | Sequence[Path],
    *,
    storage_options: dict[str, str] | None = None,
) -> dict[str, pl.DataType]
```

Read an Avro file's schema and return the equivalent Polars schema.

Only reads the file header, making it fast even for large files.

**Parameters:**

| Parameter         | Type   | Default  | Description                      |
| ----------------- | ------ | -------- | -------------------------------- |
| `source`          | `str`  | required | Path to Avro file                |
| `storage_options` | `dict` | `None`   | Configuration for S3 connections |

**Returns:**

`dict[str, pl.DataType]` - Dictionary mapping column names to Polars data types.

**Examples:**

```python
import jetliner

schema = jetliner.read_avro_schema("data.avro")
print(schema)
# {'user_id': Int64, 'name': String, 'amount': Float64}

# From S3
schema = jetliner.read_avro_schema(
    "s3://bucket/data.avro",
    storage_options={"region": "us-east-1"}
)
```

---

## Classes

### AvroReader

Single-file Avro reader for batch iteration. Use when you need control over batch processing, progress tracking, or error inspection.

**Constructor:**

```python
AvroReader(
    path: str,
    *,
    batch_size: int = 100_000,
    buffer_blocks: int = 4,
    buffer_bytes: int = 64 * 1024 * 1024,
    ignore_errors: bool = False,
    projected_columns: list[str] | None = None,
    storage_options: dict[str, str] | None = None,
    read_chunk_size: int | None = None,
    max_block_size: int | None = 512 * 1024 * 1024,
)
```

**Properties:**

| Property          | Type                | Description                                      |
| ----------------- | ------------------- | ------------------------------------------------ |
| `schema`          | `str`               | Avro schema as JSON string                       |
| `schema_dict`     | `dict`              | Avro schema as Python dictionary                 |
| `batch_size`      | `int`               | Target rows per batch                            |
| `pending_records` | `int`               | Records buffered for next batch                  |
| `is_finished`     | `bool`              | Whether iteration is complete                    |
| `error_count`     | `int`               | Number of errors (with `ignore_errors=True`)     |
| `errors`          | `list[BadBlockError]` | Error details (with `ignore_errors=True`)      |

**Usage:**

```python
import jetliner

with jetliner.AvroReader("data.avro") as reader:
    print(f"Schema: {reader.schema_dict}")
    for batch in reader:
        process(batch)

# With error handling
with jetliner.AvroReader("data.avro", ignore_errors=True) as reader:
    for batch in reader:
        process(batch)
    if reader.error_count > 0:
        for err in reader.errors:
            print(f"[{err.kind}] Block {err.block_index}: {err.message}")
```

---

### MultiAvroReader

Multi-file Avro reader for batch iteration with row index continuity across files.

**Constructor:**

```python
MultiAvroReader(
    paths: list[str],
    *,
    batch_size: int = 100_000,
    buffer_blocks: int = 4,
    buffer_bytes: int = 64 * 1024 * 1024,
    ignore_errors: bool = False,
    projected_columns: list[str] | None = None,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
    include_file_paths: str | None = None,
    storage_options: dict[str, str] | None = None,
    read_chunk_size: int | None = None,
    max_block_size: int | None = 512 * 1024 * 1024,
)
```

**Properties:**

| Property               | Type                | Description                                      |
| ---------------------- | ------------------- | ------------------------------------------------ |
| `schema`               | `str`               | Unified Avro schema as JSON string               |
| `schema_dict`          | `dict`              | Unified Avro schema as Python dictionary         |
| `rows_read`            | `int`               | Total rows read so far                           |
| `total_sources`        | `int`               | Number of source files                           |
| `current_source_index` | `int`               | Index of file currently being read (0-based)     |
| `is_finished`          | `bool`              | Whether iteration is complete                    |
| `error_count`          | `int`               | Number of errors (with `ignore_errors=True`)     |
| `errors`               | `list[BadBlockError]` | Error details (with `ignore_errors=True`)      |

---

### BadBlockError

Structured error information from skip mode reading. Returned in the `errors` list of `AvroReader` and `MultiAvroReader`.

**Properties:**

| Property       | Type           | Description                                    |
| -------------- | -------------- | ---------------------------------------------- |
| `kind`         | `str`          | Error type (e.g., "InvalidSyncMarker")         |
| `block_index`  | `int`          | Block number where error occurred (0-based)    |
| `record_index` | `int \| None`  | Record number within block, if applicable      |
| `file_offset`  | `int`          | Byte offset where error occurred               |
| `message`      | `str`          | Human-readable error description               |
| `filepath`     | `str \| None`  | Source file path, if known                     |

**Methods:**

- `to_dict()` → `dict`: Convert to dictionary for logging/serialization

---

## Exceptions

All Jetliner exceptions inherit from `JetlinerError`:

```python
import jetliner

try:
    df = jetliner.scan_avro("data.avro").collect()
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

### Structured Exception Types

For programmatic error handling, Jetliner provides structured exception types with metadata attributes:

#### PyDecodeError

Raised when a record cannot be decoded.

**Attributes:**

| Attribute      | Type  | Description                         |
| -------------- | ----- | ----------------------------------- |
| `block_index`  | `int` | Index of the block containing error |
| `record_index` | `int` | Index of the record within block    |
| `offset`       | `int` | Byte offset in the file             |
| `message`      | `str` | Error description                   |

```python
try:
    df = jetliner.read_avro("corrupted.avro")
except jetliner.PyDecodeError as e:
    print(f"Error at block {e.block_index}, record {e.record_index}")
    print(f"Offset: {e.offset}")
    print(f"Message: {e.message}")
```

#### PyParseError

Raised when the file format is invalid.

**Attributes:**

| Attribute | Type  | Description          |
| --------- | ----- | -------------------- |
| `offset`  | `int` | Byte offset of error |
| `message` | `str` | Error description    |

#### PySourceError

Raised for file or S3 access errors.

**Attributes:**

| Attribute | Type  | Description            |
| --------- | ----- | ---------------------- |
| `path`    | `str` | Path that caused error |
| `message` | `str` | Error description      |

#### PySchemaError

Raised when the schema is invalid.

**Attributes:**

| Attribute | Type  | Description       |
| --------- | ----- | ----------------- |
| `message` | `str` | Error description |

#### PyCodecError

Raised when decompression fails.

**Attributes:**

| Attribute | Type  | Description       |
| --------- | ----- | ----------------- |
| `message` | `str` | Error description |

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

| Key                     | Description                                   | Example                 |
| ----------------------- | --------------------------------------------- | ----------------------- |
| `endpoint`              | Custom S3 endpoint (MinIO, LocalStack, R2)    | `http://localhost:9000` |
| `aws_access_key_id`     | AWS access key                                | `AKIAIOSFODNN7EXAMPLE`  |
| `aws_secret_access_key` | AWS secret key                                | `wJalrXUtnFEMI/...`     |
| `region`                | AWS region                                    | `us-east-1`             |
| `max_retries`           | Maximum retry attempts for transient failures | `5`                     |

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
