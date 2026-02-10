# Design Document: Jetliner

## Overview

This document describes the architecture and design of Jetliner, a high-performance Rust library for streaming Avro data into Polars DataFrames. Named after the Avro Jetliner — the first jet airliner to fly in North America — the library emphasizes speed and streaming (lines/rows). It provides Python bindings via PyO3 and supports reading from both S3 and local filesystem.

### Key Design Decisions

1. **Direct Arrow Integration**: Deserialize Avro directly into Arrow arrays (Polars' underlying format), avoiding intermediate representations
2. **PyO3 + pyo3-polars**: Use PyO3 for Python bindings with pyo3-polars for zero-copy DataFrame transfer
3. **Async I/O with Tokio**: Use async runtime for S3 operations and prefetching
4. **Block-oriented Processing**: Process Avro blocks as the unit of work for streaming and parallelism
5. **Polars IO Plugin**: Use `register_io_source` for LazyFrame integration with query optimization

### Dual API Strategy

We provide two complementary APIs:

1. **`scan()` → LazyFrame via IO Plugin**: Integrates with Polars query optimizer for projection pushdown, predicate pushdown, early stopping, and streaming engine support. Recommended for most use cases.

2. **`open()` → Iterator**: Direct DataFrame iteration for streaming control, progress tracking, or custom batch processing. Useful when you need fine-grained control over memory or processing.

Both APIs share the same Rust core — the IO plugin just wraps the iterator in a generator that Polars can optimize.

## Architecture

**Note on Async**: The Rust core uses Tokio async internally for S3 I/O and prefetching, but the Python API is **synchronous only**. Python asyncio support (`__aiter__`/`__anext__`) is explicitly out of scope—the complexity of bridging Tokio and Python's asyncio event loops is not justified for this use case. The sync Python API blocks on the internal Tokio runtime.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Python API Layer                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │  scan()         │  │  open()         │  │  BatchIterator      │  │
│  │  → LazyFrame    │  │  → Iterator     │  │  (__iter__)         │  │
│  │  (IO Plugin)    │  │  (direct)       │  │                     │  │
│  └────────┬────────┘  └────────┬────────┘  └──────────┬──────────┘  │
│           │                    │                      │             │
│           │    ┌───────────────┴──────────────────────┘             │
│           │    │  (both use same Rust core)                         │
│           ▼    ▼                                                    │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  register_io_source (Polars query optimization)             │    │
│  │  - projection pushdown (with_columns)                       │    │
│  │  - predicate pushdown (predicate)                           │    │
│  │  - early stopping (n_rows)                                  │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PyO3 Binding Layer                              │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  pyo3-polars: PyDataFrame ←→ DataFrame (zero-copy)          │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Core Rust Library                              │
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐   │
│  │ StreamSource │    │  AvroParser  │    │  DataFrameBuilder    │   │
│  │  (S3/Local)  │───▶│  (blocks)    │───▶│  (Arrow columns)     │   │
│  └──────────────┘    └──────────────┘    └──────────────────────┘   │
│         │                   │                      │                 │
│         ▼                   ▼                      ▼                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐   │
│  │ PrefetchBuf  │    │    Codec     │    │  RecordDecoder       │   │
│  │  (async)     │    │ (decompress) │    │  (Full/Projected)    │   │
│  └──────────────┘    └──────────────┘    └──────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### StreamSource Trait

Abstracts over data sources with async range-request support.

```rust
#[async_trait]
pub trait StreamSource: Send + Sync {
    /// Read bytes from offset with length
    async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, SourceError>;

    /// Get total file size
    async fn size(&self) -> Result<u64, SourceError>;

    /// Read from offset to end
    async fn read_from(&self, offset: u64) -> Result<Bytes, SourceError>;
}

pub struct S3Source {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
}

/// Configuration options for S3 connections (passed via storage_options)
pub struct S3Config {
    pub endpoint_url: Option<String>,
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    pub region: Option<String>,
}

impl S3Source {
    /// Create S3Source with optional configuration overrides
    pub async fn new(
        bucket: String,
        key: String,
        config: Option<S3Config>,
    ) -> Result<Self, SourceError>;
}

pub struct LocalSource {
    file: tokio::fs::File,
    path: PathBuf,
}
```

### AvroHeader

Parsed file header containing schema and metadata.

```rust
pub struct AvroHeader {
    pub magic: [u8; 4],           // "Obj\x01"
    pub metadata: HashMap<String, Vec<u8>>,
    pub sync_marker: [u8; 16],
    pub schema: AvroSchema,
    pub codec: Codec,
    pub header_size: u64,         // Offset where blocks begin
}

impl AvroHeader {
    pub fn parse(bytes: &[u8]) -> Result<Self, ParseError>;
    pub fn schema_json(&self) -> String;
}
```

### AvroSchema

Represents the Avro schema with support for all types.

```rust
pub enum AvroSchema {
    // Primitives
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,

    // Complex
    Record(RecordSchema),
    Enum(EnumSchema),
    Array(Box<AvroSchema>),
    Map(Box<AvroSchema>),
    Union(Vec<AvroSchema>),
    Fixed(FixedSchema),

    // Named reference (resolved during parsing)
    Named(String),
}

pub struct RecordSchema {
    pub name: String,
    pub namespace: Option<String>,
    pub fields: Vec<FieldSchema>,
    pub doc: Option<String>,
}

pub struct FieldSchema {
    pub name: String,
    pub schema: AvroSchema,
    pub default: Option<Value>,
    pub doc: Option<String>,
}

// Logical types as wrappers
pub struct LogicalType {
    pub base: AvroSchema,
    pub logical_type: LogicalTypeName,
}

pub enum LogicalTypeName {
    Decimal { precision: u32, scale: u32 },
    Uuid,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    Duration,
}
```

### Codec

Compression codec abstraction.

```rust
pub enum Codec {
    Null,
    Snappy,
    Deflate,
    Zstd,
    Bzip2,
    Xz,
}

impl Codec {
    pub fn from_name(name: &str) -> Result<Self, CodecError>;

    pub fn decompress(&self, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>, CodecError>;
}
```

### AvroBlock

A single data block from the file.

```rust
pub struct AvroBlock {
    pub record_count: i64,
    pub compressed_size: i64,
    pub data: Bytes,              // Compressed block data
    pub sync_marker: [u8; 16],
    pub file_offset: u64,         // Position in file (for error reporting)
    pub block_index: usize,       // Sequential block number
}

pub struct DecompressedBlock {
    pub record_count: i64,
    pub data: Bytes,              // Decompressed data
    pub block_index: usize,
}
```

### ReadBufferConfig

Configuration for read buffering, with source-aware defaults optimized for different I/O characteristics.

**Design Rationale:**

Local disk and S3 have fundamentally different I/O characteristics:
- **Local disk**: Low latency (~0.1ms), OS page cache handles prefetching, small reads are cheap
- **S3**: High latency (~50-100ms), each request costs money, larger reads amortize overhead

We use source-aware defaults with user override capability for tuning in production.

```rust
/// Configuration for BlockReader's internal read buffer
#[derive(Debug, Clone)]
pub struct ReadBufferConfig {
    /// Size of each read chunk from the source
    /// - Local default: 64KB (OS page cache handles the rest)
    /// - S3 default: 4MB (amortize HTTP request overhead)
    pub chunk_size: usize,

    /// Threshold (0.0-1.0) at which to trigger a prefetch
    /// When buffer falls below this fraction of chunk_size, fetch more data
    /// - Local default: 0.0 (fetch only when empty - OS handles prefetch)
    /// - S3 default: 0.5 (fetch when 50% consumed - hide latency)
    pub prefetch_threshold: f32,
}

impl ReadBufferConfig {
    /// Default config for local filesystem
    pub const LOCAL_DEFAULT: Self = Self {
        chunk_size: 64 * 1024,      // 64KB
        prefetch_threshold: 0.0,     // Fetch only when empty
    };

    /// Default config for S3 (optimized for high-latency, pay-per-request)
    pub const S3_DEFAULT: Self = Self {
        chunk_size: 4 * 1024 * 1024, // 4MB (Polars cloud uses this)
        prefetch_threshold: 0.5,      // Fetch at 50% consumed
    };

    /// Create config with custom chunk size (threshold defaults to 0.0)
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        Self { chunk_size, prefetch_threshold: 0.0 }
    }

    /// Create config with both custom chunk size and prefetch threshold
    pub fn new(chunk_size: usize, prefetch_threshold: f32) -> Self {
        Self { chunk_size, prefetch_threshold: prefetch_threshold.clamp(0.0, 1.0) }
    }
}
```

**Industry Reference Points:**
- Spark: 64-128MB chunks for S3
- Polars cloud: 4MB chunks
- DuckDB: 1-10MB chunks
- Our default (4MB) balances memory usage with request reduction

### BlockReader

Reads and parses blocks from a source with internal read buffering to minimize I/O operations.

**Read Buffering Strategy:**

The BlockReader maintains an internal read buffer to avoid redundant I/O operations. When reading from the source, it fetches data in configurable chunks and retains unused bytes for subsequent block parsing. This is critical for S3 performance where each `read_range` call is an HTTP request with ~50-100ms latency and per-request costs.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        READ BUFFER LIFECYCLE                               │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  Initial State:     read_buffer = []                                       │
│                                                                            │
│  After 1st read:    read_buffer = [████████████████████████] (chunk_size)  │
│                                   ^block1^                                 │
│                                                                            │
│  After parse:       read_buffer = [        ████████████████] (retained)    │
│                                           ^block2^                         │
│                                                                            │
│  S3 eager refill:   When buffer < 50% of chunk_size, trigger async fetch   │
│                     read_buffer = [████████████████████████████████████]   │
│                                                                            │
│  Local lazy refill: When buffer empty, fetch next chunk                    │
│                     read_buffer = [████████████████████████]               │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

**Key Behaviors:**
- Retains unused bytes after parsing each block
- Only calls `source.read_range()` when buffer is exhausted or below prefetch threshold
- For blocks larger than the buffer, reads exactly the needed bytes
- Maintains accurate `current_offset` for error reporting
- Source-aware defaults: 64KB chunks for local, 4MB for S3

```rust
pub struct BlockReader<S: StreamSource> {
    source: S,
    header: AvroHeader,
    current_offset: u64,
    block_index: usize,
    file_size: u64,
    /// Internal read buffer - retains unused bytes between block reads
    read_buffer: Bytes,
    /// Offset in file where read_buffer starts
    buffer_file_offset: u64,
    /// Read buffer configuration
    buffer_config: ReadBufferConfig,
}

impl<S: StreamSource> BlockReader<S> {
    /// Create with default config (64KB chunks, no eager prefetch)
    pub async fn new(source: S) -> Result<Self, ReaderError>;

    /// Create with custom buffer configuration
    pub async fn with_config(source: S, config: ReadBufferConfig) -> Result<Self, ReaderError>;

    /// Read the next block, using buffered data when possible.
    ///
    /// This method:
    /// 1. Checks if read_buffer contains enough data for the next block
    /// 2. If buffer below prefetch_threshold, fetches more data from source
    /// 3. Parses the block from the buffer
    /// 4. Advances buffer position, retaining unused bytes
    ///
    /// # Requirements
    /// - 3.8: Retain unused bytes from previous I/O operations
    /// - 3.9: Parse multiple small blocks without additional I/O
    /// - 3.10: Minimize total I/O operations
    pub async fn next_block(&mut self) -> Result<Option<AvroBlock>, ReaderError>;

    pub fn header(&self) -> &AvroHeader;

    pub async fn seek_to_sync(&mut self, position: u64) -> Result<bool, ReaderError>;

    /// Skip past corrupted data and find the next valid sync marker.
    ///
    /// This method is specifically designed for error recovery in skip mode.
    /// Unlike `seek_to_sync`, which searches from a given position, this method:
    /// 1. Starts searching from current_offset + 1 (to skip past the current bad data)
    /// 2. Scans forward looking for the file's sync marker
    /// 3. Positions the reader immediately after the found sync marker
    ///
    /// Returns true if a sync marker was found and the reader is positioned
    /// to read the next block, false if no more sync markers exist (EOF).
    pub async fn skip_to_next_sync(&mut self) -> Result<bool, ReaderError>;

    /// Reset the reader to the beginning of the blocks.
    /// Also clears the read buffer.
    pub fn reset(&mut self);
}
```

### PrefetchBuffer

Async buffer that prefetches upcoming blocks.

```rust
pub struct PrefetchBuffer<S: StreamSource> {
    reader: BlockReader<S>,
    buffer: VecDeque<DecompressedBlock>,
    max_buffer_blocks: usize,
    max_buffer_bytes: usize,
    current_buffer_bytes: usize,
    fetch_task: Option<JoinHandle<Result<Option<DecompressedBlock>, ReaderError>>>,
    codec: Codec,
}

impl<S: StreamSource> PrefetchBuffer<S> {
    pub fn new(reader: BlockReader<S>, config: BufferConfig) -> Self;

    /// Get next decompressed block, triggering prefetch
    pub async fn next(&mut self) -> Result<Option<DecompressedBlock>, ReaderError>;

    /// Check buffer status
    pub fn buffered_blocks(&self) -> usize;
    pub fn buffered_bytes(&self) -> usize;
}

pub struct BufferConfig {
    pub max_blocks: usize,        // Default: 4
    pub max_bytes: usize,         // Default: 64MB
}
```

### RecordDecoder

Decodes Avro records from binary data directly into Arrow arrays.

```rust
pub struct RecordDecoder {
    schema: AvroSchema,
    arrow_schema: ArrowSchema,
    builders: Vec<Box<dyn ArrayBuilder>>,
}

impl RecordDecoder {
    pub fn new(schema: &AvroSchema) -> Result<Self, SchemaError>;

    /// Decode records from a block into the internal builders
    pub fn decode_block(&mut self, block: &DecompressedBlock) -> Result<usize, DecodeError>;

    /// Finish current batch and return Arrow arrays, resetting builders
    pub fn finish_batch(&mut self) -> Result<Vec<ArrayRef>, DecodeError>;

    /// Current record count in builders
    pub fn pending_records(&self) -> usize;
}
```

### DataFrameBuilder

Converts Arrow arrays to Polars DataFrame.

```rust
pub struct DataFrameBuilder {
    decoder: RecordDecoder,
    batch_size: usize,
    error_mode: ErrorMode,
    errors: Vec<ReadError>,
}

pub enum ErrorMode {
    Strict,           // Fail on first error
    Skip,             // Skip bad records/blocks, log errors
}

impl DataFrameBuilder {
    pub fn new(schema: &AvroSchema, config: BuilderConfig) -> Result<Self, SchemaError>;

    /// Add a block's records to the builder
    pub fn add_block(&mut self, block: DecompressedBlock) -> Result<(), BuilderError>;

    /// Build DataFrame if we have enough records (or force=true)
    pub fn build(&mut self, force: bool) -> Result<Option<DataFrame>, BuilderError>;

    /// Get accumulated errors
    pub fn errors(&self) -> &[ReadError];

    /// Clear errors
    pub fn clear_errors(&mut self);
}

pub struct BuilderConfig {
    pub batch_size: usize,        // Target rows per DataFrame (default: 100_000)
    pub error_mode: ErrorMode,
}
```

### AvroStreamReader (Main Entry Point)

Orchestrates the full pipeline.

```rust
pub struct AvroStreamReader<S: StreamSource> {
    buffer: PrefetchBuffer<S>,
    builder: DataFrameBuilder,
    config: ReaderConfig,
    finished: bool,
}

pub struct ReaderConfig {
    pub batch_size: usize,
    pub buffer_config: BufferConfig,
    pub read_buffer_config: ReadBufferConfig,  // BlockReader I/O buffering
    pub error_mode: ErrorMode,
    pub reader_schema: Option<AvroSchema>,  // For schema evolution
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            batch_size: 100_000,
            buffer_config: BufferConfig::default(),
            read_buffer_config: ReadBufferConfig::LOCAL_DEFAULT,
            error_mode: ErrorMode::Skip,
            reader_schema: None,
        }
    }
}

impl ReaderConfig {
    /// Create config optimized for S3 sources
    pub fn for_s3() -> Self {
        Self {
            read_buffer_config: ReadBufferConfig::S3_DEFAULT,
            ..Default::default()
        }
    }
}

impl<S: StreamSource> AvroStreamReader<S> {
    pub async fn open(source: S, config: ReaderConfig) -> Result<Self, ReaderError>;

    pub fn schema(&self) -> &AvroSchema;

    pub async fn next_batch(&mut self) -> Result<Option<DataFrame>, ReaderError>;

    pub fn errors(&self) -> &[ReadError];

    pub fn is_finished(&self) -> bool;
}
```

## Data Models

### Type Mapping: Avro → Arrow → Polars

| Avro Type | Arrow Type                    | Polars Type  |
| --------- | ----------------------------- | ------------ |
| null      | Null                          | Null         |
| boolean   | Boolean                       | Boolean      |
| int       | Int32                         | Int32        |
| long      | Int64                         | Int64        |
| float     | Float32                       | Float32      |
| double    | Float64                       | Float64      |
| bytes     | LargeBinary                   | Binary       |
| string    | LargeUtf8                     | Utf8         |
| record    | Struct                        | Struct       |
| enum      | Dictionary(Int32, Utf8)       | Categorical  |
| array     | LargeList                     | List         |
| map       | LargeList(Struct{key, value}) | List(Struct) |
| union     | Union or nullable base        | varies       |
| fixed     | FixedSizeBinary               | Binary       |

### Logical Type Mapping

| Avro Logical Type | Arrow Type                  | Polars Type |
| ----------------- | --------------------------- | ----------- |
| decimal           | Decimal128                  | Decimal     |
| uuid              | FixedSizeBinary(16) or Utf8 | Utf8        |
| date              | Date32                      | Date        |
| time-millis       | Time32(Millisecond)         | Time        |
| time-micros       | Time64(Microsecond)         | Time        |
| timestamp-millis  | Timestamp(Millisecond, UTC) | Datetime    |
| timestamp-micros  | Timestamp(Microsecond, UTC) | Datetime    |
| duration          | FixedSizeBinary(12)         | Duration    |

### Union Handling

Avro unions are complex. Our strategy:

1. **`["null", T]` unions**: Map to nullable Arrow type T
2. **`[T, "null"]` unions**: Same as above (null position doesn't matter)
3. **Multi-type unions**: Map to Arrow Union type (dense union)

```rust
fn union_to_arrow(variants: &[AvroSchema]) -> ArrowDataType {
    // Check for nullable pattern
    let non_null: Vec<_> = variants.iter()
        .filter(|s| !matches!(s, AvroSchema::Null))
        .collect();

    if non_null.len() == 1 && variants.len() == 2 {
        // Simple nullable type
        return avro_to_arrow(non_null[0]); // Arrow handles nullability separately
    }

    // Complex union - use Arrow Union
    let fields: Vec<_> = variants.iter()
        .enumerate()
        .map(|(i, s)| Field::new(format!("_{}", i), avro_to_arrow(s), true))
        .collect();

    ArrowDataType::Union(fields, UnionMode::Dense)
}
```

### Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("Source error: {0}")]
    Source(#[from] SourceError),

    #[error("Parse error at offset {offset}: {message}")]
    Parse { offset: u64, message: String },

    #[error("Schema error: {0}")]
    Schema(#[from] SchemaError),

    #[error("Decode error in block {block_index}, record {record_index}: {message}")]
    Decode { block_index: usize, record_index: usize, message: String },

    #[error("Codec error: {0}")]
    Codec(#[from] CodecError),
}

#[derive(Debug)]
pub struct ReadError {
    pub kind: ReadErrorKind,
    pub block_index: usize,
    pub record_index: Option<usize>,
    pub offset: u64,
    pub message: String,
}

pub enum ReadErrorKind {
    InvalidSyncMarker,
    DecompressionFailed,
    RecordDecodeFailed,
    SchemaViolation,
}
```


## Python API Design

### Two API Patterns

The library provides two complementary APIs:

1. **`scan()` - LazyFrame with Query Optimization** (recommended for most use cases)
   - Returns a Polars `LazyFrame` via `register_io_source`
   - Enables projection pushdown (only read needed columns)
   - Enables predicate pushdown (filter at source level)
   - Enables early stopping (`head()`, `limit()`)
   - Integrates with Polars streaming engine

2. **`open()` - Iterator for Streaming Control**
   - Returns an iterator yielding `DataFrame` batches
   - Full control over batch processing
   - Useful for custom streaming pipelines, progress tracking, or memory-constrained environments

### Module Structure

```python
import jetliner
import polars as pl

# ============================================================
# RECOMMENDED: scan() with LazyFrame and query optimization
# ============================================================

# Basic scan - returns LazyFrame
lf = jetliner.scan("s3://bucket/file.avro")
lf = jetliner.scan("/path/to/file.avro")

# Query with projection pushdown - only reads col1, col2 from disk
result = (
    jetliner.scan("file.avro")
    .select(["col1", "col2"])
    .collect()
)

# Query with predicate pushdown - filters during read, not after
result = (
    jetliner.scan("file.avro")
    .filter(pl.col("status") == "active")
    .filter(pl.col("amount") > 100)
    .collect()
)

# Early stopping - stops reading after 1000 rows
result = jetliner.scan("file.avro").head(1000).collect()

# Full query optimization example
result = (
    jetliner.scan("s3://bucket/large_file.avro")
    .select(["user_id", "amount", "timestamp"])
    .filter(pl.col("amount") > 0)
    .group_by("user_id")
    .agg(pl.col("amount").sum())
    .head(100)
    .collect()
)

# ============================================================
# ALTERNATIVE: open() for streaming/batch control
# ============================================================

# Iterator-based reading
reader = jetliner.AvroReader("s3://bucket/file.avro")
reader = jetliner.AvroReader("/path/to/file.avro")
reader = jetliner.AvroReader(
    "s3://bucket/file.avro",
    batch_size=100_000,
    buffer_blocks=4,
    buffer_bytes=64 * 1024 * 1024,
    ignore_errors=True,
)

# S3-compatible services (MinIO, LocalStack, R2, etc.)
reader = jetliner.AvroReader(
    "s3://bucket/file.avro",
    storage_options={
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
    },
)

# Sync iteration (async iteration is NOT supported)
for df in reader:
    process(df)

# Context manager
with jetliner.AvroReader("file.avro") as reader:
    for df in reader:
        process(df)

# Schema inspection
reader = jetliner.AvroReader("file.avro")
print(reader.schema)  # JSON string
print(reader.schema_dict)  # Python dict

# Error inspection (after iteration in skip mode)
with jetliner.AvroReader("file.avro", ignore_errors=True) as reader:
    for df in reader:
        process(df)

    # Quick check
    if reader.error_count > 0:
        print(f"Skipped {reader.error_count} errors")

    # Detailed inspection
    for err in reader.errors:
        print(f"[{err.kind}] Block {err.block_index}: {err.message}")
        # Or as dict: err.to_dict()
```

### PyO3 Implementation

```rust
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

#[pyclass]
pub struct AvroReader {
    inner: Option<AvroStreamReader<BoxedSource>>,
    runtime: tokio::runtime::Runtime,
    errors: Vec<ReadError>,
}

#[pymethods]
impl AvroReader {
    #[new]
    #[pyo3(signature = (path, batch_size=100_000, buffer_blocks=4, buffer_bytes=67_108_864, strict=false))]
    fn new(
        path: &str,
        batch_size: usize,
        buffer_blocks: usize,
        buffer_bytes: usize,
        strict: bool,
    ) -> PyResult<Self> {
        let runtime = tokio::runtime::Runtime::new()?;
        let source = runtime.block_on(create_source(path))?;
        let config = ReaderConfig {
            batch_size,
            buffer_config: BufferConfig {
                max_blocks: buffer_blocks,
                max_bytes: buffer_bytes,
            },
            error_mode: if strict { ErrorMode::Strict } else { ErrorMode::Skip },
            reader_schema: None,
        };
        let reader = runtime.block_on(AvroStreamReader::open(source, config))?;
        Ok(Self {
            inner: Some(reader),
            runtime,
            errors: Vec::new(),
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyDataFrame>> {
        let reader = slf.inner.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyStopIteration, _>(()))?;

        match slf.runtime.block_on(reader.next_batch()) {
            Ok(Some(df)) => Ok(Some(PyDataFrame(df))),
            Ok(None) => {
                slf.errors = reader.errors().to_vec();
                Err(PyErr::new::<pyo3::exceptions::PyStopIteration, _>(()))
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
        }
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<&PyAny>,
        _exc_val: Option<&PyAny>,
        _exc_tb: Option<&PyAny>,
    ) -> bool {
        self.inner = None;
        false
    }

    #[getter]
    fn schema(&self) -> PyResult<String> {
        self.inner.as_ref()
            .map(|r| r.schema().to_json())
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader closed"))
    }

    #[getter]
    fn errors(&self) -> Vec<PyReadError> {
        self.errors.iter().map(PyReadError::from).collect()
    }

    #[getter]
    fn error_count(&self) -> usize {
        self.errors.len()
    }
}

#[pyclass]
pub struct PyReadError {
    #[pyo3(get)]
    kind: String,           // "InvalidSyncMarker", "DecompressionFailed", etc.
    #[pyo3(get)]
    block_index: usize,
    #[pyo3(get)]
    record_index: Option<usize>,
    #[pyo3(get)]
    offset: u64,            // File offset where error occurred
    #[pyo3(get)]
    message: String,
}

#[pymethods]
impl PyReadError {
    fn to_dict(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("kind", &self.kind)?;
            dict.set_item("block_index", self.block_index)?;
            dict.set_item("record_index", self.record_index)?;
            dict.set_item("offset", self.offset)?;
            dict.set_item("message", &self.message)?;
            Ok(dict.into())
        })
    }

    fn __repr__(&self) -> String {
        match self.record_index {
            Some(rec) => format!(
                "ReadError(kind='{}', block={}, record={}, offset={}, message='{}')",
                self.kind, self.block_index, rec, self.offset, self.message
            ),
            None => format!(
                "ReadError(kind='{}', block={}, offset={}, message='{}')",
                self.kind, self.block_index, self.offset, self.message
            ),
        }
    }
}
```

### Error Exposure Design

The Python API exposes errors accumulated during skip-mode reading without complicating the normal iteration flow:

**Design Principles:**
1. **Keep happy path simple** - Normal iteration just yields DataFrames
2. **Errors accumulate silently** - No callbacks or mid-stream exceptions for skipped errors
3. **Inspect after reading** - Check `error_count` or iterate `errors` after completion
4. **Structured data** - Each error has typed fields for programmatic access

**Usage Pattern:**
```python
# Simple case - just check if errors occurred
with jetliner.AvroReader("file.avro", ignore_errors=True) as reader:
    for df in reader:
        process(df)

    if reader.error_count > 0:
        print(f"Warning: {reader.error_count} errors during read")

# Detailed inspection
with jetliner.AvroReader("file.avro", ignore_errors=True) as reader:
    for df in reader:
        process(df)

    for err in reader.errors:
        # Structured access
        print(f"[{err.kind}] Block {err.block_index} at offset {err.offset}")
        print(f"  {err.message}")

        # Or as dict for logging/serialization
        log_error(err.to_dict())
```

**Error Kinds:**
- `InvalidSyncMarker` - Block sync marker doesn't match file header
- `DecompressionFailed` - Codec failed to decompress block data
- `BlockParseFailed` - Block header parsing failed (truncated, invalid varints)
- `RecordDecodeFailed` - Record data doesn't match schema
- `SchemaViolation` - Data violates schema constraints
```

### IO Plugin Implementation (scan API)

The `scan()` function uses Polars' `register_io_source` to enable query optimizations. The implementation bridges Rust I/O with Python's generator protocol.

**Why build this instead of using existing solutions?**
- Polars has `read_avro` but **no `scan_avro`** — no lazy/streaming Avro support
- Existing `polars_io::avro::AvroReader` requires `Read + Seek` — can't stream from S3
- `polars-fastavro` uses Python/fastavro intermediary — 30-80x slower, no S3 support
- Our library: Native Rust decoding + S3 streaming + IO plugin = unique value

```python
# Python-side implementation in jetliner/__init__.py
import polars as pl
from polars.io.plugins import register_io_source
from typing import Iterator
from .jetliner import AvroReader, read_avro_schema

def scan_avro(
    path: str,
    *,
    buffer_blocks: int = 4,
    buffer_bytes: int = 64 * 1024 * 1024,
    read_chunk_size: int | None = None,
    ignore_errors: bool = False,
    storage_options: dict[str, str] | None = None,
) -> pl.LazyFrame:
    """
    Scan an Avro file, returning a LazyFrame with query optimization support.

    Supports projection pushdown, predicate pushdown, and early stopping.

    Parameters
    ----------
    path : str
        Path to Avro file (local path or s3:// URI)
    buffer_blocks : int
        Number of blocks to prefetch (default: 4)
    buffer_bytes : int
        Maximum bytes to buffer (default: 64MB)
    read_chunk_size : int | None
        Size of each I/O read operation in bytes. If None, uses source-aware defaults.
    ignore_errors : bool
        If True, skip bad records. If False, fail on first error (default: False)
    storage_options : dict[str, str] | None
        Configuration for S3 connections (endpoint, credentials, region)
    """
    # Parse schema to get Polars schema (calls into Rust)
    polars_schema = read_avro_schema(path)

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        """Generator that yields DataFrames, respecting pushdown hints."""

        if batch_size is None:
            batch_size = 100_000

        # Create Rust reader with projection info
        reader = AvroReader(
            path,
            batch_size=batch_size,
            buffer_blocks=buffer_blocks,
            buffer_bytes=buffer_bytes,
            read_chunk_size=read_chunk_size,
            ignore_errors=ignore_errors,
            projected_columns=with_columns,
            storage_options=storage_options,
        )

        rows_yielded = 0

        for df in reader:
            # Apply predicate pushdown (filter at Python level for now)
            if predicate is not None:
                df = df.filter(predicate)

            # Handle early stopping
            if n_rows is not None:
                remaining = n_rows - rows_yielded
                if remaining <= 0:
                    break
                if df.height > remaining:
                    df = df.head(remaining)

            rows_yielded += df.height
            yield df

            if n_rows is not None and rows_yielded >= n_rows:
                break

    return register_io_source(
        io_source=source_generator,
        schema=polars_schema,
    )

```

### Projection Pushdown Implementation

Projection pushdown uses **two separate decoder types** to avoid runtime overhead when projection isn't needed. Rust's monomorphization generates optimized code for each path.

```rust
/// Trait for record decoding with static dispatch
pub trait RecordDecode: Send {
    fn decode_record(&mut self, data: &mut &[u8]) -> Result<(), DecodeError>;
    fn finish_batch(&mut self) -> Result<Vec<ArrayRef>, DecodeError>;
    fn pending_records(&self) -> usize;
}

/// Full decoder - no projection, no overhead
pub struct FullRecordDecoder {
    schema: AvroSchema,
    arrow_schema: ArrowSchema,
    builders: Vec<Box<dyn ArrayBuilder>>,
}

impl FullRecordDecoder {
    pub fn new(schema: &AvroSchema) -> Result<Self, SchemaError> {
        let arrow_schema = avro_to_arrow_schema(schema);
        let builders = schema.fields()
            .iter()
            .map(|field| create_builder_for_type(&field.schema))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { schema, arrow_schema, builders })
    }
}

impl RecordDecode for FullRecordDecoder {
    fn decode_record(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        // Decode all fields directly - no projection checks
        for (field, builder) in self.schema.fields().iter().zip(&mut self.builders) {
            decode_field(data, &field.schema, builder)?;
        }
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<Vec<ArrayRef>, DecodeError> {
        self.builders.iter_mut()
            .map(|b| Ok(b.finish()))
            .collect()
    }

    fn pending_records(&self) -> usize {
        self.builders.first().map_or(0, |b| b.len())
    }
}

/// Projected decoder - only builds selected columns
pub struct ProjectedRecordDecoder {
    schema: AvroSchema,
    arrow_schema: ArrowSchema,
    builders: Vec<Option<Box<dyn ArrayBuilder>>>,  // None for skipped columns
    projected_names: HashSet<String>,
}

impl ProjectedRecordDecoder {
    pub fn new(schema: &AvroSchema, columns: &[String]) -> Result<Self, SchemaError> {
        let projected_names: HashSet<_> = columns.iter().cloned().collect();
        let arrow_schema = avro_to_arrow_schema_projected(schema, &projected_names);

        let builders = schema.fields()
            .iter()
            .map(|field| {
                if projected_names.contains(&field.name) {
                    Some(create_builder_for_type(&field.schema))
                } else {
                    None
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { schema, arrow_schema, builders, projected_names })
    }
}

impl RecordDecode for ProjectedRecordDecoder {
    fn decode_record(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        for (field, builder_opt) in self.schema.fields().iter().zip(&mut self.builders) {
            match builder_opt {
                Some(builder) => decode_field(data, &field.schema, builder)?,
                None => skip_field(data, &field.schema)?,
            }
        }
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<Vec<ArrayRef>, DecodeError> {
        self.builders.iter_mut()
            .filter_map(|b| b.as_mut().map(|builder| Ok(builder.finish())))
            .collect()
    }

    fn pending_records(&self) -> usize {
        self.builders.iter()
            .find_map(|b| b.as_ref().map(|builder| builder.len()))
            .unwrap_or(0)
    }
}

/// Factory enum - returns appropriate decoder type
pub enum RecordDecoder {
    Full(FullRecordDecoder),
    Projected(ProjectedRecordDecoder),
}

impl RecordDecoder {
    pub fn new(
        schema: &AvroSchema,
        projected_columns: Option<&[String]>,
    ) -> Result<Self, SchemaError> {
        match projected_columns {
            None => Ok(RecordDecoder::Full(FullRecordDecoder::new(schema)?)),
            Some(cols) => Ok(RecordDecoder::Projected(ProjectedRecordDecoder::new(schema, cols)?)),
        }
    }
}

impl RecordDecode for RecordDecoder {
    fn decode_record(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        match self {
            RecordDecoder::Full(d) => d.decode_record(data),
            RecordDecoder::Projected(d) => d.decode_record(data),
        }
    }

    fn finish_batch(&mut self) -> Result<Vec<ArrayRef>, DecodeError> {
        match self {
            RecordDecoder::Full(d) => d.finish_batch(),
            RecordDecoder::Projected(d) => d.finish_batch(),
        }
    }

    fn pending_records(&self) -> usize {
        match self {
            RecordDecoder::Full(d) => d.pending_records(),
            RecordDecoder::Projected(d) => d.pending_records(),
        }
    }
}
```

**Why two types instead of runtime checks?**
- `FullRecordDecoder`: Zero overhead — no Option checks, no HashSet lookups, tight decode loop
- `ProjectedRecordDecoder`: Pays the cost only when projection is actually used
- The enum dispatch (`match self`) happens once per record, which is negligible
- Rust's optimizer can inline the trait methods, making the enum match nearly free

**Skip function for non-projected fields:**

```rust
/// Skip over an Avro field without decoding its value
fn skip_field(data: &mut &[u8], schema: &AvroSchema) -> Result<(), DecodeError> {
    match schema {
        AvroSchema::Null => {}
        AvroSchema::Boolean => { *data = &data[1..]; }
        AvroSchema::Int | AvroSchema::Long => { skip_varint(data)?; }
        AvroSchema::Float => { *data = &data[4..]; }
        AvroSchema::Double => { *data = &data[8..]; }
        AvroSchema::Bytes | AvroSchema::String => {
            let len = decode_varint(data)? as usize;
            *data = &data[len..];
        }
        AvroSchema::Fixed(f) => { *data = &data[f.size..]; }
        AvroSchema::Array(inner) => { skip_array(data, inner)?; }
        AvroSchema::Map(inner) => { skip_map(data, inner)?; }
        AvroSchema::Union(variants) => {
            let idx = decode_varint(data)? as usize;
            skip_field(data, &variants[idx])?;
        }
        AvroSchema::Record(r) => {
            for field in &r.fields {
                skip_field(data, &field.schema)?;
            }
        }
        AvroSchema::Enum(_) => { skip_varint(data)?; }
    }
    Ok(())
}
```

### Predicate Pushdown Strategy

Predicate pushdown is handled at the Python level initially (filter after building DataFrame). This is the approach shown in Polars' own IO plugin example.

**Why not filter during decode?**
- Polars expressions (`pl.Expr`) are complex to evaluate in Rust
- Would require reimplementing expression evaluation
- Filtering post-build is still efficient for most cases
- Polars' own example does it this way

**Future optimization path**: For simple predicates on primitive columns (e.g., `col("x") > 5`), we could:
1. Parse the predicate expression in Python
2. Pass simple filter conditions to Rust
3. Skip records that fail the filter during decode

This is out of scope for initial implementation.

### Future Optimization Opportunities (Deferred)

These optimizations are identified but deferred to post-MVP:

1. **Decode-level field skipping for fixed-width types**: For schemas with many large fixed-width columns that aren't projected, we could compute byte offsets at schema parse time and seek directly past them without parsing.

2. **Block-level predicate evaluation**: Unlike Parquet, Avro doesn't have block-level statistics (min/max). However, for sorted data or with external metadata, we could skip entire blocks.

3. **Parallel record decoding within blocks**: Currently we decode records sequentially within a block. For very wide schemas, parallel field decoding could help.

4. **SIMD-accelerated varint decoding**: Avro uses varint encoding extensively. SIMD could accelerate this for large batches.

5. **Memory-mapped local files**: For local files, mmap could reduce copies vs read() calls.

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*


### Property 1: Schema Round-Trip

*For any* valid Avro schema, parsing the schema from JSON, printing it back to JSON, and parsing again SHALL produce an equivalent schema object.

**Validates: Requirements 1.9**

### Property 2: Data Round-Trip with Type Preservation

*For any* valid Avro record conforming to a schema, serializing to Avro binary format, deserializing into a Polars DataFrame, and comparing values SHALL preserve all data values and map to appropriate Polars types.

**Validates: Requirements 5.4, 5.5, 5.6, 5.7, 5.8, 5.9**

### Property 3: All Avro Types Deserialize Correctly

*For any* Avro value of any supported type (primitives: null, boolean, int, long, float, double, bytes, string; complex: records, enums, arrays, maps, unions, fixed; logical: decimal, uuid, date, time-millis, time-micros, timestamp-millis, timestamp-micros, duration), deserializing from valid Avro binary SHALL produce the correct value.

**Validates: Requirements 1.4, 1.5, 1.6**

### Property 4: All Codecs Decompress Correctly

*For any* Avro block compressed with any supported codec (null, snappy, deflate, zstd, bzip2, xz), decompressing and deserializing SHALL produce the original records.

**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6**

### Property 5: Sync Marker Validation

*For any* Avro file, all blocks' sync markers SHALL match the sync marker declared in the file header. When a mismatched sync marker is encountered, the reader SHALL report an error.

**Validates: Requirements 1.3**

### Property 6: Named Type Resolution

*For any* Avro schema containing named type references (records referencing other records, arrays of named types, etc.), the parser SHALL correctly resolve all references to their definitions.

**Validates: Requirements 1.7**

### Property 7: Batch Size Limit Respected

*For any* configured batch_size and any Avro file, each yielded DataFrame SHALL contain at most batch_size rows (except possibly the final batch which may contain fewer).

**Validates: Requirements 3.4**

### Property 8: Range Requests Return Correct Data

*For any* file and any valid byte range [offset, offset+length), reading that range SHALL return exactly the bytes at those positions in the file.

**Validates: Requirements 4.4**

### Property 9: Null Preservation in Unions

*For any* Avro record containing nullable fields (union with null), null values SHALL be preserved as null in the resulting DataFrame, and non-null values SHALL be preserved with their correct values.

**Validates: Requirements 5.5**

### Property 10: Resilient Reading Skips Bad Data

*For any* Avro file containing some valid blocks/records and some corrupted blocks/records, when in non-strict mode, the reader SHALL successfully read all valid data and track errors for corrupted data.

**Validates: Requirements 7.1, 7.2, 7.3**

### Property 11: Strict Mode Fails on First Error

*For any* Avro file containing corrupted data, when in strict mode, the reader SHALL fail immediately upon encountering the first error.

**Validates: Requirements 7.5**

### Property 12: Schema Resolution with Reader Schema

*For any* Avro file written with a writer schema and read with a compatible reader schema, schema resolution SHALL correctly handle: field defaults, field reordering, type promotions (int→long, float→double), and missing optional fields.

**Validates: Requirements 9.2**

### Property 13: Seek to Sync Marker

*For any* Avro file and any valid sync marker position within that file, seeking to that sync marker and reading SHALL produce the same records as reading sequentially from that block.

**Validates: Requirements 3.7**

### Property 14: Projection Preserves Selected Columns

*For any* Avro file and any subset of columns, reading with projection SHALL produce a DataFrame containing exactly those columns with the same values as reading all columns and then selecting.

**Validates: Requirements 6a.2**

### Property 15: Early Stopping Respects Row Limit

*For any* Avro file and any row limit N, reading with `n_rows=N` SHALL produce at most N rows, and those rows SHALL be the first N rows of the file.

**Validates: Requirements 6a.4**

### Property 16: BlockReader I/O Efficiency

*For any* sequence of Avro blocks totaling B bytes, the BlockReader SHALL issue at most `ceil(B / chunk_size)` I/O operations to read all blocks, where `chunk_size` is the configured read chunk size.

**Validates: Requirements 3.10**

## Memory Management and Backpressure

### Lessons from Existing Implementations

**polars-fastavro**: Uses Python/fastavro as intermediary. Result: 30-80x slower than native. Key insight: avoid Python intermediaries for hot paths.

**polars_io::avro**: Uses `apache-avro` crate which allocates a `Value` enum for every field. Result: ~7x slower than direct Arrow decoding. Key insight: decode directly into Arrow builders.

**Native Polars Avro** (deprecated): Was faster because it decoded directly into Arrow buffers. We follow this approach with streaming support.

### Memory Lifecycle Through the Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MEMORY BUFFER LOCATIONS                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  [1] Network/Disk Buffer     [2] Prefetch Buffer    [3] Arrow Builders      │
│  ┌─────────────────────┐     ┌─────────────────┐    ┌─────────────────┐     │
│  │ S3 range response   │     │ Decompressed    │    │ Column data     │     │
│  │ or mmap'd file      │────▶│ block bytes     │───▶│ being built     │     │
│  │                     │     │ (VecDeque)      │    │                 │     │
│  │ ~64KB-1MB per req   │     │ max N blocks    │    │ grows until     │     │
│  │                     │     │ or M bytes      │    │ batch_size      │     │
│  └─────────────────────┘     └─────────────────┘    └─────────────────┘     │
│           │                          │                      │               │
│           │ dropped after            │ dropped after        ▼               │
│           │ decompression            │ decode         [4] DataFrame         │
│           ▼                          ▼               ┌─────────────────┐    │
│      (freed)                    (freed)              │ Returned to     │    │
│                                                      │ Python, then    │    │
│                                                      │ freed when      │    │
│                                                      │ Python releases │    │
│                                                      └─────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Memory Bounds**:
- **[1] Network buffer**: Transient, ~1MB max per request
- **[2] Prefetch buffer**: Bounded by `buffer_blocks` (default 4) and `buffer_bytes` (default 64MB)
- **[3] Arrow builders**: Bounded by `batch_size` rows × schema width
- **[4] DataFrame**: Owned by Python after yield; Rust releases reference

**Backpressure**: When the prefetch buffer is full, the async fetch task pauses until the consumer drains blocks. This prevents unbounded memory growth regardless of network speed.

## Error Handling

### Error Categories

1. **Fatal Errors** (always fail, cannot recover):
   - Invalid magic bytes
   - I/O errors (network failure, file not found)
   - Authentication failures
   - Schema parse failures
   - Unknown codec

2. **Recoverable Errors** (skip in non-strict mode):
   - Sync marker mismatch (skip to next valid sync)
   - Block decompression failure (skip block)
   - Record decode failure (skip record)
   - Schema violation in data (skip record)

### Sync Marker Recovery

When a block has an invalid sync marker in skip mode, the reader must recover by finding the next valid sync marker. This is handled by `BlockReader::skip_to_next_sync()`:

```rust
// In PrefetchBuffer::next() when InvalidSyncMarker error occurs:
Err(ReaderError::InvalidSyncMarker { .. }) => {
    match self.error_mode {
        ErrorMode::Strict => return Err(e),
        ErrorMode::Skip => {
            // Log the error with sufficient detail for diagnosis (Req 7.7)
            // Include: block index, file offset, expected vs actual sync marker
            self.errors.push(ReadError::new(
                ReadErrorKind::InvalidSyncMarker,
                self.reader.block_index(),
                None,
                self.reader.current_offset(),
                format!(
                    "Invalid sync marker at offset {}: expected {:02x?}, got {:02x?}",
                    offset, expected_sync, actual_sync
                ),
            ));

            // Skip to the next valid sync marker
            // This scans forward from current position to find the file's sync marker
            let (found, bytes_skipped) = self.reader.skip_to_next_sync().await?;

            if !found {
                // No more sync markers found - we're done
                self.finished = true;
                return Ok(None);
            }

            // Log recovery information
            log::warn!(
                "Skipped {} bytes to recover at block {}",
                bytes_skipped, self.reader.block_index()
            );

            // Continue to try reading the next block
            continue;
        }
    }
}
```

The key insight is that `skip_to_next_sync()` must search FORWARD from the current position, not from the position where the bad sync marker was found. This is because:
1. When we detect an invalid sync marker, we've already read past the block data
2. The bytes we read as "sync marker" don't match, so we're at an unknown position
3. We need to scan forward to find the next occurrence of the correct sync marker

**Logging Requirements (Req 7.1, 7.2, 7.7)**:
- Block index where error occurred
- File offset where error was detected
- Expected vs actual sync marker bytes (for diagnosis)
- Number of bytes skipped during recovery
- Clear indication of whether recovery succeeded

### Error Propagation

```rust
// In non-strict mode
match decode_record(&data) {
    Ok(record) => builder.append(record),
    Err(e) => {
        errors.push(ReadError {
            kind: ReadErrorKind::RecordDecodeFailed,
            block_index,
            record_index: Some(record_index),
            offset: current_offset,
            message: e.to_string(),
        });
        // Continue to next record
    }
}

// In strict mode
let record = decode_record(&data)?; // Propagate error immediately
```

### Python Exception Mapping

| Rust Error                        | Python Exception             |
| --------------------------------- | ---------------------------- |
| SourceError::NotFound             | FileNotFoundError            |
| SourceError::PermissionDenied     | PermissionError              |
| SourceError::AuthenticationFailed | jetliner.AuthenticationError |
| ReaderError::Parse                | jetliner.ParseError          |
| ReaderError::Schema               | jetliner.SchemaError         |
| ReaderError::Codec                | jetliner.CodecError          |
| ReaderError::Decode               | jetliner.DecodeError         |

## Testing Strategy

### Unit Tests

Unit tests verify specific examples and edge cases:

- Magic byte validation (valid and invalid)
- Each codec with known test vectors
- Each Avro type with boundary values
- Error messages contain expected context
- Python API conformance (iterator protocol, context manager)

### Property-Based Tests

Property-based tests verify universal properties using the `proptest` crate in Rust:

```rust
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Property 1: Schema round-trip
    #[test]
    fn schema_round_trip(schema in arb_avro_schema()) {
        let json = schema.to_json();
        let parsed = AvroSchema::parse(&json).unwrap();
        let json2 = parsed.to_json();
        let parsed2 = AvroSchema::parse(&json2).unwrap();
        prop_assert_eq!(parsed, parsed2);
    }

    // Property 3: All types deserialize
    #[test]
    fn all_types_deserialize(value in arb_avro_value()) {
        let schema = value.schema();
        let bytes = serialize(&value, &schema);
        let decoded = deserialize(&bytes, &schema).unwrap();
        prop_assert_eq!(value, decoded);
    }
}
```

### Test Data Generators

Custom generators for Avro structures:

```rust
fn arb_avro_schema() -> impl Strategy<Value = AvroSchema> {
    prop_oneof![
        Just(AvroSchema::Null),
        Just(AvroSchema::Boolean),
        Just(AvroSchema::Int),
        Just(AvroSchema::Long),
        Just(AvroSchema::Float),
        Just(AvroSchema::Double),
        Just(AvroSchema::Bytes),
        Just(AvroSchema::String),
        arb_record_schema().prop_map(AvroSchema::Record),
        arb_enum_schema().prop_map(AvroSchema::Enum),
        arb_avro_schema().prop_map(|s| AvroSchema::Array(Box::new(s))),
        // ... etc
    ]
}

fn arb_avro_value_for_schema(schema: &AvroSchema) -> impl Strategy<Value = AvroValue> {
    match schema {
        AvroSchema::Int => any::<i32>().prop_map(AvroValue::Int).boxed(),
        AvroSchema::Long => any::<i64>().prop_map(AvroValue::Long).boxed(),
        AvroSchema::String => any::<String>().prop_map(AvroValue::String).boxed(),
        // ... etc
    }
}
```

### Integration Tests

- Read Apache Avro's official interoperability test files
    - See Appendix: A_avro_java_test_research.md and B_e2e-test-plan.md
- Round-trip tests with real S3 (using localstack or minio)
- Large file tests (verify memory stays bounded)
- Concurrent access tests

### Performance Benchmarks

Using `criterion` crate:

```rust
fn benchmark_read_throughput(c: &mut Criterion) {
    let file = generate_test_file(1_000_000); // 1M records

    c.bench_function("read_1m_records", |b| {
        b.iter(|| {
            let reader = AvroStreamReader::open(&file, default_config()).unwrap();
            let mut count = 0;
            while let Some(df) = reader.next_batch().unwrap() {
                count += df.height();
            }
            count
        })
    });
}
```

### Test Configuration

- Property tests: minimum 100 iterations per property
- Each property test tagged with: `Feature: jetliner, Property N: {description}`
- CI runs full test suite including property tests
- Benchmarks run separately, results tracked over time

## Dependencies

### Rust Crates

```toml
[dependencies]
# Core
tokio = { version = "1", features = ["rt-multi-thread", "fs", "io-util"] }
bytes = "1"
thiserror = "1"

# Avro parsing (we may implement our own for performance)
# apache-avro = "0.16"  # Reference, but likely custom implementation

# Arrow/Polars
arrow = { version = "52", features = ["ffi"] }
polars = { version = "0.39", features = ["dtype-struct", "dtype-categorical"] }

# Compression
snap = "1"           # Snappy
flate2 = "1"         # Deflate
zstd = "0.13"
bzip2 = "0.4"
xz2 = "0.1"

# S3
aws-sdk-s3 = "1"
aws-config = "1"

# Python bindings
pyo3 = { version = "0.20", features = ["extension-module"] }
pyo3-polars = "0.12"

# Testing
proptest = "1"
criterion = "0.5"
```

### Python Dependencies

```toml
[project]
requires-python = ">=3.11"
dependencies = [
    "polars>=0.20",
]

[build-system]
requires = ["maturin>=1.4"]
build-backend = "maturin"
```

## Project Structure

```
jetliner/
├── Cargo.toml
├── pyproject.toml
├── src/
│   ├── lib.rs              # Library root, PyO3 module definition
│   ├── schema/
│   │   ├── mod.rs
│   │   ├── parser.rs       # JSON schema parsing
│   │   ├── types.rs        # AvroSchema enum
│   │   └── resolution.rs   # Schema resolution logic
│   ├── codec/
│   │   ├── mod.rs
│   │   └── decompress.rs   # Codec implementations
│   ├── source/
│   │   ├── mod.rs
│   │   ├── traits.rs       # StreamSource trait
│   │   ├── s3.rs           # S3Source
│   │   └── local.rs        # LocalSource
│   ├── reader/
│   │   ├── mod.rs
│   │   ├── header.rs       # Header parsing
│   │   ├── block.rs        # Block reading
│   │   ├── buffer.rs       # PrefetchBuffer
│   │   └── decode.rs       # Record decoding
│   ├── convert/
│   │   ├── mod.rs
│   │   ├── arrow.rs        # Avro → Arrow conversion
│   │   └── polars.rs       # Arrow → Polars DataFrame
│   ├── python/
│   │   ├── mod.rs
│   │   ├── reader.rs       # AvroReader PyClass
│   │   └── errors.rs       # Python exception types
│   └── error.rs            # Error types
├── tests/
│   ├── schema_tests.rs
│   ├── codec_tests.rs
│   ├── reader_tests.rs
│   ├── property_tests.rs   # All property-based tests
│   └── interop/            # Apache Avro test files
└── benches/
    └── throughput.rs
```
