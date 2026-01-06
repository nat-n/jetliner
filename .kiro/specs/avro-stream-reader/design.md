# Design Document: Avro Stream Reader

## Overview

This document describes the architecture and design of a high-performance Rust library for streaming Avro data into Polars DataFrames. The library provides Python bindings via PyO3 and supports reading from both S3 and local filesystem.

### Key Design Decisions

1. **Direct Arrow Integration**: Deserialize Avro directly into Arrow arrays (Polars' underlying format), avoiding intermediate representations
2. **PyO3 + pyo3-polars**: Use PyO3 for Python bindings with pyo3-polars for zero-copy DataFrame transfer
3. **Async I/O with Tokio**: Use async runtime for S3 operations and prefetching
4. **Block-oriented Processing**: Process Avro blocks as the unit of work for streaming and parallelism

### Why Not Polars Plugins?

Polars plugins are designed for adding custom expressions/functions to query processing, not for data source integration. Our use case (streaming data source) is better served by:
- Building DataFrames in Rust using polars crate directly
- Returning them to Python via pyo3-polars' `PyDataFrame` wrapper
- This gives us full control over streaming, buffering, and memory management

## Architecture

**Note on Async**: The Rust core uses Tokio async internally for S3 I/O and prefetching, but the Python API is **synchronous only**. Python asyncio support (`__aiter__`/`__anext__`) is explicitly out of scope—the complexity of bridging Tokio and Python's asyncio event loops is not justified for this use case. The sync Python API blocks on the internal Tokio runtime.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Python API Layer                             │
│  ┌─────────────────┐  ┌─────────────────┐                           │
│  │  AvroReader     │  │  BatchIterator  │                           │
│  │  (entry point)  │  │  (__iter__)     │                           │
│  └────────┬────────┘  └────────┬────────┘                           │
└───────────┼────────────────────┼────────────────────────────────────┘
            │                    │
            ▼                    ▼
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
│  │ PrefetchBuf  │    │    Codec     │    │   SchemaConverter    │   │
│  │  (async)     │    │ (decompress) │    │  (Avro→Arrow types)  │   │
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

### BlockReader

Reads and parses blocks from a source.

```rust
pub struct BlockReader<S: StreamSource> {
    source: S,
    header: AvroHeader,
    current_offset: u64,
    block_index: usize,
}

impl<S: StreamSource> BlockReader<S> {
    pub async fn new(source: S) -> Result<Self, ReaderError>;

    pub async fn next_block(&mut self) -> Result<Option<AvroBlock>, ReaderError>;

    pub fn header(&self) -> &AvroHeader;

    pub async fn seek_to_sync(&mut self, sync_marker: &[u8; 16]) -> Result<bool, ReaderError>;
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
    pub error_mode: ErrorMode,
    pub reader_schema: Option<AvroSchema>,  // For schema evolution
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

### Module Structure

```python
import avro_stream

# Main entry point
reader = avro_stream.open("s3://bucket/file.avro")
reader = avro_stream.open("/path/to/file.avro")
reader = avro_stream.open(
    "s3://bucket/file.avro",
    batch_size=100_000,
    buffer_blocks=4,
    buffer_bytes=64 * 1024 * 1024,
    strict=False,
)

# Sync iteration (async iteration is NOT supported)
for df in reader:
    process(df)

# Context manager
with avro_stream.open("file.avro") as reader:
    for df in reader:
        process(df)

# Schema inspection
reader = avro_stream.open("file.avro")
print(reader.schema)  # JSON string
print(reader.schema_dict)  # Python dict

# Error inspection (after iteration)
for error in reader.errors:
    print(f"Block {error.block_index}: {error.message}")
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
}

#[pyclass]
pub struct PyReadError {
    #[pyo3(get)]
    block_index: usize,
    #[pyo3(get)]
    record_index: Option<usize>,
    #[pyo3(get)]
    message: String,
}
```

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

## Memory Management and Backpressure

### Lessons from Existing Implementations

**polars-avro (hafaio)**: Uses `apache-avro` crate which is "fully compliant but does a lot of unnecessary memory allocation and object creation." Result: ~7x slower than native Polars. Key insight: avoid the `apache-avro` crate's `Value` enum which allocates for every field.

**Native Polars Avro** (deprecated): Was faster because it decoded directly into Arrow buffers. We should follow this approach but with streaming support.

### Memory Lifecycle Through the Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MEMORY BUFFER LOCATIONS                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
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
│                                                                              │
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

| Rust Error                        | Python Exception                |
| --------------------------------- | ------------------------------- |
| SourceError::NotFound             | FileNotFoundError               |
| SourceError::PermissionDenied     | PermissionError                 |
| SourceError::AuthenticationFailed | avro_stream.AuthenticationError |
| ReaderError::Parse                | avro_stream.ParseError          |
| ReaderError::Schema               | avro_stream.SchemaError         |
| ReaderError::Codec                | avro_stream.CodecError          |
| ReaderError::Decode               | avro_stream.DecodeError         |

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
- Each property test tagged with: `Feature: avro-stream-reader, Property N: {description}`
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
avro-stream/
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
