# Appendix G: Pipeline Parallelism Feasibility Analysis

**Date:** 2026-01-30
**Context:** Evaluating approaches to overlap I/O, decompression, and decoding stages for improved throughput in the Avro streaming pipeline.

**Critical Constraint:** Any implementation MUST add zero overhead to simple cases (single-block files, sequential access) and MUST maintain bounded memory guarantees.

---

## Executive Summary

Pipeline parallelism is **feasible** with moderate-to-high complexity. The recommended approach is a phased implementation:

1. **Phase 1:** Async I/O prefetch with bounded channel (lowest risk, highest value)
2. **Phase 2:** Parallel decompression via task pool
3. **Phase 3:** Parallel decode with ordering guarantees (highest complexity)

**Expected Benefits:**
- 30-50% throughput improvement for I/O-bound workloads (S3, slow disks)
- 20-40% improvement for CPU-bound workloads (heavy compression, complex schemas)
- Near-linear scaling with block count for multi-block files

**Key Risks:**
- Ordering guarantees add complexity
- Backpressure implementation is subtle
- Error propagation across async boundaries
- Testing concurrent code is inherently difficult

---

## Current Architecture Analysis

### Data Flow (Sequential)

```
┌─────────────┐     ┌──────────────┐     ┌───────────────┐     ┌───────────────┐
│ BlockReader │────▶│ PrefetchBuf  │────▶│ DataFrameBld  │────▶│   DataFrame   │
│  (I/O)      │     │ (Decompress) │     │ (Decode+Build)│     │   (Output)    │
└─────────────┘     └──────────────┘     └───────────────┘     └───────────────┘
        │                   │                    │
        ▼                   ▼                    ▼
   read_range()      codec.decompress()   decode_record()
   ~50-100ms (S3)    ~1-10ms/block        ~0.1-1ms/record
```

### Current Implementation Details

**BlockReader** (`src/reader/block.rs`):
- Maintains internal read buffer with configurable chunk size
- S3: 4MB chunks, 50% prefetch threshold
- Local: 64KB chunks, no eager prefetch
- Handles sync marker validation and error recovery
- **Key insight:** Already has scaffolding for async but executes synchronously

**PrefetchBuffer** (`src/reader/buffer.rs`):
- Has `JoinHandle` field for background tasks (currently unused!)
- `maybe_start_prefetch()` and `start_prefetch()` methods exist but are no-ops
- Comment: "fetch and decompress synchronously in next() instead of spawning tasks"
- **Key insight:** Infrastructure exists but was never completed

**DataFrameBuilder** (`src/convert/dataframe.rs`):
- Calls `decoder.reserve_for_batch(record_count)` for pre-allocation
- Processes blocks sequentially via `add_block()`
- Accumulates records until batch_size, then builds DataFrame
- **Key insight:** Stateful, not easily parallelizable without redesign

### Timing Analysis (Typical Workload)

| Stage               | Local File       | S3 Source        | Notes                    |
| ------------------- | ---------------- | ---------------- | ------------------------ |
| I/O (read_range)    | 0.1-1ms          | 50-100ms         | S3 latency dominates     |
| Decompress (snappy) | 1-5ms            | 1-5ms            | CPU-bound                |
| Decompress (zstd)   | 5-20ms           | 5-20ms           | Higher compression ratio |
| Decode (simple)     | 0.1ms/1K records | 0.1ms/1K records | Primitive types          |
| Decode (complex)    | 1-5ms/1K records | 1-5ms/1K records | Nested structs, arrays   |
| Arrow build         | 0.5-2ms/batch    | 0.5-2ms/batch    | Memory allocation        |

**Observation:** For S3 sources, I/O latency (50-100ms) dwarfs all other stages combined. Pipeline parallelism can hide this latency almost entirely.


---

## Architecture Options

### Option A: Channel-Based Pipeline (Recommended)

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   I/O Task  │───▶│  Decompress │───▶│   Decode    │───▶│   Build     │
│  (tokio)    │    │    Task     │    │    Task     │    │    Task     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       │                  │                  │                  │
       ▼                  ▼                  ▼                  ▼
   bounded_tx         bounded_tx         bounded_tx         DataFrame
   (backpressure)    (backpressure)    (backpressure)
```

**Implementation Sketch:**

```rust
use tokio::sync::mpsc;

struct PipelineConfig {
    io_buffer_size: usize,      // Max blocks waiting for decompress
    decomp_buffer_size: usize,  // Max blocks waiting for decode
    decode_buffer_size: usize,  // Max decoded batches waiting for build
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            io_buffer_size: 4,      // ~4 blocks in flight
            decomp_buffer_size: 2,  // Decompress is fast, small buffer
            decode_buffer_size: 2,  // Decode batches ready for build
        }
    }
}

struct PipelinedReader<S: StreamSource> {
    // Channels for pipeline stages
    io_tx: mpsc::Sender<AvroBlock>,
    decomp_rx: mpsc::Receiver<DecompressedBlock>,

    // Task handles for cleanup
    io_task: JoinHandle<Result<(), ReaderError>>,
    decomp_task: JoinHandle<Result<(), ReaderError>>,

    // State
    config: PipelineConfig,
    finished: bool,
}
```

**Pros:**
- Natural backpressure via bounded channels
- Clean separation of concerns
- Easy to reason about ordering (FIFO channels)
- Graceful shutdown via channel close

**Cons:**
- Channel overhead (~50-100ns per send/recv)
- More complex error propagation
- Need to handle task panics

### Option B: Task-Based with Ordering

```rust
use tokio::task::JoinSet;

struct OrderedTaskPool {
    pending: JoinSet<(usize, Result<DecompressedBlock, ReaderError>)>,
    next_expected: usize,
    completed: BTreeMap<usize, DecompressedBlock>,
    max_in_flight: usize,
}

impl OrderedTaskPool {
    async fn submit(&mut self, block_index: usize, block: AvroBlock, codec: Codec) {
        // Wait if at capacity (backpressure)
        while self.pending.len() >= self.max_in_flight {
            self.drain_one().await;
        }

        self.pending.spawn(async move {
            let result = codec.decompress(&block.data)
                .map(|data| DecompressedBlock::new(
                    block.record_count,
                    Bytes::from(data),
                    block.block_index,
                ));
            (block_index, result)
        });
    }

    async fn next_in_order(&mut self) -> Option<Result<DecompressedBlock, ReaderError>> {
        loop {
            // Check if next expected is already completed
            if let Some(block) = self.completed.remove(&self.next_expected) {
                self.next_expected += 1;
                return Some(Ok(block));
            }

            // Wait for more tasks to complete
            match self.pending.join_next().await {
                Some(Ok((idx, result))) => {
                    match result {
                        Ok(block) => {
                            if idx == self.next_expected {
                                self.next_expected += 1;
                                return Some(Ok(block));
                            } else {
                                self.completed.insert(idx, block);
                            }
                        }
                        Err(e) => return Some(Err(e.into())),
                    }
                }
                Some(Err(e)) => return Some(Err(ReaderError::Internal(e.to_string()))),
                None => return None, // All tasks done
            }
        }
    }
}
```

**Pros:**
- More flexible parallelism (not strictly pipelined)
- Can parallelize within a stage (multiple decompressions)
- Lower overhead than channels for small workloads

**Cons:**
- Ordering logic is complex and error-prone
- BTreeMap overhead for out-of-order completion
- Harder to implement backpressure correctly

### Option C: Rayon Thread Pool (Not Recommended for Async)

```rust
use rayon::prelude::*;

// Batch multiple blocks and decompress in parallel
fn decompress_batch(blocks: Vec<AvroBlock>, codec: Codec) -> Vec<Result<DecompressedBlock, CodecError>> {
    blocks.par_iter()
        .map(|block| {
            codec.decompress(&block.data)
                .map(|data| DecompressedBlock::new(
                    block.record_count,
                    Bytes::from(data),
                    block.block_index,
                ))
        })
        .collect()
}
```

**Pros:**
- Simple API
- Excellent CPU utilization for batch operations
- No async complexity

**Cons:**
- Doesn't integrate well with tokio async runtime
- Blocks the async executor during parallel work
- Not suitable for I/O-bound stages
- Loses streaming benefits (must batch first)

### Recommendation: Hybrid Approach

Use **Option A (channels)** for the I/O → Decompress pipeline, and **Option B (task pool)** for parallel decompression within the decompress stage:

```
┌─────────────┐    ┌─────────────────────────┐    ┌─────────────┐
│   I/O Task  │───▶│   Decompress Pool       │───▶│   Decode    │
│  (tokio)    │    │  (OrderedTaskPool)      │    │  (serial)   │
└─────────────┘    └─────────────────────────┘    └─────────────┘
       │                      │                         │
       ▼                      ▼                         ▼
   bounded_tx            max_in_flight              DataFrame
   (backpressure)        (backpressure)
```

This gives us:
- Async I/O with prefetch (hides S3 latency)
- Parallel decompression (utilizes multiple cores)
- Serial decode (maintains ordering, simpler correctness)
- Bounded memory at every stage


---

## Ordering Guarantees

### The Ordering Problem

Avro files have strict record ordering semantics. Records must appear in the output DataFrame in the same order they appear in the file. With parallel processing, blocks may complete out of order:

```
Time →
Block 1: [Read]────[Decompress]────────────[Done]
Block 2: [Read]──────[Decompress]──[Done]          ← Finishes first!
Block 3: [Read]────────[Decompress]────[Done]
```

If we naively emit blocks as they complete, Block 2 would appear before Block 1 in the output.

### Solution: Sequence Numbers + Reordering Buffer

```rust
/// A block tagged with its sequence number for ordering
struct SequencedBlock {
    sequence: usize,
    block: DecompressedBlock,
}

/// Reorders out-of-order blocks back into sequence
struct ReorderBuffer {
    next_sequence: usize,
    pending: BTreeMap<usize, DecompressedBlock>,
    max_pending: usize,  // Backpressure limit
}

impl ReorderBuffer {
    fn insert(&mut self, seq: usize, block: DecompressedBlock) -> Option<DecompressedBlock> {
        if seq == self.next_sequence {
            self.next_sequence += 1;
            Some(block)
        } else {
            self.pending.insert(seq, block);
            None
        }
    }

    fn drain_ready(&mut self) -> impl Iterator<Item = DecompressedBlock> + '_ {
        std::iter::from_fn(move || {
            self.pending.remove(&self.next_sequence).map(|block| {
                self.next_sequence += 1;
                block
            })
        })
    }

    fn is_full(&self) -> bool {
        self.pending.len() >= self.max_pending
    }
}
```

### Invariants That Must Hold

1. **Sequence Monotonicity:** Each block gets a unique, monotonically increasing sequence number assigned at read time
2. **No Gaps:** Every sequence number from 0 to N-1 must eventually be emitted
3. **FIFO Output:** Blocks are emitted in strictly increasing sequence order
4. **Bounded Buffer:** The reorder buffer never exceeds `max_pending` entries

### Proof Sketch

Given:
- I/O stage assigns sequence numbers 0, 1, 2, ... in order
- Each block eventually completes (no infinite hangs)
- Reorder buffer waits for `next_sequence` before emitting

Then:
- Block 0 will eventually complete and be emitted first
- Block 1 will eventually complete; if before Block 0, it waits in buffer
- By induction, all blocks emit in order

The bounded buffer guarantee requires backpressure: if the buffer is full, the producer must wait. This is enforced by the channel capacity or explicit checks.

---

## Backpressure Design

### Current Bounded Memory Model

The current `BufferConfig` provides bounded memory:

```rust
pub struct BufferConfig {
    pub max_blocks: usize,    // Default: 4
    pub max_bytes: usize,     // Default: 64MB
}
```

### Pipeline Backpressure Requirements

Each stage must respect memory bounds:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   I/O       │    │  Decompress │    │   Decode    │
│  Stage      │    │   Stage     │    │   Stage     │
└─────────────┘    └─────────────┘    └─────────────┘
      │                  │                  │
      ▼                  ▼                  ▼
  max_blocks=4      max_blocks=2      batch_size
  max_bytes=64MB    max_bytes=32MB    (rows limit)
```

### Implementation with Bounded Channels

```rust
use tokio::sync::mpsc;

// Bounded channel provides natural backpressure
let (tx, rx) = mpsc::channel::<AvroBlock>(config.max_blocks);

// Producer blocks when channel is full
async fn io_stage(tx: mpsc::Sender<AvroBlock>, reader: BlockReader) {
    while let Some(block) = reader.next_block().await? {
        // This will await if channel is full (backpressure!)
        tx.send(block).await.map_err(|_| ReaderError::ChannelClosed)?;
    }
}

// Consumer pulls at its own pace
async fn decompress_stage(
    rx: mpsc::Receiver<AvroBlock>,
    tx: mpsc::Sender<DecompressedBlock>,
    codec: Codec,
) {
    while let Some(block) = rx.recv().await {
        let decompressed = codec.decompress(&block.data)?;
        tx.send(DecompressedBlock::new(...)).await?;
    }
}
```

### Byte-Based Backpressure

Channel capacity alone doesn't bound memory in bytes. We need explicit tracking:

```rust
struct ByteBoundedChannel<T> {
    inner: mpsc::Sender<T>,
    current_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
    notify: Arc<Notify>,
}

impl<T: HasSize> ByteBoundedChannel<T> {
    async fn send(&self, item: T) -> Result<(), SendError<T>> {
        let size = item.size();

        // Wait until we have capacity
        loop {
            let current = self.current_bytes.load(Ordering::Acquire);
            if current + size <= self.max_bytes {
                break;
            }
            self.notify.notified().await;
        }

        self.current_bytes.fetch_add(size, Ordering::Release);
        self.inner.send(item).await
    }
}
```

### Backpressure Verification

To verify backpressure works correctly:

1. **Slow Consumer Test:** Consumer sleeps 100ms per block, verify producer blocks
2. **Memory Bound Test:** Track peak memory, verify it stays under limit
3. **Throughput Test:** Verify backpressure doesn't cause deadlock


---

## Zero-Overhead for Simple Cases

### Critical Requirement

Single-block files and sequential access patterns MUST NOT pay parallelism overhead. This is achievable through:

### Option A: Adaptive Pipeline Depth

```rust
enum PipelineMode {
    /// No parallelism - direct synchronous calls
    Sequential,
    /// Full pipeline with async stages
    Parallel(PipelineConfig),
}

impl PipelineMode {
    fn choose(file_size: u64, block_count_hint: Option<usize>) -> Self {
        // Heuristics for choosing mode
        const MIN_SIZE_FOR_PARALLEL: u64 = 1024 * 1024;  // 1MB
        const MIN_BLOCKS_FOR_PARALLEL: usize = 2;

        if file_size < MIN_SIZE_FOR_PARALLEL {
            return PipelineMode::Sequential;
        }

        if let Some(count) = block_count_hint {
            if count < MIN_BLOCKS_FOR_PARALLEL {
                return PipelineMode::Sequential;
            }
        }

        PipelineMode::Parallel(PipelineConfig::default())
    }
}
```

### Option B: Lazy Pipeline Initialization

```rust
struct LazyPipeline<S: StreamSource> {
    /// Start in sequential mode
    mode: PipelineState<S>,
}

enum PipelineState<S: StreamSource> {
    /// Haven't read first block yet
    Initial(BlockReader<S>),
    /// Single block file - stay sequential
    Sequential(BlockReader<S>),
    /// Multi-block file - switch to parallel
    Parallel(ParallelPipeline<S>),
}

impl<S: StreamSource> LazyPipeline<S> {
    async fn next(&mut self) -> Result<Option<DecompressedBlock>, ReaderError> {
        match &mut self.mode {
            PipelineState::Initial(reader) => {
                // Read first block to determine mode
                let first_block = reader.next_block().await?;

                if first_block.is_none() {
                    return Ok(None);
                }

                // Peek ahead to see if there are more blocks
                let has_more = !reader.is_finished();

                if has_more {
                    // Switch to parallel mode
                    self.mode = PipelineState::Parallel(
                        ParallelPipeline::new(reader, first_block.unwrap())
                    );
                } else {
                    // Stay sequential
                    self.mode = PipelineState::Sequential(reader);
                    return self.decompress_sync(first_block.unwrap());
                }

                self.next().await
            }
            PipelineState::Sequential(reader) => {
                // Direct synchronous path - zero overhead
                match reader.next_block().await? {
                    Some(block) => self.decompress_sync(block),
                    None => Ok(None),
                }
            }
            PipelineState::Parallel(pipeline) => {
                pipeline.next().await
            }
        }
    }
}
```

### Verification

Benchmark the sequential path before and after implementation:

```bash
# Create single-block test file
python -c "import fastavro; ..."

# Before implementation
cargo bench --bench read_throughput -- single_block_baseline

# After implementation
cargo bench --bench read_throughput -- single_block_with_pipeline

# Compare: should be within noise margin (<2% difference)
```

---

## Correctness Analysis

### Invariants

1. **Record Order:** Records appear in output in file order
2. **Completeness:** Every record in the file appears exactly once in output
3. **Memory Bound:** Peak memory never exceeds configured limits
4. **Error Propagation:** Errors in any stage propagate to caller
5. **Graceful Shutdown:** All tasks terminate cleanly on error or completion

### Potential Race Conditions

| Race Condition    | Scenario                                     | Mitigation                         |
| ----------------- | -------------------------------------------- | ---------------------------------- |
| Lost block        | Producer sends, consumer crashes before recv | Channel close propagates error     |
| Duplicate block   | Retry logic sends same block twice           | Sequence numbers detect duplicates |
| Out-of-order emit | Fast block overtakes slow block              | Reorder buffer enforces ordering   |
| Memory explosion  | Producer faster than consumer                | Bounded channels + byte tracking   |
| Deadlock          | Circular wait on channels                    | Unidirectional flow, no cycles     |
| Use-after-free    | Block data freed while in use                | `Bytes` uses Arc, safe sharing     |

### Formal Verification Approach

For critical sections, consider using `loom` for exhaustive concurrency testing:

```rust
#[cfg(loom)]
mod loom_tests {
    use loom::sync::Arc;
    use loom::thread;

    #[test]
    fn test_reorder_buffer_concurrent() {
        loom::model(|| {
            let buffer = Arc::new(Mutex::new(ReorderBuffer::new(4)));

            // Spawn producers that insert out of order
            let handles: Vec<_> = (0..3).map(|i| {
                let buf = buffer.clone();
                thread::spawn(move || {
                    buf.lock().unwrap().insert(i, make_block(i));
                })
            }).collect();

            for h in handles {
                h.join().unwrap();
            }

            // Verify ordering
            let mut buf = buffer.lock().unwrap();
            let blocks: Vec<_> = buf.drain_ready().collect();
            assert_eq!(blocks.len(), 3);
            for (i, block) in blocks.iter().enumerate() {
                assert_eq!(block.block_index, i);
            }
        });
    }
}
```


---

## Testing Strategy

### Unit Tests

#### 1. Reorder Buffer Tests
```rust
#[test]
fn test_reorder_in_order() {
    let mut buf = ReorderBuffer::new(4);
    assert_eq!(buf.insert(0, block(0)), Some(block(0)));
    assert_eq!(buf.insert(1, block(1)), Some(block(1)));
}

#[test]
fn test_reorder_out_of_order() {
    let mut buf = ReorderBuffer::new(4);
    assert_eq!(buf.insert(2, block(2)), None);  // Buffered
    assert_eq!(buf.insert(1, block(1)), None);  // Buffered
    assert_eq!(buf.insert(0, block(0)), Some(block(0)));  // Triggers drain

    let drained: Vec<_> = buf.drain_ready().collect();
    assert_eq!(drained, vec![block(1), block(2)]);
}

#[test]
fn test_reorder_backpressure() {
    let mut buf = ReorderBuffer::new(2);
    buf.insert(2, block(2));
    buf.insert(3, block(3));
    assert!(buf.is_full());  // Can't insert more until drain
}
```

#### 2. Channel Backpressure Tests
```rust
#[tokio::test]
async fn test_bounded_channel_blocks() {
    let (tx, mut rx) = mpsc::channel::<i32>(2);

    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();

    // Third send should block
    let send_task = tokio::spawn(async move {
        tx.send(3).await.unwrap();
    });

    // Give it time to block
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!send_task.is_finished());

    // Receive one, unblocking the send
    rx.recv().await;
    send_task.await.unwrap();
}
```

#### 3. Sequence Number Assignment Tests
```rust
#[test]
fn test_sequence_numbers_monotonic() {
    let mut reader = MockBlockReader::new(10);
    let mut sequences = Vec::new();

    while let Some(block) = reader.next_block() {
        sequences.push(block.sequence);
    }

    // Verify strictly increasing
    for window in sequences.windows(2) {
        assert_eq!(window[1], window[0] + 1);
    }
}
```

### Integration Tests

#### 1. Ordering Correctness
```python
def test_pipeline_preserves_order():
    """Records must appear in output in file order."""
    # Create file with sequential IDs
    records = [{"id": i, "data": f"record_{i}"} for i in range(10000)]
    write_avro("test.avro", records, block_size=100)  # Many small blocks

    # Read with pipeline parallelism
    df = jetliner.scan("test.avro").collect()

    # Verify order preserved
    ids = df["id"].to_list()
    assert ids == list(range(10000))
```

#### 2. Completeness
```python
def test_pipeline_no_lost_records():
    """Every record must appear exactly once."""
    n_records = 50000
    records = [{"id": i} for i in range(n_records)]
    write_avro("test.avro", records)

    df = jetliner.scan("test.avro").collect()

    assert df.height() == n_records
    assert df["id"].n_unique() == n_records
```

#### 3. Memory Bounds
```python
def test_pipeline_memory_bounded():
    """Peak memory should stay under configured limit."""
    import tracemalloc

    tracemalloc.start()

    # Large file, small buffer
    config = {"max_blocks": 2, "max_bytes": 1024 * 1024}  # 1MB
    df = jetliner.scan("large.avro", buffer_config=config).collect()

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # Peak should be bounded (with some overhead for Python)
    assert peak < 10 * 1024 * 1024  # 10MB total
```

### Property Tests

```rust
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jetliner, Property G.1: Pipeline preserves record ordering
    #[test]
    fn prop_pipeline_preserves_order(
        n_blocks in 1..20usize,
        records_per_block in 1..100usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Create test data with sequential IDs
            let file = create_test_file(n_blocks, records_per_block);

            // Read with pipeline
            let df = read_with_pipeline(&file).await.unwrap();

            // Verify ordering
            let ids: Vec<i64> = df.column("id").unwrap()
                .i64().unwrap()
                .into_no_null_iter()
                .collect();

            let expected: Vec<i64> = (0..(n_blocks * records_per_block) as i64).collect();
            prop_assert_eq!(ids, expected);
        });
    }

    /// Feature: jetliner, Property G.2: Pipeline handles all block counts
    #[test]
    fn prop_pipeline_handles_any_block_count(n_blocks in 0..50usize) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let file = create_test_file(n_blocks, 10);
            let df = read_with_pipeline(&file).await.unwrap();
            prop_assert_eq!(df.height(), n_blocks * 10);
        });
    }
}
```

### Stress Tests

#### 1. Slow Consumer
```rust
#[tokio::test]
async fn stress_slow_consumer() {
    let file = create_large_test_file(100);  // 100 blocks

    let mut reader = PipelinedReader::new(file, PipelineConfig {
        io_buffer_size: 4,
        ..Default::default()
    }).await.unwrap();

    let mut count = 0;
    while let Some(block) = reader.next().await.unwrap() {
        // Simulate slow processing
        tokio::time::sleep(Duration::from_millis(50)).await;
        count += 1;
    }

    assert_eq!(count, 100);
}
```

#### 2. Fast Producer (S3 Simulation)
```rust
#[tokio::test]
async fn stress_fast_producer() {
    // Mock source that returns data instantly (simulates cached S3)
    let source = InstantMockSource::new(large_file_data());

    let mut reader = PipelinedReader::new(source, PipelineConfig {
        io_buffer_size: 2,  // Small buffer
        ..Default::default()
    }).await.unwrap();

    // Verify backpressure prevents memory explosion
    let start_mem = get_memory_usage();

    let mut count = 0;
    while let Some(_) = reader.next().await.unwrap() {
        count += 1;
        let current_mem = get_memory_usage();
        assert!(current_mem - start_mem < 100 * 1024 * 1024);  // <100MB growth
    }
}
```

#### 3. Mixed Block Sizes
```rust
#[tokio::test]
async fn stress_mixed_block_sizes() {
    // Create file with varying block sizes
    let blocks = vec![
        (10, 100),      // 10 records, 100 bytes
        (1000, 50000),  // 1000 records, 50KB
        (5, 20),        // 5 records, 20 bytes
        (500, 100000),  // 500 records, 100KB
    ];

    let file = create_file_with_blocks(&blocks);
    let df = read_with_pipeline(&file).await.unwrap();

    let expected_records: usize = blocks.iter().map(|(n, _)| n).sum();
    assert_eq!(df.height(), expected_records);
}
```

### Chaos Tests

#### 1. Random Delays
```rust
#[tokio::test]
async fn chaos_random_delays() {
    let source = RandomDelaySource::new(
        real_source,
        Duration::from_millis(0)..Duration::from_millis(100),
    );

    let df = read_with_pipeline(source).await.unwrap();
    verify_correctness(&df);
}
```

#### 2. Simulated Errors
```rust
#[tokio::test]
async fn chaos_random_errors() {
    let source = ErrorInjectingSource::new(
        real_source,
        0.1,  // 10% error rate
    );

    // In skip mode, should recover
    let result = read_with_pipeline_skip_mode(source).await;
    assert!(result.is_ok());

    // Verify we got some data
    let df = result.unwrap();
    assert!(df.height() > 0);
}
```

### Deterministic Testing

For reproducible concurrent tests, use `tokio::time::pause()`:

```rust
#[tokio::test]
async fn deterministic_pipeline_timing() {
    tokio::time::pause();  // Use simulated time

    let mut reader = PipelinedReader::new(...).await.unwrap();

    // Advance time deterministically
    tokio::time::advance(Duration::from_millis(100)).await;

    let block = reader.next().await.unwrap();
    assert!(block.is_some());
}
```


---

## Performance Optimization Opportunities

### 1. Parallel Decompression

Multiple blocks can be decompressed simultaneously:

```rust
struct ParallelDecompressor {
    pool: OrderedTaskPool,
    codec: Codec,
    max_parallel: usize,
}

impl ParallelDecompressor {
    async fn decompress_batch(&mut self, blocks: Vec<AvroBlock>) -> Vec<DecompressedBlock> {
        // Submit all blocks to the pool
        for block in blocks {
            self.pool.submit(block.block_index, block, self.codec).await;
        }

        // Collect results in order
        let mut results = Vec::with_capacity(blocks.len());
        while let Some(result) = self.pool.next_in_order().await {
            results.push(result?);
        }
        results
    }
}
```

**Expected Benefit:** 2-4x speedup for CPU-bound decompression (zstd, bzip2)

### 2. SIMD Opportunities in Decompression

Some codecs can benefit from SIMD:

| Codec   | SIMD Opportunity              | Crate Support                |
| ------- | ----------------------------- | ---------------------------- |
| Snappy  | CRC32 calculation             | `crc32fast` uses SIMD        |
| Deflate | Already optimized in `flate2` | Uses `miniz_oxide`           |
| Zstd    | Highly optimized              | `zstd` crate uses native lib |
| LZ4     | Good SIMD potential           | Not currently supported      |

**Recommendation:** The codec crates already use SIMD where beneficial. Focus optimization efforts elsewhere.

### 3. Memory Pooling

Reduce allocation overhead by reusing buffers:

```rust
struct DecompressPool {
    buffers: Vec<Vec<u8>>,
    max_buffer_size: usize,
}

impl DecompressPool {
    fn get_buffer(&mut self, min_size: usize) -> Vec<u8> {
        // Try to reuse an existing buffer
        if let Some(pos) = self.buffers.iter().position(|b| b.capacity() >= min_size) {
            let mut buf = self.buffers.swap_remove(pos);
            buf.clear();
            return buf;
        }

        // Allocate new buffer
        Vec::with_capacity(min_size.max(self.max_buffer_size))
    }

    fn return_buffer(&mut self, buf: Vec<u8>) {
        if self.buffers.len() < 8 {  // Keep up to 8 buffers
            self.buffers.push(buf);
        }
        // Otherwise drop it
    }
}
```

**Expected Benefit:** 5-15% reduction in allocation overhead for high-throughput scenarios

### 4. Batch Size Tuning

Optimal batch sizes depend on the workload:

| Scenario                    | Optimal Batch Size | Rationale                     |
| --------------------------- | ------------------ | ----------------------------- |
| Small records, many columns | 10,000-50,000      | Amortize DataFrame overhead   |
| Large records, few columns  | 50,000-200,000     | Maximize cache efficiency     |
| Streaming to downstream     | 10,000-20,000      | Lower latency to first result |
| Full file scan              | 100,000+           | Maximize throughput           |

```rust
impl BuilderConfig {
    fn auto_tune(schema: &AvroSchema, estimated_record_size: usize) -> Self {
        let batch_size = match estimated_record_size {
            0..=100 => 100_000,
            101..=1000 => 50_000,
            1001..=10000 => 20_000,
            _ => 10_000,
        };

        Self { batch_size, ..Default::default() }
    }
}
```

### 5. Prefetch Depth Tuning

For S3, deeper prefetch hides more latency:

```rust
impl PipelineConfig {
    fn for_s3() -> Self {
        Self {
            io_buffer_size: 8,      // More blocks in flight
            decomp_buffer_size: 4,  // Parallel decompression
            decode_buffer_size: 2,
        }
    }

    fn for_local() -> Self {
        Self {
            io_buffer_size: 2,      // OS handles prefetch
            decomp_buffer_size: 2,
            decode_buffer_size: 1,
        }
    }
}
```

---

## User-Facing Tuning Parameters

### Proposed API Surface

The following parameters should be exposed to users for tuning pipeline behavior:

```python
# Python API
df = jetliner.scan(
    "s3://bucket/large-file.avro",

    # Existing parameters (unchanged)
    batch_size=100_000,
    error_mode="strict",

    # New pipeline parameters
    pipeline_mode="auto",           # "auto", "sequential", "parallel"
    prefetch_blocks=4,              # Max blocks to prefetch (I/O stage)
    prefetch_bytes=64 * 1024 * 1024,  # Max bytes to prefetch (64MB default)
    parallel_decompress=2,          # Max concurrent decompressions

    # Existing I/O parameters (already exposed)
    read_chunk_size=4 * 1024 * 1024,  # 4MB for S3
).collect()
```

### Parameter Reference

| Parameter             | Type | Default    | Range                                  | Description                                    |
| --------------------- | ---- | ---------- | -------------------------------------- | ---------------------------------------------- |
| `pipeline_mode`       | str  | `"auto"`   | `"auto"`, `"sequential"`, `"parallel"` | Controls pipeline parallelism                  |
| `prefetch_blocks`     | int  | `4`        | `1-16`                                 | Max blocks buffered between I/O and decompress |
| `prefetch_bytes`      | int  | `67108864` | `1MB-256MB`                            | Max bytes buffered (backpressure trigger)      |
| `parallel_decompress` | int  | `2`        | `1-8`                                  | Concurrent decompression tasks                 |
| `read_chunk_size`     | int  | varies     | `64KB-16MB`                            | Bytes per I/O read (existing)                  |

### Parameter Interactions

```
                    prefetch_blocks=4
                          │
                          ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   I/O       │───▶│  Decompress │───▶│   Decode    │
│   Stage     │    │   Stage     │    │   Stage     │
└─────────────┘    └─────────────┘    └─────────────┘
      │                  │                  │
      ▼                  ▼                  ▼
 read_chunk_size    parallel_decompress   batch_size
    (4MB S3)            (2)              (100K rows)
```

**Memory Budget Calculation:**
```
Peak Memory ≈ prefetch_blocks × avg_block_size
            + parallel_decompress × avg_decompressed_size
            + batch_size × avg_row_size
```

### Recommended Configurations

#### S3 / High-Latency Sources
```python
# Maximize latency hiding
jetliner.scan(
    "s3://bucket/file.avro",
    pipeline_mode="parallel",
    prefetch_blocks=8,           # Deep prefetch
    prefetch_bytes=128*1024*1024,  # 128MB buffer
    parallel_decompress=4,       # Utilize multiple cores
    read_chunk_size=4*1024*1024, # 4MB chunks (amortize HTTP overhead)
)
```

#### Local SSD / Low-Latency Sources
```python
# Minimal overhead, let OS handle prefetch
jetliner.scan(
    "/fast/ssd/file.avro",
    pipeline_mode="auto",        # Will likely choose sequential
    prefetch_blocks=2,           # Small buffer
    prefetch_bytes=16*1024*1024, # 16MB buffer
    parallel_decompress=2,       # Some parallelism for heavy codecs
    read_chunk_size=64*1024,     # 64KB chunks
)
```

#### Memory-Constrained Environment
```python
# Minimize memory footprint
jetliner.scan(
    "file.avro",
    pipeline_mode="sequential",  # No parallelism overhead
    prefetch_blocks=1,           # Minimal buffering
    prefetch_bytes=4*1024*1024,  # 4MB max
    parallel_decompress=1,       # Serial decompression
    batch_size=10_000,           # Smaller batches
)
```

#### Maximum Throughput (Large Memory)
```python
# Maximize throughput, memory is not a concern
jetliner.scan(
    "huge-file.avro",
    pipeline_mode="parallel",
    prefetch_blocks=16,          # Very deep prefetch
    prefetch_bytes=256*1024*1024,  # 256MB buffer
    parallel_decompress=8,       # Max parallelism
    batch_size=500_000,          # Large batches
)
```

### Auto-Tuning Heuristics

When `pipeline_mode="auto"` (default), the system chooses based on:

```python
def choose_pipeline_mode(source_type, file_size, estimated_blocks):
    # Small files: sequential (no parallelism overhead)
    if file_size < 1_MB:
        return "sequential"

    # Single block: sequential
    if estimated_blocks <= 1:
        return "sequential"

    # S3 sources: always parallel (latency hiding is critical)
    if source_type == "s3":
        return "parallel"

    # Large local files with multiple blocks: parallel
    if file_size > 100_MB and estimated_blocks > 4:
        return "parallel"

    # Default: sequential for local files
    return "sequential"
```

### Rust Configuration Struct

```rust
/// User-facing pipeline configuration
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Pipeline mode selection
    pub mode: PipelineMode,
    /// Max blocks to buffer between I/O and decompress
    pub prefetch_blocks: usize,
    /// Max bytes to buffer (backpressure trigger)
    pub prefetch_bytes: usize,
    /// Max concurrent decompression tasks
    pub parallel_decompress: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PipelineMode {
    /// Automatically choose based on source and file characteristics
    #[default]
    Auto,
    /// Force sequential processing (no parallelism)
    Sequential,
    /// Force parallel pipeline
    Parallel,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            mode: PipelineMode::Auto,
            prefetch_blocks: 4,
            prefetch_bytes: 64 * 1024 * 1024,  // 64MB
            parallel_decompress: 2,
        }
    }
}

impl PipelineConfig {
    /// Configuration optimized for S3 sources
    pub fn for_s3() -> Self {
        Self {
            mode: PipelineMode::Parallel,
            prefetch_blocks: 8,
            prefetch_bytes: 128 * 1024 * 1024,  // 128MB
            parallel_decompress: 4,
        }
    }

    /// Configuration for memory-constrained environments
    pub fn low_memory() -> Self {
        Self {
            mode: PipelineMode::Sequential,
            prefetch_blocks: 1,
            prefetch_bytes: 4 * 1024 * 1024,  // 4MB
            parallel_decompress: 1,
        }
    }
}
```

### Validation and Bounds

```rust
impl PipelineConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        // prefetch_blocks: 1-16
        if self.prefetch_blocks < 1 || self.prefetch_blocks > 16 {
            return Err(ConfigError::InvalidValue {
                param: "prefetch_blocks",
                value: self.prefetch_blocks.to_string(),
                valid_range: "1-16",
            });
        }

        // prefetch_bytes: 1MB-256MB
        const MIN_BYTES: usize = 1024 * 1024;
        const MAX_BYTES: usize = 256 * 1024 * 1024;
        if self.prefetch_bytes < MIN_BYTES || self.prefetch_bytes > MAX_BYTES {
            return Err(ConfigError::InvalidValue {
                param: "prefetch_bytes",
                value: format!("{} bytes", self.prefetch_bytes),
                valid_range: "1MB-256MB",
            });
        }

        // parallel_decompress: 1-8
        if self.parallel_decompress < 1 || self.parallel_decompress > 8 {
            return Err(ConfigError::InvalidValue {
                param: "parallel_decompress",
                value: self.parallel_decompress.to_string(),
                valid_range: "1-8",
            });
        }

        Ok(())
    }
}
```

### Observability

Users should be able to observe pipeline behavior for tuning:

```python
# Get pipeline statistics after read
reader = jetliner.AvroReader("file.avro", pipeline_config={...})
df = reader.collect()

stats = reader.pipeline_stats()
print(stats)
# PipelineStats {
#   mode_used: "parallel",
#   blocks_read: 47,
#   blocks_prefetched: 42,        # How many were ready when needed
#   prefetch_wait_ms: 120,        # Time spent waiting for prefetch
#   decompress_parallel_max: 3,   # Peak concurrent decompressions
#   decompress_total_ms: 890,     # Total decompression time
#   backpressure_events: 2,       # Times producer had to wait
# }
```

This helps users understand if their configuration is optimal:
- High `prefetch_wait_ms` → increase `prefetch_blocks`
- Low `decompress_parallel_max` → decrease `parallel_decompress` (wasting resources)
- High `backpressure_events` → consumer is slow, consider smaller `batch_size`

---

## Error Handling Across Pipeline Stages

### Error Propagation Model

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   I/O       │───▶│  Decompress │───▶│   Decode    │
│   Stage     │    │   Stage     │    │   Stage     │
└─────────────┘    └─────────────┘    └─────────────┘
      │                  │                  │
      ▼                  ▼                  ▼
  SourceError       CodecError        DecodeError
      │                  │                  │
      └──────────────────┴──────────────────┘
                         │
                         ▼
                   ReaderError (unified)
```

### Strict Mode

In strict mode, any error terminates the pipeline:

```rust
async fn io_stage(tx: Sender<AvroBlock>, reader: BlockReader) -> Result<(), ReaderError> {
    while let Some(block) = reader.next_block().await? {  // ? propagates error
        if tx.send(block).await.is_err() {
            // Receiver dropped - downstream error
            return Err(ReaderError::PipelineAborted);
        }
    }
    Ok(())
}
```

### Skip Mode

In skip mode, errors are logged and processing continues:

```rust
async fn decompress_stage_skip(
    rx: Receiver<AvroBlock>,
    tx: Sender<DecompressedBlock>,
    codec: Codec,
    errors: Arc<Mutex<Vec<ReadError>>>,
) -> Result<(), ReaderError> {
    while let Some(block) = rx.recv().await {
        match codec.decompress(&block.data) {
            Ok(data) => {
                let decompressed = DecompressedBlock::new(
                    block.record_count,
                    Bytes::from(data),
                    block.block_index,
                );
                if tx.send(decompressed).await.is_err() {
                    break;  // Downstream closed
                }
            }
            Err(e) => {
                // Log error and continue
                errors.lock().unwrap().push(ReadError::new(
                    ReadErrorKind::DecompressionFailed { codec: codec.name().to_string() },
                    block.block_index,
                    None,
                    block.file_offset,
                    e.to_string(),
                ));
                // Don't send anything for this block - it's skipped
            }
        }
    }
    Ok(())
}
```

### Ordering with Skipped Blocks

When blocks are skipped in skip mode, we must handle gaps in sequence numbers:

```rust
impl ReorderBuffer {
    fn mark_skipped(&mut self, seq: usize) {
        // If this was the next expected, advance
        if seq == self.next_sequence {
            self.next_sequence += 1;
            // Drain any that are now ready
            while self.pending.contains_key(&self.next_sequence) {
                self.next_sequence += 1;
            }
        } else {
            // Mark as skipped for later
            self.skipped.insert(seq);
        }
    }
}
```


---

## Implementation Roadmap

### Phase 1: Async I/O Prefetch (Week 1-2)

**Goal:** Complete the existing `PrefetchBuffer` scaffolding to actually prefetch.

**Tasks:**
1. Implement `start_prefetch()` to spawn background I/O task
2. Use bounded channel for I/O → decompress handoff
3. Implement backpressure via channel capacity
4. Add tests for prefetch behavior
5. Benchmark S3 latency hiding

**Risk:** Low - building on existing infrastructure
**Expected Benefit:** 30-50% improvement for S3 sources

**Code Changes:**
- `src/reader/buffer.rs`: Complete prefetch implementation
- `src/reader/stream.rs`: Wire up prefetch to stream reader

### Phase 2: Parallel Decompression (Week 3-4)

**Goal:** Decompress multiple blocks concurrently while maintaining order.

**Tasks:**
1. Implement `OrderedTaskPool` for parallel decompression
2. Add `ReorderBuffer` for ordering guarantees
3. Integrate with `PrefetchBuffer`
4. Add configuration for parallelism level
5. Benchmark CPU utilization

**Risk:** Medium - ordering logic is subtle
**Expected Benefit:** 20-40% improvement for CPU-bound codecs

**Code Changes:**
- New `src/reader/parallel.rs`: Parallel decompression infrastructure
- `src/reader/buffer.rs`: Integrate parallel decompression
- `src/reader/mod.rs`: Export new types

### Phase 3: Adaptive Pipeline (Week 5)

**Goal:** Zero overhead for simple cases.

**Tasks:**
1. Implement `PipelineMode` enum
2. Add heuristics for mode selection
3. Implement lazy pipeline initialization
4. Verify no regression for single-block files
5. Add configuration override

**Risk:** Low - mostly conditional logic
**Expected Benefit:** Ensures no regression for simple cases

### Phase 4: Integration & Polish (Week 6)

**Goal:** Production-ready implementation.

**Tasks:**
1. Expose pipeline configuration in Python API
2. Add comprehensive documentation
3. Performance benchmarking suite
4. Edge case testing
5. Memory profiling

**Deliverables:**
- Updated `ReaderConfig` with pipeline options
- Python API changes in `jetliner/__init__.py`
- Benchmark results documentation
- Updated user guide

---

## Benchmarking Strategy

### Metrics to Track

| Metric               | Description                | Target                        |
| -------------------- | -------------------------- | ----------------------------- |
| Throughput (MB/s)    | Raw data processing rate   | >500 MB/s local, >100 MB/s S3 |
| Latency to first row | Time until first DataFrame | <100ms local, <200ms S3       |
| Peak memory          | Maximum RSS during read    | <2x configured buffer size    |
| CPU utilization      | Cores used during read     | >80% for parallel stages      |

### Benchmark Suite

```rust
// benches/pipeline_throughput.rs

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_pipeline_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_modes");

    for n_blocks in [1, 10, 100, 1000] {
        let file = create_test_file(n_blocks, 1000);

        group.bench_with_input(
            BenchmarkId::new("sequential", n_blocks),
            &file,
            |b, file| {
                b.iter(|| read_sequential(file))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("parallel", n_blocks),
            &file,
            |b, file| {
                b.iter(|| read_parallel(file))
            },
        );
    }

    group.finish();
}

fn bench_s3_latency_hiding(c: &mut Criterion) {
    let mut group = c.benchmark_group("s3_latency");

    for latency_ms in [0, 50, 100, 200] {
        let source = MockS3Source::with_latency(Duration::from_millis(latency_ms));

        group.bench_with_input(
            BenchmarkId::new("prefetch_depth_1", latency_ms),
            &source,
            |b, source| {
                b.iter(|| read_with_prefetch(source, 1))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("prefetch_depth_4", latency_ms),
            &source,
            |b, source| {
                b.iter(|| read_with_prefetch(source, 4))
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_pipeline_modes, bench_s3_latency_hiding);
criterion_main!(benches);
```

### Regression Detection

Add CI job to detect performance regressions:

```yaml
# .github/workflows/bench.yml
- name: Run benchmarks
  run: cargo bench --bench pipeline_throughput -- --save-baseline pr

- name: Compare to main
  run: cargo bench --bench pipeline_throughput -- --baseline main --compare
```

---

## Complexity and Risk Assessment

### Overall Assessment

| Factor                    | Rating | Notes                                         |
| ------------------------- | ------ | --------------------------------------------- |
| Implementation complexity | High   | Multiple async stages, ordering, backpressure |
| Integration complexity    | Medium | Builds on existing infrastructure             |
| Testing complexity        | High   | Concurrent code is hard to test exhaustively  |
| Correctness risk          | Medium | Ordering bugs could cause data corruption     |
| Performance risk          | Low    | Can always fall back to sequential            |
| Maintenance burden        | Medium | More code paths to maintain                   |

### Risk Mitigation

| Risk                   | Mitigation                                             |
| ---------------------- | ------------------------------------------------------ |
| Ordering bugs          | Extensive property tests, sequence number verification |
| Memory leaks           | Bounded channels, explicit cleanup on error            |
| Deadlocks              | Unidirectional flow, timeout on channel ops            |
| Performance regression | Adaptive mode, benchmark CI                            |
| Complexity creep       | Clear phase boundaries, feature flags                  |

### Go/No-Go Criteria

**Proceed if:**
- Phase 1 shows >20% improvement for S3 workloads
- No correctness issues found in property tests
- Memory bounds are respected under stress

**Reconsider if:**
- Overhead exceeds 5% for single-block files
- Ordering bugs persist after multiple fix attempts
- Implementation exceeds 2x estimated time

---

## Alternatives Considered

### Alternative 1: Process-Level Parallelism

**Idea:** Use multiple processes to read different parts of the file.

**Why rejected:**
- Avro blocks don't have fixed positions (variable-length headers)
- Would require pre-scanning the file for block boundaries
- IPC overhead for combining results
- Doesn't help with S3 latency (still sequential requests)

### Alternative 2: Memory-Mapped I/O

**Idea:** Use mmap for local files to let OS handle prefetch.

**Why rejected:**
- Doesn't work for S3
- OS prefetch isn't aware of Avro block boundaries
- Current buffered I/O already performs well for local files
- Would add platform-specific code paths

### Alternative 3: Vectored I/O (readv)

**Idea:** Read multiple blocks in a single syscall.

**Why rejected:**
- Avro blocks have variable sizes (unknown until header parsed)
- Would require pre-scanning or fixed-size reads
- Marginal benefit over current buffered approach
- Not supported by S3 API

### Alternative 4: GPU Decompression

**Idea:** Offload decompression to GPU.

**Why rejected:**
- Massive complexity increase
- Not all systems have suitable GPUs
- PCIe transfer overhead may exceed benefit
- Codec libraries don't support GPU
- Overkill for typical workloads

---

## Conclusion

Pipeline parallelism is feasible and worthwhile for Jetliner. The recommended approach is:

1. **Start with Phase 1** (async I/O prefetch) as it provides the highest value for S3 workloads with lowest risk
2. **Add Phase 2** (parallel decompression) for CPU-bound workloads
3. **Implement Phase 3** (adaptive mode) to ensure zero overhead for simple cases
4. **Maintain strict correctness** through comprehensive testing at each phase

Expected outcome: 30-50% throughput improvement for S3 sources, 20-40% for CPU-bound local workloads, with no regression for simple cases.

---

## References

### Internal
- `src/reader/buffer.rs` — Current PrefetchBuffer with unused scaffolding
- `src/reader/block.rs` — BlockReader with read buffering
- `src/reader/stream.rs` — AvroStreamReader orchestration
- `src/convert/dataframe.rs` — DataFrameBuilder
- `src/codec/mod.rs` — Decompression implementations

### External
- [Tokio mpsc channels](https://docs.rs/tokio/latest/tokio/sync/mpsc/)
- [Crossbeam channels](https://docs.rs/crossbeam-channel/latest/crossbeam_channel/) (alternative)
- [Loom concurrency testing](https://docs.rs/loom/latest/loom/)
- [Apache Arrow parallel readers](https://arrow.apache.org/docs/cpp/parquet.html#parallel-column-decoding)
- [Polars streaming engine](https://docs.pola.rs/user-guide/concepts/streaming/)

---

## Document History

- **2026-01-30:** Initial feasibility analysis
- **Author:** Claude (assisted by human review)
- **Status:** Draft — awaiting review and prioritization
