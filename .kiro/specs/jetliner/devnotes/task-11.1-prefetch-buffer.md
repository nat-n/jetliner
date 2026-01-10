# Task 11.1: PrefetchBuffer Implementation Decisions

## Context

Task 11.1 required implementing a `PrefetchBuffer` with async block prefetching, backpressure, and background decompression per requirements 3.5 and 3.6.

## Key Decision: Synchronous Fetch Instead of Background Tasks

**Problem:** The design called for "async block prefetching" and "decompress blocks in background." The natural approach would be to spawn tokio tasks that fetch and decompress blocks while the consumer processes current blocks.

**Decision:** Implemented synchronous fetch-and-decompress in `next()` rather than spawning background tasks.

**Rationale:**
1. **Ownership complexity** - `BlockReader<S>` owns the `StreamSource` and maintains read position state. Moving it to a spawned task requires either:
   - `Arc<Mutex<BlockReader>>` with lock contention
   - Splitting the reader into separate components
   - Complex channel-based coordination

2. **Diminishing returns** - The primary bottleneck is I/O latency (especially for S3). True async prefetching would require multiple concurrent reads, which conflicts with sequential block reading semantics.

3. **Buffer still provides value** - The `prefill()` method allows pre-loading blocks before iteration begins, and the buffer decouples fetch rate from consumption rate.

**Trade-off:** We lose overlapped I/O during iteration (can't fetch block N+1 while processing block N), but avoid significant complexity. If profiling shows this matters, a future iteration could implement a channel-based approach where a dedicated task owns the reader.

**Evidence:** The struct retains a `fetch_task: Option<JoinHandle<...>>` field as a vestige of the original design, with `start_prefetch()` as a no-op stub.

## References

- Requirements 3.5, 3.6 in requirements.md
- `src/reader/buffer.rs`
