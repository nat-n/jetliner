# Task 15.1: AvroReaderCore PyClass - Async Reader Ownership Pattern

## Context

Implementing `AvroReaderCore` PyClass required bridging Rust's async `AvroStreamReader` with Python's synchronous iterator protocol (`__iter__`/`__next__`).

## Problem

Initial attempt used `Option<AvroStreamReader<BoxedSource>>` directly in the struct:

```rust
#[pyclass]
pub struct AvroReaderCore {
    inner: Option<AvroStreamReader<BoxedSource>>,
    runtime: tokio::runtime::Runtime,
    // ...
}
```

This caused borrow checker errors in `__next__`:
```
error[E0502]: cannot borrow `slf` as immutable because it is also borrowed as mutable
```

The issue: `slf.inner.as_mut()` borrows `slf` mutably, then `slf.runtime.block_on()` tries to borrow `slf` immutably.

## Decision

Use `Arc<Mutex<Option<...>>>` to decouple the reader from the struct:

```rust
#[pyclass]
pub struct AvroReaderCore {
    inner: Arc<Mutex<Option<AvroStreamReader<BoxedSource>>>>,
    runtime: tokio::runtime::Runtime,
    schema_json: String,  // Cached at construction
    batch_size: usize,    // Cached at construction
    // ...
}
```

In `__next__`:
```rust
let inner = slf.inner.clone();  // Clone Arc, not the reader
let result = slf.runtime.block_on(async {
    let mut guard = inner.lock().await;
    // ... use guard
});
```

## Consequences

- **Pro**: Clean separation allows runtime and reader to be accessed independently
- **Pro**: `tokio::sync::Mutex` integrates naturally with async code inside `block_on`
- **Con**: Slight overhead from Arc clone and mutex lock per `__next__` call
- **Workaround**: Properties like `schema` and `batch_size` are cached at construction to avoid locking for read-only access

## Related

- `src/python/reader.rs` - Implementation
- `src/source/traits.rs` - Added `StreamSource` impl for `BoxedSource` to enable dynamic dispatch
