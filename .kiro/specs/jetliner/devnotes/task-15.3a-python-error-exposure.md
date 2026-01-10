# Task 15.3a: Python Error Exposure Implementation

## Context

Implementing structured error exposure for Python required bridging Rust's async error collection with PyO3's borrowing rules.

## Decision: Error Collection Timing in `__next__`

**Problem:** Errors accumulate in the Rust `AvroStreamReader` during iteration. When iteration ends (`Ok(None)`), we need to:
1. Collect errors from the reader
2. Release the reader (set `inner` to `None`)
3. Store errors in the Python object's `errors` field

However, `PyRefMut<'_, Self>` cannot be mutated inside an async block while also accessing `self.runtime.block_on()`.

**Decision:** Split the operation into two phases:
1. Inside the async block: collect errors into a `Vec<PyReadError>` *before* releasing the reader
2. Outside the async block: store the collected errors in a separate `block_on()` call

**Implementation:**
```rust
// Phase 1: Collect errors before releasing reader
let result = slf.runtime.block_on(async {
    // ...
    Ok(None) => {
        let collected_errors = reader.errors().iter()
            .map(PyReadError::from_read_error).collect();
        *guard = None;  // Release reader
        Ok((None, Some(collected_errors)))
    }
});

// Phase 2: Store errors outside async block
if let Ok((None, Some(ref collected_errors))) = result {
    let errors_to_store = collected_errors.clone();
    slf.runtime.block_on(async {
        *errors_arc.lock().await = errors_to_store;
    });
}
```

**Why not collect errors lazily?** Once the reader is released (`*guard = None`), the errors are gone. We must capture them at the moment of iteration completion.

**Location:** `src/python/reader.rs`, `__next__` methods in both `AvroReaderCore` and `AvroReader`
