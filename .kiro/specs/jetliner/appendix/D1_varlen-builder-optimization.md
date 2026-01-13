# Performance Optimization Analysis: Variable-Length Type Builders

**Date:** 2026-01-13
**Context:** After diversifying benchmark column types to include `bytes`, the `large_wide` benchmark revealed that variable-length columns (string, bytes) are 5.5x slower per column than numeric columns.

---

## Summary

**Root Cause:** `StringBuilder` and `BinaryBuilder` use `Vec<String>` and `Vec<Vec<u8>>` respectively, causing millions of small heap allocations (one per value).

**Solution:** Use direct Arrow array construction with contiguous buffers, similar to the `ListBuilder` fix in commit 34e9288.

**Expected gain:** 40-50% reduction in variable-length column decode time.

---

## Benchmark Evidence

Profiling `large_wide` (100 columns, 1M records) with column type projection:

| Column subset | Count | Time | Per-column |
|--------------|-------|------|------------|
| Numeric only (long, int, double, float, bool) | 71 | 1.9s | 27ms |
| Variable-length only (string + bytes) | 29 | 4.3s | **148ms** |
| All columns | 100 | 4.4s | 44ms |

Variable-length columns are **5.5x slower per column** than numeric columns.

---

## The Bottleneck

**Location:** `src/reader/record_decoder.rs:861-918`

**Problem:** Each value decoded creates a separate heap allocation:

```rust
// BinaryBuilder - one Vec<u8> allocation per value
struct BinaryBuilder {
    values: Vec<Vec<u8>>,  // 1M records = 1M Vec allocations per column
}

fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
    let value = decode_bytes(data)?;  // Allocates new Vec<u8>
    self.values.push(value);
}

// StringBuilder - same pattern with String
struct StringBuilder {
    values: Vec<String>,  // 1M records = 1M String allocations per column
}
```

For `large_wide` with 29 variable-length columns: **29 million heap allocations**.

---

## Solution: Direct Arrow Array Construction

Replace per-value allocations with Arrow's native contiguous buffer approach:

**Current approach:**
```
decode → Vec<u8> allocation → push to Vec<Vec<u8>> → convert to Series
```

**Optimized approach:**
```
decode → append to single data buffer + record offset → build BinaryArray directly
```

Use `polars_arrow::array::BinaryArray` (or `Utf8Array` for strings):
- Single contiguous data buffer for all values
- Offsets array tracking where each value starts/ends
- Same pattern successfully used for `ListBuilder` fix (commit 34e9288, 2.6x speedup)

---

## Implementation Approach

### For BinaryBuilder:

```rust
struct BinaryBuilder {
    name: String,
    data: Vec<u8>,           // Single contiguous buffer
    offsets: Vec<i64>,       // Value boundaries (starts with 0)
}

fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
    let len = decode_long(data)?;
    let bytes = &data[..len as usize];
    self.data.extend_from_slice(bytes);
    self.offsets.push(self.data.len() as i64);
    *data = &data[len as usize..];
    Ok(())
}

fn finish(&mut self) -> Result<Series, DecodeError> {
    // Build BinaryArray directly from data + offsets
    // Convert to Series
}
```

### For StringBuilder:

Same pattern, but must validate UTF-8 at some point (either during decode or at finish).

---

## Verification Requirements

1. **Unit tests:** Verify identical output for all string/bytes edge cases
   - Empty values, very long values (>64KB)
   - Unicode edge cases (emoji, RTL, combining characters)
   - Mixed lengths within same column

2. **Property tests:** Random string/bytes data, verify optimized == current implementation

3. **Integration:** Run full benchmark suite, verify data integrity (existing hash-based validation)

4. **Benchmark:** Confirm 40-50% reduction in variable-length column time

---

## References

- `src/reader/record_decoder.rs:861-918` - Current StringBuilder/BinaryBuilder
- `src/reader/record_decoder.rs` ListBuilder - Reference implementation using direct array construction
- Commit 34e9288 - ListBuilder optimization achieving 2.6x speedup
- `polars_arrow::array::BinaryArray` / `Utf8Array` - Target array types

---

## Document History

- **2026-01-13:** Initial analysis based on column type profiling
- **Author:** Claude
- **Status:** Ready for implementation
