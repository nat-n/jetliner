# Performance Fix: Direct ListArray Construction

**Date:** 2026-01-12
**Context:** Performance regression investigation on `large_complex` benchmark

## Problem

The `large_complex` benchmark showed jetliner at ~2.9s vs polars_avro at ~1.7s (70% slower). Root cause was the slice-per-element pattern in `ListBuilder::finish()` and `MapBuilder::finish()`.

With 1M records containing:
- `tags`: 0-5 strings per row (~2.5M elements)
- `contacts`: 0-3 structs per row (~1.5M elements)
- `metadata`: 0-3 map entries per row (~1M elements)

This created ~5M `Series::slice()` calls, each creating an Arc-wrapped Series with metadata overhead.

## Solution

Replace slice-per-element with direct `ListArray` construction using `polars_arrow`:

```rust
use polars_arrow::array::ListArray;
use polars_arrow::offset::Offsets;

// Instead of:
for window in offsets.windows(2) {
    let slice = inner_series.slice(start, len);
    builder.append_series(&slice)?;
}

// Now:
let inner_chunks = inner_series.to_arrow(0, CompatLevel::newest());
let list_dtype = ListArray::<i64>::default_datatype(inner_chunks.dtype().clone());
let list_arr = ListArray::<i64>::new(
    list_dtype,
    unsafe { Offsets::new_unchecked(offsets).into() },
    inner_chunks,
    None,
);
let list_chunked = ListChunked::with_chunk(name, list_arr);
```

## Key Insight

The pattern was discovered in `polars_core::series::ops::reshape::implode()` which constructs a ListArray directly from offsets and values. The `polars_arrow` crate (added as dependency) provides the necessary `ListArray` and `Offsets` types.

## Results

| Metric         | Before     | After      | Improvement |
| -------------- | ---------- | ---------- | ----------- |
| jetliner_scan  | 2.9s       | 1.13s      | 2.6x faster |
| vs polars_avro | 70% slower | 53% faster | -           |

## Safety

The `Offsets::new_unchecked()` is safe because:
1. Offsets start at 0 (initialized in `ListBuilder::new()`)
2. Offsets are monotonically increasing (only incremented during decode)
3. Final offset equals inner array length (validated by construction)
