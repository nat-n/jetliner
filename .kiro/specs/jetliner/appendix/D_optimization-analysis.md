# Performance Optimization Analysis: Complex Avro Types

**Date:** 2026-01-12
**Context:** Comparative benchmarking revealed that jetliner is 50-100% slower than polars-avro when reading complex Avro structures.

**Mission Critical:** Correctness and reliability take absolute precedence over performance gains.

---

## Summary

**Root Cause:** `ListBuilder::finish()` (src/reader/record_decoder.rs:1189-1208) creates a new `Series` slice for every list element, resulting in ~5M unnecessary slice operations for 1M complex records.

**Solution:** Three optimizations in order of increasing risk/reward:
1. **Pre-allocate builders** (Low Risk, ~5-15% gain) - Safe to implement
2. **Reuse builders across batches** (Medium Risk, ~10-20% gain) - Requires careful state management
3. **Eliminate slice-per-element** (High Risk, ~30-60% gain) - Needs extensive validation

---

## Why polars-avro is Faster

**polars-avro:** Uses `MutableListArray` with integrated offset management - directly appends to list without intermediate slices.

**jetliner:** Two-phase: decode all items → slice each element → append slices. Creates new `Series` for every list element.

**Impact on large_complex (1M records):**
- `contacts` array (0-3 records/row): ~1.5M slice operations
- `tags` array (0-5 strings/row): ~2.5M slice operations
- `metadata` map (0-3 entries/row): ~1M slice operations
- **Total: ~5M unnecessary slices**

---

## The Bottleneck

**Location:** `src/reader/record_decoder.rs:1189-1208` - `ListBuilder::finish()`

**Problem:**
```rust
for window in offsets.windows(2) {
    let slice = inner_series.slice(start, len);  // New Series per list element
    builder.append_series(&slice)?;
}
```

Each `slice()` creates a new `Series` (Arc-wrapped, metadata overhead). For arrays of records, also forces struct materialization.

**Overhead by type:**
- Arrays of primitives: 20-30%
- Arrays of records: 50-100%
- Maps: 30-40%

---

## Optimization 1: Pre-allocate Builders [LOW RISK] ⭐ START HERE

**Approach:** Add `reserve(additional: usize)` method to `FieldBuilder` enum. Call it when Avro block counts are known.

**Key points:**
- Avro blocks include item counts - use as capacity hints
- Pre-allocate in `ListBuilder::decode()` when block count is decoded
- Pre-allocate in `FullRecordDecoder::new()` with expected batch size

**Risk:** None - `reserve()` is just a hint. Builders grow automatically if wrong.

**Verification:** Run existing tests (must pass unchanged), benchmark for 5-15% improvement.

**Expected gain:** 5-15% from reduced reallocation

---

## Optimization 2: Reuse Builders Across Batches [MEDIUM RISK]

**Approach:** Add `reset()` method to `FieldBuilder` that clears data but retains allocated capacity. Call between batches instead of dropping and recreating builders.

**Critical correctness requirements:**
- **Must reset ALL state** in every builder variant (offsets, counts, validity, etc.)
- **Must handle nested builders recursively** (List<Struct>, List<List>, etc.)
- Missing any state → data from previous batch leaks into next batch

**Example of what needs resetting:**
```rust
FieldBuilder::List(b) => {
    b.offsets.clear();
    b.offsets.push(0);  // Always starts with 0
    b.current_offset = 0;
    b.inner.reset();  // MUST recursively reset
}
```

**Risk:** Data corruption if reset is incomplete.

**Verification required:**
- Unit test each FieldBuilder variant's reset()
- Test nested builders (List<Struct>, List<List>, Struct with List fields)
- Test with nulls/validity bitmaps
- Property test: reset → build → verify only new data present
- Integration test: run full benchmark suite, verify exact match with/without reset

**Expected gain:** 10-20% from reduced allocation overhead

---

## Optimization 3: Eliminate Slice-Per-Element [HIGH RISK] ⚠️ HIGHEST IMPACT

**Problem:** For each list in the column, current code creates a new `Series` slice:
```rust
for window in offsets.windows(2) {
    let slice = inner_series.slice(start, len);  // New Arc<Series> + metadata
    builder.append_series(&slice)?;
}
```

**Solution:** Build lists directly from value array and offsets without creating intermediate Series objects.

**Approach:**
1. Try optimized path for primitive types (Int64, Float64, etc.)
2. Fall back to safe slicing for complex types or uncertain cases
3. Start with simplest case: single-chunk, no nulls, Int64 only
4. Gradually expand to more types after thorough testing

**Key implementation points:**
- Use `cont_slice()` to get direct access to values array (avoids Series overhead)
- Use `ListPrimitiveChunkedBuilder::append_slice()` to append array slices directly
- Handle validity bitmaps correctly for nullable columns
- Conservative fallback: multi-chunk, unknown types, or any error → use safe slicing

**Critical correctness risks:**

1. **Multi-chunk Series:** `cont_slice()` returns `None` if not single chunk → must check `n_chunks() == 1`

2. **Null handling:** Must slice validity bitmap alongside values, or lose null information

3. **Type safety:** Downcast to specific type may fail → propagate errors, fall back on failure

4. **Offset errors:** Off-by-one in offset indexing → wrong array lengths or panics

5. **Strings:** Variable-length UTF-8 adds complexity → defer optimization, use slicing initially

**Implementation strategy:**
- Start with Int64 only, no nulls, single chunk (simplest case)
- Add extensive unit + property tests comparing optimized vs slicing
- Gradually expand: Int32/Float64 → null support → more types
- Always have safe fallback path for uncertain cases

**Verification required:**
- Unit tests: each primitive type, with/without nulls, edge cases (empty, single-element, large lists)
- Property tests: random lists of varying lengths with/without nulls, verify optimized == slicing
- Integration: run full benchmark suite with optimization on/off, verify exact match
- Stress test: 10M+ records, deeply nested arrays, mixed null patterns

**Expected gain:** 30-60% on complex scenarios

---

## Implementation Guidance for Agents

**For Optimization 1 (Pre-allocate):**
- Safe to implement in one session
- Add `reserve()` method to all `FieldBuilder` variants
- Call from `ListBuilder::decode()` when block count is known
- Verify existing tests still pass

**For Optimization 2 (Reset builders):**
- Implement in one session with careful attention to completeness
- For each `FieldBuilder` variant, identify ALL state that needs clearing
- Test reset by: build batch 1 → finish → reset → build batch 2 → verify batch 2 has only new data
- Property test with random data to catch state leakage

**For Optimization 3 (Eliminate slicing):**
- Most complex - consider multiple sessions
- Start with minimal viable case: Int64, no nulls, single chunk
- Implement fallback first, then add optimization
- Extensive testing before expanding to more types
- Never commit without proving correctness via tests

**Testing approach:**
- Write tests FIRST that verify correctness
- Tests should compare new implementation against current (known-good) implementation
- Use property testing for confidence with random inputs
- Integration test must verify benchmarks produce identical results

---

## References

### Internal
- `/Users/nat.noordanus/jetstream/src/reader/record_decoder.rs` - Current implementation
- `/Users/nat.noordanus/jetstream/benches/python/compare.py` - Benchmark suite
- `/Users/nat.noordanus/jetstream/benches/python/generate_data.py` - Test data generation

### External
- [Polars arrow2 avro reader](https://github.com/jorgecarleitao/arrow2/tree/main/src/io/avro)
- [Apache Arrow Avro implementation](https://github.com/apache/arrow-rs/tree/master/arrow-avro)
- [Apache Avro specification](https://avro.apache.org/docs/current/spec.html)
- [Polars ListChunkedBuilder API](https://docs.rs/polars/latest/polars/)

---

## Appendix A: Benchmark Data Schema

The `large_complex` schema that exposed this performance issue:

```json
{
  "type": "record",
  "name": "LargeComplexRecord",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {
      "name": "address",
      "type": {
        "type": "record",
        "name": "Address",
        "fields": [
          {"name": "street", "type": "string"},
          {"name": "city", "type": "string"},
          {"name": "zip", "type": "string"},
          {
            "name": "coordinates",
            "type": ["null", {
              "type": "record",
              "name": "Coordinates",
              "fields": [
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"}
              ]
            }]
          }
        ]
      }
    },
    {
      "name": "contacts",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Contact",
          "fields": [
            {"name": "type", "type": "string"},
            {"name": "value", "type": "string"},
            {"name": "primary", "type": "boolean"}
          ]
        }
      }
    },
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "metadata", "type": {"type": "map", "values": "string"}},
    {"name": "score", "type": ["null", "double"]}
  ]
}
```

**Key Performance Factors:**
- 1M records
- `contacts`: 0-3 records per row → ~1.5M array elements → 1.5M slices
- `tags`: 0-5 strings per row → ~2.5M array elements → 2.5M slices
- `metadata`: 0-3 entries per row → ~1M array elements → 1M slices
- **Total: ~5M slice operations that optimization eliminates**

---

## Document History

- **2026-01-12:** Initial analysis based on comparative benchmark findings
- **Author:** Claude (assisted by human review)
- **Status:** Draft - awaiting implementation
- **Priority:** High (performance), Critical (correctness)
