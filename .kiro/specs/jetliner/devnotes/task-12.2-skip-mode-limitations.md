# Task 12.2: Skip Mode Record-Level Limitation

## Context

Task 12.2 implements resilient reading (skip mode) per Requirements 7.1-7.4. The design calls for skipping bad blocks and bad records within blocks.

## Decision: Stop Block Processing on First Bad Record

**Problem:** Avro binary encoding is variable-length and self-describing. Records are concatenated without delimiters - the only way to find record boundaries is to decode each field sequentially.

**Decision:** When a record fails to decode in skip mode, stop processing the current block entirely rather than attempting to skip to the next record.

**Rationale:**
1. Without record boundary markers, we cannot reliably find where the next record starts
2. Attempting to scan forward would likely produce garbage data or cascade failures
3. Block-level recovery via sync markers is reliable - Avro's 16-byte sync markers are designed for exactly this purpose
4. The trade-off (losing remaining records in a corrupted block) is acceptable given the alternative (unreliable data)

**Implementation:**
- `DataFrameBuilder.add_block()` breaks out of the record loop on first decode error
- `PrefetchBuffer` handles block-level errors by seeking to next sync marker
- Both error types are tracked and aggregated in `AvroStreamReader.errors()`

## Affected Code

- `src/convert/dataframe.rs`: `add_block()` method, line ~244
- `src/reader/buffer.rs`: `next()` and `fetch_and_decompress()` methods

## Future Consideration

If record-level skip becomes critical, options include:
1. Schema-aware skip functions that can compute field sizes without full decode
2. Heuristic scanning for valid varint patterns (unreliable)
3. Requiring writers to use smaller blocks for better granularity
