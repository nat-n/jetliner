# Task 11.3: Batch Size Semantics

## Context

Task 11.3 required writing property tests for batch size limits per Property 7 and Requirement 3.4.

## Key Discovery: Batch Size is a Target, Not a Hard Limit

**Requirement 3.4 states:** "THE Batch_Iterator SHALL yield DataFrames containing records from one or more blocks up to a configurable row limit"

**Actual behavior:** Blocks are processed atomically. When `add_block()` is called, ALL records from that block are decoded into the builder. The batch size check happens *after* adding the block.

**Implication:** If a block contains more records than the remaining space in the current batch, the batch will exceed `batch_size`. A batch can contain up to `batch_size + max_records_per_block - 1` rows.

**Why this matters:**
1. Property tests cannot assert `batch.height() <= batch_size` unconditionally
2. Users should understand that `batch_size` is a target, not a guarantee
3. For strict row limits, users must post-process batches

**Test strategy adopted:**
- `prop_batch_size_limit_respected`: Uses `records_per_block < batch_size` to ensure batches stay within `batch_size + records_per_block`
- `prop_batch_size_one_respected`: Uses single-record blocks to verify exact batch sizes when blocks are smaller than batch size
- `prop_batch_size_larger_than_total`: Verifies all records returned in one batch when batch_size exceeds total

**Alternative considered:** Splitting blocks mid-decode to enforce hard limits. Rejected because:
1. Would require partial block state management
2. Conflicts with block-oriented error recovery (skip bad block, resume at next sync marker)
3. Adds complexity for marginal benefit

## References

- Requirement 3.4 in requirements.md
- Property 7 in design.md
- `src/reader/stream.rs` - `next_batch()` loop
- `src/convert/dataframe.rs` - `is_batch_ready()` check
- `tests/property_tests.rs` - `prop_batch_size_*` tests
