# Functional Coverage Gap Analysis

**Date**: 2026-01-09 (Revised)
**Purpose**: Identify gaps in testing for functionality, correctness, and robustness before performance benchmarks

## Executive Summary

After thorough review of requirements, design properties, existing tests, and test data, I've identified **3 critical gaps**, **2 moderate gaps**, and **1 nice-to-have improvement**.

**Current Test Status**:
- 511 Rust tests passing (355 unit + 59 property + 70 schema + 27 spec compliance)
- 166 Python tests passing
- 16 Python tests XFAIL (known/documented limitations)

**Key Insight**: Property tests with synthetic data provide strong correctness guarantees for type handling. The Avro binary format is well-specified and simple, so types that pass property tests are very likely to work with real files. E2E tests with real files are primarily valuable for:
1. Validating the full streaming pipeline (multi-block handling with real file I/O)
2. Testing error recovery with real corruption patterns
3. Proving interoperability claims (marketing/confidence)

---

## Gap Analysis by Category

### CRITICAL GAPS (Should Fix Before 1.0)

#### Gap 1: No Multi-Block File Testing

**Problem**: All test files are small (< 1KB) with single blocks. No validation that:
- Sync markers work correctly across multiple blocks
- Memory stays bounded with large files
- Block boundary handling is correct with real file I/O

**Why Critical**: Property tests exercise the decoding logic but don't test the full streaming pipeline with real file I/O, block prefetching, and sync marker validation across actual block boundaries.

**Requirements Affected**: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 8.2

**Recommendation**: Generate large test file (10K+ records) and add tests for:
- Correct record count across blocks
- Memory doesn't grow unbounded
- Batch size respected across block boundaries

---

#### Gap 2: Property 14 (Projection) and Property 15 (Early Stopping) Not Implemented

**Problem**: Design document specifies Property 14 and Property 15, but these property tests don't exist in `tests/property_tests.rs`.

**Property 14**: "For any Avro file and any subset of columns, reading with projection SHALL produce a DataFrame containing exactly those columns with the same values as reading all columns and then selecting."

**Property 15**: "For any Avro file and any row limit N, reading with `n_rows=N` SHALL produce at most N rows, and those rows SHALL be the first N rows of the file."

**Why Critical**: These are specified in the design document as correctness properties. Missing them means the design spec is incomplete.

**Requirements Affected**: 6a.2, 6a.4

**Recommendation**: Implement these property tests.

---

#### Gap 3: No Error Recovery Testing with Actual Corrupted Files

**Problem**: Error handling tests only use valid files. Skip mode recovery is tested via property tests with synthetic corruption, but never with real corrupted files.

**What's Missing**:
- File with invalid magic bytes
- Truncated file (EOF mid-block)
- File with corrupted sync marker
- File with corrupted compressed data
- File with invalid record data

**Why Critical**: Property tests use synthetic corruption patterns. Real-world corruption may have different characteristics that expose edge cases in the recovery logic.

**Requirements Affected**: 7.1, 7.2, 7.3, 7.4, 7.5

**Recommendation**: Generate corrupted test files and add E2E tests for error recovery.

---

### MODERATE GAPS (Should Fix)

#### Gap 4: No S3 Integration Tests

**Problem**: S3Source is implemented but only tested via property tests for range requests. No actual S3 integration tests exist.

**Requirements Affected**: 4.2, 4.5, 4.6, 4.7

**Recommendation**: Add integration tests using localstack or minio (can be optional/CI-only).

---

#### Gap 5: No Edge Case Value Testing

**Problem**: No tests for boundary values:
- Max/min int32, int64 values
- NaN, Infinity, -Infinity for floats
- Empty strings, empty bytes, empty arrays, empty maps
- Very long strings (> 64KB)
- Unicode edge cases (emoji, RTL, combining characters)

**Requirements Affected**: 1.4, 1.5

**Recommendation**: Add edge case tests for boundary values.

---

### NICE-TO-HAVE (Lower Priority)

#### Gap 6: Schema Evolution E2E Tests

**Problem**: Schema resolution is implemented and has property tests (Property 12), but no E2E tests with real files demonstrate reading with reader schema different from writer schema.

**What Schema Evolution Covers**:
- **New fields with defaults**: Reader schema has fields the writer didn't have - use default values
- **Removed fields**: Reader doesn't want fields the writer included - skip during decode
- **Field reordering**: Reader has fields in different order - match by name, not position
- **Type promotions**: Writer used `int`, reader wants `long` - widen the value

**Current Testing**:
- Property 12 generates random writer schemas, random compatible reader schemas, and verifies resolution logic with synthetic data
- Since Avro's binary format is deterministic, if resolution works with synthetic data, it should work with real files

**Why Nice-to-Have**: Property 12 already validates schema resolution correctness. E2E tests would primarily serve as interoperability proof (files written by Java/fastavro/etc.).

**Recommended Testing Strategy** (if implemented):
- Single E2E test with one real file written by fastavro
- Test the most common case: new field with default value
- Don't need exhaustive coverage - property tests handle that

**Requirements Affected**: 9.2, 9.4

---

## Summary Table

| Gap                     | Severity     | Requirements | Effort | Priority |
| ----------------------- | ------------ | ------------ | ------ | -------- |
| 1. Multi-Block Testing  | Critical     | 3.1-3.6, 8.2 | Medium | HIGH     |
| 2. Property 14/15       | Critical     | 6a.2, 6a.4   | Medium | HIGH     |
| 3. Error Recovery E2E   | Critical     | 7.1-7.5      | Medium | HIGH     |
| 4. S3 Integration       | Moderate     | 4.2, 4.5-4.7 | Medium | MEDIUM   |
| 5. Edge Case Values     | Moderate     | 1.4, 1.5     | Low    | MEDIUM   |
| 6. Schema Evolution E2E | Nice-to-Have | 9.2, 9.4     | Medium | LOW      |

---

## Recommended New Tasks

Based on this analysis, I recommend adding the following tasks to `tasks.md`:

### Task 17.3: Multi-Block and Large File Testing

```markdown
- [ ] 17.3 Multi-block and large file testing
  - [ ] 17.3.1 Generate large test file (10K+ records)
    - Create file that spans multiple blocks
    - Include variety of record sizes
    - _Requirements: 3.1, 3.4_

  - [ ] 17.3.2 Add multi-block E2E tests
    - Verify correct total record count
    - Verify batch size respected across block boundaries
    - Verify sync markers validated between blocks
    - _Requirements: 3.1, 3.4, 1.3_

  - [ ] 17.3.3 Add memory efficiency tests
    - Verify memory doesn't grow with file size
    - Verify memory bounded by batch_size and buffer config
    - _Requirements: 8.2, 3.5, 3.6_
```

### Task 17.4: Missing Property Tests

```markdown
- [ ] 17.4 Implement missing property tests
  - [ ] 17.4.1 Implement Property 14: Projection Preserves Selected Columns
    - Generate random schemas and column subsets
    - Verify projected read equals full read + select
    - **Property 14: Projection Preserves Selected Columns**
    - **Validates: Requirements 6a.2**

  - [ ] 17.4.2 Implement Property 15: Early Stopping Respects Row Limit
    - Generate random files and row limits
    - Verify at most N rows returned
    - Verify rows are first N rows of file
    - **Property 15: Early Stopping Respects Row Limit**
    - **Validates: Requirements 6a.4**
```

### Task 17.5: Error Recovery E2E Tests

```markdown
- [ ] 17.5 Error recovery E2E tests with corrupted files
  - [ ] 17.5.1 Generate corrupted test files
    - File with invalid magic bytes
    - Truncated file (EOF mid-block)
    - File with corrupted sync marker
    - File with corrupted compressed data
    - File with invalid record data
    - _Requirements: 7.1, 7.2_

  - [ ] 17.5.2 Add skip mode recovery tests
    - Test recovery from each corruption type
    - Verify valid data before/after corruption is read
    - Verify error tracking (error_count, errors list)
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [ ] 17.5.3 Add strict mode failure tests
    - Verify immediate failure on each corruption type
    - Verify descriptive error messages
    - _Requirements: 7.5, 7.7_
```

---

## Known Limitations (Documented, Not Gaps)

These are intentional limitations that are properly documented and have helpful error messages:

1. **Array/Map as top-level schema**: Returns helpful error message suggesting workaround
2. **N-ary trees with array children**: Complex recursive structures with arrays not supported
3. **Python asyncio**: Sync-only API is intentional design decision
4. **Standalone .avsc files**: Not supported; schemas come from embedded headers in .avro files

---

## Conclusion

The codebase has excellent foundational testing with strong property test coverage that validates correctness for all type handling, codec decompression, and schema resolution. The 3 critical gaps focus on areas where property tests cannot fully substitute for E2E validation:

1. **Multi-block streaming** - Property tests don't exercise the full I/O pipeline
2. **Missing Properties 14/15** - Design spec compliance
3. **Error recovery with real corruption** - Synthetic corruption may miss real-world patterns

**Recommended Priority Order**:
1. Task 17.4 (Missing Properties) - Design document compliance, quick win
2. Task 17.3 (Multi-Block) - Real-world file handling validation
3. Task 17.5 (Error Recovery) - Robustness validation

After these 3 tasks, functional coverage will be comprehensive enough to proceed with performance benchmarks (Task 18).
