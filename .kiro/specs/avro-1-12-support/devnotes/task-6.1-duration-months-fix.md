# Task 6.1: Duration Type Test - Months Handling Fix

## Context

While implementing the Python integration test for the duration logical type (Task 6.1), the tests revealed a bug in the existing Rust implementation.

## Issue Discovered

The `DurationBuilder::decode` method in `src/reader/record_decoder.rs` was not including the months component in the duration calculation. The code had:

```rust
let _months = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
// ... months was unused
let total_micros = (days as i64 * 24 * 60 * 60 * 1_000_000) + (milliseconds as i64 * 1_000);
```

This violated Requirement 2.3: "THE Jetliner SHALL approximate months as 30 days for the conversion."

## Fix Applied

Updated the decode method to include months approximated as 30 days:

```rust
let months = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
// ...
let months_as_days = months as i64 * 30;
let total_micros = ((months_as_days + days as i64) * 24 * 60 * 60 * 1_000_000)
    + (milliseconds as i64 * 1_000);
```

## Rationale

The Avro specification defines duration as a fixed[12] containing three unsigned 32-bit integers (months, days, milliseconds). Since Polars Duration type doesn't natively support months, the standard approach is to approximate months as 30 days, which is consistent with the Avro spec recommendation and Requirement 2.3.

## Tests Added

Seven tests were added to `python/tests/types/test_logical_types.py`:

1. `test_read_duration_file` - Verifies file can be read
2. `test_duration_dtype` - Verifies Duration(us) dtype
3. `test_duration_milliseconds_only` - Tests milliseconds-only duration
4. `test_duration_days_only` - Tests days-only duration
5. `test_duration_months_approximated_as_30_days` - Tests months approximation
6. `test_duration_all_components` - Tests combined months/days/milliseconds
7. `test_duration_larger_values` - Tests larger duration values

All tests pass after the fix.
