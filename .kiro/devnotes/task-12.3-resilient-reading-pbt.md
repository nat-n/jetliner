# Task 12.3: Resilient Reading Property Test Discovery

## Context

Property test for resilient reading (Property 10) was implemented to validate Requirements 7.1, 7.2, 7.3 - that the reader can skip corrupted blocks and continue reading valid data.

## Discovery: seek_to_sync Doesn't Work for Error Recovery

**Problem:** Two of four property tests failed:
- `prop_resilient_reading_skips_bad_blocks`
- `prop_resilient_reading_multiple_corruptions`

**Root Cause:** The existing `seek_to_sync(position)` method was being called with `current_offset` after an InvalidSyncMarker error. However, at that point:
1. The block data has already been read
2. The 16 bytes we read as "sync marker" didn't match
3. `current_offset` now points AFTER the bad sync marker
4. Searching from `current_offset` finds nothing because we're already past the corruption

**Solution:** Added new method `skip_to_next_sync()` to the design (see design.md "Sync Marker Recovery" section). This method:
- Starts searching from `current_offset + 1` (skip past current position)
- Scans forward for the file's correct sync marker
- Returns bytes skipped for logging

## Outcome

- Task 12.5 created to implement the fix
- Design document updated with `skip_to_next_sync()` specification
- Demonstrates value of PBT for catching subtle state-dependent bugs

## Files

- `tests/property_tests.rs`: Property tests (2 passing, 2 failing pending 12.5)
- `src/reader/block.rs`: Will need `skip_to_next_sync()` implementation
- `src/reader/buffer.rs`: Will need to use new method in error recovery
