# Enum Schema Evolution with Default Symbol

## Context

The Avro 1.12 specification allows enum schemas to have a `default` symbol for schema evolution. When a writer schema has symbols not present in the reader schema, the reader should use its default symbol instead of erroring.

## Decision

Implemented `decode_enum_with_resolution` function in `src/reader/decode.rs` that:

1. Decodes the enum index using the writer schema's symbol count
2. Looks up the symbol in the writer schema
3. Attempts to find that symbol in the reader schema
4. If not found, uses the reader's default symbol (if specified)
5. Errors if symbol not found and no default is specified

## Key Implementation Details

- The function returns the **reader's** index for the symbol, not the writer's index
- This is important because the reader's symbol ordering may differ from the writer's
- Symbol reordering between schemas is handled correctly (e.g., writer has [BLUE, GREEN, RED], reader has [RED, GREEN, BLUE])
- The default symbol must exist in the reader's symbol list (validated at decode time)

## Test Coverage

Added tests in:
- `src/reader/decode.rs`: Unit tests for `decode_enum_with_resolution`
  - Same symbols
  - Reordered symbols
  - Unknown symbol with default
  - Unknown symbol without default (error case)
  - Writer index out of range (error case)
  - Reader subset scenario

- `src/schema/reader_writer_resolution.rs`: Integration tests
  - Enum field default in record resolution
  - Full enum schema evolution with default symbol
  - Error case without default

## Performance Considerations

- The resolution function uses `symbol_index()` which is O(n) for symbol lookup
- For hot paths with many enum values, consider caching the symbol mapping
- Current implementation prioritizes correctness over micro-optimization

## Spec Reference

Avro Specification - Schema Resolution - Enums:
> if the writer's symbol is not present in the reader's enum and the reader has a default value, then that value is used, otherwise an error is signalled.
