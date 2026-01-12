# Map/List Builder Fix for Complex Inner Types

## Context

When running `poe bench-compare-quick`, the `large_complex` benchmark scenario (1M records with nested structs/arrays/maps) failed with:

```
pyo3_runtime.PanicException: not yet implemented: Avro maps are mapped to MapArrays
```

## Root Cause

The schema mapping was correct - `src/convert/arrow.rs` correctly maps Avro Map to `List<Struct{key: String, value: T}>`. The problem was in the builder implementation in `src/reader/record_decoder.rs`.

Both `ListBuilder::finish()` and `MapBuilder::finish()` used `ListPrimitiveChunkedBuilder::<Int64Type>` which only supports **numeric primitive** inner types with matching type parameter. Despite the name suggesting it works with "primitives", it fails for:
- `String` inner type (arrays of strings)
- `Struct` inner type (maps, which are `List<Struct{key, value}>`)
- Mismatched numeric types (e.g., using `Int64Type` builder for `Int32` data)

## Solution

Created a `ListBuilderKind` enum that wraps type-specific builders:
- For numeric types (Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64): use the optimized `ListPrimitiveChunkedBuilder<T>` with the correct type parameter
- For all other types (String, Binary, Struct, List, etc.): use `AnonymousOwnedListBuilder`

The builder type is determined once at schema parse time (in `ListBuilder::new()`) based on the inner element's DataType, so there's no runtime overhead per batch.

For `MapBuilder`, we always use `AnonymousOwnedListBuilder` since maps always have `Struct{key, value}` as inner type.

## Key Insight

`ListPrimitiveChunkedBuilder<T>` requires the type parameter `T` to match the actual data type. You can't use `ListPrimitiveChunkedBuilder<Int64Type>` for Int32 data - it will fail with a type mismatch error.

## Files Changed

- `src/reader/record_decoder.rs`:
  - Added import for `AnonymousOwnedListBuilder`
  - Added `ListBuilderKind` enum with type-specific variants
  - Updated `ListBuilder` to store `inner_dtype` and create the right builder kind
  - Updated `MapBuilder::finish()` to use `AnonymousOwnedListBuilder`

## Testing

- All existing tests pass
- Arrays of ints use optimized primitive builder
- Arrays of strings use general builder
- Maps inside records correctly decode as `List(Struct{key, value})`
- The `large_complex` benchmark scenario now works
