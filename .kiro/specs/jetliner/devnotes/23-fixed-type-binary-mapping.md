# DevNote: Fixed Type Mapped to Binary (not Array)

## Context

Task 23 implemented support for Avro `fixed` type. The design document specified:
```
| fixed     | FixedSizeBinary               | Binary       |
```

## Decision

Map Avro `fixed` directly to `DataType::Binary` instead of `DataType::Array(UInt8, size)`.

### Why Not Array?

The original implementation used `DataType::Array(Box::new(DataType::UInt8), fixed.size)` which is semantically correct (fixed-size array of bytes). However, this caused a panic in pyo3-polars during schema conversion:

```
thread '<unnamed>' panicked at polars-core/src/datatypes/mod.rs:357:1:
not implemented
```

The panic occurs in pyo3-polars when converting the Polars schema to Python. The `Array` type's schema conversion path isn't fully implemented for the Rust→Python FFI boundary.

### Why Not FixedSizeBinary?

`FixedSizeBinary(usize)` exists in `polars_arrow::datatypes::ArrowDataType` but:
1. Polars doesn't expose it as a user-facing `DataType` variant
2. There's no `FixedSizeBinaryChunked` type to construct Series from
3. The Python API only exposes `pl.Binary`, not `pl.FixedSizeBinary`

### Why Binary Works

`DataType::Binary` is:
- Fully supported in pyo3-polars FFI
- What users see in Python regardless of Arrow-level type
- Semantically appropriate (fixed is just binary data with a size constraint)

Size validation happens at decode time via `decode_fixed(data, size)` which reads exactly N bytes and fails if insufficient data is available.

## Implementation

Changed in `src/convert/arrow.rs`:
```rust
AvroSchema::Fixed(_fixed) => Ok(DataType::Binary)
```

Changed `FixedBuilder` in `src/reader/record_decoder.rs` to use `MutableBinaryViewArray` (same as `BinaryBuilder`) instead of reshaping to Array.

## Tradeoffs

| Aspect          | Array Approach | Binary Approach          |
| --------------- | -------------- | ------------------------ |
| Type safety     | Size in type   | Size validated at decode |
| pyo3-polars     | Panics         | Works                    |
| User visibility | `array[u8, N]` | `binary`                 |
| Complexity      | Higher         | Lower                    |

## Verification

All fixed type tests pass including:
- Multiple sizes (1 byte to 1024 bytes)
- Multiple columns with different sizes
- Projection pushdown
- Nested records
- Nullable fixed (union with null)
- Multi-batch streaming

## Future

If pyo3-polars adds support for `Array` type schema conversion, we could revisit using `DataType::Array(UInt8, size)` for better type-level size information. Monitor polars-core and pyo3-polars for updates.
