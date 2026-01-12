# Devnote: Fix parse_avro_schema panic with Enum types

## Context

When calling `jetliner.scan()` or `jetliner.parse_avro_schema()` on Avro files containing map types (which internally use Enum for categorical keys), the library panicked with:

```
pyo3_runtime.PanicException: activate dtype
```

## Root Cause

The `pyo3-polars` crate's `PySchema` type cannot serialize `DataType::Enum` with `FrozenCategories` to Python. When the Polars schema contained an Enum type (created from Avro map key types), the automatic serialization failed.

## Solution

Implemented a custom `dtype_to_py()` function in `src/python/reader.rs` that manually constructs Python polars DataType objects by calling the polars Python module directly, bypassing `pyo3-polars`' automatic serialization.

Key implementation details:

1. **schema_to_py()**: Converts a Polars Schema to a Python `polars.Schema` object by iterating fields and calling `dtype_to_py()` for each.

2. **dtype_to_py()**: Handles all DataType variants:
   - Simple types: Returns the class attribute (e.g., `polars.Int64`)
   - Parameterized types: Calls constructors (e.g., `polars.Datetime("us", "UTC")`)
   - Complex types: Recursively converts inner types (List, Struct, Array)
   - **Enum**: Falls back to `polars.Categorical` since Enum serialization is problematic

3. **Compilation fixes**:
   - `DataType::Decimal(precision, scale)` - Both are `usize` in Polars 0.52, not `Option<usize>`
   - `DataType::Object` - Takes single `&'static str`, not two parameters; removed from match

## Testing

- `jetliner.scan('benches/data/large_complex.avro').collect()` now works (1M rows, 6 columns including nested structs/arrays/maps)
- `jetliner.parse_avro_schema()` returns correct Python schema
- All 302 Python tests pass
- All 27 Rust tests pass

## Trade-offs

- Enum types are exposed as Categorical in Python, losing the frozen categories information
- This is acceptable since the data is still correctly typed and queryable
