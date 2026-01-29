# Unknown Logical Type Preservation

## Context

The Avro specification states that unknown logical types should be "ignored" and the data treated as the underlying base type. However, there are two ways to interpret "ignored":

1. **Discard entirely**: Return only the base schema, losing the logical type name
2. **Preserve for inspection**: Keep the logical type wrapper but treat data as base type

## Decision

We chose option 2: preserve unknown logical types as `LogicalTypeName::Unknown(String)`.

## Rationale

- **Schema fidelity**: Users can inspect `schema_dict` and see the original logical type name
- **Forward compatibility**: If a newer Avro version adds a logical type, files written with it can still be read and the type name is visible
- **Zero runtime cost**: The `Unknown` variant adds no overhead to data decoding - it simply delegates to the base type
- **No API changes**: Existing code continues to work; the schema inspection API naturally exposes the preserved name

## Implementation

- `LogicalTypeName::Unknown(String)` variant added to preserve custom type names
- Parser creates `Unknown` variant instead of returning bare base schema
- Arrow converter delegates to base type for `Unknown` variants
- Record decoder creates builder for base type when encountering `Unknown`
- `schema_dict` serialization includes the `logicalType` field with the custom name

## Files Changed

- `src/schema/types.rs`: Added `Unknown(String)` variant, `is_unknown()` helper
- `src/schema/parser.rs`: Changed unknown handling to preserve as `LogicalType`
- `src/convert/arrow.rs`: Added `Unknown(_)` arm delegating to base type
- `src/reader/record_decoder.rs`: Added `Unknown(_)` arm for builder creation
- `src/reader/decode.rs`: Added `Unknown(_)` arm in `decode_logical_value()`
- `tests/property_tests.rs`: Added `Unknown(_)` arm in value generator
- `tests/schema_tests.rs`: Updated test to verify preservation behavior
- `python/tests/types/test_logical_types.py`: Added 7 tests for unknown types
