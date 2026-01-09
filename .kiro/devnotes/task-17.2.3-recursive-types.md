# Task 17.2.3: Recursive Type Handling via JSON Serialization

**Date:** 2026-01-09
**Status:** Implemented

## Context

Avro supports recursive types (e.g., linked lists, trees) where a record references itself:

```json
{
  "type": "record",
  "name": "LongList",
  "fields": [
    {"name": "value", "type": "long"},
    {"name": "next", "type": ["LongList", "null"]}
  ]
}
```

Arrow/Polars does not support recursive schemas - all types must be finite and fully defined.

## Decision

Serialize recursive fields to JSON strings.

When a `Named` type reference is encountered that cannot be resolved to a concrete non-recursive type:
1. Detect it during Arrow type mapping (`src/convert/arrow.rs`)
2. Map to `DataType::String` instead of trying to build a recursive Struct
3. Use `RecursiveBuilder` to decode the full structure via `decode_value_with_context`
4. Serialize the decoded `AvroValue` to JSON via `AvroValue::to_json()`

## Alternatives Considered

1. **Flatten to fixed depth** - Rejected: arbitrary depth limit, schema explosion, data loss if depth exceeded
2. **Separate table per level** - Rejected: complex schema transformation, difficult to query/reconstruct
3. **Error on recursive types** - Rejected: reduces interoperability, Spark's approach but not user-friendly

## Implementation

Key files:
- `src/convert/arrow.rs`: `Named` types map to `DataType::String`
- `src/reader/record_decoder.rs`: `RecursiveBuilder` struct
- `src/reader/decode.rs`: `AvroValue::to_json()` method

**Threading the root schema:** `RecursiveBuilder` needs the full schema to resolve named type references:
1. Added `new_with_root(name, schema, root_schema)` to `FieldBuilder`
2. Builds `SchemaResolutionContext` from root schema
3. Uses `decode_value_with_context` which resolves `Named` references

**JSON serialization handles:**
- Bytes/Fixed → Base64 encoded strings
- Decimals → String with scale applied
- Duration → Object with months/days/milliseconds
- Unions → Unwrapped to inner value (index discarded)

## Consequences

**Pros:**
- Full data preservation (no depth limits)
- Works with any recursion pattern (linked lists, trees, graphs)
- JSON queryable with Polars `str.json_decode()`

**Cons:**
- Loss of columnar efficiency for recursive fields
- JSON parsing overhead to access nested data
- Type information embedded in JSON rather than schema

## Example Output

```
┌───────┬────────────────────────────────────────────────┐
│ value ┆ next                                           │
│ i64   ┆ str                                            │
╞═══════╪════════════════════════════════════════════════╡
│ 989   ┆ {"next":null,"value":990}                      │
│ 314   ┆ {"next":{"next":null,"value":312},"value":313} │
└───────┴────────────────────────────────────────────────┘
```

## Test Coverage

Verified with `fastavro/recursive.avro` - linked list pattern correctly serialized to JSON strings.
