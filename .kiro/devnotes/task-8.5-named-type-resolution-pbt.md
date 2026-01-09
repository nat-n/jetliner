# Task 8.5: Named Type Resolution Property Test

## Context

Property 6 tests that named type references (e.g., `AvroSchema::Named("com.example.User")`) are correctly resolved to their actual definitions during schema resolution.

## Decision: Semantic Equality vs JSON String Equality

### Problem

The `prop_resolved_schema_round_trip` test was failing because it compared JSON strings directly:

```
Left:  {"name":"duration","size":12,"type":"fixed"}
Right: {"name":"duration","namespace":"_UkIOs5_D1u0_","size":12,"type":"fixed"}
```

### Root Cause

Avro namespace inheritance: when a named type (enum, fixed) is nested inside a record and has no explicit namespace, it inherits the enclosing record's namespace. This happens during parsing, not serialization.

1. Original schema: nested fixed has `namespace: None`
2. Serialize to JSON: no namespace field emitted
3. Parse JSON: parser inherits enclosing record's namespace
4. Result: parsed schema has explicit namespace, original didn't

Both are semantically equivalent per Avro spec, but JSON strings differ.

### Solution

Added `schemas_semantically_equal()` helper that compares schema structure while ignoring namespace differences on nested named types. Compares:
- Type discriminant
- Names (without namespace qualification)
- Field structure recursively
- Symbols, sizes, logical types

Location: `tests/property_tests.rs`, end of file.

## Related

- `src/schema/parser.rs`: namespace inheritance logic in `parse_record_schema()`
- `src/schema/resolution.rs`: `SchemaResolutionContext` builds named type registry
