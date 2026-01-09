# Task 8.4: Named Type Resolution for Decoding

## Context

Implementing schema type resolution for named types (records, enums, fixed) during Avro decoding.

## Key Architectural Insight: Two Decoding Paths

**Discovery:** During implementation, it became clear that there are two distinct decoding paths in the design, and named type resolution serves different purposes in each:

### Path 1: AvroValue Intermediate (Testing/Debug)

- `decode_value_with_context()` → `AvroValue` enum
- Uses `SchemaResolutionContext` to resolve `Named` references at decode time
- **Not on the hot path** for DataFrame loading
- Useful for: testing, debugging, schema inspection, skip functions

### Path 2: Direct Arrow Builders (Production)

- `RecordDecoder` (task 9.2) → Arrow `ArrayBuilder`s directly
- Schema resolution happens **once** before decoding starts
- Uses `SchemaResolutionContext::resolve()` to get fully resolved schema
- Builds appropriate Arrow builders based on resolved schema structure
- **This is the performance-critical path**

**Implication:** The `decode_value_with_context()` functions added in this task are not wasted work - they're needed for testing and the skip functions used in projection pushdown. But they're not what will be used for the actual DataFrame loading.

## Implementation Details

### Resolution Context Building

Two approaches available:
1. `SchemaParser::named_types()` - context built during JSON parsing
2. `SchemaResolutionContext::build_from_schema()` - context built from parsed schema

Both produce equivalent results. The parser approach is more efficient if you're parsing anyway.

### Recursive Type Handling

Self-referential types (e.g., linked lists) are handled by tracking the resolution path:
- If a `Named` reference points to a type already in the current resolution path, keep it as `Named`
- This prevents infinite recursion while preserving the schema structure

**Location:** `SchemaResolutionContext::resolve_with_path()`

## Test Coverage

Added 7 unit tests for context-based decoding:
- Basic named type resolution
- Nested named types
- Named types in unions, arrays, maps
- Unresolved reference error handling
- Skip with context

## Related

- Task 8.3 devnote discusses similar pattern for logical types (AvroValue vs Arrow)
- Task 9.2 will implement the direct Arrow path using resolved schemas
