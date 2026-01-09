# Task 2.1: JSON Schema Parser Implementation

## Context

Implementing the Avro JSON schema parser required handling several edge cases not fully specified in the task description.

## Decisions

### 1. Recursive Type Support via Pre-registration

**Problem:** Avro schemas can be self-referential (e.g., a `LinkedList` record with a `next` field of type `LinkedList`).

**Decision:** Register named types in the `named_types` HashMap *before* parsing their fields, using a placeholder `Named(fullname)`. This allows field type resolution to find the type even during its own definition.

**Location:** `src/schema/parser.rs`, `parse_record_schema()`

### 2. Unknown Logical Types Return Base Type

**Problem:** The Avro spec says unknown logical types should be ignored, but doesn't specify the exact behavior.

**Decision:** When encountering an unknown `logicalType`, return the base type without wrapping it in `LogicalType`. This matches the spec's intent that readers should be able to process data even if they don't understand a logical type annotation.

**Location:** `src/schema/parser.rs`, `parse_logical_type()` match arm for `_other`

### 3. Namespace Inheritance for Nested Types

**Problem:** Nested named types (records/enums/fixed inside records) should inherit the enclosing namespace if not explicitly specified.

**Decision:** Track `current_namespace` in the parser state. When parsing nested types, inherit from parent if no explicit namespace. Restore previous namespace after parsing nested type to handle deeply nested structures correctly.

**Location:** `src/schema/parser.rs`, namespace handling in `parse_record_schema()`

### 4. Module Structure

**Decision:** Split schema code into `src/schema/{mod.rs, types.rs, parser.rs}` rather than a single file. This separates type definitions from parsing logic and prepares for the pretty-printer (task 2.2) to be added cleanly.

## Test Coverage

44 tests covering primitives, complex types, logical types, named references, namespace resolution, and error cases.
