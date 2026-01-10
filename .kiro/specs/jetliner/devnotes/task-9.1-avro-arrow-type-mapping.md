# Task 9.1: Avro to Arrow Type Mapping

## Context

Implementing type conversion from Avro schemas to Polars/Arrow data types in `src/convert/arrow.rs`.

## Decisions

### 1. Recursive Named Types → String (JSON)

**Problem:** Avro supports recursive types via named references (e.g., a `Node` record containing a field of type `Node`). Arrow/Polars doesn't natively support recursive data structures.

**Decision:** Map `AvroSchema::Named` references to `DataType::String`, with the expectation that recursive values will be JSON-serialized.

**Rationale:**
- Arrow's type system is columnar and doesn't support self-referential types
- JSON serialization preserves arbitrary nesting depth
- Alternative (flattening) would lose structural information

### 2. Avro Duration → Duration(Microseconds)

**Problem:** Avro duration is a fixed[12] containing three little-endian u32s: months, days, milliseconds. Arrow/Polars `Duration` is a single time unit (e.g., microseconds).

**Decision:** Map to `Duration(Microseconds)`. The actual conversion of months/days to a single duration value must happen at decode time, not type mapping.

**Trade-off:** This is inherently lossy—months and days have variable lengths. The decoder (task 8.3) preserves the raw components in `AvroValue::Duration { months, days, milliseconds }` for downstream handling.

### 3. Complex Unions → Error

**Problem:** Avro unions like `["string", "int", "null"]` have multiple non-null types. Arrow has a Union type, but Polars support is limited.

**Decision:** Return `SchemaError::UnsupportedType` for unions with >1 non-null variant. Simple nullable unions (`["null", T]`) map to the base type T.

**Rationale:**
- Polars Union support is experimental/incomplete
- Most real-world Avro uses nullable unions, not multi-type unions
- Clear error is better than silent data loss

### 4. Polars 0.52 Enum API

**Note:** Polars 0.52 changed the Enum type construction:
- Use `FrozenCategories::new(symbols.iter())` to create category set
- Use `DataType::from_frozen_categories(categories)` to create the type
- Previous `DataType::Enum(Arc<RevMapping>, _)` pattern no longer works

**Location:** `avro_to_arrow()` match arm for `AvroSchema::Enum`
