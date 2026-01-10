# Task 8.3: Logical Type Decoding

## Context

Implementing logical type decoding for Avro binary format in `src/reader/decode.rs`.

## Decisions

### 1. Decimal Representation as Raw Bytes

**Problem:** Avro decimals are stored as big-endian two's complement bytes. Converting to a Rust numeric type (e.g., `rust_decimal::Decimal` or `i128`) would require adding dependencies and handling arbitrary precision.

**Decision:** Keep decimal as raw bytes in `AvroValue::Decimal { unscaled, precision, scale }`. The conversion to Arrow `Decimal128` will happen in the Arrow/Polars integration layer (task 9.x), which is the appropriate place since Arrow has its own decimal representation.

**Rationale:**
- Avoids adding `num-bigint` or similar dependency to core decoder
- Preserves full precision without truncation concerns
- Arrow integration will need the raw bytes anyway for `Decimal128` construction

### 2. Time Value Validation

**Problem:** Avro spec doesn't explicitly require validation of time values, but invalid times (e.g., negative or > 24 hours) would cause issues downstream.

**Decision:** Added range validation:
- `time-millis`: [0, 86400000) - 24 hours in milliseconds
- `time-micros`: [0, 86400000000) - 24 hours in microseconds

**Trade-off:** Strict validation may reject data that other Avro implementations accept. If this causes interop issues, validation could be made optional via a config flag.

**Location:** `decode_time_millis()`, `decode_time_micros()`

### 3. UUID String Validation

**Problem:** UUID can be stored as string or fixed[16]. String format should be validated.

**Decision:** Only validate length (36 chars = 32 hex + 4 dashes). Full format validation (hex chars, dash positions) deferred to avoid regex dependency and performance overhead in hot path.

**Location:** `decode_uuid_string()`

### 4. LocalTimestamp Handling

**Problem:** Avro 1.10+ added `local-timestamp-millis` and `local-timestamp-micros` which are semantically different from regular timestamps (no timezone).

**Decision:** Decode identically to regular timestamps, returning `TimestampMillis`/`TimestampMicros`. The semantic difference (local vs UTC) is metadata that should be preserved in the schema, not the decoded value.

**Location:** `decode_logical_value()` match arms

### 5. Duration Byte Order

**Note:** Avro spec specifies duration as fixed[12] with three **little-endian** unsigned 32-bit integers (months, days, milliseconds). This is unusual since most Avro numeric types are big-endian or varint. Implementation uses `u32::from_le_bytes()`.

**Location:** `decode_duration()`

## AvroValue Enum Extension

Added logical type variants to `AvroValue`:
- `Decimal { unscaled, precision, scale }`
- `Uuid(String)`
- `Date(i32)`
- `TimeMillis(i32)`, `TimeMicros(i64)`
- `TimestampMillis(i64)`, `TimestampMicros(i64)`
- `Duration { months, days, milliseconds }`

This provides type-safe representation vs. falling back to base types (Int/Long/Bytes).
