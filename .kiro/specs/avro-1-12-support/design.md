# Design Document: Avro 1.12.0 Support

## Overview

This design document describes the implementation of Avro 1.12.0 nanosecond timestamp support and the addition of missing test coverage for existing features in the Jetliner project. The implementation follows the existing patterns established for other logical types (timestamp-millis, timestamp-micros) and extends them to support nanosecond precision.

## Architecture

The implementation touches four main components in the existing architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                        Avro File                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Schema Parser                               │
│  src/schema/parser.rs                                           │
│  - Parse "timestamp-nanos" logical type                         │
│  - Parse "local-timestamp-nanos" logical type                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Schema Types                                │
│  src/schema/types.rs                                            │
│  - Add TimestampNanos to LogicalTypeName enum                   │
│  - Add LocalTimestampNanos to LogicalTypeName enum              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Arrow Converter                              │
│  src/convert/arrow.rs                                           │
│  - Map TimestampNanos → Datetime(Nanoseconds, UTC)              │
│  - Map LocalTimestampNanos → Datetime(Nanoseconds, None)        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Record Decoder                               │
│  src/reader/record_decoder.rs                                   │
│  - Add DatetimeBuilder support for Nanoseconds TimeUnit         │
└─────────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### 1. Schema Types Extension (src/schema/types.rs)

Add two new variants to the `LogicalTypeName` enum:

```rust
pub enum LogicalTypeName {
    // ... existing variants ...
    /// Timestamp in nanoseconds since Unix epoch (Avro 1.12.0+).
    TimestampNanos,
    /// Local timestamp in nanoseconds (no timezone) (Avro 1.12.0+).
    LocalTimestampNanos,
}

impl LogicalTypeName {
    pub fn name(&self) -> &'static str {
        match self {
            // ... existing matches ...
            LogicalTypeName::TimestampNanos => "timestamp-nanos",
            LogicalTypeName::LocalTimestampNanos => "local-timestamp-nanos",
        }
    }
}
```

### 2. Schema Parser Extension (src/schema/parser.rs)

Add parsing for the new logical types in `parse_logical_type`:

```rust
fn parse_logical_type(...) -> Result<AvroSchema, SchemaError> {
    let logical_type = match logical_type_name {
        // ... existing matches ...
        "timestamp-nanos" => LogicalTypeName::TimestampNanos,
        "local-timestamp-nanos" => LogicalTypeName::LocalTimestampNanos,
        // ...
    };
}
```

### 3. Arrow Converter Extension (src/convert/arrow.rs)

Add mapping for the new logical types in `logical_to_arrow`:

```rust
fn logical_to_arrow(logical: &LogicalType) -> Result<DataType, SchemaError> {
    match &logical.logical_type {
        // ... existing matches ...
        LogicalTypeName::TimestampNanos => {
            Ok(DataType::Datetime(TimeUnit::Nanoseconds, Some(TimeZone::UTC)))
        }
        LogicalTypeName::LocalTimestampNanos => {
            Ok(DataType::Datetime(TimeUnit::Nanoseconds, None))
        }
    }
}
```

### 4. Record Decoder Extension (src/reader/record_decoder.rs)

The existing `DatetimeBuilder` already supports different `TimeUnit` values. We need to add handling in the `FieldBuilder::create_builder` match arm for the new logical types:

```rust
LogicalTypeName::TimestampNanos => Ok(FieldBuilder::Datetime(
    DatetimeBuilder::new(name, TimeUnit::Nanoseconds, Some(TimeZone::UTC)),
)),
LogicalTypeName::LocalTimestampNanos => Ok(FieldBuilder::Datetime(
    DatetimeBuilder::new(name, TimeUnit::Nanoseconds, None),
)),
```

### 5. Decode Module Extension (src/reader/decode.rs)

Add new AvroValue variants and decode functions:

```rust
pub enum AvroValue {
    // ... existing variants ...
    /// Timestamp in nanoseconds since Unix epoch
    TimestampNanos(i64),
}

pub fn decode_timestamp_nanos(data: &mut &[u8]) -> Result<AvroValue, DecodeError> {
    let nanos = decode_long(data)?;
    Ok(AvroValue::TimestampNanos(nanos))
}
```

### 6. Schema Compatibility Extension (src/schema/compatibility.rs)

Add compatibility rules for the new logical types:

```rust
(LogicalTypeName::TimestampNanos, LogicalTypeName::TimestampNanos)
| (LogicalTypeName::LocalTimestampNanos, LogicalTypeName::LocalTimestampNanos) => {
    CompatibilityResult::compatible()
}
```

### 8. Big-Decimal Schema Types (src/schema/types.rs)

Add BigDecimal variant to the LogicalTypeName enum:

```rust
pub enum LogicalTypeName {
    // ... existing variants ...
    /// Big-decimal with variable scale stored in value (Avro 1.12.0+).
    BigDecimal,
}

impl LogicalTypeName {
    pub fn name(&self) -> &'static str {
        match self {
            // ... existing matches ...
            LogicalTypeName::BigDecimal => "big-decimal",
        }
    }
}
```

### 9. Big-Decimal Schema Parser (src/schema/parser.rs)

Add parsing for big-decimal in `parse_logical_type`:

```rust
fn parse_logical_type(...) -> Result<AvroSchema, SchemaError> {
    let logical_type = match logical_type_name {
        // ... existing matches ...
        "big-decimal" => LogicalTypeName::BigDecimal,
        // ...
    };
}
```

### 10. Big-Decimal Arrow Converter (src/convert/arrow.rs)

Map big-decimal to String:

```rust
fn logical_to_arrow(logical: &LogicalType) -> Result<DataType, SchemaError> {
    match &logical.logical_type {
        // ... existing matches ...
        LogicalTypeName::BigDecimal => {
            // Map to String to preserve exact decimal representation
            // Variable scale per value prevents using Polars Decimal type
            Ok(DataType::String)
        }
    }
}
```

### 11. Big-Decimal Decoder (src/reader/decode.rs)

Add BigDecimal variant to AvroValue and decode function:

```rust
pub enum AvroValue {
    // ... existing variants ...
    /// Big-decimal value (as string to preserve exact representation)
    BigDecimal(String),
}

/// Decode a big-decimal value from bytes.
///
/// Big-decimal encoding (Avro 1.12.0+):
/// - Scale: varint (zigzag encoded)
/// - Unscaled value: big-endian two's complement bytes
pub fn decode_big_decimal(data: &mut &[u8]) -> Result<AvroValue, DecodeError> {
    let bytes = decode_bytes(data)?;
    if bytes.is_empty() {
        return Ok(AvroValue::BigDecimal("0".to_string()));
    }

    // Read scale from the first bytes (varint)
    let mut cursor = &bytes[..];
    let scale = decode_int(&mut cursor)?;

    // Remaining bytes are the unscaled value
    let unscaled_bytes = cursor;

    // Convert to decimal string
    let decimal_str = big_decimal_bytes_to_string(unscaled_bytes, scale as u32);
    Ok(AvroValue::BigDecimal(decimal_str))
}
```

### 12. Big-Decimal Record Decoder Builder (src/reader/record_decoder.rs)

Add BigDecimalBuilder for direct Arrow conversion:

```rust
/// Builder for big-decimal values (stored as strings).
struct BigDecimalBuilder {
    name: String,
    values: Vec<String>,
}

impl BigDecimalBuilder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let bytes = decode_bytes(data)?;
        let decimal_str = if bytes.is_empty() {
            "0".to_string()
        } else {
            // Read scale (varint) then unscaled value
            let mut cursor = &bytes[..];
            let scale = decode_int(&mut cursor)?;
            big_decimal_bytes_to_string(cursor, scale as u32)
        };
        self.values.push(decimal_str);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        Ok(Series::new(self.name.clone().into(), values))
    }
}
```

### 7. Schema Validation (src/schema/parser.rs)

The existing validation already handles most edge cases. We need to verify:

1. **Empty namespace components**: `validate_simple_name` should reject empty strings
2. **Invalid enum symbols**: `validate_name` should reject symbols starting with digits in strict mode

The current implementation already has this logic - we just need tests to verify it works correctly.

## Data Models

### Nanosecond Timestamp Representation

| Avro Logical Type     | Base Type | Arrow/Polars Type  | Value Range           |
| --------------------- | --------- | ------------------ | --------------------- |
| timestamp-nanos       | long      | Datetime(ns, UTC)  | ±292 years from epoch |
| local-timestamp-nanos | long      | Datetime(ns, None) | ±292 years from epoch |

The nanosecond timestamp is stored as a signed 64-bit integer representing nanoseconds since the Unix epoch (1970-01-01T00:00:00Z). This provides a range of approximately ±292 years from the epoch.

### Duration Representation (for test coverage)

| Component    | Size    | Type   | Description            |
| ------------ | ------- | ------ | ---------------------- |
| months       | 4 bytes | u32 LE | Number of months       |
| days         | 4 bytes | u32 LE | Number of days         |
| milliseconds | 4 bytes | u32 LE | Number of milliseconds |

Total: 12 bytes (fixed[12])

### Big-Decimal Representation (Avro 1.12.0+)

The `big-decimal` logical type differs from regular `decimal` in that the scale is stored in the value itself rather than in the schema. This allows variable-scale decimals within a single column.

**Encoding Format:**
| Component      | Size     | Type                        | Description                             |
| -------------- | -------- | --------------------------- | --------------------------------------- |
| scale          | variable | varint (zigzag encoded)     | Number of digits after decimal point    |
| unscaled value | variable | big-endian two's complement | The integer value before applying scale |

**Example:** The value `123.45` with scale=2 is encoded as:
- Scale: 2 (varint: 0x04 after zigzag encoding)
- Unscaled: 12345 (big-endian bytes: 0x30, 0x39)

**Polars Mapping Decision:**
The `big-decimal` type maps to `String` in Polars because:
1. Polars `Decimal` requires fixed precision/scale at the type level
2. `big-decimal` allows variable scale per value
3. String preserves exact representation without precision loss
4. Users can convert to numeric types in Polars if needed

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Nanosecond Timestamp Schema Round-Trip

*For any* valid Avro schema containing timestamp-nanos or local-timestamp-nanos logical types, serializing the schema to JSON and parsing it back SHALL produce an equivalent schema object.

This property extends the existing schema round-trip property test to include the new Avro 1.12.0 nanosecond timestamp logical types. The test generates schemas with timestamp-nanos and local-timestamp-nanos, converts them to JSON, parses the JSON back, and verifies equivalence.

**Validates: Requirements 1.1, 1.2, 1.7**

### Property 2: Nanosecond Timestamp Arrow Type Mapping

*For any* timestamp-nanos logical type schema, the Arrow converter SHALL produce `Datetime(Nanoseconds, UTC)`. *For any* local-timestamp-nanos logical type schema, the Arrow converter SHALL produce `Datetime(Nanoseconds, None)`.

This property verifies the deterministic mapping from Avro logical types to Polars/Arrow data types. The test generates nanosecond timestamp schemas and verifies the correct DataType is produced.

**Validates: Requirements 1.3, 1.4**

### Property 3: Negative Block Count Decoding Equivalence

*For any* array or map data, encoding with negative block counts (with byte size prefix) and decoding SHALL produce the same result as encoding with positive block counts and decoding.

This property verifies that the Avro reader correctly handles the alternative block encoding format where negative counts indicate a byte size follows. The test generates random array/map data, encodes it both ways, and verifies both decodings produce identical results.

**Validates: Requirements 4.1, 4.2, 4.3**

### Property 4: Big-Decimal String Representation Preserves Value

*For any* valid big-decimal value (scale + unscaled integer), the decoded string representation SHALL be parseable back to the original numeric value with the same scale.

This property verifies that the string representation of big-decimal values is lossless. The test generates random scale and unscaled integer values, encodes them as big-decimal bytes, decodes to string, and verifies the string can be parsed back to the original value.

**Validates: Requirements 8.3, 8.4, 8.5**

## Error Handling

### Unknown Logical Type Handling

Per the Avro specification, unknown logical types should be ignored and the base type used instead. This is already implemented in the parser:

```rust
_other => {
    // Unknown logical type - return base type per Avro spec
    return Ok(base_schema);
}
```

### Unknown Codec Error Messages

When an unknown codec is encountered, the error should be informative:

```rust
Err(ReaderError::UnsupportedCodec {
    codec: codec_name.to_string(),
    hint: "Check if the codec feature is enabled (e.g., --features snappy)".to_string(),
})
```

## Testing Strategy

### Dual Testing Approach

This implementation uses both unit tests and property-based tests as complementary approaches:
- **Unit tests**: Verify specific examples, edge cases, and error conditions
- **Property tests**: Verify universal properties across many generated inputs

### Unit Tests (Rust)

Unit tests live alongside source code in `#[cfg(test)]` modules:

1. **Schema parsing tests** (`src/schema/parser.rs`): Verify timestamp-nanos and local-timestamp-nanos are parsed correctly
2. **Arrow mapping tests** (`src/convert/arrow.rs`): Verify correct Polars DataType is produced
3. **Compatibility tests** (`src/schema/compatibility.rs`): Verify schema compatibility rules work correctly

### Property-Based Tests (Rust)

Property tests live in `tests/property_tests.rs` and use `proptest` with minimum 100 iterations.

Each test is tagged with the format: **Feature: avro-1-12-support, Property N: description**

1. **Property 1 test**: Extend existing `arb_logical_type()` generator to include TimestampNanos and LocalTimestampNanos, verifying schema round-trip
2. **Property 2 test**: Generate nanosecond timestamp schemas and verify Arrow type mapping
3. **Property 3 test**: Generate array/map data, encode with negative block counts, verify decoding equivalence

### Integration Tests (Python)

Python integration tests live in `python/tests/types/` and use `fastavro` to generate test files:

1. **test_timestamp_nanos_type**: End-to-end test for timestamp-nanos reading
2. **test_local_timestamp_nanos_type**: End-to-end test for local-timestamp-nanos reading
3. **test_duration_type**: End-to-end test for duration logical type (Requirement 2)
4. **test_uuid_fixed16_type**: End-to-end test for UUID on fixed[16] (Requirement 3)
5. **test_negative_block_count**: Test array/map with negative block counts (Requirement 4)
6. **test_unknown_codec_error**: Verify helpful error message for unknown codecs (Requirement 5)
7. **test_complex_union_error**: Verify helpful error message for complex unions (Requirement 7)
8. **test_big_decimal_type**: End-to-end test for big-decimal logical type (Requirement 8)

### Schema Validation Tests (Rust)

Schema validation edge case tests in `tests/schema_tests.rs`:

1. **test_empty_namespace_component_rejected**: Verify `a..b` namespace is rejected in strict mode
2. **test_invalid_enum_symbol_rejected**: Verify symbols starting with digits are rejected in strict mode
3. **test_valid_names_accepted**: Verify valid names work in both modes

### Test File Generation

Python tests use `fastavro` to generate test Avro files. For nanosecond timestamps (Avro 1.12.0), we construct schemas manually since fastavro may not fully support these yet. The test will write raw Avro bytes with the correct schema and data encoding.

### Running Tests

```bash
# Run Rust unit tests
poe test-rust

# Run property tests
poe test test-property

# Run Python integration tests
poe test
```

