//! Avro binary decoder for primitive and complex types.
//!
//! This module provides functions to decode Avro binary data into Rust values.
//! The decoder follows the Avro specification for binary encoding:
//! - Varints use zigzag encoding for signed integers
//! - Floats and doubles are little-endian IEEE 754
//! - Bytes and strings are length-prefixed
//!
//! # Requirements
//! - 1.4: Support all Avro primitive types
//! - 1.5: Support all Avro complex types (records, enums, arrays, maps, unions, fixed)
//! - 1.6: Support logical types (decimal, uuid, date, time, timestamp, duration)

use crate::error::DecodeError;
use crate::schema::{AvroSchema, EnumSchema, LogicalTypeName, RecordSchema};

/// Decode a null value (no-op, consumes no bytes).
///
/// Avro null values have no binary representation.
#[inline]
pub fn decode_null(_data: &mut &[u8]) -> Result<(), DecodeError> {
    Ok(())
}

/// Decode a boolean value.
///
/// Avro booleans are encoded as a single byte: 0x00 for false, 0x01 for true.
#[inline]
pub fn decode_boolean(data: &mut &[u8]) -> Result<bool, DecodeError> {
    if data.is_empty() {
        return Err(DecodeError::UnexpectedEof);
    }
    let byte = data[0];
    *data = &data[1..];
    match byte {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DecodeError::InvalidData(format!(
            "Invalid boolean value: {}, expected 0 or 1",
            byte
        ))),
    }
}

/// Decode a 32-bit signed integer (zigzag varint encoded).
///
/// Avro ints are encoded as variable-length zigzag integers.
#[inline]
pub fn decode_int(data: &mut &[u8]) -> Result<i32, DecodeError> {
    let long = decode_long(data)?;
    // Check for overflow
    if long < i32::MIN as i64 || long > i32::MAX as i64 {
        return Err(DecodeError::InvalidData(format!(
            "Integer overflow: {} does not fit in i32",
            long
        )));
    }
    Ok(long as i32)
}

/// Decode a 64-bit signed integer (zigzag varint encoded).
///
/// Avro longs are encoded as variable-length zigzag integers.
/// The zigzag encoding maps signed integers to unsigned integers:
/// - 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
/// - Formula: (n << 1) ^ (n >> 63) for encoding
/// - Formula: (n >> 1) ^ -(n & 1) for decoding
#[inline]
pub fn decode_long(data: &mut &[u8]) -> Result<i64, DecodeError> {
    super::varint::decode_zigzag(data)
}

/// Decode an unsigned variable-length integer.
///
/// Varints use the same encoding as Protocol Buffers:
/// - Each byte has 7 bits of data and 1 continuation bit (MSB)
/// - The continuation bit indicates if more bytes follow
/// - Bytes are in little-endian order
#[inline]
pub fn decode_varint(data: &mut &[u8]) -> Result<u64, DecodeError> {
    super::varint::decode_varint(data)
}

/// Decode a 32-bit IEEE 754 floating-point number (little-endian).
#[inline]
pub fn decode_float(data: &mut &[u8]) -> Result<f32, DecodeError> {
    if data.len() < 4 {
        return Err(DecodeError::UnexpectedEof);
    }
    let bytes: [u8; 4] = [data[0], data[1], data[2], data[3]];
    *data = &data[4..];
    Ok(f32::from_le_bytes(bytes))
}

/// Decode a 64-bit IEEE 754 floating-point number (little-endian).
#[inline]
pub fn decode_double(data: &mut &[u8]) -> Result<f64, DecodeError> {
    if data.len() < 8 {
        return Err(DecodeError::UnexpectedEof);
    }
    let bytes: [u8; 8] = [
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ];
    *data = &data[8..];
    Ok(f64::from_le_bytes(bytes))
}

/// Decode a byte array (length-prefixed).
///
/// Avro bytes are encoded as a long (length) followed by that many bytes.
#[inline]
pub fn decode_bytes(data: &mut &[u8]) -> Result<Vec<u8>, DecodeError> {
    let len = decode_long(data)?;
    if len < 0 {
        return Err(DecodeError::InvalidData(format!(
            "Negative bytes length: {}",
            len
        )));
    }
    let len = len as usize;

    if data.len() < len {
        return Err(DecodeError::UnexpectedEof);
    }

    let bytes = data[..len].to_vec();
    *data = &data[len..];
    Ok(bytes)
}

/// Decode a UTF-8 string (length-prefixed).
///
/// Avro strings are encoded as a long (length in bytes) followed by UTF-8 bytes.
#[inline]
pub fn decode_string(data: &mut &[u8]) -> Result<String, DecodeError> {
    let bytes = decode_bytes(data)?;
    String::from_utf8(bytes).map_err(DecodeError::from)
}

/// Decode bytes without copying (returns a slice reference).
///
/// This is useful when you want to avoid allocation and the data
/// will be processed immediately.
#[inline]
pub fn decode_bytes_ref<'a>(data: &mut &'a [u8]) -> Result<&'a [u8], DecodeError> {
    let len = decode_long(data)?;
    if len < 0 {
        return Err(DecodeError::InvalidData(format!(
            "Negative bytes length: {}",
            len
        )));
    }
    let len = len as usize;

    if data.len() < len {
        return Err(DecodeError::UnexpectedEof);
    }

    let bytes = &data[..len];
    *data = &data[len..];
    Ok(bytes)
}

/// Decode a UTF-8 string without copying (returns a &str reference).
///
/// This is useful when you want to avoid allocation and the data
/// will be processed immediately.
#[inline]
pub fn decode_string_ref<'a>(data: &mut &'a [u8]) -> Result<&'a str, DecodeError> {
    let bytes = decode_bytes_ref(data)?;
    std::str::from_utf8(bytes)
        .map_err(|e| DecodeError::InvalidData(format!("Invalid UTF-8: {}", e)))
}

// ============================================================================
// Complex Type Decoders
// ============================================================================

/// Represents a decoded Avro value.
///
/// This enum is used to represent decoded complex types that need to be
/// returned as structured data. For direct Arrow conversion, we decode
/// directly into builders without using this intermediate representation.
#[derive(Debug, Clone, PartialEq)]
pub enum AvroValue {
    /// Null value
    Null,
    /// Boolean value
    Boolean(bool),
    /// 32-bit signed integer
    Int(i32),
    /// 64-bit signed integer
    Long(i64),
    /// 32-bit floating point
    Float(f32),
    /// 64-bit floating point
    Double(f64),
    /// Byte array
    Bytes(Vec<u8>),
    /// UTF-8 string
    String(String),
    /// Record with named fields
    Record(Vec<(String, AvroValue)>),
    /// Enum variant (index and symbol name)
    Enum(i32, String),
    /// Array of values
    Array(Vec<AvroValue>),
    /// Map with string keys
    Map(Vec<(String, AvroValue)>),
    /// Union variant (index and value)
    Union(i32, Box<AvroValue>),
    /// Fixed-size byte array
    Fixed(Vec<u8>),

    // Logical type values
    /// Decimal value (unscaled bytes, precision, scale)
    Decimal {
        /// The unscaled value as big-endian two's complement bytes
        unscaled: Vec<u8>,
        /// The precision (total number of digits)
        precision: u32,
        /// The scale (number of digits after decimal point)
        scale: u32,
    },
    /// UUID value (as string)
    Uuid(String),
    /// Date value (days since Unix epoch, 1970-01-01)
    Date(i32),
    /// Time in milliseconds since midnight
    TimeMillis(i32),
    /// Time in microseconds since midnight
    TimeMicros(i64),
    /// Timestamp in milliseconds since Unix epoch
    TimestampMillis(i64),
    /// Timestamp in microseconds since Unix epoch
    TimestampMicros(i64),
    /// Duration (months, days, milliseconds)
    Duration {
        /// Number of months
        months: u32,
        /// Number of days
        days: u32,
        /// Number of milliseconds
        milliseconds: u32,
    },
}

impl AvroValue {
    /// Convert the AvroValue to a serde_json::Value for JSON serialization.
    ///
    /// This is useful for serializing recursive types to JSON strings.
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::{json, Map, Value};

        match self {
            AvroValue::Null => Value::Null,
            AvroValue::Boolean(b) => Value::Bool(*b),
            AvroValue::Int(i) => Value::Number((*i).into()),
            AvroValue::Long(l) => Value::Number((*l).into()),
            AvroValue::Float(f) => serde_json::Number::from_f64(*f as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            AvroValue::Double(d) => serde_json::Number::from_f64(*d)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            AvroValue::Bytes(b) => {
                // Encode bytes as base64 string
                Value::String(base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    b,
                ))
            }
            AvroValue::String(s) => Value::String(s.clone()),
            AvroValue::Record(fields) => {
                let mut map = Map::new();
                for (name, value) in fields {
                    map.insert(name.clone(), value.to_json());
                }
                Value::Object(map)
            }
            AvroValue::Enum(_index, symbol) => Value::String(symbol.clone()),
            AvroValue::Array(items) => Value::Array(items.iter().map(|v| v.to_json()).collect()),
            AvroValue::Map(entries) => {
                let mut map = Map::new();
                for (key, value) in entries {
                    map.insert(key.clone(), value.to_json());
                }
                Value::Object(map)
            }
            AvroValue::Union(_index, value) => value.to_json(),
            AvroValue::Fixed(b) => {
                // Encode fixed bytes as base64 string
                Value::String(base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    b,
                ))
            }
            AvroValue::Decimal {
                unscaled,
                precision: _,
                scale,
            } => {
                // Convert decimal to string representation
                let value = bytes_to_decimal_string(unscaled, *scale);
                Value::String(value)
            }
            AvroValue::Uuid(s) => Value::String(s.clone()),
            AvroValue::Date(days) => Value::Number((*days).into()),
            AvroValue::TimeMillis(ms) => Value::Number((*ms).into()),
            AvroValue::TimeMicros(us) => Value::Number((*us).into()),
            AvroValue::TimestampMillis(ms) => Value::Number((*ms).into()),
            AvroValue::TimestampMicros(us) => Value::Number((*us).into()),
            AvroValue::Duration {
                months,
                days,
                milliseconds,
            } => {
                json!({
                    "months": months,
                    "days": days,
                    "milliseconds": milliseconds
                })
            }
        }
    }
}

/// Convert decimal bytes to a string representation.
fn bytes_to_decimal_string(bytes: &[u8], scale: u32) -> String {
    if bytes.is_empty() {
        return "0".to_string();
    }

    // Convert big-endian two's complement to i128
    let is_negative = bytes[0] & 0x80 != 0;
    let mut value: i128 = if is_negative { -1 } else { 0 };
    for &byte in bytes {
        value = (value << 8) | (byte as i128);
    }

    // Format with scale
    if scale == 0 {
        return value.to_string();
    }

    let abs_value = value.abs();
    let divisor = 10i128.pow(scale);
    let integer_part = abs_value / divisor;
    let fractional_part = abs_value % divisor;

    let sign = if value < 0 { "-" } else { "" };
    format!(
        "{}{}.{:0>width$}",
        sign,
        integer_part,
        fractional_part,
        width = scale as usize
    )
}

/// Decode a fixed-size byte array.
///
/// Avro fixed types are encoded as raw bytes with a predetermined size
/// specified in the schema.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `size` - The fixed size in bytes from the schema
///
/// # Returns
/// A vector containing exactly `size` bytes
#[inline]
pub fn decode_fixed(data: &mut &[u8], size: usize) -> Result<Vec<u8>, DecodeError> {
    if data.len() < size {
        return Err(DecodeError::UnexpectedEof);
    }
    let bytes = data[..size].to_vec();
    *data = &data[size..];
    Ok(bytes)
}

/// Decode a fixed-size byte array without copying (returns a slice reference).
///
/// This is useful when you want to avoid allocation and the data
/// will be processed immediately.
#[inline]
pub fn decode_fixed_ref<'a>(data: &mut &'a [u8], size: usize) -> Result<&'a [u8], DecodeError> {
    if data.len() < size {
        return Err(DecodeError::UnexpectedEof);
    }
    let bytes = &data[..size];
    *data = &data[size..];
    Ok(bytes)
}

/// Decode an enum value.
///
/// Avro enums are encoded as a varint index into the symbol list.
/// The index is a non-negative integer.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `schema` - The enum schema containing the symbol list
///
/// # Returns
/// A tuple of (index, symbol_name)
#[inline]
pub fn decode_enum(data: &mut &[u8], schema: &EnumSchema) -> Result<(i32, String), DecodeError> {
    let index = decode_int(data)?;

    if index < 0 || index as usize >= schema.symbols.len() {
        return Err(DecodeError::InvalidData(format!(
            "Enum index {} out of range for enum '{}' with {} symbols",
            index,
            schema.name,
            schema.symbols.len()
        )));
    }

    let symbol = schema.symbols[index as usize].clone();
    Ok((index, symbol))
}

/// Decode an enum value, returning just the index.
///
/// This is useful when you only need the index and want to avoid
/// the string allocation.
#[inline]
pub fn decode_enum_index(data: &mut &[u8], num_symbols: usize) -> Result<i32, DecodeError> {
    let index = decode_int(data)?;

    if index < 0 || index as usize >= num_symbols {
        return Err(DecodeError::InvalidData(format!(
            "Enum index {} out of range (0..{})",
            index, num_symbols
        )));
    }

    Ok(index)
}

/// Decode an array of values.
///
/// Avro arrays are encoded as a series of blocks. Each block consists of:
/// - A long count of items in the block (can be negative, see below)
/// - If count is negative, the absolute value is the count and is followed
///   by a long byte size of the block (for skipping)
/// - The encoded items
/// - A zero count indicates the end of the array
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `item_schema` - The schema for array items
///
/// # Returns
/// A vector of decoded values
pub fn decode_array(
    data: &mut &[u8],
    item_schema: &AvroSchema,
) -> Result<Vec<AvroValue>, DecodeError> {
    let mut items = Vec::new();

    loop {
        let count = decode_long(data)?;

        if count == 0 {
            // End of array
            break;
        }

        let item_count = if count < 0 {
            // Negative count means the block has a byte size prefix
            // Read and discard the byte size (we don't use it for decoding)
            let _byte_size = decode_long(data)?;
            (-count) as usize
        } else {
            count as usize
        };

        // Reserve space for efficiency
        items.reserve(item_count);

        // Decode each item in the block
        for _ in 0..item_count {
            let value = decode_value(data, item_schema)?;
            items.push(value);
        }
    }

    Ok(items)
}

/// Decode a map with string keys.
///
/// Avro maps are encoded similarly to arrays, as a series of blocks.
/// Each block consists of:
/// - A long count of key-value pairs in the block
/// - If count is negative, absolute value is count followed by byte size
/// - The encoded key-value pairs (key is always a string)
/// - A zero count indicates the end of the map
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `value_schema` - The schema for map values
///
/// # Returns
/// A vector of (key, value) pairs
pub fn decode_map(
    data: &mut &[u8],
    value_schema: &AvroSchema,
) -> Result<Vec<(String, AvroValue)>, DecodeError> {
    let mut entries = Vec::new();

    loop {
        let count = decode_long(data)?;

        if count == 0 {
            // End of map
            break;
        }

        let entry_count = if count < 0 {
            // Negative count means the block has a byte size prefix
            let _byte_size = decode_long(data)?;
            (-count) as usize
        } else {
            count as usize
        };

        // Reserve space for efficiency
        entries.reserve(entry_count);

        // Decode each key-value pair in the block
        for _ in 0..entry_count {
            let key = decode_string(data)?;
            let value = decode_value(data, value_schema)?;
            entries.push((key, value));
        }
    }

    Ok(entries)
}

/// Decode a union value.
///
/// Avro unions are encoded as:
/// - A varint index indicating which schema variant is used
/// - The value encoded according to that schema variant
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `variants` - The list of possible schema variants
///
/// # Returns
/// A tuple of (variant_index, decoded_value)
pub fn decode_union(
    data: &mut &[u8],
    variants: &[AvroSchema],
) -> Result<(i32, AvroValue), DecodeError> {
    let index = decode_int(data)?;

    if index < 0 || index as usize >= variants.len() {
        return Err(DecodeError::InvalidData(format!(
            "Union index {} out of range (0..{})",
            index,
            variants.len()
        )));
    }

    let variant_schema = &variants[index as usize];
    let value = decode_value(data, variant_schema)?;

    Ok((index, value))
}

/// Decode a union value, returning just the index.
///
/// This is useful when you need to determine the variant before
/// deciding how to decode the value.
#[inline]
pub fn decode_union_index(data: &mut &[u8], num_variants: usize) -> Result<i32, DecodeError> {
    let index = decode_int(data)?;

    if index < 0 || index as usize >= num_variants {
        return Err(DecodeError::InvalidData(format!(
            "Union index {} out of range (0..{})",
            index, num_variants
        )));
    }

    Ok(index)
}

/// Decode a record value.
///
/// Avro records are encoded as a sequence of field values in the order
/// they appear in the schema. There are no field markers or delimiters.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `schema` - The record schema
///
/// # Returns
/// A vector of (field_name, value) pairs
pub fn decode_record(
    data: &mut &[u8],
    schema: &RecordSchema,
) -> Result<Vec<(String, AvroValue)>, DecodeError> {
    let mut fields = Vec::with_capacity(schema.fields.len());

    for field in &schema.fields {
        let value = decode_value(data, &field.schema)?;
        fields.push((field.name.clone(), value));
    }

    Ok(fields)
}

/// Decode any Avro value based on its schema.
///
/// This is the main entry point for decoding Avro data. It dispatches
/// to the appropriate decoder based on the schema type.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `schema` - The schema describing the value to decode
///
/// # Returns
/// The decoded value
pub fn decode_value(data: &mut &[u8], schema: &AvroSchema) -> Result<AvroValue, DecodeError> {
    match schema {
        // Primitive types
        AvroSchema::Null => {
            decode_null(data)?;
            Ok(AvroValue::Null)
        }
        AvroSchema::Boolean => {
            let v = decode_boolean(data)?;
            Ok(AvroValue::Boolean(v))
        }
        AvroSchema::Int => {
            let v = decode_int(data)?;
            Ok(AvroValue::Int(v))
        }
        AvroSchema::Long => {
            let v = decode_long(data)?;
            Ok(AvroValue::Long(v))
        }
        AvroSchema::Float => {
            let v = decode_float(data)?;
            Ok(AvroValue::Float(v))
        }
        AvroSchema::Double => {
            let v = decode_double(data)?;
            Ok(AvroValue::Double(v))
        }
        AvroSchema::Bytes => {
            let v = decode_bytes(data)?;
            Ok(AvroValue::Bytes(v))
        }
        AvroSchema::String => {
            let v = decode_string(data)?;
            Ok(AvroValue::String(v))
        }

        // Complex types
        AvroSchema::Record(record_schema) => {
            let fields = decode_record(data, record_schema)?;
            Ok(AvroValue::Record(fields))
        }
        AvroSchema::Enum(enum_schema) => {
            let (index, symbol) = decode_enum(data, enum_schema)?;
            Ok(AvroValue::Enum(index, symbol))
        }
        AvroSchema::Array(item_schema) => {
            let items = decode_array(data, item_schema)?;
            Ok(AvroValue::Array(items))
        }
        AvroSchema::Map(value_schema) => {
            let entries = decode_map(data, value_schema)?;
            Ok(AvroValue::Map(entries))
        }
        AvroSchema::Union(variants) => {
            let (index, value) = decode_union(data, variants)?;
            Ok(AvroValue::Union(index, Box::new(value)))
        }
        AvroSchema::Fixed(fixed_schema) => {
            let bytes = decode_fixed(data, fixed_schema.size)?;
            Ok(AvroValue::Fixed(bytes))
        }

        // Named type reference - should be resolved before decoding
        AvroSchema::Named(name) => Err(DecodeError::InvalidData(format!(
            "Unresolved named type reference: '{}'. Named types must be resolved before decoding.",
            name
        ))),

        // Logical types - decode with logical type interpretation
        AvroSchema::Logical(logical) => {
            decode_logical_value(data, &logical.logical_type, &logical.base)
        }
    }
}

/// Decode any Avro value based on its schema, resolving named type references.
///
/// This function is similar to `decode_value` but uses a resolution context
/// to resolve `Named` type references during decoding. This is useful when
/// decoding data with schemas that contain named type references.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `schema` - The schema describing the value to decode
/// * `context` - The resolution context containing named type definitions
///
/// # Returns
/// The decoded value
///
/// # Requirements
/// - 1.7: WHEN a schema contains named types, THE Avro_Reader SHALL resolve type references correctly
pub fn decode_value_with_context(
    data: &mut &[u8],
    schema: &AvroSchema,
    context: &crate::schema::SchemaResolutionContext,
) -> Result<AvroValue, DecodeError> {
    match schema {
        // Primitive types - same as decode_value
        AvroSchema::Null => {
            decode_null(data)?;
            Ok(AvroValue::Null)
        }
        AvroSchema::Boolean => {
            let v = decode_boolean(data)?;
            Ok(AvroValue::Boolean(v))
        }
        AvroSchema::Int => {
            let v = decode_int(data)?;
            Ok(AvroValue::Int(v))
        }
        AvroSchema::Long => {
            let v = decode_long(data)?;
            Ok(AvroValue::Long(v))
        }
        AvroSchema::Float => {
            let v = decode_float(data)?;
            Ok(AvroValue::Float(v))
        }
        AvroSchema::Double => {
            let v = decode_double(data)?;
            Ok(AvroValue::Double(v))
        }
        AvroSchema::Bytes => {
            let v = decode_bytes(data)?;
            Ok(AvroValue::Bytes(v))
        }
        AvroSchema::String => {
            let v = decode_string(data)?;
            Ok(AvroValue::String(v))
        }

        // Complex types - use context for nested decoding
        AvroSchema::Record(record_schema) => {
            let fields = decode_record_with_context(data, record_schema, context)?;
            Ok(AvroValue::Record(fields))
        }
        AvroSchema::Enum(enum_schema) => {
            let (index, symbol) = decode_enum(data, enum_schema)?;
            Ok(AvroValue::Enum(index, symbol))
        }
        AvroSchema::Array(item_schema) => {
            let items = decode_array_with_context(data, item_schema, context)?;
            Ok(AvroValue::Array(items))
        }
        AvroSchema::Map(value_schema) => {
            let entries = decode_map_with_context(data, value_schema, context)?;
            Ok(AvroValue::Map(entries))
        }
        AvroSchema::Union(variants) => {
            let (index, value) = decode_union_with_context(data, variants, context)?;
            Ok(AvroValue::Union(index, Box::new(value)))
        }
        AvroSchema::Fixed(fixed_schema) => {
            let bytes = decode_fixed(data, fixed_schema.size)?;
            Ok(AvroValue::Fixed(bytes))
        }

        // Named type reference - resolve using context
        AvroSchema::Named(name) => match context.get(name) {
            Some(resolved_schema) => decode_value_with_context(data, resolved_schema, context),
            None => Err(DecodeError::InvalidData(format!(
                "Unresolved named type reference: '{}'. Type not found in resolution context.",
                name
            ))),
        },

        // Logical types - decode with logical type interpretation
        AvroSchema::Logical(logical) => {
            decode_logical_value(data, &logical.logical_type, &logical.base)
        }
    }
}

/// Decode a record value with resolution context.
///
/// This is similar to `decode_record` but uses a resolution context
/// to resolve named type references in field schemas.
pub fn decode_record_with_context(
    data: &mut &[u8],
    schema: &RecordSchema,
    context: &crate::schema::SchemaResolutionContext,
) -> Result<Vec<(String, AvroValue)>, DecodeError> {
    let mut fields = Vec::with_capacity(schema.fields.len());

    for field in &schema.fields {
        let value = decode_value_with_context(data, &field.schema, context)?;
        fields.push((field.name.clone(), value));
    }

    Ok(fields)
}

/// Decode an array with resolution context.
///
/// This is similar to `decode_array` but uses a resolution context
/// to resolve named type references in item schemas.
pub fn decode_array_with_context(
    data: &mut &[u8],
    item_schema: &AvroSchema,
    context: &crate::schema::SchemaResolutionContext,
) -> Result<Vec<AvroValue>, DecodeError> {
    let mut items = Vec::new();

    loop {
        let count = decode_long(data)?;

        if count == 0 {
            break;
        }

        let item_count = if count < 0 {
            let _byte_size = decode_long(data)?;
            (-count) as usize
        } else {
            count as usize
        };

        items.reserve(item_count);

        for _ in 0..item_count {
            let value = decode_value_with_context(data, item_schema, context)?;
            items.push(value);
        }
    }

    Ok(items)
}

/// Decode a map with resolution context.
///
/// This is similar to `decode_map` but uses a resolution context
/// to resolve named type references in value schemas.
pub fn decode_map_with_context(
    data: &mut &[u8],
    value_schema: &AvroSchema,
    context: &crate::schema::SchemaResolutionContext,
) -> Result<Vec<(String, AvroValue)>, DecodeError> {
    let mut entries = Vec::new();

    loop {
        let count = decode_long(data)?;

        if count == 0 {
            break;
        }

        let entry_count = if count < 0 {
            let _byte_size = decode_long(data)?;
            (-count) as usize
        } else {
            count as usize
        };

        entries.reserve(entry_count);

        for _ in 0..entry_count {
            let key = decode_string(data)?;
            let value = decode_value_with_context(data, value_schema, context)?;
            entries.push((key, value));
        }
    }

    Ok(entries)
}

/// Decode a union with resolution context.
///
/// This is similar to `decode_union` but uses a resolution context
/// to resolve named type references in variant schemas.
pub fn decode_union_with_context(
    data: &mut &[u8],
    variants: &[AvroSchema],
    context: &crate::schema::SchemaResolutionContext,
) -> Result<(i32, AvroValue), DecodeError> {
    let index = decode_int(data)?;

    if index < 0 || index as usize >= variants.len() {
        return Err(DecodeError::InvalidData(format!(
            "Union index {} out of range (0..{})",
            index,
            variants.len()
        )));
    }

    let variant_schema = &variants[index as usize];
    let value = decode_value_with_context(data, variant_schema, context)?;

    Ok((index, value))
}

// ============================================================================
// Logical Type Decoders
// ============================================================================

/// Decode a decimal value from bytes.
///
/// Avro decimal logical type is stored as bytes (or fixed) containing
/// the unscaled value as a big-endian two's complement integer.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `precision` - The total number of digits
/// * `scale` - The number of digits after the decimal point
///
/// # Returns
/// A Decimal AvroValue containing the unscaled bytes, precision, and scale
#[inline]
pub fn decode_decimal_bytes(
    data: &mut &[u8],
    precision: u32,
    scale: u32,
) -> Result<AvroValue, DecodeError> {
    let unscaled = decode_bytes(data)?;
    Ok(AvroValue::Decimal {
        unscaled,
        precision,
        scale,
    })
}

/// Decode a decimal value from fixed bytes.
///
/// Avro decimal logical type can also be stored as fixed-size bytes.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `size` - The fixed size in bytes
/// * `precision` - The total number of digits
/// * `scale` - The number of digits after the decimal point
///
/// # Returns
/// A Decimal AvroValue containing the unscaled bytes, precision, and scale
#[inline]
pub fn decode_decimal_fixed(
    data: &mut &[u8],
    size: usize,
    precision: u32,
    scale: u32,
) -> Result<AvroValue, DecodeError> {
    let unscaled = decode_fixed(data, size)?;
    Ok(AvroValue::Decimal {
        unscaled,
        precision,
        scale,
    })
}

/// Decode a UUID value from a string.
///
/// Avro UUID logical type is typically stored as a string in the format
/// "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
///
/// # Returns
/// A Uuid AvroValue containing the UUID string
#[inline]
pub fn decode_uuid_string(data: &mut &[u8]) -> Result<AvroValue, DecodeError> {
    let uuid_str = decode_string(data)?;
    // Basic validation: UUID should be 36 characters (32 hex + 4 dashes)
    if uuid_str.len() != 36 {
        return Err(DecodeError::InvalidData(format!(
            "Invalid UUID string length: expected 36, got {}",
            uuid_str.len()
        )));
    }
    Ok(AvroValue::Uuid(uuid_str))
}

/// Decode a UUID value from fixed 16 bytes.
///
/// Avro UUID logical type can also be stored as fixed[16] bytes.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
///
/// # Returns
/// A Uuid AvroValue containing the UUID as a formatted string
#[inline]
pub fn decode_uuid_fixed(data: &mut &[u8]) -> Result<AvroValue, DecodeError> {
    let bytes = decode_fixed(data, 16)?;
    // Format as UUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    let uuid_str = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    );
    Ok(AvroValue::Uuid(uuid_str))
}

/// Decode a date value.
///
/// Avro date logical type is stored as an int representing the number of
/// days since the Unix epoch (January 1, 1970).
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
///
/// # Returns
/// A Date AvroValue containing days since epoch
#[inline]
pub fn decode_date(data: &mut &[u8]) -> Result<AvroValue, DecodeError> {
    let days = decode_int(data)?;
    Ok(AvroValue::Date(days))
}

/// Decode a time-millis value.
///
/// Avro time-millis logical type is stored as an int representing the
/// number of milliseconds after midnight, 00:00:00.000.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
///
/// # Returns
/// A TimeMillis AvroValue containing milliseconds since midnight
#[inline]
pub fn decode_time_millis(data: &mut &[u8]) -> Result<AvroValue, DecodeError> {
    let millis = decode_int(data)?;
    // Validate: must be in range [0, 86400000) (24 hours in milliseconds)
    if !(0..86_400_000).contains(&millis) {
        return Err(DecodeError::InvalidData(format!(
            "Invalid time-millis value: {} (must be in range [0, 86400000))",
            millis
        )));
    }
    Ok(AvroValue::TimeMillis(millis))
}

/// Decode a time-micros value.
///
/// Avro time-micros logical type is stored as a long representing the
/// number of microseconds after midnight, 00:00:00.000000.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
///
/// # Returns
/// A TimeMicros AvroValue containing microseconds since midnight
#[inline]
pub fn decode_time_micros(data: &mut &[u8]) -> Result<AvroValue, DecodeError> {
    let micros = decode_long(data)?;
    // Validate: must be in range [0, 86400000000) (24 hours in microseconds)
    if !(0..86_400_000_000).contains(&micros) {
        return Err(DecodeError::InvalidData(format!(
            "Invalid time-micros value: {} (must be in range [0, 86400000000))",
            micros
        )));
    }
    Ok(AvroValue::TimeMicros(micros))
}

/// Decode a timestamp-millis value.
///
/// Avro timestamp-millis logical type is stored as a long representing the
/// number of milliseconds since the Unix epoch (January 1, 1970 00:00:00 UTC).
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
///
/// # Returns
/// A TimestampMillis AvroValue containing milliseconds since epoch
#[inline]
pub fn decode_timestamp_millis(data: &mut &[u8]) -> Result<AvroValue, DecodeError> {
    let millis = decode_long(data)?;
    Ok(AvroValue::TimestampMillis(millis))
}

/// Decode a timestamp-micros value.
///
/// Avro timestamp-micros logical type is stored as a long representing the
/// number of microseconds since the Unix epoch (January 1, 1970 00:00:00 UTC).
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
///
/// # Returns
/// A TimestampMicros AvroValue containing microseconds since epoch
#[inline]
pub fn decode_timestamp_micros(data: &mut &[u8]) -> Result<AvroValue, DecodeError> {
    let micros = decode_long(data)?;
    Ok(AvroValue::TimestampMicros(micros))
}

/// Decode a duration value.
///
/// Avro duration logical type is stored as fixed[12] bytes containing
/// three little-endian unsigned 32-bit integers:
/// - months (4 bytes)
/// - days (4 bytes)
/// - milliseconds (4 bytes)
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
///
/// # Returns
/// A Duration AvroValue containing months, days, and milliseconds
#[inline]
pub fn decode_duration(data: &mut &[u8]) -> Result<AvroValue, DecodeError> {
    let bytes = decode_fixed(data, 12)?;

    // Parse as three little-endian u32 values
    let months = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let days = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let milliseconds = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);

    Ok(AvroValue::Duration {
        months,
        days,
        milliseconds,
    })
}

/// Decode a logical type value based on its schema.
///
/// This function dispatches to the appropriate logical type decoder based on
/// the logical type name and underlying base type.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `logical_type` - The logical type name with parameters
/// * `base` - The underlying Avro schema
///
/// # Returns
/// The decoded logical type value
pub fn decode_logical_value(
    data: &mut &[u8],
    logical_type: &LogicalTypeName,
    base: &AvroSchema,
) -> Result<AvroValue, DecodeError> {
    match logical_type {
        LogicalTypeName::Decimal { precision, scale } => match base {
            AvroSchema::Bytes => decode_decimal_bytes(data, *precision, *scale),
            AvroSchema::Fixed(fixed) => decode_decimal_fixed(data, fixed.size, *precision, *scale),
            _ => Err(DecodeError::InvalidData(format!(
                "Decimal logical type requires bytes or fixed base type, got {:?}",
                base
            ))),
        },
        LogicalTypeName::Uuid => match base {
            AvroSchema::String => decode_uuid_string(data),
            AvroSchema::Fixed(fixed) if fixed.size == 16 => decode_uuid_fixed(data),
            AvroSchema::Fixed(fixed) => Err(DecodeError::InvalidData(format!(
                "UUID fixed type must be 16 bytes, got {}",
                fixed.size
            ))),
            _ => Err(DecodeError::InvalidData(format!(
                "UUID logical type requires string or fixed[16] base type, got {:?}",
                base
            ))),
        },
        LogicalTypeName::Date => match base {
            AvroSchema::Int => decode_date(data),
            _ => Err(DecodeError::InvalidData(format!(
                "Date logical type requires int base type, got {:?}",
                base
            ))),
        },
        LogicalTypeName::TimeMillis => match base {
            AvroSchema::Int => decode_time_millis(data),
            _ => Err(DecodeError::InvalidData(format!(
                "time-millis logical type requires int base type, got {:?}",
                base
            ))),
        },
        LogicalTypeName::TimeMicros => match base {
            AvroSchema::Long => decode_time_micros(data),
            _ => Err(DecodeError::InvalidData(format!(
                "time-micros logical type requires long base type, got {:?}",
                base
            ))),
        },
        LogicalTypeName::TimestampMillis | LogicalTypeName::LocalTimestampMillis => match base {
            AvroSchema::Long => decode_timestamp_millis(data),
            _ => Err(DecodeError::InvalidData(format!(
                "timestamp-millis logical type requires long base type, got {:?}",
                base
            ))),
        },
        LogicalTypeName::TimestampMicros | LogicalTypeName::LocalTimestampMicros => match base {
            AvroSchema::Long => decode_timestamp_micros(data),
            _ => Err(DecodeError::InvalidData(format!(
                "timestamp-micros logical type requires long base type, got {:?}",
                base
            ))),
        },
        LogicalTypeName::Duration => match base {
            AvroSchema::Fixed(fixed) if fixed.size == 12 => decode_duration(data),
            AvroSchema::Fixed(fixed) => Err(DecodeError::InvalidData(format!(
                "Duration fixed type must be 12 bytes, got {}",
                fixed.size
            ))),
            _ => Err(DecodeError::InvalidData(format!(
                "Duration logical type requires fixed[12] base type, got {:?}",
                base
            ))),
        },
    }
}

// ============================================================================
// Skip Functions (for projection pushdown)
// ============================================================================

/// Skip over a varint without decoding its value.
///
/// This is useful for skipping fields during projection pushdown.
#[inline]
pub fn skip_varint(data: &mut &[u8]) -> Result<(), DecodeError> {
    super::varint::skip_varint(data)
}

/// Skip over a fixed-size value.
#[inline]
pub fn skip_fixed(data: &mut &[u8], size: usize) -> Result<(), DecodeError> {
    if data.len() < size {
        return Err(DecodeError::UnexpectedEof);
    }
    *data = &data[size..];
    Ok(())
}

/// Skip over a bytes or string value.
#[inline]
pub fn skip_bytes(data: &mut &[u8]) -> Result<(), DecodeError> {
    let len = decode_long(data)?;
    if len < 0 {
        return Err(DecodeError::InvalidData(format!(
            "Negative bytes length: {}",
            len
        )));
    }
    let len = len as usize;
    if data.len() < len {
        return Err(DecodeError::UnexpectedEof);
    }
    *data = &data[len..];
    Ok(())
}

/// Skip over an array value.
pub fn skip_array(data: &mut &[u8], item_schema: &AvroSchema) -> Result<(), DecodeError> {
    loop {
        let count = decode_long(data)?;
        if count == 0 {
            break;
        }

        let item_count = if count < 0 {
            // Block has byte size - we can skip directly
            let byte_size = decode_long(data)?;
            if byte_size < 0 {
                return Err(DecodeError::InvalidData(format!(
                    "Negative block byte size: {}",
                    byte_size
                )));
            }
            skip_fixed(data, byte_size as usize)?;
            continue; // Skip to next block
        } else {
            count as usize
        };

        // Skip each item
        for _ in 0..item_count {
            skip_value(data, item_schema)?;
        }
    }
    Ok(())
}

/// Skip over a map value.
pub fn skip_map(data: &mut &[u8], value_schema: &AvroSchema) -> Result<(), DecodeError> {
    loop {
        let count = decode_long(data)?;
        if count == 0 {
            break;
        }

        let entry_count = if count < 0 {
            // Block has byte size - we can skip directly
            let byte_size = decode_long(data)?;
            if byte_size < 0 {
                return Err(DecodeError::InvalidData(format!(
                    "Negative block byte size: {}",
                    byte_size
                )));
            }
            skip_fixed(data, byte_size as usize)?;
            continue; // Skip to next block
        } else {
            count as usize
        };

        // Skip each key-value pair
        for _ in 0..entry_count {
            skip_bytes(data)?; // Skip key (string)
            skip_value(data, value_schema)?; // Skip value
        }
    }
    Ok(())
}

/// Skip over any Avro value based on its schema.
///
/// This is used for projection pushdown to skip non-projected fields
/// without fully decoding them.
pub fn skip_value(data: &mut &[u8], schema: &AvroSchema) -> Result<(), DecodeError> {
    match schema {
        AvroSchema::Null => Ok(()),
        AvroSchema::Boolean => skip_fixed(data, 1),
        AvroSchema::Int | AvroSchema::Long => skip_varint(data),
        AvroSchema::Float => skip_fixed(data, 4),
        AvroSchema::Double => skip_fixed(data, 8),
        AvroSchema::Bytes | AvroSchema::String => skip_bytes(data),
        AvroSchema::Fixed(fixed_schema) => skip_fixed(data, fixed_schema.size),
        AvroSchema::Enum(_) => skip_varint(data),
        AvroSchema::Array(item_schema) => skip_array(data, item_schema),
        AvroSchema::Map(value_schema) => skip_map(data, value_schema),
        AvroSchema::Union(variants) => {
            let index = decode_int(data)?;
            if index < 0 || index as usize >= variants.len() {
                return Err(DecodeError::InvalidData(format!(
                    "Union index {} out of range (0..{})",
                    index,
                    variants.len()
                )));
            }
            skip_value(data, &variants[index as usize])
        }
        AvroSchema::Record(record_schema) => {
            for field in &record_schema.fields {
                skip_value(data, &field.schema)?;
            }
            Ok(())
        }
        AvroSchema::Named(name) => Err(DecodeError::InvalidData(format!(
            "Cannot skip unresolved named type: '{}'",
            name
        ))),
        AvroSchema::Logical(logical) => skip_value(data, &logical.base),
    }
}

/// Skip over any Avro value based on its schema, resolving named type references.
///
/// This is similar to `skip_value` but uses a resolution context
/// to resolve `Named` type references during skipping.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced)
/// * `schema` - The schema describing the value to skip
/// * `context` - The resolution context containing named type definitions
pub fn skip_value_with_context(
    data: &mut &[u8],
    schema: &AvroSchema,
    context: &crate::schema::SchemaResolutionContext,
) -> Result<(), DecodeError> {
    match schema {
        AvroSchema::Null => Ok(()),
        AvroSchema::Boolean => skip_fixed(data, 1),
        AvroSchema::Int | AvroSchema::Long => skip_varint(data),
        AvroSchema::Float => skip_fixed(data, 4),
        AvroSchema::Double => skip_fixed(data, 8),
        AvroSchema::Bytes | AvroSchema::String => skip_bytes(data),
        AvroSchema::Fixed(fixed_schema) => skip_fixed(data, fixed_schema.size),
        AvroSchema::Enum(_) => skip_varint(data),
        AvroSchema::Array(item_schema) => skip_array_with_context(data, item_schema, context),
        AvroSchema::Map(value_schema) => skip_map_with_context(data, value_schema, context),
        AvroSchema::Union(variants) => {
            let index = decode_int(data)?;
            if index < 0 || index as usize >= variants.len() {
                return Err(DecodeError::InvalidData(format!(
                    "Union index {} out of range (0..{})",
                    index,
                    variants.len()
                )));
            }
            skip_value_with_context(data, &variants[index as usize], context)
        }
        AvroSchema::Record(record_schema) => {
            for field in &record_schema.fields {
                skip_value_with_context(data, &field.schema, context)?;
            }
            Ok(())
        }
        AvroSchema::Named(name) => match context.get(name) {
            Some(resolved_schema) => skip_value_with_context(data, resolved_schema, context),
            None => Err(DecodeError::InvalidData(format!(
                "Cannot skip unresolved named type: '{}'. Type not found in resolution context.",
                name
            ))),
        },
        AvroSchema::Logical(logical) => skip_value_with_context(data, &logical.base, context),
    }
}

/// Skip over an array with resolution context.
fn skip_array_with_context(
    data: &mut &[u8],
    item_schema: &AvroSchema,
    context: &crate::schema::SchemaResolutionContext,
) -> Result<(), DecodeError> {
    loop {
        let count = decode_long(data)?;
        if count == 0 {
            break;
        }

        let item_count = if count < 0 {
            let byte_size = decode_long(data)?;
            if byte_size < 0 {
                return Err(DecodeError::InvalidData(format!(
                    "Negative block byte size: {}",
                    byte_size
                )));
            }
            skip_fixed(data, byte_size as usize)?;
            continue;
        } else {
            count as usize
        };

        for _ in 0..item_count {
            skip_value_with_context(data, item_schema, context)?;
        }
    }
    Ok(())
}

/// Skip over a map with resolution context.
fn skip_map_with_context(
    data: &mut &[u8],
    value_schema: &AvroSchema,
    context: &crate::schema::SchemaResolutionContext,
) -> Result<(), DecodeError> {
    loop {
        let count = decode_long(data)?;
        if count == 0 {
            break;
        }

        let entry_count = if count < 0 {
            let byte_size = decode_long(data)?;
            if byte_size < 0 {
                return Err(DecodeError::InvalidData(format!(
                    "Negative block byte size: {}",
                    byte_size
                )));
            }
            skip_fixed(data, byte_size as usize)?;
            continue;
        } else {
            count as usize
        };

        for _ in 0..entry_count {
            skip_bytes(data)?; // Skip key (string)
            skip_value_with_context(data, value_schema, context)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Null decoding tests
    // ========================================================================

    #[test]
    fn test_decode_null() {
        let data: &[u8] = &[];
        let mut cursor = data;
        assert!(decode_null(&mut cursor).is_ok());
        assert_eq!(cursor.len(), 0);

        // Null doesn't consume any bytes
        let data: &[u8] = &[0x01, 0x02];
        let mut cursor = data;
        assert!(decode_null(&mut cursor).is_ok());
        assert_eq!(cursor.len(), 2);
    }

    // ========================================================================
    // Boolean decoding tests
    // ========================================================================

    #[test]
    fn test_decode_boolean_false() {
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        assert_eq!(decode_boolean(&mut cursor).unwrap(), false);
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_decode_boolean_true() {
        let data: &[u8] = &[0x01];
        let mut cursor = data;
        assert_eq!(decode_boolean(&mut cursor).unwrap(), true);
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_decode_boolean_invalid() {
        let data: &[u8] = &[0x02];
        let mut cursor = data;
        assert!(matches!(
            decode_boolean(&mut cursor),
            Err(DecodeError::InvalidData(_))
        ));
    }

    #[test]
    fn test_decode_boolean_eof() {
        let data: &[u8] = &[];
        let mut cursor = data;
        assert!(matches!(
            decode_boolean(&mut cursor),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    // ========================================================================
    // Varint decoding tests
    // ========================================================================

    #[test]
    fn test_decode_varint_single_byte() {
        // 0 -> 0x00
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 0);

        // 1 -> 0x01
        let data: &[u8] = &[0x01];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 1);

        // 127 -> 0x7F
        let data: &[u8] = &[0x7F];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 127);
    }

    #[test]
    fn test_decode_varint_multi_byte() {
        // 128 -> 0x80 0x01
        let data: &[u8] = &[0x80, 0x01];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 128);

        // 16383 -> 0xFF 0x7F
        let data: &[u8] = &[0xFF, 0x7F];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 16383);

        // 16384 -> 0x80 0x80 0x01
        let data: &[u8] = &[0x80, 0x80, 0x01];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 16384);
    }

    #[test]
    fn test_decode_varint_large() {
        // Max i64 as unsigned: 9223372036854775807
        // Encoded as: 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0x7F
        let data: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), i64::MAX as u64);
    }

    #[test]
    fn test_decode_varint_eof() {
        let data: &[u8] = &[];
        let mut cursor = data;
        assert!(matches!(
            decode_varint(&mut cursor),
            Err(DecodeError::UnexpectedEof)
        ));

        // Incomplete multi-byte varint
        let data: &[u8] = &[0x80]; // Continuation bit set but no more bytes
        let mut cursor = data;
        assert!(matches!(
            decode_varint(&mut cursor),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    // ========================================================================
    // Int/Long (zigzag) decoding tests
    // ========================================================================

    #[test]
    fn test_decode_long_zigzag() {
        // Zigzag encoding: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...

        // 0 -> 0x00
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        assert_eq!(decode_long(&mut cursor).unwrap(), 0);

        // -1 -> 0x01
        let data: &[u8] = &[0x01];
        let mut cursor = data;
        assert_eq!(decode_long(&mut cursor).unwrap(), -1);

        // 1 -> 0x02
        let data: &[u8] = &[0x02];
        let mut cursor = data;
        assert_eq!(decode_long(&mut cursor).unwrap(), 1);

        // -2 -> 0x03
        let data: &[u8] = &[0x03];
        let mut cursor = data;
        assert_eq!(decode_long(&mut cursor).unwrap(), -2);

        // 2 -> 0x04
        let data: &[u8] = &[0x04];
        let mut cursor = data;
        assert_eq!(decode_long(&mut cursor).unwrap(), 2);
    }

    #[test]
    fn test_decode_long_larger_values() {
        // 64 -> zigzag(64) = 128 -> 0x80 0x01
        let data: &[u8] = &[0x80, 0x01];
        let mut cursor = data;
        assert_eq!(decode_long(&mut cursor).unwrap(), 64);

        // -64 -> zigzag(-64) = 127 -> 0x7F
        let data: &[u8] = &[0x7F];
        let mut cursor = data;
        assert_eq!(decode_long(&mut cursor).unwrap(), -64);

        // -65 -> zigzag(-65) = 129 -> 0x81 0x01
        let data: &[u8] = &[0x81, 0x01];
        let mut cursor = data;
        assert_eq!(decode_long(&mut cursor).unwrap(), -65);
    }

    #[test]
    fn test_decode_int() {
        // Same as long but with i32 range check
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        assert_eq!(decode_int(&mut cursor).unwrap(), 0);

        let data: &[u8] = &[0x01];
        let mut cursor = data;
        assert_eq!(decode_int(&mut cursor).unwrap(), -1);

        let data: &[u8] = &[0x02];
        let mut cursor = data;
        assert_eq!(decode_int(&mut cursor).unwrap(), 1);
    }

    // ========================================================================
    // Float/Double decoding tests
    // ========================================================================

    #[test]
    fn test_decode_float() {
        // 0.0f32 in little-endian
        let data: &[u8] = &[0x00, 0x00, 0x00, 0x00];
        let mut cursor = data;
        assert_eq!(decode_float(&mut cursor).unwrap(), 0.0f32);

        // 1.0f32 in little-endian: 0x3F800000
        let data: &[u8] = &[0x00, 0x00, 0x80, 0x3F];
        let mut cursor = data;
        assert_eq!(decode_float(&mut cursor).unwrap(), 1.0f32);

        // -1.0f32 in little-endian: 0xBF800000
        let data: &[u8] = &[0x00, 0x00, 0x80, 0xBF];
        let mut cursor = data;
        assert_eq!(decode_float(&mut cursor).unwrap(), -1.0f32);

        // 3.14f32 approximately
        let expected = 3.14f32;
        let data = expected.to_le_bytes();
        let mut cursor = &data[..];
        let result = decode_float(&mut cursor).unwrap();
        assert!((result - expected).abs() < f32::EPSILON);
    }

    #[test]
    fn test_decode_float_eof() {
        let data: &[u8] = &[0x00, 0x00, 0x00]; // Only 3 bytes
        let mut cursor = data;
        assert!(matches!(
            decode_float(&mut cursor),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    #[test]
    fn test_decode_double() {
        // 0.0f64 in little-endian
        let data: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut cursor = data;
        assert_eq!(decode_double(&mut cursor).unwrap(), 0.0f64);

        // 1.0f64 in little-endian: 0x3FF0000000000000
        let data: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F];
        let mut cursor = data;
        assert_eq!(decode_double(&mut cursor).unwrap(), 1.0f64);

        // -1.0f64 in little-endian: 0xBFF0000000000000
        let data: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xBF];
        let mut cursor = data;
        assert_eq!(decode_double(&mut cursor).unwrap(), -1.0f64);

        // 3.141592653589793 (pi)
        let expected = std::f64::consts::PI;
        let data = expected.to_le_bytes();
        let mut cursor = &data[..];
        let result = decode_double(&mut cursor).unwrap();
        assert!((result - expected).abs() < f64::EPSILON);
    }

    #[test]
    fn test_decode_double_eof() {
        let data: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]; // Only 7 bytes
        let mut cursor = data;
        assert!(matches!(
            decode_double(&mut cursor),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    // ========================================================================
    // Bytes decoding tests
    // ========================================================================

    #[test]
    fn test_decode_bytes_empty() {
        // Length 0 -> 0x00
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        assert_eq!(decode_bytes(&mut cursor).unwrap(), Vec::<u8>::new());
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_decode_bytes_simple() {
        // Length 5 -> zigzag(5) = 10 -> 0x0A, followed by 5 bytes
        let data: &[u8] = &[0x0A, 0x01, 0x02, 0x03, 0x04, 0x05];
        let mut cursor = data;
        assert_eq!(decode_bytes(&mut cursor).unwrap(), vec![1, 2, 3, 4, 5]);
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_decode_bytes_with_trailing() {
        // Length 3 -> zigzag(3) = 6 -> 0x06, followed by 3 bytes, then extra
        let data: &[u8] = &[0x06, 0xAA, 0xBB, 0xCC, 0xFF, 0xFF];
        let mut cursor = data;
        assert_eq!(decode_bytes(&mut cursor).unwrap(), vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(cursor, &[0xFF, 0xFF]);
    }

    #[test]
    fn test_decode_bytes_eof() {
        // Claims 10 bytes but only has 3
        let data: &[u8] = &[0x14, 0x01, 0x02, 0x03]; // 0x14 = zigzag(10) = 20
        let mut cursor = data;
        assert!(matches!(
            decode_bytes(&mut cursor),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    #[test]
    fn test_decode_bytes_negative_length() {
        // Negative length: -1 -> zigzag(-1) = 1 -> 0x01
        let data: &[u8] = &[0x01]; // This decodes to -1
        let mut cursor = data;
        assert!(matches!(
            decode_bytes(&mut cursor),
            Err(DecodeError::InvalidData(_))
        ));
    }

    // ========================================================================
    // String decoding tests
    // ========================================================================

    #[test]
    fn test_decode_string_empty() {
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        assert_eq!(decode_string(&mut cursor).unwrap(), "");
    }

    #[test]
    fn test_decode_string_simple() {
        // "hello" = 5 bytes -> zigzag(5) = 10 -> 0x0A
        let data: &[u8] = &[0x0A, b'h', b'e', b'l', b'l', b'o'];
        let mut cursor = data;
        assert_eq!(decode_string(&mut cursor).unwrap(), "hello");
    }

    #[test]
    fn test_decode_string_utf8() {
        // "日本語" = 9 bytes (3 chars × 3 bytes each)
        let s = "日本語";
        let bytes = s.as_bytes();
        let len = bytes.len() as i64; // 9
        let zigzag_len = ((len << 1) ^ (len >> 63)) as u8; // 18 -> 0x12

        let mut data = vec![zigzag_len];
        data.extend_from_slice(bytes);

        let mut cursor = &data[..];
        assert_eq!(decode_string(&mut cursor).unwrap(), "日本語");
    }

    #[test]
    fn test_decode_string_invalid_utf8() {
        // Invalid UTF-8 sequence
        let data: &[u8] = &[0x04, 0xFF, 0xFE]; // Length 2, invalid bytes
        let mut cursor = data;
        assert!(matches!(
            decode_string(&mut cursor),
            Err(DecodeError::InvalidUtf8(_))
        ));
    }

    // ========================================================================
    // Reference decoding tests (zero-copy)
    // ========================================================================

    #[test]
    fn test_decode_bytes_ref() {
        let data: &[u8] = &[0x06, 0x01, 0x02, 0x03, 0xFF];
        let mut cursor = data;
        let result = decode_bytes_ref(&mut cursor).unwrap();
        assert_eq!(result, &[0x01, 0x02, 0x03]);
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_decode_string_ref() {
        let data: &[u8] = &[0x0A, b'h', b'e', b'l', b'l', b'o'];
        let mut cursor = data;
        let result = decode_string_ref(&mut cursor).unwrap();
        assert_eq!(result, "hello");
    }

    // ========================================================================
    // Sequential decoding tests
    // ========================================================================

    #[test]
    fn test_decode_multiple_values() {
        // Encode: boolean(true), int(42), string("test")
        // true -> 0x01
        // 42 -> zigzag(42) = 84 -> 0x54
        // "test" -> length 4 -> zigzag(4) = 8 -> 0x08, then "test"
        let data: &[u8] = &[0x01, 0x54, 0x08, b't', b'e', b's', b't'];
        let mut cursor = data;

        assert_eq!(decode_boolean(&mut cursor).unwrap(), true);
        assert_eq!(decode_int(&mut cursor).unwrap(), 42);
        assert_eq!(decode_string(&mut cursor).unwrap(), "test");
        assert!(cursor.is_empty());
    }

    // ========================================================================
    // Fixed decoding tests
    // ========================================================================

    #[test]
    fn test_decode_fixed() {
        // Fixed 4 bytes
        let data: &[u8] = &[0x01, 0x02, 0x03, 0x04, 0xFF];
        let mut cursor = data;
        assert_eq!(
            decode_fixed(&mut cursor, 4).unwrap(),
            vec![0x01, 0x02, 0x03, 0x04]
        );
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_decode_fixed_empty() {
        let data: &[u8] = &[0xFF];
        let mut cursor = data;
        assert_eq!(decode_fixed(&mut cursor, 0).unwrap(), Vec::<u8>::new());
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_decode_fixed_eof() {
        let data: &[u8] = &[0x01, 0x02];
        let mut cursor = data;
        assert!(matches!(
            decode_fixed(&mut cursor, 4),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    #[test]
    fn test_decode_fixed_ref() {
        let data: &[u8] = &[0x01, 0x02, 0x03, 0x04, 0xFF];
        let mut cursor = data;
        let result = decode_fixed_ref(&mut cursor, 4).unwrap();
        assert_eq!(result, &[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(cursor, &[0xFF]);
    }

    // ========================================================================
    // Enum decoding tests
    // ========================================================================

    #[test]
    fn test_decode_enum() {
        use crate::schema::EnumSchema;

        let schema = EnumSchema::new(
            "Color",
            vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()],
        );

        // Index 0 -> RED
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        let (index, symbol) = decode_enum(&mut cursor, &schema).unwrap();
        assert_eq!(index, 0);
        assert_eq!(symbol, "RED");

        // Index 1 -> GREEN (zigzag(1) = 2)
        let data: &[u8] = &[0x02];
        let mut cursor = data;
        let (index, symbol) = decode_enum(&mut cursor, &schema).unwrap();
        assert_eq!(index, 1);
        assert_eq!(symbol, "GREEN");

        // Index 2 -> BLUE (zigzag(2) = 4)
        let data: &[u8] = &[0x04];
        let mut cursor = data;
        let (index, symbol) = decode_enum(&mut cursor, &schema).unwrap();
        assert_eq!(index, 2);
        assert_eq!(symbol, "BLUE");
    }

    #[test]
    fn test_decode_enum_out_of_range() {
        use crate::schema::EnumSchema;

        let schema = EnumSchema::new("Color", vec!["RED".to_string(), "GREEN".to_string()]);

        // Index 3 is out of range (zigzag(3) = 6)
        let data: &[u8] = &[0x06];
        let mut cursor = data;
        assert!(matches!(
            decode_enum(&mut cursor, &schema),
            Err(DecodeError::InvalidData(_))
        ));
    }

    #[test]
    fn test_decode_enum_negative_index() {
        use crate::schema::EnumSchema;

        let schema = EnumSchema::new("Color", vec!["RED".to_string()]);

        // Negative index: -1 -> zigzag(-1) = 1
        let data: &[u8] = &[0x01];
        let mut cursor = data;
        assert!(matches!(
            decode_enum(&mut cursor, &schema),
            Err(DecodeError::InvalidData(_))
        ));
    }

    #[test]
    fn test_decode_enum_index() {
        // Index 2 -> zigzag(2) = 4
        let data: &[u8] = &[0x04];
        let mut cursor = data;
        assert_eq!(decode_enum_index(&mut cursor, 5).unwrap(), 2);
    }

    // ========================================================================
    // Array decoding tests
    // ========================================================================

    #[test]
    fn test_decode_array_empty() {
        // Empty array: just a zero count
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        let result = decode_array(&mut cursor, &AvroSchema::Int).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_decode_array_single_block() {
        // Array with 3 ints: [1, 2, 3]
        // Count: 3 -> zigzag(3) = 6
        // Values: 1 -> 2, 2 -> 4, 3 -> 6
        // End: 0
        let data: &[u8] = &[0x06, 0x02, 0x04, 0x06, 0x00];
        let mut cursor = data;
        let result = decode_array(&mut cursor, &AvroSchema::Int).unwrap();
        assert_eq!(
            result,
            vec![AvroValue::Int(1), AvroValue::Int(2), AvroValue::Int(3),]
        );
    }

    #[test]
    fn test_decode_array_multiple_blocks() {
        // Array with 2 blocks: [1, 2] and [3]
        // Block 1: count 2 -> 4, values 1 -> 2, 2 -> 4
        // Block 2: count 1 -> 2, value 3 -> 6
        // End: 0
        let data: &[u8] = &[0x04, 0x02, 0x04, 0x02, 0x06, 0x00];
        let mut cursor = data;
        let result = decode_array(&mut cursor, &AvroSchema::Int).unwrap();
        assert_eq!(
            result,
            vec![AvroValue::Int(1), AvroValue::Int(2), AvroValue::Int(3),]
        );
    }

    #[test]
    fn test_decode_array_negative_count_with_size() {
        // Array with negative count (includes byte size)
        // Count: -2 -> zigzag(-2) = 3
        // Byte size: 2 -> zigzag(2) = 4 (2 bytes for two single-byte varints)
        // Values: 1 -> 2, 2 -> 4
        // End: 0
        let data: &[u8] = &[0x03, 0x04, 0x02, 0x04, 0x00];
        let mut cursor = data;
        let result = decode_array(&mut cursor, &AvroSchema::Int).unwrap();
        assert_eq!(result, vec![AvroValue::Int(1), AvroValue::Int(2),]);
    }

    #[test]
    fn test_decode_array_of_strings() {
        // Array with 2 strings: ["hi", "bye"]
        // Count: 2 -> 4
        // "hi": len 2 -> 4, then "hi"
        // "bye": len 3 -> 6, then "bye"
        // End: 0
        let data: &[u8] = &[0x04, 0x04, b'h', b'i', 0x06, b'b', b'y', b'e', 0x00];
        let mut cursor = data;
        let result = decode_array(&mut cursor, &AvroSchema::String).unwrap();
        assert_eq!(
            result,
            vec![
                AvroValue::String("hi".to_string()),
                AvroValue::String("bye".to_string()),
            ]
        );
    }

    // ========================================================================
    // Map decoding tests
    // ========================================================================

    #[test]
    fn test_decode_map_empty() {
        // Empty map: just a zero count
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        let result = decode_map(&mut cursor, &AvroSchema::Int).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_decode_map_single_entry() {
        // Map with 1 entry: {"a": 42}
        // Count: 1 -> 2
        // Key "a": len 1 -> 2, then "a"
        // Value 42: zigzag(42) = 84 -> 0x54
        // End: 0
        let data: &[u8] = &[0x02, 0x02, b'a', 0x54, 0x00];
        let mut cursor = data;
        let result = decode_map(&mut cursor, &AvroSchema::Int).unwrap();
        assert_eq!(result, vec![("a".to_string(), AvroValue::Int(42)),]);
    }

    #[test]
    fn test_decode_map_multiple_entries() {
        // Map with 2 entries: {"x": 1, "y": 2}
        // Count: 2 -> 4
        // Key "x": len 1 -> 2, then "x"
        // Value 1: zigzag(1) = 2
        // Key "y": len 1 -> 2, then "y"
        // Value 2: zigzag(2) = 4
        // End: 0
        let data: &[u8] = &[0x04, 0x02, b'x', 0x02, 0x02, b'y', 0x04, 0x00];
        let mut cursor = data;
        let result = decode_map(&mut cursor, &AvroSchema::Int).unwrap();
        assert_eq!(
            result,
            vec![
                ("x".to_string(), AvroValue::Int(1)),
                ("y".to_string(), AvroValue::Int(2)),
            ]
        );
    }

    #[test]
    fn test_decode_map_string_values() {
        // Map with string values: {"key": "value"}
        // Count: 1 -> 2
        // Key "key": len 3 -> 6, then "key"
        // Value "value": len 5 -> 10, then "value"
        // End: 0
        let data: &[u8] = &[
            0x02, 0x06, b'k', b'e', b'y', 0x0A, b'v', b'a', b'l', b'u', b'e', 0x00,
        ];
        let mut cursor = data;
        let result = decode_map(&mut cursor, &AvroSchema::String).unwrap();
        assert_eq!(
            result,
            vec![("key".to_string(), AvroValue::String("value".to_string())),]
        );
    }

    // ========================================================================
    // Union decoding tests
    // ========================================================================

    #[test]
    fn test_decode_union_null() {
        // Union ["null", "int"] with null value
        // Index 0 -> null
        let variants = vec![AvroSchema::Null, AvroSchema::Int];
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        let (index, value) = decode_union(&mut cursor, &variants).unwrap();
        assert_eq!(index, 0);
        assert_eq!(value, AvroValue::Null);
    }

    #[test]
    fn test_decode_union_int() {
        // Union ["null", "int"] with int value 42
        // Index 1 -> zigzag(1) = 2
        // Value 42 -> zigzag(42) = 84 -> 0x54
        let variants = vec![AvroSchema::Null, AvroSchema::Int];
        let data: &[u8] = &[0x02, 0x54];
        let mut cursor = data;
        let (index, value) = decode_union(&mut cursor, &variants).unwrap();
        assert_eq!(index, 1);
        assert_eq!(value, AvroValue::Int(42));
    }

    #[test]
    fn test_decode_union_string() {
        // Union ["null", "string"] with string value "test"
        // Index 1 -> 2
        // String "test": len 4 -> 8, then "test"
        let variants = vec![AvroSchema::Null, AvroSchema::String];
        let data: &[u8] = &[0x02, 0x08, b't', b'e', b's', b't'];
        let mut cursor = data;
        let (index, value) = decode_union(&mut cursor, &variants).unwrap();
        assert_eq!(index, 1);
        assert_eq!(value, AvroValue::String("test".to_string()));
    }

    #[test]
    fn test_decode_union_out_of_range() {
        let variants = vec![AvroSchema::Null, AvroSchema::Int];
        // Index 3 is out of range (zigzag(3) = 6)
        let data: &[u8] = &[0x06];
        let mut cursor = data;
        assert!(matches!(
            decode_union(&mut cursor, &variants),
            Err(DecodeError::InvalidData(_))
        ));
    }

    #[test]
    fn test_decode_union_index() {
        // Index 1 -> zigzag(1) = 2
        let data: &[u8] = &[0x02];
        let mut cursor = data;
        assert_eq!(decode_union_index(&mut cursor, 3).unwrap(), 1);
    }

    // ========================================================================
    // Record decoding tests
    // ========================================================================

    #[test]
    fn test_decode_record_simple() {
        use crate::schema::{FieldSchema, RecordSchema};

        let schema = RecordSchema::new(
            "Person",
            vec![
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("age", AvroSchema::Int),
            ],
        );

        // Record: {"name": "Alice", "age": 30}
        // "Alice": len 5 -> 10, then "Alice"
        // 30: zigzag(30) = 60 -> 0x3C
        let data: &[u8] = &[0x0A, b'A', b'l', b'i', b'c', b'e', 0x3C];
        let mut cursor = data;
        let result = decode_record(&mut cursor, &schema).unwrap();
        assert_eq!(
            result,
            vec![
                ("name".to_string(), AvroValue::String("Alice".to_string())),
                ("age".to_string(), AvroValue::Int(30)),
            ]
        );
    }

    #[test]
    fn test_decode_record_with_nullable() {
        use crate::schema::{FieldSchema, RecordSchema};

        let schema = RecordSchema::new(
            "Item",
            vec![
                FieldSchema::new("id", AvroSchema::Int),
                FieldSchema::new(
                    "description",
                    AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]),
                ),
            ],
        );

        // Record with null description: {"id": 1, "description": null}
        // id: 1 -> 2
        // description: union index 0 (null)
        let data: &[u8] = &[0x02, 0x00];
        let mut cursor = data;
        let result = decode_record(&mut cursor, &schema).unwrap();
        assert_eq!(
            result,
            vec![
                ("id".to_string(), AvroValue::Int(1)),
                (
                    "description".to_string(),
                    AvroValue::Union(0, Box::new(AvroValue::Null))
                ),
            ]
        );

        // Record with string description: {"id": 2, "description": "test"}
        // id: 2 -> 4
        // description: union index 1 -> 2, then string "test"
        let data: &[u8] = &[0x04, 0x02, 0x08, b't', b'e', b's', b't'];
        let mut cursor = data;
        let result = decode_record(&mut cursor, &schema).unwrap();
        assert_eq!(
            result,
            vec![
                ("id".to_string(), AvroValue::Int(2)),
                (
                    "description".to_string(),
                    AvroValue::Union(1, Box::new(AvroValue::String("test".to_string())))
                ),
            ]
        );
    }

    #[test]
    fn test_decode_record_nested() {
        use crate::schema::{FieldSchema, RecordSchema};

        let address_schema = RecordSchema::new(
            "Address",
            vec![
                FieldSchema::new("city", AvroSchema::String),
                FieldSchema::new("zip", AvroSchema::Int),
            ],
        );

        let person_schema = RecordSchema::new(
            "Person",
            vec![
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("address", AvroSchema::Record(address_schema)),
            ],
        );

        // Record: {"name": "Bob", "address": {"city": "NYC", "zip": 10001}}
        // "Bob": len 3 -> 6, then "Bob"
        // "NYC": len 3 -> 6, then "NYC"
        // 10001: zigzag(10001) = 20002 -> encoded as varint
        let mut data = vec![0x06, b'B', b'o', b'b', 0x06, b'N', b'Y', b'C'];
        // 10001 in zigzag = 20002, which is 0xA2 0x9C 0x01 in varint
        data.extend_from_slice(&[0xA2, 0x9C, 0x01]);

        let mut cursor = &data[..];
        let result = decode_record(&mut cursor, &person_schema).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "name");
        assert_eq!(result[0].1, AvroValue::String("Bob".to_string()));
        assert_eq!(result[1].0, "address");

        if let AvroValue::Record(addr_fields) = &result[1].1 {
            assert_eq!(addr_fields.len(), 2);
            assert_eq!(
                addr_fields[0],
                ("city".to_string(), AvroValue::String("NYC".to_string()))
            );
            assert_eq!(addr_fields[1], ("zip".to_string(), AvroValue::Int(10001)));
        } else {
            panic!("Expected Record value for address");
        }
    }

    // ========================================================================
    // decode_value tests (generic decoder)
    // ========================================================================

    #[test]
    fn test_decode_value_primitives() {
        // Null
        let data: &[u8] = &[];
        let mut cursor = data;
        assert_eq!(
            decode_value(&mut cursor, &AvroSchema::Null).unwrap(),
            AvroValue::Null
        );

        // Boolean
        let data: &[u8] = &[0x01];
        let mut cursor = data;
        assert_eq!(
            decode_value(&mut cursor, &AvroSchema::Boolean).unwrap(),
            AvroValue::Boolean(true)
        );

        // Int
        let data: &[u8] = &[0x54]; // 42
        let mut cursor = data;
        assert_eq!(
            decode_value(&mut cursor, &AvroSchema::Int).unwrap(),
            AvroValue::Int(42)
        );

        // Long
        let data: &[u8] = &[0x54]; // 42
        let mut cursor = data;
        assert_eq!(
            decode_value(&mut cursor, &AvroSchema::Long).unwrap(),
            AvroValue::Long(42)
        );

        // Float
        let data = 3.14f32.to_le_bytes();
        let mut cursor = &data[..];
        if let AvroValue::Float(v) = decode_value(&mut cursor, &AvroSchema::Float).unwrap() {
            assert!((v - 3.14f32).abs() < f32::EPSILON);
        } else {
            panic!("Expected Float");
        }

        // Double
        let data = 3.14f64.to_le_bytes();
        let mut cursor = &data[..];
        if let AvroValue::Double(v) = decode_value(&mut cursor, &AvroSchema::Double).unwrap() {
            assert!((v - 3.14f64).abs() < f64::EPSILON);
        } else {
            panic!("Expected Double");
        }

        // Bytes
        let data: &[u8] = &[0x06, 0x01, 0x02, 0x03];
        let mut cursor = data;
        assert_eq!(
            decode_value(&mut cursor, &AvroSchema::Bytes).unwrap(),
            AvroValue::Bytes(vec![1, 2, 3])
        );

        // String
        let data: &[u8] = &[0x06, b'a', b'b', b'c'];
        let mut cursor = data;
        assert_eq!(
            decode_value(&mut cursor, &AvroSchema::String).unwrap(),
            AvroValue::String("abc".to_string())
        );
    }

    #[test]
    fn test_decode_value_fixed() {
        use crate::schema::FixedSchema;

        let schema = AvroSchema::Fixed(FixedSchema::new("MD5", 16));
        let data: &[u8] = &[0; 16];
        let mut cursor = data;
        assert_eq!(
            decode_value(&mut cursor, &schema).unwrap(),
            AvroValue::Fixed(vec![0; 16])
        );
    }

    #[test]
    fn test_decode_value_named_error() {
        let schema = AvroSchema::Named("UnresolvedType".to_string());
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        assert!(matches!(
            decode_value(&mut cursor, &schema),
            Err(DecodeError::InvalidData(_))
        ));
    }

    // ========================================================================
    // Skip function tests
    // ========================================================================

    #[test]
    fn test_skip_varint() {
        // Single byte varint
        let data: &[u8] = &[0x7F, 0xFF];
        let mut cursor = data;
        skip_varint(&mut cursor).unwrap();
        assert_eq!(cursor, &[0xFF]);

        // Multi-byte varint
        let data: &[u8] = &[0x80, 0x80, 0x01, 0xFF];
        let mut cursor = data;
        skip_varint(&mut cursor).unwrap();
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_skip_fixed() {
        let data: &[u8] = &[0x01, 0x02, 0x03, 0x04, 0xFF];
        let mut cursor = data;
        skip_fixed(&mut cursor, 4).unwrap();
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_skip_bytes() {
        // String "hello" (len 5 -> 10)
        let data: &[u8] = &[0x0A, b'h', b'e', b'l', b'l', b'o', 0xFF];
        let mut cursor = data;
        skip_bytes(&mut cursor).unwrap();
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_skip_value_primitives() {
        // Skip null
        let data: &[u8] = &[0xFF];
        let mut cursor = data;
        skip_value(&mut cursor, &AvroSchema::Null).unwrap();
        assert_eq!(cursor, &[0xFF]);

        // Skip boolean
        let data: &[u8] = &[0x01, 0xFF];
        let mut cursor = data;
        skip_value(&mut cursor, &AvroSchema::Boolean).unwrap();
        assert_eq!(cursor, &[0xFF]);

        // Skip int
        let data: &[u8] = &[0x54, 0xFF]; // 42
        let mut cursor = data;
        skip_value(&mut cursor, &AvroSchema::Int).unwrap();
        assert_eq!(cursor, &[0xFF]);

        // Skip float
        let mut data = 3.14f32.to_le_bytes().to_vec();
        data.push(0xFF);
        let mut cursor = &data[..];
        skip_value(&mut cursor, &AvroSchema::Float).unwrap();
        assert_eq!(cursor, &[0xFF]);

        // Skip double
        let mut data = 3.14f64.to_le_bytes().to_vec();
        data.push(0xFF);
        let mut cursor = &data[..];
        skip_value(&mut cursor, &AvroSchema::Double).unwrap();
        assert_eq!(cursor, &[0xFF]);

        // Skip string
        let data: &[u8] = &[0x06, b'a', b'b', b'c', 0xFF];
        let mut cursor = data;
        skip_value(&mut cursor, &AvroSchema::String).unwrap();
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_skip_array() {
        // Array [1, 2, 3] followed by marker
        let data: &[u8] = &[0x06, 0x02, 0x04, 0x06, 0x00, 0xFF];
        let mut cursor = data;
        skip_array(&mut cursor, &AvroSchema::Int).unwrap();
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_skip_map() {
        // Map {"a": 1} followed by marker
        let data: &[u8] = &[0x02, 0x02, b'a', 0x02, 0x00, 0xFF];
        let mut cursor = data;
        skip_map(&mut cursor, &AvroSchema::Int).unwrap();
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_skip_union() {
        // Union with int value 42
        let variants = vec![AvroSchema::Null, AvroSchema::Int];
        let data: &[u8] = &[0x02, 0x54, 0xFF]; // index 1, value 42
        let mut cursor = data;
        skip_value(&mut cursor, &AvroSchema::Union(variants)).unwrap();
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_skip_record() {
        use crate::schema::{FieldSchema, RecordSchema};

        let schema = RecordSchema::new(
            "Person",
            vec![
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("age", AvroSchema::Int),
            ],
        );

        // Record {"name": "Alice", "age": 30} followed by marker
        let data: &[u8] = &[0x0A, b'A', b'l', b'i', b'c', b'e', 0x3C, 0xFF];
        let mut cursor = data;
        skip_value(&mut cursor, &AvroSchema::Record(schema)).unwrap();
        assert_eq!(cursor, &[0xFF]);
    }

    // ========================================================================
    // Logical type decoding tests
    // ========================================================================

    #[test]
    fn test_decode_decimal_bytes() {
        // Decimal with precision 10, scale 2
        // Unscaled value: 12345 as big-endian bytes
        // 12345 = 0x3039
        let data: &[u8] = &[0x04, 0x30, 0x39]; // length 2, then bytes
        let mut cursor = data;
        let result = decode_decimal_bytes(&mut cursor, 10, 2).unwrap();

        match result {
            AvroValue::Decimal {
                unscaled,
                precision,
                scale,
            } => {
                assert_eq!(unscaled, vec![0x30, 0x39]);
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected Decimal value"),
        }
    }

    #[test]
    fn test_decode_decimal_fixed() {
        // Decimal with precision 10, scale 2, fixed size 4
        // Unscaled value: 12345 as big-endian bytes (padded to 4 bytes)
        let data: &[u8] = &[0x00, 0x00, 0x30, 0x39];
        let mut cursor = data;
        let result = decode_decimal_fixed(&mut cursor, 4, 10, 2).unwrap();

        match result {
            AvroValue::Decimal {
                unscaled,
                precision,
                scale,
            } => {
                assert_eq!(unscaled, vec![0x00, 0x00, 0x30, 0x39]);
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected Decimal value"),
        }
    }

    #[test]
    fn test_decode_uuid_string() {
        // UUID as string: "550e8400-e29b-41d4-a716-446655440000"
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let len = uuid_str.len() as i64;
        let zigzag_len = ((len << 1) ^ (len >> 63)) as u8; // 72 -> 0x48

        let mut data = vec![zigzag_len];
        data.extend_from_slice(uuid_str.as_bytes());

        let mut cursor = &data[..];
        let result = decode_uuid_string(&mut cursor).unwrap();

        match result {
            AvroValue::Uuid(s) => {
                assert_eq!(s, uuid_str);
            }
            _ => panic!("Expected Uuid value"),
        }
    }

    #[test]
    fn test_decode_uuid_fixed() {
        // UUID as fixed 16 bytes
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];

        let mut cursor = &uuid_bytes[..];
        let result = decode_uuid_fixed(&mut cursor).unwrap();

        match result {
            AvroValue::Uuid(s) => {
                assert_eq!(s, "550e8400-e29b-41d4-a716-446655440000");
            }
            _ => panic!("Expected Uuid value"),
        }
    }

    #[test]
    fn test_decode_date() {
        // Date: 100 days since Unix epoch
        // 100 -> zigzag(100) = 200 -> 0xC8 0x01
        let data: &[u8] = &[0xC8, 0x01];
        let mut cursor = data;
        let result = decode_date(&mut cursor).unwrap();

        match result {
            AvroValue::Date(days) => {
                assert_eq!(days, 100);
            }
            _ => panic!("Expected Date value"),
        }
    }

    #[test]
    fn test_decode_time_millis() {
        // Time: 1000 milliseconds (1 second)
        // 1000 -> zigzag(1000) = 2000 -> 0xD0 0x0F
        let data: &[u8] = &[0xD0, 0x0F];
        let mut cursor = data;
        let result = decode_time_millis(&mut cursor).unwrap();

        match result {
            AvroValue::TimeMillis(millis) => {
                assert_eq!(millis, 1000);
            }
            _ => panic!("Expected TimeMillis value"),
        }
    }

    #[test]
    fn test_decode_time_millis_invalid() {
        // Invalid time: 86400000 (24 hours, out of range)
        // 86400000 -> zigzag(86400000) = 172800000
        let data: &[u8] = &[0x80, 0xC0, 0xE8, 0x52];
        let mut cursor = data;
        assert!(matches!(
            decode_time_millis(&mut cursor),
            Err(DecodeError::InvalidData(_))
        ));
    }

    #[test]
    fn test_decode_time_micros() {
        // Time: 1000000 microseconds (1 second)
        // 1000000 -> zigzag(1000000) = 2000000 -> varint
        let data: &[u8] = &[0x80, 0x89, 0x7A];
        let mut cursor = data;
        let result = decode_time_micros(&mut cursor).unwrap();

        match result {
            AvroValue::TimeMicros(micros) => {
                assert_eq!(micros, 1000000);
            }
            _ => panic!("Expected TimeMicros value"),
        }
    }

    #[test]
    fn test_decode_time_micros_invalid() {
        // Invalid time: 86400000000 (24 hours, out of range)
        let data: &[u8] = &[0x80, 0x80, 0xA0, 0xD0, 0xA8, 0x50];
        let mut cursor = data;
        assert!(matches!(
            decode_time_micros(&mut cursor),
            Err(DecodeError::InvalidData(_))
        ));
    }

    #[test]
    fn test_decode_timestamp_millis() {
        // Timestamp: 1000000 milliseconds since epoch
        // 1000000 -> zigzag(1000000) = 2000000 -> varint
        let data: &[u8] = &[0x80, 0x89, 0x7A];
        let mut cursor = data;
        let result = decode_timestamp_millis(&mut cursor).unwrap();

        match result {
            AvroValue::TimestampMillis(millis) => {
                assert_eq!(millis, 1000000);
            }
            _ => panic!("Expected TimestampMillis value"),
        }
    }

    #[test]
    fn test_decode_timestamp_micros() {
        // Timestamp: 1000000000 microseconds since epoch
        // 1000000000 -> zigzag(1000000000) = 2000000000 -> varint
        // 2000000000 = 0x80 0xa8 0xd6 0xb9 0x07
        let data: &[u8] = &[0x80, 0xa8, 0xd6, 0xb9, 0x07];
        let mut cursor = data;
        let result = decode_timestamp_micros(&mut cursor).unwrap();

        match result {
            AvroValue::TimestampMicros(micros) => {
                assert_eq!(micros, 1000000000);
            }
            _ => panic!("Expected TimestampMicros value"),
        }
    }

    #[test]
    fn test_decode_duration() {
        // Duration: 1 month, 2 days, 3000 milliseconds
        // Stored as 3 little-endian u32 values (12 bytes total)
        let mut data = vec![];
        data.extend_from_slice(&1u32.to_le_bytes()); // months
        data.extend_from_slice(&2u32.to_le_bytes()); // days
        data.extend_from_slice(&3000u32.to_le_bytes()); // milliseconds

        let mut cursor = &data[..];
        let result = decode_duration(&mut cursor).unwrap();

        match result {
            AvroValue::Duration {
                months,
                days,
                milliseconds,
            } => {
                assert_eq!(months, 1);
                assert_eq!(days, 2);
                assert_eq!(milliseconds, 3000);
            }
            _ => panic!("Expected Duration value"),
        }
    }

    #[test]
    fn test_decode_logical_value_decimal() {
        use crate::schema::LogicalTypeName;

        // Test decimal with bytes base
        let data: &[u8] = &[0x04, 0x30, 0x39]; // length 2, then bytes
        let mut cursor = data;
        let result = decode_logical_value(
            &mut cursor,
            &LogicalTypeName::Decimal {
                precision: 10,
                scale: 2,
            },
            &AvroSchema::Bytes,
        )
        .unwrap();

        match result {
            AvroValue::Decimal {
                precision, scale, ..
            } => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected Decimal value"),
        }
    }

    #[test]
    fn test_decode_logical_value_uuid() {
        use crate::schema::LogicalTypeName;

        // Test UUID with string base
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let len = uuid_str.len() as i64;
        let zigzag_len = ((len << 1) ^ (len >> 63)) as u8;

        let mut data = vec![zigzag_len];
        data.extend_from_slice(uuid_str.as_bytes());

        let mut cursor = &data[..];
        let result =
            decode_logical_value(&mut cursor, &LogicalTypeName::Uuid, &AvroSchema::String).unwrap();

        match result {
            AvroValue::Uuid(s) => {
                assert_eq!(s, uuid_str);
            }
            _ => panic!("Expected Uuid value"),
        }
    }

    #[test]
    fn test_decode_logical_value_date() {
        use crate::schema::LogicalTypeName;

        // 100 days -> zigzag(100) = 200 -> 0xC8 0x01
        let data: &[u8] = &[0xC8, 0x01];
        let mut cursor = data;
        let result =
            decode_logical_value(&mut cursor, &LogicalTypeName::Date, &AvroSchema::Int).unwrap();

        match result {
            AvroValue::Date(days) => {
                assert_eq!(days, 100);
            }
            _ => panic!("Expected Date value"),
        }
    }

    #[test]
    fn test_decode_logical_value_time_millis() {
        use crate::schema::LogicalTypeName;

        // 1000 milliseconds -> zigzag(1000) = 2000 -> 0xD0 0x0F
        let data: &[u8] = &[0xD0, 0x0F];
        let mut cursor = data;
        let result =
            decode_logical_value(&mut cursor, &LogicalTypeName::TimeMillis, &AvroSchema::Int)
                .unwrap();

        match result {
            AvroValue::TimeMillis(millis) => {
                assert_eq!(millis, 1000);
            }
            _ => panic!("Expected TimeMillis value"),
        }
    }

    #[test]
    fn test_decode_logical_value_time_micros() {
        use crate::schema::LogicalTypeName;

        // 1000000 microseconds -> zigzag(1000000) = 2000000 -> varint
        let data: &[u8] = &[0x80, 0x89, 0x7A];
        let mut cursor = data;
        let result =
            decode_logical_value(&mut cursor, &LogicalTypeName::TimeMicros, &AvroSchema::Long)
                .unwrap();

        match result {
            AvroValue::TimeMicros(micros) => {
                assert_eq!(micros, 1000000);
            }
            _ => panic!("Expected TimeMicros value"),
        }
    }

    #[test]
    fn test_decode_logical_value_timestamp_millis() {
        use crate::schema::LogicalTypeName;

        // 1000000 milliseconds -> zigzag(1000000) = 2000000 -> varint
        let data: &[u8] = &[0x80, 0x89, 0x7A];
        let mut cursor = data;
        let result = decode_logical_value(
            &mut cursor,
            &LogicalTypeName::TimestampMillis,
            &AvroSchema::Long,
        )
        .unwrap();

        match result {
            AvroValue::TimestampMillis(millis) => {
                assert_eq!(millis, 1000000);
            }
            _ => panic!("Expected TimestampMillis value"),
        }
    }

    #[test]
    fn test_decode_logical_value_timestamp_micros() {
        use crate::schema::LogicalTypeName;

        // 1000000000 microseconds -> zigzag(1000000000) = 2000000000 -> varint
        // 2000000000 = 0x80 0xa8 0xd6 0xb9 0x07
        let data: &[u8] = &[0x80, 0xa8, 0xd6, 0xb9, 0x07];
        let mut cursor = data;
        let result = decode_logical_value(
            &mut cursor,
            &LogicalTypeName::TimestampMicros,
            &AvroSchema::Long,
        )
        .unwrap();

        match result {
            AvroValue::TimestampMicros(micros) => {
                assert_eq!(micros, 1000000000);
            }
            _ => panic!("Expected TimestampMicros value"),
        }
    }

    #[test]
    fn test_decode_logical_value_duration() {
        use crate::schema::{FixedSchema, LogicalTypeName};

        let mut data = vec![];
        data.extend_from_slice(&1u32.to_le_bytes()); // months
        data.extend_from_slice(&2u32.to_le_bytes()); // days
        data.extend_from_slice(&3000u32.to_le_bytes()); // milliseconds

        let mut cursor = &data[..];
        let result = decode_logical_value(
            &mut cursor,
            &LogicalTypeName::Duration,
            &AvroSchema::Fixed(FixedSchema::new("duration", 12)),
        )
        .unwrap();

        match result {
            AvroValue::Duration {
                months,
                days,
                milliseconds,
            } => {
                assert_eq!(months, 1);
                assert_eq!(days, 2);
                assert_eq!(milliseconds, 3000);
            }
            _ => panic!("Expected Duration value"),
        }
    }

    #[test]
    fn test_decode_value_with_logical_type() {
        use crate::schema::{LogicalType, LogicalTypeName};

        // Test that decode_value properly handles logical types
        let schema = AvroSchema::Logical(LogicalType::new(AvroSchema::Int, LogicalTypeName::Date));

        let data: &[u8] = &[0xC8, 0x01]; // 100 days
        let mut cursor = data;
        let result = decode_value(&mut cursor, &schema).unwrap();

        match result {
            AvroValue::Date(days) => {
                assert_eq!(days, 100);
            }
            _ => panic!("Expected Date value, got {:?}", result),
        }
    }

    #[test]
    fn test_decode_logical_value_invalid_base_type() {
        use crate::schema::LogicalTypeName;

        // Try to decode a date with wrong base type (should be int, not long)
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        let result = decode_logical_value(&mut cursor, &LogicalTypeName::Date, &AvroSchema::Long);

        assert!(matches!(result, Err(DecodeError::InvalidData(_))));
    }

    // ========================================================================
    // decode_value_with_context tests (named type resolution during decoding)
    // ========================================================================

    #[test]
    fn test_decode_value_with_context_resolves_named_type() {
        use crate::schema::{FieldSchema, RecordSchema, SchemaResolutionContext};

        // Create a simple record schema
        let inner_record =
            RecordSchema::new("Inner", vec![FieldSchema::new("value", AvroSchema::Int)])
                .with_namespace("com.example");

        // Build resolution context
        let mut context = SchemaResolutionContext::new();
        context.register(
            "com.example.Inner".to_string(),
            AvroSchema::Record(inner_record),
        );

        // Create a Named reference to the inner record
        let named_schema = AvroSchema::Named("com.example.Inner".to_string());

        // Encode a record: {"value": 42}
        // 42 -> zigzag(42) = 84 -> 0x54
        let data: &[u8] = &[0x54];
        let mut cursor = data;

        // Decode using context - should resolve Named to the actual record
        let result = decode_value_with_context(&mut cursor, &named_schema, &context).unwrap();

        match result {
            AvroValue::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0, "value");
                assert_eq!(fields[0].1, AvroValue::Int(42));
            }
            _ => panic!("Expected Record value, got {:?}", result),
        }
    }

    #[test]
    fn test_decode_value_with_context_nested_named_types() {
        use crate::schema::{FieldSchema, RecordSchema, SchemaResolutionContext};

        // Create inner record
        let inner_record =
            RecordSchema::new("Inner", vec![FieldSchema::new("id", AvroSchema::Long)])
                .with_namespace("test");

        // Create outer record that references inner by name
        let outer_record = RecordSchema::new(
            "Outer",
            vec![
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("inner", AvroSchema::Named("test.Inner".to_string())),
            ],
        )
        .with_namespace("test");

        // Build context with both records
        let mut context = SchemaResolutionContext::new();
        context.register("test.Inner".to_string(), AvroSchema::Record(inner_record));
        context.register(
            "test.Outer".to_string(),
            AvroSchema::Record(outer_record.clone()),
        );

        // Encode: {"name": "test", "inner": {"id": 100}}
        // "test": len 4 -> 8, then "test"
        // 100 -> zigzag(100) = 200 -> 0xC8 0x01
        let data: &[u8] = &[0x08, b't', b'e', b's', b't', 0xC8, 0x01];
        let mut cursor = data;

        let result =
            decode_value_with_context(&mut cursor, &AvroSchema::Record(outer_record), &context)
                .unwrap();

        match result {
            AvroValue::Record(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "name");
                assert_eq!(fields[0].1, AvroValue::String("test".to_string()));
                assert_eq!(fields[1].0, "inner");
                match &fields[1].1 {
                    AvroValue::Record(inner_fields) => {
                        assert_eq!(inner_fields.len(), 1);
                        assert_eq!(inner_fields[0].0, "id");
                        assert_eq!(inner_fields[0].1, AvroValue::Long(100));
                    }
                    _ => panic!("Expected Record for inner field"),
                }
            }
            _ => panic!("Expected Record value"),
        }
    }

    #[test]
    fn test_decode_value_with_context_named_in_union() {
        use crate::schema::{FieldSchema, RecordSchema, SchemaResolutionContext};

        // Create a record that will be referenced
        let item_record =
            RecordSchema::new("Item", vec![FieldSchema::new("count", AvroSchema::Int)]);

        // Build context
        let mut context = SchemaResolutionContext::new();
        context.register("Item".to_string(), AvroSchema::Record(item_record));

        // Create a union with null and Named reference
        let union_schema = AvroSchema::Union(vec![
            AvroSchema::Null,
            AvroSchema::Named("Item".to_string()),
        ]);

        // Test decoding null variant
        let data: &[u8] = &[0x00]; // union index 0 (null)
        let mut cursor = data;
        let result = decode_value_with_context(&mut cursor, &union_schema, &context).unwrap();
        match result {
            AvroValue::Union(0, value) => {
                assert_eq!(*value, AvroValue::Null);
            }
            _ => panic!("Expected Union with Null"),
        }

        // Test decoding record variant
        // union index 1 -> 2, then record {"count": 5}
        // 5 -> zigzag(5) = 10 -> 0x0A
        let data: &[u8] = &[0x02, 0x0A];
        let mut cursor = data;
        let result = decode_value_with_context(&mut cursor, &union_schema, &context).unwrap();
        match result {
            AvroValue::Union(1, value) => match *value {
                AvroValue::Record(fields) => {
                    assert_eq!(fields.len(), 1);
                    assert_eq!(fields[0].1, AvroValue::Int(5));
                }
                _ => panic!("Expected Record in union"),
            },
            _ => panic!("Expected Union with Record"),
        }
    }

    #[test]
    fn test_decode_value_with_context_named_in_array() {
        use crate::schema::{FieldSchema, RecordSchema, SchemaResolutionContext};

        // Create a record
        let point_record = RecordSchema::new(
            "Point",
            vec![
                FieldSchema::new("x", AvroSchema::Int),
                FieldSchema::new("y", AvroSchema::Int),
            ],
        );

        // Build context
        let mut context = SchemaResolutionContext::new();
        context.register("Point".to_string(), AvroSchema::Record(point_record));

        // Create array of Named references
        let array_schema = AvroSchema::Array(Box::new(AvroSchema::Named("Point".to_string())));

        // Encode array with 2 points: [{x: 1, y: 2}, {x: 3, y: 4}]
        // count: 2 -> 4
        // point 1: x=1 -> 2, y=2 -> 4
        // point 2: x=3 -> 6, y=4 -> 8
        // end: 0
        let data: &[u8] = &[0x04, 0x02, 0x04, 0x06, 0x08, 0x00];
        let mut cursor = data;

        let result = decode_value_with_context(&mut cursor, &array_schema, &context).unwrap();
        match result {
            AvroValue::Array(items) => {
                assert_eq!(items.len(), 2);
                match &items[0] {
                    AvroValue::Record(fields) => {
                        assert_eq!(fields[0].1, AvroValue::Int(1));
                        assert_eq!(fields[1].1, AvroValue::Int(2));
                    }
                    _ => panic!("Expected Record"),
                }
                match &items[1] {
                    AvroValue::Record(fields) => {
                        assert_eq!(fields[0].1, AvroValue::Int(3));
                        assert_eq!(fields[1].1, AvroValue::Int(4));
                    }
                    _ => panic!("Expected Record"),
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_decode_value_with_context_named_in_map() {
        use crate::schema::{FieldSchema, RecordSchema, SchemaResolutionContext};

        // Create a record
        let config_record = RecordSchema::new(
            "Config",
            vec![FieldSchema::new("enabled", AvroSchema::Boolean)],
        );

        // Build context
        let mut context = SchemaResolutionContext::new();
        context.register("Config".to_string(), AvroSchema::Record(config_record));

        // Create map with Named value type
        let map_schema = AvroSchema::Map(Box::new(AvroSchema::Named("Config".to_string())));

        // Encode map: {"feature": {enabled: true}}
        // count: 1 -> 2
        // key "feature": len 7 -> 14, then "feature"
        // value: enabled=true -> 0x01
        // end: 0
        let data: &[u8] = &[
            0x02, 0x0E, b'f', b'e', b'a', b't', b'u', b'r', b'e', 0x01, 0x00,
        ];
        let mut cursor = data;

        let result = decode_value_with_context(&mut cursor, &map_schema, &context).unwrap();
        match result {
            AvroValue::Map(entries) => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].0, "feature");
                match &entries[0].1 {
                    AvroValue::Record(fields) => {
                        assert_eq!(fields[0].1, AvroValue::Boolean(true));
                    }
                    _ => panic!("Expected Record"),
                }
            }
            _ => panic!("Expected Map"),
        }
    }

    #[test]
    fn test_decode_value_with_context_unresolved_named_error() {
        use crate::schema::SchemaResolutionContext;

        // Empty context - no types registered
        let context = SchemaResolutionContext::new();

        // Try to decode a Named reference that doesn't exist
        let named_schema = AvroSchema::Named("NonExistent".to_string());
        let data: &[u8] = &[0x00];
        let mut cursor = data;

        let result = decode_value_with_context(&mut cursor, &named_schema, &context);
        assert!(matches!(result, Err(DecodeError::InvalidData(_))));
    }

    #[test]
    fn test_skip_value_with_context_named_type() {
        use crate::schema::{FieldSchema, RecordSchema, SchemaResolutionContext};

        // Create a record
        let record = RecordSchema::new(
            "Data",
            vec![
                FieldSchema::new("a", AvroSchema::Int),
                FieldSchema::new("b", AvroSchema::String),
            ],
        );

        // Build context
        let mut context = SchemaResolutionContext::new();
        context.register("Data".to_string(), AvroSchema::Record(record));

        // Create Named reference
        let named_schema = AvroSchema::Named("Data".to_string());

        // Encode: {a: 10, b: "hi"} followed by extra byte
        // a: 10 -> 20 -> 0x14
        // b: "hi" -> len 2 -> 4, then "hi"
        let data: &[u8] = &[0x14, 0x04, b'h', b'i', 0xFF];
        let mut cursor = data;

        // Skip the named type
        skip_value_with_context(&mut cursor, &named_schema, &context).unwrap();

        // Should have consumed exactly the record, leaving 0xFF
        assert_eq!(cursor, &[0xFF]);
    }
}
