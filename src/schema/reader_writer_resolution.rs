//! Schema resolution for reader/writer schema evolution.
//!
//! This module implements schema resolution during decoding per the Avro specification.
//! It handles:
//! - Field defaults for missing fields
//! - Field reordering
//! - Type promotions (int→long, float→double, etc.)
//!
//! # Requirements
//! - 9.2: WHERE an external reader schema is provided, THE Avro_Reader SHALL use
//!   schema resolution rules per Avro spec

use std::collections::HashMap;

use serde_json::Value;

use crate::error::{DecodeError, SchemaError};
use crate::schema::{AvroSchema, FieldSchema, LogicalTypeName, RecordSchema};

/// A resolved field mapping from writer to reader schema.
///
/// This struct describes how to decode a single field when the reader
/// and writer schemas differ.
#[derive(Debug, Clone)]
pub enum ResolvedField {
    /// Field exists in both schemas - decode from writer, possibly with promotion
    Present {
        /// Index of the field in the writer schema
        writer_index: usize,
        /// The writer field schema
        writer_schema: AvroSchema,
        /// The reader field schema
        reader_schema: AvroSchema,
        /// Type promotion to apply (if any)
        promotion: Option<TypePromotion>,
    },
    /// Field missing in writer - use default value
    Default {
        /// The default value from the reader schema
        default_value: Value,
        /// The reader field schema
        reader_schema: AvroSchema,
    },
}

/// Type promotions supported by Avro schema resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypePromotion {
    /// int → long
    IntToLong,
    /// int → float
    IntToFloat,
    /// int → double
    IntToDouble,
    /// long → float
    LongToFloat,
    /// long → double
    LongToDouble,
    /// float → double
    FloatToDouble,
    /// string → bytes
    StringToBytes,
    /// bytes → string
    BytesToString,
}

impl TypePromotion {
    /// Determine the type promotion needed between writer and reader types.
    ///
    /// Returns `None` if no promotion is needed (types are identical),
    /// `Some(promotion)` if a valid promotion exists, or an error if
    /// the types are incompatible.
    pub fn from_schemas(
        writer: &AvroSchema,
        reader: &AvroSchema,
    ) -> Result<Option<Self>, SchemaError> {
        // Strip logical types for comparison
        let writer_base = match writer {
            AvroSchema::Logical(l) => &l.base,
            _ => writer,
        };
        let reader_base = match reader {
            AvroSchema::Logical(l) => &l.base,
            _ => reader,
        };

        match (writer_base, reader_base) {
            // Same types - no promotion needed
            (AvroSchema::Null, AvroSchema::Null)
            | (AvroSchema::Boolean, AvroSchema::Boolean)
            | (AvroSchema::Int, AvroSchema::Int)
            | (AvroSchema::Long, AvroSchema::Long)
            | (AvroSchema::Float, AvroSchema::Float)
            | (AvroSchema::Double, AvroSchema::Double)
            | (AvroSchema::Bytes, AvroSchema::Bytes)
            | (AvroSchema::String, AvroSchema::String) => Ok(None),

            // int promotions
            (AvroSchema::Int, AvroSchema::Long) => Ok(Some(TypePromotion::IntToLong)),
            (AvroSchema::Int, AvroSchema::Float) => Ok(Some(TypePromotion::IntToFloat)),
            (AvroSchema::Int, AvroSchema::Double) => Ok(Some(TypePromotion::IntToDouble)),

            // long promotions
            (AvroSchema::Long, AvroSchema::Float) => Ok(Some(TypePromotion::LongToFloat)),
            (AvroSchema::Long, AvroSchema::Double) => Ok(Some(TypePromotion::LongToDouble)),

            // float promotion
            (AvroSchema::Float, AvroSchema::Double) => Ok(Some(TypePromotion::FloatToDouble)),

            // string/bytes interchangeable
            (AvroSchema::String, AvroSchema::Bytes) => Ok(Some(TypePromotion::StringToBytes)),
            (AvroSchema::Bytes, AvroSchema::String) => Ok(Some(TypePromotion::BytesToString)),

            // Complex types - check recursively
            (AvroSchema::Record(_), AvroSchema::Record(_))
            | (AvroSchema::Enum(_), AvroSchema::Enum(_))
            | (AvroSchema::Array(_), AvroSchema::Array(_))
            | (AvroSchema::Map(_), AvroSchema::Map(_))
            | (AvroSchema::Union(_), AvroSchema::Union(_))
            | (AvroSchema::Fixed(_), AvroSchema::Fixed(_)) => Ok(None),

            // Incompatible types
            _ => Err(SchemaError::IncompatibleSchemas(format!(
                "Cannot promote {:?} to {:?}",
                writer_base, reader_base
            ))),
        }
    }
}

/// Schema resolution context for reader/writer schema evolution.
///
/// This struct holds the mapping between writer and reader schemas,
/// enabling efficient decoding with schema evolution.
#[derive(Debug, Clone)]
pub struct ReaderWriterResolution {
    /// The writer schema (from the file)
    pub writer_schema: RecordSchema,
    /// The reader schema (provided by the user)
    pub reader_schema: RecordSchema,
    /// Resolved field mappings in reader schema order
    pub resolved_fields: Vec<ResolvedField>,
    /// Indices of writer fields to skip (not in reader schema)
    pub writer_fields_to_skip: Vec<usize>,
}

impl ReaderWriterResolution {
    /// Create a new schema resolution from writer and reader schemas.
    ///
    /// This validates compatibility and builds the field mapping.
    ///
    /// # Arguments
    /// * `writer_schema` - The schema used to write the data
    /// * `reader_schema` - The schema to use for reading
    ///
    /// # Returns
    /// A resolution context, or an error if schemas are incompatible.
    pub fn new(
        writer_schema: &RecordSchema,
        reader_schema: &RecordSchema,
    ) -> Result<Self, SchemaError> {
        // Build a map of writer fields by name (including aliases)
        let writer_fields_by_name: HashMap<&str, (usize, &FieldSchema)> = writer_schema
            .fields
            .iter()
            .enumerate()
            .flat_map(|(idx, field)| {
                let mut names = vec![(field.name.as_str(), (idx, field))];
                for alias in &field.aliases {
                    names.push((alias.as_str(), (idx, field)));
                }
                names
            })
            .collect();

        let mut resolved_fields = Vec::with_capacity(reader_schema.fields.len());
        let mut used_writer_indices = std::collections::HashSet::new();

        // Resolve each reader field
        for reader_field in &reader_schema.fields {
            // Try to find matching writer field by name or alias
            let writer_match = find_matching_writer_field(
                &reader_field.name,
                &reader_field.aliases,
                &writer_fields_by_name,
            );

            match writer_match {
                Some((writer_idx, writer_field)) => {
                    // Field exists in both schemas
                    used_writer_indices.insert(writer_idx);

                    // Check type compatibility and determine promotion
                    let promotion =
                        TypePromotion::from_schemas(&writer_field.schema, &reader_field.schema)?;

                    resolved_fields.push(ResolvedField::Present {
                        writer_index: writer_idx,
                        writer_schema: writer_field.schema.clone(),
                        reader_schema: reader_field.schema.clone(),
                        promotion,
                    });
                }
                None => {
                    // Field not in writer - must have default
                    match &reader_field.default {
                        Some(default_value) => {
                            resolved_fields.push(ResolvedField::Default {
                                default_value: default_value.clone(),
                                reader_schema: reader_field.schema.clone(),
                            });
                        }
                        None => {
                            return Err(SchemaError::IncompatibleSchemas(format!(
                                "Reader field '{}' not in writer schema and has no default",
                                reader_field.name
                            )));
                        }
                    }
                }
            }
        }

        // Determine which writer fields to skip (not in reader schema)
        let writer_fields_to_skip: Vec<usize> = (0..writer_schema.fields.len())
            .filter(|idx| !used_writer_indices.contains(idx))
            .collect();

        Ok(Self {
            writer_schema: writer_schema.clone(),
            reader_schema: reader_schema.clone(),
            resolved_fields,
            writer_fields_to_skip,
        })
    }

    /// Check if any field reordering is needed.
    ///
    /// Returns true if the writer fields appear in a different order
    /// than the reader expects them.
    pub fn needs_reordering(&self) -> bool {
        let mut last_writer_idx = None;
        for field in &self.resolved_fields {
            if let ResolvedField::Present { writer_index, .. } = field {
                if let Some(last) = last_writer_idx {
                    if *writer_index < last {
                        return true;
                    }
                }
                last_writer_idx = Some(*writer_index);
            }
        }
        false
    }

    /// Check if any type promotions are needed.
    pub fn needs_promotions(&self) -> bool {
        self.resolved_fields.iter().any(|f| {
            matches!(
                f,
                ResolvedField::Present {
                    promotion: Some(_),
                    ..
                }
            )
        })
    }

    /// Check if any default values are needed.
    pub fn needs_defaults(&self) -> bool {
        self.resolved_fields
            .iter()
            .any(|f| matches!(f, ResolvedField::Default { .. }))
    }
}

/// Find a matching writer field by name or alias.
fn find_matching_writer_field<'a>(
    name: &str,
    aliases: &[String],
    writer_fields: &HashMap<&str, (usize, &'a FieldSchema)>,
) -> Option<(usize, &'a FieldSchema)> {
    // Try direct name match
    if let Some(&(idx, field)) = writer_fields.get(name) {
        return Some((idx, field));
    }

    // Try aliases
    for alias in aliases {
        if let Some(&(idx, field)) = writer_fields.get(alias.as_str()) {
            return Some((idx, field));
        }
    }

    None
}

/// Apply a type promotion to a decoded value.
///
/// This function converts a value from the writer type to the reader type
/// according to Avro's promotion rules.
pub fn apply_promotion(
    value: crate::reader::decode::AvroValue,
    promotion: TypePromotion,
) -> Result<crate::reader::decode::AvroValue, DecodeError> {
    use crate::reader::decode::AvroValue;

    match (value, promotion) {
        // int → long
        (AvroValue::Int(v), TypePromotion::IntToLong) => Ok(AvroValue::Long(v as i64)),

        // int → float
        (AvroValue::Int(v), TypePromotion::IntToFloat) => Ok(AvroValue::Float(v as f32)),

        // int → double
        (AvroValue::Int(v), TypePromotion::IntToDouble) => Ok(AvroValue::Double(v as f64)),

        // long → float
        (AvroValue::Long(v), TypePromotion::LongToFloat) => Ok(AvroValue::Float(v as f32)),

        // long → double
        (AvroValue::Long(v), TypePromotion::LongToDouble) => Ok(AvroValue::Double(v as f64)),

        // float → double
        (AvroValue::Float(v), TypePromotion::FloatToDouble) => Ok(AvroValue::Double(v as f64)),

        // string → bytes
        (AvroValue::String(s), TypePromotion::StringToBytes) => {
            Ok(AvroValue::Bytes(s.into_bytes()))
        }

        // bytes → string
        (AvroValue::Bytes(b), TypePromotion::BytesToString) => {
            let s = String::from_utf8(b).map_err(|e| {
                DecodeError::InvalidData(format!("Cannot convert bytes to string: {}", e))
            })?;
            Ok(AvroValue::String(s))
        }

        // Mismatched promotion
        (value, promotion) => Err(DecodeError::TypeMismatch(format!(
            "Cannot apply {:?} promotion to {:?}",
            promotion, value
        ))),
    }
}

/// Convert a JSON default value to an AvroValue.
///
/// This is used when a reader field has a default value and the
/// corresponding writer field is missing.
pub fn json_to_avro_value(
    json: &Value,
    schema: &AvroSchema,
) -> Result<crate::reader::decode::AvroValue, DecodeError> {
    use crate::reader::decode::AvroValue;

    match (json, schema) {
        // Null
        (Value::Null, AvroSchema::Null) => Ok(AvroValue::Null),

        // Boolean
        (Value::Bool(b), AvroSchema::Boolean) => Ok(AvroValue::Boolean(*b)),

        // Integer types
        (Value::Number(n), AvroSchema::Int) => {
            let v = n
                .as_i64()
                .ok_or_else(|| DecodeError::InvalidData(format!("Cannot convert {} to int", n)))?;
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                return Err(DecodeError::InvalidData(format!(
                    "Value {} out of range for int",
                    v
                )));
            }
            Ok(AvroValue::Int(v as i32))
        }
        (Value::Number(n), AvroSchema::Long) => {
            let v = n
                .as_i64()
                .ok_or_else(|| DecodeError::InvalidData(format!("Cannot convert {} to long", n)))?;
            Ok(AvroValue::Long(v))
        }

        // Float types
        (Value::Number(n), AvroSchema::Float) => {
            let v = n.as_f64().ok_or_else(|| {
                DecodeError::InvalidData(format!("Cannot convert {} to float", n))
            })?;
            Ok(AvroValue::Float(v as f32))
        }
        (Value::Number(n), AvroSchema::Double) => {
            let v = n.as_f64().ok_or_else(|| {
                DecodeError::InvalidData(format!("Cannot convert {} to double", n))
            })?;
            Ok(AvroValue::Double(v))
        }

        // String
        (Value::String(s), AvroSchema::String) => Ok(AvroValue::String(s.clone())),

        // Bytes (encoded as string in JSON)
        (Value::String(s), AvroSchema::Bytes) => {
            // Avro encodes bytes as ISO-8859-1 string in JSON
            Ok(AvroValue::Bytes(s.bytes().collect()))
        }

        // Fixed (encoded as string in JSON)
        (Value::String(s), AvroSchema::Fixed(fixed)) => {
            let bytes: Vec<u8> = s.bytes().collect();
            if bytes.len() != fixed.size {
                return Err(DecodeError::InvalidData(format!(
                    "Fixed default has wrong size: expected {}, got {}",
                    fixed.size,
                    bytes.len()
                )));
            }
            Ok(AvroValue::Fixed(bytes))
        }

        // Enum (string symbol)
        (Value::String(s), AvroSchema::Enum(enum_schema)) => {
            let index = enum_schema.symbol_index(s).ok_or_else(|| {
                DecodeError::InvalidData(format!(
                    "Unknown enum symbol '{}' for enum '{}'",
                    s, enum_schema.name
                ))
            })?;
            Ok(AvroValue::Enum(index as i32, s.clone()))
        }

        // Array
        (Value::Array(arr), AvroSchema::Array(item_schema)) => {
            let items: Result<Vec<AvroValue>, DecodeError> = arr
                .iter()
                .map(|item| json_to_avro_value(item, item_schema))
                .collect();
            Ok(AvroValue::Array(items?))
        }

        // Map
        (Value::Object(obj), AvroSchema::Map(value_schema)) => {
            let entries: Result<Vec<(String, AvroValue)>, DecodeError> = obj
                .iter()
                .map(|(k, v)| {
                    let value = json_to_avro_value(v, value_schema)?;
                    Ok((k.clone(), value))
                })
                .collect();
            Ok(AvroValue::Map(entries?))
        }

        // Record
        (Value::Object(obj), AvroSchema::Record(record_schema)) => {
            let fields: Result<Vec<(String, AvroValue)>, DecodeError> = record_schema
                .fields
                .iter()
                .map(|field| {
                    let value = match obj.get(&field.name) {
                        Some(v) => json_to_avro_value(v, &field.schema)?,
                        None => match &field.default {
                            Some(default) => json_to_avro_value(default, &field.schema)?,
                            None => {
                                return Err(DecodeError::InvalidData(format!(
                                    "Missing required field '{}' in default value",
                                    field.name
                                )));
                            }
                        },
                    };
                    Ok((field.name.clone(), value))
                })
                .collect();
            Ok(AvroValue::Record(fields?))
        }

        // Union - the default value must match the first variant
        (json, AvroSchema::Union(variants)) => {
            // For unions, the default value is for the first variant
            if variants.is_empty() {
                return Err(DecodeError::InvalidData("Empty union".to_string()));
            }

            // Try to match the JSON value to the first variant
            let first_variant = &variants[0];
            let value = json_to_avro_value(json, first_variant)?;
            Ok(AvroValue::Union(0, Box::new(value)))
        }

        // Logical types - delegate to base type
        (json, AvroSchema::Logical(logical)) => {
            let base_value = json_to_avro_value(json, &logical.base)?;
            // Convert to appropriate logical type value
            convert_to_logical_value(base_value, &logical.logical_type)
        }

        // Type mismatch
        (json, schema) => Err(DecodeError::InvalidData(format!(
            "Cannot convert JSON {:?} to schema {:?}",
            json, schema
        ))),
    }
}

/// Convert a base value to a logical type value.
fn convert_to_logical_value(
    value: crate::reader::decode::AvroValue,
    logical_type: &LogicalTypeName,
) -> Result<crate::reader::decode::AvroValue, DecodeError> {
    use crate::reader::decode::AvroValue;

    match (value, logical_type) {
        (AvroValue::Int(days), LogicalTypeName::Date) => Ok(AvroValue::Date(days)),
        (AvroValue::Int(millis), LogicalTypeName::TimeMillis) => Ok(AvroValue::TimeMillis(millis)),
        (AvroValue::Long(micros), LogicalTypeName::TimeMicros) => Ok(AvroValue::TimeMicros(micros)),
        (AvroValue::Long(millis), LogicalTypeName::TimestampMillis) => {
            Ok(AvroValue::TimestampMillis(millis))
        }
        (AvroValue::Long(micros), LogicalTypeName::TimestampMicros) => {
            Ok(AvroValue::TimestampMicros(micros))
        }
        (AvroValue::Long(millis), LogicalTypeName::LocalTimestampMillis) => {
            Ok(AvroValue::TimestampMillis(millis))
        }
        (AvroValue::Long(micros), LogicalTypeName::LocalTimestampMicros) => {
            Ok(AvroValue::TimestampMicros(micros))
        }
        (AvroValue::String(s), LogicalTypeName::Uuid) => Ok(AvroValue::Uuid(s)),
        (AvroValue::Bytes(b), LogicalTypeName::Decimal { precision, scale }) => {
            Ok(AvroValue::Decimal {
                unscaled: b,
                precision: *precision,
                scale: *scale,
            })
        }
        (AvroValue::Fixed(b), LogicalTypeName::Decimal { precision, scale }) => {
            Ok(AvroValue::Decimal {
                unscaled: b,
                precision: *precision,
                scale: *scale,
            })
        }
        (AvroValue::Fixed(b), LogicalTypeName::Duration) if b.len() == 12 => {
            let months = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            let days = u32::from_le_bytes([b[4], b[5], b[6], b[7]]);
            let milliseconds = u32::from_le_bytes([b[8], b[9], b[10], b[11]]);
            Ok(AvroValue::Duration {
                months,
                days,
                milliseconds,
            })
        }
        (value, _) => Ok(value), // Pass through if no conversion needed
    }
}

/// Decode a record using schema resolution.
///
/// This function decodes a record written with the writer schema and
/// produces a value conforming to the reader schema, handling:
/// - Field reordering
/// - Type promotions
/// - Default values for missing fields
///
/// # Arguments
/// * `data` - The binary data to decode
/// * `resolution` - The schema resolution context
///
/// # Returns
/// The decoded record as an AvroValue::Record
pub fn decode_record_with_resolution(
    data: &mut &[u8],
    resolution: &ReaderWriterResolution,
) -> Result<crate::reader::decode::AvroValue, DecodeError> {
    use crate::reader::decode::{decode_value, skip_value, AvroValue};

    // First, decode all writer fields in order, storing them by index
    let mut writer_values: Vec<Option<AvroValue>> =
        vec![None; resolution.writer_schema.fields.len()];

    for (idx, field) in resolution.writer_schema.fields.iter().enumerate() {
        if resolution.writer_fields_to_skip.contains(&idx) {
            // Skip this field - not needed by reader
            skip_value(data, &field.schema)?;
        } else {
            // Decode and store
            let value = decode_value(data, &field.schema)?;
            writer_values[idx] = Some(value);
        }
    }

    // Now build the reader record in reader field order
    let mut reader_fields = Vec::with_capacity(resolution.reader_schema.fields.len());

    for (reader_idx, resolved) in resolution.resolved_fields.iter().enumerate() {
        let reader_field_name = &resolution.reader_schema.fields[reader_idx].name;

        let value = match resolved {
            ResolvedField::Present {
                writer_index,
                promotion,
                ..
            } => {
                let writer_value = writer_values[*writer_index].take().ok_or_else(|| {
                    DecodeError::InvalidData(format!(
                        "Writer field {} was not decoded",
                        writer_index
                    ))
                })?;

                // Apply promotion if needed
                match promotion {
                    Some(p) => apply_promotion(writer_value, *p)?,
                    None => writer_value,
                }
            }
            ResolvedField::Default {
                default_value,
                reader_schema,
            } => {
                // Use the default value
                json_to_avro_value(default_value, reader_schema)?
            }
        };

        reader_fields.push((reader_field_name.clone(), value));
    }

    Ok(AvroValue::Record(reader_fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::decode::AvroValue;
    use crate::reader::varint::encode_zigzag;

    /// Helper to create a simple record schema
    fn simple_record(name: &str, fields: Vec<FieldSchema>) -> RecordSchema {
        RecordSchema::new(name, fields)
    }

    // ========================================================================
    // TypePromotion tests
    // ========================================================================

    #[test]
    fn test_type_promotion_same_types() {
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::Int, &AvroSchema::Int).unwrap(),
            None
        );
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::Long, &AvroSchema::Long).unwrap(),
            None
        );
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::String, &AvroSchema::String).unwrap(),
            None
        );
    }

    #[test]
    fn test_type_promotion_int_promotions() {
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::Int, &AvroSchema::Long).unwrap(),
            Some(TypePromotion::IntToLong)
        );
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::Int, &AvroSchema::Float).unwrap(),
            Some(TypePromotion::IntToFloat)
        );
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::Int, &AvroSchema::Double).unwrap(),
            Some(TypePromotion::IntToDouble)
        );
    }

    #[test]
    fn test_type_promotion_long_promotions() {
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::Long, &AvroSchema::Float).unwrap(),
            Some(TypePromotion::LongToFloat)
        );
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::Long, &AvroSchema::Double).unwrap(),
            Some(TypePromotion::LongToDouble)
        );
    }

    #[test]
    fn test_type_promotion_float_to_double() {
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::Float, &AvroSchema::Double).unwrap(),
            Some(TypePromotion::FloatToDouble)
        );
    }

    #[test]
    fn test_type_promotion_string_bytes() {
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::String, &AvroSchema::Bytes).unwrap(),
            Some(TypePromotion::StringToBytes)
        );
        assert_eq!(
            TypePromotion::from_schemas(&AvroSchema::Bytes, &AvroSchema::String).unwrap(),
            Some(TypePromotion::BytesToString)
        );
    }

    #[test]
    fn test_type_promotion_incompatible() {
        assert!(TypePromotion::from_schemas(&AvroSchema::Int, &AvroSchema::String).is_err());
        assert!(TypePromotion::from_schemas(&AvroSchema::Long, &AvroSchema::Int).is_err());
        assert!(TypePromotion::from_schemas(&AvroSchema::Double, &AvroSchema::Float).is_err());
    }

    // ========================================================================
    // apply_promotion tests
    // ========================================================================

    #[test]
    fn test_apply_promotion_int_to_long() {
        let result = apply_promotion(AvroValue::Int(42), TypePromotion::IntToLong).unwrap();
        assert_eq!(result, AvroValue::Long(42));
    }

    #[test]
    fn test_apply_promotion_int_to_float() {
        let result = apply_promotion(AvroValue::Int(42), TypePromotion::IntToFloat).unwrap();
        assert_eq!(result, AvroValue::Float(42.0));
    }

    #[test]
    fn test_apply_promotion_int_to_double() {
        let result = apply_promotion(AvroValue::Int(42), TypePromotion::IntToDouble).unwrap();
        assert_eq!(result, AvroValue::Double(42.0));
    }

    #[test]
    fn test_apply_promotion_long_to_float() {
        let result = apply_promotion(AvroValue::Long(42), TypePromotion::LongToFloat).unwrap();
        assert_eq!(result, AvroValue::Float(42.0));
    }

    #[test]
    fn test_apply_promotion_long_to_double() {
        let result = apply_promotion(AvroValue::Long(42), TypePromotion::LongToDouble).unwrap();
        assert_eq!(result, AvroValue::Double(42.0));
    }

    #[test]
    fn test_apply_promotion_float_to_double() {
        let result = apply_promotion(AvroValue::Float(3.14), TypePromotion::FloatToDouble).unwrap();
        assert_eq!(result, AvroValue::Double(3.14f32 as f64));
    }

    #[test]
    fn test_apply_promotion_string_to_bytes() {
        let result = apply_promotion(
            AvroValue::String("hello".to_string()),
            TypePromotion::StringToBytes,
        )
        .unwrap();
        assert_eq!(result, AvroValue::Bytes(b"hello".to_vec()));
    }

    #[test]
    fn test_apply_promotion_bytes_to_string() {
        let result = apply_promotion(
            AvroValue::Bytes(b"hello".to_vec()),
            TypePromotion::BytesToString,
        )
        .unwrap();
        assert_eq!(result, AvroValue::String("hello".to_string()));
    }

    // ========================================================================
    // ReaderWriterResolution tests
    // ========================================================================

    #[test]
    fn test_resolution_identical_schemas() {
        let schema = simple_record(
            "Test",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        );

        let resolution = ReaderWriterResolution::new(&schema, &schema).unwrap();

        assert_eq!(resolution.resolved_fields.len(), 2);
        assert!(resolution.writer_fields_to_skip.is_empty());
        assert!(!resolution.needs_reordering());
        assert!(!resolution.needs_promotions());
        assert!(!resolution.needs_defaults());
    }

    #[test]
    fn test_resolution_field_reordering() {
        let writer = simple_record(
            "Test",
            vec![
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("id", AvroSchema::Long),
            ],
        );

        let reader = simple_record(
            "Test",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        );

        let resolution = ReaderWriterResolution::new(&writer, &reader).unwrap();

        assert_eq!(resolution.resolved_fields.len(), 2);
        assert!(resolution.needs_reordering());

        // Check field mapping
        match &resolution.resolved_fields[0] {
            ResolvedField::Present { writer_index, .. } => assert_eq!(*writer_index, 1), // id is at index 1 in writer
            _ => panic!("Expected Present field"),
        }
        match &resolution.resolved_fields[1] {
            ResolvedField::Present { writer_index, .. } => assert_eq!(*writer_index, 0), // name is at index 0 in writer
            _ => panic!("Expected Present field"),
        }
    }

    #[test]
    fn test_resolution_type_promotion() {
        let writer = simple_record("Test", vec![FieldSchema::new("value", AvroSchema::Int)]);

        let reader = simple_record("Test", vec![FieldSchema::new("value", AvroSchema::Long)]);

        let resolution = ReaderWriterResolution::new(&writer, &reader).unwrap();

        assert!(resolution.needs_promotions());

        match &resolution.resolved_fields[0] {
            ResolvedField::Present { promotion, .. } => {
                assert_eq!(*promotion, Some(TypePromotion::IntToLong));
            }
            _ => panic!("Expected Present field"),
        }
    }

    #[test]
    fn test_resolution_default_value() {
        let writer = simple_record("Test", vec![FieldSchema::new("id", AvroSchema::Long)]);

        let reader = simple_record(
            "Test",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String)
                    .with_default(serde_json::json!("unknown")),
            ],
        );

        let resolution = ReaderWriterResolution::new(&writer, &reader).unwrap();

        assert!(resolution.needs_defaults());
        assert_eq!(resolution.resolved_fields.len(), 2);

        match &resolution.resolved_fields[1] {
            ResolvedField::Default { default_value, .. } => {
                assert_eq!(*default_value, serde_json::json!("unknown"));
            }
            _ => panic!("Expected Default field"),
        }
    }

    #[test]
    fn test_resolution_missing_field_no_default_error() {
        let writer = simple_record("Test", vec![FieldSchema::new("id", AvroSchema::Long)]);

        let reader = simple_record(
            "Test",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String), // No default!
            ],
        );

        let result = ReaderWriterResolution::new(&writer, &reader);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolution_extra_writer_field() {
        let writer = simple_record(
            "Test",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("extra", AvroSchema::String),
            ],
        );

        let reader = simple_record("Test", vec![FieldSchema::new("id", AvroSchema::Long)]);

        let resolution = ReaderWriterResolution::new(&writer, &reader).unwrap();

        assert_eq!(resolution.resolved_fields.len(), 1);
        assert_eq!(resolution.writer_fields_to_skip, vec![1]); // extra field at index 1
    }

    #[test]
    fn test_resolution_field_alias() {
        let writer = simple_record("Test", vec![FieldSchema::new("user_id", AvroSchema::Long)]);

        let mut id_field = FieldSchema::new("id", AvroSchema::Long);
        id_field.aliases = vec!["user_id".to_string()];
        let reader = simple_record("Test", vec![id_field]);

        let resolution = ReaderWriterResolution::new(&writer, &reader).unwrap();

        assert_eq!(resolution.resolved_fields.len(), 1);
        match &resolution.resolved_fields[0] {
            ResolvedField::Present { writer_index, .. } => assert_eq!(*writer_index, 0),
            _ => panic!("Expected Present field"),
        }
    }

    // ========================================================================
    // json_to_avro_value tests
    // ========================================================================

    #[test]
    fn test_json_to_avro_null() {
        let result = json_to_avro_value(&serde_json::json!(null), &AvroSchema::Null).unwrap();
        assert_eq!(result, AvroValue::Null);
    }

    #[test]
    fn test_json_to_avro_boolean() {
        let result = json_to_avro_value(&serde_json::json!(true), &AvroSchema::Boolean).unwrap();
        assert_eq!(result, AvroValue::Boolean(true));
    }

    #[test]
    fn test_json_to_avro_int() {
        let result = json_to_avro_value(&serde_json::json!(42), &AvroSchema::Int).unwrap();
        assert_eq!(result, AvroValue::Int(42));
    }

    #[test]
    fn test_json_to_avro_long() {
        let result = json_to_avro_value(&serde_json::json!(42), &AvroSchema::Long).unwrap();
        assert_eq!(result, AvroValue::Long(42));
    }

    #[test]
    fn test_json_to_avro_float() {
        let result = json_to_avro_value(&serde_json::json!(3.14), &AvroSchema::Float).unwrap();
        assert_eq!(result, AvroValue::Float(3.14f32));
    }

    #[test]
    fn test_json_to_avro_double() {
        let result = json_to_avro_value(&serde_json::json!(3.14), &AvroSchema::Double).unwrap();
        assert_eq!(result, AvroValue::Double(3.14));
    }

    #[test]
    fn test_json_to_avro_string() {
        let result = json_to_avro_value(&serde_json::json!("hello"), &AvroSchema::String).unwrap();
        assert_eq!(result, AvroValue::String("hello".to_string()));
    }

    #[test]
    fn test_json_to_avro_array() {
        let result = json_to_avro_value(
            &serde_json::json!([1, 2, 3]),
            &AvroSchema::Array(Box::new(AvroSchema::Int)),
        )
        .unwrap();
        assert_eq!(
            result,
            AvroValue::Array(vec![
                AvroValue::Int(1),
                AvroValue::Int(2),
                AvroValue::Int(3)
            ])
        );
    }

    #[test]
    fn test_json_to_avro_map() {
        let result = json_to_avro_value(
            &serde_json::json!({"a": 1, "b": 2}),
            &AvroSchema::Map(Box::new(AvroSchema::Int)),
        )
        .unwrap();

        match result {
            AvroValue::Map(entries) => {
                assert_eq!(entries.len(), 2);
                // Note: order may vary
            }
            _ => panic!("Expected Map"),
        }
    }

    // ========================================================================
    // decode_record_with_resolution tests
    // ========================================================================

    #[test]
    fn test_decode_with_resolution_identical_schemas() {
        let schema = simple_record(
            "Test",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        );

        let resolution = ReaderWriterResolution::new(&schema, &schema).unwrap();

        // Encode: id=42, name="Alice"
        let mut data = Vec::new();
        data.extend_from_slice(&encode_zigzag(42));
        data.extend_from_slice(&encode_zigzag(5)); // string length
        data.extend_from_slice(b"Alice");

        let mut cursor = data.as_slice();
        let result = decode_record_with_resolution(&mut cursor, &resolution).unwrap();

        match result {
            AvroValue::Record(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0], ("id".to_string(), AvroValue::Long(42)));
                assert_eq!(
                    fields[1],
                    ("name".to_string(), AvroValue::String("Alice".to_string()))
                );
            }
            _ => panic!("Expected Record"),
        }
    }

    #[test]
    fn test_decode_with_resolution_field_reordering() {
        let writer = simple_record(
            "Test",
            vec![
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("id", AvroSchema::Long),
            ],
        );

        let reader = simple_record(
            "Test",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        );

        let resolution = ReaderWriterResolution::new(&writer, &reader).unwrap();

        // Encode in writer order: name="Alice", id=42
        let mut data = Vec::new();
        data.extend_from_slice(&encode_zigzag(5)); // string length
        data.extend_from_slice(b"Alice");
        data.extend_from_slice(&encode_zigzag(42));

        let mut cursor = data.as_slice();
        let result = decode_record_with_resolution(&mut cursor, &resolution).unwrap();

        // Result should be in reader order: id, name
        match result {
            AvroValue::Record(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0], ("id".to_string(), AvroValue::Long(42)));
                assert_eq!(
                    fields[1],
                    ("name".to_string(), AvroValue::String("Alice".to_string()))
                );
            }
            _ => panic!("Expected Record"),
        }
    }

    #[test]
    fn test_decode_with_resolution_type_promotion() {
        let writer = simple_record("Test", vec![FieldSchema::new("value", AvroSchema::Int)]);

        let reader = simple_record("Test", vec![FieldSchema::new("value", AvroSchema::Long)]);

        let resolution = ReaderWriterResolution::new(&writer, &reader).unwrap();

        // Encode: value=42 (as int)
        let mut data = Vec::new();
        data.extend_from_slice(&encode_zigzag(42));

        let mut cursor = data.as_slice();
        let result = decode_record_with_resolution(&mut cursor, &resolution).unwrap();

        // Result should have value promoted to long
        match result {
            AvroValue::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0], ("value".to_string(), AvroValue::Long(42)));
            }
            _ => panic!("Expected Record"),
        }
    }

    #[test]
    fn test_decode_with_resolution_default_value() {
        let writer = simple_record("Test", vec![FieldSchema::new("id", AvroSchema::Long)]);

        let reader = simple_record(
            "Test",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String)
                    .with_default(serde_json::json!("unknown")),
            ],
        );

        let resolution = ReaderWriterResolution::new(&writer, &reader).unwrap();

        // Encode: id=42 (no name field)
        let mut data = Vec::new();
        data.extend_from_slice(&encode_zigzag(42));

        let mut cursor = data.as_slice();
        let result = decode_record_with_resolution(&mut cursor, &resolution).unwrap();

        // Result should have default name
        match result {
            AvroValue::Record(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0], ("id".to_string(), AvroValue::Long(42)));
                assert_eq!(
                    fields[1],
                    ("name".to_string(), AvroValue::String("unknown".to_string()))
                );
            }
            _ => panic!("Expected Record"),
        }
    }

    #[test]
    fn test_decode_with_resolution_skip_extra_field() {
        let writer = simple_record(
            "Test",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("extra", AvroSchema::String),
            ],
        );

        let reader = simple_record("Test", vec![FieldSchema::new("id", AvroSchema::Long)]);

        let resolution = ReaderWriterResolution::new(&writer, &reader).unwrap();

        // Encode: id=42, extra="ignored"
        let mut data = Vec::new();
        data.extend_from_slice(&encode_zigzag(42));
        data.extend_from_slice(&encode_zigzag(7)); // string length
        data.extend_from_slice(b"ignored");

        let mut cursor = data.as_slice();
        let result = decode_record_with_resolution(&mut cursor, &resolution).unwrap();

        // Result should only have id
        match result {
            AvroValue::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0], ("id".to_string(), AvroValue::Long(42)));
            }
            _ => panic!("Expected Record"),
        }

        // Cursor should be at end (extra field was skipped)
        assert!(cursor.is_empty());
    }
}
