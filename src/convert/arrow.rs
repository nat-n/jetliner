//! Avro to Arrow type mapping
//!
//! This module implements the type mapping from Avro schemas to Arrow data types.
//! The mapping follows the design specification:
//!
//! | Avro Type | Arrow Type                    | Polars Type  |
//! |-----------|-------------------------------|--------------|
//! | null      | Null                          | Null         |
//! | boolean   | Boolean                       | Boolean      |
//! | int       | Int32                         | Int32        |
//! | long      | Int64                         | Int64        |
//! | float     | Float32                       | Float32      |
//! | double    | Float64                       | Float64      |
//! | bytes     | LargeBinary                   | Binary       |
//! | string    | LargeUtf8                     | Utf8         |
//! | record    | Struct                        | Struct       |
//! | enum      | Enum                          | Enum         |
//! | array     | LargeList                     | List         |
//! | map       | LargeList(Struct{key, value}) | List(Struct) |
//! | union     | Union or nullable base        | varies       |
//! | fixed     | Array(UInt8, size)            | Array        |

use crate::error::SchemaError;
use crate::schema::{AvroSchema, LogicalTypeName, RecordSchema};
use polars::prelude::*;

/// Convert an Avro schema to a Polars DataType.
///
/// This function maps Avro types to their corresponding Polars/Arrow types
/// according to the type mapping specification.
///
/// # Arguments
/// * `schema` - The Avro schema to convert
///
/// # Returns
/// The corresponding Polars DataType, or an error if the schema cannot be converted.
///
/// # Example
/// ```
/// use jetliner::schema::AvroSchema;
/// use jetliner::convert::avro_to_arrow;
/// use polars::prelude::DataType;
///
/// let arrow_type = avro_to_arrow(&AvroSchema::Int).unwrap();
/// assert_eq!(arrow_type, DataType::Int32);
/// ```
pub fn avro_to_arrow(schema: &AvroSchema) -> Result<DataType, SchemaError> {
    match schema {
        // Primitive types
        AvroSchema::Null => Ok(DataType::Null),
        AvroSchema::Boolean => Ok(DataType::Boolean),
        AvroSchema::Int => Ok(DataType::Int32),
        AvroSchema::Long => Ok(DataType::Int64),
        AvroSchema::Float => Ok(DataType::Float32),
        AvroSchema::Double => Ok(DataType::Float64),
        AvroSchema::Bytes => Ok(DataType::Binary),
        AvroSchema::String => Ok(DataType::String),

        // Complex types
        AvroSchema::Record(record) => record_to_arrow(record),
        AvroSchema::Enum(enum_schema) => {
            // Avro enums have fixed, known categories defined in the schema
            // This maps perfectly to Polars Enum type (not Categorical)
            // - Enum: fixed categories known upfront (Avro enum case)
            // - Categorical: categories inferred at runtime
            let categories = FrozenCategories::new(enum_schema.symbols.iter().map(|s| s.as_str()))
                .map_err(|e| {
                    SchemaError::InvalidSchema(format!("Failed to create enum categories: {}", e))
                })?;
            Ok(DataType::from_frozen_categories(categories))
        }
        AvroSchema::Array(items) => {
            let inner_type = avro_to_arrow(items)?;
            Ok(DataType::List(Box::new(inner_type)))
        }
        AvroSchema::Map(values) => {
            // Map is represented as List of Struct with "key" and "value" fields
            let value_type = avro_to_arrow(values)?;
            let struct_fields = vec![
                Field::new("key".into(), DataType::String),
                Field::new("value".into(), value_type),
            ];
            Ok(DataType::List(Box::new(DataType::Struct(struct_fields))))
        }
        AvroSchema::Union(variants) => union_to_arrow(variants),
        AvroSchema::Fixed(_fixed) => {
            // Fixed maps to Binary in Polars.
            //
            // Ideally we'd use FixedSizeBinary(size) to preserve the size constraint
            // in the type, but Polars doesn't expose this as a user-facing DataType.
            // Array(UInt8, size) was attempted but causes pyo3-polars panics during
            // schema conversion. See devnotes/23-fixed-type-binary-mapping.md.
            //
            // TODO: If Polars adds DataType::FixedSizeBinary, switch to it for better
            // type-level size guarantees.
            Ok(DataType::Binary)
        }

        // Named type reference - this occurs for recursive types
        // Recursive types are represented as JSON strings since Arrow/Polars
        // doesn't natively support recursive data structures
        AvroSchema::Named(_name) => {
            // For recursive references, we use String (JSON) representation
            // This allows us to serialize arbitrarily deep recursive structures
            Ok(DataType::String)
        }

        // Logical types
        AvroSchema::Logical(logical) => logical_to_arrow(logical),
    }
}

/// Convert an Avro record schema to an Arrow Struct type.
fn record_to_arrow(record: &RecordSchema) -> Result<DataType, SchemaError> {
    let fields: Result<Vec<Field>, SchemaError> = record
        .fields
        .iter()
        .map(|field| {
            let dtype = avro_to_arrow(&field.schema)?;
            Ok(Field::new(field.name.clone().into(), dtype))
        })
        .collect();

    Ok(DataType::Struct(fields?))
}

/// Convert an Avro union to an Arrow type.
///
/// Union handling strategy:
/// 1. `["null", T]` or `[T, "null"]` unions: Map to nullable Arrow type T
/// 2. Multi-type unions: Currently not fully supported, returns first non-null type
fn union_to_arrow(variants: &[AvroSchema]) -> Result<DataType, SchemaError> {
    // Filter out null variants
    let non_null: Vec<&AvroSchema> = variants
        .iter()
        .filter(|s| !matches!(s, AvroSchema::Null))
        .collect();

    match non_null.len() {
        0 => {
            // Union of only nulls
            Ok(DataType::Null)
        }
        1 => {
            // Simple nullable type - this is the common case
            // Arrow handles nullability separately via the field's nullable flag
            avro_to_arrow(non_null[0])
        }
        _ => {
            // Complex union with multiple non-null types
            // For now, we don't fully support Arrow Union types in Polars
            // Return an error or use a fallback strategy
            Err(SchemaError::UnsupportedType(format!(
                "Complex unions with {} non-null variants are not yet supported. \
                 Only nullable unions (e.g., [\"null\", \"string\"]) are supported.",
                non_null.len()
            )))
        }
    }
}

/// Convert an Avro logical type to an Arrow type.
///
/// Logical type mapping:
/// | Avro Logical Type | Arrow Type                  | Polars Type |
/// |-------------------|-----------------------------| ------------|
/// | decimal           | Decimal128                  | Decimal     |
/// | uuid              | String (Utf8)               | Utf8        |
/// | date              | Date32                      | Date        |
/// | time-millis       | Time32(Millisecond)         | Time        |
/// | time-micros       | Time64(Microsecond)         | Time        |
/// | timestamp-millis  | Datetime(Millisecond, UTC)  | Datetime    |
/// | timestamp-micros  | Datetime(Microsecond, UTC)  | Datetime    |
/// | duration          | Duration(Microsecond)       | Duration    |
fn logical_to_arrow(logical: &crate::schema::LogicalType) -> Result<DataType, SchemaError> {
    match &logical.logical_type {
        LogicalTypeName::Decimal { precision, scale } => {
            // Polars Decimal type with precision and scale
            Ok(DataType::Decimal(*precision as usize, *scale as usize))
        }
        LogicalTypeName::Uuid => {
            // UUID is typically stored as string in Polars
            Ok(DataType::String)
        }
        LogicalTypeName::Date => {
            // Date32 - days since Unix epoch
            Ok(DataType::Date)
        }
        LogicalTypeName::TimeMillis => {
            // Time with millisecond precision
            Ok(DataType::Time)
        }
        LogicalTypeName::TimeMicros => {
            // Time with microsecond precision
            Ok(DataType::Time)
        }
        LogicalTypeName::TimestampMillis => {
            // Timestamp with millisecond precision, UTC timezone
            Ok(DataType::Datetime(
                TimeUnit::Milliseconds,
                Some(TimeZone::UTC),
            ))
        }
        LogicalTypeName::TimestampMicros => {
            // Timestamp with microsecond precision, UTC timezone
            Ok(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(TimeZone::UTC),
            ))
        }
        LogicalTypeName::LocalTimestampMillis => {
            // Local timestamp (no timezone) with millisecond precision
            Ok(DataType::Datetime(TimeUnit::Milliseconds, None))
        }
        LogicalTypeName::LocalTimestampMicros => {
            // Local timestamp (no timezone) with microsecond precision
            Ok(DataType::Datetime(TimeUnit::Microseconds, None))
        }
        LogicalTypeName::Duration => {
            // Duration - Avro duration is months/days/millis, but we map to microseconds
            // This is a simplification; full duration support would need custom handling
            Ok(DataType::Duration(TimeUnit::Microseconds))
        }
    }
}

/// Convert an Avro schema to an Arrow Field.
///
/// This creates a named field with the appropriate data type and nullability.
///
/// # Arguments
/// * `name` - The field name
/// * `schema` - The Avro schema for the field
///
/// # Returns
/// An Arrow Field with the appropriate type and nullability.
pub fn avro_to_arrow_field(name: &str, schema: &AvroSchema) -> Result<Field, SchemaError> {
    let dtype = avro_to_arrow(schema)?;
    // Note: Polars Field doesn't have a nullable flag in the same way Arrow does
    // The nullability is implicit in the data type or handled at the Series level
    Ok(Field::new(name.into(), dtype))
}

/// Convert an Avro record schema to an Arrow Schema.
///
/// This is the main entry point for converting a top-level Avro schema
/// (which should be a record) to an Arrow schema suitable for DataFrame creation.
///
/// # Arguments
/// * `schema` - The Avro schema (should be a Record type)
///
/// # Returns
/// An Arrow Schema with fields corresponding to the record's fields.
///
/// # Non-Record Schemas
/// If the schema is not a record type, it will be wrapped in a synthetic record
/// with a single "value" field. This allows non-record schemas to be converted
/// to a single-column DataFrame.
pub fn avro_to_arrow_schema(schema: &AvroSchema) -> Result<Schema, SchemaError> {
    match schema {
        AvroSchema::Record(record) => {
            let fields: Result<Vec<Field>, SchemaError> = record
                .fields
                .iter()
                .map(|field| avro_to_arrow_field(&field.name, &field.schema))
                .collect();

            Ok(Schema::from_iter(fields?))
        }
        _ => {
            // Wrap non-record schema in a synthetic record with a "value" field
            let dtype = avro_to_arrow(schema)?;
            let field = Field::new("value".into(), dtype);
            Ok(Schema::from_iter(vec![field]))
        }
    }
}

/// Convert an Avro record schema to an Arrow Schema with projection.
///
/// Only includes fields that are in the projected_names set.
///
/// # Arguments
/// * `schema` - The Avro schema (should be a Record type)
/// * `projected_names` - Set of field names to include
///
/// # Returns
/// An Arrow Schema with only the projected fields.
///
/// # Non-Record Schemas
/// If the schema is not a record type, it will be wrapped in a synthetic record
/// with a single "value" field. Projection will only work if "value" is in the
/// projected_names set.
pub fn avro_to_arrow_schema_projected(
    schema: &AvroSchema,
    projected_names: &std::collections::HashSet<String>,
) -> Result<Schema, SchemaError> {
    match schema {
        AvroSchema::Record(record) => {
            let fields: Result<Vec<Field>, SchemaError> = record
                .fields
                .iter()
                .filter(|field| projected_names.contains(&field.name))
                .map(|field| avro_to_arrow_field(&field.name, &field.schema))
                .collect();

            Ok(Schema::from_iter(fields?))
        }
        _ => {
            // Wrap non-record schema in a synthetic record with a "value" field
            // Only include if "value" is in the projected names
            if projected_names.contains("value") {
                let dtype = avro_to_arrow(schema)?;
                let field = Field::new("value".into(), dtype);
                Ok(Schema::from_iter(vec![field]))
            } else {
                // No fields projected
                Ok(Schema::from_iter(Vec::<Field>::new()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{EnumSchema, FieldSchema, FixedSchema, LogicalType, RecordSchema};

    #[test]
    fn test_primitive_types() {
        assert_eq!(avro_to_arrow(&AvroSchema::Null).unwrap(), DataType::Null);
        assert_eq!(
            avro_to_arrow(&AvroSchema::Boolean).unwrap(),
            DataType::Boolean
        );
        assert_eq!(avro_to_arrow(&AvroSchema::Int).unwrap(), DataType::Int32);
        assert_eq!(avro_to_arrow(&AvroSchema::Long).unwrap(), DataType::Int64);
        assert_eq!(
            avro_to_arrow(&AvroSchema::Float).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            avro_to_arrow(&AvroSchema::Double).unwrap(),
            DataType::Float64
        );
        assert_eq!(avro_to_arrow(&AvroSchema::Bytes).unwrap(), DataType::Binary);
        assert_eq!(
            avro_to_arrow(&AvroSchema::String).unwrap(),
            DataType::String
        );
    }

    #[test]
    fn test_array_type() {
        let array_schema = AvroSchema::Array(Box::new(AvroSchema::Int));
        let result = avro_to_arrow(&array_schema).unwrap();
        assert_eq!(result, DataType::List(Box::new(DataType::Int32)));
    }

    #[test]
    fn test_map_type() {
        let map_schema = AvroSchema::Map(Box::new(AvroSchema::String));
        let result = avro_to_arrow(&map_schema).unwrap();

        let expected_struct = DataType::Struct(vec![
            Field::new("key".into(), DataType::String),
            Field::new("value".into(), DataType::String),
        ]);
        assert_eq!(result, DataType::List(Box::new(expected_struct)));
    }

    #[test]
    fn test_nullable_union() {
        // ["null", "string"] union
        let union_schema = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]);
        let result = avro_to_arrow(&union_schema).unwrap();
        assert_eq!(result, DataType::String);

        // ["string", "null"] union (order shouldn't matter)
        let union_schema2 = AvroSchema::Union(vec![AvroSchema::String, AvroSchema::Null]);
        let result2 = avro_to_arrow(&union_schema2).unwrap();
        assert_eq!(result2, DataType::String);
    }

    #[test]
    fn test_record_type() {
        let record = RecordSchema::new(
            "TestRecord",
            vec![
                FieldSchema::new("id", AvroSchema::Int),
                FieldSchema::new("name", AvroSchema::String),
            ],
        );
        let result = avro_to_arrow(&AvroSchema::Record(record)).unwrap();

        let expected = DataType::Struct(vec![
            Field::new("id".into(), DataType::Int32),
            Field::new("name".into(), DataType::String),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_enum_type() {
        let enum_schema =
            EnumSchema::new("Status", vec!["ACTIVE".to_string(), "INACTIVE".to_string()]);
        let result = avro_to_arrow(&AvroSchema::Enum(enum_schema)).unwrap();

        // Should be an Enum type
        match result {
            DataType::Enum(_, _) => {} // Expected
            _ => panic!("Expected Enum type, got {:?}", result),
        }
    }

    #[test]
    fn test_fixed_type() {
        let fixed_schema = FixedSchema::new("Hash", 16);
        let result = avro_to_arrow(&AvroSchema::Fixed(fixed_schema)).unwrap();
        // Fixed maps to Binary (not Array) for better pyo3-polars compatibility
        assert_eq!(result, DataType::Binary);
    }

    #[test]
    fn test_logical_date() {
        let logical = LogicalType::new(AvroSchema::Int, LogicalTypeName::Date);
        let result = avro_to_arrow(&AvroSchema::Logical(logical)).unwrap();
        assert_eq!(result, DataType::Date);
    }

    #[test]
    fn test_logical_timestamp_millis() {
        let logical = LogicalType::new(AvroSchema::Long, LogicalTypeName::TimestampMillis);
        let result = avro_to_arrow(&AvroSchema::Logical(logical)).unwrap();
        assert_eq!(
            result,
            DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::UTC))
        );
    }

    #[test]
    fn test_logical_timestamp_micros() {
        let logical = LogicalType::new(AvroSchema::Long, LogicalTypeName::TimestampMicros);
        let result = avro_to_arrow(&AvroSchema::Logical(logical)).unwrap();
        assert_eq!(
            result,
            DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
        );
    }

    #[test]
    fn test_logical_decimal() {
        let logical = LogicalType::new(
            AvroSchema::Bytes,
            LogicalTypeName::Decimal {
                precision: 10,
                scale: 2,
            },
        );
        let result = avro_to_arrow(&AvroSchema::Logical(logical)).unwrap();
        assert_eq!(result, DataType::Decimal(10, 2));
    }

    #[test]
    fn test_logical_uuid() {
        let logical = LogicalType::new(AvroSchema::String, LogicalTypeName::Uuid);
        let result = avro_to_arrow(&AvroSchema::Logical(logical)).unwrap();
        assert_eq!(result, DataType::String);
    }

    #[test]
    fn test_logical_time_millis() {
        let logical = LogicalType::new(AvroSchema::Int, LogicalTypeName::TimeMillis);
        let result = avro_to_arrow(&AvroSchema::Logical(logical)).unwrap();
        assert_eq!(result, DataType::Time);
    }

    #[test]
    fn test_logical_time_micros() {
        let logical = LogicalType::new(AvroSchema::Long, LogicalTypeName::TimeMicros);
        let result = avro_to_arrow(&AvroSchema::Logical(logical)).unwrap();
        assert_eq!(result, DataType::Time);
    }

    #[test]
    fn test_logical_duration() {
        let logical = LogicalType::new(
            AvroSchema::Fixed(FixedSchema::new("duration", 12)),
            LogicalTypeName::Duration,
        );
        let result = avro_to_arrow(&AvroSchema::Logical(logical)).unwrap();
        assert_eq!(result, DataType::Duration(TimeUnit::Microseconds));
    }

    #[test]
    fn test_logical_local_timestamp_millis() {
        let logical = LogicalType::new(AvroSchema::Long, LogicalTypeName::LocalTimestampMillis);
        let result = avro_to_arrow(&AvroSchema::Logical(logical)).unwrap();
        assert_eq!(result, DataType::Datetime(TimeUnit::Milliseconds, None));
    }

    #[test]
    fn test_logical_local_timestamp_micros() {
        let logical = LogicalType::new(AvroSchema::Long, LogicalTypeName::LocalTimestampMicros);
        let result = avro_to_arrow(&AvroSchema::Logical(logical)).unwrap();
        assert_eq!(result, DataType::Datetime(TimeUnit::Microseconds, None));
    }

    #[test]
    fn test_avro_to_arrow_schema() {
        let record = RecordSchema::new(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new(
                    "email",
                    AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]),
                ),
            ],
        );

        let schema = avro_to_arrow_schema(&AvroSchema::Record(record)).unwrap();

        assert_eq!(schema.len(), 3);
        assert_eq!(schema.get_field("id").unwrap().dtype(), &DataType::Int64);
        assert_eq!(schema.get_field("name").unwrap().dtype(), &DataType::String);
        assert_eq!(
            schema.get_field("email").unwrap().dtype(),
            &DataType::String
        );
    }

    #[test]
    fn test_nested_record() {
        let address = RecordSchema::new(
            "Address",
            vec![
                FieldSchema::new("street", AvroSchema::String),
                FieldSchema::new("city", AvroSchema::String),
            ],
        );

        let person = RecordSchema::new(
            "Person",
            vec![
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("address", AvroSchema::Record(address)),
            ],
        );

        let result = avro_to_arrow(&AvroSchema::Record(person)).unwrap();

        let expected_address = DataType::Struct(vec![
            Field::new("street".into(), DataType::String),
            Field::new("city".into(), DataType::String),
        ]);
        let expected = DataType::Struct(vec![
            Field::new("name".into(), DataType::String),
            Field::new("address".into(), expected_address),
        ]);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_complex_union_error() {
        // Union with multiple non-null types should error
        let union_schema =
            AvroSchema::Union(vec![AvroSchema::String, AvroSchema::Int, AvroSchema::Null]);
        let result = avro_to_arrow(&union_schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_recursive_named_type_to_string() {
        // Recursive named types are represented as JSON strings
        let named = AvroSchema::Named("SomeType".to_string());
        let result = avro_to_arrow(&named).unwrap();
        assert_eq!(result, DataType::String);
    }

    #[test]
    fn test_projected_schema() {
        let record = RecordSchema::new(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("email", AvroSchema::String),
                FieldSchema::new("age", AvroSchema::Int),
            ],
        );

        let mut projected = std::collections::HashSet::new();
        projected.insert("id".to_string());
        projected.insert("name".to_string());

        let schema =
            avro_to_arrow_schema_projected(&AvroSchema::Record(record), &projected).unwrap();

        assert_eq!(schema.len(), 2);
        assert!(schema.get_field("id").is_some());
        assert!(schema.get_field("name").is_some());
        assert!(schema.get_field("email").is_none());
        assert!(schema.get_field("age").is_none());
    }
}
