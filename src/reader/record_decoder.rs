//! Record decoder for direct Avro-to-Arrow conversion.
//!
//! This module provides the `RecordDecode` trait and `FullRecordDecoder` implementation
//! for decoding Avro records directly into Arrow/Polars column builders without
//! intermediate `AvroValue` representation.
//!
//! # Requirements
//! - 5.1: Construct Polars DataFrames entirely in Rust to minimize cross-language overhead
//! - 5.2: Deserialize Avro data directly into Polars Arrow-backed column builders
//! - 5.3: Avoid intermediate data representations between Avro bytes and Polars columns

use crate::convert::avro_to_arrow_schema;
use crate::error::{DecodeError, SchemaError};
use crate::schema::{AvroSchema, LogicalTypeName, RecordSchema};
use polars::prelude::*;
use polars_arrow::array::ListArray;
use polars_arrow::offset::Offsets;

use super::decode::{
    decode_boolean, decode_bytes, decode_double, decode_enum_index, decode_fixed, decode_float,
    decode_int, decode_long, decode_null, decode_string,
};

/// Trait for record decoding with direct Arrow builder integration.
///
/// This trait defines the interface for decoding Avro records directly into
/// Arrow column builders, avoiding intermediate `AvroValue` representations.
///
/// # Design Rationale
/// Using a trait allows for different decoder implementations:
/// - `FullRecordDecoder`: Decodes all fields (no projection)
/// - `ProjectedRecordDecoder`: Only decodes selected columns (future implementation)
pub trait RecordDecode: Send {
    /// Decode a single record from the binary data into internal builders.
    ///
    /// # Arguments
    /// * `data` - Mutable reference to the byte slice cursor (advanced during decoding)
    ///
    /// # Returns
    /// `Ok(())` on success, or a `DecodeError` if decoding fails.
    fn decode_record(&mut self, data: &mut &[u8]) -> Result<(), DecodeError>;

    /// Finish the current batch and return the accumulated data as Polars Series.
    ///
    /// This method consumes the data in the internal builders and returns
    /// a vector of Series, one for each field in the schema. The builders
    /// are reset for the next batch.
    ///
    /// # Returns
    /// A vector of `Series` representing the decoded columns.
    fn finish_batch(&mut self) -> Result<Vec<Series>, DecodeError>;

    /// Get the number of records currently pending in the builders.
    ///
    /// This is useful for determining when to flush a batch.
    fn pending_records(&self) -> usize;

    /// Get the schema for the decoded records.
    fn schema(&self) -> &Schema;
}

/// Full record decoder that decodes all fields without projection.
///
/// This decoder creates Arrow builders for every field in the schema and
/// decodes all field values directly into those builders. It provides
/// zero overhead for non-projected reads.
///
/// # Performance
/// - No intermediate `AvroValue` allocation
/// - Direct decoding into typed builders
/// - Efficient batch accumulation
pub struct FullRecordDecoder {
    /// The Avro schema for records being decoded
    avro_schema: RecordSchema,
    /// The Polars schema derived from the Avro schema
    polars_schema: Schema,
    /// Column builders, one per field
    builders: Vec<FieldBuilder>,
    /// Number of records currently in the builders
    record_count: usize,
    /// Resolution context for named type references (used for recursive types)
    #[allow(dead_code)]
    resolution_context: crate::schema::SchemaResolutionContext,
}

impl FullRecordDecoder {
    /// Create a new FullRecordDecoder for the given schema.
    ///
    /// # Arguments
    /// * `schema` - The Avro schema (must be a Record type)
    ///
    /// # Returns
    /// A new decoder, or an error if the schema is not a record type.
    pub fn new(schema: &AvroSchema) -> Result<Self, SchemaError> {
        let record_schema = match schema {
            AvroSchema::Record(r) => r.clone(),
            _ => {
                return Err(SchemaError::InvalidSchema(
                    "FullRecordDecoder requires a record schema".to_string(),
                ))
            }
        };

        let polars_schema = avro_to_arrow_schema(schema)?;

        // Build resolution context for named type references (needed for recursive types)
        let resolution_context = crate::schema::SchemaResolutionContext::build_from_schema(schema);

        // Create builders for each field, passing the root schema for recursive type resolution
        let builders: Result<Vec<FieldBuilder>, SchemaError> = record_schema
            .fields
            .iter()
            .map(|field| FieldBuilder::new_with_root(&field.name, &field.schema, schema))
            .collect();

        Ok(Self {
            avro_schema: record_schema,
            polars_schema,
            builders: builders?,
            record_count: 0,
            resolution_context,
        })
    }
}

impl FullRecordDecoder {
    /// Reserve capacity for expected number of records in the next batch.
    ///
    /// Call this before processing a block when the record count is known
    /// to reduce reallocation overhead during the hot decode loop.
    ///
    /// # Arguments
    /// * `record_count` - The number of records expected
    pub fn reserve_for_batch(&mut self, record_count: usize) {
        for builder in &mut self.builders {
            builder.reserve(record_count);
        }
    }
}

impl RecordDecode for FullRecordDecoder {
    fn decode_record(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        // Decode each field in order
        for (field, builder) in self.avro_schema.fields.iter().zip(self.builders.iter_mut()) {
            builder.decode_field(data, &field.schema)?;
        }
        self.record_count += 1;
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<Vec<Series>, DecodeError> {
        let series: Result<Vec<Series>, DecodeError> = self
            .builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();

        self.record_count = 0;
        series
    }

    fn pending_records(&self) -> usize {
        self.record_count
    }

    fn schema(&self) -> &Schema {
        &self.polars_schema
    }
}

/// Projected record decoder that only decodes selected columns.
///
/// This decoder creates Arrow builders only for projected columns and
/// skips over non-projected fields without decoding them. This provides
/// significant memory and CPU savings when only a subset of columns is needed.
///
/// # Performance
/// - Only allocates memory for projected columns
/// - Uses efficient skip functions for non-projected fields
/// - No intermediate `AvroValue` allocation
///
/// # Requirements
/// - 5.5: Preserve null values from Avro unions containing null
/// - 6a.2: Only allocate memory for projected columns
pub struct ProjectedRecordDecoder {
    /// The Avro schema for records being decoded
    avro_schema: RecordSchema,
    /// The Polars schema derived from the projected columns
    polars_schema: Schema,
    /// Column builders, None for skipped columns
    builders: Vec<Option<FieldBuilder>>,
    /// Number of records currently in the builders
    record_count: usize,
}

impl ProjectedRecordDecoder {
    /// Create a new ProjectedRecordDecoder for the given schema and projected columns.
    ///
    /// # Arguments
    /// * `schema` - The Avro schema (must be a Record type)
    /// * `columns` - The list of column names to project
    ///
    /// # Returns
    /// A new decoder, or an error if the schema is not a record type.
    pub fn new(schema: &AvroSchema, columns: &[String]) -> Result<Self, SchemaError> {
        let record_schema = match schema {
            AvroSchema::Record(r) => r.clone(),
            _ => {
                return Err(SchemaError::InvalidSchema(
                    "ProjectedRecordDecoder requires a record schema".to_string(),
                ))
            }
        };

        let projected_names: std::collections::HashSet<String> = columns.iter().cloned().collect();

        // Create projected Polars schema
        let polars_schema =
            crate::convert::avro_to_arrow_schema_projected(schema, &projected_names)?;

        // Create builders only for projected columns, passing root schema for recursive types
        let builders: Result<Vec<Option<FieldBuilder>>, SchemaError> = record_schema
            .fields
            .iter()
            .map(|field| {
                if projected_names.contains(&field.name) {
                    Ok(Some(FieldBuilder::new_with_root(
                        &field.name,
                        &field.schema,
                        schema,
                    )?))
                } else {
                    Ok(None)
                }
            })
            .collect();

        Ok(Self {
            avro_schema: record_schema,
            polars_schema,
            builders: builders?,
            record_count: 0,
        })
    }

    /// Reserve capacity for expected number of records in the next batch.
    ///
    /// Call this before processing a block when the record count is known
    /// to reduce reallocation overhead during the hot decode loop.
    ///
    /// # Arguments
    /// * `record_count` - The number of records expected
    pub fn reserve_for_batch(&mut self, record_count: usize) {
        for builder in self.builders.iter_mut().flatten() {
            builder.reserve(record_count);
        }
    }
}

impl RecordDecode for ProjectedRecordDecoder {
    fn decode_record(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        // Decode or skip each field in order
        for (field, builder_opt) in self.avro_schema.fields.iter().zip(self.builders.iter_mut()) {
            match builder_opt {
                Some(builder) => {
                    // Decode the field into the builder
                    builder.decode_field(data, &field.schema)?;
                }
                None => {
                    // Skip the field without decoding
                    super::decode::skip_value(data, &field.schema)?;
                }
            }
        }
        self.record_count += 1;
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<Vec<Series>, DecodeError> {
        // Only finish builders that exist (projected columns)
        let series: Result<Vec<Series>, DecodeError> = self
            .builders
            .iter_mut()
            .filter_map(|builder_opt| builder_opt.as_mut().map(|b| b.finish()))
            .collect();

        self.record_count = 0;
        series
    }

    fn pending_records(&self) -> usize {
        self.record_count
    }

    fn schema(&self) -> &Schema {
        &self.polars_schema
    }
}

/// Factory enum that wraps Full and Projected decoder variants.
///
/// This enum provides a unified interface for record decoding, automatically
/// choosing the appropriate decoder based on whether projection is requested.
///
/// # Design Rationale
/// Using an enum with two variants allows:
/// - Zero overhead for non-projected reads (FullRecordDecoder)
/// - Efficient projected reads (ProjectedRecordDecoder)
/// - Single interface for callers
pub enum RecordDecoder {
    /// Full decoder - decodes all fields
    Full(FullRecordDecoder),
    /// Projected decoder - only decodes selected columns
    Projected(ProjectedRecordDecoder),
}

impl RecordDecoder {
    /// Create a new RecordDecoder, choosing the appropriate variant.
    ///
    /// # Arguments
    /// * `schema` - The Avro schema (must be a Record type)
    /// * `projected_columns` - Optional list of columns to project. If None,
    ///   creates a FullRecordDecoder. If Some, creates a ProjectedRecordDecoder.
    ///
    /// # Returns
    /// A new decoder, or an error if the schema is not a record type.
    pub fn new(
        schema: &AvroSchema,
        projected_columns: Option<&[String]>,
    ) -> Result<Self, SchemaError> {
        match projected_columns {
            None => Ok(RecordDecoder::Full(FullRecordDecoder::new(schema)?)),
            Some(cols) => Ok(RecordDecoder::Projected(ProjectedRecordDecoder::new(
                schema, cols,
            )?)),
        }
    }

    /// Reserve capacity for expected number of records in the next batch.
    ///
    /// Call this before processing a block when the record count is known
    /// to reduce reallocation overhead during the hot decode loop.
    ///
    /// # Arguments
    /// * `record_count` - The number of records expected
    pub fn reserve_for_batch(&mut self, record_count: usize) {
        match self {
            RecordDecoder::Full(d) => d.reserve_for_batch(record_count),
            RecordDecoder::Projected(d) => d.reserve_for_batch(record_count),
        }
    }
}

impl RecordDecode for RecordDecoder {
    fn decode_record(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        match self {
            RecordDecoder::Full(d) => d.decode_record(data),
            RecordDecoder::Projected(d) => d.decode_record(data),
        }
    }

    fn finish_batch(&mut self) -> Result<Vec<Series>, DecodeError> {
        match self {
            RecordDecoder::Full(d) => d.finish_batch(),
            RecordDecoder::Projected(d) => d.finish_batch(),
        }
    }

    fn pending_records(&self) -> usize {
        match self {
            RecordDecoder::Full(d) => d.pending_records(),
            RecordDecoder::Projected(d) => d.pending_records(),
        }
    }

    fn schema(&self) -> &Schema {
        match self {
            RecordDecoder::Full(d) => d.schema(),
            RecordDecoder::Projected(d) => d.schema(),
        }
    }
}

/// Builder for a single field/column.
///
/// This enum wraps different builder types for different Avro types,
/// allowing efficient direct decoding without intermediate representations.
enum FieldBuilder {
    /// Null values (no actual data stored)
    Null(NullBuilder),
    /// Boolean values
    Boolean(BooleanBuilder),
    /// 32-bit integers
    Int32(Int32Builder),
    /// 64-bit integers
    Int64(Int64Builder),
    /// 32-bit floats
    Float32(Float32Builder),
    /// 64-bit floats
    Float64(Float64Builder),
    /// Binary data
    Binary(BinaryBuilder),
    /// UTF-8 strings
    String(StringBuilder),
    /// Nullable wrapper around another builder
    Nullable(NullableBuilder),
    /// List/Array of values
    List(ListBuilder),
    /// Map (stored as List of Struct with key/value)
    Map(MapBuilder),
    /// Struct (nested record)
    Struct(StructBuilder),
    /// Enum (stored as categorical)
    Enum(EnumBuilder),
    /// Fixed-size binary
    Fixed(FixedBuilder),
    /// Date (days since epoch)
    Date(DateBuilder),
    /// Time (milliseconds or microseconds)
    Time(TimeBuilder),
    /// Datetime/Timestamp
    Datetime(DatetimeBuilder),
    /// Duration
    Duration(DurationBuilder),
    /// Decimal
    Decimal(DecimalBuilder),
    /// Recursive type (serialized to JSON string)
    Recursive(RecursiveBuilder),
}

impl FieldBuilder {
    /// Create a new builder for the given schema.
    #[allow(dead_code)]
    fn new(name: &str, schema: &AvroSchema) -> Result<Self, SchemaError> {
        Self::create_builder(name, schema, schema)
    }

    /// Create a new builder for the given schema with a root schema for context.
    fn new_with_root(
        name: &str,
        schema: &AvroSchema,
        root_schema: &AvroSchema,
    ) -> Result<Self, SchemaError> {
        Self::create_builder(name, schema, root_schema)
    }

    fn create_builder(
        name: &str,
        schema: &AvroSchema,
        root_schema: &AvroSchema,
    ) -> Result<Self, SchemaError> {
        match schema {
            AvroSchema::Null => Ok(FieldBuilder::Null(NullBuilder::new(name))),
            AvroSchema::Boolean => Ok(FieldBuilder::Boolean(BooleanBuilder::new(name))),
            AvroSchema::Int => Ok(FieldBuilder::Int32(Int32Builder::new(name))),
            AvroSchema::Long => Ok(FieldBuilder::Int64(Int64Builder::new(name))),
            AvroSchema::Float => Ok(FieldBuilder::Float32(Float32Builder::new(name))),
            AvroSchema::Double => Ok(FieldBuilder::Float64(Float64Builder::new(name))),
            AvroSchema::Bytes => Ok(FieldBuilder::Binary(BinaryBuilder::new(name))),
            AvroSchema::String => Ok(FieldBuilder::String(StringBuilder::new(name))),

            AvroSchema::Union(variants) => {
                // Check for nullable pattern: ["null", T] or [T, "null"]
                let non_null: Vec<&AvroSchema> = variants
                    .iter()
                    .filter(|s| !matches!(s, AvroSchema::Null))
                    .collect();

                if non_null.len() == 1 && variants.len() == 2 {
                    // Simple nullable type
                    let null_index = variants
                        .iter()
                        .position(|s| matches!(s, AvroSchema::Null))
                        .unwrap();
                    let inner_builder =
                        Box::new(Self::create_builder(name, non_null[0], root_schema)?);
                    Ok(FieldBuilder::Nullable(NullableBuilder::new(
                        name,
                        inner_builder,
                        null_index,
                    )))
                } else if non_null.is_empty() {
                    // Union of only nulls
                    Ok(FieldBuilder::Null(NullBuilder::new(name)))
                } else {
                    // Complex union - not yet supported
                    Err(SchemaError::UnsupportedType(format!(
                        "Complex unions with {} non-null variants are not yet supported",
                        non_null.len()
                    )))
                }
            }

            AvroSchema::Array(items) => {
                let inner_builder = Box::new(Self::create_builder("item", items, root_schema)?);
                Ok(FieldBuilder::List(ListBuilder::new(
                    name,
                    inner_builder,
                    items,
                )))
            }

            AvroSchema::Map(values) => {
                let value_builder = Box::new(Self::create_builder("value", values, root_schema)?);
                Ok(FieldBuilder::Map(MapBuilder::new(
                    name,
                    value_builder,
                    values,
                )))
            }

            AvroSchema::Record(record) => {
                let field_builders: Result<Vec<FieldBuilder>, SchemaError> = record
                    .fields
                    .iter()
                    .map(|f| Self::create_builder(&f.name, &f.schema, root_schema))
                    .collect();
                Ok(FieldBuilder::Struct(StructBuilder::new(
                    name,
                    record.clone(),
                    field_builders?,
                )))
            }

            AvroSchema::Enum(enum_schema) => Ok(FieldBuilder::Enum(EnumBuilder::new(
                name,
                enum_schema.symbols.clone(),
            ))),

            AvroSchema::Fixed(fixed_schema) => Ok(FieldBuilder::Fixed(FixedBuilder::new(
                name,
                fixed_schema.size,
            ))),

            AvroSchema::Named(type_name) => {
                // Recursive type reference - serialize to JSON string
                // This handles self-referential types like linked lists
                Ok(FieldBuilder::Recursive(RecursiveBuilder::new(
                    name,
                    type_name.clone(),
                    root_schema,
                )))
            }

            AvroSchema::Logical(logical) => {
                match &logical.logical_type {
                    LogicalTypeName::Date => Ok(FieldBuilder::Date(DateBuilder::new(name))),
                    LogicalTypeName::TimeMillis => Ok(FieldBuilder::Time(TimeBuilder::new(
                        name,
                        TimeUnit::Milliseconds,
                    ))),
                    LogicalTypeName::TimeMicros => Ok(FieldBuilder::Time(TimeBuilder::new(
                        name,
                        TimeUnit::Microseconds,
                    ))),
                    LogicalTypeName::TimestampMillis => Ok(FieldBuilder::Datetime(
                        DatetimeBuilder::new(name, TimeUnit::Milliseconds, Some(TimeZone::UTC)),
                    )),
                    LogicalTypeName::TimestampMicros => Ok(FieldBuilder::Datetime(
                        DatetimeBuilder::new(name, TimeUnit::Microseconds, Some(TimeZone::UTC)),
                    )),
                    LogicalTypeName::LocalTimestampMillis => Ok(FieldBuilder::Datetime(
                        DatetimeBuilder::new(name, TimeUnit::Milliseconds, None),
                    )),
                    LogicalTypeName::LocalTimestampMicros => Ok(FieldBuilder::Datetime(
                        DatetimeBuilder::new(name, TimeUnit::Microseconds, None),
                    )),
                    LogicalTypeName::Duration => {
                        Ok(FieldBuilder::Duration(DurationBuilder::new(name)))
                    }
                    LogicalTypeName::Decimal { precision, scale } => {
                        Ok(FieldBuilder::Decimal(DecimalBuilder::new(
                            name,
                            *precision as usize,
                            *scale as usize,
                            &logical.base,
                        )?))
                    }
                    LogicalTypeName::Uuid => {
                        // UUID is stored as string
                        Ok(FieldBuilder::String(StringBuilder::new(name)))
                    }
                }
            }
        }
    }

    /// Decode a field value from the data and append to the builder.
    fn decode_field(&mut self, data: &mut &[u8], schema: &AvroSchema) -> Result<(), DecodeError> {
        match self {
            FieldBuilder::Null(b) => b.decode(data),
            FieldBuilder::Boolean(b) => b.decode(data),
            FieldBuilder::Int32(b) => b.decode(data),
            FieldBuilder::Int64(b) => b.decode(data),
            FieldBuilder::Float32(b) => b.decode(data),
            FieldBuilder::Float64(b) => b.decode(data),
            FieldBuilder::Binary(b) => b.decode(data),
            FieldBuilder::String(b) => b.decode(data),
            FieldBuilder::Nullable(b) => b.decode(data, schema),
            FieldBuilder::List(b) => b.decode(data),
            FieldBuilder::Map(b) => b.decode(data),
            FieldBuilder::Struct(b) => b.decode(data),
            FieldBuilder::Enum(b) => b.decode(data),
            FieldBuilder::Fixed(b) => b.decode(data),
            FieldBuilder::Date(b) => b.decode(data),
            FieldBuilder::Time(b) => b.decode(data),
            FieldBuilder::Datetime(b) => b.decode(data),
            FieldBuilder::Duration(b) => b.decode(data),
            FieldBuilder::Decimal(b) => b.decode(data),
            FieldBuilder::Recursive(b) => b.decode(data, schema),
        }
    }

    /// Finish building and return the Series.
    fn finish(&mut self) -> Result<Series, DecodeError> {
        match self {
            FieldBuilder::Null(b) => b.finish(),
            FieldBuilder::Boolean(b) => b.finish(),
            FieldBuilder::Int32(b) => b.finish(),
            FieldBuilder::Int64(b) => b.finish(),
            FieldBuilder::Float32(b) => b.finish(),
            FieldBuilder::Float64(b) => b.finish(),
            FieldBuilder::Binary(b) => b.finish(),
            FieldBuilder::String(b) => b.finish(),
            FieldBuilder::Nullable(b) => b.finish(),
            FieldBuilder::List(b) => b.finish(),
            FieldBuilder::Map(b) => b.finish(),
            FieldBuilder::Struct(b) => b.finish(),
            FieldBuilder::Enum(b) => b.finish(),
            FieldBuilder::Fixed(b) => b.finish(),
            FieldBuilder::Date(b) => b.finish(),
            FieldBuilder::Time(b) => b.finish(),
            FieldBuilder::Datetime(b) => b.finish(),
            FieldBuilder::Duration(b) => b.finish(),
            FieldBuilder::Decimal(b) => b.finish(),
            FieldBuilder::Recursive(b) => b.finish(),
        }
    }

    /// Reserve capacity for additional elements in this builder.
    ///
    /// This is a hint to pre-allocate memory and reduce reallocations.
    /// If the builder cannot determine appropriate capacity or doesn't
    /// support reservation, this is a no-op. Builders will still grow
    /// automatically if the hint is incorrect.
    ///
    /// # Arguments
    /// * `additional` - The number of additional elements expected
    fn reserve(&mut self, additional: usize) {
        match self {
            FieldBuilder::Null(b) => b.reserve(additional),
            FieldBuilder::Boolean(b) => b.reserve(additional),
            FieldBuilder::Int32(b) => b.reserve(additional),
            FieldBuilder::Int64(b) => b.reserve(additional),
            FieldBuilder::Float32(b) => b.reserve(additional),
            FieldBuilder::Float64(b) => b.reserve(additional),
            FieldBuilder::Binary(b) => b.reserve(additional),
            FieldBuilder::String(b) => b.reserve(additional),
            FieldBuilder::Nullable(b) => b.reserve(additional),
            FieldBuilder::List(b) => b.reserve(additional),
            FieldBuilder::Map(b) => b.reserve(additional),
            FieldBuilder::Struct(b) => b.reserve(additional),
            FieldBuilder::Enum(b) => b.reserve(additional),
            FieldBuilder::Fixed(b) => b.reserve(additional),
            FieldBuilder::Date(b) => b.reserve(additional),
            FieldBuilder::Time(b) => b.reserve(additional),
            FieldBuilder::Datetime(b) => b.reserve(additional),
            FieldBuilder::Duration(b) => b.reserve(additional),
            FieldBuilder::Decimal(b) => b.reserve(additional),
            FieldBuilder::Recursive(b) => b.reserve(additional),
        }
    }
}

// ============================================================================
// Individual Builder Implementations
// ============================================================================

/// Builder for null values.
struct NullBuilder {
    name: String,
    count: usize,
}

impl NullBuilder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            count: 0,
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        decode_null(data)?;
        self.count += 1;
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let count = self.count;
        self.count = 0;
        Ok(Series::new_null(self.name.clone().into(), count))
    }

    fn reserve(&mut self, _additional: usize) {
        // No-op: null values have no storage
    }
}

/// Builder for boolean values.
struct BooleanBuilder {
    name: String,
    values: Vec<bool>,
}

impl BooleanBuilder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = decode_boolean(data)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        Ok(Series::new(self.name.clone().into(), values))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for 32-bit integers.
struct Int32Builder {
    name: String,
    values: Vec<i32>,
}

impl Int32Builder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = decode_int(data)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        Ok(Series::new(self.name.clone().into(), values))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for 64-bit integers.
struct Int64Builder {
    name: String,
    values: Vec<i64>,
}

impl Int64Builder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = decode_long(data)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        Ok(Series::new(self.name.clone().into(), values))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for 32-bit floats.
struct Float32Builder {
    name: String,
    values: Vec<f32>,
}

impl Float32Builder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = decode_float(data)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        Ok(Series::new(self.name.clone().into(), values))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for 64-bit floats.
struct Float64Builder {
    name: String,
    values: Vec<f64>,
}

impl Float64Builder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = decode_double(data)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        Ok(Series::new(self.name.clone().into(), values))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for binary data.
struct BinaryBuilder {
    name: String,
    values: Vec<Vec<u8>>,
}

impl BinaryBuilder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = decode_bytes(data)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        let series = Series::new(self.name.clone().into(), values);
        Ok(series)
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for UTF-8 strings.
struct StringBuilder {
    name: String,
    values: Vec<String>,
}

impl StringBuilder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = decode_string(data)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        Ok(Series::new(self.name.clone().into(), values))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for nullable values (union with null).
struct NullableBuilder {
    name: String,
    inner: Box<FieldBuilder>,
    validity: Vec<bool>,
    null_index: usize,
}

impl NullableBuilder {
    fn new(name: &str, inner: Box<FieldBuilder>, null_index: usize) -> Self {
        Self {
            name: name.to_string(),
            inner,
            validity: Vec::new(),
            null_index,
        }
    }

    fn decode(&mut self, data: &mut &[u8], schema: &AvroSchema) -> Result<(), DecodeError> {
        // Decode union index
        let index = decode_int(data)?;

        if index as usize == self.null_index {
            // Null value - append placeholder to inner builder
            self.validity.push(false);
            // We need to append a default value to the inner builder
            self.append_null_to_inner(schema)?;
        } else {
            // Non-null value
            self.validity.push(true);
            // Get the actual schema for the non-null variant
            if let AvroSchema::Union(variants) = schema {
                let variant_schema = &variants[index as usize];
                self.inner.decode_field(data, variant_schema)?;
            } else {
                return Err(DecodeError::InvalidData(
                    "Expected union schema for nullable field".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn append_null_to_inner(&mut self, schema: &AvroSchema) -> Result<(), DecodeError> {
        // Get the non-null variant schema
        let inner_schema = if let AvroSchema::Union(variants) = schema {
            variants
                .iter()
                .find(|s| !matches!(s, AvroSchema::Null))
                .ok_or_else(|| {
                    DecodeError::InvalidData("Union has no non-null variants".to_string())
                })?
        } else {
            return Err(DecodeError::InvalidData(
                "Expected union schema".to_string(),
            ));
        };

        // Append a default/placeholder value based on the inner type
        self.append_default_value(inner_schema)
    }

    fn append_default_value(&mut self, schema: &AvroSchema) -> Result<(), DecodeError> {
        match &mut *self.inner {
            FieldBuilder::Boolean(b) => b.values.push(false),
            FieldBuilder::Int32(b) => b.values.push(0),
            FieldBuilder::Int64(b) => b.values.push(0),
            FieldBuilder::Float32(b) => b.values.push(0.0),
            FieldBuilder::Float64(b) => b.values.push(0.0),
            FieldBuilder::Binary(b) => b.values.push(Vec::new()),
            FieldBuilder::String(b) => b.values.push(String::new()),
            FieldBuilder::List(b) => b.offsets.push(b.current_offset),
            FieldBuilder::Map(b) => b.offsets.push(b.current_offset),
            FieldBuilder::Struct(b) => {
                // Append default values to all nested fields
                for (field, builder) in b.schema.fields.iter().zip(b.builders.iter_mut()) {
                    append_default_to_builder(builder, &field.schema)?;
                }
            }
            FieldBuilder::Enum(b) => b.indices.push(0),
            FieldBuilder::Fixed(b) => b.values.push(vec![0u8; b.size]),
            FieldBuilder::Date(b) => b.values.push(0),
            FieldBuilder::Time(b) => b.values.push(0),
            FieldBuilder::Datetime(b) => b.values.push(0),
            FieldBuilder::Duration(b) => b.values.push(0),
            FieldBuilder::Decimal(b) => b.values.push(0),
            FieldBuilder::Null(b) => b.count += 1,
            FieldBuilder::Nullable(b) => {
                // Nested nullable - append null
                b.validity.push(false);
                if let AvroSchema::Union(variants) = schema {
                    if let Some(inner_schema) =
                        variants.iter().find(|s| !matches!(s, AvroSchema::Null))
                    {
                        b.append_default_value(inner_schema)?;
                    }
                }
            }
            FieldBuilder::Recursive(b) => {
                // For recursive types, append "null" as the default JSON value
                b.values.push("null".to_string());
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let inner_series = self.inner.finish()?;
        let validity = std::mem::take(&mut self.validity);

        // Create a boolean chunked array for validity mask (true = valid, false = null)
        let mask = BooleanChunked::new("mask".into(), &validity);

        // Create a null series with the same dtype as the inner series.
        // This is necessary because zip_with requires both series to have compatible types.
        // Using Series::new_null creates a Null dtype series which can't be combined with
        // complex types like List or Struct.
        let null_series = Series::full_null(
            self.name.clone().into(),
            inner_series.len(),
            inner_series.dtype(),
        );

        // Use zip_with to apply the null mask - where mask is true, use inner_series value,
        // where mask is false, use null_series value (which is null)
        let result = inner_series
            .zip_with(&mask, &null_series)
            .map_err(|e| DecodeError::InvalidData(format!("Failed to apply null mask: {}", e)))?;

        Ok(result.with_name(self.name.clone().into()))
    }

    fn reserve(&mut self, additional: usize) {
        self.validity.reserve(additional);
        self.inner.reserve(additional);
    }
}

/// Helper function to append default values to a builder.
fn append_default_to_builder(
    builder: &mut FieldBuilder,
    schema: &AvroSchema,
) -> Result<(), DecodeError> {
    match builder {
        FieldBuilder::Boolean(b) => b.values.push(false),
        FieldBuilder::Int32(b) => b.values.push(0),
        FieldBuilder::Int64(b) => b.values.push(0),
        FieldBuilder::Float32(b) => b.values.push(0.0),
        FieldBuilder::Float64(b) => b.values.push(0.0),
        FieldBuilder::Binary(b) => b.values.push(Vec::new()),
        FieldBuilder::String(b) => b.values.push(String::new()),
        FieldBuilder::Null(b) => b.count += 1,
        FieldBuilder::List(b) => b.offsets.push(b.current_offset),
        FieldBuilder::Map(b) => b.offsets.push(b.current_offset),
        FieldBuilder::Struct(b) => {
            for (field, field_builder) in b.schema.fields.iter().zip(b.builders.iter_mut()) {
                append_default_to_builder(field_builder, &field.schema)?;
            }
        }
        FieldBuilder::Enum(b) => b.indices.push(0),
        FieldBuilder::Fixed(b) => b.values.push(vec![0u8; b.size]),
        FieldBuilder::Date(b) => b.values.push(0),
        FieldBuilder::Time(b) => b.values.push(0),
        FieldBuilder::Datetime(b) => b.values.push(0),
        FieldBuilder::Duration(b) => b.values.push(0),
        FieldBuilder::Decimal(b) => b.values.push(0),
        FieldBuilder::Nullable(b) => {
            b.validity.push(false);
            if let AvroSchema::Union(variants) = schema {
                if let Some(inner_schema) = variants.iter().find(|s| !matches!(s, AvroSchema::Null))
                {
                    b.append_default_value(inner_schema)?;
                }
            }
        }
        FieldBuilder::Recursive(b) => {
            // For recursive types, append "null" as the default JSON value
            b.values.push("null".to_string());
        }
    }
    Ok(())
}

/// Builder for list/array values.
struct ListBuilder {
    name: String,
    inner: Box<FieldBuilder>,
    inner_schema: AvroSchema,
    offsets: Vec<i64>,
    current_offset: i64,
}

impl ListBuilder {
    fn new(name: &str, inner: Box<FieldBuilder>, inner_schema: &AvroSchema) -> Self {
        Self {
            name: name.to_string(),
            inner,
            inner_schema: inner_schema.clone(),
            offsets: vec![0], // Start with offset 0
            current_offset: 0,
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        // Decode array blocks
        loop {
            let count = decode_long(data)?;

            if count == 0 {
                // End of array
                break;
            }

            let item_count = if count < 0 {
                // Negative count means block has byte size prefix
                let _byte_size = decode_long(data)?;
                (-count) as usize
            } else {
                count as usize
            };

            // Pre-allocate for this array's items
            self.inner.reserve(item_count);

            // Decode each item
            for _ in 0..item_count {
                self.inner.decode_field(data, &self.inner_schema)?;
                self.current_offset += 1;
            }
        }

        self.offsets.push(self.current_offset);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let inner_series = self.inner.finish()?;
        let offsets = std::mem::take(&mut self.offsets);
        self.offsets = vec![0];
        self.current_offset = 0;

        // Direct ListArray construction - avoids slice-per-element overhead
        // Get the underlying arrow array from the inner series
        let inner_chunks = inner_series.to_arrow(0, CompatLevel::newest());
        let inner_dtype = inner_chunks.dtype().clone();

        // Create ListArray directly from offsets and values
        let list_dtype = ListArray::<i64>::default_datatype(inner_dtype);
        let list_arr = ListArray::<i64>::new(
            list_dtype,
            // SAFETY: offsets are monotonically increasing (we build them that way)
            unsafe { Offsets::new_unchecked(offsets).into() },
            inner_chunks,
            None,
        );

        // Wrap in ListChunked
        let list_chunked = ListChunked::with_chunk(self.name.clone().into(), list_arr);
        Ok(list_chunked.into_series())
    }

    fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
        // Inner builder grows automatically during decode
    }
}

/// Builder for map values (stored as List of Struct with key/value).
struct MapBuilder {
    name: String,
    key_builder: StringBuilder,
    value_builder: Box<FieldBuilder>,
    value_schema: AvroSchema,
    offsets: Vec<i64>,
    current_offset: i64,
}

impl MapBuilder {
    fn new(name: &str, value_builder: Box<FieldBuilder>, value_schema: &AvroSchema) -> Self {
        Self {
            name: name.to_string(),
            key_builder: StringBuilder::new("key"),
            value_builder,
            value_schema: value_schema.clone(),
            offsets: vec![0],
            current_offset: 0,
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        // Decode map blocks
        loop {
            let count = decode_long(data)?;

            if count == 0 {
                // End of map
                break;
            }

            let entry_count = if count < 0 {
                // Negative count means block has byte size prefix
                let _byte_size = decode_long(data)?;
                (-count) as usize
            } else {
                count as usize
            };

            // Pre-allocate for this map's entries
            self.key_builder.reserve(entry_count);
            self.value_builder.reserve(entry_count);

            // Decode each key-value pair
            for _ in 0..entry_count {
                // Decode key (always string)
                self.key_builder.decode(data)?;
                // Decode value
                self.value_builder.decode_field(data, &self.value_schema)?;
                self.current_offset += 1;
            }
        }

        self.offsets.push(self.current_offset);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let key_series = self.key_builder.finish()?;
        let value_series = self.value_builder.finish()?;
        let offsets = std::mem::take(&mut self.offsets);
        self.offsets = vec![0];
        self.current_offset = 0;

        // Create struct series from key and value
        let struct_series = StructChunked::from_series(
            "entries".into(),
            key_series.len(),
            [key_series, value_series].iter(),
        )
        .map_err(|e| DecodeError::InvalidData(format!("Failed to create struct: {}", e)))?
        .into_series();

        // Direct ListArray construction - avoids slice-per-element overhead
        let inner_chunks = struct_series.to_arrow(0, CompatLevel::newest());
        let inner_dtype = inner_chunks.dtype().clone();

        let list_dtype = ListArray::<i64>::default_datatype(inner_dtype);
        let list_arr = ListArray::<i64>::new(
            list_dtype,
            // SAFETY: offsets are monotonically increasing (we build them that way)
            unsafe { Offsets::new_unchecked(offsets).into() },
            inner_chunks,
            None,
        );

        let list_chunked = ListChunked::with_chunk(self.name.clone().into(), list_arr);
        Ok(list_chunked.into_series())
    }

    fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
        // Key/value builders grow during decode
    }
}

/// Builder for struct/record values.
struct StructBuilder {
    name: String,
    schema: RecordSchema,
    builders: Vec<FieldBuilder>,
}

impl StructBuilder {
    fn new(name: &str, schema: RecordSchema, builders: Vec<FieldBuilder>) -> Self {
        Self {
            name: name.to_string(),
            schema,
            builders,
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        // Decode each field in order
        for (field, builder) in self.schema.fields.iter().zip(self.builders.iter_mut()) {
            builder.decode_field(data, &field.schema)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let field_series: Result<Vec<Series>, DecodeError> =
            self.builders.iter_mut().map(|b| b.finish()).collect();

        let field_series = field_series?;
        let len = field_series.first().map(|s| s.len()).unwrap_or(0);

        let struct_chunked =
            StructChunked::from_series(self.name.clone().into(), len, field_series.iter())
                .map_err(|e| DecodeError::InvalidData(format!("Failed to create struct: {}", e)))?;

        Ok(struct_chunked.into_series())
    }

    fn reserve(&mut self, additional: usize) {
        for builder in &mut self.builders {
            builder.reserve(additional);
        }
    }
}

/// Builder for enum values (stored as categorical).
struct EnumBuilder {
    name: String,
    symbols: Vec<String>,
    indices: Vec<u32>,
}

impl EnumBuilder {
    fn new(name: &str, symbols: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            symbols,
            indices: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let index = decode_enum_index(data, self.symbols.len())?;
        self.indices.push(index as u32);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let indices = std::mem::take(&mut self.indices);

        // Create categorical from indices and symbols
        // First create the DataType with the categories
        let categories = FrozenCategories::new(self.symbols.iter().map(|s| s.as_str()))
            .map_err(|e| DecodeError::InvalidData(format!("Failed to create categories: {}", e)))?;
        let dtype = DataType::from_frozen_categories(categories);

        // Create the physical indices chunked array
        let physical = UInt32Chunked::from_vec(self.name.clone().into(), indices);

        // Create categorical from physical indices and dtype using Categorical32Type
        let ca = unsafe {
            CategoricalChunked::<Categorical32Type>::from_cats_and_dtype_unchecked(physical, dtype)
        };

        Ok(ca.into_series())
    }

    fn reserve(&mut self, additional: usize) {
        self.indices.reserve(additional);
    }
}

/// Builder for fixed-size binary values.
struct FixedBuilder {
    name: String,
    size: usize,
    values: Vec<Vec<u8>>,
}

impl FixedBuilder {
    fn new(name: &str, size: usize) -> Self {
        Self {
            name: name.to_string(),
            size,
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = decode_fixed(data, self.size)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);

        // Convert to Array type (fixed-size list of u8)
        // First create a flat series of all bytes
        let inner_values: Vec<u8> = values.iter().flatten().copied().collect();
        let inner_ca = UInt8Chunked::from_vec("".into(), inner_values);
        let inner_series = inner_ca.into_series();

        // Create fixed-size array by reshaping
        let array_chunked = inner_series
            .reshape_array(&[
                ReshapeDimension::Infer,
                ReshapeDimension::new_dimension(self.size as u64),
            ])
            .map_err(|e| DecodeError::InvalidData(format!("Failed to reshape to array: {}", e)))?;

        Ok(array_chunked.with_name(self.name.clone().into()))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for date values (days since epoch).
struct DateBuilder {
    name: String,
    values: Vec<i32>,
}

impl DateBuilder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = decode_int(data)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        let ca = Int32Chunked::new(self.name.clone().into(), &values);
        Ok(ca.into_date().into_series())
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for time values.
struct TimeBuilder {
    name: String,
    unit: TimeUnit,
    values: Vec<i64>,
}

impl TimeBuilder {
    fn new(name: &str, unit: TimeUnit) -> Self {
        Self {
            name: name.to_string(),
            unit,
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = match self.unit {
            TimeUnit::Milliseconds => decode_int(data)? as i64,
            TimeUnit::Microseconds | TimeUnit::Nanoseconds => decode_long(data)?,
        };
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        let ca = Int64Chunked::new(self.name.clone().into(), &values);
        Ok(ca.into_time().into_series())
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for datetime/timestamp values.
struct DatetimeBuilder {
    name: String,
    unit: TimeUnit,
    timezone: Option<TimeZone>,
    values: Vec<i64>,
}

impl DatetimeBuilder {
    fn new(name: &str, unit: TimeUnit, timezone: Option<TimeZone>) -> Self {
        Self {
            name: name.to_string(),
            unit,
            timezone,
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let value = decode_long(data)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        let ca = Int64Chunked::new(self.name.clone().into(), &values);
        Ok(ca
            .into_datetime(self.unit, self.timezone.clone())
            .into_series())
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for duration values.
struct DurationBuilder {
    name: String,
    values: Vec<i64>,
}

impl DurationBuilder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        // Avro duration is fixed[12]: months (u32), days (u32), milliseconds (u32)
        let bytes = decode_fixed(data, 12)?;

        // Parse as three little-endian u32 values
        let _months = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let days = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let milliseconds = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);

        // Convert to microseconds (Polars duration unit)
        // Note: We lose month precision here as Polars Duration doesn't support months
        let total_micros = (days as i64 * 24 * 60 * 60 * 1_000_000) + (milliseconds as i64 * 1_000);
        self.values.push(total_micros);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        let ca = Int64Chunked::new(self.name.clone().into(), &values);
        Ok(ca.into_duration(TimeUnit::Microseconds).into_series())
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for decimal values.
struct DecimalBuilder {
    name: String,
    precision: usize,
    scale: usize,
    fixed_size: Option<usize>,
    values: Vec<i128>,
}

impl DecimalBuilder {
    fn new(
        name: &str,
        precision: usize,
        scale: usize,
        base: &AvroSchema,
    ) -> Result<Self, SchemaError> {
        let fixed_size = match base {
            AvroSchema::Fixed(f) => Some(f.size),
            AvroSchema::Bytes => None,
            _ => {
                return Err(SchemaError::InvalidSchema(format!(
                    "Decimal must have bytes or fixed base type, got {:?}",
                    base
                )))
            }
        };

        Ok(Self {
            name: name.to_string(),
            precision,
            scale,
            fixed_size,
            values: Vec::new(),
        })
    }

    fn decode(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        let bytes = match self.fixed_size {
            Some(size) => decode_fixed(data, size)?,
            None => decode_bytes(data)?,
        };

        // Convert big-endian two's complement bytes to i128
        let value = bytes_to_i128(&bytes)?;
        self.values.push(value);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        let ca = Int128Chunked::from_vec(self.name.clone().into(), values);
        Ok(ca
            .into_decimal_unchecked(self.precision, self.scale)
            .into_series())
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Builder for recursive type values (serialized to JSON string).
///
/// Recursive types (like linked lists or trees) cannot be directly represented
/// in Arrow/Polars since they don't support recursive data structures.
/// Instead, we serialize the recursive structure to a JSON string.
struct RecursiveBuilder {
    name: String,
    /// The full schema for the recursive type (needed for decoding)
    schema: AvroSchema,
    /// Resolution context for resolving named type references
    context: crate::schema::SchemaResolutionContext,
    values: Vec<String>,
}

impl RecursiveBuilder {
    fn new(name: &str, type_name: String, root_schema: &AvroSchema) -> Self {
        // Build resolution context from the root schema
        let context = crate::schema::SchemaResolutionContext::build_from_schema(root_schema);

        // Get the actual schema for this type from the context
        let schema = context
            .get(&type_name)
            .cloned()
            .unwrap_or(AvroSchema::Named(type_name));

        Self {
            name: name.to_string(),
            schema,
            context,
            values: Vec::new(),
        }
    }

    fn decode(&mut self, data: &mut &[u8], _schema: &AvroSchema) -> Result<(), DecodeError> {
        // Decode the value to JSON using the context-aware decoder
        let value = super::decode::decode_value_with_context(data, &self.schema, &self.context)?;
        let json_str = serde_json::to_string(&value.to_json()).map_err(|e| {
            DecodeError::InvalidData(format!("Failed to serialize recursive type to JSON: {}", e))
        })?;
        self.values.push(json_str);
        Ok(())
    }

    fn finish(&mut self) -> Result<Series, DecodeError> {
        let values = std::mem::take(&mut self.values);
        Ok(Series::new(self.name.clone().into(), values))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

/// Convert big-endian two's complement bytes to i128.
fn bytes_to_i128(bytes: &[u8]) -> Result<i128, DecodeError> {
    if bytes.is_empty() {
        return Ok(0);
    }

    if bytes.len() > 16 {
        return Err(DecodeError::InvalidData(format!(
            "Decimal value too large: {} bytes (max 16)",
            bytes.len()
        )));
    }

    // Sign extend based on the first byte
    let is_negative = bytes[0] & 0x80 != 0;
    let mut result: i128 = if is_negative { -1 } else { 0 };

    for &byte in bytes {
        result = (result << 8) | (byte as i128);
    }

    Ok(result)
}
