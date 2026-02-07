//! Schema compatibility checking for Avro schema evolution.
//!
//! This module implements schema compatibility checking per the Avro specification.
//! It determines whether a reader schema is compatible with a writer schema,
//! enabling schema evolution scenarios.
//!
//! # Requirements
//! - 9.4: WHEN schema resolution fails, THE Avro_Reader SHALL return a descriptive error
//!   explaining the incompatibility

use crate::error::SchemaError;
use crate::schema::{AvroSchema, EnumSchema, FieldSchema, FixedSchema, LogicalType, RecordSchema};

/// Result of a schema compatibility check.
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    /// Whether the schemas are compatible.
    pub is_compatible: bool,
    /// List of incompatibilities found (empty if compatible).
    pub incompatibilities: Vec<SchemaIncompatibility>,
}

impl CompatibilityResult {
    /// Create a compatible result.
    pub fn compatible() -> Self {
        Self {
            is_compatible: true,
            incompatibilities: Vec::new(),
        }
    }

    /// Create an incompatible result with a single incompatibility.
    pub fn incompatible(incompatibility: SchemaIncompatibility) -> Self {
        Self {
            is_compatible: false,
            incompatibilities: vec![incompatibility],
        }
    }

    /// Create an incompatible result with multiple incompatibilities.
    pub fn incompatible_many(incompatibilities: Vec<SchemaIncompatibility>) -> Self {
        Self {
            is_compatible: incompatibilities.is_empty(),
            incompatibilities,
        }
    }

    /// Merge another result into this one.
    pub fn merge(&mut self, other: CompatibilityResult) {
        if !other.is_compatible {
            self.is_compatible = false;
        }
        self.incompatibilities.extend(other.incompatibilities);
    }

    /// Convert to a SchemaError if incompatible.
    pub fn to_error(&self) -> Option<SchemaError> {
        if self.is_compatible {
            None
        } else {
            let messages: Vec<String> = self
                .incompatibilities
                .iter()
                .map(|i| i.to_string())
                .collect();
            Some(SchemaError::IncompatibleSchemas(messages.join("; ")))
        }
    }
}

/// Describes a specific schema incompatibility.
#[derive(Debug, Clone)]
pub struct SchemaIncompatibility {
    /// Path to the incompatible element (e.g., "field 'address'.field 'city'").
    pub path: String,
    /// Description of the incompatibility.
    pub reason: IncompatibilityReason,
}

impl std::fmt::Display for SchemaIncompatibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.path.is_empty() {
            write!(f, "{}", self.reason)
        } else {
            write!(f, "at {}: {}", self.path, self.reason)
        }
    }
}

/// Reasons for schema incompatibility.
#[derive(Debug, Clone)]
pub enum IncompatibilityReason {
    /// Types are fundamentally incompatible.
    TypeMismatch {
        writer_type: String,
        reader_type: String,
    },
    /// Reader schema has a required field not in writer schema and no default.
    MissingRequiredField { field_name: String },
    /// Named types have different names.
    NameMismatch {
        writer_name: String,
        reader_name: String,
    },
    /// Fixed types have different sizes.
    FixedSizeMismatch {
        writer_size: usize,
        reader_size: usize,
    },
    /// Enum symbol in writer not found in reader and no default.
    MissingEnumSymbol { symbol: String },
    /// Union variant in writer not compatible with any reader variant.
    UnionVariantIncompatible { variant_index: usize },
    /// Logical type mismatch.
    LogicalTypeMismatch {
        writer_logical: String,
        reader_logical: String,
    },
    /// Decimal precision/scale incompatibility.
    DecimalIncompatible { reason: String },
}

impl std::fmt::Display for IncompatibilityReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncompatibilityReason::TypeMismatch {
                writer_type,
                reader_type,
            } => {
                write!(
                    f,
                    "type mismatch: writer has '{}', reader expects '{}'",
                    writer_type, reader_type
                )
            }
            IncompatibilityReason::MissingRequiredField { field_name } => {
                write!(
                    f,
                    "reader has required field '{}' not present in writer schema and no default value",
                    field_name
                )
            }
            IncompatibilityReason::NameMismatch {
                writer_name,
                reader_name,
            } => {
                write!(
                    f,
                    "name mismatch: writer has '{}', reader expects '{}'",
                    writer_name, reader_name
                )
            }
            IncompatibilityReason::FixedSizeMismatch {
                writer_size,
                reader_size,
            } => {
                write!(
                    f,
                    "fixed size mismatch: writer has {} bytes, reader expects {} bytes",
                    writer_size, reader_size
                )
            }
            IncompatibilityReason::MissingEnumSymbol { symbol } => {
                write!(
                    f,
                    "enum symbol '{}' in writer not found in reader and no default",
                    symbol
                )
            }
            IncompatibilityReason::UnionVariantIncompatible { variant_index } => {
                write!(
                    f,
                    "union variant at index {} in writer is not compatible with any reader variant",
                    variant_index
                )
            }
            IncompatibilityReason::LogicalTypeMismatch {
                writer_logical,
                reader_logical,
            } => {
                write!(
                    f,
                    "logical type mismatch: writer has '{}', reader expects '{}'",
                    writer_logical, reader_logical
                )
            }
            IncompatibilityReason::DecimalIncompatible { reason } => {
                write!(f, "decimal incompatibility: {}", reason)
            }
        }
    }
}

/// Check if a reader schema is compatible with a writer schema.
///
/// This implements the Avro schema resolution rules. A reader schema is compatible
/// with a writer schema if data written with the writer schema can be read using
/// the reader schema.
///
/// # Arguments
/// * `writer_schema` - The schema used to write the data
/// * `reader_schema` - The schema used to read the data
///
/// # Returns
/// A `CompatibilityResult` indicating whether the schemas are compatible and
/// listing any incompatibilities found.
///
/// # Avro Compatibility Rules
/// - Primitive types must match exactly, except for promotions:
///   - int can be promoted to long, float, or double
///   - long can be promoted to float or double
///   - float can be promoted to double
///   - string and bytes are interchangeable
/// - Records: reader fields must be present in writer or have defaults
/// - Enums: writer symbols must be in reader or reader has default
/// - Arrays: element types must be compatible
/// - Maps: value types must be compatible
/// - Unions: each writer variant must match some reader variant
/// - Fixed: must have same name and size
pub fn check_compatibility(
    writer_schema: &AvroSchema,
    reader_schema: &AvroSchema,
) -> CompatibilityResult {
    check_compatibility_with_path(writer_schema, reader_schema, String::new())
}

/// Internal compatibility check with path tracking for error messages.
fn check_compatibility_with_path(
    writer_schema: &AvroSchema,
    reader_schema: &AvroSchema,
    path: String,
) -> CompatibilityResult {
    // Handle logical types - compare underlying types and logical type names
    match (writer_schema, reader_schema) {
        (AvroSchema::Logical(w_logical), AvroSchema::Logical(r_logical)) => {
            return check_logical_type_compatibility(w_logical, r_logical, &path);
        }
        (AvroSchema::Logical(w_logical), _) => {
            // Writer has logical type, reader doesn't - check base type compatibility
            return check_compatibility_with_path(&w_logical.base, reader_schema, path);
        }
        (_, AvroSchema::Logical(r_logical)) => {
            // Reader has logical type, writer doesn't - check base type compatibility
            return check_compatibility_with_path(writer_schema, &r_logical.base, path);
        }
        _ => {}
    }

    // Handle unions specially
    if let AvroSchema::Union(writer_variants) = writer_schema {
        return check_writer_union_compatibility(writer_variants, reader_schema, &path);
    }

    if let AvroSchema::Union(reader_variants) = reader_schema {
        return check_reader_union_compatibility(writer_schema, reader_variants, &path);
    }

    // Check type-specific compatibility
    match (writer_schema, reader_schema) {
        // Exact primitive matches
        (AvroSchema::Null, AvroSchema::Null)
        | (AvroSchema::Boolean, AvroSchema::Boolean)
        | (AvroSchema::Int, AvroSchema::Int)
        | (AvroSchema::Long, AvroSchema::Long)
        | (AvroSchema::Float, AvroSchema::Float)
        | (AvroSchema::Double, AvroSchema::Double)
        | (AvroSchema::Bytes, AvroSchema::Bytes)
        | (AvroSchema::String, AvroSchema::String) => CompatibilityResult::compatible(),

        // Type promotions (int -> long, float, double)
        (AvroSchema::Int, AvroSchema::Long)
        | (AvroSchema::Int, AvroSchema::Float)
        | (AvroSchema::Int, AvroSchema::Double) => CompatibilityResult::compatible(),

        // Type promotions (long -> float, double)
        (AvroSchema::Long, AvroSchema::Float) | (AvroSchema::Long, AvroSchema::Double) => {
            CompatibilityResult::compatible()
        }

        // Type promotions (float -> double)
        (AvroSchema::Float, AvroSchema::Double) => CompatibilityResult::compatible(),

        // String and bytes are interchangeable
        (AvroSchema::String, AvroSchema::Bytes) | (AvroSchema::Bytes, AvroSchema::String) => {
            CompatibilityResult::compatible()
        }

        // Record compatibility
        (AvroSchema::Record(writer_record), AvroSchema::Record(reader_record)) => {
            check_record_compatibility(writer_record, reader_record, &path)
        }

        // Enum compatibility
        (AvroSchema::Enum(writer_enum), AvroSchema::Enum(reader_enum)) => {
            check_enum_compatibility(writer_enum, reader_enum, &path)
        }

        // Array compatibility
        (AvroSchema::Array(writer_items), AvroSchema::Array(reader_items)) => {
            let item_path = if path.is_empty() {
                "array items".to_string()
            } else {
                format!("{}.items", path)
            };
            check_compatibility_with_path(writer_items, reader_items, item_path)
        }

        // Map compatibility
        (AvroSchema::Map(writer_values), AvroSchema::Map(reader_values)) => {
            let value_path = if path.is_empty() {
                "map values".to_string()
            } else {
                format!("{}.values", path)
            };
            check_compatibility_with_path(writer_values, reader_values, value_path)
        }

        // Fixed compatibility
        (AvroSchema::Fixed(writer_fixed), AvroSchema::Fixed(reader_fixed)) => {
            check_fixed_compatibility(writer_fixed, reader_fixed, &path)
        }

        // Named type references - should be resolved before compatibility check
        (AvroSchema::Named(w_name), AvroSchema::Named(r_name)) => {
            if w_name == r_name {
                CompatibilityResult::compatible()
            } else {
                CompatibilityResult::incompatible(SchemaIncompatibility {
                    path,
                    reason: IncompatibilityReason::NameMismatch {
                        writer_name: w_name.clone(),
                        reader_name: r_name.clone(),
                    },
                })
            }
        }

        // Type mismatch
        _ => CompatibilityResult::incompatible(SchemaIncompatibility {
            path,
            reason: IncompatibilityReason::TypeMismatch {
                writer_type: schema_type_name(writer_schema),
                reader_type: schema_type_name(reader_schema),
            },
        }),
    }
}

/// Check compatibility of record schemas.
fn check_record_compatibility(
    writer_record: &RecordSchema,
    reader_record: &RecordSchema,
    path: &str,
) -> CompatibilityResult {
    let mut result = CompatibilityResult::compatible();

    // Check name compatibility (including aliases)
    if !names_match(writer_record, reader_record) {
        return CompatibilityResult::incompatible(SchemaIncompatibility {
            path: path.to_string(),
            reason: IncompatibilityReason::NameMismatch {
                writer_name: writer_record.fullname(),
                reader_name: reader_record.fullname(),
            },
        });
    }

    // Build a map of writer fields by name (including aliases)
    let writer_fields: std::collections::HashMap<&str, &FieldSchema> = writer_record
        .fields
        .iter()
        .flat_map(|f| {
            let mut names = vec![f.name.as_str()];
            names.extend(f.aliases.iter().map(|a| a.as_str()));
            names.into_iter().map(move |name| (name, f))
        })
        .collect();

    // Check each reader field
    for reader_field in &reader_record.fields {
        let field_path = if path.is_empty() {
            format!("field '{}'", reader_field.name)
        } else {
            format!("{}.field '{}'", path, reader_field.name)
        };

        // Try to find matching writer field (by name or alias)
        let writer_field =
            find_matching_field(&reader_field.name, &reader_field.aliases, &writer_fields);

        match writer_field {
            Some(wf) => {
                // Field exists in both - check type compatibility
                let field_result =
                    check_compatibility_with_path(&wf.schema, &reader_field.schema, field_path);
                result.merge(field_result);
            }
            None => {
                // Field not in writer - must have default in reader
                if reader_field.default.is_none() {
                    result.merge(CompatibilityResult::incompatible(SchemaIncompatibility {
                        path: field_path,
                        reason: IncompatibilityReason::MissingRequiredField {
                            field_name: reader_field.name.clone(),
                        },
                    }));
                }
            }
        }
    }

    result
}

/// Check if record names match (considering aliases).
fn names_match(writer_record: &RecordSchema, reader_record: &RecordSchema) -> bool {
    let writer_fullname = writer_record.fullname();
    let reader_fullname = reader_record.fullname();

    // Direct name match
    if writer_fullname == reader_fullname {
        return true;
    }

    // Check if writer name matches any reader alias
    if reader_record.aliases.iter().any(|alias| {
        let alias_fullname = match &reader_record.namespace {
            Some(ns) if !alias.contains('.') => format!("{}.{}", ns, alias),
            _ => alias.clone(),
        };
        alias_fullname == writer_fullname
    }) {
        return true;
    }

    // Check if reader name matches any writer alias
    writer_record.aliases.iter().any(|alias| {
        let alias_fullname = match &writer_record.namespace {
            Some(ns) if !alias.contains('.') => format!("{}.{}", ns, alias),
            _ => alias.clone(),
        };
        alias_fullname == reader_fullname
    })
}

/// Find a matching field by name or alias.
fn find_matching_field<'a>(
    name: &str,
    aliases: &[String],
    writer_fields: &std::collections::HashMap<&str, &'a FieldSchema>,
) -> Option<&'a FieldSchema> {
    // Try direct name match
    if let Some(field) = writer_fields.get(name) {
        return Some(*field);
    }

    // Try aliases
    for alias in aliases {
        if let Some(field) = writer_fields.get(alias.as_str()) {
            return Some(*field);
        }
    }

    None
}

/// Check compatibility of enum schemas.
fn check_enum_compatibility(
    writer_enum: &EnumSchema,
    reader_enum: &EnumSchema,
    path: &str,
) -> CompatibilityResult {
    // Check name compatibility
    if writer_enum.fullname() != reader_enum.fullname() {
        // Check aliases
        let writer_matches_alias = reader_enum.aliases.iter().any(|alias| {
            let alias_fullname = match &reader_enum.namespace {
                Some(ns) if !alias.contains('.') => format!("{}.{}", ns, alias),
                _ => alias.clone(),
            };
            alias_fullname == writer_enum.fullname()
        });

        if !writer_matches_alias {
            return CompatibilityResult::incompatible(SchemaIncompatibility {
                path: path.to_string(),
                reason: IncompatibilityReason::NameMismatch {
                    writer_name: writer_enum.fullname(),
                    reader_name: reader_enum.fullname(),
                },
            });
        }
    }

    // Check that all writer symbols exist in reader (or reader has default)
    let reader_symbols: std::collections::HashSet<&str> =
        reader_enum.symbols.iter().map(|s| s.as_str()).collect();

    let mut result = CompatibilityResult::compatible();

    for symbol in &writer_enum.symbols {
        if !reader_symbols.contains(symbol.as_str()) {
            // Symbol not in reader - check if reader has default
            if reader_enum.default.is_none() {
                result.merge(CompatibilityResult::incompatible(SchemaIncompatibility {
                    path: path.to_string(),
                    reason: IncompatibilityReason::MissingEnumSymbol {
                        symbol: symbol.clone(),
                    },
                }));
            }
        }
    }

    result
}

/// Check compatibility of fixed schemas.
fn check_fixed_compatibility(
    writer_fixed: &FixedSchema,
    reader_fixed: &FixedSchema,
    path: &str,
) -> CompatibilityResult {
    // Check name compatibility
    if writer_fixed.fullname() != reader_fixed.fullname() {
        // Check aliases
        let writer_matches_alias = reader_fixed.aliases.iter().any(|alias| {
            let alias_fullname = match &reader_fixed.namespace {
                Some(ns) if !alias.contains('.') => format!("{}.{}", ns, alias),
                _ => alias.clone(),
            };
            alias_fullname == writer_fixed.fullname()
        });

        if !writer_matches_alias {
            return CompatibilityResult::incompatible(SchemaIncompatibility {
                path: path.to_string(),
                reason: IncompatibilityReason::NameMismatch {
                    writer_name: writer_fixed.fullname(),
                    reader_name: reader_fixed.fullname(),
                },
            });
        }
    }

    // Check size
    if writer_fixed.size != reader_fixed.size {
        return CompatibilityResult::incompatible(SchemaIncompatibility {
            path: path.to_string(),
            reason: IncompatibilityReason::FixedSizeMismatch {
                writer_size: writer_fixed.size,
                reader_size: reader_fixed.size,
            },
        });
    }

    CompatibilityResult::compatible()
}

/// Check compatibility when writer schema is a union.
fn check_writer_union_compatibility(
    writer_variants: &[AvroSchema],
    reader_schema: &AvroSchema,
    path: &str,
) -> CompatibilityResult {
    // If reader is also a union, each writer variant must match some reader variant
    if let AvroSchema::Union(reader_variants) = reader_schema {
        let mut result = CompatibilityResult::compatible();

        for (idx, writer_variant) in writer_variants.iter().enumerate() {
            let variant_compatible = reader_variants.iter().any(|rv| {
                check_compatibility_with_path(writer_variant, rv, String::new()).is_compatible
            });

            if !variant_compatible {
                result.merge(CompatibilityResult::incompatible(SchemaIncompatibility {
                    path: path.to_string(),
                    reason: IncompatibilityReason::UnionVariantIncompatible { variant_index: idx },
                }));
            }
        }

        return result;
    }

    // Writer is union, reader is not - each writer variant must be compatible with reader
    let mut result = CompatibilityResult::compatible();

    for (idx, writer_variant) in writer_variants.iter().enumerate() {
        let variant_path = if path.is_empty() {
            format!("union variant {}", idx)
        } else {
            format!("{}.union variant {}", path, idx)
        };

        let variant_result =
            check_compatibility_with_path(writer_variant, reader_schema, variant_path);
        result.merge(variant_result);
    }

    result
}

/// Check compatibility when reader schema is a union (but writer is not).
fn check_reader_union_compatibility(
    writer_schema: &AvroSchema,
    reader_variants: &[AvroSchema],
    path: &str,
) -> CompatibilityResult {
    // Writer must be compatible with at least one reader variant
    let compatible_with_any = reader_variants
        .iter()
        .any(|rv| check_compatibility_with_path(writer_schema, rv, String::new()).is_compatible);

    if compatible_with_any {
        CompatibilityResult::compatible()
    } else {
        CompatibilityResult::incompatible(SchemaIncompatibility {
            path: path.to_string(),
            reason: IncompatibilityReason::TypeMismatch {
                writer_type: schema_type_name(writer_schema),
                reader_type: "union".to_string(),
            },
        })
    }
}

/// Check compatibility of logical types.
fn check_logical_type_compatibility(
    writer_logical: &LogicalType,
    reader_logical: &LogicalType,
    path: &str,
) -> CompatibilityResult {
    use crate::schema::LogicalTypeName;

    // Check base type compatibility first
    let base_result =
        check_compatibility_with_path(&writer_logical.base, &reader_logical.base, path.to_string());
    if !base_result.is_compatible {
        return base_result;
    }

    // Check logical type name compatibility
    match (&writer_logical.logical_type, &reader_logical.logical_type) {
        // Same logical types
        (LogicalTypeName::Uuid, LogicalTypeName::Uuid)
        | (LogicalTypeName::Date, LogicalTypeName::Date)
        | (LogicalTypeName::TimeMillis, LogicalTypeName::TimeMillis)
        | (LogicalTypeName::TimeMicros, LogicalTypeName::TimeMicros)
        | (LogicalTypeName::TimestampMillis, LogicalTypeName::TimestampMillis)
        | (LogicalTypeName::TimestampMicros, LogicalTypeName::TimestampMicros)
        | (LogicalTypeName::Duration, LogicalTypeName::Duration)
        | (LogicalTypeName::LocalTimestampMillis, LogicalTypeName::LocalTimestampMillis)
        | (LogicalTypeName::LocalTimestampMicros, LogicalTypeName::LocalTimestampMicros)
        | (LogicalTypeName::TimestampNanos, LogicalTypeName::TimestampNanos)
        | (LogicalTypeName::LocalTimestampNanos, LogicalTypeName::LocalTimestampNanos)
        | (LogicalTypeName::BigDecimal, LogicalTypeName::BigDecimal) => {
            CompatibilityResult::compatible()
        }

        // Decimal compatibility - precision and scale must match
        (
            LogicalTypeName::Decimal {
                precision: w_prec,
                scale: w_scale,
            },
            LogicalTypeName::Decimal {
                precision: r_prec,
                scale: r_scale,
            },
        ) => {
            if w_prec != r_prec || w_scale != r_scale {
                CompatibilityResult::incompatible(SchemaIncompatibility {
                    path: path.to_string(),
                    reason: IncompatibilityReason::DecimalIncompatible {
                        reason: format!(
                            "precision/scale mismatch: writer has ({}, {}), reader expects ({}, {})",
                            w_prec, w_scale, r_prec, r_scale
                        ),
                    },
                })
            } else {
                CompatibilityResult::compatible()
            }
        }

        // Different logical types
        _ => CompatibilityResult::incompatible(SchemaIncompatibility {
            path: path.to_string(),
            reason: IncompatibilityReason::LogicalTypeMismatch {
                writer_logical: writer_logical.logical_type.name().to_string(),
                reader_logical: reader_logical.logical_type.name().to_string(),
            },
        }),
    }
}

/// Get a human-readable type name for error messages.
fn schema_type_name(schema: &AvroSchema) -> String {
    match schema {
        AvroSchema::Null => "null".to_string(),
        AvroSchema::Boolean => "boolean".to_string(),
        AvroSchema::Int => "int".to_string(),
        AvroSchema::Long => "long".to_string(),
        AvroSchema::Float => "float".to_string(),
        AvroSchema::Double => "double".to_string(),
        AvroSchema::Bytes => "bytes".to_string(),
        AvroSchema::String => "string".to_string(),
        AvroSchema::Record(r) => format!("record '{}'", r.fullname()),
        AvroSchema::Enum(e) => format!("enum '{}'", e.fullname()),
        AvroSchema::Array(_) => "array".to_string(),
        AvroSchema::Map(_) => "map".to_string(),
        AvroSchema::Union(_) => "union".to_string(),
        AvroSchema::Fixed(f) => format!("fixed '{}' ({} bytes)", f.fullname(), f.size),
        AvroSchema::Named(n) => format!("named '{}'", n),
        AvroSchema::Logical(l) => {
            format!("{} ({})", l.logical_type.name(), schema_type_name(&l.base))
        }
    }
}

/// Validate that a reader schema is compatible with a writer schema.
///
/// This is a convenience function that returns a `Result` instead of
/// `CompatibilityResult`, suitable for use in error handling chains.
///
/// # Arguments
/// * `writer_schema` - The schema used to write the data
/// * `reader_schema` - The schema used to read the data
///
/// # Returns
/// `Ok(())` if compatible, `Err(SchemaError)` with detailed message if not.
///
/// # Example
/// ```
/// use jetliner::{AvroSchema, validate_schema_compatibility};
///
/// let writer = AvroSchema::Int;
/// let reader = AvroSchema::Long;
///
/// // This is compatible (int can be promoted to long)
/// assert!(validate_schema_compatibility(&writer, &reader).is_ok());
///
/// let reader2 = AvroSchema::String;
/// // This is not compatible
/// assert!(validate_schema_compatibility(&writer, &reader2).is_err());
/// ```
pub fn validate_schema_compatibility(
    writer_schema: &AvroSchema,
    reader_schema: &AvroSchema,
) -> Result<(), SchemaError> {
    let result = check_compatibility(writer_schema, reader_schema);
    match result.to_error() {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldSchema, LogicalTypeName};

    // Helper to create a simple record schema
    fn simple_record(name: &str, fields: Vec<FieldSchema>) -> AvroSchema {
        AvroSchema::Record(RecordSchema::new(name, fields))
    }

    // ==================== Primitive Type Tests ====================

    #[test]
    fn test_same_primitive_types_compatible() {
        let primitives = vec![
            AvroSchema::Null,
            AvroSchema::Boolean,
            AvroSchema::Int,
            AvroSchema::Long,
            AvroSchema::Float,
            AvroSchema::Double,
            AvroSchema::Bytes,
            AvroSchema::String,
        ];

        for schema in primitives {
            let result = check_compatibility(&schema, &schema);
            assert!(
                result.is_compatible,
                "Same type should be compatible: {:?}",
                schema
            );
        }
    }

    #[test]
    fn test_int_promotions() {
        // int -> long
        let result = check_compatibility(&AvroSchema::Int, &AvroSchema::Long);
        assert!(result.is_compatible, "int should promote to long");

        // int -> float
        let result = check_compatibility(&AvroSchema::Int, &AvroSchema::Float);
        assert!(result.is_compatible, "int should promote to float");

        // int -> double
        let result = check_compatibility(&AvroSchema::Int, &AvroSchema::Double);
        assert!(result.is_compatible, "int should promote to double");
    }

    #[test]
    fn test_long_promotions() {
        // long -> float
        let result = check_compatibility(&AvroSchema::Long, &AvroSchema::Float);
        assert!(result.is_compatible, "long should promote to float");

        // long -> double
        let result = check_compatibility(&AvroSchema::Long, &AvroSchema::Double);
        assert!(result.is_compatible, "long should promote to double");
    }

    #[test]
    fn test_float_promotion() {
        // float -> double
        let result = check_compatibility(&AvroSchema::Float, &AvroSchema::Double);
        assert!(result.is_compatible, "float should promote to double");
    }

    #[test]
    fn test_string_bytes_interchangeable() {
        let result = check_compatibility(&AvroSchema::String, &AvroSchema::Bytes);
        assert!(
            result.is_compatible,
            "string should be compatible with bytes"
        );

        let result = check_compatibility(&AvroSchema::Bytes, &AvroSchema::String);
        assert!(
            result.is_compatible,
            "bytes should be compatible with string"
        );
    }

    #[test]
    fn test_incompatible_primitives() {
        // int -> string (not compatible)
        let result = check_compatibility(&AvroSchema::Int, &AvroSchema::String);
        assert!(!result.is_compatible);
        assert_eq!(result.incompatibilities.len(), 1);

        // boolean -> int (not compatible)
        let result = check_compatibility(&AvroSchema::Boolean, &AvroSchema::Int);
        assert!(!result.is_compatible);

        // Reverse promotions should not work
        let result = check_compatibility(&AvroSchema::Long, &AvroSchema::Int);
        assert!(!result.is_compatible, "long should not demote to int");

        let result = check_compatibility(&AvroSchema::Double, &AvroSchema::Float);
        assert!(!result.is_compatible, "double should not demote to float");
    }

    // ==================== Record Tests ====================

    #[test]
    fn test_identical_records_compatible() {
        let writer = simple_record(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        );
        let reader = simple_record(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        );

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_record_with_extra_writer_field() {
        // Writer has extra field - reader ignores it (compatible)
        let writer = simple_record(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("email", AvroSchema::String),
            ],
        );
        let reader = simple_record(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        );

        let result = check_compatibility(&writer, &reader);
        assert!(
            result.is_compatible,
            "Extra writer fields should be ignored"
        );
    }

    #[test]
    fn test_record_with_extra_reader_field_with_default() {
        // Reader has extra field with default - compatible
        let writer = simple_record(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        );
        let reader = simple_record(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("email", AvroSchema::String)
                    .with_default(serde_json::json!("unknown@example.com")),
            ],
        );

        let result = check_compatibility(&writer, &reader);
        assert!(
            result.is_compatible,
            "Extra reader field with default should be compatible"
        );
    }

    #[test]
    fn test_record_with_extra_reader_field_without_default() {
        // Reader has extra field without default - incompatible
        let writer = simple_record(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        );
        let reader = simple_record(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("email", AvroSchema::String), // No default!
            ],
        );

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);
        assert!(result.incompatibilities.iter().any(|i| matches!(
            &i.reason,
            IncompatibilityReason::MissingRequiredField { field_name } if field_name == "email"
        )));
    }

    #[test]
    fn test_record_field_type_promotion() {
        // Field type promotion (int -> long)
        let writer = simple_record("Data", vec![FieldSchema::new("value", AvroSchema::Int)]);
        let reader = simple_record("Data", vec![FieldSchema::new("value", AvroSchema::Long)]);

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible, "Field type promotion should work");
    }

    #[test]
    fn test_record_name_mismatch() {
        let writer = simple_record("User", vec![FieldSchema::new("id", AvroSchema::Long)]);
        let reader = simple_record("Person", vec![FieldSchema::new("id", AvroSchema::Long)]);

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);
        assert!(result
            .incompatibilities
            .iter()
            .any(|i| matches!(&i.reason, IncompatibilityReason::NameMismatch { .. })));
    }

    #[test]
    fn test_record_with_alias() {
        let writer = simple_record("User", vec![FieldSchema::new("id", AvroSchema::Long)]);

        let mut reader_record =
            RecordSchema::new("Person", vec![FieldSchema::new("id", AvroSchema::Long)]);
        reader_record.aliases = vec!["User".to_string()];
        let reader = AvroSchema::Record(reader_record);

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible, "Record alias should match");
    }

    // ==================== Enum Tests ====================

    #[test]
    fn test_identical_enums_compatible() {
        let writer = AvroSchema::Enum(EnumSchema::new(
            "Color",
            vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()],
        ));
        let reader = AvroSchema::Enum(EnumSchema::new(
            "Color",
            vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()],
        ));

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_enum_reader_has_more_symbols() {
        // Reader has more symbols - compatible (writer symbols are subset)
        let writer = AvroSchema::Enum(EnumSchema::new(
            "Color",
            vec!["RED".to_string(), "GREEN".to_string()],
        ));
        let reader = AvroSchema::Enum(EnumSchema::new(
            "Color",
            vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()],
        ));

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_enum_writer_has_more_symbols_no_default() {
        // Writer has symbol not in reader, no default - incompatible
        let writer = AvroSchema::Enum(EnumSchema::new(
            "Color",
            vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()],
        ));
        let reader = AvroSchema::Enum(EnumSchema::new(
            "Color",
            vec!["RED".to_string(), "GREEN".to_string()],
        ));

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);
        assert!(result.incompatibilities.iter().any(|i| matches!(
            &i.reason,
            IncompatibilityReason::MissingEnumSymbol { symbol } if symbol == "BLUE"
        )));
    }

    #[test]
    fn test_enum_writer_has_more_symbols_with_default() {
        // Writer has symbol not in reader, but reader has default - compatible
        let writer = AvroSchema::Enum(EnumSchema::new(
            "Color",
            vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()],
        ));
        let mut reader_enum =
            EnumSchema::new("Color", vec!["RED".to_string(), "GREEN".to_string()]);
        reader_enum.default = Some("RED".to_string());
        let reader = AvroSchema::Enum(reader_enum);

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    // ==================== Array Tests ====================

    #[test]
    fn test_array_compatible() {
        let writer = AvroSchema::Array(Box::new(AvroSchema::Int));
        let reader = AvroSchema::Array(Box::new(AvroSchema::Int));

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_array_with_promotion() {
        let writer = AvroSchema::Array(Box::new(AvroSchema::Int));
        let reader = AvroSchema::Array(Box::new(AvroSchema::Long));

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible, "Array element promotion should work");
    }

    #[test]
    fn test_array_incompatible_elements() {
        let writer = AvroSchema::Array(Box::new(AvroSchema::Int));
        let reader = AvroSchema::Array(Box::new(AvroSchema::String));

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);
    }

    // ==================== Map Tests ====================

    #[test]
    fn test_map_compatible() {
        let writer = AvroSchema::Map(Box::new(AvroSchema::String));
        let reader = AvroSchema::Map(Box::new(AvroSchema::String));

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_map_with_promotion() {
        let writer = AvroSchema::Map(Box::new(AvroSchema::Int));
        let reader = AvroSchema::Map(Box::new(AvroSchema::Long));

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible, "Map value promotion should work");
    }

    // ==================== Fixed Tests ====================

    #[test]
    fn test_fixed_compatible() {
        let writer = AvroSchema::Fixed(FixedSchema::new("Hash", 32));
        let reader = AvroSchema::Fixed(FixedSchema::new("Hash", 32));

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_fixed_size_mismatch() {
        let writer = AvroSchema::Fixed(FixedSchema::new("Hash", 32));
        let reader = AvroSchema::Fixed(FixedSchema::new("Hash", 64));

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);
        assert!(result.incompatibilities.iter().any(|i| matches!(
            &i.reason,
            IncompatibilityReason::FixedSizeMismatch {
                writer_size: 32,
                reader_size: 64
            }
        )));
    }

    #[test]
    fn test_fixed_name_mismatch() {
        let writer = AvroSchema::Fixed(FixedSchema::new("Hash", 32));
        let reader = AvroSchema::Fixed(FixedSchema::new("Checksum", 32));

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);
    }

    // ==================== Union Tests ====================

    #[test]
    fn test_union_compatible() {
        let writer = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]);
        let reader = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]);

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_union_reader_has_more_variants() {
        let writer = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]);
        let reader = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String, AvroSchema::Int]);

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_union_writer_variant_not_in_reader() {
        let writer = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String, AvroSchema::Int]);
        let reader = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]);

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);
    }

    #[test]
    fn test_non_union_to_union() {
        // Writer is not union, reader is union containing compatible type
        let writer = AvroSchema::String;
        let reader = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]);

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_non_union_to_union_incompatible() {
        let writer = AvroSchema::Int;
        let reader = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]);

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);
    }

    // ==================== Logical Type Tests ====================

    #[test]
    fn test_logical_type_compatible() {
        let writer = AvroSchema::Logical(LogicalType::new(
            AvroSchema::Long,
            LogicalTypeName::TimestampMillis,
        ));
        let reader = AvroSchema::Logical(LogicalType::new(
            AvroSchema::Long,
            LogicalTypeName::TimestampMillis,
        ));

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_logical_type_mismatch() {
        let writer = AvroSchema::Logical(LogicalType::new(
            AvroSchema::Long,
            LogicalTypeName::TimestampMillis,
        ));
        let reader = AvroSchema::Logical(LogicalType::new(
            AvroSchema::Long,
            LogicalTypeName::TimestampMicros,
        ));

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);
    }

    #[test]
    fn test_decimal_compatible() {
        let writer = AvroSchema::Logical(LogicalType::new(
            AvroSchema::Bytes,
            LogicalTypeName::Decimal {
                precision: 10,
                scale: 2,
            },
        ));
        let reader = AvroSchema::Logical(LogicalType::new(
            AvroSchema::Bytes,
            LogicalTypeName::Decimal {
                precision: 10,
                scale: 2,
            },
        ));

        let result = check_compatibility(&writer, &reader);
        assert!(result.is_compatible);
    }

    #[test]
    fn test_decimal_precision_mismatch() {
        let writer = AvroSchema::Logical(LogicalType::new(
            AvroSchema::Bytes,
            LogicalTypeName::Decimal {
                precision: 10,
                scale: 2,
            },
        ));
        let reader = AvroSchema::Logical(LogicalType::new(
            AvroSchema::Bytes,
            LogicalTypeName::Decimal {
                precision: 15,
                scale: 2,
            },
        ));

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);
    }

    // ==================== Error Message Tests ====================

    #[test]
    fn test_error_message_contains_path() {
        let writer = simple_record(
            "User",
            vec![FieldSchema::new(
                "address",
                simple_record(
                    "Address",
                    vec![FieldSchema::new("city", AvroSchema::Int)], // Wrong type
                ),
            )],
        );
        let reader = simple_record(
            "User",
            vec![FieldSchema::new(
                "address",
                simple_record(
                    "Address",
                    vec![FieldSchema::new("city", AvroSchema::String)],
                ),
            )],
        );

        let result = check_compatibility(&writer, &reader);
        assert!(!result.is_compatible);

        let error = result.to_error().unwrap();
        let error_msg = error.to_string();
        assert!(
            error_msg.contains("address") && error_msg.contains("city"),
            "Error should contain field path: {}",
            error_msg
        );
    }

    #[test]
    fn test_validate_schema_compatibility_ok() {
        let result = validate_schema_compatibility(&AvroSchema::Int, &AvroSchema::Long);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_schema_compatibility_err() {
        let result = validate_schema_compatibility(&AvroSchema::Int, &AvroSchema::String);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("type mismatch"));
    }
}
