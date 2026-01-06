//! Avro schema types and representations.
//!
//! This module defines the complete Avro schema type system including
//! primitives, complex types, and logical types.

use serde_json::Value;

/// Represents an Avro schema.
///
/// Supports all Avro primitive types, complex types, and named type references.
#[derive(Debug, Clone, PartialEq)]
pub enum AvroSchema {
    // Primitive types
    /// Null type - no value.
    Null,
    /// Boolean type.
    Boolean,
    /// 32-bit signed integer.
    Int,
    /// 64-bit signed integer.
    Long,
    /// 32-bit IEEE 754 floating-point.
    Float,
    /// 64-bit IEEE 754 floating-point.
    Double,
    /// Sequence of bytes.
    Bytes,
    /// Unicode string.
    String,

    // Complex types
    /// Record type with named fields.
    Record(RecordSchema),
    /// Enumeration type.
    Enum(EnumSchema),
    /// Array of items with a single schema.
    Array(Box<AvroSchema>),
    /// Map with string keys and values of a single schema.
    Map(Box<AvroSchema>),
    /// Union of multiple schemas.
    Union(Vec<AvroSchema>),
    /// Fixed-size byte array.
    Fixed(FixedSchema),

    /// Named type reference (resolved during parsing).
    Named(String),

    /// Logical type wrapper.
    Logical(LogicalType),
}

/// Schema for a record type.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordSchema {
    /// The name of the record.
    pub name: String,
    /// Optional namespace for the record.
    pub namespace: Option<String>,
    /// The fields of the record.
    pub fields: Vec<FieldSchema>,
    /// Optional documentation.
    pub doc: Option<String>,
    /// Aliases for this record.
    pub aliases: Vec<String>,
}

impl RecordSchema {
    /// Create a new RecordSchema with the given name and fields.
    pub fn new(name: impl Into<String>, fields: Vec<FieldSchema>) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            fields,
            doc: None,
            aliases: Vec::new(),
        }
    }

    /// Set the namespace.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the documentation.
    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }

    /// Get the fully qualified name.
    pub fn fullname(&self) -> String {
        match &self.namespace {
            Some(ns) => format!("{}.{}", ns, self.name),
            None => self.name.clone(),
        }
    }
}

/// Schema for a field within a record.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldSchema {
    /// The name of the field.
    pub name: String,
    /// The schema of the field's value.
    pub schema: AvroSchema,
    /// Optional default value for the field.
    pub default: Option<Value>,
    /// Optional documentation.
    pub doc: Option<String>,
    /// Field ordering (ascending, descending, ignore).
    pub order: FieldOrder,
    /// Aliases for this field.
    pub aliases: Vec<String>,
}

impl FieldSchema {
    /// Create a new FieldSchema with the given name and schema.
    pub fn new(name: impl Into<String>, schema: AvroSchema) -> Self {
        Self {
            name: name.into(),
            schema,
            default: None,
            doc: None,
            order: FieldOrder::Ascending,
            aliases: Vec::new(),
        }
    }

    /// Set the default value.
    pub fn with_default(mut self, default: Value) -> Self {
        self.default = Some(default);
        self
    }

    /// Set the documentation.
    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }
}

/// Field ordering for record comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FieldOrder {
    #[default]
    Ascending,
    Descending,
    Ignore,
}

/// Schema for an enumeration type.
#[derive(Debug, Clone, PartialEq)]
pub struct EnumSchema {
    /// The name of the enum.
    pub name: String,
    /// Optional namespace for the enum.
    pub namespace: Option<String>,
    /// The symbols (variants) of the enum.
    pub symbols: Vec<String>,
    /// Optional documentation.
    pub doc: Option<String>,
    /// Aliases for this enum.
    pub aliases: Vec<String>,
    /// Default symbol (for schema resolution).
    pub default: Option<String>,
}

impl EnumSchema {
    /// Create a new EnumSchema with the given name and symbols.
    pub fn new(name: impl Into<String>, symbols: Vec<String>) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            symbols,
            doc: None,
            aliases: Vec::new(),
            default: None,
        }
    }

    /// Set the namespace.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Get the fully qualified name.
    pub fn fullname(&self) -> String {
        match &self.namespace {
            Some(ns) => format!("{}.{}", ns, self.name),
            None => self.name.clone(),
        }
    }

    /// Get the index of a symbol.
    pub fn symbol_index(&self, symbol: &str) -> Option<usize> {
        self.symbols.iter().position(|s| s == symbol)
    }
}

/// Schema for a fixed-size byte array.
#[derive(Debug, Clone, PartialEq)]
pub struct FixedSchema {
    /// The name of the fixed type.
    pub name: String,
    /// Optional namespace for the fixed type.
    pub namespace: Option<String>,
    /// The size in bytes.
    pub size: usize,
    /// Optional documentation.
    pub doc: Option<String>,
    /// Aliases for this fixed type.
    pub aliases: Vec<String>,
}

impl FixedSchema {
    /// Create a new FixedSchema with the given name and size.
    pub fn new(name: impl Into<String>, size: usize) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            size,
            doc: None,
            aliases: Vec::new(),
        }
    }

    /// Set the namespace.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Get the fully qualified name.
    pub fn fullname(&self) -> String {
        match &self.namespace {
            Some(ns) => format!("{}.{}", ns, self.name),
            None => self.name.clone(),
        }
    }
}

/// Logical type wrapper around a base schema.
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalType {
    /// The underlying Avro schema.
    pub base: Box<AvroSchema>,
    /// The logical type name and parameters.
    pub logical_type: LogicalTypeName,
}

impl LogicalType {
    /// Create a new LogicalType.
    pub fn new(base: AvroSchema, logical_type: LogicalTypeName) -> Self {
        Self {
            base: Box::new(base),
            logical_type,
        }
    }
}

/// Logical type names with their parameters.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalTypeName {
    /// Decimal with precision and scale.
    Decimal { precision: u32, scale: u32 },
    /// UUID (typically stored as string or fixed[16]).
    Uuid,
    /// Date (days since Unix epoch).
    Date,
    /// Time in milliseconds.
    TimeMillis,
    /// Time in microseconds.
    TimeMicros,
    /// Timestamp in milliseconds since Unix epoch.
    TimestampMillis,
    /// Timestamp in microseconds since Unix epoch.
    TimestampMicros,
    /// Duration (months, days, milliseconds).
    Duration,
    /// Local timestamp in milliseconds (no timezone).
    LocalTimestampMillis,
    /// Local timestamp in microseconds (no timezone).
    LocalTimestampMicros,
}

impl LogicalTypeName {
    /// Get the string name of the logical type.
    pub fn name(&self) -> &'static str {
        match self {
            LogicalTypeName::Decimal { .. } => "decimal",
            LogicalTypeName::Uuid => "uuid",
            LogicalTypeName::Date => "date",
            LogicalTypeName::TimeMillis => "time-millis",
            LogicalTypeName::TimeMicros => "time-micros",
            LogicalTypeName::TimestampMillis => "timestamp-millis",
            LogicalTypeName::TimestampMicros => "timestamp-micros",
            LogicalTypeName::Duration => "duration",
            LogicalTypeName::LocalTimestampMillis => "local-timestamp-millis",
            LogicalTypeName::LocalTimestampMicros => "local-timestamp-micros",
        }
    }
}

impl AvroSchema {
    /// Check if this schema is a primitive type.
    pub fn is_primitive(&self) -> bool {
        matches!(
            self,
            AvroSchema::Null
                | AvroSchema::Boolean
                | AvroSchema::Int
                | AvroSchema::Long
                | AvroSchema::Float
                | AvroSchema::Double
                | AvroSchema::Bytes
                | AvroSchema::String
        )
    }

    /// Check if this schema is a named type (record, enum, or fixed).
    pub fn is_named(&self) -> bool {
        matches!(
            self,
            AvroSchema::Record(_) | AvroSchema::Enum(_) | AvroSchema::Fixed(_)
        )
    }

    /// Get the name of a named type, if applicable.
    pub fn name(&self) -> Option<&str> {
        match self {
            AvroSchema::Record(r) => Some(&r.name),
            AvroSchema::Enum(e) => Some(&e.name),
            AvroSchema::Fixed(f) => Some(&f.name),
            AvroSchema::Named(n) => Some(n),
            _ => None,
        }
    }

    /// Get the fully qualified name of a named type, if applicable.
    pub fn fullname(&self) -> Option<String> {
        match self {
            AvroSchema::Record(r) => Some(r.fullname()),
            AvroSchema::Enum(e) => Some(e.fullname()),
            AvroSchema::Fixed(f) => Some(f.fullname()),
            AvroSchema::Named(n) => Some(n.clone()),
            _ => None,
        }
    }

    /// Check if this schema represents a nullable type (union with null).
    pub fn is_nullable(&self) -> bool {
        match self {
            AvroSchema::Union(variants) => variants.iter().any(|v| matches!(v, AvroSchema::Null)),
            _ => false,
        }
    }

    /// For a nullable union, get the non-null schema.
    pub fn nullable_inner(&self) -> Option<&AvroSchema> {
        match self {
            AvroSchema::Union(variants) if variants.len() == 2 => {
                variants.iter().find(|v| !matches!(v, AvroSchema::Null))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_types() {
        assert!(AvroSchema::Null.is_primitive());
        assert!(AvroSchema::Boolean.is_primitive());
        assert!(AvroSchema::Int.is_primitive());
        assert!(AvroSchema::Long.is_primitive());
        assert!(AvroSchema::Float.is_primitive());
        assert!(AvroSchema::Double.is_primitive());
        assert!(AvroSchema::Bytes.is_primitive());
        assert!(AvroSchema::String.is_primitive());
    }

    #[test]
    fn test_record_schema() {
        let fields = vec![
            FieldSchema::new("id", AvroSchema::Long),
            FieldSchema::new("name", AvroSchema::String),
        ];
        let record = RecordSchema::new("User", fields).with_namespace("com.example");

        assert_eq!(record.name, "User");
        assert_eq!(record.namespace, Some("com.example".to_string()));
        assert_eq!(record.fullname(), "com.example.User");
        assert_eq!(record.fields.len(), 2);
    }

    #[test]
    fn test_enum_schema() {
        let symbols = vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()];
        let enum_schema = EnumSchema::new("Color", symbols);

        assert_eq!(enum_schema.symbol_index("RED"), Some(0));
        assert_eq!(enum_schema.symbol_index("GREEN"), Some(1));
        assert_eq!(enum_schema.symbol_index("BLUE"), Some(2));
        assert_eq!(enum_schema.symbol_index("YELLOW"), None);
    }

    #[test]
    fn test_fixed_schema() {
        let fixed = FixedSchema::new("MD5", 16).with_namespace("com.example");

        assert_eq!(fixed.name, "MD5");
        assert_eq!(fixed.size, 16);
        assert_eq!(fixed.fullname(), "com.example.MD5");
    }

    #[test]
    fn test_logical_type() {
        let decimal = LogicalType::new(
            AvroSchema::Bytes,
            LogicalTypeName::Decimal {
                precision: 10,
                scale: 2,
            },
        );

        assert_eq!(decimal.logical_type.name(), "decimal");
        assert_eq!(*decimal.base, AvroSchema::Bytes);
    }

    #[test]
    fn test_nullable_union() {
        let nullable_string = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]);

        assert!(nullable_string.is_nullable());
        assert_eq!(nullable_string.nullable_inner(), Some(&AvroSchema::String));

        let non_nullable = AvroSchema::String;
        assert!(!non_nullable.is_nullable());
        assert_eq!(non_nullable.nullable_inner(), None);
    }

    #[test]
    fn test_named_types() {
        let record = AvroSchema::Record(RecordSchema::new("Test", vec![]));
        assert!(record.is_named());
        assert_eq!(record.name(), Some("Test"));

        let enum_type = AvroSchema::Enum(EnumSchema::new("Status", vec!["OK".to_string()]));
        assert!(enum_type.is_named());

        let fixed = AvroSchema::Fixed(FixedSchema::new("Hash", 32));
        assert!(fixed.is_named());

        assert!(!AvroSchema::Int.is_named());
    }

    #[test]
    fn test_field_schema_with_default() {
        let field = FieldSchema::new("count", AvroSchema::Int)
            .with_default(serde_json::json!(0))
            .with_doc("The count value");

        assert_eq!(field.name, "count");
        assert_eq!(field.default, Some(serde_json::json!(0)));
        assert_eq!(field.doc, Some("The count value".to_string()));
    }
}
