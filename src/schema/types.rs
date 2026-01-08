//! Avro schema types and representations.
//!
//! This module defines the complete Avro schema type system including
//! primitives, complex types, and logical types.

use serde_json::{json, Map, Value};

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

    /// Serialize the record schema to a JSON Value.
    pub fn to_json_value(&self) -> Value {
        let mut obj = Map::new();
        obj.insert("type".to_string(), json!("record"));
        obj.insert("name".to_string(), json!(&self.name));

        if let Some(ns) = &self.namespace {
            obj.insert("namespace".to_string(), json!(ns));
        }

        if let Some(doc) = &self.doc {
            obj.insert("doc".to_string(), json!(doc));
        }

        if !self.aliases.is_empty() {
            obj.insert("aliases".to_string(), json!(&self.aliases));
        }

        let fields: Vec<Value> = self.fields.iter().map(|f| f.to_json_value()).collect();
        obj.insert("fields".to_string(), Value::Array(fields));

        Value::Object(obj)
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

    /// Serialize the field schema to a JSON Value.
    pub fn to_json_value(&self) -> Value {
        let mut obj = Map::new();
        obj.insert("name".to_string(), json!(&self.name));
        obj.insert("type".to_string(), self.schema.to_json_value());

        if let Some(default) = &self.default {
            obj.insert("default".to_string(), default.clone());
        }

        if let Some(doc) = &self.doc {
            obj.insert("doc".to_string(), json!(doc));
        }

        if self.order != FieldOrder::Ascending {
            let order_str = match self.order {
                FieldOrder::Ascending => "ascending",
                FieldOrder::Descending => "descending",
                FieldOrder::Ignore => "ignore",
            };
            obj.insert("order".to_string(), json!(order_str));
        }

        if !self.aliases.is_empty() {
            obj.insert("aliases".to_string(), json!(&self.aliases));
        }

        Value::Object(obj)
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

    /// Serialize the enum schema to a JSON Value.
    pub fn to_json_value(&self) -> Value {
        let mut obj = Map::new();
        obj.insert("type".to_string(), json!("enum"));
        obj.insert("name".to_string(), json!(&self.name));

        if let Some(ns) = &self.namespace {
            obj.insert("namespace".to_string(), json!(ns));
        }

        if let Some(doc) = &self.doc {
            obj.insert("doc".to_string(), json!(doc));
        }

        if !self.aliases.is_empty() {
            obj.insert("aliases".to_string(), json!(&self.aliases));
        }

        obj.insert("symbols".to_string(), json!(&self.symbols));

        if let Some(default) = &self.default {
            obj.insert("default".to_string(), json!(default));
        }

        Value::Object(obj)
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

    /// Serialize the fixed schema to a JSON Value.
    pub fn to_json_value(&self) -> Value {
        let mut obj = Map::new();
        obj.insert("type".to_string(), json!("fixed"));
        obj.insert("name".to_string(), json!(&self.name));

        if let Some(ns) = &self.namespace {
            obj.insert("namespace".to_string(), json!(ns));
        }

        if let Some(doc) = &self.doc {
            obj.insert("doc".to_string(), json!(doc));
        }

        if !self.aliases.is_empty() {
            obj.insert("aliases".to_string(), json!(&self.aliases));
        }

        obj.insert("size".to_string(), json!(self.size));

        Value::Object(obj)
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

    /// Serialize the logical type to a JSON Value.
    ///
    /// The logical type is serialized as the base type with additional
    /// logicalType and any type-specific fields.
    pub fn to_json_value(&self) -> Value {
        // For logical types, we need to serialize the base type as an object
        // and add the logicalType field
        let mut obj = match &*self.base {
            AvroSchema::Int => {
                let mut m = Map::new();
                m.insert("type".to_string(), json!("int"));
                m
            }
            AvroSchema::Long => {
                let mut m = Map::new();
                m.insert("type".to_string(), json!("long"));
                m
            }
            AvroSchema::Bytes => {
                let mut m = Map::new();
                m.insert("type".to_string(), json!("bytes"));
                m
            }
            AvroSchema::String => {
                let mut m = Map::new();
                m.insert("type".to_string(), json!("string"));
                m
            }
            AvroSchema::Fixed(f) => {
                // For fixed, we need to include all the fixed fields
                let base_value = f.to_json_value();
                if let Value::Object(m) = base_value {
                    m
                } else {
                    let mut m = Map::new();
                    m.insert("type".to_string(), json!("fixed"));
                    m
                }
            }
            _ => {
                // Fallback for other base types
                let mut m = Map::new();
                m.insert("type".to_string(), self.base.to_json_value());
                m
            }
        };

        obj.insert("logicalType".to_string(), json!(self.logical_type.name()));

        // Add type-specific fields
        if let LogicalTypeName::Decimal { precision, scale } = &self.logical_type {
            obj.insert("precision".to_string(), json!(precision));
            if *scale > 0 {
                obj.insert("scale".to_string(), json!(scale));
            }
        }

        Value::Object(obj)
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

    /// Serialize the schema to a JSON string.
    ///
    /// This produces canonical Avro schema JSON that can be parsed back
    /// to an equivalent schema.
    ///
    /// # Example
    /// ```
    /// use jetliner::schema::AvroSchema;
    ///
    /// let schema = AvroSchema::String;
    /// assert_eq!(schema.to_json(), r#""string""#);
    /// ```
    pub fn to_json(&self) -> String {
        let value = self.to_json_value();
        serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string())
    }

    /// Serialize the schema to a JSON Value.
    ///
    /// This is useful when you need to embed the schema in a larger JSON structure.
    pub fn to_json_value(&self) -> Value {
        match self {
            // Primitive types serialize as simple strings
            AvroSchema::Null => json!("null"),
            AvroSchema::Boolean => json!("boolean"),
            AvroSchema::Int => json!("int"),
            AvroSchema::Long => json!("long"),
            AvroSchema::Float => json!("float"),
            AvroSchema::Double => json!("double"),
            AvroSchema::Bytes => json!("bytes"),
            AvroSchema::String => json!("string"),

            // Complex types
            AvroSchema::Record(r) => r.to_json_value(),
            AvroSchema::Enum(e) => e.to_json_value(),
            AvroSchema::Array(items) => {
                json!({
                    "type": "array",
                    "items": items.to_json_value()
                })
            }
            AvroSchema::Map(values) => {
                json!({
                    "type": "map",
                    "values": values.to_json_value()
                })
            }
            AvroSchema::Union(variants) => {
                Value::Array(variants.iter().map(|v| v.to_json_value()).collect())
            }
            AvroSchema::Fixed(f) => f.to_json_value(),

            // Named type reference - just the name string
            AvroSchema::Named(name) => json!(name),

            // Logical type wrapper
            AvroSchema::Logical(lt) => lt.to_json_value(),
        }
    }
}
