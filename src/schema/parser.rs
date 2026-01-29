//! JSON schema parser for Avro schemas.
//!
//! Parses Avro schema JSON into the AvroSchema type hierarchy.

use std::collections::HashMap;

use serde_json::Value;

use crate::error::SchemaError;
use crate::schema::{
    AvroSchema, EnumSchema, FieldOrder, FieldSchema, FixedSchema, LogicalType, LogicalTypeName,
    RecordSchema,
};

/// Parse an Avro schema from a JSON string.
///
/// # Arguments
/// * `json` - JSON string representing an Avro schema
///
/// # Returns
/// The parsed AvroSchema or a SchemaError
///
/// # Example
/// ```
/// use jetliner::schema::parse_schema;
///
/// let schema = parse_schema(r#""string""#).unwrap();
/// ```
pub fn parse_schema(json: &str) -> Result<AvroSchema, SchemaError> {
    parse_schema_with_options(json, false)
}

/// Parse an Avro schema from a JSON string with validation options.
///
/// # Arguments
/// * `json` - JSON string representing an Avro schema
/// * `strict` - Whether to enforce strict schema validation
///
/// In strict mode:
/// - Union types cannot contain duplicate types
/// - Union types cannot contain nested unions
/// - Names must follow Avro naming rules (start with letter/underscore, contain only alphanumeric/underscore)
///
/// In permissive mode (default), these violations generate warnings but don't fail parsing.
/// For read-only use, permissive parsing maximizes compatibility with existing files.
///
/// # Returns
/// The parsed AvroSchema or a SchemaError
///
/// # Example
/// ```
/// use jetliner::schema::parse_schema_with_options;
///
/// // Permissive mode - warnings only
/// let schema = parse_schema_with_options(r#"["int", "int"]"#, false).unwrap();
///
/// // Strict mode - fails on duplicate types in union
/// let result = parse_schema_with_options(r#"["int", "int"]"#, true);
/// assert!(result.is_err());
/// ```
pub fn parse_schema_with_options(json: &str, strict: bool) -> Result<AvroSchema, SchemaError> {
    let value: Value = serde_json::from_str(json)
        .map_err(|e| SchemaError::ParseError(format!("Invalid JSON: {}", e)))?;

    let mut parser = SchemaParser::new().with_strict(strict);
    parser.parse(&value)
}

/// Schema parser with named type resolution context.
///
/// Maintains a registry of named types (records, enums, fixed) for resolving
/// type references during parsing.
#[derive(Debug, Default)]
pub struct SchemaParser {
    /// Registry of named types by their fully qualified name
    named_types: HashMap<String, AvroSchema>,
    /// Current namespace for resolving unqualified names
    current_namespace: Option<String>,
    /// Whether to enforce strict schema validation
    strict_schema: bool,
}

impl SchemaParser {
    /// Create a new SchemaParser with default settings (permissive mode).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new SchemaParser with strict validation enabled.
    ///
    /// In strict mode:
    /// - Union types cannot contain duplicate types
    /// - Union types cannot contain nested unions
    /// - Names must follow Avro naming rules (start with letter/underscore, contain only alphanumeric/underscore)
    ///
    /// In permissive mode (default), these violations generate warnings but don't fail parsing.
    pub fn new_strict() -> Self {
        Self {
            named_types: HashMap::new(),
            current_namespace: None,
            strict_schema: true,
        }
    }

    /// Set whether to use strict schema validation.
    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict_schema = strict;
        self
    }

    /// Parse a JSON value into an AvroSchema.
    pub fn parse(&mut self, value: &Value) -> Result<AvroSchema, SchemaError> {
        match value {
            Value::String(s) => self.parse_string_schema(s),
            Value::Object(obj) => self.parse_object_schema(obj),
            Value::Array(arr) => self.parse_union_schema(arr),
            _ => Err(SchemaError::InvalidSchema(format!(
                "Expected string, object, or array, found: {:?}",
                value
            ))),
        }
    }

    /// Parse a primitive type or named type reference from a string.
    fn parse_string_schema(&self, s: &str) -> Result<AvroSchema, SchemaError> {
        match s {
            "null" => Ok(AvroSchema::Null),
            "boolean" => Ok(AvroSchema::Boolean),
            "int" => Ok(AvroSchema::Int),
            "long" => Ok(AvroSchema::Long),
            "float" => Ok(AvroSchema::Float),
            "double" => Ok(AvroSchema::Double),
            "bytes" => Ok(AvroSchema::Bytes),
            "string" => Ok(AvroSchema::String),
            name => {
                // Try to resolve as a named type reference
                let fullname = self.resolve_name(name);
                if self.named_types.contains_key(&fullname) {
                    Ok(AvroSchema::Named(fullname))
                } else {
                    // Return as Named - it may be defined later or in a recursive context
                    Ok(AvroSchema::Named(fullname))
                }
            }
        }
    }

    /// Parse a complex type from a JSON object.
    fn parse_object_schema(
        &mut self,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<AvroSchema, SchemaError> {
        // Check for logical type first
        if let Some(logical_type) = obj.get("logicalType") {
            return self.parse_logical_type(obj, logical_type);
        }

        // Get the type field
        let type_str = obj
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SchemaError::InvalidSchema("Missing 'type' field".to_string()))?;

        match type_str {
            // Primitive types can also appear as objects (with logical types)
            "null" => Ok(AvroSchema::Null),
            "boolean" => Ok(AvroSchema::Boolean),
            "int" => self.maybe_wrap_logical(obj, AvroSchema::Int),
            "long" => self.maybe_wrap_logical(obj, AvroSchema::Long),
            "float" => Ok(AvroSchema::Float),
            "double" => Ok(AvroSchema::Double),
            "bytes" => self.maybe_wrap_logical(obj, AvroSchema::Bytes),
            "string" => self.maybe_wrap_logical(obj, AvroSchema::String),

            // Complex types
            "record" => self.parse_record_schema(obj),
            "enum" => self.parse_enum_schema(obj),
            "array" => self.parse_array_schema(obj),
            "map" => self.parse_map_schema(obj),
            "fixed" => self.parse_fixed_schema(obj),

            // Type could be a named reference
            other => {
                let fullname = self.resolve_name(other);
                if self.named_types.contains_key(&fullname) {
                    Ok(AvroSchema::Named(fullname))
                } else {
                    Err(SchemaError::UnsupportedType(format!(
                        "Unknown type: {}",
                        other
                    )))
                }
            }
        }
    }

    /// Parse a union schema from a JSON array.
    fn parse_union_schema(&mut self, arr: &[Value]) -> Result<AvroSchema, SchemaError> {
        if arr.is_empty() {
            return Err(SchemaError::InvalidSchema(
                "Union schema cannot be empty".to_string(),
            ));
        }

        let variants: Result<Vec<AvroSchema>, SchemaError> =
            arr.iter().map(|v| self.parse(v)).collect();

        let variants = variants?;

        // Validate union rules
        self.validate_union(&variants)?;

        Ok(AvroSchema::Union(variants))
    }

    /// Parse a record schema.
    fn parse_record_schema(
        &mut self,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<AvroSchema, SchemaError> {
        let name = obj
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SchemaError::InvalidSchema("Record missing 'name' field".to_string()))?
            .to_string();

        // Validate name
        self.validate_name(&name, "Record")?;

        let namespace = obj
            .get("namespace")
            .and_then(|v| v.as_str())
            .map(String::from);

        // Validate namespace if present
        if let Some(ref ns) = namespace {
            self.validate_namespace(ns, "Record")?;
        }

        // Update current namespace for nested types
        let prev_namespace = self.current_namespace.clone();
        if namespace.is_some() {
            self.current_namespace = namespace.clone();
        } else if self.current_namespace.is_none() {
            // If name contains a dot, extract namespace from it
            if let Some(dot_pos) = name.rfind('.') {
                self.current_namespace = Some(name[..dot_pos].to_string());
            }
        }

        let fullname = match &namespace {
            Some(ns) => format!("{}.{}", ns, name),
            None => match &self.current_namespace {
                Some(ns) if !name.contains('.') => format!("{}.{}", ns, name),
                _ => name.clone(),
            },
        };

        // Register the type before parsing fields (for recursive types)
        // Use a placeholder that will be replaced
        self.named_types
            .insert(fullname.clone(), AvroSchema::Named(fullname.clone()));

        let doc = obj.get("doc").and_then(|v| v.as_str()).map(String::from);

        let aliases = obj
            .get("aliases")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        // Parse fields
        let fields_value = obj
            .get("fields")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                SchemaError::InvalidSchema("Record missing 'fields' array".to_string())
            })?;

        let fields: Result<Vec<FieldSchema>, SchemaError> = fields_value
            .iter()
            .map(|f| self.parse_field_schema(f))
            .collect();

        // Restore previous namespace
        self.current_namespace = prev_namespace;

        let record = RecordSchema {
            name: if name.contains('.') {
                name.rsplit('.').next().unwrap_or(&name).to_string()
            } else {
                name
            },
            namespace: namespace.or_else(|| {
                if fullname.contains('.') {
                    Some(
                        fullname
                            .rsplit_once('.')
                            .map(|(ns, _)| ns.to_string())
                            .unwrap_or_default(),
                    )
                } else {
                    None
                }
            }),
            fields: fields?,
            doc,
            aliases,
        };

        let schema = AvroSchema::Record(record);

        // Update the registry with the actual schema
        self.named_types.insert(fullname, schema.clone());

        Ok(schema)
    }

    /// Parse a field schema within a record.
    fn parse_field_schema(&mut self, value: &Value) -> Result<FieldSchema, SchemaError> {
        let obj = value
            .as_object()
            .ok_or_else(|| SchemaError::InvalidSchema("Field must be an object".to_string()))?;

        let name = obj
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SchemaError::InvalidSchema("Field missing 'name'".to_string()))?
            .to_string();

        // Validate field name
        self.validate_name(&name, "Field")?;

        let type_value = obj
            .get("type")
            .ok_or_else(|| SchemaError::InvalidSchema("Field missing 'type'".to_string()))?;

        let schema = self.parse(type_value)?;

        let default = obj.get("default").cloned();

        let doc = obj.get("doc").and_then(|v| v.as_str()).map(String::from);

        let order = obj
            .get("order")
            .and_then(|v| v.as_str())
            .map(|s| match s {
                "ascending" => FieldOrder::Ascending,
                "descending" => FieldOrder::Descending,
                "ignore" => FieldOrder::Ignore,
                _ => FieldOrder::Ascending,
            })
            .unwrap_or(FieldOrder::Ascending);

        let aliases = obj
            .get("aliases")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(FieldSchema {
            name,
            schema,
            default,
            doc,
            order,
            aliases,
        })
    }

    /// Parse an enum schema.
    fn parse_enum_schema(
        &mut self,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<AvroSchema, SchemaError> {
        let name = obj
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SchemaError::InvalidSchema("Enum missing 'name' field".to_string()))?
            .to_string();

        // Validate name
        self.validate_name(&name, "Enum")?;

        let namespace = obj
            .get("namespace")
            .and_then(|v| v.as_str())
            .map(String::from);

        // Validate namespace if present
        if let Some(ref ns) = namespace {
            self.validate_namespace(ns, "Enum")?;
        }

        let fullname = match &namespace {
            Some(ns) => format!("{}.{}", ns, name),
            None => match &self.current_namespace {
                Some(ns) if !name.contains('.') => format!("{}.{}", ns, name),
                _ => name.clone(),
            },
        };

        let symbols = obj
            .get("symbols")
            .and_then(|v| v.as_array())
            .ok_or_else(|| SchemaError::InvalidSchema("Enum missing 'symbols' array".to_string()))?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect::<Vec<_>>();

        if symbols.is_empty() {
            return Err(SchemaError::InvalidSchema(
                "Enum must have at least one symbol".to_string(),
            ));
        }

        // Validate each symbol name
        for symbol in &symbols {
            self.validate_name(symbol, "Enum symbol")?;
        }

        let doc = obj.get("doc").and_then(|v| v.as_str()).map(String::from);

        let aliases = obj
            .get("aliases")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let default = obj
            .get("default")
            .and_then(|v| v.as_str())
            .map(String::from);

        let enum_schema = EnumSchema {
            name: if name.contains('.') {
                name.rsplit('.').next().unwrap_or(&name).to_string()
            } else {
                name
            },
            namespace: namespace.or_else(|| {
                if fullname.contains('.') {
                    Some(
                        fullname
                            .rsplit_once('.')
                            .map(|(ns, _)| ns.to_string())
                            .unwrap_or_default(),
                    )
                } else {
                    None
                }
            }),
            symbols,
            doc,
            aliases,
            default,
        };

        let schema = AvroSchema::Enum(enum_schema);
        self.named_types.insert(fullname, schema.clone());

        Ok(schema)
    }

    /// Parse an array schema.
    fn parse_array_schema(
        &mut self,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<AvroSchema, SchemaError> {
        let items = obj
            .get("items")
            .ok_or_else(|| SchemaError::InvalidSchema("Array missing 'items' field".to_string()))?;

        let item_schema = self.parse(items)?;
        Ok(AvroSchema::Array(Box::new(item_schema)))
    }

    /// Parse a map schema.
    fn parse_map_schema(
        &mut self,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<AvroSchema, SchemaError> {
        let values = obj
            .get("values")
            .ok_or_else(|| SchemaError::InvalidSchema("Map missing 'values' field".to_string()))?;

        let value_schema = self.parse(values)?;
        Ok(AvroSchema::Map(Box::new(value_schema)))
    }

    /// Parse a fixed schema.
    fn parse_fixed_schema(
        &mut self,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<AvroSchema, SchemaError> {
        let name = obj
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SchemaError::InvalidSchema("Fixed missing 'name' field".to_string()))?
            .to_string();

        // Validate name
        self.validate_name(&name, "Fixed")?;

        let namespace = obj
            .get("namespace")
            .and_then(|v| v.as_str())
            .map(String::from);

        // Validate namespace if present
        if let Some(ref ns) = namespace {
            self.validate_namespace(ns, "Fixed")?;
        }

        let fullname = match &namespace {
            Some(ns) => format!("{}.{}", ns, name),
            None => match &self.current_namespace {
                Some(ns) if !name.contains('.') => format!("{}.{}", ns, name),
                _ => name.clone(),
            },
        };

        let size =
            obj.get("size").and_then(|v| v.as_u64()).ok_or_else(|| {
                SchemaError::InvalidSchema("Fixed missing 'size' field".to_string())
            })? as usize;

        let doc = obj.get("doc").and_then(|v| v.as_str()).map(String::from);

        let aliases = obj
            .get("aliases")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let fixed_schema = FixedSchema {
            name: if name.contains('.') {
                name.rsplit('.').next().unwrap_or(&name).to_string()
            } else {
                name
            },
            namespace: namespace.or_else(|| {
                if fullname.contains('.') {
                    Some(
                        fullname
                            .rsplit_once('.')
                            .map(|(ns, _)| ns.to_string())
                            .unwrap_or_default(),
                    )
                } else {
                    None
                }
            }),
            size,
            doc,
            aliases,
        };

        let schema = AvroSchema::Fixed(fixed_schema);
        self.named_types.insert(fullname, schema.clone());

        Ok(schema)
    }

    /// Parse a logical type annotation.
    fn parse_logical_type(
        &mut self,
        obj: &serde_json::Map<String, Value>,
        logical_type_value: &Value,
    ) -> Result<AvroSchema, SchemaError> {
        let logical_type_name = logical_type_value.as_str().ok_or_else(|| {
            SchemaError::InvalidSchema("logicalType must be a string".to_string())
        })?;

        // Get the base type
        let type_str = obj.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
            SchemaError::InvalidSchema("Logical type missing 'type' field".to_string())
        })?;

        let base_schema = match type_str {
            "null" => AvroSchema::Null,
            "boolean" => AvroSchema::Boolean,
            "int" => AvroSchema::Int,
            "long" => AvroSchema::Long,
            "float" => AvroSchema::Float,
            "double" => AvroSchema::Double,
            "bytes" => AvroSchema::Bytes,
            "string" => AvroSchema::String,
            "fixed" => self.parse_fixed_schema(obj)?,
            other => {
                return Err(SchemaError::InvalidSchema(format!(
                    "Invalid base type for logical type: {}",
                    other
                )))
            }
        };

        let logical_type = match logical_type_name {
            "decimal" => {
                let precision = obj
                    .get("precision")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| {
                        SchemaError::InvalidSchema("Decimal missing 'precision'".to_string())
                    })? as u32;

                let scale = obj.get("scale").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

                LogicalTypeName::Decimal { precision, scale }
            }
            "uuid" => LogicalTypeName::Uuid,
            "date" => LogicalTypeName::Date,
            "time-millis" => LogicalTypeName::TimeMillis,
            "time-micros" => LogicalTypeName::TimeMicros,
            "timestamp-millis" => LogicalTypeName::TimestampMillis,
            "timestamp-micros" => LogicalTypeName::TimestampMicros,
            "timestamp-nanos" => LogicalTypeName::TimestampNanos,
            "duration" => LogicalTypeName::Duration,
            "local-timestamp-millis" => LogicalTypeName::LocalTimestampMillis,
            "local-timestamp-micros" => LogicalTypeName::LocalTimestampMicros,
            "local-timestamp-nanos" => LogicalTypeName::LocalTimestampNanos,
            "big-decimal" => {
                // big-decimal only supports bytes base type (not fixed)
                if !matches!(base_schema, AvroSchema::Bytes) {
                    return Err(SchemaError::InvalidSchema(
                        "big-decimal logical type requires bytes base type".to_string(),
                    ));
                }
                LogicalTypeName::BigDecimal
            }
            other => {
                // Unknown logical type - preserve the name for schema inspection
                // but treat data as the base type per Avro spec
                LogicalTypeName::Unknown(other.to_string())
            }
        };

        Ok(AvroSchema::Logical(LogicalType::new(
            base_schema,
            logical_type,
        )))
    }

    /// Check if the object has a logical type and wrap if needed.
    fn maybe_wrap_logical(
        &mut self,
        obj: &serde_json::Map<String, Value>,
        base: AvroSchema,
    ) -> Result<AvroSchema, SchemaError> {
        if let Some(logical_type) = obj.get("logicalType") {
            self.parse_logical_type(obj, logical_type)
        } else {
            Ok(base)
        }
    }

    /// Resolve a type name to its fully qualified form.
    fn resolve_name(&self, name: &str) -> String {
        if name.contains('.') {
            // Already fully qualified
            name.to_string()
        } else if let Some(ns) = &self.current_namespace {
            format!("{}.{}", ns, name)
        } else {
            name.to_string()
        }
    }

    /// Get a named type from the registry.
    pub fn get_named_type(&self, name: &str) -> Option<&AvroSchema> {
        self.named_types.get(name)
    }

    /// Get all registered named types.
    pub fn named_types(&self) -> &HashMap<String, AvroSchema> {
        &self.named_types
    }

    /// Validate that a name follows Avro naming rules.
    ///
    /// Avro names must:
    /// - Start with [A-Za-z_]
    /// - Contain only [A-Za-z0-9_]
    ///
    /// Fullnames (containing dots) are also valid - each component is validated separately.
    fn validate_name(&self, name: &str, context: &str) -> Result<(), SchemaError> {
        if name.is_empty() {
            let msg = format!("{} name cannot be empty", context);
            if self.strict_schema {
                return Err(SchemaError::InvalidSchema(msg));
            } else {
                eprintln!("Warning: {}", msg);
                return Ok(());
            }
        }

        // If name contains dots, it's a fullname - validate each component separately
        if name.contains('.') {
            for part in name.split('.') {
                self.validate_simple_name(part, context)?;
            }
            return Ok(());
        }

        self.validate_simple_name(name, context)
    }

    /// Validate that a namespace follows Avro naming rules.
    ///
    /// Per Avro spec: A namespace is a dot-separated sequence of names.
    /// The empty string may also be used as a namespace to indicate the null namespace.
    /// The null namespace may not be used in a dot-separated sequence of names.
    /// Grammar: <empty> | <name>[(<dot><name>)*]
    fn validate_namespace(&self, namespace: &str, context: &str) -> Result<(), SchemaError> {
        // Empty string is valid (null namespace)
        if namespace.is_empty() {
            return Ok(());
        }

        // Validate each component of the namespace
        for part in namespace.split('.') {
            self.validate_simple_name(part, &format!("{} namespace component", context))?;
        }

        Ok(())
    }

    /// Validate a simple name (no dots).
    fn validate_simple_name(&self, name: &str, context: &str) -> Result<(), SchemaError> {
        if name.is_empty() {
            let msg = format!("{} name component cannot be empty", context);
            if self.strict_schema {
                return Err(SchemaError::InvalidSchema(msg));
            } else {
                eprintln!("Warning: {}", msg);
                return Ok(());
            }
        }

        // Check first character
        let first_char = name.chars().next().unwrap();
        if !first_char.is_ascii_alphabetic() && first_char != '_' {
            let msg = format!(
                "{} name '{}' must start with a letter or underscore",
                context, name
            );
            if self.strict_schema {
                return Err(SchemaError::InvalidSchema(msg));
            } else {
                eprintln!("Warning: {}", msg);
                return Ok(());
            }
        }

        // Check remaining characters
        for ch in name.chars() {
            if !ch.is_ascii_alphanumeric() && ch != '_' {
                let msg = format!(
                    "{} name '{}' contains invalid character '{}' (only alphanumeric and underscore allowed)",
                    context, name, ch
                );
                if self.strict_schema {
                    return Err(SchemaError::InvalidSchema(msg));
                } else {
                    eprintln!("Warning: {}", msg);
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Validate union schema rules.
    ///
    /// Avro unions must:
    /// - Not contain duplicate types
    /// - Not contain nested unions
    fn validate_union(&self, variants: &[AvroSchema]) -> Result<(), SchemaError> {
        // Check for nested unions
        for (i, variant) in variants.iter().enumerate() {
            if matches!(variant, AvroSchema::Union(_)) {
                let msg = format!(
                    "Union contains nested union at position {} (unions cannot be nested)",
                    i
                );
                if self.strict_schema {
                    return Err(SchemaError::InvalidSchema(msg));
                } else {
                    eprintln!("Warning: {}", msg);
                }
            }
        }

        // Check for duplicate types
        let mut seen_types = std::collections::HashSet::new();
        for (i, variant) in variants.iter().enumerate() {
            let type_key = self.get_type_key(variant);
            if !seen_types.insert(type_key.clone()) {
                let msg = format!(
                    "Union contains duplicate type '{}' at position {}",
                    type_key, i
                );
                if self.strict_schema {
                    return Err(SchemaError::InvalidSchema(msg));
                } else {
                    eprintln!("Warning: {}", msg);
                }
            }
        }

        Ok(())
    }

    /// Get a unique key for a schema type (for duplicate detection in unions).
    fn get_type_key(&self, schema: &AvroSchema) -> String {
        match schema {
            AvroSchema::Null => "null".to_string(),
            AvroSchema::Boolean => "boolean".to_string(),
            AvroSchema::Int => "int".to_string(),
            AvroSchema::Long => "long".to_string(),
            AvroSchema::Float => "float".to_string(),
            AvroSchema::Double => "double".to_string(),
            AvroSchema::Bytes => "bytes".to_string(),
            AvroSchema::String => "string".to_string(),
            AvroSchema::Array(_) => "array".to_string(),
            AvroSchema::Map(_) => "map".to_string(),
            AvroSchema::Record(r) => format!("record:{}", r.fullname()),
            AvroSchema::Enum(e) => format!("enum:{}", e.fullname()),
            AvroSchema::Fixed(f) => format!("fixed:{}", f.fullname()),
            AvroSchema::Named(n) => format!("named:{}", n),
            AvroSchema::Union(_) => "union".to_string(),
            AvroSchema::Logical(lt) => format!("logical:{}", self.get_type_key(&lt.base)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Primitive Type Parsing ====================

    #[test]
    fn parse_null() {
        let schema = parse_schema(r#""null""#).unwrap();
        assert!(matches!(schema, AvroSchema::Null));
    }

    #[test]
    fn parse_boolean() {
        let schema = parse_schema(r#""boolean""#).unwrap();
        assert!(matches!(schema, AvroSchema::Boolean));
    }

    #[test]
    fn parse_int() {
        let schema = parse_schema(r#""int""#).unwrap();
        assert!(matches!(schema, AvroSchema::Int));
    }

    #[test]
    fn parse_long() {
        let schema = parse_schema(r#""long""#).unwrap();
        assert!(matches!(schema, AvroSchema::Long));
    }

    #[test]
    fn parse_float() {
        let schema = parse_schema(r#""float""#).unwrap();
        assert!(matches!(schema, AvroSchema::Float));
    }

    #[test]
    fn parse_double() {
        let schema = parse_schema(r#""double""#).unwrap();
        assert!(matches!(schema, AvroSchema::Double));
    }

    #[test]
    fn parse_bytes() {
        let schema = parse_schema(r#""bytes""#).unwrap();
        assert!(matches!(schema, AvroSchema::Bytes));
    }

    #[test]
    fn parse_string() {
        let schema = parse_schema(r#""string""#).unwrap();
        assert!(matches!(schema, AvroSchema::String));
    }

    #[test]
    fn parse_primitive_as_object() {
        let schema = parse_schema(r#"{"type": "int"}"#).unwrap();
        assert!(matches!(schema, AvroSchema::Int));
    }

    // ==================== Name Validation ====================

    #[test]
    fn valid_simple_name() {
        let schema = parse_schema(r#"{"type": "record", "name": "User", "fields": []}"#).unwrap();
        assert!(matches!(schema, AvroSchema::Record(_)));
    }

    #[test]
    fn valid_name_with_underscore() {
        let schema =
            parse_schema(r#"{"type": "record", "name": "_User_123", "fields": []}"#).unwrap();
        assert!(matches!(schema, AvroSchema::Record(_)));
    }

    #[test]
    fn valid_fullname_with_dots() {
        let schema =
            parse_schema(r#"{"type": "record", "name": "com.example.User", "fields": []}"#)
                .unwrap();
        if let AvroSchema::Record(r) = schema {
            assert_eq!(r.fullname(), "com.example.User");
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn strict_rejects_name_starting_with_digit() {
        let result =
            parse_schema_with_options(r#"{"type": "record", "name": "1User", "fields": []}"#, true);
        assert!(result.is_err());
    }

    #[test]
    fn strict_rejects_name_with_invalid_char() {
        let result = parse_schema_with_options(
            r#"{"type": "record", "name": "User-Name", "fields": []}"#,
            true,
        );
        assert!(result.is_err());
    }

    #[test]
    fn strict_rejects_empty_name() {
        let result =
            parse_schema_with_options(r#"{"type": "record", "name": "", "fields": []}"#, true);
        assert!(result.is_err());
    }

    #[test]
    fn permissive_allows_invalid_name() {
        // Permissive mode warns but doesn't fail
        let result = parse_schema(r#"{"type": "record", "name": "1User", "fields": []}"#);
        assert!(result.is_ok());
    }

    // ==================== Namespace Handling ====================

    #[test]
    fn namespace_from_explicit_field() {
        let schema = parse_schema(
            r#"{"type": "record", "name": "User", "namespace": "com.example", "fields": []}"#,
        )
        .unwrap();
        if let AvroSchema::Record(r) = schema {
            assert_eq!(r.namespace, Some("com.example".to_string()));
            assert_eq!(r.fullname(), "com.example.User");
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn namespace_inherited_by_nested_type() {
        let schema = parse_schema(
            r#"{
            "type": "record",
            "name": "Outer",
            "namespace": "com.example",
            "fields": [{
                "name": "inner",
                "type": {
                    "type": "record",
                    "name": "Inner",
                    "fields": []
                }
            }]
        }"#,
        )
        .unwrap();

        if let AvroSchema::Record(r) = schema {
            if let AvroSchema::Record(inner) = &r.fields[0].schema {
                assert_eq!(inner.fullname(), "com.example.Inner");
            } else {
                panic!("Expected nested Record");
            }
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn empty_namespace_is_valid() {
        let result = parse_schema_with_options(
            r#"{"type": "record", "name": "User", "namespace": "", "fields": []}"#,
            true,
        );
        assert!(result.is_ok());
    }

    // ==================== Union Schemas ====================

    #[test]
    fn parse_simple_union() {
        let schema = parse_schema(r#"["null", "string"]"#).unwrap();
        if let AvroSchema::Union(variants) = schema {
            assert_eq!(variants.len(), 2);
            assert!(matches!(variants[0], AvroSchema::Null));
            assert!(matches!(variants[1], AvroSchema::String));
        } else {
            panic!("Expected Union");
        }
    }

    #[test]
    fn empty_union_is_error() {
        let result = parse_schema(r#"[]"#);
        assert!(result.is_err());
    }

    #[test]
    fn strict_rejects_duplicate_types_in_union() {
        let result = parse_schema_with_options(r#"["int", "int"]"#, true);
        assert!(result.is_err());
    }

    #[test]
    fn strict_rejects_nested_union() {
        let result = parse_schema_with_options(r#"["null", ["string", "int"]]"#, true);
        assert!(result.is_err());
    }

    #[test]
    fn permissive_allows_duplicate_types() {
        let result = parse_schema(r#"["int", "int"]"#);
        assert!(result.is_ok());
    }

    // ==================== Record Schemas ====================

    #[test]
    fn parse_record_with_fields() {
        let schema = parse_schema(
            r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        }"#,
        )
        .unwrap();

        if let AvroSchema::Record(r) = schema {
            assert_eq!(r.name, "User");
            assert_eq!(r.fields.len(), 2);
            assert_eq!(r.fields[0].name, "id");
            assert_eq!(r.fields[1].name, "name");
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn record_missing_name_is_error() {
        let result = parse_schema(r#"{"type": "record", "fields": []}"#);
        assert!(result.is_err());
    }

    #[test]
    fn record_missing_fields_is_error() {
        let result = parse_schema(r#"{"type": "record", "name": "User"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn record_with_doc_and_aliases() {
        let schema = parse_schema(
            r#"{
            "type": "record",
            "name": "User",
            "doc": "A user record",
            "aliases": ["Person", "Account"],
            "fields": []
        }"#,
        )
        .unwrap();

        if let AvroSchema::Record(r) = schema {
            assert_eq!(r.doc, Some("A user record".to_string()));
            assert_eq!(r.aliases, vec!["Person", "Account"]);
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn field_with_default_value() {
        let schema = parse_schema(
            r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "count", "type": "int", "default": 0}
            ]
        }"#,
        )
        .unwrap();

        if let AvroSchema::Record(r) = schema {
            assert_eq!(r.fields[0].default, Some(serde_json::json!(0)));
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn field_order_parsing() {
        let schema = parse_schema(
            r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "a", "type": "int", "order": "ascending"},
                {"name": "b", "type": "int", "order": "descending"},
                {"name": "c", "type": "int", "order": "ignore"}
            ]
        }"#,
        )
        .unwrap();

        if let AvroSchema::Record(r) = schema {
            assert!(matches!(r.fields[0].order, FieldOrder::Ascending));
            assert!(matches!(r.fields[1].order, FieldOrder::Descending));
            assert!(matches!(r.fields[2].order, FieldOrder::Ignore));
        } else {
            panic!("Expected Record");
        }
    }

    // ==================== Enum Schemas ====================

    #[test]
    fn parse_enum() {
        let schema = parse_schema(
            r#"{
            "type": "enum",
            "name": "Color",
            "symbols": ["RED", "GREEN", "BLUE"]
        }"#,
        )
        .unwrap();

        if let AvroSchema::Enum(e) = schema {
            assert_eq!(e.name, "Color");
            assert_eq!(e.symbols, vec!["RED", "GREEN", "BLUE"]);
        } else {
            panic!("Expected Enum");
        }
    }

    #[test]
    fn enum_missing_symbols_is_error() {
        let result = parse_schema(r#"{"type": "enum", "name": "Color"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn enum_empty_symbols_is_error() {
        let result = parse_schema(r#"{"type": "enum", "name": "Color", "symbols": []}"#);
        assert!(result.is_err());
    }

    #[test]
    fn enum_with_default() {
        let schema = parse_schema(
            r#"{
            "type": "enum",
            "name": "Color",
            "symbols": ["RED", "GREEN", "BLUE"],
            "default": "RED"
        }"#,
        )
        .unwrap();

        if let AvroSchema::Enum(e) = schema {
            assert_eq!(e.default, Some("RED".to_string()));
        } else {
            panic!("Expected Enum");
        }
    }

    // ==================== Array Schemas ====================

    #[test]
    fn parse_array() {
        let schema = parse_schema(r#"{"type": "array", "items": "string"}"#).unwrap();
        if let AvroSchema::Array(items) = schema {
            assert!(matches!(*items, AvroSchema::String));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn array_missing_items_is_error() {
        let result = parse_schema(r#"{"type": "array"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn nested_array() {
        let schema =
            parse_schema(r#"{"type": "array", "items": {"type": "array", "items": "int"}}"#)
                .unwrap();
        if let AvroSchema::Array(outer) = schema {
            if let AvroSchema::Array(inner) = *outer {
                assert!(matches!(*inner, AvroSchema::Int));
            } else {
                panic!("Expected nested Array");
            }
        } else {
            panic!("Expected Array");
        }
    }

    // ==================== Map Schemas ====================

    #[test]
    fn parse_map() {
        let schema = parse_schema(r#"{"type": "map", "values": "long"}"#).unwrap();
        if let AvroSchema::Map(values) = schema {
            assert!(matches!(*values, AvroSchema::Long));
        } else {
            panic!("Expected Map");
        }
    }

    #[test]
    fn map_missing_values_is_error() {
        let result = parse_schema(r#"{"type": "map"}"#);
        assert!(result.is_err());
    }

    // ==================== Fixed Schemas ====================

    #[test]
    fn parse_fixed() {
        let schema = parse_schema(r#"{"type": "fixed", "name": "MD5", "size": 16}"#).unwrap();
        if let AvroSchema::Fixed(f) = schema {
            assert_eq!(f.name, "MD5");
            assert_eq!(f.size, 16);
        } else {
            panic!("Expected Fixed");
        }
    }

    #[test]
    fn fixed_missing_size_is_error() {
        let result = parse_schema(r#"{"type": "fixed", "name": "MD5"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn fixed_missing_name_is_error() {
        let result = parse_schema(r#"{"type": "fixed", "size": 16}"#);
        assert!(result.is_err());
    }

    // ==================== Logical Types ====================

    #[test]
    fn parse_date() {
        let schema = parse_schema(r#"{"type": "int", "logicalType": "date"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(*lt.base, AvroSchema::Int));
            assert!(matches!(lt.logical_type, LogicalTypeName::Date));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_time_millis() {
        let schema = parse_schema(r#"{"type": "int", "logicalType": "time-millis"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(lt.logical_type, LogicalTypeName::TimeMillis));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_time_micros() {
        let schema = parse_schema(r#"{"type": "long", "logicalType": "time-micros"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(lt.logical_type, LogicalTypeName::TimeMicros));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_timestamp_millis() {
        let schema =
            parse_schema(r#"{"type": "long", "logicalType": "timestamp-millis"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(lt.logical_type, LogicalTypeName::TimestampMillis));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_timestamp_micros() {
        let schema =
            parse_schema(r#"{"type": "long", "logicalType": "timestamp-micros"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(lt.logical_type, LogicalTypeName::TimestampMicros));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_timestamp_nanos() {
        let schema = parse_schema(r#"{"type": "long", "logicalType": "timestamp-nanos"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(lt.logical_type, LogicalTypeName::TimestampNanos));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_local_timestamp_millis() {
        let schema =
            parse_schema(r#"{"type": "long", "logicalType": "local-timestamp-millis"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(
                lt.logical_type,
                LogicalTypeName::LocalTimestampMillis
            ));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_local_timestamp_micros() {
        let schema =
            parse_schema(r#"{"type": "long", "logicalType": "local-timestamp-micros"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(
                lt.logical_type,
                LogicalTypeName::LocalTimestampMicros
            ));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_local_timestamp_nanos() {
        let schema =
            parse_schema(r#"{"type": "long", "logicalType": "local-timestamp-nanos"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(
                lt.logical_type,
                LogicalTypeName::LocalTimestampNanos
            ));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_uuid() {
        let schema = parse_schema(r#"{"type": "string", "logicalType": "uuid"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(*lt.base, AvroSchema::String));
            assert!(matches!(lt.logical_type, LogicalTypeName::Uuid));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_decimal_bytes() {
        let schema = parse_schema(
            r#"{"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}"#,
        )
        .unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(*lt.base, AvroSchema::Bytes));
            if let LogicalTypeName::Decimal { precision, scale } = lt.logical_type {
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            } else {
                panic!("Expected Decimal");
            }
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_decimal_fixed() {
        let schema = parse_schema(
            r#"{"type": "fixed", "name": "Money", "size": 8, "logicalType": "decimal", "precision": 18, "scale": 4}"#,
        )
        .unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(*lt.base, AvroSchema::Fixed(_)));
            if let LogicalTypeName::Decimal { precision, scale } = lt.logical_type {
                assert_eq!(precision, 18);
                assert_eq!(scale, 4);
            } else {
                panic!("Expected Decimal");
            }
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn decimal_scale_defaults_to_zero() {
        let schema =
            parse_schema(r#"{"type": "bytes", "logicalType": "decimal", "precision": 10}"#)
                .unwrap();
        if let AvroSchema::Logical(lt) = schema {
            if let LogicalTypeName::Decimal { scale, .. } = lt.logical_type {
                assert_eq!(scale, 0);
            } else {
                panic!("Expected Decimal");
            }
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn decimal_missing_precision_is_error() {
        let result = parse_schema(r#"{"type": "bytes", "logicalType": "decimal"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn parse_duration() {
        let schema = parse_schema(
            r#"{"type": "fixed", "name": "duration", "size": 12, "logicalType": "duration"}"#,
        )
        .unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(lt.logical_type, LogicalTypeName::Duration));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn parse_big_decimal() {
        let schema = parse_schema(r#"{"type": "bytes", "logicalType": "big-decimal"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(*lt.base, AvroSchema::Bytes));
            assert!(matches!(lt.logical_type, LogicalTypeName::BigDecimal));
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn big_decimal_requires_bytes_base() {
        let result = parse_schema(
            r#"{"type": "fixed", "name": "bd", "size": 16, "logicalType": "big-decimal"}"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn unknown_logical_type_preserved_as_logical() {
        let schema = parse_schema(r#"{"type": "string", "logicalType": "custom-type"}"#).unwrap();
        // Unknown logical types are now preserved as Logical with Unknown variant
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(*lt.base, AvroSchema::String));
            assert!(
                matches!(&lt.logical_type, LogicalTypeName::Unknown(name) if name == "custom-type")
            );
            assert_eq!(lt.logical_type.name(), "custom-type");
            assert!(lt.logical_type.is_unknown());
        } else {
            panic!("Expected Logical with Unknown variant, got {:?}", schema);
        }
    }

    #[test]
    fn unknown_logical_type_on_int_base() {
        let schema = parse_schema(r#"{"type": "int", "logicalType": "my-custom-int"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(*lt.base, AvroSchema::Int));
            assert!(
                matches!(&lt.logical_type, LogicalTypeName::Unknown(name) if name == "my-custom-int")
            );
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn unknown_logical_type_on_bytes_base() {
        let schema = parse_schema(r#"{"type": "bytes", "logicalType": "encrypted-data"}"#).unwrap();
        if let AvroSchema::Logical(lt) = schema {
            assert!(matches!(*lt.base, AvroSchema::Bytes));
            assert!(
                matches!(&lt.logical_type, LogicalTypeName::Unknown(name) if name == "encrypted-data")
            );
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn unknown_logical_type_on_fixed_base() {
        let schema = parse_schema(
            r#"{"type": "fixed", "name": "CustomFixed", "size": 32, "logicalType": "hash-sha256"}"#,
        )
        .unwrap();
        if let AvroSchema::Logical(lt) = schema {
            if let AvroSchema::Fixed(f) = &*lt.base {
                assert_eq!(f.name, "CustomFixed");
                assert_eq!(f.size, 32);
            } else {
                panic!("Expected Fixed base type");
            }
            assert!(
                matches!(&lt.logical_type, LogicalTypeName::Unknown(name) if name == "hash-sha256")
            );
        } else {
            panic!("Expected Logical");
        }
    }

    #[test]
    fn unknown_logical_type_serializes_correctly() {
        let schema = parse_schema(r#"{"type": "string", "logicalType": "custom-type"}"#).unwrap();
        let json = schema.to_json();
        // Should serialize back with the logicalType preserved
        assert!(json.contains("\"logicalType\":\"custom-type\""));
        assert!(json.contains("\"type\":\"string\""));
    }

    #[test]
    fn unknown_logical_type_in_record_field() {
        let schema = parse_schema(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "custom_field", "type": {"type": "string", "logicalType": "my-custom-type"}}
            ]
        }"#,
        )
        .unwrap();

        if let AvroSchema::Record(r) = schema {
            assert_eq!(r.fields.len(), 2);
            if let AvroSchema::Logical(lt) = &r.fields[1].schema {
                assert!(
                    matches!(&lt.logical_type, LogicalTypeName::Unknown(name) if name == "my-custom-type")
                );
            } else {
                panic!("Expected Logical for custom_field");
            }
        } else {
            panic!("Expected Record");
        }
    }

    // ==================== Recursive Types ====================

    #[test]
    fn parse_self_recursive_record() {
        let schema = parse_schema(
            r#"{
            "type": "record",
            "name": "Node",
            "fields": [
                {"name": "value", "type": "int"},
                {"name": "next", "type": ["null", "Node"]}
            ]
        }"#,
        )
        .unwrap();

        if let AvroSchema::Record(r) = schema {
            assert_eq!(r.name, "Node");
            if let AvroSchema::Union(variants) = &r.fields[1].schema {
                assert!(matches!(variants[1], AvroSchema::Named(_)));
            } else {
                panic!("Expected Union");
            }
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn parse_mutually_recursive_records() {
        let schema = parse_schema(
            r#"{
            "type": "record",
            "name": "A",
            "fields": [{
                "name": "b",
                "type": {
                    "type": "record",
                    "name": "B",
                    "fields": [{"name": "a", "type": ["null", "A"]}]
                }
            }]
        }"#,
        )
        .unwrap();

        assert!(matches!(schema, AvroSchema::Record(_)));
    }

    // ==================== Named Type References ====================

    #[test]
    fn named_type_reference_resolved() {
        let mut parser = SchemaParser::new();
        let schema = parser
            .parse(
                &serde_json::from_str(
                    r#"{
                "type": "record",
                "name": "Outer",
                "fields": [
                    {"name": "inner", "type": {"type": "record", "name": "Inner", "fields": []}},
                    {"name": "inner_ref", "type": "Inner"}
                ]
            }"#,
                )
                .unwrap(),
            )
            .unwrap();

        if let AvroSchema::Record(r) = schema {
            // Second field should reference the Inner type
            assert!(matches!(r.fields[1].schema, AvroSchema::Named(_)));
        } else {
            panic!("Expected Record");
        }
    }

    // ==================== Error Cases ====================

    #[test]
    fn invalid_json_is_error() {
        let result = parse_schema("not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn number_is_invalid_schema() {
        let result = parse_schema("42");
        assert!(result.is_err());
    }

    #[test]
    fn unknown_type_is_error() {
        let result = parse_schema(r#"{"type": "unknown_type"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn missing_type_field_is_error() {
        let result = parse_schema(r#"{"name": "Foo"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn field_missing_name_is_error() {
        let result =
            parse_schema(r#"{"type": "record", "name": "User", "fields": [{"type": "int"}]}"#);
        assert!(result.is_err());
    }

    #[test]
    fn field_missing_type_is_error() {
        let result =
            parse_schema(r#"{"type": "record", "name": "User", "fields": [{"name": "id"}]}"#);
        assert!(result.is_err());
    }

    // ==================== SchemaParser API ====================

    #[test]
    fn parser_tracks_named_types() {
        let mut parser = SchemaParser::new();
        parser
            .parse(
                &serde_json::from_str(
                    r#"{"type": "record", "name": "User", "namespace": "com.example", "fields": []}"#,
                )
                .unwrap(),
            )
            .unwrap();

        assert!(parser.get_named_type("com.example.User").is_some());
        assert_eq!(parser.named_types().len(), 1);
    }

    #[test]
    fn parser_new_strict_mode() {
        let mut parser = SchemaParser::new_strict();
        let result = parser.parse(&serde_json::from_str(r#"["int", "int"]"#).unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn parser_with_strict_builder() {
        let mut parser = SchemaParser::new().with_strict(true);
        let result = parser.parse(&serde_json::from_str(r#"["int", "int"]"#).unwrap());
        assert!(result.is_err());
    }
}
