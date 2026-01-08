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
#[derive(Debug)]
pub struct SchemaParser {
    /// Registry of named types by their fully qualified name
    named_types: HashMap<String, AvroSchema>,
    /// Current namespace for resolving unqualified names
    current_namespace: Option<String>,
    /// Whether to enforce strict schema validation
    strict_schema: bool,
}

impl Default for SchemaParser {
    fn default() -> Self {
        Self {
            named_types: HashMap::new(),
            current_namespace: None,
            strict_schema: false,
        }
    }
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
            "duration" => LogicalTypeName::Duration,
            "local-timestamp-millis" => LogicalTypeName::LocalTimestampMillis,
            "local-timestamp-micros" => LogicalTypeName::LocalTimestampMicros,
            _other => {
                // Unknown logical type - return base type per Avro spec
                // (unknown logical types should be ignored)
                return Ok(base_schema);
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
