//! Schema resolution for named type references.
//!
//! This module provides functionality to resolve Named type references
//! in Avro schemas to their actual definitions. This is required before
//! decoding Avro data, as the decoder needs the full schema structure.
//!
//! # Requirements
//! - 1.7: WHEN a schema contains named types, THE Avro_Reader SHALL resolve type references correctly

use std::collections::HashMap;

use crate::error::SchemaError;
use crate::schema::{AvroSchema, FieldSchema, LogicalType, RecordSchema};

/// A context for resolving named type references.
///
/// This struct holds a registry of named types (records, enums, fixed)
/// that can be used to resolve `Named` references in schemas.
#[derive(Debug, Clone, Default)]
pub struct SchemaResolutionContext {
    /// Registry of named types by their fully qualified name
    named_types: HashMap<String, AvroSchema>,
}

impl SchemaResolutionContext {
    /// Create a new empty resolution context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a resolution context from a HashMap of named types.
    pub fn from_named_types(named_types: HashMap<String, AvroSchema>) -> Self {
        Self { named_types }
    }

    /// Register a named type in the context.
    ///
    /// # Arguments
    /// * `name` - The fully qualified name of the type
    /// * `schema` - The schema definition
    pub fn register(&mut self, name: String, schema: AvroSchema) {
        self.named_types.insert(name, schema);
    }

    /// Get a named type from the context.
    pub fn get(&self, name: &str) -> Option<&AvroSchema> {
        self.named_types.get(name)
    }

    /// Check if a named type exists in the context.
    pub fn contains(&self, name: &str) -> bool {
        self.named_types.contains_key(name)
    }

    /// Get all registered named types.
    pub fn named_types(&self) -> &HashMap<String, AvroSchema> {
        &self.named_types
    }

    /// Build a resolution context by extracting all named types from a schema.
    ///
    /// This recursively traverses the schema and registers all named types
    /// (records, enums, fixed) that it encounters.
    pub fn build_from_schema(schema: &AvroSchema) -> Self {
        let mut context = Self::new();
        context.extract_named_types(schema);
        context
    }

    /// Extract and register all named types from a schema.
    fn extract_named_types(&mut self, schema: &AvroSchema) {
        match schema {
            AvroSchema::Record(record) => {
                let fullname = record.fullname();
                // Register the record itself
                self.named_types.insert(fullname, schema.clone());
                // Recursively extract from fields
                for field in &record.fields {
                    self.extract_named_types(&field.schema);
                }
            }
            AvroSchema::Enum(enum_schema) => {
                let fullname = enum_schema.fullname();
                self.named_types.insert(fullname, schema.clone());
            }
            AvroSchema::Fixed(fixed_schema) => {
                let fullname = fixed_schema.fullname();
                self.named_types.insert(fullname, schema.clone());
            }
            AvroSchema::Array(item_schema) => {
                self.extract_named_types(item_schema);
            }
            AvroSchema::Map(value_schema) => {
                self.extract_named_types(value_schema);
            }
            AvroSchema::Union(variants) => {
                for variant in variants {
                    self.extract_named_types(variant);
                }
            }
            AvroSchema::Logical(logical) => {
                self.extract_named_types(&logical.base);
            }
            // Primitives and Named references don't contain named type definitions
            _ => {}
        }
    }

    /// Resolve all Named references in a schema.
    ///
    /// This creates a new schema where all `Named` references are replaced
    /// with their actual definitions from the context.
    ///
    /// # Arguments
    /// * `schema` - The schema to resolve
    ///
    /// # Returns
    /// A new schema with all Named references resolved, or an error if
    /// a reference cannot be resolved.
    ///
    /// # Note
    /// For recursive types (e.g., a linked list), the resolution stops at
    /// the first level to avoid infinite recursion. The Named reference
    /// is kept for self-references.
    pub fn resolve(&self, schema: &AvroSchema) -> Result<AvroSchema, SchemaError> {
        self.resolve_with_path(schema, &mut Vec::new())
    }

    /// Internal resolution with path tracking to handle recursive types.
    fn resolve_with_path(
        &self,
        schema: &AvroSchema,
        path: &mut Vec<String>,
    ) -> Result<AvroSchema, SchemaError> {
        match schema {
            AvroSchema::Named(name) => {
                // Check if this is a recursive reference (already in path)
                if path.contains(name) {
                    // Keep the Named reference for recursive types
                    return Ok(schema.clone());
                }

                // Look up the named type
                match self.named_types.get(name) {
                    Some(resolved) => {
                        // Add to path to track recursion
                        path.push(name.clone());
                        let result = self.resolve_with_path(resolved, path);
                        path.pop();
                        result
                    }
                    None => Err(SchemaError::InvalidSchema(format!(
                        "Unresolved named type reference: '{}'",
                        name
                    ))),
                }
            }
            AvroSchema::Record(record) => {
                // Add record to path before resolving fields
                let fullname = record.fullname();
                path.push(fullname.clone());

                let resolved_fields: Result<Vec<FieldSchema>, SchemaError> = record
                    .fields
                    .iter()
                    .map(|field| {
                        let resolved_schema = self.resolve_with_path(&field.schema, path)?;
                        Ok(FieldSchema {
                            name: field.name.clone(),
                            schema: resolved_schema,
                            default: field.default.clone(),
                            doc: field.doc.clone(),
                            order: field.order,
                            aliases: field.aliases.clone(),
                        })
                    })
                    .collect();

                path.pop();

                Ok(AvroSchema::Record(RecordSchema {
                    name: record.name.clone(),
                    namespace: record.namespace.clone(),
                    fields: resolved_fields?,
                    doc: record.doc.clone(),
                    aliases: record.aliases.clone(),
                }))
            }
            AvroSchema::Array(item_schema) => {
                let resolved_items = self.resolve_with_path(item_schema, path)?;
                Ok(AvroSchema::Array(Box::new(resolved_items)))
            }
            AvroSchema::Map(value_schema) => {
                let resolved_values = self.resolve_with_path(value_schema, path)?;
                Ok(AvroSchema::Map(Box::new(resolved_values)))
            }
            AvroSchema::Union(variants) => {
                let resolved_variants: Result<Vec<AvroSchema>, SchemaError> = variants
                    .iter()
                    .map(|v| self.resolve_with_path(v, path))
                    .collect();
                Ok(AvroSchema::Union(resolved_variants?))
            }
            AvroSchema::Logical(logical) => {
                let resolved_base = self.resolve_with_path(&logical.base, path)?;
                Ok(AvroSchema::Logical(LogicalType::new(
                    resolved_base,
                    logical.logical_type.clone(),
                )))
            }
            // Primitives, Enum, and Fixed don't need resolution
            _ => Ok(schema.clone()),
        }
    }
}

/// Resolve all Named references in a schema using the provided context.
///
/// This is a convenience function that creates a resolution context from
/// the schema and resolves all Named references.
///
/// # Arguments
/// * `schema` - The schema to resolve
///
/// # Returns
/// A new schema with all Named references resolved.
pub fn resolve_schema(schema: &AvroSchema) -> Result<AvroSchema, SchemaError> {
    let context = SchemaResolutionContext::build_from_schema(schema);
    context.resolve(schema)
}

/// Resolve Named references in a schema using an existing context.
///
/// This is useful when you have a pre-built context from parsing.
///
/// # Arguments
/// * `schema` - The schema to resolve
/// * `context` - The resolution context containing named type definitions
///
/// # Returns
/// A new schema with all Named references resolved.
pub fn resolve_schema_with_context(
    schema: &AvroSchema,
    context: &SchemaResolutionContext,
) -> Result<AvroSchema, SchemaError> {
    context.resolve(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{EnumSchema, FixedSchema};

    #[test]
    fn test_build_context_from_simple_record() {
        let record = RecordSchema::new(
            "User",
            vec![
                FieldSchema::new("id", AvroSchema::Long),
                FieldSchema::new("name", AvroSchema::String),
            ],
        )
        .with_namespace("com.example");

        let schema = AvroSchema::Record(record);
        let context = SchemaResolutionContext::build_from_schema(&schema);

        assert!(context.contains("com.example.User"));
    }

    #[test]
    fn test_build_context_from_nested_records() {
        let address = RecordSchema::new(
            "Address",
            vec![
                FieldSchema::new("street", AvroSchema::String),
                FieldSchema::new("city", AvroSchema::String),
            ],
        )
        .with_namespace("com.example");

        let person = RecordSchema::new(
            "Person",
            vec![
                FieldSchema::new("name", AvroSchema::String),
                FieldSchema::new("address", AvroSchema::Record(address)),
            ],
        )
        .with_namespace("com.example");

        let schema = AvroSchema::Record(person);
        let context = SchemaResolutionContext::build_from_schema(&schema);

        assert!(context.contains("com.example.Person"));
        assert!(context.contains("com.example.Address"));
    }

    #[test]
    fn test_build_context_with_enum_and_fixed() {
        let color_enum = EnumSchema::new(
            "Color",
            vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()],
        )
        .with_namespace("com.example");

        let hash_fixed = FixedSchema::new("Hash", 32).with_namespace("com.example");

        let record = RecordSchema::new(
            "Item",
            vec![
                FieldSchema::new("color", AvroSchema::Enum(color_enum)),
                FieldSchema::new("hash", AvroSchema::Fixed(hash_fixed)),
            ],
        )
        .with_namespace("com.example");

        let schema = AvroSchema::Record(record);
        let context = SchemaResolutionContext::build_from_schema(&schema);

        assert!(context.contains("com.example.Item"));
        assert!(context.contains("com.example.Color"));
        assert!(context.contains("com.example.Hash"));
    }

    #[test]
    fn test_resolve_simple_named_reference() {
        let user = RecordSchema::new("User", vec![FieldSchema::new("name", AvroSchema::String)])
            .with_namespace("com.example");

        let mut context = SchemaResolutionContext::new();
        context.register("com.example.User".to_string(), AvroSchema::Record(user));

        let named_ref = AvroSchema::Named("com.example.User".to_string());
        let resolved = context.resolve(&named_ref).unwrap();

        match resolved {
            AvroSchema::Record(r) => {
                assert_eq!(r.name, "User");
                assert_eq!(r.namespace, Some("com.example".to_string()));
            }
            _ => panic!("Expected Record schema"),
        }
    }

    #[test]
    fn test_resolve_named_reference_in_union() {
        let user = RecordSchema::new("User", vec![FieldSchema::new("name", AvroSchema::String)])
            .with_namespace("com.example");

        let mut context = SchemaResolutionContext::new();
        context.register("com.example.User".to_string(), AvroSchema::Record(user));

        let union_schema = AvroSchema::Union(vec![
            AvroSchema::Null,
            AvroSchema::Named("com.example.User".to_string()),
        ]);

        let resolved = context.resolve(&union_schema).unwrap();

        match resolved {
            AvroSchema::Union(variants) => {
                assert_eq!(variants.len(), 2);
                assert_eq!(variants[0], AvroSchema::Null);
                match &variants[1] {
                    AvroSchema::Record(r) => {
                        assert_eq!(r.name, "User");
                    }
                    _ => panic!("Expected Record in union"),
                }
            }
            _ => panic!("Expected Union schema"),
        }
    }

    #[test]
    fn test_resolve_named_reference_in_array() {
        let item = RecordSchema::new("Item", vec![FieldSchema::new("value", AvroSchema::Int)]);

        let mut context = SchemaResolutionContext::new();
        context.register("Item".to_string(), AvroSchema::Record(item));

        let array_schema = AvroSchema::Array(Box::new(AvroSchema::Named("Item".to_string())));

        let resolved = context.resolve(&array_schema).unwrap();

        match resolved {
            AvroSchema::Array(items) => match &*items {
                AvroSchema::Record(r) => {
                    assert_eq!(r.name, "Item");
                }
                _ => panic!("Expected Record in array"),
            },
            _ => panic!("Expected Array schema"),
        }
    }

    #[test]
    fn test_resolve_recursive_type() {
        // Create a linked list type that references itself
        let linked_list = RecordSchema::new(
            "LinkedList",
            vec![
                FieldSchema::new("value", AvroSchema::Int),
                FieldSchema::new(
                    "next",
                    AvroSchema::Union(vec![
                        AvroSchema::Null,
                        AvroSchema::Named("LinkedList".to_string()),
                    ]),
                ),
            ],
        );

        let schema = AvroSchema::Record(linked_list);
        let context = SchemaResolutionContext::build_from_schema(&schema);
        let resolved = context.resolve(&schema).unwrap();

        // The resolved schema should have the recursive reference preserved
        match resolved {
            AvroSchema::Record(r) => {
                assert_eq!(r.name, "LinkedList");
                match &r.fields[1].schema {
                    AvroSchema::Union(variants) => {
                        assert_eq!(variants.len(), 2);
                        // The recursive reference should be kept as Named
                        match &variants[1] {
                            AvroSchema::Named(name) => {
                                assert_eq!(name, "LinkedList");
                            }
                            _ => panic!("Expected Named reference for recursive type"),
                        }
                    }
                    _ => panic!("Expected Union for next field"),
                }
            }
            _ => panic!("Expected Record schema"),
        }
    }

    #[test]
    fn test_resolve_unresolved_reference_error() {
        let context = SchemaResolutionContext::new();
        let named_ref = AvroSchema::Named("NonExistent".to_string());

        let result = context.resolve(&named_ref);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_schema_convenience_function() {
        let inner = RecordSchema::new("Inner", vec![FieldSchema::new("value", AvroSchema::Int)])
            .with_namespace("com.example");

        let outer = RecordSchema::new(
            "Outer",
            vec![
                FieldSchema::new("inner", AvroSchema::Record(inner)),
                FieldSchema::new("ref", AvroSchema::Named("com.example.Inner".to_string())),
            ],
        )
        .with_namespace("com.example");

        let schema = AvroSchema::Record(outer);
        let resolved = resolve_schema(&schema).unwrap();

        match resolved {
            AvroSchema::Record(r) => {
                // The ref field should now be resolved to the actual Inner record
                match &r.fields[1].schema {
                    AvroSchema::Record(inner_ref) => {
                        assert_eq!(inner_ref.name, "Inner");
                    }
                    _ => panic!("Expected Record for ref field"),
                }
            }
            _ => panic!("Expected Record schema"),
        }
    }
}
