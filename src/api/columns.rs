//! Column selection and resolution.
//!
//! This module provides types and functions for selecting columns by name or index,
//! and resolving them against an Avro schema.
//!
//! # Requirements
//! - 3.5: When column names are provided, the reader only decodes and returns those columns
//! - 3.6: When column indices are provided, the reader maps them to column names using the schema
//! - 3.7: When an invalid column name is provided, the reader raises a descriptive error
//! - 3.8: When an invalid column index is provided, the reader raises an `IndexError`
//! - 3.10: Column resolution is implemented in Rust

use std::sync::Arc;

use crate::error::SchemaError;
use crate::schema::{AvroSchema, RecordSchema};

/// Column selection by name or index.
///
/// This enum represents how columns can be selected for projection:
/// - By name: A list of column names to include
/// - By index: A list of 0-based column indices to include
///
/// # Example
/// ```
/// use jetliner::api::ColumnSelection;
///
/// // Select by name
/// let by_name = ColumnSelection::Names(vec!["id".into(), "name".into()]);
///
/// // Select by index
/// let by_index = ColumnSelection::Indices(vec![0, 2, 5]);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnSelection {
    /// Select columns by name.
    Names(Vec<Arc<str>>),
    /// Select columns by 0-based index.
    Indices(Vec<usize>),
}

impl ColumnSelection {
    /// Create a column selection from names.
    pub fn from_names<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        Self::Names(names.into_iter().map(Into::into).collect())
    }

    /// Create a column selection from indices.
    pub fn from_indices<I>(indices: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        Self::Indices(indices.into_iter().collect())
    }

    /// Check if the selection is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Names(names) => names.is_empty(),
            Self::Indices(indices) => indices.is_empty(),
        }
    }

    /// Get the number of columns selected.
    pub fn len(&self) -> usize {
        match self {
            Self::Names(names) => names.len(),
            Self::Indices(indices) => indices.len(),
        }
    }
}

/// Result of resolving column selection against a schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedColumns {
    /// The resolved column names in order.
    pub names: Vec<Arc<str>>,
    /// The resolved column indices in order.
    pub indices: Vec<usize>,
}

impl ResolvedColumns {
    /// Create a new `ResolvedColumns`.
    pub fn new(names: Vec<Arc<str>>, indices: Vec<usize>) -> Self {
        Self { names, indices }
    }

    /// Check if the resolution is empty.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Get the number of resolved columns.
    pub fn len(&self) -> usize {
        self.names.len()
    }
}

/// Resolve column selection against a record schema.
///
/// This function validates the column selection and returns the resolved
/// column names and indices.
///
/// # Arguments
/// * `selection` - The column selection (by name or index)
/// * `schema` - The Avro record schema to resolve against
///
/// # Returns
/// A `ResolvedColumns` containing the validated column names and indices.
///
/// # Errors
/// - Returns `SchemaError::InvalidSchema` if a column name doesn't exist
/// - Returns `SchemaError::InvalidSchema` if a column index is out of range
///
/// # Requirements
/// - 3.7: When an invalid column name is provided, raise a descriptive error
/// - 3.8: When an invalid column index is provided, raise an error
pub fn resolve_columns(
    selection: &ColumnSelection,
    schema: &RecordSchema,
) -> Result<ResolvedColumns, SchemaError> {
    match selection {
        ColumnSelection::Names(names) => resolve_by_names(names, schema),
        ColumnSelection::Indices(indices) => resolve_by_indices(indices, schema),
    }
}

/// Resolve column selection by names.
fn resolve_by_names(
    names: &[Arc<str>],
    schema: &RecordSchema,
) -> Result<ResolvedColumns, SchemaError> {
    let mut resolved_names = Vec::with_capacity(names.len());
    let mut resolved_indices = Vec::with_capacity(names.len());

    // Build a map of field names to indices for efficient lookup
    let field_map: std::collections::HashMap<&str, usize> = schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name.as_str(), i))
        .collect();

    for name in names {
        match field_map.get(name.as_ref()) {
            Some(&index) => {
                resolved_names.push(name.clone());
                resolved_indices.push(index);
            }
            None => {
                // Build a helpful error message with available columns
                let available: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
                return Err(SchemaError::InvalidSchema(format!(
                    "Column '{}' not found in schema. Available columns: [{}]",
                    name,
                    available.join(", ")
                )));
            }
        }
    }

    Ok(ResolvedColumns::new(resolved_names, resolved_indices))
}

/// Resolve column selection by indices.
fn resolve_by_indices(
    indices: &[usize],
    schema: &RecordSchema,
) -> Result<ResolvedColumns, SchemaError> {
    let num_fields = schema.fields.len();
    let mut resolved_names = Vec::with_capacity(indices.len());
    let mut resolved_indices = Vec::with_capacity(indices.len());

    for &index in indices {
        if index >= num_fields {
            return Err(SchemaError::InvalidSchema(format!(
                "Column index {} is out of range. Schema has {} columns (valid indices: 0-{})",
                index,
                num_fields,
                num_fields.saturating_sub(1)
            )));
        }

        let field = &schema.fields[index];
        resolved_names.push(Arc::from(field.name.as_str()));
        resolved_indices.push(index);
    }

    Ok(ResolvedColumns::new(resolved_names, resolved_indices))
}

/// Resolve all columns from a schema (no projection).
///
/// This is a convenience function that returns all columns from the schema.
pub fn resolve_all_columns(schema: &RecordSchema) -> ResolvedColumns {
    let names: Vec<Arc<str>> = schema
        .fields
        .iter()
        .map(|f| Arc::from(f.name.as_str()))
        .collect();
    let indices: Vec<usize> = (0..schema.fields.len()).collect();

    ResolvedColumns::new(names, indices)
}

/// Extract the record schema from an Avro schema.
///
/// Returns `None` if the schema is not a record type.
pub fn extract_record_schema(schema: &AvroSchema) -> Option<&RecordSchema> {
    match schema {
        AvroSchema::Record(record) => Some(record),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldOrder, FieldSchema};

    fn create_test_schema() -> RecordSchema {
        RecordSchema {
            name: "TestRecord".to_string(),
            namespace: None,
            doc: None,
            aliases: Vec::new(),
            fields: vec![
                FieldSchema {
                    name: "id".to_string(),
                    doc: None,
                    schema: AvroSchema::Int,
                    default: None,
                    order: FieldOrder::Ascending,
                    aliases: Vec::new(),
                },
                FieldSchema {
                    name: "name".to_string(),
                    doc: None,
                    schema: AvroSchema::String,
                    default: None,
                    order: FieldOrder::Ascending,
                    aliases: Vec::new(),
                },
                FieldSchema {
                    name: "age".to_string(),
                    doc: None,
                    schema: AvroSchema::Int,
                    default: None,
                    order: FieldOrder::Ascending,
                    aliases: Vec::new(),
                },
                FieldSchema {
                    name: "email".to_string(),
                    doc: None,
                    schema: AvroSchema::String,
                    default: None,
                    order: FieldOrder::Ascending,
                    aliases: Vec::new(),
                },
            ],
        }
    }

    #[test]
    fn test_column_selection_from_names() {
        let selection = ColumnSelection::from_names(["id", "name"]);
        match selection {
            ColumnSelection::Names(names) => {
                assert_eq!(names.len(), 2);
                assert_eq!(&*names[0], "id");
                assert_eq!(&*names[1], "name");
            }
            _ => panic!("Expected Names variant"),
        }
    }

    #[test]
    fn test_column_selection_from_indices() {
        let selection = ColumnSelection::from_indices([0, 2, 3]);
        match selection {
            ColumnSelection::Indices(indices) => {
                assert_eq!(indices, vec![0, 2, 3]);
            }
            _ => panic!("Expected Indices variant"),
        }
    }

    #[test]
    fn test_column_selection_is_empty() {
        let empty_names = ColumnSelection::Names(vec![]);
        let empty_indices = ColumnSelection::Indices(vec![]);
        let non_empty = ColumnSelection::from_names(["id"]);

        assert!(empty_names.is_empty());
        assert!(empty_indices.is_empty());
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn test_column_selection_len() {
        let names = ColumnSelection::from_names(["id", "name", "age"]);
        let indices = ColumnSelection::from_indices([0, 1]);

        assert_eq!(names.len(), 3);
        assert_eq!(indices.len(), 2);
    }

    #[test]
    fn test_resolve_by_names_success() {
        let schema = create_test_schema();
        let selection = ColumnSelection::from_names(["name", "id"]);

        let resolved = resolve_columns(&selection, &schema).unwrap();

        assert_eq!(resolved.names.len(), 2);
        assert_eq!(&*resolved.names[0], "name");
        assert_eq!(&*resolved.names[1], "id");
        assert_eq!(resolved.indices, vec![1, 0]);
    }

    #[test]
    fn test_resolve_by_names_invalid() {
        let schema = create_test_schema();
        let selection = ColumnSelection::from_names(["id", "nonexistent"]);

        let result = resolve_columns(&selection, &schema);
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            SchemaError::InvalidSchema(msg) => {
                assert!(msg.contains("nonexistent"));
                assert!(msg.contains("not found"));
                assert!(msg.contains("Available columns"));
            }
            _ => panic!("Expected InvalidSchema error"),
        }
    }

    #[test]
    fn test_resolve_by_indices_success() {
        let schema = create_test_schema();
        let selection = ColumnSelection::from_indices([2, 0, 3]);

        let resolved = resolve_columns(&selection, &schema).unwrap();

        assert_eq!(resolved.names.len(), 3);
        assert_eq!(&*resolved.names[0], "age");
        assert_eq!(&*resolved.names[1], "id");
        assert_eq!(&*resolved.names[2], "email");
        assert_eq!(resolved.indices, vec![2, 0, 3]);
    }

    #[test]
    fn test_resolve_by_indices_out_of_range() {
        let schema = create_test_schema();
        let selection = ColumnSelection::from_indices([0, 10]); // 10 is out of range

        let result = resolve_columns(&selection, &schema);
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            SchemaError::InvalidSchema(msg) => {
                assert!(msg.contains("10"));
                assert!(msg.contains("out of range"));
                assert!(msg.contains("4 columns"));
            }
            _ => panic!("Expected InvalidSchema error"),
        }
    }

    #[test]
    fn test_resolve_all_columns() {
        let schema = create_test_schema();
        let resolved = resolve_all_columns(&schema);

        assert_eq!(resolved.names.len(), 4);
        assert_eq!(&*resolved.names[0], "id");
        assert_eq!(&*resolved.names[1], "name");
        assert_eq!(&*resolved.names[2], "age");
        assert_eq!(&*resolved.names[3], "email");
        assert_eq!(resolved.indices, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_resolved_columns_is_empty() {
        let empty = ResolvedColumns::new(vec![], vec![]);
        let non_empty = ResolvedColumns::new(vec!["id".into()], vec![0]);

        assert!(empty.is_empty());
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn test_extract_record_schema() {
        let record = AvroSchema::Record(create_test_schema());
        let non_record = AvroSchema::Int;

        assert!(extract_record_schema(&record).is_some());
        assert!(extract_record_schema(&non_record).is_none());
    }
}
