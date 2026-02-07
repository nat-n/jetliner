//! Source resolution and schema unification.
//!
//! This module provides types and functions for resolving multiple file sources
//! and unifying their schemas for multi-file reading.
//!
//! # Requirements
//! - 2.7: When multiple files are provided, the reader reads them in sequence
//! - 2.8: When multiple files have incompatible schemas, an error is raised

use std::sync::Arc;

use crate::error::{ReaderError, SchemaError, SourceError};
use crate::reader::AvroHeader;
use crate::schema::AvroSchema;
use crate::source::{LocalSource, S3Config, S3Source, StreamSource};

use super::glob::{expand_path_async, is_s3_uri};

/// Resolved sources after glob expansion and schema validation.
///
/// This struct holds the expanded list of file paths and the unified schema
/// that will be used for reading all files.
///
/// # Requirements
/// - 2.7: Store expanded paths for sequential reading
/// - 2.8: Store validated schema after unification
#[derive(Debug, Clone)]
pub struct ResolvedSources {
    /// The expanded list of file paths (after glob expansion).
    pub paths: Vec<String>,
    /// The unified schema from the first file (canonical schema).
    pub schema: AvroSchema,
    /// Per-file column reorder mapping. None = no reorder needed.
    /// When Some, contains column names in canonical order for df.select().
    pub column_mappings: Vec<Option<Vec<String>>>,
}

impl ResolvedSources {
    /// Create a new `ResolvedSources` with the given paths and schema.
    ///
    /// Initializes column_mappings with None for each file (no reordering needed).
    pub fn new(paths: Vec<String>, schema: AvroSchema) -> Self {
        let column_mappings = vec![None; paths.len()];
        Self {
            paths,
            schema,
            column_mappings,
        }
    }

    /// Create a new `ResolvedSources` with explicit column mappings.
    pub fn with_column_mappings(
        paths: Vec<String>,
        schema: AvroSchema,
        column_mappings: Vec<Option<Vec<String>>>,
    ) -> Self {
        Self {
            paths,
            schema,
            column_mappings,
        }
    }

    /// Resolve sources from input paths.
    ///
    /// This method:
    /// 1. Expands glob patterns if enabled (including S3 glob patterns)
    /// 2. Reads the schema from the first file
    /// 3. Validates that all files have identical schemas
    ///
    /// # Arguments
    /// * `sources` - Input paths (may contain glob patterns)
    /// * `glob_enabled` - Whether to expand glob patterns
    /// * `s3_config` - Optional S3 configuration
    ///
    /// # Returns
    /// A `ResolvedSources` containing expanded paths and unified schema.
    ///
    /// # Errors
    /// - `SourceError::NotFound` if no files match the patterns
    /// - `SchemaError` if schemas differ between files
    pub async fn resolve(
        sources: &[String],
        glob_enabled: bool,
        s3_config: Option<&S3Config>,
    ) -> Result<Self, ReaderError> {
        if sources.is_empty() {
            return Err(ReaderError::Configuration(
                "No source files provided".to_string(),
            ));
        }

        // Expand glob patterns (including S3 glob patterns)
        // Note: Glob expansion results are already sorted for determinism (in glob.rs),
        // but we preserve the user's ordering of sources. If they pass [file2, file1],
        // that order is maintained.
        let mut paths = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for source in sources {
            let expanded = expand_path_async(source, glob_enabled, s3_config).await?;
            for path in expanded {
                // Deduplicate while preserving order (first occurrence wins)
                if seen.insert(path.clone()) {
                    paths.push(path);
                }
            }
        }

        if paths.is_empty() {
            return Err(ReaderError::Source(SourceError::NotFound(
                "No files match the provided patterns".to_string(),
            )));
        }

        // Unify schemas across all files
        let unified = unify_schemas(&paths, s3_config).await?;

        Ok(Self::with_column_mappings(
            paths,
            unified.schema,
            unified.column_mappings,
        ))
    }

    /// Get the number of resolved paths.
    pub fn len(&self) -> usize {
        self.paths.len()
    }

    /// Check if there are no resolved paths.
    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// Check if this is a single-file source.
    pub fn is_single_file(&self) -> bool {
        self.paths.len() == 1
    }
}

/// Read the schema from a single file.
///
/// This function opens the file, reads the header, and extracts the schema.
/// Supports both local files and S3 URIs.
///
/// Errors are wrapped with [`ReaderError::InFile`] to preserve the file path
/// that caused the error.
async fn read_schema_from_file(
    path: &str,
    s3_config: Option<&S3Config>,
) -> Result<AvroSchema, ReaderError> {
    read_schema_from_file_inner(path, s3_config)
        .await
        .map_err(|e| e.in_file(path))
}

/// Inner implementation without path wrapping.
async fn read_schema_from_file_inner(
    path: &str,
    s3_config: Option<&S3Config>,
) -> Result<AvroSchema, ReaderError> {
    if is_s3_uri(path) {
        // Open S3 source
        let source = S3Source::from_uri_with_config(path, s3_config.cloned()).await?;

        // Read enough bytes for the header (typically < 4KB, but we read more to be safe)
        let header_bytes = source.read_range(0, 64 * 1024).await?;

        // Parse the header to extract the schema
        let header = AvroHeader::parse(&header_bytes)?;

        return Ok(header.schema);
    }

    // Open local file
    let source = LocalSource::open(path).await?;

    // Read enough bytes for the header (typically < 4KB, but we read more to be safe)
    let header_bytes = source.read_range(0, 64 * 1024).await?;

    // Parse the header to extract the schema
    let header = AvroHeader::parse(&header_bytes)?;

    Ok(header.schema)
}

/// Result of schema unification.
#[derive(Debug)]
pub struct UnifiedSchemas {
    /// The canonical schema (from the first file).
    pub schema: AvroSchema,
    /// Per-file column mappings. None = no reorder needed.
    pub column_mappings: Vec<Option<Vec<String>>>,
}

/// Unify schemas across multiple files.
///
/// This function reads the schema from each file and validates that all
/// files have compatible schemas (same fields, possibly different order).
///
/// # Arguments
/// * `paths` - List of file paths to read schemas from
/// * `s3_config` - Optional S3 configuration
///
/// # Returns
/// A `UnifiedSchemas` containing the canonical schema and column mappings.
///
/// # Errors
/// - `SchemaError::IncompatibleSchemas` if schemas have different fields or types
pub async fn unify_schemas(
    paths: &[String],
    s3_config: Option<&S3Config>,
) -> Result<UnifiedSchemas, ReaderError> {
    if paths.is_empty() {
        return Err(ReaderError::Configuration(
            "No files to unify schemas from".to_string(),
        ));
    }

    // Read schema from the first file (canonical schema)
    let canonical_schema = read_schema_from_file(&paths[0], s3_config).await?;

    // First file always uses canonical order (no mapping needed)
    let mut column_mappings: Vec<Option<Vec<String>>> = vec![None];

    // If only one file, no need to check compatibility
    if paths.len() == 1 {
        return Ok(UnifiedSchemas {
            schema: canonical_schema,
            column_mappings,
        });
    }

    // Validate all schemas are compatible with the canonical schema
    for (i, path) in paths.iter().enumerate().skip(1) {
        let schema = read_schema_from_file(path, s3_config).await?;

        match check_schema_compatibility(&canonical_schema, &schema) {
            SchemaCompatibility::Identical => {
                // Fast path: no reordering needed
                column_mappings.push(None);
            }
            SchemaCompatibility::FieldOrderDiffers { canonical_order } => {
                // Same fields, different order: store mapping
                column_mappings.push(Some(canonical_order));
            }
            SchemaCompatibility::Incompatible { reason } => {
                return Err(ReaderError::Schema(SchemaError::IncompatibleSchemas(
                    format!(
                        "Schema mismatch between '{}' and '{}': {}",
                        paths[0], paths[i], reason
                    ),
                )));
            }
        }
    }

    Ok(UnifiedSchemas {
        schema: canonical_schema,
        column_mappings,
    })
}

/// Result of schema compatibility check.
#[derive(Debug, Clone)]
pub enum SchemaCompatibility {
    /// Schemas are byte-identical (no reordering needed).
    Identical,
    /// Schemas have the same fields in different order.
    /// Contains column names in canonical order for df.select().
    FieldOrderDiffers { canonical_order: Vec<String> },
    /// Schemas are incompatible (different fields or types).
    Incompatible { reason: String },
}

/// Check schema compatibility for multi-file reading.
///
/// Returns:
/// - `Identical` if schemas are byte-identical (fast path)
/// - `FieldOrderDiffers` if schemas have same fields in different order
/// - `Incompatible` if schemas have different fields or types
///
/// This enables reading files with the same fields in different order
/// by detecting the mismatch and providing a column mapping.
fn check_schema_compatibility(canonical: &AvroSchema, other: &AvroSchema) -> SchemaCompatibility {
    // Fast path: identical JSON
    if canonical.to_json() == other.to_json() {
        return SchemaCompatibility::Identical;
    }

    // Slow path: check if field-order-equivalent for record schemas
    let (canonical_record, other_record) = match (canonical, other) {
        (AvroSchema::Record(c), AvroSchema::Record(o)) => (c, o),
        _ => {
            return SchemaCompatibility::Incompatible {
                reason: "Non-record schemas must be identical".to_string(),
            };
        }
    };

    // Check record names match
    if canonical_record.name != other_record.name {
        return SchemaCompatibility::Incompatible {
            reason: format!(
                "Record names differ: '{}' vs '{}'",
                canonical_record.name, other_record.name
            ),
        };
    }

    // Check field counts match
    if canonical_record.fields.len() != other_record.fields.len() {
        return SchemaCompatibility::Incompatible {
            reason: format!(
                "Field count differs: {} vs {}",
                canonical_record.fields.len(),
                other_record.fields.len()
            ),
        };
    }

    // Build a map of field name -> (index, schema) for the other schema
    let other_fields: std::collections::HashMap<&str, (usize, &AvroSchema)> = other_record
        .fields
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name.as_str(), (i, &f.schema)))
        .collect();

    // Check each canonical field exists in other with compatible type
    let mut canonical_order = Vec::with_capacity(canonical_record.fields.len());
    for canonical_field in &canonical_record.fields {
        match other_fields.get(canonical_field.name.as_str()) {
            Some((_idx, other_schema)) => {
                // Field exists, check if types are identical
                if canonical_field.schema.to_json() != other_schema.to_json() {
                    return SchemaCompatibility::Incompatible {
                        reason: format!("Field '{}' has different type", canonical_field.name),
                    };
                }
                canonical_order.push(canonical_field.name.clone());
            }
            None => {
                return SchemaCompatibility::Incompatible {
                    reason: format!("Field '{}' missing in other schema", canonical_field.name),
                };
            }
        }
    }

    SchemaCompatibility::FieldOrderDiffers { canonical_order }
}

/// Get the field names from a record schema.
///
/// Returns `None` if the schema is not a record type.
pub fn get_record_field_names(schema: &AvroSchema) -> Option<Vec<Arc<str>>> {
    match schema {
        AvroSchema::Record(record) => Some(
            record
                .fields
                .iter()
                .map(|f| Arc::from(f.name.as_str()))
                .collect(),
        ),
        _ => None,
    }
}

/// Find missing fields between two record schemas.
///
/// Returns the field names that are in `canonical` but not in `other`.
pub fn find_missing_fields(canonical: &AvroSchema, other: &AvroSchema) -> Vec<Arc<str>> {
    find_missing_fields_with_schemas(canonical, other)
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

/// Find missing fields between two record schemas, including their schemas.
///
/// Returns tuples of (field_name, field_schema) for fields in `canonical` but not in `other`.
pub fn find_missing_fields_with_schemas(
    canonical: &AvroSchema,
    other: &AvroSchema,
) -> Vec<(Arc<str>, AvroSchema)> {
    let canonical_record = match canonical {
        AvroSchema::Record(record) => record,
        _ => return Vec::new(),
    };

    let other_fields = match get_record_field_names(other) {
        Some(fields) => fields,
        None => {
            // If other is not a record, all canonical fields are missing
            return canonical_record
                .fields
                .iter()
                .map(|f| (Arc::from(f.name.as_str()), f.schema.clone()))
                .collect();
        }
    };

    canonical_record
        .fields
        .iter()
        .filter(|f| !other_fields.iter().any(|o| o.as_ref() == f.name.as_str()))
        .map(|f| (Arc::from(f.name.as_str()), f.schema.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldSchema, RecordSchema};

    fn create_test_record_schema(name: &str, fields: &[&str]) -> AvroSchema {
        let field_schemas: Vec<FieldSchema> = fields
            .iter()
            .map(|f| FieldSchema::new(*f, AvroSchema::String))
            .collect();
        AvroSchema::Record(RecordSchema::new(name, field_schemas))
    }

    #[test]
    fn test_resolved_sources_new() {
        let paths = vec!["file1.avro".to_string(), "file2.avro".to_string()];
        let schema = AvroSchema::String;
        let resolved = ResolvedSources::new(paths.clone(), schema.clone());

        assert_eq!(resolved.paths, paths);
        assert_eq!(resolved.len(), 2);
        assert!(!resolved.is_empty());
        assert!(!resolved.is_single_file());
    }

    #[test]
    fn test_resolved_sources_single_file() {
        let paths = vec!["file.avro".to_string()];
        let schema = AvroSchema::String;
        let resolved = ResolvedSources::new(paths, schema);

        assert!(resolved.is_single_file());
        assert_eq!(resolved.len(), 1);
    }

    #[test]
    fn test_check_schema_compatibility_identical() {
        let schema1 = create_test_record_schema("Test", &["id", "name"]);
        let schema2 = create_test_record_schema("Test", &["id", "name"]);

        assert!(matches!(
            check_schema_compatibility(&schema1, &schema2),
            SchemaCompatibility::Identical
        ));
    }

    #[test]
    fn test_check_schema_compatibility_different_field_order() {
        let schema1 = create_test_record_schema("Test", &["id", "name", "value"]);
        let schema2 = create_test_record_schema("Test", &["name", "id", "value"]);

        match check_schema_compatibility(&schema1, &schema2) {
            SchemaCompatibility::FieldOrderDiffers { canonical_order } => {
                // canonical_order should be the field names in schema1's order
                assert_eq!(canonical_order, vec!["id", "name", "value"]);
            }
            other => panic!("Expected FieldOrderDiffers, got {:?}", other),
        }
    }

    #[test]
    fn test_check_schema_compatibility_different_fields() {
        let schema1 = create_test_record_schema("Test", &["id", "name"]);
        let schema2 = create_test_record_schema("Test", &["id", "email"]);

        assert!(matches!(
            check_schema_compatibility(&schema1, &schema2),
            SchemaCompatibility::Incompatible { .. }
        ));
    }

    #[test]
    fn test_check_schema_compatibility_different_field_count() {
        let schema1 = create_test_record_schema("Test", &["id", "name"]);
        let schema2 = create_test_record_schema("Test", &["id"]);

        assert!(matches!(
            check_schema_compatibility(&schema1, &schema2),
            SchemaCompatibility::Incompatible { .. }
        ));
    }

    #[test]
    fn test_check_schema_compatibility_different_record_names() {
        let schema1 = create_test_record_schema("TestA", &["id", "name"]);
        let schema2 = create_test_record_schema("TestB", &["id", "name"]);

        match check_schema_compatibility(&schema1, &schema2) {
            SchemaCompatibility::Incompatible { reason } => {
                assert!(reason.contains("Record names differ"));
            }
            other => panic!("Expected Incompatible, got {:?}", other),
        }
    }

    #[test]
    fn test_check_schema_compatibility_non_record() {
        let schema1 = AvroSchema::String;
        let schema2 = AvroSchema::String;

        // Identical non-record schemas
        assert!(matches!(
            check_schema_compatibility(&schema1, &schema2),
            SchemaCompatibility::Identical
        ));

        // Different non-record schemas
        let schema3 = AvroSchema::Int;
        assert!(matches!(
            check_schema_compatibility(&schema1, &schema3),
            SchemaCompatibility::Incompatible { .. }
        ));
    }

    #[test]
    fn test_get_record_field_names() {
        let schema = create_test_record_schema("Test", &["id", "name", "email"]);
        let fields = get_record_field_names(&schema).unwrap();

        assert_eq!(fields.len(), 3);
        assert_eq!(&*fields[0], "id");
        assert_eq!(&*fields[1], "name");
        assert_eq!(&*fields[2], "email");
    }

    #[test]
    fn test_get_record_field_names_non_record() {
        let schema = AvroSchema::String;
        assert!(get_record_field_names(&schema).is_none());
    }

    #[test]
    fn test_find_missing_fields() {
        let canonical = create_test_record_schema("Test", &["id", "name", "email"]);
        let other = create_test_record_schema("Test", &["id", "name"]);

        let missing = find_missing_fields(&canonical, &other);
        assert_eq!(missing.len(), 1);
        assert_eq!(&*missing[0], "email");
    }

    #[test]
    fn test_find_missing_fields_none_missing() {
        let canonical = create_test_record_schema("Test", &["id", "name"]);
        let other = create_test_record_schema("Test", &["id", "name", "email"]);

        let missing = find_missing_fields(&canonical, &other);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_find_missing_fields_non_record() {
        let canonical = AvroSchema::String;
        let other = AvroSchema::String;

        let missing = find_missing_fields(&canonical, &other);
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_empty_sources() {
        let result = ResolvedSources::resolve(&[], true, None).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ReaderError::Configuration(msg) => {
                assert!(msg.contains("No source files"));
            }
            _ => panic!("Expected Configuration error"),
        }
    }

    #[tokio::test]
    async fn test_unify_schemas_empty_paths() {
        let result = unify_schemas(&[], None).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ReaderError::Configuration(msg) => {
                assert!(msg.contains("No files"));
            }
            _ => panic!("Expected Configuration error"),
        }
    }

    #[tokio::test]
    async fn test_resolve_single_file_with_test_data() {
        // Use actual test data file
        let result = ResolvedSources::resolve(
            &["tests/data/apache-avro/weather.avro".to_string()],
            false,
            None,
        )
        .await;

        match result {
            Ok(resolved) => {
                assert_eq!(resolved.len(), 1);
                assert!(resolved.is_single_file());
                assert!(matches!(resolved.schema, AvroSchema::Record(_)));
            }
            Err(e) => {
                // File might not exist in all test environments
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_multiple_files_with_glob() {
        // Use glob pattern to find multiple files
        let result = ResolvedSources::resolve(
            &["tests/data/apache-avro/weather*.avro".to_string()],
            true,
            None,
        )
        .await;

        match result {
            Ok(resolved) => {
                // Should find multiple weather files
                assert!(resolved.len() >= 1);
                assert!(matches!(resolved.schema, AvroSchema::Record(_)));
            }
            Err(e) => {
                // File might not exist in all test environments
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_unify_schemas_single_file() {
        let result =
            unify_schemas(&["tests/data/apache-avro/weather.avro".to_string()], None).await;

        match result {
            Ok(unified) => {
                assert!(matches!(unified.schema, AvroSchema::Record(_)));
                // Single file should have no column mapping
                assert_eq!(unified.column_mappings.len(), 1);
                assert!(unified.column_mappings[0].is_none());
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_resolved_sources_is_empty() {
        let schema = AvroSchema::String;
        let empty = ResolvedSources::new(vec![], schema.clone());
        let non_empty = ResolvedSources::new(vec!["file.avro".to_string()], schema);

        assert!(empty.is_empty());
        assert!(!non_empty.is_empty());
    }

    // =========================================================================
    // File ordering tests
    // =========================================================================

    #[tokio::test]
    async fn test_resolve_preserves_user_provided_order() {
        // When user provides explicit file list, order should be preserved
        // even if the paths would sort differently
        let result = ResolvedSources::resolve(
            &[
                "tests/data/codecs/zstd.avro".to_string(),
                "tests/data/codecs/deflate.avro".to_string(),
                "tests/data/codecs/snappy.avro".to_string(),
            ],
            false, // glob disabled - treat as literal paths
            None,
        )
        .await;

        match result {
            Ok(resolved) => {
                // Order should match what the user provided, NOT alphabetical
                assert_eq!(resolved.paths.len(), 3);
                assert!(resolved.paths[0].ends_with("zstd.avro"));
                assert!(resolved.paths[1].ends_with("deflate.avro"));
                assert!(resolved.paths[2].ends_with("snappy.avro"));
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_deduplicates_preserving_first_occurrence() {
        // When the same file appears multiple times, keep only the first occurrence
        let result = ResolvedSources::resolve(
            &[
                "tests/data/codecs/zstd.avro".to_string(),
                "tests/data/codecs/deflate.avro".to_string(),
                "tests/data/codecs/zstd.avro".to_string(), // duplicate
            ],
            false,
            None,
        )
        .await;

        match result {
            Ok(resolved) => {
                // Should have only 2 unique files
                assert_eq!(resolved.paths.len(), 2);
                // Order should preserve first occurrences
                assert!(resolved.paths[0].ends_with("zstd.avro"));
                assert!(resolved.paths[1].ends_with("deflate.avro"));
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_glob_results_are_sorted() {
        // Glob expansion results should be sorted for determinism
        let result = ResolvedSources::resolve(
            &["tests/data/codecs/*.avro".to_string()],
            true, // glob enabled
            None,
        )
        .await;

        match result {
            Ok(resolved) => {
                // Verify paths are sorted
                let mut sorted = resolved.paths.clone();
                sorted.sort();
                assert_eq!(
                    resolved.paths, sorted,
                    "Glob expansion results should be sorted"
                );
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_multiple_globs_preserve_pattern_order() {
        // When multiple glob patterns are provided, results from each pattern
        // should appear in the order the patterns were provided
        let result = ResolvedSources::resolve(
            &[
                "tests/data/codecs/zstd*.avro".to_string(), // matches zstd.avro
                "tests/data/codecs/deflate*.avro".to_string(), // matches deflate.avro
            ],
            true,
            None,
        )
        .await;

        match result {
            Ok(resolved) => {
                // zstd files should come before deflate files (pattern order)
                // even though alphabetically deflate < zstd
                let zstd_idx = resolved
                    .paths
                    .iter()
                    .position(|p| p.contains("zstd"))
                    .unwrap_or(usize::MAX);
                let deflate_idx = resolved
                    .paths
                    .iter()
                    .position(|p| p.contains("deflate"))
                    .unwrap_or(usize::MAX);
                assert!(
                    zstd_idx < deflate_idx,
                    "First glob pattern results should come before second pattern results"
                );
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_mixed_explicit_and_glob_preserves_order() {
        // Mix of explicit paths and glob patterns should preserve overall order
        let result = ResolvedSources::resolve(
            &[
                "tests/data/codecs/zstd.avro".to_string(),     // explicit
                "tests/data/codecs/deflate*.avro".to_string(), // glob
            ],
            true,
            None,
        )
        .await;

        match result {
            Ok(resolved) => {
                // zstd (explicit) should come before deflate (from glob)
                let zstd_idx = resolved
                    .paths
                    .iter()
                    .position(|p| p.contains("zstd"))
                    .unwrap_or(usize::MAX);
                let deflate_idx = resolved
                    .paths
                    .iter()
                    .position(|p| p.contains("deflate"))
                    .unwrap_or(usize::MAX);
                assert!(
                    zstd_idx < deflate_idx,
                    "Explicit path should come before glob results"
                );
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }
}
