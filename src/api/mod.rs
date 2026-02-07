//! Public API module for Jetliner.
//!
//! This module provides the public Rust API for scanning and reading Avro files,
//! following Polars conventions for API consistency.
//!
//! # Module Structure
//! - `args`: Scan arguments (`ScanArgsAvro`, `RowIndex`)
//! - `options`: Avro-specific reader options (`AvroOptions`)
//! - `columns`: Column selection and resolution
//! - `glob`: Glob pattern handling
//! - `sources`: Source resolution and schema unification
//! - `row_index`: Row index tracking for multi-file reading
//! - `file_path`: File path injection for multi-file reading
//! - `multi_source`: Multi-source reader orchestration
//! - `scan`: Scan functions for LazyFrame creation
//! - `read`: Read functions for eager DataFrame loading
//! - `schema`: Schema reading functions
//!
//! # Requirements
//! - 13.1: The library exposes a `ScanArgsAvro` struct with scan configuration options
//! - 13.2: The library exposes an `AvroOptions` struct with Avro-specific reader options
//! - 13.3: The library exposes a `scan_avro()` function returning `PolarsResult<LazyFrame>`
//! - 13.4: The library exposes a `read_avro()` function returning `PolarsResult<DataFrame>`
//! - 13.5: The library exposes a `read_avro_schema()` function returning `PolarsResult<Schema>`

pub mod args;
pub mod columns;
pub mod file_path;
pub mod glob;
pub mod multi_source;
pub mod options;
pub mod read;
pub mod row_index;
pub mod scan;
pub mod schema;
pub mod sources;

// Re-export main types
pub use args::{IdxSize, RowIndex, ScanArgsAvro};
pub use columns::{
    extract_record_schema, resolve_all_columns, resolve_columns, ColumnSelection, ResolvedColumns,
};
pub use file_path::FilePathInjector;
pub use glob::{
    expand_local_glob, expand_path, expand_path_async, expand_paths, expand_paths_async,
    expand_s3_glob, is_glob_pattern, is_s3_uri, parse_s3_glob_uri,
};
pub use multi_source::{AvroMultiStreamReader, MultiSourceConfig};
pub use options::AvroOptions;
pub use row_index::RowIndexTracker;
pub use sources::{find_missing_fields, get_record_field_names, unify_schemas, ResolvedSources};

// Re-export public API functions
pub use read::{read_avro, read_avro_sources};
pub use scan::{collect_scan, resolve_scan_config, ScanConfig};
pub use schema::read_avro_schema;
