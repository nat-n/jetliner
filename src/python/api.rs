//! Python API functions for Jetliner.
//!
//! This module provides the main Python API functions:
//! - `scan_avro`: Scan Avro files returning a LazyFrame
//! - `read_avro`: Read Avro files returning a DataFrame
//! - `read_avro_schema`: Extract Polars schema from an Avro file
//!
//! # Requirements
//! - 1.1: Expose `scan_avro()` function that returns `pl.LazyFrame`
//! - 1.2: Expose `read_avro()` function that returns `pl.DataFrame`
//! - 1.3: Expose `read_avro_schema()` function that returns `pl.Schema`
//! - 4.1-4.4: Row limiting support
//! - 9.1: Avro-specific options as top-level kwargs
//! - 10.1: LazyFrame integration
//! - 11.1-11.4: Read function behavior
//! - 12.1-12.2: Error handling mode

use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_polars::PyDataFrame;
use std::collections::HashMap;
use std::sync::Arc;

use crate::api::args::{IdxSize, RowIndex, ScanArgsAvro};
use crate::api::options::AvroOptions;
use crate::api::read::read_avro_sources;
use crate::api::schema::read_avro_schema as rust_read_avro_schema;
use crate::api::sources::ResolvedSources;
use crate::convert::avro_to_arrow_schema;
use crate::source::S3Config;

use super::errors::{map_polars_error_acquire_gil, map_reader_error_acquire_gil};
use super::types::{PyColumnSelection, PyFileSource};

/// Scan Avro file(s), returning a LazyFrame with query optimization support.
///
/// This function uses Polars' IO plugin system to enable query optimizations:
/// - Projection pushdown: Only read columns that are actually used in the query
/// - Predicate pushdown: Apply filters during reading, not after
/// - Early stopping: Stop reading after the requested number of rows
#[pyfunction]
#[pyo3(signature = (
    source,
    *,
    n_rows = None,
    row_index_name = None,
    row_index_offset = 0,
    glob = true,
    include_file_paths = None,
    ignore_errors = false,
    storage_options = None,
    buffer_blocks = 4,
    buffer_bytes = 67_108_864,
    read_chunk_size = None,
    batch_size = 100_000,
    max_block_size = 536_870_912
))]
#[allow(clippy::too_many_arguments)]
pub fn scan_avro(
    py: Python<'_>,
    source: PyFileSource,
    n_rows: Option<usize>,
    row_index_name: Option<String>,
    row_index_offset: i64,
    glob: bool,
    include_file_paths: Option<String>,
    ignore_errors: bool,
    storage_options: Option<HashMap<String, String>>,
    buffer_blocks: usize,
    buffer_bytes: usize,
    read_chunk_size: Option<usize>,
    batch_size: usize,
    max_block_size: Option<usize>,
) -> PyResult<Py<PyAny>> {
    // Validate row_index_offset is non-negative
    if row_index_offset < 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "row_index_offset cannot be negative",
        ));
    }
    let row_index_offset = row_index_offset as IdxSize;

    // Get the source paths
    let raw_paths = source.into_paths();

    // Resolve sources (expand globs, validate schemas)
    let (resolved_paths, base_schema) =
        _resolve_avro_sources(py, raw_paths, glob, storage_options.clone())?;

    // Import polars
    let polars_mod = py.import("polars")?;

    // Build final schema with row_index and include_file_paths columns using Python
    // This is simpler than converting Python dtypes to Rust and back
    let locals = PyDict::new(py);
    locals.set_item("pl", polars_mod)?;
    locals.set_item("base_schema", &base_schema)?;
    locals.set_item("row_index_name", &row_index_name)?;
    locals.set_item("include_file_paths", &include_file_paths)?;

    let schema_code = r#"
import polars as pl
from collections import OrderedDict

# Build final schema
schema_dict = OrderedDict()

# Add row_index as first column if specified
if row_index_name is not None:
    schema_dict[row_index_name] = pl.UInt32

# Add all original columns from base schema
for name, dtype in base_schema.items():
    schema_dict[name] = dtype

# Add include_file_paths as last column if specified
if include_file_paths is not None:
    schema_dict[include_file_paths] = pl.String

final_schema = pl.Schema(schema_dict)
"#;

    let builtins = py.import("builtins")?;
    let exec_fn = builtins.getattr("exec")?;
    exec_fn.call1((schema_code, py.None(), &locals))?;

    let final_schema = locals.get_item("final_schema")?.ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to create final schema")
    })?;

    // Import register_io_source
    let polars_io = py.import("polars.io.plugins")?;
    let register_io_source = polars_io.getattr("register_io_source")?;

    // Get MultiAvroReader from our module
    let jetliner_mod = py.import("jetliner")?;
    let multi_avro_reader_class = jetliner_mod.getattr("MultiAvroReader")?;

    // Add generator parameters to locals
    locals.set_item("resolved_paths", &resolved_paths)?;
    locals.set_item("n_rows", n_rows)?;
    locals.set_item("row_index_offset", row_index_offset)?;
    locals.set_item("ignore_errors", ignore_errors)?;

    // Convert storage_options to Python dict
    match &storage_options {
        Some(opts) => {
            let d = PyDict::new(py);
            for (k, v) in opts {
                d.set_item(k, v)?;
            }
            locals.set_item("storage_options", d)?;
        }
        None => {
            locals.set_item("storage_options", py.None())?;
        }
    };

    locals.set_item("buffer_blocks", buffer_blocks)?;
    locals.set_item("buffer_bytes", buffer_bytes)?;
    locals.set_item("read_chunk_size", read_chunk_size)?;
    locals.set_item("batch_size", batch_size)?;
    locals.set_item("max_block_size", max_block_size)?;
    locals.set_item("MultiAvroReader", multi_avro_reader_class)?;

    // Simplified generator that uses MultiAvroReader
    let generator_code = r#"
def _create_source_generator(resolved_paths, n_rows, row_index_name, row_index_offset,
                             include_file_paths, ignore_errors, storage_options,
                             buffer_blocks, buffer_bytes, read_chunk_size, batch_size,
                             max_block_size, MultiAvroReader):
    def source_generator(with_columns, predicate, n_rows_hint, batch_size_hint):
        # Use hints from Polars optimizer if available
        effective_batch_size = batch_size_hint if batch_size_hint is not None else batch_size
        effective_n_rows = n_rows_hint if n_rows_hint is not None else n_rows

        # Create MultiAvroReader with all options
        reader = MultiAvroReader(
            resolved_paths,
            batch_size=effective_batch_size,
            buffer_blocks=buffer_blocks,
            buffer_bytes=buffer_bytes,
            ignore_errors=ignore_errors,
            projected_columns=with_columns,
            n_rows=effective_n_rows,
            row_index_name=row_index_name,
            row_index_offset=row_index_offset,
            include_file_paths=include_file_paths,
            storage_options=storage_options,
            read_chunk_size=read_chunk_size,
            max_block_size=max_block_size,
        )

        # Yield batches, applying predicate filter if provided
        for df in reader:
            if predicate is not None:
                df = df.filter(predicate)
            yield df

    return source_generator

_generator_func = _create_source_generator(
    resolved_paths, n_rows, row_index_name, row_index_offset, include_file_paths,
    ignore_errors, storage_options, buffer_blocks, buffer_bytes, read_chunk_size,
    batch_size, max_block_size, MultiAvroReader
)
"#;

    // Execute the Python code to create the generator
    exec_fn.call1((generator_code, py.None(), &locals))?;

    // Get the generator function
    let generator_func = locals.get_item("_generator_func")?.ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to create generator function")
    })?;

    // Register the IO source
    let kwargs = pyo3::types::PyDict::new(py);
    kwargs.set_item("io_source", generator_func)?;
    kwargs.set_item("schema", final_schema)?;
    let lazy_frame = register_io_source.call((), Some(&kwargs))?;

    Ok(lazy_frame.unbind())
}

/// Read Avro file(s), returning a DataFrame.
#[pyfunction]
#[pyo3(signature = (
    source,
    *,
    columns = None,
    n_rows = None,
    row_index_name = None,
    row_index_offset = 0,
    glob = true,
    include_file_paths = None,
    ignore_errors = false,
    storage_options = None,
    buffer_blocks = 4,
    buffer_bytes = 67_108_864,
    read_chunk_size = None,
    batch_size = 100_000,
    max_block_size = 536_870_912
))]
#[allow(clippy::too_many_arguments)]
pub fn read_avro(
    _py: Python<'_>,
    source: PyFileSource,
    columns: Option<PyColumnSelection>,
    n_rows: Option<usize>,
    row_index_name: Option<String>,
    row_index_offset: i64,
    glob: bool,
    include_file_paths: Option<String>,
    ignore_errors: bool,
    storage_options: Option<HashMap<String, String>>,
    buffer_blocks: usize,
    buffer_bytes: usize,
    read_chunk_size: Option<usize>,
    batch_size: usize,
    max_block_size: Option<usize>,
) -> PyResult<PyDataFrame> {
    // Validate row_index_offset is non-negative
    if row_index_offset < 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "row_index_offset cannot be negative",
        ));
    }
    let row_index_offset = row_index_offset as IdxSize;

    // Convert storage_options to S3Config
    let s3_config = storage_options.map(|opts| S3Config::from_dict(&opts));

    // Build RowIndex if name is provided
    let row_index = row_index_name.map(|name| RowIndex {
        name: Arc::from(name.as_str()),
        offset: row_index_offset,
    });

    // Build ScanArgsAvro
    let args = ScanArgsAvro {
        n_rows,
        row_index,
        s3_config,
        glob,
        include_file_paths: include_file_paths.map(|s| Arc::from(s.as_str())),
        ignore_errors,
    };

    // Build AvroOptions
    let opts = AvroOptions {
        buffer_blocks,
        buffer_bytes,
        read_chunk_size,
        batch_size,
        max_decompressed_block_size: max_block_size,
    };

    // Convert column selection
    let column_selection = columns.map(|c| c.into_selection());

    // Get the source paths
    let paths = source.into_paths();
    let first_path = paths.first().cloned().unwrap_or_default();

    // Call the Rust read_avro_sources function
    // Note: read_avro_sources returns ReaderError which preserves InFile path context
    let df = read_avro_sources(&paths, column_selection, args, opts)
        .map_err(|e| map_reader_error_acquire_gil(&first_path, e))?;

    Ok(PyDataFrame(df))
}

/// Read the schema from an Avro file.
#[pyfunction]
#[pyo3(signature = (source, *, storage_options = None))]
pub fn read_avro_schema(
    py: Python<'_>,
    source: PyFileSource,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Py<PyAny>> {
    // Convert storage_options to S3Config
    let s3_config = storage_options.map(|opts| S3Config::from_dict(&opts));

    // Get the first path (schema reading is single-file)
    let path = source
        .first_path()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("source cannot be empty"))?;

    // Call the Rust read_avro_schema function
    let schema = rust_read_avro_schema(path, s3_config.as_ref())
        .map_err(|e| map_polars_error_acquire_gil(path, e))?;

    // Convert to Python schema
    schema_to_py(py, &schema)
}

/// Convert a Polars Schema to a Python polars.Schema object.
fn schema_to_py(py: Python<'_>, schema: &polars::prelude::Schema) -> PyResult<Py<PyAny>> {
    let polars_mod = py.import("polars")?;

    // Build a dict of field_name -> DataType
    let py_dict = PyDict::new(py);

    for (name, dtype) in schema.iter() {
        let py_dtype = dtype_to_py(py, &polars_mod, dtype)?;
        py_dict.set_item(name.as_str(), py_dtype)?;
    }

    // Create polars.Schema from the dict
    let schema_class = polars_mod.getattr("Schema")?;
    let py_schema = schema_class.call1((py_dict,))?;

    Ok(py_schema.unbind())
}

/// Convert a Polars DataType to a Python polars DataType object.
#[allow(clippy::only_used_in_recursion)]
fn dtype_to_py<'py>(
    py: Python<'py>,
    polars_mod: &Bound<'py, pyo3::types::PyModule>,
    dtype: &polars::prelude::DataType,
) -> PyResult<Bound<'py, PyAny>> {
    use polars::prelude::DataType;

    match dtype {
        DataType::Null => polars_mod.getattr("Null"),
        DataType::Boolean => polars_mod.getattr("Boolean"),
        DataType::Int8 => polars_mod.getattr("Int8"),
        DataType::Int16 => polars_mod.getattr("Int16"),
        DataType::Int32 => polars_mod.getattr("Int32"),
        DataType::Int64 => polars_mod.getattr("Int64"),
        DataType::UInt8 => polars_mod.getattr("UInt8"),
        DataType::UInt16 => polars_mod.getattr("UInt16"),
        DataType::UInt32 => polars_mod.getattr("UInt32"),
        DataType::UInt64 => polars_mod.getattr("UInt64"),
        DataType::Float32 => polars_mod.getattr("Float32"),
        DataType::Float64 => polars_mod.getattr("Float64"),
        DataType::String => polars_mod.getattr("String"),
        DataType::Binary => polars_mod.getattr("Binary"),
        DataType::Date => polars_mod.getattr("Date"),
        DataType::Time => polars_mod.getattr("Time"),

        DataType::Datetime(time_unit, tz) => {
            let tu_str = match time_unit {
                polars::prelude::TimeUnit::Nanoseconds => "ns",
                polars::prelude::TimeUnit::Microseconds => "us",
                polars::prelude::TimeUnit::Milliseconds => "ms",
            };
            let tz_str = tz.as_ref().map(|t| t.to_string());
            polars_mod.getattr("Datetime")?.call1((tu_str, tz_str))
        }

        DataType::Duration(time_unit) => {
            let tu_str = match time_unit {
                polars::prelude::TimeUnit::Nanoseconds => "ns",
                polars::prelude::TimeUnit::Microseconds => "us",
                polars::prelude::TimeUnit::Milliseconds => "ms",
            };
            polars_mod.getattr("Duration")?.call1((tu_str,))
        }

        DataType::Decimal(precision, scale) => {
            let precision = *precision as i64;
            let scale = *scale as i64;
            polars_mod.getattr("Decimal")?.call1((precision, scale))
        }

        DataType::List(inner) => {
            let inner_py = dtype_to_py(py, polars_mod, inner)?;
            polars_mod.getattr("List")?.call1((inner_py,))
        }

        DataType::Array(inner, size) => {
            let inner_py = dtype_to_py(py, polars_mod, inner)?;
            polars_mod.getattr("Array")?.call1((inner_py, *size))
        }

        DataType::Struct(fields) => {
            let py_fields: Vec<Bound<'py, PyAny>> = fields
                .iter()
                .map(|f| {
                    let field_dtype = dtype_to_py(py, polars_mod, f.dtype())?;
                    let field_class = polars_mod.getattr("Field")?;
                    field_class.call1((f.name().as_str(), field_dtype))
                })
                .collect::<PyResult<Vec<_>>>()?;
            polars_mod.getattr("Struct")?.call1((py_fields,))
        }

        DataType::Enum(frozen_cats, _) => {
            let categories: Vec<&str> = frozen_cats.categories().values_iter().collect();
            let py_categories = pyo3::types::PyList::new(py, &categories)?;
            polars_mod.getattr("Enum")?.call1((py_categories,))
        }

        DataType::Categorical(_, _) => polars_mod.getattr("Categorical"),
        DataType::Unknown(_) => polars_mod.getattr("String"),
        DataType::BinaryOffset => polars_mod.getattr("Binary"),
        _ => polars_mod.getattr("String"),
    }
}

/// Resolve Avro sources, expanding globs and validating schemas.
///
/// This is an internal function used by `scan_avro` and `read_avro` to:
/// 1. Expand glob patterns in source paths
/// 2. Deduplicate and sort paths
/// 3. Read and unify schemas from all files
///
/// # Arguments
/// * `sources` - List of source paths (may contain glob patterns)
/// * `glob` - Whether to expand glob patterns
/// * `storage_options` - Optional S3 configuration
///
/// # Returns
/// A tuple of (resolved_paths, polars_schema) where:
/// * `resolved_paths` - Expanded and sorted list of file paths
/// * `polars_schema` - Unified Polars schema validated across all files
///
/// # Note
/// This function is internal (underscore prefix) and not part of the public API.
/// It is used by `scan_avro` and `read_avro` for path resolution.
#[pyfunction]
#[pyo3(signature = (sources, glob = true, storage_options = None))]
pub fn _resolve_avro_sources(
    py: Python<'_>,
    sources: Vec<String>,
    glob: bool,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<(Vec<String>, Py<PyAny>)> {
    // Get first path for error messages
    let first_path = sources.first().cloned().unwrap_or_default();

    // Convert storage_options to S3Config
    let s3_config = storage_options.map(|opts| S3Config::from_dict(&opts));

    // Create runtime for async operations
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create runtime: {}",
                e
            ))
        })?;

    // Resolve sources (expand globs, validate schemas)
    let resolved = runtime.block_on(async {
        ResolvedSources::resolve(&sources, glob, s3_config.as_ref())
            .await
            .map_err(|e| map_reader_error_acquire_gil(&first_path, e))
    })?;

    // Convert Avro schema to Polars schema
    let polars_schema = avro_to_arrow_schema(&resolved.schema)
        .map_err(|e| map_reader_error_acquire_gil(&first_path, e.into()))?;

    // Convert Polars schema to Python
    let py_schema = schema_to_py(py, &polars_schema)?;

    Ok((resolved.paths, py_schema))
}
