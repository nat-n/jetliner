//! Python type wrappers for Jetliner.
//!
//! This module provides PyO3 type wrappers for converting Python types
//! to their Rust equivalents.
//!
//! # Types
//! - `PyFileSource`: Accepts `str`, `Path`, `Sequence[str]`, `Sequence[Path]`
//! - `PyColumnSelection`: Accepts `Sequence[str]` or `Sequence[int]`
//!
//! # Requirements
//! - 2.1: The `source` parameter accepts `str` (file path or S3 URI)
//! - 2.2: The `source` parameter accepts `pathlib.Path` objects
//! - 2.3: The `source` parameter accepts a `Sequence` of `str` or `Path`
//! - 3.2: The `columns` parameter accepts `Sequence[str]` for column names
//! - 3.3: The `columns` parameter accepts `Sequence[int]` for column indices

use pyo3::prelude::*;
use pyo3::types::PySequence;
use std::sync::Arc;

use crate::api::args::IdxSize;
use crate::api::columns::ColumnSelection;

/// Python file source type.
///
/// Accepts various Python types for specifying file sources:
/// - `str`: A single file path or S3 URI
/// - `pathlib.Path`: A single Path object
/// - `Sequence[str]`: Multiple file paths
/// - `Sequence[Path]`: Multiple Path objects
///
/// # Requirements
/// - 2.1: Accept `str` (file path or S3 URI)
/// - 2.2: Accept `pathlib.Path` objects
/// - 2.3: Accept `Sequence` of `str` or `Path` for multiple files
pub struct PyFileSource {
    /// The resolved file paths as strings.
    paths: Vec<String>,
}

impl PyFileSource {
    /// Get the resolved file paths.
    pub fn paths(&self) -> &[String] {
        &self.paths
    }

    /// Convert to a vector of strings (consuming self).
    pub fn into_paths(self) -> Vec<String> {
        self.paths
    }

    /// Get the first path (for single-file operations).
    pub fn first_path(&self) -> Option<&str> {
        self.paths.first().map(|s| s.as_str())
    }

    /// Check if this represents a single file.
    pub fn is_single(&self) -> bool {
        self.paths.len() == 1
    }
}

impl<'py> FromPyObject<'py> for PyFileSource {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        // Try to extract as a single string first
        if let Ok(s) = ob.extract::<String>() {
            return Ok(PyFileSource { paths: vec![s] });
        }

        // Try to extract as a pathlib.Path (has __fspath__ method)
        if let Ok(path_str) = extract_path_str(ob) {
            return Ok(PyFileSource {
                paths: vec![path_str],
            });
        }

        // Try to extract as a sequence (list, tuple, etc.)
        if let Ok(seq) = ob.downcast::<PySequence>() {
            let mut paths = Vec::new();
            for i in 0..seq.len()? {
                let item = seq.get_item(i)?;

                // Try string first
                if let Ok(s) = item.extract::<String>() {
                    paths.push(s);
                    continue;
                }

                // Try pathlib.Path
                if let Ok(path_str) = extract_path_str(&item) {
                    paths.push(path_str);
                    continue;
                }

                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Expected str or Path, got {}",
                    item.get_type()
                        .name()
                        .map(|n| n.to_string())
                        .unwrap_or_else(|_| "unknown".to_string())
                )));
            }

            if paths.is_empty() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "source cannot be an empty sequence",
                ));
            }

            return Ok(PyFileSource { paths });
        }

        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
            "Expected str, Path, or sequence of str/Path, got {}",
            ob.get_type()
                .name()
                .map(|n| n.to_string())
                .unwrap_or_else(|_| "unknown".to_string())
        )))
    }
}

/// Extract a string from a pathlib.Path object.
///
/// This calls `str(path)` on the Python object to get the path string.
fn extract_path_str(ob: &Bound<'_, PyAny>) -> PyResult<String> {
    // Check if it has __fspath__ (pathlib.Path protocol)
    if ob.hasattr("__fspath__")? {
        // Call __fspath__ to get the path string
        let fspath = ob.call_method0("__fspath__")?;
        return fspath.extract::<String>();
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Object is not a path-like object",
    ))
}

/// Python column selection type.
///
/// Accepts various Python types for specifying column selection:
/// - `Sequence[str]`: Column names
/// - `Sequence[int]`: Column indices (0-based)
///
/// # Requirements
/// - 3.2: Accept `Sequence[str]` for column names
/// - 3.3: Accept `Sequence[int]` for column indices
pub struct PyColumnSelection {
    /// The resolved column selection.
    selection: ColumnSelection,
}

impl PyColumnSelection {
    /// Get the resolved column selection.
    pub fn selection(&self) -> &ColumnSelection {
        &self.selection
    }

    /// Convert to a ColumnSelection (consuming self).
    pub fn into_selection(self) -> ColumnSelection {
        self.selection
    }
}

impl<'py> FromPyObject<'py> for PyColumnSelection {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        // Must be a sequence (list, tuple, etc.)
        let seq = ob.downcast::<PySequence>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Expected sequence of str or int, got {}",
                ob.get_type()
                    .name()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|_| "unknown".to_string())
            ))
        })?;

        let len = seq.len()?;
        if len == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "columns cannot be an empty sequence",
            ));
        }

        // Check the first element to determine the type
        let first = seq.get_item(0)?;

        if first.extract::<String>().is_ok() {
            // Extract as strings
            let mut names = Vec::with_capacity(len);
            for i in 0..len {
                let item = seq.get_item(i)?;
                let name = item.extract::<String>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Expected str at index {}, got {}",
                        i,
                        item.get_type()
                            .name()
                            .map(|n| n.to_string())
                            .unwrap_or_else(|_| "unknown".to_string())
                    ))
                })?;
                names.push(Arc::from(name.as_str()));
            }
            Ok(PyColumnSelection {
                selection: ColumnSelection::Names(names),
            })
        } else if first.extract::<i64>().is_ok() {
            // Extract as integers
            let mut indices = Vec::with_capacity(len);
            for i in 0..len {
                let item = seq.get_item(i)?;
                let idx = item.extract::<i64>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Expected int at index {}, got {}",
                        i,
                        item.get_type()
                            .name()
                            .map(|n| n.to_string())
                            .unwrap_or_else(|_| "unknown".to_string())
                    ))
                })?;

                if idx < 0 {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Column index cannot be negative: {}",
                        idx
                    )));
                }

                indices.push(idx as usize);
            }
            Ok(PyColumnSelection {
                selection: ColumnSelection::Indices(indices),
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Expected sequence of str or int, got sequence starting with {}",
                first
                    .get_type()
                    .name()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|_| "unknown".to_string())
            )))
        }
    }
}

/// Python path-like type for single file paths.
///
/// Accepts `str` or `pathlib.Path` for specifying a single file path.
/// This is a simpler alternative to `PyFileSource` for functions that
/// only accept a single file (like `open()`).
///
/// # Requirements
/// - 1.7: The `open()` function accepts `str` or `Path` for the source parameter
pub struct PyPathLike {
    /// The resolved file path as a string.
    path: String,
}

impl PyPathLike {
    /// Get the resolved file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Convert to a String (consuming self).
    pub fn into_path(self) -> String {
        self.path
    }
}

impl<'py> FromPyObject<'py> for PyPathLike {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        // Try to extract as a string first
        if let Ok(s) = ob.extract::<String>() {
            return Ok(PyPathLike { path: s });
        }

        // Try to extract as a pathlib.Path (has __fspath__ method)
        if let Ok(path_str) = extract_path_str(ob) {
            return Ok(PyPathLike { path: path_str });
        }

        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
            "Expected str or Path, got {}",
            ob.get_type()
                .name()
                .map(|n| n.to_string())
                .unwrap_or_else(|_| "unknown".to_string())
        )))
    }
}

#[cfg(test)]
mod tests {
    // Note: These tests require Python runtime, so they're integration tests
    // Unit tests for the Rust logic are in the api::columns module
}

/// Validate and convert a Python `row_index_offset` (i64) to `IdxSize` (u32).
///
/// Python exposes `row_index_offset` as a signed integer for ergonomic negative
/// value detection. This function validates the value is within the valid range
/// `[0, u32::MAX]` and converts it to `IdxSize`.
///
/// # Errors
/// - `PyValueError` if the offset is negative
/// - `PyValueError` if the offset exceeds `u32::MAX` (4,294,967,295)
pub fn validate_row_index_offset(offset: i64) -> PyResult<IdxSize> {
    if offset < 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "row_index_offset cannot be negative",
        ));
    }
    IdxSize::try_from(offset).map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "row_index_offset {} exceeds maximum value of {}",
            offset,
            IdxSize::MAX
        ))
    })
}
