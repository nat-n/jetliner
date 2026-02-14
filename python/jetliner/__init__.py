# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Nat Noordanus

"""
Jetliner - High-performance Avro streaming reader for Polars DataFrames.

This module provides three complementary APIs for reading Avro files:

1. **scan_avro()** - LazyFrame with Query Optimization (recommended for most use cases)
   - Returns a Polars LazyFrame via register_io_source
   - Enables projection pushdown (only read needed columns)
   - Enables predicate pushdown (filter at source level)
   - Enables early stopping (head(), limit())
   - Integrates with Polars streaming engine

2. **read_avro()** - Eager DataFrame Loading
   - Returns a Polars DataFrame directly
   - Supports column selection via `columns` parameter
   - Equivalent to `scan_avro(...).collect()` with eager projection

3. **AvroReader** - Iterator for Streaming Control
   - Directly instantiate the class for streaming iteration
   - Yields DataFrame batches with full control over processing
   - Error inspection via `.errors` and `.error_count` properties
   - Schema access via `.schema` and `.schema_dict` properties
   - Useful for custom streaming pipelines, progress tracking, or error handling

Classes:
    AvroReader: Directly instantiated iterator for streaming control over single files
    MultiAvroReader: Multi-file iterator (used internally by scan/read)
    BadBlockError: Structured error information from skip mode reading

Exceptions:
    All exceptions inherit from JetlinerError and provide structured attributes.
    See jetliner.exceptions module for details.

Example usage:
    >>> import jetliner
    >>> import polars as pl
    >>>
    >>> # Using scan_avro() with query optimization
    >>> result = (
    ...     jetliner.scan_avro("data.avro")
    ...     .select(["col1", "col2"])
    ...     .filter(pl.col("amount") > 100)
    ...     .head(1000)
    ...     .collect()
    ... )
    >>>
    >>> # Using read_avro() for eager loading with column selection
    >>> df = jetliner.read_avro("data.avro", columns=["col1", "col2"], n_rows=1000)
    >>>
    >>> # Using AvroReader for streaming control with error handling
    >>> with jetliner.AvroReader("data.avro", ignore_errors=True) as reader:
    ...     for df in reader:
    ...         process(df)
    ...     if reader.error_count > 0:
    ...         print(f"Skipped {reader.error_count} errors")
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import polars as pl

from .jetliner import (
    # New API functions
    scan_avro as _scan_avro,
    read_avro as _read_avro,
    read_avro_schema as _read_avro_schema,
    # Internal functions (underscore prefix, not in __all__)
    # Used by scan_avro's generated Python code
    _resolve_avro_sources,  # noqa: F401
    # Classes
    AvroReader,
    MultiAvroReader,
    BadBlockError,
)

# Exception types (defined in Python for proper inheritance hierarchy)
from .exceptions import (
    JetlinerError,
    DecodeError,
    ParseError,
    SourceError,
    SchemaError,
    CodecError,
    AuthenticationError,
    FileNotFoundError,
    PermissionError,
    ConfigurationError,
)


# Type aliases for documentation and type checking
FileSource = str | Path | Sequence[str] | Sequence[Path]
"""Type alias for file source parameters.

Accepts:
- str: A single file path or S3 URI
- Path: A pathlib.Path object
- Sequence[str]: Multiple file paths
- Sequence[Path]: Multiple Path objects
"""

# Type-annotated wrapper functions for the Rust bindings
def scan_avro(
    source: FileSource,
    *,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
    glob: bool = True,
    include_file_paths: str | None = None,
    ignore_errors: bool = False,
    storage_options: dict[str, str] | None = None,
    buffer_blocks: int = 4,
    buffer_bytes: int = 64 * 1024 * 1024,
    read_chunk_size: int | None = None,
    batch_size: int = 100_000,
    max_block_size: int | None = 512 * 1024 * 1024,
) -> pl.LazyFrame:
    """
    Scan Avro file(s), returning a LazyFrame with query optimization support.

    This function uses Polars' IO plugin system to enable query optimizations:
    - Projection pushdown: Only read columns that are actually used in the query
    - Predicate pushdown: Apply filters during reading, not after
    - Early stopping: Stop reading after the requested number of rows

    Parameters
    ----------
    source : str | Path | Sequence[str] | Sequence[Path]
        Path to Avro file(s). Supports:
        - Local filesystem paths: `/path/to/file.avro`, `./relative/path.avro`
        - S3 URIs: `s3://bucket/key.avro`
        - Glob patterns with standard wildcards:
          - `*` matches any characters except `/` (e.g., `data/*.avro`)
          - `**` matches zero or more directories (e.g., `data/**/*.avro`)
          - `?` matches a single character (e.g., `file?.avro`)
          - `[...]` matches character ranges (e.g., `data/[0-9]*.avro`)
          - `{a,b,c}` matches alternatives (e.g., `data/{2023,2024}/*.avro`)
        - Multiple files: `["file1.avro", "file2.avro"]`
        When multiple files are provided, schemas must be compatible.
    n_rows : int | None, default None
        Maximum number of rows to read across all files. None means read all rows.
    row_index_name : str | None, default None
        If provided, adds a row index column with this name as the first column.
        With multiple files, the index continues across files.
    row_index_offset : int, default 0
        Starting value for the row index (only used if row_index_name is set).
    glob : bool, default True
        If True, expand glob patterns in the source path. Set to False to treat
        patterns as literal filenames.
    include_file_paths : str | None, default None
        If provided, adds a column with this name containing the source file path
        for each row. Useful for tracking data provenance with multiple files.
    ignore_errors : bool, default False
        If True, skip corrupted blocks/records and continue reading. If False,
        fail immediately on first error. Errors are not accessible in scan mode.
    storage_options : dict[str, str] | None, default None
        Configuration for S3 connections. Supported keys:
        - endpoint: Custom S3 endpoint (for MinIO, LocalStack, R2, etc.)
        - aws_access_key_id: AWS access key (overrides environment)
        - aws_secret_access_key: AWS secret key (overrides environment)
        - region: AWS region (overrides environment)
        Values here take precedence over environment variables.
    buffer_blocks : int, default 4
        Number of blocks to prefetch for better I/O performance.
    buffer_bytes : int, default 64MB
        Maximum bytes to buffer during prefetching.
    read_chunk_size : int | None, default None
        Read buffer chunk size in bytes. If None, auto-detects based on source:
        64KB for local files, 4MB for S3. Larger values reduce I/O operations
        but use more memory.
    batch_size : int, default 100,000
        Minimum rows per DataFrame batch. Rows accumulate until this threshold
        is reached, then yield. Batches may exceed this since blocks are processed
        whole. Final batch may be smaller if fewer rows remain.
    max_block_size : int | None, default 512MB
        Maximum decompressed block size in bytes. Blocks that would decompress
        to more than this limit are rejected. Set to None to disable the limit.
        Default: 512MB (536870912 bytes).

        This protects against decompression bombs - maliciously crafted files
        where a small compressed block expands to consume excessive memory.

    Returns
    -------
    pl.LazyFrame
        A LazyFrame that can be used with Polars query operations.

    Examples
    --------
    >>> import jetliner
    >>> import polars as pl
    >>>
    >>> # Basic scan
    >>> lf = jetliner.scan_avro("data.avro")
    >>>
    >>> # With query optimization
    >>> result = (
    ...     jetliner.scan_avro("data/*.avro")
    ...     .select(["col1", "col2"])
    ...     .filter(pl.col("amount") > 100)
    ...     .head(1000)
    ...     .collect()
    ... )
    >>>
    >>> # Multiple files with row tracking
    >>> result = (
    ...     jetliner.scan_avro(
    ...         ["file1.avro", "file2.avro"],
    ...         row_index_name="idx",
    ...         include_file_paths="source"
    ...     )
    ...     .collect()
    ... )
    >>>
    >>> # S3 with custom endpoint (MinIO, LocalStack, R2)
    >>> lf = jetliner.scan_avro(
    ...     "s3://bucket/data.avro",
    ...     storage_options={
    ...         "endpoint": "http://localhost:9000",
    ...         "aws_access_key_id": "minioadmin",
    ...         "aws_secret_access_key": "minioadmin",
    ...     }
    ... )
    """
    return _scan_avro(
        source,
        n_rows=n_rows,
        row_index_name=row_index_name,
        row_index_offset=row_index_offset,
        glob=glob,
        include_file_paths=include_file_paths,
        ignore_errors=ignore_errors,
        storage_options=storage_options,
        buffer_blocks=buffer_blocks,
        buffer_bytes=buffer_bytes,
        read_chunk_size=read_chunk_size,
        batch_size=batch_size,
        max_block_size=max_block_size,
    )


def read_avro(
    source: FileSource,
    *,
    columns: Sequence[str] | Sequence[int] | None = None,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
    glob: bool = True,
    include_file_paths: str | None = None,
    ignore_errors: bool = False,
    storage_options: dict[str, str] | None = None,
    buffer_blocks: int = 4,
    buffer_bytes: int = 64 * 1024 * 1024,
    read_chunk_size: int | None = None,
    batch_size: int = 100_000,
    max_block_size: int | None = 512 * 1024 * 1024,
) -> pl.DataFrame:
    """
    Read Avro file(s), returning a DataFrame.

    Eagerly reads data into memory. For large files or when you need query
    optimization, use `scan_avro()` instead.

    Parameters
    ----------
    source : str | Path | Sequence[str] | Sequence[Path]
        Path to Avro file(s). Supports:
        - Local filesystem paths: `/path/to/file.avro`, `./relative/path.avro`
        - S3 URIs: `s3://bucket/key.avro`
        - Glob patterns with standard wildcards:
          - `*` matches any characters except `/` (e.g., `data/*.avro`)
          - `**` matches zero or more directories (e.g., `data/**/*.avro`)
          - `?` matches a single character (e.g., `file?.avro`)
          - `[...]` matches character ranges (e.g., `data/[0-9]*.avro`)
          - `{a,b,c}` matches alternatives (e.g., `data/{2023,2024}/*.avro`)
        - Multiple files: `["file1.avro", "file2.avro"]`
        When multiple files are provided, schemas must be compatible.
    columns : Sequence[str] | Sequence[int] | None, default None
        Columns to read. Projection happens during decoding for efficiency. Can be:
        - List of column names: `["col1", "col2"]`
        - List of column indices (0-based): `[0, 2, 5]`
        - None to read all columns
    n_rows : int | None, default None
        Maximum number of rows to read across all files. None means read all rows.
    row_index_name : str | None, default None
        If provided, adds a row index column with this name as the first column.
        With multiple files, the index continues across files.
    row_index_offset : int, default 0
        Starting value for the row index (only used if row_index_name is set).
    glob : bool, default True
        If True, expand glob patterns in the source path. Set to False to treat
        patterns as literal filenames.
    include_file_paths : str | None, default None
        If provided, adds a column with this name containing the source file path
        for each row. Useful for tracking data provenance with multiple files.
    ignore_errors : bool, default False
        If True, skip corrupted blocks/records and continue reading. If False,
        fail immediately on first error. Errors are not accessible in read mode.
    storage_options : dict[str, str] | None, default None
        Configuration for S3 connections. Supported keys:
        - endpoint: Custom S3 endpoint (for MinIO, LocalStack, R2, etc.)
        - aws_access_key_id: AWS access key (overrides environment)
        - aws_secret_access_key: AWS secret key (overrides environment)
        - region: AWS region (overrides environment)
        Values here take precedence over environment variables.
    buffer_blocks : int, default 4
        Number of blocks to prefetch for better I/O performance.
    buffer_bytes : int, default 64MB
        Maximum bytes to buffer during prefetching.
    read_chunk_size : int | None, default None
        Read buffer chunk size in bytes. If None, auto-detects based on source:
        64KB for local files, 4MB for S3. Larger values reduce I/O operations
        but use more memory.
    batch_size : int, default 100,000
        Minimum rows per DataFrame batch. Rows accumulate until this threshold
        is reached, then yield. Batches may exceed this since blocks are processed
        whole. Final batch may be smaller if fewer rows remain.
    max_block_size : int | None, default 512MB
        Maximum decompressed block size in bytes. Blocks that would decompress
        to more than this limit are rejected. Set to None to disable the limit.
        Default: 512MB (536870912 bytes).

        This protects against decompression bombs - maliciously crafted files
        where a small compressed block expands to consume excessive memory.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the Avro data.

    Examples
    --------
    >>> import jetliner
    >>>
    >>> # Read all columns
    >>> df = jetliner.read_avro("data.avro")
    >>>
    >>> # Read specific columns by name
    >>> df = jetliner.read_avro("data.avro", columns=["col1", "col2"])
    >>>
    >>> # Read specific columns by index
    >>> df = jetliner.read_avro("data.avro", columns=[0, 2, 5])
    >>>
    >>> # Read with row limit
    >>> df = jetliner.read_avro("data.avro", n_rows=1000)
    >>>
    >>> # Multiple files with row tracking
    >>> df = jetliner.read_avro(
    ...     ["file1.avro", "file2.avro"],
    ...     row_index_name="idx",
    ...     include_file_paths="source"
    ... )
    """
    return _read_avro(
        source,
        columns=columns,
        n_rows=n_rows,
        row_index_name=row_index_name,
        row_index_offset=row_index_offset,
        glob=glob,
        include_file_paths=include_file_paths,
        ignore_errors=ignore_errors,
        storage_options=storage_options,
        buffer_blocks=buffer_blocks,
        buffer_bytes=buffer_bytes,
        read_chunk_size=read_chunk_size,
        batch_size=batch_size,
        max_block_size=max_block_size,
    )


def read_avro_schema(
    source: FileSource,
    *,
    storage_options: dict[str, str] | None = None,
) -> pl.Schema:
    """
    Read the schema from an Avro file without reading data.

    Returns the Polars schema that would result from reading the file.
    Avro types are mapped to Polars types (e.g., Avro records become Structs,
    Avro arrays become Lists, Avro enums become Categorical).

    Parameters
    ----------
    source : str | Path | Sequence[str] | Sequence[Path]
        Path to Avro file. If multiple files are provided, reads schema from
        the first file only.
    storage_options : dict[str, str] | None, default None
        Configuration for S3 connections. Supported keys:
        - endpoint: Custom S3 endpoint (for MinIO, LocalStack, R2, etc.)
        - aws_access_key_id: AWS access key (overrides environment)
        - aws_secret_access_key: AWS secret key (overrides environment)
        - region: AWS region (overrides environment)
        Values here take precedence over environment variables.

    Returns
    -------
    pl.Schema
        The Polars schema corresponding to the Avro file's schema.

    Examples
    --------
    >>> import jetliner
    >>>
    >>> # Read schema from local file
    >>> schema = jetliner.read_avro_schema("data.avro")
    >>> print(schema)
    >>>
    >>> # Read schema from S3
    >>> schema = jetliner.read_avro_schema(
    ...     "s3://bucket/data.avro",
    ...     storage_options={"region": "us-west-2"}
    ... )
    >>> print(schema.names())  # Column names
    >>> print(schema.dtypes())  # Column types
    """
    return _read_avro_schema(source, storage_options=storage_options)


__all__ = [
    # API functions
    "scan_avro",
    "read_avro",
    "read_avro_schema",
    # Classes
    "AvroReader",
    "MultiAvroReader",
    "BadBlockError",
    # Exception types (hierarchy: JetlinerError -> specific errors)
    "JetlinerError",
    "DecodeError",
    "ParseError",
    "SourceError",
    "SchemaError",
    "CodecError",
    "AuthenticationError",
    "FileNotFoundError",
    "PermissionError",
    "ConfigurationError",
    # Type aliases
    "FileSource",
]
