# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Nat Noordanus

"""
Jetliner - High-performance Avro streaming reader for Polars DataFrames.

This module provides two complementary APIs for reading Avro files:

1. **scan()** - LazyFrame with Query Optimization (recommended for most use cases)
   - Returns a Polars LazyFrame via register_io_source
   - Enables projection pushdown (only read needed columns)
   - Enables predicate pushdown (filter at source level)
   - Enables early stopping (head(), limit())
   - Integrates with Polars streaming engine

2. **open()** - Iterator for Streaming Control
   - Returns an iterator yielding DataFrame batches
   - Full control over batch processing
   - Useful for custom streaming pipelines, progress tracking, or memory-constrained environments

Both APIs share the same Rust core - the IO plugin just wraps the iterator in a generator
that Polars can optimize.

Example usage:
    >>> import jetliner
    >>> import polars as pl
    >>>
    >>> # Using scan() with query optimization
    >>> result = (
    ...     jetliner.scan("data.avro")
    ...     .select(["col1", "col2"])
    ...     .filter(pl.col("amount") > 100)
    ...     .head(1000)
    ...     .collect()
    ... )
    >>>
    >>> # Using open() for streaming control
    >>> with jetliner.open("data.avro") as reader:
    ...     for df in reader:
    ...         process(df)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

import polars as pl
from polars.io.plugins import register_io_source

from .jetliner import (
    # Functions
    open,
    parse_avro_schema,
    # Classes
    AvroReader,
    AvroReaderCore,
    # Exception types
    JetlinerError,
    ParseError,
    SchemaError,
    CodecError,
    DecodeError,
    SourceError,
)

if TYPE_CHECKING:
    pass


def scan(
    path: str,
    *,
    buffer_blocks: int = 4,
    buffer_bytes: int = 64 * 1024 * 1024,
    strict: bool = False,
) -> pl.LazyFrame:
    """
    Scan an Avro file, returning a LazyFrame with query optimization support.

    This function uses Polars' IO plugin system to enable query optimizations:
    - Projection pushdown: Only read columns that are actually used in the query
    - Predicate pushdown: Apply filters during reading, not after
    - Early stopping: Stop reading after the requested number of rows

    Parameters
    ----------
    path : str
        Path to Avro file. Supports:
        - Local filesystem paths: `/path/to/file.avro`, `./relative/path.avro`
        - S3 URIs: `s3://bucket/key.avro`
    buffer_blocks : int, default 4
        Number of blocks to prefetch for better I/O performance.
    buffer_bytes : int, default 64MB
        Maximum bytes to buffer during prefetching.
    strict : bool, default False
        If True, fail on first error. If False, skip bad records and continue.

    Returns
    -------
    pl.LazyFrame
        A LazyFrame that can be used with Polars query operations.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    PermissionError
        If access is denied.
    jetliner.ParseError
        If the file is not a valid Avro file.
    jetliner.SchemaError
        If the schema is invalid or cannot be converted.
    jetliner.SourceError
        For S3 or filesystem errors.

    Examples
    --------
    Basic scan - returns LazyFrame:

    >>> lf = jetliner.scan("data.avro")
    >>> lf = jetliner.scan("s3://bucket/file.avro")

    Query with projection pushdown - only reads col1, col2 from disk:

    >>> result = (
    ...     jetliner.scan("file.avro")
    ...     .select(["col1", "col2"])
    ...     .collect()
    ... )

    Query with predicate pushdown - filters during read, not after:

    >>> result = (
    ...     jetliner.scan("file.avro")
    ...     .filter(pl.col("status") == "active")
    ...     .filter(pl.col("amount") > 100)
    ...     .collect()
    ... )

    Early stopping - stops reading after 1000 rows:

    >>> result = jetliner.scan("file.avro").head(1000).collect()

    Full query optimization example:

    >>> result = (
    ...     jetliner.scan("s3://bucket/large_file.avro")
    ...     .select(["user_id", "amount", "timestamp"])
    ...     .filter(pl.col("amount") > 0)
    ...     .group_by("user_id")
    ...     .agg(pl.col("amount").sum())
    ...     .head(100)
    ...     .collect()
    ... )

    Notes
    -----
    The scan() function is recommended for most use cases because it enables
    Polars query optimizations. Use open() instead when you need:
    - Fine-grained control over batch processing
    - Progress tracking during iteration
    - Custom memory management

    Requirements
    -----------
    - 6a.1: Return LazyFrame via register_io_source
    - 6a.2: Projection pushdown (only allocate memory for selected columns)
    - 6a.3: Predicate pushdown (apply filter to each batch)
    - 6a.4: Early stopping (stop reading after row limit)
    - 6a.5: Expose Avro schema as Polars schema for query planning
    - 6a.6: Respect batch_size hint from query engine
    """
    # Parse schema to get Polars schema (calls into Rust)
    # This reads only the header to extract the schema
    polars_schema = parse_avro_schema(path)

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        """
        Generator that yields DataFrames, respecting pushdown hints.

        This generator is called by Polars' IO plugin system with optimization hints:
        - with_columns: List of column names to read (projection pushdown)
        - predicate: Filter expression to apply (predicate pushdown)
        - n_rows: Maximum number of rows to return (early stopping)
        - batch_size: Suggested batch size from query engine
        """
        # Use provided batch_size or default to 100,000
        effective_batch_size = batch_size if batch_size is not None else 100_000

        # Create Rust reader with projection info
        # The projected_columns parameter enables builder-level filtering
        # so we only allocate memory for the columns we need
        reader = AvroReaderCore(
            path,
            batch_size=effective_batch_size,
            buffer_blocks=buffer_blocks,
            buffer_bytes=buffer_bytes,
            strict=strict,
            projected_columns=with_columns,  # Pass projection to Rust
        )

        rows_yielded = 0

        for df in reader:
            # Apply predicate pushdown (filter at Python level)
            # This filters each batch as it's read, reducing memory usage
            if predicate is not None:
                df = df.filter(predicate)

            # Handle early stopping
            if n_rows is not None:
                remaining = n_rows - rows_yielded
                if remaining <= 0:
                    break
                if df.height > remaining:
                    df = df.head(remaining)

            rows_yielded += df.height
            yield df

            # Stop if we've hit the row limit
            if n_rows is not None and rows_yielded >= n_rows:
                break

    return register_io_source(
        io_source=source_generator,
        schema=polars_schema,
    )


__all__ = [
    # Functions
    "open",
    "scan",
    "parse_avro_schema",
    # Classes
    "AvroReader",
    "AvroReaderCore",
    # Exception types
    "JetlinerError",
    "ParseError",
    "SchemaError",
    "CodecError",
    "DecodeError",
    "SourceError",
]
