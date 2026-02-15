"""Type stubs for the jetliner Rust extension module.

This file provides type annotations and documentation for classes implemented
in Rust via PyO3. It enables IDE autocompletion, type checking, and allows
mkdocstrings to generate API documentation.

NOTE: This file must be kept in sync with src/python/reader.rs when the
Rust interface changes.
"""

from collections.abc import Iterator
from os import PathLike
from typing import Any

import polars as pl


class BadBlockError:
    """Structured error information from skip mode reading.

    When reading with ``ignore_errors=True``, errors are accumulated rather than
    causing immediate failure. After iteration completes, errors are accessible
    via the reader's ``.errors`` property as a list of ``BadBlockError`` objects.

    Attributes
    ----------
    kind : str
        Error type string. One of:

        - ``"InvalidSyncMarker"`` - Block sync marker doesn't match file header
        - ``"DecompressionFailed(codec)"`` - Codec decompression failed
        - ``"BlockParseFailed"`` - Block metadata couldn't be parsed
        - ``"RecordDecodeFailed"`` - Record data couldn't be decoded
        - ``"SchemaViolation"`` - Data doesn't match expected schema
    block_index : int
        Block number where the error occurred (0-based).
    record_index : int | None
        Record number within the block, if applicable (0-based).
    file_offset : int
        Byte offset in the file where the error occurred.
    message : str
        Human-readable error description.
    filepath : str | None
        Source file path, if known (useful for multi-file reads).

    Examples
    --------
    >>> with jetliner.AvroReader("file.avro", ignore_errors=True) as reader:
    ...     for df in reader:
    ...         process(df)
    ...
    ...     for err in reader.errors:
    ...         print(f"[{err.kind}] Block {err.block_index}: {err.message}")
    ...         # Or as dict for logging/serialization
    ...         log_error(err.to_dict())
    """

    kind: str
    """Error type string (e.g., "InvalidSyncMarker", "DecompressionFailed(snappy)")."""

    block_index: int
    """Block index where error occurred (0-based)."""

    record_index: int | None
    """Record index within block (0-based), or None if not applicable."""

    file_offset: int
    """Byte offset in the file where error occurred."""

    message: str
    """Human-readable error message."""

    filepath: str | None
    """Source file path, or None if unknown."""

    def to_dict(self) -> dict[str, Any]:
        """Convert the error to a Python dictionary.

        Returns
        -------
        dict[str, Any]
            A dict with keys: kind, block_index, record_index, file_offset,
            message, filepath.

        Examples
        --------
        >>> err_dict = err.to_dict()
        >>> print(err_dict["kind"])  # "InvalidSyncMarker"
        >>> print(err_dict["block_index"])  # 5
        """
        ...

    def __repr__(self) -> str:
        """Return a detailed string representation of the error."""
        ...

    def __str__(self) -> str:
        """Return a user-friendly string representation of the error."""
        ...


class AvroReader(Iterator[pl.DataFrame]):
    """Single-file Avro reader for batch iteration over DataFrames.

    Use this class when you need control over batch processing:

    - Progress tracking between batches
    - Per-batch error inspection (with ``ignore_errors=True``)
    - Writing batches to external sinks (database, API, etc.)
    - Early termination based on content

    For most use cases, prefer ``scan_avro()`` (lazy with query optimization) or
    ``read_avro()`` (eager loading). Use ``AvroReader`` when you need explicit
    iteration control.

    For reading multiple files with batch control, use ``MultiAvroReader``.

    Examples
    --------
    Basic iteration:

    >>> for df in jetliner.AvroReader("data.avro"):
    ...     print(df.shape)

    With context manager (recommended):

    >>> with jetliner.AvroReader("data.avro") as reader:
    ...     for df in reader:
    ...         process(df)

    Error inspection in skip mode:

    >>> with jetliner.AvroReader("data.avro", ignore_errors=True) as reader:
    ...     for df in reader:
    ...         process(df)
    ...     if reader.error_count > 0:
    ...         print(f"Skipped {reader.error_count} errors")
    ...         for err in reader.errors:
    ...             print(f"  [{err.kind}] Block {err.block_index}: {err.message}")

    Schema inspection:

    >>> with jetliner.AvroReader("data.avro") as reader:
    ...     print(reader.schema_dict["fields"])
    """

    def __init__(
        self,
        path: str | PathLike[str],
        *,
        batch_size: int = 100_000,
        buffer_blocks: int = 4,
        buffer_bytes: int = 67_108_864,
        ignore_errors: bool = False,
        projected_columns: list[str] | None = None,
        storage_options: dict[str, str] | None = None,
        read_chunk_size: int | None = None,
        max_block_size: int | None = 536_870_912,
    ) -> None:
        """Create a new AvroReader.

        Parameters
        ----------
        path : str | PathLike[str]
            Path to the Avro file. Supports local paths and s3:// URIs.
        batch_size : int, default 100_000
            Target number of rows per DataFrame.
        buffer_blocks : int, default 4
            Number of blocks to prefetch.
        buffer_bytes : int, default 64MB
            Maximum bytes to buffer.
        ignore_errors : bool, default False
            If True, skip bad records and continue; if False, fail on first error.
        projected_columns : list[str] | None, default None
            List of column names to read. None reads all columns.
        storage_options : dict[str, str] | None, default None
            Dict for S3 configuration with keys like ``endpoint``,
            ``aws_access_key_id``, ``aws_secret_access_key``, ``region``.
        read_chunk_size : int | None, default None
            Read buffer chunk size in bytes. When None, auto-detects:
            64KB for local files, 4MB for S3. Larger values reduce I/O operations
            but use more memory.
        max_block_size : int | None, default 512MB
            Maximum decompressed block size in bytes. Blocks exceeding this limit
            are rejected. Set to None to disable. Protects against decompression bombs.

        Raises
        ------
        FileNotFoundError
            If the file does not exist.
        PermissionError
            If access is denied.
        RuntimeError
            For other errors (S3, parsing, etc.).

        Examples
        --------
        Local file:

        >>> reader = jetliner.AvroReader("/path/to/file.avro")

        S3 file:

        >>> reader = jetliner.AvroReader("s3://bucket/key.avro")

        With options:

        >>> reader = jetliner.AvroReader(
        ...     "file.avro",
        ...     batch_size=50000,
        ...     ignore_errors=True
        ... )

        S3-compatible services (MinIO, LocalStack, R2):

        >>> reader = jetliner.AvroReader(
        ...     "s3://bucket/key.avro",
        ...     storage_options={
        ...         "endpoint": "http://localhost:9000",
        ...         "aws_access_key_id": "minioadmin",
        ...         "aws_secret_access_key": "minioadmin",
        ...     }
        ... )
        """
        ...

    def __iter__(self) -> "AvroReader":
        """Return self as the iterator."""
        ...

    def __next__(self) -> pl.DataFrame:
        """Get the next DataFrame batch.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame containing the next batch of records.

        Raises
        ------
        StopIteration
            When all records have been read or reader is closed.
        DecodeError
            If an error occurs during reading (in strict mode).
        ParseError
            If the file structure is invalid.
        """
        ...

    def __enter__(self) -> "AvroReader":
        """Enter the context manager.

        Returns
        -------
        AvroReader
            Self for use in ``with`` statements.
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        """Exit the context manager.

        Releases resources held by the reader. After this call,
        the reader cannot be used for iteration.

        Returns
        -------
        bool
            False to indicate exceptions should not be suppressed.
        """
        ...

    @property
    def schema(self) -> str:
        """Get the Avro schema as a JSON string.

        Returns
        -------
        str
            The Avro schema as a JSON string.

        Examples
        --------
        >>> reader = jetliner.AvroReader("file.avro")
        >>> print(reader.schema)  # JSON string
        """
        ...

    @property
    def schema_dict(self) -> dict[str, Any]:
        """Get the Avro schema as a Python dictionary.

        Returns
        -------
        dict[str, Any]
            The Avro schema as a Python dict.

        Raises
        ------
        ValueError
            If the schema JSON cannot be parsed.

        Examples
        --------
        >>> reader = jetliner.AvroReader("file.avro")
        >>> schema = reader.schema_dict
        >>> print(schema["name"])  # Record name
        >>> print(schema["fields"])  # List of fields
        """
        ...

    @property
    def polars_schema(self) -> pl.Schema:
        """Get the Polars schema for the DataFrames being produced.

        When projection is used, only the projected columns are included.

        Returns
        -------
        pl.Schema
            A Polars Schema with column names and their data types.

        Examples
        --------
        >>> reader = jetliner.AvroReader("file.avro")
        >>> print(reader.polars_schema)  # Schema({'id': Int64, 'name': String})
        """
        ...

    @property
    def batch_size(self) -> int:
        """Get the target number of rows per DataFrame batch.

        Returns
        -------
        int
            The batch size.
        """
        ...

    @property
    def rows_read(self) -> int:
        """Get the total number of rows returned across all batches.

        Returns
        -------
        int
            Total rows read so far.
        """
        ...

    @property
    def pending_records(self) -> int:
        """Get the number of records waiting to be returned in the next batch.

        Returns
        -------
        int
            Number of pending records.
        """
        ...

    @property
    def is_finished(self) -> bool:
        """Check if all records have been read or the reader is closed.

        Returns
        -------
        bool
            True if finished, False otherwise.
        """
        ...

    @property
    def errors(self) -> list[BadBlockError]:
        """Get accumulated errors from skip mode reading.

        In skip mode (``ignore_errors=True``), errors are accumulated rather than
        causing immediate failure. This property returns all errors encountered.

        Returns
        -------
        list[BadBlockError]
            List of errors encountered during reading.

        Examples
        --------
        >>> with jetliner.AvroReader("file.avro", ignore_errors=True) as reader:
        ...     for df in reader:
        ...         process(df)
        ...
        ...     for err in reader.errors:
        ...         print(f"[{err.kind}] Block {err.block_index}: {err.message}")
        """
        ...

    @property
    def error_count(self) -> int:
        """Get the number of errors encountered during reading.

        Quick check for whether any errors occurred, without iterating
        through the errors list.

        Returns
        -------
        int
            Number of errors.

        Examples
        --------
        >>> with jetliner.AvroReader("file.avro", ignore_errors=True) as reader:
        ...     for df in reader:
        ...         process(df)
        ...
        ...     if reader.error_count > 0:
        ...         print(f"Warning: {reader.error_count} errors during read")
        """
        ...


class MultiAvroReader(Iterator[pl.DataFrame]):
    """Multi-file Avro reader for batch iteration over DataFrames.

    Use this class when you need to read multiple Avro files with batch-level control:

    - Progress tracking across files and batches
    - Row index continuity across files
    - File path tracking per row
    - Per-batch error inspection (with ``ignore_errors=True``)

    For most use cases, prefer ``scan_avro()`` (lazy with query optimization) or
    ``read_avro()`` (eager loading) — both support multiple files. Use ``MultiAvroReader``
    when you need explicit iteration control over multi-file reads.

    For single-file batch iteration, use ``AvroReader``.

    Examples
    --------
    Basic multi-file iteration:

    >>> reader = MultiAvroReader(["file1.avro", "file2.avro"])
    >>> for df in reader:
    ...     print(f"Batch: {df.shape}")

    With row tracking and file path injection:

    >>> with MultiAvroReader(
    ...     ["file1.avro", "file2.avro"],
    ...     row_index_name="idx",
    ...     include_file_paths="source_file"
    ... ) as reader:
    ...     for df in reader:
    ...         print(f"File {reader.current_source_index + 1}/{reader.total_sources}")
    ...         process(df)

    With row limit across all files:

    >>> with MultiAvroReader(
    ...     ["file1.avro", "file2.avro"],
    ...     n_rows=100_000
    ... ) as reader:
    ...     for df in reader:
    ...         process(df)
    ...     print(f"Read {reader.rows_read} rows total")
    """

    def __init__(
        self,
        paths: list[str | PathLike[str]],
        *,
        batch_size: int = 100_000,
        buffer_blocks: int = 4,
        buffer_bytes: int = 67_108_864,
        ignore_errors: bool = False,
        projected_columns: list[str] | None = None,
        n_rows: int | None = None,
        row_index_name: str | None = None,
        row_index_offset: int = 0,
        include_file_paths: str | None = None,
        storage_options: dict[str, str] | None = None,
        read_chunk_size: int | None = None,
        max_block_size: int | None = 536_870_912,
    ) -> None:
        """Create a new MultiAvroReader.

        Parameters
        ----------
        paths : list[str | PathLike[str]]
            List of paths to read. Glob patterns should be expanded before
            passing. Supports local paths and s3:// URIs (all paths must use
            the same source type).
        batch_size : int, default 100_000
            Target number of rows per DataFrame.
        buffer_blocks : int, default 4
            Number of blocks to prefetch.
        buffer_bytes : int, default 64MB
            Maximum bytes to buffer.
        ignore_errors : bool, default False
            If True, skip bad records and continue; if False, fail on first error.
        projected_columns : list[str] | None, default None
            List of column names to read. None reads all columns.
        n_rows : int | None, default None
            Maximum number of rows to read across all files. None reads all.
        row_index_name : str | None, default None
            If provided, adds a row index column with this name, continuous across files.
        row_index_offset : int, default 0
            Starting value for the row index.
        include_file_paths : str | None, default None
            If provided, adds a column with this name containing the source file path.
        storage_options : dict[str, str] | None, default None
            Dict for S3 configuration with keys like ``endpoint``,
            ``aws_access_key_id``, ``aws_secret_access_key``, ``region``.
        read_chunk_size : int | None, default None
            Read buffer chunk size in bytes. When None, auto-detects:
            64KB for local files, 4MB for S3.
        max_block_size : int | None, default 512MB
            Maximum decompressed block size in bytes. Blocks exceeding this limit
            are rejected. Set to None to disable.

        Raises
        ------
        FileNotFoundError
            If any file does not exist.
        PermissionError
            If access is denied.
        SchemaError
            If file schemas are incompatible.
        RuntimeError
            For other errors (S3, parsing, etc.).
        """
        ...

    def __iter__(self) -> "MultiAvroReader":
        """Return self as the iterator."""
        ...

    def __next__(self) -> pl.DataFrame:
        """Get the next DataFrame batch.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame containing the next batch of records.

        Raises
        ------
        StopIteration
            When all records have been read or reader is closed.
        """
        ...

    def __enter__(self) -> "MultiAvroReader":
        """Enter the context manager.

        Returns
        -------
        MultiAvroReader
            Self for use in ``with`` statements.
        """
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        """Exit the context manager.

        Releases the current file handle and marks the reader as finished,
        preventing further iteration. Accumulated errors and metadata
        (schema, rows_read, etc.) remain accessible after exit.

        Returns
        -------
        bool
            False to indicate exceptions should not be suppressed.
        """
        ...

    @property
    def schema(self) -> str:
        """Get the Avro schema as a JSON string (unified across files).

        Returns
        -------
        str
            The Avro schema as a JSON string.
        """
        ...

    @property
    def schema_dict(self) -> dict[str, Any]:
        """Get the Avro schema as a Python dictionary.

        Returns
        -------
        dict[str, Any]
            The Avro schema as a Python dict.

        Raises
        ------
        ValueError
            If the schema JSON cannot be parsed.
        """
        ...

    @property
    def polars_schema(self) -> pl.Schema:
        """Get the Polars schema for the DataFrames being produced.

        When projection is used, only the projected columns are included.

        Returns
        -------
        pl.Schema
            A Polars Schema with column names and their data types.
        """
        ...

    @property
    def batch_size(self) -> int:
        """Get the target number of rows per DataFrame batch.

        Returns
        -------
        int
            The batch size.
        """
        ...

    @property
    def rows_read(self) -> int:
        """Get the total number of rows read so far across all files.

        Returns
        -------
        int
            Total rows read.
        """
        ...

    @property
    def total_sources(self) -> int:
        """Get the total number of source files.

        Returns
        -------
        int
            Number of source files.
        """
        ...

    @property
    def current_source_index(self) -> int:
        """Get the index of the file currently being read (0-based).

        Returns
        -------
        int
            Current source index.
        """
        ...

    @property
    def is_finished(self) -> bool:
        """Check if iteration is complete.

        Returns
        -------
        bool
            True if finished, False otherwise.
        """
        ...

    @property
    def errors(self) -> list[BadBlockError]:
        """Get accumulated errors from skip mode reading.

        Available during and after iteration when using ``ignore_errors=True``.

        Returns
        -------
        list[BadBlockError]
            List of errors encountered.
        """
        ...

    @property
    def error_count(self) -> int:
        """Get the number of errors encountered.

        Returns
        -------
        int
            Number of errors.
        """
        ...
