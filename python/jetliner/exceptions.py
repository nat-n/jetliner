# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Nat Noordanus

"""
Jetliner exception classes with structured metadata.

All Jetliner exceptions inherit from JetlinerError, allowing users to catch
any Jetliner-specific error with a single except clause:

    try:
        df = jetliner.read_avro("file.avro")
    except jetliner.JetlinerError as e:
        print(f"Jetliner error: {e}")
        print(f"Variant: {e.variant}")

Exceptions that map to common Python builtins also inherit from those builtins,
allowing idiomatic Python exception handling:

    try:
        df = jetliner.read_avro("nonexistent.avro")
    except FileNotFoundError as e:
        print(f"File not found: {e.path}")

Each exception type exposes structured attributes for programmatic access:

    try:
        df = jetliner.read_avro("corrupted.avro")
    except jetliner.DecodeError as e:
        print(f"Error at block {e.block_index}, record {e.record_index}")
        print(f"Variant: {e.variant}")
        print(f"File offset: {e.file_offset}")
"""

from __future__ import annotations

import builtins


class JetlinerError(Exception):
    """Base exception for all Jetliner errors.

    Users can catch this to handle any Jetliner-specific error.

    Attributes:
        message: Human-readable error message
        variant: The specific error variant (e.g., "InvalidData", "NotFound")
    """

    message: str
    variant: str

    def __init__(self, message: str, variant: str = "Unknown"):
        super().__init__(message)
        self.message = message
        self.variant = variant

    def to_dict(self) -> dict:
        """Convert exception attributes to a dictionary."""
        return {
            "message": self.message,
            "variant": self.variant,
        }


class DecodeError(JetlinerError):
    """Error decoding Avro record data.

    Attributes:
        message: Human-readable error message
        variant: The specific decode error variant (e.g., "InvalidData", "InvalidUtf8")
        path: The file path where the error occurred (if available)
        block_index: The block number where the error occurred
        record_index: The record number within the block
        file_offset: The absolute file offset where the error occurred
        block_offset: The offset within the block where the error occurred
    """

    def __init__(
        self,
        message: str,
        variant: str = "Decode",
        path: str | None = None,
        block_index: int | None = None,
        record_index: int | None = None,
        file_offset: int | None = None,
        block_offset: int | None = None,
    ):
        super().__init__(message, variant)
        self.path = path
        self.block_index = block_index
        self.record_index = record_index
        self.file_offset = file_offset
        self.block_offset = block_offset

    def __str__(self) -> str:
        parts = ["Decode error"]
        if self.path:
            parts.append(f"in '{self.path}'")
        if self.block_index is not None and self.record_index is not None:
            parts.append(f"at block {self.block_index}, record {self.record_index}:")
        elif self.block_index is not None:
            parts.append(f"at block {self.block_index}:")
        else:
            parts.append(":")
        parts.append(self.message)
        return " ".join(parts)

    def __repr__(self) -> str:
        return (
            f"DecodeError(message={self.message!r}, variant={self.variant!r}, "
            f"path={self.path!r}, block_index={self.block_index}, "
            f"record_index={self.record_index}, file_offset={self.file_offset}, "
            f"block_offset={self.block_offset})"
        )

    def to_dict(self) -> dict:
        """Convert exception attributes to a dictionary."""
        return {
            "message": self.message,
            "variant": self.variant,
            "path": self.path,
            "block_index": self.block_index,
            "record_index": self.record_index,
            "file_offset": self.file_offset,
            "block_offset": self.block_offset,
        }


class ParseError(JetlinerError):
    """Error parsing Avro file structure.

    Raised when the file header, magic bytes, or sync markers are invalid.

    Attributes:
        message: Human-readable error message
        variant: The specific parse error variant (e.g., "InvalidMagic", "InvalidSyncMarker")
        path: The file path where the error occurred (if available)
        file_offset: The file offset where the error occurred
    """

    def __init__(
        self,
        message: str,
        variant: str = "Parse",
        path: str | None = None,
        file_offset: int | None = None,
    ):
        super().__init__(message, variant)
        self.path = path
        self.file_offset = file_offset

    def __str__(self) -> str:
        parts = ["Parse error"]
        if self.path:
            parts.append(f"in '{self.path}'")
        if self.file_offset is not None:
            parts.append(f"at offset {self.file_offset}:")
        else:
            parts.append(":")
        parts.append(self.message)
        return " ".join(parts)

    def __repr__(self) -> str:
        return (
            f"ParseError(message={self.message!r}, variant={self.variant!r}, "
            f"path={self.path!r}, file_offset={self.file_offset})"
        )

    def to_dict(self) -> dict:
        """Convert exception attributes to a dictionary."""
        return {
            "message": self.message,
            "variant": self.variant,
            "path": self.path,
            "file_offset": self.file_offset,
        }


class SourceError(JetlinerError):
    """Error accessing data source.

    Raised for I/O errors, S3 errors, and other source access issues.
    Note: FileNotFoundError, PermissionError, and AuthenticationError are
    raised for those specific cases.

    Attributes:
        message: Human-readable error message
        variant: The specific source error variant (e.g., "S3Error", "Io")
        path: The file path that caused the error
    """

    def __init__(
        self,
        message: str,
        variant: str = "Source",
        path: str | None = None,
    ):
        super().__init__(message, variant)
        self.path = path

    def __str__(self) -> str:
        if self.path:
            return f"Source error for '{self.path}': {self.message}"
        return f"Source error: {self.message}"

    def __repr__(self) -> str:
        return (
            f"SourceError(message={self.message!r}, variant={self.variant!r}, "
            f"path={self.path!r})"
        )

    def to_dict(self) -> dict:
        """Convert exception attributes to a dictionary."""
        return {
            "message": self.message,
            "variant": self.variant,
            "path": self.path,
        }


class SchemaError(JetlinerError):
    """Error with Avro schema.

    Raised for invalid schemas, unsupported types, or schema incompatibilities.

    Attributes:
        message: Human-readable error message
        variant: The specific schema error variant (e.g., "InvalidSchema", "UnsupportedType")
        path: The file path where the error occurred (if available)
        schema_context: Optional additional context (e.g., field name, type)
    """

    def __init__(
        self,
        message: str,
        variant: str = "Schema",
        path: str | None = None,
        schema_context: str | None = None,
    ):
        super().__init__(message, variant)
        self.path = path
        self.schema_context = schema_context

    def __str__(self) -> str:
        parts = ["Schema error"]
        if self.path:
            parts.append(f"in '{self.path}'")
        parts.append(f": {self.message}")
        if self.schema_context:
            parts.append(f" (context: {self.schema_context})")
        return "".join(parts)

    def __repr__(self) -> str:
        return (
            f"SchemaError(message={self.message!r}, variant={self.variant!r}, "
            f"path={self.path!r}, schema_context={self.schema_context!r})"
        )

    def to_dict(self) -> dict:
        """Convert exception attributes to a dictionary."""
        return {
            "message": self.message,
            "variant": self.variant,
            "path": self.path,
            "schema_context": self.schema_context,
        }


class CodecError(JetlinerError):
    """Error with compression codec.

    Raised for unsupported codecs or decompression failures.

    Attributes:
        message: Human-readable error message
        variant: The specific codec error variant (e.g., "UnsupportedCodec", "DecompressionError")
        path: The file path where the error occurred (if available)
        codec: The codec name that caused the error
        block_index: The block number where the error occurred (if available)
        file_offset: The file offset where the error occurred (if available)
    """

    def __init__(
        self,
        message: str,
        variant: str = "Codec",
        path: str | None = None,
        codec: str = "",
        block_index: int | None = None,
        file_offset: int | None = None,
    ):
        super().__init__(message, variant)
        self.path = path
        self.codec = codec
        self.block_index = block_index
        self.file_offset = file_offset

    def __str__(self) -> str:
        parts = ["Codec error"]
        if self.codec:
            parts.append(f"({self.codec})")
        if self.path:
            parts.append(f"in '{self.path}'")
        parts.append(f": {self.message}")
        return " ".join(parts)

    def __repr__(self) -> str:
        return (
            f"CodecError(message={self.message!r}, variant={self.variant!r}, "
            f"path={self.path!r}, codec={self.codec!r}, "
            f"block_index={self.block_index}, file_offset={self.file_offset})"
        )

    def to_dict(self) -> dict:
        """Convert exception attributes to a dictionary."""
        return {
            "message": self.message,
            "variant": self.variant,
            "path": self.path,
            "codec": self.codec,
            "block_index": self.block_index,
            "file_offset": self.file_offset,
        }


class AuthenticationError(JetlinerError):
    """S3/cloud authentication failure.

    Raised when authentication fails (invalid credentials, expired tokens, etc.).
    This is distinct from PermissionError which is for valid credentials with
    insufficient permissions.

    Attributes:
        message: Human-readable error message
        variant: The specific authentication error variant
        path: The file path where the error occurred (if available)
    """

    def __init__(
        self,
        message: str,
        variant: str = "AuthenticationFailed",
        path: str | None = None,
    ):
        super().__init__(message, variant)
        self.path = path

    def __str__(self) -> str:
        if self.path:
            return f"Authentication failed for '{self.path}': {self.message}"
        return f"Authentication failed: {self.message}"

    def __repr__(self) -> str:
        return (
            f"AuthenticationError(message={self.message!r}, variant={self.variant!r}, "
            f"path={self.path!r})"
        )

    def to_dict(self) -> dict:
        """Convert exception attributes to a dictionary."""
        return {
            "message": self.message,
            "variant": self.variant,
            "path": self.path,
        }


class FileNotFoundError(JetlinerError, builtins.FileNotFoundError):
    """File or S3 object not found.

    Inherits from both JetlinerError and builtins.FileNotFoundError, allowing
    idiomatic Python exception handling:

        try:
            df = jetliner.read_avro("missing.avro")
        except FileNotFoundError:
            print("File not found!")

    Attributes:
        message: Human-readable error message
        variant: The specific error variant
        path: The file path that was not found
        errno: Error number (2 = ENOENT for file not found)
        filename: The file path that was not found (alias for path)
    """

    def __init__(
        self,
        message: str,
        variant: str = "NotFound",
        path: str | None = None,
    ):
        import errno as errno_module

        JetlinerError.__init__(self, message, variant)
        # Call OSError init with (errno, strerror, filename) for proper attribute setup
        builtins.FileNotFoundError.__init__(
            self, errno_module.ENOENT, message, path
        )
        self.path = path
        # Ensure filename is set (OSError should set it but we ensure it)
        self.filename = path

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return (
            f"FileNotFoundError(message={self.message!r}, variant={self.variant!r}, "
            f"path={self.path!r})"
        )

    def to_dict(self) -> dict:
        """Convert exception attributes to a dictionary."""
        return {
            "message": self.message,
            "variant": self.variant,
            "path": self.path,
        }


class PermissionError(JetlinerError, builtins.PermissionError):
    """Permission denied (but authenticated).

    Raised when credentials are valid but lack permission for the requested
    operation. This is distinct from AuthenticationError which is for
    invalid/missing credentials.

    Inherits from both JetlinerError and builtins.PermissionError, allowing
    idiomatic Python exception handling:

        try:
            df = jetliner.read_avro("s3://restricted-bucket/file.avro")
        except PermissionError:
            print("Access denied!")

    Attributes:
        message: Human-readable error message
        variant: The specific error variant
        path: The file path that caused the error
        errno: Error number (13 = EACCES for permission denied)
        filename: The file path that caused the error (alias for path)
    """

    def __init__(
        self,
        message: str,
        variant: str = "PermissionDenied",
        path: str | None = None,
    ):
        import errno as errno_module

        JetlinerError.__init__(self, message, variant)
        # Call OSError init with (errno, strerror, filename) for proper attribute setup
        builtins.PermissionError.__init__(
            self, errno_module.EACCES, message, path
        )
        self.path = path
        # Ensure filename is set (OSError should set it but we ensure it)
        self.filename = path

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return (
            f"PermissionError(message={self.message!r}, variant={self.variant!r}, "
            f"path={self.path!r})"
        )

    def to_dict(self) -> dict:
        """Convert exception attributes to a dictionary."""
        return {
            "message": self.message,
            "variant": self.variant,
            "path": self.path,
        }


class ConfigurationError(JetlinerError, builtins.ValueError):
    """Invalid configuration parameters.

    Raised when invalid parameters are passed to Jetliner functions
    (e.g., invalid batch size, unsupported options).

    Inherits from both JetlinerError and builtins.ValueError, allowing
    idiomatic Python exception handling:

        try:
            reader = jetliner.open("file.avro", batch_size=-1)
        except ValueError:
            print("Invalid configuration!")

    Attributes:
        message: Human-readable error message
        variant: The specific error variant
    """

    def __init__(
        self,
        message: str,
        variant: str = "Configuration",
    ):
        JetlinerError.__init__(self, message, variant)
        builtins.ValueError.__init__(self, message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return (
            f"ConfigurationError(message={self.message!r}, variant={self.variant!r})"
        )

    def to_dict(self) -> dict:
        """Convert exception attributes to a dictionary."""
        return {
            "message": self.message,
            "variant": self.variant,
        }
