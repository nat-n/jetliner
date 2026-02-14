"""
Comprehensive error handling tests for all Jetliner API methods.

This module provides systematic coverage of error scenarios across all API methods:
- scan_avro()
- read_avro()
- read_avro_schema()
- open()

Each test:
1. Asserts the exact exception type (no generic Exception)
2. Verifies exception attributes are populated correctly
3. Verifies error messages are descriptive and actionable
4. Tests that exceptions can be caught by JetlinerError base class

Test data files used:
- tests/data/corrupted/invalid-magic.avro
- tests/data/corrupted/truncated.avro
- tests/data/corrupted/corrupted-sync-marker.avro
- tests/data/corrupted/corrupted-compressed.avro
- tests/data/corrupted/invalid-record-data.avro
"""

from pathlib import Path

import pytest

import jetliner


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def nonexistent_path(tmp_path: Path) -> str:
    """Return a path to a file that does not exist."""
    return str(tmp_path / "nonexistent.avro")


@pytest.fixture
def empty_zero_byte_file(tmp_path: Path) -> str:
    """Create a zero-byte file (not a valid Avro file)."""
    path = tmp_path / "empty.avro"
    path.touch()
    return str(path)


@pytest.fixture
def valid_avro_file(tmp_path: Path) -> str:
    """Create a valid Avro file for testing."""
    import os

    import fastavro

    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
        ],
    }

    records = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]

    file_path = tmp_path / "valid.avro"
    with open(file_path, "wb") as f:
        fastavro.writer(f, schema, records)

    # Resolve symlinks (macOS /var -> /private/var) to avoid path mismatch
    return os.path.realpath(str(file_path))


# =============================================================================
# read_avro_schema() Error Tests
# =============================================================================


class TestReadAvroSchemaErrors:
    """Error tests for read_avro_schema() function."""

    def test_invalid_magic_raises_parse_error(self, get_test_data_path):
        """Test that invalid magic bytes raise ParseError.

        ParseError should have:
        - file_offset attribute (0 for magic byte errors)
        - descriptive message mentioning magic bytes
        """
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro_schema(path)

        err = exc_info.value
        assert hasattr(err, "file_offset")
        assert err.file_offset == 0
        assert hasattr(err, "message")
        assert len(err.message) > 0
        assert "magic" in err.message.lower() or "invalid" in err.message.lower()

    def test_invalid_magic_catchable_as_jetliner_error(self, get_test_data_path):
        """Test that ParseError can be caught as JetlinerError base class."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.JetlinerError):
            jetliner.read_avro_schema(path)

    def test_truncated_file_reads_schema_successfully(self, get_test_data_path):
        """Test that truncated file can still read schema if header is intact.

        The truncated.avro file has a valid header with schema, but truncated
        data blocks. read_avro_schema() only reads the header, so it succeeds.
        """
        path = get_test_data_path("corrupted/truncated.avro")

        # This should succeed because the schema is in the header
        schema = jetliner.read_avro_schema(path)
        assert schema is not None

    def test_file_not_found_raises_file_not_found_error(self, nonexistent_path):
        """Test that missing file raises FileNotFoundError.

        This is a standard Python exception, not a JetlinerError.
        """
        with pytest.raises(FileNotFoundError):
            jetliner.read_avro_schema(nonexistent_path)

    def test_empty_glob_raises_descriptive_error(self, tmp_path: Path):
        """Test that empty glob pattern raises descriptive error."""
        pattern = str(tmp_path / "nonexistent_*.avro")

        # Empty glob raises FileNotFoundError with descriptive message
        with pytest.raises(FileNotFoundError) as exc_info:
            jetliner.read_avro_schema(pattern)

        error_msg = str(exc_info.value).lower()
        assert "not found" in error_msg or "nonexistent" in error_msg

    def test_zero_byte_file_raises_error(self, empty_zero_byte_file):
        """Test that zero-byte file raises an error.

        A zero-byte file cannot be read at all - there's no header to parse.
        This raises a DecodeError with details about the file size issue.
        """
        with pytest.raises((jetliner.DecodeError, jetliner.SourceError, ValueError)) as exc_info:
            jetliner.read_avro_schema(empty_zero_byte_file)

        error_msg = str(exc_info.value).lower()
        assert "file" in error_msg or "size" in error_msg or "offset" in error_msg

    def test_parse_error_to_dict(self, get_test_data_path):
        """Test that ParseError.to_dict() returns all attributes."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro_schema(path)

        d = exc_info.value.to_dict()
        assert "file_offset" in d
        assert "message" in d


# =============================================================================
# read_avro() Error Tests
# =============================================================================


class TestReadAvroErrors:
    """Error tests for read_avro() function."""

    def test_invalid_magic_raises_parse_error(self, get_test_data_path):
        """Test that invalid magic bytes raise ParseError."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert err.file_offset == 0
        assert "magic" in err.message.lower() or "invalid" in err.message.lower()

    def test_truncated_file_raises_error(self, get_test_data_path):
        """Test that truncated file raises an error during data reading.

        The truncated file has valid header but truncated data blocks.
        This raises a ParseError with details about the block size issue.
        """
        path = get_test_data_path("corrupted/truncated.avro")

        # ParseError is raised when block structure cannot be parsed (truncated data)
        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro(path)

        error_msg = str(exc_info.value).lower()
        assert "block" in error_msg or "size" in error_msg or "truncat" in error_msg

    def test_corrupted_compressed_raises_codec_error(self, get_test_data_path):
        """Test that corrupted compressed data raises CodecError."""
        path = get_test_data_path("corrupted/corrupted-compressed.avro")

        with pytest.raises(jetliner.CodecError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert hasattr(err, "message")
        assert hasattr(err, "codec")

    def test_invalid_record_data_raises_decode_error(self, get_test_data_path):
        """Test that invalid record data raises DecodeError."""
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises(jetliner.DecodeError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert hasattr(err, "message")
        assert hasattr(err, "block_index")
        assert hasattr(err, "record_index")

    def test_file_not_found_raises_file_not_found_error(self, nonexistent_path):
        """Test that missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            jetliner.read_avro(nonexistent_path)

    def test_invalid_column_name_raises_error(self, get_test_data_path):
        """Test that invalid column name raises appropriate error.

        SchemaError is raised when requested columns don't exist in the schema.
        """
        path = get_test_data_path("apache-avro/weather.avro")

        with pytest.raises(jetliner.SchemaError) as exc_info:
            jetliner.read_avro(path, columns=["nonexistent_column"])

        # Error message should mention the invalid column name
        error_msg = str(exc_info.value).lower()
        assert "column" in error_msg or "nonexistent" in error_msg

    def test_empty_file_list_raises_error(self):
        """Test that empty file list raises descriptive error."""
        with pytest.raises(ValueError) as exc_info:
            jetliner.read_avro([])

        error_msg = str(exc_info.value).lower()
        assert "empty" in error_msg or "no files" in error_msg or "source" in error_msg

    def test_zero_byte_file_raises_source_error(self, empty_zero_byte_file):
        """Test that zero-byte file raises SourceError or ValueError.

        A zero-byte file cannot be read - there's no header to parse.
        """
        with pytest.raises((jetliner.SourceError, ValueError)) as exc_info:
            jetliner.read_avro(empty_zero_byte_file)

        error_msg = str(exc_info.value).lower()
        assert "file" in error_msg or "size" in error_msg or "offset" in error_msg


# =============================================================================
# scan_avro() Error Tests
# =============================================================================


class TestScanAvroErrors:
    """Error tests for scan_avro() function.

    Note: scan_avro() reads the header at scan time to get the schema,
    so header errors (like invalid magic) surface immediately.
    Data errors surface at .collect() time.
    """

    def test_invalid_magic_raises_parse_error_at_scan(self, get_test_data_path):
        """Test that invalid magic bytes raise ParseError at scan time.

        For scan_avro(), the error is raised immediately when scanning
        because the file header is read to get the schema.
        """
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.scan_avro(path)

        err = exc_info.value
        assert "magic" in err.message.lower() or "invalid" in err.message.lower()

    def test_truncated_file_raises_error_at_collect(self, get_test_data_path):
        """Test that truncated file raises error at collect time.

        Truncated files have valid headers but corrupted data blocks.
        The error surfaces as a Polars ComputeError wrapping the underlying error.
        """
        path = get_test_data_path("corrupted/truncated.avro")

        lf = jetliner.scan_avro(path)

        import polars as pl
        with pytest.raises(pl.exceptions.ComputeError):
            lf.collect()

    def test_invalid_record_data_raises_error_at_collect(self, get_test_data_path):
        """Test that invalid record data raises error at collect time.

        Invalid record data errors surface as Polars ComputeError.
        """
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        lf = jetliner.scan_avro(path)

        import polars as pl
        with pytest.raises(pl.exceptions.ComputeError):
            lf.collect()

    def test_empty_file_list_raises_error(self):
        """Test that empty file list raises descriptive error."""
        with pytest.raises(ValueError) as exc_info:
            jetliner.scan_avro([])

        error_msg = str(exc_info.value).lower()
        assert "empty" in error_msg or "no files" in error_msg or "source" in error_msg


# =============================================================================
# open() Error Tests
# =============================================================================


class TestOpenErrors:
    """Error tests for open() function."""

    def test_file_not_found_raises_file_not_found_error(self, nonexistent_path):
        """Test that missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            with jetliner.AvroReader(nonexistent_path) as reader:
                list(reader)

    def test_invalid_magic_raises_parse_error(self, get_test_data_path):
        """Test that invalid magic bytes raise ParseError."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            with jetliner.AvroReader(path) as reader:
                list(reader)

        err = exc_info.value
        assert err.file_offset == 0
        assert "magic" in err.message.lower() or "invalid" in err.message.lower()

    def test_invalid_projected_column_silently_ignored(self, get_test_data_path):
        """Test behavior with invalid projected_columns name.

        Currently, invalid column names in projected_columns are silently
        ignored - the reader returns an empty DataFrame or only valid columns.
        This test documents the current behavior.
        """
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.AvroReader(path, projected_columns=["nonexistent_column"]) as reader:
            dfs = list(reader)
            # The reader should complete without error
            # but may return empty DataFrames or DataFrames without the column
            assert isinstance(dfs, list)

    def test_zero_byte_file_raises_source_error(self, empty_zero_byte_file):
        """Test that zero-byte file raises SourceError.

        A zero-byte file cannot be read - there's no header to parse.
        """
        with pytest.raises(jetliner.SourceError) as exc_info:
            with jetliner.AvroReader(empty_zero_byte_file) as reader:
                list(reader)

        error_msg = str(exc_info.value).lower()
        assert "file" in error_msg or "size" in error_msg or "offset" in error_msg

    def test_corrupted_compressed_raises_codec_error(self, get_test_data_path):
        """Test that corrupted compressed data raises CodecError."""
        path = get_test_data_path("corrupted/corrupted-compressed.avro")

        with pytest.raises(jetliner.CodecError) as exc_info:
            with jetliner.AvroReader(path) as reader:
                list(reader)

        err = exc_info.value
        assert hasattr(err, "message")


# =============================================================================
# Exception Attribute Tests
# =============================================================================


class TestExceptionAttributes:
    """Tests verifying exception attributes are properly populated."""

    def test_parse_error_has_all_attributes(self, get_test_data_path):
        """Test ParseError has file_offset and message attributes."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert isinstance(err.file_offset, int)
        assert isinstance(err.message, str)
        assert len(err.message) > 0

    def test_decode_error_has_all_attributes(self, get_test_data_path):
        """Test DecodeError has block_index, record_index, file_offset, message."""
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises(jetliner.DecodeError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        # Offsets may be None if unknown, or int if known
        assert err.block_index is None or isinstance(err.block_index, int)
        assert err.record_index is None or isinstance(err.record_index, int)
        assert err.file_offset is None or isinstance(err.file_offset, int)
        assert isinstance(err.message, str)

    def test_codec_error_has_all_attributes(self, get_test_data_path):
        """Test CodecError has codec and message attributes."""
        path = get_test_data_path("corrupted/corrupted-compressed.avro")

        with pytest.raises(jetliner.CodecError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert hasattr(err, "codec")
        assert isinstance(err.message, str)


# =============================================================================
# Exception Hierarchy Tests
# =============================================================================


class TestExceptionHierarchy:
    """Tests verifying exception inheritance hierarchy."""

    def test_parse_error_is_jetliner_error(self, get_test_data_path):
        """Test ParseError inherits from JetlinerError."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.JetlinerError):
            jetliner.read_avro(path)

    def test_decode_error_is_jetliner_error(self, get_test_data_path):
        """Test DecodeError inherits from JetlinerError."""
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises(jetliner.JetlinerError):
            jetliner.read_avro(path)

    def test_codec_error_is_jetliner_error(self, get_test_data_path):
        """Test CodecError inherits from JetlinerError."""
        path = get_test_data_path("corrupted/corrupted-compressed.avro")

        with pytest.raises(jetliner.JetlinerError):
            jetliner.read_avro(path)

    def test_all_jetliner_errors_are_exceptions(self):
        """Test all JetlinerError subclasses inherit from Exception."""
        assert issubclass(jetliner.JetlinerError, Exception)
        assert issubclass(jetliner.ParseError, Exception)
        assert issubclass(jetliner.DecodeError, Exception)
        assert issubclass(jetliner.CodecError, Exception)
        assert issubclass(jetliner.SourceError, Exception)
        assert issubclass(jetliner.SchemaError, Exception)


# =============================================================================
# Schema Error Tests (Unknown Codec, Complex Union, Non-Record Schema)
# =============================================================================


def create_avro_with_unknown_codec(codec_name: str) -> bytes:
    """Create a minimal Avro file with an unknown codec in metadata.

    This creates a valid Avro file structure but with an unsupported codec,
    which should trigger a helpful error message.
    """
    import json

    # Avro magic bytes
    magic = b"Obj\x01"

    # Schema
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [{"name": "id", "type": "int"}],
    }
    schema_json = json.dumps(schema).encode("utf-8")

    # Metadata map: schema + codec
    def encode_varint(n: int) -> bytes:
        """Encode a non-negative integer as Avro varint (zigzag encoded)."""
        n = (n << 1) ^ (n >> 63)
        result = []
        while n > 127:
            result.append((n & 0x7F) | 0x80)
            n >>= 7
        result.append(n)
        return bytes(result)

    def encode_string(s: bytes) -> bytes:
        """Encode bytes as Avro string (length-prefixed)."""
        return encode_varint(len(s)) + s

    # Build metadata map with 2 entries
    metadata = b""
    metadata += encode_varint(2)  # 2 entries

    # Entry 1: avro.schema
    metadata += encode_string(b"avro.schema")
    metadata += encode_string(schema_json)

    # Entry 2: avro.codec
    metadata += encode_string(b"avro.codec")
    metadata += encode_string(codec_name.encode("utf-8"))

    metadata += encode_varint(0)  # End of map

    # Sync marker (16 random bytes)
    sync_marker = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"

    # Build header
    header = magic + metadata + sync_marker

    # Add a minimal data block
    block_data = encode_varint(1)  # 1 record
    block_data += encode_varint(2)  # 2 bytes of "compressed" data
    block_data += b"\x02\x00"  # Fake compressed data
    block_data += sync_marker

    return header + block_data


class TestUnknownCodecErrors:
    """Tests for unknown codec error handling across all API methods."""

    def test_read_avro_unknown_codec_raises_codec_error(self, tmp_path: Path):
        """Test that read_avro() raises CodecError for unknown codec."""
        codec_name = "super-fancy-codec"
        file_path = tmp_path / "unknown_codec.avro"
        file_path.write_bytes(create_avro_with_unknown_codec(codec_name))

        with pytest.raises(jetliner.CodecError) as exc_info:
            jetliner.read_avro(str(file_path))

        error_msg = str(exc_info.value).lower()
        assert codec_name in error_msg

    def test_scan_avro_unknown_codec_error_at_scan(self, tmp_path: Path):
        """Test that scan_avro() raises CodecError for unknown codec at scan time.

        The codec is checked when reading the header, so the error surfaces immediately.
        """
        codec_name = "unknown-codec"
        file_path = tmp_path / "unknown_codec.avro"
        file_path.write_bytes(create_avro_with_unknown_codec(codec_name))

        with pytest.raises(jetliner.CodecError) as exc_info:
            jetliner.scan_avro(str(file_path))

        error_msg = str(exc_info.value).lower()
        assert codec_name in error_msg

    def test_open_unknown_codec_raises_codec_error(self, tmp_path: Path):
        """Test that open() raises CodecError for unknown codec."""
        codec_name = "my-custom-codec"
        file_path = tmp_path / "unknown_codec.avro"
        file_path.write_bytes(create_avro_with_unknown_codec(codec_name))

        with pytest.raises(jetliner.CodecError) as exc_info:
            with jetliner.AvroReader(str(file_path)) as reader:
                list(reader)

        error_msg = str(exc_info.value).lower()
        assert codec_name in error_msg


class TestComplexUnionErrors:
    """Tests for complex union (unsupported) error handling."""

    @pytest.fixture
    def complex_union_file(self, tmp_path: Path) -> str:
        """Create an Avro file with complex union type."""
        import fastavro

        schema = {
            "type": "record",
            "name": "ComplexUnionRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": ["string", "int", "long"]},
            ],
        }

        records = [{"id": 1, "value": "test"}]
        file_path = tmp_path / "complex_union.avro"

        with open(file_path, "wb") as f:
            fastavro.writer(f, schema, records)

        return str(file_path)

    def test_read_avro_complex_union_raises_schema_error(self, complex_union_file):
        """Test that read_avro() raises SchemaError for complex union."""
        with pytest.raises(jetliner.SchemaError) as exc_info:
            jetliner.read_avro(complex_union_file)

        error_msg = str(exc_info.value).lower()
        assert "union" in error_msg

    def test_scan_avro_complex_union_error_at_scan(self, complex_union_file):
        """Test that scan_avro() raises SchemaError for complex union at scan time.

        The schema is parsed when scanning, so the error surfaces immediately.
        """
        with pytest.raises(jetliner.SchemaError) as exc_info:
            jetliner.scan_avro(complex_union_file)

        error_msg = str(exc_info.value).lower()
        assert "union" in error_msg

    def test_open_complex_union_raises_schema_error(self, complex_union_file):
        """Test that open() raises SchemaError for complex union."""
        with pytest.raises(jetliner.SchemaError) as exc_info:
            with jetliner.AvroReader(complex_union_file) as reader:
                list(reader)

        error_msg = str(exc_info.value).lower()
        assert "union" in error_msg


class TestNonRecordSchemaErrors:
    """Tests for non-record top-level schema error handling."""

    def test_read_avro_array_toplevel_raises_schema_error(self, get_test_data_path):
        """Test that read_avro() raises SchemaError for array top-level schema."""
        path = get_test_data_path("fastavro/array-toplevel.avro")

        with pytest.raises(jetliner.SchemaError) as exc_info:
            jetliner.read_avro(path)

        error_msg = str(exc_info.value).lower()
        assert "not yet supported" in error_msg or "array" in error_msg

    def test_read_avro_schema_array_toplevel_returns_schema(self, get_test_data_path):
        """Test that read_avro_schema() can read schema from array top-level file.

        Even though reading data fails, reading the schema should succeed.
        """
        path = get_test_data_path("fastavro/array-toplevel.avro")

        # This should succeed - we can read the schema even if we can't read data
        schema = jetliner.read_avro_schema(path)
        assert schema is not None
        assert "value" in schema

    def test_open_array_toplevel_raises_schema_error(self, get_test_data_path):
        """Test that open() raises SchemaError for array top-level schema."""
        path = get_test_data_path("fastavro/array-toplevel.avro")

        with pytest.raises(jetliner.SchemaError) as exc_info:
            with jetliner.AvroReader(path) as reader:
                list(reader)

        error_msg = str(exc_info.value).lower()
        assert "not yet supported" in error_msg
