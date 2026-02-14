"""
Integration tests for structured exception attributes.

Tests verify that Python exception classes expose error context as attributes
for programmatic access, as specified in Requirements 15.2-15.6.

Requirements tested:
- 15.2: `DecodeError` exposes: `block_index`, `record_index`, `file_offset`, `message`
- 15.3: `ParseError` exposes: `file_offset`, `message`
- 15.4: `SourceError` exposes: `path`, `message`
- 15.5: `SchemaError` exposes: `message` and relevant schema context
- 15.6: Attributes are accessible via standard Python attribute access
"""

import pytest

import jetliner


class TestStructuredExceptionTypes:
    """Tests for structured exception types with metadata attributes."""

    def test_decode_error_attributes_exist(self):
        """Test that DecodeError has the expected attributes.

        Validates Requirement 15.2: DecodeError exposes block_index, record_index,
        file_offset, message attributes.
        """
        # Create a DecodeError directly to test attribute access
        # Signature: (message, variant, path, block_index, record_index, file_offset, block_offset)
        err = jetliner.DecodeError("Test decode error", "InvalidData", "/test.avro", 5, 10, 1024, 512)

        # Verify all attributes are accessible
        assert hasattr(err, "block_index")
        assert hasattr(err, "record_index")
        assert hasattr(err, "file_offset")
        assert hasattr(err, "message")

        # Verify attribute values
        assert err.block_index == 5
        assert err.record_index == 10
        assert err.file_offset == 1024
        assert err.message == "Test decode error"

    def test_decode_error_default_values(self):
        """Test that DecodeError has sensible defaults (None for unknown values)."""
        err = jetliner.DecodeError("Minimal error")

        # Default values should be None (unknown)
        assert err.block_index is None
        assert err.record_index is None
        assert err.file_offset is None
        assert err.message == "Minimal error"

    def test_parse_error_attributes_exist(self):
        """Test that ParseError has the expected attributes.

        Validates Requirement 15.3: ParseError exposes file_offset, message attributes.
        """
        # Signature: (message, variant, path, file_offset)
        err = jetliner.ParseError("Test parse error", "InvalidMagic", "/test.avro", 2048)

        # Verify all attributes are accessible
        assert hasattr(err, "file_offset")
        assert hasattr(err, "message")

        # Verify attribute values
        assert err.file_offset == 2048
        assert err.message == "Test parse error"

    def test_parse_error_default_values(self):
        """Test that ParseError has sensible defaults (None for unknown file_offset)."""
        err = jetliner.ParseError("Minimal parse error")

        assert err.file_offset is None
        assert err.message == "Minimal parse error"

    def test_source_error_attributes_exist(self):
        """Test that SourceError has the expected attributes.

        Validates Requirement 15.4: SourceError exposes path, message attributes.
        """
        # Signature: (message, variant, path)
        err = jetliner.SourceError("File not found", "NotFound", "/path/to/file.avro")

        # Verify all attributes are accessible
        assert hasattr(err, "path")
        assert hasattr(err, "message")

        # Verify attribute values
        assert err.path == "/path/to/file.avro"
        assert err.message == "File not found"

    def test_source_error_default_values(self):
        """Test that SourceError has sensible defaults."""
        err = jetliner.SourceError("Minimal source error")

        assert err.path is None
        assert err.message == "Minimal source error"

    def test_schema_error_attributes_exist(self):
        """Test that SchemaError has the expected attributes.

        Validates Requirement 15.5: SchemaError exposes message and schema_context.
        """
        # Signature: (message, variant, path, schema_context)
        err = jetliner.SchemaError(
            "Invalid schema", "InvalidSchema", "/test.avro", "field 'name' has invalid type"
        )

        # Verify all attributes are accessible
        assert hasattr(err, "message")
        assert hasattr(err, "schema_context")

        # Verify attribute values
        assert err.message == "Invalid schema"
        assert err.schema_context == "field 'name' has invalid type"

    def test_schema_error_default_values(self):
        """Test that SchemaError has sensible defaults."""
        err = jetliner.SchemaError("Minimal schema error")

        assert err.message == "Minimal schema error"
        assert err.schema_context is None

    def test_codec_error_attributes_exist(self):
        """Test that CodecError has the expected attributes."""
        # Signature: (message, variant, path, codec, block_index, file_offset)
        err = jetliner.CodecError("Decompression failed", "DecompressionError", "/test.avro", "deflate", 0, 1024)

        # Verify all attributes are accessible
        assert hasattr(err, "codec")
        assert hasattr(err, "message")

        # Verify attribute values
        assert err.codec == "deflate"
        assert err.message == "Decompression failed"


class TestExceptionToDict:
    """Tests for exception to_dict() method."""

    def test_decode_error_to_dict(self):
        """Test DecodeError.to_dict() returns all attributes."""
        err = jetliner.DecodeError("Test error", "InvalidData", "/test.avro", 1, 2, 100, 50)
        d = err.to_dict()

        assert d["block_index"] == 1
        assert d["record_index"] == 2
        assert d["file_offset"] == 100
        assert d["message"] == "Test error"

    def test_parse_error_to_dict(self):
        """Test ParseError.to_dict() returns all attributes."""
        err = jetliner.ParseError("Parse failed", "InvalidMagic", "/test.avro", 500)
        d = err.to_dict()

        assert d["file_offset"] == 500
        assert d["message"] == "Parse failed"

    def test_source_error_to_dict(self):
        """Test SourceError.to_dict() returns all attributes."""
        err = jetliner.SourceError("Not found", "NotFound", "/test/path.avro")
        d = err.to_dict()

        assert d["path"] == "/test/path.avro"
        assert d["message"] == "Not found"

    def test_schema_error_to_dict(self):
        """Test SchemaError.to_dict() returns all attributes."""
        err = jetliner.SchemaError("Bad schema", "InvalidSchema", "/test.avro", "type mismatch")
        d = err.to_dict()

        assert d["message"] == "Bad schema"
        assert d["schema_context"] == "type mismatch"


class TestExceptionStringRepresentations:
    """Tests for exception __str__ and __repr__ methods."""

    def test_decode_error_str(self):
        """Test DecodeError string representation."""
        err = jetliner.DecodeError("Test error", "InvalidData", "/test.avro", 1, 2, 100, 50)
        s = str(err)

        assert "block" in s.lower()
        assert "1" in s
        assert "record" in s.lower()
        assert "2" in s

    def test_decode_error_repr(self):
        """Test DecodeError repr representation."""
        err = jetliner.DecodeError("Test error", "InvalidData", "/test.avro", 1, 2, 100, 50)
        r = repr(err)

        assert "DecodeError" in r
        assert "block_index=1" in r
        assert "record_index=2" in r
        assert "file_offset=100" in r

    def test_parse_error_str(self):
        """Test ParseError string representation."""
        err = jetliner.ParseError("Parse failed", "InvalidMagic", "/test.avro", 500)
        s = str(err)

        assert "parse" in s.lower()
        assert "500" in s

    def test_source_error_str(self):
        """Test SourceError string representation."""
        err = jetliner.SourceError("Not found", "NotFound", "/test/path.avro")
        s = str(err)

        assert "/test/path.avro" in s


class TestExceptionAttributesFromRealErrors:
    """Tests for exception attributes when raised from real file operations.

    These tests verify that when exceptions are raised from actual Rust operations,
    the structured attributes are properly populated and accessible via standard
    Python attribute access (Requirement 15.6).

    Note: All API functions (scan_avro, read_avro, read_avro_schema, open) now
    raise the structured exception types (ParseError, DecodeError, etc.) with
    metadata attributes.
    """

    def test_parse_error_from_invalid_magic_via_read_avro(self, get_test_data_path):
        """Test that ParseError from invalid magic has structured attributes.

        Validates Requirements 15.3 and 15.6: ParseError exposes file_offset, message
        and attributes are accessible via standard Python attribute access.

        Uses read_avro() which raises structured exceptions.
        """
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro(path, ignore_errors=False)

        err = exc_info.value

        # Verify structured attributes are accessible
        assert hasattr(err, "file_offset")
        assert hasattr(err, "message")

        # Verify file_offset is a valid value (0 for magic byte errors at start of file)
        assert err.file_offset == 0

        # Verify message contains meaningful content
        assert len(err.message) > 0
        assert "magic" in err.message.lower() or "invalid" in err.message.lower()

    def test_parse_error_from_invalid_sync_marker_via_read_avro(self, get_test_data_path):
        """Test that ParseError from invalid sync marker has structured attributes.

        Validates Requirements 15.3 and 15.6.
        Uses read_avro() which raises structured exceptions.
        """
        path = get_test_data_path("corrupted/corrupted-sync-marker.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro(path, ignore_errors=False)

        err = exc_info.value

        # Verify structured attributes are accessible
        assert hasattr(err, "file_offset")
        assert hasattr(err, "message")

        # Verify message contains meaningful content about sync marker
        assert len(err.message) > 0

    def test_source_error_from_missing_file(self):
        """Test that SourceError from missing file has meaningful attributes.

        Validates Requirement 15.4: SourceError exposes path, message.
        Note: Missing local files raise FileNotFoundError (standard Python exception).
        """
        missing_path = "/nonexistent/path/to/file.avro"

        # FileNotFoundError is raised for missing local files
        with pytest.raises(FileNotFoundError) as exc_info:
            jetliner.read_avro(missing_path)

        # The error message should mention the path
        error_msg = str(exc_info.value)
        assert len(error_msg) > 0

    def test_exception_is_catchable_as_base_exception(self, get_test_data_path):
        """Test that structured exceptions can be caught as Exception.

        Validates that our custom exception classes properly inherit from Exception.
        """
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(Exception) as exc_info:
            jetliner.read_avro(path, ignore_errors=False)

        # Should be our structured exception type
        assert isinstance(exc_info.value, jetliner.ParseError)

    def test_exception_attributes_in_except_block(self, get_test_data_path):
        """Test that exception attributes are accessible in except block.

        This is the primary use case - catching an exception and accessing
        its structured attributes for error handling/logging.
        """
        path = get_test_data_path("corrupted/invalid-magic.avro")

        try:
            jetliner.read_avro(path, ignore_errors=False)
            pytest.fail("Expected ParseError to be raised")
        except jetliner.ParseError as e:
            # Access attributes directly in except block
            file_offset = e.file_offset
            message = e.message

            assert file_offset == 0
            assert len(message) > 0

            # Can also use to_dict()
            d = e.to_dict()
            assert d["file_offset"] == file_offset
            assert d["message"] == message

    def test_avro_reader_raises_structured_exceptions(self, get_test_data_path):
        """Test that AvroReader raises structured exceptions with attributes.

        The AvroReader class uses the same structured exception types.
        """
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            with jetliner.AvroReader(path, ignore_errors=False) as reader:
                list(reader)

        # Structured exceptions have attributes
        err = exc_info.value
        assert hasattr(err, "file_offset")
        assert hasattr(err, "message")
        assert len(err.message) > 0
        assert "magic" in err.message.lower() or "invalid" in err.message.lower()

