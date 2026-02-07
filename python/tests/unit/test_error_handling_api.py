"""
Unit tests for error handling API and exception types.

Tests cover:
- File not found error handling - Requirements 6.4, 6.5
- Invalid Avro file error handling
- Exception type hierarchy
- Custom exception classes with structured metadata
"""

import os
import tempfile

import pytest

import jetliner


class TestErrorHandling:
    """Tests for error handling and exception types."""

    def test_file_not_found_error(self):
        """Test that FileNotFoundError is raised for missing files."""
        with pytest.raises(FileNotFoundError):
            jetliner.open("/nonexistent/path/to/file.avro")

    def test_invalid_avro_file(self):
        """Test that ParseError is raised for invalid Avro files."""
        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            f.write(b"This is not an Avro file")
            temp_path = f.name

        try:
            with pytest.raises(jetliner.ParseError):
                jetliner.open(temp_path)
        finally:
            os.unlink(temp_path)

    def test_exception_types_exist(self):
        """Test that all expected exception types are defined."""
        assert hasattr(jetliner, "JetlinerError")
        assert hasattr(jetliner, "ParseError")
        assert hasattr(jetliner, "SchemaError")
        assert hasattr(jetliner, "CodecError")
        assert hasattr(jetliner, "DecodeError")
        assert hasattr(jetliner, "SourceError")

    def test_exception_hierarchy(self):
        """Test that custom exceptions inherit from JetlinerError."""
        assert issubclass(jetliner.ParseError, jetliner.JetlinerError)
        assert issubclass(jetliner.SchemaError, jetliner.JetlinerError)
        assert issubclass(jetliner.CodecError, jetliner.JetlinerError)
        assert issubclass(jetliner.DecodeError, jetliner.JetlinerError)
        assert issubclass(jetliner.SourceError, jetliner.JetlinerError)
