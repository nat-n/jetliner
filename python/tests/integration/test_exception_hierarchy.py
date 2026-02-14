"""
Integration tests for the enhanced exception hierarchy.

Tests verify:
1. All exceptions have the `variant` attribute
2. All exceptions have the `path` attribute where applicable
3. Dual inheritance works (e.g., FileNotFoundError is both JetlinerError AND builtins.FileNotFoundError)
4. New exception types have correct attributes
5. Exception attributes are correctly populated from Rust errors

Requirements:
- Issue #3: AuthenticationFailed -> AuthenticationError (not PermissionError)
- Issue #4: Dual inheritance from JetlinerError AND builtins
- Issue #7: Renamed offsets (file_offset, block_offset)
"""

import builtins

import pytest

import jetliner


class TestVariantAttribute:
    """Tests for the `variant` attribute on all exception types."""

    def test_decode_error_has_variant_attribute(self):
        """DecodeError should have a variant attribute."""
        err = jetliner.DecodeError("test", "InvalidData")
        assert hasattr(err, "variant")
        assert err.variant == "InvalidData"

    def test_decode_error_default_variant(self):
        """DecodeError should have default variant 'Decode'."""
        err = jetliner.DecodeError("test")
        assert err.variant == "Decode"

    def test_parse_error_has_variant_attribute(self):
        """ParseError should have a variant attribute."""
        err = jetliner.ParseError("test", "InvalidMagic")
        assert hasattr(err, "variant")
        assert err.variant == "InvalidMagic"

    def test_parse_error_default_variant(self):
        """ParseError should have default variant 'Parse'."""
        err = jetliner.ParseError("test")
        assert err.variant == "Parse"

    def test_source_error_has_variant_attribute(self):
        """SourceError should have a variant attribute."""
        err = jetliner.SourceError("test", "S3Error")
        assert hasattr(err, "variant")
        assert err.variant == "S3Error"

    def test_schema_error_has_variant_attribute(self):
        """SchemaError should have a variant attribute."""
        err = jetliner.SchemaError("test", "UnsupportedType")
        assert hasattr(err, "variant")
        assert err.variant == "UnsupportedType"

    def test_codec_error_has_variant_attribute(self):
        """CodecError should have a variant attribute."""
        err = jetliner.CodecError("test", "DecompressionError")
        assert hasattr(err, "variant")
        assert err.variant == "DecompressionError"

    def test_authentication_error_has_variant_attribute(self):
        """AuthenticationError should have a variant attribute."""
        err = jetliner.AuthenticationError("test", "AuthenticationFailed")
        assert hasattr(err, "variant")
        assert err.variant == "AuthenticationFailed"

    def test_file_not_found_error_has_variant_attribute(self):
        """FileNotFoundError should have a variant attribute."""
        err = jetliner.FileNotFoundError("test", "NotFound")
        assert hasattr(err, "variant")
        assert err.variant == "NotFound"

    def test_permission_error_has_variant_attribute(self):
        """PermissionError should have a variant attribute."""
        err = jetliner.PermissionError("test", "PermissionDenied")
        assert hasattr(err, "variant")
        assert err.variant == "PermissionDenied"

    def test_configuration_error_has_variant_attribute(self):
        """ConfigurationError should have a variant attribute."""
        err = jetliner.ConfigurationError("test", "Configuration")
        assert hasattr(err, "variant")
        assert err.variant == "Configuration"

    def test_jetliner_error_has_variant_attribute(self):
        """Base JetlinerError should have a variant attribute."""
        err = jetliner.JetlinerError("test", "Custom")
        assert hasattr(err, "variant")
        assert err.variant == "Custom"


class TestPathAttribute:
    """Tests for the `path` attribute on exceptions."""

    def test_decode_error_has_path_attribute(self):
        """DecodeError should have a path attribute."""
        err = jetliner.DecodeError("test", "InvalidData", "/path/to/file.avro")
        assert hasattr(err, "path")
        assert err.path == "/path/to/file.avro"

    def test_decode_error_path_default_is_none(self):
        """DecodeError path should default to None."""
        err = jetliner.DecodeError("test")
        assert err.path is None

    def test_parse_error_has_path_attribute(self):
        """ParseError should have a path attribute."""
        err = jetliner.ParseError("test", "Parse", "/path/to/file.avro")
        assert hasattr(err, "path")
        assert err.path == "/path/to/file.avro"

    def test_source_error_has_path_attribute(self):
        """SourceError should have a path attribute."""
        err = jetliner.SourceError("test", "Source", "/path/to/file.avro")
        assert hasattr(err, "path")
        assert err.path == "/path/to/file.avro"

    def test_schema_error_has_path_attribute(self):
        """SchemaError should have a path attribute."""
        err = jetliner.SchemaError("test", "Schema", "/path/to/file.avro")
        assert hasattr(err, "path")
        assert err.path == "/path/to/file.avro"

    def test_codec_error_has_path_attribute(self):
        """CodecError should have a path attribute."""
        err = jetliner.CodecError("test", "Codec", "/path/to/file.avro")
        assert hasattr(err, "path")
        assert err.path == "/path/to/file.avro"

    def test_authentication_error_has_path_attribute(self):
        """AuthenticationError should have a path attribute."""
        err = jetliner.AuthenticationError("test", "AuthenticationFailed", "s3://bucket/key")
        assert hasattr(err, "path")
        assert err.path == "s3://bucket/key"

    def test_file_not_found_error_has_path_attribute(self):
        """FileNotFoundError should have a path attribute."""
        err = jetliner.FileNotFoundError("test", "NotFound", "/missing/file.avro")
        assert hasattr(err, "path")
        assert err.path == "/missing/file.avro"

    def test_permission_error_has_path_attribute(self):
        """PermissionError should have a path attribute."""
        err = jetliner.PermissionError("test", "PermissionDenied", "s3://bucket/key")
        assert hasattr(err, "path")
        assert err.path == "s3://bucket/key"


class TestRenamedOffsets:
    """Tests for renamed offset attributes (file_offset, block_offset)."""

    def test_decode_error_has_file_offset(self):
        """DecodeError should have file_offset attribute."""
        err = jetliner.DecodeError(
            "test", "InvalidData", "/path",
            block_index=1, record_index=2, file_offset=1024, block_offset=128
        )
        assert hasattr(err, "file_offset")
        assert err.file_offset == 1024

    def test_decode_error_has_block_offset(self):
        """DecodeError should have block_offset attribute."""
        err = jetliner.DecodeError(
            "test", "InvalidData", "/path",
            block_index=1, record_index=2, file_offset=1024, block_offset=128
        )
        assert hasattr(err, "block_offset")
        assert err.block_offset == 128

    def test_decode_error_offset_defaults(self):
        """DecodeError offsets should default to None (unknown)."""
        err = jetliner.DecodeError("test")
        assert err.file_offset is None
        assert err.block_offset is None

    def test_parse_error_has_file_offset(self):
        """ParseError should have file_offset attribute."""
        err = jetliner.ParseError("test", "Parse", "/path", file_offset=512)
        assert hasattr(err, "file_offset")
        assert err.file_offset == 512

    def test_parse_error_file_offset_default(self):
        """ParseError file_offset should default to None (unknown)."""
        err = jetliner.ParseError("test")
        assert err.file_offset is None

    def test_codec_error_has_file_offset(self):
        """CodecError should have file_offset attribute."""
        err = jetliner.CodecError(
            "test", "Codec", "/path", codec="deflate",
            block_index=3, file_offset=4096
        )
        assert hasattr(err, "file_offset")
        assert err.file_offset == 4096


class TestDualInheritance:
    """Tests for dual inheritance (JetlinerError AND builtin)."""

    def test_file_not_found_is_jetliner_error(self):
        """FileNotFoundError should be a JetlinerError."""
        err = jetliner.FileNotFoundError("test", "NotFound", "/path")
        assert isinstance(err, jetliner.JetlinerError)

    def test_file_not_found_is_builtin_file_not_found(self):
        """FileNotFoundError should be a builtins.FileNotFoundError."""
        err = jetliner.FileNotFoundError("test", "NotFound", "/path")
        assert isinstance(err, builtins.FileNotFoundError)

    def test_file_not_found_catchable_as_builtin(self):
        """FileNotFoundError should be catchable with bare FileNotFoundError."""
        try:
            raise jetliner.FileNotFoundError("test", "NotFound", "/path")
        except FileNotFoundError as e:
            assert e.variant == "NotFound"
            assert e.path == "/path"

    def test_file_not_found_has_builtin_attributes(self):
        """FileNotFoundError should have OSError attributes (errno, filename)."""
        import errno

        err = jetliner.FileNotFoundError("test", "NotFound", "/path/to/file.avro")
        # OSError attributes
        assert err.errno == errno.ENOENT, f"Expected errno {errno.ENOENT}, got {err.errno}"
        assert err.filename == "/path/to/file.avro"
        # Also accessible via strerror
        assert err.strerror is not None

    def test_permission_error_is_jetliner_error(self):
        """PermissionError should be a JetlinerError."""
        err = jetliner.PermissionError("test", "PermissionDenied", "/path")
        assert isinstance(err, jetliner.JetlinerError)

    def test_permission_error_is_builtin_permission_error(self):
        """PermissionError should be a builtins.PermissionError."""
        err = jetliner.PermissionError("test", "PermissionDenied", "/path")
        assert isinstance(err, builtins.PermissionError)

    def test_permission_error_catchable_as_builtin(self):
        """PermissionError should be catchable with bare PermissionError."""
        try:
            raise jetliner.PermissionError("test", "PermissionDenied", "/path")
        except PermissionError as e:
            assert e.variant == "PermissionDenied"
            assert e.path == "/path"

    def test_permission_error_has_builtin_attributes(self):
        """PermissionError should have OSError attributes (errno, filename)."""
        import errno

        err = jetliner.PermissionError("test", "PermissionDenied", "/restricted/path.avro")
        # OSError attributes
        assert err.errno == errno.EACCES, f"Expected errno {errno.EACCES}, got {err.errno}"
        assert err.filename == "/restricted/path.avro"
        # Also accessible via strerror
        assert err.strerror is not None

    def test_configuration_error_is_jetliner_error(self):
        """ConfigurationError should be a JetlinerError."""
        err = jetliner.ConfigurationError("test", "Configuration")
        assert isinstance(err, jetliner.JetlinerError)

    def test_configuration_error_is_builtin_value_error(self):
        """ConfigurationError should be a builtins.ValueError."""
        err = jetliner.ConfigurationError("test", "Configuration")
        assert isinstance(err, builtins.ValueError)

    def test_configuration_error_catchable_as_value_error(self):
        """ConfigurationError should be catchable with bare ValueError."""
        try:
            raise jetliner.ConfigurationError("invalid batch_size", "Configuration")
        except ValueError as e:
            assert e.variant == "Configuration"


class TestAuthenticationError:
    """Tests specific to AuthenticationError (Issue #3)."""

    def test_authentication_error_exists(self):
        """AuthenticationError should be importable from jetliner."""
        assert hasattr(jetliner, "AuthenticationError")

    def test_authentication_error_is_jetliner_error(self):
        """AuthenticationError should inherit from JetlinerError."""
        err = jetliner.AuthenticationError("test")
        assert isinstance(err, jetliner.JetlinerError)

    def test_authentication_error_is_not_permission_error(self):
        """AuthenticationError should NOT be a PermissionError.

        Issue #3: AuthenticationFailed should map to a dedicated exception,
        not PermissionError, because they are semantically different.
        """
        err = jetliner.AuthenticationError("test")
        # AuthenticationError is distinct from PermissionError
        assert not isinstance(err, builtins.PermissionError)

    def test_authentication_error_attributes(self):
        """AuthenticationError should have all expected attributes."""
        err = jetliner.AuthenticationError(
            "Invalid credentials",
            "AuthenticationFailed",
            "s3://bucket/key"
        )
        assert err.message == "Invalid credentials"
        assert err.variant == "AuthenticationFailed"
        assert err.path == "s3://bucket/key"

    def test_authentication_error_to_dict(self):
        """AuthenticationError.to_dict() should return all attributes."""
        err = jetliner.AuthenticationError("test", "AuthenticationFailed", "s3://bucket/key")
        d = err.to_dict()
        assert d["message"] == "test"
        assert d["variant"] == "AuthenticationFailed"
        assert d["path"] == "s3://bucket/key"


class TestConfigurationError:
    """Tests specific to ConfigurationError."""

    def test_configuration_error_exists(self):
        """ConfigurationError should be importable from jetliner."""
        assert hasattr(jetliner, "ConfigurationError")

    def test_configuration_error_attributes(self):
        """ConfigurationError should have all expected attributes."""
        err = jetliner.ConfigurationError("Invalid batch_size: -1", "Configuration")
        assert err.message == "Invalid batch_size: -1"
        assert err.variant == "Configuration"

    def test_configuration_error_to_dict(self):
        """ConfigurationError.to_dict() should return all attributes."""
        err = jetliner.ConfigurationError("test", "Configuration")
        d = err.to_dict()
        assert d["message"] == "test"
        assert d["variant"] == "Configuration"


class TestToDictWithNewAttributes:
    """Tests for to_dict() method with new attributes."""

    def test_decode_error_to_dict_has_variant(self):
        """DecodeError.to_dict() should include variant."""
        err = jetliner.DecodeError("test", "InvalidUtf8", "/path", 1, 2, 1024, 128)
        d = err.to_dict()
        assert "variant" in d
        assert d["variant"] == "InvalidUtf8"

    def test_decode_error_to_dict_has_path(self):
        """DecodeError.to_dict() should include path."""
        err = jetliner.DecodeError("test", "InvalidData", "/path/to/file.avro")
        d = err.to_dict()
        assert "path" in d
        assert d["path"] == "/path/to/file.avro"

    def test_decode_error_to_dict_has_both_offsets(self):
        """DecodeError.to_dict() should include file_offset and block_offset."""
        err = jetliner.DecodeError("test", "Decode", "/path", 1, 2, 1024, 128)
        d = err.to_dict()
        assert "file_offset" in d
        assert "block_offset" in d
        assert d["file_offset"] == 1024
        assert d["block_offset"] == 128

    def test_parse_error_to_dict_has_variant_and_path(self):
        """ParseError.to_dict() should include variant and path."""
        err = jetliner.ParseError("test", "InvalidMagic", "/path/to/file.avro", 512)
        d = err.to_dict()
        assert d["variant"] == "InvalidMagic"
        assert d["path"] == "/path/to/file.avro"
        assert d["file_offset"] == 512

    def test_source_error_to_dict_has_variant(self):
        """SourceError.to_dict() should include variant."""
        err = jetliner.SourceError("test", "S3Error", "/path")
        d = err.to_dict()
        assert d["variant"] == "S3Error"

    def test_schema_error_to_dict_has_variant_and_path(self):
        """SchemaError.to_dict() should include variant and path."""
        err = jetliner.SchemaError("test", "UnsupportedType", "/path", "enum")
        d = err.to_dict()
        assert d["variant"] == "UnsupportedType"
        assert d["path"] == "/path"
        assert d["schema_context"] == "enum"

    def test_codec_error_to_dict_has_all_fields(self):
        """CodecError.to_dict() should include all new fields."""
        err = jetliner.CodecError("test", "DecompressionError", "/path", "zstd", 5, 8192)
        d = err.to_dict()
        assert d["variant"] == "DecompressionError"
        assert d["path"] == "/path"
        assert d["codec"] == "zstd"
        assert d["block_index"] == 5
        assert d["file_offset"] == 8192

    def test_file_not_found_to_dict(self):
        """FileNotFoundError.to_dict() should include all attributes."""
        err = jetliner.FileNotFoundError("File not found: /path", "NotFound", "/path")
        d = err.to_dict()
        assert d["message"] == "File not found: /path"
        assert d["variant"] == "NotFound"
        assert d["path"] == "/path"

    def test_permission_error_to_dict(self):
        """PermissionError.to_dict() should include all attributes."""
        err = jetliner.PermissionError("Access denied", "PermissionDenied", "s3://bucket/key")
        d = err.to_dict()
        assert d["message"] == "Access denied"
        assert d["variant"] == "PermissionDenied"
        assert d["path"] == "s3://bucket/key"


class TestExceptionInheritanceHierarchy:
    """Tests for complete exception inheritance hierarchy."""

    def test_all_exceptions_inherit_from_jetliner_error(self):
        """All Jetliner exceptions should inherit from JetlinerError."""
        assert issubclass(jetliner.DecodeError, jetliner.JetlinerError)
        assert issubclass(jetliner.ParseError, jetliner.JetlinerError)
        assert issubclass(jetliner.SourceError, jetliner.JetlinerError)
        assert issubclass(jetliner.SchemaError, jetliner.JetlinerError)
        assert issubclass(jetliner.CodecError, jetliner.JetlinerError)
        assert issubclass(jetliner.AuthenticationError, jetliner.JetlinerError)
        assert issubclass(jetliner.FileNotFoundError, jetliner.JetlinerError)
        assert issubclass(jetliner.PermissionError, jetliner.JetlinerError)
        assert issubclass(jetliner.ConfigurationError, jetliner.JetlinerError)

    def test_jetliner_error_inherits_from_exception(self):
        """JetlinerError should inherit from Exception."""
        assert issubclass(jetliner.JetlinerError, Exception)

    def test_all_exceptions_catchable_as_jetliner_error(self):
        """All Jetliner exceptions should be catchable as JetlinerError."""
        exceptions = [
            jetliner.DecodeError("test"),
            jetliner.ParseError("test"),
            jetliner.SourceError("test"),
            jetliner.SchemaError("test"),
            jetliner.CodecError("test"),
            jetliner.AuthenticationError("test"),
            jetliner.FileNotFoundError("test"),
            jetliner.PermissionError("test"),
            jetliner.ConfigurationError("test"),
        ]

        for exc in exceptions:
            try:
                raise exc
            except jetliner.JetlinerError as e:
                assert hasattr(e, "message")
                assert hasattr(e, "variant")

    def test_dual_inheritance_types(self):
        """Verify which exceptions have dual inheritance."""
        # These should have dual inheritance
        assert issubclass(jetliner.FileNotFoundError, builtins.FileNotFoundError)
        assert issubclass(jetliner.PermissionError, builtins.PermissionError)
        assert issubclass(jetliner.ConfigurationError, builtins.ValueError)

        # These should NOT have dual inheritance with builtins
        assert not issubclass(jetliner.AuthenticationError, builtins.PermissionError)
        assert not issubclass(jetliner.DecodeError, builtins.ValueError)


class TestExceptionFromRealOperations:
    """Tests for exception attributes when raised from real Rust operations."""

    def test_file_not_found_from_read_avro_has_attributes(self):
        """FileNotFoundError from read_avro should have variant and path."""
        nonexistent = "/nonexistent/path/to/file.avro"

        with pytest.raises(jetliner.FileNotFoundError) as exc_info:
            jetliner.read_avro(nonexistent)

        err = exc_info.value
        # Should have all the new attributes
        assert hasattr(err, "variant")
        assert hasattr(err, "path")
        assert err.variant == "NotFound"
        # Path should be set
        assert err.path is not None

    def test_file_not_found_from_read_avro_catchable_as_builtin(self):
        """FileNotFoundError from read_avro should be catchable as builtin."""
        nonexistent = "/nonexistent/path/to/file.avro"

        # Should be catchable with bare FileNotFoundError
        with pytest.raises(FileNotFoundError):
            jetliner.read_avro(nonexistent)

    def test_parse_error_from_invalid_magic_has_variant(self, get_test_data_path):
        """ParseError from invalid magic should have correct variant."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert hasattr(err, "variant")
        # Variant should be set (e.g., "InvalidMagic" or similar)
        assert err.variant is not None
        assert len(err.variant) > 0

    def test_parse_error_from_invalid_magic_has_path(self, get_test_data_path):
        """ParseError from invalid magic should have path attribute."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert hasattr(err, "path")
        # Path should be set to the file path
        assert err.path is not None

    def test_decode_error_from_corrupted_data_has_new_attributes(self, get_test_data_path):
        """DecodeError from corrupted data should have file_offset and block_offset."""
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises(jetliner.DecodeError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        # Should have the new offset attributes
        assert hasattr(err, "file_offset")
        assert hasattr(err, "block_offset")
        assert hasattr(err, "variant")
        assert hasattr(err, "path")

    def test_codec_error_from_corrupted_compressed_has_variant(self, get_test_data_path):
        """CodecError from corrupted compressed data should have variant."""
        path = get_test_data_path("corrupted/corrupted-compressed.avro")

        with pytest.raises(jetliner.CodecError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert hasattr(err, "variant")
        # Variant should be set (e.g., "DecompressionError")
        assert err.variant is not None


class TestPathAttributeFromRealOperations:
    """Tests verifying `path` attribute is consistently populated from real operations.

    This addresses review item 6.2: Ensure filepath is populated correctly across all
    error types when reading files, not just checking that the attribute exists.
    """

    def test_file_not_found_path_matches_input(self, tmp_path):
        """FileNotFoundError.path should match the requested file path."""
        # Use an absolute path to ensure consistent comparison
        nonexistent = str(tmp_path / "does_not_exist.avro")

        with pytest.raises(jetliner.FileNotFoundError) as exc_info:
            jetliner.read_avro(nonexistent)

        err = exc_info.value
        assert err.path == nonexistent, f"Expected path '{nonexistent}', got '{err.path}'"

    def test_parse_error_path_matches_input(self, get_test_data_path):
        """ParseError.path should match the file that caused the error."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert err.path == path, f"Expected path '{path}', got '{err.path}'"

    def test_decode_error_path_matches_input(self, get_test_data_path):
        """DecodeError.path should match the file that caused the error."""
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises(jetliner.DecodeError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert err.path == path, f"Expected path '{path}', got '{err.path}'"

    def test_codec_error_path_matches_input(self, get_test_data_path):
        """CodecError.path should match the file that caused the error."""
        path = get_test_data_path("corrupted/corrupted-compressed.avro")

        with pytest.raises(jetliner.CodecError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert err.path == path, f"Expected path '{path}', got '{err.path}'"

    def test_scan_avro_file_not_found_path_matches_input(self, tmp_path):
        """FileNotFoundError from scan_avro should have correct path."""
        nonexistent = str(tmp_path / "scan_does_not_exist.avro")

        with pytest.raises(jetliner.FileNotFoundError) as exc_info:
            jetliner.scan_avro(nonexistent)

        err = exc_info.value
        assert err.path == nonexistent, f"Expected path '{nonexistent}', got '{err.path}'"

    def test_scan_avro_parse_error_path_matches_input(self, get_test_data_path):
        """ParseError from scan_avro().collect() should have correct path."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.scan_avro(path).collect()

        err = exc_info.value
        assert err.path == path, f"Expected path '{path}', got '{err.path}'"

    def test_avro_reader_file_not_found_path_matches_input(self, tmp_path):
        """FileNotFoundError from AvroReader should have correct path."""
        nonexistent = str(tmp_path / "open_does_not_exist.avro")

        with pytest.raises(jetliner.FileNotFoundError) as exc_info:
            with jetliner.AvroReader(nonexistent) as reader:
                list(reader)

        err = exc_info.value
        assert err.path == nonexistent, f"Expected path '{nonexistent}', got '{err.path}'"

    def test_avro_reader_parse_error_path_matches_input(self, get_test_data_path):
        """ParseError from AvroReader iterator should have correct path."""
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            with jetliner.AvroReader(path) as reader:
                list(reader)

        err = exc_info.value
        assert err.path == path, f"Expected path '{path}', got '{err.path}'"

    def test_multi_file_error_identifies_correct_file(self, tmp_path, get_test_data_path):
        """When reading multiple files, error should identify the problematic file."""
        import shutil

        # Create a valid file
        valid_path = get_test_data_path("apache-avro/weather.avro")
        valid_copy = tmp_path / "valid.avro"
        shutil.copy(valid_path, valid_copy)

        # Get path to corrupted file
        corrupted_path = get_test_data_path("corrupted/invalid-magic.avro")

        # Put valid file first, corrupted second - order should be preserved
        input_paths = [str(valid_copy), corrupted_path]

        # Read both files - should fail on the corrupted one (second file)
        with pytest.raises(jetliner.ParseError) as exc_info:
            jetliner.read_avro(input_paths)

        err = exc_info.value
        # The error path should be the corrupted file, not the valid one
        assert err.path == corrupted_path, (
            f"Error path should identify corrupted file '{corrupted_path}', "
            f"got '{err.path}'"
        )

    def test_schema_error_path_matches_input(self, tmp_path):
        """SchemaError.path should match the file that caused the error."""
        import fastavro

        # Create a file with a zero-field record schema (unsupported by Polars)
        schema = {
            "type": "record",
            "name": "EmptyRecord",
            "fields": [],
        }
        file_path = tmp_path / "zero_fields.avro"
        with open(file_path, "wb") as f:
            fastavro.writer(f, schema, [])

        path = str(file_path)

        with pytest.raises(jetliner.SchemaError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        assert err.path == path, f"Expected path '{path}', got '{err.path}'"


class TestDecodeErrorPositionAttributes:
    """Tests verifying DecodeError includes block_index and record_index.

    This addresses the issue where BuilderError::Decode was not carrying
    positional context (block_index, record_index) when decode errors occurred.
    """

    def test_decode_error_has_block_index_attribute(self, get_test_data_path):
        """DecodeError should have block_index attribute populated."""
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises(jetliner.DecodeError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        # block_index should be a non-negative integer, not None
        assert err.block_index is not None, "block_index should not be None"
        assert isinstance(err.block_index, int), f"block_index should be int, got {type(err.block_index)}"
        assert err.block_index >= 0, f"block_index should be >= 0, got {err.block_index}"

    def test_decode_error_has_record_index_attribute(self, get_test_data_path):
        """DecodeError should have record_index attribute populated when traceable to a record.

        Note: record_index is `None` if the error occurred during batch finalization
        and cannot be attributed to a specific record. For record-level decode errors,
        it should be a non-negative integer.
        """
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises(jetliner.DecodeError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        # For record-level decode errors, record_index should be populated
        # (it may be None for batch finalization errors, but invalid-record-data
        # should trigger a record-level error)
        if err.record_index is not None:
            assert isinstance(err.record_index, int), f"record_index should be int, got {type(err.record_index)}"
            assert err.record_index >= 0, f"record_index should be >= 0, got {err.record_index}"

    def test_decode_error_position_in_error_message(self, get_test_data_path):
        """DecodeError message or repr should include position information."""
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises(jetliner.DecodeError) as exc_info:
            jetliner.read_avro(path)

        err = exc_info.value
        r = repr(err)
        # The repr should include block_index (always) and record_index (when known)
        assert "block_index=" in r, f"repr should include block_index, got: {r}"
        # record_index is in the repr whether it's a value or None
        assert "record_index=" in r, f"repr should include record_index, got: {r}"

    def test_decode_error_from_scan_has_position_in_message(self, get_test_data_path):
        """DecodeError from scan_avro().collect() should have position in message.

        Note: Due to the IO plugin architecture, errors from scan_avro().collect()
        are wrapped in Polars' ComputeError. However, the position information
        should still be present in the error message.
        """
        import polars

        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises(polars.exceptions.ComputeError) as exc_info:
            jetliner.scan_avro(path).collect()

        # The error message should contain position information
        msg = str(exc_info.value)
        assert "block" in msg.lower(), f"Error message should include block info: {msg}"
        assert "record" in msg.lower(), f"Error message should include record info: {msg}"

    def test_decode_error_from_avro_reader_has_position(self, get_test_data_path):
        """DecodeError from AvroReader iterator should have position attributes."""
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises(jetliner.DecodeError) as exc_info:
            with jetliner.AvroReader(path) as reader:
                list(reader)

        err = exc_info.value
        # block_index should always be populated
        assert err.block_index is not None, "block_index should not be None from AvroReader"
        # record_index may be None for batch finalization errors, but for
        # record-level decode errors it should be populated
        if err.record_index is not None:
            assert isinstance(err.record_index, int), f"record_index should be int, got {type(err.record_index)}"


class TestExceptionStringRepresentations:
    """Tests for exception __str__ and __repr__ with new attributes."""

    def test_decode_error_repr_includes_new_fields(self):
        """DecodeError repr should include file_offset and block_offset."""
        err = jetliner.DecodeError("test", "InvalidData", "/path", 1, 2, 1024, 128)
        r = repr(err)

        assert "file_offset=1024" in r
        assert "block_offset=128" in r
        assert "variant='InvalidData'" in r
        assert "path='/path'" in r

    def test_parse_error_repr_includes_new_fields(self):
        """ParseError repr should include variant and path."""
        err = jetliner.ParseError("test", "InvalidMagic", "/path", 512)
        r = repr(err)

        assert "variant='InvalidMagic'" in r
        assert "path='/path'" in r
        assert "file_offset=512" in r

    def test_authentication_error_repr(self):
        """AuthenticationError repr should include all fields."""
        err = jetliner.AuthenticationError("creds invalid", "AuthenticationFailed", "s3://bucket")
        r = repr(err)

        assert "AuthenticationError" in r
        assert "AuthenticationFailed" in r
        assert "s3://bucket" in r

    def test_file_not_found_str_is_message(self):
        """FileNotFoundError str should be the message."""
        err = jetliner.FileNotFoundError("File not found: /path", "NotFound", "/path")
        assert str(err) == "File not found: /path"

    def test_permission_error_str_is_message(self):
        """PermissionError str should be the message."""
        err = jetliner.PermissionError("Access denied", "PermissionDenied", "s3://bucket")
        assert str(err) == "Access denied"

    def test_configuration_error_str_is_message(self):
        """ConfigurationError str should be the message."""
        err = jetliner.ConfigurationError("Invalid batch_size", "Configuration")
        assert str(err) == "Invalid batch_size"
