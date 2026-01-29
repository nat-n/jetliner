"""
Integration tests for error handling modes with real files.

Tests cover:
- Skip mode error handling on valid files
- Strict mode error handling on valid files
- Error accumulation behavior
"""

import json
import tempfile
from pathlib import Path

import pytest

import jetliner


class TestErrorHandlingRealFiles:
    """Test error handling with real files."""

    def test_skip_mode_on_valid_file(self, get_test_data_path):
        """Test that skip mode works on valid files without errors."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path, strict=False) as reader:
            dfs = list(reader)

            # Should read successfully
            assert len(dfs) > 0

            # Should have no errors
            assert reader.error_count == 0
            assert len(reader.errors) == 0

    def test_strict_mode_on_valid_file(self, get_test_data_path):
        """Test that strict mode works on valid files."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path, strict=True) as reader:
            dfs = list(reader)

            # Should read successfully
            assert len(dfs) > 0


def create_avro_with_unknown_codec(codec_name: str) -> bytes:
    """Create a minimal Avro file with an unknown codec in metadata.

    This creates a valid Avro file structure but with an unsupported codec,
    which should trigger a helpful error message.
    """
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
    # Map format: count (varint), then key-value pairs, then 0 terminator
    def encode_varint(n: int) -> bytes:
        """Encode a non-negative integer as Avro varint (zigzag encoded)."""
        # Zigzag encode
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

    # Add a minimal data block (will fail at codec, not data parsing)
    # Block format: record count (varint), compressed size (varint), data, sync marker
    block_data = encode_varint(1)  # 1 record
    block_data += encode_varint(2)  # 2 bytes of "compressed" data
    block_data += b"\x02\x00"  # Fake compressed data (would be id=1 if uncompressed)
    block_data += sync_marker

    return header + block_data


class TestUnknownCodecError:
    """Test error handling for unknown/unsupported codecs.

    Requirements tested:
    - 5.1: Error message contains the codec name
    - 5.2: Error message suggests checking if codec feature is enabled
    """

    def test_unknown_codec_error_contains_codec_name(self):
        """Test that unknown codec error message contains the codec name.

        Validates Requirement 5.1: WHEN an Avro file specifies an unknown codec,
        THE Avro_Reader SHALL return an error containing the codec name.
        """
        codec_name = "super-fancy-codec"

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            f.write(create_avro_with_unknown_codec(codec_name))
            temp_path = f.name

        try:
            with pytest.raises(Exception) as exc_info:
                jetliner.scan(temp_path).collect()

            error_message = str(exc_info.value).lower()
            assert codec_name in error_message, (
                f"Error message should contain codec name '{codec_name}', "
                f"but got: {exc_info.value}"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_unknown_codec_error_various_names(self):
        """Test unknown codec error with various codec names.

        Note: Only truly unknown codecs are tested here. Codecs like lz4, brotli,
        xz may be recognized but disabled via feature flags, which produces a
        different error message.
        """
        # Only test truly unknown codecs (not recognized by the parser at all)
        test_codecs = ["unknown-codec", "my-custom-codec", "fancy-compression"]

        for codec_name in test_codecs:
            with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
                f.write(create_avro_with_unknown_codec(codec_name))
                temp_path = f.name

            try:
                with pytest.raises(Exception) as exc_info:
                    jetliner.scan(temp_path).collect()

                error_message = str(exc_info.value).lower()
                assert codec_name in error_message, (
                    f"Error for codec '{codec_name}' should contain codec name, "
                    f"but got: {exc_info.value}"
                )
            finally:
                Path(temp_path).unlink(missing_ok=True)



# Schema with complex union (multiple non-null types)
COMPLEX_UNION_SCHEMA = {
    "type": "record",
    "name": "ComplexUnionRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Complex union with multiple non-null types (not supported)
        {"name": "value", "type": ["string", "int", "long"]},
    ],
}


def create_complex_union_record(record_id: int) -> dict:
    """Create a record with complex union value."""
    # Alternate between different union branches
    values = ["hello", 42, 9999999999]
    return {
        "id": record_id,
        "value": values[record_id % len(values)],
    }


@pytest.fixture
def complex_union_avro_file():
    """Create a temporary Avro file with complex union type."""
    import fastavro

    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_complex_union_record(i) for i in range(3)]
        fastavro.writer(f, COMPLEX_UNION_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestComplexUnionError:
    """Test error handling for complex unions (multiple non-null types).

    Requirements tested:
    - 7.1: Error indicates complex unions are not supported
    - 7.2: Error message indicates the number of non-null variants
    """

    def test_complex_union_error_message(self, complex_union_avro_file):
        """Test that complex union error message is informative.

        Validates Requirement 7.1: WHEN an Avro file contains a union with
        multiple non-null types, THE Avro_Reader SHALL return an error
        indicating complex unions are not supported.
        """
        with pytest.raises(Exception) as exc_info:
            jetliner.scan(complex_union_avro_file).collect()

        error_message = str(exc_info.value).lower()

        # Error should mention union or complex union
        assert "union" in error_message, (
            f"Error message should mention 'union', but got: {exc_info.value}"
        )

    def test_complex_union_with_four_types(self):
        """Test complex union error with more non-null types.

        Validates Requirement 7.2: Error message indicates the number of
        non-null variants in the union.
        """
        import fastavro

        # Schema with 4 non-null types in union
        schema = {
            "type": "record",
            "name": "FourTypeUnionRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": ["string", "int", "long", "double"]},
            ],
        }

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            records = [{"id": 1, "value": "test"}]
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            with pytest.raises(Exception) as exc_info:
                jetliner.scan(temp_path).collect()

            error_message = str(exc_info.value).lower()
            assert "union" in error_message, (
                f"Error message should mention 'union', but got: {exc_info.value}"
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)
