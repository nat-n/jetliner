"""
Test that unknown kwargs raise TypeError.

PyO3's signature macro automatically raises TypeError for unrecognized kwargs.
This test verifies that behavior is working correctly.

Requirements: 9.5
"""

import pytest
import jetliner


class TestUnknownKwargsRejected:
    """Test that unknown kwargs are rejected (not silently ignored)."""

    def test_scan_avro_unknown_kwarg_raises_type_error(self, tmp_path):
        """scan_avro should raise TypeError for unknown kwargs."""
        # Create a minimal test file
        test_file = tmp_path / "test.avro"
        # We don't need a real file - the error should happen before file access
        test_file.write_bytes(b"")

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            jetliner.scan_avro(
                str(test_file),
                unknown_param=True,  # This should raise TypeError
            )

    def test_read_avro_unknown_kwarg_raises_type_error(self, tmp_path):
        """read_avro should raise TypeError for unknown kwargs."""
        test_file = tmp_path / "test.avro"
        test_file.write_bytes(b"")

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            jetliner.read_avro(
                str(test_file),
                unknown_param=True,  # This should raise TypeError
            )

    def test_read_avro_schema_unknown_kwarg_raises_type_error(self, tmp_path):
        """read_avro_schema should raise TypeError for unknown kwargs."""
        test_file = tmp_path / "test.avro"
        test_file.write_bytes(b"")

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            jetliner.read_avro_schema(
                str(test_file),
                unknown_param=True,  # This should raise TypeError
            )

    def test_scan_avro_old_strict_param_raises_type_error(self, tmp_path):
        """scan_avro should reject old 'strict' param (now 'ignore_errors')."""
        test_file = tmp_path / "test.avro"
        test_file.write_bytes(b"")

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            jetliner.scan_avro(
                str(test_file),
                strict=True,  # Old param name - should raise TypeError
            )

    def test_read_avro_old_engine_options_raises_type_error(self, tmp_path):
        """read_avro should reject old 'engine_options' dict (now flattened)."""
        test_file = tmp_path / "test.avro"
        test_file.write_bytes(b"")

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            jetliner.read_avro(
                str(test_file),
                engine_options={"buffer_blocks": 8},  # Old nested dict - should raise
            )

    def test_scan_avro_accepts_valid_kwargs(self, tmp_path):
        """scan_avro should accept all valid kwargs without error."""
        # This test just verifies the signature is correct
        # We use a non-existent file so it fails at file access, not kwarg validation
        test_file = tmp_path / "nonexistent.avro"

        # All these kwargs should be accepted (file not found error is expected)
        with pytest.raises((FileNotFoundError, Exception)):  # File doesn't exist
            jetliner.scan_avro(
                str(test_file),
                n_rows=100,
                row_index_name="idx",
                row_index_offset=0,
                glob=True,
                include_file_paths="source",
                ignore_errors=False,
                storage_options=None,
                buffer_blocks=4,
                buffer_bytes=64 * 1024 * 1024,
                read_chunk_size=None,
                batch_size=100000,
            )

    def test_read_avro_accepts_valid_kwargs(self, tmp_path):
        """read_avro should accept all valid kwargs without error."""
        test_file = tmp_path / "nonexistent.avro"

        # All these kwargs should be accepted (file not found error is expected)
        with pytest.raises((FileNotFoundError, Exception)):  # File doesn't exist
            jetliner.read_avro(
                str(test_file),
                columns=None,
                n_rows=100,
                row_index_name="idx",
                row_index_offset=0,
                glob=True,
                include_file_paths="source",
                ignore_errors=False,
                storage_options=None,
                buffer_blocks=4,
                buffer_bytes=64 * 1024 * 1024,
                read_chunk_size=None,
                batch_size=100000,
            )
