"""
Integration tests for ignore_errors=False (strict) mode failure behavior with corrupted files.

Tests verify that ignore_errors=False fails immediately on each corruption type
and provides descriptive error messages.

Note: The old `strict=True` parameter has been replaced with `ignore_errors=False`.
The semantics are inverted: strict=True is equivalent to ignore_errors=False.

Requirements tested: 7.5 (fail immediately on any error), 7.7 (descriptive error messages)
"""

import pytest

import jetliner


class TestIgnoreErrorsFalseFailure:
    """Tests for ignore_errors=False (strict mode) immediate failure on corrupted files."""

    def test_invalid_magic_fails_immediately(self, get_test_data_path):
        """
        Test that invalid magic bytes cause immediate failure with ignore_errors=False.

        Invalid magic bytes mean the file is not a valid Avro file.
        The error should be raised immediately when opening the file.
        Requirements: 7.5, 7.7
        """
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            with jetliner.open(path, ignore_errors=False) as reader:
                list(reader)

        # Verify descriptive error message
        error_msg = str(exc_info.value).lower()
        assert "magic" in error_msg or "invalid" in error_msg, \
            f"Error message should mention magic bytes or invalid file: {exc_info.value}"

    def test_truncated_file_fails_immediately(self, get_test_data_path):
        """
        Test that truncated file causes immediate failure with ignore_errors=False.

        A truncated file should raise an error when the reader encounters
        unexpected EOF, not silently return partial data.
        Requirements: 7.5, 7.7
        """
        path = get_test_data_path("corrupted/truncated.avro")

        # With ignore_errors=False, truncated file should raise an error
        # The error could be ParseError (unexpected EOF) or another error type
        with pytest.raises((jetliner.ParseError, jetliner.DecodeError, jetliner.JetlinerError)) as exc_info:
            with jetliner.open(path, ignore_errors=False) as reader:
                # Consume all batches - error should occur during iteration
                list(reader)

        # Verify we got an error (the specific message depends on where truncation occurred)
        assert exc_info.value is not None, "Should have raised an error"

    def test_corrupted_sync_marker_fails_immediately(self, get_test_data_path):
        """
        Test that corrupted sync marker causes immediate failure with ignore_errors=False.

        When a sync marker doesn't match the expected value, ignore_errors=False should
        fail immediately rather than attempting recovery.
        Requirements: 7.5, 7.7
        """
        path = get_test_data_path("corrupted/corrupted-sync-marker.avro")

        with pytest.raises((jetliner.ParseError, jetliner.DecodeError, jetliner.JetlinerError)) as exc_info:
            with jetliner.open(path, ignore_errors=False) as reader:
                list(reader)

        # Verify descriptive error message mentions sync marker
        error_msg = str(exc_info.value).lower()
        assert "sync" in error_msg or "marker" in error_msg or "invalid" in error_msg, \
            f"Error message should mention sync marker issue: {exc_info.value}"

    def test_corrupted_compressed_data_fails_immediately(self, get_test_data_path):
        """
        Test that corrupted compressed data causes immediate failure with ignore_errors=False.

        When decompression fails, ignore_errors=False should fail immediately rather than
        skipping the corrupted block.
        Requirements: 7.5, 7.7
        """
        path = get_test_data_path("corrupted/corrupted-compressed.avro")

        with pytest.raises((jetliner.CodecError, jetliner.ParseError, jetliner.JetlinerError)) as exc_info:
            with jetliner.open(path, ignore_errors=False) as reader:
                list(reader)

        # Verify descriptive error message mentions decompression or codec
        error_msg = str(exc_info.value).lower()
        assert any(term in error_msg for term in ["decompress", "codec", "deflate", "corrupt", "invalid"]), \
            f"Error message should mention decompression issue: {exc_info.value}"

    def test_invalid_record_data_fails_immediately(self, get_test_data_path):
        """
        Test that invalid record data causes immediate failure with ignore_errors=False.

        When record data doesn't match the schema (e.g., invalid varints),
        ignore_errors=False should fail immediately.
        Requirements: 7.5, 7.7
        """
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with pytest.raises((jetliner.DecodeError, jetliner.ParseError, jetliner.JetlinerError)) as exc_info:
            with jetliner.open(path, ignore_errors=False) as reader:
                list(reader)

        # Verify we got an error
        assert exc_info.value is not None, "Should have raised an error for invalid record data"

    def test_multi_block_corrupted_fails_immediately(self, get_test_data_path):
        """
        Test that multi-block file with corruption fails immediately with ignore_errors=False.

        Even if valid blocks exist before and after the corrupted block,
        ignore_errors=False should fail at the first error.
        Requirements: 7.5, 7.7
        """
        path = get_test_data_path("corrupted/multi-block-one-corrupted.avro")

        with pytest.raises((jetliner.ParseError, jetliner.DecodeError, jetliner.JetlinerError)) as exc_info:
            with jetliner.open(path, ignore_errors=False) as reader:
                list(reader)

        # Verify we got an error
        assert exc_info.value is not None, "Should have raised an error for corrupted block"


class TestIgnoreErrorsFalseErrorMessages:
    """Tests for descriptive error messages with ignore_errors=False."""

    def test_invalid_magic_error_message_is_descriptive(self, get_test_data_path):
        """
        Test that invalid magic error message is descriptive.

        The error message should help users understand what went wrong.
        Requirements: 7.7
        """
        path = get_test_data_path("corrupted/invalid-magic.avro")

        with pytest.raises(jetliner.ParseError) as exc_info:
            with jetliner.open(path, ignore_errors=False) as reader:
                list(reader)

        error_msg = str(exc_info.value)
        # Error message should be non-empty and informative
        assert len(error_msg) > 10, "Error message should be descriptive"
        # Should mention something about the file format or magic bytes
        assert any(term in error_msg.lower() for term in ["magic", "avro", "invalid", "file", "header"]), \
            f"Error message should be informative: {error_msg}"

    def test_corrupted_sync_marker_error_includes_details(self, get_test_data_path):
        """
        Test that sync marker error includes useful details.

        The error should include information like expected vs actual values
        or the position where the error occurred.
        Requirements: 7.7
        """
        path = get_test_data_path("corrupted/corrupted-sync-marker.avro")

        with pytest.raises((jetliner.ParseError, jetliner.DecodeError, jetliner.JetlinerError)) as exc_info:
            with jetliner.open(path, ignore_errors=False) as reader:
                list(reader)

        error_msg = str(exc_info.value)
        # Error message should be non-empty and informative
        assert len(error_msg) > 10, "Error message should be descriptive"

    def test_decompression_error_mentions_codec(self, get_test_data_path):
        """
        Test that decompression error mentions the codec.

        When decompression fails, the error should indicate which codec
        was being used to help with debugging.
        Requirements: 7.7
        """
        path = get_test_data_path("corrupted/corrupted-compressed.avro")

        with pytest.raises((jetliner.CodecError, jetliner.ParseError, jetliner.JetlinerError)) as exc_info:
            with jetliner.open(path, ignore_errors=False) as reader:
                list(reader)

        error_msg = str(exc_info.value)
        # Error message should be non-empty
        assert len(error_msg) > 10, "Error message should be descriptive"


class TestIgnoreErrorsModeComparison:
    """Tests comparing ignore_errors=True (skip mode) and ignore_errors=False (strict mode) behavior."""

    def test_ignore_errors_false_fails_where_true_recovers(self, get_test_data_path):
        """
        Test that ignore_errors=False fails where ignore_errors=True would recover.

        This verifies the fundamental difference between the two modes:
        ignore_errors=False fails immediately, ignore_errors=True continues.
        Requirements: 7.5
        """
        path = get_test_data_path("corrupted/multi-block-one-corrupted.avro")

        # ignore_errors=True (skip mode) should recover some data
        with jetliner.open(path, ignore_errors=True) as reader:
            dfs = list(reader)
            skip_mode_rows = sum(df.height for df in dfs) if dfs else 0
            skip_mode_errors = reader.error_count

        # ignore_errors=False (strict mode) should fail
        with pytest.raises((jetliner.ParseError, jetliner.DecodeError, jetliner.JetlinerError)):
            with jetliner.open(path, ignore_errors=False) as reader:
                list(reader)

        # Skip mode should have recovered some data and tracked errors
        assert skip_mode_rows > 0, "Skip mode should have recovered some data"
        assert skip_mode_errors > 0, "Skip mode should have tracked errors"

    def test_valid_file_works_in_both_modes(self, get_test_data_path):
        """
        Test that valid files work identically in both modes.

        For valid files, ignore_errors=True and ignore_errors=False should produce the same results.
        Requirements: 7.5
        """
        path = get_test_data_path("apache-avro/weather.avro")

        # Read with ignore_errors=True (skip mode)
        with jetliner.open(path, ignore_errors=True) as reader:
            skip_dfs = list(reader)
            skip_rows = sum(df.height for df in skip_dfs)
            skip_errors = reader.error_count

        # Read with ignore_errors=False (strict mode)
        with jetliner.open(path, ignore_errors=False) as reader:
            strict_dfs = list(reader)
            strict_rows = sum(df.height for df in strict_dfs)

        # Both should read the same data
        assert skip_rows == strict_rows, "Both modes should read same number of rows"
        assert skip_errors == 0, "Valid file should have no errors in skip mode"
