"""
Integration tests for ignore_errors=True (skip mode) error recovery with corrupted files.

Tests cover:
- Recovery from each corruption type (invalid magic, truncated, corrupted sync marker,
  corrupted compressed data, invalid record data, multi-block with one corrupted)
- Verification that valid data before/after corruption is read
- Error tracking (error_count, errors list)

Note: The old `strict=False` parameter has been replaced with `ignore_errors=True`.
The semantics are inverted: strict=False is equivalent to ignore_errors=True.

Requirements tested: 7.1 (skip bad blocks), 7.2 (skip bad records), 7.3 (track errors), 7.4 (error summary)
"""

import pytest
import polars as pl

import jetliner


class TestSkipModeRecovery:
    """Tests for ignore_errors=True (skip mode) error recovery with corrupted files."""

    def test_invalid_magic_fails_even_in_skip_mode(self, get_test_data_path):
        """
        Test that invalid magic bytes cause failure even with ignore_errors=True.

        Invalid magic bytes mean the file is not a valid Avro file at all,
        so this is a fatal error that cannot be recovered from.
        """
        path = get_test_data_path("corrupted/invalid-magic.avro")

        # Invalid magic is a fatal error - cannot recover
        with pytest.raises(jetliner.ParseError):
            with jetliner.AvroReader(path, ignore_errors=True) as reader:
                list(reader)

    def test_truncated_file_recovery(self, get_test_data_path):
        """
        Test recovery from truncated file with ignore_errors=True.

        A truncated file should read valid data up to the truncation point.
        The reader should track the error and continue gracefully.
        """
        path = get_test_data_path("corrupted/truncated.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            dfs = list(reader)

            # Should have read some data before truncation
            if len(dfs) > 0:
                total_rows = sum(df.height for df in dfs)
                assert total_rows > 0, "Should have read some records before truncation"

            # May or may not have errors depending on where truncation occurred
            # If truncation is clean at block boundary, no error
            # If truncation is mid-block, should have error
            # Either way, the reader should complete without raising

    def test_corrupted_sync_marker_recovery(self, get_test_data_path):
        """
        Test recovery from corrupted sync marker with ignore_errors=True.

        The reader should detect the invalid sync marker, skip to the next
        valid sync marker, and continue reading. Valid data before and after
        the corrupted block should be recovered.
        """
        path = get_test_data_path("corrupted/corrupted-sync-marker.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            dfs = list(reader)

            # Should have read some data
            total_rows = sum(df.height for df in dfs) if dfs else 0

            # Should have at least one error for the corrupted sync marker
            assert reader.error_count >= 1, "Should have detected sync marker error"

            # Verify error details
            errors = reader.errors
            assert len(errors) >= 1, "Should have error objects"

            # Check that we have an InvalidSyncMarker error
            error_kinds = [err.kind for err in errors]
            assert any(
                "sync" in kind.lower() or "marker" in kind.lower()
                for kind in error_kinds
            ), f"Expected sync marker error, got: {error_kinds}"

            # Should have recovered some data (before and/or after corruption)
            assert total_rows > 0, "Should have recovered some records"

    def test_corrupted_compressed_data_recovery(self, get_test_data_path):
        """
        Test recovery from corrupted compressed data with ignore_errors=True.

        The reader should detect decompression failure, skip the corrupted block,
        and continue reading subsequent blocks.
        """
        path = get_test_data_path("corrupted/corrupted-compressed.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            _dfs = list(reader)

            # Should have at least one error for decompression failure
            assert reader.error_count >= 1, "Should have detected decompression error"

            # Verify error details
            errors = reader.errors
            assert len(errors) >= 1, "Should have error objects"

            # Check error kind
            error_kinds = [err.kind for err in errors]
            assert any(
                "decompress" in kind.lower() or "codec" in kind.lower()
                for kind in error_kinds
            ), f"Expected decompression error, got: {error_kinds}"

    def test_invalid_record_data_recovery(self, get_test_data_path):
        """
        Test recovery from invalid record data with ignore_errors=True.

        The reader should detect the invalid record data, skip the bad record(s),
        and continue reading. Valid records should be recovered.
        """
        path = get_test_data_path("corrupted/invalid-record-data.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            dfs = list(reader)

            # Should have read some data
            _total_rows = sum(df.height for df in dfs) if dfs else 0

            # Should have at least one error for invalid record data
            assert reader.error_count >= 1, "Should have detected record decode error"

            # Verify error details
            errors = reader.errors
            assert len(errors) >= 1, "Should have error objects"

    def test_multi_block_one_corrupted_recovery(self, get_test_data_path):
        """
        Test recovery from multi-block file with one corrupted block.

        This is the key test for skip mode: verify that valid data before
        and after the corrupted block is recovered.
        """
        path = get_test_data_path("corrupted/multi-block-one-corrupted.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            dfs = list(reader)

            # Should have read data from valid blocks
            total_rows = sum(df.height for df in dfs) if dfs else 0

            # Should have at least one error for the corrupted block
            assert reader.error_count >= 1, "Should have detected corruption error"

            # Should have recovered significant data (most blocks are valid)
            # The file has ~300 records across multiple blocks, with only one corrupted
            # We should recover at least some records
            assert total_rows > 0, "Should have recovered records from valid blocks"

            # Verify error tracking
            errors = reader.errors
            assert len(errors) >= 1, "Should have error objects"

            # Each error should have required properties
            for err in errors:
                assert hasattr(err, "kind"), "Error should have kind"
                assert hasattr(err, "block_index"), "Error should have block_index"
                assert hasattr(err, "message"), "Error should have message"


class TestErrorTracking:
    """Tests for error tracking properties with ignore_errors=True."""

    def test_error_properties_structure(self, get_test_data_path):
        """Test that error objects have the expected structure."""
        path = get_test_data_path("corrupted/corrupted-sync-marker.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            list(reader)

            if reader.error_count > 0:
                err = reader.errors[0]

                # Check required properties
                assert hasattr(err, "kind"), "Error should have kind property"
                assert hasattr(
                    err, "block_index"
                ), "Error should have block_index property"
                assert hasattr(err, "message"), "Error should have message property"
                assert hasattr(err, "file_offset"), "Error should have file_offset property"

                # Check types
                assert isinstance(err.kind, str), "kind should be a string"
                assert isinstance(err.block_index, int), "block_index should be an int"
                assert isinstance(err.message, str), "message should be a string"
                assert isinstance(err.file_offset, int), "file_offset should be an int"

    def test_error_to_dict_method(self, get_test_data_path):
        """Test that error objects can be converted to dict."""
        path = get_test_data_path("corrupted/corrupted-sync-marker.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            list(reader)

            if reader.error_count > 0:
                err = reader.errors[0]

                # Check to_dict method exists and works
                assert hasattr(err, "to_dict"), "Error should have to_dict method"
                err_dict = err.to_dict()

                assert isinstance(err_dict, dict), "to_dict should return a dict"
                assert "kind" in err_dict, "dict should have kind"
                assert "block_index" in err_dict, "dict should have block_index"
                assert "message" in err_dict, "dict should have message"

    def test_error_count_matches_errors_length(self, get_test_data_path):
        """Test that error_count matches len(errors)."""
        path = get_test_data_path("corrupted/multi-block-one-corrupted.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            list(reader)

            assert reader.error_count == len(
                reader.errors
            ), "error_count should match len(errors)"

    def test_no_errors_on_valid_file(self, get_test_data_path):
        """Test that valid files produce no errors with ignore_errors=True."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            dfs = list(reader)

            assert len(dfs) > 0, "Should read data from valid file"
            assert reader.error_count == 0, "Valid file should have no errors"
            assert len(reader.errors) == 0, "Valid file should have empty errors list"


class TestDataIntegrityAfterRecovery:
    """Tests to verify data integrity after error recovery."""

    def test_recovered_data_has_correct_schema(self, get_test_data_path):
        """Test that recovered data has the correct schema."""
        path = get_test_data_path("corrupted/multi-block-one-corrupted.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            dfs = list(reader)

            if len(dfs) > 0:
                # Combine all DataFrames
                combined = pl.concat(dfs)

                # Check schema matches expected weather schema
                assert "station" in combined.columns, "Should have station column"
                assert "time" in combined.columns, "Should have time column"
                assert "temp" in combined.columns, "Should have temp column"

                # Check data types
                assert combined["station"].dtype == pl.Utf8, "station should be string"
                assert combined["time"].dtype == pl.Int64, "time should be int64"
                assert combined["temp"].dtype == pl.Int32, "temp should be int32"

    def test_recovered_data_values_are_valid(self, get_test_data_path):
        """Test that recovered data values are valid (not corrupted)."""
        path = get_test_data_path("corrupted/multi-block-one-corrupted.avro")

        with jetliner.AvroReader(path, ignore_errors=True) as reader:
            dfs = list(reader)

            if len(dfs) > 0:
                combined = pl.concat(dfs)

                # Check that station names follow expected pattern
                stations = combined["station"].to_list()
                for station in stations:
                    assert station.startswith(
                        "STATION-"
                    ), f"Station name should start with STATION-, got: {station}"

                # Check that time values are reasonable (positive timestamps)
                times = combined["time"].to_list()
                for t in times:
                    assert t > 0, f"Time should be positive, got: {t}"

                # Check that temp values are in expected range
                temps = combined["temp"].to_list()
                for temp in temps:
                    assert 0 <= temp <= 1000, f"Temp should be in range, got: {temp}"
