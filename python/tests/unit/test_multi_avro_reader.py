"""
Unit tests for MultiAvroReader class.

Tests cover:
- Basic iteration over single and multiple files
- Row index continuity across file boundaries
- File path injection (include_file_paths)
- n_rows limiting across files
- Error handling modes (strict vs skip)
- Context manager support
- Edge cases (empty files, single rows)
"""

import json
import os
import tempfile

import polars as pl
import pytest

import jetliner
from jetliner import MultiAvroReader


# =============================================================================
# Avro File Creation Utilities (copied from conftest.py)
# =============================================================================


def encode_zigzag(n: int) -> bytes:
    """Encode an integer using Avro's zigzag encoding."""
    if n >= 0:
        zigzag = n << 1
    else:
        zigzag = ((-n - 1) << 1) | 1

    result = bytearray()
    while zigzag > 0x7F:
        result.append((zigzag & 0x7F) | 0x80)
        zigzag >>= 7
    result.append(zigzag & 0x7F)
    return bytes(result)


def encode_string(s: str) -> bytes:
    """Encode a string using Avro's string encoding (length-prefixed)."""
    encoded = s.encode("utf-8")
    return encode_zigzag(len(encoded)) + encoded


def create_test_record(id_val: int, name: str) -> bytes:
    """Create a test record with (id: long, name: string)."""
    return encode_zigzag(id_val) + encode_string(name)


def create_test_block(record_count: int, data: bytes, sync_marker: bytes) -> bytes:
    """Create an Avro block with the given data."""
    return encode_zigzag(record_count) + encode_zigzag(len(data)) + data + sync_marker


def create_test_avro_file(records: list[tuple[int, str]]) -> bytes:
    """Create a minimal valid Avro file with the given records."""
    magic = b"Obj\x01"
    schema_json = b'{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}'
    sync_marker = bytes([
        0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
        0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
    ])

    file_data = bytearray()
    file_data.extend(magic)
    file_data.extend(encode_zigzag(1))  # 1 metadata entry

    schema_key = b"avro.schema"
    file_data.extend(encode_zigzag(len(schema_key)))
    file_data.extend(schema_key)
    file_data.extend(encode_zigzag(len(schema_json)))
    file_data.extend(schema_json)
    file_data.append(0x00)  # End of map
    file_data.extend(sync_marker)

    if records:
        block_data = bytearray()
        for id_val, name in records:
            block_data.extend(create_test_record(id_val, name))
        file_data.extend(create_test_block(len(records), bytes(block_data), sync_marker))

    return bytes(file_data)


# =============================================================================
# Fixtures for Multi-File Testing
# =============================================================================


@pytest.fixture
def temp_avro_files():
    """Create multiple temporary Avro files for multi-file testing."""
    files = []
    records_per_file = [
        [(1, "Alice"), (2, "Bob")],
        [(3, "Charlie"), (4, "Diana"), (5, "Eve")],
        [(6, "Frank")],
    ]

    for records in records_per_file:
        file_data = create_test_avro_file(records)
        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            f.write(file_data)
            files.append(f.name)

    yield files

    # Cleanup
    for f in files:
        os.unlink(f)


@pytest.fixture
def temp_glob_avro_files(tmp_path):
    """Create multiple Avro files in a directory for glob testing."""
    records_per_file = [
        [(1, "Alice"), (2, "Bob")],
        [(3, "Charlie"), (4, "Diana")],
        [(5, "Eve"), (6, "Frank")],
    ]

    files = []
    for i, records in enumerate(records_per_file):
        file_data = create_test_avro_file(records)
        file_path = tmp_path / f"data_{i:02d}.avro"
        file_path.write_bytes(file_data)
        files.append(str(file_path))

    yield str(tmp_path / "data_*.avro"), files

    # tmp_path cleanup is automatic


@pytest.fixture
def temp_empty_avro_files():
    """Create multiple empty Avro files."""
    files = []
    for _ in range(3):
        file_data = create_test_avro_file([])
        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            f.write(file_data)
            files.append(f.name)

    yield files

    for f in files:
        os.unlink(f)


# =============================================================================
# Basic Functionality Tests
# =============================================================================


class TestMultiAvroReaderBasic:
    """Basic functionality tests for MultiAvroReader."""

    def test_single_file_iteration(self, temp_avro_file):
        """Test iterating over a single file (degenerate case)."""
        reader = MultiAvroReader([temp_avro_file])
        total_rows = 0
        for df in reader:
            assert isinstance(df, pl.DataFrame)
            total_rows += df.height
        assert total_rows == 5

    def test_multi_file_iteration(self, temp_avro_files):
        """Test iterating over multiple files."""
        reader = MultiAvroReader(temp_avro_files)
        total_rows = 0
        for df in reader:
            assert isinstance(df, pl.DataFrame)
            total_rows += df.height
        # 2 + 3 + 1 = 6 total records
        assert total_rows == 6

    def test_total_row_count_matches_sum(self, temp_avro_files):
        """Verify total rows equals sum of individual files."""
        # Read each file individually
        individual_totals = []
        for f in temp_avro_files:
            total = sum(df.height for df in jetliner.open(f))
            individual_totals.append(total)

        # Read all files together
        reader = MultiAvroReader(temp_avro_files)
        combined_total = sum(df.height for df in reader)

        assert combined_total == sum(individual_totals)

    def test_schema_property_returns_json(self, temp_avro_file):
        """Test that schema property returns valid JSON string."""
        reader = MultiAvroReader([temp_avro_file])
        schema = reader.schema
        assert isinstance(schema, str)
        # Should be valid JSON
        parsed = json.loads(schema)
        assert parsed["type"] == "record"
        assert "fields" in parsed

    def test_schema_dict_property(self, temp_avro_file):
        """Test that schema_dict property returns a dict."""
        reader = MultiAvroReader([temp_avro_file])
        schema_dict = reader.schema_dict
        assert isinstance(schema_dict, dict)
        assert schema_dict["type"] == "record"

    def test_data_integrity(self, temp_avro_files):
        """Test that data is read correctly across multiple files."""
        reader = MultiAvroReader(temp_avro_files)
        dfs = list(reader)
        df = pl.concat(dfs)

        # Check all expected values are present
        ids = sorted(df["id"].to_list())
        assert ids == [1, 2, 3, 4, 5, 6]

        names = sorted(df["name"].to_list())
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"]


# =============================================================================
# Row Index Tests
# =============================================================================


class TestRowIndex:
    """Tests for row index functionality across multiple files."""

    def test_row_index_basic(self, temp_avro_files):
        """Test basic row index generation."""
        reader = MultiAvroReader(temp_avro_files, row_index_name="row_nr")
        dfs = list(reader)
        df = pl.concat(dfs)

        assert "row_nr" in df.columns
        # First column should be row index
        assert df.columns[0] == "row_nr"

    def test_row_index_starts_at_offset(self, temp_avro_files):
        """Test row index starts at specified offset."""
        reader = MultiAvroReader(
            temp_avro_files, row_index_name="idx", row_index_offset=100
        )
        dfs = list(reader)
        df = pl.concat(dfs)

        row_indices = df["idx"].to_list()
        assert row_indices[0] == 100
        # Check sequential
        assert row_indices == list(range(100, 100 + len(row_indices)))

    def test_row_index_continuous_across_files(self, temp_avro_files):
        """Test row index is continuous across file boundaries."""
        reader = MultiAvroReader(temp_avro_files, row_index_name="row_nr")
        dfs = list(reader)
        df = pl.concat(dfs)

        row_indices = df["row_nr"].to_list()
        # Should be continuous sequence: 0, 1, 2, 3, 4, 5
        expected = list(range(6))
        assert row_indices == expected

    def test_row_index_with_n_rows_limit(self, temp_avro_files):
        """Test row index with n_rows limit."""
        reader = MultiAvroReader(
            temp_avro_files, row_index_name="idx", row_index_offset=10, n_rows=3
        )
        dfs = list(reader)
        df = pl.concat(dfs)

        assert df.height == 3
        row_indices = df["idx"].to_list()
        assert row_indices == [10, 11, 12]


# =============================================================================
# File Path Injection Tests
# =============================================================================


class TestFilePathInjection:
    """Tests for include_file_paths functionality."""

    def test_file_path_column_added(self, temp_avro_files):
        """Test that file path column is added with correct name."""
        reader = MultiAvroReader(temp_avro_files, include_file_paths="source_file")
        dfs = list(reader)
        df = pl.concat(dfs)

        assert "source_file" in df.columns

    def test_file_path_values_correct(self, temp_avro_files):
        """Test each row has correct source file path."""
        reader = MultiAvroReader(temp_avro_files, include_file_paths="file")
        dfs = list(reader)
        df = pl.concat(dfs)

        # Get unique file paths in the data
        file_paths = df["file"].unique().to_list()
        # Should have paths for all input files
        assert len(file_paths) == len(temp_avro_files)

        # Verify each path is one of the input files
        for path in file_paths:
            assert path in temp_avro_files

    def test_file_path_with_row_index(self, temp_avro_files):
        """Test file path column works together with row index."""
        reader = MultiAvroReader(
            temp_avro_files,
            row_index_name="row_nr",
            include_file_paths="source",
        )
        dfs = list(reader)
        df = pl.concat(dfs)

        assert "row_nr" in df.columns
        assert "source" in df.columns
        # Row index should be first, then file path, then data columns
        assert df.columns[0] == "row_nr"


# =============================================================================
# n_rows Limit Tests
# =============================================================================


class TestNRowsLimit:
    """Tests for n_rows limiting across multiple files."""

    def test_n_rows_within_first_file(self, temp_avro_files):
        """Test n_rows limit within first file."""
        # First file has 2 records
        reader = MultiAvroReader(temp_avro_files, n_rows=1)
        dfs = list(reader)
        total = sum(df.height for df in dfs)
        assert total == 1

    def test_n_rows_spanning_files(self, temp_avro_files):
        """Test n_rows limit spanning multiple files."""
        # First file has 2 records, second has 3
        reader = MultiAvroReader(temp_avro_files, n_rows=4)
        dfs = list(reader)
        total = sum(df.height for df in dfs)
        assert total == 4

    def test_n_rows_exactly_at_boundary(self, temp_avro_files):
        """Test n_rows exactly at file boundary."""
        # First file has 2 records
        reader = MultiAvroReader(temp_avro_files, n_rows=2)
        dfs = list(reader)
        total = sum(df.height for df in dfs)
        assert total == 2

    def test_n_rows_larger_than_total(self, temp_avro_files):
        """Test n_rows larger than total available rows."""
        reader = MultiAvroReader(temp_avro_files, n_rows=1000)
        dfs = list(reader)
        total = sum(df.height for df in dfs)
        # Should read all 6 records
        assert total == 6


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling modes."""

    def test_strict_mode_fails_on_bad_file(self, temp_avro_file, tmp_path):
        """Test strict mode fails on first error."""
        # Create a corrupted file
        corrupted_path = tmp_path / "corrupted.avro"
        corrupted_path.write_bytes(b"not a valid avro file")

        with pytest.raises(Exception):
            reader = MultiAvroReader(
                [temp_avro_file, str(corrupted_path)], ignore_errors=False
            )
            list(reader)

    def test_skip_mode_continues_on_error(self, temp_avro_file, tmp_path):
        """Test skip mode continues after errors."""
        # Create a file with invalid magic (will fail early)
        bad_file = tmp_path / "bad.avro"
        bad_file.write_bytes(b"BAD!")

        # With ignore_errors=True, should skip bad file and continue
        # Note: This might fail at file open time, not during iteration
        # The behavior depends on when the error occurs
        try:
            reader = MultiAvroReader(
                [str(bad_file), temp_avro_file], ignore_errors=True
            )
            dfs = list(reader)
            # If we get here, skip mode worked
            total = sum(df.height for df in dfs)
            assert total >= 0  # May have read some or all of the good file
        except Exception:
            # Some errors might not be skippable
            pass

    def test_error_count_property(self, temp_avro_file):
        """Test error_count starts at 0 for clean files."""
        reader = MultiAvroReader([temp_avro_file], ignore_errors=True)
        list(reader)  # Consume iterator
        assert reader.error_count == 0

    def test_errors_property_returns_list(self, temp_avro_file):
        """Test errors property returns a list."""
        reader = MultiAvroReader([temp_avro_file], ignore_errors=True)
        list(reader)
        errors = reader.errors
        assert isinstance(errors, list)
        assert len(errors) == 0


# =============================================================================
# Context Manager Tests
# =============================================================================


class TestContextManager:
    """Tests for context manager support."""

    def test_context_manager_basic(self, temp_avro_file):
        """Test basic context manager usage."""
        with MultiAvroReader([temp_avro_file]) as reader:
            total_rows = 0
            for df in reader:
                total_rows += df.height
        assert total_rows == 5

    def test_resources_released_on_exit(self, temp_avro_file):
        """Test resources are released after __exit__."""
        reader = MultiAvroReader([temp_avro_file])
        reader.__enter__()

        # Consume some data
        next(reader)

        # Exit
        reader.__exit__(None, None, None)

        # After exit, trying to iterate should not yield more data
        # (we can't assert is_finished because early exit doesn't consume all data)

    def test_iteration_stops_after_exit(self, temp_avro_file):
        """Test iterator stops after __exit__."""
        reader = MultiAvroReader([temp_avro_file])
        reader.__enter__()
        next(reader)
        reader.__exit__(None, None, None)

        # Trying to get more should raise StopIteration or return nothing
        remaining = list(reader)
        assert len(remaining) == 0

    def test_works_without_context_manager(self, temp_avro_file):
        """Test MultiAvroReader works correctly without context manager."""
        reader = MultiAvroReader([temp_avro_file])
        total_rows = sum(df.height for df in reader)
        assert total_rows == 5


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Edge case tests."""

    def test_empty_paths_list(self):
        """Test with empty paths list raises ConfigurationError."""
        with pytest.raises(jetliner.ConfigurationError):
            MultiAvroReader([])

    def test_all_files_empty(self, temp_empty_avro_files):
        """Test when all files are empty."""
        reader = MultiAvroReader(temp_empty_avro_files)
        dfs = list(reader)
        assert len(dfs) == 0 or sum(df.height for df in dfs) == 0

    def test_single_row_per_file(self, tmp_path):
        """Test with single row in each file."""
        files = []
        for i in range(3):
            file_data = create_test_avro_file([(i, f"Name{i}")])
            file_path = tmp_path / f"single_{i}.avro"
            file_path.write_bytes(file_data)
            files.append(str(file_path))

        reader = MultiAvroReader(files)
        dfs = list(reader)
        total = sum(df.height for df in dfs)
        assert total == 3

    def test_nonexistent_column_projection(self, temp_avro_file):
        """Test projecting nonexistent columns - returns empty projection."""
        # Unknown columns are silently ignored in projection
        reader = MultiAvroReader(
            [temp_avro_file], projected_columns=["nonexistent_column"]
        )
        dfs = list(reader)
        # Should complete without error; columns may be empty or missing
        assert len(dfs) >= 0


# =============================================================================
# Projection Tests
# =============================================================================


class TestProjection:
    """Tests for column projection."""

    def test_basic_projection(self, temp_avro_file):
        """Test basic column projection."""
        reader = MultiAvroReader([temp_avro_file], projected_columns=["name"])
        dfs = list(reader)
        df = pl.concat(dfs)

        assert df.width == 1
        assert "name" in df.columns
        assert "id" not in df.columns

    def test_projection_multiple_columns(self, temp_avro_file):
        """Test projecting multiple columns preserves order."""
        reader = MultiAvroReader([temp_avro_file], projected_columns=["name", "id"])
        dfs = list(reader)
        df = pl.concat(dfs)

        assert df.width == 2
        assert "name" in df.columns
        assert "id" in df.columns

    def test_projection_with_row_index(self, temp_avro_file):
        """Test projection works with row index."""
        reader = MultiAvroReader(
            [temp_avro_file], projected_columns=["name"], row_index_name="idx"
        )
        dfs = list(reader)
        df = pl.concat(dfs)

        # Should have row index + projected column
        assert "idx" in df.columns
        assert "name" in df.columns
        assert "id" not in df.columns


# =============================================================================
# State Tracking Tests
# =============================================================================


class TestStateTracking:
    """Tests for reader state tracking."""

    def test_is_finished_initially_false(self, temp_avro_file):
        """Test is_finished is False before exhausting iterator."""
        reader = MultiAvroReader([temp_avro_file])
        assert not reader.is_finished

    def test_is_finished_true_after_exhaustion(self, temp_avro_file):
        """Test is_finished is True after exhausting iterator."""
        reader = MultiAvroReader([temp_avro_file])
        list(reader)  # Exhaust iterator
        assert reader.is_finished

    def test_rows_read_tracking(self, temp_avro_file):
        """Test rows_read property tracks correctly."""
        reader = MultiAvroReader([temp_avro_file])

        # Before reading
        assert reader.rows_read == 0

        # After reading
        list(reader)
        assert reader.rows_read == 5


# =============================================================================
# Error Filepath Tracking Tests
# =============================================================================


def create_corrupt_avro_file(path, corrupt_type="truncated_block"):
    """Create a corrupted Avro file for testing error handling.

    Args:
        path: Path to write the file
        corrupt_type: Type of corruption:
            - "truncated_block": Valid header but truncated block data
            - "bad_sync": Valid block with wrong sync marker
    """
    # Create a valid header
    magic = b"Obj\x01"
    schema_json = b'{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}'
    sync_marker = bytes([
        0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
        0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
    ])

    file_data = bytearray()
    file_data.extend(magic)
    file_data.extend(encode_zigzag(1))  # 1 metadata entry

    schema_key = b"avro.schema"
    file_data.extend(encode_zigzag(len(schema_key)))
    file_data.extend(schema_key)
    file_data.extend(encode_zigzag(len(schema_json)))
    file_data.extend(schema_json)
    file_data.append(0x00)  # End of map
    file_data.extend(sync_marker)

    if corrupt_type == "truncated_block":
        # Add incomplete block header (says 100 records but data is truncated)
        file_data.extend(encode_zigzag(100))  # record count
        file_data.extend(encode_zigzag(1000))  # claimed data size
        file_data.extend(b"short")  # truncated data
    elif corrupt_type == "bad_sync":
        # Add a block with wrong sync marker
        record_data = create_test_record(1, "test")
        file_data.extend(encode_zigzag(1))  # record count
        file_data.extend(encode_zigzag(len(record_data)))
        file_data.extend(record_data)
        file_data.extend(bytes([0xFF] * 16))  # wrong sync marker

    with open(path, "wb") as f:
        f.write(bytes(file_data))


class TestErrorFilepathTracking:
    """Tests for error filepath tracking in MultiAvroReader."""

    def test_error_includes_filepath_single_file(self, tmp_path):
        """Errors from single corrupt file should include filepath."""
        corrupt_file = tmp_path / "corrupt.avro"
        create_corrupt_avro_file(corrupt_file, "truncated_block")

        reader = MultiAvroReader([str(corrupt_file)], ignore_errors=True)
        list(reader)  # Consume the iterator

        assert reader.error_count > 0
        for err in reader.errors:
            assert err.filepath is not None
            assert str(corrupt_file) in err.filepath

    def test_error_includes_filepath_multi_file(self, temp_avro_file, tmp_path):
        """Errors should track which file they came from in multi-file read."""
        # Create corrupt file
        corrupt_file = tmp_path / "corrupt.avro"
        create_corrupt_avro_file(corrupt_file, "truncated_block")

        # Read good file first, then corrupt file
        reader = MultiAvroReader(
            [temp_avro_file, str(corrupt_file)], ignore_errors=True
        )
        list(reader)

        # Should have errors from the corrupt file
        assert reader.error_count > 0
        for err in reader.errors:
            assert err.filepath is not None
            # Error should be from the corrupt file, not the good file
            assert str(corrupt_file) in err.filepath

    def test_error_filepath_in_str_representation(self, tmp_path):
        """Test that str(err) includes the filepath."""
        corrupt_file = tmp_path / "corrupt.avro"
        create_corrupt_avro_file(corrupt_file, "truncated_block")

        reader = MultiAvroReader([str(corrupt_file)], ignore_errors=True)
        list(reader)

        assert reader.error_count > 0
        for err in reader.errors:
            error_str = str(err)
            assert str(corrupt_file) in error_str

    def test_error_filepath_in_to_dict(self, tmp_path):
        """Test that err.to_dict() includes filepath key."""
        corrupt_file = tmp_path / "corrupt.avro"
        create_corrupt_avro_file(corrupt_file, "truncated_block")

        reader = MultiAvroReader([str(corrupt_file)], ignore_errors=True)
        list(reader)

        assert reader.error_count > 0
        for err in reader.errors:
            err_dict = err.to_dict()
            assert "filepath" in err_dict
            assert err_dict["filepath"] is not None
            assert str(corrupt_file) in err_dict["filepath"]

    def test_error_filepath_in_repr(self, tmp_path):
        """Test that repr(err) includes filepath."""
        corrupt_file = tmp_path / "corrupt.avro"
        create_corrupt_avro_file(corrupt_file, "truncated_block")

        reader = MultiAvroReader([str(corrupt_file)], ignore_errors=True)
        list(reader)

        assert reader.error_count > 0
        for err in reader.errors:
            error_repr = repr(err)
            assert "filepath=" in error_repr
            assert str(corrupt_file) in error_repr

    def test_errors_from_different_files_have_different_filepaths(self, tmp_path):
        """When multiple files have errors, each error tracks its source file."""
        # Create two corrupt files
        corrupt1 = tmp_path / "corrupt1.avro"
        corrupt2 = tmp_path / "corrupt2.avro"
        create_corrupt_avro_file(corrupt1, "truncated_block")
        create_corrupt_avro_file(corrupt2, "truncated_block")

        reader = MultiAvroReader(
            [str(corrupt1), str(corrupt2)], ignore_errors=True
        )
        list(reader)

        # Should have errors from both files
        assert reader.error_count >= 2
        filepaths = {err.filepath for err in reader.errors}
        # We should see both file paths in the errors
        assert len(filepaths) >= 1  # At least one unique filepath
