"""
Unit tests for iterator protocol (open API).

Tests cover:
- Iterator protocol implementation - Requirements 6.1, 6.2
- Iteration yields Polars DataFrames
- Reading all records from files
- Multiple blocks handling
- Empty file handling
- Resource cleanup after iteration
- Path object support - Requirement 1.7
"""

from pathlib import Path

import polars as pl

import jetliner


class TestIteratorProtocol:
    """Tests for Python iterator protocol implementation."""

    def test_open_returns_iterator(self, temp_avro_file):
        """Test that open() returns an iterable object."""
        reader = jetliner.AvroReader(temp_avro_file)
        assert hasattr(reader, "__iter__")
        assert hasattr(reader, "__next__")

    def test_iteration_yields_dataframes(self, temp_avro_file):
        """Test that iteration yields Polars DataFrames."""
        reader = jetliner.AvroReader(temp_avro_file)
        for df in reader:
            assert isinstance(df, pl.DataFrame)

    def test_iteration_reads_all_records(self, temp_avro_file):
        """Test that iteration reads all records from the file."""
        total_rows = 0
        for df in jetliner.AvroReader(temp_avro_file):
            total_rows += df.height

        assert total_rows == 5  # 5 records in fixture

    def test_iteration_correct_data(self, temp_avro_file):
        """Test that iteration returns correct data values."""
        dfs = list(jetliner.AvroReader(temp_avro_file))
        df = pl.concat(dfs)

        assert df.height == 5
        assert df.width == 2
        assert "id" in df.columns
        assert "name" in df.columns

        ids = df["id"].to_list()
        names = df["name"].to_list()

        assert ids == [1, 2, 3, 4, 5]
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]

    def test_iteration_multiple_blocks(self, temp_multi_block_file):
        """Test iteration over file with multiple blocks."""
        total_rows = 0
        for df in jetliner.AvroReader(temp_multi_block_file):
            total_rows += df.height

        assert total_rows == 7  # 2 + 2 + 3 records

    def test_iteration_empty_file(self, temp_empty_avro_file):
        """Test iteration over empty file yields no DataFrames."""
        dfs = list(jetliner.AvroReader(temp_empty_avro_file))
        assert len(dfs) == 0

    def test_resources_released_after_iteration(self, temp_avro_file):
        """Test that resources are released after iteration completes."""
        reader = jetliner.AvroReader(temp_avro_file)

        # Consume all data
        for _ in reader:
            pass

        # Reader should be finished
        assert reader.is_finished


class TestPathObjectSupport:
    """Tests for pathlib.Path support in open() - Requirement 1.7."""

    def test_open_accepts_path_object(self, temp_avro_file):
        """Test that open() accepts pathlib.Path objects."""
        path = Path(temp_avro_file)
        reader = jetliner.AvroReader(path)
        assert hasattr(reader, "__iter__")
        assert hasattr(reader, "__next__")

    def test_open_path_reads_same_data_as_string(self, temp_avro_file):
        """Test that Path and string produce identical results."""
        # Read with string path
        dfs_str = list(jetliner.AvroReader(temp_avro_file))
        df_str = pl.concat(dfs_str)

        # Read with Path object
        path = Path(temp_avro_file)
        dfs_path = list(jetliner.AvroReader(path))
        df_path = pl.concat(dfs_path)

        # Results should be identical
        assert df_str.equals(df_path)

    def test_open_path_with_context_manager(self, temp_avro_file):
        """Test that Path works with context manager."""
        path = Path(temp_avro_file)
        total_rows = 0
        with jetliner.AvroReader(path) as reader:
            for df in reader:
                total_rows += df.height
        assert total_rows == 5

    def test_open_path_with_options(self, temp_avro_file):
        """Test that Path works with additional options."""
        path = Path(temp_avro_file)
        reader = jetliner.AvroReader(
            path,
            batch_size=50_000,
            buffer_blocks=2,
        )
        dfs = list(reader)
        df = pl.concat(dfs)
        assert df.height == 5


# =============================================================================
# Error Filepath Tracking Tests for AvroReader (open API)
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


def create_corrupt_avro_file(path):
    """Create a corrupted Avro file with truncated block for testing."""
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

    # Add truncated block (claims 100 records but has minimal data)
    file_data.extend(encode_zigzag(100))  # record count
    file_data.extend(encode_zigzag(1000))  # claimed data size
    file_data.extend(b"short")  # truncated data

    with open(path, "wb") as f:
        f.write(bytes(file_data))


class TestErrorFilepathTrackingAvroReader:
    """Tests for error filepath tracking in AvroReader (open API)."""

    def test_error_includes_filepath(self, tmp_path):
        """Errors from corrupt file should include filepath."""
        corrupt_file = tmp_path / "corrupt.avro"
        create_corrupt_avro_file(corrupt_file)

        with jetliner.AvroReader(str(corrupt_file), ignore_errors=True) as reader:
            list(reader)  # Consume the iterator

            assert reader.error_count > 0
            for err in reader.errors:
                assert err.filepath is not None
                assert str(corrupt_file) in err.filepath

    def test_error_filepath_in_str(self, tmp_path):
        """Test that str(err) includes filepath."""
        corrupt_file = tmp_path / "corrupt.avro"
        create_corrupt_avro_file(corrupt_file)

        with jetliner.AvroReader(str(corrupt_file), ignore_errors=True) as reader:
            list(reader)

            assert reader.error_count > 0
            for err in reader.errors:
                error_str = str(err)
                assert str(corrupt_file) in error_str

    def test_error_filepath_in_to_dict(self, tmp_path):
        """Test that err.to_dict() includes filepath key."""
        corrupt_file = tmp_path / "corrupt.avro"
        create_corrupt_avro_file(corrupt_file)

        with jetliner.AvroReader(str(corrupt_file), ignore_errors=True) as reader:
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
        create_corrupt_avro_file(corrupt_file)

        with jetliner.AvroReader(str(corrupt_file), ignore_errors=True) as reader:
            list(reader)

            assert reader.error_count > 0
            for err in reader.errors:
                error_repr = repr(err)
                assert "filepath=" in error_repr
                assert str(corrupt_file) in error_repr
