"""
Unit tests for Path object support in AvroReader and MultiAvroReader.

Tests verify that pathlib.Path objects are accepted in addition to strings,
matching the behavior of scan_avro/read_avro and the open() function.
"""

from pathlib import Path

import polars as pl
import pytest

import jetliner
from jetliner import AvroReader, MultiAvroReader


class TestAvroReaderPathSupport:
    """Tests for Path support in AvroReader."""

    def test_accepts_pathlib_path(self, temp_avro_file):
        """AvroReader should accept pathlib.Path objects."""
        path = Path(temp_avro_file)
        reader = AvroReader(path)
        total_rows = sum(df.height for df in reader)
        assert total_rows == 5

    def test_accepts_string_path(self, temp_avro_file):
        """AvroReader should still accept string paths (regression test)."""
        reader = AvroReader(temp_avro_file)
        total_rows = sum(df.height for df in reader)
        assert total_rows == 5

    def test_path_with_all_options(self, temp_avro_file):
        """Path objects should work with all AvroReader options."""
        path = Path(temp_avro_file)
        reader = AvroReader(
            path,
            batch_size=50_000,
            buffer_blocks=2,
            buffer_bytes=32 * 1024 * 1024,
            ignore_errors=True,
            projected_columns=["name"],
        )
        dfs = list(reader)
        df = pl.concat(dfs) if dfs else pl.DataFrame()
        assert df.width == 1
        assert "name" in df.columns

    def test_path_schema_access(self, temp_avro_file):
        """Schema should be accessible when using Path."""
        path = Path(temp_avro_file)
        reader = AvroReader(path)
        schema = reader.schema
        assert isinstance(schema, str)
        assert "TestRecord" in schema

    def test_path_with_context_manager(self, temp_avro_file):
        """Path should work with context manager usage."""
        path = Path(temp_avro_file)
        with AvroReader(path) as reader:
            total_rows = sum(df.height for df in reader)
        assert total_rows == 5

    def test_rejects_invalid_types(self, temp_avro_file):
        """AvroReader should reject invalid path types with clear error."""
        with pytest.raises(TypeError, match="Expected str or Path"):
            AvroReader(123)

        with pytest.raises(TypeError, match="Expected str or Path"):
            AvroReader(["not", "a", "path"])


class TestMultiAvroReaderPathSupport:
    """Tests for Path support in MultiAvroReader."""

    def test_accepts_list_of_paths(self, temp_avro_file):
        """MultiAvroReader should accept a list of pathlib.Path objects."""
        paths = [Path(temp_avro_file)]
        reader = MultiAvroReader(paths)
        total_rows = sum(df.height for df in reader)
        assert total_rows == 5

    def test_accepts_mixed_paths_and_strings(self, temp_avro_file, tmp_path):
        """MultiAvroReader should accept mixed Path and string inputs."""
        # Create a second file
        from conftest import create_test_avro_file

        records = [(10, "Zoe"), (11, "Yuri")]
        file_data = create_test_avro_file(records)
        second_file = tmp_path / "second.avro"
        second_file.write_bytes(file_data)

        # Mix Path and string
        paths = [Path(temp_avro_file), str(second_file)]
        reader = MultiAvroReader(paths)
        total_rows = sum(df.height for df in reader)
        assert total_rows == 7  # 5 + 2

    def test_accepts_list_of_strings(self, temp_avro_file):
        """MultiAvroReader should still accept string paths (regression test)."""
        reader = MultiAvroReader([temp_avro_file])
        total_rows = sum(df.height for df in reader)
        assert total_rows == 5

    def test_multiple_path_objects(self, tmp_path):
        """MultiAvroReader should handle multiple Path objects."""
        from conftest import create_test_avro_file

        # Create multiple files
        files = []
        for i in range(3):
            records = [(i * 10 + j, f"Name{i}{j}") for j in range(2)]
            file_data = create_test_avro_file(records)
            file_path = tmp_path / f"file_{i}.avro"
            file_path.write_bytes(file_data)
            files.append(file_path)  # Path objects, not strings

        reader = MultiAvroReader(files)
        total_rows = sum(df.height for df in reader)
        assert total_rows == 6  # 2 records per file * 3 files

    def test_paths_with_all_options(self, temp_avro_file):
        """Path objects should work with all MultiAvroReader options."""
        paths = [Path(temp_avro_file)]
        reader = MultiAvroReader(
            paths,
            batch_size=50_000,
            buffer_blocks=2,
            buffer_bytes=32 * 1024 * 1024,
            ignore_errors=True,
            projected_columns=["name"],
            n_rows=3,
            row_index_name="idx",
            row_index_offset=100,
            include_file_paths="source",
        )
        dfs = list(reader)
        df = pl.concat(dfs) if dfs else pl.DataFrame()

        assert df.height == 3
        assert "idx" in df.columns
        assert "source" in df.columns
        assert "name" in df.columns
        assert df["idx"].to_list() == [100, 101, 102]

    def test_paths_with_context_manager(self, temp_avro_file):
        """Path objects should work with context manager usage."""
        paths = [Path(temp_avro_file)]
        with MultiAvroReader(paths) as reader:
            total_rows = sum(df.height for df in reader)
        assert total_rows == 5

    def test_rejects_invalid_element_types(self, temp_avro_file):
        """MultiAvroReader should reject invalid types in path list."""
        with pytest.raises(TypeError, match="Expected str or Path"):
            MultiAvroReader([123])

        with pytest.raises(TypeError, match="Expected str or Path"):
            MultiAvroReader([temp_avro_file, 456])


class TestOpenFunctionPathSupport:
    """Tests verifying open() function Path support (should already work)."""

    def test_open_accepts_path(self, temp_avro_file):
        """open() should accept pathlib.Path objects."""
        path = Path(temp_avro_file)
        with jetliner.AvroReader(path) as reader:
            total_rows = sum(df.height for df in reader)
        assert total_rows == 5

    def test_open_accepts_string(self, temp_avro_file):
        """open() should accept string paths (regression test)."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            total_rows = sum(df.height for df in reader)
        assert total_rows == 5
