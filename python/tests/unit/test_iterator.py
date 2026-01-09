"""
Unit tests for iterator protocol (open API).

Tests cover:
- Iterator protocol implementation - Requirements 6.1, 6.2
- Iteration yields Polars DataFrames
- Reading all records from files
- Multiple blocks handling
- Empty file handling
- Resource cleanup after iteration
"""

import polars as pl

import jetliner


class TestIteratorProtocol:
    """Tests for Python iterator protocol implementation."""

    def test_open_returns_iterator(self, temp_avro_file):
        """Test that open() returns an iterable object."""
        reader = jetliner.open(temp_avro_file)
        assert hasattr(reader, "__iter__")
        assert hasattr(reader, "__next__")

    def test_iteration_yields_dataframes(self, temp_avro_file):
        """Test that iteration yields Polars DataFrames."""
        reader = jetliner.open(temp_avro_file)
        for df in reader:
            assert isinstance(df, pl.DataFrame)

    def test_iteration_reads_all_records(self, temp_avro_file):
        """Test that iteration reads all records from the file."""
        total_rows = 0
        for df in jetliner.open(temp_avro_file):
            total_rows += df.height

        assert total_rows == 5  # 5 records in fixture

    def test_iteration_correct_data(self, temp_avro_file):
        """Test that iteration returns correct data values."""
        dfs = list(jetliner.open(temp_avro_file))
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
        for df in jetliner.open(temp_multi_block_file):
            total_rows += df.height

        assert total_rows == 7  # 2 + 2 + 3 records

    def test_iteration_empty_file(self, temp_empty_avro_file):
        """Test iteration over empty file yields no DataFrames."""
        dfs = list(jetliner.open(temp_empty_avro_file))
        assert len(dfs) == 0

    def test_resources_released_after_iteration(self, temp_avro_file):
        """Test that resources are released after iteration completes."""
        reader = jetliner.open(temp_avro_file)

        # Consume all data
        for _ in reader:
            pass

        # Reader should be finished
        assert reader.is_finished
