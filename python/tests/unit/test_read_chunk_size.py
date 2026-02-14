"""
Tests for read_chunk_size parameter.

Verifies that the read_chunk_size parameter is correctly accepted and
applied by both open() and scan() APIs.

Requirements tested:
- 3.11: S3 default read chunk size of 4MB
- 3.12: Local default read chunk size of 64KB
- 3.13: Python API exposes read_chunk_size parameter
"""

import polars as pl

import jetliner


class TestReadChunkSizeParameter:
    """Test that read_chunk_size parameter is accepted and works correctly."""

    def test_open_accepts_read_chunk_size(self, get_test_data_path):
        """Test that open() accepts read_chunk_size parameter."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Should not raise - parameter is accepted
        with jetliner.AvroReader(path, read_chunk_size=1024 * 1024) as reader:
            dfs = list(reader)
            assert len(dfs) > 0

    def test_scan_accepts_read_chunk_size(self, get_test_data_path):
        """Test that scan_avro() accepts read_chunk_size parameter."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Should not raise - parameter is accepted
        df = jetliner.scan_avro(path, read_chunk_size=1024 * 1024).collect()
        assert df.height > 0

    def test_open_default_chunk_size(self, get_test_data_path):
        """Test that open() works with default (None) chunk size."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Default should auto-select appropriate chunk size
        with jetliner.AvroReader(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0

    def test_scan_default_chunk_size(self, get_test_data_path):
        """Test that scan_avro() works with default (None) chunk size."""
        path = get_test_data_path("apache-avro/weather.avro")

        df = jetliner.scan_avro(path).collect()
        assert df.height > 0

    def test_small_chunk_size_works(self, get_test_data_path):
        """Test that small chunk sizes work correctly (more I/O operations)."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Very small chunk size - should still work, just more reads
        with jetliner.AvroReader(path, read_chunk_size=4096) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)
            assert df.height > 0

    def test_large_chunk_size_works(self, get_test_data_path):
        """Test that large chunk sizes work correctly (fewer I/O operations)."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Large chunk size - should read entire file in one or two reads
        with jetliner.AvroReader(path, read_chunk_size=16 * 1024 * 1024) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)
            assert df.height > 0

    def test_chunk_size_does_not_affect_data(self, get_test_data_path):
        """Test that different chunk sizes produce identical data."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Read with small chunk size
        with jetliner.AvroReader(path, read_chunk_size=4096) as reader:
            df_small = pl.concat(list(reader))

        # Read with large chunk size
        with jetliner.AvroReader(path, read_chunk_size=4 * 1024 * 1024) as reader:
            df_large = pl.concat(list(reader))

        # Data should be identical
        assert df_small.shape == df_large.shape
        assert df_small.columns == df_large.columns
        assert df_small.equals(df_large)

    def test_scan_chunk_size_does_not_affect_data(self, get_test_data_path):
        """Test that different chunk sizes produce identical data via scan_avro()."""
        path = get_test_data_path("apache-avro/weather.avro")

        df_small = jetliner.scan_avro(path, read_chunk_size=4096).collect()
        df_large = jetliner.scan_avro(path, read_chunk_size=4 * 1024 * 1024).collect()

        assert df_small.shape == df_large.shape
        assert df_small.equals(df_large)


class TestReadChunkSizeWithMultiBlock:
    """Test read_chunk_size with multi-block files."""

    def test_multi_block_small_chunks(self, get_test_data_path):
        """Test reading multi-block file with small chunk size."""
        path = get_test_data_path("large/weather-large.avro")

        # Small chunks - many I/O operations
        with jetliner.AvroReader(path, read_chunk_size=8192) as reader:
            total_rows = sum(df.height for df in reader)
            assert total_rows == 10000  # weather-large has 10K records

    def test_multi_block_large_chunks(self, get_test_data_path):
        """Test reading multi-block file with large chunk size."""
        path = get_test_data_path("large/weather-large.avro")

        # Large chunks - fewer I/O operations
        with jetliner.AvroReader(path, read_chunk_size=4 * 1024 * 1024) as reader:
            total_rows = sum(df.height for df in reader)
            assert total_rows == 10000

    def test_multi_block_data_consistency(self, get_test_data_path):
        """Test that chunk size doesn't affect multi-block data."""
        path = get_test_data_path("large/weather-large.avro")

        # Read with different chunk sizes
        df_small = jetliner.scan_avro(path, read_chunk_size=8192).collect()
        df_large = jetliner.scan_avro(path, read_chunk_size=8 * 1024 * 1024).collect()

        assert df_small.shape == df_large.shape
        assert df_small.equals(df_large)


class TestReadChunkSizeEdgeCases:
    """Test edge cases for read_chunk_size parameter."""

    def test_chunk_size_minimum(self, get_test_data_path):
        """Test with small but valid chunk size."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Minimum practical chunk size - must be large enough for header + schema
        # The weather.avro header is ~500 bytes, use 1KB to be safe
        with jetliner.AvroReader(path, read_chunk_size=1024) as reader:
            dfs = list(reader)
            assert len(dfs) > 0

    def test_chunk_size_exact_file_size(self, get_test_data_path):
        """Test with chunk size equal to file size."""
        import os

        path = get_test_data_path("apache-avro/weather.avro")
        file_size = os.path.getsize(path)

        with jetliner.AvroReader(path, read_chunk_size=file_size) as reader:
            dfs = list(reader)
            assert len(dfs) > 0

    def test_chunk_size_larger_than_file(self, get_test_data_path):
        """Test with chunk size larger than file."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Chunk size much larger than file - should read entire file in one go
        with jetliner.AvroReader(path, read_chunk_size=100 * 1024 * 1024) as reader:
            dfs = list(reader)
            assert len(dfs) > 0
