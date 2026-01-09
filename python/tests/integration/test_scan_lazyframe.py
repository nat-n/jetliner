"""
Integration tests for scan() API returning LazyFrame.

Tests cover:
- scan() returns LazyFrame - Requirement 6a.1
- LazyFrame collection behavior
- Lazy evaluation semantics
- Integration with Polars lazy API
"""


import polars as pl

import jetliner


class TestScanReturnsLazyFrame:
    """Tests for scan() function returning LazyFrame."""

    def test_scan_returns_lazyframe(self, temp_avro_file):
        """Test that scan() returns a Polars LazyFrame."""
        lf = jetliner.scan(temp_avro_file)
        assert isinstance(lf, pl.LazyFrame)

    def test_scan_collect_returns_dataframe(self, temp_avro_file):
        """Test that collecting a scanned LazyFrame returns a DataFrame."""
        lf = jetliner.scan(temp_avro_file)
        df = lf.collect()
        assert isinstance(df, pl.DataFrame)

    def test_scan_reads_all_data(self, temp_avro_file):
        """Test that scan() reads all data when collected."""
        df = jetliner.scan(temp_avro_file).collect()

        assert df.height == 5
        assert df.width == 2
        assert "id" in df.columns
        assert "name" in df.columns

    def test_scan_correct_schema(self, temp_avro_file):
        """Test that scan() exposes correct schema."""
        lf = jetliner.scan(temp_avro_file)
        schema = lf.collect_schema()

        assert "id" in schema
        assert "name" in schema
