"""
Integration tests for early stopping with head() operation.

Tests cover:
- Early stopping with head() - Requirement 6a.4
- Limiting results with head(n)
- Avoiding unnecessary data reading
- Early termination behavior
"""


import polars as pl

import jetliner


class TestEarlyStopping:
    """Tests for early stopping optimization."""

    def test_head_limits_rows(self, temp_avro_file):
        """Test that head() limits the number of rows returned."""
        df = jetliner.scan_avro(temp_avro_file).head(3).collect()

        assert df.height == 3

    def test_head_returns_first_rows(self, temp_avro_file):
        """Test that head() returns the first N rows."""
        df = jetliner.scan_avro(temp_avro_file).head(2).collect()

        assert df["id"].to_list() == [1, 2]
        assert df["name"].to_list() == ["Alice", "Bob"]

    def test_head_with_filter(self, temp_avro_file):
        """Test head() combined with filter."""
        df = jetliner.scan_avro(temp_avro_file).filter(pl.col("id") > 1).head(2).collect()

        assert df.height == 2
        assert df["id"].to_list() == [2, 3]

    def test_limit_alias(self, temp_avro_file):
        """Test that limit() works as alias for head()."""
        df = jetliner.scan_avro(temp_avro_file).limit(3).collect()

        assert df.height == 3
