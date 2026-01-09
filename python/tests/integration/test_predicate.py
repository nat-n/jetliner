"""
Integration tests for predicate pushdown (filtering).

Tests cover:
- Predicate pushdown - Requirement 6a.3
- Filter operation with various predicates
- Combining filters with other operations
- Predicate evaluation correctness
"""


import polars as pl

import jetliner


class TestPredicatePushdown:
    """Tests for predicate pushdown optimization."""

    def test_filter_equality(self, temp_avro_file):
        """Test filtering with equality predicate."""
        df = jetliner.scan(temp_avro_file).filter(pl.col("id") == 3).collect()

        assert df.height == 1
        assert df["id"].to_list() == [3]
        assert df["name"].to_list() == ["Charlie"]

    def test_filter_comparison(self, temp_avro_file):
        """Test filtering with comparison predicate."""
        df = jetliner.scan(temp_avro_file).filter(pl.col("id") > 3).collect()

        assert df.height == 2
        assert df["id"].to_list() == [4, 5]

    def test_filter_string(self, temp_avro_file):
        """Test filtering on string column."""
        df = jetliner.scan(temp_avro_file).filter(pl.col("name") == "Alice").collect()

        assert df.height == 1
        assert df["name"].to_list() == ["Alice"]

    def test_filter_combined_with_select(self, temp_avro_file):
        """Test combining filter with select."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter(pl.col("id") > 2)
            .select("name")
            .collect()
        )

        assert df.width == 1
        assert df.height == 3
        assert df["name"].to_list() == ["Charlie", "Diana", "Eve"]
