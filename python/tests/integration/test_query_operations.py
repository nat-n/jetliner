"""
Integration tests for comprehensive query operations.

Tests cover:
- Full query pipelines (filter + select + limit)
- Aggregation queries
- Sorting operations
- Complex query combinations
"""


import polars as pl

import jetliner



class TestComprehensiveQueries:
    """Test comprehensive query patterns with real files."""

    def test_full_query_pipeline(self, get_test_data_path):
        """Test a full query pipeline with projection, filter, and limit."""
        path = get_test_data_path("apache-avro/weather.avro")

        df = (
            jetliner.scan(path)
            .select(["station", "temp"])
            .filter(pl.col("temp").is_not_null())
            .head(10)
            .collect()
        )

        assert df.height <= 10
        assert df.width == 2
        assert "station" in df.columns
        assert "temp" in df.columns

    def test_aggregation_query(self, get_test_data_path):
        """Test aggregation query on real data."""
        path = get_test_data_path("apache-avro/weather.avro")

        df = (
            jetliner.scan(path)
            .group_by("station")
            .agg(pl.col("temp").mean().alias("avg_temp"))
            .collect()
        )

        # Should have aggregated results
        assert "station" in df.columns
        assert "avg_temp" in df.columns

    def test_sorting_query(self, get_test_data_path):
        """Test sorting query on real data."""
        path = get_test_data_path("apache-avro/weather.avro")

        df = jetliner.scan(path).sort("temp", descending=True).head(5).collect()

        # Should be sorted by temp descending
        temps = df["temp"].to_list()
        assert temps == sorted(temps, reverse=True)


# =============================================================================
# Phase 8: Interoperability Validation
# =============================================================================
