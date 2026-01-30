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
        """Test a full query pipeline with projection, filter, and limit.

        Uses weather.avro which has 5 records with temps: 0, 22, -11, 111, 78
        Filtering for temp > 0 should return 4 records, then head(3) limits to 3.
        """
        path = get_test_data_path("apache-avro/weather.avro")

        df = (
            jetliner.scan(path)
            .select(["station", "temp"])
            .filter(pl.col("temp") > 0)  # Filter for positive temps
            .head(3)
            .collect()
        )

        # Should have exactly 3 rows (limited by head)
        assert df.height == 3, f"Expected 3 rows after head(3), got {df.height}"
        assert df.width == 2, f"Expected 2 columns, got {df.width}"
        assert "station" in df.columns
        assert "temp" in df.columns

        # All temps should be positive (filter applied)
        temps = df["temp"].to_list()
        assert all(t > 0 for t in temps), f"All temps should be > 0, got {temps}"

    def test_filter_with_negative_values(self, get_test_data_path):
        """Test filtering that includes negative values.

        Weather file has temp=-11 in record 3. Filter for temp < 0 should find it.
        """
        path = get_test_data_path("apache-avro/weather.avro")

        df = (
            jetliner.scan(path)
            .filter(pl.col("temp") < 0)
            .collect()
        )

        # Should have exactly 1 row with negative temp
        assert df.height == 1, f"Expected 1 row with negative temp, got {df.height}"
        assert df["temp"][0] == -11, f"Expected temp=-11, got {df['temp'][0]}"

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
