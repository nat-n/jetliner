"""
Integration tests for collect_batches() with scan_avro().

These tests verify that our IO source delivers correct data when consumed
via Polars' collect_batches pull-based streaming path. Each test compares
the concatenated iterator output against collect() as ground truth.
"""

import polars as pl

import jetliner


class TestCollectBatchesDataIntegrity:
    """Verify collect_batches delivers the same data as collect()."""

    def test_data_matches_collect(self, get_test_data_path):
        """Concatenated collect_batches output must equal collect() on a multi-block file."""
        path = get_test_data_path("large/weather-large.avro")
        expected = jetliner.scan_avro(path).collect()

        result = pl.concat(list(jetliner.scan_avro(path).collect_batches()))
        assert result.sort("station", "time").equals(
            expected.sort("station", "time")
        )

    def test_composed_query_matches_collect(self, get_test_data_path):
        """A pipeline with projection, predicate, and slice delivers
        the same result via collect_batches as via collect()."""
        path = get_test_data_path("large/weather-large.avro")

        lf = (
            jetliner.scan_avro(path)
            .select(["station", "temp"])
            .filter(pl.col("temp") > 0)
            .head(50)
        )

        expected = lf.collect()

        result = pl.concat(list(lf.collect_batches()))
        assert result.sort("station", "temp").equals(
            expected.sort("station", "temp")
        )
