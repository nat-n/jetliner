"""
Integration tests for sink_batches() with scan_avro().

These tests verify that our IO source delivers correct data when consumed
via Polars' sink_batches streaming path, which exercises a different
execution path than collect(). Each test compares sink_batches output
against collect() as ground truth.
"""

import polars as pl

import jetliner


class TestSinkBatchesDataIntegrity:
    """Verify sink_batches delivers the same data as collect()."""

    def test_data_matches_collect(self, get_test_data_path):
        """Concatenated sink_batches output must equal collect() on a multi-block file."""
        path = get_test_data_path("large/weather-large.avro")
        expected = jetliner.scan_avro(path).collect()

        collected = []
        jetliner.scan_avro(path).sink_batches(
            lambda df: collected.append(df)
        )

        result = pl.concat(collected)
        assert result.sort("station", "time").equals(
            expected.sort("station", "time")
        )

    def test_composed_query_matches_collect(self, get_test_data_path):
        """A pipeline with projection, predicate, and slice delivers
        the same result via sink_batches as via collect()."""
        path = get_test_data_path("large/weather-large.avro")

        lf = (
            jetliner.scan_avro(path)
            .select(["station", "temp"])
            .filter(pl.col("temp") > 0)
            .head(50)
        )

        expected = lf.collect()

        collected = []
        lf.sink_batches(lambda df: collected.append(df))

        result = pl.concat(collected)
        assert result.sort("station", "temp").equals(
            expected.sort("station", "temp")
        )
