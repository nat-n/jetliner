"""
Integration tests for edge cases and robustness.

Tests cover:
- Multiple reads of the same file
- Concurrent file access
- Reader reuse after exhaustion
- Edge case handling
"""


import polars as pl

import jetliner



class TestEdgeCasesAndRobustness:
    """Test edge cases and robustness with real files."""

    def test_multiple_reads_same_file(self, get_test_data_path):
        """Test reading the same file multiple times."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Read multiple times
        results = []
        for _ in range(3):
            df = jetliner.scan(path).collect()
            results.append(df)

        # All reads should produce identical results
        for i, df in enumerate(results[1:], 1):
            assert df.equals(results[0]), f"Read {i} differs from first read"

    def test_concurrent_file_access(self, get_test_data_path):
        """Test that multiple readers can access files concurrently."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Open multiple readers
        reader1 = jetliner.open(path)
        reader2 = jetliner.open(path)

        # Both should be able to read
        df1 = pl.concat(list(reader1))
        df2 = pl.concat(list(reader2))

        assert df1.equals(df2)

    def test_reader_reuse_after_exhaustion(self, get_test_data_path):
        """Test behavior when trying to reuse exhausted reader."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path) as reader:
            # Exhaust the reader
            dfs = list(reader)
            assert len(dfs) > 0

            # Reader should be finished
            assert reader.is_finished

            # Further iteration should yield nothing or raise an error
            # The reader raises JetlinerError when closed - this is acceptable behavior
            try:
                more_dfs = list(reader)
                assert len(more_dfs) == 0
            except (StopIteration, jetliner.JetlinerError):
                pass  # Both are acceptable behaviors for exhausted reader


# =============================================================================
# Phase 10: Performance Sanity Checks
# =============================================================================
