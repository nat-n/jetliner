"""
Tests for streaming behavior and batch iteration.

Tests the reader's ability to stream data in batches:
- Batch iteration behavior with various batch sizes
- Batch size affects number of batches returned
- Data integrity across batch boundaries
- Resource cleanup after iteration
"""

import polars as pl

import jetliner


class TestStreamingBehavior:
    """Test streaming behavior with real files."""

    def test_batch_size_affects_batch_count(self, get_test_data_path):
        """Test that smaller batch sizes produce more batches.

        With 10,000 records:
        - batch_size=100 should produce ~100 batches
        - batch_size=1000 should produce ~10 batches
        - batch_size=10000 should produce 1-2 batches
        """
        path = get_test_data_path("large/weather-large.avro")

        # Small batch size
        with jetliner.AvroReader(path, batch_size=100) as reader:
            small_batches = list(reader)

        # Medium batch size
        with jetliner.AvroReader(path, batch_size=1000) as reader:
            medium_batches = list(reader)

        # Large batch size
        with jetliner.AvroReader(path, batch_size=10000) as reader:
            large_batches = list(reader)

        # Verify batch counts are inversely related to batch size
        assert len(small_batches) > len(medium_batches), "Smaller batch size should produce more batches"
        assert len(medium_batches) > len(large_batches), "Medium batch size should produce more batches than large"

        # Verify approximate expected counts
        assert len(small_batches) >= 50, f"Expected ~100 batches with batch_size=100, got {len(small_batches)}"
        assert len(medium_batches) >= 5, f"Expected ~10 batches with batch_size=1000, got {len(medium_batches)}"
        assert len(large_batches) >= 1, f"Expected 1-2 batches with batch_size=10000, got {len(large_batches)}"

        # Verify total row counts are identical
        small_total = sum(b.height for b in small_batches)
        medium_total = sum(b.height for b in medium_batches)
        large_total = sum(b.height for b in large_batches)

        assert small_total == 10000, f"Expected 10000 total rows, got {small_total}"
        assert medium_total == 10000, f"Expected 10000 total rows, got {medium_total}"
        assert large_total == 10000, f"Expected 10000 total rows, got {large_total}"

    def test_batch_data_integrity(self, get_test_data_path):
        """Test that data is identical whether read in batches or bulk."""
        path = get_test_data_path("large/weather-large.avro")

        # Bulk read
        bulk_df = jetliner.scan_avro(path).collect()

        # Streaming read with small batches
        with jetliner.AvroReader(path, batch_size=500) as reader:
            streaming_dfs = list(reader)

        streaming_df = pl.concat(streaming_dfs)

        # Data should be identical
        assert streaming_df.height == bulk_df.height, "Row counts should match"
        assert streaming_df.columns == bulk_df.columns, "Columns should match"
        assert streaming_df.equals(bulk_df), "Data should be identical"

    def test_streaming_resource_cleanup(self, get_test_data_path):
        """Test that resources are properly cleaned up after streaming."""
        path = get_test_data_path("large/weather-large.avro")

        # Open and partially consume
        with jetliner.AvroReader(path, batch_size=1000) as reader:
            # Read just a few batches
            for i, batch in enumerate(reader):
                if i >= 3:
                    break
                assert batch.height > 0

        # Context manager should clean up - no resource leak
        # Opening again should work fine
        with jetliner.AvroReader(path, batch_size=1000) as reader:
            batches = list(reader)
            assert len(batches) > 0
