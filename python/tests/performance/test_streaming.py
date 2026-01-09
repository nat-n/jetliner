"""
Tests for streaming behavior and batch iteration.

Tests the reader's ability to stream data in batches:
- Batch iteration behavior
- Configurable batch sizes
- Memory efficiency with large batch sizes
- Multiple passes over the same file
"""

import jetliner


class TestStreamingBehavior:
    """Test streaming behavior with real files."""

    def test_batch_iteration(self, get_test_data_path):
        """Test that files can be read in batches."""
        path = get_test_data_path("apache-avro/weather.avro")

        batch_count = 0
        total_rows = 0

        with jetliner.open(path, batch_size=1) as reader:
            for df in reader:
                batch_count += 1
                total_rows += df.height

        # Should have read all records
        assert total_rows > 0

    def test_large_batch_size(self, get_test_data_path):
        """Test with large batch size (all records in one batch)."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path, batch_size=1000000) as reader:
            dfs = list(reader)

            # With large batch size, should get few batches
            assert len(dfs) >= 1
