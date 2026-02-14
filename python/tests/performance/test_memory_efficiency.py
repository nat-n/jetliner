"""
Tests for memory efficiency and bounded memory usage.

These tests verify that:
- Memory doesn't grow unbounded with file size (Requirement 8.2)
- Memory is bounded by batch_size and buffer config (Requirements 3.5, 3.6)

Note: Python's tracemalloc only tracks Python memory allocations, not Rust memory.
These tests focus on behavioral verification that the streaming architecture works
correctly with different configurations, ensuring memory-bounded operation.
"""

import jetliner


class TestMemoryEfficiency:
    """Test memory efficiency with real files.

    These tests verify the behavioral aspects of memory-bounded streaming:
    - Batch size controls DataFrame sizes (bounded memory per batch)
    - Buffer configuration is respected
    - Large files can be processed without loading entirely into memory
    - Multiple passes work correctly (memory released between iterations)
    """

    def test_batch_size_controls_dataframe_size(self, get_test_data_path):
        """
        Test that batch_size approximately controls the rows per DataFrame.

        This verifies memory is bounded by batch_size - each yielded DataFrame
        contains approximately batch_size rows. The actual count may slightly
        exceed the target when block boundaries don't align perfectly.

        Requirements: 8.2, 3.4
        """
        path = get_test_data_path("large/weather-large.avro")

        batch_size = 500
        max_rows_seen = 0
        total_rows = 0
        batch_count = 0

        with jetliner.AvroReader(path, batch_size=batch_size) as reader:
            for df in reader:
                batch_count += 1
                rows = df.height
                total_rows += rows
                max_rows_seen = max(max_rows_seen, rows)

        # Verify we processed multiple batches (streaming behavior)
        assert batch_count > 1, f"Expected multiple batches, got {batch_count}"

        # Verify batch size is approximately respected (memory bounded)
        # Allow 20% tolerance for block boundary alignment
        tolerance = 1.2
        assert max_rows_seen <= batch_size * tolerance, (
            f"Batch size not respected: max rows {max_rows_seen} > "
            f"batch_size {batch_size} * {tolerance} = {batch_size * tolerance}"
        )

        # Verify all data was read
        assert total_rows > 0, "Expected to read some data"

    def test_small_batch_size_produces_more_batches(self, get_test_data_path):
        """
        Test that smaller batch sizes produce more batches.

        This verifies the streaming architecture - smaller batches mean
        more iterations but bounded memory per iteration.

        Requirements: 8.2, 3.4
        """
        path = get_test_data_path("large/weather-large.avro")

        # Count batches with small batch size
        small_batch_count = 0
        small_total = 0
        with jetliner.AvroReader(path, batch_size=100) as reader:
            for df in reader:
                small_batch_count += 1
                small_total += df.height

        # Count batches with large batch size
        large_batch_count = 0
        large_total = 0
        with jetliner.AvroReader(path, batch_size=10000) as reader:
            for df in reader:
                large_batch_count += 1
                large_total += df.height

        # Same total rows
        assert small_total == large_total, (
            f"Total rows differ: small={small_total}, large={large_total}"
        )

        # Smaller batch size should produce more batches
        assert small_batch_count > large_batch_count, (
            f"Expected more batches with smaller batch_size. "
            f"Small: {small_batch_count}, Large: {large_batch_count}"
        )

    def test_buffer_config_accepted(self, get_test_data_path):
        """
        Test that buffer configuration parameters are accepted and work.

        Verifies the prefetch buffer configuration is respected.

        Requirements: 3.5, 3.6
        """
        path = get_test_data_path("large/weather-large.avro")

        # Test with minimal buffer config
        total_minimal = 0
        with jetliner.AvroReader(
            path,
            batch_size=1000,
            buffer_blocks=1,
            buffer_bytes=1024 * 1024,  # 1 MB
        ) as reader:
            for df in reader:
                total_minimal += df.height

        # Test with larger buffer config
        total_large = 0
        with jetliner.AvroReader(
            path,
            batch_size=1000,
            buffer_blocks=8,
            buffer_bytes=64 * 1024 * 1024,  # 64 MB
        ) as reader:
            for df in reader:
                total_large += df.height

        # Both should read the same data
        assert total_minimal == total_large, (
            f"Buffer config affected data: minimal={total_minimal}, large={total_large}"
        )
        assert total_minimal > 0, "Expected to read some data"

    def test_multiple_passes_work(self, get_test_data_path):
        """
        Test that multiple passes over the same file work correctly.

        This verifies memory is properly released between iterations,
        allowing the file to be read multiple times.

        Requirements: 8.2
        """
        path = get_test_data_path("large/weather-large.avro")

        totals = []

        # Read the file multiple times
        for _ in range(3):
            total = 0
            with jetliner.AvroReader(path, batch_size=1000) as reader:
                for df in reader:
                    total += df.height
            totals.append(total)

        # All passes should read the same amount
        assert all(t == totals[0] for t in totals), (
            f"Multiple passes returned different totals: {totals}"
        )
        assert totals[0] > 0, "Expected to read some data"

    def test_large_file_streams_without_loading_all(self, get_test_data_path):
        """
        Test that large files are streamed, not loaded entirely.

        Verifies streaming behavior by checking that we get multiple batches
        and can process incrementally.

        Requirements: 8.2, 3.1, 3.2
        """
        path = get_test_data_path("large/weather-large.avro")

        batch_sizes = []
        cumulative_rows = []
        running_total = 0

        with jetliner.AvroReader(path, batch_size=500) as reader:
            for df in reader:
                rows = df.height
                batch_sizes.append(rows)
                running_total += rows
                cumulative_rows.append(running_total)

        # Should have multiple batches (streaming, not all-at-once)
        assert len(batch_sizes) > 1, (
            f"Expected multiple batches for streaming, got {len(batch_sizes)}"
        )

        # Cumulative should grow incrementally
        assert cumulative_rows == sorted(cumulative_rows), (
            "Cumulative rows should be monotonically increasing"
        )

        # Final total should match expected
        assert running_total > 0, "Expected to read some data"

    def test_scan_with_early_stopping(self, get_test_data_path):
        """
        Test that early stopping prevents reading entire file.

        This is a key memory optimization - stop reading when we have enough rows.

        Requirements: 6a.4, 8.2
        """
        path = get_test_data_path("large/weather-large.avro")

        # Read with early stopping
        limit = 100
        df_limited = jetliner.scan_avro(path).head(limit).collect()

        # Read all
        df_all = jetliner.scan_avro(path).collect()

        # Limited should have exactly the limit
        assert df_limited.height == limit, (
            f"Expected {limit} rows, got {df_limited.height}"
        )

        # All should have more
        assert df_all.height > limit, (
            f"Expected more than {limit} rows in full file, got {df_all.height}"
        )

    def test_projection_reduces_columns(self, get_test_data_path):
        """
        Test that projection returns only requested columns.

        Projection pushdown means we only allocate memory for selected columns.

        Requirements: 6a.2, 8.2
        """
        path = get_test_data_path("large/weather-large.avro")

        # Read all columns
        df_all = jetliner.scan_avro(path).collect()

        # Read single column
        df_one = jetliner.scan_avro(path).select(["station"]).collect()

        # Same number of rows
        assert df_all.height == df_one.height, (
            f"Row count differs: all={df_all.height}, one={df_one.height}"
        )

        # Different number of columns
        assert len(df_all.columns) > len(df_one.columns), (
            f"Expected fewer columns with projection. "
            f"All: {df_all.columns}, One: {df_one.columns}"
        )

        # Projected column should be present
        assert "station" in df_one.columns, "Expected 'station' column in projection"

    def test_streaming_with_processing(self, get_test_data_path):
        """
        Test streaming with per-batch processing.

        Simulates real-world usage where each batch is processed and discarded,
        verifying memory-bounded operation.

        Requirements: 8.2, 3.3
        """
        path = get_test_data_path("large/weather-large.avro")

        # Simulate processing: compute stats per batch
        batch_stats = []

        with jetliner.AvroReader(path, batch_size=500) as reader:
            for df in reader:
                # Process this batch
                stats = {
                    "rows": df.height,
                    "columns": len(df.columns),
                }
                batch_stats.append(stats)
                # DataFrame goes out of scope here, memory should be released

        # Verify we processed multiple batches
        assert len(batch_stats) > 1, "Expected multiple batches"

        # Verify consistent schema across batches
        column_counts = [s["columns"] for s in batch_stats]
        assert all(c == column_counts[0] for c in column_counts), (
            f"Column count varied across batches: {set(column_counts)}"
        )
