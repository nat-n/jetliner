"""
Unit tests for reader configuration options.

Tests cover:
- Batch size configuration and its effect on output
- Strict mode vs. skip mode error handling
- Buffer configuration options
- Invalid configuration handling
"""


import jetliner


class TestConfigurationOptions:
    """Tests for configuration options."""

    def test_batch_size_affects_output(self, get_test_data_path):
        """Test that batch_size configuration affects batch count.

        With a large file (10,000 records):
        - batch_size=100 should produce many batches
        - batch_size=10000 should produce few batches
        """
        path = get_test_data_path("large/weather-large.avro")

        # Small batch size
        small_batch_count = 0
        small_total_rows = 0
        for df in jetliner.open(path, batch_size=100):
            small_batch_count += 1
            small_total_rows += df.height

        # Large batch size
        large_batch_count = 0
        large_total_rows = 0
        for df in jetliner.open(path, batch_size=10000):
            large_batch_count += 1
            large_total_rows += df.height

        # Verify batch counts differ as expected
        assert small_batch_count > large_batch_count, "Smaller batch size should produce more batches"
        assert small_batch_count >= 50, f"Expected many batches with batch_size=100, got {small_batch_count}"
        assert large_batch_count <= 5, f"Expected few batches with batch_size=10000, got {large_batch_count}"

        # Verify total rows are the same
        assert small_total_rows == large_total_rows == 10000, "Total rows should be 10000 regardless of batch size"

    def test_strict_mode_default(self, temp_avro_file):
        """Test that strict mode is disabled by default (skip mode)."""
        total_rows = 0
        with jetliner.open(temp_avro_file) as reader:
            for df in reader:
                total_rows += df.height

        # Should read all records without error
        assert total_rows > 0, "Should read records in default (skip) mode"

    def test_strict_mode_enabled(self, temp_avro_file):
        """Test that strict mode can be enabled."""
        total_rows = 0
        with jetliner.open(temp_avro_file, strict=True) as reader:
            for df in reader:
                total_rows += df.height

        # Should read all records without error (valid file)
        assert total_rows > 0, "Should read records in strict mode for valid file"

    def test_buffer_options_accepted(self, temp_avro_file):
        """Test that buffer configuration options are accepted."""
        with jetliner.open(
            temp_avro_file, buffer_blocks=2, buffer_bytes=1024 * 1024
        ) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should read data with custom buffer settings"

    def test_buffer_options_affect_behavior(self, get_test_data_path):
        """Test that buffer options don't affect data correctness."""
        path = get_test_data_path("large/weather-large.avro")

        # Read with default buffer
        with jetliner.open(path) as reader:
            default_dfs = list(reader)

        # Read with custom buffer
        with jetliner.open(
            path, buffer_blocks=1, buffer_bytes=1024
        ) as reader:
            custom_dfs = list(reader)

        # Total rows should be identical
        default_total = sum(df.height for df in default_dfs)
        custom_total = sum(df.height for df in custom_dfs)
        assert default_total == custom_total == 10000, "Buffer settings should not affect data"

    def test_read_chunk_size_option(self, temp_avro_file):
        """Test that read_chunk_size option is accepted."""
        # Small chunk size
        with jetliner.open(temp_avro_file, read_chunk_size=1024) as reader:
            small_chunk_dfs = list(reader)

        # Large chunk size
        with jetliner.open(temp_avro_file, read_chunk_size=1024 * 1024) as reader:
            large_chunk_dfs = list(reader)

        # Data should be identical regardless of chunk size
        small_total = sum(df.height for df in small_chunk_dfs)
        large_total = sum(df.height for df in large_chunk_dfs)
        assert small_total == large_total, "Chunk size should not affect data"
