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
        for df in jetliner.AvroReader(path, batch_size=100):
            small_batch_count += 1
            small_total_rows += df.height

        # Large batch size
        large_batch_count = 0
        large_total_rows = 0
        for df in jetliner.AvroReader(path, batch_size=10000):
            large_batch_count += 1
            large_total_rows += df.height

        # Verify batch counts differ as expected
        assert small_batch_count > large_batch_count, "Smaller batch size should produce more batches"
        assert small_batch_count >= 50, f"Expected many batches with batch_size=100, got {small_batch_count}"
        assert large_batch_count <= 5, f"Expected few batches with batch_size=10000, got {large_batch_count}"

        # Verify total rows are the same
        assert small_total_rows == large_total_rows == 10000, "Total rows should be 10000 regardless of batch size"

    def test_default_mode(self, temp_avro_file):
        """Test that default mode works for valid files."""
        total_rows = 0
        with jetliner.AvroReader(temp_avro_file) as reader:
            for df in reader:
                total_rows += df.height

        # Should read all records without error
        assert total_rows > 0, "Should read records in default mode"

    def test_strict_mode_enabled(self, temp_avro_file):
        """Test that strict mode (ignore_errors=False) can be enabled."""
        total_rows = 0
        with jetliner.AvroReader(temp_avro_file, ignore_errors=False) as reader:
            for df in reader:
                total_rows += df.height

        # Should read all records without error (valid file)
        assert total_rows > 0, "Should read records in strict mode for valid file"

    def test_buffer_options_accepted(self, temp_avro_file):
        """Test that buffer configuration options are accepted."""
        with jetliner.AvroReader(
            temp_avro_file, buffer_blocks=2, buffer_bytes=1024 * 1024
        ) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should read data with custom buffer settings"

    def test_buffer_options_affect_behavior(self, get_test_data_path):
        """Test that buffer options don't affect data correctness."""
        path = get_test_data_path("large/weather-large.avro")

        # Read with default buffer
        with jetliner.AvroReader(path) as reader:
            default_dfs = list(reader)

        # Read with custom buffer
        with jetliner.AvroReader(
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
        with jetliner.AvroReader(temp_avro_file, read_chunk_size=1024) as reader:
            small_chunk_dfs = list(reader)

        # Large chunk size
        with jetliner.AvroReader(temp_avro_file, read_chunk_size=1024 * 1024) as reader:
            large_chunk_dfs = list(reader)

        # Data should be identical regardless of chunk size
        small_total = sum(df.height for df in small_chunk_dfs)
        large_total = sum(df.height for df in large_chunk_dfs)
        assert small_total == large_total, "Chunk size should not affect data"


class TestMaxBlockSize:
    """Tests for max_block_size decompression bomb protection."""

    def test_max_block_size_accepted(self, temp_avro_file):
        """Test that max_block_size parameter is accepted."""
        # Large limit - should work normally
        with jetliner.AvroReader(temp_avro_file, max_block_size=512 * 1024 * 1024) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should read data with max_block_size set"

    def test_max_block_size_none_disables_limit(self, temp_avro_file):
        """Test that max_block_size=None disables the limit."""
        with jetliner.AvroReader(temp_avro_file, max_block_size=None) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should read data with no block size limit"

    def test_max_block_size_rejects_oversized_blocks(self, get_test_data_path):
        """Test that a very small max_block_size rejects normal blocks.

        Use deflate-compressed file to test the streaming codec path.
        """
        import pytest
        from jetliner.exceptions import CodecError

        # Use a deflate-compressed file - the decompressed blocks should exceed 100 bytes
        path = get_test_data_path("apache-avro/weather-deflate.avro")

        # Very small limit should reject the decompressed blocks
        with pytest.raises(CodecError) as exc_info:
            with jetliner.AvroReader(path, max_block_size=100) as reader:
                list(reader)

        assert "exceeds limit" in str(exc_info.value).lower()

    def test_max_block_size_with_ignore_errors_skips_blocks(self, get_test_data_path):
        """Test that max_block_size + ignore_errors skips oversized blocks."""
        path = get_test_data_path("apache-avro/weather-deflate.avro")

        # With ignore_errors=True, should skip oversized blocks instead of raising
        with jetliner.AvroReader(path, max_block_size=100, ignore_errors=True) as reader:
            # Should complete without error, possibly with no data
            # (all blocks might be skipped if they all exceed 100 bytes)
            _ = list(reader)

    def test_scan_avro_max_block_size_accepted(self, temp_avro_file):
        """Test that scan_avro accepts max_block_size parameter."""
        lf = jetliner.scan_avro(temp_avro_file, max_block_size=512 * 1024 * 1024)
        df = lf.collect()
        assert df.height > 0, "Should read data via scan_avro with max_block_size"

    def test_read_avro_max_block_size_accepted(self, temp_avro_file):
        """Test that read_avro accepts max_block_size parameter."""
        df = jetliner.read_avro(temp_avro_file, max_block_size=512 * 1024 * 1024)
        assert df.height > 0, "Should read data via read_avro with max_block_size"
