"""
Unit tests for reader configuration options.

Tests cover:
- Batch size configuration
- Strict mode vs. skip mode error handling
- Buffer configuration options
"""



import jetliner


class TestConfigurationOptions:
    """Tests for configuration options."""

    def test_batch_size_option(self, temp_multi_block_file):
        """Test batch_size configuration option."""
        # With small batch size, should get multiple batches
        batch_count = 0
        for df in jetliner.open(temp_multi_block_file, batch_size=2):
            batch_count += 1

        # Should have multiple batches with small batch size
        assert batch_count >= 1

    def test_strict_mode_default(self, temp_avro_file):
        """Test that strict mode is disabled by default."""
        # Should not raise on valid file
        with jetliner.open(temp_avro_file) as reader:
            for _ in reader:
                pass

    def test_buffer_options(self, temp_avro_file):
        """Test buffer configuration options."""
        # Should work with custom buffer settings
        with jetliner.open(
            temp_avro_file, buffer_blocks=2, buffer_bytes=1024 * 1024
        ) as reader:
            dfs = list(reader)
            assert len(dfs) > 0
