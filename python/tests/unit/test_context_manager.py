"""
Unit tests for context manager protocol.

Tests cover:
- Context manager protocol implementation - Requirement 6.6
- Resource cleanup on normal exit
- Resource cleanup on exception
- Proper enter/exit behavior
"""



import jetliner


class TestContextManager:
    """Tests for context manager protocol implementation."""

    def test_context_manager_enter_exit(self, temp_avro_file):
        """Test that context manager protocol works correctly."""
        with jetliner.open(temp_avro_file) as reader:
            assert reader is not None
            # Should be able to iterate
            dfs = list(reader)
            assert len(dfs) > 0

    def test_context_manager_releases_resources(self, temp_avro_file):
        """Test that context manager releases resources on exit."""
        with jetliner.open(temp_avro_file) as reader:
            for _ in reader:
                pass

        # After exiting context, reader should be finished
        assert reader.is_finished

    def test_context_manager_on_exception(self, temp_avro_file):
        """Test that context manager releases resources even on exception."""
        try:
            with jetliner.open(temp_avro_file) as reader:
                for df in reader:
                    raise ValueError("Test exception")
        except ValueError:
            pass

        # Resources should still be released
        assert reader.is_finished
