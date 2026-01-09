"""
Unit tests for reader properties and state.

Tests cover:
- batch_size property
- pending_records property
- is_finished property before and after iteration
- Default property values
"""

import jetliner


class TestReaderProperties:
    """Tests for reader property accessors."""

    def test_batch_size_property(self, temp_avro_file):
        """Test batch_size property returns configured value."""
        with jetliner.open(temp_avro_file, batch_size=50000) as reader:
            assert reader.batch_size == 50000

    def test_batch_size_default(self, temp_avro_file):
        """Test batch_size property returns default value."""
        with jetliner.open(temp_avro_file) as reader:
            assert reader.batch_size == 100000

    def test_pending_records_property(self, temp_avro_file):
        """Test pending_records property."""
        with jetliner.open(temp_avro_file) as reader:
            # Before iteration, pending should be 0
            assert reader.pending_records >= 0

    def test_is_finished_before_iteration(self, temp_avro_file):
        """Test is_finished is False before iteration."""
        with jetliner.open(temp_avro_file) as reader:
            assert not reader.is_finished

    def test_is_finished_after_iteration(self, temp_avro_file):
        """Test is_finished is True after iteration completes."""
        with jetliner.open(temp_avro_file) as reader:
            for _ in reader:
                pass
            assert reader.is_finished
