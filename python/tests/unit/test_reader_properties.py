"""
Unit tests for reader properties and state.

Tests cover:
- batch_size property (AvroReader and MultiAvroReader)
- rows_read property (AvroReader and MultiAvroReader)
- pending_records property
- is_finished property before and after iteration
- Default property values
"""

import jetliner
from jetliner import MultiAvroReader


class TestAvroReaderProperties:
    """Tests for AvroReader property accessors."""

    def test_batch_size_property(self, temp_avro_file):
        """Test batch_size property returns configured value."""
        with jetliner.AvroReader(temp_avro_file, batch_size=50000) as reader:
            assert reader.batch_size == 50000

    def test_batch_size_default(self, temp_avro_file):
        """Test batch_size property returns default value."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            assert reader.batch_size == 100000

    def test_rows_read_initially_zero(self, temp_avro_file):
        """Test rows_read is 0 before any iteration."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            assert reader.rows_read == 0

    def test_rows_read_after_full_iteration(self, temp_avro_file):
        """Test rows_read reflects total rows after exhausting iterator."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            for _ in reader:
                pass
            assert reader.rows_read == 5

    def test_rows_read_survives_early_exit(self, temp_avro_file):
        """Test rows_read retains value after __exit__ mid-iteration."""
        reader = jetliner.AvroReader(temp_avro_file, batch_size=2)
        reader.__enter__()
        first_batch = next(reader)
        rows_after_one_batch = reader.rows_read
        assert rows_after_one_batch == first_batch.height
        reader.__exit__(None, None, None)
        # Value should survive after reader is released
        assert reader.rows_read == rows_after_one_batch

    def test_rows_read_zero_on_empty_file(self, temp_empty_avro_file):
        """Test rows_read is 0 after iterating an empty file."""
        with jetliner.AvroReader(temp_empty_avro_file) as reader:
            for _ in reader:
                pass
            assert reader.rows_read == 0

    def test_rows_read_increments_per_batch(self, temp_avro_file):
        """Test rows_read increases monotonically with each batch."""
        with jetliner.AvroReader(temp_avro_file, batch_size=2) as reader:
            prev = 0
            for df in reader:
                current = reader.rows_read
                assert current > prev
                prev = current
            assert reader.rows_read == 5

    def test_pending_records_property(self, temp_avro_file):
        """Test pending_records property."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            # Before iteration, pending should be 0
            assert reader.pending_records >= 0

    def test_is_finished_before_iteration(self, temp_avro_file):
        """Test is_finished is False before iteration."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            assert not reader.is_finished

    def test_is_finished_after_iteration(self, temp_avro_file):
        """Test is_finished is True after iteration completes."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            for _ in reader:
                pass
            assert reader.is_finished


class TestMultiAvroReaderProperties:
    """Tests for MultiAvroReader property accessors."""

    def test_batch_size_property(self, temp_avro_file):
        """Test batch_size property returns configured value."""
        with MultiAvroReader([temp_avro_file], batch_size=50000) as reader:
            assert reader.batch_size == 50000

    def test_batch_size_default(self, temp_avro_file):
        """Test batch_size property returns default value."""
        with MultiAvroReader([temp_avro_file]) as reader:
            assert reader.batch_size == 100000

    def test_rows_read_initially_zero(self, temp_avro_file):
        """Test rows_read is 0 before any iteration."""
        reader = MultiAvroReader([temp_avro_file])
        assert reader.rows_read == 0

    def test_rows_read_after_full_iteration(self, temp_avro_file):
        """Test rows_read reflects total rows after exhausting iterator."""
        reader = MultiAvroReader([temp_avro_file])
        list(reader)
        assert reader.rows_read == 5
