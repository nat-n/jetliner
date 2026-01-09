"""
Unit tests for core reader functionality.

Tests cover:
- Core reader iteration behavior
- Reader with projection (column selection)
- Schema property access
"""

import jetliner


class TestAvroReaderCore:
    """Tests for AvroReaderCore internal class."""

    def test_reader_core_iteration(self, temp_avro_file):
        """Test that AvroReaderCore can iterate over records."""
        reader = jetliner.AvroReaderCore(temp_avro_file)
        total_rows = 0
        for df in reader:
            total_rows += df.height
        assert total_rows == 5

    def test_reader_core_with_projection(self, temp_avro_file):
        """Test AvroReaderCore with projected_columns parameter."""
        reader = jetliner.AvroReaderCore(temp_avro_file, projected_columns=["name"])
        for df in reader:
            assert df.width == 1
            assert "name" in df.columns
            assert "id" not in df.columns

    def test_reader_core_schema_property(self, temp_avro_file):
        """Test AvroReaderCore schema property."""
        reader = jetliner.AvroReaderCore(temp_avro_file)
        schema = reader.schema
        assert isinstance(schema, str)
        assert "TestRecord" in schema
