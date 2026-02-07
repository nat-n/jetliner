"""
Unit tests for schema access and inspection.

Tests cover:
- schema property returns JSON string
- schema_dict property returns parsed dictionary
- read_avro_schema() function
"""

import jetliner


class TestSchemaAccess:
    """Tests for schema inspection functionality."""

    def test_schema_property_returns_json(self, temp_avro_file):
        """Test that schema property returns JSON string."""
        with jetliner.open(temp_avro_file) as reader:
            schema = reader.schema
            assert isinstance(schema, str)
            assert "TestRecord" in schema

    def test_schema_dict_property(self, temp_avro_file):
        """Test that schema_dict property returns Python dict."""
        with jetliner.open(temp_avro_file) as reader:
            schema_dict = reader.schema_dict
            assert isinstance(schema_dict, dict)
            assert schema_dict.get("name") == "TestRecord"
            assert "fields" in schema_dict

    def test_read_avro_schema_function(self, temp_avro_file):
        """Test read_avro_schema() function."""
        schema = jetliner.read_avro_schema(temp_avro_file)
        # Should return a Polars schema
        assert "id" in schema
        assert "name" in schema
