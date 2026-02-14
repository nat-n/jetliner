"""
Integration tests for schema inspection with real files.

Tests cover:
- Schema property access from real files
- Schema JSON string format
- Schema dictionary parsing
- read_avro_schema() function with real files
"""

import jetliner


class TestSchemaInspection:
    """Test schema inspection capabilities with real files."""

    def test_weather_schema_json(self, get_test_data_path):
        """Test schema JSON extraction from weather file."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.AvroReader(path) as reader:
            schema_json = reader.schema

            assert isinstance(schema_json, str)
            assert "Weather" in schema_json or "weather" in schema_json.lower()
            assert "station" in schema_json
            assert "time" in schema_json
            assert "temp" in schema_json

    def test_weather_schema_dict(self, get_test_data_path):
        """Test schema dict extraction from weather file."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.AvroReader(path) as reader:
            schema_dict = reader.schema_dict

            assert isinstance(schema_dict, dict)
            assert schema_dict.get("type") == "record"

            # Check fields
            fields = schema_dict.get("fields", [])
            field_names = [f["name"] for f in fields]
            assert "station" in field_names
            assert "time" in field_names
            assert "temp" in field_names

    def test_read_avro_schema_function(self, get_test_data_path):
        """Test read_avro_schema() function with real file."""
        path = get_test_data_path("apache-avro/weather.avro")

        polars_schema = jetliner.read_avro_schema(path)

        # Should return a Polars schema dict
        assert "station" in polars_schema
        assert "time" in polars_schema
        assert "temp" in polars_schema
