"""
Integration tests for data type validation and handling.

Tests cover:
- Data type validation (schema vs actual data)
- Data value correctness
- Type mapping from Avro to Polars
"""


import polars as pl

import jetliner


class TestDataTypeValidation:
    """Test that data types are correctly mapped to Polars types."""

    def test_weather_data_types(self, get_test_data_path):
        """Test data type mapping for weather file."""
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan(path).collect()

        # station should be string
        assert df["station"].dtype == pl.Utf8

        # time should be integer (long)
        assert df["time"].dtype in [pl.Int64, pl.Int32]

        # temp should be integer
        assert df["temp"].dtype in [pl.Int32, pl.Int64]

    def test_weather_data_values(self, get_test_data_path):
        """Test that data values are correctly read."""
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan(path).collect()

        # Station should be non-empty strings
        stations = df["station"].to_list()
        assert all(isinstance(s, str) and len(s) > 0 for s in stations)

        # Time should be integers (timestamps)
        times = df["time"].to_list()
        assert all(isinstance(t, int) for t in times)

        # Temp should be integers
        temps = df["temp"].to_list()
        assert all(isinstance(t, int) for t in temps)
