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


class TestArrayMultipleBlocks:
    """Test reading arrays that may span multiple blocks.

    Validates Requirement 4.3: Arrays split across multiple blocks should be
    correctly decoded with all items preserved.
    """

    def test_array_multiple_blocks(self, tmp_path):
        """Test that arrays with many items are correctly decoded.

        This test creates records with large arrays to ensure the reader
        correctly handles array data that may span multiple encoding blocks.
        """
        import fastavro

        # Schema with array field
        schema = {
            "type": "record",
            "name": "ArrayRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "values", "type": {"type": "array", "items": "int"}},
            ],
        }

        # Create records with varying array sizes
        # Include empty, small, medium, and large arrays
        records = [
            {"id": 0, "values": []},  # Empty array
            {"id": 1, "values": [1, 2, 3]},  # Small array
            {"id": 2, "values": list(range(100))},  # Medium array
            {"id": 3, "values": list(range(1000))},  # Large array
            {"id": 4, "values": list(range(10000))},  # Very large array
        ]

        # Write to file
        avro_path = tmp_path / "array_test.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Read with jetliner
        df = jetliner.scan(str(avro_path)).collect()

        # Verify record count
        assert df.height == 5

        # Verify array contents
        values_col = df["values"].to_list()

        # Empty array
        assert values_col[0] == []

        # Small array
        assert values_col[1] == [1, 2, 3]

        # Medium array
        assert values_col[2] == list(range(100))

        # Large array
        assert values_col[3] == list(range(1000))

        # Very large array
        assert values_col[4] == list(range(10000))

    def test_nested_array_multiple_blocks(self, tmp_path):
        """Test nested arrays with many items are correctly decoded."""
        import fastavro

        # Schema with nested array field
        schema = {
            "type": "record",
            "name": "NestedArrayRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {
                    "name": "matrix",
                    "type": {"type": "array", "items": {"type": "array", "items": "int"}},
                },
            ],
        }

        # Create records with nested arrays
        records = [
            {"id": 0, "matrix": []},  # Empty outer array
            {"id": 1, "matrix": [[]]},  # Single empty inner array
            {"id": 2, "matrix": [[1, 2], [3, 4], [5, 6]]},  # Small matrix
            {"id": 3, "matrix": [list(range(i * 10, (i + 1) * 10)) for i in range(100)]},  # Large matrix
        ]

        # Write to file
        avro_path = tmp_path / "nested_array_test.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Read with jetliner
        df = jetliner.scan(str(avro_path)).collect()

        # Verify record count
        assert df.height == 4

        # Verify nested array contents
        matrix_col = df["matrix"].to_list()

        # Empty outer array
        assert matrix_col[0] == []

        # Single empty inner array
        assert matrix_col[1] == [[]]

        # Small matrix
        assert matrix_col[2] == [[1, 2], [3, 4], [5, 6]]

        # Large matrix - verify dimensions and sample values
        large_matrix = matrix_col[3]
        assert len(large_matrix) == 100
        assert large_matrix[0] == list(range(0, 10))
        assert large_matrix[99] == list(range(990, 1000))
