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
        """Test data type mapping for weather file.

        The weather.avro schema specifies:
        - station: string -> pl.Utf8
        - time: long -> pl.Int64
        - temp: int -> pl.Int32
        """
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan(path).collect()

        # station should be string (Utf8)
        assert df["station"].dtype == pl.Utf8, f"station should be Utf8, got {df['station'].dtype}"

        # time should be Int64 (Avro long)
        assert df["time"].dtype == pl.Int64, f"time should be Int64 (long), got {df['time'].dtype}"

        # temp should be Int32 (Avro int)
        assert df["temp"].dtype == pl.Int32, f"temp should be Int32 (int), got {df['temp'].dtype}"

    def test_weather_data_values(self, get_test_data_path):
        """Test that data values are correctly read.

        Verifies specific known values from the weather.avro file.
        """
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan(path).collect()

        # Verify exact record count
        assert df.height == 5, f"Expected 5 records, got {df.height}"

        # Verify first record values
        first_row = df.row(0)
        assert first_row[0] == "011990-99999", f"First station should be '011990-99999', got {first_row[0]}"
        assert first_row[1] == -619524000000, f"First time should be -619524000000, got {first_row[1]}"
        assert first_row[2] == 0, f"First temp should be 0, got {first_row[2]}"

        # Verify negative temperature is handled correctly (record 3)
        third_row = df.row(2)
        assert third_row[2] == -11, f"Third temp should be -11, got {third_row[2]}"

        # Verify all stations are non-empty strings
        stations = df["station"].to_list()
        assert all(isinstance(s, str) and len(s) > 0 for s in stations)

        # Verify all times are integers (timestamps)
        times = df["time"].to_list()
        assert all(isinstance(t, int) for t in times)

        # Verify all temps are integers
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
