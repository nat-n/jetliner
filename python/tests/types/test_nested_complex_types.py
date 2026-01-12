"""
Tests for nested complex types in Avro files.

Tests handling of:
- Arrays of structs
- Maps with struct values
- Nested arrays (arrays within arrays)
- Schema serialization for complex types (List<Struct>)

These tests verify the fixes for:
- PanicException when reading arrays/maps in record fields
- parse_avro_schema() panic on List(Struct{...}) types

Requirements tested:
- 1.5: Complex type edge cases
"""

import tempfile
from pathlib import Path

import fastavro
import polars as pl
import pytest

import jetliner


# Schema with nested complex types
NESTED_COMPLEX_SCHEMA = {
    "type": "record",
    "name": "NestedComplexRecord",
    "namespace": "test.jetliner",
    "doc": "Record containing nested complex types",
    "fields": [
        {"name": "id", "type": "int"},
        # Simple nested record (matches large_complex benchmark's "address" field)
        {
            "name": "address",
            "type": {
                "type": "record",
                "name": "SimpleAddress",
                "fields": [
                    {"name": "street", "type": "string"},
                    {"name": "city", "type": "string"},
                    {"name": "zip", "type": "string"},
                ],
            },
        },
        # Array of strings (matches large_complex benchmark's "tags" field)
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        # Map of strings (matches large_complex benchmark's "metadata" field)
        {"name": "metadata", "type": {"type": "map", "values": "string"}},
        # Nullable double (matches large_complex benchmark's "score" field)
        {"name": "score", "type": ["null", "double"]},
        # Array of structs
        {
            "name": "array_of_structs",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Person",
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"},
                    ],
                },
            },
        },
        # Map with struct values
        {
            "name": "map_of_structs",
            "type": {
                "type": "map",
                "values": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                        {"name": "street", "type": "string"},
                        {"name": "city", "type": "string"},
                    ],
                },
            },
        },
        # Nested arrays (array of arrays)
        {
            "name": "nested_arrays",
            "type": {
                "type": "array",
                "items": {"type": "array", "items": "int"},
            },
        },
        # Array of maps
        {
            "name": "array_of_maps",
            "type": {
                "type": "array",
                "items": {"type": "map", "values": "string"},
            },
        },
        # Map of arrays
        {
            "name": "map_of_arrays",
            "type": {
                "type": "map",
                "values": {"type": "array", "items": "int"},
            },
        },
    ],
}


def create_nested_complex_record(record_id: int) -> dict:
    """Create a record with nested complex types."""
    return {
        "id": record_id,
        # Simple nested record (like large_complex "address")
        "address": {
            "street": f"{100 + record_id} Main St",
            "city": "Seattle",
            "zip": f"{98000 + record_id}",
        },
        # Array of strings (like large_complex "tags")
        "tags": [f"tag_{i}" for i in range(record_id % 5 + 1)],
        # Map of strings (like large_complex "metadata")
        "metadata": {f"key_{i}": f"value_{record_id}_{i}" for i in range(record_id % 3 + 1)},
        # Nullable double (like large_complex "score")
        "score": float(record_id * 10.5) if record_id % 3 != 0 else None,
        # Array of structs
        "array_of_structs": [
            {"name": f"Person{record_id}_1", "age": 25 + record_id},
            {"name": f"Person{record_id}_2", "age": 30 + record_id},
        ],
        # Map of structs
        "map_of_structs": {
            "home": {"street": f"123 Main St #{record_id}", "city": "Seattle"},
            "work": {"street": f"456 Office Blvd #{record_id}", "city": "Bellevue"},
        },
        # Nested arrays
        "nested_arrays": [
            [1, 2, 3],
            [4, 5],
            [6, 7, 8, 9],
        ],
        # Array of maps
        "array_of_maps": [
            {"key1": f"value1_{record_id}", "key2": f"value2_{record_id}"},
            {"a": "alpha", "b": "beta"},
        ],
        # Map of arrays
        "map_of_arrays": {
            "numbers": [1, 2, 3, 4, 5],
            "more": [10, 20, 30],
        },
    }


@pytest.fixture
def nested_complex_avro_file():
    """Create a temporary Avro file with nested complex types."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_nested_complex_record(i) for i in range(3)]
        fastavro.writer(f, NESTED_COMPLEX_SCHEMA, records)
        temp_path = f.name

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


class TestNestedComplexTypes:
    """Test reading Avro files with nested complex types."""

    def test_read_nested_complex_file(self, nested_complex_avro_file):
        """Test that nested complex type file can be read without errors."""
        with jetliner.open(nested_complex_avro_file) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)
            assert df.height == 3, "Should have 3 records"

    def test_simple_nested_record(self, nested_complex_avro_file):
        """Test simple nested record (struct with primitive fields) is read correctly.

        This matches the 'address' field in large_complex benchmark.
        """
        df = jetliner.scan(nested_complex_avro_file).collect()

        first_row = df.head(1)
        address = first_row["address"][0]

        assert address["street"] == "100 Main St"
        assert address["city"] == "Seattle"
        assert address["zip"] == "98000"

    def test_array_of_strings(self, nested_complex_avro_file):
        """Test array of strings is read correctly.

        This matches the 'tags' field in large_complex benchmark.
        """
        df = jetliner.scan(nested_complex_avro_file).collect()

        # First record (id=0) has 1 tag
        first_row = df.head(1)
        tags = first_row["tags"][0]
        assert tags.len() == 1
        assert tags[0] == "tag_0"

        # Second record (id=1) has 2 tags
        second_row = df.slice(1, 1)
        tags = second_row["tags"][0]
        assert tags.len() == 2
        assert tags[0] == "tag_0"
        assert tags[1] == "tag_1"

    def test_map_of_strings(self, nested_complex_avro_file):
        """Test map of strings is read correctly.

        This matches the 'metadata' field in large_complex benchmark.
        """
        df = jetliner.scan(nested_complex_avro_file).collect()

        first_row = df.head(1)
        metadata = first_row["metadata"][0]

        # First record (id=0) has 1 entry
        assert metadata.len() == 1
        entry = metadata[0]
        assert entry["key"] == "key_0"
        assert entry["value"] == "value_0_0"

    def test_nullable_double(self, nested_complex_avro_file):
        """Test nullable double (union with null) is read correctly.

        This matches the 'score' field in large_complex benchmark.
        """
        df = jetliner.scan(nested_complex_avro_file).collect()

        # Record 0: score is None (0 % 3 == 0)
        assert df["score"][0] is None

        # Record 1: score is 10.5 (1 * 10.5)
        assert df["score"][1] == 10.5

        # Record 2: score is 21.0 (2 * 10.5)
        assert df["score"][2] == 21.0

    def test_array_of_structs(self, nested_complex_avro_file):
        """Test array of structs is read correctly."""
        df = jetliner.scan(nested_complex_avro_file).collect()

        first_row = df.head(1)
        array_of_structs = first_row["array_of_structs"][0]

        assert array_of_structs.len() == 2, "Should have 2 persons"

        # Check first person
        person1 = array_of_structs[0]
        assert person1["name"] == "Person0_1"
        assert person1["age"] == 25

        # Check second person
        person2 = array_of_structs[1]
        assert person2["name"] == "Person0_2"
        assert person2["age"] == 30

    def test_map_of_structs(self, nested_complex_avro_file):
        """Test map of structs is read correctly."""
        df = jetliner.scan(nested_complex_avro_file).collect()

        first_row = df.head(1)
        map_of_structs = first_row["map_of_structs"][0]

        # Maps are stored as List<Struct{key, value}>
        assert map_of_structs.len() == 2, "Should have 2 entries"

        # Convert to dict for easier checking
        map_dict = {entry["key"]: entry["value"] for entry in map_of_structs}

        assert "home" in map_dict
        assert "work" in map_dict

        home_addr = map_dict["home"]
        assert home_addr["street"] == "123 Main St #0"
        assert home_addr["city"] == "Seattle"

    def test_nested_arrays(self, nested_complex_avro_file):
        """Test nested arrays (array of arrays) is read correctly."""
        df = jetliner.scan(nested_complex_avro_file).collect()

        first_row = df.head(1)
        nested_arrays = first_row["nested_arrays"][0]

        assert nested_arrays.len() == 3, "Should have 3 inner arrays"

        # Check inner arrays
        assert nested_arrays[0].to_list() == [1, 2, 3]
        assert nested_arrays[1].to_list() == [4, 5]
        assert nested_arrays[2].to_list() == [6, 7, 8, 9]

    def test_array_of_maps(self, nested_complex_avro_file):
        """Test array of maps is read correctly."""
        df = jetliner.scan(nested_complex_avro_file).collect()

        first_row = df.head(1)
        array_of_maps = first_row["array_of_maps"][0]

        assert array_of_maps.len() == 2, "Should have 2 maps"

        # First map
        first_map = array_of_maps[0]
        first_map_dict = {entry["key"]: entry["value"] for entry in first_map}
        assert first_map_dict["key1"] == "value1_0"
        assert first_map_dict["key2"] == "value2_0"

    def test_map_of_arrays(self, nested_complex_avro_file):
        """Test map of arrays is read correctly."""
        df = jetliner.scan(nested_complex_avro_file).collect()

        first_row = df.head(1)
        map_of_arrays = first_row["map_of_arrays"][0]

        # Convert to dict
        map_dict = {entry["key"]: entry["value"] for entry in map_of_arrays}

        assert "numbers" in map_dict
        assert "more" in map_dict

        # Values may be lists or Series depending on nesting
        numbers = map_dict["numbers"]
        more = map_dict["more"]
        numbers_list = numbers.to_list() if hasattr(numbers, "to_list") else list(numbers)
        more_list = more.to_list() if hasattr(more, "to_list") else list(more)

        assert numbers_list == [1, 2, 3, 4, 5]
        assert more_list == [10, 20, 30]


class TestSchemaSerializationComplexTypes:
    """Test parse_avro_schema() correctly handles complex types.

    This tests the fix for pyo3-polars serialization issues with
    List(Struct{...}) types.
    """

    def test_parse_schema_array_of_structs(self, nested_complex_avro_file):
        """Test parse_avro_schema returns correct type for array of structs."""
        schema = jetliner.parse_avro_schema(nested_complex_avro_file)

        # Should have array_of_structs field
        assert "array_of_structs" in schema

        dtype = schema["array_of_structs"]
        # Should be List type
        assert dtype.base_type() == pl.List
        # Inner type should be Struct
        inner = dtype.inner
        assert inner.base_type() == pl.Struct

    def test_parse_schema_map_of_structs(self, nested_complex_avro_file):
        """Test parse_avro_schema returns correct type for map of structs."""
        schema = jetliner.parse_avro_schema(nested_complex_avro_file)

        # Should have map_of_structs field
        assert "map_of_structs" in schema

        dtype = schema["map_of_structs"]
        # Maps are List<Struct{key, value}>
        assert dtype.base_type() == pl.List
        inner = dtype.inner
        assert inner.base_type() == pl.Struct

    def test_parse_schema_nested_arrays(self, nested_complex_avro_file):
        """Test parse_avro_schema returns correct type for nested arrays."""
        schema = jetliner.parse_avro_schema(nested_complex_avro_file)

        assert "nested_arrays" in schema

        dtype = schema["nested_arrays"]
        # Should be List<List<Int>>
        assert dtype.base_type() == pl.List
        inner = dtype.inner
        assert inner.base_type() == pl.List

    def test_parse_schema_all_fields_present(self, nested_complex_avro_file):
        """Test parse_avro_schema returns all expected fields."""
        schema = jetliner.parse_avro_schema(nested_complex_avro_file)

        expected_fields = [
            "id",
            "array_of_structs",
            "map_of_structs",
            "nested_arrays",
            "array_of_maps",
            "map_of_arrays",
        ]

        for field in expected_fields:
            assert field in schema, f"Missing field: {field}"

    def test_parse_schema_edge_cases_file(self, get_test_data_path):
        """Test parse_avro_schema works with edge cases file containing arrays/maps."""
        path = get_test_data_path("edge-cases/edge-cases.avro")

        schema = jetliner.parse_avro_schema(path)

        # Check array fields
        assert "array_empty" in schema
        assert schema["array_empty"].base_type() == pl.List

        assert "array_strings" in schema
        assert schema["array_strings"].base_type() == pl.List

        # Check map fields
        assert "map_empty" in schema
        assert schema["map_empty"].base_type() == pl.List  # Maps are List<Struct>

        assert "map_unicode_keys" in schema
        assert schema["map_unicode_keys"].base_type() == pl.List


class TestComplexTypeProjection:
    """Test projection works correctly with complex types."""

    def test_project_array_of_structs(self, nested_complex_avro_file):
        """Test projection of array of structs column."""
        df = (
            jetliner.scan(nested_complex_avro_file)
            .select(["id", "array_of_structs"])
            .collect()
        )

        assert df.width == 2
        assert "id" in df.columns
        assert "array_of_structs" in df.columns

    def test_project_map_column(self, nested_complex_avro_file):
        """Test projection of map column."""
        df = (
            jetliner.scan(nested_complex_avro_file)
            .select(["id", "map_of_structs"])
            .collect()
        )

        assert df.width == 2
        assert "id" in df.columns
        assert "map_of_structs" in df.columns

    def test_project_nested_arrays(self, nested_complex_avro_file):
        """Test projection of nested arrays column."""
        df = (
            jetliner.scan(nested_complex_avro_file)
            .select(["id", "nested_arrays"])
            .collect()
        )

        assert df.width == 2
        assert "nested_arrays" in df.columns
