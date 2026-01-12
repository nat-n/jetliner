"""
Tests for Avro nullable types (unions with null).

Tests handling of:
- Nullable primitives: ["null", "string"], ["null", "int"], etc.
- Nullable complex types: ["null", "array"], ["null", "record"]
- Null-first vs value-first unions

Requirements tested:
- 5.5: Preserve null values from Avro unions containing null
"""

import tempfile
from pathlib import Path

import fastavro
import polars as pl
import pytest

import jetliner


# Schema with nullable primitives only (no complex types that cause issues)
NULLABLE_PRIMITIVES_SCHEMA = {
    "type": "record",
    "name": "NullablePrimitivesRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Nullable string (null first)
        {"name": "nullable_string", "type": ["null", "string"]},
        # Nullable int (value first)
        {"name": "nullable_int", "type": ["int", "null"]},
        # Nullable long
        {"name": "nullable_long", "type": ["null", "long"]},
        # Nullable double
        {"name": "nullable_double", "type": ["null", "double"]},
        # Nullable boolean
        {"name": "nullable_bool", "type": ["null", "boolean"]},
        # Nullable bytes
        {"name": "nullable_bytes", "type": ["null", "bytes"]},
    ],
}


# Schema with nullable array (works correctly)
NULLABLE_ARRAY_SCHEMA = {
    "type": "record",
    "name": "NullableArrayRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        {
            "name": "nullable_array",
            "type": ["null", {"type": "array", "items": "int"}],
        },
    ],
}


# Schema with nullable record (causes pyo3-polars panic)
NULLABLE_RECORD_SCHEMA = {
    "type": "record",
    "name": "NullableRecordRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        {
            "name": "nullable_record",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "InnerRecord",
                    "fields": [
                        {"name": "value", "type": "int"},
                    ],
                },
            ],
        },
    ],
}


# Full schema with all nullable types (includes nullable record which causes panic)
NULLABLE_TYPES_SCHEMA = {
    "type": "record",
    "name": "NullableTypesRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "nullable_string", "type": ["null", "string"]},
        {"name": "nullable_int", "type": ["int", "null"]},
        {"name": "nullable_long", "type": ["null", "long"]},
        {"name": "nullable_double", "type": ["null", "double"]},
        {"name": "nullable_bool", "type": ["null", "boolean"]},
        {"name": "nullable_bytes", "type": ["null", "bytes"]},
        {
            "name": "nullable_array",
            "type": ["null", {"type": "array", "items": "int"}],
        },
        {
            "name": "nullable_record",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "InnerRecord",
                    "fields": [
                        {"name": "value", "type": "int"},
                    ],
                },
            ],
        },
    ],
}


def create_nullable_primitives_record(record_id: int) -> dict:
    """Create a record with nullable primitive values.

    Pattern: even IDs have values, odd IDs have nulls.
    """
    is_null = record_id % 2 == 1

    return {
        "id": record_id,
        "nullable_string": None if is_null else f"value_{record_id}",
        "nullable_int": None if is_null else record_id * 10,
        "nullable_long": None if is_null else record_id * 1000,
        "nullable_double": None if is_null else record_id * 1.5,
        "nullable_bool": None if is_null else (record_id % 4 == 0),
        "nullable_bytes": None if is_null else bytes([record_id] * 4),
    }


def create_nullable_array_record(record_id: int) -> dict:
    """Create a record with nullable array."""
    is_null = record_id % 2 == 1
    return {
        "id": record_id,
        "nullable_array": None if is_null else [record_id, record_id + 1],
    }


def create_nullable_record_record(record_id: int) -> dict:
    """Create a record with nullable nested record."""
    is_null = record_id % 2 == 1
    return {
        "id": record_id,
        "nullable_record": None if is_null else {"value": record_id},
    }


def create_full_nullable_record(record_id: int) -> dict:
    """Create a record with all nullable types."""
    is_null = record_id % 2 == 1

    return {
        "id": record_id,
        "nullable_string": None if is_null else f"value_{record_id}",
        "nullable_int": None if is_null else record_id * 10,
        "nullable_long": None if is_null else record_id * 1000,
        "nullable_double": None if is_null else record_id * 1.5,
        "nullable_bool": None if is_null else (record_id % 4 == 0),
        "nullable_bytes": None if is_null else bytes([record_id] * 4),
        "nullable_array": None if is_null else [record_id, record_id + 1],
        "nullable_record": None if is_null else {"value": record_id},
    }


@pytest.fixture
def nullable_primitives_avro_file():
    """Create a temporary Avro file with nullable primitive types."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_nullable_primitives_record(i) for i in range(4)]
        fastavro.writer(f, NULLABLE_PRIMITIVES_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def nullable_array_avro_file():
    """Create a temporary Avro file with nullable array type."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_nullable_array_record(i) for i in range(4)]
        fastavro.writer(f, NULLABLE_ARRAY_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def nullable_record_avro_file():
    """Create a temporary Avro file with nullable record type."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_nullable_record_record(i) for i in range(4)]
        fastavro.writer(f, NULLABLE_RECORD_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def nullable_types_avro_file():
    """Create a temporary Avro file with all nullable types."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_full_nullable_record(i) for i in range(4)]
        fastavro.writer(f, NULLABLE_TYPES_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestNullablePrimitives:
    """Test reading Avro files with nullable primitive types (these work)."""

    def test_read_nullable_primitives_file(self, nullable_primitives_avro_file):
        """Test that nullable primitives file can be read without errors."""
        df = jetliner.scan(nullable_primitives_avro_file).collect()
        assert df.height == 4

    def test_nullable_string(self, nullable_primitives_avro_file):
        """Test nullable string is read correctly."""
        df = jetliner.scan(nullable_primitives_avro_file).collect()

        # Even IDs have values, odd IDs are null
        assert df["nullable_string"][0] == "value_0"
        assert df["nullable_string"][1] is None
        assert df["nullable_string"][2] == "value_2"
        assert df["nullable_string"][3] is None

    def test_nullable_int(self, nullable_primitives_avro_file):
        """Test nullable int (value-first union) is read correctly."""
        df = jetliner.scan(nullable_primitives_avro_file).collect()

        assert df["nullable_int"][0] == 0
        assert df["nullable_int"][1] is None
        assert df["nullable_int"][2] == 20
        assert df["nullable_int"][3] is None

    def test_nullable_long(self, nullable_primitives_avro_file):
        """Test nullable long is read correctly."""
        df = jetliner.scan(nullable_primitives_avro_file).collect()

        assert df["nullable_long"][0] == 0
        assert df["nullable_long"][1] is None
        assert df["nullable_long"][2] == 2000
        assert df["nullable_long"][3] is None

    def test_nullable_double(self, nullable_primitives_avro_file):
        """Test nullable double is read correctly."""
        df = jetliner.scan(nullable_primitives_avro_file).collect()

        assert df["nullable_double"][0] == 0.0
        assert df["nullable_double"][1] is None
        assert df["nullable_double"][2] == 3.0
        assert df["nullable_double"][3] is None

    def test_nullable_boolean(self, nullable_primitives_avro_file):
        """Test nullable boolean is read correctly."""
        df = jetliner.scan(nullable_primitives_avro_file).collect()

        assert df["nullable_bool"][0] is True  # 0 % 4 == 0
        assert df["nullable_bool"][1] is None
        assert df["nullable_bool"][2] is False  # 2 % 4 != 0
        assert df["nullable_bool"][3] is None

    def test_nullable_bytes(self, nullable_primitives_avro_file):
        """Test nullable bytes is read correctly."""
        df = jetliner.scan(nullable_primitives_avro_file).collect()

        assert df["nullable_bytes"][0] == bytes([0, 0, 0, 0])
        assert df["nullable_bytes"][1] is None
        assert df["nullable_bytes"][2] == bytes([2, 2, 2, 2])
        assert df["nullable_bytes"][3] is None

    def test_null_count(self, nullable_primitives_avro_file):
        """Test null counts are correct."""
        df = jetliner.scan(nullable_primitives_avro_file).collect()

        # Half the records should have nulls
        assert df["nullable_string"].null_count() == 2
        assert df["nullable_int"].null_count() == 2
        assert df["nullable_double"].null_count() == 2

    def test_nullable_dtypes(self, nullable_primitives_avro_file):
        """Test that nullable types have correct dtypes."""
        df = jetliner.scan(nullable_primitives_avro_file).collect()

        assert df["nullable_string"].dtype == pl.String
        assert df["nullable_int"].dtype == pl.Int32
        assert df["nullable_long"].dtype == pl.Int64
        assert df["nullable_double"].dtype == pl.Float64
        assert df["nullable_bool"].dtype == pl.Boolean
        assert df["nullable_bytes"].dtype == pl.Binary


class TestNullableArray:
    """Test reading Avro files with nullable array type (this works)."""

    def test_read_nullable_array_file(self, nullable_array_avro_file):
        """Test that nullable array file can be read without errors."""
        df = jetliner.scan(nullable_array_avro_file).collect()
        assert df.height == 4

    def test_nullable_array_values(self, nullable_array_avro_file):
        """Test nullable array is read correctly."""
        df = jetliner.scan(nullable_array_avro_file).collect()

        # Non-null arrays
        arr0 = df["nullable_array"][0]
        assert arr0 is not None
        assert arr0.to_list() == [0, 1]

        # Null array
        assert df["nullable_array"][1] is None

        arr2 = df["nullable_array"][2]
        assert arr2 is not None
        assert arr2.to_list() == [2, 3]

        assert df["nullable_array"][3] is None

    def test_nullable_array_dtype(self, nullable_array_avro_file):
        """Test nullable array has correct dtype."""
        df = jetliner.scan(nullable_array_avro_file).collect()
        assert df["nullable_array"].dtype == pl.List(pl.Int32)

    def test_nullable_array_null_count(self, nullable_array_avro_file):
        """Test nullable array null count."""
        df = jetliner.scan(nullable_array_avro_file).collect()
        assert df["nullable_array"].null_count() == 2


class TestNullableRecord:
    """Test reading Avro files with nullable record type."""

    def test_read_nullable_record_file(self, nullable_record_avro_file):
        """Test that nullable record file can be read without errors."""
        df = jetliner.scan(nullable_record_avro_file).collect()
        assert df.height == 4

    def test_nullable_record_values(self, nullable_record_avro_file):
        """Test nullable record is read correctly."""
        df = jetliner.scan(nullable_record_avro_file).collect()

        # Non-null record
        rec0 = df["nullable_record"][0]
        assert rec0 is not None
        assert rec0["value"] == 0

        # Null record
        assert df["nullable_record"][1] is None

        rec2 = df["nullable_record"][2]
        assert rec2 is not None
        assert rec2["value"] == 2

    def test_nullable_record_dtype(self, nullable_record_avro_file):
        """Test nullable record has correct dtype."""
        df = jetliner.scan(nullable_record_avro_file).collect()
        assert df["nullable_record"].dtype == pl.Struct({"value": pl.Int32})


class TestFullNullableTypes:
    """Test reading Avro files with all nullable types (includes nullable record)."""

    def test_read_full_nullable_file(self, nullable_types_avro_file):
        """Test that full nullable types file can be read without errors."""
        df = jetliner.scan(nullable_types_avro_file).collect()
        assert df.height == 4

    def test_all_nullable_types(self, nullable_types_avro_file):
        """Test all nullable types are read correctly."""
        df = jetliner.scan(nullable_types_avro_file).collect()

        # Check primitives
        assert df["nullable_string"][0] == "value_0"
        assert df["nullable_string"][1] is None

        # Check array
        assert df["nullable_array"][0].to_list() == [0, 1]
        assert df["nullable_array"][1] is None

        # Check record
        assert df["nullable_record"][0]["value"] == 0
        assert df["nullable_record"][1] is None
