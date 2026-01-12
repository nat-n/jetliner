"""
Tests for Avro enum and fixed types.

Tests handling of:
- enum (categorical values)
- fixed (fixed-size binary)

Requirements tested:
- 1.4: Primitive type support
- 1.5: Complex type support

Note: Enum and fixed types cause pyo3-polars panic during schema conversion.
All tests are marked xfail until the pyo3-polars issue is resolved.
"""

import tempfile
from pathlib import Path

import fastavro
import polars as pl
import pytest

import jetliner


# Schema with enum type
ENUM_SCHEMA = {
    "type": "record",
    "name": "EnumRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        {
            "name": "status",
            "type": {
                "type": "enum",
                "name": "Status",
                "symbols": ["PENDING", "ACTIVE", "COMPLETED", "CANCELLED"],
            },
        },
        {
            "name": "priority",
            "type": {
                "type": "enum",
                "name": "Priority",
                "symbols": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
            },
        },
    ],
}

# Schema with fixed type
FIXED_SCHEMA = {
    "type": "record",
    "name": "FixedRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # 16-byte fixed (like UUID binary)
        {
            "name": "uuid_bytes",
            "type": {"type": "fixed", "name": "uuid_fixed", "size": 16},
        },
        # 32-byte fixed (like SHA-256 hash)
        {
            "name": "hash",
            "type": {"type": "fixed", "name": "hash_fixed", "size": 32},
        },
    ],
}


def create_enum_record(record_id: int) -> dict:
    """Create a record with enum values."""
    statuses = ["PENDING", "ACTIVE", "COMPLETED", "CANCELLED"]
    priorities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    return {
        "id": record_id,
        "status": statuses[record_id % len(statuses)],
        "priority": priorities[record_id % len(priorities)],
    }


def create_fixed_record(record_id: int) -> dict:
    """Create a record with fixed-size binary values."""
    # Create 16-byte UUID-like value
    uuid_bytes = bytes([record_id] * 16)
    # Create 32-byte hash-like value
    hash_bytes = bytes([(record_id + i) % 256 for i in range(32)])
    return {
        "id": record_id,
        "uuid_bytes": uuid_bytes,
        "hash": hash_bytes,
    }


@pytest.fixture
def enum_avro_file():
    """Create a temporary Avro file with enum types."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_enum_record(i) for i in range(5)]
        fastavro.writer(f, ENUM_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def fixed_avro_file():
    """Create a temporary Avro file with fixed types."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_fixed_record(i) for i in range(3)]
        fastavro.writer(f, FIXED_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestEnumType:
    """Test reading Avro files with enum types."""

    @pytest.mark.xfail(reason="Enum type causes pyo3-polars panic on schema conversion")
    def test_read_enum_file(self, enum_avro_file):
        """Test that enum file can be read without errors."""
        df = jetliner.scan(enum_avro_file).collect()
        assert df.height == 5

    @pytest.mark.xfail(reason="Enum type causes pyo3-polars panic on schema conversion")
    def test_enum_dtype(self, enum_avro_file):
        """Test enum is read as Categorical type."""
        df = jetliner.scan(enum_avro_file).collect()

        # Enum should be Categorical
        assert df["status"].dtype == pl.Categorical
        assert df["priority"].dtype == pl.Categorical

    @pytest.mark.xfail(reason="Enum type causes pyo3-polars panic on schema conversion")
    def test_enum_values(self, enum_avro_file):
        """Test enum values are read correctly."""
        df = jetliner.scan(enum_avro_file).collect()

        # Check status values cycle through
        assert df["status"][0] == "PENDING"
        assert df["status"][1] == "ACTIVE"
        assert df["status"][2] == "COMPLETED"
        assert df["status"][3] == "CANCELLED"
        assert df["status"][4] == "PENDING"  # Wraps around

    @pytest.mark.xfail(reason="Enum type causes pyo3-polars panic on schema conversion")
    def test_enum_categories(self, enum_avro_file):
        """Test enum categories are preserved."""
        df = jetliner.scan(enum_avro_file).collect()

        # Get unique categories
        categories = df["status"].cat.get_categories().to_list()

        # Should contain all enum symbols
        assert "PENDING" in categories
        assert "ACTIVE" in categories
        assert "COMPLETED" in categories
        assert "CANCELLED" in categories


class TestFixedType:
    """Test reading Avro files with fixed types."""

    @pytest.mark.xfail(reason="Fixed type causes pyo3-polars panic on schema conversion")
    def test_read_fixed_file(self, fixed_avro_file):
        """Test that fixed file can be read without errors."""
        df = jetliner.scan(fixed_avro_file).collect()
        assert df.height == 3

    @pytest.mark.xfail(reason="Fixed type causes pyo3-polars panic on schema conversion")
    def test_fixed_dtype(self, fixed_avro_file):
        """Test fixed is read as Binary type."""
        df = jetliner.scan(fixed_avro_file).collect()

        # Fixed should be Binary
        assert df["uuid_bytes"].dtype == pl.Binary
        assert df["hash"].dtype == pl.Binary

    @pytest.mark.xfail(reason="Fixed type causes pyo3-polars panic on schema conversion")
    def test_fixed_size_preserved(self, fixed_avro_file):
        """Test fixed-size values have correct length."""
        df = jetliner.scan(fixed_avro_file).collect()

        # UUID should be 16 bytes
        assert len(df["uuid_bytes"][0]) == 16

        # Hash should be 32 bytes
        assert len(df["hash"][0]) == 32

    @pytest.mark.xfail(reason="Fixed type causes pyo3-polars panic on schema conversion")
    def test_fixed_values(self, fixed_avro_file):
        """Test fixed values are read correctly."""
        df = jetliner.scan(fixed_avro_file).collect()

        # First record: uuid_bytes should be all zeros
        assert df["uuid_bytes"][0] == bytes([0] * 16)

        # Second record: uuid_bytes should be all ones
        assert df["uuid_bytes"][1] == bytes([1] * 16)

        # Check hash pattern
        expected_hash = bytes([(0 + i) % 256 for i in range(32)])
        assert df["hash"][0] == expected_hash
