"""
Tests for Avro enum and fixed types.

Tests handling of:
- enum (categorical values)
- fixed (fixed-size binary)

Requirements tested:
- 1.4: Primitive type support
- 1.5: Complex type support

Note: Enum types work correctly. Fixed types cause pyo3-polars panic during
schema conversion - tests are marked xfail until resolved.
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

    def test_read_enum_file(self, enum_avro_file):
        """Test that enum file can be read without errors."""
        df = jetliner.scan(enum_avro_file).collect()
        assert df.height == 5

    def test_enum_dtype(self, enum_avro_file):
        """Test enum is read as Enum type (not Categorical)."""
        df = jetliner.scan(enum_avro_file).collect()

        # Avro enums have fixed categories, so they map to Polars Enum type
        # (Categorical is for when categories are inferred at runtime)
        assert df["status"].dtype == pl.Enum(
            ["PENDING", "ACTIVE", "COMPLETED", "CANCELLED"]
        )
        assert df["priority"].dtype == pl.Enum(["LOW", "MEDIUM", "HIGH", "CRITICAL"])

    def test_enum_dtype_open_api(self, enum_avro_file):
        """Test enum is read as Enum type via open() API (not just scan())."""
        with jetliner.open(enum_avro_file) as reader:
            dfs = list(reader)
        df = pl.concat(dfs)

        # Both APIs should return proper Enum types
        assert df["status"].dtype == pl.Enum(
            ["PENDING", "ACTIVE", "COMPLETED", "CANCELLED"]
        )
        assert df["priority"].dtype == pl.Enum(["LOW", "MEDIUM", "HIGH", "CRITICAL"])

    def test_enum_values(self, enum_avro_file):
        """Test enum values are read correctly."""
        df = jetliner.scan(enum_avro_file).collect()

        # Check status values cycle through
        assert df["status"][0] == "PENDING"
        assert df["status"][1] == "ACTIVE"
        assert df["status"][2] == "COMPLETED"
        assert df["status"][3] == "CANCELLED"
        assert df["status"][4] == "PENDING"  # Wraps around

    def test_enum_categories(self, enum_avro_file):
        """Test enum categories are preserved."""
        df = jetliner.scan(enum_avro_file).collect()

        # Get unique categories - for Enum type, use the dtype's categories
        categories = list(df["status"].dtype.categories)

        # Should contain all enum symbols in order
        assert categories == ["PENDING", "ACTIVE", "COMPLETED", "CANCELLED"]


class TestEnumEdgeCases:
    """Test edge cases for enum handling."""

    def test_enum_single_symbol(self):
        """Test enum with only one symbol."""
        schema = {
            "type": "record",
            "name": "SingleEnum",
            "fields": [
                {
                    "name": "only",
                    "type": {
                        "type": "enum",
                        "name": "OnlyOne",
                        "symbols": ["SINGLE"],
                    },
                },
            ],
        }
        records = [{"only": "SINGLE"} for _ in range(3)]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan(temp_path).collect()
            assert df.height == 3
            assert df["only"].dtype == pl.Enum(["SINGLE"])
            assert all(v == "SINGLE" for v in df["only"])
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_enum_many_symbols(self):
        """Test enum with many symbols (tests U8/U16 physical type selection)."""
        # 300 symbols requires U16 physical type (> 255)
        symbols = [f"SYM_{i}" for i in range(300)]
        schema = {
            "type": "record",
            "name": "ManyEnum",
            "fields": [
                {
                    "name": "many",
                    "type": {
                        "type": "enum",
                        "name": "ManySymbols",
                        "symbols": symbols,
                    },
                },
            ],
        }
        # Use symbols at various indices including beyond U8 range
        records = [
            {"many": "SYM_0"},
            {"many": "SYM_100"},
            {"many": "SYM_255"},
            {"many": "SYM_256"},
            {"many": "SYM_299"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan(temp_path).collect()
            assert df.height == 5
            assert df["many"].dtype == pl.Enum(symbols)
            assert df["many"][0] == "SYM_0"
            assert df["many"][2] == "SYM_255"
            assert df["many"][3] == "SYM_256"
            assert df["many"][4] == "SYM_299"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_enum_long_symbol_names(self):
        """Test enum with long symbol names (Avro spec allows any length)."""
        # Note: Avro spec requires symbols to match [A-Za-z_][A-Za-z0-9_]*
        # Unicode is NOT allowed in enum symbols per the spec
        symbols = [
            "VERY_LONG_SYMBOL_NAME_THAT_GOES_ON_AND_ON_" + str(i)
            for i in range(10)
        ]
        schema = {
            "type": "record",
            "name": "LongSymbolEnum",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "LongStatus",
                        "symbols": symbols,
                    },
                },
            ],
        }
        records = [{"status": symbols[i % len(symbols)]} for i in range(10)]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan(temp_path).collect()
            assert df.height == 10
            assert df["status"].dtype == pl.Enum(symbols)
            assert df["status"][0] == symbols[0]
            assert df["status"][5] == symbols[5]
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_enum_large_file_multiple_batches(self):
        """Test enum handling across multiple batches."""
        schema = {
            "type": "record",
            "name": "BatchEnum",
            "fields": [
                {"name": "id", "type": "int"},
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["A", "B", "C"],
                    },
                },
            ],
        }
        # Create enough records to span multiple batches
        num_records = 10000
        statuses = ["A", "B", "C"]
        records = [
            {"id": i, "status": statuses[i % 3]} for i in range(num_records)
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            # Use small batch size to force multiple batches
            with jetliner.open(temp_path, batch_size=1000) as reader:
                dfs = list(reader)

            # Should have multiple batches
            assert len(dfs) >= 2

            # Each batch should have Enum type
            for df in dfs:
                assert df["status"].dtype == pl.Enum(["A", "B", "C"])

            # Concatenated result should be correct
            df = pl.concat(dfs)
            assert df.height == num_records
            assert df["status"].dtype == pl.Enum(["A", "B", "C"])

            # Verify values
            for i in range(min(100, num_records)):
                expected = statuses[i % 3]
                assert df["status"][i] == expected
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_enum_projection(self):
        """Test that enum columns work with projection pushdown."""
        schema = {
            "type": "record",
            "name": "ProjectEnum",
            "fields": [
                {"name": "id", "type": "int"},
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["X", "Y", "Z"],
                    },
                },
                {"name": "value", "type": "double"},
            ],
        }
        records = [
            {"id": i, "status": ["X", "Y", "Z"][i % 3], "value": float(i)}
            for i in range(100)
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            # Project only the enum column
            df = jetliner.scan(temp_path).select(["status"]).collect()
            assert df.height == 100
            assert df.width == 1
            assert df["status"].dtype == pl.Enum(["X", "Y", "Z"])

            # Project enum with other columns
            df = jetliner.scan(temp_path).select(["id", "status"]).collect()
            assert df.height == 100
            assert df.width == 2
            assert df["status"].dtype == pl.Enum(["X", "Y", "Z"])
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_enum_filter(self):
        """Test that enum columns work with predicate pushdown."""
        schema = {
            "type": "record",
            "name": "FilterEnum",
            "fields": [
                {"name": "id", "type": "int"},
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["ACTIVE", "INACTIVE", "PENDING"],
                    },
                },
            ],
        }
        records = [
            {"id": i, "status": ["ACTIVE", "INACTIVE", "PENDING"][i % 3]}
            for i in range(99)  # 33 of each
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            # Filter on enum value
            df = (
                jetliner.scan(temp_path)
                .filter(pl.col("status") == "ACTIVE")
                .collect()
            )
            assert df.height == 33
            assert all(v == "ACTIVE" for v in df["status"])
            assert df["status"].dtype == pl.Enum(["ACTIVE", "INACTIVE", "PENDING"])
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_enum_multiple_columns(self):
        """Test multiple enum columns in the same record."""
        schema = {
            "type": "record",
            "name": "MultiEnum",
            "fields": [
                {
                    "name": "color",
                    "type": {
                        "type": "enum",
                        "name": "Color",
                        "symbols": ["RED", "GREEN", "BLUE"],
                    },
                },
                {
                    "name": "size",
                    "type": {
                        "type": "enum",
                        "name": "Size",
                        "symbols": ["SMALL", "MEDIUM", "LARGE"],
                    },
                },
                {
                    "name": "shape",
                    "type": {
                        "type": "enum",
                        "name": "Shape",
                        "symbols": ["CIRCLE", "SQUARE", "TRIANGLE"],
                    },
                },
            ],
        }
        records = [
            {
                "color": ["RED", "GREEN", "BLUE"][i % 3],
                "size": ["SMALL", "MEDIUM", "LARGE"][i % 3],
                "shape": ["CIRCLE", "SQUARE", "TRIANGLE"][i % 3],
            }
            for i in range(9)
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan(temp_path).collect()
            assert df.height == 9

            # All three columns should be Enum type with correct categories
            assert df["color"].dtype == pl.Enum(["RED", "GREEN", "BLUE"])
            assert df["size"].dtype == pl.Enum(["SMALL", "MEDIUM", "LARGE"])
            assert df["shape"].dtype == pl.Enum(["CIRCLE", "SQUARE", "TRIANGLE"])

            # Verify values
            assert df["color"][0] == "RED"
            assert df["size"][0] == "SMALL"
            assert df["shape"][0] == "CIRCLE"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    @pytest.mark.xfail(
        reason="Nullable enum (union with null) causes polars-core panic - not yet implemented"
    )
    def test_enum_nullable(self):
        """Test nullable enum (union with null)."""
        schema = {
            "type": "record",
            "name": "NullableEnum",
            "fields": [
                {"name": "id", "type": "int"},
                {
                    "name": "status",
                    "type": [
                        "null",
                        {
                            "type": "enum",
                            "name": "Status",
                            "symbols": ["ON", "OFF"],
                        },
                    ],
                },
            ],
        }
        records = [
            {"id": 0, "status": "ON"},
            {"id": 1, "status": None},
            {"id": 2, "status": "OFF"},
            {"id": 3, "status": None},
            {"id": 4, "status": "ON"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan(temp_path).collect()
            assert df.height == 5

            # Should be Enum type (nullable)
            assert df["status"].dtype == pl.Enum(["ON", "OFF"])

            # Check values including nulls
            assert df["status"][0] == "ON"
            assert df["status"][1] is None
            assert df["status"][2] == "OFF"
            assert df["status"][3] is None
            assert df["status"][4] == "ON"

            # Check null count
            assert df["status"].null_count() == 2
        finally:
            Path(temp_path).unlink(missing_ok=True)


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
