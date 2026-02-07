"""
Tests for Avro logical types.

Tests handling of:
- date (days since epoch)
- time-millis / time-micros
- timestamp-millis / timestamp-micros
- decimal (arbitrary precision)
- uuid (string logical type)
- duration (fixed 12 bytes)

Requirements tested:
- 1.6: Logical type support

Note: Some logical types have known issues with value interpretation.
Tests are marked xfail where jetliner reads the file but values differ
from expected due to implementation gaps.
"""

import tempfile
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import fastavro
import polars as pl
import pytest

import jetliner


# Schema with logical types
LOGICAL_TYPES_SCHEMA = {
    "type": "record",
    "name": "LogicalTypesRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Date - days since Unix epoch
        {
            "name": "birth_date",
            "type": {"type": "int", "logicalType": "date"},
        },
        # Time in milliseconds
        {
            "name": "start_time_ms",
            "type": {"type": "int", "logicalType": "time-millis"},
        },
        # Time in microseconds
        {
            "name": "start_time_us",
            "type": {"type": "long", "logicalType": "time-micros"},
        },
        # Timestamp in milliseconds (UTC)
        {
            "name": "created_at_ms",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
        # Timestamp in microseconds (UTC)
        {
            "name": "created_at_us",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },
        # UUID as string
        {
            "name": "uuid_field",
            "type": {"type": "string", "logicalType": "uuid"},
        },
        # Decimal with precision and scale
        {
            "name": "price",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 10,
                "scale": 2,
            },
        },
    ],
}


def create_logical_types_record(record_id: int) -> dict:
    """Create a record with logical type values."""
    # Use explicit dates to avoid leap year issues with timedelta(days=365)
    dates = [date(2020, 1, 1), date(2021, 1, 1), date(2022, 1, 1)]
    base_time = time(9, 30, 0)
    # Use UTC-aware datetimes to avoid timezone conversion issues
    base_datetime = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    return {
        "id": record_id,
        "birth_date": dates[record_id] if record_id < len(dates) else dates[0],
        "start_time_ms": base_time,
        "start_time_us": base_time,
        "created_at_ms": base_datetime + timedelta(days=record_id),
        "created_at_us": base_datetime + timedelta(days=record_id),
        "uuid_field": str(UUID(int=record_id + 1)),
        "price": Decimal(f"{100 + record_id}.{record_id:02d}"),
    }


@pytest.fixture
def logical_types_avro_file():
    """Create a temporary Avro file with logical types."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_logical_types_record(i) for i in range(3)]
        fastavro.writer(f, LOGICAL_TYPES_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestLogicalTypes:
    """Test reading Avro files with logical types."""

    def test_read_logical_types_file(self, logical_types_avro_file):
        """Test that logical types file can be read without errors."""
        df = jetliner.scan_avro(logical_types_avro_file).collect()
        assert df.height == 3

    def test_date_type(self, logical_types_avro_file):
        """Test date logical type is read correctly."""
        df = jetliner.scan_avro(logical_types_avro_file).collect()

        # Check dtype
        assert df["birth_date"].dtype == pl.Date

        # Check values
        assert df["birth_date"][0] == date(2020, 1, 1)
        assert df["birth_date"][1] == date(2021, 1, 1)
        assert df["birth_date"][2] == date(2022, 1, 1)

    def test_time_millis_type(self, logical_types_avro_file):
        """Test time-millis logical type is read correctly."""
        df = jetliner.scan_avro(logical_types_avro_file).collect()

        # Check dtype is Time
        assert df["start_time_ms"].dtype == pl.Time

        # Check value (9:30:00)
        assert df["start_time_ms"][0] == time(9, 30, 0)

    def test_time_micros_type(self, logical_types_avro_file):
        """Test time-micros logical type is read correctly."""
        df = jetliner.scan_avro(logical_types_avro_file).collect()

        # Check dtype is Time
        assert df["start_time_us"].dtype == pl.Time

        # Check value (9:30:00)
        assert df["start_time_us"][0] == time(9, 30, 0)

    def test_timestamp_millis_type(self, logical_types_avro_file):
        """Test timestamp-millis logical type is read correctly."""
        df = jetliner.scan_avro(logical_types_avro_file).collect()

        # Check dtype is Datetime
        assert df["created_at_ms"].dtype == pl.Datetime("ms", "UTC")

        # Check values - we use UTC-aware datetimes in the fixture
        expected = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert df["created_at_ms"][0] == expected

    def test_timestamp_micros_type(self, logical_types_avro_file):
        """Test timestamp-micros logical type is read correctly."""
        df = jetliner.scan_avro(logical_types_avro_file).collect()

        # Check dtype is Datetime with microsecond precision
        assert df["created_at_us"].dtype == pl.Datetime("us", "UTC")

    def test_uuid_type(self, logical_types_avro_file):
        """Test uuid logical type is read as string."""
        df = jetliner.scan_avro(logical_types_avro_file).collect()

        # UUID is stored as String
        assert df["uuid_field"].dtype == pl.String

        # Check it's a valid UUID string
        uuid_str = df["uuid_field"][0]
        assert UUID(uuid_str) is not None

    def test_decimal_type(self, logical_types_avro_file):
        """Test decimal logical type is read correctly."""
        df = jetliner.scan_avro(logical_types_avro_file).collect()

        # Check dtype is Decimal
        assert df["price"].dtype == pl.Decimal(precision=10, scale=2)

        # Check values
        assert float(df["price"][0]) == 100.00
        assert float(df["price"][1]) == 101.01
        assert float(df["price"][2]) == 102.02


# Schema with temporal logical types only (for isolated testing)
TEMPORAL_TYPES_SCHEMA = {
    "type": "record",
    "name": "TemporalTypesRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Date - days since Unix epoch
        {
            "name": "birth_date",
            "type": {"type": "int", "logicalType": "date"},
        },
        # Time in milliseconds
        {
            "name": "start_time_ms",
            "type": {"type": "int", "logicalType": "time-millis"},
        },
        # Time in microseconds
        {
            "name": "start_time_us",
            "type": {"type": "long", "logicalType": "time-micros"},
        },
        # Timestamp in milliseconds (UTC)
        {
            "name": "created_at_ms",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
        # Timestamp in microseconds (UTC)
        {
            "name": "created_at_us",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },
        # UUID as string
        {
            "name": "uuid_field",
            "type": {"type": "string", "logicalType": "uuid"},
        },
    ],
}


def create_temporal_record(record_id: int) -> dict:
    """Create a record with temporal logical type values."""
    # Use explicit dates to avoid leap year issues with timedelta(days=365)
    dates = [date(2020, 1, 1), date(2021, 1, 1), date(2022, 1, 1)]
    base_time = time(9, 30, 0)
    # Use UTC-aware datetimes to avoid timezone conversion issues
    base_datetime = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    return {
        "id": record_id,
        "birth_date": dates[record_id] if record_id < len(dates) else dates[0],
        "start_time_ms": base_time,
        "start_time_us": base_time,
        "created_at_ms": base_datetime + timedelta(days=record_id),
        "created_at_us": base_datetime + timedelta(days=record_id),
        "uuid_field": str(UUID(int=record_id + 1)),
    }


@pytest.fixture
def temporal_types_avro_file():
    """Create a temporary Avro file with temporal logical types."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_temporal_record(i) for i in range(3)]
        fastavro.writer(f, TEMPORAL_TYPES_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestTemporalLogicalTypes:
    """Test reading Avro files with temporal logical types."""

    def test_read_temporal_types_file(self, temporal_types_avro_file):
        """Test that temporal types file can be read without errors."""
        df = jetliner.scan_avro(temporal_types_avro_file).collect()
        assert df.height == 3

    def test_date_dtype(self, temporal_types_avro_file):
        """Test date logical type has correct dtype."""
        df = jetliner.scan_avro(temporal_types_avro_file).collect()
        assert df["birth_date"].dtype == pl.Date

    def test_time_millis_dtype(self, temporal_types_avro_file):
        """Test time-millis logical type has correct dtype."""
        df = jetliner.scan_avro(temporal_types_avro_file).collect()
        assert df["start_time_ms"].dtype == pl.Time

    def test_time_micros_dtype(self, temporal_types_avro_file):
        """Test time-micros logical type has correct dtype."""
        df = jetliner.scan_avro(temporal_types_avro_file).collect()
        assert df["start_time_us"].dtype == pl.Time

    def test_timestamp_millis_dtype(self, temporal_types_avro_file):
        """Test timestamp-millis logical type has correct dtype."""
        df = jetliner.scan_avro(temporal_types_avro_file).collect()
        assert df["created_at_ms"].dtype == pl.Datetime("ms", "UTC")

    def test_timestamp_micros_dtype(self, temporal_types_avro_file):
        """Test timestamp-micros logical type has correct dtype."""
        df = jetliner.scan_avro(temporal_types_avro_file).collect()
        assert df["created_at_us"].dtype == pl.Datetime("us", "UTC")

    def test_uuid_dtype(self, temporal_types_avro_file):
        """Test uuid logical type has correct dtype (String)."""
        df = jetliner.scan_avro(temporal_types_avro_file).collect()
        assert df["uuid_field"].dtype == pl.String

    def test_uuid_values(self, temporal_types_avro_file):
        """Test uuid values are valid UUID strings."""
        df = jetliner.scan_avro(temporal_types_avro_file).collect()
        for i in range(3):
            uuid_str = df["uuid_field"][i]
            # Should be a valid UUID
            assert UUID(uuid_str) is not None


# Schema with decimal only for isolated testing
DECIMAL_SCHEMA = {
    "type": "record",
    "name": "DecimalRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        {
            "name": "price",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 10,
                "scale": 2,
            },
        },
    ],
}


def create_decimal_record(record_id: int) -> dict:
    """Create a record with decimal value."""
    return {
        "id": record_id,
        "price": Decimal(f"{100 + record_id}.{record_id:02d}"),
    }


@pytest.fixture
def decimal_avro_file():
    """Create a temporary Avro file with decimal type."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_decimal_record(i) for i in range(3)]
        fastavro.writer(f, DECIMAL_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestDecimalType:
    """Test reading Avro files with decimal logical type."""

    def test_read_decimal_file(self, decimal_avro_file):
        """Test that decimal file can be read without errors."""
        df = jetliner.scan_avro(decimal_avro_file).collect()
        assert df.height == 3

    def test_decimal_dtype(self, decimal_avro_file):
        """Test decimal has correct dtype."""
        df = jetliner.scan_avro(decimal_avro_file).collect()
        assert df["price"].dtype == pl.Decimal(precision=10, scale=2)

    def test_decimal_values(self, decimal_avro_file):
        """Test decimal values are read correctly."""
        df = jetliner.scan_avro(decimal_avro_file).collect()
        assert float(df["price"][0]) == 100.00
        assert float(df["price"][1]) == 101.01
        assert float(df["price"][2]) == 102.02


# Schema with timestamp-nanos logical type (Avro 1.12.0+)
# Note: fastavro may not fully support timestamp-nanos, so we pass raw long values
TIMESTAMP_NANOS_SCHEMA = {
    "type": "record",
    "name": "TimestampNanosRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Timestamp in nanoseconds (UTC) - Avro 1.12.0+
        {
            "name": "created_at_ns",
            "type": {"type": "long", "logicalType": "timestamp-nanos"},
        },
    ],
}


def create_timestamp_nanos_record(record_id: int) -> dict:
    """Create a record with timestamp-nanos value.

    Since fastavro may not fully support timestamp-nanos logical type,
    we pass the raw nanosecond value as a long integer.
    """
    # Base datetime: 2020-01-01 12:00:00 UTC
    # In nanoseconds since epoch: 1577880000000000000
    base_ns = 1577880000_000_000_000  # 2020-01-01 12:00:00 UTC in nanoseconds
    # Add record_id days worth of nanoseconds
    ns_per_day = 24 * 60 * 60 * 1_000_000_000
    timestamp_ns = base_ns + (record_id * ns_per_day)

    return {
        "id": record_id,
        "created_at_ns": timestamp_ns,
    }


@pytest.fixture
def timestamp_nanos_avro_file():
    """Create a temporary Avro file with timestamp-nanos type."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_timestamp_nanos_record(i) for i in range(3)]
        fastavro.writer(f, TIMESTAMP_NANOS_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestTimestampNanosType:
    """Test reading Avro files with timestamp-nanos logical type (Avro 1.12.0+).

    Requirements tested:
    - 1.3: Arrow_Converter maps TimestampNanos to Datetime(Nanoseconds, UTC)
    - 1.5: Record_Decoder decodes timestamp-nanos as nanoseconds since epoch
    """

    def test_read_timestamp_nanos_file(self, timestamp_nanos_avro_file):
        """Test that timestamp-nanos file can be read without errors."""
        df = jetliner.scan_avro(timestamp_nanos_avro_file).collect()
        assert df.height == 3

    def test_timestamp_nanos_dtype(self, timestamp_nanos_avro_file):
        """Test timestamp-nanos has correct dtype: Datetime(ns, UTC).

        Validates Requirement 1.3: Arrow_Converter maps TimestampNanos to
        Datetime type with Nanoseconds precision and UTC timezone.
        """
        df = jetliner.scan_avro(timestamp_nanos_avro_file).collect()
        assert df["created_at_ns"].dtype == pl.Datetime("ns", "UTC")

    def test_timestamp_nanos_values(self, timestamp_nanos_avro_file):
        """Test timestamp-nanos values are correctly decoded.

        Validates Requirement 1.5: Record_Decoder decodes timestamp-nanos
        field as nanoseconds since Unix epoch.
        """
        df = jetliner.scan_avro(timestamp_nanos_avro_file).collect()

        # Expected values: 2020-01-01 12:00:00 UTC + record_id days
        expected_base = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        # Check each record
        for i in range(3):
            expected = expected_base + timedelta(days=i)
            actual = df["created_at_ns"][i]
            assert actual == expected, f"Record {i}: expected {expected}, got {actual}"

    def test_timestamp_nanos_nanosecond_precision(self, timestamp_nanos_avro_file):
        """Test that nanosecond precision is preserved.

        This test verifies that the nanosecond precision is maintained
        when reading timestamp-nanos values.
        """
        df = jetliner.scan_avro(timestamp_nanos_avro_file).collect()

        # The dtype should be nanoseconds, not milliseconds or microseconds
        dtype = df["created_at_ns"].dtype
        assert dtype == pl.Datetime("ns", "UTC")

        # Verify the time unit is nanoseconds
        assert dtype.time_unit == "ns"
        assert dtype.time_zone == "UTC"


# Schema with local-timestamp-nanos logical type (Avro 1.12.0+)
# Note: fastavro may not fully support local-timestamp-nanos, so we pass raw long values
LOCAL_TIMESTAMP_NANOS_SCHEMA = {
    "type": "record",
    "name": "LocalTimestampNanosRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Local timestamp in nanoseconds (no timezone) - Avro 1.12.0+
        {
            "name": "local_created_at_ns",
            "type": {"type": "long", "logicalType": "local-timestamp-nanos"},
        },
    ],
}


def create_local_timestamp_nanos_record(record_id: int) -> dict:
    """Create a record with local-timestamp-nanos value.

    Since fastavro may not fully support local-timestamp-nanos logical type,
    we pass the raw nanosecond value as a long integer.

    Local timestamps represent wall-clock time without timezone information.
    """
    # Base datetime: 2020-01-01 12:00:00 (local, no timezone)
    # In nanoseconds since epoch: 1577880000000000000
    base_ns = 1577880000_000_000_000  # 2020-01-01 12:00:00 in nanoseconds
    # Add record_id days worth of nanoseconds
    ns_per_day = 24 * 60 * 60 * 1_000_000_000
    timestamp_ns = base_ns + (record_id * ns_per_day)

    return {
        "id": record_id,
        "local_created_at_ns": timestamp_ns,
    }


@pytest.fixture
def local_timestamp_nanos_avro_file():
    """Create a temporary Avro file with local-timestamp-nanos type."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_local_timestamp_nanos_record(i) for i in range(3)]
        fastavro.writer(f, LOCAL_TIMESTAMP_NANOS_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestLocalTimestampNanosType:
    """Test reading Avro files with local-timestamp-nanos logical type (Avro 1.12.0+).

    Requirements tested:
    - 1.4: Arrow_Converter maps LocalTimestampNanos to Datetime(Nanoseconds, None)
    - 1.6: Record_Decoder decodes local-timestamp-nanos without timezone conversion
    """

    def test_read_local_timestamp_nanos_file(self, local_timestamp_nanos_avro_file):
        """Test that local-timestamp-nanos file can be read without errors."""
        df = jetliner.scan_avro(local_timestamp_nanos_avro_file).collect()
        assert df.height == 3

    def test_local_timestamp_nanos_dtype(self, local_timestamp_nanos_avro_file):
        """Test local-timestamp-nanos has correct dtype: Datetime(ns, None).

        Validates Requirement 1.4: Arrow_Converter maps LocalTimestampNanos to
        Datetime type with Nanoseconds precision and NO timezone (None).
        """
        df = jetliner.scan_avro(local_timestamp_nanos_avro_file).collect()
        # Local timestamps have no timezone (None)
        assert df["local_created_at_ns"].dtype == pl.Datetime("ns", None)

    def test_local_timestamp_nanos_values(self, local_timestamp_nanos_avro_file):
        """Test local-timestamp-nanos values are correctly decoded.

        Validates Requirement 1.6: Record_Decoder decodes local-timestamp-nanos
        field as nanoseconds without timezone conversion.
        """
        df = jetliner.scan_avro(local_timestamp_nanos_avro_file).collect()

        # Expected values: 2020-01-01 12:00:00 (local) + record_id days
        # Note: No timezone - these are wall-clock times
        expected_base = datetime(2020, 1, 1, 12, 0, 0)  # No timezone

        # Check each record
        for i in range(3):
            expected = expected_base + timedelta(days=i)
            actual = df["local_created_at_ns"][i]
            assert actual == expected, f"Record {i}: expected {expected}, got {actual}"

    def test_local_timestamp_nanos_no_timezone(self, local_timestamp_nanos_avro_file):
        """Test that local-timestamp-nanos has no timezone.

        This test verifies that local timestamps are stored without
        timezone information, as per the Avro specification.
        """
        df = jetliner.scan_avro(local_timestamp_nanos_avro_file).collect()

        # The dtype should be nanoseconds with no timezone
        dtype = df["local_created_at_ns"].dtype
        assert dtype == pl.Datetime("ns", None)

        # Verify the time unit is nanoseconds
        assert dtype.time_unit == "ns"
        # Verify there is no timezone
        assert dtype.time_zone is None

    def test_local_timestamp_nanos_nanosecond_precision(
        self, local_timestamp_nanos_avro_file
    ):
        """Test that nanosecond precision is preserved for local timestamps.

        This test verifies that the nanosecond precision is maintained
        when reading local-timestamp-nanos values.
        """
        df = jetliner.scan_avro(local_timestamp_nanos_avro_file).collect()

        # The dtype should be nanoseconds, not milliseconds or microseconds
        dtype = df["local_created_at_ns"].dtype
        assert dtype.time_unit == "ns"


# Schema with duration logical type
# Duration is stored as fixed[12] bytes: months (u32 LE), days (u32 LE), milliseconds (u32 LE)
DURATION_SCHEMA = {
    "type": "record",
    "name": "DurationRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Duration logical type on fixed[12]
        {
            "name": "duration_field",
            "type": {
                "type": "fixed",
                "name": "duration_fixed",
                "size": 12,
                "logicalType": "duration",
            },
        },
    ],
}


def create_duration_bytes(months: int, days: int, milliseconds: int) -> bytes:
    """Create duration bytes in Avro format.

    Avro duration is stored as fixed[12] bytes containing three
    little-endian unsigned 32-bit integers:
    - months (4 bytes)
    - days (4 bytes)
    - milliseconds (4 bytes)
    """
    import struct

    return struct.pack("<III", months, days, milliseconds)


def create_duration_record(record_id: int) -> dict:
    """Create a record with duration value.

    Creates test durations with varying months, days, and milliseconds
    to verify all components are correctly interpreted.
    """
    # Test cases with different combinations of months, days, milliseconds
    test_durations = [
        # (months, days, milliseconds)
        (0, 0, 1000),  # 1 second only
        (0, 1, 0),  # 1 day only
        (1, 0, 0),  # 1 month only (should be approximated as 30 days)
        (1, 2, 3000),  # 1 month, 2 days, 3 seconds
        (2, 15, 43200000),  # 2 months, 15 days, 12 hours
    ]

    months, days, milliseconds = test_durations[record_id % len(test_durations)]

    return {
        "id": record_id,
        "duration_field": create_duration_bytes(months, days, milliseconds),
    }


@pytest.fixture
def duration_avro_file():
    """Create a temporary Avro file with duration type."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_duration_record(i) for i in range(5)]
        fastavro.writer(f, DURATION_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestDurationType:
    """Test reading Avro files with duration logical type.

    Requirements tested:
    - 2.1: Jetliner returns DataFrame with Duration columns
    - 2.2: Jetliner correctly interprets fixed[12] bytes as months, days, milliseconds
    - 2.3: Jetliner approximates months as 30 days for conversion
    """

    def test_read_duration_file(self, duration_avro_file):
        """Test that duration file can be read without errors.

        Validates Requirement 2.1: Jetliner returns DataFrame with Duration columns.
        """
        df = jetliner.scan_avro(duration_avro_file).collect()
        assert df.height == 5

    def test_duration_dtype(self, duration_avro_file):
        """Test duration has correct dtype: Duration(us).

        Validates Requirement 2.1: Jetliner returns DataFrame with Duration columns.
        Duration is mapped to Polars Duration type with microsecond precision.
        """
        df = jetliner.scan_avro(duration_avro_file).collect()
        assert df["duration_field"].dtype == pl.Duration("us")

    def test_duration_milliseconds_only(self, duration_avro_file):
        """Test duration with only milliseconds component.

        Validates Requirement 2.2: Jetliner correctly interprets milliseconds.
        Record 0: 0 months, 0 days, 1000 milliseconds = 1 second
        """
        df = jetliner.scan_avro(duration_avro_file).collect()

        # Record 0: 1000 milliseconds = 1,000,000 microseconds
        expected_us = 1000 * 1000  # 1 second in microseconds
        actual = df["duration_field"][0]
        assert actual == timedelta(microseconds=expected_us)

    def test_duration_days_only(self, duration_avro_file):
        """Test duration with only days component.

        Validates Requirement 2.2: Jetliner correctly interprets days.
        Record 1: 0 months, 1 day, 0 milliseconds
        """
        df = jetliner.scan_avro(duration_avro_file).collect()

        # Record 1: 1 day = 24 * 60 * 60 * 1,000,000 microseconds
        expected_us = 1 * 24 * 60 * 60 * 1_000_000
        actual = df["duration_field"][1]
        assert actual == timedelta(microseconds=expected_us)

    def test_duration_months_approximated_as_30_days(self, duration_avro_file):
        """Test duration with months component approximated as 30 days.

        Validates Requirement 2.3: Jetliner approximates months as 30 days.
        Record 2: 1 month, 0 days, 0 milliseconds = 30 days
        """
        df = jetliner.scan_avro(duration_avro_file).collect()

        # Record 2: 1 month = 30 days = 30 * 24 * 60 * 60 * 1,000,000 microseconds
        expected_us = 30 * 24 * 60 * 60 * 1_000_000
        actual = df["duration_field"][2]
        assert actual == timedelta(microseconds=expected_us)

    def test_duration_all_components(self, duration_avro_file):
        """Test duration with all components: months, days, milliseconds.

        Validates Requirements 2.2, 2.3: All components correctly interpreted.
        Record 3: 1 month, 2 days, 3000 milliseconds
        = 30 days + 2 days + 3 seconds
        = 32 days + 3 seconds
        """
        df = jetliner.scan_avro(duration_avro_file).collect()

        # Record 3: 1 month (30 days) + 2 days + 3000 ms
        months_us = 30 * 24 * 60 * 60 * 1_000_000  # 30 days in microseconds
        days_us = 2 * 24 * 60 * 60 * 1_000_000  # 2 days in microseconds
        ms_us = 3000 * 1000  # 3000 milliseconds in microseconds
        expected_us = months_us + days_us + ms_us

        actual = df["duration_field"][3]
        assert actual == timedelta(microseconds=expected_us)

    def test_duration_larger_values(self, duration_avro_file):
        """Test duration with larger values.

        Validates Requirements 2.2, 2.3: Larger values correctly interpreted.
        Record 4: 2 months, 15 days, 43200000 milliseconds (12 hours)
        = 60 days + 15 days + 12 hours
        = 75 days + 12 hours
        """
        df = jetliner.scan_avro(duration_avro_file).collect()

        # Record 4: 2 months (60 days) + 15 days + 43200000 ms (12 hours)
        months_us = 2 * 30 * 24 * 60 * 60 * 1_000_000  # 60 days in microseconds
        days_us = 15 * 24 * 60 * 60 * 1_000_000  # 15 days in microseconds
        ms_us = 43200000 * 1000  # 12 hours in microseconds
        expected_us = months_us + days_us + ms_us

        actual = df["duration_field"][4]
        assert actual == timedelta(microseconds=expected_us)


# Schema with UUID logical type on fixed[16]
# UUID can be stored as either string or fixed[16] bytes
UUID_FIXED16_SCHEMA = {
    "type": "record",
    "name": "UuidFixed16Record",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # UUID logical type on fixed[16]
        {
            "name": "uuid_field",
            "type": {
                "type": "fixed",
                "name": "uuid_fixed",
                "size": 16,
                "logicalType": "uuid",
            },
        },
    ],
}


def uuid_to_bytes(uuid_obj: UUID) -> bytes:
    """Convert a UUID object to 16 bytes.

    Returns the UUID as 16 bytes in big-endian format (standard UUID byte order).
    """
    return uuid_obj.bytes


def create_uuid_fixed16_record(record_id: int) -> dict:
    """Create a record with UUID stored as fixed[16] bytes.

    Creates test UUIDs with known values to verify correct byte-to-string conversion.
    """
    # Test UUIDs with known values
    test_uuids = [
        # Standard UUID format: 8-4-4-4-12 hex digits
        UUID("550e8400-e29b-41d4-a716-446655440000"),  # Well-known test UUID
        UUID("00000000-0000-0000-0000-000000000001"),  # Minimal UUID
        UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),  # Maximum UUID
        UUID("12345678-1234-5678-1234-567812345678"),  # Pattern UUID
        UUID("a0b1c2d3-e4f5-6789-abcd-ef0123456789"),  # Mixed case hex
    ]

    uuid_obj = test_uuids[record_id % len(test_uuids)]

    return {
        "id": record_id,
        "uuid_field": uuid_to_bytes(uuid_obj),
    }


@pytest.fixture
def uuid_fixed16_avro_file():
    """Create a temporary Avro file with UUID stored as fixed[16]."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_uuid_fixed16_record(i) for i in range(5)]
        fastavro.writer(f, UUID_FIXED16_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestUuidFixed16Type:
    """Test reading Avro files with UUID logical type on fixed[16].

    Requirements tested:
    - 3.1: Jetliner returns DataFrame with String columns containing valid UUID strings
    - 3.2: Jetliner correctly formats 16 bytes as standard UUID string (8-4-4-4-12 format)
    """

    def test_read_uuid_fixed16_file(self, uuid_fixed16_avro_file):
        """Test that UUID fixed[16] file can be read without errors.

        Validates Requirement 3.1: Jetliner returns DataFrame with String columns.
        """
        df = jetliner.scan_avro(uuid_fixed16_avro_file).collect()
        assert df.height == 5

    def test_uuid_fixed16_dtype(self, uuid_fixed16_avro_file):
        """Test UUID fixed[16] has correct dtype: String.

        Validates Requirement 3.1: Jetliner returns DataFrame with String columns
        containing valid UUID strings.
        """
        df = jetliner.scan_avro(uuid_fixed16_avro_file).collect()
        assert df["uuid_field"].dtype == pl.String

    def test_uuid_fixed16_format(self, uuid_fixed16_avro_file):
        """Test UUID bytes are correctly formatted as standard UUID string.

        Validates Requirement 3.2: Jetliner correctly formats 16 bytes as
        standard UUID string (8-4-4-4-12 format like "550e8400-e29b-41d4-a716-446655440000").
        """
        df = jetliner.scan_avro(uuid_fixed16_avro_file).collect()

        # Check each UUID is in valid format
        for i in range(5):
            uuid_str = df["uuid_field"][i]
            # Should be a valid UUID string that can be parsed
            parsed_uuid = UUID(uuid_str)
            assert parsed_uuid is not None

            # Verify format: 8-4-4-4-12 (36 characters total with dashes)
            assert len(uuid_str) == 36
            parts = uuid_str.split("-")
            assert len(parts) == 5
            assert len(parts[0]) == 8  # First group: 8 hex chars
            assert len(parts[1]) == 4  # Second group: 4 hex chars
            assert len(parts[2]) == 4  # Third group: 4 hex chars
            assert len(parts[3]) == 4  # Fourth group: 4 hex chars
            assert len(parts[4]) == 12  # Fifth group: 12 hex chars

    def test_uuid_fixed16_known_values(self, uuid_fixed16_avro_file):
        """Test UUID fixed[16] values match expected UUIDs.

        Validates Requirement 3.2: Jetliner correctly formats 16 bytes as
        standard UUID string.
        """
        df = jetliner.scan_avro(uuid_fixed16_avro_file).collect()

        # Expected UUIDs (lowercase, as per standard UUID string format)
        expected_uuids = [
            "550e8400-e29b-41d4-a716-446655440000",
            "00000000-0000-0000-0000-000000000001",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "12345678-1234-5678-1234-567812345678",
            "a0b1c2d3-e4f5-6789-abcd-ef0123456789",
        ]

        for i, expected in enumerate(expected_uuids):
            actual = df["uuid_field"][i]
            # Compare as UUID objects to handle case differences
            assert UUID(actual) == UUID(expected), (
                f"Record {i}: expected {expected}, got {actual}"
            )

    def test_uuid_fixed16_roundtrip(self, uuid_fixed16_avro_file):
        """Test UUID values can be parsed back to UUID objects.

        Validates Requirement 3.1: String columns contain valid UUID strings.
        """
        df = jetliner.scan_avro(uuid_fixed16_avro_file).collect()

        # All UUIDs should be parseable
        for i in range(5):
            uuid_str = df["uuid_field"][i]
            # Should not raise an exception
            uuid_obj = UUID(uuid_str)
            # Converting back to string should give same format
            assert str(uuid_obj) == uuid_str.lower()


# Schema with local-timestamp-millis logical type
# Local timestamps have no timezone - they represent wall clock time
LOCAL_TIMESTAMP_MILLIS_SCHEMA = {
    "type": "record",
    "name": "LocalTimestampMillisRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Local timestamp in milliseconds (no timezone)
        {
            "name": "local_created_at_ms",
            "type": {"type": "long", "logicalType": "local-timestamp-millis"},
        },
    ],
}


def create_local_timestamp_millis_record(record_id: int) -> dict:
    """Create a record with local-timestamp-millis value.

    Local timestamps represent wall clock time without timezone.
    The value is milliseconds since midnight 1970-01-01 in local time.
    """
    # Base: 2020-01-01 12:00:00.000 local time
    # In milliseconds since epoch (treating as local time)
    base_ms = 1577880000000  # 2020-01-01T12:00:00.000
    # Add record_id days worth of milliseconds
    ms_per_day = 24 * 60 * 60 * 1000
    timestamp_ms = base_ms + (record_id * ms_per_day)

    return {
        "id": record_id,
        "local_created_at_ms": timestamp_ms,
    }


@pytest.fixture
def local_timestamp_millis_avro_file():
    """Create a temporary Avro file with local-timestamp-millis type."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_local_timestamp_millis_record(i) for i in range(3)]
        fastavro.writer(f, LOCAL_TIMESTAMP_MILLIS_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestLocalTimestampMillisType:
    """Test reading Avro files with local-timestamp-millis logical type.

    Local timestamps represent wall clock time without timezone information.
    They should map to Polars Datetime with no timezone (None).
    """

    def test_read_local_timestamp_millis_file(self, local_timestamp_millis_avro_file):
        """Test that local-timestamp-millis file can be read without errors."""
        df = jetliner.scan_avro(local_timestamp_millis_avro_file).collect()
        assert df.height == 3

    def test_local_timestamp_millis_dtype(self, local_timestamp_millis_avro_file):
        """Test local-timestamp-millis has correct dtype: Datetime(ms, None).

        Local timestamps should have NO timezone (None), distinguishing them
        from regular timestamps which have UTC timezone.
        """
        df = jetliner.scan_avro(local_timestamp_millis_avro_file).collect()
        assert df["local_created_at_ms"].dtype == pl.Datetime("ms", None)

    def test_local_timestamp_millis_no_timezone(self, local_timestamp_millis_avro_file):
        """Test that local-timestamp-millis has no timezone.

        This is the key distinction from timestamp-millis which has UTC timezone.
        """
        df = jetliner.scan_avro(local_timestamp_millis_avro_file).collect()

        dtype = df["local_created_at_ms"].dtype
        assert dtype.time_unit == "ms"
        assert dtype.time_zone is None

    def test_local_timestamp_millis_values(self, local_timestamp_millis_avro_file):
        """Test local-timestamp-millis values are correctly decoded."""
        df = jetliner.scan_avro(local_timestamp_millis_avro_file).collect()

        for i in range(3):
            # Convert to datetime for comparison (treating as naive/local)
            expected = datetime(2020, 1, 1, 12, 0, 0) + timedelta(days=i)
            actual = df["local_created_at_ms"][i]
            assert actual == expected, f"Record {i}: expected {expected}, got {actual}"


# Schema with local-timestamp-micros logical type
LOCAL_TIMESTAMP_MICROS_SCHEMA = {
    "type": "record",
    "name": "LocalTimestampMicrosRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Local timestamp in microseconds (no timezone)
        {
            "name": "local_created_at_us",
            "type": {"type": "long", "logicalType": "local-timestamp-micros"},
        },
    ],
}


def create_local_timestamp_micros_record(record_id: int) -> dict:
    """Create a record with local-timestamp-micros value.

    Local timestamps represent wall clock time without timezone.
    The value is microseconds since midnight 1970-01-01 in local time.
    """
    # Base: 2020-01-01 12:00:00.000000 local time
    # In microseconds since epoch (treating as local time)
    base_us = 1577880000000000  # 2020-01-01T12:00:00.000000
    # Add record_id days worth of microseconds
    us_per_day = 24 * 60 * 60 * 1_000_000
    timestamp_us = base_us + (record_id * us_per_day)

    return {
        "id": record_id,
        "local_created_at_us": timestamp_us,
    }


@pytest.fixture
def local_timestamp_micros_avro_file():
    """Create a temporary Avro file with local-timestamp-micros type."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_local_timestamp_micros_record(i) for i in range(3)]
        fastavro.writer(f, LOCAL_TIMESTAMP_MICROS_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestLocalTimestampMicrosType:
    """Test reading Avro files with local-timestamp-micros logical type.

    Local timestamps represent wall clock time without timezone information.
    They should map to Polars Datetime with no timezone (None).
    """

    def test_read_local_timestamp_micros_file(self, local_timestamp_micros_avro_file):
        """Test that local-timestamp-micros file can be read without errors."""
        df = jetliner.scan_avro(local_timestamp_micros_avro_file).collect()
        assert df.height == 3

    def test_local_timestamp_micros_dtype(self, local_timestamp_micros_avro_file):
        """Test local-timestamp-micros has correct dtype: Datetime(us, None).

        Local timestamps should have NO timezone (None), distinguishing them
        from regular timestamps which have UTC timezone.
        """
        df = jetliner.scan_avro(local_timestamp_micros_avro_file).collect()
        assert df["local_created_at_us"].dtype == pl.Datetime("us", None)

    def test_local_timestamp_micros_no_timezone(self, local_timestamp_micros_avro_file):
        """Test that local-timestamp-micros has no timezone.

        This is the key distinction from timestamp-micros which has UTC timezone.
        """
        df = jetliner.scan_avro(local_timestamp_micros_avro_file).collect()

        dtype = df["local_created_at_us"].dtype
        assert dtype.time_unit == "us"
        assert dtype.time_zone is None

    def test_local_timestamp_micros_values(self, local_timestamp_micros_avro_file):
        """Test local-timestamp-micros values are correctly decoded."""
        df = jetliner.scan_avro(local_timestamp_micros_avro_file).collect()

        for i in range(3):
            # Convert to datetime for comparison (treating as naive/local)
            expected = datetime(2020, 1, 1, 12, 0, 0) + timedelta(days=i)
            actual = df["local_created_at_us"][i]
            assert actual == expected, f"Record {i}: expected {expected}, got {actual}"

    def test_local_timestamp_micros_precision(self, local_timestamp_micros_avro_file):
        """Test that microsecond precision is preserved."""
        df = jetliner.scan_avro(local_timestamp_micros_avro_file).collect()

        dtype = df["local_created_at_us"].dtype
        assert dtype.time_unit == "us"


# ============================================================================
# Big-Decimal Logical Type Tests (Avro 1.12.0+)
# ============================================================================

# Schema with big-decimal logical type
# Big-decimal is stored as bytes containing [scale_varint][unscaled_bytes]
BIG_DECIMAL_SCHEMA = {
    "type": "record",
    "name": "BigDecimalRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Big-decimal logical type on bytes (Avro 1.12.0+)
        {
            "name": "value",
            "type": {"type": "bytes", "logicalType": "big-decimal"},
        },
    ],
}


def encode_zigzag(value: int) -> bytes:
    """Encode a signed integer as zigzag varint."""
    # Zigzag encoding: (n << 1) ^ (n >> 63) for 64-bit
    if value >= 0:
        zigzag = value << 1
    else:
        zigzag = ((-value - 1) << 1) | 1

    # Varint encoding
    result = bytearray()
    while zigzag > 0x7F:
        result.append((zigzag & 0x7F) | 0x80)
        zigzag >>= 7
    result.append(zigzag & 0x7F)
    return bytes(result)


def int_to_big_endian_twos_complement(value: int) -> bytes:
    """Convert an integer to big-endian two's complement bytes.

    Returns the minimal representation (no unnecessary leading bytes).
    """
    if value == 0:
        return b"\x00"

    # Determine the number of bytes needed
    if value > 0:
        # For positive numbers, we need enough bytes to hold the value
        # plus potentially one more byte if the high bit would be set
        byte_length = (value.bit_length() + 8) // 8
    else:
        # For negative numbers, we need enough bytes for the magnitude
        byte_length = ((-value - 1).bit_length() + 8) // 8

    # Convert to bytes using two's complement
    if value >= 0:
        result = value.to_bytes(byte_length, byteorder="big", signed=False)
        # Remove leading zeros, but keep one if next byte has high bit set
        while len(result) > 1 and result[0] == 0 and (result[1] & 0x80) == 0:
            result = result[1:]
    else:
        result = value.to_bytes(byte_length, byteorder="big", signed=True)
        # Remove leading 0xFF bytes, but keep one if next byte doesn't have high bit
        while len(result) > 1 and result[0] == 0xFF and (result[1] & 0x80) != 0:
            result = result[1:]

    return result


def create_big_decimal_bytes(unscaled: int, scale: int) -> bytes:
    """Create big-decimal bytes in Avro format.

    Big-decimal encoding (Avro 1.12.0+):
    - Scale: varint (zigzag encoded)
    - Unscaled value: big-endian two's complement bytes
    """
    scale_bytes = encode_zigzag(scale)
    unscaled_bytes = int_to_big_endian_twos_complement(unscaled)
    return scale_bytes + unscaled_bytes


def create_big_decimal_record(record_id: int) -> dict:
    """Create a record with big-decimal value.

    Creates test big-decimals with various scales and values.
    """
    # Test cases: (unscaled, scale, expected_string)
    test_values = [
        # Scale 0: integer values
        (12345, 0),  # "12345"
        (-67890, 0),  # "-67890"
        # Scale 2: typical currency
        (12345, 2),  # "123.45"
        (-9999, 2),  # "-99.99"
        # Scale 10: high precision
        (123456789012345, 10),  # "12345.6789012345"
        # Scale 18: very high precision
        (1000000000000000000, 18),  # "1.000000000000000000"
        # Small values with high scale (leading zeros after decimal)
        (5, 3),  # "0.005"
        (-123, 5),  # "-0.00123"
    ]

    unscaled, scale = test_values[record_id % len(test_values)]

    return {
        "id": record_id,
        "value": create_big_decimal_bytes(unscaled, scale),
    }


@pytest.fixture
def big_decimal_avro_file():
    """Create a temporary Avro file with big-decimal type."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_big_decimal_record(i) for i in range(8)]
        fastavro.writer(f, BIG_DECIMAL_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestBigDecimalType:
    """Test reading Avro files with big-decimal logical type (Avro 1.12.0+).

    Requirements tested:
    - 8.2: Arrow_Converter maps BigDecimal to String type
    - 8.3: Record_Decoder correctly decodes big-decimal values
    - 8.4: Decoded string preserves exact decimal representation
    """

    def test_read_big_decimal_file(self, big_decimal_avro_file):
        """Test that big-decimal file can be read without errors.

        Validates Requirement 8.2: BigDecimal maps to String type.
        """
        df = jetliner.scan_avro(big_decimal_avro_file).collect()
        assert df.height == 8

    def test_big_decimal_dtype(self, big_decimal_avro_file):
        """Test big-decimal has correct dtype: String.

        Validates Requirement 8.2: Arrow_Converter maps BigDecimal to String
        to preserve exact decimal representation without precision loss.
        """
        df = jetliner.scan_avro(big_decimal_avro_file).collect()
        assert df["value"].dtype == pl.String

    def test_big_decimal_scale_0(self, big_decimal_avro_file):
        """Test big-decimal with scale=0 (integer values).

        Validates Requirement 8.3, 8.4: Integer values decoded correctly.
        """
        df = jetliner.scan_avro(big_decimal_avro_file).collect()

        # Record 0: unscaled=12345, scale=0 -> "12345"
        assert df["value"][0] == "12345"

        # Record 1: unscaled=-67890, scale=0 -> "-67890"
        assert df["value"][1] == "-67890"

    def test_big_decimal_scale_2(self, big_decimal_avro_file):
        """Test big-decimal with scale=2 (typical currency).

        Validates Requirement 8.3, 8.4: Currency values decoded correctly.
        """
        df = jetliner.scan_avro(big_decimal_avro_file).collect()

        # Record 2: unscaled=12345, scale=2 -> "123.45"
        assert df["value"][2] == "123.45"

        # Record 3: unscaled=-9999, scale=2 -> "-99.99"
        assert df["value"][3] == "-99.99"

    def test_big_decimal_high_precision(self, big_decimal_avro_file):
        """Test big-decimal with high precision (scale=10, 18).

        Validates Requirement 8.3, 8.4: High precision values decoded correctly.
        """
        df = jetliner.scan_avro(big_decimal_avro_file).collect()

        # Record 4: unscaled=123456789012345, scale=10 -> "12345.6789012345"
        assert df["value"][4] == "12345.6789012345"

        # Record 5: unscaled=1000000000000000000, scale=18 -> "1.000000000000000000"
        assert df["value"][5] == "1.000000000000000000"

    def test_big_decimal_leading_zeros(self, big_decimal_avro_file):
        """Test big-decimal with leading zeros after decimal point.

        Validates Requirement 8.3, 8.4: Values like 0.005 decoded correctly.
        """
        df = jetliner.scan_avro(big_decimal_avro_file).collect()

        # Record 6: unscaled=5, scale=3 -> "0.005"
        assert df["value"][6] == "0.005"

        # Record 7: unscaled=-123, scale=5 -> "-0.00123"
        assert df["value"][7] == "-0.00123"

    def test_big_decimal_values_parseable(self, big_decimal_avro_file):
        """Test that all big-decimal string values are parseable as Decimal.

        Validates Requirement 8.4: String representation is valid decimal.
        """
        df = jetliner.scan_avro(big_decimal_avro_file).collect()

        for i in range(df.height):
            value_str = df["value"][i]
            # Should be parseable as Decimal without error
            parsed = Decimal(value_str)
            assert parsed is not None, f"Record {i}: Failed to parse '{value_str}'"


# ============================================================================
# Big-Decimal Edge Case Tests (Task 13.3)
# ============================================================================


class TestBigDecimalEdgeCases:
    """Test edge cases for big-decimal logical type.

    Requirements tested:
    - 8.3: Record_Decoder correctly decodes big-decimal edge cases
    - 8.4: Decoded string preserves exact decimal representation
    """

    def test_big_decimal_zero_value(self):
        """Test big-decimal with zero value.

        Validates Requirement 8.3, 8.4: Zero value decoded correctly.
        """
        schema = {
            "type": "record",
            "name": "ZeroDecimalRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": {"type": "bytes", "logicalType": "big-decimal"}},
            ],
        }

        # Zero with scale 0: unscaled=0, scale=0 -> "0"
        zero_bytes = create_big_decimal_bytes(0, 0)
        records = [{"id": 0, "value": zero_bytes}]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan_avro(temp_path).collect()
            assert df["value"][0] == "0"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_big_decimal_zero_with_scale(self):
        """Test big-decimal zero with various scales.

        Validates Requirement 8.3, 8.4: Zero with scale decoded correctly.
        """
        schema = {
            "type": "record",
            "name": "ZeroScaleRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": {"type": "bytes", "logicalType": "big-decimal"}},
            ],
        }

        # Zero with scale 2: unscaled=0, scale=2 -> "0.00"
        records = [
            {"id": 0, "value": create_big_decimal_bytes(0, 2)},
            {"id": 1, "value": create_big_decimal_bytes(0, 5)},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan_avro(temp_path).collect()
            assert df["value"][0] == "0.00"
            assert df["value"][1] == "0.00000"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_big_decimal_large_scale(self):
        """Test big-decimal with large scale values.

        Validates Requirement 8.3, 8.4: Large scale values decoded correctly.
        """
        schema = {
            "type": "record",
            "name": "LargeScaleRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": {"type": "bytes", "logicalType": "big-decimal"}},
            ],
        }

        # Large scale: unscaled=1, scale=30 -> "0.000000000000000000000000000001"
        records = [
            {"id": 0, "value": create_big_decimal_bytes(1, 30)},
            {"id": 1, "value": create_big_decimal_bytes(123, 25)},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan_avro(temp_path).collect()
            # Verify values are parseable and have correct scale
            val0 = Decimal(df["value"][0])
            val1 = Decimal(df["value"][1])
            assert val0 == Decimal("0.000000000000000000000000000001")
            assert val1 == Decimal("0.0000000000000000000000123")
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_big_decimal_large_unscaled_value(self):
        """Test big-decimal with large unscaled values.

        Validates Requirement 8.3, 8.4: Large values decoded correctly.
        """
        schema = {
            "type": "record",
            "name": "LargeValueRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": {"type": "bytes", "logicalType": "big-decimal"}},
            ],
        }

        # Large unscaled value (beyond i64 range)
        large_value = 10**30  # 1 followed by 30 zeros
        records = [
            {"id": 0, "value": create_big_decimal_bytes(large_value, 0)},
            {"id": 1, "value": create_big_decimal_bytes(large_value, 10)},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan_avro(temp_path).collect()
            # Verify values are parseable
            val0 = Decimal(df["value"][0])
            val1 = Decimal(df["value"][1])
            assert val0 == Decimal("1" + "0" * 30)
            assert val1 == Decimal("1" + "0" * 20)  # 10^30 / 10^10 = 10^20
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_big_decimal_negative_large_value(self):
        """Test big-decimal with large negative values.

        Validates Requirement 8.3, 8.4: Large negative values decoded correctly.
        """
        schema = {
            "type": "record",
            "name": "NegativeLargeRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": {"type": "bytes", "logicalType": "big-decimal"}},
            ],
        }

        # Large negative value
        large_negative = -(10**20)
        records = [
            {"id": 0, "value": create_big_decimal_bytes(large_negative, 0)},
            {"id": 1, "value": create_big_decimal_bytes(large_negative, 5)},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan_avro(temp_path).collect()
            val0 = Decimal(df["value"][0])
            val1 = Decimal(df["value"][1])
            assert val0 == Decimal("-" + "1" + "0" * 20)
            assert val1 == Decimal("-" + "1" + "0" * 15)  # -10^20 / 10^5 = -10^15
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_big_decimal_single_digit_values(self):
        """Test big-decimal with single digit unscaled values.

        Validates Requirement 8.3, 8.4: Small values decoded correctly.
        """
        schema = {
            "type": "record",
            "name": "SingleDigitRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": {"type": "bytes", "logicalType": "big-decimal"}},
            ],
        }

        records = [
            {"id": 0, "value": create_big_decimal_bytes(1, 0)},  # "1"
            {"id": 1, "value": create_big_decimal_bytes(1, 1)},  # "0.1"
            {"id": 2, "value": create_big_decimal_bytes(1, 5)},  # "0.00001"
            {"id": 3, "value": create_big_decimal_bytes(-1, 0)},  # "-1"
            {"id": 4, "value": create_big_decimal_bytes(-1, 3)},  # "-0.001"
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan_avro(temp_path).collect()
            assert df["value"][0] == "1"
            assert df["value"][1] == "0.1"
            assert df["value"][2] == "0.00001"
            assert df["value"][3] == "-1"
            assert df["value"][4] == "-0.001"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_big_decimal_boundary_values(self):
        """Test big-decimal with boundary values (max i64, etc).

        Validates Requirement 8.3, 8.4: Boundary values decoded correctly.
        """
        schema = {
            "type": "record",
            "name": "BoundaryRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": {"type": "bytes", "logicalType": "big-decimal"}},
            ],
        }

        # i64 max and min values
        i64_max = 2**63 - 1
        i64_min = -(2**63)

        records = [
            {"id": 0, "value": create_big_decimal_bytes(i64_max, 0)},
            {"id": 1, "value": create_big_decimal_bytes(i64_min, 0)},
            {"id": 2, "value": create_big_decimal_bytes(i64_max, 10)},
        ]

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
            fastavro.writer(f, schema, records)
            temp_path = f.name

        try:
            df = jetliner.scan_avro(temp_path).collect()
            # Verify values are parseable and correct
            val0 = Decimal(df["value"][0])
            val1 = Decimal(df["value"][1])
            val2 = Decimal(df["value"][2])
            assert val0 == Decimal(str(i64_max))
            assert val1 == Decimal(str(i64_min))
            # i64_max with scale 10
            expected_val2 = Decimal(str(i64_max)) / Decimal(10**10)
            assert val2 == expected_val2
        finally:
            Path(temp_path).unlink(missing_ok=True)


# ============================================================================
# Unknown/Custom Logical Type Tests
# ============================================================================

# Schema with unknown/custom logical type
UNKNOWN_LOGICAL_TYPE_SCHEMA = {
    "type": "record",
    "name": "UnknownLogicalTypeRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Custom logical type on string base - should be treated as string
        {
            "name": "custom_string",
            "type": {"type": "string", "logicalType": "my-custom-type"},
        },
        # Custom logical type on int base - should be treated as int
        {
            "name": "custom_int",
            "type": {"type": "int", "logicalType": "special-counter"},
        },
        # Custom logical type on long base - should be treated as long
        {
            "name": "custom_long",
            "type": {"type": "long", "logicalType": "nanosecond-offset"},
        },
        # Custom logical type on bytes base - should be treated as bytes
        {
            "name": "custom_bytes",
            "type": {"type": "bytes", "logicalType": "encrypted-data"},
        },
    ],
}


def create_unknown_logical_type_record(record_id: int) -> dict:
    """Create a record with unknown/custom logical type values.

    Per Avro spec, unknown logical types should be treated as their base type.
    """
    return {
        "id": record_id,
        "custom_string": f"custom-value-{record_id}",
        "custom_int": 100 + record_id,
        "custom_long": 1000000000 + record_id,
        "custom_bytes": f"bytes-{record_id}".encode("utf-8"),
    }


@pytest.fixture
def unknown_logical_type_avro_file():
    """Create a temporary Avro file with unknown/custom logical types."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_unknown_logical_type_record(i) for i in range(3)]
        fastavro.writer(f, UNKNOWN_LOGICAL_TYPE_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestUnknownLogicalTypes:
    """Test reading Avro files with unknown/custom logical types.

    Per Avro specification, unknown logical types should be ignored and
    the data should be treated as the underlying base type. Jetliner
    preserves the logical type name in the schema for inspection while
    treating the data as the base type.
    """

    def test_read_unknown_logical_types_file(self, unknown_logical_type_avro_file):
        """Test that files with unknown logical types can be read without errors."""
        df = jetliner.scan_avro(unknown_logical_type_avro_file).collect()
        assert df.height == 3

    def test_unknown_string_logical_type_treated_as_string(
        self, unknown_logical_type_avro_file
    ):
        """Test unknown logical type on string base is treated as string."""
        df = jetliner.scan_avro(unknown_logical_type_avro_file).collect()

        # Should be String dtype (base type)
        assert df["custom_string"].dtype == pl.String

        # Values should be readable as strings
        assert df["custom_string"][0] == "custom-value-0"
        assert df["custom_string"][1] == "custom-value-1"
        assert df["custom_string"][2] == "custom-value-2"

    def test_unknown_int_logical_type_treated_as_int(
        self, unknown_logical_type_avro_file
    ):
        """Test unknown logical type on int base is treated as int."""
        df = jetliner.scan_avro(unknown_logical_type_avro_file).collect()

        # Should be Int32 dtype (base type for Avro int)
        assert df["custom_int"].dtype == pl.Int32

        # Values should be readable as integers
        assert df["custom_int"][0] == 100
        assert df["custom_int"][1] == 101
        assert df["custom_int"][2] == 102

    def test_unknown_long_logical_type_treated_as_long(
        self, unknown_logical_type_avro_file
    ):
        """Test unknown logical type on long base is treated as long."""
        df = jetliner.scan_avro(unknown_logical_type_avro_file).collect()

        # Should be Int64 dtype (base type for Avro long)
        assert df["custom_long"].dtype == pl.Int64

        # Values should be readable as longs
        assert df["custom_long"][0] == 1000000000
        assert df["custom_long"][1] == 1000000001
        assert df["custom_long"][2] == 1000000002

    def test_unknown_bytes_logical_type_treated_as_bytes(
        self, unknown_logical_type_avro_file
    ):
        """Test unknown logical type on bytes base is treated as bytes."""
        df = jetliner.scan_avro(unknown_logical_type_avro_file).collect()

        # Should be Binary dtype (base type for Avro bytes)
        assert df["custom_bytes"].dtype == pl.Binary

        # Values should be readable as bytes
        assert df["custom_bytes"][0] == b"bytes-0"
        assert df["custom_bytes"][1] == b"bytes-1"
        assert df["custom_bytes"][2] == b"bytes-2"

    def test_schema_dict_preserves_unknown_logical_type_name(
        self, unknown_logical_type_avro_file
    ):
        """Test that schema_dict preserves the custom logical type name.

        This verifies that while data is treated as the base type,
        the schema inspection API preserves the logical type information.
        """
        reader = jetliner.AvroReader(unknown_logical_type_avro_file)
        schema_dict = reader.schema_dict

        # Find the custom_string field
        fields = schema_dict.get("fields", [])
        custom_string_field = next(
            (f for f in fields if f.get("name") == "custom_string"), None
        )

        assert custom_string_field is not None

        # The schema should preserve the logicalType
        field_type = custom_string_field.get("type", {})
        if isinstance(field_type, dict):
            assert field_type.get("logicalType") == "my-custom-type"
            assert field_type.get("type") == "string"


# Schema with unknown logical type on fixed base
UNKNOWN_FIXED_LOGICAL_TYPE_SCHEMA = {
    "type": "record",
    "name": "UnknownFixedLogicalTypeRecord",
    "namespace": "test.jetliner",
    "fields": [
        {"name": "id", "type": "int"},
        # Custom logical type on fixed[8] base
        {
            "name": "custom_fixed",
            "type": {
                "type": "fixed",
                "name": "custom_fixed_type",
                "size": 8,
                "logicalType": "my-fixed-type",
            },
        },
    ],
}


def create_unknown_fixed_logical_type_record(record_id: int) -> dict:
    """Create a record with unknown logical type on fixed base."""
    # Create 8 bytes of data
    fixed_bytes = bytes([record_id] * 8)
    return {
        "id": record_id,
        "custom_fixed": fixed_bytes,
    }


@pytest.fixture
def unknown_fixed_logical_type_avro_file():
    """Create a temporary Avro file with unknown logical type on fixed."""
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        records = [create_unknown_fixed_logical_type_record(i) for i in range(3)]
        fastavro.writer(f, UNKNOWN_FIXED_LOGICAL_TYPE_SCHEMA, records)
        temp_path = f.name

    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


class TestUnknownFixedLogicalType:
    """Test unknown logical type on fixed base type."""

    def test_read_unknown_fixed_logical_type(self, unknown_fixed_logical_type_avro_file):
        """Test that unknown logical type on fixed is treated as binary."""
        df = jetliner.scan_avro(unknown_fixed_logical_type_avro_file).collect()
        assert df.height == 3

        # Should be Binary dtype (base type for fixed)
        assert df["custom_fixed"].dtype == pl.Binary

        # Values should be readable as bytes
        assert df["custom_fixed"][0] == bytes([0] * 8)
        assert df["custom_fixed"][1] == bytes([1] * 8)
        assert df["custom_fixed"][2] == bytes([2] * 8)
