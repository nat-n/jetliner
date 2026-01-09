"""Shared fixtures and utilities for Jetliner tests."""

import os
import tempfile
from pathlib import Path

import pytest


# =============================================================================
# Real File Test Data Helper
# =============================================================================


@pytest.fixture
def get_test_data_path():
    """Fixture that returns a function to get test data file paths."""

    def _get_path(relative_path: str) -> str:
        """Get absolute path to test data file.

        Args:
            relative_path: Path relative to tests/data directory,
                          e.g., "apache-avro/weather.avro"

        Returns:
            Absolute path to the test data file
        """
        tests_dir = Path(__file__).parent.parent.parent / "tests" / "data"
        return str(tests_dir / relative_path)

    return _get_path


# =============================================================================
# Synthetic Avro File Generation Helpers
# =============================================================================


def encode_zigzag(n: int) -> bytes:
    """Encode an integer using Avro's zigzag encoding."""
    # Convert to unsigned zigzag
    if n >= 0:
        zigzag = n << 1
    else:
        zigzag = ((-n - 1) << 1) | 1

    # Variable-length encoding
    result = bytearray()
    while zigzag > 0x7F:
        result.append((zigzag & 0x7F) | 0x80)
        zigzag >>= 7
    result.append(zigzag & 0x7F)
    return bytes(result)


def encode_string(s: str) -> bytes:
    """Encode a string using Avro's string encoding (length-prefixed)."""
    encoded = s.encode("utf-8")
    return encode_zigzag(len(encoded)) + encoded


def create_test_record(id_val: int, name: str) -> bytes:
    """Create a test record with (id: long, name: string)."""
    return encode_zigzag(id_val) + encode_string(name)


def create_test_block(record_count: int, data: bytes, sync_marker: bytes) -> bytes:
    """Create an Avro block with the given data."""
    return encode_zigzag(record_count) + encode_zigzag(len(data)) + data + sync_marker


def create_test_avro_file(records: list[tuple[int, str]]) -> bytes:
    """
    Create a minimal valid Avro file with the given records.

    Each record is a tuple of (id: int, name: str).
    Returns the raw bytes of the Avro file.
    """
    # Magic bytes
    magic = b"Obj\x01"

    # Schema JSON
    schema_json = b'{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}'

    # Sync marker (16 bytes)
    sync_marker = bytes(
        [
            0xDE,
            0xAD,
            0xBE,
            0xEF,
            0xCA,
            0xFE,
            0xBA,
            0xBE,
            0x12,
            0x34,
            0x56,
            0x78,
            0x9A,
            0xBC,
            0xDE,
            0xF0,
        ]
    )

    # Build header
    file_data = bytearray()
    file_data.extend(magic)

    # Metadata map: 1 entry (schema)
    file_data.extend(encode_zigzag(1))  # 1 entry

    # Schema key
    schema_key = b"avro.schema"
    file_data.extend(encode_zigzag(len(schema_key)))
    file_data.extend(schema_key)

    # Schema value
    file_data.extend(encode_zigzag(len(schema_json)))
    file_data.extend(schema_json)

    # End of map
    file_data.append(0x00)

    # Sync marker
    file_data.extend(sync_marker)

    # Create block with all records
    if records:
        block_data = bytearray()
        for id_val, name in records:
            block_data.extend(create_test_record(id_val, name))

        file_data.extend(
            create_test_block(len(records), bytes(block_data), sync_marker)
        )

    return bytes(file_data)


def create_multi_block_avro_file(blocks: list[list[tuple[int, str]]]) -> bytes:
    """
    Create an Avro file with multiple blocks.

    Each block is a list of records (id: int, name: str).
    """
    # Magic bytes
    magic = b"Obj\x01"

    # Schema JSON
    schema_json = b'{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}'

    # Sync marker (16 bytes)
    sync_marker = bytes(
        [
            0xDE,
            0xAD,
            0xBE,
            0xEF,
            0xCA,
            0xFE,
            0xBA,
            0xBE,
            0x12,
            0x34,
            0x56,
            0x78,
            0x9A,
            0xBC,
            0xDE,
            0xF0,
        ]
    )

    # Build header
    file_data = bytearray()
    file_data.extend(magic)

    # Metadata map: 1 entry (schema)
    file_data.extend(encode_zigzag(1))

    # Schema key
    schema_key = b"avro.schema"
    file_data.extend(encode_zigzag(len(schema_key)))
    file_data.extend(schema_key)

    # Schema value
    file_data.extend(encode_zigzag(len(schema_json)))
    file_data.extend(schema_json)

    # End of map
    file_data.append(0x00)

    # Sync marker
    file_data.extend(sync_marker)

    # Create blocks
    for block_records in blocks:
        if block_records:
            block_data = bytearray()
            for id_val, name in block_records:
                block_data.extend(create_test_record(id_val, name))

            file_data.extend(
                create_test_block(len(block_records), bytes(block_data), sync_marker)
            )

    return bytes(file_data)


# =============================================================================
# Synthetic Test File Fixtures
# =============================================================================


@pytest.fixture
def temp_avro_file():
    """Create a temporary Avro file with test data."""
    records = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "Diana"),
        (5, "Eve"),
    ]
    file_data = create_test_avro_file(records)

    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        f.write(file_data)
        temp_path = f.name

    yield temp_path

    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def temp_multi_block_file():
    """Create a temporary Avro file with multiple blocks."""
    blocks = [
        [(1, "Alice"), (2, "Bob")],
        [(3, "Charlie"), (4, "Diana")],
        [(5, "Eve"), (6, "Frank"), (7, "Grace")],
    ]
    file_data = create_multi_block_avro_file(blocks)

    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        f.write(file_data)
        temp_path = f.name

    yield temp_path

    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def temp_empty_avro_file():
    """Create a temporary Avro file with no records."""
    file_data = create_test_avro_file([])

    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        f.write(file_data)
        temp_path = f.name

    yield temp_path

    # Cleanup
    os.unlink(temp_path)
