"""
Unit tests for Jetliner Python API.

Tests cover:
- Iterator protocol (open API) - Requirements 6.1, 6.2
- Context manager - Requirement 6.6
- Error handling - Requirements 6.4, 6.5
- scan() returns LazyFrame - Requirement 6a.1
- Projection pushdown - Requirement 6a.2
- Predicate pushdown - Requirement 6a.3
- Early stopping with head() - Requirement 6a.4
"""

import io
import os
import struct
import tempfile
from pathlib import Path

import polars as pl
import pytest

import jetliner


# =============================================================================
# Test Data Helpers
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
    encoded = s.encode('utf-8')
    return encode_zigzag(len(encoded)) + encoded


def create_test_record(id_val: int, name: str) -> bytes:
    """Create a test record with (id: long, name: string)."""
    return encode_zigzag(id_val) + encode_string(name)


def create_test_block(record_count: int, data: bytes, sync_marker: bytes) -> bytes:
    """Create an Avro block with the given data."""
    return (
        encode_zigzag(record_count) +
        encode_zigzag(len(data)) +
        data +
        sync_marker
    )


def create_test_avro_file(records: list[tuple[int, str]]) -> bytes:
    """
    Create a minimal valid Avro file with the given records.

    Each record is a tuple of (id: int, name: str).
    Returns the raw bytes of the Avro file.
    """
    # Magic bytes
    magic = b'Obj\x01'

    # Schema JSON
    schema_json = b'{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}'

    # Sync marker (16 bytes)
    sync_marker = bytes([
        0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
        0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0
    ])

    # Build header
    file_data = bytearray()
    file_data.extend(magic)

    # Metadata map: 1 entry (schema)
    file_data.extend(encode_zigzag(1))  # 1 entry

    # Schema key
    schema_key = b'avro.schema'
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

        file_data.extend(create_test_block(len(records), bytes(block_data), sync_marker))

    return bytes(file_data)


def create_multi_block_avro_file(blocks: list[list[tuple[int, str]]]) -> bytes:
    """
    Create an Avro file with multiple blocks.

    Each block is a list of records (id: int, name: str).
    """
    # Magic bytes
    magic = b'Obj\x01'

    # Schema JSON
    schema_json = b'{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}'

    # Sync marker (16 bytes)
    sync_marker = bytes([
        0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
        0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0
    ])

    # Build header
    file_data = bytearray()
    file_data.extend(magic)

    # Metadata map: 1 entry (schema)
    file_data.extend(encode_zigzag(1))

    # Schema key
    schema_key = b'avro.schema'
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

            file_data.extend(create_test_block(len(block_records), bytes(block_data), sync_marker))

    return bytes(file_data)


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

    with tempfile.NamedTemporaryFile(suffix='.avro', delete=False) as f:
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

    with tempfile.NamedTemporaryFile(suffix='.avro', delete=False) as f:
        f.write(file_data)
        temp_path = f.name

    yield temp_path

    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def temp_empty_avro_file():
    """Create a temporary Avro file with no records."""
    file_data = create_test_avro_file([])

    with tempfile.NamedTemporaryFile(suffix='.avro', delete=False) as f:
        f.write(file_data)
        temp_path = f.name

    yield temp_path

    # Cleanup
    os.unlink(temp_path)


# =============================================================================
# Test: Iterator Protocol (open API) - Requirements 6.1, 6.2
# =============================================================================

class TestIteratorProtocol:
    """Tests for Python iterator protocol implementation."""

    def test_open_returns_iterator(self, temp_avro_file):
        """Test that open() returns an iterable object."""
        reader = jetliner.open(temp_avro_file)
        assert hasattr(reader, '__iter__')
        assert hasattr(reader, '__next__')

    def test_iteration_yields_dataframes(self, temp_avro_file):
        """Test that iteration yields Polars DataFrames."""
        reader = jetliner.open(temp_avro_file)
        for df in reader:
            assert isinstance(df, pl.DataFrame)

    def test_iteration_reads_all_records(self, temp_avro_file):
        """Test that iteration reads all records from the file."""
        total_rows = 0
        for df in jetliner.open(temp_avro_file):
            total_rows += df.height

        assert total_rows == 5  # 5 records in fixture

    def test_iteration_correct_data(self, temp_avro_file):
        """Test that iteration returns correct data values."""
        dfs = list(jetliner.open(temp_avro_file))
        df = pl.concat(dfs)

        assert df.height == 5
        assert df.width == 2
        assert "id" in df.columns
        assert "name" in df.columns

        ids = df["id"].to_list()
        names = df["name"].to_list()

        assert ids == [1, 2, 3, 4, 5]
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]

    def test_iteration_multiple_blocks(self, temp_multi_block_file):
        """Test iteration over file with multiple blocks."""
        total_rows = 0
        for df in jetliner.open(temp_multi_block_file):
            total_rows += df.height

        assert total_rows == 7  # 2 + 2 + 3 records

    def test_iteration_empty_file(self, temp_empty_avro_file):
        """Test iteration over empty file yields no DataFrames."""
        dfs = list(jetliner.open(temp_empty_avro_file))
        assert len(dfs) == 0

    def test_resources_released_after_iteration(self, temp_avro_file):
        """Test that resources are released after iteration completes."""
        reader = jetliner.open(temp_avro_file)

        # Consume all data
        for _ in reader:
            pass

        # Reader should be finished
        assert reader.is_finished


# =============================================================================
# Test: Context Manager - Requirement 6.6
# =============================================================================

class TestContextManager:
    """Tests for context manager protocol implementation."""

    def test_context_manager_enter_exit(self, temp_avro_file):
        """Test that context manager protocol works correctly."""
        with jetliner.open(temp_avro_file) as reader:
            assert reader is not None
            # Should be able to iterate
            dfs = list(reader)
            assert len(dfs) > 0

    def test_context_manager_releases_resources(self, temp_avro_file):
        """Test that context manager releases resources on exit."""
        with jetliner.open(temp_avro_file) as reader:
            for _ in reader:
                pass

        # After exiting context, reader should be finished
        assert reader.is_finished

    def test_context_manager_on_exception(self, temp_avro_file):
        """Test that context manager releases resources even on exception."""
        try:
            with jetliner.open(temp_avro_file) as reader:
                for df in reader:
                    raise ValueError("Test exception")
        except ValueError:
            pass

        # Resources should still be released
        assert reader.is_finished


# =============================================================================
# Test: Error Handling - Requirements 6.4, 6.5
# =============================================================================

class TestErrorHandling:
    """Tests for error handling and exception types."""

    def test_file_not_found_error(self):
        """Test that FileNotFoundError is raised for missing files."""
        with pytest.raises(FileNotFoundError):
            jetliner.open("/nonexistent/path/to/file.avro")

    def test_invalid_avro_file(self):
        """Test that ParseError is raised for invalid Avro files."""
        with tempfile.NamedTemporaryFile(suffix='.avro', delete=False) as f:
            f.write(b"This is not an Avro file")
            temp_path = f.name

        try:
            with pytest.raises(jetliner.ParseError):
                jetliner.open(temp_path)
        finally:
            os.unlink(temp_path)

    def test_exception_types_exist(self):
        """Test that all expected exception types are defined."""
        assert hasattr(jetliner, 'JetlinerError')
        assert hasattr(jetliner, 'ParseError')
        assert hasattr(jetliner, 'SchemaError')
        assert hasattr(jetliner, 'CodecError')
        assert hasattr(jetliner, 'DecodeError')
        assert hasattr(jetliner, 'SourceError')

    def test_exception_hierarchy(self):
        """Test that custom exceptions inherit from JetlinerError."""
        assert issubclass(jetliner.ParseError, jetliner.JetlinerError)
        assert issubclass(jetliner.SchemaError, jetliner.JetlinerError)
        assert issubclass(jetliner.CodecError, jetliner.JetlinerError)
        assert issubclass(jetliner.DecodeError, jetliner.JetlinerError)
        assert issubclass(jetliner.SourceError, jetliner.JetlinerError)


# =============================================================================
# Test: scan() Returns LazyFrame - Requirement 6a.1
# =============================================================================

class TestScanReturnsLazyFrame:
    """Tests for scan() function returning LazyFrame."""

    def test_scan_returns_lazyframe(self, temp_avro_file):
        """Test that scan() returns a Polars LazyFrame."""
        lf = jetliner.scan(temp_avro_file)
        assert isinstance(lf, pl.LazyFrame)

    def test_scan_collect_returns_dataframe(self, temp_avro_file):
        """Test that collecting a scanned LazyFrame returns a DataFrame."""
        lf = jetliner.scan(temp_avro_file)
        df = lf.collect()
        assert isinstance(df, pl.DataFrame)

    def test_scan_reads_all_data(self, temp_avro_file):
        """Test that scan() reads all data when collected."""
        df = jetliner.scan(temp_avro_file).collect()

        assert df.height == 5
        assert df.width == 2
        assert "id" in df.columns
        assert "name" in df.columns

    def test_scan_correct_schema(self, temp_avro_file):
        """Test that scan() exposes correct schema."""
        lf = jetliner.scan(temp_avro_file)
        schema = lf.collect_schema()

        assert "id" in schema
        assert "name" in schema


# =============================================================================
# Test: Projection Pushdown - Requirement 6a.2
# =============================================================================

class TestProjectionPushdown:
    """Tests for projection pushdown optimization."""

    def test_select_single_column(self, temp_avro_file):
        """Test selecting a single column."""
        df = jetliner.scan(temp_avro_file).select("id").collect()

        assert df.width == 1
        assert "id" in df.columns
        assert "name" not in df.columns

    def test_select_multiple_columns(self, temp_avro_file):
        """Test selecting multiple columns."""
        df = jetliner.scan(temp_avro_file).select(["id", "name"]).collect()

        assert df.width == 2
        assert "id" in df.columns
        assert "name" in df.columns

    def test_projection_preserves_data(self, temp_avro_file):
        """Test that projection preserves correct data values."""
        df = jetliner.scan(temp_avro_file).select("name").collect()

        names = df["name"].to_list()
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]


# =============================================================================
# Test: Predicate Pushdown - Requirement 6a.3
# =============================================================================

class TestPredicatePushdown:
    """Tests for predicate pushdown optimization."""

    def test_filter_equality(self, temp_avro_file):
        """Test filtering with equality predicate."""
        df = jetliner.scan(temp_avro_file).filter(pl.col("id") == 3).collect()

        assert df.height == 1
        assert df["id"].to_list() == [3]
        assert df["name"].to_list() == ["Charlie"]

    def test_filter_comparison(self, temp_avro_file):
        """Test filtering with comparison predicate."""
        df = jetliner.scan(temp_avro_file).filter(pl.col("id") > 3).collect()

        assert df.height == 2
        assert df["id"].to_list() == [4, 5]

    def test_filter_string(self, temp_avro_file):
        """Test filtering on string column."""
        df = jetliner.scan(temp_avro_file).filter(pl.col("name") == "Alice").collect()

        assert df.height == 1
        assert df["name"].to_list() == ["Alice"]

    def test_filter_combined_with_select(self, temp_avro_file):
        """Test combining filter with select."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter(pl.col("id") > 2)
            .select("name")
            .collect()
        )

        assert df.width == 1
        assert df.height == 3
        assert df["name"].to_list() == ["Charlie", "Diana", "Eve"]


# =============================================================================
# Test: Early Stopping with head() - Requirement 6a.4
# =============================================================================

class TestEarlyStopping:
    """Tests for early stopping optimization."""

    def test_head_limits_rows(self, temp_avro_file):
        """Test that head() limits the number of rows returned."""
        df = jetliner.scan(temp_avro_file).head(3).collect()

        assert df.height == 3

    def test_head_returns_first_rows(self, temp_avro_file):
        """Test that head() returns the first N rows."""
        df = jetliner.scan(temp_avro_file).head(2).collect()

        assert df["id"].to_list() == [1, 2]
        assert df["name"].to_list() == ["Alice", "Bob"]

    def test_head_with_filter(self, temp_avro_file):
        """Test head() combined with filter."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter(pl.col("id") > 1)
            .head(2)
            .collect()
        )

        assert df.height == 2
        assert df["id"].to_list() == [2, 3]

    def test_limit_alias(self, temp_avro_file):
        """Test that limit() works as alias for head()."""
        df = jetliner.scan(temp_avro_file).limit(3).collect()

        assert df.height == 3


# =============================================================================
# Test: Schema Access
# =============================================================================

class TestSchemaAccess:
    """Tests for schema inspection functionality."""

    def test_schema_property_returns_json(self, temp_avro_file):
        """Test that schema property returns JSON string."""
        with jetliner.open(temp_avro_file) as reader:
            schema = reader.schema
            assert isinstance(schema, str)
            assert "TestRecord" in schema

    def test_schema_dict_property(self, temp_avro_file):
        """Test that schema_dict property returns Python dict."""
        with jetliner.open(temp_avro_file) as reader:
            schema_dict = reader.schema_dict
            assert isinstance(schema_dict, dict)
            assert schema_dict.get("name") == "TestRecord"
            assert "fields" in schema_dict

    def test_parse_avro_schema_function(self, temp_avro_file):
        """Test parse_avro_schema() function."""
        schema = jetliner.parse_avro_schema(temp_avro_file)
        # Should return a Polars schema
        assert "id" in schema
        assert "name" in schema


# =============================================================================
# Test: Configuration Options
# =============================================================================

class TestConfigurationOptions:
    """Tests for configuration options."""

    def test_batch_size_option(self, temp_multi_block_file):
        """Test batch_size configuration option."""
        # With small batch size, should get multiple batches
        batch_count = 0
        for df in jetliner.open(temp_multi_block_file, batch_size=2):
            batch_count += 1

        # Should have multiple batches with small batch size
        assert batch_count >= 1

    def test_strict_mode_default(self, temp_avro_file):
        """Test that strict mode is disabled by default."""
        # Should not raise on valid file
        with jetliner.open(temp_avro_file) as reader:
            for _ in reader:
                pass

    def test_buffer_options(self, temp_avro_file):
        """Test buffer configuration options."""
        # Should work with custom buffer settings
        with jetliner.open(
            temp_avro_file,
            buffer_blocks=2,
            buffer_bytes=1024 * 1024
        ) as reader:
            dfs = list(reader)
            assert len(dfs) > 0


# =============================================================================
# Test: Reader Properties
# =============================================================================

class TestReaderProperties:
    """Tests for reader property accessors."""

    def test_batch_size_property(self, temp_avro_file):
        """Test batch_size property returns configured value."""
        with jetliner.open(temp_avro_file, batch_size=50000) as reader:
            assert reader.batch_size == 50000

    def test_batch_size_default(self, temp_avro_file):
        """Test batch_size property returns default value."""
        with jetliner.open(temp_avro_file) as reader:
            assert reader.batch_size == 100000

    def test_pending_records_property(self, temp_avro_file):
        """Test pending_records property."""
        with jetliner.open(temp_avro_file) as reader:
            # Before iteration, pending should be 0
            assert reader.pending_records >= 0

    def test_is_finished_before_iteration(self, temp_avro_file):
        """Test is_finished is False before iteration."""
        with jetliner.open(temp_avro_file) as reader:
            assert not reader.is_finished

    def test_is_finished_after_iteration(self, temp_avro_file):
        """Test is_finished is True after iteration completes."""
        with jetliner.open(temp_avro_file) as reader:
            for _ in reader:
                pass
            assert reader.is_finished


# =============================================================================
# Test: Error Accumulation (Skip Mode) - Requirements 7.3, 7.4
# =============================================================================

class TestErrorAccumulation:
    """Tests for error accumulation in skip mode."""

    def test_errors_property_exists(self, temp_avro_file):
        """Test that errors property exists and returns a list."""
        with jetliner.open(temp_avro_file, strict=False) as reader:
            for _ in reader:
                pass
            errors = reader.errors
            assert isinstance(errors, list)

    def test_error_count_property(self, temp_avro_file):
        """Test that error_count property returns an integer."""
        with jetliner.open(temp_avro_file, strict=False) as reader:
            for _ in reader:
                pass
            assert isinstance(reader.error_count, int)

    def test_no_errors_on_valid_file(self, temp_avro_file):
        """Test that no errors are accumulated for valid files."""
        with jetliner.open(temp_avro_file, strict=False) as reader:
            for _ in reader:
                pass
            assert reader.error_count == 0
            assert len(reader.errors) == 0


# =============================================================================
# Test: AvroReaderCore (Internal Class)
# =============================================================================

class TestAvroReaderCore:
    """Tests for AvroReaderCore internal class."""

    def test_reader_core_iteration(self, temp_avro_file):
        """Test that AvroReaderCore can iterate over records."""
        reader = jetliner.AvroReaderCore(temp_avro_file)
        total_rows = 0
        for df in reader:
            total_rows += df.height
        assert total_rows == 5

    def test_reader_core_with_projection(self, temp_avro_file):
        """Test AvroReaderCore with projected_columns parameter."""
        reader = jetliner.AvroReaderCore(
            temp_avro_file,
            projected_columns=["name"]
        )
        for df in reader:
            assert df.width == 1
            assert "name" in df.columns
            assert "id" not in df.columns

    def test_reader_core_schema_property(self, temp_avro_file):
        """Test AvroReaderCore schema property."""
        reader = jetliner.AvroReaderCore(temp_avro_file)
        schema = reader.schema
        assert isinstance(schema, str)
        assert "TestRecord" in schema
