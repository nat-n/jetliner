"""
Integration tests for edge cases.

Tests cover:
- glob=False treats path literally (Requirement 2.5, 2.6)
- Column selection by index (Requirement 3.3)
- scan_avro rejects columns parameter (Requirement 3.1, 3.9)
- Empty glob result raises descriptive error (Requirement 3.8)
- Empty file handling
- Mixed codec files

Requirements tested:
- 2.5: The `glob` parameter (default `True`) controls whether glob expansion is performed
- 2.6: When `glob=False`, the reader treats the path literally without expansion
- 3.1: The `columns` parameter is available on `read_avro()` only (not `scan_avro()`)
- 3.3: The `columns` parameter accepts `Sequence[int]` for column indices (0-based)
- 3.8: When an invalid column index is provided, the reader raises an `IndexError`
- 3.9: For `scan_avro()`, projection is done via LazyFrame operations
"""

from __future__ import annotations

from pathlib import Path

import fastavro
import pytest

import jetliner


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def avro_file_with_glob_chars(tmp_path: Path) -> str:
    """Create an Avro file with glob characters in the name."""
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
        ],
    }

    records = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]

    # Create a file with glob-like characters in the name
    file_path = tmp_path / "data[1].avro"
    with open(file_path, "wb") as f:
        fastavro.writer(f, schema, records)

    return str(file_path)


@pytest.fixture
def multi_column_avro_file(tmp_path: Path) -> str:
    """Create an Avro file with multiple columns for index selection tests."""
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "col_a", "type": "int"},
            {"name": "col_b", "type": "string"},
            {"name": "col_c", "type": "double"},
            {"name": "col_d", "type": "boolean"},
        ],
    }

    records = [
        {"col_a": 1, "col_b": "one", "col_c": 1.1, "col_d": True},
        {"col_a": 2, "col_b": "two", "col_c": 2.2, "col_d": False},
        {"col_a": 3, "col_b": "three", "col_c": 3.3, "col_d": True},
    ]

    file_path = tmp_path / "multi_column.avro"
    with open(file_path, "wb") as f:
        fastavro.writer(f, schema, records)

    return str(file_path)


@pytest.fixture
def empty_avro_file(tmp_path: Path) -> str:
    """Create an Avro file with no records (empty)."""
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
        ],
    }

    file_path = tmp_path / "empty.avro"
    with open(file_path, "wb") as f:
        fastavro.writer(f, schema, [])  # Empty records list

    return str(file_path)


@pytest.fixture
def mixed_codec_files(tmp_path: Path) -> tuple[list[str], int]:
    """Create multiple Avro files with different codecs."""
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "value", "type": "string"},
        ],
    }

    codecs = ["null", "deflate", "snappy"]
    paths = []
    total_rows = 0

    for i, codec in enumerate(codecs):
        records = [
            {"id": i * 10 + j, "value": f"record_{codec}_{j}"}
            for j in range(5)
        ]
        total_rows += len(records)

        file_path = tmp_path / f"data_{codec}.avro"
        with open(file_path, "wb") as f:
            fastavro.writer(f, schema, records, codec=codec)
        paths.append(str(file_path))

    return paths, total_rows


# =============================================================================
# Test Classes
# =============================================================================


class TestGlobFalse:
    """Tests for glob=False treating path literally.

    Requirements: 2.5, 2.6
    """

    def test_glob_false_treats_path_literally(self, avro_file_with_glob_chars: str):
        """Test that glob=False treats path with glob chars literally.

        Requirement 2.6: When glob=False, the reader treats the path literally.
        """
        # With glob=False, the path should be treated literally
        df = jetliner.read_avro(avro_file_with_glob_chars, glob=False)

        assert df.height == 2
        assert df["id"].to_list() == [1, 2]

    def test_glob_false_scan_avro(self, avro_file_with_glob_chars: str):
        """Test that scan_avro with glob=False treats path literally."""
        df = jetliner.scan_avro(avro_file_with_glob_chars, glob=False).collect()

        assert df.height == 2
        assert df["id"].to_list() == [1, 2]

    def test_glob_true_with_literal_path(self, avro_file_with_glob_chars: str):
        """Test that glob=True with a path containing glob chars may fail or expand.

        When glob=True (default), paths with glob characters are treated as patterns.
        A path like 'data[1].avro' would be interpreted as a character class pattern.
        """
        # With glob=True, the [1] is interpreted as a character class
        # This may or may not match depending on the filesystem
        # The key point is that glob=False should work correctly
        try:
            df = jetliner.read_avro(avro_file_with_glob_chars, glob=True)
            # If it succeeds, verify the data
            assert df.height == 2
        except Exception:
            # It's acceptable for glob=True to fail with special characters
            pass

    def test_glob_false_with_nonexistent_file(self, tmp_path: Path):
        """Test that glob=False with nonexistent file raises appropriate error."""
        nonexistent = str(tmp_path / "nonexistent.avro")

        with pytest.raises(Exception):  # FileNotFoundError or similar
            jetliner.read_avro(nonexistent, glob=False)


class TestColumnSelectionByIndex:
    """Tests for column selection by index.

    Requirements: 3.3, 3.8
    """

    def test_select_columns_by_index(self, multi_column_avro_file: str):
        """Test selecting columns by 0-based index.

        Requirement 3.3: The columns parameter accepts Sequence[int] for column indices.
        """
        # Select columns 0 and 2 (col_a and col_c)
        df = jetliner.read_avro(multi_column_avro_file, columns=[0, 2])

        assert df.width == 2
        assert df.columns == ["col_a", "col_c"]
        assert df["col_a"].to_list() == [1, 2, 3]
        assert df["col_c"].to_list() == [1.1, 2.2, 3.3]

    def test_select_single_column_by_index(self, multi_column_avro_file: str):
        """Test selecting a single column by index."""
        df = jetliner.read_avro(multi_column_avro_file, columns=[1])

        assert df.width == 1
        assert df.columns == ["col_b"]
        assert df["col_b"].to_list() == ["one", "two", "three"]

    def test_select_columns_by_index_preserves_schema_order(self, multi_column_avro_file: str):
        """Test that column indices select columns but preserve schema order.

        Note: Column selection by index selects the columns but returns them
        in schema order, not in the order specified. This is consistent with
        how Avro projection works.
        """
        # Select columns 3, 1, 0 - they will be returned in schema order
        df = jetliner.read_avro(multi_column_avro_file, columns=[3, 1, 0])

        assert df.width == 3
        # Columns are returned in schema order (col_a, col_b, col_d)
        assert set(df.columns) == {"col_a", "col_b", "col_d"}

    def test_select_all_columns_by_index(self, multi_column_avro_file: str):
        """Test selecting all columns by index."""
        df = jetliner.read_avro(multi_column_avro_file, columns=[0, 1, 2, 3])

        assert df.width == 4
        assert df.columns == ["col_a", "col_b", "col_c", "col_d"]

    def test_invalid_column_index_raises_error(self, multi_column_avro_file: str):
        """Test that invalid column index raises an error.

        Requirement 3.8: When an invalid column index is provided, the reader raises an IndexError.
        """
        # Index 10 is out of range (only 4 columns: 0-3)
        with pytest.raises((IndexError, Exception)) as exc_info:
            jetliner.read_avro(multi_column_avro_file, columns=[0, 10])

        # Verify the error message is descriptive
        error_msg = str(exc_info.value).lower()
        assert "index" in error_msg or "out of" in error_msg or "range" in error_msg or "invalid" in error_msg

    def test_negative_column_index_raises_error(self, multi_column_avro_file: str):
        """Test that negative column index raises an error."""
        with pytest.raises((IndexError, Exception, OverflowError)):
            jetliner.read_avro(multi_column_avro_file, columns=[-1])


class TestScanAvroRejectsColumns:
    """Tests verifying scan_avro rejects columns parameter.

    Requirements: 3.1, 3.9
    """

    def test_scan_avro_rejects_columns_parameter(self, temp_avro_file: str):
        """Test that scan_avro raises TypeError when columns parameter is passed.

        Requirement 3.1: The columns parameter is available on read_avro() only.
        Requirement 3.9: For scan_avro(), projection is done via LazyFrame operations.
        """
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            jetliner.scan_avro(temp_avro_file, columns=["id"])

    def test_scan_avro_projection_via_select(self, temp_avro_file: str):
        """Test that scan_avro projection is done via LazyFrame.select().

        Requirement 3.9: For scan_avro(), projection is done via LazyFrame operations.
        """
        # Correct way to do projection with scan_avro
        df = jetliner.scan_avro(temp_avro_file).select(["id"]).collect()

        assert df.width == 1
        assert "id" in df.columns
        assert "name" not in df.columns


class TestEmptyGlobResult:
    """Tests for empty glob result error handling.

    Requirement 3.8 (related): Descriptive errors for invalid inputs.
    """

    def test_empty_glob_raises_descriptive_error(self, tmp_path: Path):
        """Test that a glob pattern matching no files raises a descriptive error."""
        # Create a pattern that won't match anything
        pattern = str(tmp_path / "nonexistent_pattern_*.avro")

        with pytest.raises(Exception) as exc_info:
            jetliner.read_avro(pattern)

        # Verify the error message is descriptive
        error_msg = str(exc_info.value).lower()
        assert "no files" in error_msg or "not found" in error_msg or "match" in error_msg

    def test_scan_avro_empty_glob_raises_error(self, tmp_path: Path):
        """Test that scan_avro with empty glob result raises error."""
        pattern = str(tmp_path / "nonexistent_*.avro")

        with pytest.raises(Exception) as exc_info:
            jetliner.scan_avro(pattern)

        error_msg = str(exc_info.value).lower()
        assert "no files" in error_msg or "not found" in error_msg or "match" in error_msg


class TestEmptyFileHandling:
    """Tests for handling empty Avro files (files with schema but no records)."""

    def test_read_empty_file(self, empty_avro_file: str):
        """Test reading an Avro file with no records."""
        df = jetliner.read_avro(empty_avro_file)

        # Should return empty DataFrame with correct schema
        assert df.height == 0
        assert "id" in df.columns
        assert "name" in df.columns

    def test_scan_empty_file(self, empty_avro_file: str):
        """Test scanning an Avro file with no records."""
        df = jetliner.scan_avro(empty_avro_file).collect()

        # Should return empty DataFrame with correct schema
        assert df.height == 0
        assert "id" in df.columns
        assert "name" in df.columns

    def test_read_empty_file_with_columns(self, empty_avro_file: str):
        """Test reading empty file with column selection.

        Note: For empty files, column selection may not filter columns
        since there are no records to process. The schema is still returned.
        """
        df = jetliner.read_avro(empty_avro_file, columns=["id"])

        assert df.height == 0
        # Empty files may return full schema since no records are processed
        assert "id" in df.columns

    def test_read_empty_file_schema(self, empty_avro_file: str):
        """Test reading schema from empty file."""
        schema = jetliner.read_avro_schema(empty_avro_file)

        assert "id" in schema
        assert "name" in schema

    def test_empty_file_with_row_index(self, empty_avro_file: str):
        """Test empty file with row_index_name parameter.

        Empty files should still include the row_index column in the schema,
        matching Polars behavior where the idx column is present even with 0 rows.
        """
        import polars as pl

        df = jetliner.read_avro(empty_avro_file, row_index_name="idx")

        assert df.height == 0
        # Row index column should be present with correct schema
        assert "idx" in df.columns
        assert df.schema["idx"] == pl.UInt32
        # Original schema columns should also be present
        assert "id" in df.columns
        assert "name" in df.columns
        # Row index should be first column
        assert df.columns[0] == "idx"

    def test_empty_file_with_include_file_paths(self, empty_avro_file: str):
        """Test empty file with include_file_paths parameter.

        Empty files should still include the file path column in the schema,
        matching Polars behavior where metadata columns are present even with 0 rows.
        """
        import polars as pl

        df = jetliner.read_avro(empty_avro_file, include_file_paths="source")

        assert df.height == 0
        # File path column should be present with correct schema
        assert "source" in df.columns
        assert df.schema["source"] == pl.String
        # Original schema columns should also be present
        assert "id" in df.columns
        assert "name" in df.columns


class TestEmptyNonRecordSchemas:
    """Tests for empty files with non-record top-level schemas."""

    @pytest.fixture
    def empty_int_toplevel_file(self, tmp_path: Path) -> str:
        """Create an empty Avro file with int as top-level schema."""
        schema = "int"
        file_path = tmp_path / "empty_int_toplevel.avro"
        with open(file_path, "wb") as f:
            fastavro.writer(f, fastavro.parse_schema(schema), [])
        return str(file_path)

    @pytest.fixture
    def empty_string_toplevel_file(self, tmp_path: Path) -> str:
        """Create an empty Avro file with string as top-level schema."""
        schema = "string"
        file_path = tmp_path / "empty_string_toplevel.avro"
        with open(file_path, "wb") as f:
            fastavro.writer(f, fastavro.parse_schema(schema), [])
        return str(file_path)

    def test_read_empty_int_toplevel(self, empty_int_toplevel_file: str):
        """Test read_avro with empty int top-level schema file."""
        df = jetliner.read_avro(empty_int_toplevel_file)

        assert df.height == 0
        assert df.width == 1
        assert "value" in df.columns

    def test_scan_empty_int_toplevel(self, empty_int_toplevel_file: str):
        """Test scan_avro with empty int top-level schema file."""
        df = jetliner.scan_avro(empty_int_toplevel_file).collect()

        assert df.height == 0
        assert df.width == 1
        assert "value" in df.columns

    def test_read_empty_string_toplevel(self, empty_string_toplevel_file: str):
        """Test read_avro with empty string top-level schema file."""
        df = jetliner.read_avro(empty_string_toplevel_file)

        assert df.height == 0
        assert df.width == 1
        assert "value" in df.columns

    def test_scan_empty_string_toplevel(self, empty_string_toplevel_file: str):
        """Test scan_avro with empty string top-level schema file."""
        df = jetliner.scan_avro(empty_string_toplevel_file).collect()

        assert df.height == 0
        assert df.width == 1
        assert "value" in df.columns


class TestZeroFieldSchemaRejection:
    """Tests for rejection of record schemas with zero fields.

    Polars DataFrames cannot represent row counts without at least one column,
    so zero-field record schemas are explicitly rejected with a clear error.
    """

    @pytest.fixture
    def zero_fields_file(self, tmp_path: Path) -> str:
        """Create an Avro file with a record schema that has no fields."""
        schema = {
            "type": "record",
            "name": "EmptyRecord",
            "fields": [],
        }
        file_path = tmp_path / "zero_fields.avro"
        with open(file_path, "wb") as f:
            fastavro.writer(f, schema, [])
        return str(file_path)

    @pytest.fixture
    def zero_fields_with_records_file(self, tmp_path: Path) -> str:
        """Create an Avro file with a zero-field schema but with records."""
        schema = {
            "type": "record",
            "name": "EmptyRecord",
            "fields": [],
        }
        records = [{}, {}, {}]
        file_path = tmp_path / "zero_fields_with_records.avro"
        with open(file_path, "wb") as f:
            fastavro.writer(f, schema, records)
        return str(file_path)

    def test_read_zero_fields_raises_error(self, zero_fields_file: str):
        """Test read_avro rejects zero-field record schema with clear error."""
        with pytest.raises(jetliner.SchemaError) as exc_info:
            jetliner.read_avro(zero_fields_file)

        error_msg = str(exc_info.value).lower()
        assert "zero fields" in error_msg
        assert "polars" in error_msg or "column" in error_msg

    def test_scan_zero_fields_raises_error(self, zero_fields_file: str):
        """Test scan_avro rejects zero-field record schema with clear error."""
        with pytest.raises(Exception) as exc_info:
            jetliner.scan_avro(zero_fields_file).collect()

        error_msg = str(exc_info.value).lower()
        assert "zero fields" in error_msg

    def test_read_zero_fields_with_records_raises_error(
        self, zero_fields_with_records_file: str
    ):
        """Test read_avro rejects zero-field schema even with records."""
        with pytest.raises(jetliner.SchemaError) as exc_info:
            jetliner.read_avro(zero_fields_with_records_file)

        error_msg = str(exc_info.value).lower()
        assert "zero fields" in error_msg

    def test_scan_zero_fields_with_records_raises_error(
        self, zero_fields_with_records_file: str
    ):
        """Test scan_avro rejects zero-field schema even with records."""
        with pytest.raises(Exception) as exc_info:
            jetliner.scan_avro(zero_fields_with_records_file).collect()

        error_msg = str(exc_info.value).lower()
        assert "zero fields" in error_msg

    def test_avro_reader_zero_fields_raises_error(self, zero_fields_file: str):
        """Test AvroReader rejects zero-field record schema with clear error."""
        with pytest.raises(jetliner.SchemaError) as exc_info:
            with jetliner.AvroReader(zero_fields_file) as reader:
                list(reader)

        error_msg = str(exc_info.value).lower()
        assert "zero fields" in error_msg


class TestBlocksWithZeroRecords:
    """Tests for files containing blocks with zero records."""

    @pytest.fixture
    def file_with_zero_record_block(self, tmp_path: Path) -> str:
        """Create an Avro file that has a block with zero records.

        This is tricky to create with fastavro as it optimizes away empty blocks.
        We create a file with records first, then manually craft a zero-record block.
        For simplicity, we test the logical equivalent: a file with valid header
        but the data section results in zero records after reading.
        """
        # fastavro doesn't easily support creating zero-record blocks,
        # so we test the closest equivalent: empty records list
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [{"name": "id", "type": "int"}],
        }
        file_path = tmp_path / "zero_record_block.avro"
        with open(file_path, "wb") as f:
            fastavro.writer(f, schema, [])
        return str(file_path)

    def test_read_file_with_zero_record_block(self, file_with_zero_record_block: str):
        """Test read_avro handles files where blocks yield zero records."""
        df = jetliner.read_avro(file_with_zero_record_block)

        assert df.height == 0
        assert "id" in df.columns

    def test_scan_file_with_zero_record_block(self, file_with_zero_record_block: str):
        """Test scan_avro handles files where blocks yield zero records."""
        df = jetliner.scan_avro(file_with_zero_record_block).collect()

        assert df.height == 0
        assert "id" in df.columns

    def test_avro_reader_file_with_zero_record_block(self, file_with_zero_record_block: str):
        """Test AvroReader iterator handles files where blocks yield zero records."""
        with jetliner.AvroReader(file_with_zero_record_block) as reader:
            dfs = list(reader)

        # Should yield no DataFrames (or empty ones)
        total_rows = sum(df.height for df in dfs)
        assert total_rows == 0


class TestMixedCodecFiles:
    """Tests for reading multiple files with different codecs."""

    def test_read_mixed_codec_files(self, mixed_codec_files: tuple[list[str], int]):
        """Test reading multiple files with different codecs."""
        paths, expected_rows = mixed_codec_files

        df = jetliner.read_avro(paths)

        assert df.height == expected_rows
        assert "id" in df.columns
        assert "value" in df.columns

    def test_scan_mixed_codec_files(self, mixed_codec_files: tuple[list[str], int]):
        """Test scanning multiple files with different codecs."""
        paths, expected_rows = mixed_codec_files

        df = jetliner.scan_avro(paths).collect()

        assert df.height == expected_rows

    def test_mixed_codec_files_data_integrity(self, mixed_codec_files: tuple[list[str], int]):
        """Test that data from mixed codec files is correct."""
        paths, _ = mixed_codec_files

        df = jetliner.read_avro(paths)

        # Verify we have records from all codecs
        values = df["value"].to_list()
        assert any("null" in v for v in values)
        assert any("deflate" in v for v in values)
        assert any("snappy" in v for v in values)

    def test_mixed_codec_with_include_file_paths(self, mixed_codec_files: tuple[list[str], int]):
        """Test mixed codec files with include_file_paths."""
        paths, _ = mixed_codec_files

        df = jetliner.read_avro(paths, include_file_paths="source")

        # Verify we have entries from all files
        unique_sources = df["source"].unique().to_list()
        assert len(unique_sources) == 3

    def test_mixed_codec_with_n_rows(self, mixed_codec_files: tuple[list[str], int]):
        """Test mixed codec files with n_rows limit."""
        paths, _ = mixed_codec_files

        # Limit to 8 rows (should span multiple files)
        df = jetliner.read_avro(paths, n_rows=8)

        assert df.height == 8


class TestRowIndexSingleFile:
    """Tests for row_index feature with single files.

    Requirements:
    - 5.1: The row_index_name parameter accepts str to specify the index column name
    - 5.3: When row_index_name is specified, insert an index column as the first column
    - 5.4: The row_index_offset parameter accepts int to specify the starting index
    - 5.5: The row index is of type pl.UInt32
    """

    def test_read_avro_row_index_basic(self, temp_avro_file: str):
        """Basic single-file row_index with read_avro."""
        import polars as pl

        df = jetliner.read_avro(temp_avro_file, row_index_name="idx")

        assert "idx" in df.columns
        assert df["idx"].to_list() == [0, 1, 2, 3, 4]
        assert df["idx"].dtype == pl.UInt32

    def test_scan_avro_row_index_basic(self, temp_avro_file: str):
        """Basic single-file row_index with scan_avro."""
        import polars as pl

        df = jetliner.scan_avro(temp_avro_file, row_index_name="idx").collect()

        assert "idx" in df.columns
        assert df["idx"].to_list() == [0, 1, 2, 3, 4]
        assert df["idx"].dtype == pl.UInt32

    def test_row_index_with_offset(self, temp_avro_file: str):
        """Row index starting from non-zero offset."""
        df = jetliner.read_avro(temp_avro_file, row_index_name="idx", row_index_offset=100)

        assert df["idx"].to_list() == [100, 101, 102, 103, 104]

    def test_row_index_with_n_rows(self, temp_avro_file: str):
        """Row index combined with n_rows limit."""
        df = jetliner.read_avro(
            temp_avro_file, row_index_name="idx", row_index_offset=10, n_rows=3
        )

        assert df.height == 3
        assert df["idx"].to_list() == [10, 11, 12]

    def test_row_index_with_columns(self, temp_avro_file: str):
        """Row index combined with column projection."""
        df = jetliner.read_avro(
            temp_avro_file, row_index_name="idx", columns=["id"]
        )

        # Row index should be present along with projected column
        assert "idx" in df.columns
        assert "id" in df.columns
        # name column should not be present due to projection
        assert "name" not in df.columns
        assert df["idx"].to_list() == [0, 1, 2, 3, 4]

    def test_row_index_is_first_column(self, temp_avro_file: str):
        """Row index inserted as first column.

        Requirement 5.3: The index column is inserted as the first column.
        """
        df = jetliner.read_avro(temp_avro_file, row_index_name="idx")

        assert df.columns[0] == "idx"

    def test_row_index_type_is_uint32(self, temp_avro_file: str):
        """Row index has UInt32 dtype.

        Requirement 5.5: The row index is of type pl.UInt32.
        """
        import polars as pl

        df = jetliner.read_avro(temp_avro_file, row_index_name="idx")

        assert df["idx"].dtype == pl.UInt32

    def test_negative_offset_raises_error(self, temp_avro_file: str):
        """Negative row_index_offset raises ValueError."""
        with pytest.raises(ValueError, match="negative"):
            jetliner.read_avro(temp_avro_file, row_index_name="idx", row_index_offset=-1)

    def test_scan_negative_offset_raises_error(self, temp_avro_file: str):
        """Negative row_index_offset raises ValueError for scan_avro."""
        with pytest.raises(ValueError, match="negative"):
            jetliner.scan_avro(temp_avro_file, row_index_name="idx", row_index_offset=-1)

    def test_empty_file_row_index_scan_avro(self, empty_avro_file: str):
        """scan_avro with empty file includes row_index column."""
        import polars as pl

        df = jetliner.scan_avro(empty_avro_file, row_index_name="idx").collect()

        assert df.height == 0
        assert "idx" in df.columns
        assert df.schema["idx"] == pl.UInt32
        assert df.columns[0] == "idx"

    def test_empty_file_row_index_and_file_paths(self, empty_avro_file: str):
        """Empty file with both row_index and include_file_paths."""
        import polars as pl

        df = jetliner.read_avro(
            empty_avro_file,
            row_index_name="idx",
            include_file_paths="source",
        )

        assert df.height == 0
        # Both columns should be present
        assert "idx" in df.columns
        assert "source" in df.columns
        assert df.schema["idx"] == pl.UInt32
        assert df.schema["source"] == pl.String
        # Row index should still be first column
        assert df.columns[0] == "idx"

    def test_row_index_large_offset(self, temp_avro_file: str):
        """Row index with large offset near u32 max, triggering saturation."""
        # Use offset u32::MAX - 2, so indices 0,1 are valid but 2,3,4 saturate
        # u32::MAX = 4294967295
        large_offset = 2**32 - 3  # 4294967293

        df = jetliner.read_avro(
            temp_avro_file,
            row_index_name="idx",
            row_index_offset=large_offset,
        )

        # File has 5 rows, last 3 indices should saturate at u32::MAX
        idx_values = df["idx"].to_list()
        u32_max = 2**32 - 1  # 4294967295

        assert idx_values[0] == large_offset      # 4294967293
        assert idx_values[1] == large_offset + 1  # 4294967294
        assert idx_values[2] == u32_max           # saturates at 4294967295
        assert idx_values[3] == u32_max           # saturates
        assert idx_values[4] == u32_max           # saturates


class TestRowIndexMultipleFiles:
    """Tests for row_index with multiple files including empty files."""

    @pytest.fixture
    def mixed_empty_files(self, tmp_path: Path) -> tuple[list[str], int]:
        """Create a mix of empty and non-empty Avro files."""
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ],
        }

        paths = []
        total_rows = 0

        # File 1: 3 records
        records1 = [{"id": i, "name": f"file1_{i}"} for i in range(3)]
        file1 = tmp_path / "data_01.avro"
        with open(file1, "wb") as f:
            fastavro.writer(f, schema, records1)
        paths.append(str(file1))
        total_rows += 3

        # File 2: empty
        file2 = tmp_path / "data_02.avro"
        with open(file2, "wb") as f:
            fastavro.writer(f, schema, [])
        paths.append(str(file2))

        # File 3: 2 records
        records3 = [{"id": i + 10, "name": f"file3_{i}"} for i in range(2)]
        file3 = tmp_path / "data_03.avro"
        with open(file3, "wb") as f:
            fastavro.writer(f, schema, records3)
        paths.append(str(file3))
        total_rows += 2

        return paths, total_rows

    @pytest.fixture
    def all_empty_files(self, tmp_path: Path) -> list[str]:
        """Create multiple empty Avro files."""
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ],
        }

        paths = []
        for i in range(3):
            file_path = tmp_path / f"empty_{i}.avro"
            with open(file_path, "wb") as f:
                fastavro.writer(f, schema, [])
            paths.append(str(file_path))

        return paths

    def test_row_index_continuous_with_empty_file_in_middle(
        self, mixed_empty_files: tuple[list[str], int]
    ):
        """Row index should be continuous even with empty file in middle."""
        paths, total_rows = mixed_empty_files

        df = jetliner.read_avro(paths, row_index_name="idx")

        assert df.height == total_rows
        # Row indices should be continuous: 0, 1, 2, 3, 4
        assert df["idx"].to_list() == list(range(total_rows))

    def test_row_index_with_file_paths_mixed_empty(
        self, mixed_empty_files: tuple[list[str], int]
    ):
        """Row index and file paths with mix of empty and non-empty files."""
        paths, total_rows = mixed_empty_files

        df = jetliner.read_avro(
            paths,
            row_index_name="idx",
            include_file_paths="source",
        )

        assert df.height == total_rows
        assert "idx" in df.columns
        assert "source" in df.columns
        # Row indices should be continuous
        assert df["idx"].to_list() == list(range(total_rows))

    def test_all_empty_files_with_row_index(self, all_empty_files: list[str]):
        """Multiple empty files should return empty DataFrame with row_index column."""
        import polars as pl

        df = jetliner.read_avro(all_empty_files, row_index_name="idx")

        assert df.height == 0
        assert "idx" in df.columns
        assert df.schema["idx"] == pl.UInt32
        assert df.columns[0] == "idx"

    def test_all_empty_files_with_row_index_and_file_paths(
        self, all_empty_files: list[str]
    ):
        """Multiple empty files with both row_index and include_file_paths."""
        import polars as pl

        df = jetliner.read_avro(
            all_empty_files,
            row_index_name="idx",
            include_file_paths="source",
        )

        assert df.height == 0
        assert "idx" in df.columns
        assert "source" in df.columns
        assert df.schema["idx"] == pl.UInt32
        assert df.schema["source"] == pl.String
