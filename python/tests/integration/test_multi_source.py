"""
Integration tests for multi-source reading and scan_avro/read_avro parity.

Tests cover:
- scan_avro(paths).collect() returns same rows as read_avro(paths)
- Explicit list of files
- Glob patterns (both scan_avro and read_avro support glob expansion)
- Total row count equals sum of individual file row counts
- n_rows limit across files (stops after N total, not N per file)
- row_index continuity across file boundaries
- include_file_paths with multiple files

Requirements tested:
- 2.4: When a glob pattern is provided, the reader expands it to matching files
- 2.7: When multiple files are provided, the reader reads them in sequence
- 4.3: When n_rows is specified, the reader stops after that many rows
- 4.5: When reading multiple files, the n_rows limit applies to the total across all files
- 5.6: When reading multiple files, the row index is continuous across files
- 6.3: When include_file_paths is specified, the reader adds a column containing the source file path
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
def multi_file_avro_dir(tmp_path: Path) -> tuple[Path, int]:
    """Create a directory with multiple Avro files for testing.

    Returns:
        Tuple of (directory path, total row count across all files)
    """
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "value", "type": "double"},
        ],
    }

    total_rows = 0

    # Create 3 files with different data
    for i in range(3):
        records = [
            {"id": i * 10 + j, "name": f"record_{i}_{j}", "value": float(i * 10 + j) * 1.5}
            for j in range(5)
        ]
        total_rows += len(records)

        file_path = tmp_path / f"data_{i:02d}.avro"
        with open(file_path, "wb") as f:
            fastavro.writer(f, schema, records)

    return tmp_path, total_rows


@pytest.fixture
def multi_file_paths(multi_file_avro_dir: tuple[Path, int]) -> tuple[list[str], int]:
    """Return explicit list of file paths and total row count."""
    dir_path, total_rows = multi_file_avro_dir
    paths = sorted([str(p) for p in dir_path.glob("*.avro")])
    return paths, total_rows


@pytest.fixture
def glob_pattern(multi_file_avro_dir: tuple[Path, int]) -> tuple[str, int]:
    """Return glob pattern and total row count."""
    dir_path, total_rows = multi_file_avro_dir
    pattern = str(dir_path / "*.avro")
    return pattern, total_rows


# =============================================================================
# Test Classes
# =============================================================================


class TestScanReadParity:
    """Tests verifying scan_avro().collect() and read_avro() return identical results.

    Requirements: 2.7
    """

    def test_single_file_parity(self, temp_avro_file: str):
        """Test that scan_avro and read_avro return same data for single file."""
        scan_df = jetliner.scan_avro(temp_avro_file).collect()
        read_df = jetliner.read_avro(temp_avro_file)

        # Verify same shape
        assert scan_df.height == read_df.height
        assert scan_df.width == read_df.width

        # Verify same columns
        assert scan_df.columns == read_df.columns

        # Verify same data
        assert scan_df.equals(read_df)

    def test_explicit_file_list_parity(self, multi_file_paths: tuple[list[str], int]):
        """Test parity with explicit list of files."""
        paths, expected_rows = multi_file_paths

        scan_df = jetliner.scan_avro(paths).collect()
        read_df = jetliner.read_avro(paths)

        # Verify same shape
        assert scan_df.height == read_df.height == expected_rows
        assert scan_df.width == read_df.width

        # Verify same columns
        assert scan_df.columns == read_df.columns

        # Verify same data
        assert scan_df.equals(read_df)

    def test_glob_pattern_parity(self, multi_file_avro_dir: tuple[Path, int]):
        """Test parity with glob pattern - both scan_avro and read_avro expand globs.

        Requirement 2.4: When a glob pattern is provided, the reader expands it to matching files.
        """
        dir_path, expected_rows = multi_file_avro_dir
        pattern = str(dir_path / "*.avro")

        # Both scan_avro and read_avro should expand glob patterns
        scan_df = jetliner.scan_avro(pattern).collect()
        read_df = jetliner.read_avro(pattern)

        # Verify same shape
        assert scan_df.height == read_df.height == expected_rows
        assert scan_df.width == read_df.width

        # Verify same columns
        assert scan_df.columns == read_df.columns

        # Verify same data
        assert scan_df.equals(read_df)


class TestExplicitFileList:
    """Tests for reading explicit list of files.

    Requirements: 2.7
    """

    def test_read_multiple_files_as_list(self, multi_file_paths: tuple[list[str], int]):
        """Test reading multiple files provided as a list."""
        paths, expected_rows = multi_file_paths

        df = jetliner.read_avro(paths)

        assert df.height == expected_rows
        assert "id" in df.columns
        assert "name" in df.columns
        assert "value" in df.columns

    def test_scan_multiple_files_as_list(self, multi_file_paths: tuple[list[str], int]):
        """Test scanning multiple files provided as a list."""
        paths, expected_rows = multi_file_paths

        df = jetliner.scan_avro(paths).collect()

        assert df.height == expected_rows
        assert "id" in df.columns
        assert "name" in df.columns
        assert "value" in df.columns

    def test_read_multiple_files_as_tuple(self, multi_file_paths: tuple[list[str], int]):
        """Test reading multiple files provided as a tuple."""
        paths, expected_rows = multi_file_paths

        df = jetliner.read_avro(tuple(paths))

        assert df.height == expected_rows

    def test_file_order_preserved(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that files are read in the order provided."""
        dir_path, _ = multi_file_avro_dir

        # Get paths in specific order
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Read with include_file_paths to verify order
        df = jetliner.read_avro(paths, include_file_paths="source")

        # Get unique file paths in order of appearance
        unique_sources = df["source"].unique(maintain_order=True).to_list()

        # Verify order matches input
        assert len(unique_sources) == len(paths)
        for i, (expected, actual) in enumerate(zip(paths, unique_sources)):
            assert expected == actual, f"File order mismatch at index {i}"


class TestGlobPatterns:
    """Tests for glob pattern expansion with both scan_avro and read_avro.

    Requirement 2.4: When a glob pattern is provided, the reader expands it to matching files.
    """

    def test_read_with_glob_pattern(self, glob_pattern: tuple[str, int]):
        """Test reading files with glob pattern."""
        pattern, expected_rows = glob_pattern

        df = jetliner.read_avro(pattern)

        assert df.height == expected_rows

    def test_scan_with_glob_pattern(self, glob_pattern: tuple[str, int]):
        """Test scanning files with glob pattern.

        Requirement 2.4: scan_avro expands glob patterns to matching files.
        """
        pattern, expected_rows = glob_pattern

        df = jetliner.scan_avro(pattern).collect()

        assert df.height == expected_rows

    def test_glob_with_include_file_paths(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that include_file_paths works with glob patterns."""
        dir_path, _ = multi_file_avro_dir
        pattern = str(dir_path / "*.avro")

        df = jetliner.read_avro(pattern, include_file_paths="source_file")

        # Verify source_file column exists
        assert "source_file" in df.columns

        # Verify we have paths from all files
        unique_paths = df["source_file"].unique().to_list()
        assert len(unique_paths) == 3  # We created 3 files

    def test_scan_glob_with_include_file_paths(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that scan_avro include_file_paths works with glob patterns."""
        dir_path, _ = multi_file_avro_dir
        pattern = str(dir_path / "*.avro")

        df = jetliner.scan_avro(pattern, include_file_paths="source_file").collect()

        # Verify source_file column exists
        assert "source_file" in df.columns

        # Verify we have paths from all files
        unique_paths = df["source_file"].unique().to_list()
        assert len(unique_paths) == 3  # We created 3 files

    def test_glob_deterministic_order(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that glob expansion produces deterministic ordering."""
        dir_path, _ = multi_file_avro_dir
        pattern = str(dir_path / "*.avro")

        # Read twice and verify same order
        df1 = jetliner.read_avro(pattern, include_file_paths="source")
        df2 = jetliner.read_avro(pattern, include_file_paths="source")

        sources1 = df1["source"].unique(maintain_order=True).to_list()
        sources2 = df2["source"].unique(maintain_order=True).to_list()

        assert sources1 == sources2, "Glob expansion should be deterministic"

    def test_scan_glob_deterministic_order(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that scan_avro glob expansion produces deterministic ordering."""
        dir_path, _ = multi_file_avro_dir
        pattern = str(dir_path / "*.avro")

        # Scan twice and verify same order
        df1 = jetliner.scan_avro(pattern, include_file_paths="source").collect()
        df2 = jetliner.scan_avro(pattern, include_file_paths="source").collect()

        sources1 = df1["source"].unique(maintain_order=True).to_list()
        sources2 = df2["source"].unique(maintain_order=True).to_list()

        assert sources1 == sources2, "Glob expansion should be deterministic"


# =============================================================================
# Task 7.2: Multi-file Behavior Tests
# =============================================================================


class TestMultiFileTotalRowCount:
    """Tests verifying total row count equals sum of individual file row counts.

    Requirements: 2.7
    """

    def test_total_rows_equals_sum_of_individual_files(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Verify total row count equals sum of individual file row counts."""
        dir_path, expected_total = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Read each file individually and sum row counts
        individual_row_counts = []
        for path in paths:
            df = jetliner.read_avro(path)
            individual_row_counts.append(df.height)

        sum_of_individual = sum(individual_row_counts)

        # Read all files together
        combined_df = jetliner.read_avro(paths)

        # Verify total equals sum of individual
        assert combined_df.height == sum_of_individual
        assert combined_df.height == expected_total

    def test_scan_total_rows_equals_sum_of_individual_files(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Verify scan_avro total row count equals sum of individual file row counts."""
        dir_path, expected_total = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Read each file individually and sum row counts
        individual_row_counts = []
        for path in paths:
            df = jetliner.scan_avro(path).collect()
            individual_row_counts.append(df.height)

        sum_of_individual = sum(individual_row_counts)

        # Scan all files together
        combined_df = jetliner.scan_avro(paths).collect()

        # Verify total equals sum of individual
        assert combined_df.height == sum_of_individual
        assert combined_df.height == expected_total


class TestNRowsLimitAcrossFiles:
    """Tests for n_rows limit across multiple files.

    Verifies that n_rows stops after N total rows, not N per file.

    Requirements: 4.3, 4.5
    """

    def test_n_rows_stops_after_total_limit(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that n_rows limits total rows across all files."""
        dir_path, total_rows = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Each file has 5 rows, 3 files = 15 total
        # Request 7 rows - should span first file (5) + partial second file (2)
        n_rows_limit = 7

        df = jetliner.read_avro(paths, n_rows=n_rows_limit)

        assert df.height == n_rows_limit, (
            f"Expected exactly {n_rows_limit} rows, got {df.height}"
        )

    def test_n_rows_less_than_first_file(self, multi_file_avro_dir: tuple[Path, int]):
        """Test n_rows limit smaller than first file's row count."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Each file has 5 rows, request only 3
        n_rows_limit = 3

        df = jetliner.read_avro(paths, n_rows=n_rows_limit)

        assert df.height == n_rows_limit

    def test_n_rows_exactly_one_file(self, multi_file_avro_dir: tuple[Path, int]):
        """Test n_rows limit exactly equal to first file's row count."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Each file has 5 rows
        n_rows_limit = 5

        df = jetliner.read_avro(paths, n_rows=n_rows_limit)

        assert df.height == n_rows_limit

    def test_n_rows_spans_multiple_files(self, multi_file_avro_dir: tuple[Path, int]):
        """Test n_rows limit that spans multiple files."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Each file has 5 rows, request 12 (spans 3 files: 5 + 5 + 2)
        n_rows_limit = 12

        df = jetliner.read_avro(paths, n_rows=n_rows_limit)

        assert df.height == n_rows_limit

    def test_n_rows_greater_than_total(self, multi_file_avro_dir: tuple[Path, int]):
        """Test n_rows limit greater than total available rows."""
        dir_path, total_rows = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Request more rows than available
        n_rows_limit = total_rows + 100

        df = jetliner.read_avro(paths, n_rows=n_rows_limit)

        # Should return all available rows
        assert df.height == total_rows

    def test_scan_n_rows_across_files(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that scan_avro n_rows limits total rows across files."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Each file has 5 rows, request 8
        n_rows_limit = 8

        df = jetliner.scan_avro(paths, n_rows=n_rows_limit).collect()

        assert df.height == n_rows_limit


class TestRowIndexContinuity:
    """Tests for row index continuity across file boundaries.

    Requirements: 5.6
    """

    def test_row_index_continuous_across_files(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that row_index is continuous across multiple files."""
        dir_path, total_rows = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        df = jetliner.read_avro(paths, row_index_name="idx")

        # Verify row index column exists
        assert "idx" in df.columns

        # Verify indices are continuous: 0, 1, 2, ..., n-1
        indices = df["idx"].to_list()
        expected = list(range(total_rows))
        assert indices == expected, "Row indices should be continuous"

    def test_row_index_with_offset_across_files(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that row_index_offset works correctly across multiple files."""
        dir_path, total_rows = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        offset = 100
        df = jetliner.read_avro(paths, row_index_name="idx", row_index_offset=offset)

        # Verify indices start at offset and are continuous
        indices = df["idx"].to_list()
        expected = list(range(offset, offset + total_rows))
        assert indices == expected, f"Row indices should start at {offset}"

    def test_row_index_continuous_with_n_rows_limit(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test row index continuity when n_rows limit spans files."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Limit to 8 rows (spans 2 files: 5 + 3)
        n_rows_limit = 8

        df = jetliner.read_avro(paths, row_index_name="idx", n_rows=n_rows_limit)

        # Verify indices are continuous: 0, 1, 2, ..., 7
        indices = df["idx"].to_list()
        expected = list(range(n_rows_limit))
        assert indices == expected

    def test_scan_row_index_continuous_across_files(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that scan_avro row_index is continuous across files."""
        dir_path, total_rows = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        df = jetliner.scan_avro(paths, row_index_name="idx").collect()

        # Verify indices are continuous
        indices = df["idx"].to_list()
        expected = list(range(total_rows))
        assert indices == expected


class TestRowIndexContinuityScanAvro:
    """Additional tests for scan_avro row index edge cases.

    Requirements: 5.5, 5.6
    """

    def test_scan_row_index_with_offset(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that scan_avro row_index_offset works correctly across files."""
        dir_path, total_rows = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        offset = 50
        df = jetliner.scan_avro(paths, row_index_name="idx", row_index_offset=offset).collect()

        # Verify indices start at offset and are continuous
        indices = df["idx"].to_list()
        expected = list(range(offset, offset + total_rows))
        assert indices == expected, f"Row indices should start at {offset}"

    def test_scan_row_index_with_n_rows(self, multi_file_avro_dir: tuple[Path, int]):
        """Test scan_avro row index continuity with n_rows limit."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        n_rows_limit = 8
        df = jetliner.scan_avro(paths, row_index_name="idx", n_rows=n_rows_limit).collect()

        # Verify indices are continuous: 0, 1, 2, ..., 7
        indices = df["idx"].to_list()
        expected = list(range(n_rows_limit))
        assert indices == expected

    def test_row_index_column_type_is_uint32(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that row index column is UInt32 type (Requirement 5.5)."""
        import polars as pl

        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        df = jetliner.read_avro(paths, row_index_name="idx")
        assert df["idx"].dtype == pl.UInt32, f"Expected UInt32, got {df['idx'].dtype}"

        df_scan = jetliner.scan_avro(paths, row_index_name="idx").collect()
        assert df_scan["idx"].dtype == pl.UInt32, f"Expected UInt32, got {df_scan['idx'].dtype}"


class TestIncludeFilePathsMultiFile:
    """Tests for include_file_paths with multiple files.

    Requirements: 6.3, 6.4, 6.7
    """

    def test_include_file_paths_shows_correct_source(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that include_file_paths correctly identifies source file for each row."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        df = jetliner.read_avro(paths, include_file_paths="source_file")

        # Verify source_file column exists
        assert "source_file" in df.columns

        # Verify we have entries from all files
        unique_sources = df["source_file"].unique().to_list()
        assert len(unique_sources) == len(paths)

        # Verify each source path matches one of the input paths
        for source in unique_sources:
            assert source in paths, f"Unexpected source path: {source}"

    def test_include_file_paths_row_count_per_file(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that each file contributes the correct number of rows."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        df = jetliner.read_avro(paths, include_file_paths="source_file")

        # Count rows per source file
        rows_per_file = df.group_by("source_file").len()

        # Each file should have 5 rows (as defined in fixture)
        for row in rows_per_file.iter_rows():
            source_file, count = row
            assert count == 5, f"File {source_file} should have 5 rows, got {count}"

    def test_include_file_paths_preserves_order(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that file paths appear in the order files were read."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        df = jetliner.read_avro(paths, include_file_paths="source_file")

        # Get unique sources in order of first appearance
        unique_sources = df["source_file"].unique(maintain_order=True).to_list()

        # Should match input order
        assert unique_sources == paths

    def test_scan_include_file_paths_multi_file(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that scan_avro include_file_paths works with multiple files."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        df = jetliner.scan_avro(paths, include_file_paths="source_file").collect()

        # Verify source_file column exists
        assert "source_file" in df.columns

        # Verify we have entries from all files
        unique_sources = df["source_file"].unique().to_list()
        assert len(unique_sources) == len(paths)

    def test_include_file_paths_with_row_index(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that include_file_paths and row_index work together."""
        dir_path, total_rows = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        df = jetliner.read_avro(
            paths, include_file_paths="source_file", row_index_name="idx"
        )

        # Both columns should exist
        assert "source_file" in df.columns
        assert "idx" in df.columns

        # Row index should be continuous
        indices = df["idx"].to_list()
        expected = list(range(total_rows))
        assert indices == expected

        # Source file should have correct entries
        unique_sources = df["source_file"].unique().to_list()
        assert len(unique_sources) == len(paths)

    def test_include_file_paths_with_n_rows(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that include_file_paths works correctly with n_rows limit."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Limit to 8 rows (spans 2 files: 5 + 3)
        n_rows_limit = 8

        df = jetliner.read_avro(
            paths, include_file_paths="source_file", n_rows=n_rows_limit
        )

        assert df.height == n_rows_limit

        # Should have rows from first 2 files only
        unique_sources = df["source_file"].unique().to_list()
        assert len(unique_sources) == 2

        # First file should have all 5 rows, second file should have 3
        rows_per_file = df.group_by("source_file").len().sort("source_file")
        counts = rows_per_file["len"].to_list()
        assert counts == [5, 3], f"Expected [5, 3] rows per file, got {counts}"


    def test_file_path_column_type_is_string(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that file path column is String type (Requirement 6.4)."""
        import polars as pl

        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        df = jetliner.read_avro(paths, include_file_paths="source_file")
        assert df["source_file"].dtype == pl.String, f"Expected String, got {df['source_file'].dtype}"

        df_scan = jetliner.scan_avro(paths, include_file_paths="source_file").collect()
        assert df_scan["source_file"].dtype == pl.String, f"Expected String, got {df_scan['source_file'].dtype}"

    def test_include_file_paths_single_file(self, temp_avro_file: str):
        """Test that include_file_paths works with single file (Requirement 6.7)."""
        df = jetliner.read_avro(temp_avro_file, include_file_paths="source")

        assert "source" in df.columns
        # All rows should have the same source path
        unique_sources = df["source"].unique().to_list()
        assert len(unique_sources) == 1
        assert unique_sources[0] == temp_avro_file

    def test_scan_include_file_paths_single_file(self, temp_avro_file: str):
        """Test that scan_avro include_file_paths works with single file."""
        df = jetliner.scan_avro(temp_avro_file, include_file_paths="source").collect()

        assert "source" in df.columns
        unique_sources = df["source"].unique().to_list()
        assert len(unique_sources) == 1
        assert unique_sources[0] == temp_avro_file

    def test_scan_include_file_paths_with_n_rows(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that scan_avro include_file_paths works with n_rows limit."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        # Limit to 8 rows (spans 2 files: 5 + 3)
        n_rows_limit = 8

        df = jetliner.scan_avro(
            paths, include_file_paths="source_file", n_rows=n_rows_limit
        ).collect()

        assert df.height == n_rows_limit

        # Should have rows from first 2 files only
        unique_sources = df["source_file"].unique().to_list()
        assert len(unique_sources) == 2


class TestCombinedFeatures:
    """Tests for combinations of row_index, include_file_paths, and n_rows.

    These tests verify that all features work correctly together.
    """

    def test_all_features_combined_read(self, multi_file_avro_dir: tuple[Path, int]):
        """Test read_avro with row_index, include_file_paths, and n_rows together."""
        import polars as pl

        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        n_rows_limit = 10
        offset = 100

        df = jetliner.read_avro(
            paths,
            row_index_name="idx",
            row_index_offset=offset,
            include_file_paths="source",
            n_rows=n_rows_limit,
        )

        # Verify row count
        assert df.height == n_rows_limit

        # Verify row index
        indices = df["idx"].to_list()
        expected_indices = list(range(offset, offset + n_rows_limit))
        assert indices == expected_indices

        # Verify column types
        assert df["idx"].dtype == pl.UInt32
        assert df["source"].dtype == pl.String

        # Verify file paths are present
        assert "source" in df.columns
        unique_sources = df["source"].unique().to_list()
        assert len(unique_sources) == 2  # 10 rows spans 2 files (5 + 5)

    def test_all_features_combined_scan(self, multi_file_avro_dir: tuple[Path, int]):
        """Test scan_avro with row_index, include_file_paths, and n_rows together."""
        import polars as pl

        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        n_rows_limit = 10
        offset = 100

        df = jetliner.scan_avro(
            paths,
            row_index_name="idx",
            row_index_offset=offset,
            include_file_paths="source",
            n_rows=n_rows_limit,
        ).collect()

        # Verify row count
        assert df.height == n_rows_limit

        # Verify row index
        indices = df["idx"].to_list()
        expected_indices = list(range(offset, offset + n_rows_limit))
        assert indices == expected_indices

        # Verify column types
        assert df["idx"].dtype == pl.UInt32
        assert df["source"].dtype == pl.String

        # Verify file paths are present
        assert "source" in df.columns
        unique_sources = df["source"].unique().to_list()
        assert len(unique_sources) == 2  # 10 rows spans 2 files (5 + 5)

    def test_row_index_is_first_column(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that row index is inserted as the first column (Requirement 5.3)."""
        dir_path, _ = multi_file_avro_dir
        paths = sorted([str(p) for p in dir_path.glob("*.avro")])

        df = jetliner.read_avro(paths, row_index_name="idx")
        assert df.columns[0] == "idx", f"Expected 'idx' as first column, got {df.columns[0]}"

        df_scan = jetliner.scan_avro(paths, row_index_name="idx").collect()
        assert df_scan.columns[0] == "idx", f"Expected 'idx' as first column, got {df_scan.columns[0]}"


# =============================================================================
# Task 7.5: Input Style Tests - Path Objects
# =============================================================================


class TestPathObjectInput:
    """Tests for pathlib.Path object input support.

    Requirements: 2.2 - The source parameter accepts pathlib.Path objects
    """

    def test_read_avro_with_path_object(self, temp_avro_file: str):
        """Test that read_avro accepts a pathlib.Path object."""
        path_obj = Path(temp_avro_file)

        df = jetliner.read_avro(path_obj)

        assert df.height > 0
        assert "id" in df.columns

    def test_scan_avro_with_path_object(self, temp_avro_file: str):
        """Test that scan_avro accepts a pathlib.Path object."""
        path_obj = Path(temp_avro_file)

        df = jetliner.scan_avro(path_obj).collect()

        assert df.height > 0
        assert "id" in df.columns

    def test_read_avro_with_list_of_path_objects(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that read_avro accepts a list of pathlib.Path objects."""
        dir_path, expected_rows = multi_file_avro_dir
        path_objects = sorted(dir_path.glob("*.avro"))

        df = jetliner.read_avro(path_objects)

        assert df.height == expected_rows

    def test_scan_avro_with_list_of_path_objects(
        self, multi_file_avro_dir: tuple[Path, int]
    ):
        """Test that scan_avro accepts a list of pathlib.Path objects."""
        dir_path, expected_rows = multi_file_avro_dir
        path_objects = sorted(dir_path.glob("*.avro"))

        df = jetliner.scan_avro(path_objects).collect()

        assert df.height == expected_rows

    def test_read_avro_schema_with_path_object(self, temp_avro_file: str):
        """Test that read_avro_schema accepts a pathlib.Path object."""
        path_obj = Path(temp_avro_file)

        schema = jetliner.read_avro_schema(path_obj)

        assert schema is not None
        assert "id" in schema

    def test_path_and_string_produce_same_results(self, temp_avro_file: str):
        """Test that Path object and string path produce identical results."""
        path_obj = Path(temp_avro_file)
        path_str = temp_avro_file

        df_path = jetliner.read_avro(path_obj)
        df_str = jetliner.read_avro(path_str)

        assert df_path.equals(df_str)

    def test_scan_path_and_string_produce_same_results(self, temp_avro_file: str):
        """Test that scan_avro with Path and string produce identical results."""
        path_obj = Path(temp_avro_file)
        path_str = temp_avro_file

        df_path = jetliner.scan_avro(path_obj).collect()
        df_str = jetliner.scan_avro(path_str).collect()

        assert df_path.equals(df_str)

    def test_mixed_path_and_string_list(self, multi_file_avro_dir: tuple[Path, int]):
        """Test that a mixed list of Path objects and strings works."""
        dir_path, expected_rows = multi_file_avro_dir
        all_paths = sorted(dir_path.glob("*.avro"))

        # Mix Path objects and strings
        mixed_paths = [
            all_paths[0],  # Path object
            str(all_paths[1]),  # string
            all_paths[2],  # Path object
        ]

        df = jetliner.read_avro(mixed_paths)

        assert df.height == expected_rows


# =============================================================================
# Task 7.3: Schema Validation Across Files
# =============================================================================


# =============================================================================
# Multiple Glob Patterns Tests
# =============================================================================


class TestMultipleGlobPatterns:
    """Tests for multiple glob patterns in the same call.

    Verifies that jetliner can handle a list of glob patterns where each
    pattern is expanded independently.

    Requirements: 2.4, 2.7
    """

    @pytest.fixture
    def multi_dir_avro_files(self, tmp_path: Path) -> tuple[Path, Path, int]:
        """Create two directories with Avro files for testing multiple glob patterns.

        Returns:
            Tuple of (dir1 path, dir2 path, total row count across all files)
        """
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "value", "type": "double"},
            ],
        }

        total_rows = 0

        # Create first directory with 2 files
        dir1 = tmp_path / "dir1"
        dir1.mkdir()
        for i in range(2):
            records = [
                {"id": i * 10 + j, "name": f"dir1_record_{i}_{j}", "value": float(i * 10 + j) * 1.5}
                for j in range(4)
            ]
            total_rows += len(records)
            file_path = dir1 / f"data_{i:02d}.avro"
            with open(file_path, "wb") as f:
                fastavro.writer(f, schema, records)

        # Create second directory with 3 files
        dir2 = tmp_path / "dir2"
        dir2.mkdir()
        for i in range(3):
            records = [
                {"id": 100 + i * 10 + j, "name": f"dir2_record_{i}_{j}", "value": float(100 + i * 10 + j) * 1.5}
                for j in range(3)
            ]
            total_rows += len(records)
            file_path = dir2 / f"file_{i:02d}.avro"
            with open(file_path, "wb") as f:
                fastavro.writer(f, schema, records)

        return dir1, dir2, total_rows

    def test_read_avro_multiple_glob_patterns(
        self, multi_dir_avro_files: tuple[Path, Path, int]
    ):
        """Test reading with multiple glob patterns in a list."""
        dir1, dir2, expected_rows = multi_dir_avro_files

        # Two glob patterns, each matching files in different directories
        patterns = [
            str(dir1 / "*.avro"),
            str(dir2 / "*.avro"),
        ]

        df = jetliner.read_avro(patterns)

        assert df.height == expected_rows
        assert "id" in df.columns
        assert "name" in df.columns
        assert "value" in df.columns

    def test_scan_avro_multiple_glob_patterns(
        self, multi_dir_avro_files: tuple[Path, Path, int]
    ):
        """Test scanning with multiple glob patterns in a list."""
        dir1, dir2, expected_rows = multi_dir_avro_files

        patterns = [
            str(dir1 / "*.avro"),
            str(dir2 / "*.avro"),
        ]

        df = jetliner.scan_avro(patterns).collect()

        assert df.height == expected_rows

    def test_multiple_glob_patterns_with_include_file_paths(
        self, multi_dir_avro_files: tuple[Path, Path, int]
    ):
        """Test that include_file_paths works with multiple glob patterns."""
        dir1, dir2, expected_rows = multi_dir_avro_files

        patterns = [
            str(dir1 / "*.avro"),
            str(dir2 / "*.avro"),
        ]

        df = jetliner.read_avro(patterns, include_file_paths="source_file")

        assert df.height == expected_rows
        assert "source_file" in df.columns

        # Should have files from both directories
        unique_paths = df["source_file"].unique().to_list()
        assert len(unique_paths) == 5  # 2 files from dir1 + 3 files from dir2

        # Verify we have paths from both directories
        dir1_paths = [p for p in unique_paths if "dir1" in p]
        dir2_paths = [p for p in unique_paths if "dir2" in p]
        assert len(dir1_paths) == 2
        assert len(dir2_paths) == 3

    def test_multiple_glob_patterns_with_row_index(
        self, multi_dir_avro_files: tuple[Path, Path, int]
    ):
        """Test that row_index is continuous across multiple glob patterns."""
        dir1, dir2, expected_rows = multi_dir_avro_files

        patterns = [
            str(dir1 / "*.avro"),
            str(dir2 / "*.avro"),
        ]

        df = jetliner.read_avro(patterns, row_index_name="idx")

        assert df.height == expected_rows
        assert "idx" in df.columns

        # Verify row index is continuous
        indices = df["idx"].to_list()
        expected = list(range(expected_rows))
        assert indices == expected, "Row indices should be continuous across multiple glob patterns"

    def test_multiple_glob_patterns_with_n_rows(
        self, multi_dir_avro_files: tuple[Path, Path, int]
    ):
        """Test that n_rows limit works across multiple glob patterns."""
        dir1, dir2, _ = multi_dir_avro_files

        patterns = [
            str(dir1 / "*.avro"),
            str(dir2 / "*.avro"),
        ]

        n_rows_limit = 10

        df = jetliner.read_avro(patterns, n_rows=n_rows_limit)

        assert df.height == n_rows_limit

    def test_multiple_glob_patterns_parity(
        self, multi_dir_avro_files: tuple[Path, Path, int]
    ):
        """Test that scan_avro and read_avro produce same results for multiple glob patterns."""
        dir1, dir2, expected_rows = multi_dir_avro_files

        patterns = [
            str(dir1 / "*.avro"),
            str(dir2 / "*.avro"),
        ]

        scan_df = jetliner.scan_avro(patterns).collect()
        read_df = jetliner.read_avro(patterns)

        assert scan_df.height == read_df.height == expected_rows
        assert scan_df.columns == read_df.columns
        assert scan_df.equals(read_df)

    def test_glob_pattern_mixed_with_explicit_path(
        self, multi_dir_avro_files: tuple[Path, Path, int]
    ):
        """Test mixing a glob pattern with an explicit file path."""
        dir1, dir2, _ = multi_dir_avro_files

        # Get one explicit file from dir1
        explicit_file = str(sorted(dir1.glob("*.avro"))[0])
        # Use glob pattern for dir2
        glob_pattern = str(dir2 / "*.avro")

        # Mix explicit path and glob pattern
        sources = [explicit_file, glob_pattern]

        df = jetliner.read_avro(sources, include_file_paths="source_file")

        # Should have 1 file from explicit + 3 files from glob
        unique_paths = df["source_file"].unique().to_list()
        assert len(unique_paths) == 4

    def test_overlapping_glob_patterns_deduplicated(
        self, multi_dir_avro_files: tuple[Path, Path, int]
    ):
        """Test that overlapping glob patterns are deduplicated."""
        dir1, _, _ = multi_dir_avro_files

        # Two patterns that match the same files
        patterns = [
            str(dir1 / "*.avro"),
            str(dir1 / "data_*.avro"),  # Same files, different pattern
        ]

        df = jetliner.read_avro(patterns, include_file_paths="source_file")

        # Should deduplicate - only 2 unique files from dir1
        unique_paths = df["source_file"].unique().to_list()
        assert len(unique_paths) == 2


class TestSchemaValidation:
    """Tests for schema validation behavior.

    Multi-file reads require identical schemas. Any schema mismatch raises an error.
    """

    @pytest.fixture
    def mismatched_schema_files(self, tmp_path: Path) -> tuple[list[str], int]:
        """Create files with different schemas for testing.

        Returns:
            Tuple of (list of file paths, expected total rows if schemas matched)
        """
        import fastavro

        # First file: has id, name, value
        schema1 = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "value", "type": "double"},
            ],
        }
        records1 = [
            {"id": 1, "name": "Alice", "value": 1.5},
            {"id": 2, "name": "Bob", "value": 2.5},
        ]
        file1 = tmp_path / "file1.avro"
        with open(file1, "wb") as f:
            fastavro.writer(f, schema1, records1)

        # Second file: has id, name, email (different field)
        schema2 = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
            ],
        }
        records2 = [
            {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
            {"id": 4, "name": "Diana", "email": "diana@example.com"},
        ]
        file2 = tmp_path / "file2.avro"
        with open(file2, "wb") as f:
            fastavro.writer(f, schema2, records2)

        return [str(file1), str(file2)], 4

    def test_read_avro_raises_on_schema_mismatch(
        self, mismatched_schema_files: tuple[list[str], int]
    ):
        """Test that read_avro raises error on schema mismatch."""
        paths, _ = mismatched_schema_files

        with pytest.raises(Exception) as exc_info:
            jetliner.read_avro(paths)

        # Verify error message mentions schema mismatch
        error_msg = str(exc_info.value).lower()
        assert "schema" in error_msg or "mismatch" in error_msg or "identical" in error_msg

    def test_scan_avro_raises_on_schema_mismatch(
        self, mismatched_schema_files: tuple[list[str], int]
    ):
        """Test that scan_avro raises error on schema mismatch.

        Note: scan_avro validates schemas at collect time via Polars' vstack operation.
        The error message may mention 'vstack' or 'column names don't match'.
        """
        paths, _ = mismatched_schema_files

        # Should raise an error when collecting
        with pytest.raises(Exception):
            jetliner.scan_avro(paths).collect()

    def test_identical_schemas_succeed(
        self, multi_file_paths: tuple[list[str], int]
    ):
        """Test that identical schemas work correctly."""
        paths, expected_rows = multi_file_paths

        # Should succeed without error
        df = jetliner.read_avro(paths)

        assert df.height == expected_rows


# =============================================================================
# Field-Order Coercion Tests
# =============================================================================


class TestFieldOrderCoercion:
    """Tests for field-order coercion when reading multiple Avro files.

    When files have the same fields in different order, jetliner should
    automatically reorder columns to match the canonical (first file) schema.

    This feature enables reading files with compatible schemas that differ
    only in field ordering, which is common when files are generated by
    different tools or systems.
    """

    @pytest.fixture
    def field_order_files(self, tmp_path: Path) -> tuple[str, str, str]:
        """Create two files with same fields in different order.

        Returns:
            Tuple of (file1_path, file2_path, file3_path)
            - file1: fields in order [id, name, value]
            - file2: fields in order [name, id, value]
            - file3: fields in order [value, name, id]
        """
        # File 1: canonical order [id, name, value]
        schema1 = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "value", "type": "double"},
            ],
        }
        records1 = [
            {"id": 1, "name": "Alice", "value": 1.5},
            {"id": 2, "name": "Bob", "value": 2.5},
        ]
        file1 = tmp_path / "file1.avro"
        with open(file1, "wb") as f:
            fastavro.writer(f, schema1, records1)

        # File 2: different order [name, id, value]
        schema2 = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "id", "type": "int"},
                {"name": "value", "type": "double"},
            ],
        }
        records2 = [
            {"name": "Charlie", "id": 3, "value": 3.5},
            {"name": "Diana", "id": 4, "value": 4.5},
        ]
        file2 = tmp_path / "file2.avro"
        with open(file2, "wb") as f:
            fastavro.writer(f, schema2, records2)

        # File 3: another different order [value, name, id]
        schema3 = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "value", "type": "double"},
                {"name": "name", "type": "string"},
                {"name": "id", "type": "int"},
            ],
        }
        records3 = [
            {"value": 5.5, "name": "Eve", "id": 5},
            {"value": 6.5, "name": "Frank", "id": 6},
        ]
        file3 = tmp_path / "file3.avro"
        with open(file3, "wb") as f:
            fastavro.writer(f, schema3, records3)

        return str(file1), str(file2), str(file3)

    def test_same_fields_different_order_succeeds(
        self, field_order_files: tuple[str, str, str]
    ):
        """Files with same fields in different order should read correctly."""
        file1, file2, file3 = field_order_files

        # Reading files with different field orders should succeed
        df = jetliner.read_avro([file1, file2, file3])

        # Should have all 6 rows (2 from each file)
        assert df.height == 6

        # Should have all columns
        assert set(df.columns) == {"id", "name", "value"}

    def test_columns_match_first_file_order(
        self, field_order_files: tuple[str, str, str]
    ):
        """Output column order should match first file."""
        file1, file2, file3 = field_order_files

        # Read with file1 first (canonical: id, name, value)
        df = jetliner.read_avro([file1, file2, file3])

        # Column order should match file1's schema
        assert df.columns == ["id", "name", "value"]

    def test_columns_match_second_file_when_first(
        self, field_order_files: tuple[str, str, str]
    ):
        """When file2 is first, its column order becomes canonical."""
        file1, file2, file3 = field_order_files

        # Read with file2 first (canonical: name, id, value)
        df = jetliner.read_avro([file2, file1, file3])

        # Column order should match file2's schema
        assert df.columns == ["name", "id", "value"]

    def test_data_correctly_aligned_after_reorder(
        self, field_order_files: tuple[str, str, str]
    ):
        """Values should be in correct columns after reorder."""
        file1, file2, file3 = field_order_files

        df = jetliner.read_avro([file1, file2, file3])

        # Convert to list of dicts for easier checking
        rows = df.to_dicts()

        # Verify all data is correctly aligned
        expected_data = [
            {"id": 1, "name": "Alice", "value": 1.5},
            {"id": 2, "name": "Bob", "value": 2.5},
            {"id": 3, "name": "Charlie", "value": 3.5},
            {"id": 4, "name": "Diana", "value": 4.5},
            {"id": 5, "name": "Eve", "value": 5.5},
            {"id": 6, "name": "Frank", "value": 6.5},
        ]

        assert rows == expected_data

    def test_identical_schemas_no_overhead(
        self, multi_file_paths: tuple[list[str], int]
    ):
        """Identical schemas should take fast path (no reorder)."""
        paths, expected_rows = multi_file_paths

        # This should use the fast path (identical JSON schemas)
        df = jetliner.read_avro(paths)

        assert df.height == expected_rows

        # Verify data is correct
        assert "id" in df.columns
        assert "name" in df.columns
        assert "value" in df.columns

    def test_scan_avro_same_fields_different_order(
        self, field_order_files: tuple[str, str, str]
    ):
        """scan_avro should also handle different field orders."""
        file1, file2, file3 = field_order_files

        df = jetliner.scan_avro([file1, file2, file3]).collect()

        # Should have all 6 rows
        assert df.height == 6

        # Column order should match first file
        assert df.columns == ["id", "name", "value"]

    def test_field_order_with_include_file_paths(
        self, field_order_files: tuple[str, str, str]
    ):
        """Field order coercion should work with include_file_paths."""
        file1, file2, file3 = field_order_files

        df = jetliner.read_avro(
            [file1, file2, file3], include_file_paths="source_file"
        )

        assert df.height == 6
        assert "source_file" in df.columns

        # Verify file paths are correct for each row
        rows = df.to_dicts()

        # First 2 rows from file1
        assert rows[0]["source_file"] == file1
        assert rows[1]["source_file"] == file1

        # Next 2 rows from file2
        assert rows[2]["source_file"] == file2
        assert rows[3]["source_file"] == file2

        # Last 2 rows from file3
        assert rows[4]["source_file"] == file3
        assert rows[5]["source_file"] == file3

    def test_field_order_with_row_index(
        self, field_order_files: tuple[str, str, str]
    ):
        """Field order coercion should work with row_index."""
        file1, file2, file3 = field_order_files

        df = jetliner.read_avro([file1, file2, file3], row_index_name="idx")

        assert df.height == 6
        assert "idx" in df.columns

        # Row index should be continuous
        indices = df["idx"].to_list()
        assert indices == [0, 1, 2, 3, 4, 5]

    def test_field_order_with_n_rows(
        self, field_order_files: tuple[str, str, str]
    ):
        """Field order coercion should work with n_rows limit."""
        file1, file2, file3 = field_order_files

        # Limit to 4 rows (spans file1 and file2)
        df = jetliner.read_avro([file1, file2, file3], n_rows=4)

        assert df.height == 4

        # Data should be correctly aligned
        rows = df.to_dicts()
        expected_data = [
            {"id": 1, "name": "Alice", "value": 1.5},
            {"id": 2, "name": "Bob", "value": 2.5},
            {"id": 3, "name": "Charlie", "value": 3.5},
            {"id": 4, "name": "Diana", "value": 4.5},
        ]
        assert rows == expected_data

    def test_field_order_parity_read_scan(
        self, field_order_files: tuple[str, str, str]
    ):
        """read_avro and scan_avro should produce identical results."""
        file1, file2, file3 = field_order_files

        read_df = jetliner.read_avro([file1, file2, file3])
        scan_df = jetliner.scan_avro([file1, file2, file3]).collect()

        assert read_df.equals(scan_df)

    def test_field_order_two_files_only(
        self, field_order_files: tuple[str, str, str]
    ):
        """Test with just two files having different field order."""
        file1, file2, _ = field_order_files

        df = jetliner.read_avro([file1, file2])

        assert df.height == 4
        assert df.columns == ["id", "name", "value"]

        rows = df.to_dicts()
        expected_data = [
            {"id": 1, "name": "Alice", "value": 1.5},
            {"id": 2, "name": "Bob", "value": 2.5},
            {"id": 3, "name": "Charlie", "value": 3.5},
            {"id": 4, "name": "Diana", "value": 4.5},
        ]
        assert rows == expected_data
