"""Tests for S3 glob pattern expansion.

These tests verify that jetliner can expand glob patterns for S3 URIs
and read multiple files matching the pattern.

Requirements tested:
- 2.4: When a glob pattern is provided, the reader expands it to matching files
- 8.4: The `max_retries` key controls retry count for transient S3 failures
"""

from __future__ import annotations

import polars as pl
import pytest

import jetliner

from .conftest import MockS3Context


@pytest.mark.container
class TestS3GlobExpansion:
    """Tests for S3 glob pattern expansion.

    Requirements: 2.4
    """

    def test_scan_with_glob_pattern(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that scan() expands glob patterns for S3 URIs.

        Uploads multiple Avro files to MinIO and verifies that
        jetliner.scan_avro() with a glob pattern reads all matching files.
        """
        # Upload multiple weather files with different names
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload as multiple files with a common prefix
        uri1 = mock_s3_minio.upload_file(local_path, "data/weather_2024_01.avro")
        mock_s3_minio.upload_file(local_path, "data/weather_2024_02.avro")
        mock_s3_minio.upload_file(local_path, "data/weather_2024_03.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read single file to get expected row count per file
        single_df = jetliner.read_avro(uri1, storage_options=storage_options)
        rows_per_file = single_df.height

        # Read with glob pattern
        glob_uri = f"s3://{mock_s3_minio.bucket}/data/weather_2024_*.avro"
        glob_df = jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Verify we got data from all 3 files
        assert glob_df.height == rows_per_file * 3
        assert isinstance(glob_df, pl.DataFrame)

    def test_scan_with_recursive_glob(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that scan() handles recursive glob patterns (**).

        Uploads files in nested directories and verifies that
        recursive glob patterns match files at all levels.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload files in nested directories
        mock_s3_minio.upload_file(local_path, "data/2024/01/weather.avro")
        mock_s3_minio.upload_file(local_path, "data/2024/02/weather.avro")
        mock_s3_minio.upload_file(local_path, "data/2025/01/weather.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read single file to get expected row count per file
        single_uri = f"s3://{mock_s3_minio.bucket}/data/2024/01/weather.avro"
        single_df = jetliner.read_avro(single_uri, storage_options=storage_options)
        rows_per_file = single_df.height

        # Read with recursive glob pattern
        glob_uri = f"s3://{mock_s3_minio.bucket}/data/**/*.avro"
        glob_df = jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Verify we got data from all 3 files
        assert glob_df.height == rows_per_file * 3

    def test_scan_glob_no_matches(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that scan() raises error when glob pattern matches no files."""
        local_path = get_test_data_path("apache-avro/weather.avro")
        mock_s3_minio.upload_file(local_path, "data/weather.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Try to read with a pattern that matches nothing
        glob_uri = f"s3://{mock_s3_minio.bucket}/nonexistent/*.avro"

        with pytest.raises(Exception) as exc_info:
            jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Should indicate no files matched
        assert "No" in str(exc_info.value) or "not found" in str(exc_info.value).lower()

    def test_scan_glob_disabled(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that glob=False treats the path literally."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload a file with a literal asterisk in the name (unusual but valid)
        mock_s3_minio.upload_file(local_path, "data/weather.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # With glob=False, the pattern should be treated literally
        # This should fail because there's no file named "*.avro"
        glob_uri = f"s3://{mock_s3_minio.bucket}/data/*.avro"

        with pytest.raises(Exception):
            jetliner.read_avro(glob_uri, glob=False, storage_options=storage_options)

    def test_scan_with_include_file_paths(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that include_file_paths works with glob patterns."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload multiple files
        mock_s3_minio.upload_file(local_path, "data/file1.avro")
        mock_s3_minio.upload_file(local_path, "data/file2.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read with glob pattern and include_file_paths
        glob_uri = f"s3://{mock_s3_minio.bucket}/data/*.avro"
        df = jetliner.read_avro(
            glob_uri,
            storage_options=storage_options,
            include_file_paths="source_file",
        )

        # Verify source_file column exists
        assert "source_file" in df.columns

        # Verify we have paths from both files
        unique_paths = df["source_file"].unique().to_list()
        assert len(unique_paths) == 2
        assert all("s3://" in path for path in unique_paths)


@pytest.mark.container
class TestS3GlobPagination:
    """Tests for S3 glob pagination handling.

    Verifies that glob expansion handles large buckets with pagination.
    """

    def test_glob_with_many_files(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test glob expansion with many files (tests pagination).

        Note: This test uploads fewer files than the S3 pagination limit (1000)
        but verifies the basic pagination logic works.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload 10 files (enough to verify iteration works)
        num_files = 10
        for i in range(num_files):
            mock_s3_minio.upload_file(local_path, f"batch/file_{i:03d}.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read single file to get expected row count per file
        single_uri = f"s3://{mock_s3_minio.bucket}/batch/file_000.avro"
        single_df = jetliner.read_avro(single_uri, storage_options=storage_options)
        rows_per_file = single_df.height

        # Read with glob pattern
        glob_uri = f"s3://{mock_s3_minio.bucket}/batch/*.avro"
        glob_df = jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Verify we got data from all files
        assert glob_df.height == rows_per_file * num_files


@pytest.mark.container
class TestS3RetryBehavior:
    """Tests for S3 retry behavior.

    Requirements: 8.4, 8.5, 8.6
    """

    def test_max_retries_in_storage_options(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that max_retries can be set via storage_options."""
        local_path = get_test_data_path("apache-avro/weather.avro")
        s3_uri = mock_s3_minio.upload_file(local_path, "retry_test.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
            "max_retries": "5",  # String value as per storage_options convention
        }

        # Should work without errors
        df = jetliner.read_avro(s3_uri, storage_options=storage_options)
        assert df.height > 0

    def test_zero_retries(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that max_retries=0 disables retries."""
        local_path = get_test_data_path("apache-avro/weather.avro")
        s3_uri = mock_s3_minio.upload_file(local_path, "no_retry_test.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
            "max_retries": "0",
        }

        # Should still work for successful requests
        df = jetliner.read_avro(s3_uri, storage_options=storage_options)
        assert df.height > 0


@pytest.mark.container
class TestS3GlobWithRowIndex:
    """Tests for S3 glob with row index tracking.

    Verifies that row indices are continuous across multiple files.
    """

    def test_row_index_continuous_across_files(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that row_index is continuous across glob-matched files."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload multiple files
        mock_s3_minio.upload_file(local_path, "indexed/file1.avro")
        mock_s3_minio.upload_file(local_path, "indexed/file2.avro")
        mock_s3_minio.upload_file(local_path, "indexed/file3.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read with glob pattern and row index
        glob_uri = f"s3://{mock_s3_minio.bucket}/indexed/*.avro"
        df = jetliner.read_avro(
            glob_uri,
            storage_options=storage_options,
            row_index_name="idx",
        )

        # Verify row index is continuous (0, 1, 2, ..., n-1)
        indices = df["idx"].to_list()
        expected = list(range(len(indices)))
        assert indices == expected, "Row indices should be continuous"

    def test_row_index_with_offset(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that row_index_offset works with glob patterns."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        mock_s3_minio.upload_file(local_path, "offset/file1.avro")
        mock_s3_minio.upload_file(local_path, "offset/file2.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read with row index starting at 100
        glob_uri = f"s3://{mock_s3_minio.bucket}/offset/*.avro"
        df = jetliner.read_avro(
            glob_uri,
            storage_options=storage_options,
            row_index_name="idx",
            row_index_offset=100,
        )

        # Verify row index starts at 100
        indices = df["idx"].to_list()
        expected = list(range(100, 100 + len(indices)))
        assert indices == expected, "Row indices should start at offset"


# =============================================================================
# scan_avro S3 Glob Tests
# =============================================================================


@pytest.mark.container
class TestS3GlobScanAvro:
    """Tests for scan_avro() with S3 glob patterns.

    Verifies that scan_avro expands S3 glob patterns correctly.
    Requirements: 2.4
    """

    def test_scan_avro_with_s3_glob_pattern(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that scan_avro() expands S3 glob patterns."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload multiple files
        mock_s3_minio.upload_file(local_path, "scan_glob/file1.avro")
        mock_s3_minio.upload_file(local_path, "scan_glob/file2.avro")
        mock_s3_minio.upload_file(local_path, "scan_glob/file3.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read single file to get expected row count per file
        single_uri = f"s3://{mock_s3_minio.bucket}/scan_glob/file1.avro"
        single_df = jetliner.scan_avro(single_uri, storage_options=storage_options).collect()
        rows_per_file = single_df.height

        # Read with glob pattern using scan_avro
        glob_uri = f"s3://{mock_s3_minio.bucket}/scan_glob/*.avro"
        glob_df = jetliner.scan_avro(glob_uri, storage_options=storage_options).collect()

        # Verify we got data from all 3 files
        assert glob_df.height == rows_per_file * 3
        assert isinstance(glob_df, pl.DataFrame)

    def test_scan_avro_s3_glob_with_projection(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that scan_avro() with S3 glob supports projection pushdown."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        mock_s3_minio.upload_file(local_path, "proj_glob/file1.avro")
        mock_s3_minio.upload_file(local_path, "proj_glob/file2.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        glob_uri = f"s3://{mock_s3_minio.bucket}/proj_glob/*.avro"
        df = (
            jetliner.scan_avro(glob_uri, storage_options=storage_options)
            .select(["station", "temp"])
            .collect()
        )

        # Verify projection worked
        assert df.columns == ["station", "temp"]
        assert df.height > 0

    def test_scan_avro_s3_glob_with_include_file_paths(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that scan_avro() with S3 glob supports include_file_paths."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        mock_s3_minio.upload_file(local_path, "paths_glob/file1.avro")
        mock_s3_minio.upload_file(local_path, "paths_glob/file2.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        glob_uri = f"s3://{mock_s3_minio.bucket}/paths_glob/*.avro"
        df = jetliner.scan_avro(
            glob_uri,
            storage_options=storage_options,
            include_file_paths="source_file",
        ).collect()

        # Verify source_file column exists
        assert "source_file" in df.columns

        # Verify we have paths from both files
        unique_paths = df["source_file"].unique().to_list()
        assert len(unique_paths) == 2
        assert all("s3://" in path for path in unique_paths)

    def test_scan_avro_s3_glob_parity_with_read_avro(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that scan_avro and read_avro produce same results for S3 glob."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        mock_s3_minio.upload_file(local_path, "parity_glob/file1.avro")
        mock_s3_minio.upload_file(local_path, "parity_glob/file2.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        glob_uri = f"s3://{mock_s3_minio.bucket}/parity_glob/*.avro"

        scan_df = jetliner.scan_avro(glob_uri, storage_options=storage_options).collect()
        read_df = jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Verify same shape
        assert scan_df.height == read_df.height
        assert scan_df.width == read_df.width

        # Verify same columns
        assert scan_df.columns == read_df.columns

        # Verify same data
        assert scan_df.equals(read_df)

    def test_scan_avro_s3_glob_with_n_rows(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that scan_avro() with S3 glob respects n_rows limit."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        mock_s3_minio.upload_file(local_path, "nrows_glob/file1.avro")
        mock_s3_minio.upload_file(local_path, "nrows_glob/file2.avro")
        mock_s3_minio.upload_file(local_path, "nrows_glob/file3.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        glob_uri = f"s3://{mock_s3_minio.bucket}/nrows_glob/*.avro"
        n_rows_limit = 5

        df = jetliner.scan_avro(
            glob_uri,
            storage_options=storage_options,
            n_rows=n_rows_limit,
        ).collect()

        assert df.height == n_rows_limit


# =============================================================================
# Mixed Local + S3 Source Tests
# =============================================================================


@pytest.mark.container
class TestMixedLocalAndS3Sources:
    """Tests for reading from both local files and S3 in the same call.

    Verifies that jetliner can handle a list of sources containing both
    local file paths and S3 URIs.
    """

    def test_read_avro_mixed_local_and_s3(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test reading from both local file and S3 in the same call."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload one file to S3
        s3_uri = mock_s3_minio.upload_file(local_path, "mixed/remote.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read single file to get expected row count per file
        single_df = jetliner.read_avro(local_path)
        rows_per_file = single_df.height

        # Read from both local and S3 in the same call
        df = jetliner.read_avro(
            [local_path, s3_uri],
            storage_options=storage_options,
        )

        # Verify we got data from both files
        assert df.height == rows_per_file * 2
        assert isinstance(df, pl.DataFrame)

    def test_scan_avro_mixed_local_and_s3(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test scan_avro from both local file and S3 in the same call."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload one file to S3
        s3_uri = mock_s3_minio.upload_file(local_path, "mixed_scan/remote.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read single file to get expected row count per file
        single_df = jetliner.read_avro(local_path)
        rows_per_file = single_df.height

        # Scan from both local and S3 in the same call
        df = jetliner.scan_avro(
            [local_path, s3_uri],
            storage_options=storage_options,
        ).collect()

        # Verify we got data from both files
        assert df.height == rows_per_file * 2
        assert isinstance(df, pl.DataFrame)

    def test_mixed_sources_with_include_file_paths(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that include_file_paths works with mixed local and S3 sources."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload one file to S3
        s3_uri = mock_s3_minio.upload_file(local_path, "mixed_paths/remote.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read with include_file_paths
        df = jetliner.read_avro(
            [local_path, s3_uri],
            storage_options=storage_options,
            include_file_paths="source_file",
        )

        # Verify source_file column exists
        assert "source_file" in df.columns

        # Verify we have paths from both sources
        unique_paths = df["source_file"].unique().to_list()
        assert len(unique_paths) == 2

        # One should be local path, one should be S3 URI
        has_local = any("weather.avro" in p and "s3://" not in p for p in unique_paths)
        has_s3 = any("s3://" in p for p in unique_paths)
        assert has_local, f"Should have local path in {unique_paths}"
        assert has_s3, f"Should have S3 URI in {unique_paths}"

    def test_mixed_sources_with_row_index(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that row_index is continuous across mixed local and S3 sources."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload one file to S3
        s3_uri = mock_s3_minio.upload_file(local_path, "mixed_idx/remote.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read with row index
        df = jetliner.read_avro(
            [local_path, s3_uri],
            storage_options=storage_options,
            row_index_name="idx",
        )

        # Verify row index is continuous (0, 1, 2, ..., n-1)
        indices = df["idx"].to_list()
        expected = list(range(len(indices)))
        assert indices == expected, "Row indices should be continuous across mixed sources"

    def test_mixed_sources_with_n_rows_limit(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that n_rows limit works across mixed local and S3 sources."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload one file to S3
        s3_uri = mock_s3_minio.upload_file(local_path, "mixed_nrows/remote.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        n_rows_limit = 5

        # Read with n_rows limit
        df = jetliner.read_avro(
            [local_path, s3_uri],
            storage_options=storage_options,
            n_rows=n_rows_limit,
        )

        assert df.height == n_rows_limit


# =============================================================================
# Multiple Glob Patterns Tests (S3)
# =============================================================================


@pytest.mark.container
class TestMultipleS3GlobPatterns:
    """Tests for multiple S3 glob patterns in the same call.

    Verifies that jetliner can handle a list of S3 glob patterns where each
    pattern is expanded independently.

    Requirements: 2.4, 2.7
    """

    def test_read_avro_multiple_s3_glob_patterns(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test reading with multiple S3 glob patterns in a list."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload files to two different prefixes
        mock_s3_minio.upload_file(local_path, "prefix1/file1.avro")
        mock_s3_minio.upload_file(local_path, "prefix1/file2.avro")
        mock_s3_minio.upload_file(local_path, "prefix2/data1.avro")
        mock_s3_minio.upload_file(local_path, "prefix2/data2.avro")
        mock_s3_minio.upload_file(local_path, "prefix2/data3.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read single file to get expected row count per file
        single_uri = f"s3://{mock_s3_minio.bucket}/prefix1/file1.avro"
        single_df = jetliner.read_avro(single_uri, storage_options=storage_options)
        rows_per_file = single_df.height

        # Two S3 glob patterns
        patterns = [
            f"s3://{mock_s3_minio.bucket}/prefix1/*.avro",
            f"s3://{mock_s3_minio.bucket}/prefix2/*.avro",
        ]

        df = jetliner.read_avro(patterns, storage_options=storage_options)

        # Should have data from all 5 files
        assert df.height == rows_per_file * 5

    def test_scan_avro_multiple_s3_glob_patterns(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test scanning with multiple S3 glob patterns in a list."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload files to two different prefixes
        mock_s3_minio.upload_file(local_path, "scan_prefix1/file1.avro")
        mock_s3_minio.upload_file(local_path, "scan_prefix1/file2.avro")
        mock_s3_minio.upload_file(local_path, "scan_prefix2/data1.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read single file to get expected row count per file
        single_uri = f"s3://{mock_s3_minio.bucket}/scan_prefix1/file1.avro"
        single_df = jetliner.read_avro(single_uri, storage_options=storage_options)
        rows_per_file = single_df.height

        patterns = [
            f"s3://{mock_s3_minio.bucket}/scan_prefix1/*.avro",
            f"s3://{mock_s3_minio.bucket}/scan_prefix2/*.avro",
        ]

        df = jetliner.scan_avro(patterns, storage_options=storage_options).collect()

        # Should have data from all 3 files
        assert df.height == rows_per_file * 3

    def test_multiple_s3_glob_patterns_with_include_file_paths(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that include_file_paths works with multiple S3 glob patterns."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload files to two different prefixes
        mock_s3_minio.upload_file(local_path, "paths_prefix1/file1.avro")
        mock_s3_minio.upload_file(local_path, "paths_prefix1/file2.avro")
        mock_s3_minio.upload_file(local_path, "paths_prefix2/data1.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        patterns = [
            f"s3://{mock_s3_minio.bucket}/paths_prefix1/*.avro",
            f"s3://{mock_s3_minio.bucket}/paths_prefix2/*.avro",
        ]

        df = jetliner.read_avro(
            patterns,
            storage_options=storage_options,
            include_file_paths="source_file",
        )

        assert "source_file" in df.columns

        # Should have files from both prefixes
        unique_paths = df["source_file"].unique().to_list()
        assert len(unique_paths) == 3

        # Verify we have paths from both prefixes
        prefix1_paths = [p for p in unique_paths if "paths_prefix1" in p]
        prefix2_paths = [p for p in unique_paths if "paths_prefix2" in p]
        assert len(prefix1_paths) == 2
        assert len(prefix2_paths) == 1

    def test_multiple_s3_glob_patterns_with_row_index(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that row_index is continuous across multiple S3 glob patterns."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        mock_s3_minio.upload_file(local_path, "idx_prefix1/file1.avro")
        mock_s3_minio.upload_file(local_path, "idx_prefix1/file2.avro")
        mock_s3_minio.upload_file(local_path, "idx_prefix2/data1.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        patterns = [
            f"s3://{mock_s3_minio.bucket}/idx_prefix1/*.avro",
            f"s3://{mock_s3_minio.bucket}/idx_prefix2/*.avro",
        ]

        df = jetliner.read_avro(
            patterns,
            storage_options=storage_options,
            row_index_name="idx",
        )

        # Verify row index is continuous
        indices = df["idx"].to_list()
        expected = list(range(len(indices)))
        assert indices == expected, "Row indices should be continuous across multiple S3 glob patterns"


@pytest.mark.container
class TestMixedLocalAndS3GlobPatterns:
    """Tests for mixing local glob patterns with S3 glob patterns.

    Verifies that jetliner can handle a list containing both local glob patterns
    and S3 glob patterns in the same call.
    """

    def test_read_avro_mixed_local_and_s3_glob_patterns(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
        tmp_path,
    ):
        """Test reading with both local and S3 glob patterns in the same call."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Create local files in a temp directory
        import shutil
        local_dir = tmp_path / "local_data"
        local_dir.mkdir()
        shutil.copy(local_path, local_dir / "local1.avro")
        shutil.copy(local_path, local_dir / "local2.avro")

        # Upload files to S3
        mock_s3_minio.upload_file(local_path, "s3_data/remote1.avro")
        mock_s3_minio.upload_file(local_path, "s3_data/remote2.avro")
        mock_s3_minio.upload_file(local_path, "s3_data/remote3.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read single file to get expected row count per file
        single_df = jetliner.read_avro(local_path)
        rows_per_file = single_df.height

        # Mix local glob pattern and S3 glob pattern
        patterns = [
            str(local_dir / "*.avro"),  # Local glob
            f"s3://{mock_s3_minio.bucket}/s3_data/*.avro",  # S3 glob
        ]

        df = jetliner.read_avro(patterns, storage_options=storage_options)

        # Should have data from all 5 files (2 local + 3 S3)
        assert df.height == rows_per_file * 5

    def test_scan_avro_mixed_local_and_s3_glob_patterns(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
        tmp_path,
    ):
        """Test scanning with both local and S3 glob patterns in the same call."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Create local files
        import shutil
        local_dir = tmp_path / "scan_local"
        local_dir.mkdir()
        shutil.copy(local_path, local_dir / "local1.avro")

        # Upload files to S3
        mock_s3_minio.upload_file(local_path, "scan_s3/remote1.avro")
        mock_s3_minio.upload_file(local_path, "scan_s3/remote2.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read single file to get expected row count per file
        single_df = jetliner.read_avro(local_path)
        rows_per_file = single_df.height

        patterns = [
            str(local_dir / "*.avro"),
            f"s3://{mock_s3_minio.bucket}/scan_s3/*.avro",
        ]

        df = jetliner.scan_avro(patterns, storage_options=storage_options).collect()

        # Should have data from all 3 files
        assert df.height == rows_per_file * 3

    def test_mixed_glob_patterns_with_include_file_paths(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
        tmp_path,
    ):
        """Test that include_file_paths works with mixed local and S3 glob patterns."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Create local files
        import shutil
        local_dir = tmp_path / "mixed_paths"
        local_dir.mkdir()
        shutil.copy(local_path, local_dir / "local1.avro")
        shutil.copy(local_path, local_dir / "local2.avro")

        # Upload files to S3
        mock_s3_minio.upload_file(local_path, "mixed_paths_s3/remote1.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        patterns = [
            str(local_dir / "*.avro"),
            f"s3://{mock_s3_minio.bucket}/mixed_paths_s3/*.avro",
        ]

        df = jetliner.read_avro(
            patterns,
            storage_options=storage_options,
            include_file_paths="source_file",
        )

        assert "source_file" in df.columns

        unique_paths = df["source_file"].unique().to_list()
        assert len(unique_paths) == 3

        # Verify we have both local and S3 paths
        local_paths = [p for p in unique_paths if "s3://" not in p]
        s3_paths = [p for p in unique_paths if "s3://" in p]
        assert len(local_paths) == 2
        assert len(s3_paths) == 1

    def test_mixed_glob_patterns_with_row_index(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
        tmp_path,
    ):
        """Test that row_index is continuous across mixed local and S3 glob patterns."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Create local files
        import shutil
        local_dir = tmp_path / "mixed_idx"
        local_dir.mkdir()
        shutil.copy(local_path, local_dir / "local1.avro")

        # Upload files to S3
        mock_s3_minio.upload_file(local_path, "mixed_idx_s3/remote1.avro")
        mock_s3_minio.upload_file(local_path, "mixed_idx_s3/remote2.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        patterns = [
            str(local_dir / "*.avro"),
            f"s3://{mock_s3_minio.bucket}/mixed_idx_s3/*.avro",
        ]

        df = jetliner.read_avro(
            patterns,
            storage_options=storage_options,
            row_index_name="idx",
        )

        # Verify row index is continuous
        indices = df["idx"].to_list()
        expected = list(range(len(indices)))
        assert indices == expected, "Row indices should be continuous across mixed glob patterns"

    def test_mixed_glob_patterns_parity(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
        tmp_path,
    ):
        """Test that scan_avro and read_avro produce same results for mixed glob patterns."""
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Create local files
        import shutil
        local_dir = tmp_path / "parity_mixed"
        local_dir.mkdir()
        shutil.copy(local_path, local_dir / "local1.avro")

        # Upload files to S3
        mock_s3_minio.upload_file(local_path, "parity_s3/remote1.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        patterns = [
            str(local_dir / "*.avro"),
            f"s3://{mock_s3_minio.bucket}/parity_s3/*.avro",
        ]

        scan_df = jetliner.scan_avro(patterns, storage_options=storage_options).collect()
        read_df = jetliner.read_avro(patterns, storage_options=storage_options)

        assert scan_df.height == read_df.height
        assert scan_df.columns == read_df.columns
        assert scan_df.equals(read_df)


# =============================================================================
# Recursive Glob Edge Cases Tests
# =============================================================================


@pytest.mark.container
class TestS3RecursiveGlobEdgeCases:
    """Tests for S3 recursive glob (**) edge cases.

    Verifies that the globset-based pattern matching correctly handles
    various recursive glob patterns.

    Requirements: 2.4
    """

    def test_recursive_glob_at_start(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test glob pattern with ** at the start: **/target.avro.

        Should match target.avro at any depth.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload files at different depths with target name
        mock_s3_minio.upload_file(local_path, "target.avro")
        mock_s3_minio.upload_file(local_path, "data/target.avro")
        mock_s3_minio.upload_file(local_path, "data/2024/target.avro")
        mock_s3_minio.upload_file(local_path, "data/2024/01/target.avro")
        # Also upload a non-matching file
        mock_s3_minio.upload_file(local_path, "data/other.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        single_df = jetliner.read_avro(local_path)
        rows_per_file = single_df.height

        # Pattern should match all target.avro files at any depth
        glob_uri = f"s3://{mock_s3_minio.bucket}/**/target.avro"
        df = jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Should match 4 files (not the other.avro)
        assert df.height == rows_per_file * 4

    def test_recursive_glob_at_end(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test glob pattern with ** at the end: data/**.

        Should match everything under data/ prefix.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload files at different depths under data/
        mock_s3_minio.upload_file(local_path, "data/file1.avro")
        mock_s3_minio.upload_file(local_path, "data/sub/file2.avro")
        mock_s3_minio.upload_file(local_path, "data/sub/deep/file3.avro")
        # Upload outside data/
        mock_s3_minio.upload_file(local_path, "other/file4.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        single_df = jetliner.read_avro(local_path)
        rows_per_file = single_df.height

        # Pattern should match all files under data/
        glob_uri = f"s3://{mock_s3_minio.bucket}/data/**"
        df = jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Should match 3 files (not the one in other/)
        assert df.height == rows_per_file * 3

    def test_recursive_glob_multiple_double_stars(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test glob pattern with multiple **: a/**/b/**/*.avro.

        Should match complex nested paths with 'a' and 'b' directories.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload files matching pattern
        mock_s3_minio.upload_file(local_path, "a/b/file.avro")
        mock_s3_minio.upload_file(local_path, "a/x/b/file.avro")
        mock_s3_minio.upload_file(local_path, "a/x/y/b/z/file.avro")
        # Upload files that should NOT match
        mock_s3_minio.upload_file(local_path, "a/file.avro")  # Missing 'b'
        mock_s3_minio.upload_file(local_path, "b/file.avro")  # Missing 'a'

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        single_df = jetliner.read_avro(local_path)
        rows_per_file = single_df.height

        glob_uri = f"s3://{mock_s3_minio.bucket}/a/**/b/**/*.avro"
        df = jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Should match 3 files (those with a/.../b/...)
        assert df.height == rows_per_file * 3

    def test_recursive_glob_zero_directories(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that ** can match zero directories: data/**/*.avro.

        Should match data/file.avro (** matches empty string).
        """
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload file directly in data/ (zero directories between data/ and *.avro)
        mock_s3_minio.upload_file(local_path, "data/direct.avro")
        # Also upload nested files
        mock_s3_minio.upload_file(local_path, "data/2024/nested.avro")
        mock_s3_minio.upload_file(local_path, "data/2024/01/deep.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        single_df = jetliner.read_avro(local_path)
        rows_per_file = single_df.height

        glob_uri = f"s3://{mock_s3_minio.bucket}/data/**/*.avro"
        df = jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Should match all 3 files including the direct one
        assert df.height == rows_per_file * 3

    def test_single_star_does_not_cross_directories(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test that single * does not match across directory separators.

        Pattern data/*.avro should NOT match data/subdir/file.avro.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload files at different depths
        mock_s3_minio.upload_file(local_path, "singlestar/direct.avro")
        mock_s3_minio.upload_file(local_path, "singlestar/subdir/nested.avro")
        mock_s3_minio.upload_file(local_path, "singlestar/a/b/deep.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        single_df = jetliner.read_avro(local_path)
        rows_per_file = single_df.height

        # Single * should only match direct children
        glob_uri = f"s3://{mock_s3_minio.bucket}/singlestar/*.avro"
        df = jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Should only match 1 file (direct.avro)
        assert df.height == rows_per_file * 1

    def test_recursive_glob_with_character_class(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test pattern combining character class with recursive glob.

        Pattern data/[0-9]/**/*.avro should match digit directories only.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Upload files with digit directory names
        mock_s3_minio.upload_file(local_path, "charclass/1/file.avro")
        mock_s3_minio.upload_file(local_path, "charclass/5/sub/file.avro")
        mock_s3_minio.upload_file(local_path, "charclass/9/a/b/file.avro")
        # Upload with non-digit directories (should NOT match)
        mock_s3_minio.upload_file(local_path, "charclass/a/file.avro")
        mock_s3_minio.upload_file(local_path, "charclass/12/file.avro")  # Multi-digit

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        single_df = jetliner.read_avro(local_path)
        rows_per_file = single_df.height

        glob_uri = f"s3://{mock_s3_minio.bucket}/charclass/[0-9]/**/*.avro"
        df = jetliner.read_avro(glob_uri, storage_options=storage_options)

        # Should match 3 files (1/, 5/, 9/ but not a/ or 12/)
        assert df.height == rows_per_file * 3
