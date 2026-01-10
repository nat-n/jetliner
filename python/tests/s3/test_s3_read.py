"""Tests for S3 read operations using MinIO mock.

These tests verify that jetliner can read Avro files from S3 using
the scan() and open() APIs.

Note: moto (in-process mock) cannot be used for testing the Rust S3
implementation because moto intercepts boto3 calls but the Rust AWS SDK
makes actual HTTP calls. MinIO provides a real HTTP endpoint that works
with the Rust SDK.

Requirements tested:
- 2.1: scan() reads Avro files from S3 via s3:// URI
- 2.2: open() reads Avro files from S3 via s3:// URI
- 2.5: Local and S3 reads produce identical DataFrames
"""

from __future__ import annotations

import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import jetliner

from .conftest import MockS3Context


@pytest.mark.container
class TestMinioScanOperations:
    """Tests for scan() operations using MinIO mock.

    Requirements: 2.1
    """

    def test_scan_reads_avro_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test that scan() can read an Avro file from mock S3.

        Uploads weather.avro to MinIO mock S3 and verifies that
        jetliner.scan() can read it via s3:// URI.
        """
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read via scan()
        lf = jetliner.scan(s3_weather_file_minio, storage_options=storage_options)
        df = lf.collect()

        # Verify we got data
        assert df.height > 0
        assert isinstance(df, pl.DataFrame)

        # Verify schema has expected columns from weather.avro
        assert "station" in df.columns
        assert "temp" in df.columns

    def test_scan_returns_lazyframe(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test that scan() returns a LazyFrame for S3 URIs."""
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        lf = jetliner.scan(s3_weather_file_minio, storage_options=storage_options)

        assert isinstance(lf, pl.LazyFrame)

    def test_scan_with_projection(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test that scan() with column selection works for S3."""
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        lf = jetliner.scan(s3_weather_file_minio, storage_options=storage_options)
        df = lf.select(["station", "temp"]).collect()

        assert df.columns == ["station", "temp"]
        assert df.height > 0


@pytest.mark.container
class TestMinioOpenOperations:
    """Tests for open() operations using MinIO mock.

    Requirements: 2.2
    """

    def test_open_reads_avro_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test that open() can read an Avro file from mock S3.

        Uploads weather.avro to MinIO mock S3 and verifies that
        jetliner.open() can iterate batches via s3:// URI.
        """
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        with jetliner.open(
            s3_weather_file_minio, storage_options=storage_options
        ) as reader:
            batches = list(reader)

        # Verify we got batches
        assert len(batches) > 0

        # Verify each batch is a DataFrame
        for batch in batches:
            assert isinstance(batch, pl.DataFrame)

        # Verify total row count
        total_rows = sum(batch.height for batch in batches)
        assert total_rows > 0

    def test_open_provides_schema(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test that open() provides schema access for S3 files."""
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        with jetliner.open(
            s3_weather_file_minio, storage_options=storage_options
        ) as reader:
            schema = reader.schema

        assert schema is not None
        assert len(schema) > 0
        assert "station" in schema
        assert "temp" in schema

    def test_open_context_manager(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test that open() works as a context manager for S3 files."""
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Should work without errors
        with jetliner.open(
            s3_weather_file_minio, storage_options=storage_options
        ) as reader:
            # Iterate through batches
            for batch in reader:
                assert isinstance(batch, pl.DataFrame)


@pytest.mark.container
class TestLocalS3Equivalence:
    """Tests verifying local and S3 reads produce identical results.

    Requirements: 2.5
    """

    def test_scan_local_s3_equivalence(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
        get_test_data_path,
    ):
        """Test that scan() produces identical results for local and S3.

        Reads the same Avro file from local filesystem and mock S3,
        then verifies the DataFrames are identical.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read from local
        local_df = jetliner.scan(local_path).collect()

        # Read from S3
        s3_df = jetliner.scan(
            s3_weather_file_minio, storage_options=storage_options
        ).collect()

        # Verify identical results
        assert local_df.shape == s3_df.shape
        assert local_df.columns == s3_df.columns
        assert local_df.equals(s3_df)

    def test_open_local_s3_equivalence(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
        get_test_data_path,
    ):
        """Test that open() produces identical results for local and S3.

        Reads the same Avro file from local filesystem and mock S3,
        then verifies the combined DataFrames are identical.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read from local
        with jetliner.open(local_path) as reader:
            local_batches = list(reader)
        local_df = pl.concat(local_batches) if local_batches else pl.DataFrame()

        # Read from S3
        with jetliner.open(
            s3_weather_file_minio, storage_options=storage_options
        ) as reader:
            s3_batches = list(reader)
        s3_df = pl.concat(s3_batches) if s3_batches else pl.DataFrame()

        # Verify identical results
        assert local_df.shape == s3_df.shape
        assert local_df.columns == s3_df.columns
        assert local_df.equals(s3_df)


# =============================================================================
# Property-Based Tests
# =============================================================================


@pytest.mark.container
class TestLocalS3EquivalenceProperty:
    """Property-based tests for local/S3 read equivalence.

    Feature: s3-mock-testing, Property 1: Local/S3 Read Equivalence
    Validates: Requirements 2.1, 2.2, 2.5
    """

    @settings(
        max_examples=100,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        records=st.lists(
            st.tuples(
                st.integers(min_value=-1000000, max_value=1000000),
                st.text(
                    alphabet=st.characters(
                        whitelist_categories=("L", "N"),
                        whitelist_characters=" ",
                    ),
                    min_size=1,
                    max_size=50,
                ),
            ),
            min_size=1,
            max_size=20,
        )
    )
    def test_local_s3_read_equivalence_property(
        self,
        records: list[tuple[int, str]],
        mock_s3_minio: MockS3Context,
        minio_container,
        tmp_path,
    ):
        """Property 1: Local/S3 Read Equivalence.

        For any valid Avro file, reading it from local filesystem and
        reading the same file from mock S3 SHALL produce identical
        DataFrames (same schema, same data, same row count).

        Feature: s3-mock-testing, Property 1: Local/S3 Read Equivalence
        Validates: Requirements 2.1, 2.2, 2.5
        """
        # Import the helper function from conftest
        import sys
        from pathlib import Path

        # Add the tests directory to path to import conftest helpers
        tests_dir = Path(__file__).parent.parent
        if str(tests_dir) not in sys.path:
            sys.path.insert(0, str(tests_dir))

        from conftest import create_test_avro_file

        # Generate Avro file from random records
        avro_bytes = create_test_avro_file(records)

        # Write to local file
        local_path = tmp_path / "test.avro"
        local_path.write_bytes(avro_bytes)

        # Upload to mock S3
        s3_uri = mock_s3_minio.upload_bytes(avro_bytes, f"test-{id(records)}.avro")

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read from local using scan()
        local_df = jetliner.scan(str(local_path)).collect()

        # Read from S3 using scan()
        s3_df = jetliner.scan(s3_uri, storage_options=storage_options).collect()

        # Property: DataFrames must be identical
        assert local_df.shape == s3_df.shape, (
            f"Shape mismatch: local={local_df.shape}, s3={s3_df.shape}"
        )
        assert local_df.columns == s3_df.columns, (
            f"Column mismatch: local={local_df.columns}, s3={s3_df.columns}"
        )
        assert local_df.equals(s3_df), "DataFrames are not equal"

        # Also verify open() produces identical results
        with jetliner.open(str(local_path)) as reader:
            local_batches = list(reader)
        local_open_df = pl.concat(local_batches) if local_batches else pl.DataFrame()

        with jetliner.open(s3_uri, storage_options=storage_options) as reader:
            s3_batches = list(reader)
        s3_open_df = pl.concat(s3_batches) if s3_batches else pl.DataFrame()

        assert local_open_df.equals(s3_open_df), "open() DataFrames are not equal"
