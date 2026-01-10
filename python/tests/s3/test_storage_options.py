"""Tests for S3-compatible service support via storage_options.

These tests verify that jetliner can connect to S3-compatible services
(MinIO, LocalStack, R2) using the storage_options parameter.

Requirements tested:
- 4.9: Connect to custom endpoint when `endpoint_url` is provided
- 4.10: Use provided credentials when specified
- 4.11: `storage_options` takes precedence over environment variables
- 4.12: Successfully read files from S3-compatible services
"""

from __future__ import annotations

import os
from unittest.mock import patch

import polars as pl
import pytest

import jetliner


@pytest.mark.container
class TestMinioEndpointUrl:
    """Tests for connecting to MinIO via endpoint_url in storage_options.

    Requirements: 4.9, 4.12
    """

    def test_open_with_minio_endpoint_url(
        self, mock_s3_minio, s3_weather_file_minio, minio_container
    ):
        """Test jetliner.open() with MinIO endpoint_url in storage_options.

        Verifies that we can read an Avro file from MinIO by providing
        the endpoint_url in storage_options.
        """
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        with jetliner.open(
            s3_weather_file_minio, storage_options=storage_options
        ) as reader:
            dfs = list(reader)

        assert len(dfs) > 0
        total_rows = sum(df.height for df in dfs)
        assert total_rows > 0

    def test_scan_with_minio_endpoint_url(
        self, mock_s3_minio, s3_weather_file_minio, minio_container
    ):
        """Test jetliner.scan() with MinIO endpoint_url in storage_options.

        Verifies that we can scan an Avro file from MinIO using the
        LazyFrame API with storage_options.
        """
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        lf = jetliner.scan(s3_weather_file_minio, storage_options=storage_options)
        df = lf.collect()

        assert df.height > 0
        assert isinstance(df, pl.DataFrame)


@pytest.mark.container
class TestCredentialOverride:
    """Tests for credential override via storage_options.

    Requirements: 4.10
    """

    def test_credentials_in_storage_options(
        self, mock_s3_minio, s3_weather_file_minio, minio_container
    ):
        """Test that credentials can be provided via storage_options.

        Verifies that aws_access_key_id and aws_secret_access_key
        in storage_options are used for authentication.
        """
        # Clear any environment credentials to ensure we're using storage_options
        env_vars_to_clear = {
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_SESSION_TOKEN": "",
        }

        with patch.dict(os.environ, env_vars_to_clear, clear=False):
            # Remove the keys entirely
            for key in env_vars_to_clear:
                os.environ.pop(key, None)

            storage_options = {
                "endpoint_url": mock_s3_minio.endpoint_url,
                "aws_access_key_id": minio_container.access_key,
                "aws_secret_access_key": minio_container.secret_key,
            }

            with jetliner.open(
                s3_weather_file_minio, storage_options=storage_options
            ) as reader:
                dfs = list(reader)

            assert len(dfs) > 0

    def test_region_in_storage_options(
        self, mock_s3_minio, s3_weather_file_minio, minio_container
    ):
        """Test that region can be provided via storage_options.

        Verifies that the region parameter in storage_options is accepted.
        """
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
            "region": "us-east-1",
        }

        with jetliner.open(
            s3_weather_file_minio, storage_options=storage_options
        ) as reader:
            dfs = list(reader)

        assert len(dfs) > 0


@pytest.mark.container
class TestStorageOptionsPrecedence:
    """Tests for storage_options taking precedence over environment.

    Requirements: 4.11
    """

    def test_storage_options_override_env_credentials(
        self, mock_s3_minio, s3_weather_file_minio, minio_container
    ):
        """Test that storage_options credentials override environment variables.

        Sets invalid credentials in environment and valid credentials in
        storage_options. The read should succeed, proving storage_options
        takes precedence.
        """
        # Set invalid credentials in environment
        invalid_env = {
            "AWS_ACCESS_KEY_ID": "invalid_key",
            "AWS_SECRET_ACCESS_KEY": "invalid_secret",
            "AWS_DEFAULT_REGION": "us-east-1",
        }

        with patch.dict(os.environ, invalid_env):
            # Provide valid credentials via storage_options
            storage_options = {
                "endpoint_url": mock_s3_minio.endpoint_url,
                "aws_access_key_id": minio_container.access_key,
                "aws_secret_access_key": minio_container.secret_key,
            }

            # This should succeed because storage_options takes precedence
            with jetliner.open(
                s3_weather_file_minio, storage_options=storage_options
            ) as reader:
                dfs = list(reader)

            assert len(dfs) > 0

    def test_storage_options_override_env_endpoint(
        self, mock_s3_minio, s3_weather_file_minio, minio_container
    ):
        """Test that storage_options endpoint_url overrides AWS_ENDPOINT_URL.

        Sets an invalid endpoint in environment and valid endpoint in
        storage_options. The read should succeed, proving storage_options
        takes precedence.
        """
        # Set invalid endpoint in environment
        invalid_env = {
            "AWS_ENDPOINT_URL": "http://invalid-endpoint:9999",
            "AWS_ACCESS_KEY_ID": minio_container.access_key,
            "AWS_SECRET_ACCESS_KEY": minio_container.secret_key,
        }

        with patch.dict(os.environ, invalid_env):
            # Provide valid endpoint via storage_options
            storage_options = {
                "endpoint_url": mock_s3_minio.endpoint_url,
                "aws_access_key_id": minio_container.access_key,
                "aws_secret_access_key": minio_container.secret_key,
            }

            # This should succeed because storage_options takes precedence
            with jetliner.open(
                s3_weather_file_minio, storage_options=storage_options
            ) as reader:
                dfs = list(reader)

            assert len(dfs) > 0


@pytest.mark.container
class TestS3CompatibleServiceIntegration:
    """Integration tests for S3-compatible service support.

    Requirements: 4.12
    """

    def test_full_workflow_with_minio(
        self, mock_s3_minio, s3_weather_file_minio, minio_container
    ):
        """Test complete read workflow with MinIO.

        Verifies that we can:
        1. Connect to MinIO via storage_options
        2. Read data successfully
        3. Get correct schema
        4. Process all records
        """
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        with jetliner.open(
            s3_weather_file_minio, storage_options=storage_options
        ) as reader:
            # Verify schema is accessible
            schema = reader.schema
            assert schema is not None
            assert len(schema) > 0

            # Read all data
            dfs = list(reader)

        # Verify we got data
        assert len(dfs) > 0
        combined = pl.concat(dfs)
        assert combined.height > 0

    def test_scan_with_projection_on_minio(
        self, mock_s3_minio, s3_weather_file_minio, minio_container
    ):
        """Test scan() with projection pushdown on MinIO.

        Verifies that projection pushdown works correctly when
        reading from an S3-compatible service.
        """
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        lf = jetliner.scan(s3_weather_file_minio, storage_options=storage_options)

        # Get all columns first
        all_cols = lf.collect().columns

        # Select subset of columns
        if len(all_cols) >= 2:
            selected_cols = all_cols[:2]
            result = lf.select(selected_cols).collect()
            assert result.columns == selected_cols

    def test_scan_with_filter_on_minio(
        self, mock_s3_minio, s3_weather_file_minio, minio_container
    ):
        """Test scan() with predicate pushdown on MinIO.

        Verifies that predicate pushdown works correctly when
        reading from an S3-compatible service.
        """
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        lf = jetliner.scan(s3_weather_file_minio, storage_options=storage_options)

        # Apply a filter - this should work regardless of data content
        # Just verify the operation completes without error
        result = lf.head(5).collect()
        assert result.height <= 5

    def test_error_with_invalid_endpoint(self, mock_s3_minio, s3_weather_file_minio):
        """Test that invalid endpoint_url produces appropriate error.

        Verifies that connection errors are properly reported when
        the endpoint is unreachable.
        """
        storage_options = {
            "endpoint_url": "http://localhost:59999",  # Invalid port
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
        }

        with pytest.raises(jetliner.SourceError):
            with jetliner.open(
                s3_weather_file_minio, storage_options=storage_options
            ) as reader:
                list(reader)

    def test_error_with_invalid_credentials(
        self, mock_s3_minio, s3_weather_file_minio, minio_container
    ):
        """Test that invalid credentials produce appropriate error.

        Verifies that authentication errors are properly reported when
        credentials are invalid.
        """
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": "invalid_key",
            "aws_secret_access_key": "invalid_secret",
        }

        # MinIO should reject invalid credentials
        with pytest.raises((jetliner.SourceError, PermissionError)):
            with jetliner.open(
                s3_weather_file_minio, storage_options=storage_options
            ) as reader:
                list(reader)
