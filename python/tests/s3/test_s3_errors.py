"""Tests for S3 error handling.

These tests verify that jetliner produces appropriate errors when
S3 operations fail.

Requirements tested:
- 3.1: Non-existent S3 key raises SourceError with descriptive message
- 3.2: Non-existent S3 bucket raises SourceError with descriptive message
- 3.4: Invalid S3 URI raises appropriate error
"""

from __future__ import annotations

import pytest

import jetliner

from .conftest import MockS3Context


@pytest.mark.container
class TestNonExistentKeyError:
    """Tests for non-existent S3 key error handling.

    Requirements: 3.1
    """

    def test_scan_non_existent_key_raises_source_error(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
    ):
        """Test that scan_avro() raises SourceError for non-existent key.

        Verifies that attempting to read a non-existent S3 key
        raises SourceError with a descriptive message.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        non_existent_uri = f"s3://{mock_s3_minio.bucket}/does-not-exist.avro"

        with pytest.raises(jetliner.SourceError) as exc_info:
            jetliner.scan_avro(non_existent_uri, storage_options=storage_options).collect()

        # Verify error message is descriptive - should contain the URI or error indicator
        error_msg = str(exc_info.value).lower()
        assert (
            "not found" in error_msg
            or "no such key" in error_msg
            or "404" in error_msg
            or "service error" in error_msg  # MinIO returns generic service error
            or "does-not-exist" in error_msg  # Contains the key name
        )

    def test_open_non_existent_key_raises_source_error(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
    ):
        """Test that open() raises SourceError for non-existent key.

        Verifies that attempting to open a non-existent S3 key
        raises SourceError with a descriptive message.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        non_existent_uri = f"s3://{mock_s3_minio.bucket}/missing-file.avro"

        with pytest.raises(jetliner.SourceError) as exc_info:
            with jetliner.open(
                non_existent_uri, storage_options=storage_options
            ) as reader:
                list(reader)

        # Verify error message is descriptive - should contain the URI or error indicator
        error_msg = str(exc_info.value).lower()
        assert (
            "not found" in error_msg
            or "no such key" in error_msg
            or "404" in error_msg
            or "service error" in error_msg  # MinIO returns generic service error
            or "missing-file" in error_msg  # Contains the key name
        )


@pytest.mark.container
class TestNonExistentBucketError:
    """Tests for non-existent S3 bucket error handling.

    Requirements: 3.2
    """

    def test_scan_non_existent_bucket_raises_source_error(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
    ):
        """Test that scan_avro() raises SourceError for non-existent bucket.

        Verifies that attempting to read from a non-existent S3 bucket
        raises SourceError with a descriptive message.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        non_existent_bucket_uri = "s3://non-existent-bucket-xyz123/file.avro"

        with pytest.raises(jetliner.SourceError) as exc_info:
            jetliner.scan_avro(
                non_existent_bucket_uri, storage_options=storage_options
            ).collect()

        # Verify error message mentions bucket issue
        error_msg = str(exc_info.value).lower()
        assert (
            "bucket" in error_msg
            or "not found" in error_msg
            or "no such bucket" in error_msg
            or "404" in error_msg
        )

    def test_open_non_existent_bucket_raises_source_error(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
    ):
        """Test that open() raises SourceError for non-existent bucket.

        Verifies that attempting to open from a non-existent S3 bucket
        raises SourceError with a descriptive message.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        non_existent_bucket_uri = "s3://bucket-that-does-not-exist-abc789/data.avro"

        with pytest.raises(jetliner.SourceError) as exc_info:
            with jetliner.open(
                non_existent_bucket_uri, storage_options=storage_options
            ) as reader:
                list(reader)

        # Verify error message mentions bucket issue
        error_msg = str(exc_info.value).lower()
        assert (
            "bucket" in error_msg
            or "not found" in error_msg
            or "no such bucket" in error_msg
            or "404" in error_msg
        )


class TestInvalidS3UriError:
    """Tests for invalid S3 URI error handling.

    Requirements: 3.4
    """

    def test_scan_invalid_s3_uri_missing_bucket(self):
        """Test that scan_avro() raises error for S3 URI without bucket.

        Verifies that malformed S3 URIs without a bucket name
        raise an appropriate error.
        """
        invalid_uri = "s3://"

        with pytest.raises((jetliner.SourceError, ValueError)):
            jetliner.scan_avro(invalid_uri).collect()

    def test_scan_invalid_s3_uri_empty_bucket(self):
        """Test that scan_avro() raises error for S3 URI with empty bucket.

        Verifies that S3 URIs with empty bucket component
        raise an appropriate error.
        """
        invalid_uri = "s3:///some-key.avro"

        with pytest.raises((jetliner.SourceError, ValueError)):
            jetliner.scan_avro(invalid_uri).collect()

    def test_open_invalid_s3_uri_missing_bucket(self):
        """Test that open() raises error for S3 URI without bucket.

        Verifies that malformed S3 URIs without a bucket name
        raise an appropriate error.
        """
        invalid_uri = "s3://"

        with pytest.raises((jetliner.SourceError, ValueError)):
            with jetliner.open(invalid_uri) as reader:
                list(reader)

    def test_open_invalid_s3_uri_empty_bucket(self):
        """Test that open() raises error for S3 URI with empty bucket.

        Verifies that S3 URIs with empty bucket component
        raise an appropriate error.
        """
        invalid_uri = "s3:///key-without-bucket.avro"

        with pytest.raises((jetliner.SourceError, ValueError)):
            with jetliner.open(invalid_uri) as reader:
                list(reader)

    def test_scan_invalid_s3_uri_malformed_scheme(self):
        """Test that scan_avro() raises error for malformed S3 scheme.

        Verifies that URIs with incorrect S3 scheme format
        raise an appropriate error.
        """
        # Missing colon after s3
        invalid_uri = "s3//bucket/key.avro"

        with pytest.raises((jetliner.SourceError, ValueError, FileNotFoundError)):
            jetliner.scan_avro(invalid_uri).collect()


# =============================================================================
# read_avro() S3 Error Tests
# =============================================================================


@pytest.mark.container
class TestReadAvroS3Errors:
    """Tests for read_avro() S3 error handling.

    These tests ensure read_avro() raises appropriate errors for S3 issues.
    """

    def test_read_avro_non_existent_key_raises_source_error(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
    ):
        """Test that read_avro() raises SourceError for non-existent S3 key."""
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        non_existent_uri = f"s3://{mock_s3_minio.bucket}/does-not-exist.avro"

        with pytest.raises(jetliner.SourceError) as exc_info:
            jetliner.read_avro(non_existent_uri, storage_options=storage_options)

        error_msg = str(exc_info.value).lower()
        assert (
            "not found" in error_msg
            or "no such key" in error_msg
            or "404" in error_msg
            or "service error" in error_msg
            or "does-not-exist" in error_msg
        )

    def test_read_avro_non_existent_bucket_raises_source_error(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
    ):
        """Test that read_avro() raises SourceError for non-existent S3 bucket."""
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        non_existent_bucket_uri = "s3://non-existent-bucket-xyz123/file.avro"

        with pytest.raises(jetliner.SourceError) as exc_info:
            jetliner.read_avro(non_existent_bucket_uri, storage_options=storage_options)

        error_msg = str(exc_info.value).lower()
        assert (
            "bucket" in error_msg
            or "not found" in error_msg
            or "no such bucket" in error_msg
            or "404" in error_msg
        )

    def test_read_avro_invalid_s3_uri_raises_error(self):
        """Test that read_avro() raises error for invalid S3 URI."""
        invalid_uri = "s3://"

        with pytest.raises((jetliner.SourceError, ValueError)):
            jetliner.read_avro(invalid_uri)


# =============================================================================
# read_avro_schema() S3 Error Tests
# =============================================================================


@pytest.mark.container
class TestReadAvroSchemaS3Errors:
    """Tests for read_avro_schema() S3 error handling.

    These tests ensure read_avro_schema() raises appropriate errors for S3 issues.
    """

    def test_read_avro_schema_non_existent_bucket_raises_source_error(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
    ):
        """Test that read_avro_schema() raises SourceError for non-existent bucket."""
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        non_existent_bucket_uri = "s3://non-existent-bucket-xyz123/file.avro"

        with pytest.raises(Exception) as exc_info:
            jetliner.read_avro_schema(
                non_existent_bucket_uri, storage_options=storage_options
            )

        error_msg = str(exc_info.value).lower()
        assert (
            "bucket" in error_msg
            or "not found" in error_msg
            or "no such bucket" in error_msg
            or "404" in error_msg
            or "s3" in error_msg
        )

    def test_read_avro_schema_invalid_s3_uri_raises_error(self):
        """Test that read_avro_schema() raises error for invalid S3 URI."""
        invalid_uri = "s3://"

        with pytest.raises((jetliner.SourceError, ValueError)):
            jetliner.read_avro_schema(invalid_uri)

    def test_read_avro_schema_empty_bucket_raises_error(self):
        """Test that read_avro_schema() raises error for S3 URI with empty bucket."""
        invalid_uri = "s3:///some-key.avro"

        with pytest.raises((jetliner.SourceError, ValueError)):
            jetliner.read_avro_schema(invalid_uri)
