"""Verification tests for S3 mock fixtures.

These tests verify that the moto and MinIO fixtures start/stop correctly
and can perform basic S3 operations.
"""

import pytest


class TestMotoFixture:
    """Tests for the moto in-process S3 mock fixture."""

    def test_moto_fixture_creates_bucket(self, mock_s3_moto):
        """Verify moto fixture creates the test bucket."""
        # List buckets to verify our bucket exists
        response = mock_s3_moto.client.list_buckets()
        bucket_names = [b["Name"] for b in response["Buckets"]]
        assert mock_s3_moto.bucket in bucket_names

    def test_moto_upload_bytes(self, mock_s3_moto):
        """Verify upload_bytes works with moto."""
        test_data = b"Hello, S3!"
        uri = mock_s3_moto.upload_bytes(test_data, "test.txt")

        assert uri == f"s3://{mock_s3_moto.bucket}/test.txt"

        # Verify we can read it back
        response = mock_s3_moto.client.get_object(
            Bucket=mock_s3_moto.bucket, Key="test.txt"
        )
        assert response["Body"].read() == test_data

    def test_moto_upload_file(self, mock_s3_moto, tmp_path):
        """Verify upload_file works with moto."""
        # Create a temp file to upload
        test_file = tmp_path / "test.txt"
        test_file.write_text("Test content")

        uri = mock_s3_moto.upload_file(str(test_file), "uploaded.txt")

        assert uri == f"s3://{mock_s3_moto.bucket}/uploaded.txt"

        # Verify we can read it back
        response = mock_s3_moto.client.get_object(
            Bucket=mock_s3_moto.bucket, Key="uploaded.txt"
        )
        assert response["Body"].read() == b"Test content"

    def test_moto_endpoint_url_is_none(self, mock_s3_moto):
        """Verify moto fixture has no endpoint URL (in-process mock)."""
        assert mock_s3_moto.endpoint_url is None


@pytest.mark.container
class TestMinioFixture:
    """Tests for the MinIO container-based S3 mock fixture.

    These tests require Docker to be available.
    """

    def test_minio_fixture_creates_bucket(self, mock_s3_minio):
        """Verify MinIO fixture creates a unique test bucket."""
        response = mock_s3_minio.client.list_buckets()
        bucket_names = [b["Name"] for b in response["Buckets"]]
        assert mock_s3_minio.bucket in bucket_names
        # Bucket name should be unique (contains UUID)
        assert mock_s3_minio.bucket.startswith("jetliner-test-")

    def test_minio_upload_bytes(self, mock_s3_minio):
        """Verify upload_bytes works with MinIO."""
        test_data = b"Hello, MinIO!"
        uri = mock_s3_minio.upload_bytes(test_data, "test.txt")

        assert uri == f"s3://{mock_s3_minio.bucket}/test.txt"

        # Verify we can read it back
        response = mock_s3_minio.client.get_object(
            Bucket=mock_s3_minio.bucket, Key="test.txt"
        )
        assert response["Body"].read() == test_data

    def test_minio_upload_file(self, mock_s3_minio, tmp_path):
        """Verify upload_file works with MinIO."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("MinIO test content")

        uri = mock_s3_minio.upload_file(str(test_file), "uploaded.txt")

        assert uri == f"s3://{mock_s3_minio.bucket}/uploaded.txt"

        # Verify we can read it back
        response = mock_s3_minio.client.get_object(
            Bucket=mock_s3_minio.bucket, Key="uploaded.txt"
        )
        assert response["Body"].read() == b"MinIO test content"

    def test_minio_has_endpoint_url(self, mock_s3_minio):
        """Verify MinIO fixture has an endpoint URL."""
        assert mock_s3_minio.endpoint_url is not None
        assert mock_s3_minio.endpoint_url.startswith("http://")


class TestEnvironmentFixtures:
    """Tests for environment configuration fixtures."""

    def test_s3_env_for_moto(self, mock_s3_moto, s3_env_for_moto):
        """Verify moto environment fixture sets credentials."""
        import os

        assert os.environ.get("AWS_ACCESS_KEY_ID") == "testing"
        assert os.environ.get("AWS_SECRET_ACCESS_KEY") == "testing"
        assert os.environ.get("AWS_DEFAULT_REGION") == "us-east-1"

    @pytest.mark.container
    def test_s3_env_for_minio(self, mock_s3_minio, s3_env_for_minio):
        """Verify MinIO environment fixture sets credentials and endpoint."""
        import os

        assert os.environ.get("AWS_ACCESS_KEY_ID") == "minioadmin"
        assert os.environ.get("AWS_SECRET_ACCESS_KEY") == "minioadmin"
        assert os.environ.get("AWS_DEFAULT_REGION") == "us-east-1"
        assert os.environ.get("AWS_ENDPOINT_URL") == mock_s3_minio.endpoint_url
