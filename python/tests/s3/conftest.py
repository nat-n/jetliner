"""S3 mock testing fixtures for Jetliner.

Provides two S3-compatible mock backends:
- moto: Fast in-process S3 mock for unit-style tests
- MinIO: Container-based S3 mock via testcontainers for realistic testing
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


# =============================================================================
# MockS3Context Dataclass
# =============================================================================


@dataclass
class MockS3Context:
    """Context for S3 mock operations.

    Provides helper methods to upload test files to the mock S3 bucket
    and returns s3:// URIs for use in tests.
    """

    client: S3Client
    bucket: str
    endpoint_url: str | None

    def upload_file(self, local_path: str, key: str) -> str:
        """Upload a local file to mock S3 and return s3:// URI.

        Args:
            local_path: Path to local file to upload
            key: S3 object key (path within bucket)

        Returns:
            s3:// URI for the uploaded file
        """
        self.client.upload_file(local_path, self.bucket, key)
        return f"s3://{self.bucket}/{key}"

    def upload_bytes(self, data: bytes, key: str) -> str:
        """Upload bytes to mock S3 and return s3:// URI.

        Args:
            data: Raw bytes to upload
            key: S3 object key (path within bucket)

        Returns:
            s3:// URI for the uploaded object
        """
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return f"s3://{self.bucket}/{key}"


# =============================================================================
# moto Fixture (In-Process S3 Mock)
# =============================================================================

TEST_BUCKET_NAME = "jetliner-test-bucket"


@pytest.fixture
def mock_s3_moto() -> MockS3Context:
    """Fast in-process S3 mock using moto.

    Creates a mock S3 environment with a test bucket. The mock intercepts
    boto3 calls directly, so no endpoint URL is needed.

    Yields:
        MockS3Context with client, bucket name, and None endpoint_url
    """
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )

        # Create test bucket
        client.create_bucket(Bucket=TEST_BUCKET_NAME)

        yield MockS3Context(
            client=client,
            bucket=TEST_BUCKET_NAME,
            endpoint_url=None,
        )


# =============================================================================
# MinIO Fixture (Container-Based S3 Mock)
# =============================================================================


def _delete_bucket_contents(client: S3Client, bucket: str) -> None:
    """Delete all objects in a bucket."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})


@pytest.fixture(scope="session")
def minio_container():
    """Session-scoped MinIO container - starts once, reused across tests.

    The container is started at the beginning of the test session and
    stopped at the end. This avoids container startup overhead for each test.

    Yields:
        MinioContainer instance with access to connection details

    Skips:
        If Docker is not available or MinIO container fails to start
    """
    try:
        from testcontainers.minio import MinioContainer
    except ImportError:
        pytest.skip("testcontainers[minio] not installed")

    try:
        container = MinioContainer("minio/minio:latest")
        container.start()
    except Exception as e:
        pytest.skip(f"Docker/MinIO not available: {e}")

    yield container

    container.stop()


@pytest.fixture
def mock_s3_minio(minio_container) -> MockS3Context:
    """MinIO-backed S3 mock with per-test bucket isolation.

    Creates a unique bucket for each test to ensure isolation.
    The bucket and its contents are cleaned up after the test.

    Args:
        minio_container: Session-scoped MinIO container fixture

    Yields:
        MockS3Context with client, unique bucket name, and endpoint URL
    """
    endpoint_url = (
        f"http://{minio_container.get_container_host_ip()}:"
        f"{minio_container.get_exposed_port(9000)}"
    )

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=minio_container.access_key,
        aws_secret_access_key=minio_container.secret_key,
    )

    # Create unique bucket for test isolation
    bucket_name = f"jetliner-test-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket_name)

    yield MockS3Context(
        client=client,
        bucket=bucket_name,
        endpoint_url=endpoint_url,
    )

    # Cleanup bucket after test
    _delete_bucket_contents(client, bucket_name)
    client.delete_bucket(Bucket=bucket_name)


# =============================================================================
# Environment Configuration Fixture
# =============================================================================


@pytest.fixture
def s3_env_for_moto():
    """Configure environment for Rust AWS SDK to use moto mock.

    Sets AWS credentials and region for the Rust SDK. Since moto intercepts
    boto3 calls in-process, no endpoint URL is needed for the Python side,
    but the Rust SDK needs credentials to be set.

    Note: moto doesn't provide a real HTTP endpoint, so this fixture is
    primarily for ensuring credentials are available. For actual S3 calls
    from Rust code, use the MinIO fixture instead.
    """
    env_vars = {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }

    with patch.dict(os.environ, env_vars):
        yield


@pytest.fixture
def s3_env_for_minio(mock_s3_minio: MockS3Context):
    """Configure environment for Rust AWS SDK to use MinIO endpoint.

    Sets AWS credentials, region, and endpoint URL for the Rust SDK to
    connect to the MinIO container.

    Args:
        mock_s3_minio: MinIO mock context with endpoint URL
    """
    env_vars = {
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ENDPOINT_URL": mock_s3_minio.endpoint_url,
    }

    with patch.dict(os.environ, env_vars):
        yield


@pytest.fixture
def s3_env_for_mock(request, mock_s3_moto: MockS3Context | None = None):
    """Generic environment fixture that works with either mock backend.

    This fixture detects which mock backend is being used and configures
    the environment accordingly.

    Usage:
        def test_something(mock_s3_moto, s3_env_for_mock):
            # Environment is configured for moto

        def test_something_else(mock_s3_minio, s3_env_for_mock):
            # Environment is configured for MinIO
    """
    # Check if we have a MinIO context in the request
    if "mock_s3_minio" in request.fixturenames:
        mock_ctx = request.getfixturevalue("mock_s3_minio")
        env_vars = {
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_ENDPOINT_URL": mock_ctx.endpoint_url,
        }
    else:
        # Default to moto credentials
        env_vars = {
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_DEFAULT_REGION": "us-east-1",
        }

    with patch.dict(os.environ, env_vars):
        yield


# =============================================================================
# Test Data Upload Fixtures
# =============================================================================


@pytest.fixture
def s3_weather_file(mock_s3_moto: MockS3Context, get_test_data_path) -> str:
    """Upload weather.avro to moto mock S3.

    Args:
        mock_s3_moto: moto mock context
        get_test_data_path: Fixture to get test data paths

    Returns:
        s3:// URI for the uploaded weather.avro file
    """
    local_path = get_test_data_path("apache-avro/weather.avro")
    return mock_s3_moto.upload_file(local_path, "weather.avro")


@pytest.fixture
def s3_weather_file_minio(mock_s3_minio: MockS3Context, get_test_data_path) -> str:
    """Upload weather.avro to MinIO mock S3.

    Args:
        mock_s3_minio: MinIO mock context
        get_test_data_path: Fixture to get test data paths

    Returns:
        s3:// URI for the uploaded weather.avro file
    """
    local_path = get_test_data_path("apache-avro/weather.avro")
    return mock_s3_minio.upload_file(local_path, "weather.avro")


# =============================================================================
# Pytest Markers
# =============================================================================


def pytest_configure(config):
    """Register custom markers for S3 tests."""
    config.addinivalue_line(
        "markers",
        "container: marks tests that require Docker containers (deselect with '-m \"not container\"')",
    )
