"""Tests for S3 edge cases.

These tests verify that jetliner handles various edge cases correctly
when reading Avro files from S3.

Edge cases covered:
1. Large/multi-block files - range requests across block boundaries
3. Special characters in S3 keys - spaces, unicode, nested paths
5. Empty/edge-case files - empty, single-record, null values
7. Range request edge cases - boundary conditions
8. S3 URI edge cases - special key patterns

S3 Key Naming Rules (from AWS docs):
- Keys are UTF-8 encoded, max 1024 bytes
- Safe characters: alphanumeric, !, -, _, ., *, ', (, )
- Characters requiring special handling: &, $, @, =, ;, :, +, space, comma
- Characters to avoid: backslash, {, }, ^, %, backtick, ], [, ", <, >, ~, #, |
- Period-only segments (., ..) can cause issues with some tools
"""

from __future__ import annotations

import io
import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings, assume
from hypothesis import strategies as st

import jetliner

from .conftest import MockS3Context


# =============================================================================
# S3 Key Character Strategies for Property Tests
# =============================================================================

# Safe characters that work reliably across all S3 tools and APIs
SAFE_KEY_CHARS = (
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "!-_.*'()"
)

# Characters that require URL encoding but are valid
SPECIAL_KEY_CHARS = "&$@=;:+ ,"

# Strategy for safe S3 key segments (no slashes)
safe_key_segment = st.text(
    alphabet=SAFE_KEY_CHARS,
    min_size=1,
    max_size=50,
).filter(lambda s: s not in (".", ".."))  # Avoid period-only segments

# Strategy for key segments that may include special chars
special_key_segment = st.text(
    alphabet=SAFE_KEY_CHARS + SPECIAL_KEY_CHARS,
    min_size=1,
    max_size=50,
).filter(lambda s: s.strip() and s not in (".", ".."))


# =============================================================================
# 1. Large/Multi-Block File Tests
# =============================================================================


@pytest.mark.container
class TestLargeFileHandling:
    """Tests for large/multi-block file handling from S3.

    Validates that range requests work correctly across block boundaries.
    """

    def test_multi_block_file_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test reading a multi-block Avro file from S3.

        Uses weather-large.avro which has multiple blocks to verify
        range requests work correctly across block boundaries.
        """
        local_path = get_test_data_path("large/weather-large.avro")
        s3_uri = mock_s3_minio.upload_file(str(local_path), "weather-large.avro")

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read from S3
        s3_df = jetliner.scan(s3_uri, storage_options=storage_options).collect()

        # Read from local for comparison
        local_df = jetliner.scan(str(local_path)).collect()

        # Verify identical results
        assert s3_df.shape == local_df.shape
        assert s3_df.equals(local_df)

    def test_multi_block_batch_iteration_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test batch iteration of multi-block file from S3.

        Verifies that iterating batches works correctly when blocks
        span multiple range requests.
        """
        local_path = get_test_data_path("large/weather-large.avro")
        s3_uri = mock_s3_minio.upload_file(str(local_path), "weather-large-iter.avro")

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read batches from S3
        with jetliner.open(s3_uri, storage_options=storage_options) as reader:
            s3_batches = list(reader)

        # Read batches from local
        with jetliner.open(str(local_path)) as reader:
            local_batches = list(reader)

        # Verify same number of batches and total rows
        s3_total = sum(b.height for b in s3_batches)
        local_total = sum(b.height for b in local_batches)
        assert s3_total == local_total

    def test_different_codecs_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test reading files with different compression codecs from S3.

        Verifies that deflate, snappy, and zstd compressed files
        are handled correctly when read from S3.
        """
        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        codecs = ["deflate", "snappy", "zstd"]
        results = {}

        for codec in codecs:
            local_path = get_test_data_path(f"apache-avro/weather-{codec}.avro")
            s3_uri = mock_s3_minio.upload_file(
                str(local_path), f"weather-{codec}.avro"
            )

            s3_df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
            local_df = jetliner.scan(str(local_path)).collect()

            # Each codec should produce identical results to local
            assert s3_df.equals(local_df), f"Mismatch for codec: {codec}"
            results[codec] = s3_df

        # All codecs should produce the same data
        base_df = results["deflate"]
        for codec, df in results.items():
            assert df.equals(base_df), f"Codec {codec} differs from deflate"


# =============================================================================
# 3. Special Characters in S3 Keys
# =============================================================================


@pytest.mark.container
class TestSpecialCharactersInKeys:
    """Tests for S3 keys with special characters.

    S3 allows any UTF-8 character in keys, but some require URL encoding.
    """

    def test_key_with_spaces(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test reading file with spaces in S3 key."""
        local_path = get_test_data_path("apache-avro/weather.avro")
        s3_uri = mock_s3_minio.upload_file(
            str(local_path), "folder with spaces/weather file.avro"
        )

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert df.equals(local_df)

    def test_key_with_special_safe_chars(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test reading file with safe special characters in S3 key.

        Safe chars: ! - _ . * ' ( )
        """
        local_path = get_test_data_path("apache-avro/weather.avro")
        s3_uri = mock_s3_minio.upload_file(
            str(local_path), "safe-chars_test!file.name's(copy).avro"
        )

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert df.equals(local_df)

    def test_deeply_nested_path(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test reading file with deeply nested path in S3 key."""
        local_path = get_test_data_path("apache-avro/weather.avro")
        nested_key = "level1/level2/level3/level4/level5/weather.avro"
        s3_uri = mock_s3_minio.upload_file(str(local_path), nested_key)

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert df.equals(local_df)

    def test_key_with_unicode_chars(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test reading file with unicode characters in S3 key."""
        local_path = get_test_data_path("apache-avro/weather.avro")
        # Use common unicode chars that are safe
        s3_uri = mock_s3_minio.upload_file(
            str(local_path), "données/météo.avro"
        )

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert df.equals(local_df)

    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        key_segments=st.lists(safe_key_segment, min_size=1, max_size=5),
    )
    def test_random_safe_key_names_property(
        self,
        key_segments: list[str],
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Property: Any valid S3 key with safe characters should work.

        For any key composed of safe characters and path separators,
        reading from S3 should produce identical results to local.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Build key from segments
        key = "/".join(key_segments) + ".avro"

        # Ensure key is not too long (S3 limit is 1024 bytes)
        assume(len(key.encode("utf-8")) <= 900)

        s3_uri = mock_s3_minio.upload_file(str(local_path), key)

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert df.equals(local_df), f"Mismatch for key: {key}"


# =============================================================================
# 5. Empty/Edge-Case Files
# =============================================================================


@pytest.mark.container
class TestEdgeCaseFiles:
    """Tests for edge-case Avro files from S3."""

    def test_single_record_file_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        tmp_path,
    ):
        """Test reading a single-record Avro file from S3."""
        import sys
        from pathlib import Path

        tests_dir = Path(__file__).parent.parent
        if str(tests_dir) not in sys.path:
            sys.path.insert(0, str(tests_dir))

        from conftest import create_test_avro_file

        # Create single-record file
        records = [(42, "single")]
        avro_bytes = create_test_avro_file(records)

        local_path = tmp_path / "single.avro"
        local_path.write_bytes(avro_bytes)

        s3_uri = mock_s3_minio.upload_bytes(avro_bytes, "single-record.avro")

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        s3_df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert s3_df.height == 1
        assert s3_df.equals(local_df)

    def test_file_with_null_values_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        tmp_path,
    ):
        """Test reading Avro file with null values from S3."""
        import fastavro

        # Create schema with nullable field
        schema = {
            "type": "record",
            "name": "NullableRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": ["null", "string"]},
            ],
        }

        records = [
            {"id": 1, "value": "hello"},
            {"id": 2, "value": None},
            {"id": 3, "value": "world"},
            {"id": 4, "value": None},
        ]

        # Write to bytes
        buffer = io.BytesIO()
        fastavro.writer(buffer, schema, records)
        avro_bytes = buffer.getvalue()

        local_path = tmp_path / "nullable.avro"
        local_path.write_bytes(avro_bytes)

        s3_uri = mock_s3_minio.upload_bytes(avro_bytes, "nullable.avro")

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        s3_df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert s3_df.height == 4
        assert s3_df.equals(local_df)

        # Verify nulls are preserved
        null_count = s3_df["value"].null_count()
        assert null_count == 2

    def test_no_fields_record_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test reading Avro file with no-fields record from S3."""
        local_path = get_test_data_path("fastavro/no-fields.avro")
        s3_uri = mock_s3_minio.upload_file(str(local_path), "no-fields.avro")

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        s3_df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert s3_df.equals(local_df)


# =============================================================================
# 7. Range Request Edge Cases
# =============================================================================


@pytest.mark.container
class TestRangeRequestEdgeCases:
    """Tests for range request boundary conditions."""

    def test_small_batch_size_forces_many_range_requests(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test with very small batch size to force many range requests.

        Using batch_size=1 forces the reader to make many small reads,
        testing range request handling at boundaries.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")
        s3_uri = mock_s3_minio.upload_file(str(local_path), "weather-small-batch.avro")

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read with very small batch size
        with jetliner.open(
            s3_uri, storage_options=storage_options, batch_size=1
        ) as reader:
            s3_batches = list(reader)

        with jetliner.open(str(local_path), batch_size=1) as reader:
            local_batches = list(reader)

        # Should have same number of batches
        assert len(s3_batches) == len(local_batches)

        # Combined data should be identical
        s3_df = pl.concat(s3_batches)
        local_df = pl.concat(local_batches)
        assert s3_df.equals(local_df)

    def test_head_at_various_boundaries(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test head() at various row counts to test early stopping.

        Tests that early stopping works correctly at different points
        in the file, which may hit different range request boundaries.
        """
        local_path = get_test_data_path("large/weather-large.avro")
        s3_uri = mock_s3_minio.upload_file(str(local_path), "weather-head-test.avro")

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Test various head sizes
        for n in [1, 5, 10, 50, 100, 500]:
            s3_df = (
                jetliner.scan(s3_uri, storage_options=storage_options)
                .head(n)
                .collect()
            )
            local_df = jetliner.scan(str(local_path)).head(n).collect()

            assert s3_df.equals(local_df), f"Mismatch at head({n})"


# =============================================================================
# 8. S3 URI Edge Cases
# =============================================================================


@pytest.mark.container
class TestS3UriEdgeCases:
    """Tests for S3 URI edge cases."""

    def test_key_that_looks_like_path(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test key that resembles a filesystem path."""
        local_path = get_test_data_path("apache-avro/weather.avro")
        # Key that looks like an absolute path
        s3_uri = mock_s3_minio.upload_file(
            str(local_path), "data/2024/01/15/weather.avro"
        )

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert df.equals(local_df)

    def test_key_with_dots_in_path(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test key with dots in path segments (not period-only)."""
        local_path = get_test_data_path("apache-avro/weather.avro")
        # Dots in path but not period-only segments
        s3_uri = mock_s3_minio.upload_file(
            str(local_path), "v1.0.0/data.backup/weather.avro"
        )

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert df.equals(local_df)

    def test_very_long_key(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test with a long (but valid) S3 key.

        S3 keys can be up to 1024 bytes. Test with a moderately long key.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")

        # Create a long but valid key (under 1024 bytes)
        long_prefix = "a" * 100 + "/" + "b" * 100 + "/" + "c" * 100
        long_key = f"{long_prefix}/weather.avro"

        s3_uri = mock_s3_minio.upload_file(str(local_path), long_key)

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert df.equals(local_df)

    def test_key_with_plus_sign(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test key with plus sign (requires URL encoding in some contexts)."""
        local_path = get_test_data_path("apache-avro/weather.avro")
        s3_uri = mock_s3_minio.upload_file(
            str(local_path), "data+backup/weather+2024.avro"
        )

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert df.equals(local_df)

    def test_key_with_equals_sign(
        self,
        mock_s3_minio: MockS3Context,
        minio_container,
        get_test_data_path,
    ):
        """Test key with equals sign (common in partitioned data)."""
        local_path = get_test_data_path("apache-avro/weather.avro")
        # Hive-style partitioning pattern
        s3_uri = mock_s3_minio.upload_file(
            str(local_path), "year=2024/month=01/weather.avro"
        )

        storage_options = {
            "endpoint_url": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = jetliner.scan(s3_uri, storage_options=storage_options).collect()
        local_df = jetliner.scan(str(local_path)).collect()

        assert df.equals(local_df)
