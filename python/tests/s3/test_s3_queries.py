"""Tests for S3 query operations (projection, predicate, early stopping).

These tests verify that query operations work correctly when reading
Avro files from S3 using the scan() API.

Requirements tested:
- 4.1: Projection pushdown works correctly with S3 URI
- 4.2: Predicate pushdown works correctly with S3 URI
- 4.3: Early stopping (head/limit) works correctly with S3 URI
- 4.4: Batch iteration works correctly with S3 URI
"""

from __future__ import annotations

import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import jetliner

from .conftest import MockS3Context


# =============================================================================
# Projection Pushdown Tests (Requirement 4.1)
# =============================================================================


@pytest.mark.container
class TestS3ProjectionPushdown:
    """Tests for projection pushdown with S3.

    Requirements: 4.1
    """

    def test_select_single_column_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test selecting a single column from S3 file.

        Verifies that projection pushdown works correctly when
        selecting a single column from an S3-backed Avro file.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .select("station")
            .collect()
        )

        assert df.width == 1
        assert "station" in df.columns
        assert "temp" not in df.columns
        assert df.height > 0

    def test_select_multiple_columns_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test selecting multiple columns from S3 file.

        Verifies that projection pushdown works correctly when
        selecting multiple columns from an S3-backed Avro file.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .select(["station", "temp"])
            .collect()
        )

        assert df.width == 2
        assert "station" in df.columns
        assert "temp" in df.columns
        assert df.height > 0

    def test_projection_s3_local_equivalence(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
        get_test_data_path,
    ):
        """Test that projection produces same results for local and S3.

        Verifies that selecting columns from S3 produces identical
        results to selecting from local filesystem.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read with projection from local
        local_df = jetliner.scan_avro(local_path).select(["station", "temp"]).collect()

        # Read with projection from S3
        s3_df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .select(["station", "temp"])
            .collect()
        )

        assert local_df.shape == s3_df.shape
        assert local_df.columns == s3_df.columns
        assert local_df.equals(s3_df)


# =============================================================================
# Predicate Pushdown Tests (Requirement 4.2)
# =============================================================================


@pytest.mark.container
class TestS3PredicatePushdown:
    """Tests for predicate pushdown with S3.

    Requirements: 4.2
    """

    def test_filter_equality_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test filtering with equality predicate from S3.

        Verifies that predicate pushdown works correctly when
        filtering with equality from an S3-backed Avro file.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Get all data first to find a valid station value
        all_df = jetliner.scan_avro(
            s3_weather_file_minio, storage_options=storage_options
        ).collect()
        first_station = all_df["station"][0]

        # Filter by that station
        df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .filter(pl.col("station") == first_station)
            .collect()
        )

        assert df.height >= 1
        assert all(s == first_station for s in df["station"].to_list())

    def test_filter_comparison_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test filtering with comparison predicate from S3.

        Verifies that predicate pushdown works correctly when
        filtering with comparison operators from an S3-backed Avro file.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Get all data to determine a threshold
        all_df = jetliner.scan_avro(
            s3_weather_file_minio, storage_options=storage_options
        ).collect()
        median_temp = all_df["temp"].median()

        # Filter by temperature greater than median
        df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .filter(pl.col("temp") > median_temp)
            .collect()
        )

        # All returned temps should be greater than median
        assert all(t > median_temp for t in df["temp"].to_list())

    def test_filter_combined_with_select_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test combining filter with select from S3.

        Verifies that combining predicate and projection pushdown
        works correctly with S3-backed Avro files.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Get all data to determine a threshold
        all_df = jetliner.scan_avro(
            s3_weather_file_minio, storage_options=storage_options
        ).collect()
        median_temp = all_df["temp"].median()

        # Filter and select
        df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .filter(pl.col("temp") > median_temp)
            .select("station")
            .collect()
        )

        assert df.width == 1
        assert "station" in df.columns

    def test_predicate_s3_local_equivalence(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
        get_test_data_path,
    ):
        """Test that predicate produces same results for local and S3.

        Verifies that filtering from S3 produces identical
        results to filtering from local filesystem.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Get threshold from local file
        all_df = jetliner.scan_avro(local_path).collect()
        median_temp = all_df["temp"].median()

        # Read with filter from local
        local_df = jetliner.scan_avro(local_path).filter(pl.col("temp") > median_temp).collect()

        # Read with filter from S3
        s3_df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .filter(pl.col("temp") > median_temp)
            .collect()
        )

        assert local_df.shape == s3_df.shape
        assert local_df.columns == s3_df.columns
        assert local_df.equals(s3_df)



# =============================================================================
# Early Stopping Tests (Requirement 4.3)
# =============================================================================


@pytest.mark.container
class TestS3EarlyStopping:
    """Tests for early stopping (head/limit) with S3.

    Requirements: 4.3
    """

    def test_head_limits_rows_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test that head() limits rows from S3 file.

        Verifies that early stopping works correctly when
        using head() with an S3-backed Avro file.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .head(3)
            .collect()
        )

        assert df.height == 3

    def test_limit_alias_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test that limit() works as alias for head() with S3.

        Verifies that limit() produces the same result as head()
        when reading from S3.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .limit(3)
            .collect()
        )

        assert df.height == 3

    def test_head_with_filter_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test head() combined with filter from S3.

        Verifies that combining early stopping with predicate pushdown
        works correctly with S3-backed Avro files.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Get all data to determine a threshold that returns multiple rows
        all_df = jetliner.scan_avro(
            s3_weather_file_minio, storage_options=storage_options
        ).collect()
        min_temp = all_df["temp"].min()

        # Filter and limit
        df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .filter(pl.col("temp") > min_temp)
            .head(2)
            .collect()
        )

        assert df.height <= 2

    def test_early_stopping_s3_local_equivalence(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
        get_test_data_path,
    ):
        """Test that early stopping produces same results for local and S3.

        Verifies that head() from S3 produces identical
        results to head() from local filesystem.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read with head from local
        local_df = jetliner.scan_avro(local_path).head(3).collect()

        # Read with head from S3
        s3_df = (
            jetliner.scan_avro(s3_weather_file_minio, storage_options=storage_options)
            .head(3)
            .collect()
        )

        assert local_df.shape == s3_df.shape
        assert local_df.columns == s3_df.columns
        assert local_df.equals(s3_df)


# =============================================================================
# Batch Iteration Tests (Requirement 4.4)
# =============================================================================


@pytest.mark.container
class TestS3BatchIteration:
    """Tests for batch iteration with S3.

    Requirements: 4.4
    """

    def test_open_batch_iteration_from_s3(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
    ):
        """Test that open() batch iteration works from S3.

        Verifies that iterating batches via open() works correctly
        with S3-backed Avro files.
        """
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        with jetliner.open(
            s3_weather_file_minio, storage_options=storage_options
        ) as reader:
            batches = list(reader)

        assert len(batches) > 0
        for batch in batches:
            assert isinstance(batch, pl.DataFrame)

    def test_batch_iteration_s3_local_equivalence(
        self,
        mock_s3_minio: MockS3Context,
        s3_weather_file_minio: str,
        minio_container,
        get_test_data_path,
    ):
        """Test that batch iteration produces same results for local and S3.

        Verifies that iterating batches from S3 produces identical
        combined DataFrame to iterating from local filesystem.
        """
        local_path = get_test_data_path("apache-avro/weather.avro")
        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Read batches from local
        with jetliner.open(local_path) as reader:
            local_batches = list(reader)
        local_df = pl.concat(local_batches) if local_batches else pl.DataFrame()

        # Read batches from S3
        with jetliner.open(
            s3_weather_file_minio, storage_options=storage_options
        ) as reader:
            s3_batches = list(reader)
        s3_df = pl.concat(s3_batches) if s3_batches else pl.DataFrame()

        assert local_df.shape == s3_df.shape
        assert local_df.columns == s3_df.columns
        assert local_df.equals(s3_df)


# =============================================================================
# Property-Based Tests (Property 4: Query Operation Equivalence)
# =============================================================================


@pytest.mark.container
class TestQueryOperationEquivalenceProperty:
    """Property-based tests for query operation equivalence.

    Feature: s3-mock-testing, Property 4: Query Operation Equivalence
    Validates: Requirements 4.1, 4.2, 4.3, 4.4
    """

    @settings(
        max_examples=100,
        deadline=None,  # S3 operations can be slow
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
        ),
        column_subset=st.sampled_from([["id"], ["name"], ["id", "name"]]),
        id_threshold=st.integers(min_value=-500000, max_value=500000),
        row_limit=st.integers(min_value=1, max_value=10),
    )
    def test_query_operation_equivalence_property(
        self,
        records: list[tuple[int, str]],
        column_subset: list[str],
        id_threshold: int,
        row_limit: int,
        mock_s3_minio: MockS3Context,
        minio_container,
        tmp_path,
    ):
        """Property 4: Query Operation Equivalence.

        For any valid Avro file, for any column subset (projection),
        for any filter predicate, and for any row limit, executing
        the query against S3 SHALL produce the same result as
        executing against local filesystem.

        Feature: s3-mock-testing, Property 4: Query Operation Equivalence
        Validates: Requirements 4.1, 4.2, 4.3, 4.4
        """
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
        s3_uri = mock_s3_minio.upload_bytes(avro_bytes, f"query-test-{id(records)}.avro")

        storage_options = {
            "endpoint": mock_s3_minio.endpoint_url,
            "aws_access_key_id": minio_container.access_key,
            "aws_secret_access_key": minio_container.secret_key,
        }

        # Test 1: Projection pushdown equivalence
        local_proj_df = jetliner.scan_avro(str(local_path)).select(column_subset).collect()
        s3_proj_df = (
            jetliner.scan_avro(s3_uri, storage_options=storage_options)
            .select(column_subset)
            .collect()
        )
        assert local_proj_df.equals(s3_proj_df), (
            f"Projection mismatch for columns {column_subset}"
        )

        # Test 2: Predicate pushdown equivalence
        local_pred_df = (
            jetliner.scan_avro(str(local_path))
            .filter(pl.col("id") > id_threshold)
            .collect()
        )
        s3_pred_df = (
            jetliner.scan_avro(s3_uri, storage_options=storage_options)
            .filter(pl.col("id") > id_threshold)
            .collect()
        )
        assert local_pred_df.equals(s3_pred_df), (
            f"Predicate mismatch for threshold {id_threshold}"
        )

        # Test 3: Early stopping equivalence
        local_head_df = jetliner.scan_avro(str(local_path)).head(row_limit).collect()
        s3_head_df = (
            jetliner.scan_avro(s3_uri, storage_options=storage_options)
            .head(row_limit)
            .collect()
        )
        assert local_head_df.equals(s3_head_df), (
            f"Early stopping mismatch for limit {row_limit}"
        )

        # Test 4: Combined operations equivalence
        local_combined_df = (
            jetliner.scan_avro(str(local_path))
            .filter(pl.col("id") > id_threshold)
            .select(column_subset)
            .head(row_limit)
            .collect()
        )
        s3_combined_df = (
            jetliner.scan_avro(s3_uri, storage_options=storage_options)
            .filter(pl.col("id") > id_threshold)
            .select(column_subset)
            .head(row_limit)
            .collect()
        )
        assert local_combined_df.equals(s3_combined_df), (
            "Combined operations mismatch"
        )

        # Test 5: Batch iteration equivalence (Requirement 4.4)
        with jetliner.open(str(local_path)) as reader:
            local_batches = list(reader)
        local_open_df = pl.concat(local_batches) if local_batches else pl.DataFrame()

        with jetliner.open(s3_uri, storage_options=storage_options) as reader:
            s3_batches = list(reader)
        s3_open_df = pl.concat(s3_batches) if s3_batches else pl.DataFrame()

        assert local_open_df.equals(s3_open_df), "Batch iteration mismatch"
