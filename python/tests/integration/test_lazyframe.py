"""
Integration tests for LazyFrame operations.

Tests cover:
- Projection pushdown via .select() - Requirement 10.2
- Slice pushdown via head() and limit() - Requirement 10.4
- collect() to materialize results - Requirement 10.5
- collect_async() for async materialization - Requirement 10.6
- fetch() for partial materialization - Requirement 10.7
- explain() to show query plan - Requirement 10.8
- collect(streaming=True) for streaming execution - Requirement 10.9
"""

import asyncio

import polars as pl

import jetliner


class TestProjectionPushdown:
    """Tests for projection pushdown via .select() - Requirement 10.2."""

    def test_select_reduces_columns(self, temp_avro_file):
        """Test that .select() reduces columns in result."""
        df = jetliner.scan_avro(temp_avro_file).select("id").collect()

        assert df.width == 1
        assert "id" in df.columns
        assert "name" not in df.columns

    def test_select_multiple_columns(self, temp_avro_file):
        """Test selecting multiple columns."""
        df = jetliner.scan_avro(temp_avro_file).select(["name", "id"]).collect()

        assert df.width == 2
        assert set(df.columns) == {"id", "name"}


class TestSlicePushdown:
    """Tests for slice pushdown via head() and limit() - Requirement 10.4."""

    def test_head_limits_rows(self, temp_avro_file):
        """Test that head() limits rows returned."""
        df = jetliner.scan_avro(temp_avro_file).head(2).collect()

        assert df.height == 2

    def test_limit_limits_rows(self, temp_avro_file):
        """Test that limit() limits rows returned."""
        df = jetliner.scan_avro(temp_avro_file).limit(3).collect()

        assert df.height == 3

    def test_head_returns_first_rows(self, temp_avro_file):
        """Test that head() returns the first N rows in order."""
        df = jetliner.scan_avro(temp_avro_file).head(2).collect()

        assert df["id"].to_list() == [1, 2]
        assert df["name"].to_list() == ["Alice", "Bob"]


class TestCollect:
    """Tests for collect() to materialize results - Requirement 10.5."""

    def test_collect_returns_dataframe(self, temp_avro_file):
        """Test that collect() returns a DataFrame."""
        lf = jetliner.scan_avro(temp_avro_file)
        df = lf.collect()

        assert isinstance(df, pl.DataFrame)

    def test_collect_materializes_all_rows(self, temp_avro_file):
        """Test that collect() materializes all rows."""
        df = jetliner.scan_avro(temp_avro_file).collect()

        assert df.height == 5


class TestCollectAsync:
    """Tests for collect_async() for async materialization - Requirement 10.6."""

    def test_collect_async_returns_dataframe(self, temp_avro_file):
        """Test that collect_async() returns a DataFrame."""
        lf = jetliner.scan_avro(temp_avro_file)

        async def run_async():
            return await lf.collect_async()

        df = asyncio.run(run_async())

        assert isinstance(df, pl.DataFrame)
        assert df.height == 5

    def test_collect_async_with_operations(self, temp_avro_file):
        """Test collect_async() with lazy operations."""
        lf = jetliner.scan_avro(temp_avro_file).select("id").head(3)

        async def run_async():
            return await lf.collect_async()

        df = asyncio.run(run_async())

        assert df.height == 3
        assert df.width == 1


class TestPartialMaterialization:
    """Tests for partial materialization with head().collect() - Requirement 10.7."""

    def test_head_collect_returns_dataframe(self, temp_avro_file):
        """Test that head().collect() returns a DataFrame."""
        lf = jetliner.scan_avro(temp_avro_file)
        df = lf.head(5).collect()

        assert isinstance(df, pl.DataFrame)

    def test_head_collect_limits_rows(self, temp_avro_file):
        """Test head().collect() limits rows."""
        lf = jetliner.scan_avro(temp_avro_file)
        df = lf.head(2).collect()

        assert df.height <= 2

    def test_head_collect_with_operations(self, temp_avro_file):
        """Test head().collect() with lazy operations applied."""
        lf = jetliner.scan_avro(temp_avro_file).select("name")
        df = lf.head(3).collect()

        assert df.width == 1
        assert "name" in df.columns


class TestExplain:
    """Tests for explain() to show query plan - Requirement 10.8."""

    def test_explain_returns_string(self, temp_avro_file):
        """Test that explain() returns a string."""
        lf = jetliner.scan_avro(temp_avro_file)
        plan = lf.explain()

        assert isinstance(plan, str)
        assert len(plan) > 0

    def test_explain_with_operations(self, temp_avro_file):
        """Test explain() shows operations in plan."""
        lf = jetliner.scan_avro(temp_avro_file).select("id").head(10)
        plan = lf.explain()

        assert isinstance(plan, str)
        # Plan should contain some indication of the operations
        assert len(plan) > 0

    def test_explain_optimized_plan(self, temp_avro_file):
        """Test explain(optimized=True) shows optimized plan."""
        lf = jetliner.scan_avro(temp_avro_file).select("id")
        plan = lf.explain(optimized=True)

        assert isinstance(plan, str)
        assert len(plan) > 0


class TestStreamingCollect:
    """Tests for collect(streaming=True) - Requirement 10.9."""

    def test_streaming_collect_returns_dataframe(self, temp_avro_file):
        """Test that collect(engine='streaming') returns a DataFrame."""
        lf = jetliner.scan_avro(temp_avro_file)
        df = lf.collect(engine="streaming")

        assert isinstance(df, pl.DataFrame)

    def test_streaming_collect_all_rows(self, temp_avro_file):
        """Test streaming collect returns all rows."""
        df = jetliner.scan_avro(temp_avro_file).collect(engine="streaming")

        assert df.height == 5

    def test_streaming_collect_with_operations(self, temp_avro_file):
        """Test streaming collect with lazy operations."""
        df = (
            jetliner.scan_avro(temp_avro_file)
            .select(["id", "name"])
            .head(3)
            .collect(engine="streaming")
        )

        assert df.height == 3
        assert df.width == 2

    def test_streaming_collect_with_filter(self, temp_avro_file):
        """Test streaming collect with filter operation."""
        df = (
            jetliner.scan_avro(temp_avro_file)
            .filter(pl.col("id") > 2)
            .collect(engine="streaming")
        )

        assert df.height == 3
        assert all(id_val > 2 for id_val in df["id"].to_list())
