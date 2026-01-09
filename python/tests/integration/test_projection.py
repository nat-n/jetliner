"""
Integration tests for projection pushdown (column selection).

Tests cover:
- Projection pushdown - Requirement 6a.2
- Column selection with select()
- Subset of columns reading
- Column ordering
"""



import jetliner


class TestProjectionPushdown:
    """Tests for projection pushdown optimization."""

    def test_select_single_column(self, temp_avro_file):
        """Test selecting a single column."""
        df = jetliner.scan(temp_avro_file).select("id").collect()

        assert df.width == 1
        assert "id" in df.columns
        assert "name" not in df.columns

    def test_select_multiple_columns(self, temp_avro_file):
        """Test selecting multiple columns."""
        df = jetliner.scan(temp_avro_file).select(["id", "name"]).collect()

        assert df.width == 2
        assert "id" in df.columns
        assert "name" in df.columns

    def test_projection_preserves_data(self, temp_avro_file):
        """Test that projection preserves correct data values."""
        df = jetliner.scan(temp_avro_file).select("name").collect()

        names = df["name"].to_list()
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]
