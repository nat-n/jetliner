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
        df = jetliner.scan_avro(temp_avro_file).select("id").collect()

        assert df.width == 1
        assert "id" in df.columns
        assert "name" not in df.columns

    def test_select_multiple_columns(self, temp_avro_file):
        """Test selecting multiple columns."""
        df = jetliner.scan_avro(temp_avro_file).select(["id", "name"]).collect()

        assert df.width == 2
        assert "id" in df.columns
        assert "name" in df.columns

    def test_projection_preserves_data(self, temp_avro_file):
        """Test that projection preserves correct data values."""
        df = jetliner.scan_avro(temp_avro_file).select("name").collect()

        names = df["name"].to_list()
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]


class TestNestedFieldProjection:
    """Tests for projection on nested structures.

    Coverage gap: Tests for selecting nested struct fields were missing.
    """

    def test_select_from_nested_struct(self, tmp_path):
        """Test selecting columns from a file with nested structs."""
        import fastavro

        schema = {
            "type": "record",
            "name": "PersonRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {
                    "name": "address",
                    "type": {
                        "type": "record",
                        "name": "Address",
                        "fields": [
                            {"name": "street", "type": "string"},
                            {"name": "city", "type": "string"},
                            {"name": "zip", "type": "string"},
                        ],
                    },
                },
            ],
        }

        records = [
            {"id": 1, "name": "Alice", "address": {"street": "123 Main St", "city": "Boston", "zip": "02101"}},
            {"id": 2, "name": "Bob", "address": {"street": "456 Oak Ave", "city": "Seattle", "zip": "98101"}},
        ]

        avro_path = tmp_path / "nested.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Select top-level columns only
        df = jetliner.scan_avro(str(avro_path)).select(["id", "name"]).collect()

        assert df.width == 2
        assert "id" in df.columns
        assert "name" in df.columns
        assert "address" not in df.columns
        assert df["id"].to_list() == [1, 2]

    def test_select_nested_struct_column(self, tmp_path):
        """Test selecting a nested struct column."""
        import fastavro

        schema = {
            "type": "record",
            "name": "PersonRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {
                    "name": "address",
                    "type": {
                        "type": "record",
                        "name": "Address",
                        "fields": [
                            {"name": "street", "type": "string"},
                            {"name": "city", "type": "string"},
                        ],
                    },
                },
            ],
        }

        records = [
            {"id": 1, "name": "Alice", "address": {"street": "123 Main St", "city": "Boston"}},
            {"id": 2, "name": "Bob", "address": {"street": "456 Oak Ave", "city": "Seattle"}},
        ]

        avro_path = tmp_path / "nested.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Select the nested struct column
        df = jetliner.scan_avro(str(avro_path)).select(["id", "address"]).collect()

        assert df.width == 2
        assert "id" in df.columns
        assert "address" in df.columns

        # Verify nested struct data
        addresses = df["address"].to_list()
        assert addresses[0]["street"] == "123 Main St"
        assert addresses[0]["city"] == "Boston"
        assert addresses[1]["street"] == "456 Oak Ave"
        assert addresses[1]["city"] == "Seattle"

    def test_select_from_array_column(self, tmp_path):
        """Test selecting array columns."""
        import fastavro

        schema = {
            "type": "record",
            "name": "ArrayRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "scores", "type": {"type": "array", "items": "int"}},
            ],
        }

        records = [
            {"id": 1, "tags": ["a", "b", "c"], "scores": [10, 20, 30]},
            {"id": 2, "tags": ["x", "y"], "scores": [100]},
        ]

        avro_path = tmp_path / "arrays.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Select only tags array
        df = jetliner.scan_avro(str(avro_path)).select(["id", "tags"]).collect()

        assert df.width == 2
        assert "tags" in df.columns
        assert "scores" not in df.columns
        assert df["tags"].to_list() == [["a", "b", "c"], ["x", "y"]]

    def test_select_from_map_column(self, tmp_path):
        """Test selecting map columns."""
        import fastavro

        schema = {
            "type": "record",
            "name": "MapRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "metadata", "type": {"type": "map", "values": "string"}},
                {"name": "counts", "type": {"type": "map", "values": "int"}},
            ],
        }

        records = [
            {"id": 1, "metadata": {"key1": "value1"}, "counts": {"a": 1, "b": 2}},
            {"id": 2, "metadata": {"key2": "value2"}, "counts": {"c": 3}},
        ]

        avro_path = tmp_path / "maps.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Select only metadata map
        df = jetliner.scan_avro(str(avro_path)).select(["id", "metadata"]).collect()

        assert df.width == 2
        assert "metadata" in df.columns
        assert "counts" not in df.columns

    def test_column_reordering(self, tmp_path):
        """Test that column selection can reorder columns."""
        import fastavro

        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "int"},
                {"name": "c", "type": "int"},
            ],
        }

        records = [{"a": 1, "b": 2, "c": 3}]

        avro_path = tmp_path / "reorder.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Select columns in different order
        df = jetliner.scan_avro(str(avro_path)).select(["c", "a", "b"]).collect()

        assert df.columns == ["c", "a", "b"], f"Expected ['c', 'a', 'b'], got {df.columns}"
        assert df.row(0) == (3, 1, 2)
