"""
Tests for recursive types.

Tests handling of Avro recursive types (self-referential schemas):
- Binary trees: Supported (serialized to JSON)
- Linked lists: Supported (serialized to JSON)
- N-ary trees with array children: Not yet supported (list builder constraints)

Since Arrow/Polars don't natively support recursive structures, recursive fields
are serialized to JSON strings.

Test data source: tests/data/fastavro/

Requirements tested:
- 1.5: Recursive type resolution
- 1.7: Named type references
"""


import polars as pl

import jetliner


class TestComplexRecursiveStructures:
    """
    Test reading Avro files with complex recursive structures.

    These tests verify that Jetliner can handle various recursive
    patterns including binary trees, n-ary trees, and deeply nested
    structures.
    """

    def test_binary_tree_structure(self, get_test_data_path):
        """
        Test reading file with binary tree recursive structure.

        Schema defines a TreeNode with left/right child references.
        Recursive fields are serialized to JSON strings.
        """
        path = get_test_data_path("fastavro/tree-recursive.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)

            # Should have multiple records (different tree structures)
            assert df.height > 0, "Should have at least one tree"

            # Check columns exist
            assert "value" in df.columns, "Should have 'value' column"
            assert "left" in df.columns, "Should have 'left' column"
            assert "right" in df.columns, "Should have 'right' column"

            # Recursive fields should be strings (JSON serialized)
            assert df["left"].dtype == pl.Utf8, "left should be JSON string"
            assert df["right"].dtype == pl.Utf8, "right should be JSON string"

            # Verify we can parse the JSON
            import json

            for i in range(df.height):
                left_val = df["left"][i]
                if left_val and left_val != "null":
                    parsed = json.loads(left_val)
                    assert "value" in parsed
                    assert "left" in parsed
                    assert "right" in parsed

    def test_nary_tree_structure(self, get_test_data_path):
        """
        Test reading file with n-ary tree recursive structure.

        Schema defines a GraphNode with array of children.
        Tests more complex recursive patterns with variable branching.
        """
        path = get_test_data_path("fastavro/graph-recursive.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)

            # Should have records
            assert df.height > 0, "Should have at least one graph node"

            # Check columns exist
            assert "id" in df.columns, "Should have 'id' column"
            assert "value" in df.columns, "Should have 'value' column"
            assert "children" in df.columns, "Should have 'children' column"

            # Children field should be a list of recursive nodes (JSON serialized)
            # Could be List(String) if each child is JSON, or String if whole array is JSON
            assert df["children"].dtype in [
                pl.Utf8,
                pl.List(pl.Utf8),
            ], f"Unexpected children type: {df['children'].dtype}"

    def test_tree_with_scan_api(self, get_test_data_path):
        """Test scan_avro() API with recursive tree structure."""
        path = get_test_data_path("fastavro/tree-recursive.avro")

        df = jetliner.scan_avro(path).collect()

        assert df.height > 0
        assert "value" in df.columns
        assert "left" in df.columns
        assert "right" in df.columns

    def test_tree_projection_non_recursive_field(self, get_test_data_path):
        """Test projecting only non-recursive fields (skips recursive fields)."""
        path = get_test_data_path("fastavro/tree-recursive.avro")

        # Project only the non-recursive 'value' field
        # This requires skipping the recursive 'left' and 'right' fields
        df = jetliner.scan_avro(path).select(["value"]).collect()

        assert df.width == 1
        assert "value" in df.columns
        assert "left" not in df.columns
        assert "right" not in df.columns
        assert df.height > 0

    def test_tree_projection_recursive_field_only(self, get_test_data_path):
        """Test projecting only recursive fields (skips non-recursive fields).

        This is the inverse of the typical case - we want the recursive
        fields and skip the simple 'value' field.
        """
        path = get_test_data_path("fastavro/tree-recursive.avro")

        df = jetliner.scan_avro(path).select(["left", "right"]).collect()

        assert df.width == 2
        assert "left" in df.columns
        assert "right" in df.columns
        assert "value" not in df.columns
        assert df["left"].dtype == pl.Utf8
        assert df["right"].dtype == pl.Utf8

    def test_tree_projection_single_recursive_field(self, get_test_data_path):
        """Test projecting a single recursive field.

        This tests skipping both a non-recursive field AND another recursive field.
        """
        path = get_test_data_path("fastavro/tree-recursive.avro")

        df = jetliner.scan_avro(path).select(["left"]).collect()

        assert df.width == 1
        assert "left" in df.columns
        assert "right" not in df.columns
        assert "value" not in df.columns

    def test_tree_projection_with_open_api(self, get_test_data_path):
        """Test projection on recursive structure using open() API."""
        path = get_test_data_path("fastavro/tree-recursive.avro")

        with jetliner.open(path, projected_columns=["value"]) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)

            assert df.width == 1
            assert "value" in df.columns
            assert "left" not in df.columns

    def test_tree_projection_recursive_with_open_api(self, get_test_data_path):
        """Test projecting recursive fields using open() API."""
        path = get_test_data_path("fastavro/tree-recursive.avro")

        with jetliner.open(path, projected_columns=["left"]) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)

            assert df.width == 1
            assert "left" in df.columns
            assert df["left"].dtype == pl.Utf8

    def test_nary_tree_projection(self, get_test_data_path):
        """Test projection on n-ary tree with array of recursive children.

        The graph-recursive schema has: id, value, children (array of self-refs).
        This tests skipping the recursive array field.
        """
        path = get_test_data_path("fastavro/graph-recursive.avro")

        # Project only non-recursive fields, skip the recursive 'children' array
        df = jetliner.scan_avro(path).select(["id", "value"]).collect()

        assert df.width == 2
        assert "id" in df.columns
        assert "value" in df.columns
        assert "children" not in df.columns

    def test_nary_tree_projection_recursive_only(self, get_test_data_path):
        """Test projecting only the recursive array field from n-ary tree."""
        path = get_test_data_path("fastavro/graph-recursive.avro")

        df = jetliner.scan_avro(path).select(["children"]).collect()

        assert df.width == 1
        assert "children" in df.columns
        assert df["children"].dtype == pl.List(pl.Utf8)

    def test_deeply_nested_recursive(self, get_test_data_path):
        """Test that deeply nested recursive structures are handled correctly.

        N-ary trees with array children are now supported - children field
        is a List[Utf8] where each element is a JSON-serialized child node.
        """
        path = get_test_data_path("fastavro/graph-recursive.avro")

        df = jetliner.scan_avro(path).collect()

        # Should successfully read deeply nested structure
        assert df.height > 0

        # Verify columns exist
        assert "id" in df.columns
        assert "value" in df.columns
        assert "children" in df.columns

        # Children should be List[Utf8] - each child is JSON serialized
        assert df["children"].dtype == pl.List(pl.Utf8)

        # Verify we can parse the JSON children
        import json

        for row in df.iter_rows(named=True):
            children_list = row["children"]
            if children_list:
                for child_json in children_list:
                    if child_json:
                        parsed = json.loads(child_json)
                        assert "id" in parsed
                        assert "value" in parsed
                        assert "children" in parsed
