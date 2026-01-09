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
import pytest

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

    @pytest.mark.xfail(
        reason="N-ary trees with array children not yet fully supported - list builder struct error"
    )
    def test_nary_tree_structure(self, get_test_data_path):
        """
        Test reading file with n-ary tree recursive structure.

        Schema defines a GraphNode with array of children.
        Tests more complex recursive patterns with variable branching.

        Known Limitation: Currently fails with Polars list builder error when handling
        arrays of recursive types. This is a more complex case than simple binary trees.
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
        """Test scan() API with recursive tree structure."""
        path = get_test_data_path("fastavro/tree-recursive.avro")

        df = jetliner.scan(path).collect()

        assert df.height > 0
        assert "value" in df.columns
        assert "left" in df.columns
        assert "right" in df.columns

    def test_tree_projection(self, get_test_data_path):
        """Test projection on recursive structure."""
        path = get_test_data_path("fastavro/tree-recursive.avro")

        # Should be able to project specific fields
        df = jetliner.scan(path).select(["value"]).collect()

        assert df.width == 1
        assert "value" in df.columns
        assert "left" not in df.columns

    @pytest.mark.xfail(
        reason="Deeply nested n-ary trees not yet fully supported - same issue as test_nary_tree_structure"
    )
    def test_deeply_nested_recursive(self, get_test_data_path):
        """Test that deeply nested recursive structures are handled correctly.

        Known Limitation: Same as test_nary_tree_structure - arrays of recursive types
        not yet fully supported.
        """
        path = get_test_data_path("fastavro/graph-recursive.avro")

        df = jetliner.scan(path).collect()

        # Should successfully read deeply nested structure
        assert df.height > 0

        # Verify the deep nesting is present in the JSON

        for i in range(df.height):
            children_val = df["children"][i]
            if children_val and children_val != "[]":
                # Should be able to parse as JSON
                # Exact format depends on implementation
                pass  # Just verify no errors
