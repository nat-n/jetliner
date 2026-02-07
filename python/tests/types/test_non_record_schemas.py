"""
Tests for non-record top-level schemas.

Tests handling of Avro files where the top-level schema is not a record:
- Primitive types (int, string, bytes) as top-level: Supported, wrapped in "value" column
- Arrays as top-level: Not yet supported (list builder constraints)
- Maps as top-level: Not yet supported (struct builder constraints)

Test data source: tests/data/fastavro/

Requirements tested:
- 1.4: Non-record top-level schema support
- Error messages for unsupported types
"""


import polars as pl
import pytest

import jetliner



class TestNonRecordTopLevelSchemas:
    """
    Test reading Avro files with non-record top-level schemas.

    According to the Avro spec, top-level schemas can be any type,
    not just records. For non-record schemas, Jetliner wraps the data
    in a single-column DataFrame with the column named "value".
    """

    @pytest.mark.xfail(
        reason="Array top-level schemas not yet fully supported - list builder type error"
    )
    def test_array_toplevel_schema(self, get_test_data_path):
        """
        Test reading file with array as top-level schema.

        Schema: {"type": "array", "items": "int"}
        Each record is an array of integers, wrapped in "value" column.

        Known Limitation: Currently fails with "cannot build list with different dtypes"
        error in the list builder. Array-as-record-value handling needs additional work.
        """
        path = get_test_data_path("fastavro/array-toplevel.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)

            # Should have single "value" column
            assert df.width == 1, "Should have exactly 1 column"
            assert "value" in df.columns, "Column should be named 'value'"

            # Should have records
            assert df.height > 0, "Should have at least one record"

            # Value column should be List type
            assert isinstance(
                df["value"].dtype, pl.List
            ), f"Expected List type, got {df['value'].dtype}"

    @pytest.mark.xfail(
        reason="Map top-level schemas not yet fully supported - struct handling in list builder"
    )
    def test_map_toplevel_schema(self, get_test_data_path):
        """
        Test reading file with map as top-level schema.

        Schema: {"type": "map", "values": "string"}
        Each record is a map (dict), wrapped in "value" column.

        Known Limitation: Currently fails with "inner type must be primitive, got struct[2]"
        panic in Polars list builder. Map-as-record-value handling needs additional work.
        """
        path = get_test_data_path("fastavro/map-toplevel.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)

            # Should have single "value" column
            assert df.width == 1, "Should have exactly 1 column"
            assert "value" in df.columns, "Column should be named 'value'"

            # Should have records
            assert df.height > 0, "Should have at least one record"

            # Value column should be Struct type (maps become structs in Polars)
            # Note: Polars may represent maps differently depending on implementation

    def test_int_toplevel_schema(self, get_test_data_path):
        """
        Test reading file with int as top-level schema.

        Schema: "int"
        Each record is a single integer, wrapped in "value" column.
        """
        path = get_test_data_path("fastavro/int-toplevel.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)

            # Should have single "value" column
            assert df.width == 1, "Should have exactly 1 column"
            assert "value" in df.columns, "Column should be named 'value'"

            # Should have records
            assert df.height > 0, "Should have at least one record"

            # Value column should be integer type
            assert df["value"].dtype in [
                pl.Int32,
                pl.Int64,
            ], f"Expected Int type, got {df['value'].dtype}"

    def test_string_toplevel_schema(self, get_test_data_path):
        """
        Test reading file with string as top-level schema.

        Schema: "string"
        Each record is a single string, wrapped in "value" column.
        """
        path = get_test_data_path("fastavro/string-toplevel.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)

            # Should have single "value" column
            assert df.width == 1, "Should have exactly 1 column"
            assert "value" in df.columns, "Column should be named 'value'"

            # Should have records
            assert df.height > 0, "Should have at least one record"

            # Value column should be string type
            assert (
                df["value"].dtype == pl.Utf8
            ), f"Expected Utf8 type, got {df['value'].dtype}"

    @pytest.mark.xfail(
        reason="Array top-level schemas not yet fully supported via scan API"
    )
    def test_array_toplevel_scan_api(self, get_test_data_path):
        """Test scan_avro() API with array top-level schema."""
        path = get_test_data_path("fastavro/array-toplevel.avro")

        lf = jetliner.scan_avro(path)
        assert isinstance(lf, pl.LazyFrame)

        df = lf.collect()
        assert "value" in df.columns
        assert df.height > 0

    def test_non_record_projection_works(self, get_test_data_path):
        """Test that projection works with non-record schemas."""
        path = get_test_data_path("fastavro/int-toplevel.avro")

        # Should be able to select the "value" column
        df = jetliner.scan_avro(path).select("value").collect()

        assert df.width == 1
        assert "value" in df.columns

    def test_array_toplevel_error_message(self, get_test_data_path):
        """Test that array top-level gives a helpful error message."""
        path = get_test_data_path("fastavro/array-toplevel.avro")

        # Test with open() API - raises SchemaError directly
        with pytest.raises(jetliner.SchemaError) as exc_info:
            with jetliner.open(path) as reader:
                list(reader)

        error_msg = str(exc_info.value).lower()
        assert "not yet supported" in error_msg
        assert "workaround" in error_msg
        assert "primitive types" in error_msg

        # Test with scan_avro() API - follows Polars lazy evaluation norms
        # Error surfaces during .collect() wrapped in ComputeError
        lf = jetliner.scan_avro(path)
        assert isinstance(lf, pl.LazyFrame)

        with pytest.raises(pl.exceptions.ComputeError) as exc_info:
            lf.collect()

        error_msg = str(exc_info.value).lower()
        assert "not yet supported" in error_msg

    def test_map_toplevel_error_message(self, get_test_data_path):
        """Test that map top-level gives a helpful error message."""
        path = get_test_data_path("fastavro/map-toplevel.avro")

        # Test with open() API - raises SchemaError directly
        with pytest.raises(jetliner.SchemaError) as exc_info:
            with jetliner.open(path) as reader:
                list(reader)

        error_msg = str(exc_info.value).lower()
        assert "not yet supported" in error_msg
        assert "workaround" in error_msg
        assert "primitive types" in error_msg

        # Test with scan_avro() API - follows Polars lazy evaluation norms
        # Error surfaces during .collect() wrapped in ComputeError
        lf = jetliner.scan_avro(path)
        assert isinstance(lf, pl.LazyFrame)

        with pytest.raises(pl.exceptions.ComputeError) as exc_info:
            lf.collect()

        error_msg = str(exc_info.value).lower()
        assert "not yet supported" in error_msg


# =============================================================================
# Phase 2c: Complex Recursive Structures
# =============================================================================
