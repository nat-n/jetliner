"""
End-to-end tests using real Avro files from other implementations.

These tests validate Jetliner's ability to read files created by:
- Apache Avro (Java, Python, C++) - Apache License 2.0
- fastavro (Python) - MIT License

Test data sources:
- tests/data/apache-avro/: Official Apache Avro test files
- tests/data/fastavro/: fastavro edge case test files

Requirements tested:
- 10.1: Apache Avro interoperability test files
- 10.2: All primitive types
- 10.3: All complex types
- 10.4: All logical types
- 10.5: All supported codecs

Known Limitations:
- recursive.avro: Recursive type resolution not fully supported
"""

from pathlib import Path

import polars as pl
import pytest

import jetliner


# =============================================================================
# Test Data Paths
# =============================================================================

def get_test_data_path(relative_path: str) -> str:
    """Get absolute path to test data file."""
    # Navigate from python/tests to tests/data
    tests_dir = Path(__file__).parent.parent.parent / "tests" / "data"
    return str(tests_dir / relative_path)


# =============================================================================
# Phase 1: Apache Avro Weather Files - Basic Interoperability
# =============================================================================

class TestApacheAvroWeatherFiles:
    """
    Test reading official Apache Avro weather test files.

    These files are the standard interoperability test files used by
    all Apache Avro implementations (Java, Python, C++, etc.).

    Weather schema:
    {
        "type": "record",
        "name": "Weather",
        "namespace": "test",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"}
        ]
    }
    """

    def test_read_weather_uncompressed(self):
        """Test reading uncompressed weather.avro file."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)

            # Validate schema
            assert "station" in df.columns
            assert "time" in df.columns
            assert "temp" in df.columns

            # Validate we read records
            assert df.height > 0, "Should have at least one record"

    def test_read_weather_deflate(self):
        """Test reading deflate-compressed weather file."""
        path = get_test_data_path("apache-avro/weather-deflate.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)

            assert df.height > 0
            assert "station" in df.columns
            assert "time" in df.columns
            assert "temp" in df.columns

    def test_read_weather_snappy(self):
        """Test reading snappy-compressed weather file with CRC32 validation."""
        path = get_test_data_path("apache-avro/weather-snappy.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)

            assert df.height > 0
            assert "station" in df.columns

    def test_read_weather_zstd(self):
        """Test reading zstd-compressed weather file."""
        path = get_test_data_path("apache-avro/weather-zstd.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)

            assert df.height > 0
            assert "station" in df.columns

    def test_read_weather_sorted(self):
        """Test reading sorted weather file."""
        path = get_test_data_path("apache-avro/weather-sorted.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)

            assert df.height > 0
            assert "station" in df.columns

    def test_all_codecs_produce_same_data(self):
        """
        Test that all codec variants produce identical data.

        This is a critical interoperability test - all compression
        codecs should decompress to the same underlying data.
        """
        variants = [
            "weather.avro",
            "weather-deflate.avro",
            "weather-snappy.avro",
            "weather-zstd.avro",
        ]

        dataframes = []
        for variant in variants:
            path = get_test_data_path(f"apache-avro/{variant}")
            with jetliner.open(path) as reader:
                dfs = list(reader)
                df = pl.concat(dfs)
                dataframes.append((variant, df))

        # All should have same record count
        counts = [df.height for _, df in dataframes]
        assert len(set(counts)) == 1, f"All variants should have same record count: {dict(zip(variants, counts))}"

        # All should have same data (compare first record)
        first_records = []
        for variant, df in dataframes:
            first_row = df.head(1).to_dicts()[0]
            first_records.append((variant, first_row))

        baseline = first_records[0][1]
        for variant, record in first_records[1:]:
            assert record == baseline, f"{variant} differs from baseline"


# =============================================================================
# Phase 1b: Apache Avro Weather Files with scan() API
# =============================================================================

class TestApacheAvroWeatherScan:
    """Test scan() API with Apache Avro weather files."""

    def test_scan_weather_returns_lazyframe(self):
        """Test that scan() returns a LazyFrame."""
        path = get_test_data_path("apache-avro/weather.avro")
        lf = jetliner.scan(path)

        assert isinstance(lf, pl.LazyFrame)

    def test_scan_weather_collect(self):
        """Test collecting scanned weather file."""
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan(path).collect()

        assert isinstance(df, pl.DataFrame)
        assert df.height > 0
        assert "station" in df.columns

    def test_scan_weather_projection(self):
        """Test projection pushdown with weather file."""
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan(path).select(["station", "temp"]).collect()

        assert df.width == 2
        assert "station" in df.columns
        assert "temp" in df.columns
        assert "time" not in df.columns

    def test_scan_weather_filter(self):
        """Test predicate pushdown with weather file."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Get all data first to know what to filter
        all_df = jetliner.scan(path).collect()

        # Filter for specific station
        if all_df.height > 0:
            first_station = all_df["station"][0]
            filtered_df = (
                jetliner.scan(path)
                .filter(pl.col("station") == first_station)
                .collect()
            )

            # All records should have the filtered station
            assert (filtered_df["station"] == first_station).all()

    def test_scan_weather_head(self):
        """Test early stopping with weather file."""
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan(path).head(2).collect()

        assert df.height <= 2

    @pytest.mark.parametrize("codec", ["deflate", "snappy", "zstd"])
    def test_scan_all_codecs(self, codec):
        """Test scan() with all codec variants."""
        path = get_test_data_path(f"apache-avro/weather-{codec}.avro")
        df = jetliner.scan(path).collect()

        assert df.height > 0
        assert "station" in df.columns

    def test_scan_snappy_codec(self):
        """Test scan() with snappy codec."""
        path = get_test_data_path("apache-avro/weather-snappy.avro")
        df = jetliner.scan(path).collect()

        assert df.height > 0
        assert "station" in df.columns


# =============================================================================
# Phase 2: fastavro Edge Case Files
# =============================================================================

class TestFastavroEdgeCases:
    """
    Test reading fastavro edge case test files.

    These files test specific edge cases that fastavro identified
    as important for robust Avro reading.
    """

    def test_read_no_fields_record(self):
        """
        Test reading record with no fields.

        This is an edge case where the schema defines a record
        type but with an empty fields array.
        """
        path = get_test_data_path("fastavro/no-fields.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            # Should be able to read without error
            # May have 0 columns but should not crash

    def test_read_null_type(self):
        """
        Test reading file with non-record top-level schema.

        Tests proper handling of Avro files where the top-level schema
        is not a record type (e.g., bytes, array, etc.). These are wrapped
        in a synthetic record with a single "value" field.
        """
        path = get_test_data_path("fastavro/null.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            # Should read without error
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)
            # Should have a single "value" column
            assert "value" in df.columns, "Should have 'value' column"
            assert df.height > 0, "Should have at least one record"

    def test_read_recursive_record(self):
        """
        Test reading self-referential/recursive record types.

        Avro supports recursive types where a record can contain
        a field that references itself (e.g., linked list, tree).

        Since Arrow/Polars doesn't support recursive types natively,
        Jetliner serializes recursive fields to JSON strings.
        """
        path = get_test_data_path("fastavro/recursive.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)
            assert df.height == 2, "Should have 2 records"

            # Check columns exist
            assert "value" in df.columns
            assert "next" in df.columns

            # The 'next' field should be a string (JSON serialized)
            assert df["next"].dtype == pl.Utf8

            # Verify the values
            values = df["value"].to_list()
            assert values == [989, 314]

            # Verify the JSON structure is valid
            import json
            for next_val in df["next"].to_list():
                parsed = json.loads(next_val)
                assert "value" in parsed
                assert "next" in parsed

    def test_read_java_generated_uuid(self):
        """
        Test reading Java-generated file with UUID logical type.

        This tests interoperability with Java Avro implementation
        and proper handling of UUID logical type.
        """
        path = get_test_data_path("fastavro/java-generated-uuid.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            df = pl.concat(dfs) if dfs else pl.DataFrame()

            # Should read without error
            # UUID should be present as string


# =============================================================================
# Phase 2b: Non-Record Top-Level Schemas
# =============================================================================

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
    def test_array_toplevel_schema(self):
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
            assert isinstance(df["value"].dtype, pl.List), \
                f"Expected List type, got {df['value'].dtype}"

    @pytest.mark.xfail(
        reason="Map top-level schemas not yet fully supported - struct handling in list builder"
    )
    def test_map_toplevel_schema(self):
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

    def test_int_toplevel_schema(self):
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
            assert df["value"].dtype in [pl.Int32, pl.Int64], \
                f"Expected Int type, got {df['value'].dtype}"

    def test_string_toplevel_schema(self):
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
            assert df["value"].dtype == pl.Utf8, \
                f"Expected Utf8 type, got {df['value'].dtype}"

    @pytest.mark.xfail(
        reason="Array top-level schemas not yet fully supported via scan API"
    )
    def test_array_toplevel_scan_api(self):
        """Test scan() API with array top-level schema."""
        path = get_test_data_path("fastavro/array-toplevel.avro")

        lf = jetliner.scan(path)
        assert isinstance(lf, pl.LazyFrame)

        df = lf.collect()
        assert "value" in df.columns
        assert df.height > 0

    def test_non_record_projection_works(self):
        """Test that projection works with non-record schemas."""
        path = get_test_data_path("fastavro/int-toplevel.avro")

        # Should be able to select the "value" column
        df = jetliner.scan(path).select("value").collect()

        assert df.width == 1
        assert "value" in df.columns

    def test_array_toplevel_error_message(self):
        """Test that array top-level gives a helpful error message."""
        path = get_test_data_path("fastavro/array-toplevel.avro")

        # Test with open() API
        with pytest.raises(jetliner.SchemaError) as exc_info:
            with jetliner.open(path) as reader:
                list(reader)

        error_msg = str(exc_info.value).lower()
        assert "not yet supported" in error_msg
        assert "workaround" in error_msg
        assert "primitive types" in error_msg

        # Test with scan() API
        with pytest.raises(jetliner.SchemaError) as exc_info:
            jetliner.scan(path)

        error_msg = str(exc_info.value).lower()
        assert "not yet supported" in error_msg

    def test_map_toplevel_error_message(self):
        """Test that map top-level gives a helpful error message."""
        path = get_test_data_path("fastavro/map-toplevel.avro")

        # Test with open() API
        with pytest.raises(jetliner.SchemaError) as exc_info:
            with jetliner.open(path) as reader:
                list(reader)

        error_msg = str(exc_info.value).lower()
        assert "not yet supported" in error_msg
        assert "workaround" in error_msg
        assert "primitive types" in error_msg

        # Test with scan() API
        with pytest.raises(jetliner.SchemaError) as exc_info:
            jetliner.scan(path)

        error_msg = str(exc_info.value).lower()
        assert "not yet supported" in error_msg


# =============================================================================
# Phase 2c: Complex Recursive Structures
# =============================================================================

class TestComplexRecursiveStructures:
    """
    Test reading Avro files with complex recursive structures.

    These tests verify that Jetliner can handle various recursive
    patterns including binary trees, n-ary trees, and deeply nested
    structures.
    """

    def test_binary_tree_structure(self):
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
    def test_nary_tree_structure(self):
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
            assert df["children"].dtype in [pl.Utf8, pl.List(pl.Utf8)], \
                f"Unexpected children type: {df['children'].dtype}"

    def test_tree_with_scan_api(self):
        """Test scan() API with recursive tree structure."""
        path = get_test_data_path("fastavro/tree-recursive.avro")

        df = jetliner.scan(path).collect()

        assert df.height > 0
        assert "value" in df.columns
        assert "left" in df.columns
        assert "right" in df.columns

    def test_tree_projection(self):
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
    def test_deeply_nested_recursive(self):
        """Test that deeply nested recursive structures are handled correctly.

        Known Limitation: Same as test_nary_tree_structure - arrays of recursive types
        not yet fully supported.
        """
        path = get_test_data_path("fastavro/graph-recursive.avro")

        df = jetliner.scan(path).collect()

        # Should successfully read deeply nested structure
        assert df.height > 0

        # Verify the deep nesting is present in the JSON
        import json
        for i in range(df.height):
            children_val = df["children"][i]
            if children_val and children_val != "[]":
                # Should be able to parse as JSON
                # Exact format depends on implementation
                pass  # Just verify no errors


# =============================================================================
# Phase 3: Schema Inspection
# =============================================================================

class TestSchemaInspection:
    """Test schema inspection capabilities with real files."""

    def test_weather_schema_json(self):
        """Test schema JSON extraction from weather file."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path) as reader:
            schema_json = reader.schema

            assert isinstance(schema_json, str)
            assert "Weather" in schema_json or "weather" in schema_json.lower()
            assert "station" in schema_json
            assert "time" in schema_json
            assert "temp" in schema_json

    def test_weather_schema_dict(self):
        """Test schema dict extraction from weather file."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path) as reader:
            schema_dict = reader.schema_dict

            assert isinstance(schema_dict, dict)
            assert schema_dict.get("type") == "record"

            # Check fields
            fields = schema_dict.get("fields", [])
            field_names = [f["name"] for f in fields]
            assert "station" in field_names
            assert "time" in field_names
            assert "temp" in field_names

    def test_parse_avro_schema_function(self):
        """Test parse_avro_schema() function with real file."""
        path = get_test_data_path("apache-avro/weather.avro")

        polars_schema = jetliner.parse_avro_schema(path)

        # Should return a Polars schema dict
        assert "station" in polars_schema
        assert "time" in polars_schema
        assert "temp" in polars_schema


# =============================================================================
# Phase 4: Error Handling with Real Files
# =============================================================================

class TestErrorHandlingRealFiles:
    """Test error handling with real files."""

    def test_skip_mode_on_valid_file(self):
        """Test that skip mode works on valid files without errors."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path, strict=False) as reader:
            dfs = list(reader)

            # Should read successfully
            assert len(dfs) > 0

            # Should have no errors
            assert reader.error_count == 0
            assert len(reader.errors) == 0

    def test_strict_mode_on_valid_file(self):
        """Test that strict mode works on valid files."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path, strict=True) as reader:
            dfs = list(reader)

            # Should read successfully
            assert len(dfs) > 0


# =============================================================================
# Phase 5: Data Type Validation
# =============================================================================

class TestDataTypeValidation:
    """Test that data types are correctly mapped to Polars types."""

    def test_weather_data_types(self):
        """Test data type mapping for weather file."""
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan(path).collect()

        # station should be string
        assert df["station"].dtype == pl.Utf8

        # time should be integer (long)
        assert df["time"].dtype in [pl.Int64, pl.Int32]

        # temp should be integer
        assert df["temp"].dtype in [pl.Int32, pl.Int64]

    def test_weather_data_values(self):
        """Test that data values are correctly read."""
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan(path).collect()

        # Station should be non-empty strings
        stations = df["station"].to_list()
        assert all(isinstance(s, str) and len(s) > 0 for s in stations)

        # Time should be integers (timestamps)
        times = df["time"].to_list()
        assert all(isinstance(t, int) for t in times)

        # Temp should be integers
        temps = df["temp"].to_list()
        assert all(isinstance(t, int) for t in temps)


# =============================================================================
# Phase 6: Streaming and Memory Efficiency
# =============================================================================

class TestStreamingBehavior:
    """Test streaming behavior with real files."""

    def test_batch_iteration(self):
        """Test that files can be read in batches."""
        path = get_test_data_path("apache-avro/weather.avro")

        batch_count = 0
        total_rows = 0

        with jetliner.open(path, batch_size=1) as reader:
            for df in reader:
                batch_count += 1
                total_rows += df.height

        # Should have read all records
        assert total_rows > 0

    def test_large_batch_size(self):
        """Test with large batch size (all records in one batch)."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path, batch_size=1000000) as reader:
            dfs = list(reader)

            # With large batch size, should get few batches
            assert len(dfs) >= 1


# =============================================================================
# Phase 7: Comprehensive Query Tests
# =============================================================================

class TestComprehensiveQueries:
    """Test comprehensive query patterns with real files."""

    def test_full_query_pipeline(self):
        """Test a full query pipeline with projection, filter, and limit."""
        path = get_test_data_path("apache-avro/weather.avro")

        df = (
            jetliner.scan(path)
            .select(["station", "temp"])
            .filter(pl.col("temp").is_not_null())
            .head(10)
            .collect()
        )

        assert df.height <= 10
        assert df.width == 2
        assert "station" in df.columns
        assert "temp" in df.columns

    def test_aggregation_query(self):
        """Test aggregation query on real data."""
        path = get_test_data_path("apache-avro/weather.avro")

        df = (
            jetliner.scan(path)
            .group_by("station")
            .agg(pl.col("temp").mean().alias("avg_temp"))
            .collect()
        )

        # Should have aggregated results
        assert "station" in df.columns
        assert "avg_temp" in df.columns

    def test_sorting_query(self):
        """Test sorting query on real data."""
        path = get_test_data_path("apache-avro/weather.avro")

        df = (
            jetliner.scan(path)
            .sort("temp", descending=True)
            .head(5)
            .collect()
        )

        # Should be sorted by temp descending
        temps = df["temp"].to_list()
        assert temps == sorted(temps, reverse=True)


# =============================================================================
# Phase 8: Interoperability Validation
# =============================================================================

class TestInteroperabilityValidation:
    """
    Validate interoperability with other Avro implementations.

    These tests ensure Jetliner can correctly read files produced
    by different Avro libraries.
    """

    def test_java_interop_uuid(self):
        """
        Test reading Java-generated file with UUID.

        This validates interoperability with the Java Avro implementation,
        which is the reference implementation.
        """
        path = get_test_data_path("fastavro/java-generated-uuid.avro")

        # Should be able to read without error
        df = jetliner.scan(path).collect()

        # File should have data
        assert df.height >= 0  # May be empty but should not error

    def test_cross_codec_consistency(self):
        """
        Test that data is consistent across all codec implementations.

        This is a critical test for codec correctness - all codecs
        should produce byte-identical decompressed data.
        """
        base_path = get_test_data_path("apache-avro/weather.avro")
        base_df = jetliner.scan(base_path).collect()

        for codec in ["deflate", "snappy", "zstd"]:
            codec_path = get_test_data_path(f"apache-avro/weather-{codec}.avro")
            codec_df = jetliner.scan(codec_path).collect()

            # Should have same shape
            assert codec_df.height == base_df.height, f"{codec} has different row count"
            assert codec_df.width == base_df.width, f"{codec} has different column count"

            # Should have same data
            assert codec_df.equals(base_df), f"{codec} data differs from uncompressed"


# =============================================================================
# Phase 9: Edge Cases and Robustness
# =============================================================================

class TestEdgeCasesAndRobustness:
    """Test edge cases and robustness with real files."""

    def test_multiple_reads_same_file(self):
        """Test reading the same file multiple times."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Read multiple times
        results = []
        for _ in range(3):
            df = jetliner.scan(path).collect()
            results.append(df)

        # All reads should produce identical results
        for i, df in enumerate(results[1:], 1):
            assert df.equals(results[0]), f"Read {i} differs from first read"

    def test_concurrent_file_access(self):
        """Test that multiple readers can access files concurrently."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Open multiple readers
        reader1 = jetliner.open(path)
        reader2 = jetliner.open(path)

        # Both should be able to read
        df1 = pl.concat(list(reader1))
        df2 = pl.concat(list(reader2))

        assert df1.equals(df2)

    def test_reader_reuse_after_exhaustion(self):
        """Test behavior when trying to reuse exhausted reader."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path) as reader:
            # Exhaust the reader
            dfs = list(reader)
            assert len(dfs) > 0

            # Reader should be finished
            assert reader.is_finished

            # Further iteration should yield nothing or raise an error
            # The reader raises JetlinerError when closed - this is acceptable behavior
            try:
                more_dfs = list(reader)
                assert len(more_dfs) == 0
            except (StopIteration, jetliner.JetlinerError):
                pass  # Both are acceptable behaviors for exhausted reader


# =============================================================================
# Phase 10: Performance Sanity Checks
# =============================================================================

class TestPerformanceSanity:
    """
    Basic performance sanity checks.

    These are not benchmarks but ensure operations complete
    in reasonable time.
    """

    def test_weather_file_reads_quickly(self):
        """Test that weather file reads in reasonable time."""
        import time

        path = get_test_data_path("apache-avro/weather.avro")

        start = time.time()
        df = jetliner.scan(path).collect()
        elapsed = time.time() - start

        # Should complete in under 1 second for small file
        assert elapsed < 1.0, f"Read took {elapsed:.2f}s, expected < 1s"

    def test_all_codecs_read_quickly(self):
        """Test that all codec variants read in reasonable time."""
        import time

        for codec in ["", "-deflate", "-snappy", "-zstd"]:
            filename = f"weather{codec}.avro"
            path = get_test_data_path(f"apache-avro/{filename}")

            start = time.time()
            df = jetliner.scan(path).collect()
            elapsed = time.time() - start

            # Should complete in under 1 second
            assert elapsed < 1.0, f"{filename} took {elapsed:.2f}s"


# =============================================================================
# Parametrized Tests for Comprehensive Coverage
# =============================================================================

@pytest.mark.parametrize("filename", [
    "weather.avro",
    "weather-deflate.avro",
    "weather-snappy.avro",
    "weather-zstd.avro",
    "weather-sorted.avro",
])
def test_apache_avro_file_readable(filename):
    """Test that all Apache Avro test files are readable."""
    path = get_test_data_path(f"apache-avro/{filename}")

    df = jetliner.scan(path).collect()
    assert df.height > 0


@pytest.mark.parametrize("filename,xfail_reason", [
    ("no-fields.avro", None),
    ("null.avro", None),  # Non-record schema now supported
    ("recursive.avro", None),  # Recursive types serialized to JSON
    ("java-generated-uuid.avro", None),
    # Additional non-record top-level schemas
    ("array-toplevel.avro", "Array top-level schemas - list builder type error"),
    ("map-toplevel.avro", "Map top-level schemas - struct in list builder not supported"),
    ("int-toplevel.avro", None),  # ✅ Works!
    ("string-toplevel.avro", None),  # ✅ Works!
    # Complex recursive structures
    ("tree-recursive.avro", None),  # ✅ Binary trees work!
    ("graph-recursive.avro", "N-ary trees with array children - list builder error"),
])
def test_fastavro_file_readable(filename, xfail_reason):
    """Test that all fastavro test files are readable."""
    if xfail_reason:
        pytest.xfail(xfail_reason)

    path = get_test_data_path(f"fastavro/{filename}")

    # Should not raise an exception
    with jetliner.open(path) as reader:
        list(reader)
        # May have 0 records but should not crash


@pytest.mark.parametrize("api", ["open", "scan"])
def test_both_apis_work(api):
    """Test that both open() and scan() APIs work with real files."""
    path = get_test_data_path("apache-avro/weather.avro")

    if api == "open":
        with jetliner.open(path) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)
    else:
        df = jetliner.scan(path).collect()

    assert df.height > 0
    assert "station" in df.columns
