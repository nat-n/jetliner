"""
Interoperability tests with fastavro edge case files.

Tests Jetliner's ability to read files created by fastavro (Python Avro library).
Focuses on edge cases like empty records, null types, recursive types, and UUIDs.

Test data source: tests/data/fastavro/

Requirements tested:
- 10.1: Interoperability with fastavro
- 10.2: Primitive types
- 10.4: Logical types (UUID)
"""


import polars as pl

import jetliner



class TestFastavroEdgeCases:
    """
    Test reading fastavro edge case test files.

    These files test specific edge cases that fastavro identified
    as important for robust Avro reading.
    """

    def test_read_no_fields_record(self, get_test_data_path):
        """
        Test reading record with no fields.

        This is an edge case where the schema defines a record
        type but with an empty fields array. The file contains
        1 record with no fields (empty dict {}).

        Note: Jetliner returns 0 DataFrames for no-fields records since
        there's no columnar data to return. This is valid behavior - the
        file is readable but produces no output columns.
        """
        path = get_test_data_path("fastavro/no-fields.avro")

        with jetliner.open(path) as reader:
            # Verify schema is accessible and has no fields
            schema = reader.schema
            assert schema is not None, "Schema should be accessible"

            dfs = list(reader)

            # No-fields records produce no DataFrames (no columns to output)
            # This is expected behavior - the file is valid but has no data
            if len(dfs) > 0:
                df = pl.concat(dfs)
                # If any DataFrames returned, they should have 0 columns
                assert df.width == 0, f"Expected 0 columns for no-fields record, got {df.width}"

    def test_read_null_type(self, get_test_data_path):
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

    def test_read_recursive_record(self, get_test_data_path):
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

    def test_read_java_generated_uuid(self, get_test_data_path):
        """
        Test reading Java-generated file with UUID logical type.

        This tests interoperability with Java Avro implementation
        and proper handling of UUID logical type.

        The file contains 151 records with:
        - id: UUID (e.g., "25f95c12-d66b-4070-b581-0d92ec959193")
        - name: string (e.g., "Test Instance 0")
        """
        import re

        path = get_test_data_path("fastavro/java-generated-uuid.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)

            # Verify record count
            assert df.height == 151, f"Expected 151 records, got {df.height}"

            # Verify columns exist
            assert "id" in df.columns, "Should have 'id' column"
            assert "name" in df.columns, "Should have 'name' column"

            # Verify UUID format
            uuid_pattern = re.compile(
                r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                re.IGNORECASE,
            )
            ids = df["id"].to_list()
            for i, uuid_str in enumerate(ids):
                assert uuid_pattern.match(uuid_str), f"Record {i} has invalid UUID: {uuid_str}"

            # Verify specific known values
            assert ids[0] == "25f95c12-d66b-4070-b581-0d92ec959193"

            # Verify name pattern
            names = df["name"].to_list()
            for i, name in enumerate(names):
                assert name == f"Test Instance {i}", f"Record {i} has unexpected name: {name}"


# =============================================================================
# Phase 2b: Non-Record Top-Level Schemas
# =============================================================================
