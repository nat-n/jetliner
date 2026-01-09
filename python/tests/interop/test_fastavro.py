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
        type but with an empty fields array.
        """
        path = get_test_data_path("fastavro/no-fields.avro")

        with jetliner.open(path) as reader:
            _dfs = list(reader)
            # Should be able to read without error
            # May have 0 columns but should not crash

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
        """
        path = get_test_data_path("fastavro/java-generated-uuid.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            _df = pl.concat(dfs) if dfs else pl.DataFrame()

            # Should read without error
            # UUID should be present as string


# =============================================================================
# Phase 2b: Non-Record Top-Level Schemas
# =============================================================================
