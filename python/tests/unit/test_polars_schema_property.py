"""
Unit tests for the polars_schema property on AvroReader and MultiAvroReader.

Tests cover:
- polars_schema returns a pl.Schema instance
- polars_schema contains expected column names and types
- polars_schema matches what read_avro_schema() returns for the same file
- polars_schema is available before iteration begins
- polars_schema survives after iteration completes
- polars_schema works on empty files
- polars_schema reflects projection when projected_columns is used
"""

import polars as pl

import jetliner
from jetliner import MultiAvroReader


class TestAvroReaderPolarsSchema:
    """Tests for AvroReader.polars_schema property."""

    def test_returns_polars_schema_type(self, temp_avro_file):
        """polars_schema should return a pl.Schema instance."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            schema = reader.polars_schema
            assert isinstance(schema, pl.Schema)

    def test_contains_expected_columns(self, temp_avro_file):
        """polars_schema should contain the correct column names."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            schema = reader.polars_schema
            assert "id" in schema
            assert "name" in schema

    def test_contains_expected_dtypes(self, temp_avro_file):
        """polars_schema should map Avro types to correct Polars dtypes."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            schema = reader.polars_schema
            # Avro long -> Polars Int64
            assert schema["id"] == pl.Int64
            # Avro string -> Polars String
            assert schema["name"] == pl.String

    def test_matches_read_avro_schema(self, temp_avro_file):
        """polars_schema should match what read_avro_schema() returns."""
        standalone = jetliner.read_avro_schema(temp_avro_file)
        with jetliner.AvroReader(temp_avro_file) as reader:
            assert reader.polars_schema == standalone

    def test_available_before_iteration(self, temp_avro_file):
        """polars_schema should be accessible before any data is read."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            # Access before iterating — should not raise
            schema = reader.polars_schema
            assert len(schema) == 2

    def test_available_after_iteration(self, temp_avro_file):
        """polars_schema should remain accessible after iteration completes."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            for _ in reader:
                pass
            schema = reader.polars_schema
            assert len(schema) == 2

    def test_matches_dataframe_schema(self, temp_avro_file):
        """polars_schema should match the schema of produced DataFrames."""
        with jetliner.AvroReader(temp_avro_file) as reader:
            declared = reader.polars_schema
            for df in reader:
                assert df.schema == declared

    def test_empty_file(self, temp_empty_avro_file):
        """polars_schema should work on files with no records."""
        with jetliner.AvroReader(temp_empty_avro_file) as reader:
            schema = reader.polars_schema
            assert isinstance(schema, pl.Schema)
            assert "id" in schema
            assert "name" in schema

    def test_with_projection(self, temp_avro_file):
        """polars_schema should reflect projected columns when projection is used."""
        with jetliner.AvroReader(
            temp_avro_file, projected_columns=["name"]
        ) as reader:
            schema = reader.polars_schema
            assert "name" in schema
            assert "id" not in schema
            assert len(schema) == 1

    def test_survives_context_manager_exit(self, temp_avro_file):
        """polars_schema should be accessible after __exit__."""
        reader = jetliner.AvroReader(temp_avro_file)
        reader.__enter__()
        schema_before = reader.polars_schema
        reader.__exit__(None, None, None)
        schema_after = reader.polars_schema
        assert schema_before == schema_after


class TestMultiAvroReaderPolarsSchema:
    """Tests for MultiAvroReader.polars_schema property."""

    def test_returns_polars_schema_type(self, temp_avro_file):
        """polars_schema should return a pl.Schema instance."""
        with MultiAvroReader([temp_avro_file]) as reader:
            schema = reader.polars_schema
            assert isinstance(schema, pl.Schema)

    def test_contains_expected_columns(self, temp_avro_file):
        """polars_schema should contain the correct column names."""
        with MultiAvroReader([temp_avro_file]) as reader:
            schema = reader.polars_schema
            assert "id" in schema
            assert "name" in schema

    def test_contains_expected_dtypes(self, temp_avro_file):
        """polars_schema should map Avro types to correct Polars dtypes."""
        with MultiAvroReader([temp_avro_file]) as reader:
            schema = reader.polars_schema
            assert schema["id"] == pl.Int64
            assert schema["name"] == pl.String

    def test_matches_read_avro_schema(self, temp_avro_file):
        """polars_schema should match what read_avro_schema() returns."""
        standalone = jetliner.read_avro_schema(temp_avro_file)
        with MultiAvroReader([temp_avro_file]) as reader:
            assert reader.polars_schema == standalone

    def test_available_before_iteration(self, temp_avro_file):
        """polars_schema should be accessible before any data is read."""
        reader = MultiAvroReader([temp_avro_file])
        schema = reader.polars_schema
        assert len(schema) == 2

    def test_available_after_iteration(self, temp_avro_file):
        """polars_schema should remain accessible after iteration completes."""
        reader = MultiAvroReader([temp_avro_file])
        list(reader)
        schema = reader.polars_schema
        assert len(schema) == 2

    def test_matches_dataframe_schema(self, temp_avro_file):
        """polars_schema should match the schema of produced DataFrames."""
        reader = MultiAvroReader([temp_avro_file])
        declared = reader.polars_schema
        for df in reader:
            assert df.schema == declared

    def test_multiple_files_unified_schema(self, temp_avro_file):
        """polars_schema should reflect the unified schema across multiple files."""
        with MultiAvroReader([temp_avro_file, temp_avro_file]) as reader:
            schema = reader.polars_schema
            assert "id" in schema
            assert "name" in schema

    def test_with_projection(self, temp_avro_file):
        """polars_schema should reflect projected columns when projection is used."""
        with MultiAvroReader(
            [temp_avro_file], projected_columns=["id"]
        ) as reader:
            schema = reader.polars_schema
            assert "id" in schema
            assert "name" not in schema
            assert len(schema) == 1
