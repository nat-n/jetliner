"""
Integration tests for predicate pushdown (filtering).

Tests cover:
- Predicate pushdown - Requirement 6a.3
- Filter operation with various predicates
- Combining filters with other operations
- Predicate evaluation correctness
"""


import polars as pl

import jetliner


class TestPredicatePushdown:
    """Tests for predicate pushdown optimization."""

    def test_filter_equality(self, temp_avro_file):
        """Test filtering with equality predicate."""
        df = jetliner.scan(temp_avro_file).filter(pl.col("id") == 3).collect()

        assert df.height == 1
        assert df["id"].to_list() == [3]
        assert df["name"].to_list() == ["Charlie"]

    def test_filter_comparison(self, temp_avro_file):
        """Test filtering with comparison predicate."""
        df = jetliner.scan(temp_avro_file).filter(pl.col("id") > 3).collect()

        assert df.height == 2
        assert df["id"].to_list() == [4, 5]

    def test_filter_string(self, temp_avro_file):
        """Test filtering on string column."""
        df = jetliner.scan(temp_avro_file).filter(pl.col("name") == "Alice").collect()

        assert df.height == 1
        assert df["name"].to_list() == ["Alice"]

    def test_filter_combined_with_select(self, temp_avro_file):
        """Test combining filter with select."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter(pl.col("id") > 2)
            .select("name")
            .collect()
        )

        assert df.width == 1
        assert df.height == 3
        assert df["name"].to_list() == ["Charlie", "Diana", "Eve"]


class TestComplexPredicates:
    """Tests for complex predicate combinations (AND, OR, NOT).

    Coverage gap: Tests for complex predicates were missing.
    """

    def test_filter_and_predicate(self, temp_avro_file):
        """Test filtering with AND predicate (multiple conditions)."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter((pl.col("id") > 2) & (pl.col("id") < 5))
            .collect()
        )

        # Should match id=3 and id=4
        assert df.height == 2, f"Expected 2 rows, got {df.height}"
        assert df["id"].to_list() == [3, 4]

    def test_filter_or_predicate(self, temp_avro_file):
        """Test filtering with OR predicate."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter((pl.col("id") == 1) | (pl.col("id") == 5))
            .collect()
        )

        # Should match id=1 and id=5
        assert df.height == 2, f"Expected 2 rows, got {df.height}"
        assert df["id"].to_list() == [1, 5]

    def test_filter_not_predicate(self, temp_avro_file):
        """Test filtering with NOT predicate."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter(~(pl.col("id") == 3))
            .collect()
        )

        # Should match all except id=3
        assert df.height == 4, f"Expected 4 rows, got {df.height}"
        assert 3 not in df["id"].to_list()

    def test_filter_complex_combination(self, temp_avro_file):
        """Test filtering with complex combination of AND, OR, NOT."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter(((pl.col("id") > 2) & (pl.col("id") < 5)) | (pl.col("name") == "Alice"))
            .collect()
        )

        # Should match: id=3, id=4, or name="Alice" (id=1)
        assert df.height == 3, f"Expected 3 rows, got {df.height}"
        ids = df["id"].to_list()
        assert 1 in ids  # Alice
        assert 3 in ids
        assert 4 in ids

    def test_filter_string_contains(self, temp_avro_file):
        """Test filtering with string contains predicate."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter(pl.col("name").str.contains("a"))
            .collect()
        )

        # Should match names containing 'a': Diana, Eva (case-sensitive)
        # Alice has 'A' not 'a', Charlie has 'a'
        names = df["name"].to_list()
        assert all("a" in name for name in names), f"All names should contain 'a': {names}"

    def test_filter_in_list(self, temp_avro_file):
        """Test filtering with IN list predicate."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter(pl.col("id").is_in([1, 3, 5]))
            .collect()
        )

        # Should match id in [1, 3, 5]
        assert df.height == 3, f"Expected 3 rows, got {df.height}"
        assert set(df["id"].to_list()) == {1, 3, 5}

    def test_filter_between(self, temp_avro_file):
        """Test filtering with BETWEEN predicate."""
        df = (
            jetliner.scan(temp_avro_file)
            .filter(pl.col("id").is_between(2, 4))
            .collect()
        )

        # Should match id between 2 and 4 (inclusive)
        assert df.height == 3, f"Expected 3 rows, got {df.height}"
        assert df["id"].to_list() == [2, 3, 4]


class TestNullHandlingInPredicates:
    """Tests for null handling in predicates.

    Coverage gap: Tests for null handling in predicates were missing.
    """

    def test_filter_is_null(self, tmp_path):
        """Test filtering for null values."""
        import fastavro

        schema = {
            "type": "record",
            "name": "NullableRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": ["null", "string"]},
            ],
        }

        records = [
            {"id": 1, "value": "hello"},
            {"id": 2, "value": None},
            {"id": 3, "value": "world"},
            {"id": 4, "value": None},
            {"id": 5, "value": "test"},
        ]

        avro_path = tmp_path / "nullable.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Filter for null values
        df = (
            jetliner.scan(str(avro_path))
            .filter(pl.col("value").is_null())
            .collect()
        )

        assert df.height == 2, f"Expected 2 null rows, got {df.height}"
        assert df["id"].to_list() == [2, 4]

    def test_filter_is_not_null(self, tmp_path):
        """Test filtering for non-null values."""
        import fastavro

        schema = {
            "type": "record",
            "name": "NullableRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": ["null", "string"]},
            ],
        }

        records = [
            {"id": 1, "value": "hello"},
            {"id": 2, "value": None},
            {"id": 3, "value": "world"},
            {"id": 4, "value": None},
            {"id": 5, "value": "test"},
        ]

        avro_path = tmp_path / "nullable.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Filter for non-null values
        df = (
            jetliner.scan(str(avro_path))
            .filter(pl.col("value").is_not_null())
            .collect()
        )

        assert df.height == 3, f"Expected 3 non-null rows, got {df.height}"
        assert df["id"].to_list() == [1, 3, 5]

    def test_filter_null_with_and(self, tmp_path):
        """Test combining null check with other predicates."""
        import fastavro

        schema = {
            "type": "record",
            "name": "NullableRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": ["null", "string"]},
            ],
        }

        records = [
            {"id": 1, "value": "hello"},
            {"id": 2, "value": None},
            {"id": 3, "value": "world"},
            {"id": 4, "value": None},
            {"id": 5, "value": "test"},
        ]

        avro_path = tmp_path / "nullable.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Filter for non-null values with id > 2
        df = (
            jetliner.scan(str(avro_path))
            .filter((pl.col("value").is_not_null()) & (pl.col("id") > 2))
            .collect()
        )

        assert df.height == 2, f"Expected 2 rows, got {df.height}"
        assert df["id"].to_list() == [3, 5]
