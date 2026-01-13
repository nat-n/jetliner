"""
Tests for edge case values in Avro files.

Tests handling of boundary values and special cases:
- Integer boundaries (int32/int64 max/min)
- Float special values (NaN, Infinity, -Infinity)
- Empty/long strings and bytes
- Unicode edge cases (emoji, RTL, combining characters, surrogates)
- Boolean values
- Multiple records with variations

Test data source: tests/data/edge-cases/edge-cases.avro

Requirements tested:
- 1.4: Primitive type edge cases
- 1.5: Complex type edge cases
- String encoding edge cases
"""

import math

import polars as pl
import pytest

import jetliner


class TestEdgeCaseValues:
    """
    Test reading Avro files with edge case values.

    These tests verify correct handling of:
    - Max/min int32, int64 values
    - NaN, Infinity, -Infinity for floats
    - Empty strings, empty bytes, empty arrays, empty maps
    - Very long strings (> 64KB)
    - Unicode edge cases (emoji, RTL, combining characters)

    Requirements tested: 1.4, 1.5 (primitive and complex types)
    """

    def test_read_edge_case_file(self, get_test_data_path):
        """Test that edge case file can be read without errors."""
        path = get_test_data_path("edge-cases/edge-cases.avro")

        with jetliner.open(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)
            assert df.height == 3, "Should have 3 records"

    def test_int32_boundary_values(self, get_test_data_path):
        """Test int32 max/min boundary values are read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        # First record should have max/min values
        first_row = df.head(1)

        # Max int32: 2^31 - 1 = 2147483647
        assert first_row["int32_max"][0] == 2147483647, "int32_max should be 2147483647"

        # Min int32: -2^31 = -2147483648
        assert (
            first_row["int32_min"][0] == -2147483648
        ), "int32_min should be -2147483648"

        # Zero
        assert first_row["int32_zero"][0] == 0, "int32_zero should be 0"

    def test_int64_boundary_values(self, get_test_data_path):
        """Test int64 max/min boundary values are read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)

        # Max int64: 2^63 - 1 = 9223372036854775807
        assert (
            first_row["int64_max"][0] == 9223372036854775807
        ), "int64_max should be 9223372036854775807"

        # Min int64: -2^63 = -9223372036854775808
        assert (
            first_row["int64_min"][0] == -9223372036854775808
        ), "int64_min should be -9223372036854775808"

        # Zero
        assert first_row["int64_zero"][0] == 0, "int64_zero should be 0"

    def test_float_special_values(self, get_test_data_path):
        """Test float NaN, Infinity, -Infinity are read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)

        # NaN
        assert math.isnan(first_row["float_nan"][0]), "float_nan should be NaN"

        # Positive infinity
        assert (
            first_row["float_pos_inf"][0] == float("inf")
        ), "float_pos_inf should be +inf"

        # Negative infinity
        assert (
            first_row["float_neg_inf"][0] == float("-inf")
        ), "float_neg_inf should be -inf"

        # Zero
        assert first_row["float_zero"][0] == 0.0, "float_zero should be 0.0"

    def test_double_special_values(self, get_test_data_path):
        """Test double NaN, Infinity, -Infinity are read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)

        # NaN
        assert math.isnan(first_row["double_nan"][0]), "double_nan should be NaN"

        # Positive infinity
        assert (
            first_row["double_pos_inf"][0] == float("inf")
        ), "double_pos_inf should be +inf"

        # Negative infinity
        assert (
            first_row["double_neg_inf"][0] == float("-inf")
        ), "double_neg_inf should be -inf"

        # Zero
        assert first_row["double_zero"][0] == 0.0, "double_zero should be 0.0"

        # Max double
        assert (
            first_row["double_max"][0] == 1.7976931348623157e308
        ), "double_max should be max float64"

    def test_empty_string(self, get_test_data_path):
        """Test empty string is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        assert first_row["string_empty"][0] == "", "string_empty should be empty string"

    def test_long_string(self, get_test_data_path):
        """Test long string (~10KB) is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        long_str = first_row["string_long"][0]

        # Should be 10000 characters
        assert len(long_str) == 10000, f"string_long should be 10000 chars, got {len(long_str)}"
        assert long_str == "A" * 10000, "string_long should be all 'A' characters"

    def test_unicode_emoji(self, get_test_data_path):
        """Test string with emoji is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        emoji_str = first_row["string_unicode_emoji"][0]

        # Should contain emoji
        assert "👋" in emoji_str, "Should contain wave emoji"
        assert "🌍" in emoji_str, "Should contain earth emoji"
        assert "🚀" in emoji_str, "Should contain rocket emoji"

    def test_unicode_rtl(self, get_test_data_path):
        """Test string with RTL (Arabic/Hebrew) characters is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        rtl_str = first_row["string_unicode_rtl"][0]

        # Should contain Arabic and Hebrew
        assert "مرحبا" in rtl_str, "Should contain Arabic text"
        assert "שלום" in rtl_str, "Should contain Hebrew text"
        assert "Hello" in rtl_str, "Should contain English text"

    def test_unicode_combining_characters(self, get_test_data_path):
        """Test string with combining characters is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        combining_str = first_row["string_unicode_combining"][0]

        # Should contain combining character sequences
        # e + combining acute = é
        assert "e\u0301" in combining_str or "é" in combining_str

    def test_unicode_surrogate_pairs(self, get_test_data_path):
        """Test string with surrogate pairs (musical symbols, etc.) is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        surrogate_str = first_row["string_unicode_surrogate"][0]

        # Should contain musical symbols (require surrogate pairs in UTF-16)
        assert "𝄞" in surrogate_str, "Should contain treble clef"
        assert "🎵" in surrogate_str, "Should contain musical note"

    def test_string_with_null_character(self, get_test_data_path):
        """Test string with embedded null character is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        null_str = first_row["string_null_char"][0]

        # Should contain null character
        assert "\x00" in null_str, "Should contain null character"
        assert null_str == "before\x00after", "Should preserve text around null"

    def test_string_with_newlines(self, get_test_data_path):
        """Test string with various newline characters is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        newline_str = first_row["string_newlines"][0]

        # Should contain various newlines
        assert "\n" in newline_str, "Should contain LF"
        assert "\r" in newline_str, "Should contain CR"
        assert "line1" in newline_str and "line2" in newline_str

    def test_very_long_string(self, get_test_data_path):
        """Test very long string (~100KB) is read correctly.

        This tests the MutableBinaryViewArray handling of large strings
        that exceed the inline limit and must be stored in separate buffers.
        """
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        very_long_str = first_row["string_very_long"][0]

        # Should be 100000 characters
        assert len(very_long_str) == 100000, f"string_very_long should be 100000 chars, got {len(very_long_str)}"
        assert very_long_str == "X" * 100000, "string_very_long should be all 'X' characters"

    def test_small_string_inlined(self, get_test_data_path):
        """Test small string (<12 bytes) is read correctly.

        MutableBinaryViewArray inlines strings <= 12 bytes in the view itself.
        This tests that small strings are handled correctly.
        """
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        small_str = first_row["string_small"][0]

        assert small_str == "tiny", "string_small should be 'tiny'"
        assert len(small_str) == 4, "string_small should be 4 bytes"

    def test_string_exactly_12_bytes(self, get_test_data_path):
        """Test string exactly 12 bytes is read correctly.

        This is the boundary case for MutableBinaryViewArray inline storage.
        Strings <= 12 bytes are inlined, > 12 bytes go to buffer.
        """
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        boundary_str = first_row["string_exactly_12"][0]

        assert boundary_str == "exactly12byt", "string_exactly_12 should be 'exactly12byt'"
        assert len(boundary_str) == 12, "string_exactly_12 should be exactly 12 bytes"

    def test_string_13_bytes(self, get_test_data_path):
        """Test string 13 bytes is read correctly.

        This is just over the inline limit for MutableBinaryViewArray.
        Strings > 12 bytes must be stored in separate buffers.
        """
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        over_limit_str = first_row["string_13_bytes"][0]

        assert over_limit_str == "thirteen byte", "string_13_bytes should be 'thirteen byte'"
        assert len(over_limit_str) == 13, "string_13_bytes should be 13 bytes"

    def test_mixed_string_sizes(self, get_test_data_path):
        """Test that mixed small and large strings are all read correctly.

        This verifies the MutableBinaryViewArray handles the mix of:
        - Inlined small strings (in view)
        - Buffer-stored large strings
        """
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)

        # Small (inlined)
        assert first_row["string_small"][0] == "tiny"
        assert first_row["string_exactly_12"][0] == "exactly12byt"

        # Large (buffered)
        assert first_row["string_13_bytes"][0] == "thirteen byte"
        assert len(first_row["string_long"][0]) == 10000
        assert len(first_row["string_very_long"][0]) == 100000

    def test_empty_bytes(self, get_test_data_path):
        """Test empty bytes is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        empty_bytes = first_row["bytes_empty"][0]

        assert len(empty_bytes) == 0, "bytes_empty should be empty"

    def test_long_bytes(self, get_test_data_path):
        """Test long bytes (~10KB) is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        long_bytes = first_row["bytes_long"][0]

        # Should be 10000 bytes
        assert (
            len(long_bytes) == 10000
        ), f"bytes_long should be 10000 bytes, got {len(long_bytes)}"

        # Verify pattern (i % 256 for each byte)
        for i in range(min(100, len(long_bytes))):  # Check first 100 bytes
            assert long_bytes[i] == i % 256, f"Byte {i} should be {i % 256}"

    def test_bytes_all_zeros(self, get_test_data_path):
        """Test bytes with all zeros is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        zero_bytes = first_row["bytes_all_zeros"][0]

        assert len(zero_bytes) == 100, "bytes_all_zeros should be 100 bytes"
        assert all(b == 0 for b in zero_bytes), "All bytes should be zero"

    def test_bytes_all_ones(self, get_test_data_path):
        """Test bytes with all 0xFF is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        ones_bytes = first_row["bytes_all_ones"][0]

        assert len(ones_bytes) == 100, "bytes_all_ones should be 100 bytes"
        assert all(b == 0xFF for b in ones_bytes), "All bytes should be 0xFF"

    def test_empty_array(self, get_test_data_path):
        """Test empty array is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        empty_array = first_row["array_empty"][0]

        assert empty_array.len() == 0, "array_empty should be empty"

    def test_single_element_array(self, get_test_data_path):
        """Test single element array is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        single_array = first_row["array_single"][0]

        assert single_array.len() == 1, "array_single should have 1 element"
        assert single_array[0] == 42, "array_single[0] should be 42"

    def test_large_array(self, get_test_data_path):
        """Test large array (1000 elements) is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        large_array = first_row["array_large"][0]

        assert large_array.len() == 1000, f"array_large should have 1000 elements, got {large_array.len()}"
        # Verify first and last elements
        assert large_array[0] == 0, "First element should be 0"
        assert large_array[999] == 999, "Last element should be 999"

    def test_array_of_strings(self, get_test_data_path):
        """Test array of strings is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        string_array = first_row["array_strings"][0]

        assert string_array.len() == 3, "array_strings should have 3 elements"
        assert string_array[0] == "hello"
        assert string_array[1] == "world"
        assert string_array[2] == "test"

    def test_empty_map(self, get_test_data_path):
        """Test empty map is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        empty_map = first_row["map_empty"][0]

        assert empty_map.len() == 0, "map_empty should be empty"

    def test_single_entry_map(self, get_test_data_path):
        """Test single entry map is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        single_map = first_row["map_single"][0]

        assert single_map.len() == 1, "map_single should have 1 entry"
        # Maps are stored as List<Struct{key, value}>
        entry = single_map[0]
        assert entry["key"] == "key1"
        assert entry["value"] == "value1"

    def test_map_with_unicode_keys(self, get_test_data_path):
        """Test map with unicode keys is read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)
        unicode_map = first_row["map_unicode_keys"][0]

        assert unicode_map.len() == 3, "map_unicode_keys should have 3 entries"
        # Convert to dict for easier checking
        map_dict = {entry["key"]: entry["value"] for entry in unicode_map}
        assert "🔑" in map_dict, "Should have emoji key"
        assert "مفتاح" in map_dict, "Should have Arabic key"
        assert "键" in map_dict, "Should have Chinese key"

    def test_boolean_values(self, get_test_data_path):
        """Test boolean true/false values are read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        first_row = df.head(1)

        assert first_row["bool_true"][0] is True, "bool_true should be True"
        assert first_row["bool_false"][0] is False, "bool_false should be False"

    def test_edge_case_scan_api(self, get_test_data_path):
        """Test scan() API works with edge case file."""
        path = get_test_data_path("edge-cases/edge-cases.avro")

        lf = jetliner.scan(path)
        assert isinstance(lf, pl.LazyFrame)

        df = lf.collect()
        assert df.height == 3
        assert df.width == 45  # 45 fields in schema (including arrays and maps)

    def test_edge_case_projection(self, get_test_data_path):
        """Test projection works with edge case file."""
        path = get_test_data_path("edge-cases/edge-cases.avro")

        # Select only integer columns
        df = (
            jetliner.scan(path)
            .select(["int32_max", "int64_max", "string_unicode_emoji"])
            .collect()
        )

        assert df.width == 3
        assert "int32_max" in df.columns
        assert "int64_max" in df.columns
        assert "string_unicode_emoji" in df.columns

    def test_multiple_records_variation(self, get_test_data_path):
        """Test that multiple records with variations are read correctly."""
        path = get_test_data_path("edge-cases/edge-cases.avro")
        df = jetliner.scan(path).collect()

        # Should have 3 records with different values
        assert df.height == 3

        # Second record should have different int values
        second_row = df.row(1, named=True)
        assert second_row["int32_max"] == 1, "Second record int32_max should be 1"
        assert second_row["int32_min"] == -1, "Second record int32_min should be -1"

        # Third record should have different values too
        third_row = df.row(2, named=True)
        assert (
            third_row["int32_max"] == 2147483646
        ), "Third record int32_max should be 2147483646"


# =============================================================================
# Parametrized Edge Case Tests
# =============================================================================


@pytest.mark.parametrize(
    "filename",
    [
        "edge-cases.avro",
    ],
)
def test_edge_case_file_readable(filename, get_test_data_path):
    """Test that edge case test file is readable."""
    path = get_test_data_path(f"edge-cases/{filename}")

    df = jetliner.scan(path).collect()
    assert df.height > 0
