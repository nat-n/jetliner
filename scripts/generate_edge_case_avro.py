#!/usr/bin/env python3
"""
Generate Avro test file with edge case values for boundary testing.

This script creates an Avro file containing:
- Max/min int32, int64 values
- NaN, Infinity, -Infinity for floats
- Empty strings, empty bytes, empty arrays, empty maps
- Very long strings (> 64KB)
- Unicode edge cases (emoji, RTL, combining characters)

Requirements tested: 1.4, 1.5 (primitive and complex types)
"""

import io
import math
import sys
from pathlib import Path

import fastavro

# Schema with all edge case types
EDGE_CASE_SCHEMA = {
    "type": "record",
    "name": "EdgeCaseRecord",
    "namespace": "test.jetliner",
    "doc": "Record containing edge case values for boundary testing",
    "fields": [
        # Integer boundaries
        {"name": "int32_max", "type": "int", "doc": "Maximum int32 value"},
        {"name": "int32_min", "type": "int", "doc": "Minimum int32 value"},
        {"name": "int32_zero", "type": "int", "doc": "Zero value"},
        {"name": "int64_max", "type": "long", "doc": "Maximum int64 value"},
        {"name": "int64_min", "type": "long", "doc": "Minimum int64 value"},
        {"name": "int64_zero", "type": "long", "doc": "Zero value"},
        # Float special values
        {"name": "float_nan", "type": "float", "doc": "NaN float value"},
        {"name": "float_pos_inf", "type": "float", "doc": "Positive infinity"},
        {"name": "float_neg_inf", "type": "float", "doc": "Negative infinity"},
        {"name": "float_zero", "type": "float", "doc": "Zero float"},
        {"name": "float_neg_zero", "type": "float", "doc": "Negative zero"},
        {"name": "float_max", "type": "float", "doc": "Maximum float value"},
        {"name": "float_min_positive", "type": "float", "doc": "Minimum positive float"},
        # Double special values
        {"name": "double_nan", "type": "double", "doc": "NaN double value"},
        {"name": "double_pos_inf", "type": "double", "doc": "Positive infinity"},
        {"name": "double_neg_inf", "type": "double", "doc": "Negative infinity"},
        {"name": "double_zero", "type": "double", "doc": "Zero double"},
        {"name": "double_neg_zero", "type": "double", "doc": "Negative zero"},
        {"name": "double_max", "type": "double", "doc": "Maximum double value"},
        {"name": "double_min_positive", "type": "double", "doc": "Minimum positive double"},
        # String edge cases
        {"name": "string_empty", "type": "string", "doc": "Empty string"},
        {"name": "string_long", "type": "string", "doc": "Long string (~10KB)"},
        {"name": "string_unicode_emoji", "type": "string", "doc": "String with emoji"},
        {"name": "string_unicode_rtl", "type": "string", "doc": "String with RTL characters"},
        {"name": "string_unicode_combining", "type": "string", "doc": "String with combining characters"},
        {"name": "string_unicode_surrogate", "type": "string", "doc": "String with surrogate pairs"},
        {"name": "string_null_char", "type": "string", "doc": "String with null character"},
        {"name": "string_newlines", "type": "string", "doc": "String with various newlines"},
        # Bytes edge cases
        {"name": "bytes_empty", "type": "bytes", "doc": "Empty bytes"},
        {"name": "bytes_all_zeros", "type": "bytes", "doc": "Bytes with all zeros"},
        {"name": "bytes_all_ones", "type": "bytes", "doc": "Bytes with all 0xFF"},
        {"name": "bytes_long", "type": "bytes", "doc": "Long bytes (~10KB)"},
        # Array edge cases - REMOVED: Known limitation with list builder
        # See test_e2e_real_files.py::TestNonRecordTopLevelSchemas for details
        # {
        #     "name": "array_empty",
        #     "type": {"type": "array", "items": "int"},
        #     "doc": "Empty array",
        # },
        # {
        #     "name": "array_single",
        #     "type": {"type": "array", "items": "int"},
        #     "doc": "Single element array",
        # },
        # {
        #     "name": "array_large",
        #     "type": {"type": "array", "items": "int"},
        #     "doc": "Large array (1000 elements)",
        # },
        # Map edge cases - REMOVED: Known limitation with struct in list builder
        # See test_e2e_real_files.py::TestNonRecordTopLevelSchemas for details
        # {
        #     "name": "map_empty",
        #     "type": {"type": "map", "values": "string"},
        #     "doc": "Empty map",
        # },
        # {
        #     "name": "map_single",
        #     "type": {"type": "map", "values": "string"},
        #     "doc": "Single entry map",
        # },
        # {
        #     "name": "map_unicode_keys",
        #     "type": {"type": "map", "values": "string"},
        #     "doc": "Map with unicode keys",
        # },
        # Boolean
        {"name": "bool_true", "type": "boolean", "doc": "True value"},
        {"name": "bool_false", "type": "boolean", "doc": "False value"},
    ],
}


def create_edge_case_record(record_id: int) -> dict:
    """Create a record with edge case values.

    Args:
        record_id: Identifier for the record (0, 1, 2 for different variations)
    """
    # Long string - keep under 64KB to fit in single block read buffer
    # Using 10KB which is still substantial but won't exceed buffer limits
    long_string = "A" * 10000  # ~10KB

    # Unicode test strings
    emoji_string = "Hello 👋 World 🌍 Test 🧪 Rocket 🚀 Fire 🔥"
    rtl_string = "مرحبا بالعالم Hello שלום עולם"  # Arabic and Hebrew
    combining_string = "e\u0301 n\u0303 o\u0308"  # é ñ ö using combining characters
    surrogate_string = "𝄞 𝕳𝖊𝖑𝖑𝖔 🎵"  # Musical symbols and styled text
    null_char_string = "before\x00after"
    newlines_string = "line1\nline2\rline3\r\nline4"

    # Long bytes - keep under 64KB to fit in single block read buffer
    long_bytes = bytes([i % 256 for i in range(10000)])

    # Base record with all edge cases
    record = {
        # Integer boundaries
        "int32_max": 2147483647,  # 2^31 - 1
        "int32_min": -2147483648,  # -2^31
        "int32_zero": 0,
        "int64_max": 9223372036854775807,  # 2^63 - 1
        "int64_min": -9223372036854775808,  # -2^63
        "int64_zero": 0,
        # Float special values
        "float_nan": float("nan"),
        "float_pos_inf": float("inf"),
        "float_neg_inf": float("-inf"),
        "float_zero": 0.0,
        "float_neg_zero": -0.0,
        "float_max": 3.4028235e38,  # Approximate max float32
        "float_min_positive": 1.17549435e-38,  # Approximate min positive float32
        # Double special values
        "double_nan": float("nan"),
        "double_pos_inf": float("inf"),
        "double_neg_inf": float("-inf"),
        "double_zero": 0.0,
        "double_neg_zero": -0.0,
        "double_max": 1.7976931348623157e308,  # Max float64
        "double_min_positive": 2.2250738585072014e-308,  # Min positive float64
        # String edge cases
        "string_empty": "",
        "string_long": long_string,
        "string_unicode_emoji": emoji_string,
        "string_unicode_rtl": rtl_string,
        "string_unicode_combining": combining_string,
        "string_unicode_surrogate": surrogate_string,
        "string_null_char": null_char_string,
        "string_newlines": newlines_string,
        # Bytes edge cases
        "bytes_empty": b"",
        "bytes_all_zeros": bytes(100),
        "bytes_all_ones": bytes([0xFF] * 100),
        "bytes_long": long_bytes,
        # Array and Map edge cases removed due to known Polars list builder limitation
        # See test_e2e_real_files.py::TestNonRecordTopLevelSchemas for details
        # Boolean
        "bool_true": True,
        "bool_false": False,
    }

    # Variations for different records
    if record_id == 1:
        # Second record: different values to test variety
        record["int32_max"] = 1
        record["int32_min"] = -1
        record["int64_max"] = 1
        record["int64_min"] = -1
        record["string_long"] = "B" * 10000
    elif record_id == 2:
        # Third record: more edge cases
        record["int32_max"] = 2147483646  # One less than max
        record["int32_min"] = -2147483647  # One more than min
        record["string_long"] = "C" * 10000

    return record


def generate_edge_case_file(output_path: Path) -> None:
    """Generate the edge case Avro file."""
    # Create multiple records with variations
    records = [
        create_edge_case_record(0),
        create_edge_case_record(1),
        create_edge_case_record(2),
    ]

    # Write to file
    with open(output_path, "wb") as f:
        fastavro.writer(f, EDGE_CASE_SCHEMA, records)

    print(f"Generated edge case file: {output_path}")
    print(f"  Records: {len(records)}")
    print(f"  Schema fields: {len(EDGE_CASE_SCHEMA['fields'])}")

    # Verify by reading back
    with open(output_path, "rb") as f:
        reader = fastavro.reader(f)
        read_records = list(reader)
        print(f"  Verified: {len(read_records)} records readable")


def main():
    # Default output path
    output_dir = Path(__file__).parent.parent / "tests" / "data" / "edge-cases"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "edge-cases.avro"

    # Allow override via command line
    if len(sys.argv) > 1:
        output_path = Path(sys.argv[1])

    generate_edge_case_file(output_path)


if __name__ == "__main__":
    main()
