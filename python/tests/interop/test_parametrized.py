"""
Parametrized interoperability tests for comprehensive file coverage.

Uses pytest parametrization to test multiple Apache Avro and fastavro files
in a systematic way. Ensures AvroReader and scan_avro() APIs work with all files.

Test data sources:
- tests/data/apache-avro/: All weather file variants
- tests/data/fastavro/: All edge case files

Requirements tested:
- 10.1: Comprehensive interoperability coverage
"""


import pytest

import jetliner



@pytest.mark.parametrize(
    "filename",
    [
        "weather.avro",
        "weather-deflate.avro",
        "weather-snappy.avro",
        "weather-zstd.avro",
        "weather-sorted.avro",
    ],
)
def test_apache_avro_file_readable(filename, get_test_data_path):
    """Test that all Apache Avro test files are readable."""
    path = get_test_data_path(f"apache-avro/{filename}")

    df = jetliner.scan_avro(path).collect()
    assert df.height > 0


@pytest.mark.parametrize(
    "filename,xfail_reason",
    [
        ("no-fields.avro", "Zero-field records cannot be converted to DataFrames (Polars requires at least one column)"),
        ("null.avro", None),  # Non-record schema now supported
        ("recursive.avro", None),  # Recursive types serialized to JSON
        ("java-generated-uuid.avro", None),
        # Additional non-record top-level schemas
        ("array-toplevel.avro", "Array top-level schemas - list builder type error"),
        (
            "map-toplevel.avro",
            "Map top-level schemas - struct in list builder not supported",
        ),
        ("int-toplevel.avro", None),  # ✅ Works!
        ("string-toplevel.avro", None),  # ✅ Works!
        # Complex recursive structures
        ("tree-recursive.avro", None),  # ✅ Binary trees work!
        ("graph-recursive.avro", None),  # ✅ N-ary trees work (children as List[JSON])
    ],
)
def test_fastavro_file_readable(filename, xfail_reason, get_test_data_path):
    """Test that all fastavro test files are readable."""
    if xfail_reason:
        pytest.xfail(xfail_reason)

    path = get_test_data_path(f"fastavro/{filename}")

    # Should not raise an exception
    with jetliner.AvroReader(path) as reader:
        list(reader)
        # May have 0 records but should not crash
