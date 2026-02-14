"""
Interoperability tests with official Apache Avro test files.

Tests Jetliner's ability to read files created by Apache Avro (Java, Python, C++)
implementations. Uses the standard Apache Avro weather test files.

Test data source: tests/data/apache-avro/

Requirements tested:
- 10.1: Apache Avro interoperability
- 10.5: All supported codecs (null, deflate, snappy, zstd)
"""


import polars as pl
import pytest

import jetliner



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

    def test_read_weather_uncompressed(self, get_test_data_path):
        """Test reading uncompressed weather.avro file."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.AvroReader(path) as reader:
            dfs = list(reader)
            assert len(dfs) > 0, "Should yield at least one DataFrame"

            df = pl.concat(dfs)

            # Validate schema
            assert "station" in df.columns
            assert "time" in df.columns
            assert "temp" in df.columns

            # Validate we read records
            assert df.height > 0, "Should have at least one record"

    def test_read_weather_deflate(self, get_test_data_path):
        """Test reading deflate-compressed weather file."""
        path = get_test_data_path("apache-avro/weather-deflate.avro")

        with jetliner.AvroReader(path) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)

            assert df.height > 0
            assert "station" in df.columns
            assert "time" in df.columns
            assert "temp" in df.columns

    def test_read_weather_snappy(self, get_test_data_path):
        """Test reading snappy-compressed weather file with CRC32 validation."""
        path = get_test_data_path("apache-avro/weather-snappy.avro")

        with jetliner.AvroReader(path) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)

            assert df.height > 0
            assert "station" in df.columns

    def test_read_weather_zstd(self, get_test_data_path):
        """Test reading zstd-compressed weather file."""
        path = get_test_data_path("apache-avro/weather-zstd.avro")

        with jetliner.AvroReader(path) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)

            assert df.height > 0
            assert "station" in df.columns

    def test_read_weather_sorted(self, get_test_data_path):
        """Test reading sorted weather file."""
        path = get_test_data_path("apache-avro/weather-sorted.avro")

        with jetliner.AvroReader(path) as reader:
            dfs = list(reader)
            df = pl.concat(dfs)

            assert df.height > 0
            assert "station" in df.columns

    def test_all_codecs_produce_same_data(self, get_test_data_path):
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
            with jetliner.AvroReader(path) as reader:
                dfs = list(reader)
                df = pl.concat(dfs)
                dataframes.append((variant, df))

        # All should have same record count
        counts = [df.height for _, df in dataframes]
        assert (
            len(set(counts)) == 1
        ), f"All variants should have same record count: {dict(zip(variants, counts))}"

        # All should have same data (compare first record)
        first_records = []
        for variant, df in dataframes:
            first_row = df.head(1).to_dicts()[0]
            first_records.append((variant, first_row))

        baseline = first_records[0][1]
        for variant, record in first_records[1:]:
            assert record == baseline, f"{variant} differs from baseline"



class TestApacheAvroWeatherScan:
    """Test scan_avro() API with Apache Avro weather files."""

    def test_scan_weather_returns_lazyframe(self, get_test_data_path):
        """Test that scan_avro() returns a LazyFrame."""
        path = get_test_data_path("apache-avro/weather.avro")
        lf = jetliner.scan_avro(path)

        assert isinstance(lf, pl.LazyFrame)

    def test_scan_weather_collect(self, get_test_data_path):
        """Test collecting scanned weather file."""
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan_avro(path).collect()

        assert isinstance(df, pl.DataFrame)
        assert df.height > 0
        assert "station" in df.columns

    def test_scan_weather_projection(self, get_test_data_path):
        """Test projection pushdown with weather file."""
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan_avro(path).select(["station", "temp"]).collect()

        assert df.width == 2
        assert "station" in df.columns
        assert "temp" in df.columns
        assert "time" not in df.columns

    def test_scan_weather_filter(self, get_test_data_path):
        """Test predicate pushdown with weather file."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Get all data first to know what to filter
        all_df = jetliner.scan_avro(path).collect()

        # Filter for specific station
        if all_df.height > 0:
            first_station = all_df["station"][0]
            filtered_df = (
                jetliner.scan_avro(path).filter(pl.col("station") == first_station).collect()
            )

            # All records should have the filtered station
            assert (filtered_df["station"] == first_station).all()

    def test_scan_weather_head(self, get_test_data_path):
        """Test early stopping with weather file."""
        path = get_test_data_path("apache-avro/weather.avro")
        df = jetliner.scan_avro(path).head(2).collect()

        assert df.height <= 2

    @pytest.mark.parametrize("codec", ["deflate", "snappy", "zstd"])
    def test_scan_all_codecs(self, get_test_data_path, codec):
        """Test scan_avro() with all codec variants."""
        path = get_test_data_path(f"apache-avro/weather-{codec}.avro")
        df = jetliner.scan_avro(path).collect()

        assert df.height > 0
        assert "station" in df.columns

    def test_scan_snappy_codec(self, get_test_data_path):
        """Test scan_avro() with snappy codec."""
        path = get_test_data_path("apache-avro/weather-snappy.avro")
        df = jetliner.scan_avro(path).collect()

        assert df.height > 0
        assert "station" in df.columns


# =============================================================================
# Phase 2: fastavro Edge Case Files
# =============================================================================
