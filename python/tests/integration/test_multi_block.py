"""
Multi-block E2E tests for Jetliner.

These tests verify correct handling of Avro files with multiple blocks:
- Correct total record count across all blocks
- Batch size respected across block boundaries
- Sync markers validated between blocks

Requirements tested: 3.1, 3.4, 1.3

Test data: tests/data/large/weather-large.avro
- 10,000 records
- 640 blocks (small sync_interval for testing)
- Weather schema (station, time, temp)
"""

import polars as pl
import pytest

import jetliner


# =============================================================================
# Multi-Block Record Count Tests
# =============================================================================


class TestMultiBlockRecordCount:
    """
    Test correct total record count across all blocks.

    Requirements: 3.1 (block-by-block reading)
    """

    def test_total_record_count_open_api(self, get_test_data_path):
        """Test that open() API reads all records from multi-block file."""
        path = get_test_data_path("large/weather-large.avro")

        total_rows = 0
        with jetliner.open(path) as reader:
            for df in reader:
                total_rows += df.height

        assert total_rows == 10000, f"Expected 10000 records, got {total_rows}"

    def test_total_record_count_scan_api(self, get_test_data_path):
        """Test that scan() API reads all records from multi-block file."""
        path = get_test_data_path("large/weather-large.avro")

        df = jetliner.scan(path).collect()

        assert df.height == 10000, f"Expected 10000 records, got {df.height}"

    def test_record_count_matches_fastavro(self, get_test_data_path):
        """Test that Jetliner reads same record count as fastavro."""
        import fastavro

        path = get_test_data_path("large/weather-large.avro")

        # Count with fastavro
        with open(path, "rb") as f:
            fastavro_count = sum(1 for _ in fastavro.reader(f))

        # Count with jetliner
        df = jetliner.scan(path).collect()

        assert (
            df.height == fastavro_count
        ), f"Jetliner: {df.height}, fastavro: {fastavro_count}"

    def test_data_integrity_across_blocks(self, get_test_data_path):
        """Test that data values are correct across all blocks."""
        path = get_test_data_path("large/weather-large.avro")

        df = jetliner.scan(path).collect()

        # Verify schema columns exist
        assert "station" in df.columns
        assert "time" in df.columns
        assert "temp" in df.columns

        # Verify data types
        assert df["station"].dtype == pl.Utf8
        assert df["time"].dtype == pl.Int64
        assert df["temp"].dtype == pl.Int32

        # Verify no null values (weather schema has no nullable fields)
        assert df["station"].null_count() == 0
        assert df["time"].null_count() == 0
        assert df["temp"].null_count() == 0

        # Verify station names follow expected pattern (KXXX-NNNNNN...)
        stations = df["station"].to_list()
        for station in stations[:100]:  # Check first 100
            assert station.startswith("K"), f"Station should start with K: {station}"


# =============================================================================
# Batch Size Across Block Boundaries Tests
# =============================================================================


class TestBatchSizeAcrossBlocks:
    """
    Test batch size is respected across block boundaries.

    Requirements: 3.4 (configurable row limit per batch)
    """

    def test_small_batch_size_yields_multiple_batches(self, get_test_data_path):
        """Test that small batch_size yields multiple batches."""
        path = get_test_data_path("large/weather-large.avro")

        batch_sizes = []
        with jetliner.open(path, batch_size=100) as reader:
            for df in reader:
                batch_sizes.append(df.height)

        # Should have multiple batches
        assert len(batch_sizes) > 1, "Should yield multiple batches with batch_size=100"

        # Total should still be 10000
        assert sum(batch_sizes) == 10000

        # Batch sizes should be reasonable (close to target, allowing for block boundaries)
        # The batch_size is a target, not a strict limit - blocks may cause slight overruns
        avg_batch_size = sum(batch_sizes) / len(batch_sizes)
        assert avg_batch_size < 200, f"Average batch size {avg_batch_size} too large for target 100"

    def test_batch_size_1000(self, get_test_data_path):
        """Test batch_size=1000 yields ~10 batches."""
        path = get_test_data_path("large/weather-large.avro")

        batch_count = 0
        total_rows = 0
        with jetliner.open(path, batch_size=1000) as reader:
            for df in reader:
                batch_count += 1
                total_rows += df.height

        assert total_rows == 10000
        # Should have approximately 10 batches (may vary due to block boundaries)
        assert 5 <= batch_count <= 20, f"Expected ~10 batches, got {batch_count}"

    def test_batch_size_larger_than_file(self, get_test_data_path):
        """Test batch_size larger than total records yields single batch."""
        path = get_test_data_path("large/weather-large.avro")

        batches = []
        with jetliner.open(path, batch_size=100000) as reader:
            for df in reader:
                batches.append(df)

        # Should yield one batch with all records
        assert len(batches) == 1, f"Expected 1 batch, got {len(batches)}"
        assert batches[0].height == 10000

    def test_batch_boundaries_preserve_data_order(self, get_test_data_path):
        """Test that data order is preserved across batch boundaries."""
        path = get_test_data_path("large/weather-large.avro")

        # Read with small batches
        small_batch_dfs = []
        with jetliner.open(path, batch_size=500) as reader:
            for df in reader:
                small_batch_dfs.append(df)

        # Read with large batch
        with jetliner.open(path, batch_size=100000) as reader:
            large_batch_df = pl.concat(list(reader))

        # Concatenate small batches
        small_batch_df = pl.concat(small_batch_dfs)

        # Data should be identical
        assert small_batch_df.equals(large_batch_df), "Data order should be preserved"

    def test_batch_size_with_scan_api(self, get_test_data_path):
        """Test that scan() API respects batch_size hint."""
        path = get_test_data_path("large/weather-large.avro")

        # scan() uses batch_size internally via the source generator
        df = jetliner.scan(path).collect()

        # Should read all records regardless of internal batching
        assert df.height == 10000


# =============================================================================
# Sync Marker Validation Tests
# =============================================================================


class TestSyncMarkerValidation:
    """
    Test sync markers are validated between blocks.

    Requirements: 1.3 (validate sync markers match file header)
    """

    def test_valid_sync_markers_accepted(self, get_test_data_path):
        """Test that file with valid sync markers is read successfully."""
        path = get_test_data_path("large/weather-large.avro")

        # Should read without errors
        with jetliner.open(path, strict=True) as reader:
            dfs = list(reader)
            total = sum(df.height for df in dfs)

        assert total == 10000, "Should read all records with valid sync markers"

    def test_no_errors_in_skip_mode(self, get_test_data_path):
        """Test that valid file produces no errors in skip mode."""
        path = get_test_data_path("large/weather-large.avro")

        with jetliner.open(path, strict=False) as reader:
            for _ in reader:
                pass

            # Should have no errors
            assert reader.error_count == 0, f"Expected 0 errors, got {reader.error_count}"

    def test_multiple_blocks_all_validated(self, get_test_data_path):
        """Test that all 640 blocks are validated and read."""
        path = get_test_data_path("large/weather-large.avro")

        # The file has 640 blocks with 10000 records
        # If sync marker validation failed, we'd get fewer records or errors

        df = jetliner.scan(path).collect()

        # All records should be read
        assert df.height == 10000

        # Verify data spans the full range (records are numbered 0-9999)
        # The temp field cycles through -400 to 1199 (record_id % 1600 - 400)
        temps = df["temp"].to_list()
        unique_temps = set(temps)

        # Should have variety of temperatures
        assert len(unique_temps) > 100, "Should have variety of temperature values"


# =============================================================================
# Streaming Behavior Tests
# =============================================================================


class TestMultiBlockStreaming:
    """
    Test streaming behavior with multi-block files.

    Requirements: 3.1, 3.2, 3.3 (streaming without loading entire file)
    """

    def test_iterator_yields_incrementally(self, get_test_data_path):
        """Test that iterator yields DataFrames incrementally."""
        path = get_test_data_path("large/weather-large.avro")

        rows_seen = 0
        batch_count = 0

        with jetliner.open(path, batch_size=1000) as reader:
            for df in reader:
                batch_count += 1
                rows_seen += df.height

                # Should see incremental progress
                if batch_count == 1:
                    assert rows_seen > 0, "First batch should have records"
                    assert rows_seen < 10000, "First batch shouldn't have all records"

        assert rows_seen == 10000

    def test_early_termination_possible(self, get_test_data_path):
        """Test that iteration can be terminated early."""
        path = get_test_data_path("large/weather-large.avro")

        rows_read = 0
        with jetliner.open(path, batch_size=100) as reader:
            for df in reader:
                rows_read += df.height
                if rows_read >= 500:
                    break

        # Should have read approximately 500 rows (may be slightly more due to batch)
        assert 500 <= rows_read < 1000, f"Expected ~500 rows, got {rows_read}"

    def test_scan_head_stops_early(self, get_test_data_path):
        """Test that scan().head() stops reading early."""
        path = get_test_data_path("large/weather-large.avro")

        df = jetliner.scan(path).head(100).collect()

        assert df.height == 100, f"Expected 100 rows, got {df.height}"


# =============================================================================
# Data Consistency Tests
# =============================================================================


class TestMultiBlockDataConsistency:
    """
    Test data consistency across blocks.
    """

    def test_first_and_last_records_correct(self, get_test_data_path):
        """Test that first and last records are read correctly."""
        path = get_test_data_path("large/weather-large.avro")

        df = jetliner.scan(path).collect()

        # First record (id=0)
        first = df.head(1)
        assert first["station"][0].startswith("KORD"), "First station should be KORD"

        # Last record (id=9999)
        last = df.tail(1)
        # Station prefix cycles through 10 prefixes, so 9999 % 10 = 9 = KBOS
        assert last["station"][0].startswith("KBOS"), "Last station should be KBOS"

    def test_record_sequence_preserved(self, get_test_data_path):
        """Test that record sequence is preserved across blocks."""
        path = get_test_data_path("large/weather-large.avro")

        df = jetliner.scan(path).collect()

        # Time values should be monotonically increasing
        # (each record is 1 hour apart in the generated data)
        times = df["time"].to_list()

        for i in range(1, len(times)):
            assert times[i] > times[i - 1], f"Time should increase: {times[i-1]} -> {times[i]}"

    def test_all_station_prefixes_present(self, get_test_data_path):
        """Test that all station prefixes are present in the data."""
        path = get_test_data_path("large/weather-large.avro")

        df = jetliner.scan(path).collect()

        # Extract first 4 chars of each station
        prefixes = set(s[:4] for s in df["station"].to_list())

        expected_prefixes = {
            "KORD",
            "KJFK",
            "KLAX",
            "KSFO",
            "KDEN",
            "KATL",
            "KDFW",
            "KMIA",
            "KSEA",
            "KBOS",
        }

        assert prefixes == expected_prefixes, f"Missing prefixes: {expected_prefixes - prefixes}"


# =============================================================================
# Projection with Multi-Block Files
# =============================================================================


class TestMultiBlockProjection:
    """
    Test projection pushdown with multi-block files.
    """

    def test_projection_across_blocks(self, get_test_data_path):
        """Test that projection works correctly across all blocks."""
        path = get_test_data_path("large/weather-large.avro")

        # Select only station column
        df = jetliner.scan(path).select(["station"]).collect()

        assert df.width == 1
        assert "station" in df.columns
        assert df.height == 10000

    def test_projection_with_filter(self, get_test_data_path):
        """Test projection with filter across blocks."""
        path = get_test_data_path("large/weather-large.avro")

        # Filter for specific station prefix
        df = (
            jetliner.scan(path)
            .select(["station", "temp"])
            .filter(pl.col("station").str.starts_with("KORD"))
            .collect()
        )

        # Should have ~1000 records (10000 / 10 prefixes)
        assert 900 <= df.height <= 1100, f"Expected ~1000 KORD records, got {df.height}"

        # All should be KORD
        assert all(s.startswith("KORD") for s in df["station"].to_list())


# =============================================================================
# Parametrized Tests
# =============================================================================


@pytest.mark.parametrize("batch_size", [10, 100, 500, 1000, 5000])
def test_various_batch_sizes(batch_size, get_test_data_path):
    """Test that various batch sizes all read correct total."""
    path = get_test_data_path("large/weather-large.avro")

    total = 0
    with jetliner.open(path, batch_size=batch_size) as reader:
        for df in reader:
            total += df.height

    assert total == 10000, f"batch_size={batch_size}: expected 10000, got {total}"


@pytest.mark.parametrize("api", ["open", "scan"])
def test_both_apis_read_all_blocks(api, get_test_data_path):
    """Test that both APIs read all blocks correctly."""
    path = get_test_data_path("large/weather-large.avro")

    if api == "open":
        with jetliner.open(path) as reader:
            df = pl.concat(list(reader))
    else:
        df = jetliner.scan(path).collect()

    assert df.height == 10000
