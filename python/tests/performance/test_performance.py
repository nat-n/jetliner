"""
Basic performance sanity checks.

These are NOT benchmarks - they simply ensure operations complete in reasonable time.
Helps catch performance regressions during development.

Tests:
- File reads complete within time bounds
- All codec variants perform acceptably
- Throughput meets minimum expectations
- No obvious performance bottlenecks
"""

import time

import jetliner


class TestPerformanceSanity:
    """
    Basic performance sanity checks.

    These are not benchmarks but ensure operations complete
    in reasonable time and meet minimum throughput expectations.
    """

    def test_weather_file_reads_quickly(self, get_test_data_path):
        """Test that weather file reads in reasonable time."""
        path = get_test_data_path("apache-avro/weather.avro")

        start = time.time()
        df = jetliner.scan_avro(path).collect()
        elapsed = time.time() - start

        # Should complete in under 1 second for small file
        assert elapsed < 1.0, f"Read took {elapsed:.2f}s, expected < 1s"

        # Verify we actually read data
        assert df.height == 5, f"Expected 5 records, got {df.height}"

    def test_all_codecs_read_quickly(self, get_test_data_path):
        """Test that all codec variants read in reasonable time."""
        codecs_and_expected = [
            ("weather.avro", 5),
            ("weather-deflate.avro", 5),
            ("weather-snappy.avro", 5),
            ("weather-zstd.avro", 5),
        ]

        for filename, expected_rows in codecs_and_expected:
            path = get_test_data_path(f"apache-avro/{filename}")

            start = time.time()
            df = jetliner.scan_avro(path).collect()
            elapsed = time.time() - start

            # Should complete in under 1 second
            assert elapsed < 1.0, f"{filename} took {elapsed:.2f}s"

            # Verify correct data was read
            assert df.height == expected_rows, f"{filename}: expected {expected_rows} rows, got {df.height}"

    def test_large_file_throughput(self, get_test_data_path):
        """Test that large file reads meet minimum throughput.

        The weather-large.avro file has 10,000 records.
        We expect at least 10,000 records/second throughput.
        """
        path = get_test_data_path("large/weather-large.avro")

        start = time.time()
        df = jetliner.scan_avro(path).collect()
        elapsed = time.time() - start

        # Verify we read all records
        assert df.height == 10000, f"Expected 10000 records, got {df.height}"

        # Calculate throughput
        throughput = df.height / elapsed if elapsed > 0 else float("inf")

        # Should achieve at least 10,000 records/second
        assert throughput >= 10000, f"Throughput {throughput:.0f} rec/s below minimum 10,000 rec/s"

    def test_streaming_overhead_acceptable(self, get_test_data_path):
        """Test that streaming (batch iteration) has acceptable overhead.

        Streaming should not be more than 2x slower than bulk read.
        """
        path = get_test_data_path("large/weather-large.avro")

        # Bulk read timing
        start = time.time()
        bulk_df = jetliner.scan_avro(path).collect()
        bulk_elapsed = time.time() - start

        # Streaming read timing
        start = time.time()
        with jetliner.open(path, batch_size=1000) as reader:
            streaming_dfs = list(reader)
        streaming_elapsed = time.time() - start

        # Verify same data
        streaming_df = jetliner.pl.concat(streaming_dfs)
        assert streaming_df.height == bulk_df.height

        # Streaming should not be more than 2x slower
        if bulk_elapsed > 0.01:  # Only check if bulk read took measurable time
            overhead_ratio = streaming_elapsed / bulk_elapsed
            assert overhead_ratio < 2.0, f"Streaming overhead {overhead_ratio:.1f}x exceeds 2x limit"
