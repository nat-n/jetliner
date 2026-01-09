"""
Basic performance sanity checks.

These are NOT benchmarks - they simply ensure operations complete in reasonable time.
Helps catch performance regressions during development.

Tests:
- File reads complete within time bounds
- All codec variants perform acceptably
- No obvious performance bottlenecks
"""



import jetliner


class TestPerformanceSanity:
    """
    Basic performance sanity checks.

    These are not benchmarks but ensure operations complete
    in reasonable time.
    """

    def test_weather_file_reads_quickly(self, get_test_data_path):
        """Test that weather file reads in reasonable time."""
        import time

        path = get_test_data_path("apache-avro/weather.avro")

        start = time.time()
        _df = jetliner.scan(path).collect()
        elapsed = time.time() - start

        # Should complete in under 1 second for small file
        assert elapsed < 1.0, f"Read took {elapsed:.2f}s, expected < 1s"

    def test_all_codecs_read_quickly(self, get_test_data_path):
        """Test that all codec variants read in reasonable time."""
        import time

        for codec in ["", "-deflate", "-snappy", "-zstd"]:
            filename = f"weather{codec}.avro"
            path = get_test_data_path(f"apache-avro/{filename}")

            start = time.time()
            _df = jetliner.scan(path).collect()
            elapsed = time.time() - start

            # Should complete in under 1 second
            assert elapsed < 1.0, f"{filename} took {elapsed:.2f}s"
