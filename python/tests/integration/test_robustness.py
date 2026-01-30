"""
Integration tests for edge cases and robustness.

Tests cover:
- Multiple reads of the same file
- Concurrent file access
- Reader reuse after exhaustion
- Edge case handling
"""


import polars as pl

import jetliner



class TestEdgeCasesAndRobustness:
    """Test edge cases and robustness with real files."""

    def test_multiple_reads_same_file(self, get_test_data_path):
        """Test reading the same file multiple times."""
        path = get_test_data_path("apache-avro/weather.avro")

        # Read multiple times
        results = []
        for _ in range(3):
            df = jetliner.scan(path).collect()
            results.append(df)

        # All reads should produce identical results
        for i, df in enumerate(results[1:], 1):
            assert df.equals(results[0]), f"Read {i} differs from first read"

    def test_concurrent_file_access(self, get_test_data_path):
        """Test that multiple readers can access files truly concurrently.

        Uses threading to verify concurrent access works correctly.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        path = get_test_data_path("apache-avro/weather.avro")
        results = []
        errors = []

        def read_file(thread_id):
            """Read the file and return the DataFrame."""
            try:
                with jetliner.open(path) as reader:
                    dfs = list(reader)
                    df = pl.concat(dfs) if dfs else pl.DataFrame()
                    return (thread_id, df)
            except Exception as e:
                return (thread_id, e)

        # Run 5 concurrent readers
        num_threads = 5
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(read_file, i) for i in range(num_threads)]
            for future in as_completed(futures):
                thread_id, result = future.result()
                if isinstance(result, Exception):
                    errors.append((thread_id, result))
                else:
                    results.append((thread_id, result))

        # All reads should succeed
        assert len(errors) == 0, f"Concurrent reads failed: {errors}"
        assert len(results) == num_threads, f"Expected {num_threads} results, got {len(results)}"

        # All results should be identical
        first_df = results[0][1]
        for thread_id, df in results[1:]:
            assert df.equals(first_df), f"Thread {thread_id} produced different result"

    def test_reader_reuse_after_exhaustion(self, get_test_data_path):
        """Test behavior when trying to reuse exhausted reader."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path) as reader:
            # Exhaust the reader
            dfs = list(reader)
            assert len(dfs) > 0

            # Reader should be finished
            assert reader.is_finished

            # Further iteration should yield nothing or raise an error
            # The reader raises JetlinerError when closed - this is acceptable behavior
            try:
                more_dfs = list(reader)
                assert len(more_dfs) == 0
            except (StopIteration, jetliner.JetlinerError):
                pass  # Both are acceptable behaviors for exhausted reader


# =============================================================================
# Phase 10: Performance Sanity Checks
# =============================================================================


class TestSyncMarkerInData:
    """Test that sync marker patterns in data don't confuse the reader.

    The Avro sync marker is a 16-byte random value used to separate blocks.
    If data bytes happen to match this pattern, the reader should not be
    confused and should read the file correctly.
    """

    def test_sync_marker_in_data(self, tmp_path):
        """Test that data containing sync marker bytes is read correctly.

        This test creates records with bytes fields that contain patterns
        similar to sync markers to ensure the reader correctly distinguishes
        between actual sync markers and data that happens to look like them.
        """
        import fastavro

        # Schema with bytes field
        schema = {
            "type": "record",
            "name": "BytesRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "data", "type": "bytes"},
            ],
        }

        # Create records with various byte patterns including:
        # - All zeros (could be confused with some markers)
        # - All 0xFF bytes
        # - Repeating patterns
        # - Random-looking bytes
        records = [
            {"id": 0, "data": bytes([0] * 16)},  # All zeros
            {"id": 1, "data": bytes([0xFF] * 16)},  # All 0xFF
            {"id": 2, "data": bytes([0x4F, 0x62, 0x6A, 0x01] * 4)},  # Avro magic-like pattern
            {"id": 3, "data": bytes(range(16))},  # Sequential bytes
            {"id": 4, "data": bytes([0xDE, 0xAD, 0xBE, 0xEF] * 4)},  # Common test pattern
            {"id": 5, "data": bytes([i ^ 0xAA for i in range(16)])},  # XOR pattern
        ]

        # Write to file
        avro_path = tmp_path / "sync_marker_test.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Read with jetliner
        df = jetliner.scan(str(avro_path)).collect()

        # Verify record count
        assert df.height == 6

        # Verify data contents
        data_col = df["data"].to_list()

        assert data_col[0] == bytes([0] * 16)
        assert data_col[1] == bytes([0xFF] * 16)
        assert data_col[2] == bytes([0x4F, 0x62, 0x6A, 0x01] * 4)
        assert data_col[3] == bytes(range(16))
        assert data_col[4] == bytes([0xDE, 0xAD, 0xBE, 0xEF] * 4)
        assert data_col[5] == bytes([i ^ 0xAA for i in range(16)])

    def test_sync_marker_in_string_data(self, tmp_path):
        """Test that strings containing binary-like patterns are read correctly."""
        import fastavro

        # Schema with string field
        schema = {
            "type": "record",
            "name": "StringRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "text", "type": "string"},
            ],
        }

        # Create records with various string patterns
        records = [
            {"id": 0, "text": "normal text"},
            {"id": 1, "text": "Obj\x01" * 4},  # Avro magic-like in string
            {"id": 2, "text": "\x00" * 16},  # Null bytes (valid UTF-8)
            {"id": 3, "text": "a" * 1000},  # Long string
            {"id": 4, "text": ""},  # Empty string
        ]

        # Write to file
        avro_path = tmp_path / "string_pattern_test.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Read with jetliner
        df = jetliner.scan(str(avro_path)).collect()

        # Verify record count
        assert df.height == 5

        # Verify string contents
        text_col = df["text"].to_list()

        assert text_col[0] == "normal text"
        assert text_col[1] == "Obj\x01" * 4
        assert text_col[2] == "\x00" * 16
        assert text_col[3] == "a" * 1000
        assert text_col[4] == ""

    def test_large_file_with_sync_marker_patterns(self, tmp_path):
        """Test a larger file with many records containing sync-marker-like patterns."""
        import fastavro

        # Schema with bytes field
        schema = {
            "type": "record",
            "name": "LargeRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "data", "type": "bytes"},
            ],
        }

        # Create many records with various patterns
        # This ensures we test across multiple blocks
        records = []
        for i in range(1000):
            # Vary the pattern based on record id
            pattern = bytes([(i + j) % 256 for j in range(32)])
            records.append({"id": i, "data": pattern})

        # Write to file
        avro_path = tmp_path / "large_sync_marker_test.avro"
        with open(avro_path, "wb") as f:
            fastavro.writer(f, schema, records)

        # Read with jetliner
        df = jetliner.scan(str(avro_path)).collect()

        # Verify record count
        assert df.height == 1000

        # Verify a sample of records
        data_col = df["data"].to_list()
        for i in [0, 100, 500, 999]:
            expected = bytes([(i + j) % 256 for j in range(32)])
            assert data_col[i] == expected, f"Record {i} data mismatch"
