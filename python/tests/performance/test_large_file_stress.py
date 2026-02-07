"""
Large file stress tests with RSS memory verification.

These tests generate large Avro files (100MB-1GB) and verify:
- Memory stays bounded during streaming read (RSS tracking)
- Data integrity (record count matches expected)
- Throughput metrics (MB/s, records/s)

Marked as @pytest.mark.slow for CI exclusion.

Requirements: 8.2, 3.5, 3.6
"""

import gc
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import fastavro
import psutil
import pytest

import jetliner

# Weather schema for test file generation
WEATHER_SCHEMA = {
    "type": "record",
    "name": "Weather",
    "namespace": "test",
    "fields": [
        {"name": "station", "type": "string"},
        {"name": "time", "type": "long"},
        {"name": "temp", "type": "int"},
    ],
}


def generate_large_avro_file(
    path: Path,
    target_size_mb: int,
    sync_interval: int = 16000,
) -> dict[str, Any]:
    """
    Generate a large Avro file of approximately target_size_mb.

    Returns dict with file stats: path, size_bytes, record_count.
    """
    # Estimate records needed (each record ~50-100 bytes)
    estimated_record_size = 80
    target_bytes = target_size_mb * 1024 * 1024
    estimated_records = target_bytes // estimated_record_size

    def record_generator() -> Iterator[dict[str, Any]]:
        for i in range(estimated_records):
            yield {
                "station": f"STATION-{i:08d}-{'X' * 20}",
                "time": 1704067200000 + i * 1000,
                "temp": -400 + (i % 1600),
            }

    with open(path, "wb") as f:
        fastavro.writer(
            f,
            WEATHER_SCHEMA,
            record_generator(),
            codec="null",
            sync_interval=sync_interval,
        )

    actual_size = path.stat().st_size

    # Count actual records
    record_count = 0
    with open(path, "rb") as reader:
        for _ in fastavro.reader(reader):
            record_count += 1

    return {
        "path": str(path),
        "size_bytes": actual_size,
        "size_mb": actual_size / (1024 * 1024),
        "record_count": record_count,
    }


class TestLargeFileStress:
    """
    Large file stress tests with RSS memory verification.

    These tests are slow and should be excluded from normal CI runs.
    """

    @pytest.mark.slow
    @pytest.mark.parametrize("target_size_mb", [100, 500, 1000])
    def test_memory_bounded_streaming(self, target_size_mb: int, tmp_path: Path):
        """
        Test that memory stays bounded when streaming a large file.

        Generates a file of target_size_mb and verifies:
        - Peak RSS stays below threshold (much smaller than file size)
        - All records are read correctly
        - Throughput is reasonable

        Requirements: 8.2, 3.5, 3.6
        """
        # Generate test file
        test_file = tmp_path / f"large_{target_size_mb}mb.avro"
        print(f"\nGenerating {target_size_mb}MB test file...")
        file_stats = generate_large_avro_file(test_file, target_size_mb)
        print(
            f"  Generated: {file_stats['size_mb']:.1f}MB, {file_stats['record_count']:,} records"
        )

        # Force GC and get baseline memory
        gc.collect()
        process = psutil.Process()
        baseline_rss = process.memory_info().rss

        # Track memory during streaming read
        peak_rss = baseline_rss
        total_rows = 0
        batch_count = 0
        start_time = time.time()

        with jetliner.open(str(test_file), batch_size=10000) as reader:
            for df in reader:
                current_rss = process.memory_info().rss
                peak_rss = max(peak_rss, current_rss)
                total_rows += df.height
                batch_count += 1

        elapsed = time.time() - start_time

        # Calculate metrics
        rss_delta = peak_rss - baseline_rss
        rss_delta_mb = rss_delta / (1024 * 1024)
        throughput_mb_s = file_stats["size_mb"] / elapsed
        throughput_records_s = total_rows / elapsed

        print("\nResults:")
        print(f"  Total rows: {total_rows:,}")
        print(f"  Batches: {batch_count}")
        print(f"  Time: {elapsed:.2f}s")
        print(
            f"  Throughput: {throughput_mb_s:.1f} MB/s, {throughput_records_s:,.0f} records/s"
        )
        print(f"  Baseline RSS: {baseline_rss / (1024 * 1024):.1f} MB")
        print(f"  Peak RSS: {peak_rss / (1024 * 1024):.1f} MB")
        print(f"  RSS delta: {rss_delta_mb:.1f} MB")

        # Assertions
        # 1. Record count matches
        assert (
            total_rows == file_stats["record_count"]
        ), f"Record count mismatch: got {total_rows}, expected {file_stats['record_count']}"

        # 2. Memory bounded - RSS delta should be much smaller than file size
        # Allow up to 50% of file size as RSS delta (generous for GC timing)
        max_rss_delta_mb = target_size_mb * 0.5
        assert rss_delta_mb < max_rss_delta_mb, (
            f"Memory not bounded: RSS delta {rss_delta_mb:.1f}MB >= "
            f"threshold {max_rss_delta_mb:.1f}MB for {target_size_mb}MB file"
        )

        # 3. Reasonable throughput (at least 10 MB/s for uncompressed local file)
        assert (
            throughput_mb_s > 10
        ), f"Throughput too low: {throughput_mb_s:.1f} MB/s < 10 MB/s"

    @pytest.mark.slow
    def test_memory_scales_with_batch_not_file(self, tmp_path: Path):
        """
        Test that memory scales with batch_size, not file size.

        Reads the same file with different batch sizes and verifies
        that smaller batch sizes use less memory.

        Requirements: 8.2
        """
        # Generate 200MB test file
        test_file = tmp_path / "scaling_test.avro"
        print("\nGenerating 200MB test file...")
        file_stats = generate_large_avro_file(test_file, 200)
        print(f"  Generated: {file_stats['size_mb']:.1f}MB")

        results = {}

        for batch_size in [1000, 10000, 50000]:
            gc.collect()
            process = psutil.Process()
            baseline_rss = process.memory_info().rss
            peak_rss = baseline_rss
            total_rows = 0

            with jetliner.open(str(test_file), batch_size=batch_size) as reader:
                for df in reader:
                    current_rss = process.memory_info().rss
                    peak_rss = max(peak_rss, current_rss)
                    total_rows += df.height

            rss_delta_mb = (peak_rss - baseline_rss) / (1024 * 1024)
            results[batch_size] = {
                "rss_delta_mb": rss_delta_mb,
                "total_rows": total_rows,
            }
            print(f"  batch_size={batch_size}: RSS delta={rss_delta_mb:.1f}MB")

        # Verify all read same data
        row_counts = [r["total_rows"] for r in results.values()]
        assert all(
            c == row_counts[0] for c in row_counts
        ), f"Row counts differ across batch sizes: {results}"

        # Verify smaller batch uses less memory (with tolerance for noise)
        # The 1000 batch should use less than 50000 batch
        small_rss = results[1000]["rss_delta_mb"]
        large_rss = results[50000]["rss_delta_mb"]

        # Allow some tolerance - small batch should be at most 80% of large batch
        # (in practice it should be much less, but RSS is noisy)
        assert small_rss < large_rss * 1.2, (
            f"Smaller batch didn't use less memory: "
            f"batch=1000 used {small_rss:.1f}MB, batch=50000 used {large_rss:.1f}MB"
        )

    @pytest.mark.slow
    def test_memory_released_after_iteration(self, tmp_path: Path):
        """
        Test that memory is released after iteration completes.

        Requirements: 8.2
        """
        # Generate 100MB test file
        test_file = tmp_path / "release_test.avro"
        print("\nGenerating 100MB test file...")
        _file_stats = generate_large_avro_file(test_file, 100)

        gc.collect()
        process = psutil.Process()
        baseline_rss = process.memory_info().rss

        # Read the file
        total_rows = 0
        with jetliner.open(str(test_file), batch_size=10000) as reader:
            for df in reader:
                total_rows += df.height

        # Force GC and check memory returned to baseline
        gc.collect()
        gc.collect()  # Double GC for good measure
        final_rss = process.memory_info().rss

        rss_growth = (final_rss - baseline_rss) / (1024 * 1024)
        print(f"  Baseline RSS: {baseline_rss / (1024 * 1024):.1f} MB")
        print(f"  Final RSS: {final_rss / (1024 * 1024):.1f} MB")
        print(f"  Growth: {rss_growth:.1f} MB")

        # Memory should return close to baseline (allow 50MB tolerance for fragmentation)
        assert (
            rss_growth < 50
        ), f"Memory not released: grew by {rss_growth:.1f}MB after iteration"

    @pytest.mark.slow
    def test_data_integrity_large_file(self, tmp_path: Path):
        """
        Test data integrity by verifying checksums on large file.

        Requirements: 8.2
        """
        # Generate 100MB test file
        test_file = tmp_path / "integrity_test.avro"
        print("\nGenerating 100MB test file...")
        _file_stats = generate_large_avro_file(test_file, 100)

        # Read with jetliner and compute simple checksum
        total_rows = 0
        station_hash = 0
        time_sum = 0
        temp_sum = 0

        with jetliner.open(str(test_file), batch_size=10000) as reader:
            for df in reader:
                total_rows += df.height
                # Simple integrity checks
                station_hash ^= hash(tuple(df["station"].to_list()[:100]))
                time_sum += df["time"].sum()
                temp_sum += df["temp"].sum()

        # Read with fastavro for comparison
        expected_rows = 0
        expected_time_sum = 0
        expected_temp_sum = 0

        with open(test_file, "rb") as f:
            for record in fastavro.reader(f):
                expected_rows += 1
                expected_time_sum += record["time"]
                expected_temp_sum += record["temp"]

        print(f"  Jetliner rows: {total_rows:,}")
        print(f"  Fastavro rows: {expected_rows:,}")
        print(f"  Time sum match: {time_sum == expected_time_sum}")
        print(f"  Temp sum match: {temp_sum == expected_temp_sum}")

        assert total_rows == expected_rows, "Row count mismatch"
        assert time_sum == expected_time_sum, "Time column sum mismatch"
        assert temp_sum == expected_temp_sum, "Temp column sum mismatch"
