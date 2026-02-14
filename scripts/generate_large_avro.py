#!/usr/bin/env python3
"""
Generate large Avro test file with 10K+ records spanning multiple blocks.

This script creates an Avro file for multi-block testing:
- Uses weather schema for consistency with existing tests
- Generates 10,000+ records to ensure multiple blocks
- Includes variety of record sizes to test block boundary handling
- Uses small sync_interval to guarantee multiple blocks

Requirements tested: 3.1 (block-by-block reading), 3.4 (batch size limits)
"""

import sys
from pathlib import Path

import fastavro

# Weather schema matching tests/data/apache-avro/weather.avsc
WEATHER_SCHEMA = {
    "type": "record",
    "name": "Weather",
    "namespace": "test",
    "doc": "A weather reading.",
    "fields": [
        {"name": "station", "type": "string", "order": "ignore"},
        {"name": "time", "type": "long"},
        {"name": "temp", "type": "int"},
    ],
}

# Station name prefixes for variety
STATION_PREFIXES = [
    "KORD",  # Chicago O'Hare
    "KJFK",  # JFK Airport
    "KLAX",  # Los Angeles
    "KSFO",  # San Francisco
    "KDEN",  # Denver
    "KATL",  # Atlanta
    "KDFW",  # Dallas/Fort Worth
    "KMIA",  # Miami
    "KSEA",  # Seattle
    "KBOS",  # Boston
]


def generate_weather_record(record_id: int, vary_size: bool = True) -> dict:
    """Generate a weather record with optional size variation.

    Args:
        record_id: Unique identifier for the record
        vary_size: If True, vary station name length to create different record sizes
    """
    # Base station name
    prefix = STATION_PREFIXES[record_id % len(STATION_PREFIXES)]

    if vary_size:
        # Vary station name length to create different record sizes
        # This helps test block boundary handling with varied record sizes
        extra_len = (record_id % 50) * 10  # 0 to 490 extra chars
        suffix = f"-{record_id:06d}" + ("X" * extra_len)
    else:
        suffix = f"-{record_id:06d}"

    station = f"{prefix}{suffix}"

    # Generate realistic-ish weather data
    # Time: spread across a year (in milliseconds since epoch)
    base_time = 1704067200000  # 2024-01-01 00:00:00 UTC in ms
    time_offset = record_id * 3600000  # 1 hour apart
    time = base_time + time_offset

    # Temperature: -40 to 120 F (in tenths of degrees for precision)
    temp = -400 + (record_id % 1600)  # Cycles through temperature range

    return {
        "station": station,
        "time": time,
        "temp": temp,
    }


def generate_large_file(
    output_path: Path,
    num_records: int = 10000,
    sync_interval: int = 4000,
    codec: str = "null",
    vary_sizes: bool = True,
) -> dict:
    """Generate a large Avro file with multiple blocks.

    Args:
        output_path: Path to write the Avro file
        num_records: Number of records to generate (default: 10000)
        sync_interval: Bytes between sync markers (default: 4000 for more blocks)
        codec: Compression codec (default: "null" for uncompressed)
        vary_sizes: Whether to vary record sizes (default: True)

    Returns:
        dict with file statistics
    """
    # Generate records
    records = [generate_weather_record(i, vary_sizes) for i in range(num_records)]

    # Write to file with small sync_interval to ensure multiple blocks
    with open(output_path, "wb") as f:
        fastavro.writer(
            f,
            WEATHER_SCHEMA,
            records,
            codec=codec,
            sync_interval=sync_interval,
        )

    # Get file stats
    file_size = output_path.stat().st_size

    # Count records by reading the file
    record_count = 0
    with open(output_path, "rb") as f:
        reader = fastavro.reader(f)
        # fastavro doesn't expose block count directly, but we can count records
        for _ in reader:
            record_count += 1

    # Estimate block count from file structure
    # For simplicity, we'll estimate based on file size and sync_interval
    estimated_blocks = max(1, (file_size - 200) // sync_interval)

    return {
        "file_path": str(output_path),
        "file_size": file_size,
        "num_records": record_count,
        "estimated_blocks": estimated_blocks,
        "codec": codec,
        "sync_interval": sync_interval,
    }


def main():
    # Default output path
    output_dir = Path(__file__).parent.parent / "tests" / "data" / "large"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse command line arguments
    num_records = 10000
    codec = "null"

    if len(sys.argv) > 1:
        num_records = int(sys.argv[1])
    if len(sys.argv) > 2:
        codec = sys.argv[2]

    # Generate main test file (uncompressed, varied sizes)
    output_path = output_dir / "weather-large.avro"
    stats = generate_large_file(
        output_path,
        num_records=num_records,
        sync_interval=4000,  # Small interval for more blocks
        codec=codec,
        vary_sizes=True,
    )

    print(f"Generated large test file: {stats['file_path']}")
    print(f"  Records: {stats['num_records']:,}")
    print(f"  File size: {stats['file_size']:,} bytes ({stats['file_size'] / 1024:.1f} KB)")
    print(f"  Estimated blocks: {stats['estimated_blocks']}")
    print(f"  Codec: {stats['codec']}")
    print(f"  Sync interval: {stats['sync_interval']} bytes")

    # Verify by reading back with jetliner if available
    try:
        import jetliner

        total_rows = 0
        batch_count = 0
        with jetliner.AvroReader(str(output_path)) as reader:
            for df in reader:
                total_rows += len(df)
                batch_count += 1

        print("\nVerified with jetliner:")
        print(f"  Total rows read: {total_rows:,}")
        print(f"  Batches: {batch_count}")
    except ImportError:
        print("\nNote: jetliner not available for verification")
    except Exception as e:
        print(f"\nWarning: jetliner verification failed: {e}")


if __name__ == "__main__":
    main()
