#!/usr/bin/env python3
"""
Generate corrupted Avro test files for error recovery testing.

This script creates Avro files with specific corruption patterns:
- File with invalid magic bytes
- Truncated file (EOF mid-block)
- File with corrupted sync marker
- File with corrupted compressed data
- File with invalid record data

Requirements tested: 7.1 (skip bad blocks), 7.2 (skip bad records)
"""

import io
from pathlib import Path

import fastavro

# Simple weather schema for test files
WEATHER_SCHEMA = {
    "type": "record",
    "name": "Weather",
    "namespace": "test",
    "doc": "A weather reading.",
    "fields": [
        {"name": "station", "type": "string"},
        {"name": "time", "type": "long"},
        {"name": "temp", "type": "int"},
    ],
}


def generate_valid_records(count: int = 10) -> list[dict]:
    """Generate valid weather records."""
    return [
        {
            "station": f"STATION-{i:04d}",
            "time": 1704067200000 + i * 3600000,  # 2024-01-01 + i hours
            "temp": 200 + (i % 100),  # 20.0 to 29.9 degrees
        }
        for i in range(count)
    ]


def find_sync_marker_and_positions(data: bytes) -> tuple[bytes | None, list[int]]:
    """
    Find the sync marker in an Avro file and all its positions.

    Returns (sync_marker, list_of_positions) or (None, []) if not found.
    """
    # The sync marker appears at the end of the header and after each block
    # It's a 16-byte sequence that repeats throughout the file

    # Start searching after magic (4 bytes) and some metadata
    # Headers can be 100-300 bytes typically
    search_start = 100
    best_candidate = None
    best_positions = []

    # Search for the sequence with the most occurrences
    for i in range(search_start, min(len(data) - 32, 500)):
        candidate = data[i : i + 16]
        # Count occurrences of this 16-byte sequence
        positions = []
        pos = 0
        while True:
            pos = data.find(candidate, pos)
            if pos < 0:
                break
            positions.append(pos)
            pos += 16

        # Keep track of the best candidate (most occurrences)
        if len(positions) > len(best_positions):
            best_candidate = candidate
            best_positions = positions

        # If we find a sequence that appears many times, it's definitely the sync marker
        if len(positions) >= 5:
            return candidate, positions

    # Return the best we found (if any with 2+ occurrences)
    if len(best_positions) >= 2:
        return best_candidate, best_positions

    return None, []


def generate_invalid_magic(output_path: Path) -> None:
    """
    Generate a file with invalid magic bytes.

    The Avro magic bytes should be 'Obj\\x01' but we corrupt them.
    This tests: ReaderError::InvalidMagic
    """
    # Create a valid file first
    records = generate_valid_records(5)
    buffer = io.BytesIO()
    fastavro.writer(buffer, WEATHER_SCHEMA, records, codec="null")
    data = bytearray(buffer.getvalue())

    # Corrupt the magic bytes (first 4 bytes)
    # Valid: b'Obj\x01' -> Invalid: b'Bad\x00'
    data[0:4] = b"Bad\x00"

    output_path.write_bytes(bytes(data))
    print(f"Generated invalid magic file: {output_path}")
    print("  Original magic: Obj\\x01")
    print("  Corrupted to: Bad\\x00")


def generate_truncated_file(output_path: Path) -> None:
    """
    Generate a truncated file (EOF mid-block).

    This creates a file that ends in the middle of a data block,
    simulating incomplete writes or network interruptions.
    This tests: ReaderError::Parse (unexpected EOF)
    """
    # Create a valid file with multiple records
    records = generate_valid_records(30)
    buffer = io.BytesIO()
    fastavro.writer(buffer, WEATHER_SCHEMA, records, codec="null")
    data = buffer.getvalue()

    # Find sync marker to know where blocks are
    sync_marker, positions = find_sync_marker_and_positions(data)

    if sync_marker and len(positions) >= 2:
        # positions[0] is the header sync marker
        # positions[1] is after the first block
        # Truncate in the middle of the last block
        last_sync = positions[-1]
        # Truncate 50 bytes after the last sync marker (in the middle of block data)
        truncate_point = last_sync + 16 + 50
        if truncate_point >= len(data):
            # File is too small, truncate at 80%
            truncate_point = int(len(data) * 0.8)
    else:
        # Fallback: truncate at 80% of file
        truncate_point = int(len(data) * 0.8)

    truncated_data = data[:truncate_point]

    output_path.write_bytes(truncated_data)
    print(f"Generated truncated file: {output_path}")
    print(f"  Original size: {len(data)} bytes")
    print(f"  Truncated to: {len(truncated_data)} bytes")


def generate_corrupted_sync_marker(output_path: Path) -> None:
    """
    Generate a file with a corrupted sync marker.

    The sync marker is a 16-byte value that appears after each block.
    We corrupt one of the sync markers (not the header one) to test
    sync marker validation.
    This tests: ReaderError::InvalidSyncMarker
    """
    # Create a valid file with enough records to have multiple blocks
    records = generate_valid_records(100)
    buffer = io.BytesIO()
    # Use small sync_interval to ensure multiple blocks
    fastavro.writer(buffer, WEATHER_SCHEMA, records, codec="null", sync_interval=100)
    data = bytearray(buffer.getvalue())

    sync_marker, positions = find_sync_marker_and_positions(bytes(data))

    if sync_marker and len(positions) >= 3:
        # Corrupt the THIRD sync marker (positions[2])
        # This leaves the header sync marker and first block intact
        # so the file can be opened and start reading
        corrupt_pos = positions[2]
        for i in range(16):
            data[corrupt_pos + i] ^= 0xFF
        print(f"  Corrupted sync marker at offset: {corrupt_pos}")
        print(f"  Total sync markers: {len(positions)}")
    elif sync_marker and len(positions) >= 2:
        # Corrupt the second sync marker
        corrupt_pos = positions[1]
        for i in range(16):
            data[corrupt_pos + i] ^= 0xFF
        print(f"  Corrupted sync marker at offset: {corrupt_pos}")
    else:
        print("  Warning: Could not find enough sync markers")

    output_path.write_bytes(bytes(data))
    print(f"Generated corrupted sync marker file: {output_path}")
    print(f"  File size: {len(data)} bytes")


def generate_corrupted_compressed_data(output_path: Path) -> None:
    """
    Generate a file with corrupted compressed data.

    This creates a deflate-compressed file and corrupts the compressed
    block data, which should cause decompression to fail.
    This tests: CodecError::DecompressionError
    """
    # Create a valid deflate-compressed file with multiple blocks
    # Use more records to ensure larger blocks
    records = generate_valid_records(100)
    buffer = io.BytesIO()
    fastavro.writer(buffer, WEATHER_SCHEMA, records, codec="deflate", sync_interval=500)
    data = bytearray(buffer.getvalue())

    sync_marker, positions = find_sync_marker_and_positions(bytes(data))

    if sync_marker and len(positions) >= 2:
        # Corrupt data in the first block (between positions[0]+16 and positions[1])
        # positions[0] is the header sync marker, so first block is after it
        # Skip the block header (record count + size varints, ~4 bytes)
        block_data_start = positions[0] + 16 + 6  # A bit more margin for varints
        block_data_end = positions[1] - 2  # Leave margin before sync marker

        if block_data_end > block_data_start + 10:
            # Corrupt bytes in the middle of the compressed data
            corrupt_start = block_data_start + 10
            corrupt_len = min(30, block_data_end - corrupt_start)
            for i in range(corrupt_len):
                data[corrupt_start + i] ^= 0xAA
            print(f"  Corrupted bytes: {corrupt_start}-{corrupt_start + corrupt_len}")
        else:
            # Block is small, corrupt what we can
            corrupt_start = block_data_start
            corrupt_len = max(1, (block_data_end - block_data_start) // 2)
            for i in range(corrupt_len):
                data[corrupt_start + i] ^= 0xAA
            print(f"  Corrupted bytes (small block): {corrupt_start}-{corrupt_start + corrupt_len}")
    else:
        print("  Warning: Could not find sync markers")

    output_path.write_bytes(bytes(data))
    print(f"Generated corrupted compressed data file: {output_path}")
    print("  Codec: deflate")


def generate_invalid_record_data(output_path: Path) -> None:
    """
    Generate a file with invalid record data.

    This creates a file where the record data doesn't match the schema.
    We corrupt the record data bytes to create invalid varints or
    string lengths.
    This tests: DecodeError (invalid data, type mismatch)
    """
    # Create a valid uncompressed file with multiple blocks
    records = generate_valid_records(50)
    buffer = io.BytesIO()
    fastavro.writer(buffer, WEATHER_SCHEMA, records, codec="null", sync_interval=200)
    data = bytearray(buffer.getvalue())

    sync_marker, positions = find_sync_marker_and_positions(bytes(data))

    if sync_marker and len(positions) >= 2:
        # Corrupt record data in the second block
        # Block starts after first sync marker: positions[0] + 16
        # Skip block header (record count + size varints, ~4 bytes)
        record_data_start = positions[0] + 16 + 4

        # Insert invalid varint sequence (all high bits set = infinite varint)
        if record_data_start + 10 < positions[1]:
            data[record_data_start] = 0xFF
            data[record_data_start + 1] = 0xFF
            data[record_data_start + 2] = 0xFF
            data[record_data_start + 3] = 0xFF
            data[record_data_start + 4] = 0xFF
            print(f"  Corrupted at offset: {record_data_start}")
        else:
            print("  Warning: Block too small to corrupt")
    else:
        print("  Warning: Could not find sync markers")

    output_path.write_bytes(bytes(data))
    print(f"Generated invalid record data file: {output_path}")
    print("  Corruption type: invalid varint sequence")


def generate_multi_block_with_one_corrupted(output_path: Path) -> None:
    """
    Generate a multi-block file with one corrupted block in the middle.

    This tests that skip mode can recover valid data before and after
    the corrupted block.
    """
    # Create a file with many records to ensure multiple blocks
    records = generate_valid_records(300)
    buffer = io.BytesIO()
    # Use small sync_interval to force many blocks
    fastavro.writer(buffer, WEATHER_SCHEMA, records, codec="null", sync_interval=80)
    data = bytearray(buffer.getvalue())

    sync_marker, positions = find_sync_marker_and_positions(bytes(data))

    if sync_marker and len(positions) >= 6:
        # Corrupt data in a middle block (e.g., block 4)
        # positions[0] is header sync, positions[1] is after block 1, etc.
        # Block 4 data is between positions[3]+16 and positions[4]
        block_idx = 3
        block_start = positions[block_idx] + 16 + 4  # Skip sync marker and block header
        block_end = positions[block_idx + 1]

        if block_end > block_start + 10:
            corrupt_pos = (block_start + block_end) // 2
            corrupt_len = min(20, block_end - corrupt_pos - 5)
            for i in range(corrupt_len):
                data[corrupt_pos + i] ^= 0xFF

            print(f"  Corrupted block {block_idx + 1} at offset: {corrupt_pos}")
            print(f"  Total blocks: {len(positions) - 1}")  # -1 for header sync
    elif sync_marker and len(positions) >= 3:
        # Fewer blocks, corrupt block 2
        block_start = positions[1] + 16 + 4
        block_end = positions[2]
        if block_end > block_start + 10:
            corrupt_pos = (block_start + block_end) // 2
            for i in range(min(20, block_end - corrupt_pos - 5)):
                data[corrupt_pos + i] ^= 0xFF
            print(f"  Corrupted block 2 at offset: {corrupt_pos}")
            print(f"  Total blocks: {len(positions) - 1}")
    else:
        # Fallback
        corrupt_pos = len(data) // 2
        for i in range(20):
            if corrupt_pos + i < len(data):
                data[corrupt_pos + i] ^= 0xFF
        print(f"  Fallback: Corrupted bytes at offset: {corrupt_pos}")
        print(f"  Sync positions found: {len(positions)}")

    output_path.write_bytes(bytes(data))
    print(f"Generated multi-block corrupted file: {output_path}")
    print(f"  File size: {len(data)} bytes")


def main():
    # Default output directory
    output_dir = Path(__file__).parent.parent / "tests" / "data" / "corrupted"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Generating corrupted Avro test files...")
    print(f"Output directory: {output_dir}")
    print()

    # Generate each type of corrupted file
    generate_invalid_magic(output_dir / "invalid-magic.avro")
    print()

    generate_truncated_file(output_dir / "truncated.avro")
    print()

    generate_corrupted_sync_marker(output_dir / "corrupted-sync-marker.avro")
    print()

    generate_corrupted_compressed_data(output_dir / "corrupted-compressed.avro")
    print()

    generate_invalid_record_data(output_dir / "invalid-record-data.avro")
    print()

    generate_multi_block_with_one_corrupted(output_dir / "multi-block-one-corrupted.avro")
    print()

    print("Done! Generated corrupted test files:")
    for f in sorted(output_dir.glob("*.avro")):
        print(f"  - {f.name} ({f.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
