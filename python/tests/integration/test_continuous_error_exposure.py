"""
Integration tests for continuous error exposure during iteration.

Tests verify that errors are accessible at any point during iteration,
not just after exhaustion. This covers:

- AvroReader: errors accessible mid-iteration
- AvroReader: errors accessible after early break (reader still alive)
- AvroReader: errors survive after iteration exhaustion (reader dropped)
- AvroReader: error_count consistent with len(errors) at all times
- AvroReader: no errors on valid file mid-iteration
- AvroReader: error list grows monotonically during iteration
- MultiAvroReader: errors accessible mid-iteration (accumulate across files)
- MultiAvroReader: errors accessible after early break
- MultiAvroReader: errors include correct filepath per error
- MultiAvroReader: errors from earlier files persist when reading later files
"""

from pathlib import Path

import fastavro
import pytest

import jetliner

# =============================================================================
# Shared schema and helpers
# =============================================================================

SIMPLE_SCHEMA = {
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
    ],
}


def write_valid_avro(path: str | Path, records: list[dict], sync_interval: int = 100):
    """Write a valid Avro file with controlled block boundaries."""
    with open(path, "wb") as f:
        fastavro.writer(f, SIMPLE_SCHEMA, records, codec="null", sync_interval=sync_interval)


def find_sync_marker_positions(data: bytes) -> tuple[bytes, list[int]]:
    """Find the sync marker and all its positions in an Avro file.

    Returns (sync_marker, positions) where positions[0] is the header sync marker.
    """
    # Sync marker is the last 16 bytes of the header.
    # Header ends at the first sync marker occurrence.
    # The header structure: magic(4) + metadata + sync(16)
    # We know the sync marker is 16 bytes starting at some offset after metadata.
    # Easiest: read it from the header. The header sync is at a known position
    # relative to the first block.

    # Strategy: find all 16-byte sequences that repeat (the sync marker repeats
    # at end of header and after every block).
    # For null codec files with known structure, the sync marker appears first
    # in the header. We can find it by looking for the first 16-byte sequence
    # that appears again later in the file.

    # Actually, fastavro files: header ends with sync marker. Then blocks follow,
    # each ending with the same sync marker. So we scan for repeated 16-byte patterns.

    # Simpler approach: try every offset from 4..len-16 as a candidate sync marker,
    # check if it appears at least twice. The real sync marker will appear many times.
    # But this is O(n^2). For small test files it's fine, but let's be smarter.

    # The Avro spec says: header is magic(4) + file_metadata(map) + sync(16).
    # For our null-codec files, the metadata map contains avro.schema.
    # After the map, the next 16 bytes are the sync marker.

    # Let's parse minimally: skip magic, parse the map to find its end,
    # then read 16 bytes as sync marker.
    pos = 4  # skip magic

    # Parse metadata map (block-encoded map)
    while True:
        count, bytes_read = _decode_zigzag_varint(data, pos)
        pos += bytes_read
        if count == 0:
            break
        abs_count = abs(count)
        if count < 0:
            # Block size follows
            block_size, bytes_read = _decode_zigzag_varint(data, pos)
            pos += bytes_read
            pos += block_size
        else:
            for _ in range(abs_count):
                # key (string = length-prefixed bytes)
                key_len, bytes_read = _decode_zigzag_varint(data, pos)
                pos += bytes_read
                pos += key_len
                # value (bytes = length-prefixed bytes)
                val_len, bytes_read = _decode_zigzag_varint(data, pos)
                pos += bytes_read
                pos += val_len

    sync_marker = data[pos : pos + 16]
    positions = []
    # Find all occurrences
    search_pos = 0
    while True:
        idx = data.find(sync_marker, search_pos)
        if idx == -1:
            break
        positions.append(idx)
        search_pos = idx + 16

    return sync_marker, positions


def _decode_zigzag_varint(data: bytes, pos: int) -> tuple[int, int]:
    """Decode a zigzag-encoded varint from data at pos. Returns (value, bytes_consumed)."""
    result = 0
    shift = 0
    bytes_read = 0
    while True:
        b = data[pos + bytes_read]
        bytes_read += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
    # Zigzag decode
    value = (result >> 1) ^ -(result & 1)
    return value, bytes_read


def corrupt_block_data(data: bytearray, sync_positions: list[int], block_index: int):
    """Corrupt the data region of a specific block (0-indexed, excluding header sync).

    Block N's data lies between sync_positions[N] + 16 and sync_positions[N+1].
    We corrupt bytes in the middle of that region.
    """
    # sync_positions[0] = header sync
    # sync_positions[1] = end of block 0
    # sync_positions[2] = end of block 1
    # So block `block_index` data is between sync_positions[block_index] + 16
    # and sync_positions[block_index + 1]
    start = sync_positions[block_index] + 16
    end = sync_positions[block_index + 1]

    # Corrupt the middle portion of the block data
    mid = (start + end) // 2
    corrupt_len = min(20, end - mid - 1)
    for i in range(corrupt_len):
        data[mid + i] ^= 0xFF


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def corrupted_multi_block_file(tmp_path):
    """Create a multi-block Avro file with one corrupted block in the middle.

    Structure: 5+ blocks, block 2 (0-indexed) is corrupted.
    Uses small sync_interval to force many blocks.
    Returns (path, expected_error_count).
    """
    records = [{"id": i, "name": f"record_{i}"} for i in range(200)]
    path = tmp_path / "corrupted_multi_block.avro"
    write_valid_avro(path, records, sync_interval=80)

    data = bytearray(path.read_bytes())
    sync_marker, positions = find_sync_marker_positions(bytes(data))

    assert len(positions) >= 4, (
        f"Expected at least 4 sync positions (header + 3 blocks), got {len(positions)}"
    )

    # Corrupt block 2 (third block, 0-indexed)
    corrupt_block_data(data, positions, 2)
    path.write_bytes(bytes(data))

    return str(path), 1  # 1 corrupted block


@pytest.fixture
def corrupted_two_blocks_file(tmp_path):
    """Create a multi-block Avro file with two corrupted blocks.

    Blocks 1 and 3 (0-indexed) are corrupted.
    Returns (path, expected_min_error_count).
    """
    records = [{"id": i, "name": f"record_{i}"} for i in range(300)]
    path = tmp_path / "corrupted_two_blocks.avro"
    write_valid_avro(path, records, sync_interval=80)

    data = bytearray(path.read_bytes())
    sync_marker, positions = find_sync_marker_positions(bytes(data))

    assert len(positions) >= 5, (
        f"Expected at least 5 sync positions (header + 4 blocks), got {len(positions)}"
    )

    corrupt_block_data(data, positions, 1)
    corrupt_block_data(data, positions, 3)
    path.write_bytes(bytes(data))

    return str(path), 2  # 2 corrupted blocks


@pytest.fixture
def multi_file_with_errors(tmp_path):
    """Create multiple Avro files where some have corrupted blocks.

    File layout:
    - file_0.avro: valid (50 records)
    - file_1.avro: has 1 corrupted block (200 records, block 1 corrupted)
    - file_2.avro: valid (50 records)

    Returns (paths, expected_total_error_count).
    """
    paths = []

    # File 0: valid
    p0 = tmp_path / "file_0.avro"
    write_valid_avro(p0, [{"id": i, "name": f"f0_rec_{i}"} for i in range(50)])
    paths.append(str(p0))

    # File 1: corrupted
    p1 = tmp_path / "file_1.avro"
    write_valid_avro(
        p1,
        [{"id": i, "name": f"f1_rec_{i}"} for i in range(200)],
        sync_interval=80,
    )
    data = bytearray(p1.read_bytes())
    sync_marker, positions = find_sync_marker_positions(bytes(data))
    assert len(positions) >= 3, f"Expected >= 3 sync positions, got {len(positions)}"
    corrupt_block_data(data, positions, 1)
    p1.write_bytes(bytes(data))
    paths.append(str(p1))

    # File 2: valid
    p2 = tmp_path / "file_2.avro"
    write_valid_avro(p2, [{"id": i, "name": f"f2_rec_{i}"} for i in range(50)])
    paths.append(str(p2))

    return paths, 1  # 1 corrupted block total


@pytest.fixture
def multi_file_multiple_errors(tmp_path):
    """Create multiple Avro files where two files have corrupted blocks.

    File layout:
    - file_0.avro: has 1 corrupted block (200 records)
    - file_1.avro: valid (50 records)
    - file_2.avro: has 1 corrupted block (200 records)

    Returns (paths, expected_min_error_count).
    """
    paths = []

    for idx in [0, 2]:
        p = tmp_path / f"file_{idx}.avro"
        write_valid_avro(
            p,
            [{"id": i, "name": f"f{idx}_rec_{i}"} for i in range(200)],
            sync_interval=80,
        )
        data = bytearray(p.read_bytes())
        sync_marker, positions = find_sync_marker_positions(bytes(data))
        assert len(positions) >= 3
        corrupt_block_data(data, positions, 1)
        p.write_bytes(bytes(data))
        paths.append(str(p))

    # File 1: valid (inserted in the middle)
    p1 = tmp_path / "file_1.avro"
    write_valid_avro(p1, [{"id": i, "name": f"f1_rec_{i}"} for i in range(50)])
    paths.insert(1, str(p1))

    return paths, 2  # 2 corrupted blocks total


# =============================================================================
# AvroReader: continuous error exposure
# =============================================================================


class TestAvroReaderContinuousErrors:
    """Tests that AvroReader exposes errors continuously during iteration."""

    def test_errors_accessible_mid_iteration(self, corrupted_multi_block_file):
        """Errors should be accessible via .errors during iteration, not just after."""
        path, _ = corrupted_multi_block_file

        with jetliner.AvroReader(path, ignore_errors=True, batch_size=10) as reader:
            found_errors_mid_iteration = False
            for _df in reader:
                if reader.error_count > 0:
                    found_errors_mid_iteration = True
                    # Verify the errors are real error objects
                    errors = reader.errors
                    assert len(errors) > 0
                    assert hasattr(errors[0], "kind")
                    assert hasattr(errors[0], "block_index")
                    break  # We found what we needed

            assert found_errors_mid_iteration, (
                "Expected to see errors during iteration, not just after exhaustion"
            )

    def test_errors_accessible_after_early_break(self, corrupted_multi_block_file):
        """Errors should be accessible after breaking out of iteration early."""
        path, _ = corrupted_multi_block_file

        with jetliner.AvroReader(path, ignore_errors=True, batch_size=10) as reader:
            batch_count = 0
            for _df in reader:
                batch_count += 1
                if batch_count >= 5:
                    break

            # After breaking, errors should still be accessible
            # (reader is still alive, not dropped)
            errors = reader.errors
            error_count = reader.error_count
            assert error_count == len(errors)
            # We may or may not have hit the corrupted block yet,
            # but the API should work without error

    def test_errors_survive_after_exhaustion(self, corrupted_multi_block_file):
        """Errors should remain accessible after iteration is fully exhausted."""
        path, expected_min_errors = corrupted_multi_block_file

        with jetliner.AvroReader(path, ignore_errors=True, batch_size=10) as reader:
            for _df in reader:
                pass

            # After exhaustion, errors should be available
            assert reader.error_count >= expected_min_errors
            assert len(reader.errors) >= expected_min_errors

            # Calling errors multiple times should return the same result
            errors_first = reader.errors
            errors_second = reader.errors
            assert len(errors_first) == len(errors_second)

    def test_error_count_consistent_with_errors_length(
        self, corrupted_multi_block_file
    ):
        """error_count should always equal len(errors) at any point during iteration."""
        path, _ = corrupted_multi_block_file

        with jetliner.AvroReader(path, ignore_errors=True, batch_size=10) as reader:
            for _df in reader:
                assert reader.error_count == len(reader.errors), (
                    "error_count and len(errors) must be consistent at every point"
                )

            # Also after exhaustion
            assert reader.error_count == len(reader.errors)

    def test_no_errors_mid_iteration_on_valid_file(self, temp_avro_file):
        """Valid files should show zero errors at every point during iteration."""
        with jetliner.AvroReader(
            temp_avro_file, ignore_errors=True, batch_size=2
        ) as reader:
            for _df in reader:
                assert reader.error_count == 0
                assert len(reader.errors) == 0

            assert reader.error_count == 0

    def test_errors_grow_monotonically(self, corrupted_two_blocks_file):
        """Error count should never decrease during iteration."""
        path, _ = corrupted_two_blocks_file

        with jetliner.AvroReader(path, ignore_errors=True, batch_size=10) as reader:
            prev_count = 0
            for _df in reader:
                current_count = reader.error_count
                assert current_count >= prev_count, (
                    f"Error count decreased from {prev_count} to {current_count}"
                )
                prev_count = current_count

            # Final count should be at least as large
            assert reader.error_count >= prev_count

    def test_errors_have_filepath(self, corrupted_multi_block_file):
        """Each error should include the filepath of the source file."""
        path, _ = corrupted_multi_block_file

        with jetliner.AvroReader(path, ignore_errors=True, batch_size=10) as reader:
            for _df in reader:
                pass

            for err in reader.errors:
                assert err.filepath is not None, "Error should have filepath set"
                assert path in err.filepath or err.filepath in path, (
                    f"Error filepath '{err.filepath}' should reference '{path}'"
                )

    def test_errors_accessible_outside_context_manager(
        self, corrupted_multi_block_file
    ):
        """Errors should be accessible even after the context manager exits."""
        path, expected_min_errors = corrupted_multi_block_file

        reader = jetliner.AvroReader(path, ignore_errors=True, batch_size=10)
        for _df in reader:
            pass

        # Outside any context manager, after exhaustion
        assert reader.error_count >= expected_min_errors
        assert len(reader.errors) >= expected_min_errors


# =============================================================================
# MultiAvroReader: continuous error exposure
# =============================================================================


class TestMultiAvroReaderContinuousErrors:
    """Tests that MultiAvroReader exposes errors continuously during iteration."""

    def test_errors_accessible_mid_iteration(self, multi_file_with_errors):
        """Errors should be accessible during iteration of multiple files."""
        paths, _ = multi_file_with_errors

        with jetliner.MultiAvroReader(
            paths, ignore_errors=True, batch_size=10
        ) as reader:
            found_errors_mid_iteration = False
            for _df in reader:
                if reader.error_count > 0:
                    found_errors_mid_iteration = True
                    errors = reader.errors
                    assert len(errors) > 0
                    assert hasattr(errors[0], "kind")
                    break

            assert found_errors_mid_iteration, (
                "Expected to see errors during iteration of multi-file reader"
            )

    def test_errors_accessible_after_early_break(self, multi_file_with_errors):
        """Errors should be accessible after breaking out of multi-file iteration."""
        paths, _ = multi_file_with_errors

        with jetliner.MultiAvroReader(
            paths, ignore_errors=True, batch_size=10
        ) as reader:
            batch_count = 0
            for _df in reader:
                batch_count += 1
                if batch_count >= 5:
                    break

            errors = reader.errors
            error_count = reader.error_count
            assert error_count == len(errors)

    def test_errors_accumulate_across_files(self, multi_file_multiple_errors):
        """Errors from earlier files should persist when reading later files."""
        paths, expected_min_errors = multi_file_multiple_errors

        with jetliner.MultiAvroReader(
            paths, ignore_errors=True, batch_size=10
        ) as reader:
            for _df in reader:
                pass

            assert reader.error_count >= expected_min_errors, (
                f"Expected at least {expected_min_errors} errors across files, "
                f"got {reader.error_count}"
            )

    def test_error_count_consistent_with_errors_length(self, multi_file_with_errors):
        """error_count should always equal len(errors) during multi-file iteration."""
        paths, _ = multi_file_with_errors

        with jetliner.MultiAvroReader(
            paths, ignore_errors=True, batch_size=10
        ) as reader:
            for _df in reader:
                assert reader.error_count == len(reader.errors)

            assert reader.error_count == len(reader.errors)

    def test_errors_grow_monotonically(self, multi_file_multiple_errors):
        """Error count should never decrease during multi-file iteration."""
        paths, _ = multi_file_multiple_errors

        with jetliner.MultiAvroReader(
            paths, ignore_errors=True, batch_size=10
        ) as reader:
            prev_count = 0
            for _df in reader:
                current_count = reader.error_count
                assert current_count >= prev_count, (
                    f"Error count decreased from {prev_count} to {current_count}"
                )
                prev_count = current_count

    def test_errors_include_correct_filepath(self, multi_file_with_errors):
        """Each error should reference the correct source file path."""
        paths, _ = multi_file_with_errors
        # paths[1] is the corrupted file

        with jetliner.MultiAvroReader(
            paths, ignore_errors=True, batch_size=10
        ) as reader:
            for _df in reader:
                pass

            assert reader.error_count >= 1
            for err in reader.errors:
                assert err.filepath is not None, "Error should have filepath"
                # The error should reference the corrupted file (paths[1])
                assert paths[1] in err.filepath or err.filepath in paths[1], (
                    f"Error filepath '{err.filepath}' should reference corrupted file '{paths[1]}'"
                )

    def test_errors_from_multiple_files_have_distinct_filepaths(
        self, multi_file_multiple_errors
    ):
        """Errors from different files should have different filepaths."""
        paths, _ = multi_file_multiple_errors
        # paths[0] and paths[2] are corrupted

        with jetliner.MultiAvroReader(
            paths, ignore_errors=True, batch_size=10
        ) as reader:
            for _df in reader:
                pass

            assert reader.error_count >= 2
            filepaths = {err.filepath for err in reader.errors}
            assert len(filepaths) >= 2, (
                f"Expected errors from at least 2 different files, "
                f"got filepaths: {filepaths}"
            )

    def test_no_errors_mid_iteration_on_valid_files(self, tmp_path):
        """Valid multi-file reads should show zero errors throughout."""
        paths = []
        for i in range(3):
            p = tmp_path / f"valid_{i}.avro"
            write_valid_avro(p, [{"id": j, "name": f"rec_{j}"} for j in range(20)])
            paths.append(str(p))

        with jetliner.MultiAvroReader(
            paths, ignore_errors=True, batch_size=5
        ) as reader:
            for _df in reader:
                assert reader.error_count == 0
                assert len(reader.errors) == 0

            assert reader.error_count == 0
