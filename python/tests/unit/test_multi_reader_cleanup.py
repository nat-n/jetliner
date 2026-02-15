"""
Tests for MultiAvroReader resource cleanup via __exit__.

Verifies that MultiAvroReader.__exit__ eagerly releases file handles
(the current_reader inside AvroMultiStreamReader), stops further iteration,
and preserves access to post-iteration properties like errors, rows_read,
and schema.

These tests use multi-block files to ensure iteration is NOT exhausted
by a single next() call — avoiding false positives where the reader
happens to be done before __exit__ is called.
"""

import os
import tempfile

import pytest

from conftest import create_multi_block_avro_file, create_test_avro_file
from jetliner import MultiAvroReader


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def multi_block_file():
    """Create a temp Avro file with 5 blocks of 3 records each (15 total)."""
    blocks = [
        [(i * 3 + j, f"name_{i * 3 + j}") for j in range(3)] for i in range(5)
    ]
    data = create_multi_block_avro_file(blocks)
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as f:
        f.write(data)
        path = f.name
    yield path
    os.unlink(path)


@pytest.fixture
def two_multi_block_files(tmp_path):
    """Create two temp Avro files, each with multiple blocks."""
    paths = []
    for file_idx in range(2):
        blocks = [
            [(file_idx * 100 + i * 3 + j, f"f{file_idx}_{i * 3 + j}") for j in range(3)]
            for i in range(4)
        ]
        data = create_multi_block_avro_file(blocks)
        p = tmp_path / f"file_{file_idx}.avro"
        p.write_bytes(data)
        paths.append(str(p))
    return paths



# =============================================================================
# Core cleanup behavior
# =============================================================================


class TestExitStopsIteration:
    """__exit__ must prevent further iteration, even when data remains."""

    def test_exit_stops_iteration_mid_file(self, multi_block_file):
        """After __exit__ mid-iteration, no more batches should be yielded.

        Fixture has 15 records across 5 blocks. With batch_size=3, the first
        next() yields 3 records, leaving 12 un-yielded. This guards against
        false positives where the reader is already exhausted before __exit__.
        """
        reader = MultiAvroReader([multi_block_file], batch_size=3)
        reader.__enter__()
        first = next(reader)
        assert first.height == 3

        reader.__exit__(None, None, None)

        # Prove we exited early — not all records were yielded
        assert reader.rows_read < 15, (
            f"Expected early exit but all {reader.rows_read} records were yielded; "
            "test is vacuously true"
        )

        # Must yield nothing further
        remaining = list(reader)
        assert remaining == []

    def test_exit_stops_iteration_across_files(self, two_multi_block_files):
        """After __exit__ while reading first of two files, iteration stops.

        Two files × 4 blocks × 3 records = 24 total. After one next() with
        batch_size=3, only 3 records have been yielded and the second file
        hasn't been opened.
        """
        reader = MultiAvroReader(two_multi_block_files, batch_size=3)
        reader.__enter__()
        first = next(reader)
        assert first.height == 3

        reader.__exit__(None, None, None)

        # Prove we exited early — second file was never fully consumed
        assert reader.rows_read < 24, (
            f"Expected early exit but all {reader.rows_read} records were yielded; "
            "test is vacuously true"
        )

        remaining = list(reader)
        assert remaining == []

    def test_next_raises_stop_iteration_after_exit(self, multi_block_file):
        """Explicit next() after __exit__ raises StopIteration."""
        reader = MultiAvroReader([multi_block_file], batch_size=3)
        reader.__enter__()
        first = next(reader)
        assert first.height == 3
        reader.__exit__(None, None, None)

        # Prove early exit
        assert reader.rows_read < 15

        with pytest.raises(StopIteration):
            next(reader)

    def test_with_break_stops_iteration(self, multi_block_file):
        """Breaking out of a with block stops further iteration."""
        with MultiAvroReader([multi_block_file], batch_size=3) as reader:
            for df in reader:
                break  # exit after first batch

        # Prove early exit
        assert reader.rows_read < 15, (
            f"Expected early exit but all {reader.rows_read} records were yielded; "
            "test is vacuously true"
        )

        # After with-block, iteration should be dead
        remaining = list(reader)
        assert remaining == []


# =============================================================================
# is_finished reflects closed state
# =============================================================================


class TestIsFinishedAfterExit:
    """is_finished must return True after __exit__, even on early exit."""

    def test_is_finished_true_after_early_exit(self, multi_block_file):
        """is_finished returns True after __exit__ with unconsumed data."""
        with MultiAvroReader([multi_block_file], batch_size=3) as reader:
            next(reader)
            assert not reader.is_finished
            # Guard: prove data remains un-yielded
            assert reader.rows_read < 15
        # After with-block __exit__
        assert reader.is_finished

    def test_is_finished_true_after_full_iteration_and_exit(self, multi_block_file):
        """is_finished returns True after natural exhaustion + __exit__."""
        with MultiAvroReader([multi_block_file], batch_size=100_000) as reader:
            list(reader)
        assert reader.is_finished

    def test_is_finished_true_after_exit_no_iteration(self, multi_block_file):
        """is_finished returns True after __exit__ with zero iteration."""
        with MultiAvroReader([multi_block_file]) as reader:
            pass
        assert reader.is_finished


# =============================================================================
# Properties remain accessible after __exit__
# =============================================================================


class TestPropertiesAfterExit:
    """Post-exit property access must not panic or lose data."""

    def test_rows_read_preserved_after_exit(self, multi_block_file):
        """rows_read reflects what was read before __exit__."""
        with MultiAvroReader([multi_block_file], batch_size=3) as reader:
            first = next(reader)
            rows_before = reader.rows_read
            assert rows_before == first.height
            # Guard: prove this is genuinely an early exit
            assert rows_before < 15

        # rows_read should still be accessible and unchanged
        assert reader.rows_read == rows_before

    def test_schema_accessible_after_exit(self, multi_block_file):
        """schema property works after __exit__."""
        with MultiAvroReader([multi_block_file]) as reader:
            schema = reader.schema
        # Should not raise
        assert reader.schema == schema

    def test_schema_dict_accessible_after_exit(self, multi_block_file):
        """schema_dict property works after __exit__."""
        with MultiAvroReader([multi_block_file]) as reader:
            schema_dict = reader.schema_dict
        assert reader.schema_dict == schema_dict

    def test_batch_size_accessible_after_exit(self, multi_block_file):
        """batch_size property works after __exit__."""
        with MultiAvroReader([multi_block_file], batch_size=42) as reader:
            pass
        assert reader.batch_size == 42

    def test_error_count_accessible_after_exit(self, multi_block_file):
        """error_count works after __exit__ (no errors case)."""
        with MultiAvroReader([multi_block_file]) as reader:
            list(reader)
        assert reader.error_count == 0

    def test_errors_accessible_after_exit(self, multi_block_file):
        """errors list works after __exit__ (no errors case)."""
        with MultiAvroReader([multi_block_file]) as reader:
            list(reader)
        assert len(reader.errors) == 0


# =============================================================================
# Idempotency and exception safety
# =============================================================================


class TestIdempotencyAndExceptions:
    """__exit__ must be safe to call multiple times and during exceptions."""

    def test_double_exit_is_safe(self, multi_block_file):
        """Calling __exit__ twice must not panic."""
        reader = MultiAvroReader([multi_block_file], batch_size=3)
        reader.__enter__()
        next(reader)
        reader.__exit__(None, None, None)
        # Second call should be a no-op
        reader.__exit__(None, None, None)
        assert reader.is_finished

    def test_exit_on_exception_releases_resources(self, multi_block_file):
        """__exit__ releases resources even when an exception occurred."""
        try:
            with MultiAvroReader([multi_block_file], batch_size=3) as reader:
                next(reader)
                # Guard: prove data remains
                assert reader.rows_read < 15
                raise ValueError("simulated error")
        except ValueError:
            pass

        assert reader.is_finished
        remaining = list(reader)
        assert remaining == []

    def test_exit_after_full_iteration_is_safe(self, multi_block_file):
        """__exit__ after natural exhaustion is a no-op, not an error."""
        with MultiAvroReader([multi_block_file]) as reader:
            list(reader)
        # Should not raise
        assert reader.is_finished

    def test_exit_without_enter_is_safe(self, multi_block_file):
        """Calling __exit__ without __enter__ should not crash."""
        reader = MultiAvroReader([multi_block_file])
        reader.__exit__(None, None, None)
        assert reader.is_finished
