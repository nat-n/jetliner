"""Shared fixtures and utilities for Jetliner tests."""

from pathlib import Path
import pytest


@pytest.fixture
def test_data_path():
    """Fixture that returns a function to get test data file paths."""
    def _get_path(relative_path: str) -> str:
        """Get absolute path to test data file.

        Args:
            relative_path: Path relative to tests/data directory,
                          e.g., "apache-avro/weather.avro"

        Returns:
            Absolute path to the test data file
        """
        tests_dir = Path(__file__).parent.parent.parent / "tests" / "data"
        return str(tests_dir / relative_path)

    return _get_path


def get_test_data_path(relative_path: str) -> str:
    """Get absolute path to test data file.

    Helper function (non-fixture) for use in test classes.
    Navigate from python/tests to tests/data directory.

    Args:
        relative_path: Path relative to tests/data directory,
                      e.g., "apache-avro/weather.avro"

    Returns:
        Absolute path to the test data file
    """
    # Navigate from python/tests to tests/data
    tests_dir = Path(__file__).parent.parent.parent / "tests" / "data"
    return str(tests_dir / relative_path)
