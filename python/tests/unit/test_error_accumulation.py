"""
Unit tests for error accumulation in skip mode.

Tests cover:
- Error tracking properties (errors, error_count)
- Error accumulation during reading
- Verification that valid files produce no errors
"""



import jetliner


class TestErrorAccumulation:
    """Tests for error accumulation in skip mode."""

    def test_errors_property_exists(self, temp_avro_file):
        """Test that errors property exists and returns a list."""
        with jetliner.open(temp_avro_file, strict=False) as reader:
            for _ in reader:
                pass
            errors = reader.errors
            assert isinstance(errors, list)

    def test_error_count_property(self, temp_avro_file):
        """Test that error_count property returns an integer."""
        with jetliner.open(temp_avro_file, strict=False) as reader:
            for _ in reader:
                pass
            assert isinstance(reader.error_count, int)

    def test_no_errors_on_valid_file(self, temp_avro_file):
        """Test that no errors are accumulated for valid files."""
        with jetliner.open(temp_avro_file, strict=False) as reader:
            for _ in reader:
                pass
            assert reader.error_count == 0
            assert len(reader.errors) == 0
