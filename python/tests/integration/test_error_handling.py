"""
Integration tests for error handling modes with real files.

Tests cover:
- Skip mode error handling on valid files
- Strict mode error handling on valid files
- Error accumulation behavior
"""



import jetliner


class TestErrorHandlingRealFiles:
    """Test error handling with real files."""

    def test_skip_mode_on_valid_file(self, get_test_data_path):
        """Test that skip mode works on valid files without errors."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path, strict=False) as reader:
            dfs = list(reader)

            # Should read successfully
            assert len(dfs) > 0

            # Should have no errors
            assert reader.error_count == 0
            assert len(reader.errors) == 0

    def test_strict_mode_on_valid_file(self, get_test_data_path):
        """Test that strict mode works on valid files."""
        path = get_test_data_path("apache-avro/weather.avro")

        with jetliner.open(path, strict=True) as reader:
            dfs = list(reader)

            # Should read successfully
            assert len(dfs) > 0
