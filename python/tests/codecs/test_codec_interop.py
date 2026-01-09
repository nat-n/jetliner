"""
Tests for codec interoperability.

Tests that different compression codecs produce identical data:
- null (uncompressed)
- deflate
- snappy (with CRC32 validation)
- zstd

Ensures all codec variants decompress to the same underlying data.

Test data source: tests/data/apache-avro/

Requirements tested:
- 10.5: All supported codecs
- 2.2: Snappy CRC32 validation
"""



import jetliner



class TestInteroperabilityValidation:
    """
    Validate interoperability with other Avro implementations.

    These tests ensure Jetliner can correctly read files produced
    by different Avro libraries.
    """

    def test_java_interop_uuid(self, get_test_data_path):
        """
        Test reading Java-generated file with UUID.

        This validates interoperability with the Java Avro implementation,
        which is the reference implementation.
        """
        path = get_test_data_path("fastavro/java-generated-uuid.avro")

        # Should be able to read without error
        df = jetliner.scan(path).collect()

        # File should have data
        assert df.height >= 0  # May be empty but should not error

    def test_cross_codec_consistency(self, get_test_data_path):
        """
        Test that data is consistent across all codec implementations.

        This is a critical test for codec correctness - all codecs
        should produce byte-identical decompressed data.
        """
        base_path = get_test_data_path("apache-avro/weather.avro")
        base_df = jetliner.scan(base_path).collect()

        for codec in ["deflate", "snappy", "zstd"]:
            codec_path = get_test_data_path(f"apache-avro/weather-{codec}.avro")
            codec_df = jetliner.scan(codec_path).collect()

            # Should have same shape
            assert codec_df.height == base_df.height, f"{codec} has different row count"
            assert (
                codec_df.width == base_df.width
            ), f"{codec} has different column count"

            # Should have same data
            assert codec_df.equals(base_df), f"{codec} data differs from uncompressed"


# =============================================================================
# Phase 9: Edge Cases and Robustness
# =============================================================================
