//! Compression codec support for Avro blocks
//!
//! Avro supports multiple compression codecs for block data. This module
//! provides the codec abstraction and implementations.

use crate::error::CodecError;

#[cfg(feature = "snappy")]
use snap::raw::Decoder as SnappyDecoder;

#[cfg(feature = "deflate")]
use flate2::read::DeflateDecoder;

#[cfg(feature = "bzip2")]
use bzip2::read::BzDecoder;

#[cfg(feature = "xz")]
use xz2::read::XzDecoder;

#[cfg(any(
    feature = "deflate",
    feature = "zstd",
    feature = "bzip2",
    feature = "xz"
))]
use std::io::Read;

/// Compression codec used within Avro blocks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Codec {
    /// No compression (passthrough)
    #[default]
    Null,
    /// Snappy compression with Avro framing (4-byte CRC suffix)
    Snappy,
    /// Deflate (zlib) compression
    Deflate,
    /// Zstandard compression
    Zstd,
    /// Bzip2 compression
    Bzip2,
    /// XZ/LZMA compression
    Xz,
}

impl Codec {
    /// Parse a codec from its name string as found in Avro metadata.
    ///
    /// # Arguments
    /// * `name` - The codec name (e.g., "null", "snappy", "deflate")
    ///
    /// # Returns
    /// * `Ok(Codec)` - The parsed codec
    /// * `Err(CodecError)` - If the codec name is unknown
    ///
    /// # Examples
    /// ```
    /// use jetliner::codec::Codec;
    ///
    /// let codec = Codec::from_name("null").unwrap();
    /// assert_eq!(codec, Codec::Null);
    ///
    /// let err = Codec::from_name("unknown").unwrap_err();
    /// assert!(err.to_string().contains("unknown"));
    /// ```
    pub fn from_name(name: &str) -> Result<Self, CodecError> {
        match name {
            "null" => Ok(Codec::Null),
            "snappy" => Ok(Codec::Snappy),
            "deflate" => Ok(Codec::Deflate),
            "zstd" | "zstandard" => Ok(Codec::Zstd),
            "bzip2" => Ok(Codec::Bzip2),
            "xz" => Ok(Codec::Xz),
            unknown => Err(CodecError::UnsupportedCodec(format!(
                "Unknown codec '{}'. Supported codecs: null, snappy, deflate, zstd/zstandard, bzip2, xz",
                unknown
            ))),
        }
    }

    /// Get the canonical name of this codec.
    ///
    /// This returns the name as it would appear in Avro file metadata.
    pub fn name(&self) -> &'static str {
        match self {
            Codec::Null => "null",
            Codec::Snappy => "snappy",
            Codec::Deflate => "deflate",
            Codec::Zstd => "zstd",
            Codec::Bzip2 => "bzip2",
            Codec::Xz => "xz",
        }
    }

    /// Decompress data using this codec.
    ///
    /// # Arguments
    /// * `data` - The compressed data
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - The decompressed data
    /// * `Err(CodecError)` - If decompression fails
    ///
    /// # Note
    /// For the null codec, this is a passthrough that returns a copy of the input.
    /// Other codecs will be implemented in subsequent tasks.
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        match self {
            Codec::Null => Ok(data.to_vec()),
            #[cfg(feature = "snappy")]
            Codec::Snappy => decompress_snappy(data),
            #[cfg(not(feature = "snappy"))]
            Codec::Snappy => Err(CodecError::UnsupportedCodec(
                "Snappy codec not enabled. Enable the 'snappy' feature.".to_string(),
            )),
            #[cfg(feature = "deflate")]
            Codec::Deflate => decompress_deflate(data),
            #[cfg(not(feature = "deflate"))]
            Codec::Deflate => Err(CodecError::UnsupportedCodec(
                "Deflate codec not enabled. Enable the 'deflate' feature.".to_string(),
            )),
            #[cfg(feature = "zstd")]
            Codec::Zstd => decompress_zstd(data),
            #[cfg(not(feature = "zstd"))]
            Codec::Zstd => Err(CodecError::UnsupportedCodec(
                "Zstd codec not enabled. Enable the 'zstd' feature.".to_string(),
            )),
            #[cfg(feature = "bzip2")]
            Codec::Bzip2 => decompress_bzip2(data),
            #[cfg(not(feature = "bzip2"))]
            Codec::Bzip2 => Err(CodecError::UnsupportedCodec(
                "Bzip2 codec not enabled. Enable the 'bzip2' feature.".to_string(),
            )),
            #[cfg(feature = "xz")]
            Codec::Xz => decompress_xz(data),
            #[cfg(not(feature = "xz"))]
            Codec::Xz => Err(CodecError::UnsupportedCodec(
                "Xz codec not enabled. Enable the 'xz' feature.".to_string(),
            )),
        }
    }
}

/// Decompress snappy data with Avro framing.
///
/// Avro uses a custom snappy framing format where the compressed data is followed
/// by a 4-byte CRC32 checksum of the uncompressed data (big-endian).
///
/// Format: [snappy_compressed_data][4-byte CRC32 checksum]
///
/// The CRC32 checksum uses the ISO polynomial (same as zlib/gzip) and is validated
/// after decompression to detect data corruption.
///
/// Note: The Avro spec says "CRC32" (ISO polynomial), NOT "CRC32C" (Castagnoli).
/// This is important for interoperability with Apache Avro implementations.
#[cfg(feature = "snappy")]
fn decompress_snappy(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    // Avro snappy format: compressed_data + 4-byte CRC32 checksum
    // The checksum is of the uncompressed data, stored in big-endian
    const CRC_SIZE: usize = 4;

    if data.len() < CRC_SIZE {
        return Err(CodecError::DecompressionError(
            "Snappy data too short: missing CRC checksum".to_string(),
        ));
    }

    // Split off the CRC checksum (last 4 bytes)
    let compressed_data = &data[..data.len() - CRC_SIZE];
    let crc_bytes = &data[data.len() - CRC_SIZE..];
    let expected_crc = u32::from_be_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);

    // Handle empty compressed data (just the CRC)
    if compressed_data.is_empty() {
        // Validate CRC of empty data
        let actual_crc = crc32fast::hash(&[]);
        if actual_crc != expected_crc {
            return Err(CodecError::DecompressionError(format!(
                "Snappy CRC32 checksum mismatch: expected 0x{:08X}, got 0x{:08X}",
                expected_crc, actual_crc
            )));
        }
        return Ok(Vec::new());
    }

    // Decompress using snap crate
    let mut decoder = SnappyDecoder::new();
    let decompressed = decoder.decompress_vec(compressed_data).map_err(|e| {
        CodecError::DecompressionError(format!("Snappy decompression failed: {}", e))
    })?;

    // Validate CRC32 checksum of the uncompressed data
    // Note: Avro uses CRC32 (ISO polynomial), not CRC32C (Castagnoli)
    let actual_crc = crc32fast::hash(&decompressed);
    if actual_crc != expected_crc {
        return Err(CodecError::DecompressionError(format!(
            "Snappy CRC32 checksum mismatch: expected 0x{:08X}, got 0x{:08X}",
            expected_crc, actual_crc
        )));
    }

    Ok(decompressed)
}

/// Decompress deflate (raw DEFLATE) data.
///
/// Avro uses raw DEFLATE compression (RFC 1951), not zlib or gzip wrappers.
/// The flate2 crate's DeflateDecoder handles this format.
#[cfg(feature = "deflate")]
fn decompress_deflate(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    // Handle empty input
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut decoder = DeflateDecoder::new(data);
    let mut decompressed = Vec::new();

    decoder.read_to_end(&mut decompressed).map_err(|e| {
        CodecError::DecompressionError(format!("Deflate decompression failed: {}", e))
    })?;

    Ok(decompressed)
}

/// Decompress zstd (Zstandard) data.
///
/// Avro uses standard zstd compression without any additional framing.
/// The zstd crate's Decoder handles this format.
#[cfg(feature = "zstd")]
fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    // Handle empty input
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut decoder = zstd::Decoder::new(data).map_err(|e| {
        CodecError::DecompressionError(format!("Zstd decoder initialization failed: {}", e))
    })?;

    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| CodecError::DecompressionError(format!("Zstd decompression failed: {}", e)))?;

    Ok(decompressed)
}

/// Decompress bzip2 data.
///
/// Avro uses standard bzip2 compression without any additional framing.
/// The bzip2 crate's BzDecoder handles this format.
#[cfg(feature = "bzip2")]
fn decompress_bzip2(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    // Handle empty input
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut decoder = BzDecoder::new(data);
    let mut decompressed = Vec::new();

    decoder.read_to_end(&mut decompressed).map_err(|e| {
        CodecError::DecompressionError(format!("Bzip2 decompression failed: {}", e))
    })?;

    Ok(decompressed)
}

/// Decompress xz (LZMA) data.
///
/// Avro uses standard xz compression without any additional framing.
/// The xz2 crate's XzDecoder handles this format.
#[cfg(feature = "xz")]
fn decompress_xz(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    // Handle empty input
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut decoder = XzDecoder::new(data);
    let mut decompressed = Vec::new();

    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| CodecError::DecompressionError(format!("Xz decompression failed: {}", e)))?;

    Ok(decompressed)
}

impl std::fmt::Display for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_name_null() {
        let codec = Codec::from_name("null").unwrap();
        assert_eq!(codec, Codec::Null);
    }

    #[test]
    fn test_from_name_snappy() {
        let codec = Codec::from_name("snappy").unwrap();
        assert_eq!(codec, Codec::Snappy);
    }

    #[test]
    fn test_from_name_deflate() {
        let codec = Codec::from_name("deflate").unwrap();
        assert_eq!(codec, Codec::Deflate);
    }

    #[test]
    fn test_from_name_zstd() {
        let codec = Codec::from_name("zstd").unwrap();
        assert_eq!(codec, Codec::Zstd);
    }

    #[test]
    fn test_from_name_zstandard_alias() {
        // The Avro spec uses "zstandard" as the canonical name
        let codec = Codec::from_name("zstandard").unwrap();
        assert_eq!(codec, Codec::Zstd);
    }

    #[test]
    fn test_from_name_bzip2() {
        let codec = Codec::from_name("bzip2").unwrap();
        assert_eq!(codec, Codec::Bzip2);
    }

    #[test]
    fn test_from_name_xz() {
        let codec = Codec::from_name("xz").unwrap();
        assert_eq!(codec, Codec::Xz);
    }

    #[test]
    fn test_from_name_unknown() {
        let err = Codec::from_name("unknown").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown"),
            "Error should mention the unknown codec name"
        );
        assert!(msg.contains("null"), "Error should list supported codecs");
    }

    #[test]
    fn test_from_name_empty() {
        let err = Codec::from_name("").unwrap_err();
        assert!(err.to_string().contains("Unknown codec"));
    }

    #[test]
    fn test_codec_name_roundtrip() {
        let codecs = [
            Codec::Null,
            Codec::Snappy,
            Codec::Deflate,
            Codec::Zstd,
            Codec::Bzip2,
            Codec::Xz,
        ];
        for codec in codecs {
            let name = codec.name();
            let parsed = Codec::from_name(name).unwrap();
            assert_eq!(codec, parsed);
        }
    }

    #[test]
    fn test_null_decompress() {
        let data = b"hello world";
        let result = Codec::Null.decompress(data).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_null_decompress_empty() {
        let data: &[u8] = &[];
        let result = Codec::Null.decompress(data).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_default_is_null() {
        assert_eq!(Codec::default(), Codec::Null);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Codec::Null), "null");
        assert_eq!(format!("{}", Codec::Snappy), "snappy");
    }

    #[cfg(feature = "snappy")]
    mod snappy_tests {
        use super::*;

        /// Helper to create Avro-framed snappy data (compressed + 4-byte CRC32)
        /// Note: Avro uses CRC32 (ISO polynomial), not CRC32C (Castagnoli)
        fn create_avro_snappy_data(uncompressed: &[u8]) -> Vec<u8> {
            use snap::raw::Encoder;

            let mut encoder = Encoder::new();
            let compressed = encoder.compress_vec(uncompressed).unwrap();

            // Compute CRC32 of uncompressed data (big-endian)
            let crc = crc32fast::hash(uncompressed);

            let mut result = compressed;
            result.extend_from_slice(&crc.to_be_bytes());
            result
        }

        #[test]
        fn test_snappy_decompress_simple() {
            let original = b"hello world";
            let avro_snappy_data = create_avro_snappy_data(original);

            let result = Codec::Snappy.decompress(&avro_snappy_data).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_snappy_decompress_empty() {
            let original: &[u8] = b"";
            let avro_snappy_data = create_avro_snappy_data(original);

            let result = Codec::Snappy.decompress(&avro_snappy_data).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn test_snappy_decompress_larger_data() {
            // Create some repetitive data that compresses well
            let original: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
            let avro_snappy_data = create_avro_snappy_data(&original);

            let result = Codec::Snappy.decompress(&avro_snappy_data).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_snappy_decompress_binary_data() {
            // Test with binary data including null bytes
            let original: Vec<u8> = vec![0, 1, 2, 0, 255, 254, 0, 128, 127];
            let avro_snappy_data = create_avro_snappy_data(&original);

            let result = Codec::Snappy.decompress(&avro_snappy_data).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_snappy_decompress_too_short() {
            // Data shorter than CRC size (4 bytes)
            let short_data = vec![0, 1, 2];
            let err = Codec::Snappy.decompress(&short_data).unwrap_err();
            assert!(err.to_string().contains("too short"));
        }

        #[test]
        fn test_snappy_decompress_invalid_compressed_data() {
            // Invalid snappy data followed by CRC
            let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];
            let err = Codec::Snappy.decompress(&invalid_data).unwrap_err();
            assert!(
                err.to_string().contains("decompression failed")
                    || err.to_string().contains("Decompression")
            );
        }

        #[test]
        fn test_snappy_decompress_highly_compressible() {
            // Highly compressible data (all same byte)
            let original: Vec<u8> = vec![0xAB; 10000];
            let avro_snappy_data = create_avro_snappy_data(&original);

            // Verify compression actually happened
            assert!(avro_snappy_data.len() < original.len());

            let result = Codec::Snappy.decompress(&avro_snappy_data).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_snappy_crc32_validation_failure() {
            use snap::raw::Encoder;

            let original = b"hello world";
            let mut encoder = Encoder::new();
            let compressed = encoder.compress_vec(original).unwrap();

            // Create data with wrong CRC (all zeros instead of correct checksum)
            let mut bad_data = compressed;
            bad_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

            let err = Codec::Snappy.decompress(&bad_data).unwrap_err();
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("CRC32 checksum mismatch"),
                "Expected CRC mismatch error, got: {}",
                err_msg
            );
            assert!(
                err_msg.contains("expected 0x00000000"),
                "Error should show expected CRC, got: {}",
                err_msg
            );
        }

        #[test]
        fn test_snappy_crc32_validation_corrupted_data() {
            let original = b"hello world";
            let mut avro_snappy_data = create_avro_snappy_data(original);

            // Corrupt the CRC by flipping a bit in the last byte
            let len = avro_snappy_data.len();
            avro_snappy_data[len - 1] ^= 0x01;

            let err = Codec::Snappy.decompress(&avro_snappy_data).unwrap_err();
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("CRC32 checksum mismatch"),
                "Expected CRC mismatch error, got: {}",
                err_msg
            );
        }

        #[test]
        fn test_snappy_empty_data_crc_validation() {
            // Test that empty data with correct CRC passes
            // CRC32 of empty data is 0x00000000
            let empty_crc = crc32fast::hash(&[]);
            let mut data = Vec::new();
            data.extend_from_slice(&empty_crc.to_be_bytes());

            let result = Codec::Snappy.decompress(&data).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn test_snappy_empty_data_wrong_crc() {
            // Test that empty data with wrong CRC fails
            let data = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Wrong CRC for empty data

            let err = Codec::Snappy.decompress(&data).unwrap_err();
            assert!(err.to_string().contains("CRC32 checksum mismatch"));
        }
    }

    #[cfg(feature = "deflate")]
    mod deflate_tests {
        use super::*;
        use flate2::write::DeflateEncoder;
        use flate2::Compression;
        use std::io::Write;

        /// Helper to create deflate-compressed data
        fn create_deflate_data(uncompressed: &[u8]) -> Vec<u8> {
            let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(uncompressed).unwrap();
            encoder.finish().unwrap()
        }

        #[test]
        fn test_deflate_decompress_simple() {
            let original = b"hello world";
            let compressed = create_deflate_data(original);

            let result = Codec::Deflate.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_deflate_decompress_empty() {
            let original: &[u8] = b"";
            let compressed = create_deflate_data(original);

            let result = Codec::Deflate.decompress(&compressed).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn test_deflate_decompress_empty_input() {
            // Empty input (not compressed empty data)
            let result = Codec::Deflate.decompress(&[]).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn test_deflate_decompress_larger_data() {
            // Create some repetitive data that compresses well
            let original: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
            let compressed = create_deflate_data(&original);

            let result = Codec::Deflate.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_deflate_decompress_binary_data() {
            // Test with binary data including null bytes
            let original: Vec<u8> = vec![0, 1, 2, 0, 255, 254, 0, 128, 127];
            let compressed = create_deflate_data(&original);

            let result = Codec::Deflate.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_deflate_decompress_highly_compressible() {
            // Highly compressible data (all same byte)
            let original: Vec<u8> = vec![0xAB; 10000];
            let compressed = create_deflate_data(&original);

            // Verify compression actually happened
            assert!(compressed.len() < original.len());

            let result = Codec::Deflate.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_deflate_decompress_invalid_data() {
            // Invalid deflate data
            let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
            let err = Codec::Deflate.decompress(&invalid_data).unwrap_err();
            assert!(
                err.to_string().contains("decompression failed")
                    || err.to_string().contains("Decompression")
            );
        }

        #[test]
        fn test_deflate_decompress_different_compression_levels() {
            let original = b"The quick brown fox jumps over the lazy dog. ".repeat(100);

            // Test with different compression levels
            for level in [0, 1, 6, 9] {
                let mut encoder = DeflateEncoder::new(Vec::new(), Compression::new(level));
                encoder.write_all(&original).unwrap();
                let compressed = encoder.finish().unwrap();

                let result = Codec::Deflate.decompress(&compressed).unwrap();
                assert_eq!(result, original, "Failed for compression level {}", level);
            }
        }
    }

    #[cfg(feature = "zstd")]
    mod zstd_tests {
        use super::*;
        use std::io::Write;

        /// Helper to create zstd-compressed data
        fn create_zstd_data(uncompressed: &[u8]) -> Vec<u8> {
            let mut encoder = zstd::Encoder::new(Vec::new(), 0).unwrap();
            encoder.write_all(uncompressed).unwrap();
            encoder.finish().unwrap()
        }

        #[test]
        fn test_zstd_decompress_simple() {
            let original = b"hello world";
            let compressed = create_zstd_data(original);

            let result = Codec::Zstd.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_zstd_decompress_empty() {
            let original: &[u8] = b"";
            let compressed = create_zstd_data(original);

            let result = Codec::Zstd.decompress(&compressed).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn test_zstd_decompress_empty_input() {
            // Empty input (not compressed empty data)
            let result = Codec::Zstd.decompress(&[]).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn test_zstd_decompress_larger_data() {
            // Create some repetitive data that compresses well
            let original: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
            let compressed = create_zstd_data(&original);

            let result = Codec::Zstd.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_zstd_decompress_binary_data() {
            // Test with binary data including null bytes
            let original: Vec<u8> = vec![0, 1, 2, 0, 255, 254, 0, 128, 127];
            let compressed = create_zstd_data(&original);

            let result = Codec::Zstd.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_zstd_decompress_highly_compressible() {
            // Highly compressible data (all same byte)
            let original: Vec<u8> = vec![0xAB; 10000];
            let compressed = create_zstd_data(&original);

            // Verify compression actually happened
            assert!(compressed.len() < original.len());

            let result = Codec::Zstd.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_zstd_decompress_invalid_data() {
            // Invalid zstd data
            let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
            let err = Codec::Zstd.decompress(&invalid_data).unwrap_err();
            assert!(
                err.to_string().contains("decompression failed")
                    || err.to_string().contains("Decompression")
                    || err.to_string().contains("initialization failed")
            );
        }

        #[test]
        fn test_zstd_decompress_different_compression_levels() {
            let original = b"The quick brown fox jumps over the lazy dog. ".repeat(100);

            // Test with different compression levels (zstd supports 1-22, with 0 being default)
            for level in [1, 3, 10, 19] {
                let mut encoder = zstd::Encoder::new(Vec::new(), level).unwrap();
                encoder.write_all(&original).unwrap();
                let compressed = encoder.finish().unwrap();

                let result = Codec::Zstd.decompress(&compressed).unwrap();
                assert_eq!(result, original, "Failed for compression level {}", level);
            }
        }
    }

    #[cfg(feature = "bzip2")]
    mod bzip2_tests {
        use super::*;
        use bzip2::write::BzEncoder;
        use bzip2::Compression;
        use std::io::Write;

        /// Helper to create bzip2-compressed data
        fn create_bzip2_data(uncompressed: &[u8]) -> Vec<u8> {
            let mut encoder = BzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(uncompressed).unwrap();
            encoder.finish().unwrap()
        }

        #[test]
        fn test_bzip2_decompress_simple() {
            let original = b"hello world";
            let compressed = create_bzip2_data(original);

            let result = Codec::Bzip2.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_bzip2_decompress_empty() {
            let original: &[u8] = b"";
            let compressed = create_bzip2_data(original);

            let result = Codec::Bzip2.decompress(&compressed).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn test_bzip2_decompress_empty_input() {
            // Empty input (not compressed empty data)
            let result = Codec::Bzip2.decompress(&[]).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn test_bzip2_decompress_larger_data() {
            // Create some repetitive data that compresses well
            let original: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
            let compressed = create_bzip2_data(&original);

            let result = Codec::Bzip2.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_bzip2_decompress_binary_data() {
            // Test with binary data including null bytes
            let original: Vec<u8> = vec![0, 1, 2, 0, 255, 254, 0, 128, 127];
            let compressed = create_bzip2_data(&original);

            let result = Codec::Bzip2.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_bzip2_decompress_highly_compressible() {
            // Highly compressible data (all same byte)
            let original: Vec<u8> = vec![0xAB; 10000];
            let compressed = create_bzip2_data(&original);

            // Verify compression actually happened
            assert!(compressed.len() < original.len());

            let result = Codec::Bzip2.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_bzip2_decompress_invalid_data() {
            // Invalid bzip2 data
            let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
            let err = Codec::Bzip2.decompress(&invalid_data).unwrap_err();
            assert!(
                err.to_string().contains("decompression failed")
                    || err.to_string().contains("Decompression")
            );
        }

        #[test]
        fn test_bzip2_decompress_different_compression_levels() {
            let original = b"The quick brown fox jumps over the lazy dog. ".repeat(100);

            // Test with different compression levels (bzip2 supports 1-9)
            for level in [1, 5, 9] {
                let mut encoder = BzEncoder::new(Vec::new(), Compression::new(level));
                encoder.write_all(&original).unwrap();
                let compressed = encoder.finish().unwrap();

                let result = Codec::Bzip2.decompress(&compressed).unwrap();
                assert_eq!(result, original, "Failed for compression level {}", level);
            }
        }
    }

    #[cfg(feature = "xz")]
    mod xz_tests {
        use super::*;
        use std::io::Write;
        use xz2::write::XzEncoder;

        /// Helper to create xz-compressed data
        fn create_xz_data(uncompressed: &[u8]) -> Vec<u8> {
            let mut encoder = XzEncoder::new(Vec::new(), 6);
            encoder.write_all(uncompressed).unwrap();
            encoder.finish().unwrap()
        }

        #[test]
        fn test_xz_decompress_simple() {
            let original = b"hello world";
            let compressed = create_xz_data(original);

            let result = Codec::Xz.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_xz_decompress_empty() {
            let original: &[u8] = b"";
            let compressed = create_xz_data(original);

            let result = Codec::Xz.decompress(&compressed).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn test_xz_decompress_empty_input() {
            // Empty input (not compressed empty data)
            let result = Codec::Xz.decompress(&[]).unwrap();
            assert!(result.is_empty());
        }

        #[test]
        fn test_xz_decompress_larger_data() {
            // Create some repetitive data that compresses well
            let original: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
            let compressed = create_xz_data(&original);

            let result = Codec::Xz.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_xz_decompress_binary_data() {
            // Test with binary data including null bytes
            let original: Vec<u8> = vec![0, 1, 2, 0, 255, 254, 0, 128, 127];
            let compressed = create_xz_data(&original);

            let result = Codec::Xz.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_xz_decompress_highly_compressible() {
            // Highly compressible data (all same byte)
            let original: Vec<u8> = vec![0xAB; 10000];
            let compressed = create_xz_data(&original);

            // Verify compression actually happened
            assert!(compressed.len() < original.len());

            let result = Codec::Xz.decompress(&compressed).unwrap();
            assert_eq!(result, original);
        }

        #[test]
        fn test_xz_decompress_invalid_data() {
            // Invalid xz data
            let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
            let err = Codec::Xz.decompress(&invalid_data).unwrap_err();
            assert!(
                err.to_string().contains("decompression failed")
                    || err.to_string().contains("Decompression")
            );
        }

        #[test]
        fn test_xz_decompress_different_compression_levels() {
            let original = b"The quick brown fox jumps over the lazy dog. ".repeat(100);

            // Test with different compression levels (xz supports 0-9)
            for level in [0, 3, 6, 9] {
                let mut encoder = XzEncoder::new(Vec::new(), level);
                encoder.write_all(&original).unwrap();
                let compressed = encoder.finish().unwrap();

                let result = Codec::Xz.decompress(&compressed).unwrap();
                assert_eq!(result, original, "Failed for compression level {}", level);
            }
        }
    }
}
