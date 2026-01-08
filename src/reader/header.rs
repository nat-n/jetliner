//! Avro file header parsing
//!
//! Parses the Avro Object Container File header which contains:
//! - Magic bytes ("Obj\x01")
//! - Metadata map (including schema and codec)
//! - 16-byte sync marker

use std::collections::HashMap;

use crate::codec::Codec;
use crate::error::{DecodeError, ReaderError};
use crate::schema::{parse_schema, AvroSchema};

/// The Avro magic bytes that identify an Object Container File.
/// Format: "Obj" followed by version byte (0x01)
pub const AVRO_MAGIC: [u8; 4] = [b'O', b'b', b'j', 0x01];

/// Minimum header size: magic (4) + empty map (1) + sync marker (16)
const MIN_HEADER_SIZE: usize = 4 + 1 + 16;

/// Parsed Avro file header containing schema and metadata.
///
/// The header is the first section of an Avro Object Container File and contains:
/// - Magic bytes for format identification
/// - Metadata map with schema and codec information
/// - Sync marker for block boundary detection
///
/// # Requirements
/// - 1.1: Parse file header and extract embedded schema
/// - 1.2: Validate magic bytes match Avro specification
#[derive(Debug, Clone)]
pub struct AvroHeader {
    /// The magic bytes (should be "Obj\x01")
    pub magic: [u8; 4],
    /// Metadata key-value pairs from the header
    pub metadata: HashMap<String, Vec<u8>>,
    /// 16-byte sync marker used to identify block boundaries
    pub sync_marker: [u8; 16],
    /// Parsed Avro schema from metadata
    pub schema: AvroSchema,
    /// Compression codec from metadata
    pub codec: Codec,
    /// Total size of the header in bytes (offset where blocks begin)
    pub header_size: u64,
}

impl AvroHeader {
    /// Parse an Avro header from raw bytes.
    ///
    /// # Arguments
    /// * `bytes` - The raw bytes of the Avro file (at least the header portion)
    ///
    /// # Returns
    /// The parsed header or an error if parsing fails.
    ///
    /// # Errors
    /// - `ReaderError::InvalidMagic` if magic bytes don't match
    /// - `ReaderError::Parse` if metadata or sync marker cannot be parsed
    /// - `SchemaError` if the schema JSON is invalid
    /// - `CodecError` if the codec is unknown
    ///
    /// # Requirements
    /// - 1.1: Parse file header and extract embedded schema
    /// - 1.2: Validate magic bytes match Avro specification
    pub fn parse(bytes: &[u8]) -> Result<Self, ReaderError> {
        if bytes.len() < MIN_HEADER_SIZE {
            return Err(ReaderError::Parse {
                offset: 0,
                message: format!(
                    "Header too short: expected at least {} bytes, got {}",
                    MIN_HEADER_SIZE,
                    bytes.len()
                ),
            });
        }

        let mut cursor = bytes;
        let mut offset: u64 = 0;

        // Parse magic bytes
        let magic = Self::parse_magic(&mut cursor, &mut offset)?;

        // Parse metadata map
        let metadata = Self::parse_metadata(&mut cursor, &mut offset)?;

        // Parse sync marker (16 bytes)
        let sync_marker = Self::parse_sync_marker(&mut cursor, &mut offset)?;

        // Extract and parse schema from metadata
        let schema = Self::extract_schema(&metadata)?;

        // Extract codec from metadata (defaults to "null")
        let codec = Self::extract_codec(&metadata)?;

        Ok(Self {
            magic,
            metadata,
            sync_marker,
            schema,
            codec,
            header_size: offset,
        })
    }

    /// Parse and validate the magic bytes.
    fn parse_magic(cursor: &mut &[u8], offset: &mut u64) -> Result<[u8; 4], ReaderError> {
        if cursor.len() < 4 {
            return Err(ReaderError::Parse {
                offset: *offset,
                message: "Not enough bytes for magic".to_string(),
            });
        }

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&cursor[..4]);
        *cursor = &cursor[4..];
        *offset += 4;

        if magic != AVRO_MAGIC {
            return Err(ReaderError::InvalidMagic(magic));
        }

        Ok(magic)
    }

    /// Parse the metadata map.
    ///
    /// Avro metadata is encoded as a map with string keys and bytes values.
    /// The map is encoded as a series of blocks, where each block starts with
    /// a count (can be negative for block size hint), followed by key-value pairs.
    fn parse_metadata(
        cursor: &mut &[u8],
        offset: &mut u64,
    ) -> Result<HashMap<String, Vec<u8>>, ReaderError> {
        let mut metadata = HashMap::new();

        loop {
            // Read block count (can be negative)
            let count = decode_varint_signed(cursor, offset).map_err(|e| ReaderError::Parse {
                offset: *offset,
                message: format!("Failed to decode metadata block count: {}", e),
            })?;

            if count == 0 {
                break;
            }

            // If count is negative, the absolute value is the count and
            // the next long is the block size in bytes (which we ignore)
            let actual_count = if count < 0 {
                // Read and discard block size
                let _block_size =
                    decode_varint_signed(cursor, offset).map_err(|e| ReaderError::Parse {
                        offset: *offset,
                        message: format!("Failed to decode metadata block size: {}", e),
                    })?;
                (-count) as usize
            } else {
                count as usize
            };

            // Read key-value pairs
            for _ in 0..actual_count {
                let key = decode_string(cursor, offset).map_err(|e| ReaderError::Parse {
                    offset: *offset,
                    message: format!("Failed to decode metadata key: {}", e),
                })?;

                let value = decode_bytes(cursor, offset).map_err(|e| ReaderError::Parse {
                    offset: *offset,
                    message: format!("Failed to decode metadata value for key '{}': {}", key, e),
                })?;

                metadata.insert(key, value);
            }
        }

        Ok(metadata)
    }

    /// Parse the 16-byte sync marker.
    fn parse_sync_marker(cursor: &mut &[u8], offset: &mut u64) -> Result<[u8; 16], ReaderError> {
        if cursor.len() < 16 {
            return Err(ReaderError::Parse {
                offset: *offset,
                message: format!(
                    "Not enough bytes for sync marker: expected 16, got {}",
                    cursor.len()
                ),
            });
        }

        let mut sync_marker = [0u8; 16];
        sync_marker.copy_from_slice(&cursor[..16]);
        *cursor = &cursor[16..];
        *offset += 16;

        Ok(sync_marker)
    }

    /// Extract and parse the schema from metadata.
    fn extract_schema(metadata: &HashMap<String, Vec<u8>>) -> Result<AvroSchema, ReaderError> {
        let schema_bytes = metadata
            .get("avro.schema")
            .ok_or_else(|| ReaderError::Parse {
                offset: 0,
                message: "Missing 'avro.schema' in metadata".to_string(),
            })?;

        let schema_json = std::str::from_utf8(schema_bytes).map_err(|e| ReaderError::Parse {
            offset: 0,
            message: format!("Schema is not valid UTF-8: {}", e),
        })?;

        parse_schema(schema_json).map_err(ReaderError::Schema)
    }

    /// Extract the codec from metadata.
    fn extract_codec(metadata: &HashMap<String, Vec<u8>>) -> Result<Codec, ReaderError> {
        match metadata.get("avro.codec") {
            Some(codec_bytes) => {
                let codec_name =
                    std::str::from_utf8(codec_bytes).map_err(|e| ReaderError::Parse {
                        offset: 0,
                        message: format!("Codec name is not valid UTF-8: {}", e),
                    })?;
                Codec::from_name(codec_name).map_err(ReaderError::Codec)
            }
            None => Ok(Codec::Null), // Default codec is null
        }
    }

    /// Get the schema as a JSON string.
    pub fn schema_json(&self) -> String {
        self.schema.to_json()
    }

    /// Get a metadata value by key.
    pub fn get_metadata(&self, key: &str) -> Option<&[u8]> {
        self.metadata.get(key).map(|v| v.as_slice())
    }

    /// Get a metadata value as a string.
    pub fn get_metadata_string(&self, key: &str) -> Option<&str> {
        self.metadata
            .get(key)
            .and_then(|v| std::str::from_utf8(v).ok())
    }
}

// Use shared varint functions from the varint module
use super::varint::decode_zigzag_with_offset;

/// Decode a variable-length signed integer (zigzag encoded).
///
/// Avro uses zigzag encoding for signed integers, where the sign bit
/// is moved to the least significant position.
#[inline]
fn decode_varint_signed(cursor: &mut &[u8], offset: &mut u64) -> Result<i64, DecodeError> {
    decode_zigzag_with_offset(cursor, offset)
}

/// Decode a length-prefixed string.
fn decode_string(cursor: &mut &[u8], offset: &mut u64) -> Result<String, DecodeError> {
    let bytes = decode_bytes(cursor, offset)?;
    String::from_utf8(bytes).map_err(DecodeError::InvalidUtf8)
}

/// Decode length-prefixed bytes.
fn decode_bytes(cursor: &mut &[u8], offset: &mut u64) -> Result<Vec<u8>, DecodeError> {
    let len = decode_varint_signed(cursor, offset)?;

    if len < 0 {
        return Err(DecodeError::InvalidData(format!(
            "Negative length for bytes: {}",
            len
        )));
    }

    let len = len as usize;

    if cursor.len() < len {
        return Err(DecodeError::UnexpectedEof);
    }

    let bytes = cursor[..len].to_vec();
    *cursor = &cursor[len..];
    *offset += len as u64;

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::super::varint::encode_zigzag;
    use super::*;

    #[test]
    fn test_avro_magic_constant() {
        assert_eq!(AVRO_MAGIC, [b'O', b'b', b'j', 0x01]);
    }

    #[test]
    fn test_decode_string() {
        // Length 5, then "hello"
        let data = [0x0A, b'h', b'e', b'l', b'l', b'o']; // 0x0A = zigzag(5) = 10
        let mut cursor = &data[..];
        let mut offset = 0u64;
        assert_eq!(decode_string(&mut cursor, &mut offset).unwrap(), "hello");
        assert_eq!(offset, 6);
    }

    #[test]
    fn test_decode_bytes() {
        let data = [0x06, 0x01, 0x02, 0x03]; // Length 3 (zigzag), then bytes
        let mut cursor = &data[..];
        let mut offset = 0u64;
        assert_eq!(
            decode_bytes(&mut cursor, &mut offset).unwrap(),
            vec![0x01, 0x02, 0x03]
        );
    }

    #[test]
    fn test_parse_magic_valid() {
        let data = [b'O', b'b', b'j', 0x01, 0x00]; // Magic + empty map marker
        let mut cursor = &data[..];
        let mut offset = 0u64;
        let magic = AvroHeader::parse_magic(&mut cursor, &mut offset).unwrap();
        assert_eq!(magic, AVRO_MAGIC);
        assert_eq!(offset, 4);
    }

    #[test]
    fn test_parse_magic_invalid() {
        let data = [b'A', b'v', b'r', b'o'];
        let mut cursor = &data[..];
        let mut offset = 0u64;
        let result = AvroHeader::parse_magic(&mut cursor, &mut offset);
        assert!(matches!(result, Err(ReaderError::InvalidMagic(_))));
    }

    #[test]
    fn test_parse_sync_marker() {
        let data: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let mut cursor = &data[..];
        let mut offset = 0u64;
        let sync = AvroHeader::parse_sync_marker(&mut cursor, &mut offset).unwrap();
        assert_eq!(sync, data);
        assert_eq!(offset, 16);
    }

    /// Helper to create a minimal valid Avro header
    fn create_test_header(schema_json: &str, codec: Option<&str>) -> Vec<u8> {
        let mut header = Vec::new();

        // Magic bytes
        header.extend_from_slice(&AVRO_MAGIC);

        // Count the number of metadata entries
        let entry_count: i64 = if codec.is_some() { 2 } else { 1 };

        // Metadata map: block count (zigzag encoded)
        header.extend_from_slice(&encode_zigzag(entry_count));

        // Schema entry
        let schema_key = b"avro.schema";
        header.extend_from_slice(&encode_zigzag(schema_key.len() as i64));
        header.extend_from_slice(schema_key);
        header.extend_from_slice(&encode_zigzag(schema_json.len() as i64));
        header.extend_from_slice(schema_json.as_bytes());

        // Codec entry (if provided)
        if let Some(codec_name) = codec {
            let codec_key = b"avro.codec";
            header.extend_from_slice(&encode_zigzag(codec_key.len() as i64));
            header.extend_from_slice(codec_key);
            header.extend_from_slice(&encode_zigzag(codec_name.len() as i64));
            header.extend_from_slice(codec_name.as_bytes());
        }

        // End of map
        header.push(0x00);

        // Sync marker (16 bytes)
        header.extend_from_slice(&[
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ]);

        header
    }

    #[test]
    fn test_parse_header_simple_schema() {
        let header_bytes = create_test_header(r#""string""#, None);
        let header = AvroHeader::parse(&header_bytes).unwrap();

        assert_eq!(header.magic, AVRO_MAGIC);
        assert_eq!(header.codec, Codec::Null);
        assert!(matches!(header.schema, AvroSchema::String));
        assert_eq!(
            header.sync_marker,
            [
                0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
                0xDE, 0xF0
            ]
        );
    }

    #[test]
    fn test_parse_header_with_codec() {
        let header_bytes = create_test_header(r#""int""#, Some("deflate"));
        let header = AvroHeader::parse(&header_bytes).unwrap();

        assert_eq!(header.codec, Codec::Deflate);
        assert!(matches!(header.schema, AvroSchema::Int));
    }

    #[test]
    fn test_parse_header_record_schema() {
        let schema = r#"{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}"#;
        let header_bytes = create_test_header(schema, None);
        let header = AvroHeader::parse(&header_bytes).unwrap();

        match &header.schema {
            AvroSchema::Record(r) => {
                assert_eq!(r.name, "Test");
                assert_eq!(r.fields.len(), 1);
                assert_eq!(r.fields[0].name, "id");
            }
            _ => panic!("Expected Record schema"),
        }
    }

    #[test]
    fn test_parse_header_too_short() {
        let data = [b'O', b'b', b'j'];
        let result = AvroHeader::parse(&data);
        assert!(matches!(result, Err(ReaderError::Parse { .. })));
    }

    #[test]
    fn test_parse_header_invalid_magic() {
        let mut header_bytes = create_test_header(r#""null""#, None);
        header_bytes[0] = b'X'; // Corrupt magic
        let result = AvroHeader::parse(&header_bytes);
        assert!(matches!(result, Err(ReaderError::InvalidMagic(_))));
    }

    #[test]
    fn test_parse_header_missing_schema() {
        let mut header = Vec::new();
        header.extend_from_slice(&AVRO_MAGIC);
        header.push(0x00); // Empty metadata map
        header.extend_from_slice(&[0u8; 16]); // Sync marker

        let result = AvroHeader::parse(&header);
        assert!(matches!(result, Err(ReaderError::Parse { .. })));
    }

    #[test]
    fn test_parse_header_invalid_schema_json() {
        let header_bytes = create_test_header(r#"{"invalid json"#, None);
        let result = AvroHeader::parse(&header_bytes);
        assert!(matches!(result, Err(ReaderError::Schema(_))));
    }

    #[test]
    fn test_parse_header_unknown_codec() {
        let header_bytes = create_test_header(r#""string""#, Some("unknown_codec"));
        let result = AvroHeader::parse(&header_bytes);
        assert!(matches!(result, Err(ReaderError::Codec(_))));
    }

    #[test]
    fn test_header_schema_json() {
        let header_bytes = create_test_header(r#""long""#, None);
        let header = AvroHeader::parse(&header_bytes).unwrap();
        assert_eq!(header.schema_json(), r#""long""#);
    }

    #[test]
    fn test_header_get_metadata() {
        let header_bytes = create_test_header(r#""string""#, Some("snappy"));
        let header = AvroHeader::parse(&header_bytes).unwrap();

        assert!(header.get_metadata("avro.schema").is_some());
        assert_eq!(header.get_metadata_string("avro.codec"), Some("snappy"));
        assert!(header.get_metadata("nonexistent").is_none());
    }

    #[test]
    fn test_header_size_tracking() {
        let header_bytes = create_test_header(r#""null""#, None);
        let header = AvroHeader::parse(&header_bytes).unwrap();

        // header_size should equal the total bytes consumed
        assert_eq!(header.header_size as usize, header_bytes.len());
    }
}
