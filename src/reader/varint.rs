//! Shared varint encoding and decoding utilities.
//!
//! This module provides functions for encoding and decoding variable-length integers
//! as used in the Avro binary format. Avro uses the same varint encoding as Protocol Buffers:
//! - Each byte has 7 bits of data and 1 continuation bit (MSB)
//! - The continuation bit indicates if more bytes follow
//! - Bytes are in little-endian order
//!
//! For signed integers, Avro uses zigzag encoding to map signed values to unsigned:
//! - 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
//! - Encoding formula: (n << 1) ^ (n >> 63)
//! - Decoding formula: (n >> 1) ^ -(n & 1)

use crate::error::DecodeError;

// ============================================================================
// Decoding Functions
// ============================================================================

/// Decode an unsigned variable-length integer.
///
/// Varints use the same encoding as Protocol Buffers:
/// - Each byte has 7 bits of data and 1 continuation bit (MSB)
/// - The continuation bit indicates if more bytes follow
/// - Bytes are in little-endian order
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced past the varint)
///
/// # Returns
/// The decoded unsigned 64-bit integer
///
/// # Errors
/// - `DecodeError::UnexpectedEof` if the input is truncated
/// - `DecodeError::InvalidVarint` if the varint exceeds 10 bytes
#[inline]
pub fn decode_varint(data: &mut &[u8]) -> Result<u64, DecodeError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    loop {
        if data.is_empty() {
            return Err(DecodeError::UnexpectedEof);
        }

        let byte = data[0];
        *data = &data[1..];

        // Add the 7 data bits to the result
        result |= ((byte & 0x7F) as u64) << shift;

        // Check if this is the last byte (MSB is 0)
        if byte & 0x80 == 0 {
            return Ok(result);
        }

        shift += 7;

        // Prevent overflow (max 10 bytes for 64-bit varint)
        if shift >= 64 {
            return Err(DecodeError::InvalidVarint);
        }
    }
}

/// Decode an unsigned variable-length integer, tracking the byte offset.
///
/// This variant is useful when parsing file structures where you need to
/// track the current position for error reporting.
///
/// # Arguments
/// * `cursor` - The input byte slice (cursor is advanced past the varint)
/// * `offset` - The current byte offset (incremented by bytes consumed)
///
/// # Returns
/// The decoded unsigned 64-bit integer
///
/// # Errors
/// - `DecodeError::UnexpectedEof` if the input is truncated
/// - `DecodeError::InvalidVarint` if the varint exceeds 10 bytes
#[inline]
pub fn decode_varint_with_offset(cursor: &mut &[u8], offset: &mut u64) -> Result<u64, DecodeError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    loop {
        if cursor.is_empty() {
            return Err(DecodeError::UnexpectedEof);
        }

        let byte = cursor[0];
        *cursor = &cursor[1..];
        *offset += 1;

        // Add the 7 data bits to the result
        result |= ((byte & 0x7F) as u64) << shift;

        // Check if this is the last byte (MSB is 0)
        if byte & 0x80 == 0 {
            return Ok(result);
        }

        shift += 7;

        // Prevent overflow (max 10 bytes for 64-bit varint)
        if shift >= 64 {
            return Err(DecodeError::InvalidVarint);
        }
    }
}

/// Decode a signed variable-length integer (zigzag encoded).
///
/// Avro uses zigzag encoding for signed integers, where the sign bit
/// is moved to the least significant position. This allows small negative
/// numbers to be encoded efficiently.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced past the varint)
///
/// # Returns
/// The decoded signed 64-bit integer
///
/// # Errors
/// - `DecodeError::UnexpectedEof` if the input is truncated
/// - `DecodeError::InvalidVarint` if the varint exceeds 10 bytes
#[inline]
pub fn decode_zigzag(data: &mut &[u8]) -> Result<i64, DecodeError> {
    let unsigned = decode_varint(data)?;
    // Zigzag decode: (n >> 1) ^ -(n & 1)
    Ok(((unsigned >> 1) as i64) ^ (-((unsigned & 1) as i64)))
}

/// Decode a signed variable-length integer (zigzag encoded), tracking the byte offset.
///
/// This variant is useful when parsing file structures where you need to
/// track the current position for error reporting.
///
/// # Arguments
/// * `cursor` - The input byte slice (cursor is advanced past the varint)
/// * `offset` - The current byte offset (incremented by bytes consumed)
///
/// # Returns
/// The decoded signed 64-bit integer
///
/// # Errors
/// - `DecodeError::UnexpectedEof` if the input is truncated
/// - `DecodeError::InvalidVarint` if the varint exceeds 10 bytes
#[inline]
pub fn decode_zigzag_with_offset(cursor: &mut &[u8], offset: &mut u64) -> Result<i64, DecodeError> {
    let unsigned = decode_varint_with_offset(cursor, offset)?;
    // Zigzag decode: (n >> 1) ^ -(n & 1)
    Ok(((unsigned >> 1) as i64) ^ (-((unsigned & 1) as i64)))
}

/// Skip over a varint without decoding its value.
///
/// This is useful for skipping fields during projection pushdown.
///
/// # Arguments
/// * `data` - The input byte slice (cursor is advanced past the varint)
///
/// # Errors
/// - `DecodeError::UnexpectedEof` if the input is truncated
#[inline]
pub fn skip_varint(data: &mut &[u8]) -> Result<(), DecodeError> {
    loop {
        if data.is_empty() {
            return Err(DecodeError::UnexpectedEof);
        }
        let byte = data[0];
        *data = &data[1..];
        if byte & 0x80 == 0 {
            return Ok(());
        }
    }
}

// ============================================================================
// Encoding Functions
// ============================================================================

/// Encode an unsigned integer as a variable-length integer.
///
/// # Arguments
/// * `value` - The unsigned 64-bit integer to encode
///
/// # Returns
/// A vector containing the encoded bytes
#[inline]
pub fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80; // Set continuation bit
        }
        result.push(byte);
        if value == 0 {
            break;
        }
    }
    result
}

/// Encode a signed integer as a zigzag-encoded variable-length integer.
///
/// # Arguments
/// * `value` - The signed 64-bit integer to encode
///
/// # Returns
/// A vector containing the encoded bytes
#[inline]
pub fn encode_zigzag(value: i64) -> Vec<u8> {
    // Zigzag encode: (n << 1) ^ (n >> 63)
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint(zigzag)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // decode_varint tests
    // ========================================================================

    #[test]
    fn test_decode_varint_single_byte() {
        // 0 -> 0x00
        let data: &[u8] = &[0x00];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 0);
        assert!(cursor.is_empty());

        // 1 -> 0x01
        let data: &[u8] = &[0x01];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 1);

        // 127 -> 0x7F
        let data: &[u8] = &[0x7F];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 127);
    }

    #[test]
    fn test_decode_varint_multi_byte() {
        // 128 -> 0x80 0x01
        let data: &[u8] = &[0x80, 0x01];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 128);
        assert!(cursor.is_empty());

        // 300 -> 0xAC 0x02
        let data: &[u8] = &[0xAC, 0x02];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 300);

        // 16383 -> 0xFF 0x7F
        let data: &[u8] = &[0xFF, 0x7F];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 16383);

        // 16384 -> 0x80 0x80 0x01
        let data: &[u8] = &[0x80, 0x80, 0x01];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), 16384);
    }

    #[test]
    fn test_decode_varint_large() {
        // Max i64 as unsigned: 9223372036854775807
        let data: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
        let mut cursor = data;
        assert_eq!(decode_varint(&mut cursor).unwrap(), i64::MAX as u64);
    }

    #[test]
    fn test_decode_varint_eof() {
        let data: &[u8] = &[];
        let mut cursor = data;
        assert!(matches!(
            decode_varint(&mut cursor),
            Err(DecodeError::UnexpectedEof)
        ));

        // Truncated multi-byte varint
        let data: &[u8] = &[0x80]; // Continuation bit set but no more bytes
        let mut cursor = data;
        assert!(matches!(
            decode_varint(&mut cursor),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    // ========================================================================
    // decode_varint_with_offset tests
    // ========================================================================

    #[test]
    fn test_decode_varint_with_offset() {
        let data = [0x00];
        let mut cursor = &data[..];
        let mut offset = 0u64;
        assert_eq!(
            decode_varint_with_offset(&mut cursor, &mut offset).unwrap(),
            0
        );
        assert_eq!(offset, 1);

        let data = [0x80, 0x01];
        let mut cursor = &data[..];
        let mut offset = 0u64;
        assert_eq!(
            decode_varint_with_offset(&mut cursor, &mut offset).unwrap(),
            128
        );
        assert_eq!(offset, 2);
    }

    // ========================================================================
    // decode_zigzag tests
    // ========================================================================

    #[test]
    fn test_decode_zigzag_positive() {
        // Zigzag: 0 -> 0, 1 -> 2, 2 -> 4, etc.
        let data = [0x00];
        let mut cursor = &data[..];
        assert_eq!(decode_zigzag(&mut cursor).unwrap(), 0);

        let data = [0x02];
        let mut cursor = &data[..];
        assert_eq!(decode_zigzag(&mut cursor).unwrap(), 1);

        let data = [0x04];
        let mut cursor = &data[..];
        assert_eq!(decode_zigzag(&mut cursor).unwrap(), 2);
    }

    #[test]
    fn test_decode_zigzag_negative() {
        // Zigzag: -1 -> 1, -2 -> 3, etc.
        let data = [0x01];
        let mut cursor = &data[..];
        assert_eq!(decode_zigzag(&mut cursor).unwrap(), -1);

        let data = [0x03];
        let mut cursor = &data[..];
        assert_eq!(decode_zigzag(&mut cursor).unwrap(), -2);
    }

    // ========================================================================
    // decode_zigzag_with_offset tests
    // ========================================================================

    #[test]
    fn test_decode_zigzag_with_offset() {
        let data = [0x00];
        let mut cursor = &data[..];
        let mut offset = 0u64;
        assert_eq!(
            decode_zigzag_with_offset(&mut cursor, &mut offset).unwrap(),
            0
        );
        assert_eq!(offset, 1);

        let data = [0x01];
        let mut cursor = &data[..];
        let mut offset = 0u64;
        assert_eq!(
            decode_zigzag_with_offset(&mut cursor, &mut offset).unwrap(),
            -1
        );
        assert_eq!(offset, 1);
    }

    // ========================================================================
    // skip_varint tests
    // ========================================================================

    #[test]
    fn test_skip_varint() {
        // Single byte varint
        let data: &[u8] = &[0x7F, 0xFF];
        let mut cursor = data;
        skip_varint(&mut cursor).unwrap();
        assert_eq!(cursor, &[0xFF]);

        // Multi-byte varint
        let data: &[u8] = &[0x80, 0x80, 0x01, 0xFF];
        let mut cursor = data;
        skip_varint(&mut cursor).unwrap();
        assert_eq!(cursor, &[0xFF]);
    }

    #[test]
    fn test_skip_varint_eof() {
        let data: &[u8] = &[];
        let mut cursor = data;
        assert!(matches!(
            skip_varint(&mut cursor),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    // ========================================================================
    // encode_varint tests
    // ========================================================================

    #[test]
    fn test_encode_varint() {
        assert_eq!(encode_varint(0), vec![0x00]);
        assert_eq!(encode_varint(1), vec![0x01]);
        assert_eq!(encode_varint(127), vec![0x7F]);
        assert_eq!(encode_varint(128), vec![0x80, 0x01]);
        assert_eq!(encode_varint(300), vec![0xAC, 0x02]);
    }

    #[test]
    fn test_encode_zigzag() {
        assert_eq!(encode_zigzag(0), vec![0x00]);
        assert_eq!(encode_zigzag(-1), vec![0x01]);
        assert_eq!(encode_zigzag(1), vec![0x02]);
        assert_eq!(encode_zigzag(-2), vec![0x03]);
        assert_eq!(encode_zigzag(2), vec![0x04]);
    }

    // ========================================================================
    // Round-trip tests
    // ========================================================================

    #[test]
    fn test_varint_roundtrip() {
        for value in [0u64, 1, 127, 128, 255, 256, 16383, 16384, u64::MAX / 2] {
            let encoded = encode_varint(value);
            let mut cursor = &encoded[..];
            let decoded = decode_varint(&mut cursor).unwrap();
            assert_eq!(decoded, value);
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn test_zigzag_roundtrip() {
        for value in [0i64, 1, -1, 2, -2, 127, -128, i64::MAX, i64::MIN] {
            let encoded = encode_zigzag(value);
            let mut cursor = &encoded[..];
            let decoded = decode_zigzag(&mut cursor).unwrap();
            assert_eq!(decoded, value);
            assert!(cursor.is_empty());
        }
    }
}
