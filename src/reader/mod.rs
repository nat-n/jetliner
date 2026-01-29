//! Avro file reader components
//!
//! This module provides the core reading functionality for Avro files,
//! including header parsing, block reading, streaming, and binary decoding.

mod block;
pub mod buffer;
pub mod decode;
mod header;
pub mod record_decoder;
pub mod stream;
pub mod varint;

pub use block::{AvroBlock, BlockReader, DecompressedBlock, ReadBufferConfig};
pub use buffer::{BufferConfig, PrefetchBuffer};
pub use decode::{
    // Complex type decoders
    decode_array,
    // Complex type decoders with resolution context
    decode_array_with_context,
    // Logical type decoders
    decode_big_decimal,
    // Primitive type decoders
    decode_boolean,
    decode_bytes,
    decode_bytes_ref,
    decode_double,
    decode_enum,
    decode_enum_index,
    decode_enum_with_resolution,
    decode_fixed,
    decode_fixed_ref,
    decode_float,
    decode_int,
    decode_long,
    decode_map,
    decode_map_with_context,
    decode_null,
    decode_record,
    decode_record_with_context,
    decode_string,
    decode_string_ref,
    decode_union,
    decode_union_index,
    decode_union_with_context,
    decode_value,
    decode_value_with_context,
    decode_varint,
    // Skip functions for projection pushdown
    skip_array,
    skip_bytes,
    skip_fixed,
    skip_map,
    skip_value,
    skip_value_with_context,
    skip_varint,
    // Value type
    AvroValue,
};
pub use header::AvroHeader;
pub use record_decoder::{FullRecordDecoder, ProjectedRecordDecoder, RecordDecode, RecordDecoder};
pub use stream::{AvroStreamReader, ReaderConfig};
// Re-export varint encoding functions for convenience
pub use varint::{encode_varint, encode_zigzag};
