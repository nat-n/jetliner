# Task 17.2.1: Snappy CRC32 Checksum (Not CRC32C)

## Context

Snappy-compressed Avro files from Apache Avro were failing with CRC checksum mismatches despite correct decompression.

## Problem

The Avro spec states: "Each compressed block is followed by the 4-byte, big-endian CRC32 checksum of the uncompressed data."

We initially implemented CRC32C (Castagnoli polynomial, `0x82F63B78`) because:
- Google's Snappy framing format uses CRC32C
- CRC32C is hardware-accelerated on modern CPUs
- Many modern systems prefer CRC32C

However, Avro uses **CRC32 (ISO polynomial, `0xEDB88320`)** - the same as zlib/gzip.

## Decision

Use `crc32fast` crate (ISO polynomial) instead of `crc32c` crate (Castagnoli polynomial).

## Verification

```python
# CRC32C (Castagnoli) - WRONG for Avro
>>> crc32c.crc32c(decompressed_data)
0x14D463EF

# CRC32 (ISO/zlib) - CORRECT for Avro
>>> zlib.crc32(decompressed_data)
0x5058CA11  # Matches file checksum
```

## Key Files

- `src/codec/mod.rs`: `decompress_snappy()` uses `crc32fast::hash()`
- `Cargo.toml`: `crc32fast = "1"` dependency

## References

- Avro 1.11.0 Spec, "snappy" codec: https://avro.apache.org/docs/1.11.0/spec.html
- ISO 3309 CRC32 polynomial: `0xEDB88320` (reflected)
- Castagnoli CRC32C polynomial: `0x82F63B78` (reflected)
