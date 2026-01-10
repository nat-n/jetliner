# Task 8.1: Avro Binary Decoder for Primitives

## Context

Implementing primitive type decoding for Avro binary format in `src/reader/decode.rs`.

## Decisions

### 1. Code Duplication with varint.rs

**Problem:** During implementation, I created `decode_varint` and `decode_long` (zigzag) functions in `decode.rs`. However, `src/reader/varint.rs` already contains identical functionality (`decode_varint`, `decode_zigzag`).

**Current State:** Both modules exist with overlapping code:
- `decode.rs`: `decode_varint()`, `decode_long()` (zigzag)
- `varint.rs`: `decode_varint()`, `decode_zigzag()`, plus encoding functions and `skip_varint()`

**Recommendation:** Future task should consolidate by having `decode.rs` import from `varint.rs` rather than duplicating. The `varint.rs` module is more complete (includes encoding, skip, offset-tracking variants).

**Location:** `src/reader/decode.rs` lines 55-90, `src/reader/varint.rs`

### 2. Zero-Copy String Reference Error Handling

**Problem:** `decode_string_ref` uses `std::str::from_utf8()` which returns `Utf8Error`, but `DecodeError::InvalidUtf8` expects `FromUtf8Error` (from `String::from_utf8`).

**Decision:** Use `DecodeError::InvalidData` with the error message instead of `InvalidUtf8` for the ref variant. This is a minor inconsistency but avoids adding a new error variant.

**Location:** `src/reader/decode.rs`, `decode_string_ref()`

## API Design

The decoder provides two variants for bytes/strings:
- `decode_bytes()` / `decode_string()` - allocating, returns owned data
- `decode_bytes_ref()` / `decode_string_ref()` - zero-copy, returns slice/&str

This supports both convenience (owned) and performance (zero-copy) use cases.

## Test Coverage

28 unit tests covering all primitive types, edge cases (EOF, invalid data), and sequential decoding.
