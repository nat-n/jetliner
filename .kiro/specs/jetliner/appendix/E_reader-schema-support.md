# Appendix E: Reader Schema Support

## Overview

Avro's schema evolution model involves two schemas:
- **Writer schema**: Embedded in the Avro file header, describes how data was encoded
- **Reader schema**: Provided by the application, describes what the application expects

The Avro library performs **schema resolution** to map writer-encoded data to reader expectations. This enables forward and backward compatibility between schema versions.

## Relevance to Jetliner

Jetliner reads Avro Object Container Files, which are self-contained with embedded schemas. The question is whether reader schema support is valuable for this use case.

### Use Cases Where Reader Schema Matters

1. **Reading files with evolved schemas**: Files written over time with different schema versions need to be read into a consistent DataFrame shape.

2. **Enum symbol evolution**: Writer adds new enum symbols; older readers use a default symbol for unknown values.

3. **Field defaults**: Reader expects fields that don't exist in older files; defaults are injected.

4. **Type promotions**: Writer used `int`, reader expects `long`; values are promoted during decode.

### Use Cases Where Reader Schema Is Less Relevant

1. **Single-file reads**: Each file is self-contained; output shape matches file contents.

2. **Projection**: Jetliner already supports projection via Polars pushdown without needing an explicit reader schema.

3. **Schema registry systems**: These use single-object encoding, not object container files. Not applicable to Jetliner.

## Current State

Jetliner has foundational support for schema resolution:

- `src/schema/compatibility.rs`: Full schema compatibility checking per Avro spec
- `src/schema/reader_writer_resolution.rs`: `ReaderWriterResolution` struct for field mapping, type promotions, defaults
- `src/reader/decode.rs`: `decode_enum_with_resolution()` for enum symbol mapping with defaults
- Tests verify these components work correctly in isolation

**However**, these components are not wired into the actual decode pipeline. The Python API has no way to specify a reader schema.

## API Design Options

### Option 1: Schema as Dict/JSON
```python
reader_schema = {"type": "record", "name": "Event", "fields": [...]}
jetliner.scan("file.avro", reader_schema=reader_schema)
jetliner.AvroReader("file.avro", reader_schema=reader_schema)
```

### Option 2: Schema from File
```python
jetliner.scan("file.avro", reader_schema_path="schema.avsc")
```

### Option 3: Polars-style Schema Override
```python
jetliner.scan("file.avro", schema_overrides={"status": pl.Enum([...])})
```

Option 1 is most aligned with Avro conventions. Option 3 is more Polars-idiomatic but less powerful.

## Implementation Complexity

### Why It's Not Simple

The complexity is **pervasive, not localized**. Data is encoded according to the writer schema but must be output according to the reader schema. This affects every record decoded:

| Aspect            | Impact                                           |
| ----------------- | ------------------------------------------------ |
| Field ordering    | Decode in writer order, output in reader order   |
| Type promotions   | Every value may need conversion (int→long, etc.) |
| Enum symbols      | Every enum value needs symbol lookup/mapping     |
| Missing fields    | Inject defaults for fields not in writer         |
| Extra fields      | Skip writer fields not in reader                 |
| Nested structures | Resolution cascades recursively                  |

### Component Changes Required

| Component        | Effort   | Description                               |
| ---------------- | -------- | ----------------------------------------- |
| Python API       | Low      | Add `reader_schema` parameter             |
| Rust bindings    | Low      | Parse schema, pass to reader              |
| Stream reader    | Medium   | Build resolution context, pass to decoder |
| Record decoder   | **High** | Resolution-aware field decoders           |
| Arrow conversion | Medium   | Output schema matches reader, not writer  |

The record decoder is the critical path. Currently optimized for single-schema with direct Arrow builders. Adding resolution means either:

**Option A: Intermediate representation**
- Decode to `AvroValue`, apply resolution, build Arrow
- Simpler but slower (extra allocation + copy)

**Option B: Resolution-aware decoders**
- Build specialized decoders at schema-load time
- Decode directly to Arrow with inline transformations
- Faster but significantly more complex

### Performance Considerations

Resolution adds per-value overhead in the hot path:
- Branch for "does this field need promotion?"
- Lookup for enum symbol mapping
- Reordering buffer for field sequence

Mitigation strategies:
1. **Upfront analysis**: Determine at schema-load time which fields need special handling
2. **Fast path**: If schemas are identical, skip resolution entirely
3. **Specialized decoders**: Generate resolution logic once, not per-value
4. **SIMD for promotions**: Batch int→long conversions

## Recommendation

Reader schema support is a **real Avro feature** but addresses a use case that may be uncommon for Jetliner users. Most users read files as-is into DataFrames.

**Short term**: Document as a known limitation. The foundational code exists if needed.

**If implementing**:
1. Start with Option A (intermediate representation) for correctness
2. Add fast-path for identical schemas
3. Optimize hot paths based on profiling
4. Consider Option B only if performance is critical

**Estimated effort**: 2-4 days for a solid implementation, plus comprehensive testing.

## Supporting Work Already Done

The following components are implemented and tested:

1. **Schema compatibility checking** (`src/schema/compatibility.rs`)
   - `check_compatibility()` validates reader/writer compatibility
   - Handles all Avro resolution rules (promotions, defaults, etc.)
   - Comprehensive test coverage

2. **Resolution data structures** (`src/schema/reader_writer_resolution.rs`)
   - `ReaderWriterResolution` maps writer fields to reader fields
   - `ResolvedField` enum: Present (with optional promotion) or Default
   - `TypePromotion` enum for all valid promotions
   - `decode_record_with_resolution()` for resolution-aware record decoding

3. **Enum resolution** (`src/reader/decode.rs`)
   - `decode_enum_with_resolution()` handles symbol mapping
   - Uses reader's default symbol for unknown writer symbols
   - Correctly maps symbol indices between schemas

4. **JSON-to-AvroValue conversion** (`src/schema/reader_writer_resolution.rs`)
   - `json_to_avro_value()` converts JSON defaults to AvroValue
   - Handles all Avro types including nested structures

These components are tested in isolation but not yet integrated into the main decode pipeline.
