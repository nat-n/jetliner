# Apache Avro Java Implementation - Test Research Summary

## Overview
This document summarizes research on the official Apache Avro Java implementation and its testing approach for reading Avro Object Container Files. This research supports the development of the jetliner Rust library for reading Avro files.

**Research Date:** 2026-01-08
**Official Repository:** https://github.com/apache/avro

---

## 1. License Information

### License Type
**Apache License 2.0** (Apache-2.0)

### Key Terms for Using Test Files
✅ **YES, we can use their test files** with these requirements:

1. **Include License Copy**: Include a copy of the Apache License 2.0
2. **Attribution**: Acknowledge copyright: "Copyright 2010-2025 The Apache Software Foundation"
3. **NOTICE File**: Include the NOTICE.txt file if distributing code
4. **Modifications**: State any changes made to files
5. **No Trademark Use**: Cannot use Apache trademarks without permission

### Attribution Requirements
If using Apache Avro test files:
- Main attribution: "This product includes software developed at The Apache Software Foundation (http://www.apache.org/)."
- Include copyright notice: "Copyright 2010-2025 The Apache Software Foundation"

**License URL:** https://github.com/apache/avro/blob/master/LICENSE.txt
**Notice URL:** https://github.com/apache/avro/blob/main/NOTICE.txt

---

## 2. Repository Structure

### Main Directories
```
apache/avro/
├── lang/                           # Language implementations
│   ├── java/
│   │   ├── avro/                  # Core Java library
│   │   │   ├── src/test/          # Main test suite
│   │   │   │   ├── java/          # Test classes
│   │   │   │   └── resources/     # Test resources (schemas)
│   │   ├── integration-test/      # Integration tests
│   │   ├── interop-data-test/     # Interoperability tests
│   │   ├── benchmark/             # Performance benchmarks (JMH)
│   │   └── perf/                  # Performance tests
│   ├── python/
│   ├── c/
│   ├── csharp/
│   └── rust/
└── share/
    └── test/                       # Shared cross-language test resources
        ├── data/                   # Test data files (.avro)
        ├── schemas/                # Schema definitions (.avsc)
        └── interop/                # Interoperability test data
            ├── bin/                # Test scripts
            └── rpc/                # RPC test data
```

---

## 3. Test File Locations

### Core Java Test Classes
Located in: `lang/java/avro/src/test/java/org/apache/avro/`

#### Main Test Files for Reading Avro Files

| Test File | Purpose | Key Focus Areas |
|-----------|---------|-----------------|
| **TestDataFile.java** | Core data file operations | Read/write, codecs, sync points, seeking, splitting |
| **TestDataFileReader.java** | Reader-specific edge cases | File descriptors, EOF handling, throttled streams, magic byte validation, schema validation |
| **TestDataFileCorruption.java** | Corrupted file handling | Sync marker corruption, partial recovery, error detection |
| **TestDataFileConcat.java** | File concatenation | Multi-codec concatenation, recompression, sync interval handling |
| **TestDataFileCustomSync.java** | Custom sync markers | 16-byte sync validation, deterministic serialization |
| **TestDataFileDeflate.java** | Deflate compression | Codec-specific reading |
| **TestDataFileMeta.java** | Metadata operations | Header reading, metadata extraction |
| **TestDataFileReflect.java** | Reflection-based reading | Schema inference from Java objects |

#### Additional Test Directories

**File Operations** (`lang/java/avro/src/test/java/org/apache/avro/file/`):
- `TestAllCodecs.java` - All compression codecs
- `TestCustomCodec.java` - Custom codec implementations
- `TestIOExceptionDuringWrite.java` - I/O error handling
- `TestSeekableByteArrayInput.java` - Random access patterns
- `TestSeekableInputStream.java` - Seekable stream interface
- `TestZstandardCodec.java` - Zstandard compression

**Schema & Compatibility** (`lang/java/avro/src/test/java/org/apache/avro/`):
- `TestSchemaCompatibility.java` - Reader/writer schema compatibility
- `TestSchemaValidation.java` - Schema validation rules

**Data Types** (`lang/java/avro/src/test/java/org/apache/avro/`):
- `generic/TestGenericLogicalTypes.java` - Generic API with logical types
- `specific/TestRecordWithLogicalTypes.java` - Specific records with logical types
- `reflect/TestReflectLogicalTypes.java` - Reflection API with logical types
- `data/TestTimeConversions.java` - Date/time logical type conversions
- `TestLogicalType.java` - Decimal and other logical types

---

## 4. Shared Test Data Files

### Location
`share/test/data/` - Cross-language test resources

### Available Test Files

| File | Purpose | Size | Compression |
|------|---------|------|-------------|
| **weather.avro** | Standard test data | 358 bytes | None (uncompressed) |
| **weather-deflate.avro** | Deflate compression test | - | deflate |
| **weather-snappy.avro** | Snappy compression test | - | snappy |
| **weather-zstd.avro** | Zstandard compression test | - | zstd |
| **weather-sorted.avro** | Sorted data test | - | None |
| **syncInMeta.avro** | Sync markers in metadata | - | - |
| **test.avro12** | General test file | - | - |
| **weather.json** | Human-readable test data | - | N/A (JSON) |

### Weather Schema
```json
{
  "type": "record",
  "name": "Weather",
  "namespace": "test",
  "doc": "A weather reading",
  "fields": [
    {"name": "station", "type": "string", "order": "ignore"},
    {"name": "time", "type": "long"},
    {"name": "temp", "type": "int"}
  ]
}
```

**Sample Data:**
```json
{"station":"011990-99999","time":-619524000000,"temp":0}
```

**Raw File Access:**
```
https://github.com/apache/avro/raw/refs/heads/main/share/test/data/weather.avro
```

---

## 5. Test Schemas

### Location
`share/test/schemas/` - Schema definitions for testing

### Available Schema Files

**Avro Schema Files (.avsc):**
- `interop.avsc` - Main interoperability test schema
- `weather.avsc` - Weather data schema
- `FooBarSpecificRecord.avsc` - Specific record test
- `RecordWithRequiredFields.avsc` - Required fields test
- `fooBar.avsc` - Simple test schema
- `reserved.avsc` - Reserved names test

**Avro Protocol Files (.avpr):**
- `BulkData.avpr` - Bulk data protocol
- `mail.avpr` - Email protocol example
- `namespace.avpr` - Namespace handling
- `simple.avpr` - Simple RPC protocol (used for interop tests)

**Avro IDL Files (.avdl):**
- `contexts.avdl`, `echo.avdl`, `http.avdl`, `nestedNullable.avdl`
- `schemaevolution.avdl`, `social.avdl`, `specialtypes.avdl`, `stringables.avdl`

### Interop Schema (`share/test/schemas/interop.avsc`)

This comprehensive schema tests all Avro features:

```json
{
  "type": "record",
  "name": "Interop",
  "namespace": "org.apache.avro",
  "fields": [
    {"name": "intField", "type": "int"},
    {"name": "longField", "type": "long"},
    {"name": "stringField", "type": "string"},
    {"name": "boolField", "type": "boolean"},
    {"name": "floatField", "type": "float"},
    {"name": "doubleField", "type": "double"},
    {"name": "bytesField", "type": "bytes"},
    {"name": "nullField", "type": "null"},
    {"name": "arrayField", "type": {"type": "array", "items": "double"}},
    {"name": "mapField", "type": {"type": "map", "values": "Foo"}},
    {"name": "unionField", "type": ["boolean", "double", {"type": "array", "items": "bytes"}]},
    {"name": "enumField", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}},
    {"name": "fixedField", "type": {"type": "fixed", "name": "MD5", "size": 16}},
    {"name": "recordField", "type": {"type": "record", "name": "Node", "fields": [
      {"name": "label", "type": "string"},
      {"name": "children", "type": {"type": "array", "items": "Node"}}
    ]}}
  ]
}
```

**Features Tested:**
- All primitive types (int, long, string, boolean, float, double, bytes, null)
- Complex types (array, map, union, enum, fixed, record)
- Nested/recursive records

### Schema Test Cases (`share/test/data/schema-tests.txt`)

This file contains canonical schema representations and fingerprints for validation:

**Primitive Types Tested:**
- null, boolean, int, long, float, double, bytes, string

**Complex Types Tested:**
- Arrays (empty, single element, multiple elements)
- Records (with/without namespaces, fields, defaults, ordering)
- Enums (symbol definitions, namespace support)
- Fixed (binary fixed-length data)
- Maps (with various value types)
- Unions (combining multiple types)
- Nested/recursive records

**Format:**
```
INPUT
<schema>
OUTPUT
<canonical form>
FINGERPRINT
<fingerprint hash>
```

---

## 6. Test Strategy & Scenarios

### Core Test Coverage

#### 1. **Compression Codecs**

**Tested Codecs:**
- **null** - No compression
- **deflate** - Compression levels 0, 1, 9
- **snappy** - Snappy compression
- **xz** - Compression levels 0, 1, 6
- **zstd** - Zstandard with multiple levels and dictionary options
- **brotli** - (C# implementation)

**Test Approach:**
- Read files with each codec
- Mix different codecs in concatenated files
- Test recompression during concatenation

#### 2. **File Reading Edge Cases**

| Edge Case | Test | Expected Behavior |
|-----------|------|-------------------|
| **Empty file** | Open empty file | Proper error, no resource leak |
| **Throttled input** | Return fewer bytes than requested | Correct magic header check |
| **Unexpected EOF** | Stream returns -1 early | Throw EOFException |
| **Invalid magic bytes** | Wrong header | Throw InvalidAvroMagicException |
| **Insufficient data** | File too short for header | Throw InvalidAvroMagicException |
| **Corrupted sync marker** | Zero bytes before sync | Throw AvroRuntimeException, allow recovery |
| **Legacy schemas** | Invalid chars in schema | Read successfully without validation error |

#### 3. **Sync Marker & Seeking**

**Test Scenarios:**
- Navigate forward/backward through sync points
- Find sync markers in file
- Seek to arbitrary positions
- Read from last record
- Split files at sync boundaries
- Custom 16-byte sync markers (exact size validation)
- Sync marker in metadata

#### 4. **File Operations**

**Concatenation:**
- Append files with same codec
- Append files with different codecs
- Recompression during append
- Validate record order after concat

**Splitting:**
- Split at sync boundaries
- Read partial files
- Reassemble split files

#### 5. **Schema Compatibility**

**Compatibility Types:**
- **Backward Compatibility**: Newer readers can read older data
- **Forward Compatibility**: Older readers can read newer data
- **Full Compatibility**: Both directions work

**Compatible Scenarios:**
- Type promotion: int→long, int/long→float/double
- String↔bytes conversion
- Array/map type promotion
- Enum subset compatibility
- Record field omission
- New fields with defaults
- Union branch expansion

**Incompatible Scenarios:**
- Reader field missing default value
- Type mismatches (no valid promotion)
- Enum missing symbols without defaults
- Fixed size mismatches
- Missing union branches
- Schema name/namespace conflicts

**Test Approach:**
- Encode with writer schema
- Decode with reader schema
- Validate value transformations

#### 6. **Logical Types**

**Types Tested:**
- **decimal** - BigDecimal with precision/scale
- **date** - LocalDate (days since epoch)
- **time-millis** - LocalTime (milliseconds)
- **time-micros** - LocalTime (microseconds)
- **timestamp-millis** - Instant (milliseconds)
- **timestamp-micros** - Instant (microseconds)
- **local-timestamp-millis** - LocalDateTime (milliseconds)
- **local-timestamp-micros** - LocalDateTime (microseconds)
- **uuid** - UUID type

**Test Files:**
- `lang/java/avro/src/test/java/org/apache/avro/data/TestTimeConversions.java`
- `lang/java/avro/src/test/java/org/apache/avro/specific/TestRecordWithLogicalTypes.java`
- `lang/java/avro/src/test/java/org/apache/avro/TestLogicalType.java`

#### 7. **Performance Testing**

**Location:** `lang/java/benchmark/`

**Framework:** Java Microbenchmark Harness (JMH)

**Benchmark Execution:**
```bash
cd lang/java/benchmark
mvn clean package
java -jar target/benchmarks.jar
```

**Default Settings:**
- 5 warmup iterations
- 5 measurement iterations
- 10 seconds each

**Focus Areas:**
- Generic reader performance
- DatumReader performance
- Codec performance comparisons

---

## 7. Interoperability Testing

### Purpose
Ensure all language implementations can read/write files produced by other implementations.

### Test Data Generation

**Command (Java):**
```bash
mvn -B -P interop-data-generate generate-resources
```

**Command (Python):**
```bash
./build.sh interop-data-generate
```

**Process:**
1. Each language generates interop test data using `interop.avsc` schema
2. Output files: `build/interop/data/{lang}.avro` (e.g., `java.avro`, `py.avro`)
3. Each implementation reads all other implementations' files
4. Validates data integrity across languages

### Test Execution

**Main Build Script:** `build.sh`
```bash
./build.sh interop-data-test  # Generate and test data files
./build.sh test_rpc_interop   # Test RPC interoperability
```

**RPC Testing:**
- Protocol: `share/test/schemas/simple.avpr`
- Test data: `share/test/interop/rpc/$msg`
- Test script: `share/test/interop/bin/test_rpc_interop.sh`
- Tests each (client, server, message) triple

### Official Documentation
https://cwiki.apache.org/confluence/display/AVRO/Interoperability+Testing

---

## 8. Key Implementation Details

### DataFileReader Class

**Location:** `lang/java/avro/src/main/java/org/apache/avro/file/DataFileReader.java`

**Purpose:** "Random access to files written with DataFileWriter"

**Features:**
- Extends DataFileStream
- Supports seeking to sync markers
- Handles multiple codecs
- Schema resolution (reader vs writer schema)
- Iterator-based record reading

### File Format Structure

Based on test code analysis:

1. **Magic Bytes** - 4 bytes: Avro file identifier
2. **Header** - Contains:
   - File metadata
   - Schema (writer schema)
   - Codec name
   - Sync marker (16 bytes)
3. **Data Blocks** - Each containing:
   - Block count (number of objects)
   - Block size (in bytes)
   - Serialized objects
   - Sync marker (16 bytes)

### Error Handling

**Exception Types:**
- `InvalidAvroMagicException` - Invalid file header
- `AvroRuntimeException` - Runtime errors (corrupt blocks)
- `EOFException` - Unexpected end of file
- `IOException` - I/O errors

---

## 9. Recommended Test Files for Jetliner

### Priority 1: Essential Test Files

1. **weather.avro** - Basic uncompressed file
   - Simple schema (3 fields)
   - Multiple records
   - Standard test case

2. **weather-deflate.avro** - Deflate compression
   - Most common codec
   - Essential for real-world usage

3. **weather-snappy.avro** - Snappy compression
   - Fast compression codec
   - Common in big data pipelines

4. **weather-zstd.avro** - Zstandard compression
   - Modern, high-performance codec

5. **syncInMeta.avro** - Sync marker edge case
   - Tests sync marker handling

### Priority 2: Interoperability

6. **{lang}.avro files** (generated from `interop.avsc`)
   - Java, Python, C, etc. generated files
   - Comprehensive type coverage
   - Cross-implementation validation

### Priority 3: Edge Cases

7. **test.avro12** - General test file
8. **weather-sorted.avro** - Sorted data

### Schema Files to Use

1. **interop.avsc** - Comprehensive schema covering all features
2. **weather.avsc** - Simple, realistic schema
3. **schema-tests.txt** - Schema validation test cases

---

## 10. Test Gaps & Additional Considerations

### Potential Gaps (Not extensively tested in Java suite)

1. **Very large files** - Multi-GB files, streaming
2. **Malformed schemas** - Invalid schema structures
3. **Resource limits** - Memory exhaustion, file descriptor limits
4. **Concurrent access** - Multiple readers on same file
5. **Partial writes** - Incomplete final block

### Jetliner-Specific Considerations

Since Jetliner is **read-only**, focus on:

1. **Reader robustness:**
   - Handle all corruption scenarios
   - Graceful degradation
   - Clear error messages

2. **Performance:**
   - Benchmark against Apache Avro Java
   - Optimize codec implementations
   - Efficient seek operations

3. **Rust-specific features:**
   - Zero-copy reading where possible
   - Leverage Rust's safety guarantees
   - Memory-efficient iterators

4. **Compatibility:**
   - Test against all Apache Avro generated files
   - Validate interop with Python, Java, C implementations

---

## 11. References & Links

### Official Repository & License
- **Main Repository:** https://github.com/apache/avro
- **License:** https://github.com/apache/avro/blob/master/LICENSE.txt
- **NOTICE:** https://github.com/apache/avro/blob/main/NOTICE.txt
- **Official Site:** https://avro.apache.org/

### Test Resources
- **Shared Test Data:** https://github.com/apache/avro/tree/master/share/test/data
- **Test Schemas:** https://github.com/apache/avro/tree/master/share/test/schemas
- **Weather Data (raw):** https://github.com/apache/avro/raw/refs/heads/main/share/test/data/weather.avro

### Key Test Files
- **TestDataFile.java:** https://github.com/apache/avro/blob/master/lang/java/avro/src/test/java/org/apache/avro/TestDataFile.java
- **TestDataFileReader.java:** https://github.com/apache/avro/blob/master/lang/java/avro/src/test/java/org/apache/avro/TestDataFileReader.java
- **TestSchemaCompatibility.java:** https://github.com/apache/avro/tree/master/lang/java/avro/src/test/java/org/apache/avro/TestSchemaCompatibility.java
- **SchemaCompatibility.java:** https://github.com/apache/avro/blob/main/lang/java/avro/src/main/java/org/apache/avro/SchemaCompatibility.java

### Interoperability
- **Interop Testing Wiki:** https://cwiki.apache.org/confluence/display/AVRO/Interoperability+Testing
- **Interop Schema:** https://github.com/apache/avro/blob/main/share/test/schemas/interop.avsc
- **Schema Tests:** https://github.com/apache/avro/blob/main/share/test/data/schema-tests.txt

### Documentation & Guides
- **Build Documentation:** https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=25199651
- **Baeldung Guide:** https://www.baeldung.com/java-apache-avro

---

## 12. Summary & Recommendations

### What We Can Use

✅ **Test Data Files:** All `.avro` files in `share/test/data/`
✅ **Schema Files:** All `.avsc`, `.avpr`, `.avdl` files in `share/test/schemas/`
✅ **Test Strategy:** Replicate their test scenarios
✅ **Interop Data:** Generate and use interop test files

### How to Use Them

1. **Download test files** from the repository
2. **Include Apache License 2.0** in your project
3. **Add attribution** in documentation:
   ```
   Test data sourced from Apache Avro project
   Copyright 2010-2025 The Apache Software Foundation
   Licensed under Apache License 2.0
   ```
4. **Create similar test structure** in Jetliner:
   ```
   jetliner/
   ├── tests/
   │   ├── data/           # Test .avro files
   │   ├── schemas/        # Schema definitions
   │   └── interop/        # Cross-implementation tests
   ```

### Testing Strategy for Jetliner

**Phase 1: Basic Reading**
- Start with `weather.avro` (uncompressed)
- Implement basic DataFileReader
- Validate schema parsing and record reading

**Phase 2: Compression**
- Test each codec: deflate, snappy, zstd
- Use corresponding weather-*.avro files

**Phase 3: Edge Cases**
- Corrupted files
- Invalid headers
- Sync marker handling
- EOF scenarios

**Phase 4: Interoperability**
- Generate Java interop data
- Read with Jetliner
- Validate all fields match expected values

**Phase 5: Performance**
- Benchmark against Apache Avro Java
- Optimize hot paths
- Memory profiling

**Phase 6: Comprehensive Coverage**
- All primitive types
- All complex types
- Logical types
- Schema compatibility (as reader)

---

## Conclusion

The Apache Avro Java implementation provides:
- **Comprehensive test suite** with excellent coverage
- **Permissive licensing** (Apache 2.0) allowing use of test data
- **Well-organized test resources** in `share/test/`
- **Cross-language interop testing** framework
- **Real-world schemas and data** for validation

**Key Takeaway:** Jetliner can leverage Apache Avro's extensive test suite to ensure compatibility and robustness. The test data is freely usable under Apache 2.0, and the test scenarios provide an excellent blueprint for a comprehensive Rust implementation.
