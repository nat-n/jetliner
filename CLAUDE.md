# Jetliner Agent Steering

High-performance Rust library with Python bindings for streaming Avro files into Polars DataFrames.

## Quick Reference

```bash
poe              # List all available tasks
poe check        # Run all checks (format, lint, build, test) - use before commits
poe test-rust    # Rust unit tests
poe test-property # Rust property tests (proptest)
poe build        # Build the Python extension
poe test-python  # Python E2E tests
```

## Project Structure

```
src/                    # Rust source
  schema/               # Avro schema parsing (types.rs, parser.rs)
  codec/                # Compression codecs (snappy, deflate, zstd, bzip2, xz)
  source/               # Data sources (S3Source, LocalSource, StreamSource trait)
  reader/               # Block reading, decoding, prefetch buffer
  convert/              # Avro → Arrow → Polars type mapping
  python/               # PyO3 bindings
python/
  jetliner/             # Python package (scan, open entry points)
  tests/                # E2E tests organized by concern
tests/                  # Rust integration tests
  property_tests.rs     # All property-based tests
benches/                # Criterion benchmarks
```

## Architecture (Essential)

Two Python APIs sharing the same Rust core:
- `jetliner.scan_avro(path)` → LazyFrame with query optimization (projection/predicate pushdown, early stopping)
- `jetliner.read_avro(path)` → DataFrame (eager loading)
- `jetliner.AvroReader(path)` / `jetliner.MultiAvroReader(paths)` → Iterator for streaming control

Pipeline: `StreamSource` → `BlockReader` → `PrefetchBuffer` → `RecordDecoder` → `DataFrameBuilder` → DataFrame

Key design choices:
- Direct Avro binary → Arrow arrays (no intermediate Value enum)
- Async I/O with Tokio, sync Python API (blocks on internal runtime)
- `bytes::Bytes` for zero-copy data handling
- Block-oriented processing with configurable prefetch buffer

## Specs & Documentation

Detailed specifications in `.kiro/specs/jetliner/`:
- `requirements.md` - User stories and acceptance criteria
- `design.md` - Architecture, interfaces, type mappings, properties
- `tasks.md` - Implementation plan with status (checkboxes)
- `devnotes/` - Implementation-time decision records (ADR style)
- `appendix/` - Research and analysis documents

Steering doc: `.kiro/steering/project.md` - conventions for testing, dependencies, decision records

## Current Implementation Status

**Complete**: Core streaming, all codecs, S3/local sources, Python API, IO plugin, benchmarks

**In Progress / Known Gaps**:
- Nullable complex types (arrays/records in unions) - see `devnotes/21.1-nullable-complex-types-investigation.md`
- Enum type not implemented (should → Categorical)
- Fixed type not implemented (should → Binary)
- Logical type value issues (date off-by-one, timestamp timezone)

Check `tasks.md` for current task status - tasks 1-20 complete, 21+ pending.

## Testing Conventions

- Property tests validate universal correctness (tagged: `Feature: jetliner, Property N`)
- E2E tests under `python/tests/` organized by concern (types/, s3/, corruption/, etc.)
- Use `xfail` markers for known failing tests with descriptive reasons
- Minimum 100 iterations for property tests

## Key Files for Common Tasks

| Task | Key Files |
|------|-----------|
| Add new Avro type | `src/schema/types.rs`, `src/reader/decode.rs`, `src/convert/arrow.rs` |
| Fix decoding bug | `src/reader/record_decoder.rs`, `src/reader/decode.rs` |
| Add Python API | `src/python/reader.rs`, `python/jetliner/__init__.py`, `python/jetliner/jetliner.pyi` |
| Debug type mapping | `src/convert/arrow.rs` (Avro→Arrow), check Polars conversion |
| S3 issues | `src/source/s3.rs`, `python/tests/s3/` |

## Stub File Maintenance

The file `python/jetliner/jetliner.pyi` contains type stubs for Rust extension classes
(`AvroReader`, `MultiAvroReader`, `BadBlockError`). These stubs:

- Provide IDE autocompletion and type checking for users
- Enable mkdocstrings to generate API documentation

**When modifying `src/python/reader.rs`**: Update the corresponding stubs in `jetliner.pyi`
to keep signatures, defaults, and docstrings in sync.

## Performance Notes

- Codec decompression is the hot path - avoid allocations
- Use `cargo bench` for Rust microbenchmarks (criterion)
- Target: faster than polars-avro for complex schemas

## Comparison Benchmarks

```bash
poe bench-compare              # Compare jetliner against other Avro readers
poe bench-compare --BENCH_QUICK # Faster iteration during development
poe bench-data                 # Regenerate benchmark data files
```

See `benches/python/compare.py` docstring for all options (scenarios, readers, output/comparison flags).

Design decisions: `.kiro/specs/jetliner/devnotes/18.2-comparative-benchmarks.md`
