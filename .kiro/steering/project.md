# Jetliner Project Guidelines

High-performance Polars plugin for streaming Avro files into DataFrames.

## Poe tasks

- Poe is installed globally, check available tasks by running `poe` or inspecting the task config in pyproject.toml
- Prefer running relevant tasks via poe if possible, for building, testing, linting, etc. For example, the easiest way to run cargo tests correctly is `poe test-rust`
- cmd tasks accept extra arguments which are appended to the inner command

## Performance

- Use `bytes::Bytes` for zero-copy data handling
- Prefer streaming over loading entire files into memory
- Async I/O with `tokio` for S3 and local sources
- Benchmark changes with `cargo bench` before merging
- Codec decompression is the hot path - avoid allocations there

## Testing

- Property tests use `proptest` with 100 iterations minimum
- Property tests live in `tests/property_tests.rs`
- Each property test references its design document property number
- Tag format: `Feature: jetliner, Property N: description`
- Run property tests: `cargo test --all-features prop_`
- Unit tests can live alongside source in `#[cfg(test)]` modules

## Rust Conventions

- Errors: Use `thiserror` derive macros, structured error types in `src/error.rs`
- Async: `async-trait` for trait objects, `tokio` runtime
- Prefer `&[u8]` and `Bytes` over `Vec<u8>` for read-only data
- Use `#[cfg(feature = "...")]` for optional codec support

