# Jetliner Project Guidelines

High-performance Polars plugin for streaming Avro files into DataFrames.

## Poe tasks

- Poe is installed globally, check available tasks by running `poe` or inspecting the task config in pyproject.toml
- Prefer running relevant tasks via poe if possible, for building, testing, linting, etc. For example, the easiest way to run cargo tests correctly is `poe test-rust`
- cmd tasks accept extra arguments which are appended to the inner command

## Performance

- Remember that performance is critical, always consider whether code is optimal
- Use `bytes::Bytes` for zero-copy data handling
- Prefer streaming over loading entire files into memory
- Async I/O with `tokio` for S3 and local sources
- Benchmark changes with `cargo bench` before merging
- Codec decompression is the hot path - avoid allocations there

## Testing

- E2E behavior is covered by python tests under python/tests and are organized in subdirectories and files for different concerns
- Reusable python pytest fixtures should be placed in a conftest.py
- Property tests use `proptest` with 100 iterations minimum
- Property tests live in `tests/property_tests.rs`
- Each property test references its design document property number
- Tag format: `Feature: jetliner, Property N: description`
- Run property tests: `poe test test-property`
- Unit tests can live alongside source in `#[cfg(test)]` modules

## Rust Conventions

- Errors: Use `thiserror` derive macros, structured error types in `src/error.rs`
- Async: `async-trait` for trait objects, `tokio` runtime
- Prefer `&[u8]` and `Bytes` over `Vec<u8>` for read-only data
- Use `#[cfg(feature = "...")]` for optional codec support

## Dependencies

This project uses uv for python dependency management.

- If you need a python dependency in the project install it with `uv add`
- use `uv add --dev` if it's only needed for testing or other dev processes
- when running inline python scripts use `uv run python -c '...'`

## Development process

### Implementation time decision records

If significant decisions are made whilst implementing tasks that are potentially useful in future for understanding how things work and why but beyond the granularity of the design document or the task description then on completion on the task (and not sooner) a file corresponding to the task should be created under ./kiro/<spec>/devnotes to document the important details in an concise ADR style. These documents should focus on non-obvious details that took some research, non-trivial reasoning effort, or user judgement to arrive at.

If a decision is overridden by a later decision, then the later should be documented as usual and the original should be amended to include a pointer to it's successor.
