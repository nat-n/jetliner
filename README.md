Jetliner
========

A high-performance Polars plugin written in Rust for streaming Avro files into DataFrames with minimal memory overhead and maximal throughput.

Jetliner is designed for data pipelines where Avro files live on S3 or local disk and need to land in Polars fast. It streams data block-by-block rather than loading entire files into memory, uses zero-copy techniques with `bytes::Bytes`, and handles all standard Avro codecs (null, snappy, deflate, zstd, bzip2, xz). The Rust core does the heavy lifting while Python bindings via PyO3 make it accessible from your existing Polars workflows.

## Usage

TODO...

## Benchmarks

TODO...

## Development

The project uses spec driven development via [kiro](https://kiro.dev/). See `./.kiro` for the specs.

### Project tasks

This project uses [poethepoet](https://poethepoet.natn.io/index.html) for task management.

```bash
# Install poe globally
brew tap nat-n/poethepoet
brew install nat-n/poethepoet/poethepoet
# run poe without arguments to list available tasks, defined in pyproject.toml
poe
```

There are tasks available for formatting, linting, building, and testing. The `check` task orchestrated all tasks that must complete successfully for a change to be accepted.

### Running tests

```
poe test-rust # run rust unit tests
poe test-property # run rust property tests
poe test-schema # run rust schema tests
```

Feature flags control codec support: `snappy`, `deflate`, `zstd`, `bzip2`, `xz`. Disable what you don't need with `--no-default-features --features "snappy,zstd"` to optimize build times.

## Known Limitations

### Read-Only

Jetliner is a read-only library for Avro Object Container Files (`.avro`). It does not support:
- Writing Avro files
- Reading standalone schema files (`.avsc`) — schemas are extracted from the embedded header in `.avro` files

### Recursive Types

Avro supports recursive types (e.g., linked lists, trees) where a record can contain references to itself. Since Arrow and Polars don't natively support recursive data structures, Jetliner serializes recursive fields to JSON strings. This preserves data integrity while maintaining compatibility with the Polars DataFrame model.

Example: A binary tree node with `left` and `right` children will have those fields serialized as JSON strings that can be parsed if needed after reading.

### Complex Top-Level Schemas

Avro is usually used as a table format, with a Record as the top level type. However it may also be used with any other type at the top level.

Jetliner support primitive top level schemas (int, long, string, bytes) which are treated in the resulting polars Dataframe as a Record with a single 'value' key. However complex types have the following limitations:

- **Arrays as top-level schema**: Not yet supported (Polars list builder constraints)
- **Maps as top-level schema**: Not yet supported (struct handling in list builder)

## Trivia

- The [Avro Canada C102 Jetliner](https://en.wikipedia.org/wiki/Avro_Canada_C102_Jetliner) was the worlds second purpose built jet powered airliner.

## License

Licensed under the Apache License, Version 2.0.
See LICENSE for details.
