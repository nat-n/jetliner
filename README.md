Jetliner
========

A high-performance Polars plugin written in Rust for streaming Avro files into DataFrames with minimal copying and maximal throughput.

Jetliner is designed for data pipelines where Avro files live on S3 or local disk and need to land in Polars fast. It streams data block-by-block rather than loading entire files into memory, uses zero-copy techniques with `bytes::Bytes`, and handles all standard Avro codecs (null, snappy, deflate, zstd, bzip2, xz). The Rust core does the heavy lifting while Python bindings via PyO3 make it accessible from your existing Polars workflows.

## Usage

TODO...

## Development

This project uses poethepoet for task management.

```bash
brew tap nat-n/poethepoet
brew install nat-n/poethepoet/poethepoet

# run poe without arguments to list available tasks, defined in pyproject.toml
poe
```

### Running tests

```
poe test-rust # run rust unit tests
poe test-property # run rust property tests
poe test-schema # run rust schema tests
```

Feature flags control codec support: `snappy`, `deflate`, `zstd`, `bzip2`, `xz`. Disable what you don't need with `--no-default-features --features "snappy,zstd"` to optimize build times.

### Built with Kiro 👻
See ./.kiro for the specs.
