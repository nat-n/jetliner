# Contributing

Thank you for your interest in contributing to Jetliner!

## Get in touch

If you think you've found a bug in Jetliner, or you think it's missing a feature, get in touch via the [GitHub Issues](https://github.com/nat-n/jetliner/issues).

### Reporting issues

When reporting issues, please try to include:

- Jetliner version
- Python version
- Operating system
- Minimal reproduction case
- Error messages and stack traces

## Development setup

### Tools

- Python 3.11+
- Rust toolchain (install via [rustup](https://rustup.rs/))
- [uv](https://docs.astral.sh/uv/) for Python dependency management
- [poe](https://poethepoet.natn.io/) for task management

This project was mostly implemented via Spec Driven Development with [kiro](https://kiro.dev/), so requirements, design, and tasks are tracked permanently under the .kiro/specs

### Getting started

```bash
# Clone the repository
git clone https://github.com/jetliner/jetliner.git
cd jetliner

# See all available tasks
poe

# Install Rust toolchain
poe rustup

# Create virtual environment and install dependencies
uv sync

# Build the Rust extension
poe build-dev

# Verify installation
uv run python -c "import jetliner; print('OK')"
```

## Running tests

```bash
# Run all checks (formatting, linting, tests)
poe check

# Run specific test suites
poe test-rust      # Rust unit tests
poe test-python    # Python e2e tests
```

## Code style

The following tasks are available to help with ensuring rust code meets formatting and linting standards:

```bash
poe format-rust
poe lint-rust
```

This project uses rust for python code formatting and linting python code. The following tasks are available to help:

```bash
poe format-python
poe lint-python
```

## Project structure

```
jetliner/
├── src/                   # Rust source code
│   ├── codec/             # Compression codecs
│   ├── convert/           # Arrow/DataFrame conversion
│   ├── python/            # PyO3 bindings
│   ├── reader/            # Core Avro reader
│   ├── schema/            # Schema parsing
│   └── source/            # File/S3 sources
├── python/
│   ├── jetliner/          # Python package for bindings
│   └── tests/             # Pytest tests
├── tests/                 # Rust integration tests
├── benches/               # Benchmarks
└── docs/                  # Documentation
```

## Documentation

To preview documentation locally:

```bash
poe docs-serve
```

Then open http://localhost:8000 in your browser.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
