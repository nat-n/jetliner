# Installation

This guide covers all installation methods for Jetliner, including optional dependencies and troubleshooting common issues.

## Requirements

- **Python**: 3.11 or later
- **Polars**: 0.52 or later (installed automatically as a dependency)

## Installation Methods

### From PyPI (Recommended)

=== "pip"

    ```bash
    pip install jetliner
    ```

=== "uv"

    ```bash
    uv add jetliner
    ```

=== "poetry"

    ```bash
    poetry add jetliner
    ```

### From Source

Building from source requires the Rust toolchain:

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone the repository
git clone https://github.com/jetliner/jetliner.git
cd jetliner

# Install maturin (Python-Rust build tool)
pip install maturin

# Build and install in development mode
maturin develop

# Or build a release wheel
maturin build --release
pip install target/wheels/jetliner-*.whl
```

### Development Installation

For contributing to Jetliner:

```bash
# Clone and enter the repository
git clone https://github.com/jetliner/jetliner.git
cd jetliner

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv sync --group dev

# Build the Rust extension
uv run maturin develop

# Verify installation
uv run python -c "import jetliner; print('OK')"
```

## Codec Support

Jetliner supports all standard Avro codecs out of the box:

| Codec   | Status     | Notes                              |
| ------- | ---------- | ---------------------------------- |
| null    | ✅ Built-in | No compression                     |
| snappy  | ✅ Built-in | Fast compression, moderate ratio   |
| deflate | ✅ Built-in | Good compression ratio             |
| zstd    | ✅ Built-in | Best balance of speed and ratio    |
| bzip2   | ✅ Built-in | High compression ratio, slower     |
| xz      | ✅ Built-in | Highest compression ratio, slowest |

All codecs are included in the default build. No additional dependencies are required.

### Building with Specific Codecs

When building from source, you can customize codec support using Cargo features:

```bash
# Build with only specific codecs (faster compile times)
maturin develop --cargo-extra-args="--no-default-features --features snappy,zstd"

# Build with all codecs (default)
maturin develop
```

## Verifying Installation

After installation, verify everything is working:

```python
import jetliner

# Check module loads
print(f"Module: {jetliner.__name__}")

# List available functions and classes
print(f"Exports: {jetliner.__all__}")

# Test with a simple operation (requires an Avro file)
# df = jetliner.scan("your-file.avro").collect()
```

## Troubleshooting

### ImportError: No module named 'jetliner'

**Cause**: The package isn't installed in your current Python environment.

**Solution**:
```bash
# Check which Python you're using
which python

# Install in the correct environment
pip install jetliner

# Or with uv
uv add jetliner
```

### ImportError: cannot import name 'jetliner' from 'jetliner'

**Cause**: The Rust extension wasn't built correctly.

**Solution** (when building from source):
```bash
# Rebuild the extension
maturin develop --release

# Or clean and rebuild
cargo clean
maturin develop
```

### Codec errors when reading files

**Cause**: The Avro file uses a codec that isn't available.

**Solution**: Jetliner includes all codecs by default. If you built from source with limited features, rebuild with the required codec:

```bash
maturin develop --cargo-extra-args="--features snappy,deflate,zstd"
```

### S3 connection errors

**Cause**: AWS credentials aren't configured or are invalid.

**Solution**: Configure credentials via environment variables or `storage_options`:

```bash
# Environment variables
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=us-east-1
```

Or pass credentials directly:

```python
df = jetliner.scan(
    "s3://bucket/file.avro",
    storage_options={
        "aws_access_key_id": "your-key",
        "aws_secret_access_key": "your-secret",
    }
).collect()
```

### Memory issues with large files

**Cause**: Default buffer settings may not be optimal for your use case.

**Solution**: Adjust buffer parameters:

```python
# Reduce memory usage
df = jetliner.scan(
    "large-file.avro",
    buffer_blocks=2,           # Fewer prefetched blocks
    buffer_bytes=16*1024*1024  # 16MB buffer instead of 64MB
).collect()
```

### Build errors on macOS

**Cause**: Missing Xcode command line tools.

**Solution**:
```bash
xcode-select --install
```

### Build errors on Linux

**Cause**: Missing build dependencies.

**Solution**:
```bash
# Ubuntu/Debian
sudo apt-get install build-essential pkg-config libssl-dev

# Fedora/RHEL
sudo dnf install gcc pkg-config openssl-devel
```

## Next Steps

- [Getting Started](index.md) - Quick introduction and basic usage
- [User Guide](user-guide/index.md) - Detailed guides for common workflows
- [API Reference](api/index.md) - Complete function documentation
