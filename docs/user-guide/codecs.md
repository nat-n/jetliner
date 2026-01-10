# Codec Support

Jetliner supports all standard Avro compression codecs. This guide covers codec characteristics and selection guidance.

## Supported Codecs

| Codec     | Compression | Speed     | Use Case                                 |
| --------- | ----------- | --------- | ---------------------------------------- |
| `null`    | None        | Fastest   | When storage isn't a concern             |
| `snappy`  | Low-Medium  | Very Fast | General purpose, good balance            |
| `deflate` | Medium-High | Medium    | When compression matters more than speed |
| `zstd`    | High        | Fast      | Best overall balance                     |
| `bzip2`   | Very High   | Slow      | Maximum compression, archival            |
| `xz`      | Highest     | Slowest   | Maximum compression, archival            |

All codecs are included in the default Jetliner build. No additional configuration is required.

## Codec Characteristics

### null (No Compression)

- **Compression ratio**: 1:1 (no compression)
- **Read speed**: Fastest
- **Best for**: Small files, already-compressed data, maximum read speed

```python
# Fastest reads, largest files
df = jetliner.scan("uncompressed.avro").collect()
```

### snappy

- **Compression ratio**: ~2-4x
- **Read speed**: Very fast
- **Best for**: General purpose, real-time processing

Snappy prioritizes speed over compression ratio. It's a good default choice when you need fast reads and writes.

### deflate (gzip)

- **Compression ratio**: ~4-8x
- **Read speed**: Medium
- **Best for**: When storage cost matters, network transfer

Deflate provides better compression than Snappy at the cost of slower decompression.

### zstd (Zstandard)

- **Compression ratio**: ~4-10x
- **Read speed**: Fast
- **Best for**: Best overall balance of compression and speed

Zstd typically provides better compression than Snappy with similar or better decompression speed. It's often the best choice for new projects.

### bzip2

- **Compression ratio**: ~6-12x
- **Read speed**: Slow
- **Best for**: Archival, when storage is expensive

Bzip2 provides excellent compression but slow decompression. Use for cold storage or archival.

### xz (LZMA)

- **Compression ratio**: ~8-15x
- **Read speed**: Slowest
- **Best for**: Maximum compression, long-term archival

XZ provides the highest compression ratios but the slowest decompression. Use only when storage is at a premium and read speed isn't critical.

## Codec Selection Guide

### By Use Case

| Use Case              | Recommended Codec   |
| --------------------- | ------------------- |
| Real-time analytics   | `snappy` or `zstd`  |
| Data lake storage     | `zstd`              |
| Network transfer      | `zstd` or `deflate` |
| Cold storage/archival | `bzip2` or `xz`     |
| Maximum read speed    | `null`              |
| Lambda/serverless     | `snappy` or `zstd`  |

### By Priority

**Speed priority**: `null` > `snappy` > `zstd` > `deflate` > `bzip2` > `xz`

**Compression priority**: `xz` > `bzip2` > `zstd` ≈ `deflate` > `snappy` > `null`

**Balanced**: `zstd` (best overall trade-off)

## Performance Considerations

### Decompression is the Hot Path

Codec decompression is often the bottleneck when reading Avro files. Choose codecs based on your read patterns:

- **Frequent reads**: Prefer `snappy` or `zstd`
- **Infrequent reads**: `deflate`, `bzip2`, or `xz` are acceptable

### Memory Usage

Decompression requires temporary buffers. For memory-constrained environments:

```python
import jetliner

# Smaller buffers for memory-constrained environments
df = jetliner.scan(
    "data.avro",
    buffer_blocks=2,
    buffer_bytes=16 * 1024 * 1024,  # 16MB
).collect()
```

### S3 Considerations

For S3 files, codec choice affects both storage cost and transfer time:

- **Storage cost**: Higher compression = lower cost
- **Transfer time**: Higher compression = less data to transfer
- **CPU time**: Higher compression = more decompression work

For S3, `zstd` often provides the best balance.

## Error Handling

Codec errors are raised as `CodecError`:

```python
import jetliner

try:
    df = jetliner.scan("data.avro").collect()
except jetliner.CodecError as e:
    print(f"Decompression failed: {e}")
```

Common causes:
- Corrupted compressed data
- Truncated file
- Invalid compression block

## Building with Specific Codecs

When building from source, you can customize codec support:

```bash
# All codecs (default)
maturin develop

# Specific codecs only (faster compile)
maturin develop --cargo-extra-args="--no-default-features --features snappy,zstd"

# Minimal build (null codec only)
maturin develop --cargo-extra-args="--no-default-features"
```

Available feature flags:
- `snappy`
- `deflate`
- `zstd`
- `bzip2`
- `xz`

## Checking File Codec

To check which codec a file uses:

```python
import jetliner
import json

with jetliner.open("data.avro") as reader:
    # The codec is in the file metadata
    schema = reader.schema_dict
    # Note: codec info is in the file header, not schema
```

Or use fastavro for inspection:

```python
import fastavro

with open("data.avro", "rb") as f:
    reader = fastavro.reader(f)
    print(f"Codec: {reader.codec}")
```

## Next Steps

- [Streaming Large Files](streaming.md) - Memory-efficient processing
- [Query Optimization](query-optimization.md) - Reduce data read
- [Error Handling](error-handling.md) - Handle codec errors
