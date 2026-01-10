# Performance

This section covers Jetliner's performance characteristics and optimization strategies.

## Overview

Jetliner is designed for high-throughput Avro reading with minimal memory overhead:

- **Streaming architecture**: Block-by-block reading, not full file loading
- **Zero-copy techniques**: Uses `bytes::Bytes` for efficient memory handling
- **Async I/O**: Overlaps I/O with processing via prefetching
- **Query optimization**: Projection and predicate pushdown reduce data read

## Topics

- [Benchmarks](benchmarks.md) - Performance comparisons with other readers
- [Optimization Tips](optimization.md) - Tuning for your use case

## Quick Tips

1. **Use projection pushdown**: Select only needed columns
2. **Use predicate pushdown**: Filter during read, not after
3. **Use early stopping**: `head()` stops reading early
4. **Tune buffers**: Adjust `buffer_blocks` and `buffer_bytes` for your environment
5. **Choose appropriate codecs**: `zstd` offers best balance of speed and compression
