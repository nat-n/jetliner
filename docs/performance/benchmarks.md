# Benchmarks

Performance benchmarks comparing Jetliner with other Avro readers.

!!! note "Coming Soon"
    Detailed benchmark results will be added in a future update.

## Benchmark Setup

Benchmarks are run using:

- **Hardware**: Apple M-series / Intel Xeon
- **Files**: Various sizes from 1MB to 10GB
- **Codecs**: null, snappy, zstd
- **Metrics**: Throughput (MB/s), memory usage, latency

## Running Benchmarks

To run benchmarks locally:

```bash
# Rust benchmarks
poe bench

# Read throughput benchmark
poe bench-read
```

## Preliminary Results

| Reader      | Throughput | Memory | Notes                         |
| ----------- | ---------- | ------ | ----------------------------- |
| Jetliner    | TBD        | TBD    | Streaming, query optimization |
| fastavro    | TBD        | TBD    | Pure Python                   |
| Apache Avro | TBD        | TBD    | Reference implementation      |

## Factors Affecting Performance

1. **Codec**: Decompression is often the bottleneck
2. **Schema complexity**: Nested types are slower
3. **Column count**: More columns = more work
4. **Network latency**: For S3, network dominates
5. **Buffer configuration**: Affects prefetch efficiency
