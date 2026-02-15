# User guide

## APIs

| API | Returns | Best for |
|-----|---------|----------|
| [`scan_avro()`](../api/scan_avro.md) | LazyFrame | Query optimization, large files |
| [`read_avro()`](../api/read_avro.md) | DataFrame | Simple eager loading |
| [`AvroReader`](../api/avro_reader.md) | Iterator | Streaming, progress tracking |
| [`MultiAvroReader`](../api/multi_avro_reader.md) | Iterator | Multi-file streaming control |

All APIs share the same Rust core and support local files and S3.

## Topics

- **[Data sources](data-sources.md)** — Paths, S3 configuration, supported codecs
- **[Query optimization](query-optimization.md)** — Projection pushdown, predicate pushdown, early stopping
- **[Streaming](streaming.md)** — Memory-efficient batch processing, buffer tuning
- **[Error handling](error-handling.md)** — Strict vs skip mode, error inspection
- **[Schemas](schemas.md)** — Type mapping, schema inspection
