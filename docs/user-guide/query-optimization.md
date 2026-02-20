# Query optimization

Jetliner's `scan_avro()` integrates with Polars' query optimizer to minimize I/O and memory usage. For buffer and batch size tuning, see [Streaming](streaming.md).

## Optimizations

| Optimization        | What it does                            | Benefit                       |
| ------------------- | --------------------------------------- | ----------------------------- |
| Projection pushdown | Only deserializes columns used in query | Less CPU, less memory         |
| Predicate pushdown  | Filters data during reading             | Less memory, faster queries   |
| Early stopping      | Stops reading after row limit           | Faster for `head()`/`limit()` |

## Example

```python
import jetliner
import polars as pl

# Highly optimized: deserializes 2 columns, filters during read, stops at 1000 rows
result = (
    jetliner.scan_avro("data.avro")
    .select(["user_id", "amount"])
    .filter(pl.col("amount") > 100)
    .head(1000)
    .collect()
)
```

These optimizations also apply when using `sink_batches()` or `collect_batches()` for streaming — see [Streaming](streaming.md#streaming-with-scan_avro).

Polars automatically detects which columns and filters to push down. See the [Polars user guide](https://docs.pola.rs/user-guide/lazy/optimizations/) for details on how the query optimizer works.

## Using read_avro() with columns

For eager loading, use `read_avro()` with the `columns` parameter:

```python
df = jetliner.read_avro("data.avro", columns=["user_id", "amount"])
```

## Limitations

- **Sorting**: Early stopping doesn't apply when sorting (all data must be read first)
- **Complex expressions**: Some complex filter expressions may not push down
