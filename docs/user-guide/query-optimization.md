# Query Optimization

Jetliner's `scan()` API integrates with Polars' query optimizer to minimize I/O and memory usage. This guide explains the three main optimizations and how to use them effectively.

## Overview

When you use `scan()`, Jetliner registers as a Polars IO source, enabling three key optimizations:

| Optimization        | What it does                     | Benefit                       |
| ------------------- | -------------------------------- | ----------------------------- |
| Projection pushdown | Only reads columns used in query | Less I/O, less memory         |
| Predicate pushdown  | Filters data during reading      | Less memory, faster queries   |
| Early stopping      | Stops reading after row limit    | Faster for `head()`/`limit()` |

## Projection Pushdown

Projection pushdown ensures only the columns you actually use are read from disk or S3.

### How It Works

```python
import jetliner
import polars as pl

# Only reads "user_id" and "amount" columns from the file
result = (
    jetliner.scan("data.avro")
    .select(["user_id", "amount"])
    .collect()
)
```

Without projection pushdown, the entire file would be read into memory, then columns would be selected. With pushdown, unused columns are never allocated.

### Automatic Detection

Polars automatically detects which columns are needed:

```python
# Only reads columns used in the expression
result = (
    jetliner.scan("data.avro")
    .filter(pl.col("status") == "active")  # needs "status"
    .select(pl.col("amount").sum())        # needs "amount"
    .collect()
)
# Only "status" and "amount" are read
```

### Explicit Column Selection

For clarity, explicitly select columns early in your query:

```python
result = (
    jetliner.scan("data.avro")
    .select(["user_id", "amount", "status"])  # Explicit projection
    .filter(pl.col("status") == "active")
    .group_by("user_id")
    .agg(pl.col("amount").sum())
    .collect()
)
```

## Predicate Pushdown

Predicate pushdown applies filters during reading, so filtered-out rows never consume memory.

### How It Works

```python
import jetliner
import polars as pl

# Filter is applied as each batch is read
result = (
    jetliner.scan("data.avro")
    .filter(pl.col("amount") > 100)
    .collect()
)
```

Each batch is filtered immediately after reading, before being accumulated. This dramatically reduces memory usage for selective queries.

### Multiple Filters

Chain multiple filters for compound conditions:

```python
result = (
    jetliner.scan("data.avro")
    .filter(pl.col("status") == "active")
    .filter(pl.col("amount") > 100)
    .filter(pl.col("region").is_in(["US", "EU"]))
    .collect()
)
```

Or combine them:

```python
result = (
    jetliner.scan("data.avro")
    .filter(
        (pl.col("status") == "active") &
        (pl.col("amount") > 100) &
        (pl.col("region").is_in(["US", "EU"]))
    )
    .collect()
)
```

### Supported Filter Operations

Most Polars filter expressions work with predicate pushdown:

- Comparisons: `==`, `!=`, `>`, `<`, `>=`, `<=`
- Null checks: `is_null()`, `is_not_null()`
- String operations: `str.contains()`, `str.starts_with()`
- List membership: `is_in()`
- Boolean logic: `&`, `|`, `~`

## Early Stopping

Early stopping terminates reading once the requested number of rows is reached.

### How It Works

```python
import jetliner

# Stops reading after 1000 rows
result = jetliner.scan("large_file.avro").head(1000).collect()
```

For a 10GB file where you only need 1000 rows, this might read only a few MB.

### With limit()

```python
result = jetliner.scan("data.avro").limit(500).collect()
```

### Combined with Filters

Early stopping works with filters, but note that it stops after collecting enough *filtered* rows:

```python
import polars as pl

# Stops after finding 100 rows where amount > 1000
result = (
    jetliner.scan("data.avro")
    .filter(pl.col("amount") > 1000)
    .head(100)
    .collect()
)
```

## Combining Optimizations

The real power comes from combining all three optimizations:

```python
import jetliner
import polars as pl

# Highly optimized query
result = (
    jetliner.scan("s3://bucket/huge_file.avro")
    .select(["user_id", "amount", "timestamp"])  # Projection: 3 of 50 columns
    .filter(pl.col("amount") > 0)                # Predicate: ~60% of rows
    .head(10000)                                 # Early stop: first 10k matches
    .collect()
)
```

This query might read only 1% of the original data.

## Aggregation Queries

Aggregations benefit from projection and predicate pushdown:

```python
import jetliner
import polars as pl

# Only reads user_id and amount columns
# Filters during read
result = (
    jetliner.scan("data.avro")
    .filter(pl.col("amount") > 0)
    .group_by("user_id")
    .agg(pl.col("amount").sum().alias("total"))
    .collect()
)
```

## Sorting

Sorting requires reading all data (or all filtered data), so early stopping doesn't apply:

```python
import jetliner
import polars as pl

# Must read all rows to sort, but projection still helps
result = (
    jetliner.scan("data.avro")
    .select(["user_id", "amount"])  # Projection helps
    .sort("amount", descending=True)
    .head(10)  # Applied after sort, not during read
    .collect()
)
```

## Measuring Optimization Impact

Compare optimized vs unoptimized queries:

```python
import jetliner
import polars as pl
import time

# Unoptimized: read everything, then filter and select
start = time.time()
df = jetliner.scan("large_file.avro").collect()
result = df.filter(pl.col("amount") > 100).select(["user_id", "amount"])
print(f"Unoptimized: {time.time() - start:.2f}s")

# Optimized: pushdown enabled
start = time.time()
result = (
    jetliner.scan("large_file.avro")
    .select(["user_id", "amount"])
    .filter(pl.col("amount") > 100)
    .collect()
)
print(f"Optimized: {time.time() - start:.2f}s")
```

## Best Practices

1. **Select columns early**: Put `.select()` as early as possible in your query chain
2. **Filter early**: Apply filters before aggregations or joins
3. **Use head() for sampling**: When exploring data, use `.head()` to avoid reading entire files
4. **Combine with S3**: Optimizations are especially valuable for remote files where I/O is expensive

## Limitations

- **Sorting**: Early stopping doesn't apply when sorting (all data must be read)
- **Complex expressions**: Some complex filter expressions may not push down
- **Joins**: Pushdown applies to each side of a join independently

## Next Steps

- [Streaming Large Files](streaming.md) - Memory-efficient processing
- [S3 Access](s3-access.md) - Optimize cloud data access
- [Error Handling](error-handling.md) - Handle failures gracefully
