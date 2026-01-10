# Design Decision: Non-Record Schema Handling in Jetliner

**Context**: Jetliner converts Avro → Polars DataFrames. When reading files with non-record top-level schemas (primitives, arrays, etc.), we need to choose how to represent single-value data in a DataFrame.

**Decision Date**: 2026-01-09

---

## Problem Statement

Given an Avro file with a non-record schema like:
```json
"string"  // or "bytes", "int", {"type": "array", "items": "int"}, etc.
```

Each "record" is a single value, not a structured object with fields. But DataFrames are inherently tabular and require named columns.

**Question**: What column name should we use?

---

## Options Considered

### Option 1: "value" (RECOMMENDED)

**Example**:
```python
# File with schema: "string"
df = jetliner.scan("logs.avro").collect()
# ┌─────────────────────┐
# │ value               │
# │ ---                 │
# │ str                 │
# ╞═════════════════════╡
# │ "Error: failed"     │
# │ "Info: started"     │
# └─────────────────────┘
```

**Pros**:
- ✅ **Semantic meaning**: "value" clearly indicates "this is the data"
- ✅ **Ecosystem precedent**:
  - Protocol Buffers (Google's wrapper types): `StringValue { string value = 1; }`
  - Kafka/Avro ecosystem: Single-field records commonly use "value"
  - KSQL `WRAP_SINGLE_VALUE` property convention
- ✅ **Clear intent**: Not positional, not an index — it's THE value
- ✅ **Natural queries**: `df.select("value")` reads intuitively
- ✅ **Already proposed**: Your design doc (B2) recommends this

**Cons**:
- ⚠️ Generic name might clash if user post-processes
- ⚠️ Not a Polars-specific convention

---

### Option 2: Type-based naming

**Example**:
```python
# File with schema: "string"
df = jetliner.scan("logs.avro").collect()
# Column: "string_value" or "str"

# File with schema: "bytes"
df = jetliner.scan("data.avro").collect()
# Column: "bytes_value" or "binary"
```

**Pros**:
- ✅ Self-documenting: Column name indicates type
- ✅ Reduces name collisions with different schemas

**Cons**:
- ❌ **Inconsistent column names** across different file types
- ❌ Harder to write generic code (column name varies)
- ❌ Complex for arrays/maps: `"array_of_int_value"`?
- ❌ No ecosystem precedent for this pattern

---

### Option 3: Numbered columns ("column_1")

**Example**:
```python
# File with schema: "string"
df = jetliner.scan("logs.avro").collect()
# ┌─────────────────────┐
# │ column_1            │
# │ ---                 │
# │ str                 │
# ╞═════════════════════╡
```

**Pros**:
- ✅ Matches Polars CSV convention (when `has_header=False`)
- ✅ Familiar to Polars users

**Cons**:
- ❌ **Wrong semantic**: Numbered columns are for **positional** identification in multi-column data
- ❌ **User confusion**: "column_1" suggests there might be a "column_2"
- ❌ Less intuitive: "This is position 1" vs. "This is the value"
- ❌ Not standard for wrapping scenarios

**Verdict**: Numbered patterns are for headerless tabular data, not wrapped single values.

---

### Option 4: "_" (underscore)

**Example**:
```python
df = jetliner.scan("logs.avro").collect()
# Column: "_"
```

**Pros**:
- ✅ Short and unobtrusive
- ✅ Signals "anonymous" or "default"

**Cons**:
- ❌ **Not descriptive**: What does "_" mean?
- ❌ Can cause issues in Python: `df["_"]` vs `df._`
- ❌ No ecosystem precedent
- ❌ Easily confused with internal/private convention

---

### Option 5: "item"

**Example**:
```python
df = jetliner.scan("values.avro").collect()
# Column: "item"
```

**Pros**:
- ✅ Arrow/Parquet convention for list elements
- ✅ Semantic: "an item from the file"

**Cons**:
- ⚠️ **Context mismatch**: "item" suggests membership in a collection
- ⚠️ Less intuitive for primitive scalars (string, int, bytes)
- ⚠️ Better fit for arrays, awkward for primitives

---

### Option 6: Schema-type literal ("string", "int", "bytes")

**Example**:
```python
# File with schema: "string"
df = jetliner.scan("logs.avro").collect()
# Column: "string"
```

**Pros**:
- ✅ Directly maps to Avro schema type
- ✅ Self-documenting

**Cons**:
- ❌ **Namespace pollution**: "string", "int" are Python keywords/types
- ❌ Confusing: `df["string"]` looks like you're selecting strings, not a column
- ❌ Complex for nested types: what's the column name for `{"type": "array", "items": "int"}`?

---

## Ecosystem Analysis

### What Other Systems Do

| System | Pattern | Use Case | Column Name |
|--------|---------|----------|-------------|
| **Polars CSV** | `column_N` | Headerless tabular data | `column_1`, `column_2`, ... |
| **Pandas CSV** | Integer indices | Headerless tabular data | `0`, `1`, `2`, ... |
| **Spark CSV** | `_cN` | Headerless tabular data | `_c0`, `_c1`, `_c2`, ... |
| **DuckDB CSV** | `columnN` | Headerless tabular data | `column0`, `column1`, ... |
| **Protobuf** | `value` | **Wrapper types** | `value` |
| **Kafka/Avro** | `value` | **Single-field records** | `value` |
| **Arrow Lists** | `item` | **Array elements** | `item` |

**Key Insight**:
- **Positional patterns** (`column_1`, `_c0`) → Used for **multi-column** data without headers
- **Semantic names** (`value`, `item`) → Used for **wrapped single values**

**Jetliner's case is wrapping**, not headerless tabular data → Use semantic name.

---

## Recommendation

### Use **"value"** as the default column name

**Rationale**:

1. **Semantic clarity**: Communicates "this is the wrapped data value"
2. **Ecosystem alignment**: Matches Protobuf and Kafka/Avro conventions
3. **Intuitive queries**: `df.select("value")` is self-explanatory
4. **Consistent**: Same column name regardless of Avro type
5. **Not positional**: Doesn't suggest multi-column data

### Implementation Details

```python
# Default behavior
df = jetliner.scan("primitives.avro").collect()
# → Single column: "value"

# Schema: "string" → df["value"] is pl.Utf8
# Schema: "bytes"  → df["value"] is pl.Binary
# Schema: "int"    → df["value"] is pl.Int32
# Schema: array    → df["value"] is pl.List
```

### Add Configuration Option (Optional)

For power users who want control:

```python
# Override default column name
df = jetliner.scan(
    "primitives.avro",
    unwrapped_column_name="data"
).collect()
# → Single column: "data"
```

**Benefits**:
- ✅ Sensible default for 95% of users
- ✅ Flexibility for edge cases (name collisions, organizational standards)
- ✅ Clear documentation path

---

## User Experience Scenarios

### Scenario 1: Log File (string schema)

**File**: 10,000 log lines with schema `"string"`

```python
import jetliner
import polars as pl

# Read log file
df = jetliner.scan("app.log.avro").collect()

# User can immediately use the data
log_lines = df["value"]

# Filter logs
errors = df.filter(pl.col("value").str.contains("ERROR"))

# Count occurrences
error_count = df.filter(
    pl.col("value").str.contains("ERROR")
).height
```

**UX Assessment**: ✅ Intuitive — "value" clearly means "the log line"

---

### Scenario 2: Timestamp Sequence (long schema)

**File**: Timestamps with schema `"long"`

```python
df = jetliner.scan("timestamps.avro").collect()

# Convert to datetime
df = df.with_columns(
    pl.col("value").cast(pl.Datetime).alias("timestamp")
)

# Analysis
df.select([
    pl.col("timestamp").min().alias("start"),
    pl.col("timestamp").max().alias("end"),
])
```

**UX Assessment**: ✅ Clear — "value" is the timestamp value

---

### Scenario 3: Binary Data (bytes schema)

**File**: Image thumbnails with schema `"bytes"`

```python
df = jetliner.scan("thumbnails.avro").collect()

# Process each thumbnail
for thumbnail_bytes in df["value"]:
    process_image(thumbnail_bytes)

# Filter by size
small_images = df.filter(
    pl.col("value").list.len() < 10000
)
```

**UX Assessment**: ✅ Natural — "value" is the binary payload

---

### Scenario 4: Array Records

**File**: Coordinate lists with schema `{"type": "array", "items": "int"}`

```python
df = jetliner.scan("coordinates.avro").collect()

# Each row is an array
# df["value"] is pl.List(pl.Int32)

# Extract first coordinate
df = df.with_columns(
    pl.col("value").list.get(0).alias("x"),
    pl.col("value").list.get(1).alias("y"),
)
```

**UX Assessment**: ✅ Makes sense — "value" is the coordinate array

---

## Edge Cases & Considerations

### Consideration 1: Name Collision

**Issue**: What if user later adds a column named "value"?

```python
df = jetliner.scan("data.avro").collect()
# Already has column "value"

# User tries to add another "value" column
df = df.with_columns(
    pl.lit("new").alias("value")  # Error: duplicate column name
)
```

**Solution**: Standard Polars behavior — duplicate column names raise errors. User can:
- Rename the original: `df.rename({"value": "original_value"})`
- Use different name: `pl.lit("new").alias("new_value")`

**Assessment**: Not a real problem — same as any other column name.

---

### Consideration 2: Documentation Clarity

**Issue**: Users must understand why single-column DataFrames are created.

**Solution**: Clear documentation:

```markdown
## Reading Non-Record Schemas

Jetliner supports reading Avro files with any top-level schema type.
For non-record schemas (primitives, arrays, etc.), data is returned
in a single-column DataFrame with the column named "value".

### Examples

**String schema** → Single column of strings:
```python
# File schema: "string"
df = jetliner.scan("logs.avro").collect()
# Shape: (n_rows, 1)
# Columns: ["value"]
# df["value"] is pl.Utf8
```

**Array schema** → Single column of lists:
```python
# File schema: {"type": "array", "items": "int"}
df = jetliner.scan("arrays.avro").collect()
# df["value"] is pl.List(pl.Int32)
```

**Why a single column?** DataFrames are tabular structures requiring
named columns. Non-record Avro schemas don't have field names, so
the data is wrapped in a column named "value".
```

**Assessment**: With clear docs, users will understand immediately.

---

### Consideration 3: Alternative Naming Strategy

**User feedback scenario**: Some users prefer different naming conventions.

**Solution**: Add optional parameter (low priority):

```python
# For organizations with standards
df = jetliner.scan(
    "data.avro",
    unwrapped_column_name="data"  # or "item", or "col", etc.
).collect()
```

**When to implement**:
- ⏸️ Not in initial release (YAGNI)
- ✅ Add if users request it
- ✅ Easy to add later without breaking changes

---

## Alternative Approach: Don't Wrap (Rejected)

**Idea**: For non-record schemas, return something other than a DataFrame.

```python
# Return iterator of raw values
for value in jetliner.read_values("strings.avro"):
    print(value)  # "string 1", "string 2", etc.
```

**Pros**:
- ✅ No artificial wrapping
- ✅ More natural for primitive types

**Cons**:
- ❌ **Breaks API consistency**: `scan()` always returns LazyFrame, `open()` always returns DataFrame iterator
- ❌ **Type complexity**: Return type depends on schema (Union types in Python)
- ❌ **User confusion**: "Why does this file return a DataFrame, but this one doesn't?"
- ❌ **Loses DataFrame benefits**: Can't use Polars operations, filtering, etc.
- ❌ **More code**: Need separate code paths for record vs non-record

**Verdict**: Rejected — Consistency and simplicity are more important than avoiding wrapping.

---

## Decision Matrix

| Criterion | "value" | Type-based | "column_1" | "_" | "item" |
|-----------|---------|------------|------------|-----|--------|
| **Semantic clarity** | ✅✅✅ | ✅✅ | ⚠️ | ❌ | ✅✅ |
| **Ecosystem precedent** | ✅✅✅ | ❌ | ✅ | ❌ | ✅✅ |
| **Consistency** | ✅✅✅ | ❌ | ✅✅ | ✅✅ | ✅✅ |
| **Intuitive queries** | ✅✅✅ | ✅✅ | ⚠️ | ❌ | ✅✅ |
| **No confusion** | ✅✅ | ⚠️ | ❌ | ❌❌ | ✅ |
| **Simple implementation** | ✅✅✅ | ❌ | ✅✅✅ | ✅✅✅ | ✅✅✅ |

**Winner**: "value" — Best balance of semantic meaning, ecosystem alignment, and user experience.

---

## Final Recommendation

### Primary Decision: Use "value"

```python
# For ALL non-record schemas
df = jetliner.scan("file.avro").collect()
# → Column: "value"
```

### Implementation Checklist

- [x] Use "value" as default column name
- [ ] Document behavior clearly (with examples)
- [ ] Add tests with various non-record schemas
- [ ] Update error messages (remove "must be record" restriction)
- [ ] Consider adding `unwrapped_column_name` parameter (future)

### Documentation Template

```python
"""
Read Avro file with non-record schema.

For files with non-record top-level schemas (primitives, arrays, maps),
the data is returned in a single-column DataFrame with the column named
"value".

Examples:
    >>> # String schema
    >>> df = jetliner.scan("logs.avro").collect()
    >>> df.columns
    ['value']
    >>> df["value"].dtype
    Utf8

    >>> # Bytes schema
    >>> df = jetliner.scan("binary.avro").collect()
    >>> df["value"].dtype
    Binary

    >>> # Array schema
    >>> df = jetliner.scan("arrays.avro").collect()
    >>> df["value"].dtype
    List(Int32)
"""
```

### Testing Strategy

```python
# Test cases to add
def test_non_record_string_schema():
    """String schema -> single column named 'value'"""
    df = jetliner.scan("strings.avro").collect()
    assert df.columns == ["value"]
    assert df["value"].dtype == pl.Utf8

def test_non_record_bytes_schema():
    """Bytes schema -> single column named 'value'"""
    df = jetliner.scan("fastavro/null.avro").collect()
    assert df.columns == ["value"]
    assert df["value"].dtype == pl.Binary

def test_non_record_array_schema():
    """Array schema -> single column named 'value'"""
    df = jetliner.scan("arrays.avro").collect()
    assert df.columns == ["value"]
    assert isinstance(df["value"].dtype, pl.List)

def test_non_record_scan_api():
    """scan() API works with non-record schemas"""
    lf = jetliner.scan("strings.avro")
    df = lf.select("value").collect()
    assert df.height > 0
```

---

## Summary

**Column Name**: **"value"**

**Reasoning**:
1. ✅ Semantic: "value" clearly means "the data"
2. ✅ Precedent: Protobuf, Kafka/Avro use "value" for wrappers
3. ✅ Consistent: Same name for all non-record types
4. ✅ Intuitive: `df.select("value")` is self-explanatory
5. ✅ Simple: Easy to implement and document

**User Impact**: Positive — Clear, predictable behavior with good documentation.

**Implementation**: Straightforward — Wrap non-record schemas in single field named "value".

---

## Alternatives Considered & Rejected

| Option | Why Rejected |
|--------|-------------|
| Type-based naming | Inconsistent column names, complex logic |
| "column_1" | Wrong semantic (positional, not wrapping) |
| "_" | Not descriptive, no precedent |
| "item" | Better for arrays, awkward for primitives |
| Type literal ("string") | Namespace pollution, confusing |
| Don't wrap | Breaks API consistency, loses DataFrame benefits |

---

**Decision Owner**: Jetliner Team
**Status**: RECOMMENDED — Ready for implementation
**Next Steps**: Implement wrapping logic, update documentation, add tests
