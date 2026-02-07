# Appendix F: Predicate Pushdown Feasibility Analysis

**Date:** 2026-01-30
**Context:** Evaluating approaches to push predicate evaluation earlier in the decode pipeline to reduce Arrow allocation overhead for filtered-out records.

**Critical Constraint:** Any implementation MUST add zero overhead to code paths that don't use predicates.

---

## Executive Summary

Predicate pushdown to the Rust layer is **feasible** with moderate complexity. The recommended approach is a two-phase implementation:

1. **Phase 1:** Filter during Arrow building (decode record → evaluate predicate → conditionally append to builders)
2. **Phase 2:** Compile simple predicates to Rust closures for evaluation on decoded values

Both phases can be implemented with zero overhead for non-predicate code paths using Rust's zero-cost abstractions (generics, trait objects, or enum dispatch).

**Expected Benefits:**
- 20-40% memory reduction for highly selective predicates
- 10-25% CPU reduction by avoiding Arrow array construction for filtered rows
- Better cache locality from smaller working sets

**Key Risks:**
- Predicate compilation complexity
- Semantic parity with Polars expression evaluation
- Testing burden to ensure correctness

---

## Current Architecture

### Data Flow (No Predicate)

```
┌─────────────┐     ┌──────────────┐     ┌───────────────┐     ┌───────────┐
│ Avro Block  │────▶│ RecordDecoder│────▶│ FieldBuilders │────▶│ DataFrame │
│ (compressed)│     │ decode_record│     │ (Arrow arrays)│     │           │
└─────────────┘     └──────────────┘     └───────────────┘     └───────────┘
```

### Data Flow (Current Predicate Handling)

```
┌─────────────┐     ┌──────────────┐     ┌───────────────┐     ┌───────────┐     ┌────────┐
│ Avro Block  │────▶│ RecordDecoder│────▶│ FieldBuilders │────▶│ DataFrame │────▶│ Filter │
│ (compressed)│     │ decode_record│     │ (Arrow arrays)│     │ (all rows)│     │ (Python)│
└─────────────┘     └──────────────┘     └───────────────┘     └───────────┘     └────────┘
```

The predicate is applied in Python after the full DataFrame is built:
```python
# In scan() source_generator:
if predicate is not None:
    df = df.filter(predicate)  # Applied AFTER Arrow construction
```

### Key Insight

The `RecordDecoder` already decodes field-by-field into typed builders:

```rust
impl RecordDecode for FullRecordDecoder {
    fn decode_record(&mut self, data: &mut &[u8]) -> Result<(), DecodeError> {
        for (field, builder) in self.avro_schema.fields.iter().zip(self.builders.iter_mut()) {
            builder.decode_field(data, &field.schema)?;  // Decode into builder
        }
        self.record_count += 1;
        Ok(())
    }
}
```

This is the natural insertion point for predicate evaluation.

---

## Approach 1: Filter During Arrow Building

### Concept

Decode the record into temporary storage, evaluate the predicate, then conditionally append to builders.

```rust
fn decode_record_with_predicate(
    &mut self,
    data: &mut &[u8],
    predicate: &CompiledPredicate,
) -> Result<(), DecodeError> {
    // Phase 1: Decode into temporary values
    let values = self.decode_to_values(data)?;

    // Phase 2: Evaluate predicate
    if predicate.matches(&values) {
        // Phase 3: Append to builders only if predicate matches
        self.append_values_to_builders(&values)?;
        self.record_count += 1;
    }

    Ok(())
}
```

### Implementation Details

#### Temporary Value Storage

Need a lightweight representation for decoded values before builder append:

```rust
/// Lightweight decoded value for predicate evaluation.
/// Avoids full AvroValue allocation overhead.
enum DecodedValue<'a> {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(&'a str),      // Zero-copy reference into block data
    Bytes(&'a [u8]),      // Zero-copy reference into block data
    // Complex types stored as opaque for now
    Complex(Vec<u8>),     // Raw bytes, re-decoded if needed
}
```

#### Zero-Overhead for Non-Predicate Path

Use a generic parameter or trait object:

```rust
/// Trait for predicate evaluation
trait RecordPredicate: Send + Sync {
    fn matches(&self, values: &[DecodedValue]) -> bool;
}

/// No-op predicate that always matches (zero overhead)
struct AlwaysMatch;
impl RecordPredicate for AlwaysMatch {
    #[inline(always)]
    fn matches(&self, _values: &[DecodedValue]) -> bool {
        true
    }
}

/// Generic decoder parameterized by predicate
struct FilteringRecordDecoder<P: RecordPredicate> {
    inner: FullRecordDecoder,
    predicate: P,
}
```

With monomorphization, `FilteringRecordDecoder<AlwaysMatch>` compiles to the same code as the current non-filtering path — the `matches()` call is inlined and eliminated.

### Pros

- Relatively simple to implement
- Clear separation of concerns
- Zero overhead when no predicate (via monomorphization)
- Works with any predicate that can evaluate on decoded values

### Cons

- Requires decoding ALL fields before predicate evaluation
- Temporary value storage adds some overhead even for matching records
- Complex types (arrays, maps, structs) need special handling

### Estimated Effort

- **Implementation:** 2-3 days
- **Testing:** 2-3 days
- **Risk:** Low-Medium

---

## Approach 2: Compile Predicate to Rust Closure

### Concept

Parse the Polars `Expr` in Python, extract a simplified predicate representation, pass to Rust, and compile to a closure that evaluates on decoded values.

### Predicate Representation

```rust
/// Simplified predicate that can be evaluated on decoded values
#[derive(Debug, Clone)]
enum SimplePredicate {
    // Leaf predicates
    Eq { column: String, value: ScalarValue },
    Ne { column: String, value: ScalarValue },
    Lt { column: String, value: ScalarValue },
    Le { column: String, value: ScalarValue },
    Gt { column: String, value: ScalarValue },
    Ge { column: String, value: ScalarValue },
    IsNull { column: String },
    IsNotNull { column: String },
    IsIn { column: String, values: Vec<ScalarValue> },

    // Compound predicates
    And(Box<SimplePredicate>, Box<SimplePredicate>),
    Or(Box<SimplePredicate>, Box<SimplePredicate>),
    Not(Box<SimplePredicate>),
}

#[derive(Debug, Clone)]
enum ScalarValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    // Add more as needed
}
```

### Python-Side Predicate Extraction

```python
def extract_simple_predicate(expr: pl.Expr) -> dict | None:
    """
    Extract a simple predicate from a Polars expression.
    Returns None if the expression is too complex.
    """
    # Use expr.meta.serialize() to get JSON representation
    expr_json = expr.meta.serialize(format="json")
    expr_dict = json.loads(expr_json)

    # Parse the expression tree
    return _parse_expr_node(expr_dict)

def _parse_expr_node(node: dict) -> dict | None:
    """Parse a single expression node."""
    # Handle different expression types
    # Return structured predicate or None if unsupported
    ...
```

### Rust-Side Compilation

```rust
impl SimplePredicate {
    /// Compile predicate to a closure for efficient evaluation
    fn compile(
        &self,
        schema: &RecordSchema
    ) -> Result<Box<dyn Fn(&[DecodedValue]) -> bool + Send + Sync>, PredicateError> {
        match self {
            SimplePredicate::Eq { column, value } => {
                let col_idx = schema.field_index(column)?;
                let value = value.clone();
                Ok(Box::new(move |values| {
                    values[col_idx].eq_scalar(&value)
                }))
            }
            SimplePredicate::And(left, right) => {
                let left_fn = left.compile(schema)?;
                let right_fn = right.compile(schema)?;
                Ok(Box::new(move |values| {
                    left_fn(values) && right_fn(values)
                }))
            }
            // ... other cases
        }
    }
}
```

### Supported Predicate Types

| Predicate                  | Complexity | Priority |
| -------------------------- | ---------- | -------- |
| `col == value`             | Low        | P0       |
| `col != value`             | Low        | P0       |
| `col > value`              | Low        | P0       |
| `col < value`              | Low        | P0       |
| `col >= value`             | Low        | P0       |
| `col <= value`             | Low        | P0       |
| `col.is_null()`            | Low        | P0       |
| `col.is_not_null()`        | Low        | P0       |
| `col.is_in([...])`         | Medium     | P1       |
| `col.is_between(a, b)`     | Medium     | P1       |
| `col.str.contains(...)`    | High       | P2       |
| `col.str.starts_with(...)` | High       | P2       |
| AND/OR/NOT combinations    | Medium     | P0       |

### Fallback Strategy

Unsupported predicates fall back to current Python-side filtering:

```python
def source_generator(...):
    # Try to compile predicate to Rust
    rust_predicate = None
    python_predicate = predicate

    if predicate is not None:
        simple = extract_simple_predicate(predicate)
        if simple is not None:
            rust_predicate = simple
            python_predicate = None  # Handled in Rust

    reader = AvroReaderCore(
        path,
        predicate=rust_predicate,  # Pass to Rust
        ...
    )

    for df in reader:
        # Apply remaining predicate in Python (if any)
        if python_predicate is not None:
            df = df.filter(python_predicate)
        yield df
```

### Pros

- Maximum performance benefit (filter before any Arrow allocation)
- Extensible to more predicate types over time
- Clear fallback path for unsupported predicates

### Cons

- Significant implementation complexity
- Must maintain semantic parity with Polars
- Polars Expr serialization format may change between versions
- More testing surface area

### Estimated Effort

- **Implementation:** 5-7 days
- **Testing:** 3-5 days
- **Risk:** Medium-High

---

## Zero-Overhead Guarantee

### Critical Requirement

The non-predicate code path MUST have zero overhead. This is achievable through:

#### Option A: Generic Monomorphization

```rust
trait Predicate {
    fn matches(&self, values: &[DecodedValue]) -> bool;
}

struct NoPredicate;
impl Predicate for NoPredicate {
    #[inline(always)]
    fn matches(&self, _: &[DecodedValue]) -> bool { true }
}

struct RecordDecoder<P: Predicate> {
    predicate: P,
    // ...
}
```

When `P = NoPredicate`, the compiler eliminates all predicate-related code.

#### Option B: Enum Dispatch with Branch Prediction

```rust
enum MaybePredicate {
    None,
    Some(CompiledPredicate),
}

impl MaybePredicate {
    #[inline]
    fn matches(&self, values: &[DecodedValue]) -> bool {
        match self {
            MaybePredicate::None => true,  // Branch predictor will learn this
            MaybePredicate::Some(p) => p.matches(values),
        }
    }
}
```

Less elegant but simpler to integrate with existing code.

#### Option C: Conditional Compilation (Not Recommended)

```rust
#[cfg(feature = "predicate-pushdown")]
fn decode_with_predicate(...) { ... }

#[cfg(not(feature = "predicate-pushdown"))]
fn decode_with_predicate(...) { decode_without_predicate(...) }
```

Adds build complexity, not recommended.

### Verification

Benchmark the non-predicate path before and after implementation:

```bash
# Before implementation
cargo bench --bench read_throughput -- baseline

# After implementation
cargo bench --bench read_throughput -- with_predicate_support

# Compare: should be within noise margin (<1% difference)
```

---

## Benefits Analysis

### Memory Reduction

For a query with 10% selectivity (90% of rows filtered):

| Stage          | Without Pushdown | With Pushdown |
| -------------- | ---------------- | ------------- |
| Decode         | 100%             | 100%          |
| Arrow builders | 100%             | 10%           |
| DataFrame      | 100%             | 10%           |
| Filter result  | 10%              | 10%           |

**Peak memory reduction: ~80%** for highly selective queries.

### CPU Reduction

| Operation       | Without Pushdown | With Pushdown |
| --------------- | ---------------- | ------------- |
| Avro decode     | 100%             | 100%          |
| Predicate eval  | 100% (Python)    | 100% (Rust)   |
| Arrow append    | 100%             | 10%           |
| DataFrame build | 100%             | 10%           |
| Python filter   | 100%             | 0%            |

**CPU reduction: 20-40%** depending on selectivity and schema complexity.

### Cache Efficiency

Smaller Arrow arrays = better cache utilization:
- Fewer cache misses during downstream operations
- Better memory bandwidth utilization
- Improved performance for subsequent query operations

---

## Complexity and Risk Assessment

### Approach 1: Filter During Arrow Building

| Factor                    | Assessment |
| ------------------------- | ---------- |
| Implementation complexity | Medium     |
| Integration complexity    | Low        |
| Testing complexity        | Medium     |
| Semantic risk             | Low        |
| Performance risk          | Low        |
| Maintenance burden        | Low        |

### Approach 2: Compile Predicate to Rust

| Factor                    | Assessment |
| ------------------------- | ---------- |
| Implementation complexity | High       |
| Integration complexity    | Medium     |
| Testing complexity        | High       |
| Semantic risk             | Medium     |
| Performance risk          | Low        |
| Maintenance burden        | Medium     |

### Combined Risk Matrix

| Risk                                  | Likelihood | Impact | Mitigation                   |
| ------------------------------------- | ---------- | ------ | ---------------------------- |
| Semantic mismatch with Polars         | Medium     | High   | Extensive comparison testing |
| Performance regression (no predicate) | Low        | High   | Benchmark verification       |
| Polars Expr format changes            | Medium     | Medium | Version pinning, fallback    |
| Complex type handling bugs            | Medium     | Medium | Incremental rollout          |
| Memory overhead from temp values      | Low        | Low    | Profiling, optimization      |

---

## Testing Strategy

### Unit Tests

1. **Predicate compilation tests**
   - Each predicate type compiles correctly
   - Invalid predicates return appropriate errors
   - Edge cases (empty strings, null values, boundary numbers)

2. **Predicate evaluation tests**
   - Each predicate type evaluates correctly on decoded values
   - AND/OR/NOT combinations
   - Type coercion edge cases

3. **Zero-overhead verification**
   - Compile-time verification that NoPredicate path is optimized
   - Runtime benchmark comparison

### Integration Tests

1. **Correctness tests**
   ```python
   def test_predicate_pushdown_correctness():
       # Read with predicate pushdown
       df_pushdown = jetliner.scan(file, predicate=pred).collect()

       # Read without pushdown, filter in Python
       df_baseline = jetliner.scan(file).collect().filter(pred)

       # Must be identical
       assert_frame_equal(df_pushdown, df_baseline)
   ```

2. **Selectivity sweep**
   - Test with 0%, 10%, 50%, 90%, 100% selectivity
   - Verify correct row counts and values

3. **Type coverage**
   - Test predicates on each supported column type
   - Test nullable columns
   - Test nested types (if supported)

### Property Tests

```rust
#[test]
fn predicate_evaluation_matches_polars(
    records: Vec<TestRecord>,
    predicate: ArbitraryPredicate,
) {
    // Evaluate predicate in Rust
    let rust_results: Vec<bool> = records.iter()
        .map(|r| predicate.matches(r))
        .collect();

    // Evaluate same predicate via Polars
    let df = records_to_dataframe(&records);
    let polars_results = df.filter(predicate.to_polars_expr())
        .column("__row_idx__")
        .to_vec();

    // Must match
    assert_eq!(rust_results, polars_results);
}
```

### Performance Tests

1. **Regression test (no predicate)**
   ```python
   def test_no_predicate_no_regression():
       # Baseline: current implementation
       t_baseline = benchmark(lambda: jetliner.scan(file).collect())

       # New: with predicate support but no predicate
       t_new = benchmark(lambda: jetliner.scan(file).collect())

       # Must be within 2% (noise margin)
       assert abs(t_new - t_baseline) / t_baseline < 0.02
   ```

2. **Benefit test (with predicate)**
   ```python
   def test_predicate_pushdown_benefit():
       pred = pl.col("id") < 100  # 10% selectivity

       # Without pushdown (current)
       t_without = benchmark(lambda:
           jetliner.scan(file).collect().filter(pred))

       # With pushdown
       t_with = benchmark(lambda:
           jetliner.scan(file, predicate=pred).collect())

       # Should be faster
       assert t_with < t_without * 0.9  # At least 10% improvement
   ```

---

## Implementation Roadmap

### Phase 1: Foundation (Week 1)

1. Define `DecodedValue` enum for temporary storage
2. Define `RecordPredicate` trait
3. Implement `AlwaysMatch` (no-op predicate)
4. Add generic parameter to `RecordDecoder`
5. Verify zero overhead via benchmarks

### Phase 2: Simple Predicates (Week 2)

1. Define `SimplePredicate` enum
2. Implement predicate compilation for:
   - Equality (`==`, `!=`)
   - Comparison (`<`, `<=`, `>`, `>=`)
   - Null checks (`is_null`, `is_not_null`)
3. Add Python-side predicate extraction (basic)
4. Integration tests for simple predicates

### Phase 3: Compound Predicates (Week 3)

1. Implement AND/OR/NOT compilation
2. Implement `is_in` predicate
3. Extend Python extraction for compound predicates
4. Property tests for predicate evaluation

### Phase 4: Integration & Polish (Week 4)

1. Wire up to `scan()` API
2. Implement fallback for unsupported predicates
3. Performance benchmarking
4. Documentation
5. Edge case testing

---

## Alternatives Considered

### Alternative 1: Use Polars Expression Engine Directly

**Idea:** Pass the Polars `Expr` to Rust and use Polars' own evaluation.

**Why rejected:**
- Polars expressions operate on Series/DataFrame, not individual values
- Would require building mini-DataFrames per record (defeats purpose)
- Tight coupling to Polars internals

### Alternative 2: JIT Compilation (e.g., Cranelift)

**Idea:** JIT compile predicates to native code for maximum performance.

**Why rejected:**
- Massive complexity increase
- Compilation overhead may exceed evaluation savings
- Overkill for typical predicate complexity

### Alternative 3: SIMD Predicate Evaluation

**Idea:** Evaluate predicates on batches of decoded values using SIMD.

**Why rejected:**
- Requires batch-oriented decode (architectural change)
- Complex for variable-length types (strings)
- May be a future optimization on top of this work

---

## Conclusion

Predicate pushdown is feasible and worthwhile for Jetliner. The recommended approach is:

1. **Start with Approach 1** (filter during Arrow building) as it's lower risk and provides immediate benefit
2. **Layer Approach 2** (predicate compilation) on top for additional performance
3. **Maintain strict zero-overhead guarantee** for non-predicate paths
4. **Implement comprehensive testing** to ensure semantic correctness

Expected outcome: 20-40% performance improvement for selective queries with no regression for non-selective queries.

---

## References

### Internal
- `src/reader/record_decoder.rs` — Current record decoding implementation
- `src/reader/decode.rs` — Primitive value decoders
- `src/convert/dataframe.rs` — DataFrame builder
- `python/jetliner/__init__.py` — Current predicate handling in `scan()`

### External
- [Polars IO Plugins Guide](https://docs.pola.rs/user-guide/plugins/io_plugins/)
- [Polars Expr.meta.serialize](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.meta.serialize.html)
- [Apache Arrow Predicate Pushdown](https://arrow.apache.org/docs/cpp/compute.html)
- [DataFusion Physical Expressions](https://docs.rs/datafusion/latest/datafusion/physical_expr/)

---

## Document History

- **2026-01-30:** Initial feasibility analysis
- **Author:** Claude (assisted by human review)
- **Status:** Draft — awaiting review and prioritization
