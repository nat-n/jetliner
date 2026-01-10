# Task 10: Spec Compliance Review Decisions

## Context

During checkpoint 10, a comprehensive review of the Avro 1.11.1 specification was conducted against the implementation to identify compliance gaps.

## Key Decision: Optional Schema Validation

**Problem:** The Avro spec mandates several schema validation rules:
- Union types cannot contain duplicate types
- Union types cannot be nested (no union-of-union)
- Names must follow Avro naming rules (start with letter/underscore, alphanumeric thereafter)
- Default values for union fields must match the first union branch

**Decision:** Schema validation should be **optional** via a `strict_schema` flag, defaulting to permissive (false).

**Rationale:** Jetliner is a **read-only** library. For readers:
1. Permissive parsing maximizes compatibility with existing files that may have been written by non-compliant writers
2. The data is already written - rejecting it doesn't help the user
3. Strict validation matters more for writers (to prevent creating non-compliant files)
4. Users who want validation can enable `strict_schema: true`

**Implementation:** Task 10.3 implements this as:
- `strict_schema: bool` parameter in schema parser (default: false)
- When enabled: validates union rules and naming conventions
- When disabled: logs warnings but continues parsing

## Other Compliance Fixes

The review identified several required fixes (not optional):

1. **Snappy CRC32C validation** (10.1) - Required for data integrity
2. **"zstandard" codec alias** (10.2) - Spec uses "zstandard", not "zstd"
3. **Local timestamp distinction** (10.4) - Arrow type mapping must distinguish UTC vs local
4. **Varint consolidation** (10.5) - Code quality, not spec compliance

## References

- Apache Avro 1.11.1 Specification: https://avro.apache.org/docs/1.11.1/specification/
- Task 10 subtasks in tasks.md
