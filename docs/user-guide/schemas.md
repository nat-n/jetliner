# Schema Inspection

Jetliner provides multiple ways to inspect Avro schemas and understand how they map to Polars types.

## Accessing Schemas

### From an Open Reader

The `open()` API provides schema access before reading data:

```python
import jetliner

with jetliner.AvroReader("data.avro") as reader:
    # JSON string representation
    print(reader.schema)

    # Python dictionary
    schema_dict = reader.schema_dict
    print(schema_dict)
```

### Using read_avro_schema()

Get the Polars schema without reading any data:

```python
import jetliner

# Returns a Polars schema (dict of column names to types)
polars_schema = jetliner.read_avro_schema("data.avro")
print(polars_schema)
# {'user_id': Int64, 'name': String, 'amount': Float64, ...}
```

This only reads the file header, making it fast even for large files.

### From S3

Schema inspection works with S3 files:

```python
import jetliner

polars_schema = jetliner.read_avro_schema(
    "s3://bucket/data.avro",
    storage_options={"region": "us-east-1"}
)
```

## Schema Formats

### JSON String

The raw Avro schema as a JSON string:

```python
with jetliner.AvroReader("data.avro") as reader:
    schema_json = reader.schema
    print(schema_json)
```

Output:
```json
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"]}
  ]
}
```

### Python Dictionary

Parsed schema as a Python dictionary:

```python
with jetliner.AvroReader("data.avro") as reader:
    schema = reader.schema_dict

    print(f"Record name: {schema['name']}")
    print(f"Fields: {[f['name'] for f in schema['fields']]}")
```

### Polars Schema

Column names mapped to Polars data types:

```python
import jetliner

schema = jetliner.read_avro_schema("data.avro")
for name, dtype in schema.items():
    print(f"{name}: {dtype}")
```

## Type Mapping

Jetliner maps Avro types to Polars types:

### Primitive Types

| Avro Type | Polars Type |
| --------- | ----------- |
| `null`    | `Null`      |
| `boolean` | `Boolean`   |
| `int`     | `Int32`     |
| `long`    | `Int64`     |
| `float`   | `Float32`   |
| `double`  | `Float64`   |
| `bytes`   | `Binary`    |
| `string`  | `String`    |

### Logical Types

| Avro Logical Type  | Polars Type    |
| ------------------ | -------------- |
| `date`             | `Date`         |
| `time-millis`      | `Time`         |
| `time-micros`      | `Time`         |
| `timestamp-millis` | `Datetime(ms)` |
| `timestamp-micros` | `Datetime(μs)` |
| `uuid`             | `String`       |
| `decimal`          | `Decimal`      |

### Complex Types

| Avro Type | Polars Type                      |
| --------- | -------------------------------- |
| `array`   | `List`                           |
| `map`     | `Struct` (with key/value fields) |
| `record`  | `Struct`                         |
| `enum`    | `Categorical`                    |
| `fixed`   | `Binary`                         |
| `union`   | Depends on variants              |

### Union Types

Unions are handled based on their variants:

```python
# ["null", "string"] -> String (nullable)
# ["null", "int", "string"] -> Struct with type indicator
```

## Working with Schemas

### Validate Schema Before Processing

```python
import jetliner

def validate_schema(path, required_columns):
    """Check that file has required columns."""
    schema = jetliner.read_avro_schema(path)
    missing = set(required_columns) - set(schema.keys())
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    return schema

# Usage
schema = validate_schema("data.avro", ["user_id", "amount"])
```

### Schema-Driven Processing

```python
import jetliner
import polars as pl

def process_by_schema(path):
    """Process file based on its schema."""
    schema = jetliner.read_avro_schema(path)

    # Build query based on available columns
    numeric_cols = [
        name for name, dtype in schema.items()
        if dtype in (pl.Int32, pl.Int64, pl.Float32, pl.Float64)
    ]

    return (
        jetliner.scan_avro(path)
        .select(numeric_cols)
        .collect()
    )
```

### Compare Schemas

```python
import jetliner

def schemas_compatible(path1, path2):
    """Check if two files have compatible schemas."""
    schema1 = jetliner.read_avro_schema(path1)
    schema2 = jetliner.read_avro_schema(path2)

    # Check column names match
    if set(schema1.keys()) != set(schema2.keys()):
        return False

    # Check types match
    for name in schema1:
        if schema1[name] != schema2[name]:
            return False

    return True
```

## Nested Schemas

### Records (Structs)

Nested records become Polars Structs:

```python
# Avro schema:
# {"type": "record", "name": "Event", "fields": [
#   {"name": "user", "type": {
#     "type": "record", "name": "User", "fields": [
#       {"name": "id", "type": "long"},
#       {"name": "name", "type": "string"}
#     ]
#   }}
# ]}

df = jetliner.scan_avro("events.avro").collect()
# Access nested fields
df.select(pl.col("user").struct.field("name"))
```

### Arrays (Lists)

Arrays become Polars Lists:

```python
# Avro schema:
# {"name": "tags", "type": {"type": "array", "items": "string"}}

df = jetliner.scan_avro("data.avro").collect()
# Explode list column
df.explode("tags")
```

### Maps

Maps become Structs with key/value arrays:

```python
# Avro schema:
# {"name": "metadata", "type": {"type": "map", "values": "string"}}

df = jetliner.scan_avro("data.avro").collect()
# Access map as struct
df.select(pl.col("metadata"))
```

## Recursive Types

Avro supports recursive types (self-referencing records). Since Polars doesn't support recursive structures, Jetliner serializes recursive fields to JSON strings:

```python
# Avro schema with recursive type:
# {"type": "record", "name": "Node", "fields": [
#   {"name": "value", "type": "int"},
#   {"name": "children", "type": {"type": "array", "items": "Node"}}
# ]}

df = jetliner.scan_avro("tree.avro").collect()
# "children" column contains JSON strings
# Parse with: df.select(pl.col("children").str.json_decode())
```

## Known Limitations

### Top-Level Non-Record Schemas

Jetliner handles some non-record top-level schemas:

| Top-Level Type                | Support                 |
| ----------------------------- | ----------------------- |
| `record`                      | ✅ Full support          |
| `int`, `long`, `string`, etc. | ✅ Single "value" column |
| `array`                       | ❌ Not yet supported     |
| `map`                         | ❌ Not yet supported     |

### Schema Evolution

Jetliner reads the writer schema embedded in the file. Schema evolution (reader schema different from writer schema) is not currently supported.

## Next Steps

- [Codec Support](codecs.md) - Compression options
- [Error Handling](error-handling.md) - Handle schema errors
- [Query Optimization](query-optimization.md) - Use schema for efficient queries
