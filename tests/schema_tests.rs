//! Tests for Avro schema types and parsing.

use jetliner::schema::*;

// ============================================================================
// Schema Type Tests
// ============================================================================

#[test]
fn test_primitive_types() {
    assert!(AvroSchema::Null.is_primitive());
    assert!(AvroSchema::Boolean.is_primitive());
    assert!(AvroSchema::Int.is_primitive());
    assert!(AvroSchema::Long.is_primitive());
    assert!(AvroSchema::Float.is_primitive());
    assert!(AvroSchema::Double.is_primitive());
    assert!(AvroSchema::Bytes.is_primitive());
    assert!(AvroSchema::String.is_primitive());
}

// ============================================================================
// Parser Tests - Primitive Types
// ============================================================================

#[test]
fn test_parse_primitive_string_schemas() {
    assert_eq!(parse_schema(r#""null""#).unwrap(), AvroSchema::Null);
    assert_eq!(parse_schema(r#""boolean""#).unwrap(), AvroSchema::Boolean);
    assert_eq!(parse_schema(r#""int""#).unwrap(), AvroSchema::Int);
    assert_eq!(parse_schema(r#""long""#).unwrap(), AvroSchema::Long);
    assert_eq!(parse_schema(r#""float""#).unwrap(), AvroSchema::Float);
    assert_eq!(parse_schema(r#""double""#).unwrap(), AvroSchema::Double);
    assert_eq!(parse_schema(r#""bytes""#).unwrap(), AvroSchema::Bytes);
    assert_eq!(parse_schema(r#""string""#).unwrap(), AvroSchema::String);
}

#[test]
fn test_parse_primitive_object_schemas() {
    assert_eq!(
        parse_schema(r#"{"type": "null"}"#).unwrap(),
        AvroSchema::Null
    );
    assert_eq!(parse_schema(r#"{"type": "int"}"#).unwrap(), AvroSchema::Int);
    assert_eq!(
        parse_schema(r#"{"type": "string"}"#).unwrap(),
        AvroSchema::String
    );
}

#[test]
fn test_record_schema() {
    let fields = vec![
        FieldSchema::new("id", AvroSchema::Long),
        FieldSchema::new("name", AvroSchema::String),
    ];
    let record = RecordSchema::new("User", fields).with_namespace("com.example");

    assert_eq!(record.name, "User");
    assert_eq!(record.namespace, Some("com.example".to_string()));
    assert_eq!(record.fullname(), "com.example.User");
    assert_eq!(record.fields.len(), 2);
}

// ============================================================================
// Parser Tests - Record Schema
// ============================================================================

#[test]
fn test_parse_simple_record() {
    let json = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Record(r) => {
            assert_eq!(r.name, "User");
            assert_eq!(r.fields.len(), 2);
            assert_eq!(r.fields[0].name, "id");
            assert_eq!(r.fields[0].schema, AvroSchema::Long);
            assert_eq!(r.fields[1].name, "name");
            assert_eq!(r.fields[1].schema, AvroSchema::String);
        }
        _ => panic!("Expected Record schema"),
    }
}

#[test]
fn test_parse_record_with_namespace() {
    let json = r#"{
        "type": "record",
        "name": "User",
        "namespace": "com.example",
        "doc": "A user record",
        "fields": [
            {"name": "id", "type": "long"}
        ]
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Record(r) => {
            assert_eq!(r.name, "User");
            assert_eq!(r.namespace, Some("com.example".to_string()));
            assert_eq!(r.fullname(), "com.example.User");
            assert_eq!(r.doc, Some("A user record".to_string()));
        }
        _ => panic!("Expected Record schema"),
    }
}

#[test]
fn test_parse_record_with_field_defaults() {
    let json = r#"{
        "type": "record",
        "name": "Config",
        "fields": [
            {"name": "count", "type": "int", "default": 0},
            {"name": "enabled", "type": "boolean", "default": true}
        ]
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Record(r) => {
            assert_eq!(r.fields[0].default, Some(serde_json::json!(0)));
            assert_eq!(r.fields[1].default, Some(serde_json::json!(true)));
        }
        _ => panic!("Expected Record schema"),
    }
}

#[test]
fn test_parse_nested_record() {
    let json = r#"{
        "type": "record",
        "name": "Person",
        "fields": [
            {"name": "name", "type": "string"},
            {
                "name": "address",
                "type": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                        {"name": "street", "type": "string"},
                        {"name": "city", "type": "string"}
                    ]
                }
            }
        ]
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Record(r) => {
            assert_eq!(r.name, "Person");
            assert_eq!(r.fields.len(), 2);
            match &r.fields[1].schema {
                AvroSchema::Record(addr) => {
                    assert_eq!(addr.name, "Address");
                    assert_eq!(addr.fields.len(), 2);
                }
                _ => panic!("Expected nested Record schema"),
            }
        }
        _ => panic!("Expected Record schema"),
    }
}

#[test]
fn test_enum_schema() {
    let symbols = vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()];
    let enum_schema = EnumSchema::new("Color", symbols);

    assert_eq!(enum_schema.symbol_index("RED"), Some(0));
    assert_eq!(enum_schema.symbol_index("GREEN"), Some(1));
    assert_eq!(enum_schema.symbol_index("BLUE"), Some(2));
    assert_eq!(enum_schema.symbol_index("YELLOW"), None);
}

// ============================================================================
// Parser Tests - Enum Schema
// ============================================================================

#[test]
fn test_parse_enum() {
    let json = r#"{
        "type": "enum",
        "name": "Color",
        "symbols": ["RED", "GREEN", "BLUE"]
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Enum(e) => {
            assert_eq!(e.name, "Color");
            assert_eq!(e.symbols, vec!["RED", "GREEN", "BLUE"]);
        }
        _ => panic!("Expected Enum schema"),
    }
}

#[test]
fn test_parse_enum_with_namespace() {
    let json = r#"{
        "type": "enum",
        "name": "Status",
        "namespace": "com.example",
        "doc": "Status codes",
        "symbols": ["OK", "ERROR"]
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Enum(e) => {
            assert_eq!(e.name, "Status");
            assert_eq!(e.namespace, Some("com.example".to_string()));
            assert_eq!(e.fullname(), "com.example.Status");
            assert_eq!(e.doc, Some("Status codes".to_string()));
        }
        _ => panic!("Expected Enum schema"),
    }
}

#[test]
fn test_fixed_schema() {
    let fixed = FixedSchema::new("MD5", 16).with_namespace("com.example");

    assert_eq!(fixed.name, "MD5");
    assert_eq!(fixed.size, 16);
    assert_eq!(fixed.fullname(), "com.example.MD5");
}

// ============================================================================
// Parser Tests - Fixed Schema
// ============================================================================

#[test]
fn test_parse_fixed() {
    let json = r#"{
        "type": "fixed",
        "name": "MD5",
        "size": 16
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Fixed(f) => {
            assert_eq!(f.name, "MD5");
            assert_eq!(f.size, 16);
        }
        _ => panic!("Expected Fixed schema"),
    }
}

#[test]
fn test_parse_fixed_with_namespace() {
    let json = r#"{
        "type": "fixed",
        "name": "Hash",
        "namespace": "com.example",
        "size": 32
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Fixed(f) => {
            assert_eq!(f.name, "Hash");
            assert_eq!(f.namespace, Some("com.example".to_string()));
            assert_eq!(f.size, 32);
        }
        _ => panic!("Expected Fixed schema"),
    }
}

#[test]
fn test_logical_type() {
    let decimal = LogicalType::new(
        AvroSchema::Bytes,
        LogicalTypeName::Decimal {
            precision: 10,
            scale: 2,
        },
    );

    assert_eq!(decimal.logical_type.name(), "decimal");
    assert_eq!(*decimal.base, AvroSchema::Bytes);
}

// ============================================================================
// Parser Tests - Logical Types
// ============================================================================

#[test]
fn test_parse_decimal_logical_type() {
    let json = r#"{
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Logical(lt) => {
            assert_eq!(*lt.base, AvroSchema::Bytes);
            match lt.logical_type {
                LogicalTypeName::Decimal { precision, scale } => {
                    assert_eq!(precision, 10);
                    assert_eq!(scale, 2);
                }
                _ => panic!("Expected Decimal logical type"),
            }
        }
        _ => panic!("Expected Logical schema"),
    }
}

#[test]
fn test_parse_date_logical_type() {
    let json = r#"{
        "type": "int",
        "logicalType": "date"
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Logical(lt) => {
            assert_eq!(*lt.base, AvroSchema::Int);
            assert!(matches!(lt.logical_type, LogicalTypeName::Date));
        }
        _ => panic!("Expected Logical schema"),
    }
}

#[test]
fn test_parse_timestamp_logical_types() {
    let millis_json = r#"{"type": "long", "logicalType": "timestamp-millis"}"#;
    let micros_json = r#"{"type": "long", "logicalType": "timestamp-micros"}"#;

    let millis = parse_schema(millis_json).unwrap();
    let micros = parse_schema(micros_json).unwrap();

    match millis {
        AvroSchema::Logical(lt) => {
            assert!(matches!(lt.logical_type, LogicalTypeName::TimestampMillis));
        }
        _ => panic!("Expected Logical schema"),
    }

    match micros {
        AvroSchema::Logical(lt) => {
            assert!(matches!(lt.logical_type, LogicalTypeName::TimestampMicros));
        }
        _ => panic!("Expected Logical schema"),
    }
}

#[test]
fn test_parse_uuid_logical_type() {
    let json = r#"{"type": "string", "logicalType": "uuid"}"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Logical(lt) => {
            assert_eq!(*lt.base, AvroSchema::String);
            assert!(matches!(lt.logical_type, LogicalTypeName::Uuid));
        }
        _ => panic!("Expected Logical schema"),
    }
}

#[test]
fn test_parse_time_logical_types() {
    let millis_json = r#"{"type": "int", "logicalType": "time-millis"}"#;
    let micros_json = r#"{"type": "long", "logicalType": "time-micros"}"#;

    let millis = parse_schema(millis_json).unwrap();
    let micros = parse_schema(micros_json).unwrap();

    match millis {
        AvroSchema::Logical(lt) => {
            assert!(matches!(lt.logical_type, LogicalTypeName::TimeMillis));
        }
        _ => panic!("Expected Logical schema"),
    }

    match micros {
        AvroSchema::Logical(lt) => {
            assert!(matches!(lt.logical_type, LogicalTypeName::TimeMicros));
        }
        _ => panic!("Expected Logical schema"),
    }
}

#[test]
fn test_parse_duration_logical_type() {
    let json = r#"{
        "type": "fixed",
        "name": "duration",
        "size": 12,
        "logicalType": "duration"
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Logical(lt) => {
            match &*lt.base {
                AvroSchema::Fixed(f) => {
                    assert_eq!(f.size, 12);
                }
                _ => panic!("Expected Fixed base type"),
            }
            assert!(matches!(lt.logical_type, LogicalTypeName::Duration));
        }
        _ => panic!("Expected Logical schema"),
    }
}

#[test]
fn test_parse_unknown_logical_type_returns_base() {
    // Unknown logical types should be ignored per Avro spec
    let json = r#"{"type": "string", "logicalType": "unknown-type"}"#;

    let schema = parse_schema(json).unwrap();
    assert_eq!(schema, AvroSchema::String);
}

#[test]
fn test_nullable_union() {
    let nullable_string = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]);

    assert!(nullable_string.is_nullable());
    assert_eq!(nullable_string.nullable_inner(), Some(&AvroSchema::String));

    let non_nullable = AvroSchema::String;
    assert!(!non_nullable.is_nullable());
    assert_eq!(non_nullable.nullable_inner(), None);
}

// ============================================================================
// Parser Tests - Array and Map
// ============================================================================

#[test]
fn test_parse_array() {
    let json = r#"{"type": "array", "items": "string"}"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Array(items) => {
            assert_eq!(*items, AvroSchema::String);
        }
        _ => panic!("Expected Array schema"),
    }
}

#[test]
fn test_parse_array_of_records() {
    let json = r#"{
        "type": "array",
        "items": {
            "type": "record",
            "name": "Item",
            "fields": [{"name": "value", "type": "int"}]
        }
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Array(items) => match &*items {
            AvroSchema::Record(r) => {
                assert_eq!(r.name, "Item");
            }
            _ => panic!("Expected Record in array"),
        },
        _ => panic!("Expected Array schema"),
    }
}

#[test]
fn test_parse_map() {
    let json = r#"{"type": "map", "values": "long"}"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Map(values) => {
            assert_eq!(*values, AvroSchema::Long);
        }
        _ => panic!("Expected Map schema"),
    }
}

#[test]
fn test_parse_map_of_arrays() {
    let json = r#"{
        "type": "map",
        "values": {"type": "array", "items": "string"}
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Map(values) => match &*values {
            AvroSchema::Array(items) => {
                assert_eq!(**items, AvroSchema::String);
            }
            _ => panic!("Expected Array in map values"),
        },
        _ => panic!("Expected Map schema"),
    }
}

// ============================================================================
// Parser Tests - Union
// ============================================================================

#[test]
fn test_parse_union() {
    let json = r#"["null", "string"]"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Union(variants) => {
            assert_eq!(variants.len(), 2);
            assert_eq!(variants[0], AvroSchema::Null);
            assert_eq!(variants[1], AvroSchema::String);
        }
        _ => panic!("Expected Union schema"),
    }
}

#[test]
fn test_parse_complex_union() {
    let json = r#"["null", "string", "int", {"type": "array", "items": "long"}]"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Union(variants) => {
            assert_eq!(variants.len(), 4);
            assert_eq!(variants[0], AvroSchema::Null);
            assert_eq!(variants[1], AvroSchema::String);
            assert_eq!(variants[2], AvroSchema::Int);
            match &variants[3] {
                AvroSchema::Array(items) => {
                    assert_eq!(**items, AvroSchema::Long);
                }
                _ => panic!("Expected Array in union"),
            }
        }
        _ => panic!("Expected Union schema"),
    }
}

#[test]
fn test_named_types() {
    let record = AvroSchema::Record(RecordSchema::new("Test", vec![]));
    assert!(record.is_named());
    assert_eq!(record.name(), Some("Test"));

    let enum_type = AvroSchema::Enum(EnumSchema::new("Status", vec!["OK".to_string()]));
    assert!(enum_type.is_named());

    let fixed = AvroSchema::Fixed(FixedSchema::new("Hash", 32));
    assert!(fixed.is_named());

    assert!(!AvroSchema::Int.is_named());
}

#[test]
fn test_field_schema_with_default() {
    let field = FieldSchema::new("count", AvroSchema::Int)
        .with_default(serde_json::json!(0))
        .with_doc("The count value");

    assert_eq!(field.name, "count");
    assert_eq!(field.default, Some(serde_json::json!(0)));
    assert_eq!(field.doc, Some("The count value".to_string()));
}

// ============================================================================
// Parser Tests - Named Type References
// ============================================================================

#[test]
fn test_parse_named_type_reference() {
    let json = r#"{
        "type": "record",
        "name": "LinkedList",
        "fields": [
            {"name": "value", "type": "int"},
            {"name": "next", "type": ["null", "LinkedList"]}
        ]
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Record(r) => {
            assert_eq!(r.name, "LinkedList");
            match &r.fields[1].schema {
                AvroSchema::Union(variants) => {
                    assert_eq!(variants.len(), 2);
                    match &variants[1] {
                        AvroSchema::Named(name) => {
                            assert_eq!(name, "LinkedList");
                        }
                        _ => panic!("Expected Named reference"),
                    }
                }
                _ => panic!("Expected Union schema"),
            }
        }
        _ => panic!("Expected Record schema"),
    }
}

#[test]
fn test_parse_named_type_with_namespace_resolution() {
    let json = r#"{
        "type": "record",
        "name": "Outer",
        "namespace": "com.example",
        "fields": [
            {
                "name": "inner",
                "type": {
                    "type": "record",
                    "name": "Inner",
                    "fields": [{"name": "value", "type": "int"}]
                }
            },
            {"name": "ref", "type": "Inner"}
        ]
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Record(r) => {
            assert_eq!(r.fullname(), "com.example.Outer");
            // The reference to "Inner" should resolve to "com.example.Inner"
            match &r.fields[1].schema {
                AvroSchema::Named(name) => {
                    assert_eq!(name, "com.example.Inner");
                }
                _ => panic!("Expected Named reference"),
            }
        }
        _ => panic!("Expected Record schema"),
    }
}

// ============================================================================
// Parser Tests - Error Cases
// ============================================================================

#[test]
fn test_parse_invalid_json() {
    let result = parse_schema("not valid json");
    assert!(result.is_err());
}

#[test]
fn test_parse_empty_union() {
    let result = parse_schema("[]");
    assert!(result.is_err());
}

#[test]
fn test_parse_record_missing_name() {
    let json = r#"{"type": "record", "fields": []}"#;
    let result = parse_schema(json);
    assert!(result.is_err());
}

#[test]
fn test_parse_record_missing_fields() {
    let json = r#"{"type": "record", "name": "Test"}"#;
    let result = parse_schema(json);
    assert!(result.is_err());
}

#[test]
fn test_parse_enum_missing_symbols() {
    let json = r#"{"type": "enum", "name": "Test"}"#;
    let result = parse_schema(json);
    assert!(result.is_err());
}

#[test]
fn test_parse_enum_empty_symbols() {
    let json = r#"{"type": "enum", "name": "Test", "symbols": []}"#;
    let result = parse_schema(json);
    assert!(result.is_err());
}

#[test]
fn test_parse_fixed_missing_size() {
    let json = r#"{"type": "fixed", "name": "Test"}"#;
    let result = parse_schema(json);
    assert!(result.is_err());
}

#[test]
fn test_parse_array_missing_items() {
    let json = r#"{"type": "array"}"#;
    let result = parse_schema(json);
    assert!(result.is_err());
}

#[test]
fn test_parse_map_missing_values() {
    let json = r#"{"type": "map"}"#;
    let result = parse_schema(json);
    assert!(result.is_err());
}

#[test]
fn test_parse_decimal_missing_precision() {
    let json = r#"{"type": "bytes", "logicalType": "decimal"}"#;
    let result = parse_schema(json);
    assert!(result.is_err());
}

// ============================================================================
// Parser Tests - Complex Real-World Schema
// ============================================================================

#[test]
fn test_parse_complex_schema() {
    let json = r#"{
        "type": "record",
        "name": "Event",
        "namespace": "com.example.events",
        "doc": "An event record",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "type", "type": {"type": "enum", "name": "EventType", "symbols": ["CREATE", "UPDATE", "DELETE"]}},
            {"name": "payload", "type": ["null", "bytes"], "default": null},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {"name": "metadata", "type": {"type": "map", "values": "string"}}
        ]
    }"#;

    let schema = parse_schema(json).unwrap();
    match schema {
        AvroSchema::Record(r) => {
            assert_eq!(r.name, "Event");
            assert_eq!(r.namespace, Some("com.example.events".to_string()));
            assert_eq!(r.fields.len(), 6);

            // Check timestamp field has logical type
            match &r.fields[1].schema {
                AvroSchema::Logical(lt) => {
                    assert!(matches!(lt.logical_type, LogicalTypeName::TimestampMillis));
                }
                _ => panic!("Expected Logical type for timestamp"),
            }

            // Check enum field
            match &r.fields[2].schema {
                AvroSchema::Enum(e) => {
                    assert_eq!(e.name, "EventType");
                    assert_eq!(e.symbols.len(), 3);
                }
                _ => panic!("Expected Enum type"),
            }

            // Check nullable bytes field
            match &r.fields[3].schema {
                AvroSchema::Union(variants) => {
                    assert_eq!(variants.len(), 2);
                    assert!(variants[0] == AvroSchema::Null);
                    assert!(variants[1] == AvroSchema::Bytes);
                }
                _ => panic!("Expected Union type"),
            }

            // Check array field
            match &r.fields[4].schema {
                AvroSchema::Array(items) => {
                    assert_eq!(**items, AvroSchema::String);
                }
                _ => panic!("Expected Array type"),
            }

            // Check map field
            match &r.fields[5].schema {
                AvroSchema::Map(values) => {
                    assert_eq!(**values, AvroSchema::String);
                }
                _ => panic!("Expected Map type"),
            }
        }
        _ => panic!("Expected Record schema"),
    }
}

// ============================================================================
// Pretty Printer Tests (to_json)
// ============================================================================

#[test]
fn test_to_json_primitive_types() {
    assert_eq!(AvroSchema::Null.to_json(), r#""null""#);
    assert_eq!(AvroSchema::Boolean.to_json(), r#""boolean""#);
    assert_eq!(AvroSchema::Int.to_json(), r#""int""#);
    assert_eq!(AvroSchema::Long.to_json(), r#""long""#);
    assert_eq!(AvroSchema::Float.to_json(), r#""float""#);
    assert_eq!(AvroSchema::Double.to_json(), r#""double""#);
    assert_eq!(AvroSchema::Bytes.to_json(), r#""bytes""#);
    assert_eq!(AvroSchema::String.to_json(), r#""string""#);
}

#[test]
fn test_to_json_simple_record() {
    let fields = vec![
        FieldSchema::new("id", AvroSchema::Long),
        FieldSchema::new("name", AvroSchema::String),
    ];
    let record = RecordSchema::new("User", fields);
    let schema = AvroSchema::Record(record);

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "record");
    assert_eq!(parsed["name"], "User");
    assert_eq!(parsed["fields"].as_array().unwrap().len(), 2);
    assert_eq!(parsed["fields"][0]["name"], "id");
    assert_eq!(parsed["fields"][0]["type"], "long");
    assert_eq!(parsed["fields"][1]["name"], "name");
    assert_eq!(parsed["fields"][1]["type"], "string");
}

#[test]
fn test_to_json_record_with_namespace() {
    let fields = vec![FieldSchema::new("id", AvroSchema::Long)];
    let record = RecordSchema::new("User", fields)
        .with_namespace("com.example")
        .with_doc("A user record");
    let schema = AvroSchema::Record(record);

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "record");
    assert_eq!(parsed["name"], "User");
    assert_eq!(parsed["namespace"], "com.example");
    assert_eq!(parsed["doc"], "A user record");
}

#[test]
fn test_to_json_enum() {
    let symbols = vec!["RED".to_string(), "GREEN".to_string(), "BLUE".to_string()];
    let enum_schema = EnumSchema::new("Color", symbols);
    let schema = AvroSchema::Enum(enum_schema);

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "enum");
    assert_eq!(parsed["name"], "Color");
    assert_eq!(
        parsed["symbols"].as_array().unwrap(),
        &vec!["RED", "GREEN", "BLUE"]
    );
}

#[test]
fn test_to_json_fixed() {
    let fixed = FixedSchema::new("MD5", 16);
    let schema = AvroSchema::Fixed(fixed);

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "fixed");
    assert_eq!(parsed["name"], "MD5");
    assert_eq!(parsed["size"], 16);
}

#[test]
fn test_to_json_array() {
    let schema = AvroSchema::Array(Box::new(AvroSchema::String));

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "array");
    assert_eq!(parsed["items"], "string");
}

#[test]
fn test_to_json_map() {
    let schema = AvroSchema::Map(Box::new(AvroSchema::Long));

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "map");
    assert_eq!(parsed["values"], "long");
}

#[test]
fn test_to_json_union() {
    let schema = AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::String]);

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert!(parsed.is_array());
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0], "null");
    assert_eq!(arr[1], "string");
}

#[test]
fn test_to_json_named_reference() {
    let schema = AvroSchema::Named("com.example.User".to_string());

    let json = schema.to_json();
    assert_eq!(json, r#""com.example.User""#);
}

#[test]
fn test_to_json_logical_type_date() {
    let schema = AvroSchema::Logical(LogicalType::new(AvroSchema::Int, LogicalTypeName::Date));

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "int");
    assert_eq!(parsed["logicalType"], "date");
}

#[test]
fn test_to_json_logical_type_timestamp() {
    let schema = AvroSchema::Logical(LogicalType::new(
        AvroSchema::Long,
        LogicalTypeName::TimestampMillis,
    ));

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "long");
    assert_eq!(parsed["logicalType"], "timestamp-millis");
}

#[test]
fn test_to_json_logical_type_decimal() {
    let schema = AvroSchema::Logical(LogicalType::new(
        AvroSchema::Bytes,
        LogicalTypeName::Decimal {
            precision: 10,
            scale: 2,
        },
    ));

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "bytes");
    assert_eq!(parsed["logicalType"], "decimal");
    assert_eq!(parsed["precision"], 10);
    assert_eq!(parsed["scale"], 2);
}

#[test]
fn test_to_json_logical_type_duration() {
    let fixed = FixedSchema::new("duration", 12);
    let schema = AvroSchema::Logical(LogicalType::new(
        AvroSchema::Fixed(fixed),
        LogicalTypeName::Duration,
    ));

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "fixed");
    assert_eq!(parsed["name"], "duration");
    assert_eq!(parsed["size"], 12);
    assert_eq!(parsed["logicalType"], "duration");
}

#[test]
fn test_to_json_field_with_default() {
    let field = FieldSchema::new("count", AvroSchema::Int)
        .with_default(serde_json::json!(0))
        .with_doc("The count value");

    let json_value = field.to_json_value();

    assert_eq!(json_value["name"], "count");
    assert_eq!(json_value["type"], "int");
    assert_eq!(json_value["default"], 0);
    assert_eq!(json_value["doc"], "The count value");
}

#[test]
fn test_to_json_nested_record() {
    let address_fields = vec![
        FieldSchema::new("street", AvroSchema::String),
        FieldSchema::new("city", AvroSchema::String),
    ];
    let address = RecordSchema::new("Address", address_fields);

    let person_fields = vec![
        FieldSchema::new("name", AvroSchema::String),
        FieldSchema::new("address", AvroSchema::Record(address)),
    ];
    let person = RecordSchema::new("Person", person_fields);
    let schema = AvroSchema::Record(person);

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["name"], "Person");
    assert_eq!(parsed["fields"][1]["name"], "address");
    assert_eq!(parsed["fields"][1]["type"]["type"], "record");
    assert_eq!(parsed["fields"][1]["type"]["name"], "Address");
}

#[test]
fn test_to_json_complex_schema() {
    // Build a complex schema similar to the parser test
    let event_type = EnumSchema::new(
        "EventType",
        vec![
            "CREATE".to_string(),
            "UPDATE".to_string(),
            "DELETE".to_string(),
        ],
    );

    let fields = vec![
        FieldSchema::new("id", AvroSchema::String),
        FieldSchema::new(
            "timestamp",
            AvroSchema::Logical(LogicalType::new(
                AvroSchema::Long,
                LogicalTypeName::TimestampMillis,
            )),
        ),
        FieldSchema::new("type", AvroSchema::Enum(event_type)),
        FieldSchema::new(
            "payload",
            AvroSchema::Union(vec![AvroSchema::Null, AvroSchema::Bytes]),
        ),
        FieldSchema::new("tags", AvroSchema::Array(Box::new(AvroSchema::String))),
        FieldSchema::new("metadata", AvroSchema::Map(Box::new(AvroSchema::String))),
    ];

    let record = RecordSchema::new("Event", fields)
        .with_namespace("com.example.events")
        .with_doc("An event record");
    let schema = AvroSchema::Record(record);

    let json = schema.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"], "record");
    assert_eq!(parsed["name"], "Event");
    assert_eq!(parsed["namespace"], "com.example.events");
    assert_eq!(parsed["doc"], "An event record");
    assert_eq!(parsed["fields"].as_array().unwrap().len(), 6);

    // Check timestamp field
    assert_eq!(parsed["fields"][1]["type"]["type"], "long");
    assert_eq!(
        parsed["fields"][1]["type"]["logicalType"],
        "timestamp-millis"
    );

    // Check enum field
    assert_eq!(parsed["fields"][2]["type"]["type"], "enum");
    assert_eq!(parsed["fields"][2]["type"]["name"], "EventType");

    // Check union field
    assert!(parsed["fields"][3]["type"].is_array());

    // Check array field
    assert_eq!(parsed["fields"][4]["type"]["type"], "array");

    // Check map field
    assert_eq!(parsed["fields"][5]["type"]["type"], "map");
}

// ============================================================================
// Strict Schema Validation Tests
// ============================================================================

#[test]
fn test_strict_mode_rejects_duplicate_types_in_union() {
    use jetliner::schema::parse_schema_with_options;

    // Permissive mode (default) - should succeed with warning
    let json = r#"["int", "int"]"#;
    let result = parse_schema_with_options(json, false);
    assert!(
        result.is_ok(),
        "Permissive mode should allow duplicate types"
    );

    // Strict mode - should fail
    let result = parse_schema_with_options(json, true);
    assert!(result.is_err(), "Strict mode should reject duplicate types");
    let err = result.unwrap_err();
    assert!(err.to_string().contains("duplicate type"));
}

#[test]
fn test_strict_mode_rejects_nested_unions() {
    use jetliner::schema::parse_schema_with_options;

    // Permissive mode - should succeed with warning
    let json = r#"["int", ["string", "null"]]"#;
    let result = parse_schema_with_options(json, false);
    assert!(result.is_ok(), "Permissive mode should allow nested unions");

    // Strict mode - should fail
    let result = parse_schema_with_options(json, true);
    assert!(result.is_err(), "Strict mode should reject nested unions");
    let err = result.unwrap_err();
    assert!(err.to_string().contains("nested union"));
}

#[test]
fn test_strict_mode_rejects_invalid_record_names() {
    use jetliner::schema::parse_schema_with_options;

    // Name starting with number
    let json = r#"{
        "type": "record",
        "name": "123Invalid",
        "fields": [{"name": "value", "type": "int"}]
    }"#;

    // Permissive mode - should succeed with warning
    let result = parse_schema_with_options(json, false);
    assert!(result.is_ok(), "Permissive mode should allow invalid names");

    // Strict mode - should fail
    let result = parse_schema_with_options(json, true);
    assert!(result.is_err(), "Strict mode should reject invalid names");
    let err = result.unwrap_err();
    assert!(err
        .to_string()
        .contains("must start with a letter or underscore"));
}

#[test]
fn test_strict_mode_rejects_invalid_field_names() {
    use jetliner::schema::parse_schema_with_options;

    // Field name with special characters
    let json = r#"{
        "type": "record",
        "name": "Test",
        "fields": [{"name": "field-name", "type": "int"}]
    }"#;

    // Permissive mode - should succeed with warning
    let result = parse_schema_with_options(json, false);
    assert!(
        result.is_ok(),
        "Permissive mode should allow invalid field names"
    );

    // Strict mode - should fail
    let result = parse_schema_with_options(json, true);
    assert!(
        result.is_err(),
        "Strict mode should reject invalid field names"
    );
    let err = result.unwrap_err();
    assert!(err.to_string().contains("invalid character"));
}

#[test]
fn test_strict_mode_rejects_invalid_enum_names() {
    use jetliner::schema::parse_schema_with_options;

    // Enum name with special characters
    let json = r#"{
        "type": "enum",
        "name": "Status@Type",
        "symbols": ["OK", "ERROR"]
    }"#;

    // Permissive mode - should succeed with warning
    let result = parse_schema_with_options(json, false);
    assert!(
        result.is_ok(),
        "Permissive mode should allow invalid enum names"
    );

    // Strict mode - should fail
    let result = parse_schema_with_options(json, true);
    assert!(
        result.is_err(),
        "Strict mode should reject invalid enum names"
    );
    let err = result.unwrap_err();
    assert!(err.to_string().contains("invalid character"));
}

#[test]
fn test_strict_mode_rejects_invalid_enum_symbols() {
    use jetliner::schema::parse_schema_with_options;

    // Enum symbol with special characters
    let json = r#"{
        "type": "enum",
        "name": "Status",
        "symbols": ["OK", "NOT-OK"]
    }"#;

    // Permissive mode - should succeed with warning
    let result = parse_schema_with_options(json, false);
    assert!(
        result.is_ok(),
        "Permissive mode should allow invalid symbol names"
    );

    // Strict mode - should fail
    let result = parse_schema_with_options(json, true);
    assert!(
        result.is_err(),
        "Strict mode should reject invalid symbol names"
    );
    let err = result.unwrap_err();
    assert!(err.to_string().contains("invalid character"));
}

#[test]
fn test_strict_mode_rejects_invalid_fixed_names() {
    use jetliner::schema::parse_schema_with_options;

    // Fixed name with special characters
    let json = r#"{
        "type": "fixed",
        "name": "Hash.MD5",
        "size": 16
    }"#;

    // Permissive mode - should succeed with warning (dots are allowed in qualified names)
    let result = parse_schema_with_options(json, false);
    assert!(
        result.is_ok(),
        "Permissive mode should allow names with dots"
    );

    // Name with truly invalid character
    let json2 = r#"{
        "type": "fixed",
        "name": "Hash$MD5",
        "size": 16
    }"#;

    // Permissive mode - should succeed with warning
    let result = parse_schema_with_options(json2, false);
    assert!(
        result.is_ok(),
        "Permissive mode should allow invalid fixed names"
    );

    // Strict mode - should fail
    let result = parse_schema_with_options(json2, true);
    assert!(
        result.is_err(),
        "Strict mode should reject invalid fixed names"
    );
    let err = result.unwrap_err();
    assert!(err.to_string().contains("invalid character"));
}

#[test]
fn test_strict_mode_accepts_valid_names() {
    use jetliner::schema::parse_schema_with_options;

    // Valid names should work in both modes
    let json = r#"{
        "type": "record",
        "name": "ValidName123",
        "fields": [
            {"name": "_valid_field", "type": "int"},
            {"name": "AnotherField", "type": "string"}
        ]
    }"#;

    let result_permissive = parse_schema_with_options(json, false);
    assert!(
        result_permissive.is_ok(),
        "Valid names should work in permissive mode"
    );

    let result_strict = parse_schema_with_options(json, true);
    assert!(
        result_strict.is_ok(),
        "Valid names should work in strict mode"
    );
}

#[test]
fn test_strict_mode_accepts_valid_unions() {
    use jetliner::schema::parse_schema_with_options;

    // Valid union without duplicates or nesting
    let json = r#"["null", "string", "int"]"#;

    let result_permissive = parse_schema_with_options(json, false);
    assert!(
        result_permissive.is_ok(),
        "Valid unions should work in permissive mode"
    );

    let result_strict = parse_schema_with_options(json, true);
    assert!(
        result_strict.is_ok(),
        "Valid unions should work in strict mode"
    );
}

#[test]
fn test_strict_mode_complex_schema() {
    use jetliner::schema::parse_schema_with_options;

    // Complex valid schema should work in both modes
    let json = r#"{
        "type": "record",
        "name": "Event",
        "namespace": "com.example",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "data", "type": ["null", "bytes"]},
            {
                "name": "nested",
                "type": {
                    "type": "record",
                    "name": "NestedData",
                    "fields": [
                        {"name": "value", "type": "int"}
                    ]
                }
            }
        ]
    }"#;

    let result_permissive = parse_schema_with_options(json, false);
    assert!(
        result_permissive.is_ok(),
        "Complex valid schema should work in permissive mode"
    );

    let result_strict = parse_schema_with_options(json, true);
    assert!(
        result_strict.is_ok(),
        "Complex valid schema should work in strict mode"
    );
}
