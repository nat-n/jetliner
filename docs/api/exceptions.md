# Exceptions

All Jetliner exceptions inherit from `JetlinerError`, allowing you to catch any library-specific error:

```python
try:
    df = jetliner.read_avro("data.avro")
except jetliner.JetlinerError as e:
    print(f"Jetliner error: {e}")
```

Exceptions that map to Python builtins also inherit from those builtins for idiomatic handling:

```python
try:
    df = jetliner.read_avro("missing.avro")
except FileNotFoundError:
    print("File not found!")
```

## Base Exception

::: jetliner.JetlinerError

## Decoding and Parsing

::: jetliner.DecodeError

::: jetliner.ParseError

## Schema and Codec

::: jetliner.SchemaError

::: jetliner.CodecError

## Source Access

::: jetliner.SourceError

::: jetliner.AuthenticationError

::: jetliner.FileNotFoundError

::: jetliner.PermissionError

## Configuration

::: jetliner.ConfigurationError

## Error Information

::: jetliner.BadBlockError
