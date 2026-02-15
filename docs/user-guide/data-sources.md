# Data sources

Jetliner reads Avro files from local filesystem and S3. All APIs (`scan_avro`, `read_avro`, `AvroReader`, `MultiAvroReader`) accept the same path formats.

## Paths and patterns

```python
import jetliner

# Local files
df = jetliner.scan_avro("data.avro").collect()
df = jetliner.scan_avro("/absolute/path/data.avro").collect()
df = jetliner.scan_avro(Path("data.avro")).collect()  # pathlib.Path works

# S3
df = jetliner.scan_avro("s3://bucket/key.avro").collect()

# Glob patterns (local and S3)
df = jetliner.read_avro("data/*.avro")                   # all .avro in directory
df = jetliner.read_avro("s3://bucket/data/**/*.avro")    # recursive
df = jetliner.read_avro("data/{2023,2024}/*.avro")       # alternatives

# Explicit file list
df = jetliner.read_avro(["file1.avro", "file2.avro"])
```

When reading multiple files, schemas must be compatible. Use `row_index_name` for continuous row numbering and `include_file_paths` to track source files:

```python
df = jetliner.read_avro(
    "data/*.avro",
    row_index_name="idx",
    include_file_paths="source_file"
)
```

## S3 authentication

Jetliner uses the standard AWS credential chain: environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`), IAM roles, or `~/.aws/credentials`.

```python
# Uses default credentials
df = jetliner.scan_avro("s3://bucket/file.avro").collect()
```

For explicit credentials or S3-compatible services (MinIO, LocalStack, R2, etc.):

```python
df = jetliner.scan_avro(
    "s3://bucket/file.avro",
    storage_options={
        "endpoint": "http://localhost:9000",  # custom endpoint
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "region": "us-east-1",
    }
).collect()
```

### storage_options reference

| Key | Description |
|-----|-------------|
| `endpoint` | Custom S3 endpoint URL |
| `aws_access_key_id` | Access key (overrides environment) |
| `aws_secret_access_key` | Secret key (overrides environment) |
| `region` | AWS region |

## Supported codecs

All standard Avro codecs are supported automatically—no configuration needed.

| Codec | Compression | Speed | Notes |
|-------|-------------|-------|-------|
| `null` | None | Fastest | No decompression overhead |
| `snappy` | ~2-4x | Fast | Common default |
| `zstd` | ~4-10x | Fast | Best overall balance |
| `deflate` | ~4-8x | Medium | |
| `bzip2` | ~6-12x | Slow | Archival use |
| `xz` | ~8-15x | Slowest | Maximum compression |

Codec errors raise `CodecError` with details about the failure.
