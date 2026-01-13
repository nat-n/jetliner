#!/usr/bin/env python3
"""
Benchmark data generation for comparative benchmarks.

Generates Avro files with snappy compression for benchmarking:
- large_simple: 1M records, 1 string + 4 numeric columns
- large_wide: 1M records, 100 columns (mixed types)
- large_complex: 1M records with realistic complex structures:
  * Nested records (2 levels: address -> coordinates)
  * Arrays of records (contacts with type, value, primary)
  * Arrays of primitives (tags, 0-5 elements)
  * Maps (metadata, 0-3 entries)
  * Nullable fields (coordinates, score)

Usage:
    python benches/python/generate_data.py [--force]
"""

import random
import string
from pathlib import Path
from typing import Iterator

import fastavro

# Benchmark data directory (gitignored)
BENCH_DATA_DIR = Path(__file__).parent.parent / "data"

# File paths for each benchmark scenario
BENCH_FILES = {
    "large_simple": BENCH_DATA_DIR / "large_simple.avro",
    "large_wide": BENCH_DATA_DIR / "large_wide.avro",
    "large_complex": BENCH_DATA_DIR / "large_complex.avro",
}

# Record counts
LARGE_RECORDS = 1_000_000


# =============================================================================
# Schemas
# =============================================================================

LARGE_SIMPLE_SCHEMA = {
    "type": "record",
    "name": "LargeSimpleRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "int_val", "type": "int"},
        {"name": "float_val", "type": "float"},
        {"name": "double_val", "type": "double"},
        {"name": "bool_val", "type": "boolean"},
    ],
}


def _generate_wide_schema() -> dict:
    """Generate schema with 100 columns covering all Avro primitive types."""
    # Cycle through all non-null primitive types: long, string, double, int, boolean, float, bytes
    type_cycle = ["long", "string", "double", "int", "boolean", "float", "bytes"]
    fields = []
    for i in range(100):
        col_type = type_cycle[i % len(type_cycle)]
        fields.append({"name": f"col_{i:03d}", "type": col_type})
    return {
        "type": "record",
        "name": "LargeWideRecord",
        "fields": fields,
    }


LARGE_WIDE_SCHEMA = _generate_wide_schema()

LARGE_COMPLEX_SCHEMA = {
    "type": "record",
    "name": "LargeComplexRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        # Nested record (1 level deep)
        {
            "name": "address",
            "type": {
                "type": "record",
                "name": "Address",
                "fields": [
                    {"name": "street", "type": "string"},
                    {"name": "city", "type": "string"},
                    {"name": "zip", "type": "string"},
                    # Deeper nesting (2 levels deep) - coordinates inside address
                    {
                        "name": "coordinates",
                        "type": [
                            "null",
                            {
                                "type": "record",
                                "name": "Coordinates",
                                "fields": [
                                    {"name": "latitude", "type": "double"},
                                    {"name": "longitude", "type": "double"},
                                ],
                            },
                        ],
                    },
                ],
            },
        },
        # Array of records - very common in real-world Avro (e.g., order line items, phone numbers)
        {
            "name": "contacts",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Contact",
                    "fields": [
                        {"name": "type", "type": "string"},  # e.g., "email", "phone", "mobile"
                        {"name": "value", "type": "string"},
                        {"name": "primary", "type": "boolean"},
                    ],
                },
            },
        },
        # Array of primitives (tests variable length including empty)
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        # Map type (tests variable length including empty)
        # Note: polars.read_avro() doesn't support maps
        {"name": "metadata", "type": {"type": "map", "values": "string"}},
        # Union - nullable field
        {"name": "score", "type": ["null", "double"]},
    ],
}


# =============================================================================
# Data Generators
# =============================================================================


def generate_large_simple_records(n: int = LARGE_RECORDS) -> Iterator[dict]:
    """Generate records for large_simple benchmark file."""
    for i in range(n):
        yield {
            "id": i,
            "name": f"user_{i:08d}",
            "int_val": random.randint(-1000000, 1000000),
            "float_val": random.random() * 1000,
            "double_val": random.random() * 1000000,
            "bool_val": i % 2 == 0,
        }


def generate_large_wide_records(n: int = LARGE_RECORDS) -> Iterator[dict]:
    """Generate records for large_wide benchmark file (100 columns).

    Cycles through all Avro primitive types: long, string, double, int, boolean, float, bytes
    """
    # Type cycle matches _generate_wide_schema()
    type_cycle = ["long", "string", "double", "int", "boolean", "float", "bytes"]
    for i in range(n):
        record = {}
        for j in range(100):
            col_type = type_cycle[j % len(type_cycle)]
            if col_type == "long":
                record[f"col_{j:03d}"] = i + j
            elif col_type == "string":
                record[f"col_{j:03d}"] = f"val_{i}_{j}"
            elif col_type == "double":
                record[f"col_{j:03d}"] = random.random() * 1000
            elif col_type == "int":
                record[f"col_{j:03d}"] = random.randint(-1000, 1000)
            elif col_type == "boolean":
                record[f"col_{j:03d}"] = i % 2 == 0
            elif col_type == "float":
                record[f"col_{j:03d}"] = random.random() * 100
            elif col_type == "bytes":
                record[f"col_{j:03d}"] = f"bytes_{i}_{j}".encode("utf-8")
        yield record


def generate_large_complex_records(n: int = LARGE_RECORDS) -> Iterator[dict]:
    """Generate records for large_complex benchmark file.

    Tests complex Avro structures:
    - Nested records (2 levels deep: address -> coordinates)
    - Array of records (contacts)
    - Array of primitives (tags, including empty arrays)
    - Map (metadata, including empty maps)
    - Nullable fields (coordinates, score)
    """
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    contact_types = ["email", "phone", "mobile", "work", "home"]

    for i in range(n):
        # Generate coordinates for ~50% of records (tests nullable nested records)
        coordinates = None
        if i % 2 == 0:
            coordinates = {
                "latitude": 40.7128 + random.uniform(-10, 10),
                "longitude": -74.0060 + random.uniform(-10, 10),
            }

        # Generate 0-3 contacts (tests array of records with variable length)
        num_contacts = i % 4
        contacts = []
        for j in range(num_contacts):
            contacts.append({
                "type": contact_types[j % len(contact_types)],
                "value": f"contact_{i}_{j}@example.com" if j % 2 == 0 else f"+1-555-{i:04d}{j}",
                "primary": j == 0,  # First contact is primary
            })

        # Generate 0-5 tags (tests array of primitives with variable length, including empty)
        num_tags = i % 6
        tags = [f"tag_{j}" for j in range(num_tags)]

        # Generate 0-3 metadata entries (tests map with variable length, including empty)
        num_metadata = i % 4
        metadata = {f"key_{j}": f"value_{i}_{j}" for j in range(num_metadata)}

        yield {
            "id": i,
            "name": f"person_{i:08d}",
            "address": {
                "street": f"{i} Main St",
                "city": cities[i % len(cities)],
                "zip": f"{10000 + (i % 90000):05d}",
                "coordinates": coordinates,
            },
            "contacts": contacts,
            "tags": tags,
            "metadata": metadata,
            "score": random.random() * 100 if i % 3 != 0 else None,
        }


# =============================================================================
# File Generation
# =============================================================================


def generate_benchmark_file(
    path: Path,
    schema: dict,
    records_generator,
    num_records: int,
    codec: str = "snappy",
) -> dict:
    """Generate a benchmark Avro file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "wb") as f:
        fastavro.writer(
            f,
            schema,
            records_generator(num_records),
            codec=codec,
        )

    file_size = path.stat().st_size
    return {
        "path": str(path),
        "size_bytes": file_size,
        "size_mb": file_size / (1024 * 1024),
        "num_records": num_records,
        "codec": codec,
    }


def ensure_benchmark_files_exist(force: bool = False) -> dict:
    """Ensure all benchmark files exist, generating if needed."""
    stats = {}

    configs = [
        ("large_simple", LARGE_SIMPLE_SCHEMA, generate_large_simple_records, LARGE_RECORDS),
        ("large_wide", LARGE_WIDE_SCHEMA, generate_large_wide_records, LARGE_RECORDS),
        ("large_complex", LARGE_COMPLEX_SCHEMA, generate_large_complex_records, LARGE_RECORDS),
    ]

    for name, schema, generator, count in configs:
        path = BENCH_FILES[name]
        if force or not path.exists():
            print(f"Generating {name} benchmark file ({count:,} records)...")
            stats[name] = generate_benchmark_file(path, schema, generator, count)
        else:
            stats[name] = {"path": str(path), "exists": True}

    return stats


if __name__ == "__main__":
    import sys

    force = "--force" in sys.argv
    print("Generating benchmark data files...")
    print(f"Output directory: {BENCH_DATA_DIR}")
    print()

    stats = ensure_benchmark_files_exist(force=force)

    print("\nBenchmark files:")
    for name, info in stats.items():
        if "size_mb" in info:
            print(f"  {name}: {info['size_mb']:.1f} MB, {info['num_records']:,} records")
        else:
            print(f"  {name}: already exists")
