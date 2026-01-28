#!/usr/bin/env python3
"""
Comparative benchmarks for Avro reading libraries.

Compares jetliner (open and scan APIs) against:
- jetliner_open: Iterator API (streaming, no projection pushdown)
- jetliner_scan: Lazy API (full collect, supports projection pushdown)
- polars.read_avro (built-in)
- polars-avro (IO plugin)
- fastavro
- fastavro + pandas
- apache-avro

Error handling:
By default, errors are caught and displayed in results (e.g., polars doesn't support Avro maps).
Use --fail-fast to abort on first error instead.

JSON output:
Use --output to save a complete structured JSON report with metadata, configuration, and all results.
Includes raw timing data for detailed analysis and plotting.

S3 mode:
Use --s3 to benchmark S3 reads using a local MinIO container. This compares jetliner's
local file reads against S3 reads to verify the S3 code path has equivalent throughput.
Only jetliner is benchmarked in S3 mode (other libraries don't support S3 directly).

Usage:
    # Quick comparison (fewer iterations)
    python benches/python/compare.py --quick

    # Full benchmark (statistical rigor)
    python benches/python/compare.py --full

    # Specific scenario
    python benches/python/compare.py --scenario large_simple

    # Specific readers
    python benches/python/compare.py --readers jetliner_scan jetliner_open polars

    # Save complete JSON report for plotting/analysis
    python benches/python/compare.py --output results.json

    # Compare against previous run
    python benches/python/compare.py --previous baseline.json --output current.json

    # Abort on first error
    python benches/python/compare.py --fail-fast

    # S3 benchmark mode (requires Docker)
    python benches/python/compare.py --s3
"""

import argparse
import atexit
import gc
import hashlib
import json
import os
import platform
import signal
import statistics
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

import boto3
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box

console = Console()

# Global reference for cleanup on interrupt
_minio_container = None

# Ensure benchmark data exists
sys.path.insert(0, str(Path(__file__).parent))
from generate_data import BENCH_FILES, ensure_benchmark_files_exist


# =============================================================================
# Reader Functions
# =============================================================================


def read_jetliner_open(path: Path, columns: list[str] | None = None):
    """Read with jetliner.open() - iterator API, measures streaming performance.

    For projection scenarios, reads all columns then filters - shows cost of no pushdown.
    Compare against jetliner_scan which pushes projection down for better performance.
    """
    import jetliner
    import polars as pl

    # Iterate through and consume the data
    # If columns specified, select after reading (no pushdown optimization)
    last_df = None
    with jetliner.open(str(path)) as reader:
        for df in reader:
            if columns:
                df = df.select(columns)
            last_df = df  # Keep reference so it's not optimized away

    return last_df if last_df is not None else pl.DataFrame()


def read_jetliner_scan(path: Path, columns: list[str] | None = None):
    """Read with jetliner.scan() - lazy API, measures full collect performance."""
    import jetliner

    lf = jetliner.scan(str(path))
    if columns:
        lf = lf.select(columns)
    return lf.collect()


def read_polars(path: Path, columns: list[str] | None = None):
    """Read with polars.read_avro (built-in)."""
    import polars as pl

    return pl.read_avro(str(path), columns=columns)


def read_polars_avro(path: Path, columns: list[str] | None = None):
    """Read with polars-avro IO plugin."""
    import polars_avro

    lf = polars_avro.scan_avro(str(path))
    if columns:
        lf = lf.select(columns)
    return lf.collect()


def read_fastavro(path: Path, columns: list[str] | None = None):
    """Read with fastavro -> polars."""
    import fastavro
    import polars as pl

    with open(path, "rb") as f:
        records = list(fastavro.reader(f))
    df = pl.DataFrame(records)
    if columns:
        df = df.select(columns)
    return df


def read_fastavro_pandas(path: Path, columns: list[str] | None = None):
    """Read with fastavro -> pandas -> polars."""
    import fastavro
    import pandas as pd
    import polars as pl

    with open(path, "rb") as f:
        records = list(fastavro.reader(f))
    pdf = pd.DataFrame.from_records(records)
    if columns:
        pdf = pdf[columns]
    return pl.from_pandas(pdf)


def read_avro(path: Path, columns: list[str] | None = None):
    """Read with apache-avro library."""
    import polars as pl
    from avro.datafile import DataFileReader
    from avro.io import DatumReader

    with open(path, "rb") as f:
        reader = DataFileReader(f, DatumReader())
        records = list(reader)
        reader.close()
    df = pl.DataFrame(records)
    if columns:
        df = df.select(columns)
    return df


READERS = {
    "jetliner_open": read_jetliner_open,
    "jetliner_scan": read_jetliner_scan,
    "polars": read_polars,
    "polars_avro": read_polars_avro,
    "fastavro": read_fastavro,
    "fastavro_pandas": read_fastavro_pandas,
    "avro": read_avro,
}


# =============================================================================
# S3 Reader Functions (for --s3 mode)
# =============================================================================


def read_jetliner_open_s3(s3_uri: str, storage_options: dict, columns: list[str] | None = None):
    """Read from S3 with jetliner.open() - iterator API."""
    import jetliner
    import polars as pl

    last_df = None
    with jetliner.open(s3_uri, storage_options=storage_options) as reader:
        for df in reader:
            if columns:
                df = df.select(columns)
            last_df = df

    return last_df if last_df is not None else pl.DataFrame()


def read_jetliner_scan_s3(s3_uri: str, storage_options: dict, columns: list[str] | None = None):
    """Read from S3 with jetliner.scan() - lazy API."""
    import jetliner

    lf = jetliner.scan(s3_uri, storage_options=storage_options)
    if columns:
        lf = lf.select(columns)
    return lf.collect()


# =============================================================================
# MinIO Container Management
# =============================================================================


def _cleanup_minio():
    """Cleanup MinIO container on exit."""
    global _minio_container
    if _minio_container is not None:
        console.print("\n[dim]Stopping MinIO container...[/]")
        try:
            _minio_container.stop()
        except Exception:
            pass
        _minio_container = None


def _signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    console.print("\n[yellow]Interrupted - cleaning up...[/]")
    _cleanup_minio()
    sys.exit(1)


@dataclass
class MinIOContext:
    """Context for MinIO S3 operations."""
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    client: any

    def get_storage_options(self) -> dict:
        """Get storage options dict for jetliner."""
        return {
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
        }

    def upload_file(self, local_path: Path, key: str) -> str:
        """Upload a local file to MinIO and return s3:// URI."""
        self.client.upload_file(str(local_path), self.bucket, key)
        return f"s3://{self.bucket}/{key}"


def start_minio_container() -> MinIOContext:
    """Start MinIO container and return context for S3 operations."""
    global _minio_container

    try:
        from testcontainers.minio import MinioContainer
    except ImportError:
        console.print("[red]Error:[/] testcontainers[minio] not installed")
        console.print("Install with: uv add --dev testcontainers[minio]")
        sys.exit(1)

    console.print("[dim]Starting MinIO container...[/]")

    try:
        container = MinioContainer("minio/minio:latest")
        container.start()
        _minio_container = container
    except Exception as e:
        console.print(f"[red]Error:[/] Failed to start MinIO container: {e}")
        console.print("Make sure Docker is running.")
        sys.exit(1)

    endpoint_url = (
        f"http://{container.get_container_host_ip()}:"
        f"{container.get_exposed_port(9000)}"
    )

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=container.access_key,
        aws_secret_access_key=container.secret_key,
    )

    # Create benchmark bucket
    bucket_name = f"jetliner-bench-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=bucket_name)

    console.print(f"[green]✓[/] MinIO running at {endpoint_url}")

    return MinIOContext(
        endpoint_url=endpoint_url,
        access_key=container.access_key,
        secret_key=container.secret_key,
        bucket=bucket_name,
        client=client,
    )


# =============================================================================
# Benchmark Scenarios
# =============================================================================


@dataclass
class Scenario:
    name: str
    file_key: str
    columns: list[str] | None = None
    description: str = ""


SCENARIOS = {
    "large_simple": Scenario(
        name="large_simple",
        file_key="large_simple",
        description="1M records, 5 cols - raw throughput",
    ),
    "large_wide": Scenario(
        name="large_wide",
        file_key="large_wide",
        description="1M records, 100 cols - column handling",
    ),
    "large_complex": Scenario(
        name="large_complex",
        file_key="large_complex",
        description="1M records - nested records, arrays of records, maps, nullables",
    ),
    "projection": Scenario(
        name="projection",
        file_key="large_wide",
        columns=["col_000", "col_020", "col_040", "col_060", "col_080"],
        description="1M records, select 5/100 cols - projection",
    ),
}


# =============================================================================
# Benchmark Runner with Statistics
# =============================================================================


def run_timed(func: Callable, *args, **kwargs) -> tuple[any, float]:
    """Run function and return result with elapsed time."""
    gc.collect()
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return result, elapsed


# Memory measurement removed - unreliable in sequential benchmarks
# due to Python's memory arena reuse and RSS measurement limitations.
# For accurate memory profiling, use a dedicated tool like memray or memory_profiler
# with subprocess isolation.


def benchmark_reader(
    reader_func: Callable,
    file_path: Path,
    columns: list[str] | None,
    warmup_runs: int = 2,
    timed_runs: int = 5,
) -> dict:
    """Benchmark a reader function with warmup and multiple timed runs.

    Raises any exceptions that occur during warmup or timed runs.
    """
    # Warmup runs - let exceptions propagate
    for _ in range(warmup_runs):
        reader_func(file_path, columns)
        gc.collect()

    # Timed runs - let exceptions propagate
    times = []
    for _ in range(timed_runs):
        _, elapsed = run_timed(reader_func, file_path, columns)
        times.append(elapsed)
        gc.collect()

    # Calculate statistics
    mean = statistics.mean(times)
    stdev = statistics.stdev(times) if len(times) > 1 else 0
    median = statistics.median(times)
    min_time = min(times)
    max_time = max(times)

    return {
        "mean_sec": mean,
        "stdev_sec": stdev,
        "median_sec": median,
        "min_sec": min_time,
        "max_sec": max_time,
        "runs": timed_runs,
        "times": times,
    }


def compute_dataframe_hash(df) -> str:
    """Compute a hash of DataFrame contents for validation.

    Uses first and last 1000 rows to be fast on large DataFrames.

    Normalizes numeric types before hashing to compare data values rather than
    type representations. Different Avro readers use different type widths:
    - Polars-native readers (jetliner, polars, polars-avro) use Avro-native types
      (Int32 for 'int', Float32 for 'float') per the Avro specification
    - Python-based readers (fastavro, avro) promote to Python types (Int64, Float64)
      because Python doesn't distinguish int/long or float/double

    Upcasting narrow types to wide types ensures we validate data correctness
    without risking data loss from downcasting.
    """
    import polars as pl

    # For small DataFrames, hash everything
    if df.height <= 2000:
        sample = df
    else:
        # For large DataFrames, hash first and last 1000 rows
        sample = pl.concat([df.head(1000), df.tail(1000)])

    # Normalize types: upcast narrow types to wide types for consistent comparison
    sample = sample.select([
        pl.col(col).cast(pl.Int64) if dtype == pl.Int32
        else pl.col(col).cast(pl.Float64) if dtype == pl.Float32
        else pl.col(col)
        for col, dtype in sample.schema.items()
    ])

    # Convert to bytes and hash
    # Use write_csv as a stable serialization format
    csv_bytes = sample.write_csv().encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


def validate_scenario_results(
    scenario: Scenario,
    readers: dict[str, Callable],
    results: dict,
) -> None:
    """Validate that all successful readers produce the same output.

    Only prints warnings if mismatches are found.
    For jetliner_open (which returns only last chunk), validates columns but skips
    row count and data hash comparison.
    """
    file_path = BENCH_FILES[scenario.file_key]

    # Filter to only successful readers
    successful_readers = {
        name: func
        for name, func in readers.items()
        if name in results and "error" not in results[name]
    }

    if len(successful_readers) < 2:
        # Need at least 2 successful readers to compare
        return

    # Read data from each successful reader
    reader_outputs = {}
    for reader_name, reader_func in successful_readers.items():
        try:
            df = reader_func(file_path, scenario.columns)
            reader_outputs[reader_name] = {
                "df": df,
                "shape": (df.height, df.width),
                "columns": set(df.columns),
                "hash": compute_dataframe_hash(df),
            }
        except Exception:
            # Skip readers that fail during validation
            continue

    if len(reader_outputs) < 2:
        return

    # Use first non-jetliner_open reader as baseline
    # (prefer full-dataset readers for baseline comparison)
    baseline_name = None
    for name in reader_outputs.keys():
        if name != "jetliner_open":
            baseline_name = name
            break
    if baseline_name is None:
        # Only jetliner_open succeeded, can't validate
        return

    baseline = reader_outputs[baseline_name]

    # Check for mismatches
    mismatches = []
    for reader_name, output in reader_outputs.items():
        if reader_name == baseline_name:
            continue

        is_streaming = reader_name == "jetliner_open"

        # Check shape (skip row count for streaming readers)
        if not is_streaming and output["shape"] != baseline["shape"]:
            mismatches.append(
                f"  Shape mismatch: {reader_name} has {output['shape'][0]:,} rows × {output['shape'][1]} cols, "
                f"expected {baseline['shape'][0]:,} rows × {baseline['shape'][1]} cols"
            )
        elif is_streaming and output["shape"][1] != baseline["shape"][1]:
            # For streaming readers, only check column count
            mismatches.append(
                f"  Column count mismatch: {reader_name} has {output['shape'][1]} cols, "
                f"expected {baseline['shape'][1]} cols"
            )

        # Check columns (always validate)
        if output["columns"] != baseline["columns"]:
            missing = baseline["columns"] - output["columns"]
            extra = output["columns"] - baseline["columns"]
            if missing:
                mismatches.append(f"  {reader_name} missing columns: {missing}")
            if extra:
                mismatches.append(f"  {reader_name} has extra columns: {extra}")

        # Check data hash (skip for streaming readers since they return partial data)
        if not is_streaming and output["hash"] != baseline["hash"]:
            mismatches.append(
                f"  Data mismatch: {reader_name} produces different values than {baseline_name}"
            )

    # Only print if mismatches found
    if mismatches:
        console.print()
        console.print(
            f"[yellow]⚠ Validation Warning:[/] Result mismatch in scenario '{scenario.name}'"
        )
        console.print(f"  [dim](comparing against {baseline_name} as baseline)[/]")
        for msg in mismatches:
            console.print(f"[yellow]{msg}[/]")


def run_scenario(
    scenario: Scenario,
    readers: dict[str, Callable],
    warmup_runs: int = 2,
    timed_runs: int = 5,
    fail_fast: bool = False,
) -> dict:
    """Run benchmark for a scenario across all readers."""
    file_path = BENCH_FILES[scenario.file_key]

    if not file_path.exists():
        raise FileNotFoundError(f"Benchmark file not found: {file_path}")

    results = {}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("", total=len(readers))

        for reader_name, reader_func in readers.items():
            progress.update(task, description=f"Benchmarking {reader_name}...")

            try:
                result = benchmark_reader(
                    reader_func,
                    file_path,
                    scenario.columns,
                    warmup_runs=warmup_runs,
                    timed_runs=timed_runs,
                )
                results[reader_name] = result

                # Show result inline
                console.print(
                    f"  [cyan]{reader_name:<18}[/] "
                    f"[green]{result['mean_sec']:.3f}s[/] ± {result['stdev_sec']:.3f}s"
                )

            except BaseException as e:
                # Catch all exceptions including PyO3 PanicException
                # Skip KeyboardInterrupt and SystemExit to preserve user control
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise

                if fail_fast:
                    console.print(
                        f"  [cyan]{reader_name:<18}[/] [red bold]ERROR: {e}[/]"
                    )
                    console.print("[red]Aborting due to --fail-fast[/]")
                    raise
                else:
                    results[reader_name] = {"error": str(e)}
                    console.print(f"  [cyan]{reader_name:<18}[/] [red]ERROR: {e}[/]")

            progress.advance(task)

    # Validate that all successful readers produce the same output
    validate_scenario_results(scenario, readers, results)

    return results


# =============================================================================
# S3 Benchmark Runner
# =============================================================================


def benchmark_s3_reader(
    reader_func: Callable,
    s3_uri: str,
    storage_options: dict,
    columns: list[str] | None,
    warmup_runs: int = 2,
    timed_runs: int = 5,
) -> dict:
    """Benchmark an S3 reader function with warmup and multiple timed runs."""
    # Warmup runs
    for _ in range(warmup_runs):
        reader_func(s3_uri, storage_options, columns)
        gc.collect()

    # Timed runs
    times = []
    for _ in range(timed_runs):
        gc.collect()
        start = time.perf_counter()
        reader_func(s3_uri, storage_options, columns)
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    # Calculate statistics
    mean = statistics.mean(times)
    stdev = statistics.stdev(times) if len(times) > 1 else 0
    median = statistics.median(times)
    min_time = min(times)
    max_time = max(times)

    return {
        "mean_sec": mean,
        "stdev_sec": stdev,
        "median_sec": median,
        "min_sec": min_time,
        "max_sec": max_time,
        "runs": timed_runs,
        "times": times,
    }


def run_s3_benchmarks(
    minio_ctx: MinIOContext,
    scenarios: list[Scenario],
    warmup_runs: int = 2,
    timed_runs: int = 5,
    fail_fast: bool = False,
) -> dict:
    """Run S3 benchmarks comparing local vs S3 reads for jetliner.

    Uploads benchmark files to MinIO and compares:
    - jetliner_open (local) vs jetliner_open_s3
    - jetliner_scan (local) vs jetliner_scan_s3
    """
    all_results = {}
    storage_options = minio_ctx.get_storage_options()

    # Upload benchmark files to MinIO
    console.print("\n[dim]Uploading benchmark files to MinIO...[/]")
    s3_uris = {}
    for scenario in scenarios:
        file_path = BENCH_FILES[scenario.file_key]
        if scenario.file_key not in s3_uris:
            key = f"bench/{file_path.name}"
            s3_uri = minio_ctx.upload_file(file_path, key)
            s3_uris[scenario.file_key] = s3_uri
            console.print(f"  [dim]Uploaded {file_path.name}[/]")

    # Define S3 readers to benchmark
    s3_readers = {
        "jetliner_open_local": lambda path, cols: read_jetliner_open(path, cols),
        "jetliner_open_s3": lambda s3_uri, cols: read_jetliner_open_s3(s3_uri, storage_options, cols),
        "jetliner_scan_local": lambda path, cols: read_jetliner_scan(path, cols),
        "jetliner_scan_s3": lambda s3_uri, cols: read_jetliner_scan_s3(s3_uri, storage_options, cols),
    }

    for i, scenario in enumerate(scenarios, 1):
        console.print()
        header = f"[bold cyan]S3 Scenario {i}/{len(scenarios)}:[/] [bold]{scenario.name}[/] - {scenario.description}"
        file_info = f"[dim]File: {BENCH_FILES[scenario.file_key].name}[/]"
        console.print(Panel(f"{header}\n{file_info}", border_style="magenta"))

        file_path = BENCH_FILES[scenario.file_key]
        s3_uri = s3_uris[scenario.file_key]
        results = {}

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task("", total=4)

            # Benchmark local readers
            for reader_name in ["jetliner_open_local", "jetliner_scan_local"]:
                progress.update(task, description=f"Benchmarking {reader_name}...")
                try:
                    result = benchmark_reader(
                        s3_readers[reader_name],
                        file_path,
                        scenario.columns,
                        warmup_runs=warmup_runs,
                        timed_runs=timed_runs,
                    )
                    results[reader_name] = result
                    console.print(
                        f"  [cyan]{reader_name:<22}[/] "
                        f"[green]{result['mean_sec']:.3f}s[/] ± {result['stdev_sec']:.3f}s"
                    )
                except Exception as e:
                    if fail_fast:
                        raise
                    results[reader_name] = {"error": str(e)}
                    console.print(f"  [cyan]{reader_name:<22}[/] [red]ERROR: {e}[/]")
                progress.advance(task)

            # Benchmark S3 readers
            for reader_name in ["jetliner_open_s3", "jetliner_scan_s3"]:
                progress.update(task, description=f"Benchmarking {reader_name}...")
                try:
                    result = benchmark_s3_reader(
                        read_jetliner_open_s3 if "open" in reader_name else read_jetliner_scan_s3,
                        s3_uri,
                        storage_options,
                        scenario.columns,
                        warmup_runs=warmup_runs,
                        timed_runs=timed_runs,
                    )
                    results[reader_name] = result
                    console.print(
                        f"  [cyan]{reader_name:<22}[/] "
                        f"[green]{result['mean_sec']:.3f}s[/] ± {result['stdev_sec']:.3f}s"
                    )
                except Exception as e:
                    if fail_fast:
                        raise
                    results[reader_name] = {"error": str(e)}
                    console.print(f"  [cyan]{reader_name:<22}[/] [red]ERROR: {e}[/]")
                progress.advance(task)

        all_results[scenario.name] = results

    return all_results


def print_s3_comparison_table(all_results: dict):
    """Print S3 vs local comparison table."""
    console.print()

    # Main performance table
    table = Table(
        title="S3 vs Local Performance (mean ± stdev)",
        box=box.ROUNDED,
        show_header=True,
    )
    table.add_column("Scenario", style="cyan", no_wrap=True)
    table.add_column("open (local)", justify="right")
    table.add_column("open (S3)", justify="right")
    table.add_column("scan (local)", justify="right")
    table.add_column("scan (S3)", justify="right")

    for scenario_name, results in all_results.items():
        row = [scenario_name]
        for reader in ["jetliner_open_local", "jetliner_open_s3", "jetliner_scan_local", "jetliner_scan_s3"]:
            if reader in results and "error" not in results[reader]:
                r = results[reader]
                row.append(f"[green]{r['mean_sec']:.3f}s[/] ± {r['stdev_sec']:.3f}s")
            else:
                row.append("[red]ERROR[/]" if reader in results else "-")
        table.add_row(*row)

    console.print(table)

    # S3 overhead table
    console.print()
    overhead_table = Table(
        title="S3 Overhead (S3 time / local time)",
        caption="[dim]~1.0 = equivalent throughput | >1.0 = S3 slower[/dim]",
        box=box.ROUNDED,
        show_header=True,
    )
    overhead_table.add_column("Scenario", style="cyan", no_wrap=True)
    overhead_table.add_column("open() overhead", justify="right")
    overhead_table.add_column("scan() overhead", justify="right")

    for scenario_name, results in all_results.items():
        row = [scenario_name]

        # open() overhead
        if ("jetliner_open_local" in results and "error" not in results["jetliner_open_local"] and
            "jetliner_open_s3" in results and "error" not in results["jetliner_open_s3"]):
            local_time = results["jetliner_open_local"]["mean_sec"]
            s3_time = results["jetliner_open_s3"]["mean_sec"]
            overhead = s3_time / local_time
            if overhead < 1.1:
                row.append(f"[bold green]{overhead:.2f}x[/]")
            elif overhead < 1.5:
                row.append(f"[yellow]{overhead:.2f}x[/]")
            else:
                row.append(f"[red]{overhead:.2f}x[/]")
        else:
            row.append("-")

        # scan() overhead
        if ("jetliner_scan_local" in results and "error" not in results["jetliner_scan_local"] and
            "jetliner_scan_s3" in results and "error" not in results["jetliner_scan_s3"]):
            local_time = results["jetliner_scan_local"]["mean_sec"]
            s3_time = results["jetliner_scan_s3"]["mean_sec"]
            overhead = s3_time / local_time
            if overhead < 1.1:
                row.append(f"[bold green]{overhead:.2f}x[/]")
            elif overhead < 1.5:
                row.append(f"[yellow]{overhead:.2f}x[/]")
            else:
                row.append(f"[red]{overhead:.2f}x[/]")
        else:
            row.append("-")

        overhead_table.add_row(*row)

    console.print(overhead_table)


# =============================================================================
# Output Formatting
# =============================================================================


def print_comparison_table(
    all_results: dict, readers: list[str], previous_results: dict | None = None
):
    """Print a comparison table of all results using rich tables."""
    console.print()  # Blank line

    # Main performance table
    table = Table(
        title="Performance Comparison (mean ± stdev)", box=box.ROUNDED, show_header=True
    )
    table.add_column("Scenario", style="cyan", no_wrap=True)

    for reader in readers:
        table.add_column(reader, justify="right")

    for scenario_name, results in all_results.items():
        row = [scenario_name]
        for reader in readers:
            if reader in results:
                r = results[reader]
                if "error" in r:
                    row.append("[red]ERROR[/]")
                else:
                    row.append(
                        f"[green]{r['mean_sec']:.3f}s[/] ± {r['stdev_sec']:.3f}s"
                    )
            else:
                row.append("-")
        table.add_row(*row)

    console.print(table)

    # Speedup comparison table (relative to jetliner_scan)
    if "jetliner_scan" in readers:
        console.print()
        speedup_table = Table(
            title="Speedup vs jetliner_scan (baseline)",
            caption="[dim]< 1.0 = faster than baseline | > 1.0 = slower than baseline[/dim]",
            box=box.ROUNDED,
            show_header=True,
        )
        speedup_table.add_column("Scenario", style="cyan", no_wrap=True)

        for reader in readers:
            speedup_table.add_column(reader, justify="right")

        for scenario_name, results in all_results.items():
            if "jetliner_scan" not in results or "error" in results.get(
                "jetliner_scan", {}
            ):
                continue

            jetliner_time = results["jetliner_scan"]["mean_sec"]
            row = [scenario_name]

            for reader in readers:
                if reader == "jetliner_scan":
                    row.append("[bold]1.00x[/]")
                elif reader in results and "error" not in results[reader]:
                    speedup = results[reader]["mean_sec"] / jetliner_time
                    if speedup < 1.0:
                        # Faster than baseline - green
                        row.append(f"[bold green]{speedup:.2f}x[/]")
                    elif speedup > 1.5:
                        # Much slower - red
                        row.append(f"[bold red]{speedup:.2f}x[/]")
                    elif speedup > 1.1:
                        # Somewhat slower - yellow
                        row.append(f"[yellow]{speedup:.2f}x[/]")
                    else:
                        # About the same
                        row.append(f"{speedup:.2f}x")
                else:
                    row.append("-")
            speedup_table.add_row(*row)

        console.print(speedup_table)

    # Comparison with previous run (if provided)
    if previous_results and "jetliner_scan" in readers:
        console.print()
        prev_table = Table(
            title="jetliner_scan: Current vs Previous Run",
            caption="[dim]> 1.0 = faster than previous | < 1.0 = slower than previous[/dim]",
            box=box.ROUNDED,
            show_header=True,
        )
        prev_table.add_column("Scenario", style="cyan", no_wrap=True)
        prev_table.add_column("Previous", justify="right")
        prev_table.add_column("Current", justify="right")
        prev_table.add_column("Change", justify="right")

        for scenario_name, results in all_results.items():
            # Check if current run has jetliner_scan results
            if "jetliner_scan" not in results or "error" in results.get(
                "jetliner_scan", {}
            ):
                continue

            # Check if previous run has this scenario and jetliner_scan
            if scenario_name not in previous_results:
                continue
            prev_scenario = previous_results[scenario_name]
            if "jetliner_scan" not in prev_scenario or "error" in prev_scenario.get(
                "jetliner_scan", {}
            ):
                continue

            current_time = results["jetliner_scan"]["mean_sec"]
            previous_time = prev_scenario["jetliner_scan"]["mean_sec"]
            speedup = previous_time / current_time

            # Format the change
            if speedup > 1.05:
                # Faster than previous - green
                change_str = f"[bold green]{speedup:.2f}x faster[/]"
            elif speedup < 0.95:
                # Slower than previous - red
                change_str = f"[bold red]{speedup:.2f}x slower[/]"
            else:
                # About the same - neutral
                change_str = f"{speedup:.2f}x (no change)"

            prev_table.add_row(
                scenario_name,
                f"{previous_time:.3f}s",
                f"{current_time:.3f}s",
                change_str,
            )

        console.print(prev_table)


def main():
    parser = argparse.ArgumentParser(
        description="Comparative Avro reader benchmarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        choices=list(SCENARIOS.keys()) + ["all"],
        default="all",
        help="Scenario to run (default: all)",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        default=os.environ.get("BENCH_QUICK", "").lower() in ("t", "true", "1"),
        help="Quick mode: 1 warmup, 3 timed runs",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        default=os.environ.get("BENCH_FULL", "").lower() in ("t", "true", "1"),
        help="Full mode: 3 warmups, 10 timed runs",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=os.environ.get("BENCH_OUTPUT"),
        help="Output JSON file for results",
    )
    parser.add_argument(
        "--readers",
        nargs="+",
        choices=list(READERS.keys()),
        default=list(READERS.keys()),
        help="Readers to benchmark (default: all)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        default=os.environ.get("BENCH_FAIL_FAST", "").lower() in ("t", "true", "1"),
        help="Abort on first error instead of continuing (default: continue on errors)",
    )
    parser.add_argument(
        "--previous",
        type=Path,
        default=os.environ.get("BENCH_PREVIOUS"),
        help="Path to previous benchmark JSON report for comparison",
    )
    parser.add_argument(
        "--s3",
        action="store_true",
        default=os.environ.get("BENCH_S3", "").lower() in ("t", "true", "1"),
        help="S3 benchmark mode: compare local vs S3 reads using MinIO (requires Docker)",
    )
    args = parser.parse_args()

    # Register cleanup handlers for S3 mode
    if args.s3:
        atexit.register(_cleanup_minio)
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    # Determine run configuration
    if args.quick:
        console.print("[yellow]Running in quick mode (1 warmup, 3 runs)[/]")
        warmup_runs, timed_runs = 1, 3
    elif args.full:
        console.print("[green]Running in full mode (3 warmups, 10 runs)[/]")
        warmup_runs, timed_runs = 3, 10
    else:
        console.print("[cyan]Running in standard mode (2 warmups, 5 runs)[/]")
        warmup_runs, timed_runs = 2, 5

    # Ensure benchmark data exists
    console.print("[dim]Checking benchmark data files...[/]")
    ensure_benchmark_files_exist()

    # Select scenarios
    if args.scenario == "all":
        scenarios = list(SCENARIOS.values())
    else:
        scenarios = [SCENARIOS[args.scenario]]

    # S3 mode: compare local vs S3 reads
    if args.s3:
        console.print(
            Panel(
                "[bold magenta]S3 Benchmark Mode[/]\n"
                "Comparing jetliner local reads vs S3 reads using MinIO container.\n"
                "[dim]Other libraries don't support S3 directly.[/]",
                border_style="magenta",
            )
        )

        minio_ctx = start_minio_container()

        try:
            all_results = run_s3_benchmarks(
                minio_ctx,
                scenarios,
                warmup_runs=warmup_runs,
                timed_runs=timed_runs,
                fail_fast=args.fail_fast,
            )
            print_s3_comparison_table(all_results)

            # Save JSON output for S3 mode
            if args.output:
                json_report = {
                    "metadata": {
                        "timestamp": datetime.now().isoformat(),
                        "python_version": platform.python_version(),
                        "platform": platform.platform(),
                        "processor": platform.processor(),
                        "machine": platform.machine(),
                        "mode": "s3",
                    },
                    "configuration": {
                        "warmup_runs": warmup_runs,
                        "timed_runs": timed_runs,
                        "fail_fast": args.fail_fast,
                    },
                    "scenarios": {},
                    "results": all_results,
                }

                for scenario_name in all_results.keys():
                    scenario = SCENARIOS[scenario_name]
                    file_path = BENCH_FILES[scenario.file_key]
                    json_report["scenarios"][scenario_name] = {
                        "description": scenario.description,
                        "file_name": file_path.name,
                        "file_size_bytes": file_path.stat().st_size if file_path.exists() else None,
                        "columns": scenario.columns,
                    }

                with open(args.output, "w") as f:
                    json.dump(json_report, f, indent=2)
                console.print(f"\n[green]✓[/] Results saved to: [cyan]{args.output}[/]")

        finally:
            _cleanup_minio()

        return

    # Standard mode: compare all readers
    readers = {k: v for k, v in READERS.items() if k in args.readers}
    reader_names = list(readers.keys())

    # Load previous results if provided
    previous_results = None
    if args.previous:
        if not args.previous.exists():
            console.print(f"[red]Error:[/] Previous report not found: {args.previous}")
            sys.exit(1)
        try:
            with open(args.previous, "r") as f:
                previous_report = json.load(f)
                previous_results = previous_report.get("results", {})
            console.print(f"[dim]Loaded previous results from: {args.previous}[/]")
        except Exception as e:
            console.print(f"[red]Error loading previous report:[/] {e}")
            sys.exit(1)

    # Print configuration panel
    fail_fast_str = "[red]enabled[/]" if args.fail_fast else "[green]disabled[/]"
    config_info = f"""[cyan]Warmups:[/] {warmup_runs}
[cyan]Timed runs:[/] {timed_runs}
[cyan]Scenarios:[/] {', '.join(s.name for s in scenarios)}
[cyan]Readers:[/] {', '.join(reader_names)}
[cyan]Fail-fast:[/] {fail_fast_str}"""
    console.print(
        Panel(
            config_info, title="[bold]Benchmark Configuration[/]", border_style="blue"
        )
    )

    # Run benchmarks
    all_results = {}
    for i, scenario in enumerate(scenarios, 1):
        console.print()
        header = f"[bold cyan]Scenario {i}/{len(scenarios)}:[/] [bold]{scenario.name}[/] - {scenario.description}"
        file_info = f"[dim]File: {BENCH_FILES[scenario.file_key].name}[/]"
        if scenario.columns:
            col_info = f"[dim]Columns: {', '.join(scenario.columns)}[/dim]"
            console.print(
                Panel(f"{header}\n{file_info}\n{col_info}", border_style="cyan")
            )
        else:
            console.print(Panel(f"{header}\n{file_info}", border_style="cyan"))

        results = run_scenario(
            scenario, readers, warmup_runs, timed_runs, fail_fast=args.fail_fast
        )
        all_results[scenario.name] = results

    # Print comparison table
    print_comparison_table(all_results, reader_names, previous_results)

    # Save JSON output
    if args.output:
        # Build structured JSON report
        json_report = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "python_version": platform.python_version(),
                "platform": platform.platform(),
                "processor": platform.processor(),
                "machine": platform.machine(),
            },
            "configuration": {
                "warmup_runs": warmup_runs,
                "timed_runs": timed_runs,
                "fail_fast": args.fail_fast,
            },
            "scenarios": {},
            "results": {},
        }

        # Add scenario metadata
        for scenario_name in all_results.keys():
            scenario = SCENARIOS[scenario_name]
            file_path = BENCH_FILES[scenario.file_key]
            json_report["scenarios"][scenario_name] = {
                "description": scenario.description,
                "file_name": file_path.name,
                "file_size_bytes": (
                    file_path.stat().st_size if file_path.exists() else None
                ),
                "columns": scenario.columns,
            }

        # Add complete results including raw timing data
        for scenario, results in all_results.items():
            json_report["results"][scenario] = {}
            for reader, data in results.items():
                if isinstance(data, dict):
                    json_report["results"][scenario][reader] = data

        with open(args.output, "w") as f:
            json.dump(json_report, f, indent=2)
        console.print(f"\n[green]✓[/] Results saved to: [cyan]{args.output}[/]")


if __name__ == "__main__":
    main()
