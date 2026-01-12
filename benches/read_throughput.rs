//! Benchmark suite for Jetliner read throughput
//!
//! This benchmark measures the performance of reading Avro files with:
//! - Different codecs (null, snappy, deflate, zstd)
//! - Different batch sizes
//! - Read throughput (records/sec, MB/s)
//!
//! # Requirements
//! - 10.8: Include performance benchmarks with reproducible results
//!
//! # Configuration
//!
//! Benchmark behavior can be configured via environment variables:
//!
//! - `BENCH_SAMPLE_SIZE`: Number of samples to collect (default: 100)
//! - `BENCH_MEASUREMENT_TIME`: Measurement time in seconds (default: 5)
//! - `BENCH_WARM_UP_TIME`: Warm-up time in seconds (default: 3)
//! - `BENCH_NOISE_THRESHOLD`: Noise threshold as a fraction (default: 0.01 = 1%)
//!
//! # Examples
//!
//! ```bash
//! # Quick run with fewer samples
//! BENCH_SAMPLE_SIZE=50 BENCH_MEASUREMENT_TIME=3 cargo bench
//!
//! # Thorough run with more samples and longer measurement time
//! BENCH_SAMPLE_SIZE=300 BENCH_MEASUREMENT_TIME=15 cargo bench
//!
//! # High-priority run with reduced noise
//! sudo BENCH_SAMPLE_SIZE=200 nice -n -20 cargo bench
//! ```

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;

use jetliner::reader::BufferConfig;
use jetliner::{AvroStreamReader, ErrorMode, LocalSource, ReaderConfig, StreamSource};

/// Get the path to a test data file
fn test_data_path(filename: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join(filename)
}

/// Helper to run async code in benchmarks
fn run_async<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

/// Configure Criterion based on environment variables
///
/// Allows runtime configuration of benchmark parameters without recompiling.
/// See module-level documentation for available environment variables.
fn configure_criterion() -> Criterion {
    let mut criterion = Criterion::default();

    // Read sample size from env (default: 100)
    if let Ok(sample_size) = std::env::var("BENCH_SAMPLE_SIZE") {
        if let Ok(size) = sample_size.parse::<usize>() {
            criterion = criterion.sample_size(size);
            eprintln!("Configured sample size: {}", size);
        } else {
            eprintln!("Warning: Invalid BENCH_SAMPLE_SIZE value: {}", sample_size);
        }
    }

    // Read measurement time from env (default: 5 seconds)
    if let Ok(measurement_time) = std::env::var("BENCH_MEASUREMENT_TIME") {
        if let Ok(secs) = measurement_time.parse::<u64>() {
            criterion = criterion.measurement_time(Duration::from_secs(secs));
            eprintln!("Configured measurement time: {}s", secs);
        } else {
            eprintln!(
                "Warning: Invalid BENCH_MEASUREMENT_TIME value: {}",
                measurement_time
            );
        }
    }

    // Read warm-up time from env (default: 3 seconds)
    if let Ok(warm_up_time) = std::env::var("BENCH_WARM_UP_TIME") {
        if let Ok(secs) = warm_up_time.parse::<u64>() {
            criterion = criterion.warm_up_time(Duration::from_secs(secs));
            eprintln!("Configured warm-up time: {}s", secs);
        } else {
            eprintln!(
                "Warning: Invalid BENCH_WARM_UP_TIME value: {}",
                warm_up_time
            );
        }
    }

    // Read noise threshold from env (default: 0.01 = 1%)
    if let Ok(noise_threshold) = std::env::var("BENCH_NOISE_THRESHOLD") {
        if let Ok(threshold) = noise_threshold.parse::<f64>() {
            criterion = criterion.noise_threshold(threshold);
            eprintln!("Configured noise threshold: {:.1}%", threshold * 100.0);
        } else {
            eprintln!(
                "Warning: Invalid BENCH_NOISE_THRESHOLD value: {}",
                noise_threshold
            );
        }
    }

    criterion
}

/// Count total records in a file by reading all batches
async fn count_records(path: &PathBuf, batch_size: usize) -> (usize, u64) {
    let source = LocalSource::open(path).await.unwrap();
    let file_size = source.size().await.unwrap();

    let config = ReaderConfig {
        batch_size,
        buffer_config: BufferConfig::default(),
        error_mode: ErrorMode::Strict,
        reader_schema: None,
        projected_columns: None,
    };

    let mut reader = AvroStreamReader::open(source, config).await.unwrap();
    let mut total_records = 0;

    while let Some(df) = reader.next_batch().await.unwrap() {
        total_records += df.height();
    }

    (total_records, file_size)
}

/// Read all records from a file
async fn read_all_records(path: &PathBuf, batch_size: usize) -> usize {
    let source = LocalSource::open(path).await.unwrap();

    let config = ReaderConfig {
        batch_size,
        buffer_config: BufferConfig::default(),
        error_mode: ErrorMode::Strict,
        reader_schema: None,
        projected_columns: None,
    };

    let mut reader = AvroStreamReader::open(source, config).await.unwrap();
    let mut total_records = 0;

    while let Some(df) = reader.next_batch().await.unwrap() {
        total_records += df.height();
        black_box(&df);
    }

    total_records
}

/// Benchmark reading with different codecs
fn bench_codecs(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_throughput");

    // Test files with different codecs
    let codec_files = [
        ("null", "apache-avro/weather.avro"),
        ("snappy", "apache-avro/weather-snappy.avro"),
        ("deflate", "apache-avro/weather-deflate.avro"),
        ("zstd", "apache-avro/weather-zstd.avro"),
    ];

    for (codec_name, file_path) in codec_files {
        let path = test_data_path(file_path);

        // Skip if file doesn't exist
        if !path.exists() {
            eprintln!("Skipping {} - file not found: {:?}", codec_name, path);
            continue;
        }

        // Get file size and record count for throughput calculation
        let (record_count, file_size) = run_async(count_records(&path, 10_000));

        if record_count == 0 {
            eprintln!("Skipping {} - no records in file", codec_name);
            continue;
        }

        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(BenchmarkId::new("read", codec_name), &path, |b, path| {
            b.iter(|| run_async(read_all_records(path, 10_000)));
        });
    }

    group.finish();
}

/// Benchmark reading with different batch sizes
fn bench_batch_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_size_throughput");

    // Use the large weather file for batch size benchmarks
    let path = test_data_path("large/weather-large.avro");

    if !path.exists() {
        eprintln!(
            "Skipping batch size benchmarks - large file not found: {:?}",
            path
        );
        return;
    }

    // Get file size for throughput calculation
    let (_, file_size) = run_async(count_records(&path, 100_000));
    group.throughput(Throughput::Bytes(file_size));

    // Test different batch sizes
    let batch_sizes = [1_000, 10_000, 50_000, 100_000];

    for batch_size in batch_sizes {
        group.bench_with_input(
            BenchmarkId::new("read", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter(|| run_async(read_all_records(&path, batch_size)));
            },
        );
    }

    group.finish();
}

/// Benchmark reading the large file for overall throughput measurement
fn bench_large_file(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_file_throughput");

    let path = test_data_path("large/weather-large.avro");

    if !path.exists() {
        eprintln!("Skipping large file benchmark - file not found: {:?}", path);
        return;
    }

    // Get file size and record count
    let (record_count, file_size) = run_async(count_records(&path, 100_000));

    eprintln!(
        "Large file: {} records, {} bytes ({:.2} MB)",
        record_count,
        file_size,
        file_size as f64 / 1_048_576.0
    );

    // Set throughput in bytes for MB/s calculation
    group.throughput(Throughput::Bytes(file_size));

    group.bench_function("read_all", |b| {
        b.iter(|| run_async(read_all_records(&path, 100_000)));
    });

    // Also measure throughput in elements (records)
    group.throughput(Throughput::Elements(record_count as u64));

    group.bench_function("read_all_records", |b| {
        b.iter(|| run_async(read_all_records(&path, 100_000)));
    });

    group.finish();
}

/// Benchmark with projection (reading subset of columns)
fn bench_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("projection_throughput");

    let path = test_data_path("large/weather-large.avro");

    if !path.exists() {
        eprintln!(
            "Skipping projection benchmark - large file not found: {:?}",
            path
        );
        return;
    }

    let (_, file_size) = run_async(count_records(&path, 100_000));
    group.throughput(Throughput::Bytes(file_size));

    // Read all columns
    group.bench_function("all_columns", |b| {
        b.iter(|| run_async(read_all_records(&path, 100_000)));
    });

    // Read with projection (single column)
    group.bench_function("single_column", |b| {
        b.iter(|| {
            run_async(async {
                let source = LocalSource::open(&path).await.unwrap();

                let config = ReaderConfig {
                    batch_size: 100_000,
                    buffer_config: BufferConfig::default(),
                    error_mode: ErrorMode::Strict,
                    reader_schema: None,
                    projected_columns: Some(vec!["station".to_string()]),
                };

                let mut reader = AvroStreamReader::open(source, config).await.unwrap();
                let mut total_records = 0;

                while let Some(df) = reader.next_batch().await.unwrap() {
                    total_records += df.height();
                    black_box(&df);
                }

                total_records
            })
        });
    });

    // Read with projection (two columns)
    group.bench_function("two_columns", |b| {
        b.iter(|| {
            run_async(async {
                let source = LocalSource::open(&path).await.unwrap();

                let config = ReaderConfig {
                    batch_size: 100_000,
                    buffer_config: BufferConfig::default(),
                    error_mode: ErrorMode::Strict,
                    reader_schema: None,
                    projected_columns: Some(vec!["station".to_string(), "temp".to_string()]),
                };

                let mut reader = AvroStreamReader::open(source, config).await.unwrap();
                let mut total_records = 0;

                while let Some(df) = reader.next_batch().await.unwrap() {
                    total_records += df.height();
                    black_box(&df);
                }

                total_records
            })
        });
    });

    group.finish();
}

/// Benchmark buffer configuration impact
fn bench_buffer_config(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_config_throughput");

    let path = test_data_path("large/weather-large.avro");

    if !path.exists() {
        eprintln!(
            "Skipping buffer config benchmark - large file not found: {:?}",
            path
        );
        return;
    }

    let (_, file_size) = run_async(count_records(&path, 100_000));
    group.throughput(Throughput::Bytes(file_size));

    // Test different buffer configurations
    let buffer_configs: [(&str, BufferConfig); 3] = [
        ("1_block", BufferConfig::new(1, 16 * 1024 * 1024)),
        ("4_blocks", BufferConfig::new(4, 64 * 1024 * 1024)),
        ("8_blocks", BufferConfig::new(8, 128 * 1024 * 1024)),
    ];

    for (name, buffer_config) in buffer_configs {
        group.bench_function(name, |b| {
            let buffer_config = buffer_config.clone();
            b.iter(|| {
                let buffer_config = buffer_config.clone();
                run_async(async {
                    let source = LocalSource::open(&path).await.unwrap();

                    let config = ReaderConfig {
                        batch_size: 100_000,
                        buffer_config,
                        error_mode: ErrorMode::Strict,
                        reader_schema: None,
                        projected_columns: None,
                    };

                    let mut reader = AvroStreamReader::open(source, config).await.unwrap();
                    let mut total_records = 0;

                    while let Some(df) = reader.next_batch().await.unwrap() {
                        total_records += df.height();
                        black_box(&df);
                    }

                    total_records
                })
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = bench_codecs, bench_batch_sizes, bench_large_file, bench_projection, bench_buffer_config
}

criterion_main!(benches);
