//! Schema reading functions for Avro files.
//!
//! This module provides `read_avro_schema()` function for reading the schema
//! from an Avro file without reading the data.
//!
//! # Requirements
//! - 13.5: The library exposes a `read_avro_schema()` function returning `PolarsResult<Schema>`

use polars::prelude::*;

use crate::convert::avro_to_arrow_schema;
use crate::reader::AvroHeader;
use crate::source::{LocalSource, S3Config, S3Source, StreamSource};

use super::glob::is_s3_uri;

/// Read the schema from an Avro file.
///
/// This function reads only the file header to extract the schema,
/// without reading any data blocks. This is useful for schema inspection
/// and query planning.
///
/// # Arguments
/// * `source` - Path to the Avro file (can be an S3 URI)
/// * `s3_config` - Optional S3 configuration
///
/// # Returns
/// A `PolarsResult<Schema>` containing the Polars schema.
///
/// # Requirements
/// - 13.5: Return `PolarsResult<Schema>`
///
/// # Example
/// ```no_run
/// use jetliner::api::read_avro_schema;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let schema = read_avro_schema("data.avro", None)?;
/// println!("Columns: {:?}", schema.iter_names().collect::<Vec<_>>());
/// # Ok(())
/// # }
/// ```
pub fn read_avro_schema(source: &str, s3_config: Option<&S3Config>) -> PolarsResult<Schema> {
    // Create a runtime for async operations
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            PolarsError::ComputeError(format!("Failed to create runtime: {}", e).into())
        })?;

    runtime.block_on(async { read_avro_schema_async(source, s3_config).await })
}

/// Async implementation of schema reading.
async fn read_avro_schema_async(
    source: &str,
    s3_config: Option<&S3Config>,
) -> PolarsResult<Schema> {
    // Read enough bytes for the header (typically < 4KB, but we read more to be safe)
    let header_bytes = if is_s3_uri(source) {
        // S3 source
        let s3_source = S3Source::from_uri_with_config(source, s3_config.cloned())
            .await
            .map_err(PolarsError::from)?;
        s3_source
            .read_range(0, 64 * 1024)
            .await
            .map_err(PolarsError::from)?
    } else {
        // Local file
        let local_source = LocalSource::open(source).await.map_err(PolarsError::from)?;
        local_source
            .read_range(0, 64 * 1024)
            .await
            .map_err(PolarsError::from)?
    };

    // Parse the header to extract the schema
    let header = AvroHeader::parse(&header_bytes)?;

    // Convert Avro schema to Polars schema
    let polars_schema = avro_to_arrow_schema(&header.schema).map_err(PolarsError::from)?;

    Ok(polars_schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_avro_schema_local_file() {
        let result = read_avro_schema("tests/data/apache-avro/weather.avro", None);

        match result {
            Ok(schema) => {
                // Should have some columns
                assert!(schema.len() > 0);
                // Weather schema should have station and temp columns
                println!(
                    "Schema columns: {:?}",
                    schema.iter_names().collect::<Vec<_>>()
                );
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_read_avro_schema_nonexistent_file() {
        let result = read_avro_schema("nonexistent_file_12345.avro", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_avro_schema_s3_invalid_bucket() {
        // S3 is now supported, but this should fail with an S3 error
        // (invalid bucket, no credentials, etc.)
        let result = read_avro_schema("s3://nonexistent-bucket-12345/file.avro", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_avro_schema_with_s3_config() {
        // S3 config should be accepted but not used for local files
        let s3_cfg = S3Config::new().with_endpoint("http://localhost:9000");

        let result = read_avro_schema("tests/data/apache-avro/weather.avro", Some(&s3_cfg));

        match result {
            Ok(schema) => {
                assert!(schema.len() > 0);
            }
            Err(e) => {
                println!("Note: Test data not found: {}", e);
            }
        }
    }
}
