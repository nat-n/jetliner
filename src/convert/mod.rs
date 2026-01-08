//! Avro to Arrow/Polars type conversion
//!
//! This module provides type mapping from Avro schemas to Arrow data types,
//! which are then used by Polars DataFrames. It also provides the
//! `DataFrameBuilder` for converting decoded Avro records to DataFrames.

mod arrow;
mod dataframe;

pub use arrow::{
    avro_to_arrow, avro_to_arrow_field, avro_to_arrow_schema, avro_to_arrow_schema_projected,
};
pub use dataframe::{BuilderConfig, BuilderError, DataFrameBuilder, ErrorMode};
