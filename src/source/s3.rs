//! S3 source implementation
//!
//! Provides async S3 access for reading Avro files from Amazon S3.
//!
//! # Requirements
//! - 8.4: The `max_retries` key controls retry count for transient S3 failures (default: 2)
//! - 8.5: Retryable errors include: connection timeouts, 5xx responses, throttling (429)
//! - 8.6: The reader uses exponential backoff between retries
//! - 8.7: Storage options take precedence over environment variables
//! - 8.8: Retry logic is implemented in Rust

use std::collections::HashMap;
use tracing::{debug, info};

use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_sdk_s3::Client;
use bytes::Bytes;

use super::traits::StreamSource;
use crate::error::SourceError;

/// Default number of retry attempts for transient S3 failures.
pub const DEFAULT_MAX_RETRIES: usize = 2;

/// Configuration options for S3 connections.
///
/// This struct allows overriding default AWS SDK configuration,
/// enabling connections to S3-compatible services like MinIO,
/// LocalStack, or Cloudflare R2.
///
/// # Key Names
/// The `endpoint` key is used (not `endpoint_url`) to align with Polars'
/// `AmazonS3ConfigKey::Endpoint`.
///
/// # Requirements
/// - 4.8: Accept optional `storage_options` parameter
/// - 4.9: Connect to custom endpoint when `endpoint` is provided
/// - 4.10: Use provided credentials when specified
/// - 8.4: The `max_retries` key controls retry count for transient S3 failures
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    /// Custom S3 endpoint (for MinIO, LocalStack, R2, etc.)
    ///
    /// Key: "endpoint" (aligned with Polars `AmazonS3ConfigKey::Endpoint`)
    pub endpoint: Option<String>,
    /// AWS access key ID (overrides environment)
    pub aws_access_key_id: Option<String>,
    /// AWS secret access key (overrides environment)
    pub aws_secret_access_key: Option<String>,
    /// AWS region (overrides environment)
    pub region: Option<String>,
    /// Maximum retry attempts for transient failures (default: 2)
    pub max_retries: usize,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            region: None,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}

impl S3Config {
    /// Create a new empty S3Config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse S3 config from a dictionary (e.g., Python's `storage_options`).
    ///
    /// # Supported Keys
    /// - `endpoint`: Custom S3 endpoint URL
    /// - `aws_access_key_id`: AWS access key ID
    /// - `aws_secret_access_key`: AWS secret access key
    /// - `region`: AWS region
    /// - `max_retries`: Maximum retry attempts (parsed from string, default: 2)
    pub fn from_dict(opts: &HashMap<String, String>) -> Self {
        Self {
            endpoint: opts.get("endpoint").cloned(),
            aws_access_key_id: opts.get("aws_access_key_id").cloned(),
            aws_secret_access_key: opts.get("aws_secret_access_key").cloned(),
            region: opts.get("region").cloned(),
            max_retries: opts
                .get("max_retries")
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_MAX_RETRIES),
        }
    }

    /// Set the endpoint for S3-compatible services.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the AWS access key ID.
    pub fn with_access_key_id(mut self, access_key_id: impl Into<String>) -> Self {
        self.aws_access_key_id = Some(access_key_id.into());
        self
    }

    /// Set the AWS secret access key.
    pub fn with_secret_access_key(mut self, secret_access_key: impl Into<String>) -> Self {
        self.aws_secret_access_key = Some(secret_access_key.into());
        self
    }

    /// Set the AWS region.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set the maximum retry attempts.
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Check if any configuration is set (excluding max_retries default).
    pub fn is_empty(&self) -> bool {
        self.endpoint.is_none()
            && self.aws_access_key_id.is_none()
            && self.aws_secret_access_key.is_none()
            && self.region.is_none()
    }
}

/// A data source for reading from Amazon S3.
///
/// Uses the AWS SDK for S3 access with support for range requests
/// via the GetObject Range header. Retry logic is handled by the
/// AWS SDK's built-in RetryConfig, configured via `storage_options`.
///
/// # Requirements
/// - 4.2: Use AWS SDK for S3 data access
/// - 4.5: Authenticate using environment credentials
/// - 4.6: Return descriptive error on authentication failure
/// - 4.7: Return descriptive error if file does not exist
/// - 4.8: Accept optional `storage_options` parameter
/// - 4.9: Connect to custom endpoint when `endpoint` is provided
/// - 4.10: Use provided credentials when specified
/// - 8.4: Use `max_retries` from configuration (via SDK RetryConfig)
/// - 8.6: SDK uses jittered exponential backoff between retries
pub struct S3Source {
    /// AWS S3 client (with retry config baked in)
    client: Client,
    /// S3 bucket name
    bucket: String,
    /// S3 object key
    key: String,
    /// Cached object size
    object_size: u64,
}

impl S3Source {
    /// Create a new S3Source from an existing client.
    ///
    /// # Arguments
    /// * `client` - An AWS S3 client (should have retry config already set)
    /// * `bucket` - The S3 bucket name
    /// * `key` - The S3 object key
    ///
    /// # Returns
    /// A new `S3Source` instance, or an error if the object cannot be accessed.
    ///
    /// # Errors
    /// Returns `SourceError::NotFound` if the object doesn't exist.
    /// Returns `SourceError::AuthenticationFailed` if credentials are invalid.
    /// Returns `SourceError::S3Error` for other S3 errors.
    pub async fn new(client: Client, bucket: String, key: String) -> Result<Self, SourceError> {
        debug!(bucket = %bucket, key = %key, "Opening S3 object");

        // Get object metadata to verify access and cache size
        // (retry is handled by SDK's RetryConfig)
        let head_result = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| Self::map_sdk_error(&bucket, &key, e))?;

        let object_size = head_result.content_length().unwrap_or(0) as u64;

        info!(
            bucket = %bucket,
            key = %key,
            size_bytes = object_size,
            "Opened S3 object"
        );

        Ok(Self {
            client,
            bucket,
            key,
            object_size,
        })
    }

    /// Open an S3 object using environment credentials.
    ///
    /// This method loads AWS configuration from the environment, including:
    /// - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    /// - AWS_SESSION_TOKEN (for temporary credentials)
    /// - AWS_REGION or AWS_DEFAULT_REGION
    /// - Web identity tokens (for EKS/Lambda)
    ///
    /// # Arguments
    /// * `bucket` - The S3 bucket name
    /// * `key` - The S3 object key
    ///
    /// # Returns
    /// A new `S3Source` instance, or an error if the object cannot be accessed.
    pub async fn open(bucket: String, key: String) -> Result<Self, SourceError> {
        Self::open_with_config(bucket, key, None).await
    }

    /// Open an S3 object with optional configuration overrides.
    ///
    /// This method allows connecting to S3-compatible services (MinIO, LocalStack, R2)
    /// by providing custom endpoint URLs and credentials.
    ///
    /// # Arguments
    /// * `bucket` - The S3 bucket name
    /// * `key` - The S3 object key
    /// * `config` - Optional S3 configuration overrides
    ///
    /// # Returns
    /// A new `S3Source` instance, or an error if the object cannot be accessed.
    ///
    /// # Requirements
    /// - 4.8: Accept optional `storage_options` parameter
    /// - 4.9: Connect to custom endpoint when `endpoint` is provided
    /// - 4.10: Use provided credentials when specified
    /// - 4.11: `storage_options` takes precedence over environment variables
    /// - 8.4: Use `max_retries` from configuration (via SDK RetryConfig)
    pub async fn open_with_config(
        bucket: String,
        key: String,
        config: Option<S3Config>,
    ) -> Result<Self, SourceError> {
        let client = Self::build_client(config).await?;
        Self::new(client, bucket, key).await
    }

    /// Build an S3 client with optional configuration overrides.
    ///
    /// When config is provided, values take precedence over environment variables.
    /// Retry logic is delegated to the AWS SDK's built-in RetryConfig.
    pub async fn build_client(config: Option<S3Config>) -> Result<Client, SourceError> {
        let config = config.unwrap_or_default();

        // Start with default AWS config from environment
        let mut aws_config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

        // Override region if provided
        if let Some(region) = &config.region {
            aws_config_loader = aws_config_loader.region(aws_config::Region::new(region.clone()));
        }

        // Override credentials if provided
        if config.aws_access_key_id.is_some() || config.aws_secret_access_key.is_some() {
            let access_key = config.aws_access_key_id.clone().unwrap_or_default();
            let secret_key = config.aws_secret_access_key.clone().unwrap_or_default();

            let credentials = aws_sdk_s3::config::Credentials::new(
                access_key,
                secret_key,
                None, // session token
                None, // expiry
                "jetliner-storage-options",
            );
            aws_config_loader = aws_config_loader.credentials_provider(credentials);
        }

        let aws_config = aws_config_loader.load().await;

        // Configure SDK retry (max_attempts = max_retries + 1 because SDK counts initial attempt)
        let retry_config = RetryConfig::standard().with_max_attempts(config.max_retries as u32 + 1);

        // Build S3 client config, potentially with custom endpoint
        let mut s3_config_builder =
            aws_sdk_s3::config::Builder::from(&aws_config).retry_config(retry_config);

        if let Some(endpoint) = &config.endpoint {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint);
            // For S3-compatible services, we typically need path-style addressing
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let s3_config = s3_config_builder.build();
        Ok(Client::from_conf(s3_config))
    }

    /// Parse an S3 URI into bucket and key components.
    ///
    /// # Arguments
    /// * `uri` - An S3 URI in the format `s3://bucket/key`
    ///
    /// # Returns
    /// A tuple of (bucket, key), or an error if the URI is invalid.
    pub fn parse_uri(uri: &str) -> Result<(String, String), SourceError> {
        let uri = uri.strip_prefix("s3://").ok_or_else(|| {
            SourceError::S3Error(format!("Invalid S3 URI: must start with 's3://': {}", uri))
        })?;

        let (bucket, key) = uri.split_once('/').ok_or_else(|| {
            SourceError::S3Error(format!("Invalid S3 URI: missing key: s3://{}", uri))
        })?;

        if bucket.is_empty() {
            return Err(SourceError::S3Error(
                "Invalid S3 URI: empty bucket name".to_string(),
            ));
        }

        if key.is_empty() {
            return Err(SourceError::S3Error(
                "Invalid S3 URI: empty key".to_string(),
            ));
        }

        Ok((bucket.to_string(), key.to_string()))
    }

    /// Open an S3 object from a URI using environment credentials.
    ///
    /// # Arguments
    /// * `uri` - An S3 URI in the format `s3://bucket/key`
    ///
    /// # Returns
    /// A new `S3Source` instance, or an error if the object cannot be accessed.
    pub async fn from_uri(uri: &str) -> Result<Self, SourceError> {
        Self::from_uri_with_config(uri, None).await
    }

    /// Open an S3 object from a URI with optional configuration overrides.
    ///
    /// # Arguments
    /// * `uri` - An S3 URI in the format `s3://bucket/key`
    /// * `config` - Optional S3 configuration overrides
    ///
    /// # Returns
    /// A new `S3Source` instance, or an error if the object cannot be accessed.
    ///
    /// # Requirements
    /// - 4.8: Accept optional `storage_options` parameter
    /// - 4.9: Connect to custom endpoint when `endpoint` is provided
    /// - 4.10: Use provided credentials when specified
    /// - 4.12: Successfully read files from S3-compatible services
    /// - 8.4: Use `max_retries` from configuration
    pub async fn from_uri_with_config(
        uri: &str,
        config: Option<S3Config>,
    ) -> Result<Self, SourceError> {
        let (bucket, key) = Self::parse_uri(uri)?;
        Self::open_with_config(bucket, key, config).await
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Get the object key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Map AWS SDK errors to SourceError.
    pub fn map_sdk_error<E: std::fmt::Display>(bucket: &str, key: &str, err: E) -> SourceError {
        let err_str = err.to_string();

        // Check for common error patterns
        if err_str.contains("NoSuchKey") || err_str.contains("NotFound") || err_str.contains("404")
        {
            SourceError::NotFound(format!("s3://{}/{}", bucket, key))
        } else if err_str.contains("AccessDenied")
            || err_str.contains("InvalidAccessKeyId")
            || err_str.contains("SignatureDoesNotMatch")
            || err_str.contains("ExpiredToken")
            || err_str.contains("403")
        {
            SourceError::AuthenticationFailed(format!(
                "Access denied to s3://{}/{}: {}",
                bucket, key, err_str
            ))
        } else if err_str.contains("NoSuchBucket") {
            SourceError::NotFound(format!("Bucket not found: {}", bucket))
        } else {
            SourceError::S3Error(format!("S3 error for s3://{}/{}: {}", bucket, key, err_str))
        }
    }
}

#[async_trait]
impl StreamSource for S3Source {
    async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, SourceError> {
        if offset >= self.object_size {
            return Err(SourceError::S3Error(format!(
                "Offset {} is beyond object size {} for s3://{}/{}",
                offset, self.object_size, self.bucket, self.key
            )));
        }

        // Calculate the end byte (inclusive, as per HTTP Range spec)
        let end = (offset + length as u64 - 1).min(self.object_size - 1);
        let range = format!("bytes={}-{}", offset, end);

        debug!(
            bucket = %self.bucket,
            key = %self.key,
            range = %range,
            "S3 GET range request"
        );

        // Retry is handled by SDK's RetryConfig
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(&range)
            .send()
            .await
            .map_err(|e| Self::map_sdk_error(&self.bucket, &self.key, e))?;

        let body =
            response.body.collect().await.map_err(|e| {
                SourceError::S3Error(format!("Failed to read S3 response body: {}", e))
            })?;

        let bytes = body.into_bytes();
        debug!(
            bucket = %self.bucket,
            key = %self.key,
            bytes_received = bytes.len(),
            "S3 GET range completed"
        );

        Ok(bytes)
    }

    async fn size(&self) -> Result<u64, SourceError> {
        Ok(self.object_size)
    }

    async fn read_from(&self, offset: u64) -> Result<Bytes, SourceError> {
        if offset >= self.object_size {
            return Ok(Bytes::new());
        }

        // Use open-ended range request
        let range = format!("bytes={}-", offset);

        // Retry is handled by SDK's RetryConfig
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(&range)
            .send()
            .await
            .map_err(|e| Self::map_sdk_error(&self.bucket, &self.key, e))?;

        let body =
            response.body.collect().await.map_err(|e| {
                SourceError::S3Error(format!("Failed to read S3 response body: {}", e))
            })?;

        Ok(body.into_bytes())
    }
}

impl std::fmt::Debug for S3Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Source")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("object_size", &self.object_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_uri_valid() {
        let (bucket, key) = S3Source::parse_uri("s3://my-bucket/path/to/file.avro").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.avro");
    }

    #[test]
    fn test_parse_uri_simple_key() {
        let (bucket, key) = S3Source::parse_uri("s3://bucket/file.avro").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "file.avro");
    }

    #[test]
    fn test_parse_uri_missing_prefix() {
        let result = S3Source::parse_uri("bucket/key");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_uri_missing_key() {
        let result = S3Source::parse_uri("s3://bucket");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_uri_empty_bucket() {
        let result = S3Source::parse_uri("s3:///key");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_uri_empty_key() {
        let result = S3Source::parse_uri("s3://bucket/");
        assert!(result.is_err());
    }

    // S3Config tests

    #[test]
    fn test_s3_config_default() {
        let config = S3Config::default();
        assert!(config.endpoint.is_none());
        assert!(config.aws_access_key_id.is_none());
        assert!(config.aws_secret_access_key.is_none());
        assert!(config.region.is_none());
        assert_eq!(config.max_retries, DEFAULT_MAX_RETRIES);
        assert!(config.is_empty());
    }

    #[test]
    fn test_s3_config_new() {
        let config = S3Config::new();
        assert!(config.is_empty());
        assert_eq!(config.max_retries, DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn test_s3_config_with_endpoint() {
        let config = S3Config::new().with_endpoint("http://localhost:9000");
        assert_eq!(config.endpoint, Some("http://localhost:9000".to_string()));
        assert!(!config.is_empty());
    }

    #[test]
    fn test_s3_config_with_credentials() {
        let config = S3Config::new()
            .with_access_key_id("AKIAIOSFODNN7EXAMPLE")
            .with_secret_access_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        assert_eq!(
            config.aws_access_key_id,
            Some("AKIAIOSFODNN7EXAMPLE".to_string())
        );
        assert_eq!(
            config.aws_secret_access_key,
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string())
        );
        assert!(!config.is_empty());
    }

    #[test]
    fn test_s3_config_with_region() {
        let config = S3Config::new().with_region("us-west-2");
        assert_eq!(config.region, Some("us-west-2".to_string()));
        assert!(!config.is_empty());
    }

    #[test]
    fn test_s3_config_with_max_retries() {
        let config = S3Config::new().with_max_retries(5);
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn test_s3_config_builder_chain() {
        let config = S3Config::new()
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_max_retries(5);

        assert_eq!(config.endpoint, Some("http://localhost:9000".to_string()));
        assert_eq!(config.aws_access_key_id, Some("minioadmin".to_string()));
        assert_eq!(config.aws_secret_access_key, Some("minioadmin".to_string()));
        assert_eq!(config.region, Some("us-east-1".to_string()));
        assert_eq!(config.max_retries, 5);
        assert!(!config.is_empty());
    }

    #[test]
    fn test_s3_config_clone() {
        let config = S3Config::new()
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("test")
            .with_max_retries(3);

        let cloned = config.clone();
        assert_eq!(config.endpoint, cloned.endpoint);
        assert_eq!(config.aws_access_key_id, cloned.aws_access_key_id);
        assert_eq!(config.max_retries, cloned.max_retries);
    }

    #[test]
    fn test_s3_config_debug() {
        let config = S3Config::new().with_endpoint("http://localhost:9000");
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("S3Config"));
        assert!(debug_str.contains("localhost:9000"));
    }

    #[test]
    fn test_s3_config_from_dict() {
        let mut dict = HashMap::new();
        dict.insert("endpoint".to_string(), "http://localhost:9000".to_string());
        dict.insert("aws_access_key_id".to_string(), "minioadmin".to_string());
        dict.insert("aws_secret_access_key".to_string(), "secret".to_string());
        dict.insert("region".to_string(), "us-east-1".to_string());
        dict.insert("max_retries".to_string(), "5".to_string());

        let config = S3Config::from_dict(&dict);
        assert_eq!(config.endpoint, Some("http://localhost:9000".to_string()));
        assert_eq!(config.aws_access_key_id, Some("minioadmin".to_string()));
        assert_eq!(config.aws_secret_access_key, Some("secret".to_string()));
        assert_eq!(config.region, Some("us-east-1".to_string()));
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn test_s3_config_from_dict_empty() {
        let dict = HashMap::new();
        let config = S3Config::from_dict(&dict);
        assert!(config.endpoint.is_none());
        assert_eq!(config.max_retries, DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn test_s3_config_from_dict_invalid_max_retries() {
        let mut dict = HashMap::new();
        dict.insert("max_retries".to_string(), "invalid".to_string());
        let config = S3Config::from_dict(&dict);
        assert_eq!(config.max_retries, DEFAULT_MAX_RETRIES); // falls back to default
    }
}
