//! S3 source implementation
//!
//! Provides async S3 access for reading Avro files from Amazon S3.

use async_trait::async_trait;
use aws_sdk_s3::Client;
use bytes::Bytes;

use super::traits::StreamSource;
use crate::error::SourceError;

/// Configuration options for S3 connections.
///
/// This struct allows overriding default AWS SDK configuration,
/// enabling connections to S3-compatible services like MinIO,
/// LocalStack, or Cloudflare R2.
///
/// # Requirements
/// - 4.8: Accept optional `storage_options` parameter
/// - 4.9: Connect to custom endpoint when `endpoint_url` is provided
/// - 4.10: Use provided credentials when specified
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    /// Custom S3 endpoint URL (for MinIO, LocalStack, R2, etc.)
    pub endpoint_url: Option<String>,
    /// AWS access key ID (overrides environment)
    pub aws_access_key_id: Option<String>,
    /// AWS secret access key (overrides environment)
    pub aws_secret_access_key: Option<String>,
    /// AWS region (overrides environment)
    pub region: Option<String>,
}

impl S3Config {
    /// Create a new empty S3Config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the endpoint URL for S3-compatible services.
    pub fn with_endpoint_url(mut self, endpoint_url: impl Into<String>) -> Self {
        self.endpoint_url = Some(endpoint_url.into());
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

    /// Check if any configuration is set.
    pub fn is_empty(&self) -> bool {
        self.endpoint_url.is_none()
            && self.aws_access_key_id.is_none()
            && self.aws_secret_access_key.is_none()
            && self.region.is_none()
    }
}

/// A data source for reading from Amazon S3.
///
/// Uses the AWS SDK for S3 access with support for range requests
/// via the GetObject Range header.
///
/// # Requirements
/// - 4.2: Use AWS SDK for S3 data access
/// - 4.5: Authenticate using environment credentials
/// - 4.6: Return descriptive error on authentication failure
/// - 4.7: Return descriptive error if file does not exist
/// - 4.8: Accept optional `storage_options` parameter
/// - 4.9: Connect to custom endpoint when `endpoint_url` is provided
/// - 4.10: Use provided credentials when specified
pub struct S3Source {
    /// AWS S3 client
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
    /// * `client` - An AWS S3 client
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
        // Get object metadata to verify access and cache size
        let head_result = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| Self::map_sdk_error(&bucket, &key, e))?;

        let object_size = head_result.content_length().unwrap_or(0) as u64;

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
    /// - 4.9: Connect to custom endpoint when `endpoint_url` is provided
    /// - 4.10: Use provided credentials when specified
    /// - 4.11: `storage_options` takes precedence over environment variables
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
    async fn build_client(config: Option<S3Config>) -> Result<Client, SourceError> {
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

        // Build S3 client config, potentially with custom endpoint
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

        if let Some(endpoint_url) = &config.endpoint_url {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint_url);
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
    /// - 4.9: Connect to custom endpoint when `endpoint_url` is provided
    /// - 4.10: Use provided credentials when specified
    /// - 4.12: Successfully read files from S3-compatible services
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
    fn map_sdk_error<E: std::fmt::Display>(bucket: &str, key: &str, err: E) -> SourceError {
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

    async fn size(&self) -> Result<u64, SourceError> {
        Ok(self.object_size)
    }

    async fn read_from(&self, offset: u64) -> Result<Bytes, SourceError> {
        if offset >= self.object_size {
            return Ok(Bytes::new());
        }

        // Use open-ended range request
        let range = format!("bytes={}-", offset);

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
        assert!(config.endpoint_url.is_none());
        assert!(config.aws_access_key_id.is_none());
        assert!(config.aws_secret_access_key.is_none());
        assert!(config.region.is_none());
        assert!(config.is_empty());
    }

    #[test]
    fn test_s3_config_new() {
        let config = S3Config::new();
        assert!(config.is_empty());
    }

    #[test]
    fn test_s3_config_with_endpoint_url() {
        let config = S3Config::new().with_endpoint_url("http://localhost:9000");
        assert_eq!(
            config.endpoint_url,
            Some("http://localhost:9000".to_string())
        );
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
    fn test_s3_config_builder_chain() {
        let config = S3Config::new()
            .with_endpoint_url("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1");

        assert_eq!(
            config.endpoint_url,
            Some("http://localhost:9000".to_string())
        );
        assert_eq!(config.aws_access_key_id, Some("minioadmin".to_string()));
        assert_eq!(config.aws_secret_access_key, Some("minioadmin".to_string()));
        assert_eq!(config.region, Some("us-east-1".to_string()));
        assert!(!config.is_empty());
    }

    #[test]
    fn test_s3_config_clone() {
        let config = S3Config::new()
            .with_endpoint_url("http://localhost:9000")
            .with_access_key_id("test");

        let cloned = config.clone();
        assert_eq!(config.endpoint_url, cloned.endpoint_url);
        assert_eq!(config.aws_access_key_id, cloned.aws_access_key_id);
    }

    #[test]
    fn test_s3_config_debug() {
        let config = S3Config::new().with_endpoint_url("http://localhost:9000");
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("S3Config"));
        assert!(debug_str.contains("localhost:9000"));
    }
}
