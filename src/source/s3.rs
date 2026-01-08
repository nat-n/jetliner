//! S3 source implementation
//!
//! Provides async S3 access for reading Avro files from Amazon S3.

use async_trait::async_trait;
use aws_sdk_s3::Client;
use bytes::Bytes;

use super::traits::StreamSource;
use crate::error::SourceError;

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
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        let client = Client::new(&config);
        Self::new(client, bucket, key).await
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
        let (bucket, key) = Self::parse_uri(uri)?;
        Self::open(bucket, key).await
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
}
