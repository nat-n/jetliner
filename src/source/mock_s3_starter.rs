// Mock S3 Source for Error Injection Testing
// This is a starter implementation - expand as needed
//
// Usage in tests:
//   let mock = MockS3Source::new(avro_data)
//       .with_error_on_call(2, SourceError::S3Error("timeout".into()));
//   let reader = BlockReader::new(mock).await;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::sleep;

use super::traits::StreamSource;
use crate::error::SourceError;

/// Configuration for when to inject an error
#[derive(Debug, Clone)]
pub enum ErrorTrigger {
    /// Trigger on specific call number (0-indexed)
    OnCall(usize),
    /// Trigger when reading specific byte offset
    OnOffset(u64),
    /// Trigger when reading within byte range
    OnRange { start: u64, end: u64 },
}

/// A rule for injecting errors
#[derive(Debug, Clone)]
pub struct ErrorInjectionRule {
    pub trigger: ErrorTrigger,
    pub error: SourceError,
    /// How many times to apply (None = infinite)
    pub remaining: Option<usize>,
}

/// Mock S3 source for testing error scenarios
pub struct MockS3Source {
    /// The actual data to serve
    data: Bytes,
    /// Error injection rules
    error_rules: Vec<ErrorInjectionRule>,
    /// Call counter
    call_count: Arc<AtomicUsize>,
    /// Successful call counter
    success_count: Arc<AtomicUsize>,
    /// Optional latency simulation
    latency: Option<Duration>,
}

impl MockS3Source {
    /// Create a new mock source with the given data
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self {
            data: data.into(),
            error_rules: Vec::new(),
            call_count: Arc::new(AtomicUsize::new(0)),
            success_count: Arc::new(AtomicUsize::new(0)),
            latency: None,
        }
    }

    /// Add an error that triggers on a specific call number
    pub fn with_error_on_call(mut self, call_num: usize, error: SourceError) -> Self {
        self.error_rules.push(ErrorInjectionRule {
            trigger: ErrorTrigger::OnCall(call_num),
            error,
            remaining: Some(1),
        });
        self
    }

    /// Add an error that triggers when reading a specific offset
    pub fn with_error_on_offset(mut self, offset: u64, error: SourceError) -> Self {
        self.error_rules.push(ErrorInjectionRule {
            trigger: ErrorTrigger::OnOffset(offset),
            error,
            remaining: Some(1),
        });
        self
    }

    /// Add an error that triggers within a byte range
    pub fn with_error_on_range(mut self, start: u64, end: u64, error: SourceError) -> Self {
        self.error_rules.push(ErrorInjectionRule {
            trigger: ErrorTrigger::OnRange { start, end },
            error,
            remaining: Some(1),
        });
        self
    }

    /// Add network latency simulation
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = Some(latency);
        self
    }

    /// Get total number of calls made
    pub fn call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }

    /// Get number of successful calls
    pub fn success_count(&self) -> usize {
        self.success_count.load(Ordering::SeqCst)
    }

    /// Check if any error rule should trigger
    fn check_error_rules(&mut self, offset: u64) -> Option<SourceError> {
        let call_num = self.call_count.load(Ordering::SeqCst);

        for rule in &mut self.error_rules {
            let should_trigger = match &rule.trigger {
                ErrorTrigger::OnCall(n) => call_num == *n,
                ErrorTrigger::OnOffset(o) => offset == *o,
                ErrorTrigger::OnRange { start, end } => offset >= *start && offset < *end,
            };

            if should_trigger {
                if let Some(remaining) = &mut rule.remaining {
                    if *remaining > 0 {
                        *remaining -= 1;
                        return Some(rule.error.clone());
                    }
                } else {
                    return Some(rule.error.clone());
                }
            }
        }

        None
    }
}

#[async_trait]
impl StreamSource for MockS3Source {
    async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, SourceError> {
        // Increment call counter
        self.call_count.fetch_add(1, Ordering::SeqCst);

        // Simulate latency if configured
        if let Some(latency) = self.latency {
            sleep(latency).await;
        }

        // Check for error injection (need mut access - use interior mutability in real impl)
        // For now, this is a simplified version
        // TODO: Use Arc<Mutex<Vec<ErrorInjectionRule>>> for proper mutability

        // Normal read
        if offset >= self.data.len() as u64 {
            return Ok(Bytes::new());
        }

        let start = offset as usize;
        let end = (start + length).min(self.data.len());
        let result = self.data.slice(start..end);

        // Increment success counter
        self.success_count.fetch_add(1, Ordering::SeqCst);

        Ok(result)
    }

    async fn size(&self) -> Result<u64, SourceError> {
        Ok(self.data.len() as u64)
    }

    async fn read_from(&self, offset: u64) -> Result<Bytes, SourceError> {
        let length = (self.data.len() as u64).saturating_sub(offset) as usize;
        self.read_range(offset, length).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_basic_read() {
        let data = b"Hello, World!";
        let mock = MockS3Source::new(&data[..]);

        let result = mock.read_range(0, 5).await.unwrap();
        assert_eq!(&result[..], b"Hello");

        assert_eq!(mock.call_count(), 1);
        assert_eq!(mock.success_count(), 1);
    }

    #[tokio::test]
    async fn test_mock_with_latency() {
        use std::time::Instant;

        let data = b"test data";
        let mock = MockS3Source::new(&data[..]).with_latency(Duration::from_millis(50));

        let start = Instant::now();
        let _ = mock.read_range(0, 4).await.unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(50));
    }

    // TODO: Add error injection tests once interior mutability is implemented
}
