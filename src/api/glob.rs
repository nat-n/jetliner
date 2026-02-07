//! Glob pattern handling for file paths.
//!
//! This module provides functions for detecting and expanding glob patterns
//! in file paths, supporting both local filesystem and S3 sources.
//!
//! # Requirements
//! - 2.4: When a glob pattern is provided, the reader expands it to matching files
//! - 2.5: The `glob` parameter (default `True`) controls whether glob expansion is performed
//! - 2.6: When `glob=False`, the reader treats the path literally without expansion
//! - 2.9: Path normalization and glob expansion are implemented in Rust

use crate::error::SourceError;
use crate::source::{S3Config, S3Source};
use globset::GlobBuilder;

/// Characters that indicate a glob pattern.
const GLOB_CHARS: &[char] = &['*', '?', '[', ']', '{', '}'];

/// Check if a path contains glob pattern characters.
///
/// Returns `true` if the path contains any of: `*`, `?`, `[`, `]`, `{`, `}`
///
/// # Example
/// ```
/// use jetliner::api::is_glob_pattern;
///
/// assert!(is_glob_pattern("data/*.avro"));
/// assert!(is_glob_pattern("data/file?.avro"));
/// assert!(is_glob_pattern("data/[0-9].avro"));
/// assert!(!is_glob_pattern("data/file.avro"));
/// assert!(!is_glob_pattern("s3://bucket/key.avro"));
/// ```
pub fn is_glob_pattern(path: &str) -> bool {
    path.chars().any(|c| GLOB_CHARS.contains(&c))
}

/// Check if a path is an S3 URI.
///
/// Returns `true` if the path starts with `s3://`.
pub fn is_s3_uri(path: &str) -> bool {
    path.starts_with("s3://")
}

/// Expand a local filesystem glob pattern to matching files.
///
/// Returns a sorted list of matching file paths for deterministic ordering.
///
/// # Arguments
/// * `pattern` - A glob pattern (e.g., `"data/*.avro"`)
///
/// # Returns
/// A sorted vector of matching file paths.
///
/// # Errors
/// - Returns `SourceError::FileSystemError` if the pattern is invalid
/// - Returns `SourceError::NotFound` if no files match the pattern
///
/// # Example
/// ```no_run
/// use jetliner::api::glob::expand_local_glob;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let files = expand_local_glob("tests/data/**/*.avro")?;
/// for file in files {
///     println!("Found: {}", file);
/// }
/// # Ok(())
/// # }
/// ```
pub fn expand_local_glob(pattern: &str) -> Result<Vec<String>, SourceError> {
    let entries = glob::glob(pattern).map_err(|e| {
        SourceError::FileSystemError(format!("Invalid glob pattern '{}': {}", pattern, e))
    })?;

    let mut paths: Vec<String> = Vec::new();

    for entry in entries {
        match entry {
            Ok(path) => {
                // Convert PathBuf to String
                if let Some(path_str) = path.to_str() {
                    paths.push(path_str.to_string());
                } else {
                    // Skip paths that can't be converted to UTF-8
                    continue;
                }
            }
            Err(e) => {
                // Log the error but continue with other matches
                // This handles permission errors on individual files
                eprintln!("Warning: Error accessing path: {}", e);
            }
        }
    }

    if paths.is_empty() {
        return Err(SourceError::NotFound(format!(
            "No files match pattern: {}",
            pattern
        )));
    }

    // Sort for deterministic ordering
    paths.sort();

    Ok(paths)
}

/// Parse an S3 URI into bucket and prefix components for glob expansion.
///
/// For glob patterns, extracts the prefix up to the first glob character.
/// For example:
/// - `s3://bucket/path/to/*.avro` → bucket="bucket", prefix="path/to/"
/// - `s3://bucket/data/**/*.avro` → bucket="bucket", prefix="data/"
/// - `s3://bucket/*.avro` → bucket="bucket", prefix=""
///
/// # Arguments
/// * `uri` - An S3 URI that may contain glob characters
///
/// # Returns
/// A tuple of (bucket, prefix, pattern) where:
/// - bucket: The S3 bucket name
/// - prefix: The key prefix up to the first glob character
/// - pattern: The full key pattern (for matching)
///
/// # Errors
/// Returns `SourceError::S3Error` if the URI is invalid.
pub fn parse_s3_glob_uri(uri: &str) -> Result<(String, String, String), SourceError> {
    let (bucket, key) = S3Source::parse_uri(uri)?;

    // Find the byte position of the first glob character in the key
    let glob_byte_pos = key
        .char_indices()
        .find(|(_, c)| GLOB_CHARS.contains(c))
        .map(|(pos, _)| pos);

    let prefix = match glob_byte_pos {
        Some(pos) => {
            // Find the last '/' before the glob character
            let prefix_end = key[..pos].rfind('/').map(|i| i + 1).unwrap_or(0);
            key[..prefix_end].to_string()
        }
        None => {
            // No glob characters, use the full key as prefix
            key.clone()
        }
    };

    Ok((bucket, prefix, key))
}

/// Expand an S3 glob pattern to matching object keys.
///
/// This function:
/// 1. Parses the S3 URI to extract bucket and prefix
/// 2. Lists objects in the bucket with the prefix
/// 3. Filters objects matching the glob pattern
/// 4. Handles pagination for large buckets
///
/// # Arguments
/// * `uri` - An S3 URI with glob pattern (e.g., `s3://bucket/path/*.avro`)
/// * `s3_config` - Optional S3 configuration
///
/// # Returns
/// A sorted vector of matching S3 URIs.
///
/// # Errors
/// - Returns `SourceError::NotFound` if no objects match the pattern
/// - Returns `SourceError::S3Error` for S3 API errors
/// - Returns `SourceError::AuthenticationFailed` for credential errors
///
/// # Requirements
/// - 2.4: Expand glob patterns to matching files
pub async fn expand_s3_glob(
    uri: &str,
    s3_config: Option<S3Config>,
) -> Result<Vec<String>, SourceError> {
    let (bucket, prefix, pattern) = parse_s3_glob_uri(uri)?;

    // Build S3 client with optional configuration
    let client = S3Source::build_client(s3_config).await?;

    // Compile the glob pattern for matching
    // Use globset with literal_separator(true) so:
    // - `*` matches within a single directory (doesn't cross `/`)
    // - `**` matches zero or more directories (recursive)
    let glob_matcher = GlobBuilder::new(&pattern)
        .literal_separator(true)
        .build()
        .map_err(|e| {
            SourceError::FileSystemError(format!("Invalid glob pattern '{}': {}", pattern, e))
        })?
        .compile_matcher();

    let mut matching_keys: Vec<String> = Vec::new();
    let mut continuation_token: Option<String> = None;

    // Paginate through all objects with the prefix
    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(&prefix)
            .max_keys(1000);

        if let Some(token) = continuation_token.take() {
            request = request.continuation_token(token);
        }

        let response = request
            .send()
            .await
            .map_err(|e| S3Source::map_sdk_error(&bucket, "(list)", e))?;

        // Process objects in this page
        let contents = response.contents();
        for object in contents {
            if let Some(key) = object.key() {
                // Check if the key matches the glob pattern
                if glob_matcher.is_match(key) {
                    matching_keys.push(format!("s3://{}/{}", bucket, key));
                }
            }
        }

        // Check if there are more pages
        if response.is_truncated() == Some(true) {
            continuation_token = response.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    if matching_keys.is_empty() {
        return Err(SourceError::NotFound(format!(
            "No S3 objects match pattern: {}",
            uri
        )));
    }

    // Sort for deterministic ordering
    matching_keys.sort();

    Ok(matching_keys)
}

/// Expand a path that may or may not be a glob pattern.
///
/// If `glob_enabled` is true and the path contains glob characters,
/// expands the pattern. Otherwise, returns the path as-is.
///
/// # Arguments
/// * `path` - A file path or glob pattern
/// * `glob_enabled` - Whether to expand glob patterns
///
/// # Returns
/// A vector of file paths (single element if not a glob, multiple if expanded).
///
/// # Errors
/// - Returns `SourceError::FileSystemError` if glob expansion fails
/// - Returns `SourceError::NotFound` if no files match a glob pattern
pub fn expand_path(path: &str, glob_enabled: bool) -> Result<Vec<String>, SourceError> {
    // S3 paths are handled separately (not by this function)
    if is_s3_uri(path) {
        return Ok(vec![path.to_string()]);
    }

    // If glob is disabled or path doesn't contain glob chars, return as-is
    if !glob_enabled || !is_glob_pattern(path) {
        return Ok(vec![path.to_string()]);
    }

    // Expand the glob pattern
    expand_local_glob(path)
}

/// Expand multiple paths, handling glob patterns.
///
/// # Arguments
/// * `paths` - A slice of file paths or glob patterns
/// * `glob_enabled` - Whether to expand glob patterns
///
/// # Returns
/// A sorted, deduplicated vector of file paths.
///
/// # Errors
/// - Returns `SourceError::FileSystemError` if glob expansion fails
/// - Returns `SourceError::NotFound` if no files match a glob pattern
pub fn expand_paths(paths: &[String], glob_enabled: bool) -> Result<Vec<String>, SourceError> {
    let mut all_paths: Vec<String> = Vec::new();

    for path in paths {
        let expanded = expand_path(path, glob_enabled)?;
        all_paths.extend(expanded);
    }

    // Sort and deduplicate
    all_paths.sort();
    all_paths.dedup();

    if all_paths.is_empty() {
        return Err(SourceError::NotFound(
            "No source files provided or matched".to_string(),
        ));
    }

    Ok(all_paths)
}

/// Expand a path that may or may not be a glob pattern (async version).
///
/// This version handles both local and S3 glob patterns.
///
/// # Arguments
/// * `path` - A file path or glob pattern
/// * `glob_enabled` - Whether to expand glob patterns
/// * `s3_config` - Optional S3 configuration
///
/// # Returns
/// A vector of file paths (single element if not a glob, multiple if expanded).
///
/// # Errors
/// - Returns `SourceError::FileSystemError` if glob expansion fails
/// - Returns `SourceError::NotFound` if no files match a glob pattern
/// - Returns `SourceError::S3Error` for S3 API errors
pub async fn expand_path_async(
    path: &str,
    glob_enabled: bool,
    s3_config: Option<&S3Config>,
) -> Result<Vec<String>, SourceError> {
    // Handle S3 paths
    if is_s3_uri(path) {
        if glob_enabled && is_glob_pattern(path) {
            return expand_s3_glob(path, s3_config.cloned()).await;
        }
        return Ok(vec![path.to_string()]);
    }

    // Handle local paths (sync)
    expand_path(path, glob_enabled)
}

/// Expand multiple paths, handling glob patterns (async version).
///
/// This version handles both local and S3 glob patterns.
///
/// # Arguments
/// * `paths` - A slice of file paths or glob patterns
/// * `glob_enabled` - Whether to expand glob patterns
/// * `s3_config` - Optional S3 configuration
///
/// # Returns
/// A sorted, deduplicated vector of file paths.
///
/// # Errors
/// - Returns `SourceError::FileSystemError` if glob expansion fails
/// - Returns `SourceError::NotFound` if no files match a glob pattern
/// - Returns `SourceError::S3Error` for S3 API errors
pub async fn expand_paths_async(
    paths: &[String],
    glob_enabled: bool,
    s3_config: Option<&S3Config>,
) -> Result<Vec<String>, SourceError> {
    let mut all_paths: Vec<String> = Vec::new();

    for path in paths {
        let expanded = expand_path_async(path, glob_enabled, s3_config).await?;
        all_paths.extend(expanded);
    }

    // Sort and deduplicate
    all_paths.sort();
    all_paths.dedup();

    if all_paths.is_empty() {
        return Err(SourceError::NotFound(
            "No source files provided or matched".to_string(),
        ));
    }

    Ok(all_paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_glob_pattern_with_asterisk() {
        assert!(is_glob_pattern("*.avro"));
        assert!(is_glob_pattern("data/*.avro"));
        assert!(is_glob_pattern("data/**/*.avro"));
    }

    #[test]
    fn test_is_glob_pattern_with_question_mark() {
        assert!(is_glob_pattern("file?.avro"));
        assert!(is_glob_pattern("data/file?.avro"));
    }

    #[test]
    fn test_is_glob_pattern_with_brackets() {
        assert!(is_glob_pattern("[0-9].avro"));
        assert!(is_glob_pattern("data/[abc].avro"));
        assert!(is_glob_pattern("data/{a,b,c}.avro"));
    }

    #[test]
    fn test_is_glob_pattern_without_glob_chars() {
        assert!(!is_glob_pattern("file.avro"));
        assert!(!is_glob_pattern("data/file.avro"));
        assert!(!is_glob_pattern("/absolute/path/file.avro"));
    }

    #[test]
    fn test_is_glob_pattern_s3_uri() {
        // S3 URIs without glob chars
        assert!(!is_glob_pattern("s3://bucket/key.avro"));
        assert!(!is_glob_pattern("s3://bucket/path/to/file.avro"));

        // S3 URIs with glob chars
        assert!(is_glob_pattern("s3://bucket/*.avro"));
        assert!(is_glob_pattern("s3://bucket/path/*.avro"));
    }

    #[test]
    fn test_is_s3_uri() {
        assert!(is_s3_uri("s3://bucket/key"));
        assert!(is_s3_uri("s3://bucket/path/to/file.avro"));
        assert!(!is_s3_uri("file.avro"));
        assert!(!is_s3_uri("/absolute/path/file.avro"));
        assert!(!is_s3_uri("http://example.com/file.avro"));
    }

    // S3 glob URI parsing tests

    #[test]
    fn test_parse_s3_glob_uri_simple_glob() {
        let (bucket, prefix, pattern) = parse_s3_glob_uri("s3://my-bucket/*.avro").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
        assert_eq!(pattern, "*.avro");
    }

    #[test]
    fn test_parse_s3_glob_uri_with_path_prefix() {
        let (bucket, prefix, pattern) =
            parse_s3_glob_uri("s3://my-bucket/data/2024/*.avro").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "data/2024/");
        assert_eq!(pattern, "data/2024/*.avro");
    }

    #[test]
    fn test_parse_s3_glob_uri_recursive_glob() {
        let (bucket, prefix, pattern) = parse_s3_glob_uri("s3://my-bucket/data/**/*.avro").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "data/");
        assert_eq!(pattern, "data/**/*.avro");
    }

    #[test]
    fn test_parse_s3_glob_uri_question_mark() {
        let (bucket, prefix, pattern) = parse_s3_glob_uri("s3://my-bucket/file?.avro").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
        assert_eq!(pattern, "file?.avro");
    }

    #[test]
    fn test_parse_s3_glob_uri_bracket_pattern() {
        let (bucket, prefix, pattern) =
            parse_s3_glob_uri("s3://my-bucket/data/[0-9].avro").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "data/");
        assert_eq!(pattern, "data/[0-9].avro");
    }

    #[test]
    fn test_parse_s3_glob_uri_no_glob() {
        // When there's no glob, the full key becomes the prefix
        let (bucket, prefix, pattern) = parse_s3_glob_uri("s3://my-bucket/data/file.avro").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "data/file.avro");
        assert_eq!(pattern, "data/file.avro");
    }

    #[test]
    fn test_parse_s3_glob_uri_invalid() {
        // Missing s3:// prefix
        assert!(parse_s3_glob_uri("bucket/*.avro").is_err());
        // Missing key
        assert!(parse_s3_glob_uri("s3://bucket").is_err());
        // Empty bucket
        assert!(parse_s3_glob_uri("s3:///key").is_err());
    }

    #[test]
    fn test_expand_local_glob_with_test_data() {
        // Use the actual test data directory
        let result = expand_local_glob("tests/data/apache-avro/*.avro");

        match result {
            Ok(paths) => {
                assert!(!paths.is_empty());
                // Verify paths are sorted
                let mut sorted = paths.clone();
                sorted.sort();
                assert_eq!(paths, sorted);
                // Verify all paths end with .avro
                for path in &paths {
                    assert!(path.ends_with(".avro"));
                }
            }
            Err(e) => {
                // If test data doesn't exist, that's okay for this test
                println!("Note: Test data not found: {}", e);
            }
        }
    }

    #[test]
    fn test_expand_local_glob_no_matches() {
        let result = expand_local_glob("nonexistent_directory_12345/*.avro");
        assert!(result.is_err());

        match result.unwrap_err() {
            SourceError::NotFound(msg) => {
                assert!(msg.contains("No files match"));
            }
            _ => panic!("Expected NotFound error"),
        }
    }

    #[test]
    fn test_expand_local_glob_invalid_pattern() {
        // Invalid glob pattern (unclosed bracket)
        let result = expand_local_glob("data/[invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_expand_path_non_glob() {
        let result = expand_path("file.avro", true).unwrap();
        assert_eq!(result, vec!["file.avro"]);
    }

    #[test]
    fn test_expand_path_glob_disabled() {
        let result = expand_path("*.avro", false).unwrap();
        assert_eq!(result, vec!["*.avro"]);
    }

    #[test]
    fn test_expand_path_s3_uri() {
        // S3 URIs are returned as-is (S3 glob expansion is handled separately)
        let result = expand_path("s3://bucket/*.avro", true).unwrap();
        assert_eq!(result, vec!["s3://bucket/*.avro"]);
    }

    #[test]
    fn test_expand_paths_mixed() {
        let paths = vec![
            "file1.avro".to_string(),
            "file2.avro".to_string(),
            "s3://bucket/file.avro".to_string(),
        ];

        let result = expand_paths(&paths, true).unwrap();

        // Should contain all paths, sorted
        assert!(result.contains(&"file1.avro".to_string()));
        assert!(result.contains(&"file2.avro".to_string()));
        assert!(result.contains(&"s3://bucket/file.avro".to_string()));
    }

    #[test]
    fn test_expand_paths_deduplication() {
        let paths = vec![
            "file.avro".to_string(),
            "file.avro".to_string(), // duplicate
        ];

        let result = expand_paths(&paths, true).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "file.avro");
    }

    #[test]
    fn test_expand_paths_empty() {
        let paths: Vec<String> = vec![];
        let result = expand_paths(&paths, true);
        assert!(result.is_err());
    }

    // =========================================================================
    // Globset recursive pattern matching tests
    // =========================================================================
    //
    // These tests verify that the globset-based pattern matching correctly
    // handles `**` recursive patterns for S3 glob expansion.

    /// Helper to create a globset matcher with literal_separator(true)
    fn create_glob_matcher(pattern: &str) -> globset::GlobMatcher {
        GlobBuilder::new(pattern)
            .literal_separator(true)
            .build()
            .expect("valid pattern")
            .compile_matcher()
    }

    #[test]
    fn test_globset_recursive_basic() {
        // Pattern: data/**/*.avro should match files at any depth under data/
        let matcher = create_glob_matcher("data/**/*.avro");

        // Should match: files at various depths
        assert!(matcher.is_match("data/file.avro"));
        assert!(matcher.is_match("data/2024/file.avro"));
        assert!(matcher.is_match("data/2024/01/file.avro"));
        assert!(matcher.is_match("data/a/b/c/d/e/file.avro"));

        // Should NOT match: wrong extension or outside data/
        assert!(!matcher.is_match("data/file.parquet"));
        assert!(!matcher.is_match("other/file.avro"));
        assert!(!matcher.is_match("file.avro"));
    }

    #[test]
    fn test_globset_recursive_at_start() {
        // Pattern: **/foo.avro should match foo.avro at any depth
        let matcher = create_glob_matcher("**/foo.avro");

        assert!(matcher.is_match("foo.avro"));
        assert!(matcher.is_match("data/foo.avro"));
        assert!(matcher.is_match("data/2024/foo.avro"));
        assert!(matcher.is_match("a/b/c/foo.avro"));

        // Should NOT match: different filename
        assert!(!matcher.is_match("bar.avro"));
        assert!(!matcher.is_match("data/bar.avro"));
    }

    #[test]
    fn test_globset_recursive_at_end() {
        // Pattern: data/** should match everything under data/
        let matcher = create_glob_matcher("data/**");

        assert!(matcher.is_match("data/file.avro"));
        assert!(matcher.is_match("data/subdir/file.avro"));
        assert!(matcher.is_match("data/a/b/c/file.txt"));
        assert!(matcher.is_match("data/"));

        // Should NOT match: outside data/
        assert!(!matcher.is_match("other/file.avro"));
    }

    #[test]
    fn test_globset_multiple_recursive() {
        // Pattern: a/**/b/**/c.avro should match complex nested paths
        let matcher = create_glob_matcher("a/**/b/**/c.avro");

        assert!(matcher.is_match("a/b/c.avro"));
        assert!(matcher.is_match("a/x/b/c.avro"));
        assert!(matcher.is_match("a/x/y/b/c.avro"));
        assert!(matcher.is_match("a/b/z/c.avro"));
        assert!(matcher.is_match("a/x/y/b/z/w/c.avro"));

        // Should NOT match: missing required path segments
        assert!(!matcher.is_match("a/c.avro"));
        assert!(!matcher.is_match("b/c.avro"));
    }

    #[test]
    fn test_globset_zero_directories() {
        // Pattern: data/**/*.avro should match data/file.avro (** matches zero dirs)
        let matcher = create_glob_matcher("data/**/*.avro");

        // Zero directories between data/ and *.avro
        assert!(matcher.is_match("data/file.avro"));

        // One or more directories
        assert!(matcher.is_match("data/2024/file.avro"));
        assert!(matcher.is_match("data/2024/01/15/file.avro"));
    }

    #[test]
    fn test_globset_single_star_no_slash() {
        // Pattern: data/*.avro should NOT match files in subdirectories
        // because * with literal_separator(true) doesn't cross /
        let matcher = create_glob_matcher("data/*.avro");

        // Should match: files directly in data/
        assert!(matcher.is_match("data/file.avro"));
        assert!(matcher.is_match("data/weather.avro"));
        assert!(matcher.is_match("data/test123.avro"));

        // Should NOT match: files in subdirectories
        assert!(!matcher.is_match("data/2024/file.avro"));
        assert!(!matcher.is_match("data/subdir/file.avro"));
        assert!(!matcher.is_match("data/a/b/file.avro"));
    }

    #[test]
    fn test_globset_mixed_patterns() {
        // Pattern: data/[0-9]/**/*.avro combines character class with recursive glob
        let matcher = create_glob_matcher("data/[0-9]/**/*.avro");

        // Should match: digit directory with files at any depth
        assert!(matcher.is_match("data/1/file.avro"));
        assert!(matcher.is_match("data/5/sub/file.avro"));
        assert!(matcher.is_match("data/9/a/b/c/file.avro"));

        // Should NOT match: non-digit directory
        assert!(!matcher.is_match("data/a/file.avro"));
        assert!(!matcher.is_match("data/abc/file.avro"));

        // Should NOT match: multi-digit directory
        assert!(!matcher.is_match("data/12/file.avro"));
    }
}
