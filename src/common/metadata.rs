use crate::common::error::{Result, SamsaError};
use crate::proto::*;
use async_trait::async_trait;
use chrono;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct StoredBucket {
    pub id: Uuid,
    pub name: String,
    pub config: BucketConfig,
    pub state: i32, // BucketState as i32
    pub created_at: u64,
    pub deleted_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StoredStream {
    pub id: Uuid,
    pub bucket_id: Uuid,
    pub bucket: String,
    pub name: String,
    pub config: StreamConfig,
    pub created_at: u64,
    pub deleted_at: Option<u64>,
    pub next_seq_id: String,
    pub last_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableHeader {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

impl From<&Header> for SerializableHeader {
    fn from(header: &Header) -> Self {
        Self {
            name: header.name.clone(),
            value: header.value.clone(),
        }
    }
}

impl From<SerializableHeader> for Header {
    fn from(header: SerializableHeader) -> Self {
        Self {
            name: header.name,
            value: header.value,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRecord {
    pub seq_id: String,
    pub timestamp: u64,
    pub headers: Vec<SerializableHeader>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct AccessToken {
    pub id: Uuid,
    pub token: String,
    pub info: AccessTokenInfo,
    pub created_at: u64,
    pub expires_at: Option<u64>, // TTL for automatic cleanup
}

/// Generic trait for CRUD operations on stored entities
#[async_trait]
pub trait CrudRepository<T, K>: Send + Sync
where
    T: Clone + Send + Sync,
    K: Send + Sync,
{
    /// Create a new entity
    async fn create(&self, key: K, item: T) -> Result<T>;

    /// Get an entity by key
    async fn get(&self, key: &K) -> Result<T>;

    /// List entities with filtering and pagination
    async fn list(&self, prefix: &str, start_after: &str, limit: Option<u64>) -> Result<Vec<T>>;

    /// Update an entity
    async fn update(&self, key: &K, item: T) -> Result<()>;

    /// Delete an entity
    async fn delete(&self, key: &K) -> Result<()>;

    /// Check if entity exists
    async fn exists(&self, key: &K) -> Result<bool>;
}

/// Helper trait for entities that support soft deletion
pub trait SoftDeletable {
    fn is_deleted(&self) -> bool;
    fn mark_deleted(&mut self);
    fn get_key(&self) -> String;
}

impl SoftDeletable for StoredBucket {
    fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    fn mark_deleted(&mut self) {
        self.deleted_at = Some(current_timestamp());
        self.state = BucketState::Deleting as i32;
    }

    fn get_key(&self) -> String {
        self.name.clone()
    }
}

impl SoftDeletable for StoredStream {
    fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    fn mark_deleted(&mut self) {
        self.deleted_at = Some(current_timestamp());
    }

    fn get_key(&self) -> String {
        format!("{}/{}", self.bucket, self.name)
    }
}

/// Generic in-memory CRUD repository implementation
#[derive(Debug)]
pub struct InMemoryCrudRepository<T> {
    items: DashMap<String, T>,
}

impl<T> Default for InMemoryCrudRepository<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> InMemoryCrudRepository<T> {
    pub fn new() -> Self {
        Self {
            items: DashMap::new(),
        }
    }
}

#[async_trait]
impl<T> CrudRepository<T, String> for InMemoryCrudRepository<T>
where
    T: Clone + Send + Sync,
{
    async fn create(&self, key: String, item: T) -> Result<T> {
        if self.items.contains_key(&key) {
            return Err(SamsaError::AlreadyExists(format!(
                "Item with key '{key}' already exists"
            )));
        }

        self.items.insert(key, item.clone());
        Ok(item)
    }

    async fn get(&self, key: &String) -> Result<T> {
        self.items
            .get(key)
            .map(|entry| entry.clone())
            .ok_or_else(|| SamsaError::NotFound(format!("Item with key '{key}' not found")))
    }

    async fn list(&self, prefix: &str, start_after: &str, limit: Option<u64>) -> Result<Vec<T>> {
        // Use BTreeMap to maintain sorted order during collection
        // This avoids sorting the entire dataset when we only need a limited subset
        let mut sorted_items: BTreeMap<String, T> = BTreeMap::new();
        let limit_usize = limit.map(|l| l.try_into().unwrap_or(usize::MAX));

        for entry in self.items.iter() {
            let key = entry.key();

            // Apply prefix and start_after filters
            if !key.starts_with(prefix) || key.as_str() <= start_after {
                continue;
            }

            // If we have a limit and we've already collected enough items,
            // check if this item should replace one of the existing items
            if let Some(max_items) = limit_usize {
                if sorted_items.len() >= max_items {
                    // Since BTreeMap is sorted, the last key is the largest
                    if let Some(last_key) = sorted_items.keys().last().cloned() {
                        if key.as_str() >= last_key.as_str() {
                            // This item is larger than our current largest, skip it
                            continue;
                        }
                        // Remove the largest item to make room for this smaller one
                        sorted_items.remove(&last_key);
                    }
                }
            }

            sorted_items.insert(key.clone(), entry.value().clone());
        }

        // Convert to Vec, already in sorted order
        Ok(sorted_items.into_values().collect())
    }

    async fn update(&self, key: &String, item: T) -> Result<()> {
        if !self.items.contains_key(key) {
            return Err(SamsaError::NotFound(format!(
                "Item with key '{key}' not found"
            )));
        }

        self.items.insert(key.clone(), item);
        Ok(())
    }

    async fn delete(&self, key: &String) -> Result<()> {
        self.items
            .remove(key)
            .ok_or_else(|| SamsaError::NotFound(format!("Item with key '{key}' not found")))?;
        Ok(())
    }

    async fn exists(&self, key: &String) -> Result<bool> {
        Ok(self.items.contains_key(key))
    }
}

/// Trait for handling metadata storage operations
#[async_trait]
pub trait MetaDataRepository: Send + Sync {
    // Bucket operations
    async fn create_bucket(&self, name: String, config: BucketConfig) -> Result<StoredBucket>;
    async fn get_bucket(&self, name: &str) -> Result<StoredBucket>;
    async fn list_buckets(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredBucket>>;
    async fn delete_bucket(&self, name: &str) -> Result<()>;
    async fn update_bucket_config(&self, name: &str, config: BucketConfig) -> Result<()>;

    // Stream operations
    async fn create_stream(
        &self,
        bucket: &str,
        name: String,
        config: StreamConfig,
    ) -> Result<StoredStream>;
    async fn get_stream(&self, bucket: &str, name: &str) -> Result<StoredStream>;
    async fn list_streams(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredStream>>;
    async fn delete_stream(&self, bucket: &str, name: &str) -> Result<()>;
    async fn update_stream_config(
        &self,
        bucket: &str,
        name: &str,
        config: StreamConfig,
    ) -> Result<()>;
    async fn update_stream_metadata(
        &self,
        bucket: &str,
        name: &str,
        next_seq_id: String,
        last_timestamp: u64,
    ) -> Result<()>;
    async fn get_stream_tail(&self, bucket: &str, stream: &str) -> Result<(String, u64)>;

    // Access token operations
    async fn create_access_token(&self, info: AccessTokenInfo) -> Result<String>;
    async fn get_access_token(&self, id: &str) -> Result<AccessToken>;
    async fn list_access_tokens(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<AccessToken>>;
    async fn revoke_access_token(&self, id: &str) -> Result<AccessToken>;

    // Cleanup operations
    async fn cleanup_expired_tokens(&self) -> Result<usize>;
    async fn cleanup_deleted_buckets(&self, grace_period_seconds: u32) -> Result<usize>;
    async fn cleanup_deleted_streams(&self, grace_period_seconds: u32) -> Result<usize>;
}

/// In-memory implementation of MetaDataRepository using generic CRUD repositories
#[derive(Debug)]
pub struct InMemoryMetaDataRepository {
    buckets: InMemoryCrudRepository<StoredBucket>,
    streams: InMemoryCrudRepository<StoredStream>,
    access_tokens: InMemoryCrudRepository<AccessToken>,
}

impl Default for InMemoryMetaDataRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryMetaDataRepository {
    pub fn new() -> Self {
        Self {
            buckets: InMemoryCrudRepository::new(),
            streams: InMemoryCrudRepository::new(),
            access_tokens: InMemoryCrudRepository::new(),
        }
    }
}

#[async_trait]
impl MetaDataRepository for InMemoryMetaDataRepository {
    async fn create_bucket(&self, name: String, config: BucketConfig) -> Result<StoredBucket> {
        let bucket = StoredBucket {
            id: Uuid::new_v4(),
            name: name.clone(),
            config,
            state: BucketState::Active as i32,
            created_at: current_timestamp(),
            deleted_at: None,
        };

        self.buckets.create(name, bucket).await
    }

    async fn get_bucket(&self, name: &str) -> Result<StoredBucket> {
        self.buckets.get(&name.to_string()).await
    }

    async fn list_buckets(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredBucket>> {
        let mut buckets = self.buckets.list(prefix, start_after, limit).await?;

        // Filter out soft deleted buckets
        buckets.retain(|bucket| !bucket.is_deleted());

        // Sort by name for consistent ordering
        buckets.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(buckets)
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let key = name.to_string();
        let mut bucket = self.buckets.get(&key).await?;

        // Mark for deletion instead of immediate removal
        bucket.mark_deleted();
        self.buckets.update(&key, bucket).await?;

        // Mark all streams in this bucket for deletion
        let stream_keys: Vec<String> = self
            .streams
            .items
            .iter()
            .filter(|entry| entry.value().bucket == name)
            .map(|entry| entry.key().clone())
            .collect();

        for stream_key in stream_keys {
            if let Ok(mut stream) = self.streams.get(&stream_key).await {
                stream.mark_deleted();
                let _ = self.streams.update(&stream_key, stream).await;
            }
        }

        Ok(())
    }

    async fn update_bucket_config(&self, name: &str, config: BucketConfig) -> Result<()> {
        let key = name.to_string();
        let mut bucket = self.buckets.get(&key).await?;
        bucket.config = config;
        self.buckets.update(&key, bucket).await
    }

    async fn create_stream(
        &self,
        bucket: &str,
        name: String,
        config: StreamConfig,
    ) -> Result<StoredStream> {
        let key = format!("{bucket}/{name}");

        // Verify bucket exists and is not soft deleted, or auto-create it if it doesn't exist
        if let Ok(bucket_obj) = self.get_bucket(bucket).await {
            if bucket_obj.is_deleted() {
                return Err(SamsaError::NotFound(format!(
                    "Bucket '{}' not found",
                    bucket
                )));
            }
        } else if !self.buckets.exists(&bucket.to_string()).await? {
            self.create_bucket(bucket.to_string(), BucketConfig::default())
                .await?;
        }

        let stream = StoredStream {
            id: Uuid::new_v4(),
            bucket_id: Uuid::new_v4(),
            bucket: bucket.to_string(),
            name: name.clone(),
            config,
            created_at: current_timestamp(),
            deleted_at: None,
            next_seq_id: Uuid::nil().to_string(),
            last_timestamp: 0,
        };

        self.streams.create(key, stream).await
    }

    async fn get_stream(&self, bucket: &str, name: &str) -> Result<StoredStream> {
        // First check if the bucket exists and is not soft deleted
        let bucket_obj = self.get_bucket(bucket).await?;
        if bucket_obj.is_deleted() {
            return Err(SamsaError::NotFound(format!(
                "Bucket '{}' not found",
                bucket
            )));
        }

        let key = format!("{bucket}/{name}");
        let stream = self.streams.get(&key).await?;

        // Double check that the stream itself is not soft deleted
        if stream.is_deleted() {
            return Err(SamsaError::NotFound(format!(
                "Stream '{}/{}' not found",
                bucket, name
            )));
        }

        Ok(stream)
    }

    async fn list_streams(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredStream>> {
        // First check if the bucket exists and is not soft deleted
        let bucket_obj = self.get_bucket(bucket).await?;
        if bucket_obj.is_deleted() {
            return Err(SamsaError::NotFound(format!(
                "Bucket '{}' not found",
                bucket
            )));
        }

        let mut streams: Vec<_> = self
            .streams
            .items
            .iter()
            .filter(|entry| {
                let stream = entry.value();
                stream.bucket == bucket
                    && stream.name.starts_with(prefix)
                    && stream.name.as_str() > start_after
                    && !stream.is_deleted()
            })
            .map(|entry| entry.value().clone())
            .collect();

        streams.sort_by(|a, b| a.name.cmp(&b.name));

        if let Some(limit) = limit {
            streams.truncate(limit.try_into().unwrap_or(usize::MAX));
        }

        Ok(streams)
    }

    async fn delete_stream(&self, bucket: &str, name: &str) -> Result<()> {
        // First check if the bucket exists and is not soft deleted
        let bucket_obj = self.get_bucket(bucket).await?;
        if bucket_obj.is_deleted() {
            return Err(SamsaError::NotFound(format!(
                "Bucket '{}' not found",
                bucket
            )));
        }

        let key = format!("{bucket}/{name}");
        let mut stream = self.streams.get(&key).await?;

        // Check that the stream itself is not already soft deleted
        if stream.is_deleted() {
            return Err(SamsaError::NotFound(format!(
                "Stream '{}/{}' not found",
                bucket, name
            )));
        }

        stream.mark_deleted();
        self.streams.update(&key, stream).await
    }

    async fn update_stream_config(
        &self,
        bucket: &str,
        name: &str,
        config: StreamConfig,
    ) -> Result<()> {
        // First check if the bucket exists and is not soft deleted
        let bucket_obj = self.get_bucket(bucket).await?;
        if bucket_obj.is_deleted() {
            return Err(SamsaError::NotFound(format!(
                "Bucket '{}' not found",
                bucket
            )));
        }

        let key = format!("{bucket}/{name}");
        let mut stream = self.streams.get(&key).await?;

        // Check that the stream itself is not soft deleted
        if stream.is_deleted() {
            return Err(SamsaError::NotFound(format!(
                "Stream '{}/{}' not found",
                bucket, name
            )));
        }

        stream.config = config;
        self.streams.update(&key, stream).await
    }

    async fn update_stream_metadata(
        &self,
        bucket: &str,
        name: &str,
        next_seq_id: String,
        last_timestamp: u64,
    ) -> Result<()> {
        // First check if the bucket exists and is not soft deleted
        let bucket_obj = self.get_bucket(bucket).await?;
        if bucket_obj.is_deleted() {
            return Err(SamsaError::NotFound(format!(
                "Bucket '{}' not found",
                bucket
            )));
        }

        let key = format!("{bucket}/{name}");
        let mut stream = self.streams.get(&key).await?;

        // Check that the stream itself is not soft deleted
        if stream.is_deleted() {
            return Err(SamsaError::NotFound(format!(
                "Stream '{}/{}' not found",
                bucket, name
            )));
        }

        stream.next_seq_id = next_seq_id;
        stream.last_timestamp = last_timestamp;
        self.streams.update(&key, stream).await
    }

    async fn get_stream_tail(&self, bucket: &str, stream: &str) -> Result<(String, u64)> {
        let stream_info = self.get_stream(bucket, stream).await?;
        Ok((stream_info.next_seq_id, stream_info.last_timestamp))
    }

    async fn create_access_token(&self, info: AccessTokenInfo) -> Result<String> {
        let token_id = if info.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            info.id.clone()
        };

        let access_token = AccessToken {
            id: Uuid::new_v4(),
            token: Uuid::new_v4().to_string(),
            info,
            created_at: current_timestamp(),
            expires_at: Some(current_timestamp() + 86400 * 30 * 1000), // 30 days in milliseconds
        };

        let token = access_token.token.clone();
        self.access_tokens.create(token_id, access_token).await?;
        Ok(token)
    }

    async fn get_access_token(&self, id: &str) -> Result<AccessToken> {
        self.access_tokens.get(&id.to_string()).await
    }

    async fn list_access_tokens(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<AccessToken>> {
        let current_time = current_timestamp();
        let mut tokens: Vec<_> = self
            .access_tokens
            .items
            .iter()
            .filter(|entry| {
                let token = entry.value();
                let id = entry.key();
                // Filter out expired tokens and apply prefix/start_after filters
                if let Some(expires_at) = token.expires_at {
                    if current_time > expires_at {
                        return false;
                    }
                }
                id.starts_with(prefix) && id.as_str() > start_after
            })
            .map(|entry| entry.value().clone())
            .collect();

        tokens.sort_by(|a, b| a.id.cmp(&b.id));

        if let Some(limit) = limit {
            tokens.truncate(limit.try_into().unwrap_or(usize::MAX));
        }

        Ok(tokens)
    }

    async fn revoke_access_token(&self, id: &str) -> Result<AccessToken> {
        let key = id.to_string();
        let mut token = self.access_tokens.get(&key).await?;

        // Set expires_at to current time to mark as revoked
        token.expires_at = Some(current_timestamp());

        // Update the token in storage with the new expires_at
        self.access_tokens.update(&key, token.clone()).await?;

        Ok(token)
    }

    async fn cleanup_expired_tokens(&self) -> Result<usize> {
        let current_time = current_timestamp();
        let expired_tokens: Vec<String> = self
            .access_tokens
            .items
            .iter()
            .filter_map(|entry| {
                let token = entry.value();
                if let Some(expires_at) = token.expires_at {
                    if current_time > expires_at {
                        Some(entry.key().clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        let count = expired_tokens.len();
        for token_id in expired_tokens {
            let _ = self.access_tokens.delete(&token_id).await;
        }

        Ok(count)
    }

    async fn cleanup_deleted_buckets(&self, grace_period_seconds: u32) -> Result<usize> {
        let current_time = current_timestamp();
        let cutoff_time = current_time.saturating_sub(grace_period_seconds as u64 * 1000); // Convert seconds to milliseconds

        let deleted_buckets: Vec<String> = self
            .buckets
            .items
            .iter()
            .filter_map(|entry| {
                let bucket = entry.value();
                if let Some(deleted_at) = bucket.deleted_at {
                    if deleted_at < cutoff_time {
                        Some(entry.key().clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        let count = deleted_buckets.len();
        for bucket_name in deleted_buckets {
            // Remove associated streams
            let stream_keys: Vec<String> = self
                .streams
                .items
                .iter()
                .filter_map(|entry| {
                    if entry.value().bucket == bucket_name {
                        Some(entry.key().clone())
                    } else {
                        None
                    }
                })
                .collect();

            for stream_key in stream_keys {
                let _ = self.streams.delete(&stream_key).await;
            }

            let _ = self.buckets.delete(&bucket_name).await;
        }

        Ok(count)
    }

    async fn cleanup_deleted_streams(&self, grace_period_seconds: u32) -> Result<usize> {
        let current_time = current_timestamp();
        let cutoff_time = current_time.saturating_sub(grace_period_seconds as u64 * 1000); // Convert seconds to milliseconds

        let deleted_streams: Vec<String> = self
            .streams
            .items
            .iter()
            .filter_map(|entry| {
                let stream = entry.value();
                if let Some(deleted_at) = stream.deleted_at {
                    if deleted_at < cutoff_time {
                        Some(entry.key().clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        let count = deleted_streams.len();
        for stream_key in deleted_streams {
            let _ = self.streams.delete(&stream_key).await;
        }

        Ok(count)
    }
}

/// Get current timestamp in milliseconds since Unix epoch
///
/// All timestamps in the Samsa system use millisecond precision for consistency
/// and proper event ordering. This includes:
/// - StoredRecord::timestamp
/// - StoredBucket::created_at, deleted_at  
/// - StoredStream::created_at, deleted_at, last_timestamp
/// - AccessToken::created_at, expires_at
/// - StoredRecordBatch::created_at, first_timestamp, last_timestamp
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Legacy alias for current_timestamp() - all timestamps now use millisecond precision
///
/// This function is kept for backward compatibility but both functions now return
/// the same millisecond-precision timestamp.
pub fn current_timestamp_millis() -> u64 {
    current_timestamp()
}

/// Get current timestamp as UTC DateTime
///
/// Used for PostgreSQL metadata repository timestamps
pub fn current_utc_timestamp() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

/// Convert DateTime<Utc> to u64 timestamp in milliseconds
pub fn datetime_to_timestamp(dt: chrono::DateTime<chrono::Utc>) -> u64 {
    dt.timestamp_millis() as u64
}

/// Convert u64 timestamp in milliseconds to DateTime<Utc>
pub fn timestamp_to_datetime(timestamp: u64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(timestamp as i64).unwrap_or_else(chrono::Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_timestamp_consistency() {
        let ts1 = current_timestamp();
        let ts2 = current_timestamp_millis();

        // Both functions should return the same type and similar values
        assert!(ts1 > 0);
        assert!(ts2 > 0);

        // Should be within a few milliseconds of each other
        let diff = if ts1 > ts2 { ts1 - ts2 } else { ts2 - ts1 };
        assert!(diff < 100); // Should be within 100ms

        // Verify they're both in milliseconds (should be much larger than seconds since epoch)
        let seconds_since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Timestamp should be roughly 1000x larger than seconds (since it's in milliseconds)
        assert!(ts1 > seconds_since_epoch * 500); // At least 500x larger (conservative)
        assert!(ts2 > seconds_since_epoch * 500);
    }

    #[test]
    fn test_timestamp_fields_consistency() {
        // Test that all entity types use consistent timestamp types
        let bucket = StoredBucket {
            id: Uuid::new_v4(),
            name: "test-bucket".to_string(),
            config: BucketConfig::default(),
            state: 1,
            created_at: current_timestamp(),       // u64 milliseconds
            deleted_at: Some(current_timestamp()), // Option<u64> milliseconds
        };

        let stream = StoredStream {
            id: Uuid::new_v4(),
            bucket_id: Uuid::new_v4(),
            bucket: "test-bucket".to_string(),
            name: "test-stream".to_string(),
            config: StreamConfig::default(),
            created_at: current_timestamp(),       // u64 milliseconds
            deleted_at: Some(current_timestamp()), // Option<u64> milliseconds
            next_seq_id: "test-seq".to_string(),
            last_timestamp: current_timestamp(), // u64 milliseconds
        };

        let record = StoredRecord {
            seq_id: "test-seq".to_string(),
            timestamp: current_timestamp(), // u64 milliseconds
            headers: vec![],
            body: vec![],
        };

        let token = AccessToken {
            id: Uuid::new_v4(),
            token: "test-token".to_string(),
            info: AccessTokenInfo::default(),
            created_at: current_timestamp(), // u64 milliseconds
            expires_at: Some(current_timestamp() + 86400 * 30 * 1000), // Option<u64> milliseconds
        };

        // All timestamps should be in the same range (milliseconds since epoch)
        let now = current_timestamp();
        let one_second = 1000u64; // 1 second in milliseconds

        assert!(bucket.created_at.abs_diff(now) < one_second);
        assert!(stream.created_at.abs_diff(now) < one_second);
        assert!(record.timestamp.abs_diff(now) < one_second);
        assert!(token.created_at.abs_diff(now) < one_second);

        // Test that deleted_at timestamps are also consistent
        assert!(bucket.deleted_at.unwrap().abs_diff(now) < one_second);
        assert!(stream.deleted_at.unwrap().abs_diff(now) < one_second);

        // Test that token expiration is far in the future (30 days in milliseconds)
        let thirty_days_ms = 86400 * 30 * 1000u64;
        assert!(token.expires_at.unwrap() > now + (thirty_days_ms - one_second));
        assert!(token.expires_at.unwrap() < now + (thirty_days_ms + one_second));
    }

    #[test]
    fn test_grace_period_calculations() {
        let current_time = current_timestamp();
        let grace_period_seconds = 3600u32; // 1 hour

        // Test the conversion from seconds to milliseconds in grace period calculations
        let cutoff_time = current_time.saturating_sub(grace_period_seconds as u64 * 1000);

        // Cutoff should be exactly 1 hour (3600 seconds = 3,600,000 milliseconds) ago
        let expected_cutoff = current_time - (3600 * 1000);

        // Should be very close (within a few milliseconds due to execution time)
        assert!(cutoff_time.abs_diff(expected_cutoff) < 10);

        // Test edge case with zero grace period
        let zero_cutoff = current_time.saturating_sub(0u32 as u64 * 1000);
        assert_eq!(zero_cutoff, current_time);

        // Test large grace period doesn't underflow
        let large_grace_period = u32::MAX;
        let large_cutoff = current_time.saturating_sub(large_grace_period as u64 * 1000);
        assert!(large_cutoff <= current_time); // Should saturate to 0 if needed
    }
}
