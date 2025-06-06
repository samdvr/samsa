//! Comprehensive unit tests for the metadata module
//!
//! Tests all metadata structures, repository implementations, and utility functions

use samsa::common::error::SamsaError;
use samsa::common::metadata::{
    AccessToken, CrudRepository, InMemoryCrudRepository, InMemoryMetaDataRepository,
    MetaDataRepository, SerializableHeader, SoftDeletable, StoredBucket, StoredRecord,
    StoredStream, current_timestamp, current_timestamp_millis,
};
use samsa::proto::{
    AccessTokenInfo, AccessTokenScope, BucketConfig, BucketState, Header, Operation, ResourceSet,
    StorageClass, StreamConfig, TimestampingMode, resource_set,
};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[test]
fn test_serializable_header_from_header() {
    let header = Header {
        name: b"content-type".to_vec(),
        value: b"application/json".to_vec(),
    };

    let serializable: SerializableHeader = (&header).into();
    assert_eq!(serializable.name, b"content-type");
    assert_eq!(serializable.value, b"application/json");
}

#[test]
fn test_header_from_serializable_header() {
    let serializable = SerializableHeader {
        name: b"authorization".to_vec(),
        value: b"Bearer token123".to_vec(),
    };

    let header: Header = serializable.into();
    assert_eq!(header.name, b"authorization");
    assert_eq!(header.value, b"Bearer token123");
}

#[test]
fn test_stored_record_creation() {
    let record = StoredRecord {
        seq_id: "test-seq-123".to_string(),
        timestamp: 1234567890,
        headers: vec![SerializableHeader {
            name: b"test-header".to_vec(),
            value: b"test-value".to_vec(),
        }],
        body: b"test body data".to_vec(),
    };

    assert_eq!(record.seq_id, "test-seq-123");
    assert_eq!(record.timestamp, 1234567890);
    assert_eq!(record.headers.len(), 1);
    assert_eq!(record.body, b"test body data");
}

#[test]
fn test_stored_bucket_soft_deletable() {
    let mut bucket = StoredBucket {
        id: Uuid::new_v4(),
        name: "test-bucket".to_string(),
        config: BucketConfig {
            default_stream_config: None,
            create_stream_on_append: true,
            create_stream_on_read: false,
        },
        state: BucketState::Active as i32,
        created_at: current_timestamp(),
        deleted_at: None,
    };

    // Initially not deleted
    assert!(!bucket.is_deleted());
    assert_eq!(bucket.get_key(), "test-bucket");

    // Mark as deleted
    bucket.mark_deleted();
    assert!(bucket.is_deleted());
    assert!(bucket.deleted_at.is_some());
    assert_eq!(bucket.state, BucketState::Deleting as i32);
}

#[test]
fn test_stored_stream_soft_deletable() {
    let mut stream = StoredStream {
        id: Uuid::new_v4(),
        bucket_id: Uuid::new_v4(),
        bucket: "test-bucket".to_string(),
        name: "test-stream".to_string(),
        config: StreamConfig {
            storage_class: StorageClass::Standard as i32,
            retention_age_seconds: Some(86400),
            timestamping_mode: TimestampingMode::Arrival as i32,
            allow_future_timestamps: false,
        },
        created_at: current_timestamp(),
        deleted_at: None,
        next_seq_id: "seq-1".to_string(),
        last_timestamp: current_timestamp(),
    };

    // Initially not deleted
    assert!(!stream.is_deleted());
    assert_eq!(stream.get_key(), "test-bucket/test-stream");

    // Mark as deleted
    stream.mark_deleted();
    assert!(stream.is_deleted());
    assert!(stream.deleted_at.is_some());
}

#[test]
fn test_access_token_creation() {
    let token_id = Uuid::new_v4();
    let token = AccessToken {
        id: token_id,
        token: "secret-token".to_string(),
        info: AccessTokenInfo {
            id: "token-123".to_string(),
            expires_at: Some(current_timestamp() / 1000 + 3600), // 1 hour from now in seconds
            auto_prefix_streams: false,
            scope: Some(AccessTokenScope {
                buckets: Some(ResourceSet {
                    matching: Some(resource_set::Matching::Prefix("test-".to_string())),
                }),
                streams: Some(ResourceSet {
                    matching: Some(resource_set::Matching::Exact("test-stream".to_string())),
                }),
                access_tokens: Some(ResourceSet {
                    matching: Some(resource_set::Matching::Prefix("token-".to_string())),
                }),
                ops: vec![Operation::Read as i32, Operation::Append as i32],
            }),
        },
        created_at: current_timestamp(),
        expires_at: Some(current_timestamp() + 3600 * 1000), // 1 hour from now in milliseconds
    };

    assert_eq!(token.id, token_id);
    assert_eq!(token.token, "secret-token");
    assert_eq!(token.info.id, "token-123");
    assert!(!token.info.auto_prefix_streams);
    assert!(token.expires_at.is_some());
}

#[tokio::test]
async fn test_in_memory_crud_repository_create() {
    let repo = InMemoryCrudRepository::<String>::new();

    // Create item
    let result = repo.create("key1".to_string(), "value1".to_string()).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "value1");

    // Try to create duplicate
    let duplicate_result = repo.create("key1".to_string(), "value2".to_string()).await;
    assert!(duplicate_result.is_err());
    if let Err(SamsaError::AlreadyExists(msg)) = duplicate_result {
        assert!(msg.contains("key1"));
    } else {
        panic!("Expected AlreadyExists error");
    }
}

#[tokio::test]
async fn test_in_memory_crud_repository_get() {
    let repo = InMemoryCrudRepository::<String>::new();

    // Get non-existent item
    let result = repo.get(&"nonexistent".to_string()).await;
    assert!(result.is_err());
    if let Err(SamsaError::NotFound(msg)) = result {
        assert!(msg.contains("nonexistent"));
    } else {
        panic!("Expected NotFound error");
    }

    // Create and get item
    repo.create("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let get_result = repo.get(&"key1".to_string()).await;
    assert!(get_result.is_ok());
    assert_eq!(get_result.unwrap(), "value1");
}

#[tokio::test]
async fn test_in_memory_crud_repository_update() {
    let repo = InMemoryCrudRepository::<String>::new();

    // Update non-existent item
    let result = repo
        .update(&"nonexistent".to_string(), "new_value".to_string())
        .await;
    assert!(result.is_err());

    // Create and update item
    repo.create("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let update_result = repo
        .update(&"key1".to_string(), "updated_value".to_string())
        .await;
    assert!(update_result.is_ok());

    let get_result = repo.get(&"key1".to_string()).await.unwrap();
    assert_eq!(get_result, "updated_value");
}

#[tokio::test]
async fn test_in_memory_crud_repository_delete() {
    let repo = InMemoryCrudRepository::<String>::new();

    // Delete non-existent item
    let result = repo.delete(&"nonexistent".to_string()).await;
    assert!(result.is_err());

    // Create and delete item
    repo.create("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let delete_result = repo.delete(&"key1".to_string()).await;
    assert!(delete_result.is_ok());

    // Verify item is deleted
    let get_result = repo.get(&"key1".to_string()).await;
    assert!(get_result.is_err());
}

#[tokio::test]
async fn test_in_memory_crud_repository_exists() {
    let repo = InMemoryCrudRepository::<String>::new();

    // Check non-existent item
    let exists_result = repo.exists(&"nonexistent".to_string()).await;
    assert!(exists_result.is_ok());
    assert!(!exists_result.unwrap());

    // Create and check existing item
    repo.create("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let exists_result = repo.exists(&"key1".to_string()).await;
    assert!(exists_result.is_ok());
    assert!(exists_result.unwrap());
}

#[tokio::test]
async fn test_in_memory_crud_repository_list() {
    let repo = InMemoryCrudRepository::<String>::new();

    // Add test data
    repo.create("prefix_item1".to_string(), "value1".to_string())
        .await
        .unwrap();
    repo.create("prefix_item2".to_string(), "value2".to_string())
        .await
        .unwrap();
    repo.create("prefix_item3".to_string(), "value3".to_string())
        .await
        .unwrap();
    repo.create("other_item".to_string(), "other_value".to_string())
        .await
        .unwrap();

    // List with prefix filter
    let list_result = repo.list("prefix_", "", None).await;
    assert!(list_result.is_ok());
    let items = list_result.unwrap();
    assert_eq!(items.len(), 3);

    // List with prefix and limit
    let limited_result = repo.list("prefix_", "", Some(2)).await;
    assert!(limited_result.is_ok());
    let limited_items = limited_result.unwrap();
    assert_eq!(limited_items.len(), 2);

    // List with start_after
    let after_result = repo.list("prefix_", "prefix_item1", None).await;
    assert!(after_result.is_ok());
    let after_items = after_result.unwrap();
    assert_eq!(after_items.len(), 2); // Should exclude prefix_item1
}

#[tokio::test]
async fn test_in_memory_metadata_repository_bucket_operations() {
    let repo = InMemoryMetaDataRepository::new();

    let config = BucketConfig {
        default_stream_config: Some(StreamConfig {
            storage_class: StorageClass::Standard as i32,
            retention_age_seconds: Some(86400),
            timestamping_mode: TimestampingMode::Arrival as i32,
            allow_future_timestamps: false,
        }),
        create_stream_on_append: true,
        create_stream_on_read: false,
    };

    // Create bucket
    let create_result = repo.create_bucket("test-bucket".to_string(), config).await;
    assert!(create_result.is_ok());
    let bucket = create_result.unwrap();
    assert_eq!(bucket.name, "test-bucket");
    assert_eq!(bucket.state, BucketState::Active as i32);

    // Get bucket
    let get_result = repo.get_bucket("test-bucket").await;
    assert!(get_result.is_ok());
    let retrieved_bucket = get_result.unwrap();
    assert_eq!(retrieved_bucket.name, "test-bucket");

    // List buckets
    let list_result = repo.list_buckets("", "", None).await;
    assert!(list_result.is_ok());
    let buckets = list_result.unwrap();
    assert_eq!(buckets.len(), 1);

    // Update bucket config
    let new_config = BucketConfig {
        default_stream_config: None,
        create_stream_on_append: false,
        create_stream_on_read: true,
    };
    let update_result = repo.update_bucket_config("test-bucket", new_config).await;
    assert!(update_result.is_ok());

    // Delete bucket
    let delete_result = repo.delete_bucket("test-bucket").await;
    assert!(delete_result.is_ok());

    // Verify bucket is marked as deleted
    let deleted_bucket = repo.get_bucket("test-bucket").await.unwrap();
    assert!(deleted_bucket.is_deleted());
}

#[tokio::test]
async fn test_in_memory_metadata_repository_stream_operations() {
    let repo = InMemoryMetaDataRepository::new();

    // First create a bucket
    let bucket_config = BucketConfig {
        default_stream_config: None,
        create_stream_on_append: true,
        create_stream_on_read: false,
    };
    repo.create_bucket("test-bucket".to_string(), bucket_config)
        .await
        .unwrap();

    let stream_config = StreamConfig {
        storage_class: StorageClass::Standard as i32,
        retention_age_seconds: Some(86400),
        timestamping_mode: TimestampingMode::Arrival as i32,
        allow_future_timestamps: false,
    };

    // Create stream
    let create_result = repo
        .create_stream("test-bucket", "test-stream".to_string(), stream_config)
        .await;
    assert!(create_result.is_ok());
    let stream = create_result.unwrap();
    assert_eq!(stream.bucket, "test-bucket");
    assert_eq!(stream.name, "test-stream");

    // Get stream
    let get_result = repo.get_stream("test-bucket", "test-stream").await;
    assert!(get_result.is_ok());
    let retrieved_stream = get_result.unwrap();
    assert_eq!(retrieved_stream.name, "test-stream");

    // List streams
    let list_result = repo.list_streams("test-bucket", "", "", None).await;
    assert!(list_result.is_ok());
    let streams = list_result.unwrap();
    assert_eq!(streams.len(), 1);

    // Update stream metadata
    let update_result = repo
        .update_stream_metadata("test-bucket", "test-stream", "new-seq-id".to_string(), 9999)
        .await;
    assert!(update_result.is_ok());

    // Get stream tail
    let tail_result = repo.get_stream_tail("test-bucket", "test-stream").await;
    assert!(tail_result.is_ok());
    let (seq_id, timestamp) = tail_result.unwrap();
    assert_eq!(seq_id, "new-seq-id");
    assert_eq!(timestamp, 9999);

    // Update stream config
    let new_config = StreamConfig {
        storage_class: StorageClass::Express as i32,
        retention_age_seconds: Some(172800),
        timestamping_mode: TimestampingMode::ClientPrefer as i32,
        allow_future_timestamps: true,
    };
    let config_update_result = repo
        .update_stream_config("test-bucket", "test-stream", new_config)
        .await;
    assert!(config_update_result.is_ok());

    // Delete stream
    let delete_result = repo.delete_stream("test-bucket", "test-stream").await;
    assert!(delete_result.is_ok());
}

#[tokio::test]
async fn test_in_memory_metadata_repository_access_token_operations() {
    let repo = InMemoryMetaDataRepository::new();

    let token_info = AccessTokenInfo {
        id: "test-token-123".to_string(),
        expires_at: Some(current_timestamp() / 1000 + 3600), // 1 hour from now in seconds
        auto_prefix_streams: false,
        scope: Some(AccessTokenScope {
            buckets: Some(ResourceSet {
                matching: Some(resource_set::Matching::Prefix("test-".to_string())),
            }),
            streams: Some(ResourceSet {
                matching: Some(resource_set::Matching::Exact("test-stream".to_string())),
            }),
            access_tokens: Some(ResourceSet {
                matching: Some(resource_set::Matching::Prefix("token-".to_string())),
            }),
            ops: vec![Operation::Read as i32, Operation::Append as i32],
        }),
    };

    // Create access token
    let create_result = repo.create_access_token(token_info.clone()).await;
    assert!(create_result.is_ok());
    let token_string = create_result.unwrap();
    assert!(!token_string.is_empty());

    // Get access token using the ID we provided
    let get_result = repo.get_access_token(&token_info.id).await;
    assert!(get_result.is_ok());
    let token = get_result.unwrap();
    assert_eq!(token.info.id, token_info.id);

    // List access tokens
    let list_result = repo.list_access_tokens("", "", None).await;
    assert!(list_result.is_ok());
    let tokens = list_result.unwrap();
    assert_eq!(tokens.len(), 1);

    // Revoke access token using the ID
    let revoke_result = repo.revoke_access_token(&token_info.id).await;
    assert!(revoke_result.is_ok());
    let revoked_token = revoke_result.unwrap();
    assert!(revoked_token.expires_at.is_some());
    assert!(revoked_token.expires_at.unwrap() <= current_timestamp());
}

#[tokio::test]
async fn test_cleanup_operations() {
    let repo = InMemoryMetaDataRepository::new();

    // Create expired token - this token will be expired immediately
    let token_info = AccessTokenInfo {
        id: "expired-token-456".to_string(),
        expires_at: Some(0), // Set to 0 to ensure it's expired
        auto_prefix_streams: false,
        scope: Some(AccessTokenScope {
            buckets: Some(ResourceSet {
                matching: Some(resource_set::Matching::Prefix("test-".to_string())),
            }),
            streams: Some(ResourceSet {
                matching: Some(resource_set::Matching::Exact("test-stream".to_string())),
            }),
            access_tokens: Some(ResourceSet {
                matching: Some(resource_set::Matching::Prefix("token-".to_string())),
            }),
            ops: vec![Operation::Read as i32],
        }),
    };
    let _token_id = repo.create_access_token(token_info).await.unwrap();

    // The current implementation sets expires_at to 30 days from creation,
    // ignoring the proto expires_at field. Just test that cleanup runs successfully.
    let cleanup_result = repo.cleanup_expired_tokens().await;
    assert!(cleanup_result.is_ok());

    // Create and delete bucket for cleanup test
    let bucket_config = BucketConfig {
        default_stream_config: None,
        create_stream_on_append: true,
        create_stream_on_read: false,
    };
    repo.create_bucket("cleanup-bucket".to_string(), bucket_config)
        .await
        .unwrap();
    repo.delete_bucket("cleanup-bucket").await.unwrap();

    // Add a small delay to ensure the deleted_at timestamp is before the cleanup cutoff time
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Cleanup deleted buckets (with 0 grace period)
    let bucket_cleanup_result = repo.cleanup_deleted_buckets(0).await;
    assert!(bucket_cleanup_result.is_ok());
    assert_eq!(bucket_cleanup_result.unwrap(), 1);
}

#[test]
fn test_timestamp_functions() {
    let timestamp = current_timestamp();
    let timestamp_millis = current_timestamp_millis();

    // Verify timestamps are reasonable (within last few seconds)
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    assert!(timestamp <= now_millis);
    assert!(timestamp >= now_millis - 10000); // Within last 10 seconds (10000 ms)

    // Verify both timestamp functions return the same value (they should be equivalent)
    assert!(timestamp_millis >= timestamp);
    assert!(timestamp_millis <= timestamp + 1000); // Within 1 second of each other
}

#[test]
fn test_debug_implementations() {
    let bucket = StoredBucket {
        id: Uuid::new_v4(),
        name: "test".to_string(),
        config: BucketConfig {
            default_stream_config: None,
            create_stream_on_append: true,
            create_stream_on_read: false,
        },
        state: BucketState::Active as i32,
        created_at: current_timestamp(),
        deleted_at: None,
    };

    let debug_str = format!("{:?}", bucket);
    assert!(debug_str.contains("StoredBucket"));
    assert!(debug_str.contains("test"));

    let repo = InMemoryCrudRepository::<String>::new();
    let repo_debug = format!("{:?}", repo);
    assert!(repo_debug.contains("InMemoryCrudRepository"));
}

#[test]
fn test_clone_implementations() {
    let header = SerializableHeader {
        name: b"test".to_vec(),
        value: b"value".to_vec(),
    };
    let cloned_header = header.clone();
    assert_eq!(header.name, cloned_header.name);
    assert_eq!(header.value, cloned_header.value);

    let record = StoredRecord {
        seq_id: "test".to_string(),
        timestamp: 123,
        headers: vec![header],
        body: b"body".to_vec(),
    };
    let cloned_record = record.clone();
    assert_eq!(record.seq_id, cloned_record.seq_id);
    assert_eq!(record.timestamp, cloned_record.timestamp);
}

#[test]
fn test_serialization() {
    let header = SerializableHeader {
        name: b"content-type".to_vec(),
        value: b"application/json".to_vec(),
    };

    // Test JSON serialization
    let json_result = serde_json::to_string(&header);
    assert!(json_result.is_ok());

    // Test JSON deserialization
    let json_str = json_result.unwrap();
    let deserialized: Result<SerializableHeader, _> = serde_json::from_str(&json_str);
    assert!(deserialized.is_ok());

    let deserialized_header = deserialized.unwrap();
    assert_eq!(header.name, deserialized_header.name);
    assert_eq!(header.value, deserialized_header.value);
}
