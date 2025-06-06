//! Integration tests for PostgreSQL metadata repository
//!
//! These tests require a running PostgreSQL instance.

use samsa::common::db::{DatabaseConfig, init_postgres_pool, run_migrations};
use samsa::common::metadata::MetaDataRepository;
use samsa::common::postgres_metadata_repository::PostgresMetaDataRepository;
use samsa::proto::{
    AccessTokenInfo, BucketConfig, BucketState, StorageClass, StreamConfig, TimestampingMode,
};
use uuid::Uuid;

#[tokio::test]
#[ignore] // Requires PostgreSQL to be running
async fn test_postgres_bucket_operations() {
    let config = DatabaseConfig::default();
    let pool = init_postgres_pool(&config)
        .await
        .expect("Failed to connect to PostgreSQL");
    run_migrations(&pool)
        .await
        .expect("Failed to run migrations");

    let repo = PostgresMetaDataRepository::new(pool);

    // Test create bucket
    let bucket_name = format!("test-bucket-{}", Uuid::new_v4());
    let bucket_config = BucketConfig {
        default_stream_config: None,
        create_stream_on_append: true,
        create_stream_on_read: false,
    };

    let created_bucket = repo.create_bucket(bucket_name.clone(), bucket_config).await;
    assert!(created_bucket.is_ok());
    let bucket = created_bucket.unwrap();
    assert_eq!(bucket.name, bucket_name);
    assert_eq!(bucket.state, BucketState::Active as i32);

    // Test get bucket
    let retrieved_bucket = repo.get_bucket(&bucket_name).await;
    assert!(retrieved_bucket.is_ok());
    let retrieved = retrieved_bucket.unwrap();
    assert_eq!(retrieved.name, bucket_name);
    assert_eq!(
        retrieved.config.create_stream_on_append,
        bucket_config.create_stream_on_append
    );

    // Test list buckets
    let buckets = repo.list_buckets("test-bucket-", "", Some(10)).await;
    assert!(buckets.is_ok());
    let bucket_list = buckets.unwrap();
    assert!(!bucket_list.is_empty());

    // Test delete bucket
    let delete_result = repo.delete_bucket(&bucket_name).await;
    assert!(delete_result.is_ok());

    // Test bucket not found after deletion
    let deleted_bucket = repo.get_bucket(&bucket_name).await;
    assert!(deleted_bucket.is_err());
}

#[tokio::test]
#[ignore] // Requires PostgreSQL to be running
async fn test_postgres_stream_operations() {
    let config = DatabaseConfig::default();
    let pool = init_postgres_pool(&config)
        .await
        .expect("Failed to connect to PostgreSQL");
    run_migrations(&pool)
        .await
        .expect("Failed to run migrations");

    let repo = PostgresMetaDataRepository::new(pool);

    // First create a bucket
    let bucket_name = format!("test-bucket-{}", Uuid::new_v4());
    let bucket_config = BucketConfig {
        default_stream_config: None,
        create_stream_on_append: true,
        create_stream_on_read: false,
    };
    repo.create_bucket(bucket_name.clone(), bucket_config)
        .await
        .unwrap();

    // Test create stream
    let stream_name = format!("test-stream-{}", Uuid::new_v4());
    let stream_config = StreamConfig {
        storage_class: StorageClass::Standard as i32,
        retention_age_seconds: Some(86400),
        timestamping_mode: TimestampingMode::Arrival as i32,
        allow_future_timestamps: false,
    };

    let created_stream = repo
        .create_stream(&bucket_name, stream_name.clone(), stream_config)
        .await;
    assert!(created_stream.is_ok());
    let stream = created_stream.unwrap();
    assert_eq!(stream.name, stream_name);
    assert_eq!(stream.bucket, bucket_name);

    // Test get stream
    let retrieved_stream = repo.get_stream(&bucket_name, &stream_name).await;
    assert!(retrieved_stream.is_ok());
    let retrieved = retrieved_stream.unwrap();
    assert_eq!(retrieved.name, stream_name);
    assert_eq!(retrieved.bucket, bucket_name);

    // Test list streams
    let streams = repo
        .list_streams(&bucket_name, "test-stream-", "", Some(10))
        .await;
    assert!(streams.is_ok());
    let stream_list = streams.unwrap();
    assert!(!stream_list.is_empty());

    // Test get stream tail
    let tail = repo.get_stream_tail(&bucket_name, &stream_name).await;
    assert!(tail.is_ok());
    let (seq_id, timestamp) = tail.unwrap();
    assert_eq!(seq_id, "0");
    assert!(timestamp > 0);

    // Test delete stream
    let delete_result = repo.delete_stream(&bucket_name, &stream_name).await;
    assert!(delete_result.is_ok());

    // Test stream not found after deletion
    let deleted_stream = repo.get_stream(&bucket_name, &stream_name).await;
    assert!(deleted_stream.is_err());

    // Clean up bucket
    repo.delete_bucket(&bucket_name).await.unwrap();
}

#[tokio::test]
#[ignore] // Requires PostgreSQL to be running  
async fn test_postgres_access_token_operations() {
    let config = DatabaseConfig::default();
    let pool = init_postgres_pool(&config)
        .await
        .expect("Failed to connect to PostgreSQL");
    run_migrations(&pool)
        .await
        .expect("Failed to run migrations");

    let repo = PostgresMetaDataRepository::new(pool);

    // Test create access token
    let token_info = AccessTokenInfo {
        id: format!("token-{}", Uuid::new_v4()),
        expires_at: None,
        auto_prefix_streams: false,
        scope: None,
    };

    let created_token = repo.create_access_token(token_info.clone()).await;
    assert!(created_token.is_ok());
    let token_value = created_token.unwrap();
    assert!(!token_value.is_empty());

    // Test get access token
    let retrieved_token = repo.get_access_token(&token_value).await;
    assert!(retrieved_token.is_ok());
    let token = retrieved_token.unwrap();
    assert_eq!(token.token, token_value);
    assert_eq!(token.info.id, token_info.id);

    // Test list access tokens
    let tokens = repo.list_access_tokens("", "", Some(10)).await;
    assert!(tokens.is_ok());
    let token_list = tokens.unwrap();
    assert!(!token_list.is_empty());

    // Test revoke access token
    let revoke_result = repo.revoke_access_token(&token_value).await;
    assert!(revoke_result.is_ok());

    // Test token not found after revocation
    let revoked_token = repo.get_access_token(&token_value).await;
    assert!(revoked_token.is_err());
}

#[tokio::test]
#[ignore] // Requires PostgreSQL to be running
async fn test_postgres_cleanup_operations() {
    let config = DatabaseConfig::default();
    let pool = init_postgres_pool(&config)
        .await
        .expect("Failed to connect to PostgreSQL");
    run_migrations(&pool)
        .await
        .expect("Failed to run migrations");

    let repo = PostgresMetaDataRepository::new(pool);

    // Test cleanup operations don't error
    let expired_tokens = repo.cleanup_expired_tokens().await;
    assert!(expired_tokens.is_ok());

    let deleted_buckets = repo.cleanup_deleted_buckets(3600).await;
    assert!(deleted_buckets.is_ok());

    let deleted_streams = repo.cleanup_deleted_streams(3600).await;
    assert!(deleted_streams.is_ok());
}
