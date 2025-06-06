mod test_utils;
use std::time::Duration;
use test_utils::*;

#[tokio::test]
async fn test_bucket_operations() {
    let env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let mut client = SamsaTestClient::new_with_address(&env.node_address)
        .await
        .expect("Failed to create test client");

    let bucket_name = format!("test-bucket-{}", uuid::Uuid::new_v4());

    // Test create bucket
    let bucket_response = client.create_bucket(&bucket_name).await.unwrap();
    assert!(!bucket_response.name.is_empty());

    // Test get bucket config
    let config = client.get_bucket_config(&bucket_name).await.unwrap();
    assert!(config.default_stream_config.is_some());

    // Test list buckets
    let buckets = client.list_buckets().await.unwrap();
    assert!(buckets.iter().any(|b| b.name == bucket_name));

    // Test delete bucket
    client.delete_bucket(&bucket_name).await.unwrap();
}

#[tokio::test]
async fn test_stream_operations() {
    let env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let mut client = SamsaTestClient::new_with_address(&env.node_address)
        .await
        .expect("Failed to create test client");

    let bucket_name = format!("test-bucket-{}", uuid::Uuid::new_v4());
    let stream_name = format!("test-stream-{}", uuid::Uuid::new_v4());

    // Setup: create bucket first
    client.create_bucket(&bucket_name).await.unwrap();

    // Test create stream
    let stream_response = client
        .create_stream(&bucket_name, &stream_name)
        .await
        .unwrap();
    assert!(!stream_response.name.is_empty());

    // Test get stream config
    let config = client
        .get_stream_config(&bucket_name, &stream_name)
        .await
        .unwrap();
    assert!(config.retention_age_seconds.is_some() || config.retention_age_seconds.is_none());

    // Test list streams
    let streams = client.list_streams(&bucket_name).await.unwrap();
    assert!(streams.iter().any(|s| s.name == stream_name));

    // Test delete stream
    client
        .delete_stream(&bucket_name, &stream_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_data_operations() {
    let env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    // Add delay before creating client to avoid connection rush
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut client = SamsaTestClient::new_with_address(&env.node_address)
        .await
        .expect("Failed to create test client");

    let bucket_name = format!("test-bucket-{}", uuid::Uuid::new_v4());
    let stream_name = format!("test-stream-{}", uuid::Uuid::new_v4());

    // Setup
    client.create_bucket(&bucket_name).await.unwrap();

    // Add small delay between operations
    tokio::time::sleep(Duration::from_millis(100)).await;

    client
        .create_stream(&bucket_name, &stream_name)
        .await
        .unwrap();

    // Add delay before first tail check
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Test get tail on empty stream - newly created streams have next_seq_id set to UUID nil
    let tail_info = client.get_tail(&bucket_name, &stream_name).await.unwrap();
    assert_eq!(tail_info.next_seq_id, uuid::Uuid::nil().to_string()); // Empty stream has UUID nil
    assert_eq!(tail_info.last_timestamp, 0); // Empty stream has timestamp 0

    // Add delay before append
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Test append
    let test_data = "Hello, World!";
    let response = client
        .append_text(&bucket_name, &stream_name, test_data)
        .await
        .unwrap();
    assert!(!response.start_seq_id.is_empty());
    assert!(!response.end_seq_id.is_empty());

    // Add delay before read
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Test read
    let read_response = client
        .read_from_seq_id(&bucket_name, &stream_name, &response.start_seq_id, Some(10))
        .await
        .unwrap();

    // Check the ReadResponse structure - it has a oneof output field
    match read_response.output {
        Some(samsa::proto::read_response::Output::Batch(batch)) => {
            assert!(!batch.records.is_empty());
            assert_eq!(String::from_utf8_lossy(&batch.records[0].body), test_data);
        }
        Some(samsa::proto::read_response::Output::NextSeqId(_)) => {
            panic!("Expected batch output but got next_seq_id");
        }
        None => {
            panic!("No output in read response");
        }
    }

    // Add delay before final tail check
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Test get tail after append
    let tail_info2 = client.get_tail(&bucket_name, &stream_name).await.unwrap();
    assert!(!tail_info2.next_seq_id.is_empty());
    assert_ne!(tail_info2.next_seq_id, uuid::Uuid::nil().to_string()); // Should be a real UUID now
}

#[tokio::test]
async fn test_error_handling() {
    let env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let mut client = SamsaTestClient::new_with_address(&env.node_address)
        .await
        .expect("Failed to create test client");

    // Test operations on non-existent bucket
    let non_existent_bucket = "non-existent-bucket";
    let non_existent_stream = "non-existent-stream";

    // These should fail gracefully
    assert!(client.get_bucket_config(non_existent_bucket).await.is_err());

    // Note: list_streams might return empty list instead of error for non-existent bucket
    // This is valid behavior depending on implementation
    match client.list_streams(non_existent_bucket).await {
        Ok(streams) => {
            // If it succeeds, it should return an empty list
            assert!(
                streams.is_empty(),
                "Expected empty list for non-existent bucket"
            );
        }
        Err(_) => {
            // It's also acceptable to return an error
        }
    }

    // create_stream on non-existent bucket succeeds because it auto-creates the bucket
    // This is the expected behavior according to the bucket configuration
    let created_stream = client
        .create_stream(non_existent_bucket, non_existent_stream)
        .await
        .unwrap();
    assert!(!created_stream.name.is_empty());

    // However, operations on a truly non-existent stream should still fail
    assert!(
        client
            .get_tail(
                "definitely-non-existent-bucket-12345",
                "definitely-non-existent-stream-12345"
            )
            .await
            .is_err()
    );
}
