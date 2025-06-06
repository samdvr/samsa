//!  Integration Tests for Samsa
//!
//! Fast, focused integration tests covering core functionality

mod test_utils;

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use test_utils::{
    SamsaTestClient, TestEnvironment, cleanup_shared_infrastructure, cleanup_test_data,
    create_test_record, create_test_record_with_headers, init_shared_infrastructure,
    record_to_text, unique_name,
};

// Initialize shared infrastructure once for all tests
#[tokio::test]
async fn test_00_init_shared_infrastructure() {
    init_shared_infrastructure()
        .await
        .expect("Failed to initialize shared infrastructure");
    println!("✅ Shared infrastructure initialized");
}

// ================================================================================================
// CORE FUNCTIONALITY TESTS
// ================================================================================================

#[tokio::test]
async fn test_infrastructure_health() {
    cleanup_test_data()
        .await
        .expect("Failed to clean test data");

    let mut test_client = SamsaTestClient::new()
        .await
        .expect("Failed to create test client");

    let bucket_name = unique_name("health-check");
    let stream_name = unique_name("health-stream");

    // Test basic operations
    test_client
        .create_bucket(&bucket_name)
        .await
        .expect("Failed to create bucket for health check");

    test_client
        .create_stream(&bucket_name, &stream_name)
        .await
        .expect("Failed to create stream for health check");

    test_client
        .append_text(&bucket_name, &stream_name, "health check")
        .await
        .expect("Failed to append record for health check");

    let records = test_client
        .read_all(&bucket_name, &stream_name)
        .await
        .expect("Failed to read records for health check");

    assert_eq!(records.len(), 1);
    assert_eq!(record_to_text(&records[0]), "health check");

    println!("✅ Infrastructure health check passed!");
}

#[tokio::test]
async fn test_basic_workflow() {
    cleanup_test_data()
        .await
        .expect("Failed to clean test data");

    let mut client = SamsaTestClient::new()
        .await
        .expect("Failed to create test client");

    let bucket_name = unique_name("test-bucket");
    let stream_name = unique_name("test-stream");

    // Create bucket
    let bucket_info = client
        .create_bucket(&bucket_name)
        .await
        .expect("Failed to create bucket");
    assert_eq!(bucket_info.name, bucket_name);

    // Create stream
    let stream_info = client
        .create_stream(&bucket_name, &stream_name)
        .await
        .expect("Failed to create stream");
    assert_eq!(stream_info.name, stream_name);

    // Append records
    let records = vec![
        create_test_record("First message"),
        create_test_record("Second message"),
    ];

    let append_response = client
        .append_records(&bucket_name, &stream_name, records)
        .await
        .expect("Failed to append records");
    assert!(!append_response.start_seq_id.is_empty());

    // Read records back
    let read_records = client
        .read_all(&bucket_name, &stream_name)
        .await
        .expect("Failed to read records");

    assert_eq!(read_records.len(), 2);
    assert_eq!(record_to_text(&read_records[0]), "First message");
    assert_eq!(record_to_text(&read_records[1]), "Second message");

    println!("✅ Basic workflow test passed!");
}

#[tokio::test]
async fn test_multiple_buckets_and_streams() {
    cleanup_test_data()
        .await
        .expect("Failed to clean test data");

    let mut client = SamsaTestClient::new()
        .await
        .expect("Failed to create test client");

    // Create 2 buckets with 2 streams each
    let buckets = vec!["bucket-1", "bucket-2"];
    let streams = vec!["stream-a", "stream-b"];

    for bucket in &buckets {
        client
            .create_bucket(bucket)
            .await
            .expect("Failed to create bucket");

        for stream in &streams {
            client
                .create_stream(bucket, stream)
                .await
                .expect("Failed to create stream");

            // Add unique data to each stream
            let content = format!("data-{}-{}", bucket, stream);
            client
                .append_text(bucket, stream, &content)
                .await
                .expect("Failed to append record");
        }
    }

    // Verify data in each stream
    for bucket in &buckets {
        for stream in &streams {
            let records = client
                .read_all(bucket, stream)
                .await
                .expect("Failed to read records");

            assert_eq!(records.len(), 1);
            let expected = format!("data-{}-{}", bucket, stream);
            assert_eq!(record_to_text(&records[0]), expected);
        }
    }

    println!("✅ Multiple buckets and streams test passed!");
}

#[tokio::test]
async fn test_record_headers_and_data_types() {
    cleanup_test_data()
        .await
        .expect("Failed to clean test data");

    let mut client = SamsaTestClient::new()
        .await
        .expect("Failed to create test client");

    let bucket_name = unique_name("headers-bucket");
    let stream_name = unique_name("headers-stream");

    client
        .create_bucket(&bucket_name)
        .await
        .expect("Failed to create bucket");
    client
        .create_stream(&bucket_name, &stream_name)
        .await
        .expect("Failed to create stream");

    // Test different data types with headers
    let records = vec![
        create_test_record("plain text"),
        create_test_record_with_headers(
            "{\"json\": \"data\"}",
            vec![("content-type", "application/json")],
        ),
        create_test_record_with_headers(
            "Special chars: áéíóú 🚀",
            vec![("encoding", "utf-8"), ("type", "unicode")],
        ),
    ];

    client
        .append_records(&bucket_name, &stream_name, records)
        .await
        .expect("Failed to append records");

    let read_records = client
        .read_all(&bucket_name, &stream_name)
        .await
        .expect("Failed to read records");

    assert_eq!(read_records.len(), 3);

    // Verify first record (no headers)
    assert_eq!(read_records[0].headers.len(), 0);
    assert_eq!(record_to_text(&read_records[0]), "plain text");

    // Verify second record (JSON with headers)
    assert_eq!(read_records[1].headers.len(), 1);
    assert_eq!(record_to_text(&read_records[1]), "{\"json\": \"data\"}");

    // Verify third record (Unicode with multiple headers)
    assert_eq!(read_records[2].headers.len(), 2);
    assert_eq!(record_to_text(&read_records[2]), "Special chars: áéíóú 🚀");

    println!("✅ Record headers and data types test passed!");
}

// ================================================================================================
// CONFIGURATION TESTS
// ================================================================================================

#[tokio::test]
async fn test_bucket_configuration() {
    let _env = TestEnvironment::setup_minimal()
        .await
        .expect("Failed to setup test environment");

    let mut client = SamsaTestClient::new()
        .await
        .expect("Failed to create test client");

    let bucket_name = unique_name("config-bucket");

    // Create bucket with custom config
    let custom_config = samsa::proto::BucketConfig {
        default_stream_config: Some(samsa::proto::StreamConfig {
            storage_class: samsa::proto::StorageClass::Express as i32,
            retention_age_seconds: Some(3600),
            timestamping_mode: samsa::proto::TimestampingMode::ClientPrefer as i32,
            allow_future_timestamps: true,
        }),
        create_stream_on_append: false,
        create_stream_on_read: true,
    };

    let bucket_info = client
        .create_bucket_with_config(&bucket_name, custom_config)
        .await
        .expect("Failed to create bucket with config");

    assert_eq!(bucket_info.name, bucket_name);

    // Get and verify config
    let retrieved_config = client
        .get_bucket_config(&bucket_name)
        .await
        .expect("Failed to get bucket config");

    assert!(!retrieved_config.create_stream_on_append);
    assert!(retrieved_config.create_stream_on_read);

    println!("✅ Bucket configuration test passed!");
}

// ================================================================================================
// ERROR HANDLING TESTS
// ================================================================================================

#[tokio::test]
async fn test_error_scenarios() {
    let _env = TestEnvironment::setup_minimal()
        .await
        .expect("Failed to setup test environment");

    let mut client = SamsaTestClient::new()
        .await
        .expect("Failed to create test client");

    // Allow connection to fully stabilize
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Test reading from non-existent resources
    let result = client
        .read_all("non-existent-bucket", "non-existent-stream")
        .await;
    assert!(
        result.is_err(),
        "Should fail to read from non-existent resources"
    );

    // Test creating bucket and stream
    let bucket_name = unique_name("test-bucket");
    client
        .create_bucket(&bucket_name)
        .await
        .expect("Failed to create bucket first time");

    let stream_name = unique_name("test-stream");
    client
        .create_stream(&bucket_name, &stream_name)
        .await
        .expect("Failed to create stream");

    client
        .append_text(&bucket_name, &stream_name, "test record")
        .await
        .expect("Failed to append record");

    let records = client
        .read_all(&bucket_name, &stream_name)
        .await
        .expect("Failed to read records");

    assert_eq!(records.len(), 1);

    println!("✅ Error scenarios test passed!");
}

// ================================================================================================
// DELETE OPERATIONS TESTS
// ================================================================================================

#[tokio::test]
async fn test_delete_operations() {
    let _env = TestEnvironment::setup_minimal()
        .await
        .expect("Failed to setup test environment");

    let mut client = SamsaTestClient::new()
        .await
        .expect("Failed to create test client");

    // Allow connection to fully stabilize
    tokio::time::sleep(Duration::from_millis(200)).await;

    let bucket_name = unique_name("delete-bucket");
    let stream_name_1 = unique_name("delete-stream-1");
    let stream_name_2 = unique_name("delete-stream-2");

    // Create bucket and streams
    client
        .create_bucket(&bucket_name)
        .await
        .expect("Failed to create bucket");
    client
        .create_stream(&bucket_name, &stream_name_1)
        .await
        .expect("Failed to create stream 1");
    client
        .create_stream(&bucket_name, &stream_name_2)
        .await
        .expect("Failed to create stream 2");

    // Add data to streams
    client
        .append_text(&bucket_name, &stream_name_1, "data in stream 1")
        .await
        .expect("Failed to append to stream 1");

    // Verify streams exist
    let streams = client
        .list_streams(&bucket_name)
        .await
        .expect("Failed to list streams");
    assert!(streams.len() >= 2);

    // Test deleting individual stream
    client
        .delete_stream(&bucket_name, &stream_name_1)
        .await
        .expect("Failed to delete stream");

    // Test deleting entire bucket
    client
        .delete_bucket(&bucket_name)
        .await
        .expect("Failed to delete bucket");

    println!("✅ Delete operations test passed!");
}

// ================================================================================================
// BATCH OPERATIONS TEST
// ================================================================================================

#[tokio::test]
async fn test_batch_operations() {
    cleanup_test_data()
        .await
        .expect("Failed to clean test data");

    let mut client = SamsaTestClient::new()
        .await
        .expect("Failed to create test client");

    let bucket_name = unique_name("batch-bucket");
    let stream_name = unique_name("batch-stream");

    client
        .create_bucket(&bucket_name)
        .await
        .expect("Failed to create bucket");
    client
        .create_stream(&bucket_name, &stream_name)
        .await
        .expect("Failed to create stream");

    // Small batch for speed
    let batch_size = 10;
    let mut records = Vec::new();
    for i in 0..batch_size {
        records.push(create_test_record(&format!("batch-record-{}", i)));
    }

    let response = client
        .append_records(&bucket_name, &stream_name, records)
        .await
        .expect("Failed to append batch");

    assert!(!response.start_seq_id.is_empty());

    let read_records = client
        .read_all(&bucket_name, &stream_name)
        .await
        .expect("Failed to read records");

    assert_eq!(read_records.len(), batch_size);
    for (i, record) in read_records.iter().enumerate() {
        let expected = format!("batch-record-{}", i);
        assert_eq!(record_to_text(record), expected);
    }

    println!("✅ Batch operations test passed!");
}

// ================================================================================================
// TIMESTAMP TEST
// ================================================================================================

#[tokio::test]
async fn test_timestamps() {
    let _env = TestEnvironment::setup_minimal()
        .await
        .expect("Failed to setup test environment");

    let mut client = SamsaTestClient::new()
        .await
        .expect("Failed to create test client");

    // Allow connection to fully stabilize
    tokio::time::sleep(Duration::from_millis(200)).await;

    let bucket_name = unique_name("timestamp-bucket");
    let stream_name = unique_name("timestamp-stream");

    client
        .create_bucket(&bucket_name)
        .await
        .expect("Failed to create bucket");
    client
        .create_stream(&bucket_name, &stream_name)
        .await
        .expect("Failed to create stream");

    let start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Append record without explicit timestamp
    client
        .append_text(&bucket_name, &stream_name, "record without timestamp")
        .await
        .expect("Failed to append record");

    // Read and verify timestamp
    let records = client
        .read_all(&bucket_name, &stream_name)
        .await
        .expect("Failed to read records");

    assert_eq!(records.len(), 1);

    let record_timestamp = records[0].timestamp;
    assert!(record_timestamp >= start_time);
    assert!(record_timestamp <= start_time + 30000); // Within 30 seconds

    println!("✅ Timestamp test passed!");
}

// Cleanup shared infrastructure at the end
#[tokio::test]
async fn test_zz_cleanup_shared_infrastructure() {
    cleanup_shared_infrastructure();
    println!("✅ Shared infrastructure cleaned up");
}
