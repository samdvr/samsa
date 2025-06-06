//! Unit tests for Samsa
//!
//! These tests focus on testing individual components without requiring
//! external services like etcd or the full infrastructure setup.

use futures_util::stream::StreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use samsa::common::storage::Storage;
use samsa::proto::*;
use std::sync::Arc;

#[test]
fn test_proto_structures() {
    // Test that we can create basic proto structures
    let append_record = AppendRecord {
        timestamp: Some(1234567890),
        headers: vec![Header {
            name: b"content-type".to_vec(),
            value: b"application/json".to_vec(),
        }],
        body: b"test data".to_vec(),
    };

    assert_eq!(append_record.timestamp, Some(1234567890));
    assert_eq!(append_record.headers.len(), 1);
    assert_eq!(append_record.body, b"test data");
}

#[test]
fn test_bucket_config_creation() {
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

    assert!(config.default_stream_config.is_some());
    assert!(config.create_stream_on_append);
    assert!(!config.create_stream_on_read);
}

#[test]
fn test_stream_config_creation() {
    let config = StreamConfig {
        storage_class: StorageClass::Standard as i32,
        retention_age_seconds: Some(86400),
        timestamping_mode: TimestampingMode::Arrival as i32,
        allow_future_timestamps: false,
    };

    assert_eq!(config.storage_class, StorageClass::Standard as i32);
    assert_eq!(config.retention_age_seconds, Some(86400));
    assert_eq!(config.timestamping_mode, TimestampingMode::Arrival as i32);
    assert!(!config.allow_future_timestamps);
}

#[test]
fn test_append_request_creation() {
    let records = vec![
        AppendRecord {
            timestamp: None,
            headers: vec![],
            body: b"record 1".to_vec(),
        },
        AppendRecord {
            timestamp: None,
            headers: vec![],
            body: b"record 2".to_vec(),
        },
    ];

    let request = AppendRequest {
        bucket: "test-bucket".to_string(),
        stream: "test-stream".to_string(),
        records: records.clone(),
        match_seq_id: None,
        fencing_token: None,
    };

    assert_eq!(request.bucket, "test-bucket");
    assert_eq!(request.stream, "test-stream");
    assert_eq!(request.records.len(), 2);
    assert_eq!(request.records[0].body, b"record 1");
    assert_eq!(request.records[1].body, b"record 2");
}

#[test]
fn test_read_request_creation() {
    let request = ReadRequest {
        bucket: "test-bucket".to_string(),
        stream: "test-stream".to_string(),
        start: Some(read_request::Start::SeqId("test-uuid-42".to_string())),
        limit: Some(ReadLimit {
            count: Some(100),
            bytes: None,
        }),
    };

    assert_eq!(request.bucket, "test-bucket");
    assert_eq!(request.stream, "test-stream");

    match request.start {
        Some(read_request::Start::SeqId(seq)) => assert_eq!(seq, "test-uuid-42"),
        _ => panic!("Expected SeqId start"),
    }

    assert!(request.limit.is_some());
    assert_eq!(request.limit.unwrap().count, Some(100));
}

#[test]
fn test_sequenced_record_creation() {
    let record = SequencedRecord {
        seq_id: "test-uuid-42".to_string(),
        timestamp: 1234567890,
        headers: vec![Header {
            name: b"test-header".to_vec(),
            value: b"test-value".to_vec(),
        }],
        body: b"test body data".to_vec(),
    };

    assert_eq!(record.seq_id, "test-uuid-42");
    assert_eq!(record.timestamp, 1234567890);
    assert_eq!(record.headers.len(), 1);
    assert_eq!(record.body, b"test body data");
}

#[test]
fn test_header_creation_and_access() {
    let header = Header {
        name: b"authorization".to_vec(),
        value: b"Bearer token123".to_vec(),
    };

    assert_eq!(header.name, b"authorization");
    assert_eq!(header.value, b"Bearer token123");

    // Test string conversion
    let name_str = String::from_utf8_lossy(&header.name);
    let value_str = String::from_utf8_lossy(&header.value);

    assert_eq!(name_str, "authorization");
    assert_eq!(value_str, "Bearer token123");
}

#[test]
fn test_enums() {
    // Test storage class enum values
    assert_eq!(StorageClass::Unspecified as i32, 0);
    assert_eq!(StorageClass::Standard as i32, 1);
    assert_eq!(StorageClass::Express as i32, 2);

    // Test timestamping mode enum values
    assert_eq!(TimestampingMode::Unspecified as i32, 0);
    assert_eq!(TimestampingMode::ClientPrefer as i32, 1);
    assert_eq!(TimestampingMode::ClientRequire as i32, 2);
    assert_eq!(TimestampingMode::Arrival as i32, 3);

    // These test that the protobuf enums are properly defined
}

#[cfg(test)]
mod helper_functions {
    use super::*;

    #[test]
    fn test_create_test_record_helper() {
        fn create_test_record(content: &str) -> AppendRecord {
            AppendRecord {
                timestamp: None,
                headers: vec![],
                body: content.as_bytes().to_vec(),
            }
        }

        let record = create_test_record("test content");
        assert_eq!(record.body, b"test content");
        assert!(record.headers.is_empty());
        assert!(record.timestamp.is_none());
    }

    #[test]
    fn test_record_to_text_helper() {
        fn record_to_text(record: &SequencedRecord) -> String {
            String::from_utf8_lossy(&record.body).to_string()
        }

        let record = SequencedRecord {
            seq_id: "test-uuid-0".to_string(),
            timestamp: 0,
            headers: vec![],
            body: b"hello world".to_vec(),
        };

        assert_eq!(record_to_text(&record), "hello world");
    }

    #[test]
    fn test_unique_name_generation() {
        fn unique_name(prefix: &str) -> String {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            format!("{}-{}", prefix, timestamp)
        }

        let name1 = unique_name("test");

        // Add a small delay to ensure different timestamps
        std::thread::sleep(std::time::Duration::from_nanos(1));

        let name2 = unique_name("test");

        assert!(name1.starts_with("test-"));
        assert!(name2.starts_with("test-"));
        assert_ne!(name1, name2); // Should be different due to different timestamps
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;

    #[test]
    fn test_empty_bucket_name() {
        let request = CreateBucketRequest {
            bucket: "".to_string(),
            config: None,
        };

        // Empty bucket name should be detectable
        assert!(request.bucket.is_empty());
    }

    #[test]
    fn test_empty_stream_name() {
        let request = CreateStreamRequest {
            stream: "".to_string(),
            config: None,
        };

        // Empty stream name should be detectable
        assert!(request.stream.is_empty());
    }

    #[test]
    fn test_empty_records_batch() {
        let request = AppendRequest {
            bucket: "test-bucket".to_string(),
            stream: "test-stream".to_string(),
            records: vec![],
            match_seq_id: None,
            fencing_token: None,
        };

        // Empty records batch should be detectable
        assert!(request.records.is_empty());
    }

    #[test]
    fn test_read_limit_validation() {
        let limit = ReadLimit {
            count: Some(0),
            bytes: Some(0),
        };

        // Zero limits should be detectable
        assert_eq!(limit.count, Some(0));
        assert_eq!(limit.bytes, Some(0));
    }
}

#[tokio::test]
async fn test_storage_with_node_id() {
    // Test that storage correctly uses node_id in object store keys
    unsafe {
        std::env::set_var("NODE_ID", "test-node-123");
    }

    let object_store = Arc::new(InMemory::new());
    let storage = Storage::new_with_memory(object_store.clone(), "test-node-123".to_string());

    let bucket = "test-bucket";
    let stream = "test-stream";

    // Create bucket and stream
    let bucket_config = BucketConfig {
        default_stream_config: Some(StreamConfig {
            storage_class: StorageClass::Standard as i32,
            retention_age_seconds: Some(86400),
            timestamping_mode: TimestampingMode::Arrival as i32,
            allow_future_timestamps: false,
        }),
        create_stream_on_append: true,
        create_stream_on_read: false,
    };

    storage
        .create_bucket(bucket.to_string(), bucket_config)
        .await
        .unwrap();

    // Append a record
    let records = vec![AppendRecord {
        timestamp: Some(1234567890),
        headers: vec![Header {
            name: b"test-header".to_vec(),
            value: b"test-value".to_vec(),
        }],
        body: b"test body".to_vec(),
    }];

    let (start_seq_id, _end_seq_id, _start_timestamp, _end_timestamp) = storage
        .append_records(bucket, stream, records)
        .await
        .unwrap();

    // Force flush to ensure the record is written to object store
    storage.flush_stream(bucket, stream).await.unwrap();

    // Verify the record was stored with correct key format
    let prefix = format!("{}/{}/test-node-123/", bucket, stream);
    let list_result = object_store
        .list(Some(&object_store::path::Path::from(prefix)))
        .collect::<Vec<_>>()
        .await;

    assert!(!list_result.is_empty());
    let stored_object = &list_result[0].as_ref().unwrap();
    let key_parts: Vec<&str> = stored_object.location.as_ref().split('/').collect();

    // Verify key format: bucket/stream/node_id/record_id
    assert_eq!(key_parts.len(), 4);
    assert_eq!(key_parts[0], bucket);
    assert_eq!(key_parts[1], stream);
    assert_eq!(key_parts[2], "test-node-123");

    // Read records back
    let read_records = storage
        .read_records(bucket, stream, &start_seq_id, None)
        .await
        .unwrap();
    assert_eq!(read_records.len(), 1);
    assert_eq!(read_records[0].body, b"test body");
    assert_eq!(read_records[0].headers.len(), 1);
    assert_eq!(read_records[0].headers[0].name, b"test-header");
    assert_eq!(read_records[0].headers[0].value, b"test-value");

    // Clean up env var
    unsafe {
        std::env::remove_var("NODE_ID");
    }
}
