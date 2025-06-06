//! Comprehensive unit tests for the batch module
//!
//! Tests batch configuration, pending batch functionality, and serialization

use samsa::common::batch::{BatchConfig, PendingBatch, StoredRecordBatch};
use samsa::common::metadata::{SerializableHeader, StoredRecord};
use std::time::{Duration, SystemTime};

#[test]
fn test_batch_config_default() {
    let config = BatchConfig::default();

    assert_eq!(config.max_records, 1000);
    assert_eq!(config.max_bytes, 1024 * 1024); // 1MB
    assert_eq!(config.flush_interval_ms, 5000); // 5 seconds
    assert_eq!(config.max_batches_in_memory, 100);
}

#[test]
fn test_batch_config_custom() {
    let config = BatchConfig {
        max_records: 500,
        max_bytes: 512 * 1024,   // 512KB
        flush_interval_ms: 2000, // 2 seconds
        max_batches_in_memory: 50,
    };

    assert_eq!(config.max_records, 500);
    assert_eq!(config.max_bytes, 512 * 1024);
    assert_eq!(config.flush_interval_ms, 2000);
    assert_eq!(config.max_batches_in_memory, 50);
}

#[test]
fn test_pending_batch_new() {
    let batch = PendingBatch::new();

    assert!(batch.records.is_empty());
    assert_eq!(batch.current_bytes, 0);
    assert!(batch.first_seq_id.is_none());
    assert!(batch.last_seq_id.is_none());
    assert!(batch.first_timestamp.is_none());
    assert!(batch.last_timestamp.is_none());
    assert!(batch.is_empty());
}

#[test]
fn test_pending_batch_default() {
    let batch = PendingBatch::default();
    assert!(batch.is_empty());
}

#[test]
fn test_pending_batch_add_first_record() {
    let mut batch = PendingBatch::new();
    let record = StoredRecord {
        seq_id: "seq-1".to_string(),
        timestamp: 1000,
        headers: vec![SerializableHeader {
            name: b"content-type".to_vec(),
            value: b"application/json".to_vec(),
        }],
        body: b"test data".to_vec(),
    };

    let result = batch.add_record(record.clone());
    assert!(result.is_ok());

    assert_eq!(batch.records.len(), 1);
    assert!(!batch.is_empty());
    assert_eq!(batch.first_seq_id, Some("seq-1".to_string()));
    assert_eq!(batch.last_seq_id, Some("seq-1".to_string()));
    assert_eq!(batch.first_timestamp, Some(1000));
    assert_eq!(batch.last_timestamp, Some(1000));
    assert!(batch.current_bytes > 0);
}

#[test]
fn test_pending_batch_add_multiple_records() {
    let mut batch = PendingBatch::new();

    let record1 = StoredRecord {
        seq_id: "seq-1".to_string(),
        timestamp: 1000,
        headers: vec![],
        body: b"data 1".to_vec(),
    };

    let record2 = StoredRecord {
        seq_id: "seq-2".to_string(),
        timestamp: 2000,
        headers: vec![],
        body: b"data 2".to_vec(),
    };

    let record3 = StoredRecord {
        seq_id: "seq-3".to_string(),
        timestamp: 1500, // Middle timestamp
        headers: vec![],
        body: b"data 3".to_vec(),
    };

    batch.add_record(record1).unwrap();
    batch.add_record(record2).unwrap();
    batch.add_record(record3).unwrap();

    assert_eq!(batch.records.len(), 3);
    assert_eq!(batch.first_seq_id, Some("seq-1".to_string()));
    assert_eq!(batch.last_seq_id, Some("seq-3".to_string()));
    assert_eq!(batch.first_timestamp, Some(1000)); // Stays as first added
    assert_eq!(batch.last_timestamp, Some(1500)); // Updates to last added
}

#[test]
fn test_should_flush_by_record_count() {
    let config = BatchConfig {
        max_records: 2,
        max_bytes: 1024 * 1024,
        flush_interval_ms: 5000,
        max_batches_in_memory: 100,
    };

    let mut batch = PendingBatch::new();
    assert!(!batch.should_flush(&config));

    // Add first record
    let record1 = StoredRecord {
        seq_id: "seq-1".to_string(),
        timestamp: 1000,
        headers: vec![],
        body: b"data".to_vec(),
    };
    batch.add_record(record1).unwrap();
    assert!(!batch.should_flush(&config));

    // Add second record - should trigger flush
    let record2 = StoredRecord {
        seq_id: "seq-2".to_string(),
        timestamp: 2000,
        headers: vec![],
        body: b"data".to_vec(),
    };
    batch.add_record(record2).unwrap();
    assert!(batch.should_flush(&config));
}

#[test]
fn test_should_flush_by_byte_size() {
    let config = BatchConfig {
        max_records: 1000,
        max_bytes: 50, // Very small byte limit
        flush_interval_ms: 5000,
        max_batches_in_memory: 100,
    };

    let mut batch = PendingBatch::new();

    // Add a large record that exceeds byte limit
    let large_record = StoredRecord {
        seq_id: "seq-large".to_string(),
        timestamp: 1000,
        headers: vec![],
        body: vec![b'x'; 100], // 100 bytes
    };

    batch.add_record(large_record).unwrap();
    assert!(batch.should_flush(&config));
}

#[test]
fn test_should_flush_by_time() {
    let config = BatchConfig {
        max_records: 1000,
        max_bytes: 1024 * 1024,
        flush_interval_ms: 100, // 100ms
        max_batches_in_memory: 100,
    };

    let mut batch = PendingBatch::new();
    batch.created_at = SystemTime::now() - Duration::from_millis(200); // 200ms ago

    // Add a small record
    let record = StoredRecord {
        seq_id: "seq-1".to_string(),
        timestamp: 1000,
        headers: vec![],
        body: b"small".to_vec(),
    };
    batch.add_record(record).unwrap();

    // Should flush due to time
    assert!(batch.should_flush(&config));
}

#[test]
fn test_should_not_flush_within_time_limit() {
    let config = BatchConfig {
        max_records: 1000,
        max_bytes: 1024 * 1024,
        flush_interval_ms: 5000, // 5 seconds
        max_batches_in_memory: 100,
    };

    let mut batch = PendingBatch::new();

    // Add a small record
    let record = StoredRecord {
        seq_id: "seq-1".to_string(),
        timestamp: 1000,
        headers: vec![],
        body: b"small".to_vec(),
    };
    batch.add_record(record).unwrap();

    // Should not flush - within time limit, record count, and byte size
    assert!(!batch.should_flush(&config));
}

#[test]
fn test_to_stored_batch() {
    let mut batch = PendingBatch::new();

    let record1 = StoredRecord {
        seq_id: "seq-1".to_string(),
        timestamp: 1000,
        headers: vec![],
        body: b"data 1".to_vec(),
    };

    let record2 = StoredRecord {
        seq_id: "seq-2".to_string(),
        timestamp: 2000,
        headers: vec![],
        body: b"data 2".to_vec(),
    };

    batch.add_record(record1).unwrap();
    batch.add_record(record2).unwrap();

    let stored_batch = batch.to_stored_batch(
        "batch-123".to_string(),
        "test-bucket".to_string(),
        "test-stream".to_string(),
    );

    assert_eq!(stored_batch.batch_id, "batch-123");
    assert_eq!(stored_batch.bucket, "test-bucket");
    assert_eq!(stored_batch.stream, "test-stream");
    assert_eq!(stored_batch.records.len(), 2);
    assert_eq!(stored_batch.first_seq_id, "seq-1");
    assert_eq!(stored_batch.last_seq_id, "seq-2");
    assert_eq!(stored_batch.first_timestamp, 1000);
    assert_eq!(stored_batch.last_timestamp, 2000);
    assert!(stored_batch.created_at > 0);
}

#[test]
fn test_to_stored_batch_empty() {
    let batch = PendingBatch::new();

    let stored_batch = batch.to_stored_batch(
        "batch-empty".to_string(),
        "test-bucket".to_string(),
        "test-stream".to_string(),
    );

    assert_eq!(stored_batch.batch_id, "batch-empty");
    assert_eq!(stored_batch.bucket, "test-bucket");
    assert_eq!(stored_batch.stream, "test-stream");
    assert_eq!(stored_batch.records.len(), 0);
    assert_eq!(stored_batch.first_seq_id, "");
    assert_eq!(stored_batch.last_seq_id, "");
    assert_eq!(stored_batch.first_timestamp, 0);
    assert_eq!(stored_batch.last_timestamp, 0);
}

#[test]
fn test_stored_record_batch_creation() {
    let records = vec![
        StoredRecord {
            seq_id: "seq-1".to_string(),
            timestamp: 1000,
            headers: vec![],
            body: b"data 1".to_vec(),
        },
        StoredRecord {
            seq_id: "seq-2".to_string(),
            timestamp: 2000,
            headers: vec![],
            body: b"data 2".to_vec(),
        },
    ];

    let batch = StoredRecordBatch {
        batch_id: "test-batch".to_string(),
        bucket: "test-bucket".to_string(),
        stream: "test-stream".to_string(),
        records: records.clone(),
        created_at: 1234567890,
        first_seq_id: "seq-1".to_string(),
        last_seq_id: "seq-2".to_string(),
        first_timestamp: 1000,
        last_timestamp: 2000,
    };

    assert_eq!(batch.batch_id, "test-batch");
    assert_eq!(batch.bucket, "test-bucket");
    assert_eq!(batch.stream, "test-stream");
    assert_eq!(batch.records.len(), 2);
    assert_eq!(batch.created_at, 1234567890);
    assert_eq!(batch.first_seq_id, "seq-1");
    assert_eq!(batch.last_seq_id, "seq-2");
    assert_eq!(batch.first_timestamp, 1000);
    assert_eq!(batch.last_timestamp, 2000);
}

#[test]
fn test_batch_debug_format() {
    let config = BatchConfig::default();
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("BatchConfig"));
    assert!(debug_str.contains("max_records"));

    let batch = PendingBatch::new();
    let debug_str = format!("{:?}", batch);
    assert!(debug_str.contains("PendingBatch"));

    let stored_batch = StoredRecordBatch {
        batch_id: "test".to_string(),
        bucket: "bucket".to_string(),
        stream: "stream".to_string(),
        records: vec![],
        created_at: 0,
        first_seq_id: "".to_string(),
        last_seq_id: "".to_string(),
        first_timestamp: 0,
        last_timestamp: 0,
    };
    let debug_str = format!("{:?}", stored_batch);
    assert!(debug_str.contains("StoredRecordBatch"));
}

#[test]
fn test_batch_clone() {
    let config = BatchConfig::default();
    let cloned_config = config.clone();
    assert_eq!(config.max_records, cloned_config.max_records);

    let batch = PendingBatch::new();
    let cloned_batch = batch.clone();
    assert_eq!(batch.records.len(), cloned_batch.records.len());
    assert_eq!(batch.current_bytes, cloned_batch.current_bytes);
}

#[test]
fn test_serialization_compatibility() {
    let stored_batch = StoredRecordBatch {
        batch_id: "test-serialization".to_string(),
        bucket: "test-bucket".to_string(),
        stream: "test-stream".to_string(),
        records: vec![StoredRecord {
            seq_id: "seq-1".to_string(),
            timestamp: 1000,
            headers: vec![],
            body: b"test".to_vec(),
        }],
        created_at: 1234567890,
        first_seq_id: "seq-1".to_string(),
        last_seq_id: "seq-1".to_string(),
        first_timestamp: 1000,
        last_timestamp: 1000,
    };

    // Test that the struct can be serialized (should not panic)
    let json_result = serde_json::to_string(&stored_batch);
    assert!(json_result.is_ok());

    // Test deserialization
    let json_str = json_result.unwrap();
    let deserialized: Result<StoredRecordBatch, _> = serde_json::from_str(&json_str);
    assert!(deserialized.is_ok());

    let deserialized_batch = deserialized.unwrap();
    assert_eq!(deserialized_batch.batch_id, stored_batch.batch_id);
    assert_eq!(deserialized_batch.records.len(), stored_batch.records.len());
}
