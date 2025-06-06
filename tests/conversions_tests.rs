//! Comprehensive unit tests for the conversions module
//!
//! Tests protobuf conversions, trait implementations, and helper functions

use samsa::common::batch::StoredRecordBatch;
use samsa::common::conversions::{ProtobufConvert, convert_vec_from_proto, convert_vec_to_proto};
use samsa::common::metadata::{SerializableHeader, StoredRecord};
use samsa::proto;

#[test]
fn test_stored_record_to_proto() {
    let record = StoredRecord {
        seq_id: "test-seq-123".to_string(),
        timestamp: 1234567890,
        headers: vec![
            SerializableHeader {
                name: b"content-type".to_vec(),
                value: b"application/json".to_vec(),
            },
            SerializableHeader {
                name: b"authorization".to_vec(),
                value: b"Bearer token123".to_vec(),
            },
        ],
        body: b"test message body".to_vec(),
    };

    let proto_record = record.to_proto();

    assert_eq!(proto_record.seq_id, "test-seq-123");
    assert_eq!(proto_record.timestamp, 1234567890);
    assert_eq!(proto_record.headers.len(), 2);
    assert_eq!(proto_record.headers[0].name, b"content-type");
    assert_eq!(proto_record.headers[0].value, b"application/json");
    assert_eq!(proto_record.headers[1].name, b"authorization");
    assert_eq!(proto_record.headers[1].value, b"Bearer token123");
    assert_eq!(proto_record.body, b"test message body");
}

#[test]
fn test_stored_record_from_proto() {
    let proto_record = proto::StoredRecord {
        seq_id: "proto-seq-456".to_string(),
        timestamp: 9876543210,
        headers: vec![proto::Header {
            name: b"user-agent".to_vec(),
            value: b"test-client/1.0".to_vec(),
        }],
        body: b"proto message body".to_vec(),
    };

    let record = StoredRecord::from_proto(&proto_record);

    assert_eq!(record.seq_id, "proto-seq-456");
    assert_eq!(record.timestamp, 9876543210);
    assert_eq!(record.headers.len(), 1);
    assert_eq!(record.headers[0].name, b"user-agent");
    assert_eq!(record.headers[0].value, b"test-client/1.0");
    assert_eq!(record.body, b"proto message body");
}

#[test]
fn test_stored_record_bidirectional_conversion() {
    let original = StoredRecord {
        seq_id: "bidirectional-test".to_string(),
        timestamp: 1111111111,
        headers: vec![
            SerializableHeader {
                name: b"header1".to_vec(),
                value: b"value1".to_vec(),
            },
            SerializableHeader {
                name: b"header2".to_vec(),
                value: b"value2".to_vec(),
            },
        ],
        body: b"bidirectional test data".to_vec(),
    };

    // Convert to proto and back
    let proto = original.to_proto();
    let converted = StoredRecord::from_proto(&proto);

    assert_eq!(original.seq_id, converted.seq_id);
    assert_eq!(original.timestamp, converted.timestamp);
    assert_eq!(original.headers.len(), converted.headers.len());
    for (orig_header, conv_header) in original.headers.iter().zip(converted.headers.iter()) {
        assert_eq!(orig_header.name, conv_header.name);
        assert_eq!(orig_header.value, conv_header.value);
    }
    assert_eq!(original.body, converted.body);
}

#[test]
fn test_stored_record_empty_headers() {
    let record = StoredRecord {
        seq_id: "empty-headers".to_string(),
        timestamp: 0,
        headers: vec![],
        body: b"no headers".to_vec(),
    };

    let proto = record.to_proto();
    assert!(proto.headers.is_empty());

    let converted = StoredRecord::from_proto(&proto);
    assert!(converted.headers.is_empty());
    assert_eq!(converted.seq_id, "empty-headers");
    assert_eq!(converted.body, b"no headers");
}

#[test]
fn test_stored_record_empty_body() {
    let record = StoredRecord {
        seq_id: "empty-body".to_string(),
        timestamp: 123,
        headers: vec![SerializableHeader {
            name: b"test".to_vec(),
            value: b"value".to_vec(),
        }],
        body: vec![],
    };

    let proto = record.to_proto();
    assert!(proto.body.is_empty());

    let converted = StoredRecord::from_proto(&proto);
    assert!(converted.body.is_empty());
    assert_eq!(converted.headers.len(), 1);
}

#[test]
fn test_stored_record_batch_to_proto() {
    let records = vec![
        StoredRecord {
            seq_id: "seq-1".to_string(),
            timestamp: 1000,
            headers: vec![],
            body: b"record 1".to_vec(),
        },
        StoredRecord {
            seq_id: "seq-2".to_string(),
            timestamp: 2000,
            headers: vec![],
            body: b"record 2".to_vec(),
        },
    ];

    let batch = StoredRecordBatch {
        batch_id: "batch-123".to_string(),
        bucket: "test-bucket".to_string(),
        stream: "test-stream".to_string(),
        records: records.clone(),
        created_at: 1234567890,
        first_seq_id: "seq-1".to_string(),
        last_seq_id: "seq-2".to_string(),
        first_timestamp: 1000,
        last_timestamp: 2000,
    };

    let proto_batch = batch.to_proto();

    assert_eq!(proto_batch.batch_id, "batch-123");
    assert_eq!(proto_batch.bucket, "test-bucket");
    assert_eq!(proto_batch.stream, "test-stream");
    assert_eq!(proto_batch.records.len(), 2);
    assert_eq!(proto_batch.created_at, 1234567890);
    assert_eq!(proto_batch.first_seq_id, "seq-1");
    assert_eq!(proto_batch.last_seq_id, "seq-2");
    assert_eq!(proto_batch.first_timestamp, 1000);
    assert_eq!(proto_batch.last_timestamp, 2000);
}

#[test]
fn test_stored_record_batch_from_proto() {
    let proto_batch = proto::StoredRecordBatch {
        batch_id: "proto-batch".to_string(),
        bucket: "proto-bucket".to_string(),
        stream: "proto-stream".to_string(),
        records: vec![proto::StoredRecord {
            seq_id: "proto-seq-1".to_string(),
            timestamp: 3000,
            headers: vec![],
            body: b"proto record".to_vec(),
        }],
        created_at: 9876543210,
        first_seq_id: "proto-seq-1".to_string(),
        last_seq_id: "proto-seq-1".to_string(),
        first_timestamp: 3000,
        last_timestamp: 3000,
    };

    let batch = StoredRecordBatch::from_proto(&proto_batch);

    assert_eq!(batch.batch_id, "proto-batch");
    assert_eq!(batch.bucket, "proto-bucket");
    assert_eq!(batch.stream, "proto-stream");
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.created_at, 9876543210);
    assert_eq!(batch.first_seq_id, "proto-seq-1");
    assert_eq!(batch.last_seq_id, "proto-seq-1");
    assert_eq!(batch.first_timestamp, 3000);
    assert_eq!(batch.last_timestamp, 3000);
}

#[test]
fn test_stored_record_batch_bidirectional_conversion() {
    let original_batch = StoredRecordBatch {
        batch_id: "bidirectional-batch".to_string(),
        bucket: "bidirectional-bucket".to_string(),
        stream: "bidirectional-stream".to_string(),
        records: vec![
            StoredRecord {
                seq_id: "batch-seq-1".to_string(),
                timestamp: 5000,
                headers: vec![SerializableHeader {
                    name: b"batch-header".to_vec(),
                    value: b"batch-value".to_vec(),
                }],
                body: b"batch record 1".to_vec(),
            },
            StoredRecord {
                seq_id: "batch-seq-2".to_string(),
                timestamp: 6000,
                headers: vec![],
                body: b"batch record 2".to_vec(),
            },
        ],
        created_at: 1111111111,
        first_seq_id: "batch-seq-1".to_string(),
        last_seq_id: "batch-seq-2".to_string(),
        first_timestamp: 5000,
        last_timestamp: 6000,
    };

    // Convert to proto and back
    let proto = original_batch.to_proto();
    let converted = StoredRecordBatch::from_proto(&proto);

    assert_eq!(original_batch.batch_id, converted.batch_id);
    assert_eq!(original_batch.bucket, converted.bucket);
    assert_eq!(original_batch.stream, converted.stream);
    assert_eq!(original_batch.records.len(), converted.records.len());
    assert_eq!(original_batch.created_at, converted.created_at);
    assert_eq!(original_batch.first_seq_id, converted.first_seq_id);
    assert_eq!(original_batch.last_seq_id, converted.last_seq_id);
    assert_eq!(original_batch.first_timestamp, converted.first_timestamp);
    assert_eq!(original_batch.last_timestamp, converted.last_timestamp);

    // Check individual records
    for (orig_record, conv_record) in original_batch.records.iter().zip(converted.records.iter()) {
        assert_eq!(orig_record.seq_id, conv_record.seq_id);
        assert_eq!(orig_record.timestamp, conv_record.timestamp);
        assert_eq!(orig_record.body, conv_record.body);
        assert_eq!(orig_record.headers.len(), conv_record.headers.len());
    }
}

#[test]
fn test_stored_record_batch_empty_records() {
    let batch = StoredRecordBatch {
        batch_id: "empty-batch".to_string(),
        bucket: "empty-bucket".to_string(),
        stream: "empty-stream".to_string(),
        records: vec![],
        created_at: 0,
        first_seq_id: "".to_string(),
        last_seq_id: "".to_string(),
        first_timestamp: 0,
        last_timestamp: 0,
    };

    let proto = batch.to_proto();
    assert!(proto.records.is_empty());

    let converted = StoredRecordBatch::from_proto(&proto);
    assert!(converted.records.is_empty());
    assert_eq!(converted.batch_id, "empty-batch");
}

#[test]
fn test_from_trait_implementations() {
    let record = StoredRecord {
        seq_id: "from-trait-test".to_string(),
        timestamp: 777,
        headers: vec![],
        body: b"from trait test".to_vec(),
    };

    // Test From<&StoredRecord> for proto::StoredRecord
    let proto_record: proto::StoredRecord = (&record).into();
    assert_eq!(proto_record.seq_id, "from-trait-test");
    assert_eq!(proto_record.timestamp, 777);

    // Test From<&proto::StoredRecord> for StoredRecord
    let converted_record: StoredRecord = (&proto_record).into();
    assert_eq!(converted_record.seq_id, "from-trait-test");
    assert_eq!(converted_record.timestamp, 777);
}

#[test]
fn test_convert_vec_to_proto() {
    let records = vec![
        StoredRecord {
            seq_id: "vec-1".to_string(),
            timestamp: 100,
            headers: vec![],
            body: b"vec record 1".to_vec(),
        },
        StoredRecord {
            seq_id: "vec-2".to_string(),
            timestamp: 200,
            headers: vec![],
            body: b"vec record 2".to_vec(),
        },
    ];

    let proto_records: Vec<proto::StoredRecord> = convert_vec_to_proto(&records);

    assert_eq!(proto_records.len(), 2);
    assert_eq!(proto_records[0].seq_id, "vec-1");
    assert_eq!(proto_records[0].timestamp, 100);
    assert_eq!(proto_records[1].seq_id, "vec-2");
    assert_eq!(proto_records[1].timestamp, 200);
}

#[test]
fn test_convert_vec_from_proto() {
    let proto_records = vec![
        proto::StoredRecord {
            seq_id: "proto-vec-1".to_string(),
            timestamp: 300,
            headers: vec![],
            body: b"proto vec record 1".to_vec(),
        },
        proto::StoredRecord {
            seq_id: "proto-vec-2".to_string(),
            timestamp: 400,
            headers: vec![],
            body: b"proto vec record 2".to_vec(),
        },
    ];

    let records: Vec<StoredRecord> = convert_vec_from_proto(&proto_records);

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].seq_id, "proto-vec-1");
    assert_eq!(records[0].timestamp, 300);
    assert_eq!(records[1].seq_id, "proto-vec-2");
    assert_eq!(records[1].timestamp, 400);
}

#[test]
fn test_convert_vec_empty() {
    let empty_records: Vec<StoredRecord> = vec![];
    let proto_records: Vec<proto::StoredRecord> = convert_vec_to_proto(&empty_records);
    assert!(proto_records.is_empty());

    let empty_proto_records: Vec<proto::StoredRecord> = vec![];
    let records: Vec<StoredRecord> = convert_vec_from_proto(&empty_proto_records);
    assert!(records.is_empty());
}

#[test]
fn test_convert_vec_single_item() {
    let records = vec![StoredRecord {
        seq_id: "single".to_string(),
        timestamp: 999,
        headers: vec![],
        body: b"single record".to_vec(),
    }];

    let proto_records: Vec<proto::StoredRecord> = convert_vec_to_proto(&records);
    assert_eq!(proto_records.len(), 1);
    assert_eq!(proto_records[0].seq_id, "single");

    let converted_back: Vec<StoredRecord> = convert_vec_from_proto(&proto_records);
    assert_eq!(converted_back.len(), 1);
    assert_eq!(converted_back[0].seq_id, "single");
}

#[test]
fn test_header_conversion_edge_cases() {
    // Test with binary data in headers
    let record = StoredRecord {
        seq_id: "binary-headers".to_string(),
        timestamp: 0,
        headers: vec![SerializableHeader {
            name: vec![0x00, 0x01, 0x02, 0xFF],
            value: vec![0xFF, 0xFE, 0xFD, 0x00],
        }],
        body: vec![],
    };

    let proto = record.to_proto();
    let converted = StoredRecord::from_proto(&proto);

    assert_eq!(converted.headers.len(), 1);
    assert_eq!(converted.headers[0].name, vec![0x00, 0x01, 0x02, 0xFF]);
    assert_eq!(converted.headers[0].value, vec![0xFF, 0xFE, 0xFD, 0x00]);
}

#[test]
fn test_large_data_conversion() {
    // Test with large body data
    let large_body = vec![b'x'; 10000]; // 10KB of data
    let record = StoredRecord {
        seq_id: "large-data".to_string(),
        timestamp: 12345,
        headers: vec![],
        body: large_body.clone(),
    };

    let proto = record.to_proto();
    let converted = StoredRecord::from_proto(&proto);

    assert_eq!(converted.body.len(), 10000);
    assert_eq!(converted.body, large_body);
}

#[test]
fn test_unicode_data_conversion() {
    // Test with unicode data in seq_id
    let record = StoredRecord {
        seq_id: "unicode-测试-🚀-データ".to_string(),
        timestamp: 42,
        headers: vec![SerializableHeader {
            name: "unicode-header-тест".as_bytes().to_vec(),
            value: "unicode-value-🌍-世界".as_bytes().to_vec(),
        }],
        body: "unicode-body-こんにちは-мир".as_bytes().to_vec(),
    };

    let proto = record.to_proto();
    let converted = StoredRecord::from_proto(&proto);

    assert_eq!(converted.seq_id, "unicode-测试-🚀-データ");
    assert_eq!(
        String::from_utf8_lossy(&converted.headers[0].name),
        "unicode-header-тест"
    );
    assert_eq!(
        String::from_utf8_lossy(&converted.headers[0].value),
        "unicode-value-🌍-世界"
    );
    assert_eq!(
        String::from_utf8_lossy(&converted.body),
        "unicode-body-こんにちは-мир"
    );
}
