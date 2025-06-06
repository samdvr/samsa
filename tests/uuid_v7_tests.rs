use object_store::memory::InMemory;
use samsa::common::storage::Storage;
use samsa::proto::*;
use std::sync::Arc;
use uuid::Uuid;

/// Test UUID v7 pagination to ensure no infinite loops or missed records
#[tokio::test]
async fn test_uuid_v7_pagination() {
    let object_store = Arc::new(InMemory::new());
    let storage = Storage::new_with_memory(object_store, "test-node".to_string());

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

    // Append multiple records with UUID v7
    let records = vec![
        AppendRecord {
            timestamp: Some(1000),
            headers: vec![],
            body: b"record 1".to_vec(),
        },
        AppendRecord {
            timestamp: Some(2000),
            headers: vec![],
            body: b"record 2".to_vec(),
        },
        AppendRecord {
            timestamp: Some(3000),
            headers: vec![],
            body: b"record 3".to_vec(),
        },
    ];

    storage
        .append_records(bucket, stream, records)
        .await
        .unwrap();

    // Test pagination with read_records_with_pagination
    let (first_batch, next_seq_id) = storage
        .read_records_with_pagination(bucket, stream, &Uuid::nil().to_string(), Some(2))
        .await
        .unwrap();

    assert_eq!(first_batch.len(), 2);
    assert!(next_seq_id.is_some());

    // Read next batch using the returned next_seq_id
    if let Some(next_id) = next_seq_id {
        let (second_batch, _) = storage
            .read_records_with_pagination(bucket, stream, &next_id, Some(2))
            .await
            .unwrap();

        // Should get the remaining record(s)
        assert!(!second_batch.is_empty());

        // Ensure no duplicate records between batches
        for first_record in &first_batch {
            for second_record in &second_batch {
                assert_ne!(first_record.seq_id, second_record.seq_id);
            }
        }
    }
}

/// Test timestamp-based range queries
#[tokio::test]
async fn test_timestamp_range_queries() {
    let object_store = Arc::new(InMemory::new());
    let storage = Storage::new_with_memory(object_store, "test-node".to_string());

    let bucket = "test-bucket";
    let stream = "test-stream";

    // Create bucket
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

    // Append records with specific timestamps
    let records = vec![
        AppendRecord {
            timestamp: Some(1000),
            headers: vec![],
            body: b"record at 1000".to_vec(),
        },
        AppendRecord {
            timestamp: Some(2000),
            headers: vec![],
            body: b"record at 2000".to_vec(),
        },
        AppendRecord {
            timestamp: Some(3000),
            headers: vec![],
            body: b"record at 3000".to_vec(),
        },
        AppendRecord {
            timestamp: Some(4000),
            headers: vec![],
            body: b"record at 4000".to_vec(),
        },
    ];

    storage
        .append_records(bucket, stream, records)
        .await
        .unwrap();

    // Test timestamp-based reads
    let from_2000 = storage
        .read_records_from_timestamp(bucket, stream, 2000, None)
        .await
        .unwrap();

    assert_eq!(from_2000.len(), 3); // Should get records at 2000, 3000, 4000
    assert!(from_2000.iter().all(|r| r.timestamp >= 2000));

    // Test timestamp range queries
    let range_2000_3000 = storage
        .read_records_in_timestamp_range(bucket, stream, 2000, 3000, None)
        .await
        .unwrap();

    assert_eq!(range_2000_3000.len(), 2); // Should get records at 2000, 3000
    assert!(
        range_2000_3000
            .iter()
            .all(|r| r.timestamp >= 2000 && r.timestamp <= 3000)
    );
}

/// Test stream statistics functionality
#[tokio::test]
async fn test_stream_statistics() {
    let object_store = Arc::new(InMemory::new());
    let storage = Storage::new_with_memory(object_store, "test-node".to_string());

    let bucket = "test-bucket";
    let stream = "test-stream";

    // Create bucket
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

    // Test empty stream stats
    let empty_stats = storage.get_stream_stats(bucket, stream).await.unwrap();
    assert_eq!(empty_stats.total_records, 0);

    // Append some records
    let records = vec![
        AppendRecord {
            timestamp: Some(1000),
            headers: vec![],
            body: b"first record".to_vec(),
        },
        AppendRecord {
            timestamp: Some(5000),
            headers: vec![],
            body: b"last record".to_vec(),
        },
    ];

    storage
        .append_records(bucket, stream, records)
        .await
        .unwrap();

    // Test populated stream stats
    let stats = storage.get_stream_stats(bucket, stream).await.unwrap();
    assert_eq!(stats.total_records, 2);
    assert_eq!(stats.earliest_timestamp, 1000);
    assert_eq!(stats.latest_timestamp, 5000);
    assert!(!stats.earliest_seq_id.is_empty());
    assert!(!stats.latest_seq_id.is_empty());
    assert_ne!(stats.earliest_seq_id, stats.latest_seq_id);
}

/// Test that UUID v7 ordering is preserved correctly
#[tokio::test]
async fn test_uuid_v7_ordering() {
    let object_store = Arc::new(InMemory::new());
    let storage = Storage::new_with_memory(object_store, "test-node".to_string());

    let bucket = "test-bucket";
    let stream = "test-stream";

    // Create bucket
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

    // Append records in separate calls to ensure different UUID v7 timestamps
    for i in 0..5 {
        let records = vec![AppendRecord {
            timestamp: Some(1000 + i * 1000),
            headers: vec![],
            body: format!("record {}", i).into_bytes(),
        }];

        storage
            .append_records(bucket, stream, records)
            .await
            .unwrap();

        // Small delay to ensure different UUID v7 timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    }

    // Read all records
    let all_records = storage
        .read_records(bucket, stream, &Uuid::nil().to_string(), None)
        .await
        .unwrap();

    assert_eq!(all_records.len(), 5);

    // Verify records are ordered by sequence ID (UUID v7 natural ordering)
    for i in 1..all_records.len() {
        assert!(all_records[i - 1].seq_id < all_records[i].seq_id);
    }

    // Verify records are also ordered by timestamp (should match UUID v7 ordering)
    for i in 1..all_records.len() {
        assert!(all_records[i - 1].timestamp <= all_records[i].timestamp);
    }
}

/// Test pagination robustness with various UUID v7 edge cases
#[tokio::test]
async fn test_uuid_v7_pagination_edge_cases() {
    let object_store = Arc::new(InMemory::new());
    let storage = Storage::new_with_memory(object_store, "test-node".to_string());

    let bucket = "test-bucket";
    let stream = "test-stream";

    // Create bucket
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

    // Add a larger number of records to test edge cases thoroughly
    let num_records = 50;
    for i in 0..num_records {
        let records = vec![AppendRecord {
            timestamp: Some(1000 + i * 100),
            headers: vec![],
            body: format!("edge_case_record_{}", i).into_bytes(),
        }];

        storage
            .append_records(bucket, stream, records)
            .await
            .unwrap();

        // Small delay to ensure UUID v7 timestamp differences
        if i % 10 == 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
    }

    // Test 1: Paginate through all records in small pages
    let page_size = 7; // Intentionally odd page size to test edge cases
    let mut all_paginated_records = Vec::new();
    let mut current_start_after = Uuid::nil().to_string();

    loop {
        let (page_records, next_seq_id) = storage
            .read_records_with_pagination(bucket, stream, &current_start_after, Some(page_size))
            .await
            .unwrap();

        if page_records.is_empty() {
            break;
        }

        // Verify no record is duplicated across pages
        for page_record in &page_records {
            assert!(
                !all_paginated_records.iter().any(
                    |existing: &samsa::common::metadata::StoredRecord| existing.seq_id
                        == page_record.seq_id
                ),
                "Duplicate record found during pagination: {}",
                page_record.seq_id
            );
        }

        all_paginated_records.extend(page_records);

        if let Some(next_id) = next_seq_id {
            current_start_after = next_id;
        } else {
            break;
        }
    }

    // Verify we got all records
    assert_eq!(all_paginated_records.len(), num_records as usize);

    // Test 2: Verify records are in correct order across pagination
    for i in 1..all_paginated_records.len() {
        assert!(
            all_paginated_records[i - 1].seq_id < all_paginated_records[i].seq_id,
            "Records not properly ordered: {} should be < {}",
            all_paginated_records[i - 1].seq_id,
            all_paginated_records[i].seq_id
        );
    }

    // Test 3: Compare with non-paginated read to ensure consistency
    let all_records_direct = storage
        .read_records(bucket, stream, &Uuid::nil().to_string(), None)
        .await
        .unwrap();

    assert_eq!(all_paginated_records.len(), all_records_direct.len());

    for (paginated, direct) in all_paginated_records.iter().zip(all_records_direct.iter()) {
        assert_eq!(paginated.seq_id, direct.seq_id);
        assert_eq!(paginated.body, direct.body);
    }

    println!(
        "✅ UUID v7 pagination edge cases test passed with {} records",
        num_records
    );
}

/// Test exclusive start behavior to ensure no off-by-one errors
#[tokio::test]
async fn test_exclusive_start_correctness() {
    let object_store = Arc::new(InMemory::new());
    let storage = Storage::new_with_memory(object_store, "test-node".to_string());

    let bucket = "test-bucket";
    let stream = "test-stream";

    // Create bucket
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

    // Add exactly 3 records for precise testing
    let records = vec![
        AppendRecord {
            timestamp: Some(1000),
            headers: vec![],
            body: b"record_A".to_vec(),
        },
        AppendRecord {
            timestamp: Some(2000),
            headers: vec![],
            body: b"record_B".to_vec(),
        },
        AppendRecord {
            timestamp: Some(3000),
            headers: vec![],
            body: b"record_C".to_vec(),
        },
    ];

    storage
        .append_records(bucket, stream, records)
        .await
        .unwrap();

    // Get all records to know their exact seq_ids
    let all_records = storage
        .read_records(bucket, stream, &Uuid::nil().to_string(), None)
        .await
        .unwrap();

    assert_eq!(all_records.len(), 3);
    let record_a_seq_id = &all_records[0].seq_id;
    let record_b_seq_id = &all_records[1].seq_id;

    // Test exclusive start: reading after record A should start with record B
    let (after_a_records, _) = storage
        .read_records_with_pagination(bucket, stream, record_a_seq_id, None)
        .await
        .unwrap();

    assert_eq!(after_a_records.len(), 2); // Should get B and C
    assert_eq!(after_a_records[0].seq_id, *record_b_seq_id);
    assert_eq!(after_a_records[0].body, b"record_B");
    assert_eq!(after_a_records[1].body, b"record_C");

    // Test exclusive start: reading after record B should start with record C
    let (after_b_records, _) = storage
        .read_records_with_pagination(bucket, stream, record_b_seq_id, None)
        .await
        .unwrap();

    assert_eq!(after_b_records.len(), 1); // Should get only C
    assert_eq!(after_b_records[0].body, b"record_C");

    // Test that pagination token from first page works correctly
    let (first_page, next_token) = storage
        .read_records_with_pagination(bucket, stream, &Uuid::nil().to_string(), Some(1))
        .await
        .unwrap();

    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].body, b"record_A");
    assert!(next_token.is_some());

    // Use the pagination token to get the next page
    let (second_page, _) = storage
        .read_records_with_pagination(bucket, stream, &next_token.unwrap(), Some(1))
        .await
        .unwrap();

    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].body, b"record_B");
    // Should NOT include record_A again

    println!("✅ Exclusive start correctness test passed");
}

/// Test UUID v7 string comparison robustness
#[tokio::test]
async fn test_uuid_v7_string_comparison_robustness() {
    let object_store = Arc::new(InMemory::new());
    let storage = Storage::new_with_memory(object_store, "test-node".to_string());

    let bucket = "test-bucket";
    let stream = "test-stream";

    // Create bucket
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

    // Add records at different times to get diverse UUID v7 values
    let mut expected_order = Vec::new();

    for i in 0..10 {
        let body = format!("time_record_{}", i).into_bytes();
        let records = vec![AppendRecord {
            timestamp: Some(1000 + i * 1000),
            headers: vec![],
            body: body.clone(),
        }];

        storage
            .append_records(bucket, stream, records)
            .await
            .unwrap();

        expected_order.push(body);

        // Ensure different timestamp components in UUID v7
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Read all records to verify UUID v7 string comparison ordering
    let all_records = storage
        .read_records(bucket, stream, &Uuid::nil().to_string(), None)
        .await
        .unwrap();

    assert_eq!(all_records.len(), 10);

    // Verify that UUID v7 string comparison preserves chronological order
    for (i, record) in all_records.iter().enumerate() {
        assert_eq!(record.body, expected_order[i]);
    }

    // Test pagination with various starting points
    for start_idx in 0..all_records.len() {
        let start_seq_id = &all_records[start_idx].seq_id;

        let (page_records, _) = storage
            .read_records_with_pagination(bucket, stream, start_seq_id, Some(3))
            .await
            .unwrap();

        // Should get records after the start_idx (exclusive start)
        let expected_count = std::cmp::min(3, all_records.len() - start_idx - 1);
        assert_eq!(page_records.len(), expected_count);

        if expected_count > 0 {
            // Verify first returned record is indeed the next one
            assert_eq!(page_records[0].seq_id, all_records[start_idx + 1].seq_id);
        }
    }

    println!("✅ UUID v7 string comparison robustness test passed");
}

/// Test that no UUID arithmetic is being performed (regression test)
#[tokio::test]
async fn test_no_uuid_arithmetic_regression() {
    let object_store = Arc::new(InMemory::new());
    let storage = Storage::new_with_memory(object_store, "test-node".to_string());

    let bucket = "test-bucket";
    let stream = "test-stream";

    // Create bucket
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

    // Add a single record to get its exact seq_id
    let records = vec![AppendRecord {
        timestamp: Some(1000),
        headers: vec![],
        body: b"single_record".to_vec(),
    }];

    storage
        .append_records(bucket, stream, records)
        .await
        .unwrap();

    let all_records = storage
        .read_records(bucket, stream, &Uuid::nil().to_string(), None)
        .await
        .unwrap();

    assert_eq!(all_records.len(), 1);
    let actual_seq_id = &all_records[0].seq_id;

    // Test pagination using the exact seq_id from the record
    // This verifies that we're using the actual seq_id, not a manipulated version
    let (next_page, _) = storage
        .read_records_with_pagination(bucket, stream, actual_seq_id, None)
        .await
        .unwrap();

    // Should get 0 records because we're reading after the only record
    assert_eq!(next_page.len(), 0);

    // Test that we can read the record itself using inclusive start (fromSeqId)
    let from_seq_records = storage
        .read_records(bucket, stream, actual_seq_id, None)
        .await
        .unwrap();

    assert_eq!(from_seq_records.len(), 1);
    assert_eq!(from_seq_records[0].seq_id, *actual_seq_id);

    // Verify the seq_id is a valid UUID v7 (no corruption from arithmetic)
    let parsed_uuid = Uuid::parse_str(actual_seq_id);
    assert!(
        parsed_uuid.is_ok(),
        "seq_id should be a valid UUID: {}",
        actual_seq_id
    );

    // Verify it's specifically a UUID v7 (version field should be 7)
    let uuid = parsed_uuid.unwrap();
    assert_eq!(uuid.get_version_num(), 7, "Should be UUID v7");

    println!("✅ No UUID arithmetic regression test passed");
    println!("    Verified seq_id is valid UUID v7: {}", actual_seq_id);
}
