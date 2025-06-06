use crate::common::error::Result;
use crate::common::metadata::{StoredRecord, current_timestamp_millis};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRecordBatch {
    pub batch_id: String,
    pub bucket: String,
    pub stream: String,
    pub records: Vec<StoredRecord>,
    pub created_at: u64,
    pub first_seq_id: String,
    pub last_seq_id: String,
    pub first_timestamp: u64,
    pub last_timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_records: usize,
    pub max_bytes: usize,
    pub flush_interval_ms: u64,
    pub max_batches_in_memory: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_records: 1000,
            max_bytes: 1024 * 1024,  // 1MB
            flush_interval_ms: 5000, // 5 seconds
            max_batches_in_memory: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PendingBatch {
    pub records: Vec<StoredRecord>,
    pub current_bytes: usize,
    pub created_at: SystemTime,
    pub first_seq_id: Option<String>,
    pub last_seq_id: Option<String>,
    pub first_timestamp: Option<u64>,
    pub last_timestamp: Option<u64>,
}

impl Default for PendingBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingBatch {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            current_bytes: 0,
            created_at: SystemTime::now(),
            first_seq_id: None,
            last_seq_id: None,
            first_timestamp: None,
            last_timestamp: None,
        }
    }

    pub fn add_record(&mut self, record: StoredRecord) -> Result<()> {
        // Calculate size using protobuf encoding
        let proto_record: crate::proto::StoredRecord = (&record).into();
        let serialized_size = proto_record.encoded_len();

        if self.first_seq_id.is_none() {
            self.first_seq_id = Some(record.seq_id.clone());
            self.first_timestamp = Some(record.timestamp);
        }

        self.last_seq_id = Some(record.seq_id.clone());
        self.last_timestamp = Some(record.timestamp);
        self.current_bytes += serialized_size;
        self.records.push(record);

        Ok(())
    }

    pub fn should_flush(&self, config: &BatchConfig) -> bool {
        self.records.len() >= config.max_records
            || self.current_bytes >= config.max_bytes
            || self
                .created_at
                .elapsed()
                .unwrap_or(Duration::ZERO)
                .as_millis()
                >= config.flush_interval_ms as u128
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn to_stored_batch(
        &self,
        batch_id: String,
        bucket: String,
        stream: String,
    ) -> StoredRecordBatch {
        StoredRecordBatch {
            batch_id,
            bucket,
            stream,
            records: self.records.clone(),
            created_at: current_timestamp_millis(),
            first_seq_id: self.first_seq_id.clone().unwrap_or_default(),
            last_seq_id: self.last_seq_id.clone().unwrap_or_default(),
            first_timestamp: self.first_timestamp.unwrap_or(0),
            last_timestamp: self.last_timestamp.unwrap_or(0),
        }
    }
}
