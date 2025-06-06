use crate::common::batch::StoredRecordBatch;
use crate::common::metadata::{SerializableHeader, StoredRecord};
use crate::proto::*;

/// Trait for automatic bidirectional conversion between internal types and protobuf types
pub trait ProtobufConvert<T> {
    fn to_proto(&self) -> T;
    fn from_proto(proto: &T) -> Self;
}

/// Macro to generate bidirectional conversion implementations
macro_rules! impl_protobuf_convert {
    ($internal:ty, $proto:ty, $to_proto:expr, $from_proto:expr) => {
        impl ProtobufConvert<$proto> for $internal {
            fn to_proto(&self) -> $proto {
                $to_proto(self)
            }

            fn from_proto(proto: &$proto) -> Self {
                $from_proto(proto)
            }
        }

        impl From<&$internal> for $proto {
            fn from(internal: &$internal) -> Self {
                internal.to_proto()
            }
        }

        impl From<&$proto> for $internal {
            fn from(proto: &$proto) -> Self {
                <$internal>::from_proto(proto)
            }
        }
    };
}

// Implement conversions for StoredRecord
impl_protobuf_convert!(
    StoredRecord,
    crate::proto::StoredRecord,
    |record: &StoredRecord| {
        crate::proto::StoredRecord {
            seq_id: record.seq_id.clone(),
            timestamp: record.timestamp,
            headers: record
                .headers
                .iter()
                .map(|h| Header {
                    name: h.name.clone(),
                    value: h.value.clone(),
                })
                .collect(),
            body: record.body.clone(),
        }
    },
    |proto_record: &crate::proto::StoredRecord| {
        StoredRecord {
            seq_id: proto_record.seq_id.clone(),
            timestamp: proto_record.timestamp,
            headers: proto_record
                .headers
                .iter()
                .map(|h| SerializableHeader {
                    name: h.name.clone(),
                    value: h.value.clone(),
                })
                .collect(),
            body: proto_record.body.clone(),
        }
    }
);

// Implement conversions for StoredRecordBatch
impl_protobuf_convert!(
    StoredRecordBatch,
    crate::proto::StoredRecordBatch,
    |batch: &StoredRecordBatch| {
        crate::proto::StoredRecordBatch {
            batch_id: batch.batch_id.clone(),
            bucket: batch.bucket.clone(),
            stream: batch.stream.clone(),
            records: batch.records.iter().map(|r| r.to_proto()).collect(),
            created_at: batch.created_at,
            first_seq_id: batch.first_seq_id.clone(),
            last_seq_id: batch.last_seq_id.clone(),
            first_timestamp: batch.first_timestamp,
            last_timestamp: batch.last_timestamp,
        }
    },
    |proto_batch: &crate::proto::StoredRecordBatch| {
        StoredRecordBatch {
            batch_id: proto_batch.batch_id.clone(),
            bucket: proto_batch.bucket.clone(),
            stream: proto_batch.stream.clone(),
            records: proto_batch
                .records
                .iter()
                .map(StoredRecord::from_proto)
                .collect(),
            created_at: proto_batch.created_at,
            first_seq_id: proto_batch.first_seq_id.clone(),
            last_seq_id: proto_batch.last_seq_id.clone(),
            first_timestamp: proto_batch.first_timestamp,
            last_timestamp: proto_batch.last_timestamp,
        }
    }
);

/// Helper function to convert collections of items
pub fn convert_vec_to_proto<T, P>(items: &[T]) -> Vec<P>
where
    T: ProtobufConvert<P>,
{
    items.iter().map(|item| item.to_proto()).collect()
}

/// Helper function to convert collections from protobuf
pub fn convert_vec_from_proto<T, P>(protos: &[P]) -> Vec<T>
where
    T: ProtobufConvert<P>,
{
    protos.iter().map(|proto| T::from_proto(proto)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metadata::SerializableHeader;

    #[test]
    fn test_stored_record_conversion() {
        let original = StoredRecord {
            seq_id: "test-seq-id".to_string(),
            timestamp: 12345,
            headers: vec![SerializableHeader {
                name: b"content-type".to_vec(),
                value: b"application/json".to_vec(),
            }],
            body: b"test body".to_vec(),
        };

        // Convert to proto and back
        let proto = original.to_proto();
        let converted = StoredRecord::from_proto(&proto);

        assert_eq!(original.seq_id, converted.seq_id);
        assert_eq!(original.timestamp, converted.timestamp);
        assert_eq!(original.headers.len(), converted.headers.len());
        assert_eq!(original.body, converted.body);
    }

    #[test]
    fn test_from_trait_conversion() {
        let original = StoredRecord {
            seq_id: "test-seq-id".to_string(),
            timestamp: 12345,
            headers: vec![],
            body: b"test body".to_vec(),
        };

        // Test From trait implementations
        let proto: crate::proto::StoredRecord = (&original).into();
        let converted: StoredRecord = (&proto).into();

        assert_eq!(original.seq_id, converted.seq_id);
        assert_eq!(original.timestamp, converted.timestamp);
        assert_eq!(original.body, converted.body);
    }
}
