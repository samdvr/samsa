use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{Instrument, info_span};
use uuid::Uuid;

use crate::common::Storage;
use crate::common::metrics::{Timer, labels, names, utils};
use crate::common::storage::ReadFilter;
use crate::proto::*;

pub struct StreamHandler {
    storage: Arc<Storage>,
}

impl StreamHandler {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    /// Calculate the starting sequence ID for a tail offset read
    ///
    /// This is an approximation due to the distributed nature of the storage system
    /// and the use of UUID v7 sequence IDs.
    async fn calculate_tail_offset_start(
        &self,
        bucket: &str,
        stream: &str,
        offset: u64,
    ) -> Result<String, crate::common::error::SamsaError> {
        if offset == 0 {
            // Start from current tail
            let (tail_seq_id, _) = self.storage.get_stream_tail(bucket, stream).await?;
            Ok(tail_seq_id)
        } else {
            // For non-zero offset, we approximate by reading recent records
            // Note: This is an approximation and may not be exact in distributed scenarios
            // where records are written by multiple nodes concurrently
            let recent_records = self
                .storage
                .read_records(bucket, stream, &Uuid::nil().to_string(), None)
                .await?;

            if recent_records.len() > offset as usize {
                let start_idx = recent_records.len() - offset as usize;
                Ok(recent_records[start_idx].seq_id.clone())
            } else {
                // If we don't have enough records, start from the beginning
                Ok(Uuid::nil().to_string())
            }
        }
    }
}

#[tonic::async_trait]
impl stream_service_server::StreamService for StreamHandler {
    async fn check_tail(
        &self,
        request: Request<CheckTailRequest>,
    ) -> std::result::Result<Response<CheckTailResponse>, Status> {
        let req = request.into_inner();

        let (next_seq_id, last_timestamp) = self
            .storage
            .get_stream_tail(&req.bucket, &req.stream)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(CheckTailResponse {
            next_seq_id,
            last_timestamp,
            error: None,
        }))
    }

    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> std::result::Result<Response<AppendResponse>, Status> {
        let timer = Timer::new(names::GRPC_REQUEST_DURATION).with_label(labels::METHOD, "append");

        // Record gRPC request
        utils::record_counter(
            names::GRPC_REQUESTS_TOTAL,
            1,
            vec![(labels::METHOD.to_string(), "append".to_string())],
        );

        let req = request.into_inner();

        // Create tracing span for distributed tracing
        let span = info_span!(
            "grpc_append",
            method = "append",
            bucket = %req.bucket,
            stream = %req.stream,
            record_count = req.records.len(),
            otel.name = "grpc.append"
        );

        async move {
            if req.records.is_empty() {
                utils::record_counter(
                    names::GRPC_ERRORS_TOTAL,
                    1,
                    vec![
                        (labels::METHOD.to_string(), "append".to_string()),
                        (
                            labels::ERROR_TYPE.to_string(),
                            "invalid_argument".to_string(),
                        ),
                    ],
                );
                return Err(Status::invalid_argument("No records to append"));
            }

            let total_bytes: usize = req.records.iter().map(|record| record.body.len()).sum();
            let record_count = req.records.len();

            let result = self
                .storage
                .append_records(&req.bucket, &req.stream, req.records)
                .await;

            match result {
                Ok((start_seq_id, end_seq_id, start_timestamp, end_timestamp)) => {
                    let tail_result = self.storage.get_stream_tail(&req.bucket, &req.stream).await;

                    match tail_result {
                        Ok((next_seq_id, last_timestamp)) => {
                            timer.record();
                            tracing::info!(
                                "Successfully appended {} records ({} bytes) to {}/{}",
                                record_count,
                                total_bytes,
                                req.bucket,
                                req.stream
                            );

                            Ok(Response::new(AppendResponse {
                                start_seq_id,
                                end_seq_id,
                                start_timestamp,
                                end_timestamp,
                                next_seq_id,
                                last_timestamp,
                                error: None,
                            }))
                        }
                        Err(e) => {
                            utils::record_counter(
                                names::GRPC_ERRORS_TOTAL,
                                1,
                                vec![
                                    (labels::METHOD.to_string(), "append".to_string()),
                                    (labels::ERROR_TYPE.to_string(), "tail_error".to_string()),
                                ],
                            );
                            Err(Status::from(e))
                        }
                    }
                }
                Err(e) => {
                    utils::record_counter(
                        names::GRPC_ERRORS_TOTAL,
                        1,
                        vec![
                            (labels::METHOD.to_string(), "append".to_string()),
                            (labels::ERROR_TYPE.to_string(), "append_error".to_string()),
                        ],
                    );
                    tracing::error!(
                        "Failed to append records to {}/{}: {}",
                        req.bucket,
                        req.stream,
                        e
                    );
                    Err(Status::from(e))
                }
            }
        }
        .instrument(span)
        .await
    }

    type AppendSessionStream =
        Pin<Box<dyn Stream<Item = Result<AppendSessionResponse, Status>> + Send>>;

    async fn append_session(
        &self,
        request: Request<tonic::Streaming<AppendSessionRequest>>,
    ) -> std::result::Result<Response<Self::AppendSessionStream>, Status> {
        let mut stream = request.into_inner();
        let storage = self.storage.clone();

        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            while let Some(request) = stream.message().await.transpose() {
                match request {
                    Ok(req) => {
                        if req.records.is_empty() {
                            let _ = tx
                                .send(Err(Status::invalid_argument("No records to append")))
                                .await;
                            break;
                        }

                        match storage
                            .append_records(&req.bucket, &req.stream, req.records)
                            .await
                        {
                            Ok((start_seq_id, end_seq_id, start_timestamp, end_timestamp)) => {
                                match storage.get_stream_tail(&req.bucket, &req.stream).await {
                                    Ok((next_seq_id, last_timestamp)) => {
                                        let response = AppendSessionResponse {
                                            start_seq_id,
                                            end_seq_id,
                                            start_timestamp,
                                            end_timestamp,
                                            next_seq_id,
                                            last_timestamp,
                                            error: None,
                                        };
                                        if tx.send(Ok(response)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = tx.send(Err(Status::from(e))).await;
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Err(Status::from(e))).await;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::AppendSessionStream
        ))
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> std::result::Result<Response<ReadResponse>, Status> {
        let timer = Timer::new(names::GRPC_REQUEST_DURATION).with_label(labels::METHOD, "read");

        // Record gRPC request
        utils::record_counter(
            names::GRPC_REQUESTS_TOTAL,
            1,
            vec![(labels::METHOD.to_string(), "read".to_string())],
        );

        let req = request.into_inner();

        // Create tracing span for distributed tracing
        let span = info_span!(
            "grpc_read",
            method = "read",
            bucket = %req.bucket,
            stream = %req.stream,
            limit = ?req.limit.as_ref().and_then(|l| l.count),
            otel.name = "grpc.read"
        );

        async move {
            // Convert gRPC start condition to ReadFilter early
            let read_filter = match req.start {
                Some(read_request::Start::SeqId(seq_id)) => ReadFilter::FromSeqId(seq_id),
                Some(read_request::Start::Timestamp(timestamp)) => {
                    ReadFilter::FromTimestamp(timestamp)
                }
                Some(read_request::Start::TailOffset(offset)) => {
                    // Calculate tail offset starting position
                    // Note: This is an approximation due to distributed storage nature
                    match self
                        .calculate_tail_offset_start(&req.bucket, &req.stream, offset)
                        .await
                    {
                        Ok(start_seq_id) => ReadFilter::FromSeqId(start_seq_id),
                        Err(e) => {
                            utils::record_counter(
                                names::GRPC_ERRORS_TOTAL,
                                1,
                                vec![
                                    (labels::METHOD.to_string(), "read".to_string()),
                                    (
                                        labels::ERROR_TYPE.to_string(),
                                        "tail_offset_error".to_string(),
                                    ),
                                ],
                            );
                            return Err(Status::from(e));
                        }
                    }
                }
                None => ReadFilter::FromSeqId(Uuid::nil().to_string()),
            };

            // Extract count limit from ReadLimit
            let count_limit = req.limit.and_then(|l| l.count);

            // Use unified read method with filter
            let result = self
                .storage
                .read_records_with_filter(&req.bucket, &req.stream, read_filter, count_limit)
                .await;

            match result {
                Ok(records) => {
                    if records.is_empty() {
                        let tail_result =
                            self.storage.get_stream_tail(&req.bucket, &req.stream).await;

                        match tail_result {
                            Ok((next_seq_id, _)) => {
                                timer.record();
                                Ok(Response::new(ReadResponse {
                                    output: Some(read_response::Output::NextSeqId(next_seq_id)),
                                    error: None,
                                }))
                            }
                            Err(e) => {
                                utils::record_counter(
                                    names::GRPC_ERRORS_TOTAL,
                                    1,
                                    vec![
                                        (labels::METHOD.to_string(), "read".to_string()),
                                        (labels::ERROR_TYPE.to_string(), "tail_error".to_string()),
                                    ],
                                );
                                Err(Status::from(e))
                            }
                        }
                    } else {
                        // Apply byte limit if specified
                        let final_records =
                            if let Some(byte_limit) = req.limit.and_then(|l| l.bytes) {
                                let mut total_bytes = 0u64;
                                let mut limited_records = Vec::new();

                                for record in records {
                                    let record_size = record.body.len() as u64
                                        + record
                                            .headers
                                            .iter()
                                            .map(|h| (h.name.len() + h.value.len()) as u64)
                                            .sum::<u64>()
                                        + record.seq_id.len() as u64
                                        + 8; // 8 bytes for timestamp

                                    if total_bytes + record_size > byte_limit
                                        && !limited_records.is_empty()
                                    {
                                        break; // Stop if adding this record would exceed byte limit
                                    }

                                    total_bytes += record_size;
                                    limited_records.push(record);
                                }

                                limited_records
                            } else {
                                records
                            };

                        // Calculate total bytes returned for metrics
                        let total_bytes: usize = final_records
                            .iter()
                            .map(|r| {
                                r.body.len()
                                    + r.headers
                                        .iter()
                                        .map(|h| h.name.len() + h.value.len())
                                        .sum::<usize>()
                            })
                            .sum();

                        let sequenced_records: Vec<SequencedRecord> = final_records
                            .into_iter()
                            .map(|record| SequencedRecord {
                                seq_id: record.seq_id,
                                timestamp: record.timestamp,
                                headers: record
                                    .headers
                                    .into_iter()
                                    .map(|h| Header {
                                        name: h.name,
                                        value: h.value,
                                    })
                                    .collect(),
                                body: record.body,
                            })
                            .collect();

                        let batch = SequencedRecordBatch {
                            records: sequenced_records.clone(),
                        };

                        // Record read metrics
                        crate::record_read_metrics!(
                            &req.bucket,
                            &req.stream,
                            "read",
                            sequenced_records.len(),
                            total_bytes,
                            timer.elapsed().as_secs_f64()
                        );

                        timer.record();
                        tracing::info!(
                            "Successfully read {} records ({} bytes) from {}/{}",
                            sequenced_records.len(),
                            total_bytes,
                            req.bucket,
                            req.stream
                        );

                        Ok(Response::new(ReadResponse {
                            output: Some(read_response::Output::Batch(batch)),
                            error: None,
                        }))
                    }
                }
                Err(e) => {
                    utils::record_counter(
                        names::GRPC_ERRORS_TOTAL,
                        1,
                        vec![
                            (labels::METHOD.to_string(), "read".to_string()),
                            (labels::ERROR_TYPE.to_string(), "read_error".to_string()),
                        ],
                    );
                    tracing::error!("Failed to read from {}/{}: {}", req.bucket, req.stream, e);
                    Err(Status::from(e))
                }
            }
        }
        .instrument(span)
        .await
    }

    type ReadSessionStream =
        Pin<Box<dyn Stream<Item = Result<ReadSessionResponse, Status>> + Send>>;

    async fn read_session(
        &self,
        request: Request<ReadSessionRequest>,
    ) -> std::result::Result<Response<Self::ReadSessionStream>, Status> {
        let req = request.into_inner();
        let storage = self.storage.clone();

        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            // Convert gRPC start condition to ReadFilter for consistency
            let read_filter = match req.start {
                Some(read_session_request::Start::SeqId(seq_id)) => ReadFilter::FromSeqId(seq_id),
                Some(read_session_request::Start::Timestamp(timestamp)) => {
                    ReadFilter::FromTimestamp(timestamp)
                }
                Some(read_session_request::Start::TailOffset(offset)) => {
                    // For read session with tail offset, start from current tail
                    // Note: This is  for streaming - in practice you might want
                    // to calculate the actual offset position
                    match storage.get_stream_tail(&req.bucket, &req.stream).await {
                        Ok((next_seq_id, _)) => {
                            if offset == 0 {
                                ReadFilter::FromSeqId(next_seq_id)
                            } else {
                                // For non-zero offset in streaming, approximate by reading recent records
                                // This could be improved with better indexing
                                ReadFilter::FromSeqId(Uuid::nil().to_string())
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(Status::from(e))).await;
                            return;
                        }
                    }
                }
                None => ReadFilter::FromSeqId(Uuid::nil().to_string()),
            };

            // For streaming reads, we need to track the current position
            // For timestamp-based reads, we'll use a hybrid approach
            let mut current_seq_id = match &read_filter {
                ReadFilter::FromSeqId(seq_id) => seq_id.clone(),
                ReadFilter::FromTimestamp(_) | ReadFilter::TimestampRange { .. } => {
                    // For timestamp-based filters in streaming, start from beginning
                    // and let the filter handle the timestamp logic
                    Uuid::nil().to_string()
                }
            };

            let limit = req.limit.and_then(|l| l.count);

            // Track whether we've applied the initial filter for timestamp-based reads
            let mut initial_filter_applied = matches!(read_filter, ReadFilter::FromSeqId(_));

            loop {
                // For the first iteration with timestamp filters, use the filter directly
                // For subsequent iterations, use seq_id-based pagination
                let records_result = if !initial_filter_applied {
                    initial_filter_applied = true;
                    storage
                        .read_records_with_filter(
                            &req.bucket,
                            &req.stream,
                            read_filter.clone(),
                            limit,
                        )
                        .await
                        .map(|records| {
                            let next_seq_id = records.last().map(|r| r.seq_id.clone());
                            (records, next_seq_id)
                        })
                } else {
                    // Use pagination for subsequent reads
                    storage
                        .read_records_with_pagination_streaming(
                            &req.bucket,
                            &req.stream,
                            &current_seq_id,
                            limit,
                        )
                        .await
                };

                match records_result {
                    Ok((records, next_seq_id)) => {
                        if records.is_empty() {
                            // Send heartbeat if enabled
                            if req.heartbeats {
                                let response = ReadSessionResponse {
                                    output: None,
                                    error: None,
                                };
                                if tx.send(Ok(response)).await.is_err() {
                                    break;
                                }
                            }

                            // Wait a bit before checking again
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            continue;
                        }

                        let sequenced_records: Vec<SequencedRecord> = records
                            .iter()
                            .map(|record| SequencedRecord {
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
                            })
                            .collect();

                        let batch = SequencedRecordBatch {
                            records: sequenced_records,
                        };

                        let output = ReadOutput {
                            output: Some(read_output::Output::Batch(batch)),
                        };

                        let response = ReadSessionResponse {
                            output: Some(output),
                            error: None,
                        };

                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }

                        // Update current sequence ID for next iteration
                        // Use the pagination-aware next sequence ID if available
                        if let Some(next_id) = next_seq_id {
                            current_seq_id = next_id;
                        } else {
                            // No more records available, wait and continue
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            continue;
                        }

                        // Check if we've hit the limit
                        if let Some(limit_count) = limit {
                            if records.len() >= limit_count as usize {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::from(e))).await;
                        break;
                    }
                }

                // Small delay to prevent busy waiting
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ReadSessionStream
        ))
    }
}
