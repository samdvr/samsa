use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::common::{EtcdClient, Storage};
use crate::proto::*;

/// Convert millisecond timestamp (u64) to second timestamp (u64) for protobuf compatibility
fn millis_to_seconds(timestamp_millis: u64) -> u64 {
    timestamp_millis / 1000
}

/// Convert optional millisecond timestamp to optional second timestamp
fn millis_to_seconds_opt(timestamp_millis: Option<u64>) -> Option<u64> {
    timestamp_millis.map(millis_to_seconds)
}

pub struct BucketHandler {
    storage: Arc<Storage>,
    etcd_client: Arc<EtcdClient>,
}

impl BucketHandler {
    pub fn new(storage: Arc<Storage>, etcd_client: Arc<EtcdClient>) -> Self {
        Self {
            storage,
            etcd_client,
        }
    }

    /// Extract bucket name from request metadata
    fn extract_bucket_name(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<String, Status> {
        metadata
            .get("bucket")
            .and_then(|value| value.to_str().ok())
            .map(|s| s.to_string())
            .ok_or_else(|| Status::invalid_argument("Missing bucket metadata"))
    }
}

#[tonic::async_trait]
impl bucket_service_server::BucketService for BucketHandler {
    async fn list_streams(
        &self,
        request: Request<ListStreamsRequest>,
    ) -> std::result::Result<Response<ListStreamsResponse>, Status> {
        // Extract bucket from metadata before consuming the request
        let bucket = self.extract_bucket_name(request.metadata())?;
        let req = request.into_inner();

        let streams = self
            .storage
            .list_streams(&bucket, &req.prefix, &req.start_after, req.limit)
            .await?;

        let stream_infos: Vec<StreamInfo> = streams
            .into_iter()
            .map(|stream| StreamInfo {
                name: stream.name,
                created_at: millis_to_seconds(stream.created_at),
                deleted_at: millis_to_seconds_opt(stream.deleted_at),
            })
            .collect();

        let has_more = req
            .limit
            .is_some_and(|limit| stream_infos.len() >= limit as usize);

        Ok(Response::new(ListStreamsResponse {
            streams: stream_infos,
            has_more,
            error: None,
        }))
    }

    async fn create_stream(
        &self,
        request: Request<CreateStreamRequest>,
    ) -> std::result::Result<Response<CreateStreamResponse>, Status> {
        // Extract bucket from metadata before consuming the request
        let bucket = self.extract_bucket_name(request.metadata())?;
        let req = request.into_inner();

        let config = req.config.unwrap_or_default();
        let stream = self
            .storage
            .create_stream(&bucket, req.stream, config)
            .await
            .map_err(Status::from)?;

        let stream_info = StreamInfo {
            name: stream.name,
            created_at: millis_to_seconds(stream.created_at),
            deleted_at: millis_to_seconds_opt(stream.deleted_at),
        };

        Ok(Response::new(CreateStreamResponse {
            info: Some(stream_info),
            error: None,
        }))
    }

    async fn delete_stream(
        &self,
        request: Request<DeleteStreamRequest>,
    ) -> std::result::Result<Response<DeleteStreamResponse>, Status> {
        // Extract bucket from metadata before consuming the request
        let bucket = self.extract_bucket_name(request.metadata())?;
        let req = request.into_inner();

        self.storage
            .delete_stream(&bucket, &req.stream)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(DeleteStreamResponse { error: None }))
    }

    async fn get_stream_config(
        &self,
        request: Request<GetStreamConfigRequest>,
    ) -> std::result::Result<Response<GetStreamConfigResponse>, Status> {
        // Extract bucket from metadata before consuming the request
        let bucket = self.extract_bucket_name(request.metadata())?;
        let req = request.into_inner();

        let stream = self
            .storage
            .get_stream(&bucket, &req.stream)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(GetStreamConfigResponse {
            config: Some(stream.config),
            error: None,
        }))
    }

    async fn reconfigure_stream(
        &self,
        request: Request<ReconfigureStreamRequest>,
    ) -> std::result::Result<Response<ReconfigureStreamResponse>, Status> {
        // Extract bucket from metadata before consuming the request
        let bucket = self.extract_bucket_name(request.metadata())?;
        let req = request.into_inner();

        if let Some(new_config) = req.config {
            self.storage
                .update_stream_config(&bucket, &req.stream, new_config)
                .await
                .map_err(Status::from)?;

            Ok(Response::new(ReconfigureStreamResponse {
                config: Some(new_config),
                error: None,
            }))
        } else {
            Err(Status::invalid_argument("Missing stream config"))
        }
    }
}
