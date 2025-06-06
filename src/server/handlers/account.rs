use crate::common::EtcdClient;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::proto::*;

/// Convert millisecond timestamp (u64) to second timestamp (u64) for protobuf compatibility
/// This maintains backward compatibility with existing protobuf definitions while
/// using millisecond precision internally for better event ordering
fn millis_to_seconds(timestamp_millis: u64) -> u64 {
    timestamp_millis / 1000
}

/// Convert optional millisecond timestamp to optional second timestamp
fn millis_to_seconds_opt(timestamp_millis: Option<u64>) -> Option<u64> {
    timestamp_millis.map(millis_to_seconds)
}

pub struct AccountHandler {
    storage: Arc<dyn crate::common::MockableStorage>,
    etcd_client: Arc<EtcdClient>,
}

impl AccountHandler {
    pub fn new(
        storage: Arc<dyn crate::common::MockableStorage>,
        etcd_client: Arc<EtcdClient>,
    ) -> Self {
        Self {
            storage,
            etcd_client,
        }
    }
}

#[tonic::async_trait]
impl account_service_server::AccountService for AccountHandler {
    async fn list_buckets(
        &self,
        request: Request<ListBucketsRequest>,
    ) -> std::result::Result<Response<ListBucketsResponse>, Status> {
        let req = request.into_inner();

        let buckets = self
            .storage
            .list_buckets(&req.prefix, &req.start_after, req.limit)
            .await?;

        let bucket_infos: Vec<BucketInfo> = buckets
            .into_iter()
            .map(|bucket| BucketInfo {
                name: bucket.name,
                state: bucket.state,
                created_at: millis_to_seconds(bucket.created_at),
                deleted_at: millis_to_seconds_opt(bucket.deleted_at),
            })
            .collect();

        let has_more = req
            .limit
            .is_some_and(|limit| bucket_infos.len() >= limit as usize);

        Ok(Response::new(ListBucketsResponse {
            buckets: bucket_infos,
            has_more,
            error: None,
        }))
    }

    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> std::result::Result<Response<CreateBucketResponse>, Status> {
        let req = request.into_inner();

        let config = req.config.unwrap_or_default();
        let bucket = self
            .storage
            .create_bucket(req.bucket, config)
            .await
            .map_err(Status::from)?;

        let bucket_info = BucketInfo {
            name: bucket.name,
            state: bucket.state,
            created_at: millis_to_seconds(bucket.created_at),
            deleted_at: millis_to_seconds_opt(bucket.deleted_at),
        };

        Ok(Response::new(CreateBucketResponse {
            info: Some(bucket_info),
            error: None,
        }))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> std::result::Result<Response<DeleteBucketResponse>, Status> {
        let req = request.into_inner();

        self.storage
            .delete_bucket(&req.bucket)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(DeleteBucketResponse { error: None }))
    }

    async fn reconfigure_bucket(
        &self,
        request: Request<ReconfigureBucketRequest>,
    ) -> std::result::Result<Response<ReconfigureBucketResponse>, Status> {
        let req = request.into_inner();

        let mut bucket = self
            .storage
            .get_bucket(&req.bucket)
            .await
            .map_err(Status::from)?;

        if let Some(new_config) = req.config {
            bucket.config = new_config;
            self.storage
                .update_bucket_config(&bucket.name, bucket.config)
                .await
                .map_err(Status::from)?;
        }

        Ok(Response::new(ReconfigureBucketResponse {
            config: Some(bucket.config),
            error: None,
        }))
    }

    async fn get_bucket_config(
        &self,
        request: Request<GetBucketConfigRequest>,
    ) -> std::result::Result<Response<GetBucketConfigResponse>, Status> {
        let req = request.into_inner();

        let bucket = self
            .storage
            .get_bucket(&req.bucket)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(GetBucketConfigResponse {
            config: Some(bucket.config),
            error: None,
        }))
    }

    async fn issue_access_token(
        &self,
        request: Request<IssueAccessTokenRequest>,
    ) -> std::result::Result<Response<IssueAccessTokenResponse>, Status> {
        let req = request.into_inner();

        let info = req
            .info
            .ok_or_else(|| Status::invalid_argument("Missing access token info"))?;
        let token_string = self
            .storage
            .create_access_token(info)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(IssueAccessTokenResponse {
            access_token: token_string,
            error: None,
        }))
    }

    async fn revoke_access_token(
        &self,
        request: Request<RevokeAccessTokenRequest>,
    ) -> std::result::Result<Response<RevokeAccessTokenResponse>, Status> {
        let req = request.into_inner();

        let token_struct = self
            .storage
            .revoke_access_token(&req.id)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(RevokeAccessTokenResponse {
            info: Some(token_struct.info),
            error: None,
        }))
    }

    async fn list_access_tokens(
        &self,
        request: Request<ListAccessTokensRequest>,
    ) -> std::result::Result<Response<ListAccessTokensResponse>, Status> {
        let req = request.into_inner();

        let tokens = self
            .storage
            .list_access_tokens(&req.prefix, &req.start_after, req.limit)
            .await?;

        let access_token_infos: Vec<AccessTokenInfo> =
            tokens.into_iter().map(|token| token.info).collect();

        let has_more = req
            .limit
            .is_some_and(|limit| access_token_infos.len() >= limit as usize);

        Ok(Response::new(ListAccessTokensResponse {
            access_tokens: access_token_infos,
            has_more,
            error: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metadata::{AccessToken, StoredBucket};
    use crate::common::{MockMockableStorage, error::SamsaError};
    use crate::proto::account_service_server::AccountService;
    use mockall::predicate::eq;
    use uuid::Uuid;

    fn create_mock_etcd_client() -> Arc<EtcdClient> {
        Arc::new(EtcdClient::new_mock())
    }

    #[test]
    fn test_millis_to_seconds_conversion() {
        assert_eq!(millis_to_seconds(0), 0);
        assert_eq!(millis_to_seconds(999), 0);
        assert_eq!(millis_to_seconds(1000), 1);
        assert_eq!(millis_to_seconds(1999), 1);
        assert_eq!(millis_to_seconds(123456), 123);
        assert_eq!(millis_to_seconds(u64::MAX), u64::MAX / 1000);
    }

    #[test]
    fn test_millis_to_seconds_opt_conversion() {
        assert_eq!(millis_to_seconds_opt(None), None);
        assert_eq!(millis_to_seconds_opt(Some(0)), Some(0));
        assert_eq!(millis_to_seconds_opt(Some(1500)), Some(1));
    }

    #[tokio::test]
    async fn test_list_buckets_empty() {
        let mut mock_storage = MockMockableStorage::new();
        mock_storage
            .expect_list_buckets()
            .with(eq(""), eq(""), eq(None))
            .times(1)
            .returning(|_, _, _| Ok(vec![]));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(ListBucketsRequest {
            prefix: "".to_string(),
            start_after: "".to_string(),
            limit: None,
        });

        let response = handler.list_buckets(request).await.unwrap();
        let inner = response.into_inner();
        assert!(inner.buckets.is_empty());
        assert!(!inner.has_more);
    }

    #[tokio::test]
    async fn test_list_buckets_with_data_and_limit() {
        let mut mock_storage = MockMockableStorage::new();
        let returned_buckets = vec![
            StoredBucket {
                id: Uuid::new_v4(),
                name: "bucket1".to_string(),
                state: BucketState::Active as i32,
                created_at: 1000,
                deleted_at: None,
                config: BucketConfig::default(),
            },
            StoredBucket {
                id: Uuid::new_v4(),
                name: "bucket2".to_string(),
                state: BucketState::Active as i32,
                created_at: 2000,
                deleted_at: None,
                config: BucketConfig::default(),
            },
        ];
        let cloned_buckets = returned_buckets.clone();

        mock_storage
            .expect_list_buckets()
            .with(eq("prefix"), eq("start"), eq(Some(2)))
            .times(1)
            .returning(move |_, _, _| Ok(cloned_buckets.clone()));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(ListBucketsRequest {
            prefix: "prefix".to_string(),
            start_after: "start".to_string(),
            limit: Some(2),
        });

        let response = handler.list_buckets(request).await.unwrap();
        let inner = response.into_inner();

        assert_eq!(inner.buckets.len(), 2);
        assert_eq!(inner.buckets[0].name, "bucket1");
        assert_eq!(inner.buckets[0].created_at, 1);
        assert_eq!(inner.buckets[1].name, "bucket2");
        assert_eq!(inner.buckets[1].created_at, 2);
        assert!(inner.has_more);
    }

    #[tokio::test]
    async fn test_list_buckets_has_more_false() {
        let mut mock_storage = MockMockableStorage::new();
        let returned_buckets = vec![StoredBucket {
            id: Uuid::new_v4(),
            name: "bucket1".to_string(),
            state: BucketState::Active as i32,
            created_at: 1000,
            deleted_at: None,
            config: BucketConfig::default(),
        }];
        let cloned_buckets = returned_buckets.clone();

        mock_storage
            .expect_list_buckets()
            .with(eq(""), eq(""), eq(Some(5)))
            .times(1)
            .returning(move |_, _, _| Ok(cloned_buckets.clone()));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(ListBucketsRequest {
            prefix: "".to_string(),
            start_after: "".to_string(),
            limit: Some(5),
        });

        let response = handler.list_buckets(request).await.unwrap();
        let inner = response.into_inner();

        assert_eq!(inner.buckets.len(), 1);
        assert!(!inner.has_more);
    }

    #[tokio::test]
    async fn test_create_bucket() {
        let mut mock_storage = MockMockableStorage::new();
        let bucket_name = "new-bucket".to_string();
        let bucket_config = BucketConfig::default();
        let expected_stored_bucket = StoredBucket {
            id: Uuid::new_v4(),
            name: bucket_name.clone(),
            state: BucketState::Active as i32,
            created_at: 5000,
            deleted_at: None,
            config: bucket_config,
        };
        let cloned_bucket_for_mock = expected_stored_bucket.clone();

        mock_storage
            .expect_create_bucket()
            .with(eq(bucket_name.clone()), eq(bucket_config))
            .times(1)
            .returning(move |_, _| Ok(cloned_bucket_for_mock.clone()));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(CreateBucketRequest {
            bucket: bucket_name.clone(),
            config: Some(bucket_config),
        });

        let response = handler.create_bucket(request).await.unwrap();
        let inner = response.into_inner();
        assert!(inner.info.is_some());
        let info = inner.info.unwrap();
        assert_eq!(info.name, bucket_name);
        assert_eq!(info.state, expected_stored_bucket.state);
        assert_eq!(info.created_at, 5);
        assert_eq!(info.deleted_at, None);
    }

    #[tokio::test]
    async fn test_create_bucket_storage_error() {
        let mut mock_storage = MockMockableStorage::new();
        mock_storage
            .expect_create_bucket()
            .times(1)
            .returning(|_, _| Err(SamsaError::Internal("Storage failure".to_string())));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(CreateBucketRequest {
            bucket: "error-bucket".to_string(),
            config: Some(BucketConfig::default()),
        });

        let result = handler.create_bucket(request).await;
        assert!(result.is_err());
        let status = result.err().unwrap();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("Storage failure"));
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        let mut mock_storage = MockMockableStorage::new();
        let bucket_name = "delete-me".to_string();
        let bucket_name_clone = bucket_name.clone();

        mock_storage
            .expect_delete_bucket()
            .with(eq(bucket_name_clone))
            .times(1)
            .returning(|_| Ok(()));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(DeleteBucketRequest {
            bucket: bucket_name,
        });

        let response = handler.delete_bucket(request).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_get_bucket_config() {
        let mut mock_storage = MockMockableStorage::new();
        let bucket_name = "config-bucket".to_string();
        let bucket_config = BucketConfig {
            create_stream_on_append: true,
            ..Default::default()
        };
        let stored_bucket = StoredBucket {
            id: Uuid::new_v4(),
            name: bucket_name.clone(),
            state: BucketState::Active as i32,
            created_at: 12345,
            deleted_at: None,
            config: bucket_config,
        };
        let bucket_name_clone = bucket_name.clone();
        let stored_bucket_clone = stored_bucket.clone();

        mock_storage
            .expect_get_bucket()
            .with(eq(bucket_name_clone))
            .times(1)
            .returning(move |_| Ok(stored_bucket_clone.clone()));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(GetBucketConfigRequest {
            bucket: bucket_name,
        });

        let response = handler.get_bucket_config(request).await.unwrap();
        let inner_config = response.into_inner().config.unwrap();
        assert_eq!(
            inner_config.create_stream_on_append,
            bucket_config.create_stream_on_append
        );
    }

    #[tokio::test]
    async fn test_reconfigure_bucket() {
        let mut mock_storage = MockMockableStorage::new();
        let bucket_name = "reconfig-bucket".to_string();

        let initial_config = BucketConfig {
            create_stream_on_append: false,
            ..Default::default()
        };
        let initial_stored_bucket = StoredBucket {
            id: Uuid::new_v4(),
            name: bucket_name.clone(),
            state: BucketState::Active as i32,
            created_at: 1000,
            deleted_at: None,
            config: initial_config,
        };

        let new_config = BucketConfig {
            create_stream_on_append: true,
            ..Default::default()
        };

        let bucket_name_clone_get = bucket_name.clone();
        let initial_stored_bucket_clone_get = initial_stored_bucket.clone();
        mock_storage
            .expect_get_bucket()
            .with(eq(bucket_name_clone_get))
            .times(1)
            .returning(move |_| Ok(initial_stored_bucket_clone_get.clone()));

        let bucket_name_clone_update = bucket_name.clone();
        let new_config_clone_update = new_config;
        mock_storage
            .expect_update_bucket_config()
            .with(eq(bucket_name_clone_update), eq(new_config_clone_update))
            .times(1)
            .returning(|_, _| Ok(()));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(ReconfigureBucketRequest {
            bucket: bucket_name.clone(),
            config: Some(new_config),
            mask: None,
        });

        let response = handler.reconfigure_bucket(request).await.unwrap();
        let updated_config_response = response.into_inner().config.unwrap();
        assert_eq!(
            updated_config_response.create_stream_on_append,
            new_config.create_stream_on_append
        );
    }

    #[tokio::test]
    async fn test_issue_access_token() {
        let mut mock_storage = MockMockableStorage::new();
        let token_info_req = AccessTokenInfo {
            id: "".to_string(),
            expires_at: Some(1234567890),
            auto_prefix_streams: false,
            scope: None,
        };
        let token_info_req_clone = token_info_req.clone();
        let returned_token_string = "test-token-string".to_string();
        let returned_token_string_clone = returned_token_string.clone();

        mock_storage
            .expect_create_access_token()
            .with(eq(token_info_req_clone))
            .times(1)
            .returning(move |_| Ok(returned_token_string_clone.clone()));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(IssueAccessTokenRequest {
            info: Some(token_info_req),
        });

        let response = handler.issue_access_token(request).await.unwrap();
        assert_eq!(response.into_inner().access_token, returned_token_string);
    }

    #[tokio::test]
    async fn test_revoke_access_token() {
        let mut mock_storage = MockMockableStorage::new();
        let token_id_to_revoke = "token-to-revoke".to_string();
        let token_id_clone = token_id_to_revoke.clone();

        let revoked_token_metadata_struct = AccessToken {
            id: Uuid::new_v4(),
            token: "some-revoked-token-value".to_string(),
            info: AccessTokenInfo {
                id: token_id_to_revoke.clone(),
                expires_at: Some(1000),
                auto_prefix_streams: false,
                scope: None,
            },
            created_at: 500,
            expires_at: Some(1000),
        };
        let revoked_token_metadata_struct_clone = revoked_token_metadata_struct.clone();

        mock_storage
            .expect_revoke_access_token()
            .with(eq(token_id_clone))
            .times(1)
            .returning(move |_| Ok(revoked_token_metadata_struct_clone.clone()));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(RevokeAccessTokenRequest {
            id: token_id_to_revoke,
        });

        let response = handler.revoke_access_token(request).await.unwrap();
        let inner_proto_info = response.into_inner().info.unwrap();
        assert_eq!(inner_proto_info.id, revoked_token_metadata_struct.info.id);
    }

    #[tokio::test]
    async fn test_list_access_tokens() {
        let mut mock_storage = MockMockableStorage::new();
        let prefix = "prefix".to_string();
        let start_after = "start".to_string();
        let limit = Some(2u64);

        let returned_tokens = vec![
            AccessToken {
                id: Uuid::new_v4(),
                token: "val1".to_string(),
                info: AccessTokenInfo {
                    id: "token1".to_string(),
                    expires_at: Some(1),
                    auto_prefix_streams: false,
                    scope: None,
                },
                created_at: 0,
                expires_at: Some(1),
            },
            AccessToken {
                id: Uuid::new_v4(),
                token: "val2".to_string(),
                info: AccessTokenInfo {
                    id: "token2".to_string(),
                    expires_at: Some(2),
                    auto_prefix_streams: false,
                    scope: None,
                },
                created_at: 0,
                expires_at: Some(2),
            },
        ];
        let cloned_tokens = returned_tokens.clone();

        mock_storage
            .expect_list_access_tokens()
            .with(eq(prefix.clone()), eq(start_after.clone()), eq(limit))
            .times(1)
            .returning(move |_, _, _| Ok(cloned_tokens.clone()));

        let handler = AccountHandler::new(Arc::new(mock_storage), create_mock_etcd_client());
        let request = Request::new(ListAccessTokensRequest {
            prefix,
            start_after,
            limit,
        });

        let response = handler.list_access_tokens(request).await.unwrap();
        let inner = response.into_inner();
        assert_eq!(inner.access_tokens.len(), 2);
        assert_eq!(inner.access_tokens[0].id, "token1");
        assert!(inner.has_more);
    }
}
