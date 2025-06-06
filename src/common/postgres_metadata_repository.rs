use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::common::error::{Result, SamsaError};
use crate::common::metadata::{
    AccessToken, MetaDataRepository, StoredBucket, StoredStream, datetime_to_timestamp,
    timestamp_to_datetime,
};
use crate::proto::{AccessTokenInfo, BucketConfig, BucketState, StreamConfig};

#[derive(Debug)]
pub struct PostgresMetaDataRepository {
    pool: PgPool,
}

impl PostgresMetaDataRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl MetaDataRepository for PostgresMetaDataRepository {
    async fn create_bucket(&self, name: String, config: BucketConfig) -> Result<StoredBucket> {
        let bucket_id = Uuid::new_v4();
        let config_json = serde_json::to_value(config).map_err(|e| {
            SamsaError::Internal(format!("Failed to serialize bucket config: {}", e))
        })?;
        let state = BucketState::Active as i32;

        let row = sqlx::query(
            r#"
            INSERT INTO buckets (id, name, config, state, created_at)
            VALUES ($1, $2, $3, $4, NOW())
            RETURNING id, name, config, state, created_at, deleted_at
            "#,
        )
        .bind(bucket_id)
        .bind(&name)
        .bind(&config_json)
        .bind(state)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if let Some(db_err) = e.as_database_error() {
                if db_err.is_unique_violation() {
                    return SamsaError::AlreadyExists(format!(
                        "Bucket with name '{}' already exists",
                        name
                    ));
                }
            }
            SamsaError::DatabaseError(Box::new(e))
        })?;

        let config: BucketConfig = serde_json::from_value(row.get("config")).map_err(|e| {
            SamsaError::Internal(format!("Failed to deserialize bucket config: {}", e))
        })?;

        Ok(StoredBucket {
            id: row.get("id"),
            name: row.get("name"),
            config,
            state: row.get("state"),
            created_at: datetime_to_timestamp(row.get("created_at")),
            deleted_at: row
                .get::<Option<DateTime<Utc>>, _>("deleted_at")
                .map(datetime_to_timestamp),
        })
    }

    async fn get_bucket(&self, name: &str) -> Result<StoredBucket> {
        let row = sqlx::query(
            "SELECT id, name, config, state, created_at, deleted_at FROM buckets WHERE name = $1 AND deleted_at IS NULL"
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?
        .ok_or_else(|| SamsaError::NotFound(format!("Bucket '{}' not found", name)))?;

        let config: BucketConfig = serde_json::from_value(row.get("config")).map_err(|e| {
            SamsaError::Internal(format!("Failed to deserialize bucket config: {}", e))
        })?;

        Ok(StoredBucket {
            id: row.get("id"),
            name: row.get("name"),
            config,
            state: row.get("state"),
            created_at: datetime_to_timestamp(row.get("created_at")),
            deleted_at: row
                .get::<Option<DateTime<Utc>>, _>("deleted_at")
                .map(datetime_to_timestamp),
        })
    }

    async fn list_buckets(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredBucket>> {
        let sql_limit = limit.unwrap_or(1000) as i32;
        let prefix_pattern = format!("{}%", prefix);

        let rows = sqlx::query(
            r#"
            SELECT id, name, config, state, created_at, deleted_at 
            FROM buckets 
            WHERE name LIKE $1 AND name > $2 AND deleted_at IS NULL 
            ORDER BY name 
            LIMIT $3
            "#,
        )
        .bind(&prefix_pattern)
        .bind(start_after)
        .bind(sql_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        let mut buckets = Vec::new();
        for row in rows {
            let config: BucketConfig = serde_json::from_value(row.get("config")).map_err(|e| {
                SamsaError::Internal(format!("Failed to deserialize bucket config: {}", e))
            })?;

            buckets.push(StoredBucket {
                id: row.get("id"),
                name: row.get("name"),
                config,
                state: row.get("state"),
                created_at: datetime_to_timestamp(row.get("created_at")),
                deleted_at: row
                    .get::<Option<DateTime<Utc>>, _>("deleted_at")
                    .map(datetime_to_timestamp),
            });
        }

        Ok(buckets)
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        // Start a transaction
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        // Soft delete the bucket
        let result = sqlx::query(
            "UPDATE buckets SET deleted_at = NOW(), state = $1 WHERE name = $2 AND deleted_at IS NULL"
        )
        .bind(BucketState::Deleting as i32)
        .bind(name)
        .execute(&mut *tx)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        if result.rows_affected() == 0 {
            tx.rollback()
                .await
                .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;
            return Err(SamsaError::NotFound(format!("Bucket '{}' not found", name)));
        }

        // Get bucket_id to soft delete associated streams
        let bucket_row = sqlx::query("SELECT id FROM buckets WHERE name = $1")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        if let Some(bucket) = bucket_row {
            let bucket_id: Uuid = bucket.get("id");
            // Soft delete all streams in this bucket
            sqlx::query(
                "UPDATE streams SET deleted_at = NOW() WHERE bucket_id = $1 AND deleted_at IS NULL",
            )
            .bind(bucket_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;
        }

        tx.commit()
            .await
            .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;
        Ok(())
    }

    async fn update_bucket_config(&self, name: &str, config: BucketConfig) -> Result<()> {
        let config_json = serde_json::to_value(config).map_err(|e| {
            SamsaError::Internal(format!("Failed to serialize bucket config: {}", e))
        })?;

        let result =
            sqlx::query("UPDATE buckets SET config = $1 WHERE name = $2 AND deleted_at IS NULL")
                .bind(&config_json)
                .bind(name)
                .execute(&self.pool)
                .await
                .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        if result.rows_affected() == 0 {
            return Err(SamsaError::NotFound(format!("Bucket '{}' not found", name)));
        }

        Ok(())
    }

    async fn create_stream(
        &self,
        bucket: &str,
        name: String,
        config: StreamConfig,
    ) -> Result<StoredStream> {
        let stream_id = Uuid::new_v4();
        let config_json = serde_json::to_value(config).map_err(|e| {
            SamsaError::Internal(format!("Failed to serialize stream config: {}", e))
        })?;

        // Get bucket_id first
        let bucket_row =
            sqlx::query("SELECT id FROM buckets WHERE name = $1 AND deleted_at IS NULL")
                .bind(bucket)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?
                .ok_or_else(|| SamsaError::NotFound(format!("Bucket '{}' not found", bucket)))?;

        let bucket_id: Uuid = bucket_row.get("id");

        let row = sqlx::query(
            r#"
            INSERT INTO streams (id, bucket_id, name, config, created_at, next_seq_id, last_timestamp)
            VALUES ($1, $2, $3, $4, NOW(), '0', NOW())
            RETURNING id, bucket_id, name, config, created_at, deleted_at, next_seq_id, last_timestamp
            "#,
        )
        .bind(stream_id)
        .bind(bucket_id)
        .bind(&name)
        .bind(&config_json)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if let Some(db_err) = e.as_database_error() {
                if db_err.is_unique_violation() {
                    return SamsaError::AlreadyExists(format!(
                        "Stream '{}/{}' already exists",
                        bucket, name
                    ));
                }
            }
            SamsaError::DatabaseError(Box::new(e))
        })?;

        let config: StreamConfig = serde_json::from_value(row.get("config")).map_err(|e| {
            SamsaError::Internal(format!("Failed to deserialize stream config: {}", e))
        })?;

        Ok(StoredStream {
            id: row.get("id"),
            bucket_id: row.get("bucket_id"),
            bucket: bucket.to_string(),
            name: row.get("name"),
            config,
            created_at: datetime_to_timestamp(row.get("created_at")),
            deleted_at: row
                .get::<Option<DateTime<Utc>>, _>("deleted_at")
                .map(datetime_to_timestamp),
            next_seq_id: row.get("next_seq_id"),
            last_timestamp: datetime_to_timestamp(row.get("last_timestamp")),
        })
    }

    async fn get_stream(&self, bucket: &str, name: &str) -> Result<StoredStream> {
        let row = sqlx::query(
            r#"
            SELECT s.id, s.bucket_id, s.name, s.config, s.created_at, s.deleted_at, s.next_seq_id, s.last_timestamp
            FROM streams s
            JOIN buckets b ON s.bucket_id = b.id
            WHERE b.name = $1 AND s.name = $2 AND s.deleted_at IS NULL AND b.deleted_at IS NULL
            "#
        )
        .bind(bucket)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?
        .ok_or_else(|| SamsaError::NotFound(format!("Stream '{}/{}' not found", bucket, name)))?;

        let config: StreamConfig = serde_json::from_value(row.get("config")).map_err(|e| {
            SamsaError::Internal(format!("Failed to deserialize stream config: {}", e))
        })?;

        Ok(StoredStream {
            id: row.get("id"),
            bucket_id: row.get("bucket_id"),
            bucket: bucket.to_string(),
            name: row.get("name"),
            config,
            created_at: datetime_to_timestamp(row.get("created_at")),
            deleted_at: row
                .get::<Option<DateTime<Utc>>, _>("deleted_at")
                .map(datetime_to_timestamp),
            next_seq_id: row.get("next_seq_id"),
            last_timestamp: datetime_to_timestamp(row.get("last_timestamp")),
        })
    }

    async fn list_streams(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredStream>> {
        let sql_limit = limit.unwrap_or(1000) as i32;
        let prefix_pattern = format!("{}%", prefix);

        let rows = sqlx::query(
            r#"
            SELECT s.id, s.bucket_id, s.name, s.config, s.created_at, s.deleted_at, s.next_seq_id, s.last_timestamp
            FROM streams s
            JOIN buckets b ON s.bucket_id = b.id
            WHERE b.name = $1 AND s.name LIKE $2 AND s.name > $3 AND s.deleted_at IS NULL AND b.deleted_at IS NULL
            ORDER BY s.name
            LIMIT $4
            "#,
        )
        .bind(bucket)
        .bind(&prefix_pattern)
        .bind(start_after)
        .bind(sql_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        let mut streams = Vec::new();
        for row in rows {
            let config: StreamConfig = serde_json::from_value(row.get("config")).map_err(|e| {
                SamsaError::Internal(format!("Failed to deserialize stream config: {}", e))
            })?;

            streams.push(StoredStream {
                id: row.get("id"),
                bucket_id: row.get("bucket_id"),
                bucket: bucket.to_string(),
                name: row.get("name"),
                config,
                created_at: datetime_to_timestamp(row.get("created_at")),
                deleted_at: row
                    .get::<Option<DateTime<Utc>>, _>("deleted_at")
                    .map(datetime_to_timestamp),
                next_seq_id: row.get("next_seq_id"),
                last_timestamp: datetime_to_timestamp(row.get("last_timestamp")),
            });
        }

        Ok(streams)
    }

    async fn delete_stream(&self, bucket: &str, name: &str) -> Result<()> {
        let result = sqlx::query(
            r#"
            UPDATE streams SET deleted_at = NOW()
            FROM buckets b
            WHERE streams.bucket_id = b.id AND b.name = $1 AND streams.name = $2 
            AND streams.deleted_at IS NULL AND b.deleted_at IS NULL
            "#,
        )
        .bind(bucket)
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        if result.rows_affected() == 0 {
            return Err(SamsaError::NotFound(format!(
                "Stream '{}/{}' not found",
                bucket, name
            )));
        }

        Ok(())
    }

    async fn update_stream_config(
        &self,
        bucket: &str,
        name: &str,
        config: StreamConfig,
    ) -> Result<()> {
        let config_json = serde_json::to_value(config).map_err(|e| {
            SamsaError::Internal(format!("Failed to serialize stream config: {}", e))
        })?;

        let result = sqlx::query(
            r#"
            UPDATE streams SET config = $3
            FROM buckets b
            WHERE streams.bucket_id = b.id AND b.name = $1 AND streams.name = $2 
            AND streams.deleted_at IS NULL AND b.deleted_at IS NULL
            "#,
        )
        .bind(bucket)
        .bind(name)
        .bind(&config_json)
        .execute(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        if result.rows_affected() == 0 {
            return Err(SamsaError::NotFound(format!(
                "Stream '{}/{}' not found",
                bucket, name
            )));
        }

        Ok(())
    }

    async fn update_stream_metadata(
        &self,
        bucket: &str,
        name: &str,
        next_seq_id: String,
        last_timestamp: u64,
    ) -> Result<()> {
        // Convert u64 timestamp to DateTime<Utc>
        let last_timestamp_dt = timestamp_to_datetime(last_timestamp);

        let result = sqlx::query(
            r#"
            UPDATE streams SET next_seq_id = $3, last_timestamp = $4
            FROM buckets b
            WHERE streams.bucket_id = b.id AND b.name = $1 AND streams.name = $2 
            AND streams.deleted_at IS NULL AND b.deleted_at IS NULL
            "#,
        )
        .bind(bucket)
        .bind(name)
        .bind(&next_seq_id)
        .bind(last_timestamp_dt)
        .execute(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        if result.rows_affected() == 0 {
            return Err(SamsaError::NotFound(format!(
                "Stream '{}/{}' not found",
                bucket, name
            )));
        }

        Ok(())
    }

    async fn get_stream_tail(&self, bucket: &str, stream: &str) -> Result<(String, u64)> {
        let row = sqlx::query(
            r#"
            SELECT s.next_seq_id, s.last_timestamp
            FROM streams s
            JOIN buckets b ON s.bucket_id = b.id
            WHERE b.name = $1 AND s.name = $2 AND s.deleted_at IS NULL AND b.deleted_at IS NULL
            "#,
        )
        .bind(bucket)
        .bind(stream)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?
        .ok_or_else(|| SamsaError::NotFound(format!("Stream '{}/{}' not found", bucket, stream)))?;

        let next_seq_id: String = row.get("next_seq_id");
        let last_timestamp_dt: DateTime<Utc> = row.get("last_timestamp");
        let last_timestamp = datetime_to_timestamp(last_timestamp_dt);

        Ok((next_seq_id, last_timestamp))
    }

    async fn create_access_token(&self, info: AccessTokenInfo) -> Result<String> {
        let token_id = Uuid::new_v4();
        let token_value = Uuid::new_v4().to_string();
        let info_json = serde_json::to_value(&info).map_err(|e| {
            SamsaError::Internal(format!("Failed to serialize access token info: {}", e))
        })?;

        let expires_at = info.expires_at.map(|ts| {
            timestamp_to_datetime(ts) // Convert seconds to milliseconds
        });

        sqlx::query(
            r#"
            INSERT INTO access_tokens (id, token_value, info, created_at, expires_at)
            VALUES ($1, $2, $3, NOW(), $4)
            "#,
        )
        .bind(token_id)
        .bind(&token_value)
        .bind(&info_json)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        Ok(token_value)
    }

    async fn get_access_token(&self, token_value: &str) -> Result<AccessToken> {
        let row = sqlx::query(
            "SELECT id, token_value, info, created_at, expires_at FROM access_tokens WHERE token_value = $1"
        )
        .bind(token_value)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?
        .ok_or_else(|| SamsaError::NotFound("Access token not found".to_string()))?;

        let info: AccessTokenInfo = serde_json::from_value(row.get("info")).map_err(|e| {
            SamsaError::Internal(format!("Failed to deserialize access token info: {}", e))
        })?;

        Ok(AccessToken {
            id: row.get("id"),
            token: row.get("token_value"),
            info,
            created_at: datetime_to_timestamp(row.get("created_at")),
            expires_at: row
                .get::<Option<DateTime<Utc>>, _>("expires_at")
                .map(datetime_to_timestamp),
        })
    }

    async fn list_access_tokens(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<AccessToken>> {
        let sql_limit = limit.unwrap_or(1000) as i32;
        let prefix_pattern = format!("{}%", prefix);

        let rows = sqlx::query(
            r#"
            SELECT id, token_value, info, created_at, expires_at
            FROM access_tokens
            WHERE token_value LIKE $1 AND token_value > $2
            ORDER BY token_value
            LIMIT $3
            "#,
        )
        .bind(&prefix_pattern)
        .bind(start_after)
        .bind(sql_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        let mut tokens = Vec::new();
        for row in rows {
            let info: AccessTokenInfo = serde_json::from_value(row.get("info")).map_err(|e| {
                SamsaError::Internal(format!("Failed to deserialize access token info: {}", e))
            })?;

            tokens.push(AccessToken {
                id: row.get("id"),
                token: row.get("token_value"),
                info,
                created_at: datetime_to_timestamp(row.get("created_at")),
                expires_at: row
                    .get::<Option<DateTime<Utc>>, _>("expires_at")
                    .map(datetime_to_timestamp),
            });
        }

        Ok(tokens)
    }

    async fn revoke_access_token(&self, token_value: &str) -> Result<AccessToken> {
        // Get the token first
        let token = self.get_access_token(token_value).await?;

        // Delete the token
        let result = sqlx::query("DELETE FROM access_tokens WHERE token_value = $1")
            .bind(token_value)
            .execute(&self.pool)
            .await
            .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        if result.rows_affected() == 0 {
            return Err(SamsaError::NotFound("Access token not found".to_string()));
        }

        Ok(token)
    }

    async fn cleanup_expired_tokens(&self) -> Result<usize> {
        let result = sqlx::query(
            "DELETE FROM access_tokens WHERE expires_at IS NOT NULL AND expires_at < NOW()",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        Ok(result.rows_affected() as usize)
    }

    async fn cleanup_deleted_buckets(&self, grace_period_seconds: u32) -> Result<usize> {
        let result = sqlx::query(
            "DELETE FROM buckets WHERE deleted_at IS NOT NULL AND deleted_at < NOW() - INTERVAL '$1 seconds'"
        )
        .bind(grace_period_seconds as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        Ok(result.rows_affected() as usize)
    }

    async fn cleanup_deleted_streams(&self, grace_period_seconds: u32) -> Result<usize> {
        let result = sqlx::query(
            "DELETE FROM streams WHERE deleted_at IS NOT NULL AND deleted_at < NOW() - INTERVAL '$1 seconds'"
        )
        .bind(grace_period_seconds as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

        Ok(result.rows_affected() as usize)
    }
}
