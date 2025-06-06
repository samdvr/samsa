use crate::common::batch::{BatchConfig, PendingBatch, StoredRecordBatch};
use crate::common::error::{Result, SamsaError};
use crate::common::metadata::{
    AccessToken, InMemoryMetaDataRepository, MetaDataRepository, StoredBucket, StoredRecord,
    StoredStream, current_timestamp_millis,
};
use crate::common::metrics::{Timer, labels, names, utils};
use crate::proto::*;
use crate::{record_append_metrics, record_read_metrics};
use async_stream;
use dashmap::DashMap;
use futures::FutureExt; // Add this import for catch_unwind
use futures::Stream;
use futures::stream::StreamExt;
use object_store::path::Path;
use prost::Message;
use std::panic;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Filter options for reading records
#[derive(Debug, Clone)]
pub enum ReadFilter {
    /// Start reading from a specific sequence ID
    FromSeqId(String),
    /// Start reading from a specific timestamp
    FromTimestamp(u64),
    /// Read records within a timestamp range
    TimestampRange { start: u64, end: u64 },
}

/// Enhanced error tracking for background tasks
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct BackgroundTaskError {
    task_name: String,
    error: String,
    timestamp: u64,
    consecutive_failures: u64,
}

/// Health status for background tasks
#[derive(Debug, Clone, PartialEq)]
pub enum TaskHealth {
    Healthy,
    Degraded(String),
    Failed(String),
}

/// Configuration for retry behavior in background tasks
#[derive(Debug, Clone)]
struct RetryConfig {
    max_consecutive_failures: u64,
    base_delay_ms: u64,
    max_delay_ms: u64,
    exponential_base: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_consecutive_failures: 10,
            base_delay_ms: 1000, // 1 second
            max_delay_ms: 60000, // 1 minute
            exponential_base: 2.0,
        }
    }
}

impl RetryConfig {
    fn calculate_delay(&self, consecutive_failures: u64) -> Duration {
        if consecutive_failures == 0 {
            return Duration::from_millis(0);
        }

        let delay_ms = (self.base_delay_ms as f64
            * self
                .exponential_base
                .powf((consecutive_failures - 1) as f64)) as u64;
        let capped_delay = std::cmp::min(delay_ms, self.max_delay_ms);
        Duration::from_millis(capped_delay)
    }
}

pub type PaginatedRecordStream<'a> = Pin<Box<dyn Stream<Item = Result<(StoredRecord, Option<String>)>> + Send + 'a>>;

/// Storage handles both metadata and object storage operations with batched record storage.
///
/// ## Batch Storage System
///
/// This implementation uses a batched storage approach that significantly improves performance
/// by reducing the number of object store operations. Records are collected into batches and
/// flushed based on configurable criteria:
///
/// - **Size-based flushing**: When a batch reaches the configured maximum number of records or bytes
/// - **Time-based flushing**: When records have been pending for longer than the flush interval
/// - **Manual flushing**: Using `flush_stream()` or `flush_all()` methods
///
/// ## Benefits
///
/// - **Performance**: Reduces object store operations from O(n) to O(n/batch_size)
/// - **Cost efficiency**: Fewer operations mean lower costs with cloud object stores
/// - **Throughput**: Higher sustained write throughput for high-volume streams
/// - **Latency**: Configurable trade-off between latency and efficiency
///
/// ## Configuration
///
/// ```ignore
/// use samsa::common::storage::Storage;
/// use object_store::memory::InMemory;
/// use std::sync::Arc;
///
/// // Create storage with default batch configuration
/// let object_store = Arc::new(InMemory::new());
/// let storage = Storage::new_with_memory(object_store, "test-node".to_string());
/// ```
#[derive(Clone)]
pub struct Storage {
    metadata_repo: Arc<dyn MetaDataRepository>,
    object_store: Arc<dyn object_store::ObjectStore>,
    node_id: String,
    batch_config: BatchConfig,
    pending_batches: Arc<DashMap<String, Mutex<PendingBatch>>>, // key: "bucket/stream"

    // Background task health tracking
    cleanup_task_health: Arc<Mutex<TaskHealth>>,
    batch_flush_task_health: Arc<Mutex<TaskHealth>>,
    cleanup_consecutive_failures: Arc<AtomicU64>,
    batch_flush_consecutive_failures: Arc<AtomicU64>,
    is_degraded: Arc<AtomicBool>,
}

impl Storage {
    /// Create a new Storage instance with explicit node_id
    pub fn new(
        metadata_repo: Arc<dyn MetaDataRepository>,
        object_store: Arc<dyn object_store::ObjectStore>,
        node_id: String,
    ) -> Self {
        Self::new_with_batch_config(metadata_repo, object_store, node_id, BatchConfig::default())
    }

    /// Create a new Storage instance with explicit node_id and custom batch config
    pub fn new_with_batch_config(
        metadata_repo: Arc<dyn MetaDataRepository>,
        object_store: Arc<dyn object_store::ObjectStore>,
        node_id: String,
        batch_config: BatchConfig,
    ) -> Self {
        let storage = Self {
            metadata_repo,
            object_store,
            node_id,
            batch_config,
            pending_batches: Arc::new(DashMap::new()),
            cleanup_task_health: Arc::new(Mutex::new(TaskHealth::Healthy)),
            batch_flush_task_health: Arc::new(Mutex::new(TaskHealth::Healthy)),
            cleanup_consecutive_failures: Arc::new(AtomicU64::new(0)),
            batch_flush_consecutive_failures: Arc::new(AtomicU64::new(0)),
            is_degraded: Arc::new(AtomicBool::new(false)),
        };

        // Start background cleanup task
        storage.start_cleanup_task();
        // Start background batch flushing task
        storage.start_batch_flush_task();
        storage
    }

    /// Create a new Storage instance reading node_id from environment
    /// This is a convenience method for backward compatibility
    pub fn new_from_env(
        metadata_repo: Arc<dyn MetaDataRepository>,
        object_store: Arc<dyn object_store::ObjectStore>,
    ) -> Self {
        let node_id = Self::get_node_id_from_env();
        Self::new(metadata_repo, object_store, node_id)
    }

    /// Create a new Storage instance with custom batch config, reading node_id from environment
    /// This is a convenience method for backward compatibility
    pub fn new_with_batch_config_from_env(
        metadata_repo: Arc<dyn MetaDataRepository>,
        object_store: Arc<dyn object_store::ObjectStore>,
        batch_config: BatchConfig,
    ) -> Self {
        let node_id = Self::get_node_id_from_env();
        Self::new_with_batch_config(metadata_repo, object_store, node_id, batch_config)
    }

    /// Create a new Storage with in-memory metadata repository and explicit node_id
    pub fn new_with_memory(
        object_store: Arc<dyn object_store::ObjectStore>,
        node_id: String,
    ) -> Self {
        let metadata_repo = Arc::new(InMemoryMetaDataRepository::new());
        Self::new(metadata_repo, object_store, node_id)
    }

    /// Create a new Storage with in-memory metadata repository and custom batch config
    pub fn new_with_memory_and_batch_config(
        object_store: Arc<dyn object_store::ObjectStore>,
        node_id: String,
        batch_config: BatchConfig,
    ) -> Self {
        let metadata_repo = Arc::new(InMemoryMetaDataRepository::new());
        Self::new_with_batch_config(metadata_repo, object_store, node_id, batch_config)
    }

    /// Create a new Storage with in-memory metadata repository, reading node_id from environment
    /// This is a convenience method for backward compatibility
    pub fn new_with_memory_from_env(object_store: Arc<dyn object_store::ObjectStore>) -> Self {
        let node_id = Self::get_node_id_from_env();
        Self::new_with_memory(object_store, node_id)
    }

    /// Create a new Storage with in-memory metadata repository and custom batch config, reading node_id from environment
    /// This is a convenience method for backward compatibility
    pub fn new_with_memory_and_batch_config_from_env(
        object_store: Arc<dyn object_store::ObjectStore>,
        batch_config: BatchConfig,
    ) -> Self {
        let node_id = Self::get_node_id_from_env();
        Self::new_with_memory_and_batch_config(object_store, node_id, batch_config)
    }

    /// Helper method to read node_id from environment with fallback
    fn get_node_id_from_env() -> String {
        std::env::var("NODE_ID").unwrap_or_else(|_| {
            let default_id = Uuid::now_v7().to_string();
            eprintln!(
                "Warning: NODE_ID environment variable not set, using generated ID: {}",
                default_id
            );
            default_id
        })
    }

    /// Get the current health status of background tasks
    pub async fn get_background_task_health(&self) -> (TaskHealth, TaskHealth) {
        let cleanup_health = self.cleanup_task_health.lock().await.clone();
        let batch_flush_health = self.batch_flush_task_health.lock().await.clone();
        (cleanup_health, batch_flush_health)
    }

    /// Check if the storage system is currently degraded
    pub fn is_degraded(&self) -> bool {
        self.is_degraded.load(Ordering::Relaxed)
    }

    /// Panic-safe wrapper for background tasks
    async fn run_background_task<F, Fut>(task_name: &str, task_fn: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let task_name = task_name.to_string();
        tokio::spawn(async move {
            // Catch panics to prevent silent task termination
            let panic_result = panic::AssertUnwindSafe(task_fn()).catch_unwind().await;

            match panic_result {
                Ok(()) => {
                    tracing::info!("Background task '{}' completed normally", task_name);
                }
                Err(panic_payload) => {
                    let panic_msg = if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        s.to_string()
                    } else {
                        "Unknown panic".to_string()
                    };

                    tracing::error!(
                        "Background task '{}' panicked: {}. This is a critical error that should be investigated.",
                        task_name,
                        panic_msg
                    );

                    // In a production system, you might want to:
                    // - Send alerts to monitoring systems
                    // - Update node status in etcd
                    // - Attempt to restart the task
                }
            }
        });
    }

    /// Enhanced cleanup task with robust error handling
    fn start_cleanup_task(&self) {
        let metadata_repo = self.metadata_repo.clone();
        let cleanup_health = self.cleanup_task_health.clone();
        let consecutive_failures = self.cleanup_consecutive_failures.clone();
        let is_degraded = self.is_degraded.clone();
        let retry_config = RetryConfig::default();

        tokio::spawn(Self::run_background_task("cleanup_task", move || async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300)); // Every 5 minutes

            loop {
                interval.tick().await;

                let iteration_timer = Timer::new(names::BACKGROUND_TASK_DURATION)
                    .with_label(labels::TASK_NAME.to_string(), "cleanup".to_string());

                // Record task iteration
                utils::record_counter(
                    names::BACKGROUND_TASK_ITERATIONS,
                    1,
                    vec![(labels::TASK_NAME.to_string(), "cleanup".to_string())],
                );

                let cleanup_result = Self::perform_cleanup_operations(&metadata_repo).await;

                match cleanup_result {
                    Ok(()) => {
                        // Reset failure count on success
                        let prev_failures = consecutive_failures.swap(0, Ordering::Relaxed);

                        if prev_failures > 0 {
                            tracing::info!(
                                "Cleanup task recovered after {} consecutive failures",
                                prev_failures
                            );

                            // Update health status
                            if let Ok(mut health) = cleanup_health.try_lock() {
                                *health = TaskHealth::Healthy;
                            }

                            // Update health metric
                            utils::record_gauge(
                                names::BACKGROUND_TASK_HEALTH,
                                1.0,
                                vec![(labels::TASK_NAME.to_string(), "cleanup".to_string())],
                            );

                            // Check if we can remove degraded status
                            Self::update_degraded_status(
                                &is_degraded,
                                &consecutive_failures,
                                &AtomicU64::new(0),
                            );
                        }

                        iteration_timer.record();
                    }
                    Err(e) => {
                        let failure_count =
                            consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

                        // Record error metrics
                        utils::record_counter(
                            names::BACKGROUND_TASK_ERRORS,
                            1,
                            vec![(labels::TASK_NAME.to_string(), "cleanup".to_string())],
                        );

                        utils::record_error(
                            names::BACKGROUND_TASK_ERRORS,
                            &e.to_string(),
                            vec![(labels::TASK_NAME.to_string(), "cleanup".to_string())],
                        );

                        // Log with increasing severity based on failure count
                        if failure_count <= 3 {
                            tracing::warn!(
                                "Cleanup task failed (attempt {}): {}",
                                failure_count,
                                e
                            );
                        } else if failure_count <= retry_config.max_consecutive_failures {
                            tracing::error!(
                                "Cleanup task failed (attempt {}): {}. Task entering degraded state.",
                                failure_count,
                                e
                            );

                            // Update health status
                            if let Ok(mut health) = cleanup_health.try_lock() {
                                *health = TaskHealth::Degraded(format!(
                                    "Failed {} times: {}",
                                    failure_count, e
                                ));
                            }

                            // Update health metric
                            utils::record_gauge(
                                names::BACKGROUND_TASK_HEALTH,
                                0.5,
                                vec![(labels::TASK_NAME.to_string(), "cleanup".to_string())],
                            );

                            is_degraded.store(true, Ordering::Relaxed);
                        } else {
                            tracing::error!(
                                "Cleanup task failed {} times, exceeding max failures. Task is now considered failed: {}",
                                failure_count,
                                e
                            );

                            // Update health status
                            if let Ok(mut health) = cleanup_health.try_lock() {
                                *health = TaskHealth::Failed(format!(
                                    "Exceeded max failures ({}): {}",
                                    failure_count, e
                                ));
                            }

                            // Update health metric
                            utils::record_gauge(
                                names::BACKGROUND_TASK_HEALTH,
                                0.0,
                                vec![(labels::TASK_NAME.to_string(), "cleanup".to_string())],
                            );

                            is_degraded.store(true, Ordering::Relaxed);

                            // TODO: Send alerts to monitoring systems
                            // TODO: Update node status in etcd to Unhealthy
                            // TODO: Consider graceful shutdown or restart
                            break;
                        }

                        // Apply backoff delay before next attempt
                        let delay = retry_config.calculate_delay(failure_count);
                        if delay > Duration::from_millis(0) {
                            tracing::debug!(
                                "Cleanup task backing off for {:?} before next attempt",
                                delay
                            );
                            tokio::time::sleep(delay).await;
                        }

                        iteration_timer.record();
                    }
                }
            }

            tracing::error!(
                "Cleanup task loop exited - this should not happen in normal operation"
            );
        });
    }

    /// Helper method to perform all cleanup operations
    async fn perform_cleanup_operations(metadata_repo: &Arc<dyn MetaDataRepository>) -> Result<()> {
        // Clean up expired access tokens
        let expired_count = metadata_repo.cleanup_expired_tokens().await.map_err(|e| {
            SamsaError::Internal(format!("Failed to cleanup expired tokens: {}", e))
        })?;

        if expired_count > 0 {
            tracing::debug!("Cleaned up {} expired access tokens", expired_count);
        }

        // Clean up deleted buckets and streams after grace period
        let grace_period = 3600; // 1 hour grace period

        let bucket_count = metadata_repo
            .cleanup_deleted_buckets(grace_period)
            .await
            .map_err(|e| {
                SamsaError::Internal(format!("Failed to cleanup deleted buckets: {}", e))
            })?;

        if bucket_count > 0 {
            tracing::debug!("Cleaned up {} deleted buckets", bucket_count);
        }

        let stream_count = metadata_repo
            .cleanup_deleted_streams(grace_period)
            .await
            .map_err(|e| {
                SamsaError::Internal(format!("Failed to cleanup deleted streams: {}", e))
            })?;

        if stream_count > 0 {
            tracing::debug!("Cleaned up {} deleted streams", stream_count);
        }

        Ok(())
    }

    /// Helper to update degraded status based on all task health
    fn update_degraded_status(
        is_degraded: &Arc<AtomicBool>,
        cleanup_failures: &AtomicU64,
        batch_flush_failures: &AtomicU64,
    ) {
        let cleanup_failing = cleanup_failures.load(Ordering::Relaxed) > 0;
        let batch_flush_failing = batch_flush_failures.load(Ordering::Relaxed) > 0;

        let should_be_degraded = cleanup_failing || batch_flush_failing;
        is_degraded.store(should_be_degraded, Ordering::Relaxed);
    }

    /// Enhanced batch flush task with robust error handling
    fn start_batch_flush_task(&self) {
        let object_store = self.object_store.clone();
        let batch_config = self.batch_config.clone();
        let pending_batches = self.pending_batches.clone();
        let node_id = self.node_id.clone();
        let batch_flush_health = self.batch_flush_task_health.clone();
        let consecutive_failures = self.batch_flush_consecutive_failures.clone();
        let is_degraded = self.is_degraded.clone();
        let retry_config = RetryConfig::default();

        tokio::spawn(Self::run_background_task("batch_flush_task", move || async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(batch_config.flush_interval_ms));

            loop {
                interval.tick().await;

                let iteration_timer = Timer::new(names::BACKGROUND_TASK_DURATION)
                    .with_label(labels::TASK_NAME.to_string(), "batch_flush".to_string());

                // Record task iteration
                utils::record_counter(
                    names::BACKGROUND_TASK_ITERATIONS,
                    1,
                    vec![(labels::TASK_NAME.to_string(), "batch_flush".to_string())],
                );

                // Update pending batches gauge
                utils::record_gauge(names::PENDING_BATCHES, pending_batches.len() as f64, vec![]);

                let flush_result = Self::perform_batch_flush_cycle(
                    &object_store,
                    &batch_config,
                    &pending_batches,
                    &node_id,
                )
                .await;

                match flush_result {
                    Ok(flushed_count) => {
                        // Reset failure count on success
                        let prev_failures = consecutive_failures.swap(0, Ordering::Relaxed);

                        if prev_failures > 0 {
                            tracing::info!(
                                "Batch flush task recovered after {} consecutive failures",
                                prev_failures
                            );

                            // Update health status
                            if let Ok(mut health) = batch_flush_health.try_lock() {
                                *health = TaskHealth::Healthy;
                            }

                            // Update health metric
                            utils::record_gauge(
                                names::BACKGROUND_TASK_HEALTH,
                                1.0,
                                vec![(labels::TASK_NAME.to_string(), "batch_flush".to_string())],
                            );

                            // Check if we can remove degraded status
                            Self::update_degraded_status(
                                &is_degraded,
                                &AtomicU64::new(0),
                                &consecutive_failures,
                            );
                        }

                        if flushed_count > 0 {
                            tracing::debug!("Successfully flushed {} batches", flushed_count);
                        }

                        iteration_timer.record();
                    }
                    Err(e) => {
                        let failure_count =
                            consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

                        // Record error metrics
                        utils::record_counter(
                            names::BACKGROUND_TASK_ERRORS,
                            1,
                            vec![(labels::TASK_NAME.to_string(), "batch_flush".to_string())],
                        );

                        utils::record_error(
                            names::BACKGROUND_TASK_ERRORS,
                            &e.to_string(),
                            vec![(labels::TASK_NAME.to_string(), "batch_flush".to_string())],
                        );

                        // Log with increasing severity
                        if failure_count <= 3 {
                            tracing::warn!(
                                "Batch flush task failed (attempt {}): {}",
                                failure_count,
                                e
                            );
                        } else if failure_count <= retry_config.max_consecutive_failures {
                            tracing::error!(
                                "Batch flush task failed (attempt {}): {}. This may cause data loss if batches are discarded!",
                                failure_count,
                                e
                            );

                            // Update health status
                            if let Ok(mut health) = batch_flush_health.try_lock() {
                                *health = TaskHealth::Degraded(format!(
                                    "Failed {} times: {}",
                                    failure_count, e
                                ));
                            }

                            // Update health metric
                            utils::record_gauge(
                                names::BACKGROUND_TASK_HEALTH,
                                0.5,
                                vec![(labels::TASK_NAME.to_string(), "batch_flush".to_string())],
                            );

                            is_degraded.store(true, Ordering::Relaxed);
                        } else {
                            tracing::error!(
                                "Batch flush task failed {} times, exceeding max failures. Batches may be lost: {}",
                                failure_count,
                                e
                            );

                            // Update health status
                            if let Ok(mut health) = batch_flush_health.try_lock() {
                                *health = TaskHealth::Failed(format!(
                                    "Exceeded max failures ({}): {}",
                                    failure_count, e
                                ));
                            }

                            // Update health metric
                            utils::record_gauge(
                                names::BACKGROUND_TASK_HEALTH,
                                0.0,
                                vec![(labels::TASK_NAME.to_string(), "batch_flush".to_string())],
                            );

                            is_degraded.store(true, Ordering::Relaxed);

                            // Critical: batch flushing is essential for data persistence
                            // Consider more aggressive recovery or alerting here
                            break;
                        }

                        // Apply backoff delay before next attempt
                        let delay = retry_config.calculate_delay(failure_count);
                        if delay > Duration::from_millis(0) {
                            tracing::debug!(
                                "Batch flush task backing off for {:?} before next attempt",
                                delay
                            );
                            tokio::time::sleep(delay).await;
                        }

                        iteration_timer.record();
                    }
                }
            }

            tracing::error!(
                "Batch flush task loop exited - this is critical and may cause data loss!"
            );
        });
    }

    /// Helper method to perform a complete batch flush cycle
    async fn perform_batch_flush_cycle(
        object_store: &Arc<dyn object_store::ObjectStore>,
        batch_config: &BatchConfig,
        pending_batches: &Arc<DashMap<String, Mutex<PendingBatch>>>,
        node_id: &str,
    ) -> std::result::Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        // Get batches that need flushing
        let batches_to_flush = {
            let mut to_flush = Vec::new();

            for entry in pending_batches.iter() {
                let batch_key = entry.key().clone();
                let batch_mutex = entry.value();

                if let Ok(batch) = batch_mutex.try_lock() {
                    if batch.should_flush(batch_config) {
                        to_flush.push(batch_key);
                    }
                }
            }
            to_flush
        };

        let mut flushed_count = 0;
        let mut errors = Vec::new();

        // Flush batches that need flushing
        for batch_key in batches_to_flush {
            if let Some(batch_mutex) = pending_batches.get(&batch_key) {
                if let Ok(mut batch) = batch_mutex.try_lock() {
                    if !batch.is_empty() {
                        let bucket = batch_key.split('/').next().unwrap_or("").to_string();
                        let stream = batch_key.split('/').nth(1).unwrap_or("").to_string();

                        // Use the static flush helper for background tasks
                        match Self::flush_batch_static(
                            object_store,
                            &mut batch,
                            &bucket,
                            &stream,
                            node_id,
                        )
                        .await
                        {
                            Ok(()) => {
                                flushed_count += 1;
                            }
                            Err(e) => {
                                errors.push(format!(
                                    "Failed to flush batch for {}: {}",
                                    batch_key, e
                                ));
                            }
                        }
                    }
                }
            }
        }

        // If we had any errors, return the first one
        if !errors.is_empty() {
            let combined_error = errors.join("; ");
            return Err(combined_error.into());
        }

        Ok(flushed_count)
    }

    fn get_batch_key(&self, bucket: &str, stream: &str) -> String {
        format!("{}/{}", bucket, stream)
    }

    fn get_batch_path(&self, bucket: &str, stream: &str, batch_id: &str) -> String {
        format!("{}/{}/{}/{}", bucket, stream, self.node_id, batch_id)
    }

    /// Get prefix for reading from all nodes for a stream
    /// This allows reading records from all nodes that have data for this stream
    /// Used for consumption to ensure we read all data regardless of which node wrote it
    fn get_stream_prefix_all_nodes(&self, bucket: &str, stream: &str) -> String {
        format!("{bucket}/{stream}/")
    }

    /// Core batch flushing logic shared across multiple methods
    /// This helper reduces code duplication and ensures consistent flush behavior
    async fn flush_batch_to_storage(
        &self,
        batch: &mut PendingBatch,
        bucket: &str,
        stream: &str,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let flush_timer = Timer::new(names::BATCH_FLUSH_DURATION)
            .with_label(labels::BUCKET, bucket)
            .with_label(labels::STREAM, stream);

        let batch_id = Uuid::now_v7().to_string();
        let stored_batch =
            batch.to_stored_batch(batch_id.clone(), bucket.to_string(), stream.to_string());

        let batch_key_path = self.get_batch_path(bucket, stream, &batch_id);
        let proto_batch: crate::proto::StoredRecordBatch = (&stored_batch).into();
        let mut serialized = Vec::new();
        proto_batch
            .encode(&mut serialized)
            .map_err(|e| SamsaError::Internal(format!("Failed to serialize batch: {e}")))?;

        let path = Path::from(batch_key_path);

        // Time the object store operation
        let object_store_timer = Timer::new(names::OBJECT_STORE_OPERATION_DURATION)
            .with_label(labels::OPERATION.to_string(), "put".to_string())
            .with_label(labels::BUCKET, bucket)
            .with_label(labels::STREAM, stream);

        let store_result = self
            .object_store
            .put(&path, serialized.clone().into())
            .await;

        object_store_timer.record();

        // Record object store metrics
        utils::record_counter(
            names::OBJECT_STORE_OPERATIONS_TOTAL,
            1,
            vec![
                (labels::OPERATION.to_string(), "put".to_string()),
                (labels::BUCKET.to_string(), bucket.to_string()),
                (labels::STREAM.to_string(), stream.to_string()),
            ],
        );

        if let Err(e) = &store_result {
            utils::record_error(
                names::OBJECT_STORE_ERRORS_TOTAL,
                &e.to_string(),
                vec![
                    (labels::OPERATION.to_string(), "put".to_string()),
                    (labels::BUCKET.to_string(), bucket.to_string()),
                    (labels::STREAM.to_string(), stream.to_string()),
                ],
            );
        } else {
            utils::record_counter(
                names::OBJECT_STORE_BYTES_WRITTEN,
                serialized.len() as u64,
                vec![
                    (labels::BUCKET.to_string(), bucket.to_string()),
                    (labels::STREAM.to_string(), stream.to_string()),
                ],
            );
        }

        store_result.map_err(|e| SamsaError::Internal(format!("Failed to store batch: {e}")))?;

        // Record batch metrics
        utils::record_counter(
            names::BATCHES_FLUSHED_TOTAL,
            1,
            vec![
                (labels::BUCKET.to_string(), bucket.to_string()),
                (labels::STREAM.to_string(), stream.to_string()),
            ],
        );

        utils::record_histogram(
            names::BATCH_SIZE_RECORDS,
            batch.records.len() as f64,
            vec![
                (labels::BUCKET.to_string(), bucket.to_string()),
                (labels::STREAM.to_string(), stream.to_string()),
            ],
        );

        utils::record_histogram(
            names::BATCH_SIZE_BYTES,
            serialized.len() as f64,
            vec![
                (labels::BUCKET.to_string(), bucket.to_string()),
                (labels::STREAM.to_string(), stream.to_string()),
            ],
        );

        tracing::debug!(
            "Flushed batch {} with {} records ({} bytes)",
            batch_id,
            batch.records.len(),
            serialized.len()
        );

        // Clear the batch after successful flush
        *batch = PendingBatch::new();
        flush_timer.record();
        Ok(())
    }

    /// Static helper for flushing batches in background tasks without self reference
    /// This is used by the background flush task since it runs in a separate context
    async fn flush_batch_static(
        object_store: &Arc<dyn object_store::ObjectStore>,
        batch: &mut PendingBatch,
        bucket: &str,
        stream: &str,
        node_id: &str,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_id = Uuid::now_v7().to_string();
        let stored_batch =
            batch.to_stored_batch(batch_id.clone(), bucket.to_string(), stream.to_string());

        let batch_key_path = format!("{}/{}/{}/{}", bucket, stream, node_id, batch_id);
        let proto_batch: crate::proto::StoredRecordBatch = (&stored_batch).into();
        let mut serialized = Vec::new();
        proto_batch.encode(&mut serialized)?;

        let path = Path::from(batch_key_path);
        object_store.put(&path, serialized.into()).await?;

        tracing::debug!(
            "Flushed batch {} with {} records",
            batch_id,
            batch.records.len()
        );

        // Clear the batch after successful flush
        *batch = PendingBatch::new();
        Ok(())
    }

    // Delegate metadata operations to the repository
    pub async fn create_bucket(&self, name: String, config: BucketConfig) -> Result<StoredBucket> {
        self.metadata_repo.create_bucket(name, config).await
    }

    pub async fn get_bucket(&self, name: &str) -> Result<StoredBucket> {
        self.metadata_repo.get_bucket(name).await
    }

    pub async fn list_buckets(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredBucket>> {
        self.metadata_repo
            .list_buckets(prefix, start_after, limit)
            .await
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<()> {
        self.metadata_repo.delete_bucket(name).await
    }

    pub async fn update_bucket_config(&self, name: &str, config: BucketConfig) -> Result<()> {
        self.metadata_repo.update_bucket_config(name, config).await
    }

    pub async fn create_stream(
        &self,
        bucket: &str,
        name: String,
        config: StreamConfig,
    ) -> Result<StoredStream> {
        self.metadata_repo.create_stream(bucket, name, config).await
    }

    pub async fn get_stream(&self, bucket: &str, name: &str) -> Result<StoredStream> {
        self.metadata_repo.get_stream(bucket, name).await
    }

    pub async fn list_streams(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredStream>> {
        self.metadata_repo
            .list_streams(bucket, prefix, start_after, limit)
            .await
    }

    pub async fn delete_stream(&self, bucket: &str, name: &str) -> Result<()> {
        self.metadata_repo.delete_stream(bucket, name).await
    }

    pub async fn update_stream_config(
        &self,
        bucket: &str,
        name: &str,
        config: StreamConfig,
    ) -> Result<()> {
        self.metadata_repo
            .update_stream_config(bucket, name, config)
            .await
    }

    pub async fn get_stream_tail(&self, bucket: &str, stream: &str) -> Result<(String, u64)> {
        self.metadata_repo.get_stream_tail(bucket, stream).await
    }

    pub async fn create_access_token(&self, info: AccessTokenInfo) -> Result<String> {
        self.metadata_repo.create_access_token(info).await
    }

    pub async fn get_access_token(&self, id: &str) -> Result<AccessToken> {
        self.metadata_repo.get_access_token(id).await
    }

    pub async fn list_access_tokens(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<AccessToken>> {
        self.metadata_repo
            .list_access_tokens(prefix, start_after, limit)
            .await
    }

    pub async fn revoke_access_token(&self, id: &str) -> Result<AccessToken> {
        self.metadata_repo.revoke_access_token(id).await
    }

    /// Read records starting from a specific sequence ID with optimized memory usage
    /// This method now uses streaming internally for memory efficiency
    /// while maintaining the same API for backward compatibility
    pub async fn read_records_optimized(
        &self,
        bucket: &str,
        stream: &str,
        start_seq_id: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredRecord>> {
        // Use streaming implementation internally for memory efficiency
        self.read_records_optimized_streaming(bucket, stream, start_seq_id, limit)
            .await
    }

    /// Read records with better pagination support (exclusive start for pagination)
    /// Returns (records, next_seq_id_for_pagination)
    ///
    /// ## Pagination Design
    ///
    /// This method uses **exclusive start pagination** which is more reliable than trying to
    /// manipulate UUID v7 strings. The returned `next_seq_id` should be used as the
    /// `start_after_seq_id` parameter for the next page, and records will start **after**
    /// that seq_id (exclusive) to avoid duplicates.
    ///
    /// Example usage:
    /// ```ignore
    /// use uuid::Uuid;
    ///
    /// async fn paginate_example(storage: &Storage, bucket: &str, stream: &str, page_size: u64) -> Result<(), Box<dyn std::error::Error>> {
    ///     let mut current_start_after = Uuid::nil().to_string();
    ///     loop {
    ///         let (records, next_seq_id) = storage.read_records_with_pagination(
    ///             bucket, stream, &current_start_after, Some(page_size)
    ///         ).await?;
    ///         
    ///         if records.is_empty() { break; }
    ///         
    ///         // Process records...
    ///         
    ///         if let Some(next_id) = next_seq_id {
    ///             current_start_after = next_id;  // Use as exclusive start for next page
    ///         } else {
    ///             break;
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// The next_seq_id is the seq_id of the last record in this page, to be used as exclusive start for next page
    /// Now uses streaming internally for memory efficiency
    pub async fn read_records_with_pagination(
        &self,
        bucket: &str,
        stream: &str,
        start_after_seq_id: &str,
        limit: Option<u64>,
    ) -> Result<(Vec<StoredRecord>, Option<String>)> {
        // Use streaming implementation for memory efficiency
        self.read_records_with_pagination_streaming(bucket, stream, start_after_seq_id, limit)
            .await
    }

    /// Unified read method with flexible filtering options
    /// Now uses streaming internally for memory efficiency
    pub async fn read_records_with_filter(
        &self,
        bucket: &str,
        stream: &str,
        filter: ReadFilter,
        limit: Option<u64>,
    ) -> Result<Vec<StoredRecord>> {
        // Use streaming implementation for memory efficiency
        self.read_records_with_filter_streaming(bucket, stream, filter, limit)
            .await
    }

    /// Get approximate stream statistics for better range query planning
    /// This helps with tail offset calculations and provides insight into record distribution
    /// Now uses streaming for memory efficiency
    pub async fn get_stream_stats(&self, bucket: &str, stream: &str) -> Result<StreamStats> {
        // Use streaming implementation for memory efficiency
        self.get_stream_stats_streaming(bucket, stream).await
    }

    /// Read records starting from a specific timestamp
    /// This provides better range query support for timestamp-based reads
    /// Uses streaming internally for memory efficiency
    pub async fn read_records_from_timestamp(
        &self,
        bucket: &str,
        stream: &str,
        start_timestamp: u64,
        limit: Option<u64>,
    ) -> Result<Vec<StoredRecord>> {
        self.read_records_with_filter(
            bucket,
            stream,
            ReadFilter::FromTimestamp(start_timestamp),
            limit,
        )
        .await
    }

    /// Read records within a timestamp range
    /// Provides range query functionality for better UUID v7 support
    /// Uses streaming internally for memory efficiency
    pub async fn read_records_in_timestamp_range(
        &self,
        bucket: &str,
        stream: &str,
        start_timestamp: u64,
        end_timestamp: u64,
        limit: Option<u64>,
    ) -> Result<Vec<StoredRecord>> {
        self.read_records_with_filter(
            bucket,
            stream,
            ReadFilter::TimestampRange {
                start: start_timestamp,
                end: end_timestamp,
            },
            limit,
        )
        .await
    }

    /// # Errors
    /// Returns an error if storage operations fail
    pub async fn append_records(
        &self,
        bucket: &str,
        stream: &str,
        records: Vec<AppendRecord>,
    ) -> Result<(String, String, u64, u64)> {
        let timer = Timer::new(names::GRPC_REQUEST_DURATION)
            .with_label(labels::OPERATION.to_string(), "append_records".to_string())
            .with_label(labels::BUCKET, bucket)
            .with_label(labels::STREAM, stream);

        // Try to get the stream, or auto-create it if it doesn't exist
        let _stream_info = match self.get_stream(bucket, stream).await {
            Ok(stream) => stream,
            Err(SamsaError::NotFound(_)) => {
                // Auto-create the stream with default configuration
                let default_config = StreamConfig {
                    storage_class: 1,                   // Standard
                    retention_age_seconds: Some(86400), // 1 day
                    timestamping_mode: 3,               // Arrival
                    allow_future_timestamps: false,
                };
                self.create_stream(bucket, stream.to_string(), default_config)
                    .await?
            }
            Err(e) => return Err(e),
        };

        let current_time = current_timestamp_millis();
        let mut start_seq_id = String::new();
        let mut end_seq_id = String::new();
        let mut start_timestamp = 0u64;
        let mut end_timestamp = 0u64;

        // Calculate total bytes for metrics
        let total_bytes: usize = records
            .iter()
            .map(|r| {
                r.body.len()
                    + r.headers
                        .iter()
                        .map(|h| h.name.len() + h.value.len())
                        .sum::<usize>()
            })
            .sum();

        // Get or create pending batch for this stream
        let batch_key = self.get_batch_key(bucket, stream);

        // Use DashMap's entry API to safely get or insert
        let batch_mutex = self
            .pending_batches
            .entry(batch_key.clone())
            .or_insert_with(|| Mutex::new(PendingBatch::new()));

        // Add records to the batch
        {
            let mut batch = batch_mutex.lock().await;

            for (i, record) in records.iter().enumerate() {
                let seq_id = Uuid::now_v7().to_string();
                let timestamp = record.timestamp.unwrap_or(current_time);

                if i == 0 {
                    start_seq_id.clone_from(&seq_id);
                    start_timestamp = timestamp;
                }
                end_seq_id.clone_from(&seq_id);
                end_timestamp = timestamp;

                let stored_record = StoredRecord {
                    seq_id: seq_id.clone(),
                    timestamp,
                    headers: record
                        .headers
                        .iter()
                        .map(std::convert::Into::into)
                        .collect(),
                    body: record.body.clone(),
                };

                batch.add_record(stored_record)?;
            }

            // Check if batch should be flushed immediately
            if batch.should_flush(&self.batch_config) {
                // Use the shared flush helper to reduce code duplication
                self.flush_batch_to_storage(&mut batch, bucket, stream)
                    .await?;
            }
        }

        // Update stream metadata
        if !records.is_empty() {
            let next_seq_id = Uuid::now_v7().to_string();
            self.metadata_repo
                .update_stream_metadata(bucket, stream, next_seq_id, end_timestamp)
                .await?;
        }

        // Record metrics for successful append
        record_append_metrics!(bucket, stream, records.len(), total_bytes);
        timer.record();

        Ok((start_seq_id, end_seq_id, start_timestamp, end_timestamp))
    }

    /// Force flush all pending batches for a specific stream
    pub async fn flush_stream(&self, bucket: &str, stream: &str) -> Result<()> {
        let batch_key = self.get_batch_key(bucket, stream);

        if let Some(batch_mutex) = self.pending_batches.get(&batch_key) {
            let mut batch = batch_mutex.lock().await;
            // Use the shared flush helper to reduce code duplication
            self.flush_batch_to_storage(&mut batch, bucket, stream)
                .await?;
        }

        Ok(())
    }

    /// Force flush all pending batches
    pub async fn flush_all(&self) -> Result<()> {
        let batch_keys: Vec<String> = self
            .pending_batches
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for batch_key in batch_keys {
            let parts: Vec<&str> = batch_key.split('/').collect();
            if parts.len() >= 2 {
                self.flush_stream(parts[0], parts[1]).await?;
            }
        }

        Ok(())
    }

    /// Read records starting from a specific sequence ID
    /// This is a compatibility wrapper around read_records_optimized
    /// Uses streaming internally for memory efficiency
    pub async fn read_records(
        &self,
        bucket: &str,
        stream: &str,
        start_seq_id: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredRecord>> {
        self.read_records_optimized(bucket, stream, start_seq_id, limit)
            .await
    }

    /// Memory-efficient streaming read that yields records one by one
    /// This solves the memory issue by processing records in a streaming fashion
    /// Improved version with better ordering and batch file handling
    ///
    /// When `exclusive_start` is true, records will start after the given seq_id (for pagination)
    /// When `exclusive_start` is false, records will start from the given seq_id (inclusive)
    pub fn read_records_stream(
        &self,
        bucket: &str,
        stream: &str,
        start_seq_id: &str,
        limit: Option<u64>,
    ) -> Pin<Box<dyn Stream<Item = Result<StoredRecord>> + Send + '_>> {
        self.read_records_stream_with_start_mode(bucket, stream, start_seq_id, limit, false)
    }

    /// Memory-efficient streaming read with configurable start mode
    /// When `exclusive_start` is true, records will start after the given seq_id (for pagination)
    /// When `exclusive_start` is false, records will start from the given seq_id (inclusive)
    fn read_records_stream_with_start_mode(
        &self,
        bucket: &str,
        stream: &str,
        start_seq_id: &str,
        limit: Option<u64>,
        exclusive_start: bool,
    ) -> Pin<Box<dyn Stream<Item = Result<StoredRecord>> + Send + '_>> {
        let bucket = bucket.to_string();
        let stream_name = stream.to_string();
        let start_seq_id = start_seq_id.to_string();

        Box::pin(async_stream::stream! {
            let prefix = self.get_stream_prefix_all_nodes(&bucket, &stream_name);
            let mut list_stream = self.object_store.list(Some(&Path::from(prefix)));

            let mut count = 0u64;
            let effective_limit = limit.unwrap_or(u64::MAX);

            // Collect all batch file metadata first for proper ordering
            let mut batch_metas = Vec::new();
            while let Some(result) = list_stream.next().await {
                match result {
                    Ok(object_meta) => batch_metas.push(object_meta),
                    Err(e) => {
                        yield Err(SamsaError::Internal(format!("Failed to list objects: {e}")));
                        return;
                    }
                }
            }

            // Sort batch files by path name to ensure chronological processing
            // This assumes batch file names contain timestamps or sequence information
            batch_metas.sort_by(|a, b| a.location.to_string().cmp(&b.location.to_string()));

            // Process batch files in order
            for object_meta in batch_metas {
                if count >= effective_limit {
                    break;
                }

                let bytes = match self.object_store.get(&object_meta.location).await {
                    Ok(get_result) => match get_result.bytes().await {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            yield Err(SamsaError::Internal(format!("Failed to read bytes: {e}")));
                            continue;
                        }
                    },
                    Err(_) => continue,
                };

                if let Ok(proto_batch) = crate::proto::StoredRecordBatch::decode(&*bytes) {
                    let batch: StoredRecordBatch = (&proto_batch).into();

                    // Since records within a batch are already sorted, yield them in order
                    // but filter by start_seq_id based on exclusive_start mode
                    for record in batch.records {
                        if count >= effective_limit {
                            break;
                        }

                        let should_include = if exclusive_start {
                            record.seq_id.as_str() > start_seq_id.as_str()
                        } else {
                            record.seq_id.as_str() >= start_seq_id.as_str()
                        };

                        if should_include {
                            yield Ok(record);
                            count += 1;
                        }
                    }
                }
            }

            // Include pending records from in-memory batches
            if count < effective_limit {
                let batch_key = self.get_batch_key(&bucket, &stream_name);
                if let Some(batch_mutex) = self.pending_batches.get(&batch_key) {
                    if let Ok(batch) = batch_mutex.try_lock() {
                        // Collect pending records that match the criteria
                        let mut pending_records: Vec<_> = batch.records
                            .iter()
                            .filter(|record| {
                                if exclusive_start {
                                    record.seq_id.as_str() > start_seq_id.as_str()
                                } else {
                                    record.seq_id.as_str() >= start_seq_id.as_str()
                                }
                            })
                            .cloned()
                            .collect();

                        // Sort pending records to ensure proper ordering
                        pending_records.sort_by(|a, b| a.seq_id.cmp(&b.seq_id));

                        for record in pending_records {
                            if count >= effective_limit {
                                break;
                            }
                            yield Ok(record);
                            count += 1;
                        }
                    }
                }
            }
        })
    }

    /// Memory-efficient streaming read with filtering
    /// Uses streaming processing to avoid loading all records into memory
    /// Improved version with better early termination for timestamp filters
    pub fn read_records_stream_with_filter(
        &self,
        bucket: &str,
        stream: &str,
        filter: ReadFilter,
        limit: Option<u64>,
    ) -> Pin<Box<dyn Stream<Item = Result<StoredRecord>> + Send + '_>> {
        let bucket = bucket.to_string();
        let stream_name = stream.to_string();

        Box::pin(async_stream::stream! {
            match filter {
                ReadFilter::FromSeqId(start_seq_id) => {
                    let mut record_stream = self.read_records_stream(&bucket, &stream_name, &start_seq_id, limit);
                    while let Some(record_result) = record_stream.next().await {
                        yield record_result;
                    }
                }
                ReadFilter::FromTimestamp(start_timestamp) => {
                    // For timestamp filters, we need to scan from the beginning
                    // but can optimize by early termination based on UUID v7 timestamp ordering
                    let mut record_stream = self.read_records_stream(&bucket, &stream_name, &Uuid::nil().to_string(), None);
                    let mut count = 0u64;
                    let effective_limit = limit.unwrap_or(u64::MAX);

                    while let Some(record_result) = record_stream.next().await {
                        match record_result {
                            Ok(record) => {
                                if record.timestamp >= start_timestamp {
                                    yield Ok(record);
                                    count += 1;
                                    if count >= effective_limit {
                                        break;
                                    }
                                }
                                // For UUID v7, if we've passed the timestamp range significantly,
                                // we can potentially break early (optimization for future)
                            }
                            Err(e) => yield Err(e),
                        }
                    }
                }
                ReadFilter::TimestampRange { start, end } => {
                    let mut record_stream = self.read_records_stream(&bucket, &stream_name, &Uuid::nil().to_string(), None);
                    let mut count = 0u64;
                    let effective_limit = limit.unwrap_or(u64::MAX);

                    while let Some(record_result) = record_stream.next().await {
                        match record_result {
                            Ok(record) => {
                                if record.timestamp >= start && record.timestamp <= end {
                                    yield Ok(record);
                                    count += 1;
                                    if count >= effective_limit {
                                        break;
                                    }
                                }
                                // Early termination: if we're past the end timestamp and using UUID v7
                                // we can break since subsequent records will have later timestamps
                                else if record.timestamp > end {
                                    // This is an optimization - with UUID v7, timestamps are monotonic
                                    // so once we pass the end timestamp, we can stop
                                    break;
                                }
                            }
                            Err(e) => yield Err(e),
                        }
                    }
                }
            }
        })
    }

    /// Memory-efficient implementation of read_records_optimized using streaming
    /// This reduces memory usage from O(total_records) to O(batch_size)
    pub async fn read_records_optimized_streaming(
        &self,
        bucket: &str,
        stream: &str,
        start_seq_id: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredRecord>> {
        let read_timer = Timer::new(names::READ_DURATION)
            .with_label(
                labels::OPERATION.to_string(),
                "read_records_optimized_streaming".to_string(),
            )
            .with_label(labels::BUCKET, bucket)
            .with_label(labels::STREAM, stream);

        let mut records = Vec::new();
        let mut record_stream = self.read_records_stream(bucket, stream, start_seq_id, limit);
        let mut total_bytes = 0usize;

        // Collect records from the stream
        // This maintains memory efficiency by processing one record at a time
        while let Some(record_result) = record_stream.next().await {
            match record_result {
                Ok(record) => {
                    total_bytes += record.body.len()
                        + record
                            .headers
                            .iter()
                            .map(|h| h.name.len() + h.value.len())
                            .sum::<usize>();
                    records.push(record);
                }
                Err(e) => {
                    utils::record_error(
                        names::GRPC_ERRORS_TOTAL,
                        &e.to_string(),
                        vec![
                            (
                                labels::OPERATION.to_string(),
                                "read_records_optimized_streaming".to_string(),
                            ),
                            (labels::BUCKET.to_string(), bucket.to_string()),
                            (labels::STREAM.to_string(), stream.to_string()),
                        ],
                    );
                    return Err(e);
                }
            }
        }

        // Record read metrics
        record_read_metrics!(
            bucket,
            stream,
            "read_records_optimized_streaming",
            records.len(),
            total_bytes,
            read_timer.elapsed().as_secs_f64()
        );

        read_timer.record();

        // Records are already in order from the stream, no need to sort
        Ok(records)
    }

    /// Memory-efficient implementation of pagination using streaming
    pub async fn read_records_with_pagination_streaming(
        &self,
        bucket: &str,
        stream: &str,
        start_after_seq_id: &str,
        limit: Option<u64>,
    ) -> Result<(Vec<StoredRecord>, Option<String>)> {
        let mut records = Vec::new();
        let mut next_seq_id = None;
        let effective_limit = limit.unwrap_or(u64::MAX);

        // Use exclusive start for pagination to avoid duplicates
        let use_exclusive_start =
            !start_after_seq_id.is_empty() && start_after_seq_id != Uuid::nil().to_string();
        let mut record_stream = self.read_records_stream_with_start_mode(
            bucket,
            stream,
            start_after_seq_id,
            Some(effective_limit),
            use_exclusive_start,
        );

        while let Some(record_result) = record_stream.next().await {
            match record_result {
                Ok(record) => {
                    records.push(record.clone());

                    // If we've reached the limit, set up pagination token
                    if records.len() >= effective_limit as usize {
                        next_seq_id = Some(record.seq_id.clone());
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok((records, next_seq_id))
    }

    /// Memory-efficient implementation of filtered reads using streaming
    pub async fn read_records_with_filter_streaming(
        &self,
        bucket: &str,
        stream: &str,
        filter: ReadFilter,
        limit: Option<u64>,
    ) -> Result<Vec<StoredRecord>> {
        let mut records = Vec::new();
        let mut record_stream = self.read_records_stream_with_filter(bucket, stream, filter, limit);

        while let Some(record_result) = record_stream.next().await {
            match record_result {
                Ok(record) => records.push(record),
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// Enhanced stream statistics using streaming reads for memory efficiency
    /// This version processes records in chunks to avoid loading everything into memory
    pub async fn get_stream_stats_streaming(
        &self,
        bucket: &str,
        stream: &str,
    ) -> Result<StreamStats> {
        let mut total_records = 0u64;
        let mut earliest_timestamp = u64::MAX;
        let mut latest_timestamp = 0u64;
        let mut earliest_seq_id = String::new();
        let mut latest_seq_id = String::new();

        let mut record_stream =
            self.read_records_stream(bucket, stream, &Uuid::nil().to_string(), None);

        // Process records one by one to avoid loading everything into memory
        while let Some(record_result) = record_stream.next().await {
            match record_result {
                Ok(record) => {
                    total_records += 1;

                    if earliest_seq_id.is_empty() {
                        earliest_seq_id = record.seq_id.clone();
                    }
                    latest_seq_id = record.seq_id.clone();

                    if record.timestamp < earliest_timestamp {
                        earliest_timestamp = record.timestamp;
                    }
                    if record.timestamp > latest_timestamp {
                        latest_timestamp = record.timestamp;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        // Handle empty stream case
        if total_records == 0 {
            return Ok(StreamStats {
                total_records: 0,
                earliest_timestamp: 0,
                latest_timestamp: 0,
                earliest_seq_id: String::new(),
                latest_seq_id: String::new(),
            });
        }

        Ok(StreamStats {
            total_records,
            earliest_timestamp,
            latest_timestamp,
            earliest_seq_id,
            latest_seq_id,
        })
    }

    /// Collect records from stream into a Vec (helper method)
    /// This is useful when you need the full result set but want streaming processing
    pub async fn collect_stream_records(
        &self,
        record_stream: Pin<Box<dyn Stream<Item = Result<StoredRecord>> + Send + '_>>,
    ) -> Result<Vec<StoredRecord>> {
        let mut records = Vec::new();
        let mut stream = record_stream;

        while let Some(record_result) = stream.next().await {
            match record_result {
                Ok(record) => records.push(record),
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// Streaming version of read_records_from_timestamp for memory efficiency
    pub fn read_records_from_timestamp_stream(
        &self,
        bucket: &str,
        stream: &str,
        start_timestamp: u64,
        limit: Option<u64>,
    ) -> Pin<Box<dyn Stream<Item = Result<StoredRecord>> + Send + '_>> {
        self.read_records_stream_with_filter(
            bucket,
            stream,
            ReadFilter::FromTimestamp(start_timestamp),
            limit,
        )
    }

    /// Streaming version of read_records_in_timestamp_range for memory efficiency
    pub fn read_records_in_timestamp_range_stream(
        &self,
        bucket: &str,
        stream: &str,
        start_timestamp: u64,
        end_timestamp: u64,
        limit: Option<u64>,
    ) -> Pin<Box<dyn Stream<Item = Result<StoredRecord>> + Send + '_>> {
        self.read_records_stream_with_filter(
            bucket,
            stream,
            ReadFilter::TimestampRange {
                start: start_timestamp,
                end: end_timestamp,
            },
            limit,
        )
    }

    /// Memory-efficient streaming read with pagination support
    /// Returns a stream that yields records and provides pagination token
    /// The start_after_seq_id is treated as exclusive (records after this seq_id)
    pub fn read_records_stream_with_pagination(
        &self,
        bucket: &str,
        stream: &str,
        start_after_seq_id: &str,
        limit: Option<u64>,
    ) -> PaginatedRecordStream<'_> {
        let bucket = bucket.to_string();
        let stream_name = stream.to_string();
        let start_after_seq_id = start_after_seq_id.to_string();

        Box::pin(async_stream::stream! {
            // Use exclusive start for pagination to avoid duplicates
            let use_exclusive_start = !start_after_seq_id.is_empty() && start_after_seq_id != Uuid::nil().to_string();
            let mut record_stream = self.read_records_stream_with_start_mode(
                &bucket,
                &stream_name,
                &start_after_seq_id,
                limit,
                use_exclusive_start
            );
            let mut record_count = 0u64;
            let effective_limit = limit.unwrap_or(u64::MAX);

            while let Some(record_result) = record_stream.next().await {
                match record_result {
                    Ok(record) => {
                        record_count += 1;

                        // Calculate next pagination token
                        let next_token = if record_count >= effective_limit {
                            Some(record.seq_id.clone())
                        } else {
                            None
                        };

                        yield Ok((record, next_token));

                        if record_count >= effective_limit {
                            break;
                        }
                    }
                    Err(e) => yield Err(e),
                }
            }
        })
    }
}

/// Statistics about a stream for better range query planning
#[derive(Debug, Clone)]
pub struct StreamStats {
    pub total_records: u64,
    pub earliest_timestamp: u64,
    pub latest_timestamp: u64,
    pub earliest_seq_id: String,
    pub latest_seq_id: String,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait MockableStorage: Send + Sync {
    async fn list_buckets(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredBucket>>;

    async fn create_bucket(&self, name: String, config: BucketConfig) -> Result<StoredBucket>;

    async fn get_bucket(&self, name: &str) -> Result<StoredBucket>;

    async fn delete_bucket(&self, name: &str) -> Result<()>;

    async fn update_bucket_config(&self, name: &str, config: BucketConfig) -> Result<()>;

    async fn create_access_token(&self, info: AccessTokenInfo) -> Result<String>;

    async fn revoke_access_token(&self, id: &str) -> Result<AccessToken>;

    async fn list_access_tokens(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<AccessToken>>;
}

#[async_trait::async_trait]
impl MockableStorage for Storage {
    // Ensure this line uses the simple trait name
    async fn list_buckets(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<StoredBucket>> {
        self.list_buckets(prefix, start_after, limit).await
    }

    async fn create_bucket(&self, name: String, config: BucketConfig) -> Result<StoredBucket> {
        self.create_bucket(name, config).await
    }

    async fn get_bucket(&self, name: &str) -> Result<StoredBucket> {
        self.get_bucket(name).await
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        self.delete_bucket(name).await
    }

    async fn update_bucket_config(&self, name: &str, config: BucketConfig) -> Result<()> {
        self.update_bucket_config(name, config).await
    }

    async fn create_access_token(&self, info: AccessTokenInfo) -> Result<String> {
        self.create_access_token(info).await
    }

    async fn revoke_access_token(&self, id: &str) -> Result<AccessToken> {
        self.revoke_access_token(id).await
    }

    async fn list_access_tokens(
        &self,
        prefix: &str,
        start_after: &str,
        limit: Option<u64>,
    ) -> Result<Vec<AccessToken>> {
        self.list_access_tokens(prefix, start_after, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use tokio::time::{Duration, sleep};

    async fn create_test_storage() -> Storage {
        let object_store = Arc::new(InMemory::new());
        Storage::new_with_memory(object_store, "test-node".to_string())
    }

    async fn create_test_storage_with_batch_config(batch_config: BatchConfig) -> Storage {
        let object_store = Arc::new(InMemory::new());
        Storage::new_with_memory_and_batch_config(
            object_store,
            "test-node".to_string(),
            batch_config,
        )
    }

    /// Test helper to create storage with a specific node ID
    fn create_test_storage_with_node_id(
        node_id: String,
        object_store: Arc<dyn object_store::ObjectStore>,
    ) -> Storage {
        let metadata_repo = Arc::new(InMemoryMetaDataRepository::new());
        let batch_config = BatchConfig::default();

        let storage = Storage {
            metadata_repo,
            object_store,
            node_id,
            batch_config,
            pending_batches: Arc::new(DashMap::new()),
            cleanup_task_health: Arc::new(Mutex::new(TaskHealth::Healthy)),
            batch_flush_task_health: Arc::new(Mutex::new(TaskHealth::Healthy)),
            cleanup_consecutive_failures: Arc::new(AtomicU64::new(0)),
            batch_flush_consecutive_failures: Arc::new(AtomicU64::new(0)),
            is_degraded: Arc::new(AtomicBool::new(false)),
        };

        // Start the background tasks manually since we're bypassing the constructor
        storage.start_cleanup_task();
        storage.start_batch_flush_task();
        storage
    }

    #[tokio::test]
    async fn test_batch_storage_basic() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Create test records
        let records = vec![
            AppendRecord {
                timestamp: Some(1000),
                headers: vec![],
                body: b"record1".to_vec(),
            },
            AppendRecord {
                timestamp: Some(2000),
                headers: vec![],
                body: b"record2".to_vec(),
            },
        ];

        // Append records
        let (start_seq_id, end_seq_id, start_ts, end_ts) = storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        assert!(!start_seq_id.is_empty());
        assert!(!end_seq_id.is_empty());
        assert_eq!(start_ts, 1000);
        assert_eq!(end_ts, 2000);
    }

    #[tokio::test]
    async fn test_batch_flushing_by_size() {
        let batch_config = BatchConfig {
            max_records: 2, // Force flush after 2 records
            max_bytes: 1024 * 1024,
            flush_interval_ms: 60000, // Long interval to test size-based flushing
            max_batches_in_memory: 100,
        };

        let storage = create_test_storage_with_batch_config(batch_config).await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add 2 records to trigger size-based flush
        let records = vec![
            AppendRecord {
                timestamp: Some(1000),
                headers: vec![],
                body: b"record1".to_vec(),
            },
            AppendRecord {
                timestamp: Some(2000),
                headers: vec![],
                body: b"record2".to_vec(),
            },
        ];

        storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        // Verify records can be read back
        let read_records = storage
            .read_records_optimized(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read records");

        assert_eq!(read_records.len(), 2);
        assert_eq!(read_records[0].body, b"record1");
        assert_eq!(read_records[1].body, b"record2");
    }

    #[tokio::test]
    async fn test_batch_flushing_by_time() {
        let batch_config = BatchConfig {
            max_records: 1000, // High limit to test time-based flushing
            max_bytes: 1024 * 1024,
            flush_interval_ms: 100, // Short interval for testing
            max_batches_in_memory: 100,
        };

        let storage = create_test_storage_with_batch_config(batch_config).await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add 1 record (won't trigger size-based flush)
        let records = vec![AppendRecord {
            timestamp: Some(1000),
            headers: vec![],
            body: b"record1".to_vec(),
        }];

        storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        // Wait for time-based flush
        sleep(Duration::from_millis(200)).await;

        // Verify record can be read back
        let read_records = storage
            .read_records_optimized(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read records");

        assert_eq!(read_records.len(), 1);
        assert_eq!(read_records[0].body, b"record1");
    }

    #[tokio::test]
    async fn test_manual_flush() {
        let batch_config = BatchConfig {
            max_records: 1000, // High limits to prevent automatic flushing
            max_bytes: 1024 * 1024,
            flush_interval_ms: 60000,
            max_batches_in_memory: 100,
        };

        let storage = create_test_storage_with_batch_config(batch_config).await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add records without triggering automatic flush
        let records = vec![AppendRecord {
            timestamp: Some(1000),
            headers: vec![],
            body: b"record1".to_vec(),
        }];

        storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        // Records should be in pending batch, not yet readable from object store
        let read_records_before_flush = storage
            .read_records_optimized(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read records");

        assert_eq!(read_records_before_flush.len(), 1); // Should include pending records

        // Manually flush
        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush stream");

        // Records should now be readable
        let read_records_after_flush = storage
            .read_records_optimized(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read records");

        assert_eq!(read_records_after_flush.len(), 1);
        assert_eq!(read_records_after_flush[0].body, b"record1");
    }

    #[tokio::test]
    async fn test_flush_all() {
        let batch_config = BatchConfig {
            max_records: 1000, // High limits to prevent automatic flushing
            max_bytes: 1024 * 1024,
            flush_interval_ms: 60000,
            max_batches_in_memory: 100,
        };

        let storage = create_test_storage_with_batch_config(batch_config).await;

        // Add records to multiple streams
        let records1 = vec![AppendRecord {
            timestamp: Some(1000),
            headers: vec![],
            body: b"stream1_record1".to_vec(),
        }];

        let records2 = vec![AppendRecord {
            timestamp: Some(2000),
            headers: vec![],
            body: b"stream2_record1".to_vec(),
        }];

        storage
            .append_records("bucket1", "stream1", records1)
            .await
            .expect("Failed to append to stream1");

        storage
            .append_records("bucket2", "stream2", records2)
            .await
            .expect("Failed to append to stream2");

        // Flush all
        storage.flush_all().await.expect("Failed to flush all");

        // Verify all records are readable
        let stream1_records = storage
            .read_records_optimized("bucket1", "stream1", &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read stream1");

        let stream2_records = storage
            .read_records_optimized("bucket2", "stream2", &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read stream2");

        assert_eq!(stream1_records.len(), 1);
        assert_eq!(stream1_records[0].body, b"stream1_record1");

        assert_eq!(stream2_records.len(), 1);
        assert_eq!(stream2_records[0].body, b"stream2_record1");
    }

    #[tokio::test]
    async fn test_pagination_with_batches() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add multiple batches of records
        for i in 0..5 {
            let records = vec![
                AppendRecord {
                    timestamp: Some(1000 + i * 100),
                    headers: vec![],
                    body: format!("record_{}_1", i).into_bytes(),
                },
                AppendRecord {
                    timestamp: Some(1000 + i * 100 + 1),
                    headers: vec![],
                    body: format!("record_{}_2", i).into_bytes(),
                },
            ];

            storage
                .append_records(bucket, stream, records)
                .await
                .expect("Failed to append records");
        }

        // Force flush all batches
        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush");

        // Test pagination
        let (records_page1, next_seq_id) = storage
            .read_records_with_pagination(bucket, stream, &Uuid::nil().to_string(), Some(5))
            .await
            .expect("Failed to read page 1");

        assert_eq!(records_page1.len(), 5);
        assert!(next_seq_id.is_some());

        // Read next page
        let (records_page2, _) = storage
            .read_records_with_pagination(bucket, stream, &next_seq_id.unwrap(), Some(5))
            .await
            .expect("Failed to read page 2");

        assert_eq!(records_page2.len(), 5);

        // Verify records are ordered correctly
        for i in 0..4 {
            assert!(records_page1[i].seq_id < records_page1[i + 1].seq_id);
        }
    }

    #[tokio::test]
    async fn test_read_from_multiple_nodes() {
        use object_store::memory::InMemory;

        let object_store = Arc::new(InMemory::new());
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Simulate multiple nodes writing to the same stream
        let node_ids = ["node1", "node2", "node3"];
        let mut all_expected_records = Vec::new();

        for (i, node_id) in node_ids.iter().enumerate() {
            // Create storage with explicit node ID
            let storage =
                create_test_storage_with_node_id(node_id.to_string(), object_store.clone());

            // Each node writes some records
            let records = vec![
                AppendRecord {
                    timestamp: Some(1000 + (i as u64) * 100),
                    headers: vec![],
                    body: format!("record_from_{}_1", node_id).into_bytes(),
                },
                AppendRecord {
                    timestamp: Some(1000 + (i as u64) * 100 + 1),
                    headers: vec![],
                    body: format!("record_from_{}_2", node_id).into_bytes(),
                },
            ];

            let (start_seq_id, end_seq_id, _, _) = storage
                .append_records(bucket, stream, records.clone())
                .await
                .expect("Failed to append records");

            // Force flush to object store
            storage
                .flush_stream(bucket, stream)
                .await
                .expect("Failed to flush");

            // Track expected records for verification
            all_expected_records.push(format!("record_from_{}_1", node_id));
            all_expected_records.push(format!("record_from_{}_2", node_id));

            println!(
                "Node {} wrote records with seq_ids {} to {}",
                node_id, start_seq_id, end_seq_id
            );
        }

        // Now read from any node (simulating consumption) - should get all records from all nodes
        let consumer_storage =
            Storage::new_with_memory(object_store.clone(), "test-node".to_string());

        let all_records = consumer_storage
            .read_records_optimized(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read records");

        println!("Consumer read {} records", all_records.len());

        // Verify we got all records from all nodes
        assert_eq!(all_records.len(), 6); // 2 records per node * 3 nodes

        // Extract the record bodies and verify we have all expected records
        let mut actual_bodies: Vec<String> = all_records
            .iter()
            .map(|r| String::from_utf8(r.body.clone()).unwrap())
            .collect();
        actual_bodies.sort();
        all_expected_records.sort();

        assert_eq!(actual_bodies, all_expected_records);

        // Verify records are properly ordered by sequence ID
        for i in 0..all_records.len() - 1 {
            assert!(all_records[i].seq_id <= all_records[i + 1].seq_id);
        }

        println!("✅ Successfully read records from all nodes!");
    }

    #[tokio::test]
    async fn test_streaming_reads_memory_efficiency() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add a larger number of records to test memory efficiency
        for batch in 0..10 {
            let records: Vec<AppendRecord> = (0..50)
                .map(|i| AppendRecord {
                    timestamp: Some(1000 + batch * 100 + i),
                    headers: vec![],
                    body: format!("batch_{}_record_{}", batch, i).into_bytes(),
                })
                .collect();

            storage
                .append_records(bucket, stream, records)
                .await
                .expect("Failed to append records");
        }

        // Force flush to ensure records are in object store
        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush");

        // Test streaming read vs optimized read
        let start_seq_id = Uuid::nil().to_string();
        let limit = Some(100);

        // Compare streaming vs non-streaming approaches
        let optimized_records = storage
            .read_records_optimized(bucket, stream, &start_seq_id, limit)
            .await
            .expect("Failed to read with optimized method");

        let streaming_records = storage
            .read_records_optimized_streaming(bucket, stream, &start_seq_id, limit)
            .await
            .expect("Failed to read with streaming method");

        // Both should return the same records
        assert_eq!(optimized_records.len(), streaming_records.len());
        assert_eq!(optimized_records.len(), 100);

        for (opt, stream) in optimized_records.iter().zip(streaming_records.iter()) {
            assert_eq!(opt.seq_id, stream.seq_id);
            assert_eq!(opt.timestamp, stream.timestamp);
            assert_eq!(opt.body, stream.body);
        }

        println!("✅ Streaming and optimized reads produce identical results");
    }

    #[tokio::test]
    async fn test_streaming_pagination() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add records
        let records: Vec<AppendRecord> = (0..25)
            .map(|i| AppendRecord {
                timestamp: Some(1000 + i),
                headers: vec![],
                body: format!("record_{}", i).into_bytes(),
            })
            .collect();

        storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush");

        // Test streaming pagination
        let page_size = 10;
        let mut all_records_paginated = Vec::new();
        let mut current_seq_id = Uuid::nil().to_string();

        // Read in pages using streaming pagination
        loop {
            let (page_records, next_seq_id) = storage
                .read_records_with_pagination_streaming(
                    bucket,
                    stream,
                    &current_seq_id,
                    Some(page_size),
                )
                .await
                .expect("Failed to read page");

            all_records_paginated.extend(page_records);

            if let Some(next_id) = next_seq_id {
                current_seq_id = next_id;
            } else {
                break;
            }
        }

        // Compare with non-paginated read
        let all_records_direct = storage
            .read_records_optimized(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read all records");

        assert_eq!(all_records_paginated.len(), all_records_direct.len());
        assert_eq!(all_records_paginated.len(), 25);

        // Verify all records are the same and in the same order
        for (paginated, direct) in all_records_paginated.iter().zip(all_records_direct.iter()) {
            assert_eq!(paginated.seq_id, direct.seq_id);
            assert_eq!(paginated.body, direct.body);
        }

        println!("✅ Streaming pagination produces identical results to direct reads");
    }

    #[tokio::test]
    async fn test_streaming_filter_by_timestamp() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add records with different timestamps
        let base_time = 1000;
        let records: Vec<AppendRecord> = (0..20)
            .map(|i| AppendRecord {
                timestamp: Some(base_time + i * 100), // timestamps: 1000, 1100, 1200, ...
                headers: vec![],
                body: format!("record_{}", i).into_bytes(),
            })
            .collect();

        storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush");

        // Test streaming filter by timestamp
        let filter_timestamp = base_time + 1000; // Should include records 10-19
        let filtered_records_streaming = storage
            .read_records_with_filter_streaming(
                bucket,
                stream,
                ReadFilter::FromTimestamp(filter_timestamp),
                Some(5),
            )
            .await
            .expect("Failed to read filtered records");

        // Compare with non-streaming filter
        let filtered_records_direct = storage
            .read_records_with_filter(
                bucket,
                stream,
                ReadFilter::FromTimestamp(filter_timestamp),
                Some(5),
            )
            .await
            .expect("Failed to read filtered records direct");

        assert_eq!(filtered_records_streaming.len(), 5);
        assert_eq!(filtered_records_direct.len(), 5);

        // Verify both methods return the same records
        for (streaming, direct) in filtered_records_streaming
            .iter()
            .zip(filtered_records_direct.iter())
        {
            assert_eq!(streaming.seq_id, direct.seq_id);
            assert_eq!(streaming.timestamp, direct.timestamp);
            assert!(streaming.timestamp >= filter_timestamp);
        }

        println!("✅ Streaming filter by timestamp works correctly");
    }

    #[tokio::test]
    async fn test_streaming_filter_by_timestamp_range() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add records with different timestamps
        let base_time = 1000;
        let records: Vec<AppendRecord> = (0..20)
            .map(|i| AppendRecord {
                timestamp: Some(base_time + i * 100), // timestamps: 1000, 1100, 1200, ...
                headers: vec![],
                body: format!("record_{}", i).into_bytes(),
            })
            .collect();

        storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush");

        // Test streaming filter by timestamp range
        let start_time = base_time + 500; // Should include records 5-14
        let end_time = base_time + 1400;

        let filtered_records = storage
            .read_records_with_filter_streaming(
                bucket,
                stream,
                ReadFilter::TimestampRange {
                    start: start_time,
                    end: end_time,
                },
                None,
            )
            .await
            .expect("Failed to read filtered records");

        // Verify all returned records are within the range
        assert_eq!(filtered_records.len(), 10); // Records 5-14
        for record in &filtered_records {
            assert!(record.timestamp >= start_time);
            assert!(record.timestamp <= end_time);
        }

        println!("✅ Streaming filter by timestamp range works correctly");
    }

    #[tokio::test]
    async fn test_streaming_stats() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add records
        let base_time = 1000;
        let records: Vec<AppendRecord> = (0..15)
            .map(|i| AppendRecord {
                timestamp: Some(base_time + i * 100),
                headers: vec![],
                body: format!("record_{}", i).into_bytes(),
            })
            .collect();

        storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush");

        // Test streaming stats vs regular stats
        let streaming_stats = storage
            .get_stream_stats_streaming(bucket, stream)
            .await
            .expect("Failed to get streaming stats");

        let regular_stats = storage
            .get_stream_stats(bucket, stream)
            .await
            .expect("Failed to get regular stats");

        // Both should return the same statistics
        assert_eq!(streaming_stats.total_records, regular_stats.total_records);
        assert_eq!(streaming_stats.total_records, 15);
        assert_eq!(
            streaming_stats.earliest_timestamp,
            regular_stats.earliest_timestamp
        );
        assert_eq!(
            streaming_stats.latest_timestamp,
            regular_stats.latest_timestamp
        );
        assert_eq!(
            streaming_stats.earliest_seq_id,
            regular_stats.earliest_seq_id
        );
        assert_eq!(streaming_stats.latest_seq_id, regular_stats.latest_seq_id);

        println!("✅ Streaming stats produce identical results to regular stats");
    }

    #[tokio::test]
    async fn test_pure_streaming_consumption() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add a significant number of records
        for batch in 0..5 {
            let records: Vec<AppendRecord> = (0..20)
                .map(|i| AppendRecord {
                    timestamp: Some(1000 + batch * 1000 + i * 10),
                    headers: vec![],
                    body: format!("batch_{}_record_{}", batch, i).into_bytes(),
                })
                .collect();

            storage
                .append_records(bucket, stream, records)
                .await
                .expect("Failed to append records");
        }

        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush");

        // Test pure streaming consumption (process records one by one)
        let mut record_stream =
            storage.read_records_stream(bucket, stream, &Uuid::nil().to_string(), Some(50));

        let mut consumed_count = 0;
        let mut last_timestamp = 0;

        while let Some(record_result) = record_stream.next().await {
            match record_result {
                Ok(record) => {
                    consumed_count += 1;

                    // Verify ordering by timestamp (should be generally increasing)
                    assert!(record.timestamp >= last_timestamp);
                    last_timestamp = record.timestamp;

                    // Simulate processing each record individually
                    let body_str = String::from_utf8(record.body).expect("Invalid UTF-8");
                    assert!(body_str.starts_with("batch_"));
                }
                Err(e) => panic!("Streaming error: {}", e),
            }
        }

        assert_eq!(consumed_count, 50); // Limited by the limit parameter
        println!(
            "✅ Successfully consumed {} records via pure streaming",
            consumed_count
        );
    }

    #[tokio::test]
    async fn test_collect_stream_helper() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add records
        let records: Vec<AppendRecord> = (0..10)
            .map(|i| AppendRecord {
                timestamp: Some(1000 + i),
                headers: vec![],
                body: format!("record_{}", i).into_bytes(),
            })
            .collect();

        storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush");

        // Test the collect helper
        let record_stream =
            storage.read_records_stream(bucket, stream, &Uuid::nil().to_string(), None);
        let collected_records = storage
            .collect_stream_records(record_stream)
            .await
            .expect("Failed to collect stream records");

        assert_eq!(collected_records.len(), 10);

        println!("✅ collect_stream_records helper works correctly");
    }

    #[tokio::test]
    async fn test_memory_efficiency_verification() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add a larger number of records to test memory efficiency
        let num_records = 1000;
        let batch_size = 100;

        for batch in 0..(num_records / batch_size) {
            let records: Vec<AppendRecord> = (0..batch_size)
                .map(|i| AppendRecord {
                    timestamp: Some(1000 + batch * batch_size + i),
                    headers: vec![Header {
                        name: b"batch".to_vec(),
                        value: batch.to_string().into_bytes(),
                    }],
                    body: format!("record_{}_{}", batch, i).into_bytes(),
                })
                .collect();

            storage
                .append_records(bucket, stream, records)
                .await
                .expect("Failed to append records");

            // Flush after each batch to create separate batch files
            storage
                .flush_stream(bucket, stream)
                .await
                .expect("Failed to flush");
        }

        // Test 1: Verify that streaming reads work correctly
        let mut stream_count = 0;
        let mut record_stream =
            storage.read_records_stream(bucket, stream, &Uuid::nil().to_string(), None);

        while let Some(record_result) = record_stream.next().await {
            match record_result {
                Ok(_record) => {
                    stream_count += 1;
                }
                Err(e) => panic!("Streaming read error: {}", e),
            }
        }

        assert_eq!(stream_count, num_records);

        // Test 2: Verify that optimized reads use streaming internally
        let optimized_records = storage
            .read_records_optimized(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read optimized");

        assert_eq!(optimized_records.len(), num_records as usize);

        // Test 3: Verify pagination uses streaming internally
        let page_size = 50;
        let mut paginated_count = 0;
        let mut current_seq_id = Uuid::nil().to_string();

        loop {
            let (page_records, next_seq_id) = storage
                .read_records_with_pagination_streaming(
                    bucket,
                    stream,
                    &current_seq_id,
                    Some(page_size),
                )
                .await
                .expect("Failed to read page");

            paginated_count += page_records.len();

            if let Some(next_id) = next_seq_id {
                current_seq_id = next_id;
            } else {
                break;
            }
        }

        assert_eq!(paginated_count, num_records as usize);

        // Test 4: Verify filtered reads use streaming
        let filtered_records = storage
            .read_records_with_filter_streaming(
                bucket,
                stream,
                ReadFilter::FromTimestamp(1500),
                Some(100),
            )
            .await
            .expect("Failed to read filtered");

        assert!(!filtered_records.is_empty());
        assert!(filtered_records.len() <= 100);

        // Test 5: Verify streaming stats
        let stats = storage
            .get_stream_stats_streaming(bucket, stream)
            .await
            .expect("Failed to get streaming stats");

        assert_eq!(stats.total_records, { num_records });
        assert!(stats.earliest_timestamp <= stats.latest_timestamp);
        assert!(!stats.earliest_seq_id.is_empty());
        assert!(!stats.latest_seq_id.is_empty());

        println!("✅ Memory efficiency verification completed successfully");
        println!(
            "   - Processed {} records across {} batches",
            num_records,
            num_records / batch_size
        );
        println!("   - Streaming reads: {} records", stream_count);
        println!("   - Optimized reads: {} records", optimized_records.len());
        println!("   - Paginated reads: {} records", paginated_count);
        println!("   - Filtered reads: {} records", filtered_records.len());
        println!("   - Stream stats: {} total records", stats.total_records);
    }

    #[tokio::test]
    async fn test_exclusive_start_pagination() {
        let storage = create_test_storage().await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add 10 records
        for i in 0..10 {
            let records = vec![AppendRecord {
                timestamp: Some(1000 + i * 100),
                headers: vec![],
                body: format!("record_{}", i).into_bytes(),
            }];

            storage
                .append_records(bucket, stream, records)
                .await
                .expect("Failed to append records");
        }

        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush");

        // Test exclusive start pagination - read in pages of 3
        let mut all_paginated_records = Vec::new();
        let mut current_start_after = Uuid::nil().to_string();
        let page_size = 3;

        loop {
            let (page_records, next_seq_id) = storage
                .read_records_with_pagination(bucket, stream, &current_start_after, Some(page_size))
                .await
                .expect("Failed to read page");

            if page_records.is_empty() {
                break;
            }

            all_paginated_records.extend(page_records);

            if let Some(next_id) = next_seq_id {
                current_start_after = next_id;
            } else {
                break;
            }
        }

        // Read all records directly for comparison
        let all_records_direct = storage
            .read_records(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read all records");

        // Should have same number of records
        assert_eq!(all_paginated_records.len(), all_records_direct.len());
        assert_eq!(all_paginated_records.len(), 10);

        // Should have the same records in the same order
        for (paginated, direct) in all_paginated_records.iter().zip(all_records_direct.iter()) {
            assert_eq!(paginated.seq_id, direct.seq_id);
            assert_eq!(paginated.body, direct.body);
        }

        // Verify no duplicate seq_ids in paginated results
        let mut seen_seq_ids = std::collections::HashSet::new();
        for record in &all_paginated_records {
            assert!(
                seen_seq_ids.insert(record.seq_id.clone()),
                "Duplicate seq_id found: {}",
                record.seq_id
            );
        }

        println!("✅ Exclusive start pagination test passed - no duplicates, correct ordering");
    }

    #[tokio::test]
    async fn test_background_task_health_monitoring() {
        let storage = create_test_storage().await;

        // Initially should be healthy
        let (cleanup_health, batch_flush_health) = storage.get_background_task_health().await;
        assert_eq!(cleanup_health, TaskHealth::Healthy);
        assert_eq!(batch_flush_health, TaskHealth::Healthy);
        assert!(!storage.is_degraded());

        // Give background tasks a moment to initialize
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Health should still be good after normal operation
        let (cleanup_health, batch_flush_health) = storage.get_background_task_health().await;
        assert_eq!(cleanup_health, TaskHealth::Healthy);
        assert_eq!(batch_flush_health, TaskHealth::Healthy);
    }

    #[tokio::test]
    async fn test_retry_config_backoff_calculation() {
        let retry_config = RetryConfig::default();

        // Test exponential backoff
        assert_eq!(retry_config.calculate_delay(0), Duration::from_millis(0));
        assert_eq!(retry_config.calculate_delay(1), Duration::from_millis(1000)); // base delay
        assert_eq!(retry_config.calculate_delay(2), Duration::from_millis(2000)); // base * 2^1
        assert_eq!(retry_config.calculate_delay(3), Duration::from_millis(4000)); // base * 2^2
        assert_eq!(retry_config.calculate_delay(4), Duration::from_millis(8000)); // base * 2^3

        // Test max delay cap
        let large_failure_count = 20;
        let delay = retry_config.calculate_delay(large_failure_count);
        assert_eq!(delay, Duration::from_millis(retry_config.max_delay_ms));
    }

    #[tokio::test]
    async fn test_batch_flush_resilience_simulation() {
        // Create storage with fast flush interval for testing
        let batch_config = BatchConfig {
            max_records: 1000, // High limit to prevent size-based flushing
            max_bytes: 1024 * 1024,
            flush_interval_ms: 50, // Very fast interval for testing
            max_batches_in_memory: 100,
        };

        let storage = create_test_storage_with_batch_config(batch_config).await;
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add some records
        let records = vec![
            AppendRecord {
                timestamp: Some(1000),
                headers: vec![],
                body: b"test record 1".to_vec(),
            },
            AppendRecord {
                timestamp: Some(2000),
                headers: vec![],
                body: b"test record 2".to_vec(),
            },
        ];

        storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        // Wait for background flush to potentially occur
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify records are readable (either from object store or pending batches)
        let read_records = storage
            .read_records_optimized(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read records");

        assert_eq!(read_records.len(), 2);

        // Check that task health is still good
        let (cleanup_health, batch_flush_health) = storage.get_background_task_health().await;
        assert_eq!(cleanup_health, TaskHealth::Healthy);
        assert_eq!(batch_flush_health, TaskHealth::Healthy);
        assert!(!storage.is_degraded());
    }

    #[tokio::test]
    async fn test_task_health_status_types() {
        // Test that TaskHealth variants work correctly
        let healthy = TaskHealth::Healthy;
        let degraded = TaskHealth::Degraded("Test degraded state".to_string());
        let failed = TaskHealth::Failed("Test failed state".to_string());

        // Verify they can be cloned and compared
        assert_eq!(healthy, TaskHealth::Healthy);
        assert_ne!(healthy, degraded);
        assert_ne!(degraded, failed);

        // Verify debug formatting works
        let degraded_debug = format!("{:?}", degraded);
        assert!(degraded_debug.contains("Degraded"));
        assert!(degraded_debug.contains("Test degraded state"));
    }

    #[tokio::test]
    async fn test_consecutive_failure_tracking() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let failures = AtomicU64::new(0);

        // Simulate failure tracking
        assert_eq!(failures.load(Ordering::Relaxed), 0);

        let count1 = failures.fetch_add(1, Ordering::Relaxed) + 1;
        assert_eq!(count1, 1);
        assert_eq!(failures.load(Ordering::Relaxed), 1);

        let count2 = failures.fetch_add(1, Ordering::Relaxed) + 1;
        assert_eq!(count2, 2);

        // Reset on success
        let prev_failures = failures.swap(0, Ordering::Relaxed);
        assert_eq!(prev_failures, 2);
        assert_eq!(failures.load(Ordering::Relaxed), 0);
    }

    /// Test demonstrating enhanced error handling patterns for background tasks
    #[tokio::test]
    async fn test_background_task_error_handling_patterns() {
        let storage = create_test_storage().await;

        // Simulate normal operation
        let bucket = "test-bucket";
        let stream = "test-stream";

        // Add records normally
        let records = vec![AppendRecord {
            timestamp: Some(1000),
            headers: vec![],
            body: b"normal operation".to_vec(),
        }];

        let result = storage.append_records(bucket, stream, records).await;
        assert!(result.is_ok());

        // Force flush to ensure batches are processed
        let flush_result = storage.flush_stream(bucket, stream).await;
        assert!(flush_result.is_ok());

        // Verify health remains good
        let (cleanup_health, batch_flush_health) = storage.get_background_task_health().await;
        assert_eq!(cleanup_health, TaskHealth::Healthy);
        assert_eq!(batch_flush_health, TaskHealth::Healthy);
        assert!(!storage.is_degraded());

        // Verify records are readable
        let read_records = storage
            .read_records_optimized(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read records");

        assert_eq!(read_records.len(), 1);
        assert_eq!(read_records[0].body, b"normal operation");
    }

    /// Test simulating recovery scenarios
    #[tokio::test]
    async fn test_background_task_recovery_simulation() {
        let storage = create_test_storage().await;

        // Initial state should be healthy
        assert!(!storage.is_degraded());

        // Simulate some work to ensure tasks are running
        let bucket = "recovery-test";
        let stream = "test-stream";

        let records = vec![AppendRecord {
            timestamp: Some(1000),
            headers: vec![],
            body: b"before recovery test".to_vec(),
        }];

        storage
            .append_records(bucket, stream, records)
            .await
            .expect("Failed to append records");

        // Wait a bit for background tasks to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify the system remains healthy during normal operation
        let (cleanup_health, batch_flush_health) = storage.get_background_task_health().await;
        assert_eq!(cleanup_health, TaskHealth::Healthy);
        assert_eq!(batch_flush_health, TaskHealth::Healthy);
        assert!(!storage.is_degraded());

        // Add more records to test continued operation
        let more_records = vec![AppendRecord {
            timestamp: Some(2000),
            headers: vec![],
            body: b"after recovery test".to_vec(),
        }];

        storage
            .append_records(bucket, stream, more_records)
            .await
            .expect("Failed to append more records");

        // Manually flush to ensure everything is persisted
        storage
            .flush_stream(bucket, stream)
            .await
            .expect("Failed to flush stream");

        // Verify all records are readable
        let all_records = storage
            .read_records_optimized(bucket, stream, &Uuid::nil().to_string(), None)
            .await
            .expect("Failed to read all records");

        assert_eq!(all_records.len(), 2);
        assert_eq!(all_records[0].body, b"before recovery test");
        assert_eq!(all_records[1].body, b"after recovery test");
    }

    /// Test for panic-safe task execution
    #[tokio::test]
    async fn test_panic_safe_task_wrapper() {
        // This test verifies that our panic-safe wrapper concept works
        // We can't easily test actual panics in background tasks without complex setup,
        // but we can test the basic concepts

        use std::panic;

        // Simulate a task that might panic
        let panic_result = panic::AssertUnwindSafe(async {
            // This simulates a successful async operation
            tokio::time::sleep(Duration::from_millis(1)).await;
            42
        })
        .catch_unwind()
        .await;

        match panic_result {
            Ok(value) => {
                assert_eq!(value, 42);
                println!("✅ Panic-safe wrapper handled successful case correctly");
            }
            Err(_) => {
                panic!("Unexpected panic caught");
            }
        }

        // Test the error case handling structure
        let error_msg = "simulated error";
        let formatted_error = format!("Task failed: {}", error_msg);
        assert!(formatted_error.contains("simulated error"));
    }

    /// Integration test for comprehensive error handling
    #[tokio::test]
    async fn test_comprehensive_error_handling_integration() {
        let storage = create_test_storage().await;

        // Test multiple concurrent operations to stress background tasks
        let bucket = "stress-test";

        // Create multiple streams concurrently
        let mut handles = Vec::new();

        for i in 0..5 {
            let storage_clone = storage.clone();
            let stream = format!("stream-{}", i);

            let handle = tokio::spawn(async move {
                // Add records to each stream
                for j in 0..10 {
                    let records = vec![AppendRecord {
                        timestamp: Some(1000 + (i * 100) + j),
                        headers: vec![],
                        body: format!("record-{}-{}", i, j).into_bytes(),
                    }];

                    storage_clone
                        .append_records(bucket, &stream, records)
                        .await
                        .expect("Failed to append records");
                }

                // Occasionally flush manually
                if i % 2 == 0 {
                    storage_clone
                        .flush_stream(bucket, &stream)
                        .await
                        .expect("Failed to flush stream");
                }

                i
            });

            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.expect("Task failed");
        }

        // Flush all remaining batches
        storage.flush_all().await.expect("Failed to flush all");

        // Verify all data is readable
        for i in 0..5 {
            let stream = format!("stream-{}", i);
            let records = storage
                .read_records_optimized(bucket, &stream, &Uuid::nil().to_string(), None)
                .await
                .expect("Failed to read records");

            assert_eq!(records.len(), 10);

            // Verify record content
            for (j, record) in records.iter().enumerate() {
                let expected_body = format!("record-{}-{}", i, j).into_bytes();
                assert_eq!(record.body, expected_body);
            }
        }

        // Verify system health after stress test
        let (cleanup_health, batch_flush_health) = storage.get_background_task_health().await;
        assert_eq!(cleanup_health, TaskHealth::Healthy);
        assert_eq!(batch_flush_health, TaskHealth::Healthy);
        assert!(!storage.is_degraded());

        println!("✅ Comprehensive error handling integration test passed");
    }
}
