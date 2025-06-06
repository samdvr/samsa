use metrics::{
    Label, Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};
use std::time::{Duration, Instant};

/// Metric names as constants for consistency and to avoid typos
pub mod names {
    // Storage metrics
    pub const RECORDS_APPENDED_TOTAL: &str = "samsa_records_appended_total";
    pub const BYTES_APPENDED_TOTAL: &str = "samsa_bytes_appended_total";
    pub const BATCHES_FLUSHED_TOTAL: &str = "samsa_batches_flushed_total";
    pub const BATCH_FLUSH_DURATION: &str = "samsa_batch_flush_duration_seconds";
    pub const BATCH_SIZE_RECORDS: &str = "samsa_batch_size_records";
    pub const BATCH_SIZE_BYTES: &str = "samsa_batch_size_bytes";
    pub const PENDING_BATCHES: &str = "samsa_pending_batches";
    pub const PENDING_BATCH_AGE: &str = "samsa_pending_batch_age_seconds";

    // Read operation metrics
    pub const READS_TOTAL: &str = "samsa_reads_total";
    pub const READ_DURATION: &str = "samsa_read_duration_seconds";
    pub const READ_RECORDS_RETURNED: &str = "samsa_read_records_returned";
    pub const READ_BYTES_RETURNED: &str = "samsa_read_bytes_returned";

    // API metrics
    pub const GRPC_REQUESTS_TOTAL: &str = "samsa_grpc_requests_total";
    pub const GRPC_REQUEST_DURATION: &str = "samsa_grpc_request_duration_seconds";
    pub const GRPC_ERRORS_TOTAL: &str = "samsa_grpc_errors_total";

    // Etcd client metrics
    pub const ETCD_OPERATIONS_TOTAL: &str = "samsa_etcd_operations_total";
    pub const ETCD_OPERATION_DURATION: &str = "samsa_etcd_operation_duration_seconds";
    pub const ETCD_ERRORS_TOTAL: &str = "samsa_etcd_errors_total";
    pub const ETCD_CONNECTION_STATUS: &str = "samsa_etcd_connection_status";
    pub const ETCD_HEARTBEAT_FAILURES: &str = "samsa_etcd_heartbeat_failures_total";

    // Object store metrics
    pub const OBJECT_STORE_OPERATIONS_TOTAL: &str = "samsa_object_store_operations_total";
    pub const OBJECT_STORE_OPERATION_DURATION: &str =
        "samsa_object_store_operation_duration_seconds";
    pub const OBJECT_STORE_ERRORS_TOTAL: &str = "samsa_object_store_errors_total";
    pub const OBJECT_STORE_BYTES_WRITTEN: &str = "samsa_object_store_bytes_written_total";
    pub const OBJECT_STORE_BYTES_READ: &str = "samsa_object_store_bytes_read_total";

    // Background task metrics
    pub const BACKGROUND_TASK_ITERATIONS: &str = "samsa_background_task_iterations_total";
    pub const BACKGROUND_TASK_ERRORS: &str = "samsa_background_task_errors_total";
    pub const BACKGROUND_TASK_DURATION: &str = "samsa_background_task_duration_seconds";
    pub const BACKGROUND_TASK_HEALTH: &str = "samsa_background_task_health";

    // System metrics
    pub const NODE_STATUS: &str = "samsa_node_status";
    pub const ACTIVE_CONNECTIONS: &str = "samsa_active_connections";
}

/// Labels used across metrics
pub mod labels {
    pub const BUCKET: &str = "bucket";
    pub const STREAM: &str = "stream";
    pub const NODE_ID: &str = "node_id";
    pub const OPERATION: &str = "operation";
    pub const METHOD: &str = "method";
    pub const STATUS: &str = "status";
    pub const ERROR_TYPE: &str = "error_type";
    pub const TASK_NAME: &str = "task_name";
    pub const CONNECTION_TYPE: &str = "connection_type";
}

/// Initialize all metrics with descriptions
pub fn init_metrics() {
    // Storage metrics
    describe_counter!(
        names::RECORDS_APPENDED_TOTAL,
        Unit::Count,
        "Total number of records appended to streams"
    );
    describe_counter!(
        names::BYTES_APPENDED_TOTAL,
        Unit::Bytes,
        "Total bytes appended to streams"
    );
    describe_counter!(
        names::BATCHES_FLUSHED_TOTAL,
        Unit::Count,
        "Total number of batches flushed to object store"
    );
    describe_histogram!(
        names::BATCH_FLUSH_DURATION,
        Unit::Seconds,
        "Time taken to flush batches to object store"
    );
    describe_histogram!(
        names::BATCH_SIZE_RECORDS,
        Unit::Count,
        "Number of records in flushed batches"
    );
    describe_histogram!(
        names::BATCH_SIZE_BYTES,
        Unit::Bytes,
        "Size in bytes of flushed batches"
    );
    describe_gauge!(
        names::PENDING_BATCHES,
        Unit::Count,
        "Number of pending batches in memory"
    );
    describe_histogram!(
        names::PENDING_BATCH_AGE,
        Unit::Seconds,
        "Age of pending batches in memory"
    );

    // Read operation metrics
    describe_counter!(
        names::READS_TOTAL,
        Unit::Count,
        "Total number of read operations"
    );
    describe_histogram!(
        names::READ_DURATION,
        Unit::Seconds,
        "Duration of read operations"
    );
    describe_histogram!(
        names::READ_RECORDS_RETURNED,
        Unit::Count,
        "Number of records returned by read operations"
    );
    describe_histogram!(
        names::READ_BYTES_RETURNED,
        Unit::Bytes,
        "Bytes returned by read operations"
    );

    // API metrics
    describe_counter!(
        names::GRPC_REQUESTS_TOTAL,
        Unit::Count,
        "Total number of gRPC requests"
    );
    describe_histogram!(
        names::GRPC_REQUEST_DURATION,
        Unit::Seconds,
        "Duration of gRPC requests"
    );
    describe_counter!(
        names::GRPC_ERRORS_TOTAL,
        Unit::Count,
        "Total number of gRPC errors"
    );

    // Etcd client metrics
    describe_counter!(
        names::ETCD_OPERATIONS_TOTAL,
        Unit::Count,
        "Total number of etcd operations"
    );
    describe_histogram!(
        names::ETCD_OPERATION_DURATION,
        Unit::Seconds,
        "Duration of etcd operations"
    );
    describe_counter!(
        names::ETCD_ERRORS_TOTAL,
        Unit::Count,
        "Total number of etcd errors"
    );
    describe_gauge!(
        names::ETCD_CONNECTION_STATUS,
        Unit::Count,
        "Etcd connection status (1 = connected, 0 = disconnected)"
    );
    describe_counter!(
        names::ETCD_HEARTBEAT_FAILURES,
        Unit::Count,
        "Total number of etcd heartbeat failures"
    );

    // Object store metrics
    describe_counter!(
        names::OBJECT_STORE_OPERATIONS_TOTAL,
        Unit::Count,
        "Total number of object store operations"
    );
    describe_histogram!(
        names::OBJECT_STORE_OPERATION_DURATION,
        Unit::Seconds,
        "Duration of object store operations"
    );
    describe_counter!(
        names::OBJECT_STORE_ERRORS_TOTAL,
        Unit::Count,
        "Total number of object store errors"
    );
    describe_counter!(
        names::OBJECT_STORE_BYTES_WRITTEN,
        Unit::Bytes,
        "Total bytes written to object store"
    );
    describe_counter!(
        names::OBJECT_STORE_BYTES_READ,
        Unit::Bytes,
        "Total bytes read from object store"
    );

    // Background task metrics
    describe_counter!(
        names::BACKGROUND_TASK_ITERATIONS,
        Unit::Count,
        "Total number of background task iterations"
    );
    describe_counter!(
        names::BACKGROUND_TASK_ERRORS,
        Unit::Count,
        "Total number of background task errors"
    );
    describe_histogram!(
        names::BACKGROUND_TASK_DURATION,
        Unit::Seconds,
        "Duration of background task iterations"
    );
    describe_gauge!(
        names::BACKGROUND_TASK_HEALTH,
        Unit::Count,
        "Background task health status (1 = healthy, 0 = unhealthy)"
    );

    // System metrics
    describe_gauge!(
        names::NODE_STATUS,
        Unit::Count,
        "Node status (1 = ready, 0 = not ready)"
    );
    describe_gauge!(
        names::ACTIVE_CONNECTIONS,
        Unit::Count,
        "Number of active connections"
    );
}

/// Helper struct for timing operations
pub struct Timer {
    start: Instant,
    metric_name: &'static str,
    labels: Vec<(String, String)>,
}

impl Timer {
    pub fn new(metric_name: &'static str) -> Self {
        Self {
            start: Instant::now(),
            metric_name,
            labels: Vec::new(),
        }
    }

    pub fn with_label<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }

    pub fn with_labels(mut self, labels: Vec<(String, String)>) -> Self {
        self.labels.extend(labels);
        self
    }

    pub fn record(self) {
        let duration = self.start.elapsed().as_secs_f64();

        if self.labels.is_empty() {
            histogram!(self.metric_name).record(duration);
        } else {
            // Convert to Label structs which work better with metrics macros
            let labels: Vec<Label> = self
                .labels
                .into_iter()
                .map(|(k, v)| Label::new(k, v))
                .collect();
            histogram!(self.metric_name, labels).record(duration);
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

/// Utility functions for common metric operations
pub mod utils {
    use super::*;

    /// Record a counter metric with labels
    pub fn record_counter(name: &'static str, value: u64, labels: Vec<(String, String)>) {
        if labels.is_empty() {
            counter!(name).increment(value);
        } else {
            // Convert to Label structs which work better with metrics macros
            let labels: Vec<Label> = labels.into_iter().map(|(k, v)| Label::new(k, v)).collect();
            counter!(name, labels).increment(value);
        }
    }

    /// Record a gauge metric with labels
    pub fn record_gauge(name: &'static str, value: f64, labels: Vec<(String, String)>) {
        if labels.is_empty() {
            gauge!(name).set(value);
        } else {
            // Convert to Label structs which work better with metrics macros
            let labels: Vec<Label> = labels.into_iter().map(|(k, v)| Label::new(k, v)).collect();
            gauge!(name, labels).set(value);
        }
    }

    /// Record a histogram metric with labels
    pub fn record_histogram(name: &'static str, value: f64, labels: Vec<(String, String)>) {
        if labels.is_empty() {
            histogram!(name).record(value);
        } else {
            // Convert to Label structs which work better with metrics macros
            let labels: Vec<Label> = labels.into_iter().map(|(k, v)| Label::new(k, v)).collect();
            histogram!(name, labels).record(value);
        }
    }

    /// Record an error metric
    pub fn record_error(
        metric_name: &'static str,
        error_type: &str,
        labels: Vec<(String, String)>,
    ) {
        let mut all_labels = labels;
        all_labels.push((labels::ERROR_TYPE.to_string(), error_type.to_string()));
        record_counter(metric_name, 1, all_labels);
    }

    /// Record metrics for an operation result
    pub fn record_operation<T, E>(
        name: &'static str,
        _duration_metric: &'static str,
        error_metric: &'static str,
        operation: &str,
        result: &Result<T, E>,
        labels: Vec<(String, String)>,
    ) where
        E: std::fmt::Display,
    {
        let mut operation_labels = labels.clone();
        operation_labels.push((labels::OPERATION.to_string(), operation.to_string()));

        match result {
            Ok(_) => {
                operation_labels.push((labels::STATUS.to_string(), "success".to_string()));
                record_counter(name, 1, operation_labels);
            }
            Err(e) => {
                operation_labels.push((labels::STATUS.to_string(), "error".to_string()));
                record_counter(name, 1, operation_labels);
                record_error(error_metric, &e.to_string(), labels);
            }
        }
    }
}

/// Macros for common metric patterns
#[macro_export]
macro_rules! record_append_metrics {
    ($bucket:expr, $stream:expr, $records:expr, $bytes:expr) => {
        $crate::common::metrics::utils::record_counter(
            $crate::common::metrics::names::RECORDS_APPENDED_TOTAL,
            $records as u64,
            vec![
                (
                    $crate::common::metrics::labels::BUCKET.to_string(),
                    $bucket.to_string(),
                ),
                (
                    $crate::common::metrics::labels::STREAM.to_string(),
                    $stream.to_string(),
                ),
            ],
        );
        $crate::common::metrics::utils::record_counter(
            $crate::common::metrics::names::BYTES_APPENDED_TOTAL,
            $bytes as u64,
            vec![
                (
                    $crate::common::metrics::labels::BUCKET.to_string(),
                    $bucket.to_string(),
                ),
                (
                    $crate::common::metrics::labels::STREAM.to_string(),
                    $stream.to_string(),
                ),
            ],
        );
    };
}

#[macro_export]
macro_rules! record_read_metrics {
    ($bucket:expr, $stream:expr, $operation:expr, $records:expr, $bytes:expr, $duration:expr) => {
        $crate::common::metrics::utils::record_counter(
            $crate::common::metrics::names::READS_TOTAL,
            1,
            vec![
                (
                    $crate::common::metrics::labels::BUCKET.to_string(),
                    $bucket.to_string(),
                ),
                (
                    $crate::common::metrics::labels::STREAM.to_string(),
                    $stream.to_string(),
                ),
                (
                    $crate::common::metrics::labels::OPERATION.to_string(),
                    $operation.to_string(),
                ),
            ],
        );
        $crate::common::metrics::utils::record_histogram(
            $crate::common::metrics::names::READ_DURATION,
            $duration,
            vec![
                (
                    $crate::common::metrics::labels::BUCKET.to_string(),
                    $bucket.to_string(),
                ),
                (
                    $crate::common::metrics::labels::STREAM.to_string(),
                    $stream.to_string(),
                ),
                (
                    $crate::common::metrics::labels::OPERATION.to_string(),
                    $operation.to_string(),
                ),
            ],
        );
        $crate::common::metrics::utils::record_histogram(
            $crate::common::metrics::names::READ_RECORDS_RETURNED,
            $records as f64,
            vec![
                (
                    $crate::common::metrics::labels::BUCKET.to_string(),
                    $bucket.to_string(),
                ),
                (
                    $crate::common::metrics::labels::STREAM.to_string(),
                    $stream.to_string(),
                ),
            ],
        );
        $crate::common::metrics::utils::record_histogram(
            $crate::common::metrics::names::READ_BYTES_RETURNED,
            $bytes as f64,
            vec![
                (
                    $crate::common::metrics::labels::BUCKET.to_string(),
                    $bucket.to_string(),
                ),
                (
                    $crate::common::metrics::labels::STREAM.to_string(),
                    $stream.to_string(),
                ),
            ],
        );
    };
}

#[macro_export]
macro_rules! time_operation {
    ($metric_name:expr, $labels:expr, $operation:block) => {{
        let timer = $crate::common::metrics::Timer::new($metric_name).with_labels($labels);
        let result = $operation;
        timer.record();
        result
    }};
}

/// Initialize metrics exporter (call this once at startup)
pub async fn init_metrics_exporter(
    prometheus_port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use metrics_exporter_prometheus::PrometheusBuilder;

    let builder = PrometheusBuilder::new();
    builder
        .with_http_listener(([0, 0, 0, 0], prometheus_port))
        .install()?;

    // Initialize all metric descriptions
    init_metrics();

    tracing::info!("Metrics exporter initialized on port {}", prometheus_port);
    Ok(())
}
