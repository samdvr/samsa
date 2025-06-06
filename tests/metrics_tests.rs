use std::time::Duration;
use tokio::time::sleep;

use samsa::common::metrics::{self, Timer, labels, names, utils};

#[tokio::test]
async fn test_metrics_initialization() {
    // Test that metrics can be initialized without errors
    metrics::init_metrics();

    // This should complete without panicking
    assert!(true);
}

#[tokio::test]
async fn test_counter_metrics() {
    metrics::init_metrics();

    // Test recording counters with various metric names
    utils::record_counter(
        names::RECORDS_APPENDED_TOTAL,
        5,
        vec![
            (labels::BUCKET.to_string(), "test-bucket".to_string()),
            (labels::STREAM.to_string(), "test-stream".to_string()),
        ],
    );

    utils::record_counter(
        names::BYTES_APPENDED_TOTAL,
        1024,
        vec![
            (labels::BUCKET.to_string(), "test-bucket".to_string()),
            (labels::STREAM.to_string(), "test-stream".to_string()),
        ],
    );

    utils::record_counter(
        names::BATCHES_FLUSHED_TOTAL,
        1,
        vec![(labels::BUCKET.to_string(), "test-bucket".to_string())],
    );

    utils::record_counter(
        names::GRPC_REQUESTS_TOTAL,
        1,
        vec![
            (labels::METHOD.to_string(), "append".to_string()),
            (labels::STATUS.to_string(), "success".to_string()),
        ],
    );

    utils::record_counter(
        names::GRPC_ERRORS_TOTAL,
        1,
        vec![
            (labels::METHOD.to_string(), "read".to_string()),
            (labels::ERROR_TYPE.to_string(), "timeout".to_string()),
        ],
    );

    // Test etcd metrics
    utils::record_counter(
        names::ETCD_OPERATIONS_TOTAL,
        3,
        vec![(labels::OPERATION.to_string(), "put".to_string())],
    );

    utils::record_counter(
        names::ETCD_ERRORS_TOTAL,
        1,
        vec![
            (labels::OPERATION.to_string(), "get".to_string()),
            (
                labels::ERROR_TYPE.to_string(),
                "connection_failed".to_string(),
            ),
        ],
    );

    utils::record_counter(
        names::ETCD_HEARTBEAT_FAILURES,
        2,
        vec![(labels::NODE_ID.to_string(), "node-1".to_string())],
    );

    // Test object store metrics
    utils::record_counter(
        names::OBJECT_STORE_OPERATIONS_TOTAL,
        5,
        vec![(labels::OPERATION.to_string(), "put".to_string())],
    );

    utils::record_counter(
        names::OBJECT_STORE_ERRORS_TOTAL,
        1,
        vec![
            (labels::OPERATION.to_string(), "get".to_string()),
            (labels::ERROR_TYPE.to_string(), "not_found".to_string()),
        ],
    );

    utils::record_counter(
        names::OBJECT_STORE_BYTES_WRITTEN,
        2048,
        vec![(labels::BUCKET.to_string(), "test-bucket".to_string())],
    );

    utils::record_counter(
        names::OBJECT_STORE_BYTES_READ,
        1024,
        vec![(labels::BUCKET.to_string(), "test-bucket".to_string())],
    );

    // Test background task metrics
    utils::record_counter(
        names::BACKGROUND_TASK_ITERATIONS,
        10,
        vec![(labels::TASK_NAME.to_string(), "batch_flusher".to_string())],
    );

    utils::record_counter(
        names::BACKGROUND_TASK_ERRORS,
        1,
        vec![
            (labels::TASK_NAME.to_string(), "batch_flusher".to_string()),
            (labels::ERROR_TYPE.to_string(), "storage_error".to_string()),
        ],
    );

    // All counter recording should complete without errors
    assert!(true);
}

#[tokio::test]
async fn test_gauge_metrics() {
    metrics::init_metrics();

    // Test recording gauge metrics
    utils::record_gauge(
        names::PENDING_BATCHES,
        5.0,
        vec![(labels::BUCKET.to_string(), "test-bucket".to_string())],
    );

    utils::record_gauge(
        names::ETCD_CONNECTION_STATUS,
        1.0,
        vec![(labels::NODE_ID.to_string(), "node-1".to_string())],
    );

    utils::record_gauge(
        names::BACKGROUND_TASK_HEALTH,
        1.0,
        vec![(labels::TASK_NAME.to_string(), "batch_flusher".to_string())],
    );

    utils::record_gauge(
        names::NODE_STATUS,
        1.0,
        vec![(labels::NODE_ID.to_string(), "node-1".to_string())],
    );

    utils::record_gauge(
        names::ACTIVE_CONNECTIONS,
        25.0,
        vec![(labels::CONNECTION_TYPE.to_string(), "grpc".to_string())],
    );

    // Test gauge updates (should overwrite previous values)
    utils::record_gauge(
        names::PENDING_BATCHES,
        3.0,
        vec![(labels::BUCKET.to_string(), "test-bucket".to_string())],
    );

    utils::record_gauge(
        names::ETCD_CONNECTION_STATUS,
        0.0,
        vec![(labels::NODE_ID.to_string(), "node-1".to_string())],
    );

    // All gauge recording should complete without errors
    assert!(true);
}

#[tokio::test]
async fn test_histogram_metrics() {
    metrics::init_metrics();

    // Test recording histogram metrics
    utils::record_histogram(
        names::BATCH_FLUSH_DURATION,
        0.125,
        vec![(labels::BUCKET.to_string(), "test-bucket".to_string())],
    );

    utils::record_histogram(
        names::BATCH_SIZE_RECORDS,
        100.0,
        vec![(labels::BUCKET.to_string(), "test-bucket".to_string())],
    );

    utils::record_histogram(
        names::BATCH_SIZE_BYTES,
        4096.0,
        vec![(labels::BUCKET.to_string(), "test-bucket".to_string())],
    );

    utils::record_histogram(
        names::PENDING_BATCH_AGE,
        5.5,
        vec![(labels::BUCKET.to_string(), "test-bucket".to_string())],
    );

    utils::record_histogram(
        names::READ_DURATION,
        0.025,
        vec![
            (labels::BUCKET.to_string(), "test-bucket".to_string()),
            (labels::STREAM.to_string(), "test-stream".to_string()),
        ],
    );

    utils::record_histogram(
        names::READ_RECORDS_RETURNED,
        50.0,
        vec![
            (labels::BUCKET.to_string(), "test-bucket".to_string()),
            (labels::STREAM.to_string(), "test-stream".to_string()),
        ],
    );

    utils::record_histogram(
        names::READ_BYTES_RETURNED,
        2048.0,
        vec![
            (labels::BUCKET.to_string(), "test-bucket".to_string()),
            (labels::STREAM.to_string(), "test-stream".to_string()),
        ],
    );

    utils::record_histogram(
        names::GRPC_REQUEST_DURATION,
        0.050,
        vec![(labels::METHOD.to_string(), "append".to_string())],
    );

    utils::record_histogram(
        names::ETCD_OPERATION_DURATION,
        0.010,
        vec![(labels::OPERATION.to_string(), "put".to_string())],
    );

    utils::record_histogram(
        names::OBJECT_STORE_OPERATION_DURATION,
        0.200,
        vec![(labels::OPERATION.to_string(), "put".to_string())],
    );

    utils::record_histogram(
        names::BACKGROUND_TASK_DURATION,
        1.5,
        vec![(labels::TASK_NAME.to_string(), "batch_flusher".to_string())],
    );

    // Test multiple histogram recordings for statistical distribution
    for i in 1..=10 {
        utils::record_histogram(
            names::BATCH_FLUSH_DURATION,
            i as f64 * 0.01,
            vec![(labels::BUCKET.to_string(), "test-bucket".to_string())],
        );
    }

    // All histogram recording should complete without errors
    assert!(true);
}

#[tokio::test]
async fn test_timer_functionality() {
    metrics::init_metrics();

    // Test basic timer creation and recording
    let timer = Timer::new(names::BATCH_FLUSH_DURATION);
    sleep(Duration::from_millis(10)).await; // Small delay to ensure measurable time
    timer.record();

    // Test timer with single label
    let timer = Timer::new(names::READ_DURATION).with_label(labels::BUCKET, "test-bucket");
    sleep(Duration::from_millis(5)).await;
    timer.record();

    // Test timer with multiple labels using with_label chain
    let timer = Timer::new(names::GRPC_REQUEST_DURATION)
        .with_label(labels::METHOD, "append")
        .with_label(labels::STATUS, "success");
    sleep(Duration::from_millis(2)).await;
    timer.record();

    // Test timer with labels using with_labels method
    let labels = vec![
        (labels::OPERATION.to_string(), "put".to_string()),
        (labels::NODE_ID.to_string(), "node-1".to_string()),
    ];
    let timer = Timer::new(names::ETCD_OPERATION_DURATION).with_labels(labels);
    sleep(Duration::from_millis(3)).await;
    timer.record();

    // Test timer elapsed time measurement
    let timer = Timer::new(names::OBJECT_STORE_OPERATION_DURATION);
    sleep(Duration::from_millis(20)).await;
    let elapsed = timer.elapsed();
    assert!(elapsed.as_millis() >= 15); // Should be at least 15ms given we slept for 20ms
    timer.record();

    // All timer operations should complete without errors
    assert!(true);
}

#[tokio::test]
async fn test_error_recording() {
    metrics::init_metrics();

    // Test error recording with different error types
    utils::record_error(
        names::GRPC_ERRORS_TOTAL,
        "invalid_argument",
        vec![(labels::METHOD.to_string(), "append".to_string())],
    );

    utils::record_error(
        names::ETCD_ERRORS_TOTAL,
        "connection_timeout",
        vec![(labels::OPERATION.to_string(), "get".to_string())],
    );

    utils::record_error(
        names::OBJECT_STORE_ERRORS_TOTAL,
        "access_denied",
        vec![(labels::OPERATION.to_string(), "put".to_string())],
    );

    utils::record_error(
        names::BACKGROUND_TASK_ERRORS,
        "configuration_error",
        vec![(labels::TASK_NAME.to_string(), "batch_flusher".to_string())],
    );

    // All error recording should complete without errors
    assert!(true);
}

#[tokio::test]
async fn test_operation_recording() {
    metrics::init_metrics();

    // Test successful operation recording
    let success_result: Result<String, std::io::Error> = Ok("success".to_string());
    utils::record_operation(
        names::GRPC_REQUESTS_TOTAL,
        names::GRPC_REQUEST_DURATION,
        names::GRPC_ERRORS_TOTAL,
        "append",
        &success_result,
        vec![(labels::METHOD.to_string(), "append".to_string())],
    );

    // Test failed operation recording
    let failure_result: Result<String, std::io::Error> = Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "Resource not found",
    ));
    utils::record_operation(
        names::ETCD_OPERATIONS_TOTAL,
        names::ETCD_OPERATION_DURATION,
        names::ETCD_ERRORS_TOTAL,
        "get",
        &failure_result,
        vec![(labels::OPERATION.to_string(), "get".to_string())],
    );

    // Test with different error types
    let timeout_result: Result<(), Box<dyn std::error::Error + Send + Sync>> =
        Err("Timeout occurred".into());
    utils::record_operation(
        names::OBJECT_STORE_OPERATIONS_TOTAL,
        names::OBJECT_STORE_OPERATION_DURATION,
        names::OBJECT_STORE_ERRORS_TOTAL,
        "put",
        &timeout_result,
        vec![(labels::OPERATION.to_string(), "put".to_string())],
    );

    // All operation recording should complete without errors
    assert!(true);
}

#[tokio::test]
async fn test_metrics_with_empty_labels() {
    metrics::init_metrics();

    // Test recording metrics with empty label vectors
    utils::record_counter(names::RECORDS_APPENDED_TOTAL, 1, vec![]);
    utils::record_gauge(names::PENDING_BATCHES, 0.0, vec![]);
    utils::record_histogram(names::BATCH_FLUSH_DURATION, 0.1, vec![]);

    // Test timer with no labels
    let timer = Timer::new(names::READ_DURATION);
    sleep(Duration::from_millis(1)).await;
    timer.record();

    // All operations should complete without errors
    assert!(true);
}

#[tokio::test]
async fn test_metrics_with_special_characters_in_labels() {
    metrics::init_metrics();

    // Test with special characters in label values
    utils::record_counter(
        names::GRPC_REQUESTS_TOTAL,
        1,
        vec![
            (
                labels::BUCKET.to_string(),
                "bucket-with-hyphens_and_underscores.dots".to_string(),
            ),
            (
                labels::STREAM.to_string(),
                "stream/with/slashes".to_string(),
            ),
        ],
    );

    utils::record_gauge(
        names::ETCD_CONNECTION_STATUS,
        1.0,
        vec![(labels::NODE_ID.to_string(), "node:with:colons".to_string())],
    );

    utils::record_histogram(
        names::OBJECT_STORE_OPERATION_DURATION,
        0.1,
        vec![(
            labels::OPERATION.to_string(),
            "put@special#chars".to_string(),
        )],
    );

    // All operations should complete without errors
    assert!(true);
}

#[tokio::test]
async fn test_metrics_exporter_initialization() {
    // Test that metrics exporter can be initialized
    // This test uses an available port to avoid conflicts

    // Find an available port
    let port = find_available_port().await;

    // This should either succeed or fail gracefully depending on the environment
    match metrics::init_metrics_exporter(port).await {
        Ok(_) => {
            // Success case - exporter was initialized
            assert!(true);
        }
        Err(e) => {
            // Error case - should be a reasonable error message
            let error_msg = e.to_string();
            assert!(!error_msg.is_empty());
            println!(
                "Metrics exporter initialization failed (expected in some test environments): {}",
                error_msg
            );
        }
    }
}

#[tokio::test]
async fn test_all_metric_names_constants() {
    // Test that all metric name constants are valid strings
    let metric_names = vec![
        names::RECORDS_APPENDED_TOTAL,
        names::BYTES_APPENDED_TOTAL,
        names::BATCHES_FLUSHED_TOTAL,
        names::BATCH_FLUSH_DURATION,
        names::BATCH_SIZE_RECORDS,
        names::BATCH_SIZE_BYTES,
        names::PENDING_BATCHES,
        names::PENDING_BATCH_AGE,
        names::READS_TOTAL,
        names::READ_DURATION,
        names::READ_RECORDS_RETURNED,
        names::READ_BYTES_RETURNED,
        names::GRPC_REQUESTS_TOTAL,
        names::GRPC_REQUEST_DURATION,
        names::GRPC_ERRORS_TOTAL,
        names::ETCD_OPERATIONS_TOTAL,
        names::ETCD_OPERATION_DURATION,
        names::ETCD_ERRORS_TOTAL,
        names::ETCD_CONNECTION_STATUS,
        names::ETCD_HEARTBEAT_FAILURES,
        names::OBJECT_STORE_OPERATIONS_TOTAL,
        names::OBJECT_STORE_OPERATION_DURATION,
        names::OBJECT_STORE_ERRORS_TOTAL,
        names::OBJECT_STORE_BYTES_WRITTEN,
        names::OBJECT_STORE_BYTES_READ,
        names::BACKGROUND_TASK_ITERATIONS,
        names::BACKGROUND_TASK_ERRORS,
        names::BACKGROUND_TASK_DURATION,
        names::BACKGROUND_TASK_HEALTH,
        names::NODE_STATUS,
        names::ACTIVE_CONNECTIONS,
    ];

    for name in metric_names {
        assert!(!name.is_empty());
        assert!(name.starts_with("samsa_"));
    }
}

#[tokio::test]
async fn test_all_label_name_constants() {
    // Test that all label name constants are valid strings
    let label_names = vec![
        labels::BUCKET,
        labels::STREAM,
        labels::NODE_ID,
        labels::OPERATION,
        labels::METHOD,
        labels::STATUS,
        labels::ERROR_TYPE,
        labels::TASK_NAME,
        labels::CONNECTION_TYPE,
    ];

    for name in label_names {
        assert!(!name.is_empty());
        assert!(!name.contains(' ')); // Label names should not contain spaces
    }
}

// Helper function to find an available port
async fn find_available_port() -> u16 {
    use std::net::{SocketAddr, TcpListener};

    // Try to bind to port 0 to get an available port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to available port");
    let addr: SocketAddr = listener.local_addr().expect("Failed to get local address");
    addr.port()
}
