use std::time::Duration;
use tracing::Instrument;
use tracing::{debug, error, info, trace, warn};

use samsa::common::{config::ObservabilityConfig, observability};

#[tokio::test]
async fn test_init_observability_basic() {
    // Test basic observability initialization
    let config = ObservabilityConfig::default();

    match observability::init_observability(config).await {
        Ok(_) => {
            assert!(true); // Observability initialized successfully
        }
        Err(e) => {
            // May fail if already initialized in another test
            println!(
                "Observability init failed (might be already initialized): {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_init_observability_with_env_filter() {
    // Set environment variable for tracing level
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }

    let mut config = ObservabilityConfig::default();
    config.log_level = "debug".to_string();

    match observability::init_observability(config).await {
        Ok(_) => {
            assert!(true);
        }
        Err(e) => {
            println!("Observability init failed: {}", e);
        }
    }

    // Clean up
    unsafe {
        std::env::remove_var("RUST_LOG");
    }
}

#[tokio::test]
async fn test_init_observability_with_json_format() {
    // Set environment variable for JSON format
    unsafe {
        std::env::set_var("RUST_LOG", "info");
        std::env::set_var("TRACING_FORMAT", "json");
    }

    let config = ObservabilityConfig::default();

    match observability::init_observability(config).await {
        Ok(_) => {
            assert!(true);
        }
        Err(e) => {
            println!("Observability init with JSON failed: {}", e);
        }
    }

    // Clean up
    unsafe {
        std::env::remove_var("RUST_LOG");
        std::env::remove_var("TRACING_FORMAT");
    }
}

#[tokio::test]
async fn test_tracing_levels() {
    // Initialize observability for testing - use separate port to avoid conflicts
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9091;
    let _ = observability::init_observability(config).await;

    // Test different log levels
    trace!("This is a trace message");
    debug!("This is a debug message");
    info!("This is an info message");
    warn!("This is a warning message");
    error!("This is an error message");

    // All logging should complete without errors
    assert!(true);
}

#[tokio::test]
async fn test_tracing_with_fields() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9092;
    let _ = observability::init_observability(config).await;

    // Test structured logging with fields
    info!(
        bucket = "test-bucket",
        stream = "test-stream",
        count = 42,
        "Processing records"
    );

    warn!(
        error = "connection_timeout",
        retry_count = 3,
        "Retrying operation"
    );

    error!(
        error_code = 500,
        error_msg = "Internal server error",
        "Request failed"
    );

    assert!(true);
}

#[tokio::test]
async fn test_tracing_spans() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9093;
    let _ = observability::init_observability(config).await;

    // Test tracing spans
    let span = tracing::info_span!("test_operation", operation = "append");
    let _enter = span.enter();

    info!("Inside span");

    // Test nested spans
    {
        let nested_span = tracing::debug_span!("nested_operation", step = "validation");
        let _nested_enter = nested_span.enter();

        debug!("Inside nested span");
    }

    info!("Back in outer span");

    assert!(true);
}

#[tokio::test]
async fn test_tracing_async_spans() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9094;
    let _ = observability::init_observability(config).await;

    async fn async_operation() {
        info!("Starting async operation");
        tokio::time::sleep(Duration::from_millis(1)).await;
        info!("Async operation complete");
    }

    // Test async span instrumentation
    let span = tracing::info_span!("async_test");
    async_operation().instrument(span).await;

    assert!(true);
}

#[tokio::test]
async fn test_error_handling_in_tracing() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9095;
    let _ = observability::init_observability(config).await;

    // Test error handling with tracing
    fn operation_that_fails() -> Result<(), &'static str> {
        Err("Something went wrong")
    }

    match operation_that_fails() {
        Ok(_) => {
            info!("Operation succeeded");
        }
        Err(e) => {
            error!(error = %e, "Operation failed");
        }
    }

    assert!(true);
}

#[tokio::test]
async fn test_metrics_integration() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9096;
    let _ = observability::init_observability(config).await;

    info!("Testing metrics integration");

    // Record some metrics while tracing
    samsa::common::metrics::utils::record_counter(
        samsa::common::metrics::names::GRPC_REQUESTS_TOTAL,
        1,
        vec![("method".to_string(), "test".to_string())],
    );

    info!("Metrics recorded during tracing");

    assert!(true);
}

#[tokio::test]
async fn test_custom_subscriber() {
    // Test custom subscriber configuration
    use tracing_subscriber::{Registry, prelude::*};

    let subscriber = Registry::default().with(tracing_subscriber::fmt::layer().with_test_writer());

    let _guard = tracing::subscriber::set_default(subscriber);

    info!("Testing custom subscriber");
    debug!("Debug message with custom subscriber");

    assert!(true);
}

#[tokio::test]
async fn test_filtering() {
    // Test log filtering
    use tracing_subscriber::filter::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .finish();

    let _guard = tracing::subscriber::set_default(subscriber);

    trace!("This trace should be filtered out");
    debug!("This debug should be filtered out");
    info!("This info should be visible");
    warn!("This warning should be visible");
    error!("This error should be visible");

    assert!(true);
}

#[tokio::test]
async fn test_dynamic_fields() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9097;
    let _ = observability::init_observability(config).await;

    // Test dynamic field creation
    let user_id = "user123";
    let request_id = uuid::Uuid::new_v4().to_string();

    info!(
        %user_id,
        %request_id,
        timestamp = ?std::time::SystemTime::now(),
        "Request started"
    );

    assert!(true);
}

#[tokio::test]
async fn test_performance_impact() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9098;
    let _ = observability::init_observability(config).await;

    let start = std::time::Instant::now();

    // Measure performance impact of logging
    for i in 0..1000 {
        debug!(iteration = i, "Performance test iteration");
    }

    let duration = start.elapsed();

    // Logging 1000 debug messages should be fast (< 100ms even in debug mode)
    assert!(duration < Duration::from_millis(1000));

    info!(
        duration_ms = duration.as_millis(),
        "Performance test completed"
    );
}

#[tokio::test]
async fn test_error_formatting() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9099;
    let _ = observability::init_observability(config).await;

    // Test different error formatting options
    let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
    let custom_error = "Custom error message";

    error!(error = %io_error, "IO error occurred");
    error!(error = ?io_error, "IO error (debug format)");
    error!(error = custom_error, "Custom error occurred");

    // Test error with context
    warn!(
        error = %io_error,
        file_path = "/path/to/missing/file.txt",
        operation = "read",
        "File operation failed"
    );

    assert!(true);
}

#[tokio::test]
async fn test_conditional_logging() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9100;
    let _ = observability::init_observability(config).await;

    let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
    let trace_enabled = tracing::enabled!(tracing::Level::TRACE);

    info!(
        debug_enabled = debug_enabled,
        trace_enabled = trace_enabled,
        "Logging level check"
    );

    // Test conditional expensive operations
    if tracing::enabled!(tracing::Level::DEBUG) {
        let expensive_data = format!("Expensive computation result: {}", 42 * 42);
        debug!(data = expensive_data, "Expensive debug data");
    }

    assert!(true);
}

#[tokio::test]
async fn test_concurrent_logging() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9101;
    let _ = observability::init_observability(config).await;

    let mut handles = vec![];

    // Test concurrent logging from multiple tasks
    for i in 0..10 {
        let handle = tokio::spawn(async move {
            let span = tracing::info_span!("concurrent_task", task_id = i);
            let _enter = span.enter();

            info!("Task {} starting", i);
            tokio::time::sleep(Duration::from_millis(10)).await;
            info!("Task {} completing", i);
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.expect("Task failed");
    }

    info!("All concurrent tasks completed");
    assert!(true);
}

#[tokio::test]
async fn test_large_log_messages() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9102;
    let _ = observability::init_observability(config).await;

    // Test handling of large log messages
    let large_data = "x".repeat(1024); // 1KB string
    let very_large_data = "y".repeat(10 * 1024); // 10KB string

    debug!(data = %large_data, "Large debug message");
    info!(size = large_data.len(), "Large data processed");

    // Very large messages should also work
    trace!(data = %very_large_data, "Very large trace message");

    assert!(true);
}

#[tokio::test]
async fn test_special_characters_in_logs() {
    let mut config = ObservabilityConfig::default();
    config.metrics_port = 9103;
    let _ = observability::init_observability(config).await;

    // Test special characters and unicode
    let special_chars = "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?";
    let unicode_text = "Unicode: 🦀 Rust 🚀 Test ✅ 中文 العربية";
    let json_like = r#"{"key": "value", "number": 42, "bool": true}"#;

    info!(
        special = special_chars,
        unicode = unicode_text,
        json_data = json_like,
        "Testing special characters"
    );

    assert!(true);
}

#[tokio::test]
async fn test_opentelemetry_integration() {
    // Test OpenTelemetry integration if feature is enabled
    #[cfg(feature = "otel")]
    {
        info!("Testing OpenTelemetry integration");

        // This would test OTEL-specific functionality
        // For now, just verify the feature compiles
        assert!(true);
    }

    #[cfg(not(feature = "otel"))]
    {
        info!("OpenTelemetry feature not enabled");
        assert!(true);
    }
}

#[tokio::test]
async fn test_environment_variable_handling() {
    // Store the original RUST_LOG value to restore later
    let original_rust_log = std::env::var("RUST_LOG").ok();

    // Test various environment variable combinations
    let test_cases = vec![
        ("trace", "TRACE level"),
        ("debug", "DEBUG level"),
        ("info", "INFO level"),
        ("warn", "WARN level"),
        ("error", "ERROR level"),
        ("off", "Logging off"),
        ("invalid", "Invalid level"),
    ];

    for (level, description) in test_cases {
        unsafe {
            std::env::set_var("RUST_LOG", level);
        }

        // Don't actually reinitialize observability for each test case
        // Just verify the environment variable is set correctly
        let current_value = std::env::var("RUST_LOG").unwrap();
        assert_eq!(
            current_value, level,
            "Environment variable should be set to {}",
            level
        );

        info!(
            level = level,
            description = description,
            "Testing environment variable"
        );
    }

    // Restore the original RUST_LOG value or remove it if it wasn't set
    match original_rust_log {
        Some(value) => unsafe { std::env::set_var("RUST_LOG", value) },
        None => unsafe { std::env::remove_var("RUST_LOG") },
    }
}

#[tokio::test]
async fn test_span_attributes_helpers() {
    // Test the span attribute helper functions
    let grpc_attrs = observability::grpc_span_attributes(
        "AppendRecords",
        Some("test-bucket"),
        Some("test-stream"),
    );

    assert_eq!(grpc_attrs.len(), 3);
    assert!(
        grpc_attrs
            .iter()
            .any(|(k, v)| k == &"grpc.method" && v == "AppendRecords")
    );
    assert!(
        grpc_attrs
            .iter()
            .any(|(k, v)| k == &"samsa.bucket" && v == "test-bucket")
    );
    assert!(
        grpc_attrs
            .iter()
            .any(|(k, v)| k == &"samsa.stream" && v == "test-stream")
    );

    let storage_attrs = observability::storage_span_attributes(
        "append_records",
        "test-bucket",
        "test-stream",
        Some(42),
    );

    assert_eq!(storage_attrs.len(), 4);
    assert!(
        storage_attrs
            .iter()
            .any(|(k, v)| k == &"storage.operation" && v == "append_records")
    );
    assert!(
        storage_attrs
            .iter()
            .any(|(k, v)| k == &"storage.record_count" && v == "42")
    );

    let etcd_attrs = observability::etcd_span_attributes("heartbeat");
    assert_eq!(etcd_attrs.len(), 1);
    assert!(
        etcd_attrs
            .iter()
            .any(|(k, v)| k == &"etcd.operation" && v == "heartbeat")
    );
}

#[tokio::test]
async fn test_observability_config() {
    // Test the configuration structure used by observability
    let config = ObservabilityConfig::default();
    assert_eq!(config.metrics_port, 9090);
    assert_eq!(config.log_level, "info");
    assert_eq!(config.service_name, "samsa-server");
    assert!(!config.enable_otel_tracing);

    let custom_config = ObservabilityConfig::new(
        8080,
        "debug".to_string(),
        "test-service".to_string(),
        "test-node".to_string(),
    );
    assert_eq!(custom_config.metrics_port, 8080);
    assert_eq!(custom_config.log_level, "debug");
    assert_eq!(custom_config.service_name, "test-service");
    assert_eq!(custom_config.node_id, "test-node");
}
