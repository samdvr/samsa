use crate::common::config::ObservabilityConfig;
use crate::common::metrics;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize comprehensive observability (metrics + tracing)
pub async fn init_observability(
    config: ObservabilityConfig,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize metrics
    init_metrics(config.metrics_port).await?;

    // Initialize enhanced tracing
    init_tracing(&config).await?;

    tracing::info!(
        "Observability initialized - metrics on port {}, service: {}, node: {}",
        config.metrics_port,
        config.service_name,
        config.node_id
    );

    Ok(())
}

/// Initialize metrics collection and Prometheus export
async fn init_metrics(
    prometheus_port: u16,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    metrics::init_metrics_exporter(prometheus_port).await?;
    tracing::info!(
        "Metrics initialized and exported on port {}",
        prometheus_port
    );
    Ok(())
}

/// Initialize enhanced tracing with optional OpenTelemetry support
async fn init_tracing(
    config: &ObservabilityConfig,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create EnvFilter from log level
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // Base tracing setup with structured logging
    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .json(); // Use JSON format for better structured logging

    if config.enable_otel_tracing {
        #[cfg(feature = "otel")]
        {
            // Initialize OpenTelemetry tracing if enabled
            let tracer = init_otel_tracer(config).await?;
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_layer)
                .init();

            tracing::info!("Enhanced tracing initialized with OpenTelemetry support");
        }

        #[cfg(not(feature = "otel"))]
        {
            tracing::warn!("OpenTelemetry requested but not compiled with otel feature");
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .init();
        }
    } else {
        // Standard tracing setup
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();

        tracing::info!("Enhanced tracing initialized");
    }

    Ok(())
}

/// Initialize OpenTelemetry tracer (optional feature)
#[cfg(feature = "otel")]
async fn init_otel_tracer(
    config: &ObservabilityConfig,
) -> std::result::Result<opentelemetry_sdk::trace::Tracer, Box<dyn std::error::Error + Send + Sync>>
{
    use opentelemetry::{KeyValue, global};
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::{Resource, trace};

    let mut resource = Resource::new(vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("node.id", config.node_id.clone()),
    ]);

    // Add additional resource attributes from environment
    if let Ok(namespace) = std::env::var("K8S_NAMESPACE") {
        resource = resource.merge(&Resource::new(vec![KeyValue::new(
            "k8s.namespace.name",
            namespace,
        )]));
    }

    if let Ok(pod_name) = std::env::var("K8S_POD_NAME") {
        resource = resource.merge(&Resource::new(vec![KeyValue::new(
            "k8s.pod.name",
            pod_name,
        )]));
    }

    let exporter = if let Some(endpoint) = &config.otel_endpoint {
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(endpoint)
    } else {
        opentelemetry_otlp::new_exporter().tonic()
    };

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(trace::config().with_resource(resource))
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    global::set_tracer_provider(tracer.provider().unwrap());

    Ok(tracer)
}

/// Helper function to create span attributes for gRPC operations
pub fn grpc_span_attributes(
    method: &str,
    bucket: Option<&str>,
    stream: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut attrs = vec![("grpc.method", method.to_string())];

    if let Some(bucket) = bucket {
        attrs.push(("samsa.bucket", bucket.to_string()));
    }

    if let Some(stream) = stream {
        attrs.push(("samsa.stream", stream.to_string()));
    }

    attrs
}

/// Helper function to create span attributes for storage operations
pub fn storage_span_attributes(
    operation: &str,
    bucket: &str,
    stream: &str,
    record_count: Option<usize>,
) -> Vec<(&'static str, String)> {
    let mut attrs = vec![
        ("storage.operation", operation.to_string()),
        ("samsa.bucket", bucket.to_string()),
        ("samsa.stream", stream.to_string()),
    ];

    if let Some(count) = record_count {
        attrs.push(("storage.record_count", count.to_string()));
    }

    attrs
}

/// Helper function to create span attributes for etcd operations
pub fn etcd_span_attributes(operation: &str) -> Vec<(&'static str, String)> {
    vec![("etcd.operation", operation.to_string())]
}

/// Shutdown observability systems gracefully
pub async fn shutdown_observability() {
    #[cfg(feature = "otel")]
    {
        use opentelemetry::global;
        global::shutdown_tracer_provider();
    }

    tracing::info!("Observability systems shut down");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observability_config_default() {
        let config = ObservabilityConfig::default();
        assert_eq!(config.metrics_port, 9090);
        assert_eq!(config.log_level, "info");
        assert_eq!(config.service_name, "samsa-server");
        assert!(!config.enable_otel_tracing);
    }

    #[test]
    fn test_observability_config_from_env() {
        // Test with environment variables set
        unsafe {
            std::env::set_var("METRICS_PORT", "8080");
            std::env::set_var("LOG_LEVEL", "debug");
            std::env::set_var("SERVICE_NAME", "test-service");
            std::env::set_var("NODE_ID", "test-node");
        }

        let config = ObservabilityConfig::from_env();
        assert_eq!(config.metrics_port, 8080);
        assert_eq!(config.log_level, "debug");
        assert_eq!(config.service_name, "test-service");
        assert_eq!(config.node_id, "test-node");

        // Clean up
        unsafe {
            std::env::remove_var("METRICS_PORT");
            std::env::remove_var("LOG_LEVEL");
            std::env::remove_var("SERVICE_NAME");
            std::env::remove_var("NODE_ID");
        }
    }

    #[test]
    fn test_span_attributes() {
        let grpc_attrs = grpc_span_attributes("append", Some("test-bucket"), Some("test-stream"));
        assert_eq!(grpc_attrs.len(), 3);
        assert!(grpc_attrs.contains(&("grpc.method", "append".to_string())));

        let storage_attrs =
            storage_span_attributes("append_records", "test-bucket", "test-stream", Some(5));
        assert_eq!(storage_attrs.len(), 4);
        assert!(storage_attrs.contains(&("storage.operation", "append_records".to_string())));
        assert!(storage_attrs.contains(&("storage.record_count", "5".to_string())));

        let etcd_attrs = etcd_span_attributes("heartbeat");
        assert_eq!(etcd_attrs.len(), 1);
        assert!(etcd_attrs.contains(&("etcd.operation", "heartbeat".to_string())));
    }

    #[tokio::test]
    async fn test_observability_initialization() {
        let config = ObservabilityConfig::new(
            9091,
            "debug".to_string(),
            "test-service".to_string(),
            "test-node".to_string(),
        );

        // Test that initialization doesn't panic
        // In a real test environment, you'd want to verify the actual setup
        // but for unit tests, we just verify the config parsing works
        assert_eq!(config.metrics_port, 9091);
        assert_eq!(config.service_name, "test-service");
    }
}
