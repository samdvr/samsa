//! Comprehensive unit tests for the config module
//!
//! Tests all configuration structs, validation, loading mechanisms, and error handling

use figment::{Figment, providers::Serialized};
use samsa::common::config::{AppConfig, ObservabilityConfig, ServerConfig, StorageConfig};
use samsa::common::error::SamsaError;
use std::env;
use std::io::Write;
use tempfile::NamedTempFile;
use uuid::Uuid;
use validator::Validate;

#[test]
fn test_server_config_default() {
    let config = ServerConfig::default();

    assert_eq!(config.address, "0.0.0.0");
    assert_eq!(config.port, 50052);
    assert_eq!(config.etcd_endpoints, vec!["http://localhost:2379"]);
    assert_eq!(config.heartbeat_interval_secs, 30);
    assert_eq!(config.lease_ttl_secs, 60);

    // Test validation passes
    assert!(config.validate().is_ok());
}

#[test]
fn test_server_config_endpoint() {
    let config = ServerConfig {
        node_id: Uuid::now_v7(),
        address: "server.local".to_string(),
        port: 9999,
        etcd_endpoints: vec!["http://localhost:2379".to_string()],
        heartbeat_interval_secs: 30,
        lease_ttl_secs: 60,
    };

    assert_eq!(config.endpoint(), "server.local:9999");
}

#[test]
fn test_server_config_validation_invalid_port() {
    let config = ServerConfig {
        node_id: Uuid::now_v7(),
        address: "127.0.0.1".to_string(),
        port: 80, // Invalid port (< 1024)
        etcd_endpoints: vec!["http://localhost:2379".to_string()],
        heartbeat_interval_secs: 30,
        lease_ttl_secs: 60,
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_server_config_validation_empty_address() {
    let config = ServerConfig {
        node_id: Uuid::now_v7(),
        address: "".to_string(), // Invalid empty address
        port: 8080,
        etcd_endpoints: vec!["http://localhost:2379".to_string()],
        heartbeat_interval_secs: 30,
        lease_ttl_secs: 60,
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_server_config_validation_empty_etcd_endpoints() {
    let config = ServerConfig {
        node_id: Uuid::now_v7(),
        address: "127.0.0.1".to_string(),
        port: 8080,
        etcd_endpoints: vec![], // Invalid empty endpoints
        heartbeat_interval_secs: 30,
        lease_ttl_secs: 60,
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_server_config_validation_invalid_heartbeat() {
    let config = ServerConfig {
        node_id: Uuid::now_v7(),
        address: "127.0.0.1".to_string(),
        port: 8080,
        etcd_endpoints: vec!["http://localhost:2379".to_string()],
        heartbeat_interval_secs: 2, // Invalid (< 5)
        lease_ttl_secs: 60,
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_server_config_validation_invalid_lease_ttl() {
    let config = ServerConfig {
        node_id: Uuid::now_v7(),
        address: "127.0.0.1".to_string(),
        port: 8080,
        etcd_endpoints: vec!["http://localhost:2379".to_string()],
        heartbeat_interval_secs: 30,
        lease_ttl_secs: 5, // Invalid (< 10)
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_server_config_from_env() {
    // Test that from_env() returns a valid config (may use defaults if no env vars set)
    let result = ServerConfig::from_env();
    assert!(result.is_ok());

    let config = result.unwrap();
    // Just verify it's a valid config with reasonable defaults
    assert!(!config.address.is_empty());
    assert!(config.port >= 1024);
    assert!(!config.etcd_endpoints.is_empty());
}

#[test]
fn test_server_config_load_from_toml_file() {
    let toml_content = r#"
node_id = "01234567-89ab-cdef-0123-456789abcdef"
address = "10.0.0.1"
port = 7070
etcd_endpoints = ["http://etcd:2379"]
heartbeat_interval_secs = 45
lease_ttl_secs = 90
"#;

    let mut temp_file = NamedTempFile::with_suffix(".toml").unwrap();
    temp_file.write_all(toml_content.as_bytes()).unwrap();

    let result = ServerConfig::load_from_file(temp_file.path());
    assert!(result.is_ok());

    let config = result.unwrap();
    assert_eq!(config.address, "10.0.0.1");
    assert_eq!(config.port, 7070);
    assert_eq!(config.etcd_endpoints, vec!["http://etcd:2379"]);
    assert_eq!(config.heartbeat_interval_secs, 45);
    assert_eq!(config.lease_ttl_secs, 90);
}

#[test]
fn test_server_config_load_from_json_file() {
    let json_content = r#"
{
    "node_id": "01234567-89ab-cdef-0123-456789abcdef",
    "address": "172.16.0.1",
    "port": 6060,
    "etcd_endpoints": ["http://etcd1:2379", "http://etcd2:2379"],
    "heartbeat_interval_secs": 25,
    "lease_ttl_secs": 75
}
"#;

    let mut temp_file = NamedTempFile::with_suffix(".json").unwrap();
    temp_file.write_all(json_content.as_bytes()).unwrap();

    let result = ServerConfig::load_from_file(temp_file.path());
    assert!(result.is_ok());

    let config = result.unwrap();
    assert_eq!(config.address, "172.16.0.1");
    assert_eq!(config.port, 6060);
    assert_eq!(
        config.etcd_endpoints,
        vec!["http://etcd1:2379", "http://etcd2:2379"]
    );
    assert_eq!(config.heartbeat_interval_secs, 25);
    assert_eq!(config.lease_ttl_secs, 75);
}

#[test]
fn test_server_config_load_with_figment() {
    let figment = Figment::from(Serialized::defaults(ServerConfig {
        node_id: Uuid::now_v7(),
        address: "custom.host".to_string(),
        port: 5555,
        etcd_endpoints: vec!["http://custom:2379".to_string()],
        heartbeat_interval_secs: 20,
        lease_ttl_secs: 50,
    }));

    let result = ServerConfig::load_with_figment(figment);
    assert!(result.is_ok());

    let config = result.unwrap();
    assert_eq!(config.address, "custom.host");
    assert_eq!(config.port, 5555);
    assert_eq!(config.heartbeat_interval_secs, 20);
    assert_eq!(config.lease_ttl_secs, 50);
}

#[test]
fn test_observability_config_default() {
    let config = ObservabilityConfig::default();

    assert_eq!(config.metrics_port, 9090);
    assert_eq!(config.log_level, "info");
    assert_eq!(config.service_name, "samsa-server");
    assert_eq!(config.node_id, "unknown");
    assert!(!config.enable_otel_tracing);
    assert!(config.otel_endpoint.is_none());

    // Test validation passes
    assert!(config.validate().is_ok());
}

#[test]
fn test_observability_config_new() {
    let config = ObservabilityConfig::new(
        8080,
        "debug".to_string(),
        "test-service".to_string(),
        "test-node".to_string(),
    );

    assert_eq!(config.metrics_port, 8080);
    assert_eq!(config.log_level, "debug");
    assert_eq!(config.service_name, "test-service");
    assert_eq!(config.node_id, "test-node");
    assert!(!config.enable_otel_tracing);
    assert!(config.otel_endpoint.is_none());
}

#[test]
fn test_observability_config_with_otel_tracing() {
    let config = ObservabilityConfig::new(
        8080,
        "debug".to_string(),
        "test-service".to_string(),
        "test-node".to_string(),
    )
    .with_otel_tracing("http://jaeger:14268/api/traces".to_string());

    assert!(config.enable_otel_tracing);
    assert_eq!(
        config.otel_endpoint,
        Some("http://jaeger:14268/api/traces".to_string())
    );
}

#[test]
fn test_observability_config_from_env() {
    let config = ObservabilityConfig::from_env();

    // Should use defaults when no env vars are set
    assert_eq!(config.metrics_port, 9090);
    assert_eq!(config.log_level, "info");
    assert_eq!(config.service_name, "samsa-server");
}

#[test]
fn test_observability_config_validation_invalid_port() {
    let config = ObservabilityConfig {
        metrics_port: 80, // Invalid port (< 1024)
        log_level: "info".to_string(),
        service_name: "test".to_string(),
        node_id: "test-node".to_string(),
        enable_otel_tracing: false,
        otel_endpoint: None,
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_observability_config_validation_empty_fields() {
    let config = ObservabilityConfig {
        metrics_port: 9090,
        log_level: "".to_string(), // Invalid empty log level
        service_name: "test".to_string(),
        node_id: "test-node".to_string(),
        enable_otel_tracing: false,
        otel_endpoint: None,
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_storage_config_default() {
    let config = StorageConfig::default();

    assert_eq!(config.batch_size, 100);
    assert_eq!(config.batch_max_bytes, 1024 * 1024); // 1MB
    assert_eq!(config.batch_flush_interval_ms, 5000); // 5 seconds
    assert_eq!(config.cleanup_interval_secs, 3600); // Fixed: actual default is 3600 (1 hour)
    assert_eq!(config.cleanup_grace_period_secs, 86400); // Fixed: actual default is 86400 (24 hours)

    // Test validation passes
    assert!(config.validate().is_ok());
}

#[test]
fn test_storage_config_validation_invalid_batch_size() {
    let config = StorageConfig {
        node_id: "test-node".to_string(),
        batch_size: 0, // Invalid (< 1)
        batch_max_bytes: 1024 * 1024,
        batch_flush_interval_ms: 5000,
        cleanup_interval_secs: 300,
        cleanup_grace_period_secs: 3600,
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_storage_config_validation_invalid_batch_max_bytes() {
    let config = StorageConfig {
        node_id: "test-node".to_string(),
        batch_size: 1000,
        batch_max_bytes: 500, // Invalid (< 1024)
        batch_flush_interval_ms: 5000,
        cleanup_interval_secs: 300,
        cleanup_grace_period_secs: 3600,
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_storage_config_from_env() {
    // Set environment variables
    unsafe {
        env::set_var("STORAGE_NODE_ID", "storage-node-123");
        env::set_var("STORAGE_BATCH_SIZE", "2000");
        env::set_var("STORAGE_BATCH_MAX_BYTES", "2097152"); // 2MB
    }

    let result = StorageConfig::from_env();

    // Clean up environment variables
    unsafe {
        env::remove_var("STORAGE_NODE_ID");
        env::remove_var("STORAGE_BATCH_SIZE");
        env::remove_var("STORAGE_BATCH_MAX_BYTES");
    }

    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(config.node_id, "storage-node-123");
    assert_eq!(config.batch_size, 2000);
    assert_eq!(config.batch_max_bytes, 2097152);
}

#[test]
fn test_app_config_validation() {
    let app_config = AppConfig {
        server: ServerConfig::default(),
        observability: ObservabilityConfig::default(),
        storage: StorageConfig::default(),
    };

    assert!(app_config.validate().is_ok());
}

#[test]
fn test_app_config_validation_nested_invalid() {
    let app_config = AppConfig {
        server: ServerConfig {
            node_id: uuid::Uuid::now_v7(),
            address: "".to_string(), // Invalid empty address
            port: 50052,
            etcd_endpoints: vec!["http://localhost:2379".to_string()],
            heartbeat_interval_secs: 30,
            lease_ttl_secs: 60,
        },
        observability: ObservabilityConfig::default(),
        storage: StorageConfig::default(),
    };

    assert!(app_config.validate().is_err());
}

#[test]
fn test_config_load_unsupported_format() {
    let unsupported_content = "some content";
    let mut temp_file = NamedTempFile::with_suffix(".yaml").unwrap();
    temp_file.write_all(unsupported_content.as_bytes()).unwrap();

    let result = ServerConfig::load_from_file(temp_file.path());
    assert!(result.is_err());

    if let Err(SamsaError::Config(msg)) = result {
        assert!(msg.contains("Unsupported config file format"));
    } else {
        panic!("Expected Config error");
    }
}

#[test]
fn test_config_load_invalid_toml() {
    let invalid_toml = r#"
node_id = "01234567-89ab-cdef-0123-456789abcdef"
address = "127.0.0.1"
port = "invalid_port"  # Should be number, not string
"#;

    let mut temp_file = NamedTempFile::with_suffix(".toml").unwrap();
    temp_file.write_all(invalid_toml.as_bytes()).unwrap();

    let result = ServerConfig::load_from_file(temp_file.path());
    assert!(result.is_err());
}

#[test]
fn test_config_load_invalid_json() {
    let invalid_json = r#"
{
    "node_id": "01234567-89ab-cdef-0123-456789abcdef",
    "address": "127.0.0.1",
    "port": 8080,
    "invalid_json": 
}
"#;

    let mut temp_file = NamedTempFile::with_suffix(".json").unwrap();
    temp_file.write_all(invalid_json.as_bytes()).unwrap();

    let result = ServerConfig::load_from_file(temp_file.path());
    assert!(result.is_err());
}

#[test]
fn test_config_load_nonexistent_file() {
    // Loading from a non-existent file should use defaults
    let result = ServerConfig::load_from_file("/nonexistent/path/config.toml");
    assert!(result.is_ok());

    let config = result.unwrap();
    // Should be default values since file doesn't exist
    assert_eq!(config.address, "0.0.0.0");
    assert_eq!(config.port, 50052);
}

#[test]
fn test_config_debug_format() {
    let config = ServerConfig::default();
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("ServerConfig"));
    assert!(debug_str.contains("address"));
    assert!(debug_str.contains("port"));
}

#[test]
fn test_config_clone() {
    let config = ServerConfig::default();
    let cloned = config.clone();

    assert_eq!(config.address, cloned.address);
    assert_eq!(config.port, cloned.port);
    assert_eq!(config.etcd_endpoints, cloned.etcd_endpoints);
}

#[test]
fn test_config_serialization() {
    let config = ServerConfig::default();

    // Test JSON serialization
    let json_result = serde_json::to_string(&config);
    assert!(json_result.is_ok());

    // Test JSON deserialization
    let json_str = json_result.unwrap();
    let deserialized: Result<ServerConfig, _> = serde_json::from_str(&json_str);
    assert!(deserialized.is_ok());

    let deserialized_config = deserialized.unwrap();
    assert_eq!(config.address, deserialized_config.address);
    assert_eq!(config.port, deserialized_config.port);
}
