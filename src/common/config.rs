use crate::common::error::{Result, SamsaError};
use figment::{
    Figment,
    providers::{Env, Format, Json, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]

pub struct ServerConfig {
    pub node_id: Uuid,

    #[validate(length(min = 1))]
    pub address: String,

    #[validate(range(min = 1024, max = 65535))]
    pub port: u16,

    #[validate(length(min = 1))]
    pub etcd_endpoints: Vec<String>,

    #[validate(range(min = 5, max = 300))]
    pub heartbeat_interval_secs: u64,

    #[validate(range(min = 10, max = 600))]
    pub lease_ttl_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            node_id: Uuid::now_v7(),
            address: "0.0.0.0".to_string(),
            port: 50052,
            etcd_endpoints: vec!["http://localhost:2379".to_string()],
            heartbeat_interval_secs: 30,
            lease_ttl_secs: 60,
        }
    }
}

impl ServerConfig {
    /// Load configuration from multiple sources with validation
    pub fn load() -> Result<Self> {
        Self::load_from_sources(&["config/server.toml", "config/server.json"], None)
    }

    /// Load configuration from specific file path
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::load_from_sources(&[path.as_ref().to_str().unwrap()], None)
    }

    /// Load configuration with custom figment provider
    pub fn load_with_figment(figment: Figment) -> Result<Self> {
        let config: Self = figment
            .extract()
            .map_err(|e| SamsaError::Config(format!("Failed to extract configuration: {}", e)))?;

        // Validate the configuration
        config
            .validate()
            .map_err(|e| SamsaError::Config(format!("Configuration validation failed: {}", e)))?;

        // Additional validation that lease TTL > heartbeat interval
        if config.lease_ttl_secs <= config.heartbeat_interval_secs {
            return Err(SamsaError::Config(
                "Lease TTL must be greater than heartbeat interval".to_string(),
            ));
        }

        Ok(config)
    }

    /// Load configuration from multiple sources
    fn load_from_sources(config_files: &[&str], profile: Option<&str>) -> Result<Self> {
        let mut figment = Figment::from(Serialized::defaults(Self::default()));

        // Add configuration files (if they exist)
        for config_file in config_files {
            let path = Path::new(config_file);
            if path.exists() {
                match path.extension().and_then(|s| s.to_str()) {
                    Some("toml") => {
                        figment = figment.merge(Toml::file(path));
                    }
                    Some("json") => {
                        figment = figment.merge(Json::file(path));
                    }
                    _ => {
                        return Err(SamsaError::Config(format!(
                            "Unsupported config file format: {}",
                            config_file
                        )));
                    }
                }
            }
        }

        // Add environment variables with prefix
        figment = figment.merge(
            Env::prefixed("SERVER_")
                .map(|key| key.as_str().replace("SERVER_", "").to_lowercase().into()),
        );

        // Apply profile if specified
        if let Some(profile) = profile {
            figment = figment.select(profile);
        }

        Self::load_with_figment(figment)
    }

    /// Create from environment variables (for backward compatibility)
    pub fn from_env() -> Result<Self> {
        let figment = Figment::from(Serialized::defaults(Self::default())).merge(
            Env::prefixed("SERVER_")
                .map(|key| key.as_str().replace("SERVER_", "").to_lowercase().into()),
        );

        Self::load_with_figment(figment)
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ObservabilityConfig {
    #[validate(range(min = 1024, max = 65535))]
    pub metrics_port: u16,

    #[validate(length(min = 1))]
    pub log_level: String,

    #[validate(length(min = 1))]
    pub service_name: String,

    #[validate(length(min = 1))]
    pub node_id: String,

    pub enable_otel_tracing: bool,
    pub otel_endpoint: Option<String>,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_port: 9090,
            log_level: "info".to_string(),
            service_name: "samsa-server".to_string(),
            node_id: "unknown".to_string(),
            enable_otel_tracing: false,
            otel_endpoint: None,
        }
    }
}

impl ObservabilityConfig {
    /// Load configuration from multiple sources with validation
    pub fn load() -> Result<Self> {
        Self::load_from_sources(
            &["config/observability.toml", "config/observability.json"],
            None,
        )
    }

    /// Load configuration from specific file path
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::load_from_sources(&[path.as_ref().to_str().unwrap()], None)
    }

    /// Load configuration with custom figment provider
    pub fn load_with_figment(figment: Figment) -> Result<Self> {
        let config: Self = figment
            .extract()
            .map_err(|e| SamsaError::Config(format!("Failed to extract configuration: {}", e)))?;

        config
            .validate()
            .map_err(|e| SamsaError::Config(format!("Configuration validation failed: {}", e)))?;

        Ok(config)
    }

    /// Load configuration from multiple sources
    fn load_from_sources(config_files: &[&str], profile: Option<&str>) -> Result<Self> {
        let mut figment = Figment::from(Serialized::defaults(Self::default()));

        // Add configuration files (if they exist)
        for config_file in config_files {
            let path = Path::new(config_file);
            if path.exists() {
                match path.extension().and_then(|s| s.to_str()) {
                    Some("toml") => {
                        figment = figment.merge(Toml::file(path));
                    }
                    Some("json") => {
                        figment = figment.merge(Json::file(path));
                    }
                    _ => {
                        return Err(SamsaError::Config(format!(
                            "Unsupported config file format: {}",
                            config_file
                        )));
                    }
                }
            }
        }

        // Add environment variables
        figment = figment.merge(Env::raw().map(|key| key.as_str().to_lowercase().into()));

        // Apply profile if specified
        if let Some(profile) = profile {
            figment = figment.select(profile);
        }

        Self::load_with_figment(figment)
    }

    /// Create from environment variables (for backward compatibility)
    pub fn from_env() -> Self {
        // Use the new system but return the old interface for compatibility
        Self::load().unwrap_or_else(|_| Self::default())
    }

    /// Create configuration with custom values
    pub fn new(
        metrics_port: u16,
        log_level: String,
        service_name: String,
        node_id: String,
    ) -> Self {
        Self {
            metrics_port,
            log_level,
            service_name,
            node_id,
            enable_otel_tracing: false,
            otel_endpoint: None,
        }
    }

    /// Enable OpenTelemetry tracing with endpoint
    pub fn with_otel_tracing(mut self, endpoint: String) -> Self {
        self.enable_otel_tracing = true;
        self.otel_endpoint = Some(endpoint);
        self
    }
}

/// Storage configuration for batch processing and node identification
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct StorageConfig {
    #[validate(length(min = 1))]
    pub node_id: String,

    #[validate(range(min = 1, max = 10000))]
    pub batch_size: usize,

    #[validate(range(min = 1024, max = 104857600))] // 1KB to 100MB
    pub batch_max_bytes: usize,

    #[validate(range(min = 100, max = 300000))] // 100ms to 5min
    pub batch_flush_interval_ms: u64,

    #[validate(range(min = 10, max = 86400))] // 10s to 1 day
    pub cleanup_interval_secs: u64,

    #[validate(range(min = 3600, max = 2592000))] // 1 hour to 30 days
    pub cleanup_grace_period_secs: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            node_id: Uuid::now_v7().to_string(),
            batch_size: 100,
            batch_max_bytes: 1024 * 1024,     // 1MB
            batch_flush_interval_ms: 5000,    // 5 seconds
            cleanup_interval_secs: 3600,      // 1 hour
            cleanup_grace_period_secs: 86400, // 24 hours
        }
    }
}

impl StorageConfig {
    /// Load configuration from multiple sources with validation
    pub fn load() -> Result<Self> {
        Self::load_from_sources(&["config/storage.toml", "config/storage.json"], None)
    }

    /// Load configuration from specific file path
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::load_from_sources(&[path.as_ref().to_str().unwrap()], None)
    }

    /// Load configuration with custom figment provider
    pub fn load_with_figment(figment: Figment) -> Result<Self> {
        let config: Self = figment
            .extract()
            .map_err(|e| SamsaError::Config(format!("Failed to extract configuration: {}", e)))?;

        config
            .validate()
            .map_err(|e| SamsaError::Config(format!("Configuration validation failed: {}", e)))?;

        Ok(config)
    }

    /// Load configuration from multiple sources
    fn load_from_sources(config_files: &[&str], profile: Option<&str>) -> Result<Self> {
        let mut figment = Figment::from(Serialized::defaults(Self::default()));

        // Add configuration files (if they exist)
        for config_file in config_files {
            let path = Path::new(config_file);
            if path.exists() {
                match path.extension().and_then(|s| s.to_str()) {
                    Some("toml") => {
                        figment = figment.merge(Toml::file(path));
                    }
                    Some("json") => {
                        figment = figment.merge(Json::file(path));
                    }
                    _ => {
                        return Err(SamsaError::Config(format!(
                            "Unsupported config file format: {}",
                            config_file
                        )));
                    }
                }
            }
        }

        // Add environment variables with prefix
        figment = figment.merge(
            Env::prefixed("STORAGE_")
                .map(|key| key.as_str().replace("STORAGE_", "").to_lowercase().into()),
        );

        // Apply profile if specified
        if let Some(profile) = profile {
            figment = figment.select(profile);
        }

        Self::load_with_figment(figment)
    }

    /// Create from environment variables (for backward compatibility)
    pub fn from_env() -> Result<Self> {
        let figment = Figment::from(Serialized::defaults(Self::default())).merge(
            Env::prefixed("STORAGE_")
                .map(|key| key.as_str().replace("STORAGE_", "").to_lowercase().into()),
        );

        Self::load_with_figment(figment)
    }
}

/// Unified application configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate, Default)]
pub struct AppConfig {
    #[validate(nested)]
    pub server: ServerConfig,

    #[validate(nested)]
    pub observability: ObservabilityConfig,

    #[validate(nested)]
    pub storage: StorageConfig,
}

impl AppConfig {
    /// Load unified configuration from file
    pub fn load() -> Result<Self> {
        Self::load_from_sources(
            &[
                "config/app.toml",
                "config/app.json",
                "samsa.toml",
                "samsa.json",
            ],
            None,
        )
    }

    /// Load configuration from specific file path
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::load_from_sources(&[path.as_ref().to_str().unwrap()], None)
    }

    /// Load configuration with custom figment provider
    pub fn load_with_figment(figment: Figment) -> Result<Self> {
        let config: Self = figment
            .extract()
            .map_err(|e| SamsaError::Config(format!("Failed to extract configuration: {}", e)))?;

        config
            .validate()
            .map_err(|e| SamsaError::Config(format!("Configuration validation failed: {}", e)))?;

        Ok(config)
    }

    /// Load configuration from multiple sources
    fn load_from_sources(config_files: &[&str], profile: Option<&str>) -> Result<Self> {
        let mut figment = Figment::from(Serialized::defaults(Self::default()));

        // Add configuration files (if they exist)
        for config_file in config_files {
            let path = Path::new(config_file);
            if path.exists() {
                match path.extension().and_then(|s| s.to_str()) {
                    Some("toml") => {
                        figment = figment.merge(Toml::file(path));
                    }
                    Some("json") => {
                        figment = figment.merge(Json::file(path));
                    }
                    _ => {
                        return Err(SamsaError::Config(format!(
                            "Unsupported config file format: {}",
                            config_file
                        )));
                    }
                }
            }
        }

        // Add environment variables
        figment = figment.merge(Env::raw().map(|key| key.as_str().to_lowercase().into()));

        // Apply profile if specified
        if let Some(profile) = profile {
            figment = figment.select(profile);
        }

        Self::load_with_figment(figment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_server_config_validation() {
        let mut config = ServerConfig::default();

        // Test invalid heartbeat interval
        config.heartbeat_interval_secs = 3; // Too low
        assert!(config.validate().is_err());

        // Test valid heartbeat interval
        config.heartbeat_interval_secs = 30;
        assert!(config.validate().is_ok());

        // Test lease TTL vs heartbeat interval validation
        config.heartbeat_interval_secs = 60;
        config.lease_ttl_secs = 30; // TTL < heartbeat
        assert!(
            ServerConfig::load_with_figment(Figment::from(Serialized::defaults(config))).is_err()
        );
    }

    #[test]
    fn test_config_from_env() {
        unsafe {
            env::set_var("SERVER_PORT", "8080");
            env::set_var("SERVER_ADDRESS", "127.0.0.1");
        }

        let config = ServerConfig::from_env().unwrap();
        assert_eq!(config.port, 8080);
        assert_eq!(config.address, "127.0.0.1");

        unsafe {
            env::remove_var("SERVER_PORT");
            env::remove_var("SERVER_ADDRESS");
        }
    }
}
