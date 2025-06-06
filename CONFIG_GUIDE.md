# Samsa Configuration Management Guide

## Overview

Samsa uses a centralized, layered configuration system based on the `figment` library. This provides:

- **Multiple Sources**: Support for configuration files (TOML), environment variables, and defaults
- **Validation**: Built-in validation with meaningful error messages
- **Type Safety**: Strong typing with automatic deserialization
- **Layered Loading**: Defaults → Config Files → Environment Variables (in order of precedence)

## Configuration Structure

The system is organized into four main configuration sections:

### Router Configuration (`RouterConfig`)

- **address**: Bind address (default: "0.0.0.0")
- **port**: Port number (default: 50051, must be >= 1024)
- **etcd_endpoints**: List of etcd endpoints (default: ["http://localhost:2379"])

### Server Configuration (`ServerConfig`)

- **node_id**: Unique node identifier (auto-generated UUID)
- **address**: Bind address (default: "0.0.0.0")
- **port**: Port number (default: 50052, must be >= 1024)
- **etcd_endpoints**: List of etcd endpoints
- **heartbeat_interval_secs**: Heartbeat interval (5-300 seconds, default: 30)
- **lease_ttl_secs**: Lease TTL (10-600 seconds, default: 60, must be > heartbeat interval)

### Observability Configuration (`ObservabilityConfig`)

- **metrics_port**: Prometheus metrics port (default: 9090, must be >= 1024)
- **log_level**: Logging level (default: "info")
- **service_name**: Service name for tracing (default: "samsa-server")
- **node_id**: Node identifier for metrics (default: "unknown")
- **enable_otel_tracing**: Enable OpenTelemetry tracing (default: false)
- **otel_endpoint**: OpenTelemetry endpoint (optional)

### Storage Configuration (`StorageConfig`)

- **node_id**: Node identifier (auto-generated UUID)
- **batch_size**: Records per batch (1-10000, default: 100)
- **batch_max_bytes**: Max batch size in bytes (1KB-100MB, default: 1MB)
- **batch_flush_interval_ms**: Flush interval (100ms-5min, default: 5s)
- **cleanup_interval_secs**: Cleanup check interval (10s-1day, default: 1h)
- **cleanup_grace_period_secs**: Grace period for cleanup (1h-30days, default: 24h)

## Loading Configuration

### 1. Individual Component Configuration

Load specific component configuration:

```rust
use samsa::common::config::{RouterConfig, ServerConfig};

// Load router config from default sources
let router_config = RouterConfig::load()?;

// Load from specific file
let server_config = ServerConfig::load_from_file("my-config.toml")?;

// Load from environment variables
let router_config = RouterConfig::from_env()?;
```

### 2. Unified Application Configuration

Load all configurations together:

```rust
use samsa::common::config::AppConfig;

// Load from default file locations
let app_config = AppConfig::load()?;

// Access individual components
let router_config = app_config.router;
let server_config = app_config.server;
```

### 3. Custom Configuration Sources

Use custom figment providers:

```rust
use figment::{Figment, providers::{Env, Toml}};

let figment = Figment::new()
    .merge(Toml::file("custom.toml"))
    .merge(Env::prefixed("Samsa_"));

let config = RouterConfig::load_with_figment(figment)?;
```

## Configuration Sources & Precedence

Configuration is loaded in the following order (later sources override earlier ones):

1. **Defaults**: Hard-coded default values
2. **Configuration Files**: TOML files
3. **Environment Variables**: Prefixed environment variables

### Default File Locations

The system automatically looks for configuration files in these locations:

- **Unified**: `config/app.toml`, `samsa.toml`
- **Router**: `config/router.toml`
- **Server**: `config/server.toml`
- **Observability**: `config/observability.toml`
- **Storage**: `config/storage.toml`

### Environment Variables

Environment variables use component prefixes:

```bash
# Router configuration
export ROUTER_ADDRESS="0.0.0.0"
export ROUTER_PORT="8080"
export ROUTER_ETCD_ENDPOINTS="http://etcd1:2379,http://etcd2:2379"

# Server configuration
export SERVER_ADDRESS="0.0.0.0"
export SERVER_PORT="8081"
export SERVER_HEARTBEAT_INTERVAL_SECS="15"

# Storage configuration
export STORAGE_BATCH_SIZE="200"
export STORAGE_BATCH_MAX_BYTES="2097152"  # 2MB

# Observability
export METRICS_PORT="9090"
export LOG_LEVEL="info"
export SERVICE_NAME="my-samsa-service"
```

## Configuration File Format

### TOML Format

```toml
# config/app.toml
[router]
address = "0.0.0.0"
port = 50051
etcd_endpoints = ["http://localhost:2379"]

[server]
node_id = "550e8400-e29b-41d4-a716-446655440000"
address = "0.0.0.0"
port = 50052
etcd_endpoints = ["http://localhost:2379"]
heartbeat_interval_secs = 30
lease_ttl_secs = 90

[observability]
metrics_port = 9090
log_level = "info"
service_name = "samsa-server"
node_id = "node-1"
enable_otel_tracing = false

[storage]
node_id = "node-1"
batch_size = 100
batch_max_bytes = 1048576
batch_flush_interval_ms = 5000
cleanup_interval_secs = 3600
cleanup_grace_period_secs = 86400
```

## Validation

All configuration values are validated automatically:

### Built-in Validators

- **Port numbers**: Must be >= 1024
- **Time intervals**: Must be within reasonable ranges
- **Etcd endpoints**: Must be valid HTTP/HTTPS URLs
- **String fields**: Must not be empty where required
- **Numeric ranges**: Batch sizes, timeouts, etc. have sensible limits

### Custom Validation

- **Lease TTL vs Heartbeat**: Lease TTL must be greater than heartbeat interval
- **Endpoint format**: Etcd endpoints must start with http:// or https://

### Error Messages

Validation errors provide clear, actionable messages:

```
Configuration validation failed: heartbeat_interval_secs: Heartbeat interval must be between 5 and 300 seconds
```

## Best Practices

### 1. Development vs Production

Use different configuration files for different environments:

```bash
# Development
Samsa_CONFIG_FILE=config/development.toml ./server

# Production
Samsa_CONFIG_FILE=config/production.toml ./server
```

### 2. Secrets Management

For sensitive values, prefer environment variables or external secret management:

```toml
# In config file - non-sensitive
[server]
address = "0.0.0.0"
port = 50052

# Via environment - sensitive
# export ETCD_PASSWORD="secret"
```

### 3. Configuration Validation

Always validate configuration in CI/CD:

```bash
# Test configuration loading
cargo test config
```

### 4. Documentation

Document your production configuration:

```toml
# Production configuration for Samsa cluster
# Last updated: 2024-01-15
# Owner: Platform Team

[router]
# Bind to all interfaces in container
address = "0.0.0.0"
# Standard gRPC port
port = 50051
```

## Troubleshooting

### Common Issues

1. **Invalid port numbers**: Ensure ports are >= 1024
2. **TTL vs heartbeat**: Lease TTL must be greater than heartbeat interval
3. **Missing etcd endpoints**: At least one endpoint must be provided
4. **File format errors**: Ensure TOML syntax is valid

### Debug Configuration Loading

Enable debug logging to see configuration sources:

```bash
export LOG_LEVEL=debug
./server
```

This will show which configuration sources are being loaded and in what order.

## Examples

See the `config/` directory for complete examples:

- `config/example.toml` - Basic configuration
- `config/production.toml` - Production-optimized settings
- `config/development.toml` - Development settings
