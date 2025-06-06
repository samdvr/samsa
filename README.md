# Samsa - Distributed Storage System

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org)

A distributed streaming storage system built in Rust with gRPC, PostgreSQL, and etcd. This implementation focuses on scalability, observability, and operational excellence while providing a simple yet powerful API for stream-based data operations.

## Architecture

Samsa is a distributed streaming storage system with a unified architecture:

- **Unified Node**: Single server process that handles all operations (accounts, buckets, streams, data)
- **PostgreSQL**: Metadata storage with full ACID guarantees
- **Object Storage**: Configurable object storage backend (S3-compatible)
- **etcd**: Service discovery and coordination
- **Observability**: Comprehensive metrics, tracing, and logging

## Features

### Core Capabilities

- **Account Management**: Bucket lifecycle and access token management
- **Stream Operations**: Create, configure, and manage data streams
- **Data Operations**: High-performance append and read operations with batching
- **Service Discovery**: Automatic node discovery and health monitoring via etcd
- **Observability**: Prometheus metrics, OpenTelemetry tracing, structured logging

### Stream Processing

- **UUID v7 Sequence IDs**: Time-ordered, globally unique record identifiers
- **Flexible Read Patterns**: Read by sequence ID, timestamp, or tail offset
- **Streaming APIs**: Real-time subscriptions with session management
- **Batched Operations**: High-performance batching for both reads and writes
- **Configurable Storage**: Multiple storage classes and retention policies

### Enterprise Features

- **PostgreSQL Integration**: Reliable metadata storage with full ACID compliance
- **Configuration Management**: Layered configuration system with validation
- **Access Control**: Token-based authentication with fine-grained permissions
- **Metrics & Monitoring**: Comprehensive observability with Prometheus integration
- **Health Checks**: Built-in health monitoring and graceful degradation

## Quick Start

### Prerequisites

1. **etcd**: For service discovery and coordination
2. **PostgreSQL**: For metadata storage
3. **Rust**: For building the project (1.70+)

### Running the System

1. **Start Dependencies**:

   ```bash
   # Start etcd using Docker
   docker run -d --name etcd-samsa \
     -p 2379:2379 -p 2380:2380 \
     quay.io/coreos/etcd:v3.5.14 \
     /usr/local/bin/etcd \
     --data-dir=/etcd-data \
     --listen-client-urls=http://0.0.0.0:2379 \
     --advertise-client-urls=http://0.0.0.0:2379 \
     --listen-peer-urls=http://0.0.0.0:2380 \
     --initial-advertise-peer-urls=http://0.0.0.0:2380 \
     --initial-cluster=default=http://0.0.0.0:2380 \
     --initial-cluster-token=etcd-cluster-1 \
     --initial-cluster-state=new

   # Or use docker-compose for testing
   docker-compose -f docker-compose.test.yml up -d

   # Setup PostgreSQL (example with local installation)
   createdb samsa_metadata
   ```

2. **Configure the System**:

   ```bash
   # Copy example configuration
   cp config/example.toml config/app.toml

   # Edit configuration for your environment
   # See CONFIG_GUIDE.md for detailed options
   ```

3. **Start the Server**:

   ```bash
   # Build and run
   cargo build --release

   # Start with default configuration
   ./target/release/server

   # Or with custom configuration
   SERVER_PORT=50052 \
   ETCD_ENDPOINTS=http://localhost:2379 \
   cargo run --bin server
   ```

### Using the CLI

The CLI provides comprehensive access to all Samsa functionality:

```bash
# Build the CLI
cargo build --bin samsa-cli --release

# Create a bucket
./target/release/samsa-cli bucket create my-bucket --auto-create-on-append

# Create a stream
./target/release/samsa-cli stream -b my-bucket create my-stream

# Append data
echo "Hello, World!" | ./target/release/samsa-cli data -b my-bucket -s my-stream append --from-stdin

# Read data
./target/release/samsa-cli data -b my-bucket -s my-stream read
```

For detailed CLI documentation, see [CLI_README.md](src/cli/CLI_README.md).

## Configuration

Samsa uses a comprehensive, layered configuration system. Configuration can be provided via:

- **Configuration files** (TOML format)
- **Environment variables**
- **Command-line arguments**

### Key Configuration Sections

- **Server**: Node address, port, etcd endpoints, heartbeat settings
- **Storage**: Batch sizes, flush intervals, cleanup policies
- **Database**: PostgreSQL connection settings
- **Observability**: Metrics, logging, and tracing configuration

See [CONFIG_GUIDE.md](CONFIG_GUIDE.md) for comprehensive configuration documentation.

### Example Configuration

```toml
[server]
address = "0.0.0.0"
port = 50052
etcd_endpoints = ["http://localhost:2379"]
heartbeat_interval_secs = 30
lease_ttl_secs = 60

[storage]
batch_size = 100
batch_max_bytes = 1048576
batch_flush_interval_ms = 5000

[observability]
metrics_port = 9090
log_level = "info"
enable_otel_tracing = false

[database]
host = "localhost"
port = 5432
username = "postgres"
database_name = "samsa_metadata"
```

## Development

### Building

```bash
# Build all binaries
cargo build

# Build specific binaries
cargo build --bin server
cargo build --bin samsa-cli

# Build for release
cargo build --release
```

### Testing

```bash
# Run unit tests
cargo test

# Run integration tests (requires infrastructure)
make test

# Run specific test categories
make test-basic
make test-performance
make test-errors

# Development testing (faster)
make test-dev
```

### Project Structure

```
samsa/
├── src/
│   ├── cli/                    # Command-line interface
│   │   ├── main.rs            # CLI application
│   │   └── CLI_README.md      # CLI documentation
│   ├── common/                # Shared utilities and services
│   │   ├── config.rs          # Configuration management
│   │   ├── error.rs           # Error types
│   │   ├── etcd_client.rs     # etcd service discovery
│   │   ├── storage.rs         # Storage layer with batching
│   │   ├── metadata.rs        # Metadata repository interface
│   │   ├── postgres_metadata_repository.rs # PostgreSQL implementation
│   │   ├── observability.rs   # Metrics and tracing
│   │   ├── metrics.rs         # Metrics collection
│   │   └── ...               # Additional common modules
│   ├── server/                # Unified server implementation
│   │   ├── main.rs           # Server binary entry point
│   │   ├── mod.rs            # Server core logic
│   │   └── handlers/         # gRPC service handlers
│   │       ├── account.rs    # Account service implementation
│   │       ├── bucket.rs     # Bucket service implementation
│   │       └── stream.rs     # Stream service implementation
│   └── lib.rs                # Library root
├── proto/
│   └── samsa.proto              # gRPC service definitions
├── config/                   # Configuration files
│   ├── example.toml          # Example configuration
│   ├── development.toml      # Development settings
│   └── production.toml       # Production settings
├── tests/                    # Integration tests
├── examples/                 # Usage examples
├── migrations/               # Database migrations
└── scripts/                  # Utility scripts
```

### API Documentation

The system exposes three main gRPC services:

- **AccountService**: Bucket and access token management
- **BucketService**: Stream lifecycle management
- **StreamService**: Data append and read operations

See [proto/samsa.proto](proto/samsa.proto) for complete API definitions.

## Performance

The batched storage system provides excellent performance characteristics:

- **High Throughput**: Configurable batching optimizes for bulk operations
- **Low Latency**: Tunable flush intervals balance latency vs. throughput
- **Memory Efficiency**: Streaming operations with bounded memory usage
- **Scalability**: Horizontal scaling through multiple nodes

### Performance Tuning

Key configuration parameters for performance:

```toml
[storage]
batch_size = 1000              # Records per batch
batch_max_bytes = 10485760     # 10MB max batch size
batch_flush_interval_ms = 1000 # 1 second flush interval
```

## Monitoring and Observability

### Metrics

Prometheus metrics are exposed on port 9090 by default:

- Request rates and latencies
- Storage utilization and performance
- etcd connectivity and health
- Database connection pool status

### Logging

Structured logging with configurable levels:

```bash
# Enable debug logging
RUST_LOG=samsa=debug ./target/release/server

# JSON formatted logs for production
LOG_LEVEL=info SERVICE_NAME=samsa-prod ./target/release/server
```

### Tracing

Optional OpenTelemetry integration for distributed tracing:

```toml
[observability]
enable_otel_tracing = true
otel_endpoint = "http://localhost:4317"
```

## Deployment

### Production Requirements

1. **Infrastructure**:

   - etcd cluster (3+ nodes recommended)
   - PostgreSQL database (with appropriate sizing)
   - S3-compatible object storage
   - Load balancer (for multiple Samsa nodes)

2. **Configuration**:

   - Appropriate batch sizes for workload
   - Database connection pooling
   - Monitoring and alerting setup

3. **Security**:
   - TLS encryption for all communications
   - Proper access token management
   - Network security policies

### Docker Deployment

```dockerfile
FROM rust:1.87 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/server /usr/local/bin/
COPY --from=builder /app/target/release/samsa-cli /usr/local/bin/
EXPOSE 50052 9090
CMD ["server"]
```

## Troubleshooting

### Common Issues

1. **Database Connection Issues**:

   - Verify PostgreSQL is running and accessible
   - Check database credentials and permissions
   - Ensure database exists and migrations are applied

2. **etcd Connectivity**:

   - Verify etcd endpoints are correct and accessible
   - Check network connectivity and firewall rules
   - Monitor etcd cluster health

3. **Performance Issues**:
   - Adjust batch sizes based on workload patterns
   - Monitor database performance and indexing
   - Check object storage latency and throughput

### Debug Mode

```bash
# Enable comprehensive debug logging
RUST_LOG=samsa=debug,sqlx=debug cargo run --bin server

# Enable specific module debugging
RUST_LOG=samsa::storage=debug,samsa::etcd_client=trace cargo run --bin server
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run the full test suite: `make test`
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
