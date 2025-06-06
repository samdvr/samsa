# Samsa Testing Guide

This guide covers the consolidated testing setup for Samsa , which provides a unified approach to running all tests with proper infrastructure management.

## Overview

The testing system has been consolidated into a single comprehensive suite that includes:

- **Infrastructure Health Tests** - Verify basic system connectivity and setup
- **Basic Functionality Tests** - Core operations like bucket/stream creation, read/write
- **Performance Tests** - Concurrent operations, high throughput, and stress testing
- **Error Handling Tests** - Edge cases and error scenarios

## Quick Start

### Run All Tests (Recommended)

```bash
./run_consolidated_tests.sh
```

### Run Specific Test Categories

```bash
# Health check only
./run_consolidated_tests.sh health

# Basic functionality
./run_consolidated_tests.sh basic

# Performance tests
./run_consolidated_tests.sh performance

# Error handling
./run_consolidated_tests.sh errors
```

### Development Mode (Faster Tests)

```bash
# Run lightweight tests for faster development feedback
./run_consolidated_tests.sh -d basic

# Run all tests in development mode
./run_consolidated_tests.sh -d all
```

## Test Runner Options

```bash
./run_consolidated_tests.sh [OPTIONS] [TEST_FILTER]
```

### Options

- `-h, --help` - Show help message
- `-d, --dev` - Development mode (reduced test parameters)
- `-f, --full` - Full test suite (default)
- `-c, --clean` - Clean rebuild all containers
- `-k, --keep` - Keep infrastructure running after tests
- `-t, --timeout SEC` - Test timeout in seconds (default: 300)
- `-v, --verbose` - Verbose output with debug logging
- `--no-docker` - Skip Docker setup (use existing etcd)

### Test Filters

- `health` - Infrastructure health tests only
- `basic` - Basic functionality tests
- `performance` - Performance and stress tests
- `errors` - Error handling tests
- `all` - All tests (default)

## Examples

```bash
# Quick health check
./run_consolidated_tests.sh health

# Development workflow - fast basic tests
./run_consolidated_tests.sh -d basic

# Full performance testing with verbose output
./run_consolidated_tests.sh -v performance

# Clean rebuild and full test suite
./run_consolidated_tests.sh -c all

# Keep infrastructure running for debugging
./run_consolidated_tests.sh -k basic

# Use existing etcd (no Docker)
./run_consolidated_tests.sh --no-docker all
```

## Infrastructure Management

### Docker-based (Default)

The test runner automatically manages etcd using Docker Compose:

- Starts etcd container with proper configuration
- Waits for health checks to pass
- Tears down infrastructure after tests (unless `-k` is used)

### External etcd

Use `--no-docker` flag to use an existing etcd instance:

```bash
# Start your own etcd first
etcd --listen-client-urls=http://localhost:2379 --advertise-client-urls=http://localhost:2379

# Then run tests
./run_consolidated_tests.sh --no-docker all
```

## Test Categories Explained

### Infrastructure Health Tests

- Basic connectivity verification
- Service startup validation
- Simple read/write operations
- Environment health checks

**When to run:** First thing when setting up or debugging issues

### Basic Functionality Tests

- Complete workflow testing (bucket → stream → append → read)
- Batch operations
- Multiple streams and buckets
- Large records handling
- Header and timestamp functionality

**When to run:** Core development and validation

### Performance Tests

- Concurrent append operations
- High throughput single stream
- Large batch operations
- Mixed read/write workloads

**When to run:** Performance validation and stress testing

### Error Handling Tests

- Non-existent resource access
- Invalid operations
- Edge cases and boundary conditions

**When to run:** Robustness validation

## Development Workflow

### Quick Development Cycle

```bash
# 1. Make changes to code
# 2. Quick validation
./run_consolidated_tests.sh -d health

# 3. Basic functionality check
./run_consolidated_tests.sh -d basic

# 4. Full validation before commit
./run_consolidated_tests.sh all
```

### Debugging Failed Tests

```bash
# 1. Run with verbose output
./run_consolidated_tests.sh -v health

# 2. Keep infrastructure running for manual inspection
./run_consolidated_tests.sh -k -v basic

# 3. Manual cleanup when done
docker-compose -f docker-compose.test.yml down
```

### Performance Testing

```bash
# Development mode (faster)
./run_consolidated_tests.sh -d performance

# Full performance suite
./run_consolidated_tests.sh performance

# Extended performance testing
Samsa_TEST_MODE=full ./run_consolidated_tests.sh performance
```

## Configuration

### Environment Variables

- `Samsa_TEST_MODE=dev` - Enables development mode (set automatically with `-d`)
- `RUST_LOG=debug` - Enables debug logging (set automatically with `-v`)

### Test Parameters

Test parameters are automatically adjusted based on mode:

**Development Mode (`-d` flag):**

- Concurrent clients: 3 (vs 10 in full mode)
- Records per client: 5 (vs 20 in full mode)
- Throughput test records: 100 (vs 1000 in full mode)
- Batch sizes: 10-50 (vs 50-500 in full mode)

**Full Mode (default):**

- Full test parameters for comprehensive validation
- Longer running times
- Higher stress levels

## Files Structure

```
samsa/
├── run_consolidated_tests.sh      # Main test runner
├── docker-compose.test.yml        # Docker infrastructure
├── Dockerfile.etcd                # Custom etcd container
├── tests/
│   ├── consolidated_tests.rs      # All tests in one file
│   ├── test_utils.rs             # Shared test utilities
│   ├── test_config.rs            # Test configuration
│   └── README.md                 # Legacy test documentation
└── TESTING.md                    # This file
```

## Troubleshooting

### Common Issues

**Port conflicts:**

```bash
# Check what's using the ports
lsof -i :2379 -i :50051 -i :50052

# Kill conflicting processes
./run_consolidated_tests.sh -c all  # Clean restart
```

**Docker issues:**

```bash
# Ensure Docker is running
docker ps

# Clean rebuild containers
./run_consolidated_tests.sh -c all

# Use external etcd if Docker issues persist
./run_consolidated_tests.sh --no-docker all
```

**Compilation errors:**

```bash
# Check compilation first
cargo check

# Fix any compilation errors before running tests
```

**Slow tests:**

```bash
# Use development mode for faster feedback
./run_consolidated_tests.sh -d basic

# Run only health tests for quick validation
./run_consolidated_tests.sh health
```

### Test Failures

**Infrastructure health failures:**

- Check Docker is running
- Verify ports are available
- Try clean rebuild: `./run_consolidated_tests.sh -c health`

**Basic functionality failures:**

- Run health tests first
- Check for port conflicts
- Verify etcd is accessible

**Performance test failures:**

- May indicate resource constraints
- Try development mode: `-d performance`
- Check system load during tests

**Error handling test failures:**

- Usually indicate logic errors
- Run with verbose output: `-v errors`

## Integration with CI/CD

The test runner is designed for CI/CD integration:

```yaml
# Example CI configuration
test:
  script:
    - ./run_consolidated_tests.sh --no-docker all
  services:
    - etcd:v3.5.14
```

For local CI testing:

```bash
# Full CI-like test run
./run_consolidated_tests.sh -f all
```

## Migration from Old Test Setup

The new consolidated setup replaces several individual test files:

- `infrastructure_health_test.rs` → `consolidated_tests.rs::test_infrastructure_health`
- `basic_functionality_tests.rs` → `consolidated_tests.rs::test_*` (basic section)
- `performance_tests.rs` → `consolidated_tests.rs::test_*` (performance section)
- `integration_tests.rs` → Merged into consolidated tests

Old scripts like `quick_test.sh` are superseded by `run_consolidated_tests.sh`.

The new setup provides:

- ✅ Unified test execution
- ✅ Proper infrastructure management
- ✅ Better error handling
- ✅ Development/CI mode support
- ✅ Comprehensive reporting
- ✅ Docker containerization
