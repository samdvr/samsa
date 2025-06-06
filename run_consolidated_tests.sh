#!/bin/bash
# Consolidated Test Runner for Samsa 
# This script manages the entire test lifecycle including Docker infrastructure

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ETCD_CONTAINER_NAME="samsa-test-etcd"
COMPOSE_FILE="docker-compose.test.yml"
TEST_TIMEOUT_SECONDS=300
HEALTH_CHECK_MAX_ATTEMPTS=30

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS] [TEST_FILTER]

Consolidated test runner for Samsa .

Options:
    -h, --help          Show this help message
    -d, --dev           Run in development mode (faster, lighter tests)
    -f, --full          Run full test suite (default)
    -c, --clean         Clean up and rebuild all containers
    -k, --keep          Keep infrastructure running after tests
    -t, --timeout SEC   Test timeout in seconds (default: $TEST_TIMEOUT_SECONDS)
    -v, --verbose       Verbose output
    --no-docker         Skip Docker setup (use existing etcd)

Test Filters:
    health              Run only infrastructure health tests
    basic               Run basic functionality tests
    performance         Run performance tests
    errors              Run error handling tests
    all                 Run all tests (default)

Examples:
    $0                  # Run all tests with Docker infrastructure
    $0 -d basic         # Run basic tests in development mode
    $0 -c performance   # Clean rebuild and run performance tests
    $0 --no-docker all  # Run all tests using existing etcd
EOF
}

# Parse command line arguments
DEV_MODE=false
CLEAN_MODE=false
KEEP_INFRASTRUCTURE=false
TEST_TIMEOUT=$TEST_TIMEOUT_SECONDS
VERBOSE=false
NO_DOCKER=false
TEST_FILTER="all"

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -d|--dev)
            DEV_MODE=true
            shift
            ;;
        -f|--full)
            DEV_MODE=false
            shift
            ;;
        -c|--clean)
            CLEAN_MODE=true
            shift
            ;;
        -k|--keep)
            KEEP_INFRASTRUCTURE=true
            shift
            ;;
        -t|--timeout)
            TEST_TIMEOUT="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --no-docker)
            NO_DOCKER=true
            shift
            ;;
        health|basic|performance|errors|all)
            TEST_FILTER="$1"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Set environment variables
if [ "$DEV_MODE" = true ]; then
    export Samsa_TEST_MODE=dev
    log_info "Running in development mode (reduced test parameters)"
else
    export Samsa_TEST_MODE=full
    log_info "Running full test suite"
fi

if [ "$VERBOSE" = true ]; then
    export RUST_LOG=debug
    log_info "Verbose logging enabled"
fi

# Function to check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not available"
        return 1
    fi
    
    if ! docker ps &> /dev/null; then
        log_error "Docker daemon is not running"
        return 1
    fi
    
    return 0
}

# Function to check if docker-compose is available
check_docker_compose() {
    if command -v docker-compose &> /dev/null; then
        COMPOSE_CMD="docker-compose"
    elif docker compose version &> /dev/null; then
        COMPOSE_CMD="docker compose"
    else
        log_error "Neither docker-compose nor 'docker compose' is available"
        return 1
    fi
    
    return 0
}

# Function to clean up existing infrastructure
cleanup_infrastructure() {
    log_info "Cleaning up existing infrastructure..."
    
    # Stop and remove containers
    if [ "$NO_DOCKER" = false ]; then
        docker stop $ETCD_CONTAINER_NAME 2>/dev/null || true
        docker rm $ETCD_CONTAINER_NAME 2>/dev/null || true
        
        if [ -f "$COMPOSE_FILE" ]; then
            $COMPOSE_CMD -f "$COMPOSE_FILE" down -v 2>/dev/null || true
        fi
    fi
    
    # Kill any lingering processes
    pkill -f "target/.*server" 2>/dev/null || true
    
    # Clean up test data
    rm -rf test-etcd-data 2>/dev/null || true
    rm -f etcd.pid etcd.log 2>/dev/null || true
    
    log_success "Infrastructure cleanup completed"
}

# Function to build binaries
build_binaries() {
    log_info "Building Samsa binaries..."
    
    if [ "$VERBOSE" = true ]; then
        cargo build --bin server
    else
        cargo build --bin server --quiet
    fi
    
    if [ $? -ne 0 ]; then
        log_error "Failed to build binaries"
        exit 1
    fi
    
    log_success "Binaries built successfully"
}

# Function to start Docker infrastructure
start_docker_infrastructure() {
    log_info "Starting Docker infrastructure..."
    
    if [ "$CLEAN_MODE" = true ]; then
        log_info "Clean mode: rebuilding containers..."
        $COMPOSE_CMD -f "$COMPOSE_FILE" build --no-cache
    fi
    
    # Start etcd
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d etcd
    
    # Wait for etcd to be healthy
    log_info "Waiting for etcd to be ready..."
    for i in $(seq 1 $HEALTH_CHECK_MAX_ATTEMPTS); do
        if docker exec $ETCD_CONTAINER_NAME etcdctl endpoint health &>/dev/null; then
            log_success "etcd is ready"
            return 0
        fi
        
        if [ $((i % 5)) -eq 0 ]; then
            log_info "Still waiting for etcd... ($i/$HEALTH_CHECK_MAX_ATTEMPTS)"
        fi
        
        sleep 2
    done
    
    log_error "etcd failed to start within expected time"
    return 1
}

# Function to check if external etcd is available
check_external_etcd() {
    log_info "Checking for external etcd..."
    
    # Try to connect to etcd
    if curl -s http://localhost:2379/health >/dev/null 2>&1; then
        log_success "External etcd is available"
        return 0
    else
        log_warning "External etcd is not available"
        return 1
    fi
}

# Function to setup infrastructure
setup_infrastructure() {
    log_info "Setting up test infrastructure..."
    
    if [ "$NO_DOCKER" = true ]; then
        if ! check_external_etcd; then
            log_error "No Docker mode specified but external etcd is not available"
            exit 1
        fi
        return 0
    fi
    
    if ! check_docker; then
        log_error "Docker setup failed"
        exit 1
    fi
    
    if ! check_docker_compose; then
        log_error "Docker Compose setup failed"
        exit 1
    fi
    
    if ! start_docker_infrastructure; then
        log_error "Failed to start Docker infrastructure"
        exit 1
    fi
    
    log_success "Infrastructure setup completed"
}

# Determine timeout command
get_timeout_cmd() {
    if command -v timeout >/dev/null 2>&1; then
        echo "timeout"
    elif command -v gtimeout >/dev/null 2>&1; then
        echo "gtimeout"
    else
        echo ""
    fi
}

TIMEOUT_CMD=$(get_timeout_cmd)

# Function to run command with timeout if available
run_with_timeout() {
    local timeout_seconds="$1"
    shift
    
    if [ -n "$TIMEOUT_CMD" ]; then
        $TIMEOUT_CMD "$timeout_seconds" "$@"
    else
        log_info "Running without timeout (timeout command not available)"
        "$@"
    fi
}

# Function to run specific test categories
run_tests() {
    local filter="$1"
    
    log_info "Running tests with filter: $filter"
    
    # Set test timeout
    export RUST_TEST_TIME_UNIT=60000 # 60 seconds per test
    export RUST_TEST_TIME_INTEGRATION=300000 # 5 minutes for integration tests
    
    if [ "$filter" = "health" ]; then
        log_info "Running infrastructure health tests..."
        run_with_timeout $TEST_TIMEOUT cargo test test_infrastructure_health --test consolidated_tests -- --nocapture
    elif [ "$filter" = "basic" ]; then
        log_info "Running basic functionality tests..."
        run_with_timeout $TEST_TIMEOUT cargo test "test_complete_workflow|test_batch_operations|test_multiple_streams_and_buckets|test_large_records_and_batches|test_record_headers|test_stream_timestamps" --test consolidated_tests -- --nocapture
    elif [ "$filter" = "performance" ]; then
        log_info "Running performance tests..."
        run_with_timeout $TEST_TIMEOUT cargo test "test_concurrent_appends|test_high_throughput_single_stream|test_large_batch_operations" --test consolidated_tests -- --nocapture
    elif [ "$filter" = "errors" ]; then
        log_info "Running error handling tests..."
        run_with_timeout $TEST_TIMEOUT cargo test test_error_scenarios --test consolidated_tests -- --nocapture
    else
        log_info "Running all tests..."
        run_with_timeout $TEST_TIMEOUT cargo test --test consolidated_tests -- --nocapture
    fi
    
    local test_result=$?
    
    if [ $test_result -eq 0 ]; then
        log_success "Tests completed successfully!"
    else
        log_error "Tests failed with exit code $test_result"
    fi
    
    return $test_result
}

# Function to teardown infrastructure
teardown_infrastructure() {
    if [ "$KEEP_INFRASTRUCTURE" = true ]; then
        log_info "Keeping infrastructure running as requested"
        log_info "To stop later, run: $COMPOSE_CMD -f $COMPOSE_FILE down"
        return 0
    fi
    
    log_info "Tearing down test infrastructure..."
    
    if [ "$NO_DOCKER" = false ]; then
        $COMPOSE_CMD -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    fi
    
    # Clean up any lingering processes
    pkill -f "target/.*server" 2>/dev/null || true
    
    log_success "Infrastructure teardown completed"
}

# Function to display test summary
show_test_summary() {
    local test_result=$1
    local duration=$2
    
    echo ""
    echo "=================================="
    echo "         TEST SUMMARY"
    echo "=================================="
    echo "Filter: $TEST_FILTER"
    echo "Mode: $([ "$DEV_MODE" = true ] && echo "Development" || echo "Full")"
    echo "Duration: ${duration}s"
    echo "Docker: $([ "$NO_DOCKER" = true ] && echo "Disabled" || echo "Enabled")"
    
    if [ $test_result -eq 0 ]; then
        echo -e "Result: ${GREEN}PASSED${NC}"
    else
        echo -e "Result: ${RED}FAILED${NC}"
        echo ""
        echo "Troubleshooting tips:"
        echo "  - Check if ports 2379, 50052 are available"
        echo "  - Ensure Docker is running (if using Docker mode)"
        echo "  - Try running with -c flag to clean rebuild"
        echo "  - Use -v flag for verbose output"
        echo "  - Run health tests first: $0 health"
    fi
    echo "=================================="
}

# Trap to ensure cleanup on exit
trap 'teardown_infrastructure' EXIT

# Main execution
main() {
    local start_time=$(date +%s)
    
    log_info "Starting Samsa  Consolidated Test Runner"
    log_info "Test filter: $TEST_FILTER"
    log_info "Mode: $([ "$DEV_MODE" = true ] && echo "Development" || echo "Full")"
    
    # Initial cleanup
    if [ "$CLEAN_MODE" = true ]; then
        cleanup_infrastructure
    fi
    
    # Build binaries
    build_binaries
    
    # Setup infrastructure
    setup_infrastructure
    
    # Run tests
    run_tests "$TEST_FILTER"
    local test_result=$?
    
    # Calculate duration
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Show summary
    show_test_summary $test_result $duration
    
    return $test_result
}

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    cd "$SCRIPT_DIR"
    main "$@"
fi 