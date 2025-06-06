# Samsa  Test Makefile
#
# Common test operations for Samsa 
#

.PHONY: help test test-dev test-health test-basic test-performance test-errors clean build docker-up docker-down

# Default target
help:
	@echo "Samsa  Test Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  test            - Run all tests (default)"
	@echo "  test-dev        - Run all tests in development mode (faster)"
	@echo "  test-health     - Run infrastructure health tests only"
	@echo "  test-basic      - Run basic functionality tests"
	@echo "  test-performance - Run performance tests"
	@echo "  test-errors     - Run error handling tests"
	@echo "  clean           - Clean rebuild and run all tests"
	@echo "  build           - Build binaries only"
	@echo "  docker-up       - Start Docker infrastructure"
	@echo "  docker-down     - Stop Docker infrastructure"
	@echo ""
	@echo "Examples:"
	@echo "  make test-dev   # Quick development testing"
	@echo "  make clean      # Clean rebuild and full test"
	@echo "  make test-basic # Test core functionality"

# Default test target
test:
	./run_consolidated_tests.sh all

# Development mode (faster tests)
test-dev:
	./run_consolidated_tests.sh -d all

# Specific test categories
test-health:
	./run_consolidated_tests.sh health

test-basic:
	./run_consolidated_tests.sh basic

test-performance:
	./run_consolidated_tests.sh performance

test-errors:
	./run_consolidated_tests.sh errors

# Clean rebuild
clean:
	./run_consolidated_tests.sh -c all

# Build only
build:
	cargo build --bin router --bin server

# Docker management
docker-up:
	docker-compose -f docker-compose.test.yml up -d

docker-down:
	docker-compose -f docker-compose.test.yml down -v 