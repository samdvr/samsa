#!/bin/bash
# Build script for faster test execution
# This pre-compiles the binaries to avoid compilation overhead during tests

echo "Building Samsa binaries for faster test execution..."

# Build in release mode for better performance
echo "Building release binaries..."
cargo build --release --bin router --bin server

# Also build debug binaries as fallback
echo "Building debug binaries..."
cargo build --bin router --bin server

echo "✓ Binaries built successfully!"
echo "  Release: target/release/{router,server}"
echo "  Debug:   target/debug/{router,server}"
echo ""
echo "  cargo test --test basic_functionality_tests"
echo "  cargo test --test performance_tests"
echo "  cargo test --tests" 