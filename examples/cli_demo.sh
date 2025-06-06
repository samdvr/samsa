#!/bin/bash

# Samsa CLI Demo Script
# This script demonstrates the key features of the Samsa CLI

set -e

CLI="./target/release/samsa-cli"
SERVER_BIN="./target/release/server"
NODE_ADDRESS="http://127.0.0.1:50052"
ETCD_CONTAINER="samsa-demo-etcd"
SERVER_PID=""

echo "🚀 Samsa CLI Demo"
echo "=============="
echo ""

# Function to cleanup background processes
cleanup() {
    echo ""
    echo "🧹 Cleaning up..."

    
    if [ ! -z "$SERVER_PID" ]; then
        echo "  Stopping server (PID: $SERVER_PID)..."
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
    
    echo "  Stopping etcd container..."
    docker stop $ETCD_CONTAINER 2>/dev/null || true
    docker rm $ETCD_CONTAINER 2>/dev/null || true
    
    echo "✅ Cleanup complete"
}

# Set up trap to cleanup on exit
trap cleanup EXIT

# Check if binaries exist and build if needed
echo "🔧 Checking binaries..."
NEED_BUILD=false

if [ ! -f "$CLI" ]; then
    echo "  CLI binary not found"
    NEED_BUILD=true
fi

if [ ! -f "$SERVER_BIN" ]; then
    echo "  Server binary not found"
    NEED_BUILD=true
fi

if [ "$NEED_BUILD" = true ]; then
    echo "⚠️  Building binaries in release mode..."
    cargo build --release --bin samsa-cli --bin server
    echo "✅ Build complete"
else
    echo "✅ Binaries found"
fi

echo "📋 Using node at: $NODE_ADDRESS"
echo ""

# Start etcd in Docker
echo "🗃️  Starting etcd..."
if docker ps | grep -q $ETCD_CONTAINER; then
    echo "  etcd container already running"
else
    docker run -d --name $ETCD_CONTAINER \
        -p 2379:2379 -p 2380:2380 \
        quay.io/coreos/etcd:v3.5.0 \
        /usr/local/bin/etcd \
        --data-dir=/etcd-data \
        --listen-client-urls=http://0.0.0.0:2379 \
        --advertise-client-urls=http://0.0.0.0:2379 \
        --listen-peer-urls=http://0.0.0.0:2380 \
        --initial-advertise-peer-urls=http://0.0.0.0:2380 \
        --initial-cluster=default=http://0.0.0.0:2380 \
        --initial-cluster-token=etcd-cluster-1 \
        --initial-cluster-state=new > /dev/null

    echo "  Waiting for etcd to be ready..."
    
    # Wait for etcd to be responsive with better error handling
    for i in {1..30}; do
        if curl -sf http://localhost:2379/health > /dev/null 2>&1; then
            echo "  ✅ etcd is ready"
            break
        fi
        if [ $i -eq 30 ]; then
            echo "  ❌ etcd failed to start within 30 seconds"
            echo "  Check etcd logs: docker logs $ETCD_CONTAINER"
            exit 1
        fi
        echo "  Waiting for etcd... (attempt $i/30)"
        sleep 1
    done
fi

# Start server (now serves all services)
echo "🏗️  Starting Samsa node (unified services)..."
SERVER_PORT=50052 \
ETCD_ENDPOINTS=http://localhost:2379 \
$SERVER_BIN > /tmp/server.log 2>&1 &
SERVER_PID=$!

echo "  Node PID: $SERVER_PID"
echo "  Waiting for node to be ready..."

# Wait for node to be responsive using gRPC with better error reporting
for i in {1..30}; do
    if $CLI -a $NODE_ADDRESS bucket list > /dev/null 2>&1; then
        echo "  ✅ Node is ready and serving all services"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "  ❌ Node failed to start within 30 seconds. Check logs:"
        echo "     tail -20 /tmp/server.log"
        echo ""
        echo "  Recent server logs:"
        tail -10 /tmp/server.log
        exit 1
    fi
    
    # Check if server process is still running
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "  ❌ Server process died. Check logs:"
        echo "     tail -20 /tmp/server.log"
        echo ""
        echo "  Recent server logs:"
        tail -10 /tmp/server.log
        exit 1
    fi
    
    echo "  Waiting for server... (attempt $i/30)"
    sleep 1
done

echo ""
echo "🎬 Starting demo operations..."
echo ""

echo "1️⃣  Creating a bucket..."
$CLI -a $NODE_ADDRESS bucket create demo-bucket --auto-create-on-append
echo ""

echo "2️⃣  Listing buckets..."
$CLI -a $NODE_ADDRESS bucket list
echo ""

echo "3️⃣  Creating a stream..."
$CLI -a $NODE_ADDRESS stream -b demo-bucket create events --retention-age 86400 --storage-class 1
echo ""

echo "4️⃣  Listing streams..."
$CLI -a $NODE_ADDRESS stream -b demo-bucket list
echo ""

echo "5️⃣  Appending single record..."
$CLI -a $NODE_ADDRESS data -b demo-bucket -s events append --body "Hello, Samsa!"
echo ""

echo "6️⃣  Appending records from stdin..."
cat << EOF | $CLI -a $NODE_ADDRESS data -b demo-bucket -s events append --from-stdin
{"body": "Event 1", "timestamp": $(date +%s)000}
{"body": "Event 2"}
Raw text event
EOF
echo ""

echo "7️⃣  Checking stream tail..."
$CLI -a $NODE_ADDRESS stream -b demo-bucket tail events
echo ""

echo "8️⃣  Reading records (JSON format)..."
$CLI -a $NODE_ADDRESS data -b demo-bucket -s events read --format json
echo ""

echo "9️⃣  Reading records (Raw format)..."
$CLI -a $NODE_ADDRESS data -b demo-bucket -s events read --format raw --limit 2
echo ""

echo "🔟 Getting bucket configuration..."
$CLI -a $NODE_ADDRESS bucket config demo-bucket
echo ""

echo "🎯 Demo complete!"
echo ""
echo "💡 Try these commands next (in another terminal):"
echo "   - Subscribe to real-time updates:"
echo "     $CLI -a $NODE_ADDRESS data -b demo-bucket -s events subscribe --heartbeats"
echo ""
echo "   - Clean up (done automatically when this script exits):"
echo "     $CLI -a $NODE_ADDRESS stream -b demo-bucket delete events"
echo "     $CLI -a $NODE_ADDRESS bucket delete demo-bucket"
echo ""
echo "📋 Service logs available at:"
echo "   - Node: /tmp/server.log"
echo ""
echo "Press Ctrl+C to stop all services and clean up."

# Keep the script running until user interrupts
while true; do
    sleep 1
done 