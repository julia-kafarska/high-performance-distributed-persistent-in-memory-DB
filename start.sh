#!/bin/bash

# Sage Database Startup Script
# Starts all shards and coordinator in the background

echo "Starting Sage distributed database..."

# Kill any existing processes on our ports
echo "Cleaning up existing processes..."
lsof -ti:4000,4101,4102,4103 | xargs -r kill -9 2>/dev/null || true

# Wait a moment for cleanup
sleep 1

# Create data directories if they don't exist
mkdir -p data1 data2 data3

# Start shard servers
echo "Starting shard servers..."
node shard.js --port 4101 --data data1 > logs/shard1.log 2>&1 &
SHARD1_PID=$!
echo "Shard 1 started (PID: $SHARD1_PID) on port 4101"

node shard.js --port 4102 --data data2 > logs/shard2.log 2>&1 &
SHARD2_PID=$!
echo "Shard 2 started (PID: $SHARD2_PID) on port 4102"

node shard.js --port 4103 --data data3 > logs/shard3.log 2>&1 &
SHARD3_PID=$!
echo "Shard 3 started (PID: $SHARD3_PID) on port 4103"

# Wait for shards to start
echo "Waiting for shards to initialize..."
sleep 2

# Start coordinator
echo "Starting coordinator..."
node coordinator.js --port 4000 --shards http://127.0.0.1:4101,http://127.0.0.1:4102,http://127.0.0.1:4103 --vnodes 100 > logs/coordinator.log 2>&1 &
COORDINATOR_PID=$!
echo "Coordinator started (PID: $COORDINATOR_PID) on port 4000"

# Wait for coordinator to start
sleep 1

# Save PIDs for easy cleanup
echo "$SHARD1_PID $SHARD2_PID $SHARD3_PID $COORDINATOR_PID" > .sage_pids

echo ""
echo "✅ Sage database is running!"
echo ""
echo "Services:"
echo "  - Coordinator: http://localhost:4000"
echo "  - Shard 1:     http://localhost:4101"
echo "  - Shard 2:     http://localhost:4102" 
echo "  - Shard 3:     http://localhost:4103"
echo ""
echo "Test the database:"
echo "  node client_example.js"
echo ""
echo "Stop the database:"
echo "  ./stop.sh"
echo ""
echo "View logs:"
echo "  tail -f logs/*.log"