#!/bin/bash

# Sage Database Stop Script
# Stops all running database processes

echo "Stopping Sage distributed database..."

# Stop processes using saved PIDs if available
if [ -f .sage_pids ]; then
    echo "Stopping processes from PID file..."
    PIDS=$(cat .sage_pids)
    for pid in $PIDS; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping process $pid..."
            kill "$pid"
        fi
    done
    rm .sage_pids
fi

# Also kill any processes on our ports as backup
echo "Cleaning up any remaining processes on ports 4000-4103..."
lsof -ti:4000,4101,4102,4103 | xargs -r kill 2>/dev/null || true

# Wait for processes to terminate gracefully
sleep 2

# Force kill if needed
lsof -ti:4000,4101,4102,4103 | xargs -r kill -9 2>/dev/null || true

echo "✅ Sage database stopped"