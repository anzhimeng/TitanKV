#!/bin/bash
set -e

echo "Stopping all services..."
pkill -9 bench-txn || true
pkill -9 pd-server || true
pkill -9 titankv-server || true
sleep 2

# Kill stubborn processes
fuser -k -n tcp 2379 2380 9000 9090 9091 9092 >/dev/null 2>&1 || true

echo "Cleaning data directories..."
rm -rf /tmp/pd1 /tmp/node1 /tmp/node2 /tmp/node3

echo "Environment cleaned."
