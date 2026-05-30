#!/bin/bash
set -e

# Cleanup
echo "Cleaning up..."
pkill -9 titankv-server || true
pkill -9 pd-server || true
rm -rf /tmp/pd1 /tmp/node1 /tmp/node2 /tmp/node3

# Start PD
echo "Starting PD..."
mkdir -p /tmp/pd1
nohup ./bin/pd-server --data-dir=/tmp/pd1 --client-urls=http://127.0.0.1:2379 --peer-urls=http://127.0.0.1:2380 > pd.log 2>&1 &
sleep 2

# Start Nodes
echo "Starting Nodes..."
CLUSTER="1=127.0.0.1:9090,2=127.0.0.1:9091,3=127.0.0.1:9092"

mkdir -p /tmp/node1
nohup ./titankv-server --id=1 --port=9090 --pd=127.0.0.1:2379 --db_path=/tmp/node1 --cluster=$CLUSTER > node1.log 2>&1 &

mkdir -p /tmp/node2
nohup ./titankv-server --id=2 --port=9091 --pd=127.0.0.1:2379 --db_path=/tmp/node2 --cluster=$CLUSTER > node2.log 2>&1 &

mkdir -p /tmp/node3
nohup ./titankv-server --id=3 --port=9092 --pd=127.0.0.1:2379 --db_path=/tmp/node3 --cluster=$CLUSTER > node3.log 2>&1 &

echo "Cluster started. Waiting for initialization..."
sleep 5
echo "Done."
