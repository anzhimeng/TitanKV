#!/bin/bash
set -e

GC_THRESHOLD=${1:-0.5}
DIRECT_IO=${2:-false}
CONCURRENCY=${3:-20}
REQUESTS=${4:-100}
USE_TXN=${5:-true}

echo "Starting Benchmark with GC_THRESHOLD=$GC_THRESHOLD, DIRECT_IO=$DIRECT_IO, CONCURRENCY=$CONCURRENCY, REQUESTS=$REQUESTS, USE_TXN=$USE_TXN"

# 1. Clean Environment
./scripts/clean_env.sh > /dev/null 2>&1

# 2. Start PD
echo "Starting PD..."
nohup ./bin/pd-server --data-dir=/tmp/pd1 --client-urls=http://127.0.0.1:2379 --peer-urls=http://127.0.0.1:2380 > pd.log 2>&1 &
PD_PID=$!
sleep 5

# 3. Start Nodes
echo "Starting Nodes..."
CLUSTER="1=127.0.0.1:9090,2=127.0.0.1:9091,3=127.0.0.1:9092"
# Tuning Parameters: 
# Write Buffer Size: 128MB
# Block Cache Size: 256MB (per node)
# Min Blob Size: 4KB
# Bloom Filter Bits: 10
TUNING_ARGS="--write_buffer_size=134217728 --block_cache_size=268435456 --min_blob_size=4096 --bloom_filter_bits=10"

nohup ./bin/titankv-server --id=1 --port=9090 --pd=127.0.0.1:2379 --db_path=/tmp/node1 --cluster=$CLUSTER --gc_threshold=$GC_THRESHOLD --direct_io=$DIRECT_IO $TUNING_ARGS > node1.log 2>&1 &
NODE1_PID=$!
nohup ./bin/titankv-server --id=2 --port=9091 --pd=127.0.0.1:2379 --db_path=/tmp/node2 --cluster=$CLUSTER --gc_threshold=$GC_THRESHOLD --direct_io=$DIRECT_IO $TUNING_ARGS > node2.log 2>&1 &
NODE2_PID=$!
nohup ./bin/titankv-server --id=3 --port=9092 --pd=127.0.0.1:2379 --db_path=/tmp/node3 --cluster=$CLUSTER --gc_threshold=$GC_THRESHOLD --direct_io=$DIRECT_IO $TUNING_ARGS > node3.log 2>&1 &
NODE3_PID=$!

echo "Waiting for cluster to bootstrap (20s)..."
sleep 20

# 4. Run Benchmark
echo "Running Benchmark..."
timeout 300s ./bin/bench-txn -c $CONCURRENCY -n $REQUESTS -keys 100000 -pd-addr 127.0.0.1:2379 -txn=$USE_TXN

# 5. Cleanup
echo "Cleaning up..."
kill $PD_PID $NODE1_PID $NODE2_PID $NODE3_PID || true
wait $PD_PID $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
./scripts/clean_env.sh > /dev/null 2>&1
echo "Benchmark Finished."
