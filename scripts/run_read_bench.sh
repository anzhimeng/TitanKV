#!/bin/bash
set -e

CONCURRENCY=${1:-50}
REQUESTS=${2:-200}

echo "Starting ReadIndex Benchmark with CONCURRENCY=$CONCURRENCY, REQUESTS=$REQUESTS"

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
TUNING_ARGS="--write_buffer_size=134217728 --block_cache_size=268435456 --min_blob_size=4096 --bloom_filter_bits=10"

nohup ./bin/titankv-server --id=1 --port=9090 --pd=127.0.0.1:2379 --db_path=/tmp/node1 --cluster=$CLUSTER $TUNING_ARGS > node1.log 2>&1 &
NODE1_PID=$!
nohup ./bin/titankv-server --id=2 --port=9091 --pd=127.0.0.1:2379 --db_path=/tmp/node2 --cluster=$CLUSTER $TUNING_ARGS > node2.log 2>&1 &
NODE2_PID=$!
nohup ./bin/titankv-server --id=3 --port=9092 --pd=127.0.0.1:2379 --db_path=/tmp/node3 --cluster=$CLUSTER $TUNING_ARGS > node3.log 2>&1 &
NODE3_PID=$!

echo "Waiting for cluster to bootstrap (20s)..."
sleep 20

# 4. Run Benchmark
echo "Running ReadIndex Benchmark..."
./bin/bench_read -c $CONCURRENCY -n $REQUESTS -pd 127.0.0.1:2379

# 5. Cleanup
echo "Cleaning up..."
kill $PD_PID $NODE1_PID $NODE2_PID $NODE3_PID || true
wait $PD_PID $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
./scripts/clean_env.sh > /dev/null 2>&1
echo "Benchmark Finished."
