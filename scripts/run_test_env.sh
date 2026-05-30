#!/bin/bash
set -e

# Cleanup
pkill -9 titankv-server || true
pkill -9 pd-server || true
rm -rf /tmp/pd1 /tmp/node1

# Start PD
./bin/pd-server --data-dir=/tmp/pd1 --client-urls=http://127.0.0.1:2379 --peer-urls=http://127.0.0.1:2380 > pd.log 2>&1 &
echo "PD started"
sleep 2

# Start Node 1
# Use the binary in root if available, otherwise bin/
if [ -f "./titankv-server" ]; then
    ./titankv-server --id=1 --port=9091 --pd=127.0.0.1:2379 --db_path=/tmp/node1 --cluster="1=127.0.0.1:9091" > node1.log 2>&1 &
else
    ./bin/titankv-server --id=1 --port=9091 --pd=127.0.0.1:2379 --db_path=/tmp/node1 --cluster="1=127.0.0.1:9091" > node1.log 2>&1 &
fi
echo "Node 1 started"

sleep 5
echo "Cluster ready"
