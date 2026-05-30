#!/bin/bash

run_test() {
    GC=$1
    DIO=$2
    NAME="gc_${GC}_dio_${DIO}"
    echo "Running scenario: GC_Threshold=$GC, DirectIO=$DIO ..."
    
    # Run benchmark and capture output (Concurrency=50, Requests=200)
    if ./scripts/run_bench.sh $GC $DIO 50 200 > "bench_${NAME}.log" 2>&1; then
        TPS=$(grep "Txn TPS" "bench_${NAME}.log" | awk '{print $3}')
        echo "Result $NAME: TPS = $TPS"
    else
        echo "Result $NAME: FAILED (check bench_${NAME}.log)"
    fi
}

echo "Starting Optimization Benchmarks..."

# 1. Baseline
run_test 0.5 false

# 2. Aggressive GC (More frequent compaction)
run_test 0.2 false

# 3. DirectIO (Bypass OS cache)
run_test 0.5 true

echo "Optimization Benchmarks Finished."
