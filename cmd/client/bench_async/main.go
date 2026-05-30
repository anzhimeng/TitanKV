package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"titankv/pkg/client"
	"titankv/pkg/txn"
)

func main() {
	// Config
	concurrency := 200
	totalTxns := 20000
	// valueSize := 128
	pdAddr := "127.0.0.1:2379"
	
	// 1. Initialize Client
	cli, err := client.NewClient(pdAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	// defer cli.Close() // Close not implemented yet
	var wg sync.WaitGroup
	wg.Add(concurrency)

	start := time.Now()
	var successCount int64
	var latencySum int64 // microseconds

	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < totalTxns/concurrency; j++ {
				key := []byte(fmt.Sprintf("k_%d_%d", workerID, j))
				val := []byte(fmt.Sprintf("v_%d_%d", workerID, j))

				txnStart := time.Now()
				
				// Run Transaction
				tx, err := txn.NewTransaction(context.Background(), cli)
				if err != nil {
					log.Printf("NewTransaction failed: %v", err)
					continue
				}

				tx.Set(key, val)
				err = tx.Commit(context.Background())
				if err != nil {
					log.Printf("Commit failed: %v", err)
					continue
				}

				dur := time.Since(txnStart).Microseconds()
				atomic.AddInt64(&latencySum, dur)
				newCount := atomic.AddInt64(&successCount, 1)
				if newCount % 1000 == 0 {
					fmt.Printf("Completed %d txns\n", newCount)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	tps := float64(successCount) / elapsed.Seconds()
	avgLatency := float64(latencySum) / float64(successCount)

	fmt.Printf("Benchmark Result:\n")
	fmt.Printf("Total Txns: %d\n", successCount)
	fmt.Printf("Elapsed: %v\n", elapsed)
	fmt.Printf("TPS: %.2f\n", tps)
	fmt.Printf("Avg Latency: %.2f us\n", avgLatency)

	// Verify Data
	tx, err := txn.NewTransaction(context.Background(), cli)
	if err != nil {
		log.Fatalf("Verify NewTxn failed: %v", err)
	}
	val, err := tx.Get(context.Background(), []byte("k_0_0"))
	if err != nil {
		log.Printf("Verify Get k_0_0 failed: %v", err)
	} else {
		fmt.Printf("Verify k_0_0: %s\n", string(val))
	}
}
