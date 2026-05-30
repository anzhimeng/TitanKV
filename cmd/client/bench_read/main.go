package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"titankv/pkg/client"
	"titankv/pkg/txn"
)

func main() {
	pdAddr := flag.String("pd", "127.0.0.1:2379", "pd address")
	concurrency := flag.Int("c", 50, "concurrency")
	reqPerWorker := flag.Int("n", 200, "requests per worker")
	flag.Parse()

	totalRequests := (*concurrency) * (*reqPerWorker)
	fmt.Printf("🔥 TitanKV ReadIndex Benchmark\n")
	fmt.Printf("Config: Concurrency=%d, ReqPerWorker=%d, Total=%d\n", *concurrency, *reqPerWorker, totalRequests)
	fmt.Println("------------------------------------------------")

	c, err := client.NewClient(*pdAddr)
	if err != nil {
		log.Fatalf("client init fail: %v", err)
	}

	// 1. Write some data first
	fmt.Println("Pre-filling data...")
	ctx := context.Background()
	key := []byte("bench-read-key")
	val := []byte("bench-read-val")
	
	// Use Transaction for Put
	tx, err := txn.NewTransaction(ctx, c)
	if err != nil {
		log.Fatalf("NewTransaction failed: %v", err)
	}
	tx.Set(key, val)
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("Commit failed: %v", err)
	}

	// 2. Benchmark Reads
	var wg sync.WaitGroup
	var successOps uint64
	var failOps uint64
	
	startTime := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for j := 0; j < *reqPerWorker; j++ {
				// Use random sleep to simulate real workload distribution
				time.Sleep(time.Duration(r.Intn(5)) * time.Millisecond)

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				
				// Use Transaction for Get (Snapshot Read)
				// Note: NewTransaction gets a TS from PD. For pure ReadIndex bench, 
				// we might want to cache TS to isolate ReadIndex performance, 
				// but correct usage is getting new TS.
				// Since client has TS batching, this should be fine.
				tx, err := txn.NewTransaction(ctx, c)
				if err != nil {
					cancel()
					atomic.AddUint64(&failOps, 1)
					if atomic.LoadUint64(&failOps) <= 5 {
						log.Printf("NewTransaction failed: %v", err)
					}
					continue
				}
				
				_, err = tx.Get(ctx, key)
				cancel()
				
				if err != nil {
					atomic.AddUint64(&failOps, 1)
					if atomic.LoadUint64(&failOps) <= 5 {
						log.Printf("Get failed: %v", err)
					}
				} else {
					atomic.AddUint64(&successOps, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	tps := float64(successOps) / duration.Seconds()

	fmt.Println("\n================ Benchmark Result (Read) ================")
	fmt.Printf("Total Time:     %v\n", duration)
	fmt.Printf("Success:        %d\n", successOps)
	fmt.Printf("Failed:         %d\n", failOps)
	fmt.Printf("TPS:            %.2f ops/s\n", tps)
	fmt.Printf("Avg Latency:    %v\n", duration/time.Duration(successOps+failOps))
}
