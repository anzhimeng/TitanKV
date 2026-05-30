package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"titankv/pkg/client"
	"titankv/pkg/txn"
)

var (
	pdAddr      = flag.String("pd-addr", "127.0.0.1:9000", "pd grpc address")
	concurrency = flag.Int("c", 100, "concurrency")
	requests    = flag.Int("n", 200, "requests per client")
	keySize     = flag.Int("k", 8, "key size")
	valueSize   = flag.Int("v", 128, "value size")
	useTxn      = flag.Bool("txn", true, "use transaction api (true) or raw put (false)")
	keyCount    = flag.Int("keys", 10000, "number of keys")
)

func main() {
	flag.Parse()
	log.Printf("Starting benchmark: c=%d, n=%d, keys=%d, v=%d, pd=%s, txn=%v", *concurrency, *requests, *keyCount, *valueSize, *pdAddr, *useTxn)

	c, err := client.NewClient(*pdAddr)
	if err != nil {
		log.Fatalf("create client failed: %v", err)
	}

	var ops uint64
	var errs uint64
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, *concurrency**requests)

	var wg sync.WaitGroup
	start := time.Now()

	// Progress reporter
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		lastOps := uint64(0)
		for {
			select {
			case <-ticker.C:
				curOps := atomic.LoadUint64(&ops)
				curErrs := atomic.LoadUint64(&errs)
				log.Printf("Progress: %d ops (%d errors), rate: %.1f ops/s", 
					curOps, curErrs, float64(curOps-lastOps)/5.0)
				lastOps = curOps
				if curOps >= uint64(*concurrency**requests) {
					return
				}
			}
		}
	}()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			
			for j := 0; j < *requests; j++ {
				key := []byte(fmt.Sprintf("k-%08d", r.Intn(*keyCount)))
				val := make([]byte, *valueSize)
				r.Read(val)

				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				begin := time.Now()
				var err error
				for retry := 0; retry < 5; retry++ {
					if *useTxn {
						t, e := txn.NewTransaction(ctx, c)
						if e != nil {
							err = e
						} else {
							t.Set(key, val)
							err = t.Commit(ctx)
						}
					} else {
						err = c.Put(ctx, key, val)
					}
					if err == nil {
						break
					}
					time.Sleep(time.Duration(retry*10) * time.Millisecond)
				}
				cancel()
				elapsed := time.Since(begin)

				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()

				if err != nil {
					atomic.AddUint64(&errs, 1)
					if j == 0 {
						log.Printf("Worker %d first error: %v", workerID, err)
					}
				}
				atomic.AddUint64(&ops, 1)
			}
		}(i)
	}

	wg.Wait()
	totalTime := time.Since(start)

	mu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p50, p95, p99 time.Duration
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]
	}
	mu.Unlock()

	throughput := float64(ops) / totalTime.Seconds()
	log.Printf("Benchmark done. Duration: %v", totalTime)
	log.Printf("Ops: %d, Errs: %d, TPS: %.2f", ops, errs, throughput)
	log.Printf("P50: %v, P95: %v, P99: %v", p50, p95, p99)
}
