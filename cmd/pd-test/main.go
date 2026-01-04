package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"titankv/pd/api/pdpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	pdAddr      = flag.String("pd", "127.0.0.1:9000", "PD address")
	concurrency = flag.Int("c", 10, "Concurrency")
	totalReqs   = flag.Int("n", 100000, "Total requests")
)

func main() {
	flag.Parse()

	conn, err := grpc.Dial(*pdAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := pdpb.NewPDClient(conn)

	// 1. 功能验证：简单获取
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := client.GetTS(ctx, &pdpb.GetTSRequest{Count: 1})
	cancel()
	if err != nil {
		log.Fatalf("First GetTS failed: %v", err)
	}
	fmt.Printf("Initial TS: Physical=%d, Logical=%d\n", resp.Timestamp.Physical, resp.Timestamp.Logical)

	// 2. 性能压测
	fmt.Printf("Starting benchmark: %d requests, %d concurrency...\n", *totalReqs, *concurrency)
	
	var wg sync.WaitGroup
	var lastTS int64 = 0 // 用于简单的单调性检查 (非严格，并发下不好查)
	
	start := time.Now()
	reqsPerWorker := *totalReqs / *concurrency

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < reqsPerWorker; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				resp, err := client.GetTS(ctx, &pdpb.GetTSRequest{Count: 1})
				cancel()
				
				if err != nil {
					log.Printf("GetTS error: %v", err)
					return
				}
				
				// 简单的原子更新检查，只记录最新的
				tsCombined := (resp.Timestamp.Physical << 18) + resp.Timestamp.Logical
				old := atomic.LoadInt64(&lastTS)
				if tsCombined > old {
					atomic.StoreInt64(&lastTS, tsCombined)
				}
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)
	qps := float64(*totalReqs) / duration.Seconds()

	fmt.Printf("Done.\n")
	fmt.Printf("Time elapsed: %v\n", duration)
	fmt.Printf("QPS: %.2f\n", qps)
	fmt.Printf("Last TS: %d\n", atomic.LoadInt64(&lastTS))
}