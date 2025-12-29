package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"titankv/api/titankvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// 配置参数
	targetAddr    = "127.0.0.1:9091" // Leader 地址
	concurrency   = 20               // 并发 Worker 数
	reqPerWorker  = 2000             // 每个 Worker 请求次数
	totalRequests = concurrency * reqPerWorker
)

func main() {
	fmt.Printf("🔥 TitanKV 性能压测工具\n")
	fmt.Printf("配置: Concurrency=%d, ReqPerWorker=%d, Total=%d\n", concurrency, reqPerWorker, totalRequests)
	fmt.Println("------------------------------------------------")

	// 1. 数据准备阶段 (Write)
	fmt.Println(">> 阶段 1: 准备数据 (Pre-filling)...")
	prepareData()
	
	fmt.Println(">> 数据准备完成。等待 2秒 让状态机追赶...")
	time.Sleep(2 * time.Second)

	// 2. 读取压测阶段 (Read)
	fmt.Println("\n>> 阶段 2: 开始读取压测 (Benchmarking Get)...")
	runReadBenchmark()

	// 3. 最终验证
	verifyLastKey()
}

func prepareData() {
	var wg sync.WaitGroup
	var successOps uint64

	// 写入并发度可以稍微低一点，保证顺序性
	writeConcurrency := 10
	writeReqs := totalRequests / writeConcurrency

	start := time.Now()

	for i := 0; i < writeConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			conn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Worker %d connect fail: %v", workerID, err)
				return
			}
			defer conn.Close()
			c := titankvpb.NewTitanKVClient(conn)

			startID := workerID * writeReqs
			endID := startID + writeReqs

			for id := startID; id < endID; id++ {
				key := fmt.Sprintf("bench-key-%d", id)
				val := fmt.Sprintf("bench-val-%d", id)

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 写入超时给长一点
				_, err := c.Put(ctx, &titankvpb.PutRequest{Key: []byte(key), Value: []byte(val)})
				cancel()

				if err == nil {
					atomic.AddUint64(&successOps, 1)
				} else {
					// 写入失败打印日志
					log.Printf("Write failed key=%s: %v", key, err)
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("   写入完成: %d/%d, 耗时: %v\n", successOps, totalRequests, time.Since(start))
}

func runReadBenchmark() {
	var wg sync.WaitGroup
	var successOps uint64
	var failOps uint64
	var mismatchOps uint64 // 数据不一致计数

	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Worker %d connect fail: %v", workerID, err)
				return
			}
			defer conn.Close()
			c := titankvpb.NewTitanKVClient(conn)

			baseID := workerID * reqPerWorker

			for j := 0; j < reqPerWorker; j++ {
				keyID := baseID + j
				key := fmt.Sprintf("bench-key-%d", keyID)
				expectedVal := fmt.Sprintf("bench-val-%d", keyID) // 期望的值

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, err := c.Get(ctx, &titankvpb.GetRequest{Key: []byte(key)})
				cancel()

				if err != nil {
					currFail := atomic.AddUint64(&failOps, 1)
					if currFail <= 5 {
						log.Printf("Worker %d Read Error: %v", workerID, err)
					}
				} else {
					// 【增强】校验数据内容
					if string(resp.Value) != expectedVal {
						currMismatch := atomic.AddUint64(&mismatchOps, 1)
						if currMismatch <= 5 {
							log.Printf("❌ 数据不一致! Key=%s, Want=%s, Got=%s", key, expectedVal, string(resp.Value))
						}
					} else {
						atomic.AddUint64(&successOps, 1)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	tps := float64(successOps) / duration.Seconds()

	fmt.Println("\n================ 压测结果 (Read) ================")
	fmt.Printf("总耗时:     %v\n", duration)
	fmt.Printf("成功请求:   %d\n", successOps)
	fmt.Printf("失败请求:   %d\n", failOps)
	fmt.Printf("数据错误:   %d\n", mismatchOps)
	fmt.Printf("Read TPS:   %.2f req/s\n", tps)
	fmt.Println("=================================================")
}

func verifyLastKey() {
	conn, _ := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	c := titankvpb.NewTitanKVClient(conn)
	
	lastID := totalRequests - 1
	testKey := fmt.Sprintf("bench-key-%d", lastID)
	expectedVal := fmt.Sprintf("bench-val-%d", lastID)
	
	fmt.Printf("\n正在通过 ReadIndex 验证最后一条数据: %s...\n", testKey)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.Get(ctx, &titankvpb.GetRequest{Key: []byte(testKey)})
	
	if err != nil {
		fmt.Printf("❌ 最终验证失败: %v\n", err)
	} else if string(resp.Value) != expectedVal {
		fmt.Printf("❌ 最终验证数据不匹配! Got: %s\n", string(resp.Value))
	} else {
		fmt.Printf("✅ 最终验证成功! Value: %s\n", string(resp.Value))
	}
}