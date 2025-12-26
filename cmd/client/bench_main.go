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
	// 配置压测参数
	targetAddr  = "127.0.0.1:9091" // 连接 Node 1
	concurrency = 20               // 并发协程数
	totalRequests = 2000           // 每个协程发送的请求数 (总共 40,000)
)

func main() {
	fmt.Printf("🚀 开始压测...\n并发数: %d, 每个协程请求数: %d, 总计: %d\n\n", 
		concurrency, totalRequests, concurrency*totalRequests)

	var wg sync.WaitGroup
	var successCount uint64
	var failCount uint64

	startTime := time.Now()

	// 启动并发 Worker
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			// 每个 Worker 建立自己的连接 (模拟多个客户端)
			conn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Worker %d: 无法连接: %v", workerID, err)
				return
			}
			defer conn.Close()
			client := titankvpb.NewTitanKVClient(conn)

			for j := 0; j < totalRequests; j++ {
				key := fmt.Sprintf("bench-key-%d-%d", workerID, j)
				val := fmt.Sprintf("value-%d", j)

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := client.Put(ctx, &titankvpb.PutRequest{
					Key:   []byte(key),
					Value: []byte(val),
				})
				cancel()

				if err != nil {
					atomic.AddUint64(&failCount, 1)
				} else {
					atomic.AddUint64(&successCount, 1)
				}

				// 每完成 500 个打印一次进度
				if (j+1)%500 == 0 {
					fmt.Printf("Worker %d 已完成 %d 个请求\n", workerID, j+1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	tps := float64(successCount) / duration.Seconds()

	fmt.Println("\n================ 压测报告 ================")
	fmt.Printf("总计耗时:   %v\n", duration)
	fmt.Printf("成功请求:   %d\n", successCount)
	fmt.Printf("失败请求:   %d\n", failCount)
	fmt.Printf("平均 TPS:   %.2f req/s\n", tps)
	fmt.Println("==========================================")

	// 验证一致性：读回最后一个 Key
	verifyConsistency()
}

func verifyConsistency() {
	conn, _ := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	c := titankvpb.NewTitanKVClient(conn)
	
	testKey := fmt.Sprintf("bench-key-%d-%d", concurrency-1, totalRequests-1)
	fmt.Printf("\n正在通过 ReadIndex 验证最后一条数据: %s...\n", testKey)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := c.Get(ctx, &titankvpb.GetRequest{Key: []byte(testKey)})
	
	if err != nil {
		fmt.Printf("❌ 验证失败: %v\n", err)
	} else {
		fmt.Printf("✅ 验证成功! Value: %s\n", string(resp.Value))
	}
}