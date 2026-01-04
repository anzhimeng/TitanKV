package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"titankv/api/titankvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	targetAddr    = "127.0.0.1:9091"
	concurrency   = 20               // 并发线程数
	totalKeys     = 100000           // Key 的总空间大小 (0 ~ 99999)
	duration      = 5 * time.Minute  // 压测持续时间 (建议至少 5 分钟)
	valueSize     = 4096             // 4KB Value (处于 Blob 分离的边缘)
)

var (
	opsCount    uint64
	errorsCount uint64
)

func main() {
	fmt.Println("🚀 TitanKV 终极稳定性压测 (Stability Test)")
	fmt.Printf("配置: 持续 %v, 并发 %d, Key空间 %d\n", duration, concurrency, totalKeys)
	fmt.Println("------------------------------------------------")

	// 1. 初始化数据 (Pre-fill)
	fmt.Println(">> Phase 1: Pre-filling data (顺序写入)...")
	fillData()
	
	// 2. 混合负载压测 (Random R/W/D)
	fmt.Println("\n>> Phase 2: Mixed Workload (读/写/删/覆盖)...")
	fmt.Println("   (请在此时观察 Server 端的 Compaction 和 GC 日志)")
	runMixedWorkload()

	fmt.Println("\n✅ 压测结束。")
	fmt.Printf("Total Ops: %d, Errors: %d\n", opsCount, errorsCount)
}

func getClient() (*grpc.ClientConn, titankvpb.TitanKVClient) {
	conn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connect fail: %v", err)
	}
	return conn, titankvpb.NewTitanKVClient(conn)
}
func fillData() {
	conn, c := getClient()
	defer conn.Close()

	// 顺序写入，建立基础数据
	for i := 0; i < totalKeys; i++ {
		if i%10000 == 0 {
			fmt.Printf("   Filled %d/%d keys...\r", i, totalKeys)
		}
		key := fmt.Sprintf("k-%d", i)
		val := make([]byte, valueSize)
		rand.Read(val) // 随机内容
		
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.Put(ctx, &titankvpb.PutRequest{Key: []byte(key), Value: val})
		cancel()
		
		if err != nil {
			log.Fatalf("Init failed at %d: %v", i, err)
		}
	}
	fmt.Printf("   Filled %d keys. Done.\n", totalKeys)
}

func runMixedWorkload() {
	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)
	
	// 启动监控协程
	go func() {
		for time.Now().Before(deadline) {
			time.Sleep(5 * time.Second)
			currentOps := atomic.LoadUint64(&opsCount)
			fmt.Printf("[%s] Total Ops: %d, Errors: %d (Running...)\n", 
				time.Now().Format("15:04:05"), currentOps, atomic.LoadUint64(&errorsCount))
		}
	}()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Worker %d connect fail", id)
				return
			}
			defer conn.Close()
			c := titankvpb.NewTitanKVClient(conn)

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for time.Now().Before(deadline) {
				keyID := r.Intn(totalKeys) // 在 Key 空间内随机
				key := fmt.Sprintf("k-%d", keyID)
				
				op := r.Intn(100)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				
				var err error
				if op < 50 { 
					// 50% 概率：覆盖写 (Overwrite) -> 触发 GC
					val := make([]byte, r.Intn(valueSize)+100) // 随机大小
					_, err = c.Put(ctx, &titankvpb.PutRequest{Key: []byte(key), Value: val})
				} else if op < 90 {
					// 40% 概率：读取 (Read) -> 验证数据存在性
					_, err = c.Get(ctx, &titankvpb.GetRequest{Key: []byte(key)})
				} else {
					// 10% 概率：删除 (Delete) -> 产生 Tombstone -> 触发 Compaction Drop
					_, err = c.Delete(ctx, &titankvpb.DeleteRequest{Key: []byte(key)})
				}
				cancel()

				if err != nil {
					atomic.AddUint64(&errorsCount, 1)
					// log.Printf("Err: %v", err) // 错误太多时打开
				}
				atomic.AddUint64(&opsCount, 1)
				
				// 稍微 sleep 模拟真实负载，避免把客户端 CPU 跑满
				time.Sleep(time.Millisecond * 5)
			}
		}(i)
	}

	wg.Wait()
}