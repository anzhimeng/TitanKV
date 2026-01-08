package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"titankv/api/titankvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	targetAddr = "127.0.0.1:9091"
	// Key: bench-key-0 ... bench-key-9999
	// Value: 1KB
	// Total: 10MB
	totalKeys = 2000
	valueSize = 1024 
)

func main() {
	conn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connect fail: %v", err)
	}
	defer conn.Close()
	c := titankvpb.NewTitanKVClient(conn)

	fmt.Println(">> Phase 1: Injecting Data to Trigger Split...")
	
	// 1. 写入数据
	val := strings.Repeat("x", valueSize)
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("bench-key-%05d", i) // 格式化为 bench-key-00001，保证有序
		
		req := &titankvpb.PutRequest{
			Context: &titankvpb.RegionContext{RegionId: 1}, // 始终发给 Region 1
			Key:     []byte(key),
			Value:   []byte(val),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.Put(ctx, req)
		cancel()

		if err != nil {
			// 如果报错 "key not in region"，说明已经分裂了！
			log.Printf("Put %s failed: %v (Maybe split happened?)", key, err)
		}

		if i%100 == 0 {
			fmt.Printf("   Written %d keys...\r", i)
		}
	}
	fmt.Println("\n>> Phase 1 Done.")

	// 2. 等待 Split 完成
	fmt.Println(">> Waiting for Split (10s)...")
	time.Sleep(10 * time.Second)

	// 3. 验证新 Region (尝试写入两个极端的 Key)
	// 假设 SplitKey 是 "bench-key-50000" (Day 2 Mock 的)
	// 或者 "bench-key-01000" (中间)
	
	// 我们尝试写一个很大的 Key，它应该属于新 Region
	// 但如果我们发给 Region 1，应该报错
	testKey := "bench-key-99999"
	log.Printf(">> Testing Key %s on Region 1...", testKey)
	
	req := &titankvpb.PutRequest{
		Context: &titankvpb.RegionContext{RegionId: 1},
		Key:     []byte(testKey),
		Value:   []byte("val"),
	}
	_, err = c.Put(context.Background(), req)
	if err != nil {
		log.Printf("✅ Expected Error: %v (Region 1 rejected it, Split success!)", err)
	} else {
		log.Printf("❌ Unexpected Success: Region 1 accepted it. Split failed.")
	}
}