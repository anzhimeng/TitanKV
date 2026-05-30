package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"titankv/api/titankvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 假设 Node 1 是 Leader
const targetNodeAddr = "127.0.0.1:9091"
const targetRegionID = 1

func main() {
	// 1. 建立连接
	conn, err := grpc.Dial(targetNodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	c := titankvpb.NewTitanKVClient(conn)

	key := []byte("consistency-key")
	val := []byte("consistency-value-v1")
	
	// 构造 Context
	regionCtx := &titankvpb.RegionContext{
		RegionId: targetRegionID,
		// 如果 Server 开启了 Epoch 校验，需要填入正确的 Epoch
		RegionEpoch: &titankvpb.RegionEpoch{ConfVer: 1, Version: 1},
		Peer: &titankvpb.Peer{Id: 1, StoreId: 1}, // 假设发给 Node 1
	}

	// 2. 写入初始数据
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	log.Printf("Writing initial data to %s...", targetNodeAddr)
	
	_, err = c.Put(ctx, &titankvpb.PutRequest{
		Context: regionCtx, // 【关键修改】
		Key:     key,
		Value:   val,
	})
	cancel()

	if err != nil {
		log.Fatalf("❌ Init Put failed: %v. Is the cluster running?", err)
	}
	log.Println("✅ Init Put success!")

	fmt.Println("\n========================================================")
	fmt.Println("现在开始连续读取测试。")
	fmt.Println("请在另一个终端中，杀掉 Node 2 和 Node 3 (模拟网络分区)。")
	fmt.Println("========================================================")

	// 3. 循环读取
	counter := 1
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		
		start := time.Now()
		resp, err := c.Get(ctx, &titankvpb.GetRequest{
			Context: regionCtx, // 【关键修改】
			Key:     key,
		})
		duration := time.Since(start)
		cancel()

		if err != nil {
			log.Printf("[%d] ❌ Read FAILED (%v): %v", counter, duration, err)
		} else {
			log.Printf("[%d] ✅ Read SUCCESS (%v): %s", counter, duration, string(resp.Value))
		}

		counter++
		time.Sleep(1 * time.Second)
	}
}
