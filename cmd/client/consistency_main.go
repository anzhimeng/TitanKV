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

// 假设 Node 1 是 Leader，或者你只连接 Node 1
const targetNodeAddr = "127.0.0.1:9091"

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

	// 2. 写入初始数据 (确保集群正常时能写进去)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	log.Printf("Writing initial data to %s...", targetNodeAddr)
	_, err = c.Put(ctx, &titankvpb.PutRequest{Key: key, Value: val})
	cancel()

	if err != nil {
		log.Fatalf("❌ Init Put failed: %v. Is the cluster running?", err)
	}
	log.Println("✅ Init Put success!")

	fmt.Println("\n========================================================")
	fmt.Println("现在开始连续读取测试。")
	fmt.Println("请在另一个终端中，杀掉 Node 2 和 Node 3 (模拟网络分区)。")
	fmt.Println("预期行为：")
	fmt.Println("  - 正常情况：读取成功，返回 value")
	fmt.Println("  - 杀掉节点后：读取应该阻塞直到超时 (因为 ReadIndex 无法达成 Quorum)")
	fmt.Println("  - 错误行为：如果杀掉后依然能读到数据，说明发生了【脏读】，一致性未保证！")
	fmt.Println("========================================================\n")

	// 3. 循环读取，观察行为
	counter := 1
	for {
		// 给每次 Get 设置 2 秒超时
		// 如果 ReadIndex 无法确认 Leader 身份，它会卡住，直到这个 2 秒超时
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		
		start := time.Now()
		resp, err := c.Get(ctx, &titankvpb.GetRequest{Key: key})
		duration := time.Since(start)
		cancel()

		if err != nil {
			// 這是我們期待在分区发生后看到的错误！
			log.Printf("[%d] ❌ Read FAILED (%v): %v", counter, duration, err)
		} else {
			log.Printf("[%d] ✅ Read SUCCESS (%v): %s", counter, duration, string(resp.Value))
		}

		counter++
		time.Sleep(1 * time.Second)
	}
}